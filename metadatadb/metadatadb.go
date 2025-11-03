// List available archive from a source, and plan the ingest, excluding works already done
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// githubRelease is a small helper type for the GitHub API response we use.
type githubRelease struct {
	Name      string    `json:"name"`
	ID        int       `json:"id"`
	UpdatedAt time.Time `json:"updated_at"`
	Assets    []Asset   `json:"assets"`
}

type Archive struct {
	Name             string
	ID               int
	Datetime         time.Time
	LastUpdated      time.Time
	Assets           []Asset
	ProcessedVersion string
}

type Asset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

func AssetsFromJson(data string) ([]Asset, error) {
	var assets []Asset
	err := json.Unmarshal([]byte(data), &assets)
	if err != nil {
		log.Printf("Failed to unmarshal assets JSON: %v", err)
		return nil, err
	}
	return assets, nil
}

func AssetsToJson(assets []Asset) (string, error) {
	data, err := json.Marshal(assets)
	if err != nil {
		log.Printf("Failed to marshal assets to JSON: %v", err)
		return "[]", err
	}
	return string(data), nil
}

// HTTPConfig holds configuration for HTTP requests
type HTTPConfig struct {
	ArchiveURL string
}

// MetadataDB handles SQLite operations
type MetadataDB struct {
	db              *sql.DB
	config          *HTTPConfig
	archiveFolder   string
	processedFolder string
}

// NewMetadataDB creates a new database manager
func NewMetadataDB(dbPath string) (*MetadataDB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	dbm := &MetadataDB{db: db}
	err = dbm.initTable()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize table: %w", err)
	}

	return dbm, nil
}

// NewMetadataDBWithConfig creates a new database manager with HTTP configuration
func NewMetadataDBWithConfig(dbPath string, config *HTTPConfig) (*MetadataDB, error) {
	dbm, err := NewMetadataDB(dbPath)
	if err != nil {
		return nil, err
	}
	dbm.SetHTTPConfig(config)

	return dbm, nil
}

func (dbm *MetadataDB) SetFolders(archiveFolder, processedFolder string) {
	dbm.archiveFolder = archiveFolder
	dbm.processedFolder = processedFolder
}

// initTable creates the runs table if it doesn't exist
func (dbm *MetadataDB) initTable() error {
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS runs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		started TEXT NOT NULL,
		finished TEXT NOT NULL,
		release_id INTEGER NOT NULL,
		processed_version TEXT,
		assets JSON
	);`

	_, err := dbm.db.Exec(createTableSQL)
	return err
}

// SaveRun inserts a run record into the database
func (dbm *MetadataDB) SaveRun(archive Archive) error {
	insertSQL := `
	INSERT INTO runs (
		name,
		started,
		finished,
		release_id,
		processed_version,
		assets
	) VALUES (?, ?, ?, ?, ?, ?);`

	assets, err := AssetsToJson(archive.Assets)
	if err != nil {
		return fmt.Errorf("failed to convert assets to JSON: %w", err)
	}
	_, err = dbm.db.Exec(insertSQL,
		archive.Name,
		archive.Datetime.Format(time.RFC3339),
		archive.LastUpdated.Format(time.RFC3339),
		archive.ID,
		ProcessedVersionFromDate(archive.Datetime),
		assets,
	)

	return err
}

// Close closes the database connection
func (dbm *MetadataDB) Close() error {
	return dbm.db.Close()
}

// GetExistingFiles returns a list of files that are already in the database
func (dbm *MetadataDB) GetExistingFiles() (map[string]bool, error) {
	query := "SELECT name FROM runs"
	rows, err := dbm.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query existing files: %w", err)
	}
	defer rows.Close()

	existingFiles := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan name: %w", err)
		}
		existingFiles[name] = true
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return existingFiles, nil
}

// ListFilesFromHTTP lists JSON files available at the base URL
func (dbm *MetadataDB) ListFilesFromHTTP() ([]string, error) {
	if dbm.config == nil {
		return nil, fmt.Errorf("HTTP configuration not set")
	}

	// If ArchiveURL points to a GitHub releases page, use the GitHub API to list releases.
	if strings.Contains(dbm.config.ArchiveURL, "github.com") {
		releases, err := getGithubReleases(dbm.config.ArchiveURL)
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(releases))
		for _, r := range releases {
			names = append(names, r.Name)
		}
		return names, nil
	}

	return nil, fmt.Errorf("unsupported ArchiveURL format for listing files")
}

// ImportMissingFiles imports only the files that are not already in the database
func (dbm *MetadataDB) ImportMissingFiles() error {
	// Get existing files from database
	existingFiles, err := dbm.GetExistingFiles()
	if err != nil {
		return fmt.Errorf("failed to get existing files: %w", err)
	}

	// Get list of runs from HTTP server (GitHub releases supported)
	availableFiles, err := dbm.ListFilesFromHTTP()
	if err != nil {
		return fmt.Errorf("failed to list files from HTTP: %w", err)
	}

	// Find missing releases
	var missingFiles []string
	for _, filename := range availableFiles {
		if !existingFiles[filename] {
			missingFiles = append(missingFiles, filename)
		}
	}

	if len(missingFiles) == 0 {
		log.Println("No new releases to import")
		return nil
	}

	log.Printf("Found %d new releases to import: %v", len(missingFiles), missingFiles)

	return dbm.ImportFromHTTP(missingFiles)
}

// SetHTTPConfig sets the HTTP configuration for the database manager
func (dbm *MetadataDB) SetHTTPConfig(config *HTTPConfig) {
	dbm.config = config
}

// ImportFromHTTP imports runs from JSON files available via HTTP
func (dbm *MetadataDB) ImportFromHTTP(files []string) error {
	if dbm.config == nil {
		return fmt.Errorf("HTTP configuration not set")
	}

	// If ArchiveURL is GitHub releases, fetch release metadata and create Archive objects from releases
	if strings.Contains(dbm.config.ArchiveURL, "github.com") {
		releases, err := getGithubReleases(dbm.config.ArchiveURL)
		if err != nil {
			return err
		}

		// Build a map of release name -> release
		relMap := make(map[string]githubRelease)
		for _, r := range releases {
			relMap[r.Name] = r
		}

		for _, filename := range files {
			r, ok := relMap[filename]
			if !ok {
				log.Printf("Release %s not found in GitHub releases, skipping", filename)
				continue
			}

			// Parse started time from release name. Release names are like world-2025-11-01T11-47-58.104Z
			started, err := parseReleaseTime(r.Name)
			if err != nil {
				log.Printf("failed to parse time for release %s: %v", r.Name, err)
				continue
			}

			archive := Archive{
				Name:        r.Name,
				ID:          r.ID,
				Datetime:    started,
				LastUpdated: r.UpdatedAt,
				Assets:      r.Assets,
			}

			if err := dbm.SaveRun(archive); err != nil {
				return fmt.Errorf("failed to save run from %s: %w", r.Name, err)
			}
			log.Printf("Imported release %s (assets: %d)", r.Name, len(r.Assets))
		}
		return nil
	}

	return fmt.Errorf("unsupported ArchiveURL format for importing files")
}

// getGithubReleases fetches releases for a GitHub repo given a releases URL like
// https://github.com/owner/repo/releases
func getGithubReleases(releasesURL string) ([]githubRelease, error) {
	if releasesURL == "" {
		return nil, fmt.Errorf("empty releases URL")
	}
	u, err := url.Parse(releasesURL)
	if err != nil {
		return nil, fmt.Errorf("invalid releases URL: %w", err)
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 2 {
		return nil, fmt.Errorf("cannot determine owner/repo from %s", releasesURL)
	}
	owner := parts[0]
	repo := parts[1]

	api := fmt.Sprintf("https://api.github.com/repos/%s/%s/releases", owner, repo)
	client := &http.Client{}
	req, err := http.NewRequest("GET", api, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/vnd.github.v3+json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch GitHub releases: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GitHub API returned status %d: %s", resp.StatusCode, string(body))
	}
	var releases []githubRelease
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&releases); err != nil {
		return nil, fmt.Errorf("failed to parse GitHub releases response: %w", err)
	}
	// Keep only releases last updated more than one hour ago to avoid incomplete releases
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	filtered := make([]githubRelease, 0, len(releases))
	for _, r := range releases {
		if r.UpdatedAt.Before(oneHourAgo) {
			filtered = append(filtered, r)
		}
	}
	return filtered, nil
}

// parseReleaseTime converts a release name like "world-2025-11-01T11-47-58.104Z" into a time.Time
func parseReleaseTime(name string) (time.Time, error) {
	// Remove optional prefix (e.g., "world-")
	s := strings.TrimPrefix(name, "world-")

	// Find 'T' and convert the two dashes between hour/minute and minute/second back to colons
	tIdx := strings.Index(s, "T")
	if tIdx == -1 {
		return time.Time{}, fmt.Errorf("invalid release time format: %s", s)
	}
	datePart := s[:tIdx]
	timePart := s[tIdx+1:]
	// timePart looks like 11-47-58.104Z or similar; replace first two '-' with ':'
	// Only replace the first two occurrences
	replaced := timePart
	for i := 0; i < 2; i++ {
		idx := strings.Index(replaced, "-")
		if idx == -1 {
			break
		}
		replaced = replaced[:idx] + ":" + replaced[idx+1:]
	}
	full := datePart + "T" + replaced
	// Try RFC3339 parse
	t, err := time.Parse(time.RFC3339, full)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse time %s: %w", full, err)
	}
	return t, nil
}

func (dbm *MetadataDB) ListArchives() ([]Archive, error) {
	query := "SELECT release_id, name, started, processed_version, assets FROM runs ORDER BY datetime(started) ASC"
	rows, err := dbm.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query archives: %w", err)
	}
	defer rows.Close()

	var archives []Archive
	for rows.Next() {
		var releaseID int
		var name string
		var started string
		var version string
		var assetsStr string
		if err := rows.Scan(&releaseID, &name, &started, &version, &assetsStr); err != nil {
			return nil, fmt.Errorf("failed to scan archive row: %w", err)
		}
		t, err := time.Parse(time.RFC3339, started)
		if err != nil {
			return nil, fmt.Errorf("failed to parse started time: %w", err)
		}
		assets, err := AssetsFromJson(assetsStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse assets JSON: %w", err)
		}
		archives = append(archives, Archive{ID: releaseID, Name: name, Datetime: t, ProcessedVersion: version, Assets: assets})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating archive rows: %w", err)
	}
	return archives, nil
}

type Job struct {
	isDiff        bool
	base          string
	archive       Archive
	processedFile string
}

func (dbm *MetadataDB) Plan() ([]Job, error) {
	archives, err := dbm.ListArchives()
	if err != nil {
		return nil, err
	}

	jobs := make([]Job, 0)
	currentWeek := ""
	currentDay := -1
	currentBase := ""
	for _, archive := range archives {
		// Keep one archive per day
		if currentDay == archive.Datetime.Day() {
			continue
		}
		currentDay = archive.Datetime.Day()
		// Every 7 days, one get promoted to "base"
		s := strings.Split(strings.TrimPrefix(archive.ProcessedVersion, "v"), ".")
		if len(s) != 2 {
			return nil, fmt.Errorf("bad version format %s", archive.ProcessedVersion)
		}
		week := s[0]
		isDiff := true
		if week != currentWeek {
			// New base
			currentWeek = week
			isDiff = false
			// change version for base to "vX" only
			s := strings.Split(archive.ProcessedVersion, ".")
			archive.ProcessedVersion = s[0]
		}
		job := Job{
			isDiff:  isDiff,
			archive: archive,
		}
		job.processedFile = fmt.Sprintf("%s_%s.db", archive.ProcessedVersion, archive.Datetime.Format("2006-01-02T15"))
		if isDiff {
			job.base = currentBase
		} else {
			currentBase = job.processedFile
		}
		jobs = append(jobs, job)
	}

	filteredJobs, err := dbm.FilterJobs(jobs)
	if err != nil {
		return nil, err
	}

	return filteredJobs, nil
}

// FilterJobs only keeps jobs where no processed file exists for the day
func (dbm *MetadataDB) FilterJobs(jobs []Job) ([]Job, error) {
	done := make(map[string]bool)
	entries, err := os.ReadDir(dbm.processedFolder)
	if err != nil {
		return nil, fmt.Errorf("failed to read processed folder: %w", err)
	} else {
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if strings.HasSuffix(name, ".db") && len(name) >= len("2006-01-02T15.db") {
				base := strings.TrimSuffix(name, ".db")
				datePart := base[len(base)-13:] // extract trailing datetime
				if t, err := time.Parse("2006-01-02T15", datePart); err == nil {
					done[t.Format("2006-01-02T15")] = true
				}
			}
		}
	}

	res := make([]Job, 0)
	for _, job := range jobs {
		day := job.archive.Datetime.Format("2006-01-02T15")
		if !done[day] {
			res = append(res, job)
		}
	}
	return res, nil
}

// ProcessedVersionFromDate returns version in the format vX.Y where:
// X: week number since 1st Jan 2025
// Y: hour in the week (from 0 to 167) (zero-padded to 3 digits)
func ProcessedVersionFromDate(datetime time.Time) string {
	epoch := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	weeksSince := int(datetime.Sub(epoch).Hours() / (24 * 7))
	hourInWeek := int(datetime.Sub(epoch).Hours()) % (24 * 7)
	return fmt.Sprintf("v%d.%03d", weeksSince, hourInWeek)
}

// Example usage
func main() {
	// Jazza's creds are available on their repo
	httpConfig := &HTTPConfig{
		ArchiveURL: os.Getenv("ARCHIVES_URL"),
	}

	dbm, err := NewMetadataDBWithConfig("metadata.db", httpConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer dbm.Close()

	workFolder := os.Getenv("META_WORK_FOLDER")
	doneFolder := os.Getenv("META_DONE_FOLDER")
	tmpProcessedFolder := path.Join(workFolder, "processed")
	archivesFolder := path.Join(workFolder, "archives")

	dbm.SetFolders(archivesFolder, doneFolder)

	err = dbm.ImportMissingFiles()
	if err != nil {
		log.Printf("Error importing missing files: %v", err)
	}

	plan, err := dbm.Plan()
	if err != nil {
		panic(err)
	}
	planCommands := "#!/bin/bash\nset -e\n\n"
	planCommands += fmt.Sprintf("mkdir -p %s\n", tmpProcessedFolder)
	planCommands += fmt.Sprintf("mkdir -p %s\n", archivesFolder)
	planCommands += "\n"
	for _, p := range plan {
		base := ""
		if p.isDiff {
			base = fmt.Sprintf("--base %s", path.Join(doneFolder, p.base))
		}
		tmp := "/dev/shm/wplace-tmpdata"
		out := path.Join(tmpProcessedFolder, p.processedFile)

		planCommands += fmt.Sprintf("echo 'Processing archive %s'\n", p.archive.Name)
		// Download archive assets
		assetsFolder := fmt.Sprintf("%s/%d", archivesFolder, p.archive.ID)
		planCommands += fmt.Sprintf("mkdir -p %s\n", assetsFolder)
		downloadAssetCommands := []string{}
		for _, asset := range p.archive.Assets {
			downloadAssetCommands = append(downloadAssetCommands, fmt.Sprintf("curl -L -o '%s/%s' '%s'", assetsFolder, asset.Name, asset.BrowserDownloadURL))
		}
		planCommands += strings.Join(downloadAssetCommands, " && ") + "\n"
		archive := path.Join(assetsFolder, "full.tar.gz")
		planCommands += fmt.Sprintf("cat %s/*.tar.gz.* > %s\n", assetsFolder, archive)

		planCommands += fmt.Sprintf("rm -rf %s && mkdir %s && tar -xzf %s -C %s --strip-components=1\n", tmp, tmp, archive, tmp)
		planCommands += fmt.Sprintf("./ingest %s --from %s --out %s\n", base, tmp, out)
		planCommands += fmt.Sprintf("rm -rf %s\n", tmp)
		planCommands += fmt.Sprintf("rm -rf %s\n", assetsFolder)
		planCommands += fmt.Sprintf("./merge %s --target %s\n", base, out)
		planCommands += fmt.Sprintf("sqlite3 %s 'PRAGMA journal_mode = DELETE;'\n", out)
		planCommands += fmt.Sprintf("mv %s %s\n", out, path.Join(doneFolder, p.processedFile))
		planCommands += "\n"
	}
	planCommands += "echo 'All done!'\n"

	err = os.WriteFile("plan.sh", []byte(planCommands), 0755)
	if err != nil {
		log.Fatalf("Failed to write plan.sh: %v", err)
	}
	log.Println("Plan written to plan.sh")
}
