// List available archive from a source, and plan the ingest, excluding works already done
// Currently only support Jazza's archives
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Run represents the structure of your JSON data
type Run struct {
	Started        time.Time `json:"started"`
	Finished       time.Time `json:"finished"`
	TotalFilesMade int       `json:"totalFilesMade"`
	ArchiveIndex   int       `json:"archiveIndex"`
}

// HTTPConfig holds configuration for HTTP requests
type HTTPConfig struct {
	ArchiveURL string
	LogsURL    string
	Username   string
	Password   string
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
		logfile TEXT NOT NULL,
		started TEXT NOT NULL,
		finished TEXT NOT NULL,
		total_files_made INTEGER NOT NULL,
		archive_index INTEGER NOT NULL,
		processed_version TEXT
	);`

	_, err := dbm.db.Exec(createTableSQL)
	return err
}

// SaveRun inserts a run record into the database
func (dbm *MetadataDB) SaveRun(logfile string, run Run) error {
	insertSQL := `
	INSERT INTO runs (
		logfile,
		started,
		finished,
		total_files_made,
		archive_index,
		processed_version
	) VALUES (?, ?, ?, ?, ?, ?)`

	_, err := dbm.db.Exec(insertSQL,
		logfile,
		run.Started.Format(time.RFC3339),
		run.Finished.Format(time.RFC3339),
		run.TotalFilesMade,
		run.ArchiveIndex,
		ProcessedVersionFromDate(run.Started),
	)

	return err
}

// Close closes the database connection
func (dbm *MetadataDB) Close() error {
	return dbm.db.Close()
}

// GetExistingFiles returns a list of files that are already in the database
func (dbm *MetadataDB) GetExistingFiles() (map[string]bool, error) {
	query := "SELECT logfile FROM runs"
	rows, err := dbm.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query existing files: %w", err)
	}
	defer rows.Close()

	existingFiles := make(map[string]bool)
	for rows.Next() {
		var logfile string
		if err := rows.Scan(&logfile); err != nil {
			return nil, fmt.Errorf("failed to scan logfile: %w", err)
		}
		existingFiles[logfile] = true
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

	client := &http.Client{}

	// Try to get directory listing by requesting the base URL
	req, err := http.NewRequest("GET", dbm.config.LogsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add authentication if configured
	if dbm.config.Username != "" && dbm.config.Password != "" {
		req.SetBasicAuth(dbm.config.Username, dbm.config.Password)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch directory listing: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch directory listing: status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// Parse HTML content to extract JSON file names
	// This is a simple approach that looks for run-*.json patterns
	content := string(body)
	var files []string

	// Look for run-*.json patterns in the HTML content
	lines := strings.Split(content, "\n")
	for _, line := range lines {
		if strings.Contains(line, "run-") && strings.Contains(line, ".json") {
			// Extract filename from HTML content (simple approach)
			startIdx := strings.Index(line, "run-")
			if startIdx != -1 {
				remaining := line[startIdx:]
				endIdx := strings.Index(remaining, ".json")
				if endIdx != -1 {
					filename := remaining[:endIdx+5] // +5 to include ".json"
					files = append(files, filename)
				}
			}
		}
	}

	return files, nil
}

// ImportMissingFiles imports only the files that are not already in the database
func (dbm *MetadataDB) ImportMissingFiles() error {
	// Get existing files from database
	existingFiles, err := dbm.GetExistingFiles()
	if err != nil {
		return fmt.Errorf("failed to get existing files: %w", err)
	}

	// Get list of files from HTTP server
	availableFiles, err := dbm.ListFilesFromHTTP()
	if err != nil {
		return fmt.Errorf("failed to list files from HTTP: %w", err)
	}

	// Find missing files
	var missingFiles []string
	for _, filename := range availableFiles {
		if !existingFiles[filename] {
			missingFiles = append(missingFiles, filename)
		}
	}

	if len(missingFiles) == 0 {
		log.Println("No new files to import")
		return nil
	}

	log.Printf("Found %d new files to import: %v", len(missingFiles), missingFiles)

	// Import only the missing files
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

	client := &http.Client{}

	for _, filename := range files {
		url := dbm.config.LogsURL + "/" + filename

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return fmt.Errorf("failed to create request for %s: %w", filename, err)
		}

		// Add authentication if configured
		if dbm.config.Username != "" && dbm.config.Password != "" {
			req.SetBasicAuth(dbm.config.Username, dbm.config.Password)
		}

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to fetch %s: %w", filename, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to fetch %s: status %d", filename, resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body for %s: %w", filename, err)
		}

		var run Run
		err = json.Unmarshal(body, &run)
		if err != nil {
			return fmt.Errorf("failed to parse JSON from %s: %w", filename, err)
		}

		// Save the run to database
		err = dbm.SaveRun(filename, run)
		if err != nil {
			return fmt.Errorf("failed to save run from %s: %w", filename, err)
		}

		log.Printf("Successfully imported run from %s", filename)
	}

	return nil
}

type Archive struct {
	datetime time.Time
	index    int
	version  string
}

func (dbm *MetadataDB) ListArchives() ([]Archive, error) {
	query := "SELECT started, archive_index, processed_version FROM runs ORDER BY date(started) ASC"
	rows, err := dbm.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query archives: %w", err)
	}
	defer rows.Close()

	var archives []Archive
	for rows.Next() {
		var started string
		var index int
		var version string
		if err := rows.Scan(&started, &index, &version); err != nil {
			return nil, fmt.Errorf("failed to scan archive row: %w", err)
		}
		t, err := time.Parse(time.RFC3339, started)
		if err != nil {
			return nil, fmt.Errorf("failed to parse started time: %w", err)
		}
		archives = append(archives, Archive{datetime: t, index: index, version: version})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating archive rows: %w", err)
	}
	return archives, nil
}

type Job struct {
	isDiff              bool
	base                string
	archive             Archive
	archiveFile         string
	archiveFileExists   bool
	processedFile       string
	processedFileExists bool
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
		if currentDay == archive.datetime.Day() {
			continue
		}
		currentDay = archive.datetime.Day()
		// Every 7 days, one get promoted to "base"
		s := strings.Split(strings.TrimPrefix(archive.version, "v"), ".")
		if len(s) != 2 {
			return nil, fmt.Errorf("bad version format %s", archive.version)
		}
		week := s[0]
		isDiff := true
		if week != currentWeek {
			// New base
			currentWeek = week
			isDiff = false
			// change version for base to "vX" only
			s := strings.Split(archive.version, ".")
			archive.version = s[0]
		}
		job := Job{
			isDiff:  isDiff,
			archive: archive,
		}
		CheckJobDone(&job, dbm.archiveFolder, dbm.processedFolder)
		if isDiff {
			job.base = currentBase
		} else {
			currentBase = job.processedFile
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func CheckJobDone(job *Job, archiveFolder, processedFolder string) {
	archiveFile := fmt.Sprintf("tiles-%d.7z", job.archive.index)
	job.archiveFile = archiveFile
	job.archiveFileExists = false
	if archiveFolder != "" {
		if _, err := os.Stat(filepath.Join(archiveFolder, archiveFile)); err == nil {
			job.archiveFileExists = true
		}
	}

	processedFile := fmt.Sprintf("%s_%s.db", job.archive.version, job.archive.datetime.Format("2006-01-02T15"))
	job.processedFile = processedFile
	job.processedFileExists = false
	if processedFolder != "" {
		if _, err := os.Stat(filepath.Join(processedFolder, processedFile)); err == nil {
			job.processedFileExists = true
		}
	}
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
		ArchiveURL: os.Getenv("JAZZA_URL"),
		LogsURL:    os.Getenv("JAZZA_LOGS_URL"),
		Username:   os.Getenv("JAZZA_USER"),
		Password:   os.Getenv("JAZZA_PASSW"),
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
	for _, p := range plan {
		if p.processedFileExists {
			continue
		}

		base := ""
		if p.isDiff {
			base = fmt.Sprintf("--base %s", path.Join(doneFolder, p.base))
		}
		archive := path.Join(archivesFolder, p.archiveFile)
		from := path.Join("/dev/shm/wplace-tmpdata", strings.TrimSuffix(p.archiveFile, ".7z"))
		out := path.Join(tmpProcessedFolder, p.processedFile)

		if !p.archiveFileExists {
			planCommands += fmt.Sprintf("curl -u '%s:%s' -o %s %s/%s\n", httpConfig.Username, httpConfig.Password, archive, httpConfig.ArchiveURL, p.archiveFile)
		}
		planCommands += fmt.Sprintf("rm -rf /dev/shm/wplace-tmpdata/ && mkdir /dev/shm/wplace-tmpdata && 7z x %s -o/dev/shm/wplace-tmpdata/\n", archive)
		planCommands += fmt.Sprintf("./ingest %s --from %s --out %s\n", base, from, out)
		planCommands += "rm -rf /dev/shm/wplace-tmpdata/\n"
		planCommands += fmt.Sprintf("rm %s\n", archive)
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
