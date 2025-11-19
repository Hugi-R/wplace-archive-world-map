package main

import (
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
)

// GithubRelease is a small helper type for the GitHub API response we use.
type GithubRelease struct {
	Name             string           `json:"name"`
	ID               int              `json:"id"`
	UpdatedAt        time.Time        `json:"updated_at"`
	Assets           []GithubAsset    `json:"assets"`
	Datetime         time.Time        // parsed from Name
	ProcessedVersion ProcessedVersion // derived from Datetime
}

type GithubAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

// getGithubReleases fetches releases for a GitHub repo given a releases URL like
// https://github.com/owner/repo/releases
func GetGithubReleases(releasesURL string, multiPage bool) ([]GithubRelease, error) {
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

	apiBase := fmt.Sprintf("https://api.github.com/repos/%s/%s/releases", owner, repo)
	client := &http.Client{Timeout: 15 * time.Second}

	// optional token to avoid strict unauthenticated rate limits
	token := os.Getenv("GITHUB_TOKEN")

	allReleases := make([]GithubRelease, 0)
	perPage := 100
	page := 1

	for {
		api := fmt.Sprintf("%s?per_page=%d&page=%d", apiBase, perPage, page)
		req, err := http.NewRequest("GET", api, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Accept", "application/vnd.github.v3+json")
		if token != "" {
			req.Header.Set("Authorization", "token "+token)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch GitHub releases: %w", err)
		}

		// read Link header before closing body
		linkHeader := resp.Header.Get("Link")

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("GitHub API returned status %d: %s", resp.StatusCode, string(body))
		}

		var releases []GithubRelease
		dec := json.NewDecoder(resp.Body)
		if err := dec.Decode(&releases); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("failed to parse GitHub releases response: %w", err)
		}
		resp.Body.Close()

		if len(releases) == 0 {
			break
		}

		allReleases = append(allReleases, releases...)

		// If not multi-page, stop after first page.
		if !multiPage {
			break
		}
		// Else:
		// If Link header present, use it to determine if there is a next page.
		// Otherwise, stop when fewer than perPage items returned.
		if linkHeader == "" {
			if len(releases) < perPage {
				break
			}
			page++
			continue
		}
		if !hasNextLink(linkHeader) {
			break
		}
		page++
	}

	// Keep only releases last updated more than one hour ago to avoid incomplete releases
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	filtered := make([]GithubRelease, 0, len(allReleases))
	for _, r := range allReleases {
		if r.UpdatedAt.Before(oneHourAgo) {
			filtered = append(filtered, r)
		}
	}

	// Parse Datetime and ProcessedVersion for each release
	for i := range filtered {
		dt, err := parseReleaseTime(filtered[i].Name)
		if err != nil {
			return nil, fmt.Errorf("failed to parse release time for %s: %w", filtered[i].Name, err)
		}
		filtered[i].Datetime = dt
		filtered[i].ProcessedVersion = ProcessedVersionFromDate(dt)
	}

	return filtered, nil
}

// hasNextLink returns true if the Link header contains a rel="next" link.
func hasNextLink(link string) bool {
	// Example Link header:
	// <https://api.github.com/...&page=2>; rel="next", <https://api.github.com/...&page=5>; rel="last"
	parts := strings.Split(link, ",")
	for _, p := range parts {
		if strings.Contains(p, `rel="next"`) {
			return true
		}
	}
	return false
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

// ProcessedVersionFromDate store version in the format vMajor.Minor where:
// Major: week number since 1st Jan 2025
// Minor: hour in the week (from 0 to 167) (zero-padded to 3 digits)
// IsBase: true if base version (no minor when converted to string)
type ProcessedVersion struct {
	Major  int
	Minor  int
	IsBase bool
}

func ProcessedVersionFromDate(datetime time.Time) ProcessedVersion {
	epoch := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	weeksSince := int(datetime.Sub(epoch).Hours() / (24 * 7))
	hourInWeek := int(datetime.Sub(epoch).Hours()) % (24 * 7)
	return ProcessedVersion{
		Major:  weeksSince,
		Minor:  hourInWeek,
		IsBase: false,
	}
}

func ProcessedVersionFromString(s string) (ProcessedVersion, error) {
	var major, minor int
	isBase := false
	s = strings.TrimPrefix(s, "v")
	parts := strings.Split(s, ".")
	if len(parts) == 1 {
		isBase = true
		_, err := fmt.Sscanf(parts[0], "%d", &major)
		if err != nil {
			return ProcessedVersion{}, fmt.Errorf("invalid processed version: %s", s)
		}
	} else if len(parts) == 2 {
		_, err := fmt.Sscanf(parts[0], "%d", &major)
		if err != nil {
			return ProcessedVersion{}, fmt.Errorf("invalid processed version: %s", s)
		}
		_, err = fmt.Sscanf(parts[1], "%d", &minor)
		if err != nil {
			return ProcessedVersion{}, fmt.Errorf("invalid processed version: %s", s)
		}
	} else {
		return ProcessedVersion{}, fmt.Errorf("invalid processed version: %s", s)
	}
	return ProcessedVersion{
		Major:  major,
		Minor:  minor,
		IsBase: isBase,
	}, nil
}

func (pv ProcessedVersion) String() string {
	if pv.IsBase {
		return fmt.Sprintf("v%d", pv.Major)
	}
	return fmt.Sprintf("v%d.%03d", pv.Major, pv.Minor)
}

func ProcessedFileName(version ProcessedVersion, datetime time.Time) string {
	return fmt.Sprintf("%s_%s.db", version.String(), datetime.Format("2006-01-02T15"))
}

type Planner struct {
	doneFolder   string
	ghReleaseUrl string
}

type Job struct {
	isDiff        bool
	base          string
	archive       GithubRelease
	processedFile string
}

type ArchiveDone struct {
	Version  ProcessedVersion
	Datetime time.Time
	Name     string
}

type ArchiveDoneBase struct {
	Base  ArchiveDone
	Diffs []ArchiveDone
}

type ArchivesDones struct {
	Latest   ArchiveDone
	DatesSet map[time.Time]bool // Days (YYYY-MM-DD)
	All      map[int]ArchiveDoneBase
}

func MakeArchiveDones(entries []os.DirEntry) *ArchivesDones {
	latest := ArchiveDone{}
	datesSet := make(map[time.Time]bool)
	all := make(map[int]ArchiveDoneBase)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, ".db") && strings.HasPrefix(name, "v") {
			base := strings.TrimPrefix(strings.TrimSuffix(name, ".db"), "v")
			index_ := strings.Index(base, "_")
			if index_ == -1 {
				continue
			}
			versionPart := base[:index_]
			isBase := !strings.Contains(versionPart, ".")
			pv, err := ProcessedVersionFromString(versionPart)
			if err != nil {
				continue
			}
			datetimePart := base[index_+1:]
			datetime, err := time.Parse("2006-01-02T15", datetimePart)
			if err != nil {
				continue
			}
			ad := ArchiveDone{
				Version:  pv,
				Datetime: datetime,
				Name:     name,
			}
			// Update latest
			if ad.Datetime.After(latest.Datetime) {
				latest = ad
			}
			// Update dates set
			day := TimeAsDay(ad.Datetime)
			datesSet[day] = true
			// Update all
			major, ok := all[pv.Major]
			if !ok {
				major = ArchiveDoneBase{
					Diffs: make([]ArchiveDone, 0),
				}
			}
			if isBase {
				major.Base = ad
			} else {
				major.Diffs = append(major.Diffs, ad)
			}
			all[pv.Major] = major
		}
	}
	ArchivesDones := &ArchivesDones{
		Latest:   latest,
		DatesSet: datesSet,
		All:      all,
	}
	return ArchivesDones
}

func (p Planner) ListArchiveDones() (*ArchivesDones, error) {
	entries, err := os.ReadDir(p.doneFolder)
	if err != nil {
		return nil, fmt.Errorf("failed to read done folder: %w", err)
	}
	return MakeArchiveDones(entries), nil
}

func MakeJobs(releases []GithubRelease, archivesDones *ArchivesDones) ([]Job, error) {
	// Sort releases by Datetime ascending (oldest first)
	for i := 0; i < len(releases); i++ {
		for j := i + 1; j < len(releases); j++ {
			if releases[j].Datetime.Before(releases[i].Datetime) {
				releases[i], releases[j] = releases[j], releases[i]
			}
		}
	}

	jobs := make([]Job, 0)
	newDays := make(map[time.Time]bool)
	newBases := make(map[int]string)
	for _, archive := range releases {
		// skip already done days
		day := TimeAsDay(archive.Datetime)
		if _, ok := newDays[day]; ok {
			continue
		}
		if _, ok := archivesDones.DatesSet[day]; ok {
			continue
		}

		// Check if major version already done, if not, promote to major
		isDiff := false
		baseName := ""
		pv := archive.ProcessedVersion
		major, ok := archivesDones.All[pv.Major]
		if ok {
			if major.Base.Name == "" {
				return nil, fmt.Errorf("incomplete base archive data for major version %d", pv.Major)
			}
			isDiff = true
			baseName = major.Base.Name
		}
		base, ok := newBases[pv.Major]
		if ok {
			isDiff = true
			baseName = base
		}
		pv.IsBase = !isDiff
		// If not diff, record new base
		if !isDiff {
			newBases[pv.Major] = ProcessedFileName(pv, archive.Datetime)
		}

		job := Job{
			isDiff:        isDiff,
			base:          baseName,
			archive:       archive,
			processedFile: ProcessedFileName(pv, archive.Datetime),
		}
		newDays[day] = true
		jobs = append(jobs, job)
	}

	return jobs, nil
}

// PlanAll creates jobs for all available releases, filtering out those already done.
func (p Planner) PlanAll() []Job {
	archiveDone, err := p.ListArchiveDones()
	if err != nil {
		log.Fatalf("Failed to list archive dones: %v", err)
	}

	releases, err := GetGithubReleases(p.ghReleaseUrl, true)
	if err != nil {
		log.Fatalf("Failed to get GitHub releases: %v", err)
	}
	if len(releases) == 0 {
		log.Fatalf("No releases found")
	}

	jobs, err := MakeJobs(releases, archiveDone)
	if err != nil {
		log.Fatalf("Failed to make jobs: %v", err)
	}
	return jobs
}

// PlanDaily creates job for latest page of release, filtering out those already done.
func (p Planner) PlanDaily() []Job {
	archiveDone, err := p.ListArchiveDones()
	if err != nil {
		log.Fatalf("Failed to list archive dones: %v", err)
	}

	releases, err := GetGithubReleases(p.ghReleaseUrl, false)
	if err != nil {
		log.Fatalf("Failed to get GitHub releases: %v", err)
	}
	if len(releases) == 0 {
		log.Fatalf("No releases found")
	}

	jobs, err := MakeJobs(releases, archiveDone)
	if err != nil {
		log.Fatalf("Failed to make jobs: %v", err)
	}
	return jobs
}

// PlanLatest creates a job for the latest available release. Regardless of whether it's done or not.
func (p Planner) PlanLatest() []Job {
	releases, err := GetGithubReleases(p.ghReleaseUrl, false)
	if err != nil {
		log.Fatalf("Failed to get GitHub releases: %v", err)
	}
	if len(releases) == 0 {
		log.Fatalf("No releases found")
	}
	latest := releases[0]
	job := Job{
		isDiff:        false,
		archive:       latest,
		processedFile: ProcessedFileName(latest.ProcessedVersion, latest.Datetime),
	}
	return []Job{job}
}

func PlanToCommands(plan []Job, metaBinFolder, workFolder, doneFolder, extractFolder string) string {
	tmpProcessedFolder := path.Join(workFolder, "processed")
	archivesFolder := path.Join(workFolder, "archives")
	planCommands := "#!/bin/bash\nset -e\n\n"
	planCommands += fmt.Sprintf("mkdir -p %s\n", tmpProcessedFolder)
	planCommands += fmt.Sprintf("mkdir -p %s\n", archivesFolder)
	planCommands += "\n"
	for _, p := range plan {
		base := ""
		if p.isDiff {
			base = fmt.Sprintf("--base %s", path.Join(doneFolder, p.base))
		}
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

		planCommands += fmt.Sprintf("rm -rf %s && mkdir %s && tar -xzf %s -C %s --strip-components=1\n", extractFolder, extractFolder, archive, extractFolder)
		planCommands += fmt.Sprintf("%s/ingest %s --from %s --out %s\n", metaBinFolder, base, extractFolder, out)
		planCommands += fmt.Sprintf("rm -rf %s\n", extractFolder)
		planCommands += fmt.Sprintf("rm -rf %s\n", assetsFolder)
		planCommands += fmt.Sprintf("%s/merge %s --target %s\n", metaBinFolder, base, out)
		planCommands += fmt.Sprintf("sqlite3 %s 'PRAGMA journal_mode = DELETE;'\n", out)
		planCommands += fmt.Sprintf("mv %s %s\n", out, path.Join(doneFolder, p.processedFile))
		planCommands += "\n"
	}
	planCommands += "echo 'All done!'\n"
	return planCommands
}

func main() {
	metaBinFolder := os.Getenv("META_BIN_FOLDER")
	if metaBinFolder == "" {
		metaBinFolder = "."
	}

	url := os.Getenv("ARCHIVES_URL")
	if url == "" {
		url = "https://github.com/murolem/wplace-archives/releases"
	}
	workFolder := os.Getenv("META_WORK_FOLDER")
	if workFolder == "" {
		workFolder = "./wplace-work"
	}
	extractFolder := os.Getenv("META_TMP_FOLDER") // can be useful to extract on RAM disk
	if extractFolder == "" {
		extractFolder = path.Join(workFolder, "extract")
	}
	doneFolder := os.Getenv("META_DONE_FOLDER")
	if doneFolder == "" {
		doneFolder = "./wplace-done"
	}

	planner := Planner{
		doneFolder:   doneFolder,
		ghReleaseUrl: url,
	}

	plan := planner.PlanDaily()
	log.Printf("Planned %d jobs\n", len(plan))
	commands := PlanToCommands(plan, metaBinFolder, workFolder, doneFolder, extractFolder)
	fmt.Println(commands)
}

// Utils
func MakeDay(year, month, day int) time.Time {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}

func TimeAsDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
