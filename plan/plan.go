package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

// HFFile represents a file entry from the Hugging Face API response.
type HFFile struct {
	Path     string `json:"path"`
	Size     int64  `json:"size"`
	Type     string `json:"type"`
	Datetime time.Time
	ProcessedVersion ProcessedVersion
}

// HFDownloadURL builds the direct download URL for a file from a Hugging Face bucket.
func HFDownloadURL(bucketURL, filePath string) string {
	// bucketURL is like https://huggingface.co/buckets/Hugi-R/wplace-archives/tree/full
	// filePath is like full/full_2026-06-03T22-11-00Z.db
	// We need: https://huggingface.co/buckets/Hugi-R/wplace-archives/resolve/full/full_2026-06-03T22-11-00Z.db
	base := strings.Split(bucketURL, "/tree/")[0]
	return fmt.Sprintf("%s/resolve/%s", base, filePath)
}

// GetHFFiles fetches the list of files from a Hugging Face bucket.
// bucketURL is like https://huggingface.co/buckets/Hugi-R/wplace-archives/tree/full
func GetHFFiles(bucketURL string) ([]HFFile, error) {
	if bucketURL == "" {
		return nil, fmt.Errorf("empty bucket URL")
	}

	// Parse the bucket URL to extract owner, name, and path
	// URL format: https://huggingface.co/buckets/owner/name/tree/path
	// We need to build: https://huggingface.co/api/buckets/owner/name/tree/path
	apiURL := strings.Replace(bucketURL, "/buckets/", "/api/buckets/", 1)
	apiURL = strings.Replace(apiURL, "/tree/", "/tree/", 1)

	client := &http.Client{Timeout: 30 * time.Second}

	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch Hugging Face files: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Hugging Face API returned status %d: %s", resp.StatusCode, string(body))
	}

	var files []HFFile
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&files); err != nil {
		return nil, fmt.Errorf("failed to parse Hugging Face files response: %w", err)
	}

	// Filter to only include files in the requested directory (e.g., "full/")
	// Extract the target directory from the bucket URL
	targetDir := ""
	if idx := strings.Index(bucketURL, "/tree/"); idx != -1 {
		targetDir = bucketURL[idx+6:]
		if !strings.HasSuffix(targetDir, "/") {
			targetDir += "/"
		}
	}

	filtered := make([]HFFile, 0)
	for _, f := range files {
		if f.Type == "file" && strings.HasPrefix(f.Path, targetDir) {
			filtered = append(filtered, f)
		}
	}

	// Parse Datetime and ProcessedVersion for each file
	for i := range filtered {
		dt, err := parseHFFileName(filtered[i].Path)
		if err != nil {
			return nil, fmt.Errorf("failed to parse file time for %s: %w", filtered[i].Path, err)
		}
		filtered[i].Datetime = dt
		filtered[i].ProcessedVersion = ProcessedVersionFromDate(dt)
	}

	return filtered, nil
}

// parseHFFileName converts a file path like "full/full_2026-06-03T22-11-00Z.db" into a time.Time
func parseHFFileName(path string) (time.Time, error) {
	// Extract filename from path
	filename := path
	if idx := strings.LastIndex(path, "/"); idx != -1 {
		filename = path[idx+1:]
	}

	// Remove prefix (e.g., "full_") and suffix (e.g., ".db")
	s := strings.TrimSuffix(filename, ".db")
	if idx := strings.Index(s, "_"); idx != -1 {
		s = s[idx+1:]
	}

	// Find 'T' and convert the dashes between hour/minute/second back to colons
	tIdx := strings.Index(s, "T")
	if tIdx == -1 {
		return time.Time{}, fmt.Errorf("invalid file time format: %s", s)
	}
	datePart := s[:tIdx]
	timePart := s[tIdx+1:]

	// timePart looks like 22-11-00Z; replace '-' with ':'
	replaced := timePart
	for i := 0; i < 2; i++ {
		idx := strings.Index(replaced, "-")
		if idx == -1 {
			break
		}
		replaced = replaced[:idx] + ":" + replaced[idx+1:]
	}
	full := datePart + "T" + replaced

	t, err := time.Parse(time.RFC3339, full)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse time %s: %w", full, err)
	}
	return t, nil
}

// ProcessedVersion store version in the format vMajor.Minor where:
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
	doneFolder string
	hfBucketURL string
}

type Job struct {
	isDiff        bool
	base          string
	archive       HFFile
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

func MakeJobs(files []HFFile, archivesDones *ArchivesDones) ([]Job, error) {
	// Sort files by Datetime ascending (oldest first)
	for i := 0; i < len(files); i++ {
		for j := i + 1; j < len(files); j++ {
			if files[j].Datetime.Before(files[i].Datetime) {
				files[i], files[j] = files[j], files[i]
			}
		}
	}

	jobs := make([]Job, 0)
	newDays := make(map[time.Time]bool)
	newBases := make(map[int]string)
	for _, archive := range files {
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

// PlanAll creates jobs for all available files, filtering out those already done.
func (p Planner) PlanAll() []Job {
	archiveDone, err := p.ListArchiveDones()
	if err != nil {
		log.Fatalf("Failed to list archive dones: %v", err)
	}

	files, err := GetHFFiles(p.hfBucketURL)
	if err != nil {
		log.Fatalf("Failed to get Hugging Face files: %v", err)
	}
	if len(files) == 0 {
		log.Fatalf("No files found")
	}

	jobs, err := MakeJobs(files, archiveDone)
	if err != nil {
		log.Fatalf("Failed to make jobs: %v", err)
	}
	return jobs
}

// PlanDaily creates job for latest page of files, filtering out those already done.
func (p Planner) PlanDaily() []Job {
	archiveDone, err := p.ListArchiveDones()
	if err != nil {
		log.Fatalf("Failed to list archive dones: %v", err)
	}

	files, err := GetHFFiles(p.hfBucketURL)
	if err != nil {
		log.Fatalf("Failed to get Hugging Face files: %v", err)
	}
	if len(files) == 0 {
		log.Fatalf("No files found")
	}

	jobs, err := MakeJobs(files, archiveDone)
	if err != nil {
		log.Fatalf("Failed to make jobs: %v", err)
	}
	return jobs
}

// PlanLatest creates a job for the latest available file. Regardless of whether it's done or not.
func (p Planner) PlanLatest() []Job {
	files, err := GetHFFiles(p.hfBucketURL)
	if err != nil {
		log.Fatalf("Failed to get Hugging Face files: %v", err)
	}
	if len(files) == 0 {
		log.Fatalf("No files found")
	}

	// Find the latest file by datetime
	latest := files[0]
	for _, f := range files[1:] {
		if f.Datetime.After(latest.Datetime) {
			latest = f
		}
	}

	job := Job{
		isDiff:        false,
		archive:       latest,
		processedFile: ProcessedFileName(latest.ProcessedVersion, latest.Datetime),
	}
	return []Job{job}
}

// Utils
func MakeDay(year, month, day int) time.Time {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}

func TimeAsDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
