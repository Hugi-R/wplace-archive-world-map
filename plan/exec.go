package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/Hugi-R/wplace-archive-world-map/merger"
	"github.com/Hugi-R/wplace-archive-world-map/store"
)

// Download downloads the archive for the given GithubRelease into the workfolder and returns the path to the downloaded file.
// The GithubRelease is composed of multiple assets.
// The assets are a tar.gz archive splited into multiple parts (e.g. .tar.gz.aa, .tar.gz.ab, ...).
// All parts are downloaded into a single tar.gz file.
func Download(release GithubRelease, workFolder string) (string, error) {
	// collect and sort candidate parts
	parts := []struct {
		name string
		url  string
	}{}
	for _, a := range release.Assets {
		name := a.Name
		url := a.BrowserDownloadURL
		// /!\ accept full .tar.gz and split parts like .tar.gz.aa, .tar.gz.ab, ...
		if strings.Contains(name, ".tar.gz") {
			parts = append(parts, struct {
				name string
				url  string
			}{name: name, url: url})
		}
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("no tar.gz parts found in release %v", release)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].name < parts[j].name })

	// derive output filename from first part (trim the split suffix)
	first := parts[0].name
	idx := strings.Index(first, ".tar.gz")
	outName := first
	if idx != -1 {
		outName = first[:idx+len(".tar.gz")]
	}
	outPath := path.Join(workFolder, outName)

	if err := os.MkdirAll(workFolder, 0o755); err != nil {
		return "", fmt.Errorf("create workfolder: %w", err)
	}

	outFile, err := os.Create(outPath)
	if err != nil {
		return "", fmt.Errorf("create output file: %w", err)
	}
	defer outFile.Close()

	for _, p := range parts {
		log.Printf("Downloading %s -> %s", p.url, outPath)
		resp, err := http.Get(p.url)
		if err != nil {
			return "", fmt.Errorf("download %s: %w", p.url, err)
		}
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			resp.Body.Close()
			return "", fmt.Errorf("download %s: bad status %d", p.url, resp.StatusCode)
		}
		_, err = io.Copy(outFile, resp.Body)
		resp.Body.Close()
		if err != nil {
			return "", fmt.Errorf("copying %s: %w", p.url, err)
		}
	}

	return outPath, nil
}

// MoveFile moves the file from src to dst, even across different filesystems.
func MoveFile(src, dst string) error {
	// Try rename first (fastest on same filesystem)
	err := os.Rename(src, dst)
	if err == nil {
		return nil
	}

	// Fallback to copy + delete
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source file: %w", err)
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create destination file: %w", err)
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	if err != nil {
		return fmt.Errorf("copy file data: %w", err)
	}
	err = in.Close()
	if err != nil {
		return fmt.Errorf("close source file: %w", err)
	}
	err = out.Close()
	if err != nil {
		return fmt.Errorf("close destination file: %w", err)
	}
	err = os.Remove(src)
	if err != nil {
		return fmt.Errorf("remove source file: %w", err)
	}
	return nil
}

// ExecPlan executes the given plan of jobs.
// Download, ingest, merge, move, for each job.
func ExecPlan(plan []Job, workFolder, doneFolder string) error {
	tmpProcessedFolder := path.Join(workFolder, "processed")
	archivesFolder := path.Join(workFolder, "archives")
	for _, p := range plan {
		start := time.Now()
		base := ""
		if p.isDiff {
			base = path.Join(doneFolder, p.base)
		}
		out := path.Join(tmpProcessedFolder, p.processedFile)

		log.Printf("Processing archive %s", p.archive.Name)
		archive, err := Download(p.archive, archivesFolder)
		if err != nil {
			return fmt.Errorf("download archive: %w", err)
		}

		err = store.Ingest(archive, out, base, 10)
		if err != nil {
			return fmt.Errorf("ingest archive: %w", err)
		}

		// Merge from z=10 down to z=0
		err = merger.Merge(out, base, 10, 10)
		if err != nil {
			return fmt.Errorf("merge tiles: %w", err)
		}

		if err := MoveFile(out, path.Join(doneFolder, p.processedFile)); err != nil {
			return fmt.Errorf("moving processed file: %w", err)
		}
		log.Printf("Done processing archive %s in %s", p.archive.Name, time.Since(start))
	}
	return nil
}

func DisplayPlan(plan []Job) {
	log.Printf("Planned %d jobs:", len(plan))
	for _, p := range plan {
		diffStr := "FULL"
		if p.isDiff {
			diffStr = fmt.Sprintf("DIFF from %s", p.base)
		}
		log.Printf("- %s %s", p.processedFile, diffStr)
	}
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
	doneFolder := os.Getenv("META_DONE_FOLDER")
	if doneFolder == "" {
		doneFolder = "./wplace-done"
	}

	planner := Planner{
		doneFolder:   doneFolder,
		ghReleaseUrl: url,
	}

	plan := planner.PlanDaily()
	DisplayPlan(plan)
	err := ExecPlan(plan, workFolder, doneFolder)
	if err != nil {
		log.Fatalf("ExecPlan failed: %v", err)
	}
}
