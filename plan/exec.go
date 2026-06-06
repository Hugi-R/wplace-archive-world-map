package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/Hugi-R/wplace-archive-world-map/merger"
	"github.com/Hugi-R/wplace-archive-world-map/store"
)

// Download downloads the sqlite file for the given HFFile into the workfolder and returns the path to the downloaded file.
func Download(file HFFile, bucketURL, workFolder string) (string, error) {
	downloadURL := HFDownloadURL(bucketURL, file.Path)
	if downloadURL == "" {
		return "", fmt.Errorf("empty download URL for file %s", file.Path)
	}

	outName := path.Base(file.Path)
	outPath := path.Join(workFolder, outName)

	if err := os.MkdirAll(workFolder, 0o755); err != nil {
		return "", fmt.Errorf("create workfolder: %w", err)
	}

	outFile, err := os.Create(outPath)
	if err != nil {
		return "", fmt.Errorf("create output file: %w", err)
	}
	defer outFile.Close()

	log.Printf("Downloading %s -> %s", downloadURL, outPath)
	resp, err := http.Get(downloadURL)
	if err != nil {
		return "", fmt.Errorf("download %s: %w", downloadURL, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		resp.Body.Close()
		return "", fmt.Errorf("download %s: bad status %d", downloadURL, resp.StatusCode)
	}
	_, err = io.Copy(outFile, resp.Body)
	resp.Body.Close()
	if err != nil {
		return "", fmt.Errorf("copying %s: %w", downloadURL, err)
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
func ExecPlan(plan []Job, bucketURL, workFolder, doneFolder string) error {
	tmpProcessedFolder := path.Join(workFolder, "processed")
	archivesFolder := path.Join(workFolder, "archives")
	for _, p := range plan {
		start := time.Now()
		base := ""
		if p.isDiff {
			base = path.Join(doneFolder, p.base)
		}
		out := path.Join(tmpProcessedFolder, p.processedFile)

		log.Printf("Processing archive %s", p.archive.Path)
		archive, err := Download(p.archive, bucketURL, archivesFolder)
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
		log.Printf("Done processing archive %s in %s", p.archive.Path, time.Since(start))
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
	planType := flag.String("type", "daily", "Plan type: latest, daily, or all")
	flag.Parse()

	url := os.Getenv("WPLACE_ARCHIVES_URL")
	if url == "" {
		url = "https://huggingface.co/buckets/Hugi-R/wplace-archives/tree/full"
	}
	workFolder := os.Getenv("WPLACE_WORK_FOLDER")
	if workFolder == "" {
		workFolder = "./wplace-work"
	}
	doneFolder := os.Getenv("WPLACE_DONE_FOLDER")
	if doneFolder == "" {
		doneFolder = "./wplace-done"
	}

	planner := Planner{
		doneFolder:  doneFolder,
		hfBucketURL: url,
	}

	var plan []Job
	switch *planType {
	case "latest":
		plan = planner.PlanLatest()
	case "daily":
		plan = planner.PlanDaily()
	case "all":
		plan = planner.PlanAll()
	default:
		log.Fatalf("Invalid plan type: %s. Must be one of: latest, daily, all", *planType)
	}

	DisplayPlan(plan)
	err := ExecPlan(plan, url, workFolder, doneFolder)
	if err != nil {
		log.Fatalf("ExecPlan failed: %v", err)
	}
}
