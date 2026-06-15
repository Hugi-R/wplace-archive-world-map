package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hugi-R/wplace-archive-world-map/merger"
	"github.com/Hugi-R/wplace-archive-world-map/store"
)

// Download downloads the sqlite file for the given HFFile into the workfolder
// and returns the path to the downloaded file.
// It uses parallel range requests, retries, and a buffered writer for speed.
func Download(file HFFile, bucketURL, workFolder string) (string, error) {
	const (
		maxRetries   = 5
		chunkSize    = 32 * 1024 * 1024 // 32 MB per chunk
		parallelism  = 8                // concurrent chunk downloads
		bufferSize   = 4 * 1024 * 1024  // 4 MB write buffer
		totalTimeout = 30 * time.Minute
	)

	downloadURL := HFDownloadURL(bucketURL, file.Path)
	if downloadURL == "" {
		return "", fmt.Errorf("empty download URL for file %s", file.Path)
	}

	outName := path.Base(file.Path)
	outPath := path.Join(workFolder, outName)

	if err := os.MkdirAll(workFolder, 0o755); err != nil {
		return "", fmt.Errorf("create workfolder: %w", err)
	}

	client := &http.Client{Timeout: totalTimeout}

	// --- 1. HEAD request: get file size and check range support ---
	headResp, err := client.Head(downloadURL)
	if err != nil {
		return "", fmt.Errorf("HEAD %s: %w", downloadURL, err)
	}
	headResp.Body.Close()

	supportsRanges := headResp.Header.Get("Accept-Ranges") == "bytes"
	contentLength := headResp.ContentLength

	log.Printf("Downloading %s -> %s (%.2f MB, parallel=%v)",
		downloadURL, outPath,
		float64(contentLength)/1024/1024,
		supportsRanges && contentLength > 0,
	)

	outFile, err := os.Create(outPath)
	if err != nil {
		return "", fmt.Errorf("create output file: %w", err)
	}
	defer outFile.Close()

	// --- 2. Parallel chunked download (if server supports it) ---
	if supportsRanges && contentLength > chunkSize {
		if err := downloadParallel(client, downloadURL, outFile, contentLength, chunkSize, parallelism, maxRetries); err != nil {
			return "", fmt.Errorf("parallel download %s: %w", downloadURL, err)
		}
		return outPath, nil
	}

	// --- 3. Fallback: single-connection download with retries + buffered writer ---
	if err := downloadWithRetry(client, downloadURL, outFile, maxRetries, bufferSize); err != nil {
		return "", fmt.Errorf("download %s: %w", downloadURL, err)
	}
	return outPath, nil
}

// downloadParallel fetches non-overlapping byte ranges concurrently and writes
// each directly to its offset in the output file (no temp files needed).
func downloadParallel(
	client *http.Client,
	url string,
	out *os.File,
	totalSize, chunkSize int64,
	parallelism, maxRetries int,
) error {
	type chunk struct {
		index int
		start int64
		end   int64 // inclusive
	}

	var chunks []chunk
	for i, start := 0, int64(0); start < totalSize; i, start = i+1, start+chunkSize {
		end := start + chunkSize - 1
		if end >= totalSize {
			end = totalSize - 1
		}
		chunks = append(chunks, chunk{i, start, end})
	}

	// Pre-allocate file to avoid fragmentation and seek races.
	if err := out.Truncate(totalSize); err != nil {
		return fmt.Errorf("pre-allocate: %w", err)
	}

	sem := make(chan struct{}, parallelism)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	downloaded := atomic.Int64{}

	for _, c := range chunks {
		c := c
		wg.Add(1)
		sem <- struct{}{}

		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			// Abort remaining goroutines if any chunk already failed.
			mu.Lock()
			if firstErr != nil {
				mu.Unlock()
				return
			}
			mu.Unlock()

			data, err := fetchRangeWithRetry(client, url, c.start, c.end, maxRetries)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("chunk %d (%d-%d): %w", c.index, c.start, c.end, err)
				}
				mu.Unlock()
				return
			}

			if _, err := out.WriteAt(data, c.start); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("write chunk %d: %w", c.index, err)
				}
				mu.Unlock()
				return
			}

			n := downloaded.Add(int64(len(data)))
			log.Printf("  progress: %.1f%%", float64(n)/float64(totalSize)*100)
		}()
	}

	wg.Wait()
	return firstErr
}

// fetchRangeWithRetry fetches a single byte range, retrying on transient errors.
func fetchRangeWithRetry(client *http.Client, url string, start, end int64, maxRetries int) ([]byte, error) {
	var lastErr error
	for attempt := range maxRetries {
		if attempt > 0 {
			backoff := time.Duration(1<<attempt) * 500 * time.Millisecond // 1s, 2s, 4s…
			log.Printf("  retry %d for range %d-%d after %v", attempt, start, end, backoff)
			time.Sleep(backoff)
		}

		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, err // non-retryable
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			lastErr = fmt.Errorf("unexpected status %d for range request", resp.StatusCode)
			// 5xx → retry; 4xx (except 429) → give up
			if resp.StatusCode < 500 && resp.StatusCode != 429 {
				return nil, lastErr
			}
			continue
		}

		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return data, nil
	}
	return nil, fmt.Errorf("after %d retries: %w", maxRetries, lastErr)
}

// downloadWithRetry streams the full file with exponential-backoff retries.
func downloadWithRetry(client *http.Client, url string, out *os.File, maxRetries, bufferSize int) error {
	var lastErr error
	for attempt := range maxRetries {
		if attempt > 0 {
			backoff := time.Duration(1<<attempt) * 500 * time.Millisecond
			log.Printf("  retry %d for %s after %v", attempt, url, backoff)
			time.Sleep(backoff)
			if _, err := out.Seek(0, io.SeekStart); err != nil {
				return err
			}
		}

		resp, err := client.Get(url)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			resp.Body.Close()
			lastErr = fmt.Errorf("bad status %d", resp.StatusCode)
			if resp.StatusCode < 500 && resp.StatusCode != 429 {
				return lastErr
			}
			continue
		}

		w := bufio.NewWriterSize(out, bufferSize)
		_, err = io.Copy(w, resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return w.Flush()
	}
	return fmt.Errorf("after %d retries: %w", maxRetries, lastErr)
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
