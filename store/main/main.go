package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Hugi-R/wplace-archive-world-map/store"
)

func isDir(path string) bool {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return false
	}
	if fileInfo.IsDir() {
		return true
	}
	return false
}

func Main() error {
	base := flag.String("base", "", "Optional base DB path")
	from := flag.String("from", "", "Mandatory from path (folder or 7z)")
	out := flag.String("out", "", "Mandatory out DB path")
	workers := flag.Int("workers", 10, "Optional number of workers (default 10)")

	flag.Parse()

	// Check mandatory flags
	if *from == "" {
		return fmt.Errorf("missing required flag: --from")
	}
	if *out == "" {
		return fmt.Errorf("missing required flag: --out")
	}

	store.Ingest(*from, *out, *base, *workers)

	fmt.Println("Done")
	return nil
}

func main() {
	start := time.Now()
	err := Main()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	elapsed := time.Since(start)
	fmt.Printf("Elapsed time: %s\n", elapsed)
}
