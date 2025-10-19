package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
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

	tileDB, err := store.NewTileDB(*out, false)
	if err != nil {
		return fmt.Errorf("failed to create tile database %s: %w", *out, err)
	}
	defer tileDB.DB.Close()

	var reader store.Reader
	if strings.HasSuffix(*from, ".7z") {
		reader = &store.Reader7z{}
	} else if isDir(*from) {
		reader = &store.ReaderFolder{}
	} else {
		return fmt.Errorf("unsupported input format: %s", *from)
	}
	if err := reader.Open(*from); err != nil {
		return fmt.Errorf("failed to open input %s: %w", *from, err)
	}
	defer reader.Close()

	if *base != "" {
		baseDB, err := store.NewTileDB(*base, true)
		if err != nil {
			return fmt.Errorf("failed to open base tile database %s: %w", *base, err)
		}
		defer baseDB.DB.Close()
		ingester := store.NewDiffIngester(tileDB, *workers, false, baseDB)
		ingester.Ingest(reader.ReadNextGood)
	} else {
		ingester := store.NewIngester(tileDB, *workers, false)
		ingester.Ingest(reader.ReadNextGood)
	}
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
