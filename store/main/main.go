package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

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

func main() {
	base := flag.String("base", "", "Optional base DB path")
	from := flag.String("from", "", "Mandatory from path (folder or 7z)")
	out := flag.String("out", "", "Mandatory out DB path")
	workers := flag.Int("workers", 10, "Optional number of workers (default 10)")

	flag.Parse()

	// Check mandatory flags
	if *from == "" {
		fmt.Fprintln(os.Stderr, "Error: --from is required")
		os.Exit(1)
	}
	if *out == "" {
		fmt.Fprintln(os.Stderr, "Error: --out is required")
		os.Exit(1)
	}

	tileDB, err := store.NewTileDB(*out)
	if err != nil {
		fmt.Printf("Failed to create tile database: %v\n", err)
		return
	}
	defer tileDB.DB.Close()

	var reader store.Reader
	if strings.HasSuffix(*from, ".7z") {
		reader = &store.Reader7z{}
	} else if isDir(*from) {
		reader = &store.ReaderFolder{}
	} else {
		fmt.Printf("%s not supported format", *from)
		return
	}
	err = reader.Open(*from)
	if err != nil {
		fmt.Printf("Failed to read: %v\n", err)
		return
	}
	defer reader.Close()

	if *base != "" {
		baseDB, err := store.NewTileDB(*base)
		if err != nil {
			fmt.Printf("Failed to create tile database: %v\n", err)
			return
		}
		defer baseDB.DB.Close()
		ingester := store.NewDiffIngester(tileDB, *workers, false, baseDB)
		ingester.Ingest(reader.ReadNextGood)
	} else {
		ingester := store.NewIngester(tileDB, *workers, false)
		ingester.Ingest(reader.ReadNextGood)
	}
	fmt.Println("Done")
}
