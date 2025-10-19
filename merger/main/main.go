package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Hugi-R/wplace-archive-world-map/merger"
	"github.com/Hugi-R/wplace-archive-world-map/store"
)

func Main() error {

	base := flag.String("base", "", "Optional base DB path")
	target := flag.String("target", "", "Mandatory from path")
	workers := flag.Int("workers", 16, "Optional number of workers (default 16)")
	initZ := flag.Int("initz", 10, "Optional initial zoom level (default 10)")

	flag.Parse()

	// Check mandatory flags
	if *target == "" {
		return fmt.Errorf("missing required flag: --from")
	}

	tileDB, err := store.NewTileDB(*target, false)
	if err != nil {
		return fmt.Errorf("failed to create target tile database: %v", err)
	}
	defer tileDB.DB.Close()

	var baseDB *store.TileDB = nil
	if *base != "" {
		db, err := store.NewTileDB(*base, true)
		if err != nil {
			return fmt.Errorf("failed to create base tile database: %v", err)
		}
		baseDB = &db
	}

	if baseDB == nil {
		fmt.Printf("Starting merging tiles from z=%d using %d workers.\n", *initZ, *workers)
	} else {
		fmt.Printf("Starting merging tiles from z=%d using %d workers. Using %s as base.\n", *initZ, *workers, *base)
	}

	merger, err := merger.NewMerger(&tileDB, *workers, *initZ, false, baseDB)
	if err != nil {
		return fmt.Errorf("failed to create merger: %v", err)
	}
	merger.Merge()
	if baseDB != nil {
		baseDB.Close()
	}
	tileDB.Close()
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
