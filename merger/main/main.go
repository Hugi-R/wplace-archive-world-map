package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Hugi-R/wplace-archive-world-map/merger"
	"github.com/Hugi-R/wplace-archive-world-map/store"
)

func main() {

	base := flag.String("base", "", "Optional base DB path")
	target := flag.String("target", "", "Mandatory from path")
	workers := flag.Int("workers", 16, "Optional number of workers (default 16)")
	initZ := flag.Int("initz", 10, "Optional initial zoom level (default 10)")

	flag.Parse()

	// Check mandatory flags
	if *target == "" {
		fmt.Fprintln(os.Stderr, "Error: --from is required")
		os.Exit(1)
	}

	tileDB, err := store.NewTileDB(*target)
	if err != nil {
		fmt.Printf("Failed to create tile database: %v\n", err)
		return
	}
	defer tileDB.DB.Close()

	var baseDB *store.TileDB = nil
	if *base != "" {
		db, err := store.NewTileDB(*base)
		if err != nil {
			fmt.Printf("Failed to create tile database: %v\n", err)
			return
		}
		baseDB = &db
	}

	if baseDB == nil {
		fmt.Printf("Starting merging tiles from z=%d using %d workers.\n", *initZ, *workers)
	} else {
		fmt.Printf("Starting merging tiles from z=%d using %d workers. Using %s as base.\n", *initZ, *workers, *base)
	}

	merger, err := merger.NewMerger(&tileDB, *workers, *initZ, true, baseDB)
	if err != nil {
		fmt.Println("Failed to create merger:", err)
		return
	}
	merger.Merge()
	// Wait for sqlite to cool down (WAL)
	time.Sleep(3 * time.Second)
	if baseDB != nil {
		baseDB.Close()
	}
	tileDB.Close()
	fmt.Println("Done")
}
