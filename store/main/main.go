package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Hugi-R/wplace-archive-world-map/store"
)

func main() {
	base := flag.String("base", "", "Optional base DB path")
	from := flag.String("from", "", "Mandatory from path")
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

	// For now, only 7z archive are supported
	if !strings.HasSuffix(*from, ".7z") {
		fmt.Fprintln(os.Stderr, "Error: --from only support 7z archive")
		os.Exit(1)
	}

	tileDB, err := store.NewTileDB(*out)
	if err != nil {
		fmt.Printf("Failed to create tile database: %v\n", err)
		return
	}
	defer tileDB.DB.Close()

	rz := store.Reader7z{}
	err = rz.Open(*from)
	if err != nil {
		fmt.Printf("Failed to read archive: %v\n", err)
		return
	}
	defer rz.Close()

	if *base != "" {
		baseDB, err := store.NewTileDB(*base)
		if err != nil {
			fmt.Printf("Failed to create tile database: %v\n", err)
			return
		}
		defer baseDB.DB.Close()
		ingester := store.NewDiffIngester(tileDB, *workers, false, baseDB)
		ingester.Ingest(rz.ReadNextGood)
	} else {
		ingester := store.NewIngester(tileDB, *workers, false)
		ingester.Ingest(rz.ReadNextGood)
	}
	fmt.Println("Done")
}
