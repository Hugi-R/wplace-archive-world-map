package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Hugi-R/wplace-archive-world-map/merger"
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

	return merger.Merge(*target, *base, *initZ, *workers)
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
