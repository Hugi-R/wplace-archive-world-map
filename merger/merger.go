package merger

import (
	"bytes"
	"fmt"
	"image"
	"image/png"
	"os"
	"sync"
	"time"

	"github.com/Hugi-R/wplace-archive-world-map/img"
	"github.com/Hugi-R/wplace-archive-world-map/store"
)

type Merger struct {
	initialZ  int
	store     *store.TileDB
	workers   int
	metrics   metrics
	emptyTile *image.Paletted
	force     bool
	base      *store.TileDB
	useDiff   bool
}

type metrics struct {
	resChan   chan job
	merged    int64
	skipped   int64
	failed    int64
	empty     int64
	lastTile  string
	lastMerge int64
}

type job struct {
	z      int
	x      int
	y      int
	status string
}

func NewMerger(store *store.TileDB, workers int, initialZ int, force bool, base *store.TileDB) (*Merger, error) {
	if initialZ < 0 || initialZ > 10 {
		return nil, fmt.Errorf("invalid initial zoom level: %d", initialZ)
	}

	metrics := metrics{
		resChan: make(chan job),
	}
	emptyTileEncode, err := img.EncodePng(img.EmptyImagePaletted(1000))
	if err != nil {
		return nil, fmt.Errorf("failed to create empty tile: %w", err)
	}
	emptyTile, err := img.DecodeImage(emptyTileEncode)
	if err != nil {
		return nil, fmt.Errorf("failed to create empty tile: %w", err)
	}
	emptyP, ok := emptyTile.(*image.Paletted)
	if !ok {
		return nil, fmt.Errorf("empty tile image is not paletted")
	}

	return &Merger{
		initialZ:  initialZ,
		store:     store,
		workers:   workers,
		metrics:   metrics,
		emptyTile: emptyP,
		force:     force,
		base:      base,
		useDiff:   base != nil,
	}, nil
}

func (m *Merger) Merge() {
	go m.reportMetrics()

	// Traverse levels from "bottom" to "upper"
	for z := m.initialZ; z >= 0; z-- {
		m.mergeLevel(z)
		fmt.Printf("Level %d finished\n", z)
	}
}

func (m *Merger) mergeLevel(z int) {

	jobChan := make(chan job)
	wg := sync.WaitGroup{}

	for range m.workers {
		wg.Add(1)
		go m.worker(jobChan, &wg)
	}

	tiles, err := m.store.ListTiles(z + 1)
	if err != nil {
		panic(err)
	}

	jobSet := make(map[[2]uint16]bool)
	// Enqueue jobs for current zoom level
	for _, t := range tiles {
		x, y := t[0]/2, t[1]/2
		// If no job yet for this tile, do it
		exists := jobSet[[2]uint16{x, y}]
		if !exists {
			jobSet[[2]uint16{x, y}] = true
			job := job{z: z, x: int(x), y: int(y)}
			jobChan <- job
		}
	}
	fmt.Printf("Created %d jobs for level %d\n", len(jobSet), z)
	close(jobChan)
	wg.Wait()
}

func (m *Merger) reportMetrics() {
	const tickRate = 10

	ticker := time.NewTicker(tickRate * time.Second)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			merged := m.metrics.merged
			rate := float64(merged-m.metrics.lastMerge) / tickRate
			m.metrics.lastMerge = merged
			fmt.Printf("Rate: %.2f/s, Merged: %d, Skipped: %d, Empty: %d, Failed: %d, Last Tile: %s\n", rate, m.metrics.merged, m.metrics.skipped, m.metrics.empty, m.metrics.failed, m.metrics.lastTile)
		}
	}()

	for res := range m.metrics.resChan {
		m.metrics.lastTile = fmt.Sprintf("%d/%d/%d", res.z, res.x, res.y)
		switch res.status {
		case "success":
			m.metrics.merged++
		case "skip":
			m.metrics.skipped++
		case "fail":
			m.metrics.failed++
		case "empty":
			m.metrics.empty++
		}
	}
}

func (m *Merger) worker(jobChan chan job, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobChan {
		err := m.mergeTile(job.z, job.x, job.y)
		if err != nil {
			fmt.Printf("Failed to merge tile %d/%d/%d: %v\n", job.z, job.x, job.y, err)
			job.status = "fail"
			m.metrics.resChan <- job
		} else {
			job.status = "success"
			m.metrics.resChan <- job
		}
	}
}

func (m *Merger) mergeTile(z, x, y int) error {
	if z >= 11 {
		return nil
	}
	exists, _, err := m.store.StatTile(z, x, y)
	if err != nil {
		return fmt.Errorf("failed to stat tile %d/%d/%d: %w", z, x, y, err)
	}
	if exists && !m.force {
		// Tile already exists, skip
		m.metrics.resChan <- job{z: z, x: x, y: y, status: "skip"}
		return nil
	}
	newZ := z + 1
	newX := x * 2
	newY := y * 2
	images := make([]*image.Paletted, 4)
	emptyCount := 0
	for i := range 4 {
		xx, yy := (i % 2), (i / 2)
		im, empty := m.getTile(newZ, newX+xx, newY+yy)
		emptyCount += empty
		images[i] = im
	}
	if emptyCount >= 4 {
		// All tiles are empty, nothing to merge
		m.metrics.resChan <- job{z: z, x: x, y: y, status: "empty"}
		return nil
	}
	merged, err := img.FastPalettedResizeAndMerge(images[0], images[1], images[2], images[3], img.FastPaletteResize2)
	if err != nil {
		return fmt.Errorf("failed to merge tiles %d/%d/%d (empty %d): %w", z, x, y, emptyCount, err)
	}

	// If diff is enabled, compute the diff
	if m.useDiff {
		baseData, err := m.base.GetTile(z, x, y)
		if err == nil {
			bp, err := img.DecodePaletted(baseData)
			if err == nil {
				diff, changes, err := img.DiffPaletted(bp, merged)
				if err == nil {
					if changes {
						merged = diff
					} else {
						// Skip, no changes on the tile
						m.metrics.resChan <- job{z: z, x: x, y: y, status: "empty"}
						return nil
					}
				}
			}
		}
	}

	encoded, err := img.EncodePng(merged)
	if err != nil {
		return err
	}
	err = m.store.PutTileAutoCRC(z, x, y, encoded)
	return err
}

func (m *Merger) getTile(z, x, y int) (*image.Paletted, int) {
	if m.useDiff {
		return m.getDiffTile(z, x, y)
	}
	return m.getSingleTile(z, x, y)
}

func (m *Merger) getSingleTile(z, x, y int) (*image.Paletted, int) {
	data, err := m.store.GetTile(z, x, y)
	if err != nil {
		return m.emptyTile, 1
	}
	im, err := img.DecodePaletted(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to decode tile %d/%d/%d: %v", z, x, y, err)
		return m.emptyTile, 1
	}
	return im, 0
}

func (m *Merger) getDiffTile(z, x, y int) (*image.Paletted, int) {
	dataNew, err := m.store.GetTile(z, x, y)
	if err != nil {
		return m.emptyTile, 1
	}
	imNew, err := img.DecodePaletted(dataNew)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to decode tile %d/%d/%d: %v", z, x, y, err)
		return m.emptyTile, 1
	}
	dataBase, err := m.base.GetTile(z, x, y)
	if err != nil {
		return m.emptyTile, 1
	}
	imBase, err := img.DecodePaletted(dataBase)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to decode tile %d/%d/%d: %v", z, x, y, err)
		return m.emptyTile, 1
	}
	im, err := img.UnDiffPaletted(imBase, imNew)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to unDiff tile %d/%d/%d: %v", z, x, y, err)
		return m.emptyTile, 1
	}
	return im, 0
}

func (m *Merger) mergeTileAvg(z, x, y int) error {
	if z >= 11 {
		return nil
	}
	exists, _, err := m.store.StatTile(z, x, y)
	if err != nil {
		return fmt.Errorf("failed to stat tile %d/%d/%d: %w", z, x, y, err)
	}
	if exists {
		// Tile already exists, skip
		m.metrics.resChan <- job{z: z, x: x, y: y, status: "skip"}
		return nil
	}
	newZ := z + 1
	newX := x * 2
	newY := y * 2
	images := make([]image.Image, 4)
	for i := range 4 {
		xx, yy := (i % 2), (i / 2)
		data, err := m.store.GetTile(newZ, newX+xx, newY+yy)
		if err != nil {
			return fmt.Errorf("failed to get tile %d/%d/%d: %w", newZ, newX+xx, newY+yy, err)
		}
		img, _, err := image.Decode(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("failed to decode tile %d/%d/%d: %w", newZ, newX+xx, newY+yy, err)
		}
		images[i] = img
	}
	merged := img.FastResizeAndMerge(images[0], images[1], images[2], images[3], img.FastAvgResize2)
	enc := png.Encoder{
		CompressionLevel: png.DefaultCompression,
	}
	data := bytes.Buffer{}
	if err := enc.Encode(&data, merged); err != nil {
		return err
	}
	err = m.store.PutTileAutoCRC(z, x, y, data.Bytes())
	return err
}
