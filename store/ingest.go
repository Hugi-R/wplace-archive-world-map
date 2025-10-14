package store

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hugi-R/wplace-archive-world-map/img"
)

type Ingester struct {
	db       TileDB
	force    bool
	paletter img.Paletter
	metrics  *metrics
	workers  int
	useDiff  bool
	baseDB   TileDB
}

type metrics struct {
	read     atomic.Int64
	lastRead atomic.Int64
	done     atomic.Int64
	success  atomic.Int64
	fail     atomic.Int64
	skip     atomic.Int64
	crcskip  atomic.Int64
	lastDone atomic.Int64
}

type Job struct {
	Z, X, Y int
	Data    []byte
	Crc32   uint32
}

type Reader interface {
	ReadNextGood() (Job, bool, error)
	Open(string) error
	Close() error
}

func (m *metrics) Read() {
	m.read.Add(1)
}

func (m *metrics) Fail() {
	m.done.Add(1)
	m.fail.Add(1)
}

func (m *metrics) Success() {
	m.done.Add(1)
	m.success.Add(1)
}

func (m *metrics) Skip() {
	m.done.Add(1)
	m.skip.Add(1)
}

func (m *metrics) CrcSkip() {
	m.crcskip.Add(1)
}

func (m *metrics) ReportMetrics() {
	const tickRate = 5

	ticker := time.NewTicker(tickRate * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		read := m.read.Load()
		lastRead := m.lastRead.Swap(read)
		readRate := float64(read-lastRead) / tickRate
		done := m.done.Load()
		success := m.success.Load()
		skip := m.skip.Load()
		fail := m.fail.Load()
		lastDone := m.lastDone.Swap(done)
		rate := float64(done-lastDone) / tickRate
		crcskip := m.crcskip.Load()
		fmt.Printf("Rate: %.2f/s, Done: %d, Success: %d, Skip: %d, Fail: %d. Read rate: %.2f, Read: %d, CrcSkip: %d\n", rate, done, success, skip, fail, readRate, read, crcskip)
	}
}

func (g *Ingester) processData(j Job) (bool, error) {
	exists, _, err := g.db.StatTile(j.Z, j.X, j.Y)
	if (exists || err != nil) && !g.force {
		// Skip
		return true, nil
	}

	// If diff is enabled, check CRC to quickly known if there's any change
	if g.useDiff {
		exists, crc32, err := g.baseDB.StatTile(j.Z, j.X, j.Y)
		if (err == nil) && exists && (crc32 == j.Crc32) {
			// Skip, no change on tile
			g.metrics.CrcSkip()
			return true, nil
		}
	}

	pngImg, err := img.DecodeImage(j.Data)
	if err != nil {
		return false, fmt.Errorf("failed to decode tile %d/%d/%d: %w", j.Z, j.X, j.Y, err)
	}

	packed := bytes.Buffer{}
	g.paletter.PngPack(pngImg, &packed)
	packedData := packed.Bytes()

	// If diff is enabled, compute the diff
	if g.useDiff {
		baseData, err := g.baseDB.GetTile(j.Z, j.X, j.Y)
		if err == nil {
			diff, changes, err := img.Diff(baseData, packedData)
			if err == nil {
				if changes {
					packedData = diff
				} else {
					// Skip, no changes on the tile
					return true, nil
				}
			}
		}
	}

	err = g.db.PutTile(j.Z, j.X, j.Y, packedData, j.Crc32)
	return false, err
}

func (g *Ingester) worker(jobChan chan Job, wg *sync.WaitGroup) {
	defer wg.Done()
	for j := range jobChan {
		skip, err := g.processData(j)
		if err != nil {
			fmt.Printf("Failed job %v: %v", j, err)
			g.metrics.Fail()
		} else {
			if skip {
				g.metrics.Skip()
			} else {
				g.metrics.Success()
			}
		}
	}
}

func (g *Ingester) Ingest(read func() (Job, bool, error)) {
	go g.metrics.ReportMetrics()

	jobChan := make(chan Job, 200)
	var wg sync.WaitGroup
	for range g.workers {
		wg.Add(1)
		go g.worker(jobChan, &wg)
	}

	for j, ok, err := read(); ok; j, ok, err = read() {
		if err != nil {
			fmt.Printf("failed read: %v\n", err)
			continue
		}
		jobChan <- j
		g.metrics.Read()
	}
	close(jobChan)
	wg.Wait()
}

func NewIngester(tileDB TileDB, workers int, force bool) Ingester {
	m := metrics{}
	p := img.NewPaletter()
	g := Ingester{
		db:       tileDB,
		force:    force,
		workers:  workers,
		metrics:  &m,
		paletter: p,
		useDiff:  false,
	}
	return g
}

func NewDiffIngester(tileDB TileDB, workers int, force bool, baseDb TileDB) Ingester {
	g := NewIngester(tileDB, workers, force)
	g.useDiff = true
	g.baseDB = baseDb
	return g
}
