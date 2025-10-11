package store

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/bodgit/sevenzip"
)

type Reader7z struct {
	z          *sevenzip.ReadCloser
	state      int
	totalFiles int
}

func readJob(file *sevenzip.File) (Job, error) {
	rc, err := file.Open()
	if err != nil {
		return Job{}, err
	}
	defer rc.Close()
	pathParts := strings.Split(file.Name, "/")
	size := len(pathParts)
	if size < 2 {
		return Job{}, fmt.Errorf("unexpected file path structure: %s", file.Name)
	}
	z := 11
	x, err := strconv.Atoi(pathParts[size-2])
	if err != nil {
		return Job{}, fmt.Errorf("failed to parse x coordinate from path: %w", err)
	}
	y, err := strconv.Atoi(strings.TrimSuffix(pathParts[size-1], ".png"))
	if err != nil {
		return Job{}, fmt.Errorf("failed to parse y coordinate from path: %w", err)
	}

	crc32 := file.CRC32
	data, err := io.ReadAll(rc)
	if err != nil {
		return Job{}, fmt.Errorf("failed to read file %s: %w", file.Name, err)
	}

	return Job{Z: z, X: x, Y: y, Data: data, Crc32: crc32}, nil
}

func (rz *Reader7z) Open(archivePath string) error {
	r, err := sevenzip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	rz.z = r
	rz.state = 0
	rz.totalFiles = len(r.File)
	return nil
}

func (rz *Reader7z) Close() error {
	return rz.z.Close()
}

func (rz *Reader7z) ReadOne() (Job, bool, error) {
	if rz.state >= rz.totalFiles {
		return Job{}, false, nil
	}
	f := rz.z.File[rz.state]
	rz.state++
	j, err := readJob(f)
	return j, true, err
}

func (rz *Reader7z) ReadNextGood() (Job, bool, error) {
	j, ok, err := rz.ReadOne()
	if !ok {
		return j, ok, err
	}
	if err != nil {
		return rz.ReadNextGood()
	}
	return j, ok, err
}
