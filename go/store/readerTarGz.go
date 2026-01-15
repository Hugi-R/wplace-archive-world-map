package store

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"strings"
)

type ReaderTarGz struct {
	tar  *tar.Reader
	file *os.File
}

func (rtgz *ReaderTarGz) Open(archivePath string) error {
	f, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	rtgz.file = f
	uncompressedStream, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	tarReader := tar.NewReader(uncompressedStream)
	rtgz.tar = tarReader
	return nil
}

func (rtgz *ReaderTarGz) Close() error {
	return rtgz.file.Close()
}

func (rtgz *ReaderTarGz) ReadOne() (Job, bool, error) {
	header, err := rtgz.tar.Next()

	if err == io.EOF {
		return Job{}, false, nil
	}

	if err != nil {
		return Job{}, false, err
	}

	switch header.Typeflag {
	case tar.TypeDir:
		return rtgz.ReadOne()
	case tar.TypeSymlink:
		return rtgz.ReadOne()
	case tar.TypeReg:
		pathParts := strings.Split(header.Name, "/")
		size := len(pathParts)
		if size < 2 {
			return Job{}, true, fmt.Errorf("unexpected file path structure: %s", header.Name)
		}
		z := 11
		x, err := strconv.Atoi(pathParts[size-2])
		if err != nil {
			return Job{}, true, fmt.Errorf("failed to parse x coordinate from path: %w", err)
		}
		y, err := strconv.Atoi(strings.TrimSuffix(pathParts[size-1], ".png"))
		if err != nil {
			return Job{}, true, fmt.Errorf("failed to parse y coordinate from path: %w", err)
		}
		if header.Size > int64(10*1024*1024) {
			return Job{}, true, fmt.Errorf("file %s size too large: %d bytes", header.Name, header.Size)
		}
		data := make([]byte, header.Size)
		_, err = io.ReadFull(rtgz.tar, data)
		if err != nil {
			return Job{}, true, fmt.Errorf("failed to read file %s: %w", header.Name, err)
		}
		crc := crc32.ChecksumIEEE(data)

		f, _ := os.Create("img.png")
		f.Write(data)
		f.Close()

		return Job{Z: z, X: x, Y: y, Data: data, Crc32: crc}, true, nil
	default:
		return Job{}, true, fmt.Errorf("unknown type: %v in %s", header.Typeflag, header.Name)
	}
}

func (rtgz *ReaderTarGz) ReadNextGood() (Job, bool, error) {
	j, cont, err := rtgz.ReadOne()
	if !cont {
		// Cannot continue
		return j, cont, err
	}
	if err != nil {
		// Skip bad entry
		return rtgz.ReadNextGood()
	}
	return j, cont, err
}
