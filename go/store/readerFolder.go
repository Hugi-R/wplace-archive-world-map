package store

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"strings"
)

type ReaderFolder struct {
	folder      string
	stack       [][]os.DirEntry
	state       []int
	stackLevel  int
	currentPath []string
}

func (rf *ReaderFolder) readJob(file os.DirEntry) (Job, error) {
	if file.IsDir() {
		return Job{}, fmt.Errorf("%s is dir", file.Name())
	}
	pathParts := append(rf.currentPath, file.Name())
	fullPath := strings.Join(pathParts, "/")
	size := len(pathParts)
	if size < 2 {
		return Job{}, fmt.Errorf("unexpected file path structure: %s", fullPath)
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

	f, err := os.Open(fullPath)
	if err != nil {
		return Job{}, fmt.Errorf("failed to open file %s: %w", fullPath, err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return Job{}, fmt.Errorf("failed to read file %s: %w", fullPath, err)
	}
	crc := crc32.ChecksumIEEE(data)

	return Job{Z: z, X: x, Y: y, Data: data, Crc32: crc}, nil
}

func (rf *ReaderFolder) Open(folder string) error {
	rf.folder = folder
	items, err := os.ReadDir(folder)
	if err != nil {
		return err
	}
	rf.stackLevel = 0
	rf.stack = make([][]os.DirEntry, 1)
	rf.stack[rf.stackLevel] = items
	rf.state = make([]int, 1)
	rf.state[rf.stackLevel] = 0
	rf.currentPath = make([]string, 1)
	rf.currentPath[rf.stackLevel] = folder
	return nil
}

func (rf *ReaderFolder) Close() error {
	return nil
}

func (rf *ReaderFolder) ReadOne() (job Job, ok bool, err error) {
	state := rf.state[rf.stackLevel]
	if state >= len(rf.stack[rf.stackLevel]) {
		rf.closeDir()
		return Job{}, false, nil
	}
	entry := rf.stack[rf.stackLevel][state]
	rf.state[rf.stackLevel]++
	if entry.IsDir() {
		rf.openDir(entry)
		return Job{}, false, nil
	}
	j, err := rf.readJob(entry)
	return j, true, err
}

func (rf *ReaderFolder) openDir(dir os.DirEntry) error {
	pathParts := append(rf.currentPath, dir.Name())
	fullPath := strings.Join(pathParts, "/")
	// fmt.Printf("Open dir: %s\n", fullPath)
	items, err := os.ReadDir(fullPath)
	if err != nil {
		return err
	}
	rf.stack = append(rf.stack, items)
	rf.state = append(rf.state, 0)
	rf.currentPath = append(rf.currentPath, dir.Name())
	rf.stackLevel++
	return nil
}

func (rf *ReaderFolder) closeDir() {
	// fmt.Println("close dir")
	rf.stack = rf.stack[:rf.stackLevel]
	rf.state = rf.state[:rf.stackLevel]
	rf.currentPath = rf.currentPath[:rf.stackLevel]
	rf.stackLevel--
}

func (rf *ReaderFolder) ReadNextGood() (j Job, ok bool, err error) {
	j, ok, err = rf.ReadOne()
	if !ok && (rf.stackLevel == -1) {
		// end
		return j, ok, err
	}
	if err != nil || !ok {
		return rf.ReadNextGood()
	}
	return j, ok, err
}
