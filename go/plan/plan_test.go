package main

import (
	"os"
	"testing"
	"time"
)

func TestProcessedVersionFromDate(t *testing.T) {
	res := ProcessedVersionFromDate(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	if res.String() != "v0.000" {
		t.Fail()
	}
	res = ProcessedVersionFromDate(time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC))
	if res.String() != "v0.120" {
		t.Fail()
	}
	res = ProcessedVersionFromDate(time.Date(2025, 1, 7, 0, 0, 0, 0, time.UTC))
	if res.String() != "v0.144" {
		t.Fail()
	}
	res = ProcessedVersionFromDate(time.Date(2025, 1, 8, 0, 0, 0, 0, time.UTC))
	if res.String() != "v1.000" {
		t.Fail()
	}
}

type mockDirEntry struct {
	name  string
	isDir bool
}

func (m mockDirEntry) Name() string {
	return m.name
}

func (m mockDirEntry) IsDir() bool {
	return m.isDir
}

func (m mockDirEntry) Type() os.FileMode {
	return 0
}

func (m mockDirEntry) Info() (os.FileInfo, error) {
	return nil, nil
}

func TestMakeArchiveDones(t *testing.T) {
	entries := []os.DirEntry{
		mockDirEntry{name: "v0_2025-01-01T01.db", isDir: false},
		mockDirEntry{name: "v0.024_2025-01-02T02.db", isDir: false},
		mockDirEntry{name: "v0.048_2025-01-03T03.db", isDir: false},
		mockDirEntry{name: "tmp", isDir: true},
		mockDirEntry{name: "something.png", isDir: false},
		mockDirEntry{name: "v1.024_2025-01-08T02.db", isDir: false},
		mockDirEntry{name: "v1.048_2025-01-09T03.db", isDir: false},
		mockDirEntry{name: "v1_2025-01-07T01.db", isDir: false},
	}

	res := MakeArchiveDones(entries)
	if res == nil {
		t.Fatal("expected non-nil result")
	}

	if !res.Latest.Datetime.Equal(time.Date(2025, 1, 9, 3, 0, 0, 0, time.UTC)) {
		t.Fatalf("expected Latest.Date to be 2025-01-09, got %v", res.Latest.Datetime)
	}

	if len(res.DatesSet) != 6 {
		t.Fatalf("expected DatesSet to be 6, got %d entries", len(res.DatesSet))
	}

	if len(res.All) != 2 {
		t.Fatalf("expected All to be 2, got %d entries", len(res.All))
	}
}

func PV(vStr string) ProcessedVersion {
	pv, err := ProcessedVersionFromString(vStr)
	if err != nil {
		panic(err)
	}
	return pv
}

func TestMakeJobs(t *testing.T) {
	archives := []GithubRelease{
		{Datetime: time.Date(2025, 1, 9, 00, 0, 0, 0, time.UTC), ProcessedVersion: PV("v1.048")},
		{Datetime: time.Date(2025, 1, 8, 12, 0, 0, 0, time.UTC), ProcessedVersion: PV("v1.036")},
		{Datetime: time.Date(2025, 1, 8, 00, 0, 0, 0, time.UTC), ProcessedVersion: PV("v1.024")},
		{Datetime: time.Date(2025, 1, 7, 12, 0, 0, 0, time.UTC), ProcessedVersion: PV("v1.012")},
		{Datetime: time.Date(2025, 1, 7, 00, 0, 0, 0, time.UTC), ProcessedVersion: PV("v1")},
		{Datetime: time.Date(2025, 1, 3, 00, 0, 0, 0, time.UTC), ProcessedVersion: PV("v0.048")},
		{Datetime: time.Date(2025, 1, 2, 12, 0, 0, 0, time.UTC), ProcessedVersion: PV("v0.036")},
		{Datetime: time.Date(2025, 1, 2, 00, 0, 0, 0, time.UTC), ProcessedVersion: PV("v0.024")},
		{Datetime: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), ProcessedVersion: PV("v0.012")},
		{Datetime: time.Date(2025, 1, 1, 00, 0, 0, 0, time.UTC), ProcessedVersion: PV("v0")},
	}

	archivesDones := MakeArchiveDones([]os.DirEntry{
		mockDirEntry{name: "v0_2025-01-01T01.db", isDir: false},
		mockDirEntry{name: "v0.024_2025-01-02T02.db", isDir: false},
		mockDirEntry{name: "v0.048_2025-01-03T03.db", isDir: false},
	})

	jobs, err := MakeJobs(archives, archivesDones)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(jobs) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(jobs))
	}

	expectedDates := []time.Time{
		time.Date(2025, 1, 7, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 8, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 9, 0, 0, 0, 0, time.UTC),
	}

	for i, job := range jobs {
		if i == 0 {
			if job.isDiff {
				t.Fatalf("expected job %d to be full, got diff", i)
			}
		} else {
			if !job.isDiff {
				t.Fatalf("expected job %d to be diff, got full", i)
			}
			if job.base != ProcessedFileName(PV("v1"), time.Date(2025, 1, 7, 0, 0, 0, 0, time.UTC)) {
				t.Fatalf("expected job %d base to be v1.000_2025-01-07T00, got %s", i, job.base)
			}
		}
		if !job.archive.Datetime.Equal(expectedDates[i]) {
			t.Fatalf("expected job %d date to be %v, got %v", i, expectedDates[i], job.archive.Datetime)
		}
	}
}
