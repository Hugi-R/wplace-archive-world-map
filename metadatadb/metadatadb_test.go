package main

import (
	"testing"
	"time"
)

func TestProcessedVersionFromDate(t *testing.T) {
	res := ProcessedVersionFromDate(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	if res != "v0.000" {
		t.Fail()
	}
	res = ProcessedVersionFromDate(time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC))
	if res != "v0.120" {
		t.Fail()
	}
	res = ProcessedVersionFromDate(time.Date(2025, 1, 7, 0, 0, 0, 0, time.UTC))
	if res != "v0.144" {
		t.Fail()
	}
	res = ProcessedVersionFromDate(time.Date(2025, 1, 8, 0, 0, 0, 0, time.UTC))
	if res != "v1.000" {
		t.Fail()
	}
}
