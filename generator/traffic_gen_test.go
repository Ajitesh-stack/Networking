package main

import (
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

// TestDecodeGeohash validates geohash decoding correctness against expected coordinates.
func TestDecodeGeohash(t *testing.T) {
	tests := []struct {
		geohash  string
		expectedLat float64
		expectedLon float64
		tolerance   float64 // Acceptable variance due to bounding box centering
	}{
		{
			// Geohash: "qp02zt" (near Bangalore/ocean region)
			geohash:     "qp02zt",
			expectedLat: -5.462952,
			expectedLon: 90.686646,
			tolerance:   0.05,
		},
		{
			// Geohash: "qp08gt"
			geohash:     "qp08gt",
			expectedLat: -5.462952,
			expectedLon: 90.862427,
			tolerance:   0.05,
		},
		{
			// Invalid characters should be ignored safely
			geohash:     "qp08gt#xyz",
			expectedLat: -5.462952,
			expectedLon: 90.862427,
			tolerance:   0.05,
		},
	}

	for _, tc := range tests {
		t.Run(tc.geohash, func(t *testing.T) {
			lat, lon := decodeGeohash(tc.geohash)

			if math.Abs(lat-tc.expectedLat) > tc.tolerance {
				t.Errorf("expected lat close to %.6f, got %.6f (diff: %.6f)", tc.expectedLat, lat, math.Abs(lat-tc.expectedLat))
			}

			if math.Abs(lon-tc.expectedLon) > tc.tolerance {
				t.Errorf("expected lon close to %.6f, got %.6f (diff: %.6f)", tc.expectedLon, lon, math.Abs(lon-tc.expectedLon))
			}
		})
	}
}

// TestStreamDatasetValid tests streaming from a valid CSV dataset.
func TestStreamDatasetValid(t *testing.T) {
	content := `Index,geohash,demand,Weather
1,qp02zt,0.5,rainy
2,qp08gt,0.05,foggy
3,qp08gt,0.15,sunny
`
	tmpFile, err := os.CreateTemp("", "mobility_test_*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	packetChan := make(chan PacketJob, 10)
	err = streamDataset(tmpFile.Name(), packetChan)
	if err != nil {
		t.Fatalf("streamDataset failed: %v", err)
	}
	close(packetChan)

	var jobs []PacketJob
	for job := range packetChan {
		jobs = append(jobs, job)
	}

	if len(jobs) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(jobs))
	}

	// Verify job 1: Index 1, demand 0.5 (> 0.1, pacing 100 microseconds), weather rainy -> rain
	if !strings.Contains(jobs[0].Payload, "client=qp02zt") || !strings.Contains(jobs[0].Payload, "seq=1") || !strings.Contains(jobs[0].Payload, "weather=rain") {
		t.Errorf("job 0 payload invalid: %q", jobs[0].Payload)
	}
	if jobs[0].SleepDuration != 100*time.Microsecond {
		t.Errorf("expected sleep duration 100us, got %v", jobs[0].SleepDuration)
	}

	// Verify job 2: Index 2, demand 0.05 (<= 0.1, pacing 1500 microseconds), weather foggy -> fog
	if !strings.Contains(jobs[1].Payload, "client=qp08gt") || !strings.Contains(jobs[1].Payload, "seq=2") || !strings.Contains(jobs[1].Payload, "weather=fog") {
		t.Errorf("job 1 payload invalid: %q", jobs[1].Payload)
	}
	if jobs[1].SleepDuration != 1500*time.Microsecond {
		t.Errorf("expected sleep duration 1500us, got %v", jobs[1].SleepDuration)
	}

	// Verify job 3: Index 3, demand 0.15 (> 0.1, pacing 100 microseconds), weather sunny -> clear
	if !strings.Contains(jobs[2].Payload, "client=qp08gt") || !strings.Contains(jobs[2].Payload, "seq=3") || !strings.Contains(jobs[2].Payload, "weather=clear") {
		t.Errorf("job 2 payload invalid: %q", jobs[2].Payload)
	}
	if jobs[2].SleepDuration != 100*time.Microsecond {
		t.Errorf("expected sleep duration 100us, got %v", jobs[2].SleepDuration)
	}
}

// TestStreamDatasetMissingHeaders tests streaming from a CSV that lacks necessary columns.
func TestStreamDatasetMissingHeaders(t *testing.T) {
	content := `Index,geohash,demand
1,qp02zt,0.5
`
	tmpFile, err := os.CreateTemp("", "mobility_test_*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	packetChan := make(chan PacketJob, 10)
	err = streamDataset(tmpFile.Name(), packetChan)
	if err == nil {
		t.Error("expected streamDataset to return error for missing headers, got nil")
	} else if !strings.Contains(err.Error(), "essential CSV columns") {
		t.Errorf("expected error to contain 'essential CSV columns', got %v", err)
	}
}

// TestStreamDatasetInvalidFile verifies that streamDataset returns error for a missing file.
func TestStreamDatasetInvalidFile(t *testing.T) {
	packetChan := make(chan PacketJob, 10)
	err := streamDataset("non_existent_file.csv", packetChan)
	if err == nil {
		t.Error("expected error for non existent file, got nil")
	}
}

