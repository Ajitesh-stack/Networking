package main

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Ajitesh-stack/spatial-ingestion-server/metrics"
)

// TestMetricsEndpointJSON verifies the /metrics JSON endpoint correctness and structure.
func TestMetricsEndpointJSON(t *testing.T) {
	m := metrics.NewSystemMetrics()
	m.TotalPacketsProcessed = 1000
	m.CacheHits = 780
	m.CacheMisses = 220
	m.TotalInjectedLatencyMs = 45000

	// Use httptest.NewServer to run on a random available port dynamically
	ts := httptest.NewServer(newDashboardMux(m))
	defer ts.Close()

	res, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("Failed to make GET request: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Errorf("Expected status code 200, got %d", res.StatusCode)
	}

	contentType := res.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type 'application/json', got %q", contentType)
	}

	var data struct {
		TotalPackets      uint64  `json:"total_packets"`
		CacheHits         uint64  `json:"cache_hits"`
		CacheMisses       uint64  `json:"cache_misses"`
		HitRatePct        float64 `json:"hit_rate_pct"`
		InjectedLatencyMs uint64  `json:"injected_latency_ms"`
	}

	if err := json.NewDecoder(res.Body).Decode(&data); err != nil {
		t.Fatalf("Failed to decode JSON response: %v", err)
	}

	if data.TotalPackets != 1000 {
		t.Errorf("Expected total_packets 1000, got %d", data.TotalPackets)
	}
	if data.CacheHits != 780 {
		t.Errorf("Expected cache_hits 780, got %d", data.CacheHits)
	}
	if data.CacheMisses != 220 {
		t.Errorf("Expected cache_misses 220, got %d", data.CacheMisses)
	}
	if math.Abs(data.HitRatePct-78.0) > 0.01 {
		t.Errorf("Expected hit_rate_pct close to 78.0, got %f", data.HitRatePct)
	}
}

// TestHitRateEdgeCases validates cache hit rate calculation edge cases directly.
func TestHitRateEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		hits     uint64
		misses   uint64
		expected float64
	}{
		{
			name:     "Zero hits and zero misses",
			hits:     0,
			misses:   0,
			expected: 0.0,
		},
		{
			name:     "100% hits",
			hits:     500,
			misses:   0,
			expected: 100.0,
		},
		{
			name:     "0% hits",
			hits:     0,
			misses:   500,
			expected: 0.0,
		},
		{
			name:     "Normal fractional hits",
			hits:     1,
			misses:   2,
			expected: 33.33, // 1/3 = 33.3333... rounded to 2 decimal places
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := calcHitRate(tc.hits, tc.misses)
			if math.Abs(got-tc.expected) > 0.001 {
				t.Errorf("calcHitRate(%d, %d): got %f, expected %f", tc.hits, tc.misses, got, tc.expected)
			}
		})
	}
}
