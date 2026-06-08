package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/Ajitesh-stack/spatial-ingestion-server/metrics"
)

var dashboardStartTime time.Time

// StartDashboard starts a lightweight HTTP metrics dashboard on the given port.
func StartDashboard(port string, m *metrics.SystemMetrics) {
	dashboardStartTime = time.Now()
	mux := newDashboardMux(m)

	go func() {
		log.Printf("[Dashboard] Listening on http://localhost%s", port)
		if err := http.ListenAndServe(port, mux); err != nil {
			log.Printf("[Dashboard] HTTP server failed: %v", err)
		}
	}()
}

// newDashboardMux returns the ServeMux populated with metrics endpoints for testing.
func newDashboardMux(m *metrics.SystemMetrics) *http.ServeMux {
	mux := http.NewServeMux()

	// JSON endpoint returning raw metric values
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		total := atomic.LoadUint64(&m.TotalPacketsProcessed)
		hits := atomic.LoadUint64(&m.CacheHits)
		misses := atomic.LoadUint64(&m.CacheMisses)
		latency := atomic.LoadUint64(&m.TotalInjectedLatencyMs)

		hitRate := calcHitRate(hits, misses)
		uptime := int64(time.Since(dashboardStartTime).Seconds())

		modeStr := "Sequential"
		if atomic.LoadInt32(&activeMode) == ModeZipfian {
			modeStr = "Zipfian"
		}

		response := map[string]interface{}{
			"total_packets":       total,
			"cache_hits":          hits,
			"cache_misses":        misses,
			"hit_rate_pct":        hitRate,
			"injected_latency_ms": latency,
			"uptime_seconds":      uptime,
			"mode":                modeStr,
		}

		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	return mux
}

// calcHitRate calculates the cache hit rate percentage rounded to 2 decimal places.
func calcHitRate(hits, misses uint64) float64 {
	totalReads := hits + misses
	if totalReads == 0 {
		return 0.0
	}
	hitRate := (float64(hits) / float64(totalReads)) * 100.0
	return float64(int(hitRate*100+0.5)) / 100.0
}
