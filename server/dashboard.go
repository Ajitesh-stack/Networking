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

	mux := http.NewServeMux()

	// JSON endpoint returning raw metric values
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		total := atomic.LoadUint64(&m.TotalPacketsProcessed)
		hits := atomic.LoadUint64(&m.CacheHits)
		misses := atomic.LoadUint64(&m.CacheMisses)
		latency := atomic.LoadUint64(&m.TotalInjectedLatencyMs)

		hitRate := 0.0
		totalReads := hits + misses
		if totalReads > 0 {
			hitRate = (float64(hits) / float64(totalReads)) * 100.0
		}
		// Round to 2 decimal places
		hitRate = float64(int(hitRate*100+0.5)) / 100.0

		uptime := int64(time.Since(dashboardStartTime).Seconds())

		response := map[string]interface{}{
			"total_packets":       total,
			"cache_hits":          hits,
			"cache_misses":        misses,
			"hit_rate_pct":        hitRate,
			"injected_latency_ms": latency,
			"uptime_seconds":      uptime,
		}

		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	// HTML page endpoint serving the main dashboard
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(htmlPage))
	})

	go func() {
		log.Printf("[Dashboard] Listening on http://localhost%s", port)
		if err := http.ListenAndServe(port, mux); err != nil {
			log.Printf("[Dashboard] HTTP server failed: %v", err)
		}
	}()
}

const htmlPage = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Spatial Ingestion Server — Live Metrics</title>
    <style>
        body {
            background-color: #0d1117;
            color: #c9d1d9;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji";
            margin: 0;
            padding: 24px;
            display: flex;
            flex-direction: column;
            align-items: center;
        }
        .container {
            max-width: 1200px;
            width: 100%;
        }
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid #30363d;
            padding-bottom: 16px;
            margin-bottom: 24px;
        }
        h1 {
            font-size: 24px;
            margin: 0;
            color: #f0f6fc;
            display: flex;
            align-items: center;
            gap: 12px;
        }
        .status-badge {
            font-size: 13px;
            font-weight: 600;
            padding: 4px 10px;
            border-radius: 2em;
            display: flex;
            align-items: center;
            gap: 6px;
            transition: all 0.3s ease;
        }
        .status-healthy {
            background-color: rgba(46, 160, 67, 0.15);
            color: #3fb950;
            border: 1px solid rgba(46, 160, 67, 0.4);
        }
        .status-warming {
            background-color: rgba(210, 153, 34, 0.15);
            color: #d29922;
            border: 1px solid rgba(210, 153, 34, 0.4);
        }
        .status-cold {
            background-color: rgba(248, 81, 73, 0.15);
            color: #f85149;
            border: 1px solid rgba(248, 81, 73, 0.4);
        }
        .status-badge::before {
            content: "";
            display: inline-block;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background-color: currentColor;
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 16px;
            margin-bottom: 24px;
        }
        @media (max-width: 768px) {
            .grid {
                grid-template-columns: 1fr;
            }
        }
        .card {
            background-color: #161b22;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 20px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            transition: transform 0.2s, border-color 0.2s;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .card:hover {
            transform: translateY(-2px);
            border-color: #58a6ff;
        }
        .card-title {
            font-size: 12px;
            color: #8b949e;
            font-weight: 600;
            margin-bottom: 6px;
            text-transform: uppercase;
            letter-spacing: 0.8px;
        }
        .card-value {
            font-size: 28px;
            font-weight: 700;
            color: #f0f6fc;
            font-variant-numeric: tabular-nums;
        }
        .card-trend {
            margin-top: 8px;
            font-size: 12px;
            color: #8b949e;
        }
        .chart-container {
            background-color: #161b22;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 24px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .chart-title {
            font-size: 15px;
            font-weight: 600;
            color: #f0f6fc;
            margin-bottom: 16px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .chart-legend {
            display: flex;
            gap: 16px;
            font-size: 12px;
        }
        .legend-item {
            display: flex;
            align-items: center;
            gap: 6px;
            color: #8b949e;
        }
        .legend-color {
            width: 12px;
            height: 12px;
            border-radius: 2px;
        }
        canvas {
            width: 100%;
            height: 250px;
            display: block;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>📡 Spatial Ingestion Server — Live Metrics</h1>
            <div id="status-badge" class="status-badge status-cold">
                <span id="status-text">Cold</span>
            </div>
        </header>

        <div class="grid">
            <div class="card">
                <div class="card-title">Total Packets</div>
                <div id="total-packets" class="card-value">0</div>
                <div class="card-trend">Ingested telemetry frames</div>
            </div>
            <div class="card">
                <div class="card-title">Cache Hits</div>
                <div id="cache-hits" class="card-value">0</div>
                <div class="card-trend" style="color: #3fb950;">Successful LRU reads</div>
            </div>
            <div class="card">
                <div class="card-title">Cache Misses</div>
                <div id="cache-misses" class="card-value">0</div>
                <div class="card-trend" style="color: #f85149;">Reads requiring server sets</div>
            </div>
            <div class="card">
                <div class="card-title">Cache Hit Rate</div>
                <div id="hit-rate" class="card-value">0.00%</div>
                <div class="card-trend">Target: 75.00% – 82.00%</div>
            </div>
            <div class="card">
                <div class="card-title">Injected Latency</div>
                <div id="injected-latency" class="card-value">0 ms</div>
                <div class="card-trend">Weather transmission delay</div>
            </div>
            <div class="card">
                <div class="card-title">Uptime</div>
                <div id="uptime" class="card-value">00:00:00</div>
                <div class="card-trend">Since server startup</div>
            </div>
        </div>

        <div class="chart-container">
            <div class="chart-title">
                <span>Hit Rate Trend (Last 60 Seconds)</span>
                <div class="chart-legend">
                    <div class="legend-item">
                        <div class="legend-color" style="background-color: #58a6ff;"></div>
                        <span>Hit Rate</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="border: 1px dashed rgba(46, 160, 67, 0.6); background: transparent;"></div>
                        <span>Target Band (75% - 82%)</span>
                    </div>
                </div>
            </div>
            <canvas id="sparkline-chart"></canvas>
        </div>
    </div>

    <script>
        const history = [];
        const canvas = document.getElementById('sparkline-chart');
        const ctx = canvas.getContext('2d');

        function formatUptime(totalSeconds) {
            const h = Math.floor(totalSeconds / 3600).toString().padStart(2, '0');
            const m = Math.floor((totalSeconds % 3600) / 60).toString().padStart(2, '0');
            const s = (totalSeconds % 60).toString().padStart(2, '0');
            return h + ':' + m + ':' + s;
        }

        function resizeCanvas() {
            const rect = canvas.getBoundingClientRect();
            canvas.width = rect.width * window.devicePixelRatio;
            canvas.height = rect.height * window.devicePixelRatio;
            ctx.scale(window.devicePixelRatio, window.devicePixelRatio);
            drawChart();
        }

        window.addEventListener('resize', resizeCanvas);

        function updateBadge(hitRate) {
            const badge = document.getElementById('status-badge');
            const text = document.getElementById('status-text');
            badge.className = 'status-badge';
            
            if (hitRate >= 75) {
                badge.classList.add('status-healthy');
                text.textContent = 'Healthy';
            } else if (hitRate >= 50) {
                badge.classList.add('status-warming');
                text.textContent = 'Warming';
            } else {
                badge.classList.add('status-cold');
                text.textContent = 'Cold';
            }
        }

        function drawChart() {
            const w = canvas.width / window.devicePixelRatio;
            const h = canvas.height / window.devicePixelRatio;
            ctx.clearRect(0, 0, w, h);

            if (w === 0 || h === 0) return;

            // Draw Y Grid lines & Target lines (75% and 82%)
            const levels = [0, 25, 50, 75, 82, 100];
            ctx.textAlign = 'right';
            ctx.textBaseline = 'middle';
            
            levels.forEach(level => {
                const y = h - (level / 100) * (h - 40) - 20;

                // Check if target line
                if (level === 75 || level === 82) {
                    ctx.strokeStyle = 'rgba(46, 160, 67, 0.4)';
                    ctx.lineWidth = 1;
                    ctx.setLineDash([5, 5]);
                    ctx.fillStyle = '#3fb950';
                    ctx.font = '10px -apple-system, sans-serif';
                    ctx.fillText(level + '% Target', w - 10, y - 8);
                } else {
                    ctx.strokeStyle = '#21262d';
                    ctx.lineWidth = 1;
                    ctx.setLineDash([]);
                    ctx.fillStyle = '#8b949e';
                    ctx.font = '10px -apple-system, sans-serif';
                    ctx.fillText(level + '%', w - 10, y);
                }

                ctx.beginPath();
                ctx.moveTo(10, y);
                ctx.lineTo(w - 85, y);
                ctx.stroke();
            });

            ctx.setLineDash([]);

            // Draw target band shaded area
            const y82 = h - (82 / 100) * (h - 40) - 20;
            const y75 = h - (75 / 100) * (h - 40) - 20;
            ctx.fillStyle = 'rgba(46, 160, 67, 0.05)';
            ctx.fillRect(10, y82, w - 95, y75 - y82);

            // Draw the line chart
            if (history.length < 2) return;

            ctx.lineWidth = 2.5;
            ctx.strokeStyle = '#58a6ff';
            ctx.beginPath();

            const chartWidth = w - 95;
            const step = chartWidth / 59; // 60 slots

            history.forEach((rate, i) => {
                const x = 10 + i * step;
                const y = h - (rate / 100) * (h - 40) - 20;

                if (i === 0) {
                    ctx.moveTo(x, y);
                } else {
                    ctx.lineTo(x, y);
                }
            });
            ctx.stroke();

            // Draw gradient area under the line
            ctx.lineTo(10 + (history.length - 1) * step, h - 20);
            ctx.lineTo(10, h - 20);
            ctx.closePath();
            const grad = ctx.createLinearGradient(0, 0, 0, h);
            grad.addColorStop(0, 'rgba(88, 166, 255, 0.15)');
            grad.addColorStop(1, 'rgba(88, 166, 255, 0)');
            ctx.fillStyle = grad;
            ctx.fill();
        }

        async function fetchMetrics() {
            try {
                const res = await fetch('/metrics');
                const data = await res.json();

                document.getElementById('total-packets').textContent = data.total_packets.toLocaleString();
                document.getElementById('cache-hits').textContent = data.cache_hits.toLocaleString();
                document.getElementById('cache-misses').textContent = data.cache_misses.toLocaleString();
                document.getElementById('hit-rate').textContent = data.hit_rate_pct.toFixed(2) + '%';
                document.getElementById('injected-latency').textContent = data.injected_latency_ms.toLocaleString() + ' ms';
                document.getElementById('uptime').textContent = formatUptime(data.uptime_seconds);

                updateBadge(data.hit_rate_pct);

                history.push(data.hit_rate_pct);
                if (history.length > 60) {
                    history.shift();
                }

                drawChart();
            } catch (err) {
                console.error('Failed to fetch metrics:', err);
            }
        }

        // Initialize
        setTimeout(resizeCanvas, 50);
        setInterval(fetchMetrics, 1000);
        fetchMetrics();
    </script>
</body>
</html>`
