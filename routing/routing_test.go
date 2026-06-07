package routing

import (
	"math"
	"reflect"
	"testing"
)

// TestDijkstraShortestPath verifies Dijkstra's algorithm correctness in normal conditions.
func TestDijkstraShortestPath(t *testing.T) {
	g := GetTestTopology()

	// Normal path finding under "clear" weather
	path, cost, latency := g.FindShortestPath("A", "E", "clear")

	expectedPath := []string{"A", "D", "E"}
	expectedCost := 35.0
	expectedLatency := 0.0 // 0 latency for clear weather

	if !reflect.DeepEqual(path, expectedPath) {
		t.Errorf("expected path %v, got %v", expectedPath, path)
	}

	if cost != expectedCost {
		t.Errorf("expected cost %.2f, got %.2f", expectedCost, cost)
	}

	if latency.Seconds() != expectedLatency {
		t.Errorf("expected latency seconds %.2f, got %.2f", expectedLatency, latency.Seconds())
	}
}

// TestDijkstraWeatherMultipliers verifies that weather modifiers scale weights locally
// and adjust path costs/latencies correctly.
func TestDijkstraWeatherMultipliers(t *testing.T) {
	g := GetTestTopology()

	tests := []struct {
		weather         string
		expectedPath    []string
		expectedCost    float64
		expectedLatency float64 // in seconds
	}{
		{
			weather:         "clear",
			expectedPath:    []string{"A", "D", "E"},
			expectedCost:    35.0,
			expectedLatency: 0.0,
		},
		{
			weather:         "rain",
			expectedPath:    []string{"A", "D", "E"},
			expectedCost:    52.5, // 35 * 1.5
			expectedLatency: 0.05, // 50ms
		},
		{
			weather:         "fog",
			expectedPath:    []string{"A", "D", "E"},
			expectedCost:    70.0, // 35 * 2.0
			expectedLatency: 0.12, // 120ms
		},
	}

	for _, tc := range tests {
		t.Run(tc.weather, func(t *testing.T) {
			path, cost, latency := g.FindShortestPath("A", "E", tc.weather)

			if !reflect.DeepEqual(path, tc.expectedPath) {
				t.Errorf("expected path %v, got %v", tc.expectedPath, path)
			}

			if cost != tc.expectedCost {
				t.Errorf("expected cost %.2f, got %.2f", tc.expectedCost, cost)
			}

			if latency.Seconds() != tc.expectedLatency {
				t.Errorf("expected latency seconds %.3f, got %.3f", tc.expectedLatency, latency.Seconds())
			}
		})
	}
}

// TestDijkstraEdgeCases verifies disconnected nodes, same start/end nodes, and invalid nodes.
func TestDijkstraEdgeCases(t *testing.T) {
	g := NewGraph()
	g.AddEdge("A", "B", 10.0)
	g.AddEdge("B", "C", 5.0)
	// Node D is completely disconnected

	t.Run("Same start and end node", func(t *testing.T) {
		path, cost, _ := g.FindShortestPath("A", "A", "clear")
		expectedPath := []string{"A"}
		expectedCost := 0.0

		if !reflect.DeepEqual(path, expectedPath) {
			t.Errorf("expected path %v, got %v", expectedPath, path)
		}
		if cost != expectedCost {
			t.Errorf("expected cost %.2f, got %.2f", expectedCost, cost)
		}
	})

	t.Run("No path to target (disconnected graph)", func(t *testing.T) {
		path, cost, _ := g.FindShortestPath("A", "D", "clear")
		if path != nil {
			t.Errorf("expected path to be nil, got %v", path)
		}
		if cost != math.MaxFloat64 {
			t.Errorf("expected cost to be MaxFloat64, got %.2f", cost)
		}
	})

	t.Run("Non-existent start node", func(t *testing.T) {
		path, cost, _ := g.FindShortestPath("Z", "C", "clear")
		if path != nil {
			t.Errorf("expected path to be nil, got %v", path)
		}
		if cost != math.MaxFloat64 {
			t.Errorf("expected cost to be MaxFloat64, got %.2f", cost)
		}
	})
}

// BenchmarkDijkstraShortestPath measures Dijkstra path-finding performance.
func BenchmarkDijkstraShortestPath(b *testing.B) {
	g := GetTestTopology()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = g.FindShortestPath("A", "E", "rain")
	}
}
