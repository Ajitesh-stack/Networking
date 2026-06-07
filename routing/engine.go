package routing

import (
	"math"
	"time"
)

// CalculateDynamicCost calculates the scaled edge cost and sleep latency for a weather condition.
func CalculateDynamicCost(baseCost float64, weather string) (float64, time.Duration) {
	switch weather {
	case "rain":
		return baseCost * 1.5, 50 * time.Millisecond
	case "fog":
		return baseCost * 2.0, 120 * time.Millisecond
	default:
		return baseCost * 1.0, 0
	}
}

// FindShortestPath calculates Dijkstra's shortest path between start and end node
// dynamically applying weather cost multipliers locally.
// The Graph struct parameters remain completely unmodified (Read-Only) for concurrency safety.
func (g *Graph) FindShortestPath(start, end string, weather string) ([]string, float64, time.Duration) {
	dist := make(map[string]float64)
	prev := make(map[string]string)
	visited := make(map[string]bool)

	// Collect all nodes in the graph (both sources and destinations)
	nodes := make(map[string]bool)
	for from, edges := range g.AdjacencyList {
		nodes[from] = true
		for _, edge := range edges {
			nodes[edge.To] = true
		}
	}

	for node := range nodes {
		dist[node] = math.MaxFloat64
	}
	dist[start] = 0.0

	for {
		// Find unvisited node with smallest distance
		var u string
		minDist := math.MaxFloat64
		for node, d := range dist {
			if !visited[node] && d < minDist {
				minDist = d
				u = node
			}
		}

		// Stop if unvisited node is unreachable or end node is reached
		if u == "" || u == end {
			break
		}

		visited[u] = true

		// Relax neighbors
		for _, edge := range g.AdjacencyList[u] {
			if visited[edge.To] {
				continue
			}

			// Apply multiplier locally (No graph weight mutation to preserve thread safety)
			dynamicWeight, _ := CalculateDynamicCost(edge.Weight, weather)
			alt := dist[u] + dynamicWeight
			if alt < dist[edge.To] {
				dist[edge.To] = alt
				prev[edge.To] = u
			}
		}
	}

	if dist[end] == math.MaxFloat64 {
		return nil, math.MaxFloat64, 0
	}

	// Reconstruct path
	path := []string{}
	for u := end; u != ""; u = prev[u] {
		path = append([]string{u}, path...)
	}

	// Get weather-induced routing delay
	_, latencySleep := CalculateDynamicCost(0, weather)

	return path, dist[end], latencySleep
}
