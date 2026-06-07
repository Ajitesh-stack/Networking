package routing

// Edge represents a directed link to a destination node with a baseline cost.
type Edge struct {
	To     string
	Weight float64
}

// Graph represents the network topology of hubs using an adjacency list.
type Graph struct {
	AdjacencyList map[string][]Edge
}

// NewGraph creates an empty Graph.
func NewGraph() *Graph {
	return &Graph{
		AdjacencyList: make(map[string][]Edge),
	}
}

// AddEdge adds a directed link from one node to another with a baseline weight.
func (g *Graph) AddEdge(from, to string, weight float64) {
	g.AdjacencyList[from] = append(g.AdjacencyList[from], Edge{To: to, Weight: weight})
}

// GetTestTopology initializes a static network graph pre-populated with nodes A through E.
func GetTestTopology() *Graph {
	g := NewGraph()
	// Connect A to B and D
	g.AddEdge("A", "B", 10.0)
	g.AddEdge("A", "D", 25.0)

	// Connect B to C
	g.AddEdge("B", "C", 15.0)

	// Connect C to E
	g.AddEdge("C", "E", 20.0)

	// Connect D to E
	g.AddEdge("D", "E", 10.0)

	return g
}
