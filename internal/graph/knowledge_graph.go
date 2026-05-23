package graph

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// NodeType identifies the kind of entity in the knowledge graph.
type NodeType string

// NodeTopic and related values define constants for this package.
const (
	NodeTopic         NodeType = "topic"
	NodeConsumerGroup NodeType = "consumer_group"
	NodeProducer      NodeType = "producer"
	NodeSchema        NodeType = "schema"
	NodeConnector     NodeType = "connector"
)

// EdgeType identifies the kind of relationship.
type EdgeType string

// EdgeProducesTo and related values define constants for this package.
const (
	EdgeProducesTo   EdgeType = "produces_to"
	EdgeConsumesFrom EdgeType = "consumes_from"
	EdgeUsesSchema   EdgeType = "uses_schema"
	EdgeConnectsTo   EdgeType = "connects_to"
	EdgeDependsOn    EdgeType = "depends_on"
)

// Node is an entity in the knowledge graph.
type Node struct {
	ID         string         `json:"id"`
	Type       NodeType       `json:"type"`
	Name       string         `json:"name"`
	Properties map[string]any `json:"properties,omitempty"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

// Edge is a relationship between two nodes.
type Edge struct {
	From       string         `json:"from"`
	To         string         `json:"to"`
	Type       EdgeType       `json:"type"`
	Properties map[string]any `json:"properties,omitempty"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

// QueryResult is the result of a graph query.
type QueryResult struct {
	Nodes      []Node     `json:"nodes"`
	Edges      []Edge     `json:"edges"`
	Paths      [][]string `json:"paths,omitempty"`
	TotalNodes int        `json:"total_nodes"`
	TotalEdges int        `json:"total_edges"`
	QueryTime  string     `json:"query_time"`
}

// KnowledgeGraph is the embedded knowledge graph engine.
type KnowledgeGraph struct {
	nodes   map[string]Node
	edges   []Edge
	adjList map[string][]Edge // adjacency list: nodeID -> outgoing edges
	revAdj  map[string][]Edge // reverse adjacency: nodeID -> incoming edges
	mu      sync.RWMutex
	logger  *slog.Logger
	ttl     time.Duration
}

// NewKnowledgeGraph creates a new knowledge graph.
func NewKnowledgeGraph(logger *slog.Logger) *KnowledgeGraph {
	return &KnowledgeGraph{
		nodes:   make(map[string]Node),
		adjList: make(map[string][]Edge),
		revAdj:  make(map[string][]Edge),
		logger:  logger,
		ttl:     30 * time.Minute,
	}
}

// AddNode adds or updates a node.
func (kg *KnowledgeGraph) AddNode(node Node) {
	kg.mu.Lock()
	defer kg.mu.Unlock()
	node.UpdatedAt = time.Now()
	kg.nodes[node.ID] = node
}

// AddEdge adds a relationship.
func (kg *KnowledgeGraph) AddEdge(edge Edge) {
	kg.mu.Lock()
	defer kg.mu.Unlock()
	edge.UpdatedAt = time.Now()
	kg.edges = append(kg.edges, edge)
	kg.adjList[edge.From] = append(kg.adjList[edge.From], edge)
	kg.revAdj[edge.To] = append(kg.revAdj[edge.To], edge)
}

// DiscoverFromTopology populates the graph from topic and consumer group data.
func (kg *KnowledgeGraph) DiscoverFromTopology(topics []TopicInfo, groups []GroupInfo, schemas []SchemaLink) {
	kg.mu.Lock()
	defer kg.mu.Unlock()

	now := time.Now()

	for _, t := range topics {
		kg.nodes[t.Name] = Node{
			ID: t.Name, Type: NodeTopic, Name: t.Name, UpdatedAt: now,
			Properties: map[string]any{"partitions": t.Partitions, "replication_factor": t.ReplicationFactor},
		}
	}

	for _, g := range groups {
		nodeID := "group:" + g.GroupID
		kg.nodes[nodeID] = Node{
			ID: nodeID, Type: NodeConsumerGroup, Name: g.GroupID, UpdatedAt: now,
			Properties: map[string]any{"state": g.State, "members": g.MemberCount},
		}
		for _, topic := range g.Topics {
			edge := Edge{From: topic, To: nodeID, Type: EdgeConsumesFrom, UpdatedAt: now}
			kg.edges = append(kg.edges, edge)
			kg.adjList[topic] = append(kg.adjList[topic], edge)
			kg.revAdj[nodeID] = append(kg.revAdj[nodeID], edge)
		}
	}

	for _, s := range schemas {
		schemaID := "schema:" + s.Subject
		kg.nodes[schemaID] = Node{
			ID: schemaID, Type: NodeSchema, Name: s.Subject, UpdatedAt: now,
			Properties: map[string]any{"type": s.SchemaType},
		}
		if s.Topic != "" {
			edge := Edge{From: s.Topic, To: schemaID, Type: EdgeUsesSchema, UpdatedAt: now}
			kg.edges = append(kg.edges, edge)
			kg.adjList[s.Topic] = append(kg.adjList[s.Topic], edge)
			kg.revAdj[schemaID] = append(kg.revAdj[schemaID], edge)
		}
	}
}

// TopicInfo for graph discovery.
type TopicInfo struct {
	Name              string
	Partitions        int
	ReplicationFactor int
}

// GroupInfo for graph discovery.
type GroupInfo struct {
	GroupID     string
	State       string
	MemberCount int
	Topics      []string
}

// SchemaLink for graph discovery.
type SchemaLink struct {
	Subject    string
	SchemaType string
	Topic      string
}

// Query executes a graph query with a simplified Cypher-like syntax.
func (kg *KnowledgeGraph) Query(queryStr string) (*QueryResult, error) {
	start := time.Now()
	kg.mu.RLock()
	defer kg.mu.RUnlock()

	queryStr = strings.TrimSpace(queryStr)
	queryLower := strings.ToLower(queryStr)

	// Parse query patterns
	switch {
	case strings.HasPrefix(queryLower, "depends_on "):
		nodeID := strings.TrimPrefix(queryStr, strings.SplitN(queryStr, " ", 2)[0]+" ")
		return kg.findDependents(nodeID, start)

	case strings.HasPrefix(queryLower, "impacts "):
		nodeID := strings.TrimPrefix(queryStr, strings.SplitN(queryStr, " ", 2)[0]+" ")
		return kg.findImpact(nodeID, start)

	case strings.HasPrefix(queryLower, "path "):
		parts := strings.Fields(queryStr)
		if len(parts) < 4 || !strings.EqualFold(parts[2], "to") {
			return nil, fmt.Errorf("path query format: PATH <from> TO <to>")
		}
		return kg.findPath(parts[1], parts[3], start)

	case strings.HasPrefix(queryLower, "neighbors "):
		nodeID := strings.TrimPrefix(queryStr, strings.SplitN(queryStr, " ", 2)[0]+" ")
		return kg.findNeighbors(nodeID, start)

	case queryLower == "all" || queryLower == "show all":
		return kg.getAll(start)

	case strings.HasPrefix(queryLower, "type "):
		nodeType := NodeType(strings.TrimPrefix(queryStr, strings.SplitN(queryStr, " ", 2)[0]+" "))
		return kg.findByType(nodeType, start)

	default:
		// Search by name
		return kg.searchByName(queryStr, start)
	}
}

func (kg *KnowledgeGraph) findDependents(nodeID string, start time.Time) (*QueryResult, error) {
	result := &QueryResult{}

	// Find all nodes that depend on this node (incoming edges)
	if edges, ok := kg.adjList[nodeID]; ok {
		for _, e := range edges {
			result.Edges = append(result.Edges, e)
			if n, ok := kg.nodes[e.To]; ok {
				result.Nodes = append(result.Nodes, n)
			}
		}
	}
	// Also check reverse
	if edges, ok := kg.revAdj[nodeID]; ok {
		for _, e := range edges {
			result.Edges = append(result.Edges, e)
			if n, ok := kg.nodes[e.From]; ok {
				result.Nodes = append(result.Nodes, n)
			}
		}
	}

	if n, ok := kg.nodes[nodeID]; ok {
		result.Nodes = append([]Node{n}, result.Nodes...)
	}

	result.TotalNodes = len(result.Nodes)
	result.TotalEdges = len(result.Edges)
	result.QueryTime = time.Since(start).String()
	return result, nil
}

func (kg *KnowledgeGraph) findImpact(nodeID string, start time.Time) (*QueryResult, error) {
	result := &QueryResult{}
	visited := make(map[string]bool)

	// BFS to find all downstream nodes
	queue := []string{nodeID}
	visited[nodeID] = true

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if n, ok := kg.nodes[current]; ok {
			result.Nodes = append(result.Nodes, n)
		}

		for _, e := range kg.adjList[current] {
			result.Edges = append(result.Edges, e)
			if !visited[e.To] {
				visited[e.To] = true
				queue = append(queue, e.To)
			}
		}
	}

	result.TotalNodes = len(result.Nodes)
	result.TotalEdges = len(result.Edges)
	result.QueryTime = time.Since(start).String()
	return result, nil
}

func (kg *KnowledgeGraph) findPath(from, to string, start time.Time) (*QueryResult, error) {
	result := &QueryResult{}

	// BFS shortest path
	visited := map[string]bool{from: true}
	parent := map[string]string{}
	queue := []string{from}

	found := false
	for len(queue) > 0 && !found {
		current := queue[0]
		queue = queue[1:]

		for _, e := range kg.adjList[current] {
			if !visited[e.To] {
				visited[e.To] = true
				parent[e.To] = current
				result.Edges = append(result.Edges, e)

				if e.To == to {
					found = true
					break
				}
				queue = append(queue, e.To)
			}
		}
	}

	if found {
		// Reconstruct path
		var path []string
		current := to
		for current != "" {
			path = append([]string{current}, path...)
			current = parent[current]
		}
		result.Paths = append(result.Paths, path)
		for _, nodeID := range path {
			if n, ok := kg.nodes[nodeID]; ok {
				result.Nodes = append(result.Nodes, n)
			}
		}
	}

	result.TotalNodes = len(result.Nodes)
	result.TotalEdges = len(result.Edges)
	result.QueryTime = time.Since(start).String()
	return result, nil
}

func (kg *KnowledgeGraph) findNeighbors(nodeID string, start time.Time) (*QueryResult, error) {
	result := &QueryResult{}

	if n, ok := kg.nodes[nodeID]; ok {
		result.Nodes = append(result.Nodes, n)
	}

	// Outgoing
	for _, e := range kg.adjList[nodeID] {
		result.Edges = append(result.Edges, e)
		if n, ok := kg.nodes[e.To]; ok {
			result.Nodes = append(result.Nodes, n)
		}
	}

	// Incoming
	for _, e := range kg.revAdj[nodeID] {
		result.Edges = append(result.Edges, e)
		if n, ok := kg.nodes[e.From]; ok {
			result.Nodes = append(result.Nodes, n)
		}
	}

	result.TotalNodes = len(result.Nodes)
	result.TotalEdges = len(result.Edges)
	result.QueryTime = time.Since(start).String()
	return result, nil
}

func (kg *KnowledgeGraph) getAll(start time.Time) (*QueryResult, error) {
	result := &QueryResult{}
	for _, n := range kg.nodes {
		result.Nodes = append(result.Nodes, n)
	}
	result.Edges = kg.edges
	result.TotalNodes = len(result.Nodes)
	result.TotalEdges = len(result.Edges)
	result.QueryTime = time.Since(start).String()
	return result, nil
}

func (kg *KnowledgeGraph) findByType(nodeType NodeType, start time.Time) (*QueryResult, error) {
	result := &QueryResult{}
	for _, n := range kg.nodes {
		if n.Type == nodeType {
			result.Nodes = append(result.Nodes, n)
		}
	}
	result.TotalNodes = len(result.Nodes)
	result.QueryTime = time.Since(start).String()
	return result, nil
}

func (kg *KnowledgeGraph) searchByName(query string, start time.Time) (*QueryResult, error) {
	result := &QueryResult{}
	queryLower := strings.ToLower(query)
	for _, n := range kg.nodes {
		if strings.Contains(strings.ToLower(n.Name), queryLower) {
			result.Nodes = append(result.Nodes, n)
		}
	}
	result.TotalNodes = len(result.Nodes)
	result.QueryTime = time.Since(start).String()
	return result, nil
}

// GetStats returns graph statistics.
func (kg *KnowledgeGraph) GetStats() map[string]any {
	kg.mu.RLock()
	defer kg.mu.RUnlock()

	typeCounts := make(map[string]int)
	for _, n := range kg.nodes {
		typeCounts[string(n.Type)]++
	}

	edgeTypeCounts := make(map[string]int)
	for _, e := range kg.edges {
		edgeTypeCounts[string(e.Type)]++
	}

	return map[string]any{
		"total_nodes": len(kg.nodes),
		"total_edges": len(kg.edges),
		"node_types":  typeCounts,
		"edge_types":  edgeTypeCounts,
		"ttl":         kg.ttl.String(),
	}
}

// EvictStale removes nodes and edges older than TTL.
func (kg *KnowledgeGraph) EvictStale() int {
	kg.mu.Lock()
	defer kg.mu.Unlock()

	cutoff := time.Now().Add(-kg.ttl)
	evicted := 0

	for id, n := range kg.nodes {
		if n.UpdatedAt.Before(cutoff) {
			delete(kg.nodes, id)
			delete(kg.adjList, id)
			delete(kg.revAdj, id)
			evicted++
		}
	}

	var activeEdges []Edge
	for _, e := range kg.edges {
		if !e.UpdatedAt.Before(cutoff) {
			activeEdges = append(activeEdges, e)
		} else {
			evicted++
		}
	}
	kg.edges = activeEdges

	return evicted
}
