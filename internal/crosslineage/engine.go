package crosslineage

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// NodeType represents the type of entity in the lineage graph.
type NodeType string

const (
	NodeTopic     NodeType = "topic"
	NodeConnector NodeType = "connector"
	NodeStream    NodeType = "stream_app"
	NodeConsumer  NodeType = "consumer_group"
	NodeExternal  NodeType = "external"
)

// Node is an entity in the lineage graph.
type Node struct {
	ID       string            `json:"id"`
	Name     string            `json:"name"`
	Type     NodeType          `json:"type"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Edge represents a data flow relationship between nodes.
type Edge struct {
	ID        string            `json:"id"`
	SourceID  string            `json:"source_id"`
	TargetID  string            `json:"target_id"`
	Label     string            `json:"label,omitempty"`
	Type      string            `json:"type"` // produces, consumes, transforms, replicates
	Metadata  map[string]string `json:"metadata,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
}

// LineageGraph is the complete lineage visualization.
type LineageGraph struct {
	Nodes       []Node   `json:"nodes"`
	Edges       []Edge   `json:"edges"`
	TotalNodes  int      `json:"total_nodes"`
	TotalEdges  int      `json:"total_edges"`
	GeneratedAt time.Time `json:"generated_at"`
}

// Lineage describes the upstream/downstream lineage of a topic.
type Lineage struct {
	Topic      string     `json:"topic"`
	Upstream   []FlowPath `json:"upstream"`
	Downstream []FlowPath `json:"downstream"`
	Depth      int        `json:"depth"`
}

// FlowPath represents a single data flow path.
type FlowPath struct {
	From     string `json:"from"`
	To       string `json:"to"`
	Via      string `json:"via,omitempty"`
	ViaType  string `json:"via_type,omitempty"`
	FlowType string `json:"flow_type"`
}

// Engine manages cross-topic lineage tracking.
type Engine struct {
	mu     sync.RWMutex
	nodes  map[string]*Node
	edges  map[string]*Edge
	logger *slog.Logger
	nextID int64
}

// NewEngine creates a new cross-lineage engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		nodes:  make(map[string]*Node),
		edges:  make(map[string]*Edge),
		logger: logger,
	}
}

// RegisterNode adds a node to the lineage graph.
func (e *Engine) RegisterNode(name string, nodeType NodeType, metadata map[string]string) *Node {
	e.mu.Lock()
	defer e.mu.Unlock()

	id := fmt.Sprintf("%s:%s", nodeType, name)
	if existing, ok := e.nodes[id]; ok {
		for k, v := range metadata {
			if existing.Metadata == nil {
				existing.Metadata = make(map[string]string)
			}
			existing.Metadata[k] = v
		}
		return existing
	}

	node := &Node{ID: id, Name: name, Type: nodeType, Metadata: metadata}
	e.nodes[id] = node
	return node
}

// AddEdge creates a data flow edge between two nodes.
func (e *Engine) AddEdge(sourceID, targetID, label, flowType string, metadata map[string]string) (*Edge, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.nodes[sourceID]; !ok {
		return nil, fmt.Errorf("source node %q not found", sourceID)
	}
	if _, ok := e.nodes[targetID]; !ok {
		return nil, fmt.Errorf("target node %q not found", targetID)
	}

	e.nextID++
	id := fmt.Sprintf("edge_%d", e.nextID)

	edge := &Edge{
		ID:        id,
		SourceID:  sourceID,
		TargetID:  targetID,
		Label:     label,
		Type:      flowType,
		Metadata:  metadata,
		CreatedAt: time.Now(),
	}
	e.edges[id] = edge
	return edge, nil
}

// GetGraph returns the complete lineage graph.
func (e *Engine) GetGraph() *LineageGraph {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var nodes []Node
	for _, n := range e.nodes {
		nodes = append(nodes, *n)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })

	var edges []Edge
	for _, edge := range e.edges {
		edges = append(edges, *edge)
	}
	sort.Slice(edges, func(i, j int) bool { return edges[i].ID < edges[j].ID })

	return &LineageGraph{
		Nodes:       nodes,
		Edges:       edges,
		TotalNodes:  len(nodes),
		TotalEdges:  len(edges),
		GeneratedAt: time.Now(),
	}
}

// TraceLineage returns upstream and downstream lineage for a topic.
func (e *Engine) TraceLineage(topicName string, maxDepth int) *Lineage {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if maxDepth <= 0 {
		maxDepth = 5
	}

	topicID := fmt.Sprintf("topic:%s", topicName)
	lineage := &Lineage{
		Topic: topicName,
		Depth: maxDepth,
	}

	// Trace upstream (who sends data to this topic)
	visited := make(map[string]bool)
	lineage.Upstream = e.traceUpstream(topicID, maxDepth, visited)

	// Trace downstream (where does data from this topic go)
	visited = make(map[string]bool)
	lineage.Downstream = e.traceDownstream(topicID, maxDepth, visited)

	return lineage
}

func (e *Engine) traceUpstream(nodeID string, depth int, visited map[string]bool) []FlowPath {
	if depth <= 0 || visited[nodeID] {
		return nil
	}
	visited[nodeID] = true

	var paths []FlowPath
	for _, edge := range e.edges {
		if edge.TargetID == nodeID {
			source := e.nodes[edge.SourceID]
			target := e.nodes[edge.TargetID]
			if source != nil && target != nil {
				paths = append(paths, FlowPath{
					From:     source.Name,
					To:       target.Name,
					FlowType: edge.Type,
				})
				paths = append(paths, e.traceUpstream(edge.SourceID, depth-1, visited)...)
			}
		}
	}
	return paths
}

func (e *Engine) traceDownstream(nodeID string, depth int, visited map[string]bool) []FlowPath {
	if depth <= 0 || visited[nodeID] {
		return nil
	}
	visited[nodeID] = true

	var paths []FlowPath
	for _, edge := range e.edges {
		if edge.SourceID == nodeID {
			source := e.nodes[edge.SourceID]
			target := e.nodes[edge.TargetID]
			if source != nil && target != nil {
				paths = append(paths, FlowPath{
					From:     source.Name,
					To:       target.Name,
					FlowType: edge.Type,
				})
				paths = append(paths, e.traceDownstream(edge.TargetID, depth-1, visited)...)
			}
		}
	}
	return paths
}

// RemoveNode removes a node and its associated edges.
func (e *Engine) RemoveNode(nodeID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.nodes[nodeID]; !ok {
		return fmt.Errorf("node %q not found", nodeID)
	}

	// Remove associated edges
	for id, edge := range e.edges {
		if edge.SourceID == nodeID || edge.TargetID == nodeID {
			delete(e.edges, id)
		}
	}
	delete(e.nodes, nodeID)
	return nil
}

// RemoveEdge removes a single edge.
func (e *Engine) RemoveEdge(edgeID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.edges[edgeID]; !ok {
		return fmt.Errorf("edge %q not found", edgeID)
	}
	delete(e.edges, edgeID)
	return nil
}

// ImpactAnalysis describes what would be affected if a node is changed or removed.
type ImpactAnalysis struct {
	NodeID             string   `json:"node_id"`
	NodeName           string   `json:"node_name"`
	DirectDownstream   []string `json:"direct_downstream"`
	TransitiveImpact   []string `json:"transitive_impact"`
	AffectedClusters   []string `json:"affected_clusters"`
	TotalAffectedNodes int      `json:"total_affected_nodes"`
	RiskLevel          string   `json:"risk_level"`
}

// PathResult represents a path between two nodes.
type PathResult struct {
	Source    string     `json:"source"`
	Target    string     `json:"target"`
	Paths     [][]string `json:"paths"`
	ShortestLength int  `json:"shortest_length"`
}

// CrossClusterFlow represents data flow between clusters.
type CrossClusterFlow struct {
	SourceCluster string     `json:"source_cluster"`
	TargetCluster string     `json:"target_cluster"`
	Flows         []FlowPath `json:"flows"`
	FlowCount     int        `json:"flow_count"`
}

// AnalyzeImpact performs impact analysis for a given node.
func (e *Engine) AnalyzeImpact(nodeID string) (*ImpactAnalysis, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	node, ok := e.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("node %q not found", nodeID)
	}

	analysis := &ImpactAnalysis{
		NodeID:   nodeID,
		NodeName: node.Name,
	}

	// Find direct downstream
	directSet := make(map[string]bool)
	for _, edge := range e.edges {
		if edge.SourceID == nodeID {
			if target, ok := e.nodes[edge.TargetID]; ok {
				directSet[target.Name] = true
			}
		}
	}
	for name := range directSet {
		analysis.DirectDownstream = append(analysis.DirectDownstream, name)
	}

	// Transitive impact via BFS
	visited := make(map[string]bool)
	visited[nodeID] = true
	queue := []string{nodeID}
	transitiveSet := make(map[string]bool)
	clusterSet := make(map[string]bool)

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		for _, edge := range e.edges {
			if edge.SourceID == current && !visited[edge.TargetID] {
				visited[edge.TargetID] = true
				queue = append(queue, edge.TargetID)
				if target, ok := e.nodes[edge.TargetID]; ok {
					transitiveSet[target.Name] = true
					if target.Metadata != nil && target.Metadata["cluster"] != "" {
						clusterSet[target.Metadata["cluster"]] = true
					}
				}
			}
		}
	}

	for name := range transitiveSet {
		analysis.TransitiveImpact = append(analysis.TransitiveImpact, name)
	}
	for cluster := range clusterSet {
		analysis.AffectedClusters = append(analysis.AffectedClusters, cluster)
	}

	analysis.TotalAffectedNodes = len(transitiveSet)

	switch {
	case analysis.TotalAffectedNodes > 10 || len(analysis.AffectedClusters) > 2:
		analysis.RiskLevel = "critical"
	case analysis.TotalAffectedNodes > 5 || len(analysis.AffectedClusters) > 1:
		analysis.RiskLevel = "high"
	case analysis.TotalAffectedNodes > 2:
		analysis.RiskLevel = "medium"
	default:
		analysis.RiskLevel = "low"
	}

	return analysis, nil
}

// FindPaths finds all paths between two nodes up to a maximum depth.
func (e *Engine) FindPaths(sourceID, targetID string, maxDepth int) (*PathResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if _, ok := e.nodes[sourceID]; !ok {
		return nil, fmt.Errorf("source node %q not found", sourceID)
	}
	if _, ok := e.nodes[targetID]; !ok {
		return nil, fmt.Errorf("target node %q not found", targetID)
	}
	if maxDepth <= 0 {
		maxDepth = 10
	}

	result := &PathResult{
		Source: sourceID,
		Target: targetID,
	}

	// DFS to find all paths
	var dfs func(current string, path []string, depth int)
	dfs = func(current string, path []string, depth int) {
		if depth > maxDepth {
			return
		}
		if current == targetID {
			pathCopy := make([]string, len(path))
			copy(pathCopy, path)
			result.Paths = append(result.Paths, pathCopy)
			return
		}
		for _, edge := range e.edges {
			if edge.SourceID == current {
				alreadyVisited := false
				for _, p := range path {
					if p == edge.TargetID {
						alreadyVisited = true
						break
					}
				}
				if !alreadyVisited {
					dfs(edge.TargetID, append(path, edge.TargetID), depth+1)
				}
			}
		}
	}

	dfs(sourceID, []string{sourceID}, 0)

	if len(result.Paths) > 0 {
		result.ShortestLength = len(result.Paths[0])
		for _, p := range result.Paths {
			if len(p) < result.ShortestLength {
				result.ShortestLength = len(p)
			}
		}
	}

	// Limit paths returned
	if len(result.Paths) > 20 {
		result.Paths = result.Paths[:20]
	}

	return result, nil
}

// GetCrossClusterFlows identifies data flows that cross cluster boundaries.
func (e *Engine) GetCrossClusterFlows() []CrossClusterFlow {
	e.mu.RLock()
	defer e.mu.RUnlock()

	flowMap := make(map[string]*CrossClusterFlow) // "srcCluster->dstCluster"
	for _, edge := range e.edges {
		source := e.nodes[edge.SourceID]
		target := e.nodes[edge.TargetID]
		if source == nil || target == nil {
			continue
		}

		srcCluster := ""
		dstCluster := ""
		if source.Metadata != nil {
			srcCluster = source.Metadata["cluster"]
		}
		if target.Metadata != nil {
			dstCluster = target.Metadata["cluster"]
		}

		if srcCluster == "" || dstCluster == "" || srcCluster == dstCluster {
			continue
		}

		key := srcCluster + "->" + dstCluster
		if _, ok := flowMap[key]; !ok {
			flowMap[key] = &CrossClusterFlow{
				SourceCluster: srcCluster,
				TargetCluster: dstCluster,
			}
		}
		flowMap[key].Flows = append(flowMap[key].Flows, FlowPath{
			From:     source.Name,
			To:       target.Name,
			FlowType: edge.Type,
		})
		flowMap[key].FlowCount++
	}

	var result []CrossClusterFlow
	for _, f := range flowMap {
		result = append(result, *f)
	}
	return result
}
