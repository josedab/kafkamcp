package graph

import (
	"fmt"
	"strings"
	"time"
)

// CypherQuery represents a parsed Cypher-style graph query.
type CypherQuery struct {
	Type         string            `json:"type"` // match, path, shortestPath, allPaths
	StartNode    string            `json:"start_node,omitempty"`
	EndNode      string            `json:"end_node,omitempty"`
	NodeType     NodeType          `json:"node_type,omitempty"`
	EdgeType     EdgeType          `json:"edge_type,omitempty"`
	MaxDepth     int               `json:"max_depth,omitempty"`
	Filters      map[string]string `json:"filters,omitempty"`
	ReturnFields []string          `json:"return_fields,omitempty"`
}

// ExecuteCypher parses and executes a Cypher-style query string.
// Supported patterns:
//   - MATCH (n:topic) RETURN n
//   - MATCH (n:topic)-[:consumes_from]->(m) RETURN n,m
//   - MATCH (n {name: "orders"})-[*1..3]->(m) RETURN n,m
//   - SHORTEST_PATH (a)->(b) WHERE a.name = "orders" AND b.name = "analytics"
//   - ALL_PATHS (a)-[*..5]->(b) WHERE a = "orders" AND b = "analytics-group"
func (kg *KnowledgeGraph) ExecuteCypher(query string) (*QueryResult, error) {
	start := time.Now()
	kg.mu.RLock()
	defer kg.mu.RUnlock()

	query = strings.TrimSpace(query)
	queryUpper := strings.ToUpper(query)

	switch {
	case strings.HasPrefix(queryUpper, "MATCH"):
		return kg.executeCypherMatch(query, start)
	case strings.HasPrefix(queryUpper, "SHORTEST_PATH") || strings.HasPrefix(queryUpper, "SHORTEST PATH"):
		return kg.executeCypherShortestPath(query, start)
	case strings.HasPrefix(queryUpper, "ALL_PATHS") || strings.HasPrefix(queryUpper, "ALL PATHS"):
		return kg.executeCypherAllPaths(query, start)
	default:
		// Fall back to the simple query parser
		kg.mu.RUnlock()
		result, err := kg.Query(query)
		kg.mu.RLock()
		return result, err
	}
}

func (kg *KnowledgeGraph) executeCypherMatch(query string, start time.Time) (*QueryResult, error) {
	result := &QueryResult{}

	// Parse MATCH (n:type) or MATCH (n:type)-[:edge]->(m:type)
	body := strings.TrimPrefix(query, "MATCH")
	body = strings.TrimPrefix(body, "match")
	body = strings.TrimSpace(body)

	// Remove RETURN clause for parsing
	returnIdx := strings.Index(strings.ToUpper(body), "RETURN")
	if returnIdx > 0 {
		body = strings.TrimSpace(body[:returnIdx])
	}

	// Parse relationship pattern: (a)-[:type]->(b)
	if strings.Contains(body, "->") {
		parts := strings.SplitN(body, "->", 2)
		leftPart := strings.TrimSpace(parts[0])
		rightPart := strings.TrimSpace(parts[1])

		// Extract edge type from -[:type]-
		var edgeFilter EdgeType
		if bracketIdx := strings.LastIndex(leftPart, "-["); bracketIdx >= 0 {
			edgePart := leftPart[bracketIdx:]
			leftPart = strings.TrimSpace(leftPart[:bracketIdx])
			edgePart = strings.Trim(edgePart, "-[]:")
			if edgePart != "" && edgePart != "*" {
				edgeFilter = EdgeType(edgePart)
			}
		}

		// Extract start/end node types
		startType := extractNodeType(leftPart)
		startName := extractNodeName(leftPart)
		endType := extractNodeType(rightPart)

		for _, edge := range kg.edges {
			if edgeFilter != "" && edge.Type != edgeFilter {
				continue
			}
			fromNode, fromOK := kg.nodes[edge.From]
			toNode, toOK := kg.nodes[edge.To]
			if !fromOK || !toOK {
				continue
			}
			if startType != "" && fromNode.Type != startType {
				continue
			}
			if startName != "" && !strings.Contains(strings.ToLower(fromNode.Name), strings.ToLower(startName)) {
				continue
			}
			if endType != "" && toNode.Type != endType {
				continue
			}
			result.Nodes = appendUniqueNode(result.Nodes, fromNode)
			result.Nodes = appendUniqueNode(result.Nodes, toNode)
			result.Edges = append(result.Edges, edge)
		}
	} else {
		// Simple node match: MATCH (n:type)
		nodeType := extractNodeType(body)
		nodeName := extractNodeName(body)
		for _, n := range kg.nodes {
			if nodeType != "" && n.Type != nodeType {
				continue
			}
			if nodeName != "" && !strings.Contains(strings.ToLower(n.Name), strings.ToLower(nodeName)) {
				continue
			}
			result.Nodes = append(result.Nodes, n)
		}
	}

	result.TotalNodes = len(result.Nodes)
	result.TotalEdges = len(result.Edges)
	result.QueryTime = time.Since(start).String()
	return result, nil
}

func (kg *KnowledgeGraph) executeCypherShortestPath(query string, start time.Time) (*QueryResult, error) {
	// Extract WHERE clause for start/end nodes
	fromID, toID, err := extractPathEndpoints(query)
	if err != nil {
		return nil, err
	}

	return kg.findShortestPathBidirectional(fromID, toID, start)
}

func (kg *KnowledgeGraph) executeCypherAllPaths(query string, start time.Time) (*QueryResult, error) {
	fromID, toID, err := extractPathEndpoints(query)
	if err != nil {
		return nil, err
	}

	maxDepth := 5
	return kg.findAllPaths(fromID, toID, maxDepth, start)
}

func (kg *KnowledgeGraph) findShortestPathBidirectional(from, to string, start time.Time) (*QueryResult, error) {
	// Resolve node IDs by name if not direct IDs
	from = kg.resolveNodeID(from)
	to = kg.resolveNodeID(to)

	result := &QueryResult{}
	visited := map[string]bool{from: true}
	parent := map[string]string{}
	queue := []string{from}

	found := false
	for len(queue) > 0 && !found {
		current := queue[0]
		queue = queue[1:]

		// Check forward and reverse edges
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
		if found {
			break
		}
		for _, e := range kg.revAdj[current] {
			if !visited[e.From] {
				visited[e.From] = true
				parent[e.From] = current
				result.Edges = append(result.Edges, e)
				if e.From == to {
					found = true
					break
				}
				queue = append(queue, e.From)
			}
		}
	}

	if found {
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

func (kg *KnowledgeGraph) findAllPaths(from, to string, maxDepth int, start time.Time) (*QueryResult, error) {
	from = kg.resolveNodeID(from)
	to = kg.resolveNodeID(to)

	result := &QueryResult{}
	visited := make(map[string]bool)
	var currentPath []string

	var dfs func(node string, depth int)
	dfs = func(node string, depth int) {
		if depth > maxDepth {
			return
		}
		visited[node] = true
		currentPath = append(currentPath, node)

		if node == to && len(currentPath) > 1 {
			pathCopy := make([]string, len(currentPath))
			copy(pathCopy, currentPath)
			result.Paths = append(result.Paths, pathCopy)
		} else {
			for _, e := range kg.adjList[node] {
				if !visited[e.To] {
					result.Edges = append(result.Edges, e)
					dfs(e.To, depth+1)
				}
			}
		}

		currentPath = currentPath[:len(currentPath)-1]
		visited[node] = false
	}

	dfs(from, 0)

	// Collect unique nodes from paths
	nodeSet := make(map[string]bool)
	for _, path := range result.Paths {
		for _, nodeID := range path {
			if !nodeSet[nodeID] {
				nodeSet[nodeID] = true
				if n, ok := kg.nodes[nodeID]; ok {
					result.Nodes = append(result.Nodes, n)
				}
			}
		}
	}

	result.TotalNodes = len(result.Nodes)
	result.TotalEdges = len(result.Edges)
	result.QueryTime = time.Since(start).String()
	return result, nil
}

func (kg *KnowledgeGraph) resolveNodeID(nameOrID string) string {
	if _, ok := kg.nodes[nameOrID]; ok {
		return nameOrID
	}
	// Try to find by name
	nameLower := strings.ToLower(nameOrID)
	for id, n := range kg.nodes {
		if strings.EqualFold(n.Name, nameLower) {
			return id
		}
	}
	return nameOrID
}

// NaturalLanguageQuery interprets a natural language question about the graph.
func (kg *KnowledgeGraph) NaturalLanguageQuery(question string) (*QueryResult, error) {
	start := time.Now()
	kg.mu.RLock()
	defer kg.mu.RUnlock()

	q := strings.ToLower(strings.TrimSpace(question))

	switch {
	case strings.Contains(q, "what topics") || strings.Contains(q, "list topics") || strings.Contains(q, "show topics"):
		return kg.findByType(NodeTopic, start)

	case strings.Contains(q, "consumer group") || strings.Contains(q, "consumers"):
		return kg.findByType(NodeConsumerGroup, start)

	case strings.Contains(q, "schema") || strings.Contains(q, "schemas"):
		return kg.findByType(NodeSchema, start)

	case strings.Contains(q, "who consumes") || strings.Contains(q, "who reads"):
		topic := extractEntityFromNL(q, []string{"who consumes", "who reads"})
		if topic != "" {
			return kg.findConsumersOf(topic, start)
		}
		return kg.findByType(NodeConsumerGroup, start)

	case strings.Contains(q, "who produces") || strings.Contains(q, "who writes"):
		topic := extractEntityFromNL(q, []string{"who produces", "who writes"})
		if topic != "" {
			return kg.findProducersOf(topic, start)
		}
		return kg.findByType(NodeProducer, start)

	case strings.Contains(q, "impact") || strings.Contains(q, "affected"):
		entity := extractEntityFromNL(q, []string{"impact of", "affected by", "impacts"})
		if entity != "" {
			id := kg.resolveNodeID(entity)
			return kg.findImpactLocked(id, start)
		}
		return &QueryResult{QueryTime: time.Since(start).String()}, nil

	case strings.Contains(q, "depend") || strings.Contains(q, "dependencies"):
		entity := extractEntityFromNL(q, []string{"depends on", "dependencies of", "dependencies for"})
		if entity != "" {
			id := kg.resolveNodeID(entity)
			return kg.findDependentsLocked(id, start)
		}
		return &QueryResult{QueryTime: time.Since(start).String()}, nil

	case strings.Contains(q, "how many") || strings.Contains(q, "count"):
		return kg.getCountSummary(start)

	default:
		// Try to extract an entity name and search
		words := strings.Fields(q)
		for _, w := range words {
			if len(w) > 3 {
				results := kg.searchByNameLocked(w, start)
				if results.TotalNodes > 0 {
					return results, nil
				}
			}
		}
		return &QueryResult{QueryTime: time.Since(start).String()}, nil
	}
}

func (kg *KnowledgeGraph) findConsumersOf(topic string, start time.Time) (*QueryResult, error) {
	result := &QueryResult{}
	topicID := kg.resolveNodeID(topic)

	for _, e := range kg.adjList[topicID] {
		if e.Type == EdgeConsumesFrom {
			if n, ok := kg.nodes[e.To]; ok {
				result.Nodes = append(result.Nodes, n)
				result.Edges = append(result.Edges, e)
			}
		}
	}
	result.TotalNodes = len(result.Nodes)
	result.TotalEdges = len(result.Edges)
	result.QueryTime = time.Since(start).String()
	return result, nil
}

func (kg *KnowledgeGraph) findProducersOf(topic string, start time.Time) (*QueryResult, error) {
	result := &QueryResult{}
	topicID := kg.resolveNodeID(topic)

	for _, e := range kg.revAdj[topicID] {
		if e.Type == EdgeProducesTo {
			if n, ok := kg.nodes[e.From]; ok {
				result.Nodes = append(result.Nodes, n)
				result.Edges = append(result.Edges, e)
			}
		}
	}
	result.TotalNodes = len(result.Nodes)
	result.TotalEdges = len(result.Edges)
	result.QueryTime = time.Since(start).String()
	return result, nil
}

func (kg *KnowledgeGraph) findImpactLocked(nodeID string, start time.Time) (*QueryResult, error) {
	result := &QueryResult{}
	visited := make(map[string]bool)
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

func (kg *KnowledgeGraph) findDependentsLocked(nodeID string, start time.Time) (*QueryResult, error) {
	result := &QueryResult{}
	if edges, ok := kg.adjList[nodeID]; ok {
		for _, e := range edges {
			result.Edges = append(result.Edges, e)
			if n, ok := kg.nodes[e.To]; ok {
				result.Nodes = append(result.Nodes, n)
			}
		}
	}
	if edges, ok := kg.revAdj[nodeID]; ok {
		for _, e := range edges {
			result.Edges = append(result.Edges, e)
			if n, ok := kg.nodes[e.From]; ok {
				result.Nodes = append(result.Nodes, n)
			}
		}
	}
	result.TotalNodes = len(result.Nodes)
	result.TotalEdges = len(result.Edges)
	result.QueryTime = time.Since(start).String()
	return result, nil
}

func (kg *KnowledgeGraph) searchByNameLocked(query string, start time.Time) *QueryResult {
	result := &QueryResult{}
	queryLower := strings.ToLower(query)
	for _, n := range kg.nodes {
		if strings.Contains(strings.ToLower(n.Name), queryLower) {
			result.Nodes = append(result.Nodes, n)
		}
	}
	result.TotalNodes = len(result.Nodes)
	result.QueryTime = time.Since(start).String()
	return result
}

func (kg *KnowledgeGraph) getCountSummary(start time.Time) (*QueryResult, error) {
	result := &QueryResult{}
	for _, n := range kg.nodes {
		result.Nodes = append(result.Nodes, n)
	}
	result.TotalNodes = len(result.Nodes)
	result.TotalEdges = len(kg.edges)
	result.QueryTime = time.Since(start).String()
	return result, nil
}

// Helper functions

func extractNodeType(s string) NodeType {
	s = strings.Trim(s, "()")
	if idx := strings.Index(s, ":"); idx >= 0 {
		typePart := s[idx+1:]
		if nameIdx := strings.Index(typePart, " "); nameIdx >= 0 {
			typePart = typePart[:nameIdx]
		}
		typePart = strings.Trim(typePart, "{})")
		return NodeType(typePart)
	}
	return ""
}

func extractNodeName(s string) string {
	if idx := strings.Index(s, "{name:"); idx >= 0 {
		rest := s[idx+6:]
		rest = strings.TrimSpace(rest)
		rest = strings.Trim(rest, "\"'}")
		return rest
	}
	return ""
}

func extractPathEndpoints(query string) (string, string, error) {
	// Look for WHERE clause with a.name = "x" AND b.name = "y"
	upper := strings.ToUpper(query)
	whereIdx := strings.Index(upper, "WHERE")
	if whereIdx < 0 {
		return "", "", fmt.Errorf("WHERE clause required for path queries")
	}

	where := query[whereIdx+5:]
	parts := strings.Split(strings.ToLower(where), " and ")
	if len(parts) < 2 {
		return "", "", fmt.Errorf("need two conditions: WHERE a = 'from' AND b = 'to'")
	}

	from := extractValueFromCondition(parts[0])
	to := extractValueFromCondition(parts[1])
	if from == "" || to == "" {
		return "", "", fmt.Errorf("could not extract node names from WHERE clause")
	}

	return from, to, nil
}

func extractValueFromCondition(cond string) string {
	cond = strings.TrimSpace(cond)
	// Handle: a.name = "value" or a = "value"
	if idx := strings.Index(cond, "="); idx >= 0 {
		val := strings.TrimSpace(cond[idx+1:])
		val = strings.Trim(val, "\"' ")
		return val
	}
	return ""
}

func extractEntityFromNL(q string, prefixes []string) string {
	for _, prefix := range prefixes {
		if idx := strings.Index(q, prefix); idx >= 0 {
			rest := strings.TrimSpace(q[idx+len(prefix):])
			rest = strings.Trim(rest, "?. ")
			words := strings.Fields(rest)
			if len(words) > 0 {
				return words[0]
			}
		}
	}
	return ""
}

func appendUniqueNode(nodes []Node, node Node) []Node {
	for _, n := range nodes {
		if n.ID == node.ID {
			return nodes
		}
	}
	return append(nodes, node)
}
