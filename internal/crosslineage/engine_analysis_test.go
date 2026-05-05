package crosslineage

import (
	"log/slog"
	"sort"
	"testing"
)

// setupBasicGraph registers nodes with cluster metadata and connects them.
func setupBasicGraph(e *Engine) {
	e.RegisterNode("topic-a", NodeTopic, map[string]string{"cluster": "cluster-1"})
	e.RegisterNode("topic-b", NodeTopic, map[string]string{"cluster": "cluster-2"})
	e.RegisterNode("topic-c", NodeTopic, map[string]string{"cluster": "cluster-2"})
	e.AddEdge("topic:topic-a", "topic:topic-b", "replicate", "replicates", nil)
	e.AddEdge("topic:topic-b", "topic:topic-c", "transform", "transforms", nil)
}

// --- AnalyzeImpact tests ---

func TestAnalyzeImpact_NodeNotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.AnalyzeImpact("topic:nonexistent")
	if err == nil {
		t.Error("expected error for missing node")
	}
}

func TestAnalyzeImpact_SingleNodeNoEdges(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("lonely", NodeTopic, nil)

	analysis, err := e.AnalyzeImpact("topic:lonely")
	if err != nil {
		t.Fatal(err)
	}
	if analysis.TotalAffectedNodes != 0 {
		t.Errorf("expected 0 affected nodes, got %d", analysis.TotalAffectedNodes)
	}
	if analysis.RiskLevel != "low" {
		t.Errorf("expected risk level low, got %s", analysis.RiskLevel)
	}
}

func TestAnalyzeImpact_DirectDownstream(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	setupBasicGraph(e)

	analysis, err := e.AnalyzeImpact("topic:topic-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(analysis.DirectDownstream) != 1 || analysis.DirectDownstream[0] != "topic-b" {
		t.Errorf("expected direct downstream [topic-b], got %v", analysis.DirectDownstream)
	}
}

func TestAnalyzeImpact_DeepChainTransitiveImpact(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	names := []string{"n-a", "n-b", "n-c", "n-d", "n-e", "n-f"}
	for _, n := range names {
		e.RegisterNode(n, NodeTopic, nil)
	}
	for i := 0; i < len(names)-1; i++ {
		src := "topic:" + names[i]
		dst := "topic:" + names[i+1]
		e.AddEdge(src, dst, "flow", "produces", nil)
	}

	analysis, err := e.AnalyzeImpact("topic:n-a")
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{"n-b", "n-c", "n-d", "n-e", "n-f"}
	got := make([]string, len(analysis.TransitiveImpact))
	copy(got, analysis.TransitiveImpact)
	sort.Strings(got)
	sort.Strings(expected)
	if len(got) != len(expected) {
		t.Fatalf("expected %d transitive nodes, got %d: %v", len(expected), len(got), got)
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Errorf("transitive mismatch at %d: expected %s, got %s", i, expected[i], got[i])
		}
	}
}

func TestAnalyzeImpact_AffectedClusters(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	setupBasicGraph(e)

	analysis, err := e.AnalyzeImpact("topic:topic-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(analysis.AffectedClusters) == 0 {
		t.Error("expected at least one affected cluster")
	}
	found := false
	for _, c := range analysis.AffectedClusters {
		if c == "cluster-2" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected cluster-2 in affected clusters, got %v", analysis.AffectedClusters)
	}
}

func TestAnalyzeImpact_CriticalRisk(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("root", NodeTopic, nil)
	for i := 0; i < 12; i++ {
		name := "downstream-" + string(rune('a'+i))
		e.RegisterNode(name, NodeTopic, nil)
		e.AddEdge("topic:root", "topic:"+name, "flow", "produces", nil)
	}

	analysis, err := e.AnalyzeImpact("topic:root")
	if err != nil {
		t.Fatal(err)
	}
	if analysis.RiskLevel != "critical" {
		t.Errorf("expected critical risk, got %s", analysis.RiskLevel)
	}
}

func TestAnalyzeImpact_HighRisk(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("root", NodeTopic, nil)
	for i := 0; i < 6; i++ {
		name := "d-" + string(rune('a'+i))
		e.RegisterNode(name, NodeTopic, nil)
		e.AddEdge("topic:root", "topic:"+name, "flow", "produces", nil)
	}

	analysis, err := e.AnalyzeImpact("topic:root")
	if err != nil {
		t.Fatal(err)
	}
	if analysis.RiskLevel != "high" {
		t.Errorf("expected high risk, got %s", analysis.RiskLevel)
	}
}

func TestAnalyzeImpact_MediumRisk(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("root", NodeTopic, nil)
	for i := 0; i < 3; i++ {
		name := "d-" + string(rune('a'+i))
		e.RegisterNode(name, NodeTopic, nil)
		e.AddEdge("topic:root", "topic:"+name, "flow", "produces", nil)
	}

	analysis, err := e.AnalyzeImpact("topic:root")
	if err != nil {
		t.Fatal(err)
	}
	if analysis.RiskLevel != "medium" {
		t.Errorf("expected medium risk, got %s", analysis.RiskLevel)
	}
}

// --- FindPaths tests ---

func TestFindPaths_SourceNotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("b", NodeTopic, nil)
	_, err := e.FindPaths("topic:missing", "topic:b", 10)
	if err == nil {
		t.Error("expected error for missing source")
	}
}

func TestFindPaths_TargetNotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("a", NodeTopic, nil)
	_, err := e.FindPaths("topic:a", "topic:missing", 10)
	if err == nil {
		t.Error("expected error for missing target")
	}
}

func TestFindPaths_DirectPath(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("a", NodeTopic, nil)
	e.RegisterNode("b", NodeTopic, nil)
	e.AddEdge("topic:a", "topic:b", "flow", "produces", nil)

	result, err := e.FindPaths("topic:a", "topic:b", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Paths) != 1 {
		t.Fatalf("expected 1 path, got %d", len(result.Paths))
	}
	if len(result.Paths[0]) != 2 {
		t.Errorf("expected path length 2, got %d", len(result.Paths[0]))
	}
	if result.ShortestLength != 2 {
		t.Errorf("expected shortest length 2, got %d", result.ShortestLength)
	}
}

func TestFindPaths_MultiplePaths(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("a", NodeTopic, nil)
	e.RegisterNode("b", NodeTopic, nil)
	e.RegisterNode("c", NodeTopic, nil)
	e.RegisterNode("d", NodeTopic, nil)
	e.AddEdge("topic:a", "topic:b", "flow", "produces", nil)
	e.AddEdge("topic:a", "topic:c", "flow", "produces", nil)
	e.AddEdge("topic:b", "topic:d", "flow", "produces", nil)
	e.AddEdge("topic:c", "topic:d", "flow", "produces", nil)

	result, err := e.FindPaths("topic:a", "topic:d", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Paths) != 2 {
		t.Errorf("expected 2 paths, got %d", len(result.Paths))
	}
}

func TestFindPaths_DisconnectedNodes(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("x", NodeTopic, nil)
	e.RegisterNode("y", NodeTopic, nil)

	result, err := e.FindPaths("topic:x", "topic:y", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Paths) != 0 {
		t.Errorf("expected 0 paths, got %d", len(result.Paths))
	}
}

func TestFindPaths_CycleHandling(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("a", NodeTopic, nil)
	e.RegisterNode("b", NodeTopic, nil)
	e.RegisterNode("c", NodeTopic, nil)
	e.RegisterNode("d", NodeTopic, nil)
	// Cycle: a -> b -> c -> a
	e.AddEdge("topic:a", "topic:b", "flow", "produces", nil)
	e.AddEdge("topic:b", "topic:c", "flow", "produces", nil)
	e.AddEdge("topic:c", "topic:a", "flow", "produces", nil)

	// Target d is unreachable; should not infinite loop
	result, err := e.FindPaths("topic:a", "topic:d", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Paths) != 0 {
		t.Errorf("expected 0 paths to unreachable node, got %d", len(result.Paths))
	}
}

func TestFindPaths_MaxDepthZeroDefaultsTo10(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("a", NodeTopic, nil)
	e.RegisterNode("b", NodeTopic, nil)
	e.AddEdge("topic:a", "topic:b", "flow", "produces", nil)

	result, err := e.FindPaths("topic:a", "topic:b", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Paths) != 1 {
		t.Errorf("expected 1 path with default depth, got %d", len(result.Paths))
	}
}

func TestFindPaths_SameSourceAndTarget(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("a", NodeTopic, nil)

	result, err := e.FindPaths("topic:a", "topic:a", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Paths) != 1 {
		t.Fatalf("expected 1 path for source==target, got %d", len(result.Paths))
	}
	if len(result.Paths[0]) != 1 || result.Paths[0][0] != "topic:a" {
		t.Errorf("expected path [topic:a], got %v", result.Paths[0])
	}
}

func TestFindPaths_ShortestLength(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("a", NodeTopic, nil)
	e.RegisterNode("b", NodeTopic, nil)
	e.RegisterNode("c", NodeTopic, nil)
	// Direct path a->c and indirect a->b->c
	e.AddEdge("topic:a", "topic:c", "direct", "produces", nil)
	e.AddEdge("topic:a", "topic:b", "flow", "produces", nil)
	e.AddEdge("topic:b", "topic:c", "flow", "produces", nil)

	result, err := e.FindPaths("topic:a", "topic:c", 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.ShortestLength != 2 {
		t.Errorf("expected shortest length 2, got %d", result.ShortestLength)
	}
}

// --- GetCrossClusterFlows tests ---

func TestGetCrossClusterFlows_SameCluster(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("x", NodeTopic, map[string]string{"cluster": "cluster-1"})
	e.RegisterNode("y", NodeTopic, map[string]string{"cluster": "cluster-1"})
	e.AddEdge("topic:x", "topic:y", "flow", "produces", nil)

	flows := e.GetCrossClusterFlows()
	if len(flows) != 0 {
		t.Errorf("expected 0 cross-cluster flows for same cluster, got %d", len(flows))
	}
}

func TestGetCrossClusterFlows_CrossCluster(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("x", NodeTopic, map[string]string{"cluster": "cluster-a"})
	e.RegisterNode("y", NodeTopic, map[string]string{"cluster": "cluster-b"})
	e.AddEdge("topic:x", "topic:y", "replicate", "replicates", nil)

	flows := e.GetCrossClusterFlows()
	if len(flows) != 1 {
		t.Fatalf("expected 1 cross-cluster flow, got %d", len(flows))
	}
	if flows[0].SourceCluster != "cluster-a" || flows[0].TargetCluster != "cluster-b" {
		t.Errorf("unexpected clusters: %s -> %s", flows[0].SourceCluster, flows[0].TargetCluster)
	}
	if flows[0].FlowCount != 1 {
		t.Errorf("expected flow count 1, got %d", flows[0].FlowCount)
	}
}

func TestGetCrossClusterFlows_MultipleEdgesGrouped(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("x1", NodeTopic, map[string]string{"cluster": "cluster-a"})
	e.RegisterNode("x2", NodeTopic, map[string]string{"cluster": "cluster-a"})
	e.RegisterNode("y1", NodeTopic, map[string]string{"cluster": "cluster-b"})
	e.AddEdge("topic:x1", "topic:y1", "replicate", "replicates", nil)
	e.AddEdge("topic:x2", "topic:y1", "replicate", "replicates", nil)

	flows := e.GetCrossClusterFlows()
	if len(flows) != 1 {
		t.Fatalf("expected 1 grouped flow, got %d", len(flows))
	}
	if flows[0].FlowCount != 2 {
		t.Errorf("expected flow count 2, got %d", flows[0].FlowCount)
	}
}

func TestGetCrossClusterFlows_NoClusterMetadata(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("x", NodeTopic, nil)
	e.RegisterNode("y", NodeTopic, nil)
	e.AddEdge("topic:x", "topic:y", "flow", "produces", nil)

	flows := e.GetCrossClusterFlows()
	if len(flows) != 0 {
		t.Errorf("expected 0 flows when no cluster metadata, got %d", len(flows))
	}
}

func TestGetCrossClusterFlows_EmptyCluster(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("x", NodeTopic, map[string]string{"cluster": ""})
	e.RegisterNode("y", NodeTopic, map[string]string{"cluster": ""})
	e.AddEdge("topic:x", "topic:y", "flow", "produces", nil)

	flows := e.GetCrossClusterFlows()
	if len(flows) != 0 {
		t.Errorf("expected 0 flows when clusters are empty, got %d", len(flows))
	}
}

// --- RemoveEdge tests ---

func TestRemoveEdge_NotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	err := e.RemoveEdge("edge_999")
	if err == nil {
		t.Error("expected error for non-existent edge")
	}
}

func TestRemoveEdge_Success(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("a", NodeTopic, nil)
	e.RegisterNode("b", NodeTopic, nil)
	edge, err := e.AddEdge("topic:a", "topic:b", "flow", "produces", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := e.RemoveEdge(edge.ID); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	graph := e.GetGraph()
	if graph.TotalEdges != 0 {
		t.Errorf("expected 0 edges after removal, got %d", graph.TotalEdges)
	}
	if graph.TotalNodes != 2 {
		t.Errorf("expected 2 nodes still present, got %d", graph.TotalNodes)
	}
}
