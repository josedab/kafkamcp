package lineage

import (
	"strings"
	"testing"
)

func newTestTracker() *Tracker {
	t := NewTracker()
	t.AddNode(LineageNode{ID: "producer-1", Name: "UserService", Type: "producer"})
	t.AddNode(LineageNode{ID: "topic-users", Name: "users", Type: "topic"})
	t.AddNode(LineageNode{ID: "consumer-1", Name: "AnalyticsService", Type: "consumer"})
	t.AddNode(LineageNode{ID: "consumer-2", Name: "NotificationService", Type: "consumer"})

	t.AddEdge(LineageEdge{Source: "producer-1", Target: "topic-users", Type: "produces"})
	t.AddEdge(LineageEdge{Source: "topic-users", Target: "consumer-1", Type: "consumes"})
	t.AddEdge(LineageEdge{Source: "topic-users", Target: "consumer-2", Type: "consumes"})
	return t
}

func TestNewTracker(t *testing.T) {
	tr := NewTracker()
	if tr == nil {
		t.Fatal("expected non-nil tracker")
	}
	if tr.nodes == nil {
		t.Fatal("expected initialized nodes map")
	}
}

func TestAddNode(t *testing.T) {
	tr := NewTracker()
	tr.AddNode(LineageNode{ID: "n1", Name: "node1", Type: "topic", Metadata: map[string]any{"key": "val"}})
	if len(tr.nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(tr.nodes))
	}
	n := tr.nodes["n1"]
	if n.Name != "node1" || n.Type != "topic" {
		t.Fatalf("unexpected node: %+v", n)
	}
}

func TestAddEdge(t *testing.T) {
	tr := NewTracker()
	tr.AddEdge(LineageEdge{Source: "a", Target: "b", Type: "produces", Latency: "10ms"})
	if len(tr.edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(tr.edges))
	}
	if tr.edges[0].Latency != "10ms" {
		t.Fatalf("unexpected latency: %s", tr.edges[0].Latency)
	}
}

func TestAnnotatePII(t *testing.T) {
	tr := NewTracker()
	tr.AnnotatePII(PIIAnnotation{Topic: "users", Field: "email", PIIType: "email", DetectedBy: "annotation"})
	if len(tr.piiFields) != 1 {
		t.Fatalf("expected 1 PII annotation, got %d", len(tr.piiFields))
	}
}

func TestGetUpstream(t *testing.T) {
	tr := newTestTracker()
	upstream := tr.GetUpstream("topic-users")
	if len(upstream) != 1 {
		t.Fatalf("expected 1 upstream node, got %d", len(upstream))
	}
	if upstream[0].ID != "producer-1" {
		t.Fatalf("expected producer-1, got %s", upstream[0].ID)
	}
}

func TestGetUpstream_NoResults(t *testing.T) {
	tr := newTestTracker()
	upstream := tr.GetUpstream("producer-1")
	if len(upstream) != 0 {
		t.Fatalf("expected 0 upstream nodes, got %d", len(upstream))
	}
}

func TestGetDownstream(t *testing.T) {
	tr := newTestTracker()
	downstream := tr.GetDownstream("topic-users")
	if len(downstream) != 2 {
		t.Fatalf("expected 2 downstream nodes, got %d", len(downstream))
	}
	ids := map[string]bool{}
	for _, n := range downstream {
		ids[n.ID] = true
	}
	if !ids["consumer-1"] || !ids["consumer-2"] {
		t.Fatalf("unexpected downstream nodes: %v", downstream)
	}
}

func TestGetDownstream_NoResults(t *testing.T) {
	tr := newTestTracker()
	downstream := tr.GetDownstream("consumer-1")
	if len(downstream) != 0 {
		t.Fatalf("expected 0 downstream nodes, got %d", len(downstream))
	}
}

func TestTraceLineage(t *testing.T) {
	tr := newTestTracker()
	tr.AnnotatePII(PIIAnnotation{Topic: "topic-users", Field: "email", PIIType: "email", DetectedBy: "annotation"})

	graph := tr.TraceLineage("topic-users")
	if len(graph.Nodes) != 4 {
		t.Fatalf("expected 4 nodes, got %d", len(graph.Nodes))
	}
	if len(graph.Edges) != 3 {
		t.Fatalf("expected 3 edges, got %d", len(graph.Edges))
	}
	if len(graph.PIIFields) != 1 {
		t.Fatalf("expected 1 PII field, got %d", len(graph.PIIFields))
	}
}

func TestTraceLineage_Isolated(t *testing.T) {
	tr := NewTracker()
	tr.AddNode(LineageNode{ID: "isolated", Name: "isolated-topic", Type: "topic"})
	graph := tr.TraceLineage("isolated")
	if len(graph.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(graph.Nodes))
	}
	if len(graph.Edges) != 0 {
		t.Fatalf("expected 0 edges, got %d", len(graph.Edges))
	}
}

func TestTraceLineage_UnknownTopic(t *testing.T) {
	tr := NewTracker()
	graph := tr.TraceLineage("nonexistent")
	if len(graph.Nodes) != 0 {
		t.Fatalf("expected 0 nodes, got %d", len(graph.Nodes))
	}
}

func TestGenerateComplianceReport(t *testing.T) {
	tr := newTestTracker()
	tr.AnnotatePII(PIIAnnotation{Topic: "topic-users", Field: "email", PIIType: "email", DetectedBy: "pattern"})
	tr.AnnotatePII(PIIAnnotation{Topic: "topic-users", Field: "phone", PIIType: "phone", DetectedBy: "annotation"})

	report := tr.GenerateComplianceReport("GDPR")
	if report.Framework != "GDPR" {
		t.Fatalf("expected GDPR, got %s", report.Framework)
	}
	if report.TotalPIIFields != 2 {
		t.Fatalf("expected 2 total PII fields, got %d", report.TotalPIIFields)
	}
	if len(report.PIITopics) != 1 {
		t.Fatalf("expected 1 PII topic, got %d", len(report.PIITopics))
	}
	topicReport := report.PIITopics[0]
	if topicReport.Topic != "topic-users" {
		t.Fatalf("expected topic-users, got %s", topicReport.Topic)
	}
	if len(topicReport.Consumers) != 2 {
		t.Fatalf("expected 2 consumers, got %d", len(topicReport.Consumers))
	}
	if topicReport.DeletionPath == "" {
		t.Fatal("expected non-empty deletion path")
	}
	if report.Timestamp == "" {
		t.Fatal("expected non-empty timestamp")
	}
}

func TestGenerateComplianceReport_NoPII(t *testing.T) {
	tr := newTestTracker()
	report := tr.GenerateComplianceReport("CCPA")
	if report.Framework != "CCPA" {
		t.Fatalf("expected CCPA, got %s", report.Framework)
	}
	if report.TotalPIIFields != 0 {
		t.Fatalf("expected 0 PII fields, got %d", report.TotalPIIFields)
	}
}

func TestDetectPIIPatterns_FieldName(t *testing.T) {
	tr := NewTracker()
	samples := map[string][]string{
		"email_address": {"not-an-email"},
		"phone_number":  {"not-a-phone"},
		"ssn":           {"not-an-ssn"},
		"first_name":    {"Alice"},
		"street":        {"123 Main St"},
		"ip_address":    {"not-an-ip"},
	}
	annotations := tr.DetectPIIPatterns("users", samples)
	if len(annotations) < 6 {
		t.Fatalf("expected at least 6 annotations from field names, got %d", len(annotations))
	}
	types := map[string]bool{}
	for _, a := range annotations {
		types[a.PIIType] = true
		if a.DetectedBy != "pattern" {
			t.Fatalf("expected detected_by=pattern, got %s", a.DetectedBy)
		}
	}
	for _, expected := range []string{"email", "phone", "ssn", "name", "address", "ip"} {
		if !types[expected] {
			t.Fatalf("expected PII type %s to be detected", expected)
		}
	}
}

func TestDetectPIIPatterns_ValueMatch(t *testing.T) {
	tr := NewTracker()
	samples := map[string][]string{
		"contact":    {"user@example.com"},
		"tel":        {"+1-555-123-4567"},
		"tax_id":     {"123-45-6789"},
		"client_src": {"192.168.1.1"},
	}
	annotations := tr.DetectPIIPatterns("events", samples)
	types := map[string]bool{}
	for _, a := range annotations {
		types[a.PIIType] = true
	}
	for _, expected := range []string{"email", "phone", "ssn", "ip"} {
		if !types[expected] {
			t.Fatalf("expected PII type %s to be detected via value, got types: %v", expected, types)
		}
	}
}

func TestDetectPIIPatterns_NoMatch(t *testing.T) {
	tr := NewTracker()
	samples := map[string][]string{
		"count":  {"42"},
		"status": {"active"},
	}
	annotations := tr.DetectPIIPatterns("metrics", samples)
	if len(annotations) != 0 {
		t.Fatalf("expected 0 annotations, got %d", len(annotations))
	}
}

func TestBuildDeletionPath(t *testing.T) {
	path := buildDeletionPath("topic-users", []string{"AnalyticsService", "NotificationService"})
	if !strings.Contains(path, "topic-users") {
		t.Fatal("expected deletion path to contain topic")
	}
	if !strings.Contains(path, "AnalyticsService") {
		t.Fatal("expected deletion path to contain consumer")
	}
}

func TestBuildDeletionPath_NoConsumers(t *testing.T) {
	path := buildDeletionPath("topic-orphan", nil)
	if path != "topic-orphan" {
		t.Fatalf("expected 'topic-orphan', got %s", path)
	}
}

func TestDuplicateEdgesInTraceLineage(t *testing.T) {
	tr := NewTracker()
	tr.AddNode(LineageNode{ID: "a", Name: "a", Type: "topic"})
	tr.AddNode(LineageNode{ID: "b", Name: "b", Type: "consumer"})
	tr.AddEdge(LineageEdge{Source: "a", Target: "b", Type: "consumes"})
	tr.AddEdge(LineageEdge{Source: "a", Target: "b", Type: "consumes"})

	graph := tr.TraceLineage("a")
	if len(graph.Edges) != 1 {
		t.Fatalf("expected deduplicated edges, got %d", len(graph.Edges))
	}
}

func TestGetUpstream_Deduplication(t *testing.T) {
	tr := NewTracker()
	tr.AddNode(LineageNode{ID: "p", Name: "Producer", Type: "producer"})
	tr.AddNode(LineageNode{ID: "t", Name: "Topic", Type: "topic"})
	tr.AddEdge(LineageEdge{Source: "p", Target: "t", Type: "produces"})
	tr.AddEdge(LineageEdge{Source: "p", Target: "t", Type: "produces"})

	upstream := tr.GetUpstream("t")
	if len(upstream) != 1 {
		t.Fatalf("expected 1 deduplicated upstream node, got %d", len(upstream))
	}
}
