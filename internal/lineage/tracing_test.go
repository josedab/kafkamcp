package lineage

import (
	"regexp"
	"strings"
	"testing"
)

func TestGenerateTraceContext(t *testing.T) {
	ctx := GenerateTraceContext()
	if ctx == nil {
		t.Fatal("expected non-nil trace context")
	}
	if len(ctx.TraceID) != 32 {
		t.Fatalf("expected trace ID length 32, got %d", len(ctx.TraceID))
	}
	if len(ctx.SpanID) != 16 {
		t.Fatalf("expected span ID length 16, got %d", len(ctx.SpanID))
	}
	hexPattern := regexp.MustCompile(`^[0-9a-f]+$`)
	if !hexPattern.MatchString(ctx.TraceID) {
		t.Fatalf("trace ID is not valid hex: %s", ctx.TraceID)
	}
	if !hexPattern.MatchString(ctx.SpanID) {
		t.Fatalf("span ID is not valid hex: %s", ctx.SpanID)
	}
	if ctx.Flags != "01" {
		t.Fatalf("expected flags '01', got %s", ctx.Flags)
	}
	if ctx.ParentID != "" {
		t.Fatalf("expected empty parent ID, got %s", ctx.ParentID)
	}

	// Verify uniqueness
	ctx2 := GenerateTraceContext()
	if ctx.TraceID == ctx2.TraceID {
		t.Fatal("expected unique trace IDs")
	}
	if ctx.SpanID == ctx2.SpanID {
		t.Fatal("expected unique span IDs")
	}
}

func TestCreateChildContext(t *testing.T) {
	parent := GenerateTraceContext()
	child := CreateChildContext(parent)

	if child.TraceID != parent.TraceID {
		t.Fatalf("expected child trace ID %s, got %s", parent.TraceID, child.TraceID)
	}
	if child.ParentID != parent.SpanID {
		t.Fatalf("expected parent ID %s, got %s", parent.SpanID, child.ParentID)
	}
	if child.SpanID == parent.SpanID {
		t.Fatal("expected child span ID to differ from parent")
	}
	if len(child.SpanID) != 16 {
		t.Fatalf("expected child span ID length 16, got %d", len(child.SpanID))
	}
	if child.Flags != parent.Flags {
		t.Fatalf("expected flags %s, got %s", parent.Flags, child.Flags)
	}
}

func TestFormatTraceparent(t *testing.T) {
	ctx := &TraceContext{
		TraceID: "0af7651916cd43dd8448eb211c80319c",
		SpanID:  "b7ad6b7169203331",
		Flags:   "01",
	}
	tp := ctx.FormatTraceparent()
	expected := "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	if tp != expected {
		t.Fatalf("expected %s, got %s", expected, tp)
	}

	// Verify W3C format: version-traceID-spanID-flags
	parts := strings.Split(tp, "-")
	if len(parts) != 4 {
		t.Fatalf("expected 4 parts in traceparent, got %d", len(parts))
	}
	if parts[0] != "00" {
		t.Fatalf("expected version '00', got %s", parts[0])
	}
}

func TestMessageTracerRecordAndGet(t *testing.T) {
	mt := NewMessageTracer()
	traceID := "abc123def456"

	mt.RecordSpan(TraceSpan{
		TraceID:    traceID,
		SpanID:     "span-1",
		Operation:  "produce",
		Topic:      "orders",
		Partition:  0,
		Offset:     10,
		Producer:   "OrderService",
		StartTime:  "2024-01-01T00:00:00Z",
		DurationMs: 5,
		Status:     "ok",
	})
	mt.RecordSpan(TraceSpan{
		TraceID:    traceID,
		SpanID:     "span-2",
		ParentID:   "span-1",
		Operation:  "consume",
		Topic:      "orders",
		Partition:  0,
		Offset:     10,
		Consumer:   "PaymentService",
		StartTime:  "2024-01-01T00:00:01Z",
		DurationMs: 12,
		Status:     "ok",
	})

	trace, err := mt.GetTrace(traceID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if trace.TraceID != traceID {
		t.Fatalf("expected trace ID %s, got %s", traceID, trace.TraceID)
	}
	if len(trace.Spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(trace.Spans))
	}
	if trace.ProducerCount != 1 {
		t.Fatalf("expected 1 producer, got %d", trace.ProducerCount)
	}
	if trace.ConsumerCount != 1 {
		t.Fatalf("expected 1 consumer, got %d", trace.ConsumerCount)
	}
	if trace.TotalLatencyMs != 17 {
		t.Fatalf("expected total latency 17ms, got %d", trace.TotalLatencyMs)
	}
	if trace.Status != "complete" {
		t.Fatalf("expected status 'complete', got %s", trace.Status)
	}
	if len(trace.TopicHops) != 1 || trace.TopicHops[0] != "orders" {
		t.Fatalf("unexpected topic hops: %v", trace.TopicHops)
	}

	// Verify flow graph
	if trace.FlowGraph == nil {
		t.Fatal("expected non-nil flow graph")
	}
	if len(trace.FlowGraph.Nodes) != 3 {
		t.Fatalf("expected 3 flow graph nodes (producer, topic, consumer), got %d", len(trace.FlowGraph.Nodes))
	}
	if len(trace.FlowGraph.Edges) != 2 {
		t.Fatalf("expected 2 flow graph edges, got %d", len(trace.FlowGraph.Edges))
	}
}

func TestMessageTracerGetTrace_NotFound(t *testing.T) {
	mt := NewMessageTracer()
	_, err := mt.GetTrace("nonexistent-trace")
	if err == nil {
		t.Fatal("expected error for unknown trace ID")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected 'not found' in error, got: %v", err)
	}
}

func TestMessageTracerListTraces(t *testing.T) {
	mt := NewMessageTracer()

	// Empty tracer
	result := mt.ListTraces(10)
	if len(result) != 0 {
		t.Fatalf("expected 0 traces, got %d", len(result))
	}

	// Add traces
	mt.RecordSpan(TraceSpan{
		TraceID:   "trace-1",
		SpanID:    "s1",
		Operation: "produce",
		Topic:     "topic-a",
		StartTime: "2024-01-01T00:00:00Z",
		Status:    "ok",
	})
	mt.RecordSpan(TraceSpan{
		TraceID:   "trace-2",
		SpanID:    "s2",
		Operation: "produce",
		Topic:     "topic-b",
		StartTime: "2024-01-01T00:00:01Z",
		Status:    "ok",
	})
	mt.RecordSpan(TraceSpan{
		TraceID:   "trace-3",
		SpanID:    "s3",
		Operation: "consume",
		Topic:     "topic-c",
		StartTime: "2024-01-01T00:00:02Z",
		Status:    "ok",
	})

	// List all
	result = mt.ListTraces(10)
	if len(result) != 3 {
		t.Fatalf("expected 3 traces, got %d", len(result))
	}

	// Verify fields present
	for _, r := range result {
		if r["trace_id"] == nil {
			t.Fatal("expected trace_id in result")
		}
		if r["span_count"] == nil {
			t.Fatal("expected span_count in result")
		}
	}

	// List with limit
	result = mt.ListTraces(2)
	if len(result) != 2 {
		t.Fatalf("expected 2 traces with limit, got %d", len(result))
	}
}
