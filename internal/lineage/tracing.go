package lineage

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"
)

// TraceContext represents W3C Trace Context propagation headers.
type TraceContext struct {
	TraceID  string `json:"trace_id"`
	SpanID   string `json:"span_id"`
	ParentID string `json:"parent_id,omitempty"`
	Flags    string `json:"flags,omitempty"`
}

// TraceSpan represents a single span in a message's journey.
type TraceSpan struct {
	SpanID     string            `json:"span_id"`
	ParentID   string            `json:"parent_id,omitempty"`
	TraceID    string            `json:"trace_id"`
	Operation  string            `json:"operation"` // "produce", "consume", "transform"
	Topic      string            `json:"topic"`
	Partition  int32             `json:"partition"`
	Offset     int64             `json:"offset"`
	Producer   string            `json:"producer,omitempty"`
	Consumer   string            `json:"consumer,omitempty"`
	StartTime  string            `json:"start_time"`
	EndTime    string            `json:"end_time,omitempty"`
	DurationMs int64             `json:"duration_ms"`
	Status     string            `json:"status"` // "ok", "error"
	Tags       map[string]string `json:"tags,omitempty"`
}

// MessageTrace represents the complete journey of a message through Kafka.
type MessageTrace struct {
	TraceID        string      `json:"trace_id"`
	Spans          []TraceSpan `json:"spans"`
	TopicHops      []string    `json:"topic_hops"`
	TotalLatencyMs int64       `json:"total_latency_ms"`
	ProducerCount  int         `json:"producer_count"`
	ConsumerCount  int         `json:"consumer_count"`
	FlowGraph      *FlowGraph  `json:"flow_graph"`
	Status         string      `json:"status"` // "complete", "in_progress", "error"
	Timestamp      string      `json:"timestamp"`
}

// FlowGraph represents a visual flow graph of message propagation.
type FlowGraph struct {
	Nodes []FlowNode `json:"nodes"`
	Edges []FlowEdge `json:"edges"`
}

// FlowNode is a node in the message flow graph.
type FlowNode struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Type  string `json:"type"` // "producer", "topic", "consumer", "transform"
}

// FlowEdge connects two nodes in the flow graph.
type FlowEdge struct {
	From      string `json:"from"`
	To        string `json:"to"`
	Label     string `json:"label,omitempty"`
	LatencyMs int64  `json:"latency_ms,omitempty"`
}

// MessageTracer provides distributed tracing for Kafka messages.
type MessageTracer struct {
	mu       sync.RWMutex
	traces   map[string]*MessageTrace
	spans    map[string][]TraceSpan // traceID -> spans
	traceOrder []string             // insertion order for FIFO eviction
}

// NewMessageTracer creates a new message tracer.
func NewMessageTracer() *MessageTracer {
	return &MessageTracer{
		traces: make(map[string]*MessageTrace),
		spans:  make(map[string][]TraceSpan),
	}
}

// GenerateTraceContext creates a new W3C Trace Context.
func GenerateTraceContext() *TraceContext {
	return &TraceContext{
		TraceID: generateHexID(16),
		SpanID:  generateHexID(8),
		Flags:   "01",
	}
}

// CreateChildContext creates a child span context from a parent.
func CreateChildContext(parent *TraceContext) *TraceContext {
	return &TraceContext{
		TraceID:  parent.TraceID,
		SpanID:   generateHexID(8),
		ParentID: parent.SpanID,
		Flags:    parent.Flags,
	}
}

// FormatTraceparent formats the context as a W3C traceparent header value.
func (tc *TraceContext) FormatTraceparent() string {
	return fmt.Sprintf("00-%s-%s-%s", tc.TraceID, tc.SpanID, tc.Flags)
}

func generateHexID(bytes int) string {
	b := make([]byte, bytes)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// RecordSpan records a trace span for a message operation.
func (mt *MessageTracer) RecordSpan(span TraceSpan) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if span.SpanID == "" {
		span.SpanID = generateHexID(8)
	}
	if span.StartTime == "" {
		span.StartTime = time.Now().UTC().Format(time.RFC3339Nano)
	}

	mt.spans[span.TraceID] = append(mt.spans[span.TraceID], span)

	// Track insertion order for new trace IDs
	if len(mt.spans[span.TraceID]) == 1 {
		mt.traceOrder = append(mt.traceOrder, span.TraceID)
	}

	// Evict oldest traces using FIFO order
	for len(mt.spans) > 10000 && len(mt.traceOrder) > 0 {
		oldest := mt.traceOrder[0]
		mt.traceOrder = mt.traceOrder[1:]
		delete(mt.spans, oldest)
	}
}

// GetTrace retrieves the complete trace for a message by trace ID.
func (mt *MessageTracer) GetTrace(traceID string) (*MessageTrace, error) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	spans, ok := mt.spans[traceID]
	if !ok || len(spans) == 0 {
		return nil, fmt.Errorf("trace %q not found", traceID)
	}

	// Sort spans by start time
	sorted := make([]TraceSpan, len(spans))
	copy(sorted, spans)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].StartTime < sorted[j].StartTime
	})

	// Build topic hops
	topicSet := make(map[string]bool)
	var topicHops []string
	producers := make(map[string]bool)
	consumers := make(map[string]bool)
	var totalLatency int64

	for _, span := range sorted {
		if !topicSet[span.Topic] {
			topicSet[span.Topic] = true
			topicHops = append(topicHops, span.Topic)
		}
		if span.Producer != "" {
			producers[span.Producer] = true
		}
		if span.Consumer != "" {
			consumers[span.Consumer] = true
		}
		totalLatency += span.DurationMs
	}

	// Build flow graph
	flowGraph := mt.buildFlowGraph(sorted)

	status := "complete"
	for _, span := range sorted {
		if span.Status == "error" {
			status = "error"
			break
		}
	}

	return &MessageTrace{
		TraceID:        traceID,
		Spans:          sorted,
		TopicHops:      topicHops,
		TotalLatencyMs: totalLatency,
		ProducerCount:  len(producers),
		ConsumerCount:  len(consumers),
		FlowGraph:      flowGraph,
		Status:         status,
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
	}, nil
}

func (mt *MessageTracer) buildFlowGraph(spans []TraceSpan) *FlowGraph {
	graph := &FlowGraph{}
	nodeSet := make(map[string]bool)

	for _, span := range spans {
		// Add topic node
		topicNodeID := "topic:" + span.Topic
		if !nodeSet[topicNodeID] {
			nodeSet[topicNodeID] = true
			graph.Nodes = append(graph.Nodes, FlowNode{
				ID:    topicNodeID,
				Label: span.Topic,
				Type:  "topic",
			})
		}

		if span.Operation == "produce" && span.Producer != "" {
			prodNodeID := "producer:" + span.Producer
			if !nodeSet[prodNodeID] {
				nodeSet[prodNodeID] = true
				graph.Nodes = append(graph.Nodes, FlowNode{
					ID:    prodNodeID,
					Label: span.Producer,
					Type:  "producer",
				})
			}
			graph.Edges = append(graph.Edges, FlowEdge{
				From:      prodNodeID,
				To:        topicNodeID,
				LatencyMs: span.DurationMs,
			})
		}

		if span.Operation == "consume" && span.Consumer != "" {
			consNodeID := "consumer:" + span.Consumer
			if !nodeSet[consNodeID] {
				nodeSet[consNodeID] = true
				graph.Nodes = append(graph.Nodes, FlowNode{
					ID:    consNodeID,
					Label: span.Consumer,
					Type:  "consumer",
				})
			}
			graph.Edges = append(graph.Edges, FlowEdge{
				From:      topicNodeID,
				To:        consNodeID,
				LatencyMs: span.DurationMs,
			})
		}

		if span.Operation == "transform" {
			transformNodeID := "transform:" + span.SpanID
			if !nodeSet[transformNodeID] {
				nodeSet[transformNodeID] = true
				graph.Nodes = append(graph.Nodes, FlowNode{
					ID:    transformNodeID,
					Label: "transform",
					Type:  "transform",
				})
			}
		}
	}

	return graph
}

// ListTraces returns recent trace IDs with summary info.
func (mt *MessageTracer) ListTraces(limit int) []map[string]any {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	if limit <= 0 {
		limit = 50
	}

	var result []map[string]any
	for traceID, spans := range mt.spans {
		if len(result) >= limit {
			break
		}
		topicSet := make(map[string]bool)
		for _, s := range spans {
			topicSet[s.Topic] = true
		}
		topics := make([]string, 0, len(topicSet))
		for t := range topicSet {
			topics = append(topics, t)
		}
		result = append(result, map[string]any{
			"trace_id":   traceID,
			"span_count": len(spans),
			"topics":     topics,
			"start_time": spans[0].StartTime,
		})
	}
	return result
}
