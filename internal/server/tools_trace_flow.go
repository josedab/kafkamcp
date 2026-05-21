package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/lineage"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleTraceFlow(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_trace_flow",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")

		switch action {
		case "trace":
			traceID := request.GetString("trace_id", "")
			if traceID == "" {
				return mcp.NewToolResultError("trace_id is required"), nil
			}
			if deny := validateStringLength("trace_id", traceID); deny != nil {
				return deny, nil
			}

			trace, err := s.messageTracer.GetTrace(traceID)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_trace_flow", args, "", "error", 0, err, start)
				return s.toolError("Trace not found", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_trace_flow", args, "", "success", len(trace.Spans), nil, start)
			return marshalResult(trace)

		case "record":
			traceID := request.GetString("trace_id", "")
			operation := request.GetString("operation", "produce")
			topic := request.GetString("topic", "")
			producer := request.GetString("producer", "")
			consumer := request.GetString("consumer", "")
			parentSpanID := request.GetString("parent_span_id", "")

			if traceID == "" || topic == "" {
				return mcp.NewToolResultError("trace_id and topic are required for record action"), nil
			}
			if deny := validateStringLength("trace_id", traceID); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("topic", topic); deny != nil {
				return deny, nil
			}

			var partition int32
			if p, ok := args["partition"].(float64); ok {
				partition = int32(p)
			}
			var offset int64
			if o, ok := args["offset"].(float64); ok {
				offset = int64(o)
			}
			var durationMs int64
			if d, ok := args["duration_ms"].(float64); ok {
				durationMs = int64(d)
			}

			span := lineage.TraceSpan{
				TraceID:    traceID,
				ParentID:   parentSpanID,
				Operation:  operation,
				Topic:      topic,
				Partition:  partition,
				Offset:     offset,
				Producer:   producer,
				Consumer:   consumer,
				DurationMs: durationMs,
				Status:     "ok",
			}

			s.messageTracer.RecordSpan(span)
			s.logAudit(ctx, agentID, "kafka_trace_flow", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{
				"status":   "recorded",
				"trace_id": traceID,
				"span_id":  span.SpanID,
			})

		case "create_context":
			parentSpanID := request.GetString("parent_span_id", "")
			var tc *lineage.TraceContext
			if parentSpanID != "" {
				traceID := request.GetString("trace_id", "")
				if traceID == "" {
					return mcp.NewToolResultError("trace_id is required when creating child context"), nil
				}
				parent := &lineage.TraceContext{TraceID: traceID, SpanID: parentSpanID}
				var err error
				tc, err = lineage.CreateChildContext(parent)
				if err != nil {
					return s.toolError("failed to create child context", err), nil
				}
			} else {
				var err error
				tc, err = lineage.GenerateTraceContext()
				if err != nil {
					return s.toolError("failed to generate trace context", err), nil
				}
			}
			s.logAudit(ctx, agentID, "kafka_trace_flow", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{
				"trace_context": tc,
				"traceparent":   tc.FormatTraceparent(),
				"message":       "Inject traceparent header when producing messages",
			})

		case "list":
			limit := 50
			if l, ok := args["limit"].(float64); ok && l > 0 {
				limit = int(l)
			}
			traces := s.messageTracer.ListTraces(limit)
			s.logAudit(ctx, agentID, "kafka_trace_flow", args, "", "success", len(traces), nil, start)
			return marshalResult(map[string]any{"traces": traces, "total_count": len(traces)})

		default:
			return mcp.NewToolResultError("invalid action: must be trace, record, create_context, or list"), nil
		}
	})(ctx, request)
}
