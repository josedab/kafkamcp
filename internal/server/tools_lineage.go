package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/lineage"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleTraceLineage(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_trace_lineage", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		topic := request.GetString("topic", "")
		cluster := request.GetString("cluster", "")

		if deny := validateStringLength("action", action); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("topic", topic); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}

		switch action {
		case "trace":
			if topic == "" {
				return mcp.NewToolResultError("topic is required for trace action"), nil
			}

			// Auto-discover lineage from cluster metadata
			graph, err := s.kafkaManager.GetTopicGraph(ctx, cluster)
			if err == nil {
				for _, node := range graph.Nodes {
					s.lineageTracker.AddNode(lineage.LineageNode{
						ID: node.ID, Name: node.Name, Type: node.Type,
					})
				}
				for _, edge := range graph.Edges {
					s.lineageTracker.AddEdge(lineage.LineageEdge{
						Source: edge.Source, Target: edge.Target, Type: edge.Type,
					})
				}
			}

			result := s.lineageTracker.TraceLineage(topic)
			s.logAudit(ctx, agentID, "kafka_trace_lineage", args, cluster, "success", 1, nil, start)
			return marshalResult(result)

		case "annotate_pii":
			if topic == "" {
				return mcp.NewToolResultError("topic is required"), nil
			}
			piiType := request.GetString("pii_type", "email")
			field := request.GetString("field", "")
			if field == "" {
				return mcp.NewToolResultError("field is required for annotate_pii"), nil
			}
			if deny := validateStringLength("pii_type", piiType); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("field", field); deny != nil {
				return deny, nil
			}
			s.lineageTracker.AnnotatePII(lineage.PIIAnnotation{
				Topic: topic, Field: field, PIIType: piiType, DetectedBy: "manual",
			})
			s.logAudit(ctx, agentID, "kafka_trace_lineage", args, cluster, "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "annotated", "topic": topic, "field": field, "pii_type": piiType})

		case "detect_pii":
			if topic == "" {
				return mcp.NewToolResultError("topic is required"), nil
			}
			// Auto-detect from topic metadata
			detected := s.lineageTracker.DetectPIIPatterns(topic, nil)
			for _, pii := range detected {
				s.lineageTracker.AnnotatePII(pii)
			}
			s.logAudit(ctx, agentID, "kafka_trace_lineage", args, cluster, "success", len(detected), nil, start)
			return marshalResult(map[string]any{"topic": topic, "detected": detected, "count": len(detected)})

		case "compliance_report":
			framework := request.GetString("framework", "GDPR")
			if deny := validateStringLength("framework", framework); deny != nil {
				return deny, nil
			}
			report := s.lineageTracker.GenerateComplianceReport(framework)
			s.logAudit(ctx, agentID, "kafka_trace_lineage", args, cluster, "success", 1, nil, start)
			return marshalResult(report)

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: trace, annotate_pii, detect_pii, compliance_report", action)), nil
		}
	})(ctx, request)
}
