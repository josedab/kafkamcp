package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/compliance"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleComplianceScan(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_compliance_scan", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		regulation := compliance.Regulation(request.GetString("regulation", ""))
		cluster := request.GetString("cluster", "")
		topicFilter := request.GetString("topic", "")

		topicData, err := s.collectComplianceData(ctx, cluster, topicFilter)
		if err != nil {
			s.logAudit(agentID, "kafka_compliance_scan", args, cluster, "error", 0, err, start)
			return s.toolError("failed to collect topic data", err), nil
		}

		result := s.complianceEngine.Scan(s.resolveClusterName(cluster), regulation, topicData)
		s.logAudit(agentID, "kafka_compliance_scan", args, cluster, "success", len(result.Violations), nil, start)
		return marshalResult(result)
	})(ctx, request)
}

func (s *Server) handleComplianceReport(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_compliance_report", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		regulation := compliance.Regulation(request.GetString("regulation", ""))
		cluster := request.GetString("cluster", "")

		if regulation == "" {
			return mcp.NewToolResultError("regulation is required"), nil
		}

		topicData, err := s.collectComplianceData(ctx, cluster, "")
		if err != nil {
			s.logAudit(agentID, "kafka_compliance_report", args, cluster, "error", 0, err, start)
			return s.toolError("failed to collect topic data", err), nil
		}

		report := s.complianceEngine.GenerateReport(s.resolveClusterName(cluster), regulation, topicData)
		s.logAudit(agentID, "kafka_compliance_report", args, cluster, "success", 1, nil, start)
		return marshalResult(report)
	})(ctx, request)
}

func (s *Server) handleComplianceViolations(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_compliance_violations", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		regulation := compliance.Regulation(request.GetString("regulation", ""))
		severity := request.GetString("severity", "")

		violations := s.complianceEngine.GetViolations(regulation, severity)
		s.logAudit(agentID, "kafka_compliance_violations", args, "", "success", len(violations), nil, start)
		return marshalResult(map[string]any{"violations": violations, "total": len(violations)})
	})(ctx, request)
}

func (s *Server) collectComplianceData(ctx context.Context, cluster, topicFilter string) ([]compliance.TopicData, error) {
	topics, err := s.kafkaManager.ListTopics(ctx, cluster, false)
	if err != nil {
		return nil, err
	}

	var data []compliance.TopicData
	for _, t := range topics {
		if t.IsInternal {
			continue
		}
		if topicFilter != "" && t.Name != topicFilter {
			continue
		}

		td := compliance.TopicData{
			Name:        t.Name,
			RetentionMs: t.RetentionMs,
			HasMasking:  s.maskingEngine.Enabled(),
		}

		// Sample messages for PII detection
		msgs, err := s.kafkaManager.Consume(ctx, cluster, t.Name, -1, "latest", 5, 3000, "auto")
		if err == nil {
			for _, m := range msgs {
				td.SampleValues = append(td.SampleValues, fmt.Sprintf("%v", m.Value))
			}
		}

		data = append(data, td)
	}

	return data, nil
}
