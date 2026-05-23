package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/dataquality"
)

func (s *Server) registerDataQualityTools() {
	if s.dataQualityEngine == nil {
		return
	}

	s.addTool(mcp.NewTool("kafka_quality_score",
		withCategory("quality"),
		mcp.WithDescription("Get the real-time data quality score for a Kafka topic across freshness, completeness, conformance, anomaly rate, and volume consistency."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name to evaluate")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_quality_score",
		resourceType: "topic",
		action:       "read",
		resourceName: func(r mcp.CallToolRequest) string {
			if t, ok := r.GetArguments()["topic"].(string); ok {
				return t
			}
			return "*"
		},
	}, s.handleQualityScore))

	s.addTool(mcp.NewTool("kafka_quality_report",
		withCategory("quality"),
		mcp.WithDescription("Generate a detailed data quality report for a topic including trends, SLA status, and recommendations."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_quality_report",
		resourceType: "topic",
		action:       "read",
		resourceName: func(r mcp.CallToolRequest) string {
			if t, ok := r.GetArguments()["topic"].(string); ok {
				return t
			}
			return "*"
		},
	}, s.handleQualityReport))

	s.addTool(mcp.NewTool("kafka_sla_status",
		withCategory("quality"),
		mcp.WithDescription("Get the SLA compliance status for a topic or list all configured SLAs."),
		mcp.WithString("topic", mcp.Description("Topic name (omit to list all SLAs)")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_sla_status",
		resourceType: "topic",
		action:       "read",
	}, s.handleSLAStatus))

	s.addTool(mcp.NewTool("kafka_sla_configure",
		withCategory("quality"),
		mcp.WithDescription("Configure a data quality SLA for a topic with threshold alerts."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithNumber("min_freshness", mcp.Description("Minimum freshness score (0-1). Default: 0.8")),
		mcp.WithNumber("min_completeness", mcp.Description("Minimum completeness score (0-1). Default: 0.9")),
		mcp.WithNumber("min_overall", mcp.Description("Minimum overall score (0-1). Default: 0.7")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_sla_configure",
		resourceType: "topic",
		action:       "write",
	}, s.handleSLAConfigure))
}

func (s *Server) handleQualityScore(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	topic, _ := args["topic"].(string)
	if topic == "" {
		return mcp.NewToolResultError("topic is required"), nil
	}

	// Check cached score first
	if score, ok := s.dataQualityEngine.GetScore(topic); ok {
		s.logAudit(ctx, agentID, "kafka_quality_score", args, "", "success", 1, nil, start)
		return marshalResult(score)
	}

	// Evaluate with default metrics (no live data available)
	score := s.dataQualityEngine.Evaluate(dataquality.TopicMetrics{
		Topic:           topic,
		LastMessageTime: time.Now(),
		SampleSize:      0,
	})

	s.logAudit(ctx, agentID, "kafka_quality_score", args, "", "success", 1, nil, start)
	return marshalResult(score)
}

func (s *Server) handleQualityReport(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	topic, _ := args["topic"].(string)
	if topic == "" {
		return mcp.NewToolResultError("topic is required"), nil
	}

	report, err := s.dataQualityEngine.GetReport(topic)
	if err != nil {
		// Evaluate first if no data exists
		s.dataQualityEngine.Evaluate(dataquality.TopicMetrics{
			Topic:           topic,
			LastMessageTime: time.Now(),
			SampleSize:      0,
		})
		report, err = s.dataQualityEngine.GetReport(topic)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_quality_report", args, "", "error", 0, err, start)
			return s.toolError("failed to generate quality report", err), nil
		}
	}

	s.logAudit(ctx, agentID, "kafka_quality_report", args, "", "success", 1, nil, start)
	return marshalResult(report)
}

func (s *Server) handleSLAStatus(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	topic, _ := args["topic"].(string)

	if topic != "" {
		status, ok := s.dataQualityEngine.GetSLAStatus(topic)
		if !ok {
			return marshalResult(map[string]any{
				"topic":   topic,
				"status":  "no_sla",
				"message": "No SLA configured for this topic",
			})
		}
		s.logAudit(ctx, agentID, "kafka_sla_status", args, "", "success", 1, nil, start)
		return marshalResult(status)
	}

	slas := s.dataQualityEngine.ListSLAs()
	s.logAudit(ctx, agentID, "kafka_sla_status", args, "", "success", len(slas), nil, start)
	return marshalResult(map[string]any{
		"slas":  slas,
		"total": len(slas),
	})
}

func (s *Server) handleSLAConfigure(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	topic, _ := args["topic"].(string)
	if topic == "" {
		return mcp.NewToolResultError("topic is required"), nil
	}

	cfg := dataquality.SLAConfig{
		Topic:           topic,
		MinFreshness:    0.8,
		MinCompleteness: 0.9,
		MinOverallScore: 0.7,
	}

	if v, ok := args["min_freshness"].(float64); ok {
		cfg.MinFreshness = v
	}
	if v, ok := args["min_completeness"].(float64); ok {
		cfg.MinCompleteness = v
	}
	if v, ok := args["min_overall"].(float64); ok {
		cfg.MinOverallScore = v
	}

	err := s.dataQualityEngine.SetSLA(cfg)
	if err != nil {
		s.logAudit(ctx, agentID, "kafka_sla_configure", args, "", "error", 0, err, start)
		return s.toolError("failed to configure SLA", err), nil
	}

	s.logAudit(ctx, agentID, "kafka_sla_configure", args, "", "success", 1, nil, start)
	return marshalResult(map[string]any{
		"status": "configured",
		"topic":  topic,
		"sla":    cfg,
	})
}
