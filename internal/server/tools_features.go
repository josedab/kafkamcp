package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/anomaly"
	"github.com/kafkamcp/kafkamcp/internal/auth"
)

// ===================== FEATURE TOOL HANDLERS =====================

// handleAnomalies returns detected anomalies from the anomaly detection engine.
func (s *Server) handleAnomalies(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_anomalies",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		if s.anomalyDetector == nil {
			return mcp.NewToolResultError("Anomaly detection is not configured"), nil
		}

		severity := request.GetString("severity", "")
		metric := request.GetString("metric", "")
		limit := request.GetInt("limit", 50)
		sinceHours := request.GetInt("since_hours", 24)

		if deny := validateStringLength("severity", severity); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("metric", metric); deny != nil {
			return deny, nil
		}
		if limit < 1 {
			limit = 1
		}
		if limit > 1000 {
			limit = 1000
		}
		if sinceHours < 1 {
			sinceHours = 1
		}
		if sinceHours > 8760 {
			sinceHours = 8760
		}

		since := time.Now().Add(-time.Duration(sinceHours) * time.Hour)
		alerts := s.anomalyDetector.GetAlerts(since, severity, metric, limit)
		status := s.anomalyDetector.GetStatus()

		s.logAudit(ctx, agentID, "kafka_anomalies", args, "", "success", len(alerts), nil, start)
		return marshalResult(map[string]any{
			"alerts":      alerts,
			"alert_count": len(alerts),
			"status":      status,
		})
	})(ctx, request)
}

// handleIngestAnomaly allows manual data point ingestion for anomaly detection.
func (s *Server) handleIngestAnomaly(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_anomaly_ingest",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		if s.anomalyDetector == nil {
			return mcp.NewToolResultError("Anomaly detection is not configured"), nil
		}

		metric := request.GetString("metric", "")
		source := request.GetString("source", "")
		value := request.GetFloat("value", 0)

		if metric == "" || source == "" {
			return mcp.NewToolResultError("metric and source are required"), nil
		}
		if deny := validateStringLength("metric", metric); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("source", source); deny != nil {
			return deny, nil
		}

		dp := anomaly.DataPoint{
			Timestamp: time.Now(),
			Value:     value,
			Metric:    anomaly.MetricType(metric),
			Source:    source,
		}
		s.anomalyDetector.IngestPoint(dp)

		s.logAudit(ctx, agentID, "kafka_anomaly_ingest", args, "", "success", 1, nil, start)
		return marshalResult(map[string]any{"status": "ingested", "data_point": dp})
	})(ctx, request)
}

func intFromAny(v any, defaultVal int) int {
	switch val := v.(type) {
	case float64:
		return int(val)
	case int:
		return val
	case int64:
		return int(val)
	default:
		return defaultVal
	}
}
