package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/anomaly"
	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleTimeSeriesAnalyze(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_timeseries_analyze",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "analyze")
		metric := request.GetString("metric", "")
		source := request.GetString("source", "")
		sinceHours := 24
		if sh, ok := args["since_hours"].(float64); ok && sh > 0 {
			sinceHours = int(sh)
		}

		if metric == "" || source == "" {
			return mcp.NewToolResultError("metric and source are required"), nil
		}
		if deny := validateStringLength("source", source); deny != nil {
			return deny, nil
		}

		metricType := anomaly.MetricType(metric)
		since := time.Now().Add(-time.Duration(sinceHours) * time.Hour)

		switch action {
		case "analyze":
			analysis, err := s.timeseriesAnalyzer.Analyze(metricType, source, since)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_timeseries_analyze", args, "", "error", 0, err, start)
				return s.toolError("Failed to analyze time series", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_timeseries_analyze", args, "", "success", analysis.DataPoints, nil, start)
			return marshalResult(analysis)

		case "correlate":
			metric2 := request.GetString("metric2", metric)
			source2 := request.GetString("source2", "")
			if source2 == "" {
				return mcp.NewToolResultError("source2 is required for correlation"), nil
			}
			corr, err := s.timeseriesAnalyzer.CorrelateMetrics(metricType, source, anomaly.MetricType(metric2), source2, since)
			if err != nil {
				return s.toolError("Failed to correlate metrics", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_timeseries_analyze", args, "", "success", 0, nil, start)
			return marshalResult(corr)

		case "ingest":
			value := 0.0
			if v, ok := args["value"].(float64); ok {
				value = v
			}
			s.timeseriesAnalyzer.RecordPoint(metricType, source, value, time.Now())
			s.logAudit(ctx, agentID, "kafka_timeseries_analyze", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "ingested", "metric": metric, "source": source, "value": value})

		default:
			return mcp.NewToolResultError("invalid action: must be analyze, correlate, or ingest"), nil
		}
	})(ctx, request)
}
