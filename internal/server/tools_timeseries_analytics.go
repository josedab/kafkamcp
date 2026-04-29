package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/timeseries"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleTimeSeriesQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_timeseries_query",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		metric := timeseries.MetricType(request.GetString("metric", ""))
		source := request.GetString("source", "")
		window := request.GetString("window", "1h")

		if metric == "" || source == "" {
			return mcp.NewToolResultError("metric and source are required"), nil
		}

		result, err := s.timeseriesEngine.Query(metric, source, window)
		if err != nil {
			return s.toolError("Query failed", err), nil
		}

		s.logAudit(agentID, "kafka_timeseries_query", args, "", "success", result.Stats.Count, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

func (s *Server) handleTimeSeriesCompare(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_timeseries_compare",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		metric := timeseries.MetricType(request.GetString("metric", ""))
		source := request.GetString("source", "")
		baseWindow := request.GetString("base_window", "")
		compWindow := request.GetString("compare_window", "")

		if metric == "" || source == "" || baseWindow == "" || compWindow == "" {
			return mcp.NewToolResultError("metric, source, base_window, and compare_window are required"), nil
		}

		result, err := s.timeseriesEngine.Compare(metric, source, baseWindow, compWindow)
		if err != nil {
			return s.toolError("Comparison failed", err), nil
		}

		s.logAudit(agentID, "kafka_timeseries_compare", args, "", "success", 0, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

func (s *Server) handleTimeSeriesBaseline(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_timeseries_baseline",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		metric := timeseries.MetricType(request.GetString("metric", ""))
		source := request.GetString("source", "")
		baselineWindow := request.GetString("baseline_window", "7d")
		currentWindow := request.GetString("current_window", "1h")

		if metric == "" || source == "" {
			return mcp.NewToolResultError("metric and source are required"), nil
		}

		result, err := s.timeseriesEngine.Baseline(metric, source, baselineWindow, currentWindow)
		if err != nil {
			return s.toolError("Baseline analysis failed", err), nil
		}

		s.logAudit(agentID, "kafka_timeseries_baseline", args, "", "success", 0, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

func (s *Server) handleTimeSeriesList(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_timeseries_list",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		series := s.timeseriesEngine.ListSeries()
		windows := s.timeseriesEngine.GetWindows()

		s.logAudit(agentID, "kafka_timeseries_list", args, "", "success", len(series), nil, start)
		return marshalResult(map[string]any{
			"series":  series,
			"windows": windows,
			"total":   len(series),
		})
	})(ctx, request)
}
