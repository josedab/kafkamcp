package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/forecast"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleCapacityPlan(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_capacity_plan",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		horizonDays := intFromAny(args["horizon_days"], 30)
		if horizonDays < 1 {
			horizonDays = 1
		} else if horizonDays > 365 {
			horizonDays = 365
		}
		maxStorageGB := request.GetFloat("max_storage_gb", 1000)
		if maxStorageGB <= 0 {
			maxStorageGB = 1000
		}
		cluster := request.GetString("cluster", "")
		resolvedCluster := s.resolveClusterName(cluster)

		plan := s.forecaster.GenerateCapacityPlan(resolvedCluster, horizonDays, maxStorageGB)

		s.logAudit(ctx, agentID, "kafka_capacity_plan", args, cluster, "success", len(plan.Topics), nil, start)
		return marshalResult(plan)
	})(ctx, request)
}

func (s *Server) handleCapacityForecast(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_capacity_forecast",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionRead,
		resourceName: func(r mcp.CallToolRequest) string { return r.GetString("topic", "") },
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		topic := request.GetString("topic", "")
		metric := request.GetString("metric", "throughput")
		horizonDays := intFromAny(args["horizon_days"], 7)
		if horizonDays < 1 {
			horizonDays = 1
		} else if horizonDays > 365 {
			horizonDays = 365
		}
		cluster := request.GetString("cluster", "")

		if topic == "" {
			return mcp.NewToolResultError("topic is required"), nil
		}
		if deny := validateStringLength("topic", topic); deny != nil {
			return deny, nil
		}

		fc, err := s.forecaster.ForecastTopic(topic, metric, horizonDays)
		if err != nil {
			history := s.forecaster.GetMetricHistory(topic, metric)
			if len(history) < 2 {
				s.logAudit(ctx, agentID, "kafka_capacity_forecast", args, cluster, "error", 0, err, start)
				return s.toolError("Insufficient data for forecast", err), nil
			}
			values := make([]float64, len(history))
			for i, h := range history {
				values[i] = h.Value
			}
			smoothed := forecast.ExponentialSmoothing(values, 0.3, horizonDays)
			result := map[string]any{
				"topic":            topic,
				"metric":           metric,
				"method":           "exponential_smoothing",
				"current_value":    values[len(values)-1],
				"predicted_values": smoothed,
				"data_points":      len(history),
			}
			s.logAudit(ctx, agentID, "kafka_capacity_forecast", args, cluster, "success", 0, nil, start)
			return marshalResult(result)
		}

		s.logAudit(ctx, agentID, "kafka_capacity_forecast", args, cluster, "success", 0, nil, start)
		return marshalResult(fc)
	})(ctx, request)
}
