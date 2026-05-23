package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
)

func (s *Server) handlePredictiveCapacityForecast(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_capacity_forecast", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		cluster := request.GetString("cluster", "")
		cluster = s.resolveClusterName(cluster)

		horizonDays := 30
		if h, ok := args["horizon_days"].(float64); ok && h > 0 {
			horizonDays = int(h)
		}
		if horizonDays > 365 {
			horizonDays = 365
		}

		currentBrokers := 3
		if b, ok := args["current_brokers"].(float64); ok && b > 0 {
			currentBrokers = int(b)
		}

		maxStorageGB := 1000.0
		if s, ok := args["max_storage_gb"].(float64); ok && s > 0 {
			maxStorageGB = s
		}

		costPerGB := 0.10
		if c, ok := args["cost_per_gb"].(float64); ok && c > 0 {
			costPerGB = c
		}

		costPerBroker := 500.0
		if c, ok := args["cost_per_broker"].(float64); ok && c > 0 {
			costPerBroker = c
		}

		forecast := s.forecaster.PredictiveCapacityPlan(cluster, horizonDays, currentBrokers, maxStorageGB, costPerGB, costPerBroker)
		s.logAudit(ctx, agentID, "kafka_capacity_forecast", args, cluster, "success", len(forecast.TopicForecasts), nil, start)
		return marshalResult(forecast)
	})(ctx, request)
}
