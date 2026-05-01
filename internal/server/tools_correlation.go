package server

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/correlation"
	"github.com/kafkamcp/kafkamcp/internal/urlutil"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleCorrelationIngest(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_correlation_ingest",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		source := correlation.SignalSource(request.GetString("source", "custom"))
		metric := request.GetString("metric", "")
		valueStr := request.GetString("value", "0")
		severity := request.GetString("severity", "info")
		message := request.GetString("message", "")

		if metric == "" {
			return mcp.NewToolResultError("metric is required"), nil
		}

		value, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid value: %s", valueStr)), nil
		}

		signal := correlation.Signal{
			Source:   source,
			Metric:   metric,
			Value:    value,
			Severity: severity,
			Message:  message,
		}

		s.correlationEngine.IngestSignal(signal)

		s.logAudit(agentID, "kafka_correlation_ingest", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{
			"status": "ingested",
			"source": source,
			"metric": metric,
			"value":  value,
		})
	})(ctx, request)
}

func (s *Server) handleCorrelationAnalyze(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_correlation_analyze",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		window := request.GetString("window", "1h")
		metric := request.GetString("metric", "")

		var duration time.Duration
		switch window {
		case "6h":
			duration = 6 * time.Hour
		case "24h":
			duration = 24 * time.Hour
		default:
			duration = time.Hour
		}

		result := s.correlationEngine.Correlate(duration, metric)

		s.logAudit(agentID, "kafka_correlation_analyze", args, "", "success", result.SignalCount, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

func (s *Server) handleCorrelationEndpoints(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_correlation_endpoints",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")

		switch action {
		case "register":
			source := correlation.SignalSource(request.GetString("source", ""))
			url := request.GetString("url", "")
			if source == "" || url == "" {
				return mcp.NewToolResultError("source and url are required"), nil
			}
			if deny := validateStringLength("source", string(source)); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("url", url); deny != nil {
				return deny, nil
			}

			if err := urlutil.ValidateWebhookURL(url); err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("invalid endpoint URL: %v", err)), nil
			}

			s.correlationEngine.RegisterEndpoint(correlation.ExternalEndpoint{
				Source: source,
				URL:    url,
				Active: true,
			})

			s.logAudit(agentID, "kafka_correlation_endpoints", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "registered", "source": source})

		default:
			endpoints := s.correlationEngine.ListEndpoints()
			s.logAudit(agentID, "kafka_correlation_endpoints", args, "", "success", len(endpoints), nil, start)
			return marshalResult(map[string]any{"endpoints": endpoints, "total": len(endpoints)})
		}
	})(ctx, request)
}
