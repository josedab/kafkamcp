package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/forecast"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleGroupHealth(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_group_health", resourceType: auth.ResourceConsumerGroup, action: auth.ActionRead,
		resourceName: func(r mcp.CallToolRequest) string { return r.GetString("group_id", "") },
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		groupID := request.GetString("group_id", "")
		cluster := request.GetString("cluster", "")

		if groupID == "" {
			return mcp.NewToolResultError("group_id is required"), nil
		}
		if deny := validateStringLength("group_id", groupID); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}

		groupDetail, err := s.kafkaManager.DescribeConsumerGroup(ctx, cluster, groupID)
		if err != nil {
			s.logAudit(agentID, "kafka_group_health", args, cluster, "error", 0, err, start)
			return s.toolError("failed to describe consumer group", err), nil
		}

		health := s.selfHealMon.EvaluateHealth(
			groupID,
			groupDetail.TotalLag,
			len(groupDetail.Members),
			0, // commit rate not directly available
			0, // rebalance count not directly available
		)

		s.logAudit(agentID, "kafka_group_health", args, cluster, "success", 1, nil, start)
		return marshalResult(health)
	})(ctx, request)
}

func (s *Server) handlePersona(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_persona", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		personaName := request.GetString("persona", "")

		if deny := validateStringLength("action", action); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("persona", personaName); deny != nil {
			return deny, nil
		}

		switch action {
		case "list":
			personas := s.personaEngine.ListPersonas()
			active := s.personaEngine.GetActive()
			var activeName string
			if active != nil {
				activeName = active.Name
			}
			s.logAudit(agentID, "kafka_persona", args, "", "success", len(personas), nil, start)
			return marshalResult(map[string]any{"personas": personas, "active": activeName, "total": len(personas)})

		case "get":
			if personaName == "" {
				return mcp.NewToolResultError("persona name is required"), nil
			}
			p, err := s.personaEngine.GetPersona(personaName)
			if err != nil {
				return s.toolError("persona not found", err), nil
			}
			s.logAudit(agentID, "kafka_persona", args, "", "success", 1, nil, start)
			return marshalResult(p)

		case "activate":
			if personaName == "" {
				return mcp.NewToolResultError("persona name is required"), nil
			}
			if err := s.personaEngine.SetActive(personaName); err != nil {
				return s.toolError("persona activation failed", err), nil
			}
			s.logAudit(agentID, "kafka_persona", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "activated", "persona": personaName})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: list, get, activate", action)), nil
		}
	})(ctx, request)
}

func (s *Server) handleForecast(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_forecast", resourceType: auth.ResourceTopic, action: auth.ActionRead,
		resourceName: func(r mcp.CallToolRequest) string { return r.GetString("topic", "") },
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		topic := request.GetString("topic", "")
		metricName := request.GetString("metric", "throughput")
		horizonDays := intFromAny(args["horizon_days"], 7)
		cluster := request.GetString("cluster", "")

		if topic == "" {
			return mcp.NewToolResultError("topic is required"), nil
		}
		if deny := validateStringLength("topic", topic); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("metric", metricName); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}

		// Record current metrics for the topic
		detail, err := s.kafkaManager.DescribeTopic(ctx, cluster, topic)
		if err != nil {
			s.logAudit(agentID, "kafka_forecast", args, cluster, "error", 0, err, start)
			return s.toolError("failed to describe topic", err), nil
		}

		throughput := estimateThroughput(detail)
		s.forecaster.RecordMetric(forecast.Metric{
			Topic:      topic,
			MetricName: metricName,
			Value:      throughput,
			Timestamp:  time.Now(),
		})

		result, err := s.forecaster.ForecastTopic(topic, metricName, horizonDays)
		if err != nil {
			s.logAudit(agentID, "kafka_forecast", args, cluster, "error", 0, err, start)
			return s.toolError("forecast failed", err), nil
		}

		s.logAudit(agentID, "kafka_forecast", args, cluster, "success", 1, nil, start)
		return marshalResult(result)
	})(ctx, request)
}
