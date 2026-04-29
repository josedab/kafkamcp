package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleFinOpsSummary(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_finops_summary",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "summary")

		switch action {
		case "summary":
			summary := s.finOpsDashboard.GetSummary()
			s.logAudit(agentID, "kafka_finops_summary", args, "", "success", 0, nil, start)
			return marshalResult(summary)

		case "snapshot":
			snap := s.finOpsDashboard.TakeSnapshot("default")
			s.logAudit(agentID, "kafka_finops_summary", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"snapshot": snap})

		case "recommendations":
			recs := s.finOpsDashboard.GetRightsizeRecommendations()
			var totalSavings float64
			for _, r := range recs {
				totalSavings += r.Savings
			}
			s.logAudit(agentID, "kafka_finops_summary", args, "", "success", len(recs), nil, start)
			return marshalResult(map[string]any{
				"recommendations": recs,
				"total":           len(recs),
				"total_savings":   totalSavings,
			})

		case "forecast":
			months := int(request.GetFloat("months", 6))
			forecast := s.finOpsDashboard.ForecastSpend(months)
			s.logAudit(agentID, "kafka_finops_summary", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"forecast": forecast})

		case "budgets":
			budgets := s.finOpsDashboard.GetBudgets()
			s.logAudit(agentID, "kafka_finops_summary", args, "", "success", len(budgets), nil, start)
			return marshalResult(map[string]any{"budgets": budgets, "total": len(budgets)})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; use: summary, snapshot, recommendations, forecast, budgets", action)), nil
		}
	})(ctx, request)
}

func (s *Server) handleFinOpsBudget(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_finops_budget",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		name := request.GetString("name", "")
		threshold := request.GetFloat("threshold", 0)

		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}

		alert, err := s.finOpsDashboard.SetBudget(name, threshold)
		if err != nil {
			s.logAudit(agentID, "kafka_finops_budget", args, "", "error", 0, err, start)
			return s.toolError("Failed to set budget", err), nil
		}

		s.logAudit(agentID, "kafka_finops_budget", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{
			"status": "budget_set",
			"alert":  alert,
		})
	})(ctx, request)
}

func (s *Server) handleFinOpsRecordCost(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_finops_record_cost",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		topic := request.GetString("topic", "")
		monthlyCost := request.GetFloat("monthly_cost", 0)
		storageGB := request.GetFloat("storage_gb", 0)
		partitions := int(request.GetFloat("partitions", 1))
		msgsPerSec := request.GetFloat("msgs_per_sec", 0)

		if topic == "" {
			return mcp.NewToolResultError("topic is required"), nil
		}
		if deny := validateStringLength("topic", topic); deny != nil {
			return deny, nil
		}

		s.finOpsDashboard.RecordTopicCost(topic, monthlyCost, storageGB, partitions, msgsPerSec)

		s.logAudit(agentID, "kafka_finops_record_cost", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{
			"status":  "recorded",
			"topic":   topic,
			"cost":    monthlyCost,
			"message": fmt.Sprintf("Cost data recorded for topic %q: $%.2f/month", topic, monthlyCost),
		})
	})(ctx, request)
}
