package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/streamsql"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleSQLOptimize(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_sql_optimize", resourceType: auth.ResourceTopic, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		sql := request.GetString("sql", "")
		if sql == "" {
			return mcp.NewToolResultError("sql is required"), nil
		}
		if deny := validateStringLength("sql", sql); deny != nil {
			return deny, nil
		}

		budgetID := request.GetString("budget_id", "")
		var budget *streamsql.Budget
		if budgetID != "" {
			b, err := s.streamSQLEngine.GetBudget(budgetID)
			if err != nil {
				return s.toolError("budget not found", err), nil
			}
			budget = b
		}

		plan, err := s.streamSQLEngine.OptimizeQuery(sql, nil, nil, budget)
		if err != nil {
			return s.toolError("optimization failed", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_sql_optimize", args, "", "success", 1, nil, start)
		return marshalResult(plan)
	})(ctx, request)
}

func (s *Server) handleSQLBudget(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_sql_budget", resourceType: auth.ResourceCluster, action: auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")

		switch action {
		case "create":
			name := request.GetString("name", "default")
			maxPerQuery := request.GetFloat("max_cost_per_query", 10.0)
			maxPerHour := request.GetFloat("max_cost_per_hour", 100.0)

			budget := s.streamSQLEngine.CreateBudget(name, maxPerQuery, maxPerHour, 0.8)
			s.logAudit(ctx, agentID, "kafka_sql_budget", args, "", "success", 1, nil, start)
			return marshalResult(budget)

		case "get":
			budgetID := request.GetString("budget_id", "")
			if budgetID == "" {
				return mcp.NewToolResultError("budget_id is required"), nil
			}
			budget, err := s.streamSQLEngine.GetBudget(budgetID)
			if err != nil {
				return s.toolError("budget not found", err), nil
			}
			return marshalResult(budget)

		default:
			budgets := s.streamSQLEngine.ListBudgets()
			return marshalResult(map[string]any{"budgets": budgets, "total": len(budgets)})
		}
	})(ctx, request)
}
