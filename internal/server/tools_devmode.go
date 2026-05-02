package server

import (
	"context"
	"strconv"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleDevEnvironment(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_dev_environment",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		env := s.devModeEngine.GetDevEnvironment()
		s.logAudit(agentID, "kafka_dev_environment", args, "", "success", 0, nil, start)
		return marshalResult(env)
	})(ctx, request)
}

func (s *Server) handleDevSampleData(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_dev_sample_data",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		generator := request.GetString("generator", "ecommerce-orders")
		countStr := request.GetString("count", "10")

		if deny := validateStringLength("generator", generator); deny != nil {
			return deny, nil
		}

		count, err := strconv.Atoi(countStr)
		if err != nil {
			count = 10
		}

		messages := s.devModeEngine.GenerateSampleData(generator, count)

		s.logAudit(agentID, "kafka_dev_sample_data", args, "", "success", len(messages), nil, start)
		return marshalResult(map[string]any{
			"generator": generator,
			"messages":  messages,
			"count":     len(messages),
		})
	})(ctx, request)
}

func (s *Server) handleDevTutorial(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_dev_tutorial",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")
		tutorialID := request.GetString("tutorial_id", "")
		difficulty := request.GetString("difficulty", "")

		if deny := validateStringLength("tutorial_id", tutorialID); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("difficulty", difficulty); deny != nil {
			return deny, nil
		}

		switch action {
		case "get":
			if tutorialID == "" {
				return mcp.NewToolResultError("tutorial_id is required"), nil
			}
			tut, err := s.devModeEngine.GetTutorial(tutorialID)
			if err != nil {
				return s.toolError("Tutorial not found", err), nil
			}
			s.logAudit(agentID, "kafka_dev_tutorial", args, "", "success", len(tut.Steps), nil, start)
			return marshalResult(map[string]any{"tutorial": tut})

		default:
			tutorials := s.devModeEngine.ListTutorials(difficulty)
			s.logAudit(agentID, "kafka_dev_tutorial", args, "", "success", len(tutorials), nil, start)
			return marshalResult(map[string]any{"tutorials": tutorials, "total": len(tutorials)})
		}
	})(ctx, request)
}
