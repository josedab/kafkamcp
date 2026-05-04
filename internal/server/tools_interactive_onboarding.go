package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/onboarding"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleInteractiveOnboarding(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_onboard_interactive", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		sessionID := request.GetString("session_id", "")

		switch action {
		case "start":
			session := s.onboardingEngine.Interactive().StartSession(ctx, agentID)
			s.logAudit(agentID, "kafka_onboard_interactive", args, "", "success", 1, nil, start)

			firstStep := session.Steps[0]
			help := onboarding.GenerateContextualHelp(firstStep, session.Context)
			return marshalResult(map[string]any{
				"session":  session,
				"guidance": help,
				"message":  "Welcome to KafkaMCP! Let's get you set up. Follow the steps below.",
			})

		case "current_step":
			if sessionID == "" {
				return mcp.NewToolResultError("session_id is required"), nil
			}
			step, err := s.onboardingEngine.Interactive().GetCurrentStep(sessionID)
			if err != nil {
				return s.toolError("failed to get current step", err), nil
			}
			session, _ := s.onboardingEngine.Interactive().GetSession(sessionID)
			help := onboarding.GenerateContextualHelp(*step, session.Context)
			s.logAudit(agentID, "kafka_onboard_interactive", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"step": step, "guidance": help})

		case "advance":
			if sessionID == "" {
				return mcp.NewToolResultError("session_id is required"), nil
			}
			result := request.GetString("result", "completed")
			advResult, err := s.onboardingEngine.Interactive().AdvanceStep(sessionID, result)
			if err != nil {
				return s.toolError("failed to advance step", err), nil
			}
			s.logAudit(agentID, "kafka_onboard_interactive", args, "", "success", 1, nil, start)
			return marshalResult(advResult)

		case "skip":
			if sessionID == "" {
				return mcp.NewToolResultError("session_id is required"), nil
			}
			skipResult, err := s.onboardingEngine.Interactive().SkipStep(sessionID)
			if err != nil {
				return s.toolError("failed to skip step", err), nil
			}
			s.logAudit(agentID, "kafka_onboard_interactive", args, "", "success", 1, nil, start)
			return marshalResult(skipResult)

		case "progress":
			if sessionID == "" {
				return mcp.NewToolResultError("session_id is required"), nil
			}
			progress := s.onboardingEngine.Interactive().GetProgress(sessionID)
			s.logAudit(agentID, "kafka_onboard_interactive", args, "", "success", 1, nil, start)
			return marshalResult(progress)

		case "help":
			if sessionID == "" {
				return mcp.NewToolResultError("session_id is required"), nil
			}
			step, err := s.onboardingEngine.Interactive().GetCurrentStep(sessionID)
			if err != nil {
				return s.toolError("failed to get help", err), nil
			}
			session, _ := s.onboardingEngine.Interactive().GetSession(sessionID)
			help := onboarding.GenerateContextualHelp(*step, session.Context)
			s.logAudit(agentID, "kafka_onboard_interactive", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"help": help, "step": step})

		default:
			return mcp.NewToolResultError("action must be: start, current_step, advance, skip, progress, help"), nil
		}
	})(ctx, request)
}
