package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleChaosGymnasium(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_chaos_gymnasium", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")

		switch action {
		case "list_challenges":
			difficulty := request.GetString("difficulty", "")
			category := request.GetString("category", "")
			challenges := s.chaosEngine.Gymnasium().ListChallenges(difficulty, category)
			s.logAudit(ctx, agentID, "kafka_chaos_gymnasium", args, "", "success", len(challenges), nil, start)
			return marshalResult(map[string]any{"challenges": challenges, "total": len(challenges)})

		case "get_challenge":
			challengeID := request.GetString("challenge_id", "")
			if challengeID == "" {
				return mcp.NewToolResultError("challenge_id is required"), nil
			}
			ch, err := s.chaosEngine.Gymnasium().GetChallenge(challengeID)
			if err != nil {
				return s.toolError("challenge not found", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_chaos_gymnasium", args, "", "success", 1, nil, start)
			return marshalResult(ch)

		case "start_challenge":
			challengeID := request.GetString("challenge_id", "")
			if challengeID == "" {
				return mcp.NewToolResultError("challenge_id is required"), nil
			}
			attempt, err := s.chaosEngine.Gymnasium().StartChallenge(challengeID, agentID)
			if err != nil {
				return s.toolError("failed to start challenge", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_chaos_gymnasium", args, "", "success", 1, nil, start)
			return marshalResult(attempt)

		case "complete_objective":
			attemptID := request.GetString("attempt_id", "")
			objectiveID := request.GetString("objective_id", "")
			if attemptID == "" || objectiveID == "" {
				return mcp.NewToolResultError("attempt_id and objective_id are required"), nil
			}
			attempt, err := s.chaosEngine.Gymnasium().CompleteObjective(attemptID, objectiveID)
			if err != nil {
				return s.toolError("failed to complete objective", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_chaos_gymnasium", args, "", "success", 1, nil, start)
			return marshalResult(attempt)

		case "finish_attempt":
			attemptID := request.GetString("attempt_id", "")
			if attemptID == "" {
				return mcp.NewToolResultError("attempt_id is required"), nil
			}
			attempt, err := s.chaosEngine.Gymnasium().FinishAttempt(attemptID)
			if err != nil {
				return s.toolError("failed to finish attempt", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_chaos_gymnasium", args, "", "success", 1, nil, start)
			return marshalResult(attempt)

		case "progress":
			progress := s.chaosEngine.Gymnasium().GetProgress(agentID)
			s.logAudit(ctx, agentID, "kafka_chaos_gymnasium", args, "", "success", 1, nil, start)
			return marshalResult(progress)

		case "leaderboard":
			limit := 20
			if l, ok := args["limit"].(float64); ok && l > 0 {
				limit = int(l)
			}
			lb := s.chaosEngine.Gymnasium().GetLeaderboard(limit)
			s.logAudit(ctx, agentID, "kafka_chaos_gymnasium", args, "", "success", len(lb.Entries), nil, start)
			return marshalResult(lb)

		default:
			return mcp.NewToolResultError("action must be: list_challenges, get_challenge, start_challenge, complete_objective, finish_attempt, progress, leaderboard"), nil
		}
	})(ctx, request)
}
