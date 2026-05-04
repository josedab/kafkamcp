package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerChaosGymnasiumTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_chaos_gymnasium",
		withCategory("training"),
		mcp.WithDescription("Gamified chaos engineering training for AI agents. Actions: list_challenges, get_challenge, start_challenge, complete_objective, finish_attempt, progress, leaderboard."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list_challenges, get_challenge, start_challenge, complete_objective, finish_attempt, progress, leaderboard")),
		mcp.WithString("challenge_id", mcp.Description("Challenge ID (for get_challenge, start_challenge)")),
		mcp.WithString("attempt_id", mcp.Description("Attempt ID (for complete_objective, finish_attempt)")),
		mcp.WithString("objective_id", mcp.Description("Objective ID (for complete_objective)")),
		mcp.WithString("difficulty", mcp.Description("Filter: beginner, intermediate, advanced, expert")),
		mcp.WithString("category", mcp.Description("Filter: infrastructure, consumer, producer, data, network")),
		mcp.WithNumber("limit", mcp.Description("Max results for leaderboard")),
	), s.handleChaosGymnasium)
}
