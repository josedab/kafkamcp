package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/mark3labs/mcp-go/mcp"
)

// ===================== STREAMING TOOL HANDLERS =====================

// handleStream manages streaming consumer sessions: start, poll, control, list.
func (s *Server) handleStream(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	args := request.GetArguments()
	action := request.GetString("action", "start")

	if deny := s.checkRateLimit(agentID, "kafka_stream"); deny != nil {
		return deny, nil
	}

	switch action {
	case "start":
		return s.handleStreamStart(ctx, request, agentID, args, start)
	case "poll":
		return s.handleStreamPoll(ctx, request, agentID, args, start)
	case "pause", "resume", "stop":
		return s.handleStreamControl(ctx, request, agentID, args, start, action)
	case "list":
		return s.handleStreamList(agentID, args, start)
	default:
		return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: start, poll, pause, resume, stop, list", action)), nil
	}
}

func (s *Server) handleStreamStart(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	topic := request.GetString("topic", "")
	cluster := request.GetString("cluster", "")

	if topic == "" {
		return mcp.NewToolResultError("topic is required"), nil
	}
	if deny := validateStringLength("topic", topic); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_stream", auth.ResourceTopic, topic, auth.ActionRead); deny != nil {
		return deny, nil
	}

	groupID := request.GetString("group_id", "")
	if deny := validateStringLength("group_id", groupID); deny != nil {
		return deny, nil
	}

	cfg := kafka.StreamConfig{
		Topic:        topic,
		GroupID:      groupID,
		BatchSize:    request.GetInt("batch_size", 10),
		MaxMsgPerSec: request.GetInt("max_msg_per_sec", 1000),
		MaxMessages:  request.GetInt("max_messages", 1000),
		TimeoutMs:    request.GetInt("timeout_ms", 30000),
		Deserializer: request.GetString("deserializer", "auto"),
		AutoCommit:   request.GetBool("auto_commit", true),
	}

	result, err := s.kafkaManager.StartStream(ctx, cluster, cfg)
	if err != nil {
		s.logAudit(agentID, "kafka_stream", args, cluster, "error", 0, err, start)
		return s.toolError("Failed to start stream", err), nil
	}

	s.logAudit(agentID, "kafka_stream", args, cluster, "success", 0, nil, start)
	return marshalResult(result)
}

func (s *Server) handleStreamPoll(_ context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	sessionID := request.GetString("session_id", "")
	if sessionID == "" {
		return mcp.NewToolResultError("session_id is required for poll action"), nil
	}

	timeoutMs := request.GetInt("poll_timeout_ms", 5000)
	result, err := s.kafkaManager.PollStream(sessionID, timeoutMs)
	if err != nil {
		s.logAudit(agentID, "kafka_stream", args, "", "error", 0, err, start)
		return s.toolError("Failed to poll stream", err), nil
	}

	s.logAudit(agentID, "kafka_stream", args, "", "success", result.Count, nil, start)
	return marshalResult(result)
}

func (s *Server) handleStreamControl(_ context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time, action string) (*mcp.CallToolResult, error) {
	sessionID := request.GetString("session_id", "")
	if sessionID == "" {
		return mcp.NewToolResultError("session_id is required"), nil
	}

	result, err := s.kafkaManager.ControlStream(sessionID, action)
	if err != nil {
		s.logAudit(agentID, "kafka_stream", args, "", "error", 0, err, start)
		return s.toolError(fmt.Sprintf("Failed to %s stream", action), err), nil
	}

	s.logAudit(agentID, "kafka_stream", args, "", "success", 0, nil, start)
	return marshalResult(result)
}

func (s *Server) handleStreamList(agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	sessions := s.kafkaManager.ListStreams()
	s.logAudit(agentID, "kafka_stream", args, "", "success", len(sessions), nil, start)
	return marshalResult(map[string]any{
		"sessions":    sessions,
		"total_count": len(sessions),
	})
}
