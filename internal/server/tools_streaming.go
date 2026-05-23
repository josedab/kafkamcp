package server

import (
	"context"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// ===================== STREAMING TOOL HANDLERS =====================

// handleStream manages streaming consumer sessions: start, poll, control, list.
func (s *Server) handleStream(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_stream", resourceType: auth.ResourceTopic, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "start")

		switch action {
		case "start":
			return s.handleStreamStart(ctx, request, agentID, args, start)
		case "poll":
			return s.handleStreamPoll(ctx, request, agentID, args, start)
		case "pause", "resume", "stop":
			return s.handleStreamControl(ctx, request, agentID, args, start, action)
		case "list":
			return s.handleStreamList(ctx, agentID, args, start)
		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: start, poll, pause, resume, stop, list", action)), nil
		}
	})(ctx, request)
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
	// Intentional secondary auth check: the outer withToolAuth validates general stream tool
	// access, but we also need to authorize the specific topic the agent wants to stream from.
	if deny := s.checkAuth(agentID, "kafka_stream", auth.ResourceTopic, topic, auth.ActionRead); deny != nil {
		return deny, nil
	}

	groupID := request.GetString("group_id", "")
	if deny := validateStringLength("group_id", groupID); deny != nil {
		return deny, nil
	}
	if deny := validateKafkaIdentifier("group_id", groupID); deny != nil {
		return deny, nil
	}

	cfg := kafka.StreamConfig{
		Topic:        topic,
		GroupID:      groupID,
		BatchSize:    int(clampInt64(int64(request.GetInt("batch_size", 10)), 1, 100)),
		MaxMsgPerSec: int(clampInt64(int64(request.GetInt("max_msg_per_sec", 1000)), 1, 10000)),
		MaxMessages:  int(clampInt64(int64(request.GetInt("max_messages", 1000)), 1, 10000)),
		TimeoutMs:    int(clampInt64(int64(request.GetInt("timeout_ms", 30000)), 1000, 60000)),
		Deserializer: request.GetString("deserializer", "auto"),
		AutoCommit:   request.GetBool("auto_commit", true),
		AgentID:      agentID,
	}

	result, err := s.kafkaManager.StartStream(ctx, cluster, cfg)
	if err != nil {
		s.logAudit(ctx, agentID, "kafka_stream", args, cluster, "error", 0, err, start)
		return s.toolError("Failed to start stream", err), nil
	}

	s.logAudit(ctx, agentID, "kafka_stream", args, cluster, "success", 0, nil, start)
	return marshalResult(result)
}

func (s *Server) handleStreamPoll(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	sessionID := request.GetString("session_id", "")
	if sessionID == "" {
		return mcp.NewToolResultError("session_id is required for poll action"), nil
	}
	if deny := validateStringLength("session_id", sessionID); deny != nil {
		return deny, nil
	}

	// Verify session ownership
	session, ok := s.kafkaManager.GetStreamSession(sessionID)
	if !ok {
		return mcp.NewToolResultError("stream session not found"), nil
	}
	if session.AgentID != "" && session.AgentID != agentID {
		return mcp.NewToolResultError("access denied: session belongs to another agent"), nil
	}

	timeoutMs := request.GetInt("poll_timeout_ms", 5000)
	if timeoutMs < 100 {
		timeoutMs = 100
	}
	if timeoutMs > 60000 {
		timeoutMs = 60000
	}
	result, err := s.kafkaManager.PollStream(sessionID, timeoutMs)
	if err != nil {
		s.logAudit(ctx, agentID, "kafka_stream", args, "", "error", 0, err, start)
		return s.toolError("Failed to poll stream", err), nil
	}

	s.logAudit(ctx, agentID, "kafka_stream", args, "", "success", result.Count, nil, start)
	return marshalResult(result)
}

func (s *Server) handleStreamControl(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time, action string) (*mcp.CallToolResult, error) {
	sessionID := request.GetString("session_id", "")
	if sessionID == "" {
		return mcp.NewToolResultError("session_id is required"), nil
	}
	if deny := validateStringLength("session_id", sessionID); deny != nil {
		return deny, nil
	}

	// Verify session ownership
	session, ok := s.kafkaManager.GetStreamSession(sessionID)
	if !ok {
		return mcp.NewToolResultError("stream session not found"), nil
	}
	if session.AgentID != "" && session.AgentID != agentID {
		return mcp.NewToolResultError("access denied: session belongs to another agent"), nil
	}

	result, err := s.kafkaManager.ControlStream(sessionID, action)
	if err != nil {
		s.logAudit(ctx, agentID, "kafka_stream", args, "", "error", 0, err, start)
		return s.toolError(fmt.Sprintf("Failed to %s stream", action), err), nil
	}

	s.logAudit(ctx, agentID, "kafka_stream", args, "", "success", 0, nil, start)
	return marshalResult(result)
}

func (s *Server) handleStreamList(ctx context.Context, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	allSessions := s.kafkaManager.ListStreams()
	// Filter sessions to only those owned by the calling agent
	sessions := make([]kafka.StreamSessionSnapshot, 0, len(allSessions))
	for i := range allSessions {
		sess := allSessions[i]
		if sess.AgentID == "" || sess.AgentID == agentID {
			sessions = append(sessions, sess)
		}
	}
	s.logAudit(ctx, agentID, "kafka_stream", args, "", "success", len(sessions), nil, start)
	return marshalResult(map[string]any{
		"sessions":    sessions,
		"total_count": len(sessions),
	})
}
