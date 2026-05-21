package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/replay"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleReplayRecord(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_replay_record", resourceType: auth.ResourceTopic, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")

		switch action {
		case "start":
			topic := request.GetString("topic", "")
			if topic == "" {
				return mcp.NewToolResultError("topic is required"), nil
			}
			name := request.GetString("name", "")
			cluster := request.GetString("cluster", "")
			maxMessages := int(request.GetFloat("max_messages", 1000))

			rec := s.replayEngine.CreateRecording(name, topic, cluster, maxMessages)
			s.logAudit(ctx, agentID, "kafka_replay_record", args, cluster, "success", 1, nil, start)
			return marshalResult(map[string]any{
				"recording": rec,
				"message":   "Recording started. Messages will be captured. Use action=stop to finish.",
			})

		case "stop":
			id := request.GetString("recording_id", "")
			if id == "" {
				return mcp.NewToolResultError("recording_id is required"), nil
			}
			rec, err := s.replayEngine.StopRecording(id)
			if err != nil {
				return s.toolError("failed to stop recording", err), nil
			}
			return marshalResult(map[string]any{"recording": rec, "message": "Recording stopped."})

		case "get":
			id := request.GetString("recording_id", "")
			if id == "" {
				return mcp.NewToolResultError("recording_id is required"), nil
			}
			rec, err := s.replayEngine.GetRecording(id)
			if err != nil {
				return s.toolError("recording not found", err), nil
			}
			return marshalResult(rec)

		default:
			topic := request.GetString("topic", "")
			recordings := s.replayEngine.ListRecordings(topic)
			return marshalResult(map[string]any{"recordings": recordings, "total": len(recordings)})
		}
	})(ctx, request)
}

func (s *Server) handleReplayRun(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_replay_run", resourceType: auth.ResourceTopic, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")

		switch action {
		case "create":
			recordingID := request.GetString("recording_id", "")
			if recordingID == "" {
				return mcp.NewToolResultError("recording_id is required"), nil
			}
			name := request.GetString("name", "")
			timeDilation := request.GetFloat("time_dilation", 1.0)
			sandboxID := request.GetString("sandbox_id", "")

			session, err := s.replayEngine.CreateReplaySession(name, recordingID, timeDilation, sandboxID, nil)
			if err != nil {
				return s.toolError("failed to create replay session", err), nil
			}
			return marshalResult(map[string]any{
				"session": session,
				"message": "Replay session created. Use action=run to execute.",
			})

		case "run":
			sessionID := request.GetString("session_id", "")
			if sessionID == "" {
				return mcp.NewToolResultError("session_id is required"), nil
			}
			session, err := s.replayEngine.RunReplaySession(sessionID)
			if err != nil {
				return s.toolError("replay failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_replay_run", args, "", "success", session.MessagesReplayed, nil, start)
			return marshalResult(session)

		case "get":
			sessionID := request.GetString("session_id", "")
			if sessionID == "" {
				return mcp.NewToolResultError("session_id is required"), nil
			}
			session, err := s.replayEngine.GetReplaySession(sessionID)
			if err != nil {
				return s.toolError("session not found", err), nil
			}
			return marshalResult(session)

		default:
			sessions := s.replayEngine.ListReplaySessions()
			return marshalResult(map[string]any{"sessions": sessions, "total": len(sessions)})
		}
	})(ctx, request)
}

func (s *Server) handleReplayRegression(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_replay_regression", resourceType: auth.ResourceTopic, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "create_suite")

		switch action {
		case "create_suite":
			recordingID := request.GetString("recording_id", "")
			if recordingID == "" {
				return mcp.NewToolResultError("recording_id is required"), nil
			}
			name := request.GetString("name", "")
			description := request.GetString("description", "")

			defaultAssertions := []replay.Assertion{
				{Name: "has-messages", Type: "message_count", Operator: "gt", Expected: "0"},
				{Name: "no-errors", Type: "no_errors", Expected: "true"},
			}

			suite, err := s.replayEngine.CreateRegressionSuite(name, description, recordingID, defaultAssertions)
			if err != nil {
				return s.toolError("failed to create suite", err), nil
			}
			return marshalResult(map[string]any{
				"suite":   suite,
				"message": "Regression suite created with default assertions. Use action=run_suite to execute.",
			})

		case "run_suite":
			suiteID := request.GetString("suite_id", "")
			if suiteID == "" {
				return mcp.NewToolResultError("suite_id is required"), nil
			}
			result, err := s.replayEngine.RunRegressionSuite(suiteID)
			if err != nil {
				return s.toolError("regression run failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_replay_regression", args, "", "success", result.TotalTests, nil, start)
			return marshalResult(result)

		default:
			return mcp.NewToolResultError("action must be: create_suite, run_suite"), nil
		}
	})(ctx, request)
}
