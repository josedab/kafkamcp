package server

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/replay"
)

func (s *Server) handleReplayCreate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_replay_create", resourceType: auth.ResourceTopic, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "create")
		topic := request.GetString("topic", "")
		cluster := request.GetString("cluster", "")

		switch action {
		case "create":
			if topic == "" {
				return mcp.NewToolResultError("topic is required for create action"), nil
			}
			if deny := validateStringLength("topic", topic); deny != nil {
				return deny, nil
			}

			name := request.GetString("name", fmt.Sprintf("bookmark-%s-%s", topic, time.Now().Format("20060102-150405")))

			// Get current offsets from topic
			detail, err := s.kafkaManager.DescribeTopic(ctx, cluster, topic)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_replay_create", args, cluster, "error", 0, err, start)
				return s.toolError("failed to describe topic", err), nil
			}

			offsets := make(map[int32]int64)
			for _, p := range detail.Partitions {
				offsets[p.ID] = p.LatestOffset
			}

			// Get sample messages
			msgs, _ := s.kafkaManager.Consume(ctx, cluster, topic, -1, "latest", 5, 5000, "auto")
			var samples []replay.SampleMessage
			for _, m := range msgs {
				valStr := fmt.Sprintf("%v", m.Value)
				hash := fmt.Sprintf("%x", sha256.Sum256([]byte(valStr)))
				samples = append(samples, replay.SampleMessage{
					Partition: m.Partition,
					Offset:    m.Offset,
					Key:       fmt.Sprintf("%v", m.Key),
					ValueHash: hash[:16],
					Size:      len(valStr),
					Timestamp: m.Timestamp,
				})
			}

			var tags []string
			if t, ok := args["tags"].([]any); ok {
				for _, v := range t {
					if s, ok := v.(string); ok {
						tags = append(tags, s)
					}
				}
			}

			bookmark := s.replayEngine.CreateBookmark(name, topic, s.resolveClusterName(cluster), offsets, samples, tags)
			s.logAudit(ctx, agentID, "kafka_replay_create", args, cluster, "success", 1, nil, start)
			return marshalResult(bookmark)

		case "list":
			bookmarks := s.replayEngine.ListBookmarks(topic)
			s.logAudit(ctx, agentID, "kafka_replay_create", args, cluster, "success", len(bookmarks), nil, start)
			return marshalResult(map[string]any{"bookmarks": bookmarks, "total": len(bookmarks)})

		case "get":
			bmID := request.GetString("bookmark_id", "")
			if bmID == "" {
				return mcp.NewToolResultError("bookmark_id is required for get action"), nil
			}
			if deny := validateStringLength("bookmark_id", bmID); deny != nil {
				return deny, nil
			}
			bm, err := s.replayEngine.GetBookmark(bmID)
			if err != nil {
				return s.toolError("bookmark not found", err), nil
			}
			return marshalResult(bm)

		case "delete":
			bmID := request.GetString("bookmark_id", "")
			if bmID == "" {
				return mcp.NewToolResultError("bookmark_id is required for delete action"), nil
			}
			if deny := validateStringLength("bookmark_id", bmID); deny != nil {
				return deny, nil
			}
			if err := s.replayEngine.DeleteBookmark(bmID); err != nil {
				return s.toolError("failed to delete bookmark", err), nil
			}
			return marshalResult(map[string]any{"status": "deleted", "bookmark_id": bmID})

		default:
			return mcp.NewToolResultError("action must be: create, list, get, delete"), nil
		}
	})(ctx, request)
}

func (s *Server) handleReplayDiff(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_replay_diff", resourceType: auth.ResourceTopic, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		bmA := request.GetString("bookmark_a", "")
		bmB := request.GetString("bookmark_b", "")

		if bmA == "" || bmB == "" {
			return mcp.NewToolResultError("bookmark_a and bookmark_b are required"), nil
		}
		if deny := validateStringLength("bookmark_a", bmA); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("bookmark_b", bmB); deny != nil {
			return deny, nil
		}

		diff, err := s.replayEngine.Diff(bmA, bmB)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_replay_diff", args, "", "error", 0, err, start)
			return s.toolError("diff failed", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_replay_diff", args, "", "success", 1, nil, start)
		return marshalResult(diff)
	})(ctx, request)
}

func (s *Server) handleReplaySimulate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_replay_simulate", resourceType: auth.ResourceTopic, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")

		switch action {
		case "create":
			topic := request.GetString("topic", "")
			groupID := request.GetString("group_id", "")
			fromBookmark := request.GetString("from_bookmark", "")
			name := request.GetString("name", "")

			if topic == "" {
				return mcp.NewToolResultError("topic is required"), nil
			}
			if groupID == "" {
				return mcp.NewToolResultError("group_id is required"), nil
			}
			if deny := validateStringLength("topic", topic); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("group_id", groupID); deny != nil {
				return deny, nil
			}
			if deny := validateKafkaIdentifier("group_id", groupID); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("from_bookmark", fromBookmark); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("name", name); deny != nil {
				return deny, nil
			}

			// Build start offsets
			startOffsets := make(map[int32]int64)
			if fromBookmark != "" {
				bm, err := s.replayEngine.GetBookmark(fromBookmark)
				if err != nil {
					return s.toolError("bookmark not found", err), nil
				}
				startOffsets = bm.Offsets
			}

			sim := s.replayEngine.CreateSimulation(name, topic, groupID, startOffsets, fromBookmark)
			s.logAudit(ctx, agentID, "kafka_replay_simulate", args, "", "success", 1, nil, start)
			return marshalResult(sim)

		case "run":
			simID := request.GetString("simulation_id", "")
			if simID == "" {
				return mcp.NewToolResultError("simulation_id is required"), nil
			}
			if deny := validateStringLength("simulation_id", simID); deny != nil {
				return deny, nil
			}
			sim, err := s.replayEngine.RunSimulation(simID)
			if err != nil {
				return s.toolError("run failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_replay_simulate", args, "", "success", 1, nil, start)
			return marshalResult(sim)

		case "get":
			simID := request.GetString("simulation_id", "")
			if simID == "" {
				return mcp.NewToolResultError("simulation_id is required"), nil
			}
			sim, err := s.replayEngine.GetSimulation(simID)
			if err != nil {
				return s.toolError("simulation not found", err), nil
			}
			return marshalResult(sim)

		case "list":
			topic := request.GetString("topic", "")
			sims := s.replayEngine.ListSimulations(topic)
			return marshalResult(map[string]any{"simulations": sims, "total": len(sims)})

		default:
			return mcp.NewToolResultError("action must be: create, run, get, list"), nil
		}
	})(ctx, request)
}
