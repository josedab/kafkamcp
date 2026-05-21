package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/digitaltwin"
	"github.com/kafkamcp/kafkamcp/internal/migration"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleAutopilotAssess(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_autopilot_assess",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		sourceCluster := request.GetString("source_cluster", "")
		targetCluster := request.GetString("target_cluster", "")

		if deny := validateStringLength("source_cluster", sourceCluster); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("target_cluster", targetCluster); deny != nil {
			return deny, nil
		}

		// Collect topic info from both clusters
		sourceTopics := s.collectTopicInfoForMigration(ctx, sourceCluster)
		targetTopics := s.collectTopicInfoForMigration(ctx, targetCluster)

		// Collect consumer groups
		var groups []string
		groupList, err := s.kafkaManager.ListConsumerGroups(ctx, sourceCluster)
		if err == nil {
			for _, g := range groupList {
				groups = append(groups, g.GroupID)
			}
		}

		session, err := s.autopilotEngine.Assess(sourceCluster, targetCluster, sourceTopics, targetTopics, groups)
		if err != nil {
			return s.toolError("assessment failed", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_autopilot_assess", args, sourceCluster, "success", 1, nil, start)
		return marshalResult(map[string]any{
			"session":    session,
			"message":    "Assessment complete. Use kafka_autopilot_plan with session_id to generate a migration plan.",
		})
	})(ctx, request)
}

func (s *Server) handleAutopilotPlan(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_autopilot_plan",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		sessionID := request.GetString("session_id", "")
		if sessionID == "" {
			return mcp.NewToolResultError("session_id is required"), nil
		}
		if deny := validateStringLength("session_id", sessionID); deny != nil {
			return deny, nil
		}

		// Get the session to find source/target clusters
		session, err := s.autopilotEngine.GetSession(sessionID)
		if err != nil {
			return s.toolError("session not found", err), nil
		}

		sourceTopics := s.collectTopicInfoForMigration(ctx, session.SourceCluster)
		targetTopics := s.collectTopicInfoForMigration(ctx, session.TargetCluster)

		session, err = s.autopilotEngine.GeneratePlan(sessionID, sourceTopics, targetTopics)
		if err != nil {
			return s.toolError("plan generation failed", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_autopilot_plan", args, "", "success", 1, nil, start)
		return marshalResult(map[string]any{
			"session": session,
			"message": "Migration plan generated. Use kafka_autopilot_dryrun to simulate, or kafka_autopilot_execute to run.",
		})
	})(ctx, request)
}

func (s *Server) handleAutopilotDryRun(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_autopilot_dryrun",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		sessionID := request.GetString("session_id", "")
		if sessionID == "" {
			return mcp.NewToolResultError("session_id is required"), nil
		}
		if deny := validateStringLength("session_id", sessionID); deny != nil {
			return deny, nil
		}

		session, err := s.autopilotEngine.GetSession(sessionID)
		if err != nil {
			return s.toolError("session not found", err), nil
		}

		// Build twin state from source cluster
		topics := make(map[string]digitaltwin.TopicState)
		topicList, listErr := s.kafkaManager.ListTopics(ctx, session.SourceCluster, false)
		if listErr == nil {
			for _, t := range topicList {
				topics[t.Name] = digitaltwin.TopicState{
					Name:              t.Name,
					Partitions:        t.Partitions,
					ReplicationFactor: t.ReplicationFactor,
					Configs:           make(map[string]string),
				}
			}
		}

		groups := make(map[string]digitaltwin.ConsumerGroupState)
		groupList, groupErr := s.kafkaManager.ListConsumerGroups(ctx, session.SourceCluster)
		if groupErr == nil {
			for _, g := range groupList {
				groups[g.GroupID] = digitaltwin.ConsumerGroupState{
					GroupID: g.GroupID,
					State:   g.State,
					Members: g.MemberCount,
				}
			}
		}

		session, err = s.autopilotEngine.DryRun(sessionID, topics, groups, nil)
		if err != nil {
			return s.toolError("dry-run failed", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_autopilot_dryrun", args, "", "success", 1, nil, start)
		return marshalResult(map[string]any{
			"session": session,
			"message": "Dry-run complete. Review results and use kafka_autopilot_execute to proceed.",
		})
	})(ctx, request)
}

func (s *Server) handleAutopilotExecute(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_autopilot_execute",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		sessionID := request.GetString("session_id", "")
		if sessionID == "" {
			return mcp.NewToolResultError("session_id is required"), nil
		}
		if deny := validateStringLength("session_id", sessionID); deny != nil {
			return deny, nil
		}

		dryRun := request.GetBool("dry_run", false)

		session, err := s.autopilotEngine.Execute(sessionID, dryRun)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_autopilot_execute", args, "", "error", 0, err, start)
			return s.toolError("execution failed", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_autopilot_execute", args, "", "success", 1, nil, start)
		return marshalResult(map[string]any{
			"session": session,
			"message": "Migration execution complete.",
		})
	})(ctx, request)
}

func (s *Server) handleAutopilotRollback(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_autopilot_rollback",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		sessionID := request.GetString("session_id", "")
		if sessionID == "" {
			return mcp.NewToolResultError("session_id is required"), nil
		}

		session, err := s.autopilotEngine.Rollback(sessionID)
		if err != nil {
			return s.toolError("rollback failed", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_autopilot_rollback", args, "", "success", 1, nil, start)
		return marshalResult(map[string]any{
			"session": session,
			"message": "Migration rolled back successfully.",
		})
	})(ctx, request)
}

func (s *Server) handleAutopilotStatus(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_autopilot_status",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		sessionID := request.GetString("session_id", "")

		if sessionID != "" {
			session, err := s.autopilotEngine.GetSession(sessionID)
			if err != nil {
				return s.toolError("session not found", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_autopilot_status", args, "", "success", 1, nil, start)
			return marshalResult(session)
		}

		sessions := s.autopilotEngine.ListSessions()
		s.logAudit(ctx, agentID, "kafka_autopilot_status", args, "", "success", len(sessions), nil, start)
		return marshalResult(map[string]any{
			"sessions": sessions,
			"total":    len(sessions),
		})
	})(ctx, request)
}

// collectTopicInfoForMigration gathers topic info for migration planning.
func (s *Server) collectTopicInfoForMigration(ctx context.Context, cluster string) []migration.TopicInfo {
	topicList, err := s.kafkaManager.ListTopics(ctx, cluster, false)
	if err != nil {
		return nil
	}
	result := make([]migration.TopicInfo, 0, len(topicList))
	for _, t := range topicList {
		result = append(result, migration.TopicInfo{
			Name:              t.Name,
			Partitions:        t.Partitions,
			ReplicationFactor: t.ReplicationFactor,
		})
	}
	return result
}
