package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/digitaltwin"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleTwinSnapshot(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_twin_snapshot",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		cluster := request.GetString("cluster", "")

		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}

		// Build snapshot from current cluster state
		topics := make(map[string]digitaltwin.TopicState)
		groups := make(map[string]digitaltwin.ConsumerGroupState)
		brokers := []digitaltwin.BrokerState{}

		clusterName := cluster
		if clusterName == "" {
			clusterName = "default"
		}

		// Get topics from kafka manager
		topicList, err := s.kafkaManager.ListTopics(ctx, cluster, false)
		if err == nil {
			for _, t := range topicList {
				topics[t.Name] = digitaltwin.TopicState{
					Name:              t.Name,
					Partitions:        t.Partitions,
					ReplicationFactor: t.ReplicationFactor,
					Configs:           make(map[string]string),
				}
			}
		}

		// Get consumer groups
		groupList, err := s.kafkaManager.ListConsumerGroups(ctx, cluster)
		if err == nil {
			for _, g := range groupList {
				groups[g.GroupID] = digitaltwin.ConsumerGroupState{
					GroupID:  g.GroupID,
					State:   g.State,
					Members: g.MemberCount,
				}
			}
		}

		snapshot := s.digitalTwin.CaptureSnapshot(clusterName, topics, groups, brokers)

		s.logAudit(agentID, "kafka_twin_snapshot", args, cluster, "success", len(topics), nil, start)
		return marshalResult(map[string]any{
			"snapshot":    snapshot,
			"topics":     len(topics),
			"groups":     len(groups),
			"message":    "Snapshot captured. Use kafka_twin_simulate with action=create_twin to create a mutable copy.",
		})
	})(ctx, request)
}

func (s *Server) handleTwinSimulate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_twin_simulate",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")
		snapshotID := request.GetString("snapshot_id", "")
		twinID := request.GetString("twin_id", "")

		if deny := validateStringLength("snapshot_id", snapshotID); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("twin_id", twinID); deny != nil {
			return deny, nil
		}

		switch action {
		case "create_twin":
			if snapshotID == "" {
				return mcp.NewToolResultError("snapshot_id is required"), nil
			}
			twin, err := s.digitalTwin.CreateTwin(snapshotID)
			if err != nil {
				return s.toolError("Failed to create twin", err), nil
			}
			s.logAudit(agentID, "kafka_twin_simulate", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "created", "twin": twin})

		case "simulate":
			if twinID == "" {
				return mcp.NewToolResultError("twin_id is required"), nil
			}
			opType := request.GetString("op_type", "")
			if opType == "" {
				return mcp.NewToolResultError("op_type is required"), nil
			}

			params := map[string]any{}
			if topic := request.GetString("topic", ""); topic != "" {
				params["name"] = topic
			}
			if p := request.GetFloat("partitions", 0); p > 0 {
				params["partitions"] = int(p)
			}
			if rf := request.GetFloat("replication_factor", 0); rf > 0 {
				params["replication_factor"] = int(rf)
			}

			result, err := s.digitalTwin.SimulateOp(twinID, digitaltwin.SimulationOp{
				Type:   opType,
				Params: params,
			})
			if err != nil {
				return s.toolError("Simulation failed", err), nil
			}
			s.logAudit(agentID, "kafka_twin_simulate", args, "", "success", 0, nil, start)
			return marshalResult(result)

		case "diff":
			if twinID == "" {
				return mcp.NewToolResultError("twin_id is required"), nil
			}
			diff, err := s.digitalTwin.Diff(twinID)
			if err != nil {
				return s.toolError("Diff failed", err), nil
			}
			s.logAudit(agentID, "kafka_twin_simulate", args, "", "success", diff.TotalChanges, nil, start)
			return marshalResult(diff)

		case "migrate_plan":
			if twinID == "" {
				return mcp.NewToolResultError("twin_id is required"), nil
			}
			plan, err := s.digitalTwin.GenerateMigrationPlan(twinID)
			if err != nil {
				return s.toolError("Migration plan failed", err), nil
			}
			s.logAudit(agentID, "kafka_twin_simulate", args, "", "success", len(plan.Steps), nil, start)
			return marshalResult(plan)

		case "get":
			if twinID == "" {
				return mcp.NewToolResultError("twin_id is required"), nil
			}
			twin, err := s.digitalTwin.GetTwin(twinID)
			if err != nil {
				return s.toolError("Twin not found", err), nil
			}
			s.logAudit(agentID, "kafka_twin_simulate", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"twin": twin})

		case "history":
			if twinID == "" {
				return mcp.NewToolResultError("twin_id is required"), nil
			}
			history := s.digitalTwin.GetSimulationHistory(twinID)
			s.logAudit(agentID, "kafka_twin_simulate", args, "", "success", len(history), nil, start)
			return marshalResult(map[string]any{"history": history, "total": len(history)})

		default:
			twins := s.digitalTwin.ListTwins()
			s.logAudit(agentID, "kafka_twin_simulate", args, "", "success", len(twins), nil, start)
			return marshalResult(map[string]any{"twins": twins, "total": len(twins)})
		}
	})(ctx, request)
}
