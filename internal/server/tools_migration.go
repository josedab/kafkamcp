package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/migration"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleMigrationPlan(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_migration_plan", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		cluster := request.GetString("cluster", "")

		if deny := validateStringLength("action", action); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}

		switch action {
		case "plan":
			sourceCluster := request.GetString("source_cluster", "")
			targetCluster := request.GetString("target_cluster", "")
			if sourceCluster == "" || targetCluster == "" {
				return mcp.NewToolResultError("source_cluster and target_cluster are required"), nil
			}
			if deny := validateStringLength("source_cluster", sourceCluster); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("target_cluster", targetCluster); deny != nil {
				return deny, nil
			}

			sourceTopics, err := s.kafkaManager.ListTopics(ctx, sourceCluster, false)
			if err != nil {
				return s.toolError("failed to list source topics", err), nil
			}
			targetTopics, err := s.kafkaManager.ListTopics(ctx, targetCluster, false)
			if err != nil {
				return s.toolError("failed to list target topics", err), nil
			}

			var srcInfo, tgtInfo []migration.TopicInfo
			for _, t := range sourceTopics {
				srcInfo = append(srcInfo, migration.TopicInfo{Name: t.Name, Partitions: t.Partitions, ReplicationFactor: t.ReplicationFactor})
			}
			for _, t := range targetTopics {
				tgtInfo = append(tgtInfo, migration.TopicInfo{Name: t.Name, Partitions: t.Partitions, ReplicationFactor: t.ReplicationFactor})
			}

			plan := s.migrationPlanner.CreatePlan(sourceCluster, targetCluster, srcInfo, tgtInfo)
			s.logAudit(agentID, "kafka_migration_plan", args, cluster, "success", 1, nil, start)
			return marshalResult(plan)

		case "validate":
			plans := s.migrationPlanner.ListPlans()
			if len(plans) == 0 {
				return mcp.NewToolResultError("no migration plans exist"), nil
			}
			lastPlan := plans[len(plans)-1]
			warnings := migration.ValidatePlan(&lastPlan)
			s.logAudit(agentID, "kafka_migration_plan", args, cluster, "success", 1, nil, start)
			return marshalResult(map[string]any{"plan_id": lastPlan.ID, "warnings": warnings, "valid": len(warnings) == 0})

		case "list":
			plans := s.migrationPlanner.ListPlans()
			s.logAudit(agentID, "kafka_migration_plan", args, cluster, "success", len(plans), nil, start)
			return marshalResult(map[string]any{"plans": plans, "total": len(plans)})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: plan, validate, list", action)), nil
		}
	})(ctx, request)
}
