package server

import (
	"context"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/gitops"
)

// handleGitOpsDiff computes diff between desired state and live cluster.
func (s *Server) handleGitOpsDiff(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_gitops_diff",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		cluster := request.GetString("cluster", "")

		// Get live state from cluster
		topics, err := s.kafkaManager.ListTopics(ctx, cluster, false)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_gitops_diff", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to list topics", err), nil
		}

		live := gitops.LiveState{Topics: make(map[string]gitops.TopicSpec)}
		for _, t := range topics {
			detail, err := s.kafkaManager.DescribeTopic(ctx, cluster, t.Name)
			if err != nil {
				continue
			}
			live.Topics[t.Name] = gitops.TopicSpec{
				Name:              t.Name,
				Partitions:        t.Partitions,
				ReplicationFactor: t.ReplicationFactor,
				Configs:           detail.Configs,
			}
		}

		// Parse desired state from request
		desired := gitops.DesiredState{Version: "1"}
		if desiredTopics, ok := args["topics"].([]any); ok {
			for _, dt := range desiredTopics {
				if tm, ok := dt.(map[string]any); ok {
					topicName := fmt.Sprintf("%v", tm["name"])
					if deny := validateStringLength("topic_name", topicName); deny != nil {
						return deny, nil
					}
					spec := gitops.TopicSpec{
						Name:              topicName,
						Partitions:        intFromAny(tm["partitions"], 1),
						ReplicationFactor: intFromAny(tm["replication_factor"], 1),
					}
					if configs, ok := tm["configs"].(map[string]any); ok {
						spec.Configs = make(map[string]string)
						for k, v := range configs {
							if deny := validateStringLength("config_key", k); deny != nil {
								return deny, nil
							}
							spec.Configs[k] = fmt.Sprintf("%v", v)
						}
					}
					desired.Topics = append(desired.Topics, spec)
				}
			}
		}

		if errs := s.gitopsManager.ValidateDesiredState(desired); len(errs) > 0 {
			s.logger.Error("GitOps validation failed", "errors", errs)
			return mcp.NewToolResultError("GitOps desired state validation failed"), nil
		}

		diff, err := s.gitopsManager.Diff(desired, live)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_gitops_diff", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to compute diff", err), nil
		}

		plan := s.gitopsManager.GeneratePlan(diff, true)

		s.logAudit(ctx, agentID, "kafka_gitops_diff", args, cluster, "success", diff.TotalChanges, nil, start)
		return marshalResult(map[string]any{
			"diff":    diff,
			"plan":    plan,
			"summary": diff.Summary,
		})
	})(ctx, request)
}
