package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/evolution"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleSchemaPropose(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_schema_propose",
		resourceType: auth.ResourceSchema,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		subject := request.GetString("subject", "")
		schema := request.GetString("schema", "")
		schemaType := request.GetString("schema_type", "AVRO")
		cluster := request.GetString("cluster", "")

		if subject == "" {
			return mcp.NewToolResultError("subject is required"), nil
		}
		if schema == "" {
			return mcp.NewToolResultError("schema is required"), nil
		}
		if deny := validateStringLength("subject", subject); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("schema", schema); deny != nil {
			return deny, nil
		}
		switch schemaType {
		case "AVRO", "JSON", "PROTOBUF":
			// valid
		default:
			return mcp.NewToolResultError("invalid schema_type: must be AVRO, JSON, or PROTOBUF"), nil
		}

		// Get current schema for diff generation
		var currentSchema any
		var currentVersion int
		var consumerGroups []string
		topicsByGroup := make(map[string][]string)

		sc := s.getSchemaClient(cluster)
		if sc != nil {
			if detail, err := sc.GetLatestSchema(ctx, subject); err == nil {
				currentSchema = detail.Schema
				currentVersion = detail.Version
			}
		}

		// Get consumer groups for impact analysis
		resolvedCluster := s.resolveClusterName(cluster)
		if groups, err := s.kafkaManager.ListConsumerGroups(ctx, resolvedCluster); err == nil {
			for _, g := range groups {
				consumerGroups = append(consumerGroups, g.GroupID)
				if detail, err := s.kafkaManager.DescribeConsumerGroup(ctx, resolvedCluster, g.GroupID); err == nil {
					for _, m := range detail.Members {
						for _, a := range m.Assignment {
							topicsByGroup[g.GroupID] = append(topicsByGroup[g.GroupID], a.Topic)
						}
					}
				}
			}
		}

		proposal, err := s.schemaWorkflow.Propose(subject, schema, schemaType, agentID, currentSchema, currentVersion, consumerGroups, topicsByGroup)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_schema_propose", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to create schema proposal", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_schema_propose", args, cluster, "success", 0, nil, start)
		return marshalResult(proposal)
	})(ctx, request)
}

func (s *Server) handleSchemaApply(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_schema_apply",
		resourceType: auth.ResourceSchema,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		proposalID := request.GetString("proposal_id", "")
		cluster := request.GetString("cluster", "")

		switch action {
		case "review":
			reviewAction := request.GetString("review_action", "")
			note := request.GetString("note", "")
			if proposalID == "" {
				return mcp.NewToolResultError("proposal_id is required for review"), nil
			}
			if reviewAction == "" {
				return mcp.NewToolResultError("review_action is required (approve or reject)"), nil
			}
			result, err := s.schemaWorkflow.Review(proposalID, agentID, reviewAction, note)
			if err != nil {
				return s.toolError("Failed to review proposal", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_schema_apply", args, cluster, "success", 0, nil, start)
			return marshalResult(result)

		case "apply":
			if proposalID == "" {
				return mcp.NewToolResultError("proposal_id is required for apply"), nil
			}
			proposal, err := s.schemaWorkflow.GetProposal(proposalID)
			if err != nil {
				return s.toolError("Failed to get proposal", err), nil
			}
			if proposal.Status != "approved" {
				return mcp.NewToolResultError("proposal must be approved before applying"), nil
			}

			sc := s.getSchemaClient(cluster)
			if sc == nil {
				return mcp.NewToolResultError("Schema Registry not configured for this cluster"), nil
			}

			newVersion, err := sc.RegisterSchema(ctx, proposal.Subject, proposal.Schema, proposal.SchemaType)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_schema_apply", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to register schema", err), nil
			}

			if err := s.schemaWorkflow.MarkApplied(proposalID); err != nil {
				return s.toolError("Schema registered but failed to update proposal status", err), nil
			}

			s.logAudit(ctx, agentID, "kafka_schema_apply", args, cluster, "success", 0, nil, start)
			return marshalResult(map[string]any{
				"status":      "applied",
				"proposal_id": proposalID,
				"subject":     proposal.Subject,
				"new_version": newVersion,
				"message":     "Schema successfully registered in Schema Registry",
			})

		case "rollback":
			if proposalID == "" {
				return mcp.NewToolResultError("proposal_id is required for rollback"), nil
			}
			if err := s.schemaWorkflow.MarkRolledBack(proposalID); err != nil {
				return s.toolError("Failed to rollback proposal", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_schema_apply", args, cluster, "success", 0, nil, start)
			return marshalResult(map[string]any{
				"status":      "rolled_back",
				"proposal_id": proposalID,
				"message":     "Proposal marked as rolled back. Delete the schema version from Schema Registry if needed.",
			})

		case "list":
			statusFilter := request.GetString("status_filter", "")
			proposals := s.schemaWorkflow.ListProposals(statusFilter)
			s.logAudit(ctx, agentID, "kafka_schema_apply", args, cluster, "success", len(proposals), nil, start)
			return marshalResult(map[string]any{
				"proposals":   proposals,
				"total_count": len(proposals),
			})

		case "get":
			if proposalID == "" {
				return mcp.NewToolResultError("proposal_id is required for get"), nil
			}
			proposal, err := s.schemaWorkflow.GetProposal(proposalID)
			if err != nil {
				return s.toolError("Proposal not found", err), nil
			}
			return marshalResult(proposal)

		case "timeline":
			subject := request.GetString("subject", "")
			if subject == "" {
				return mcp.NewToolResultError("subject is required for timeline"), nil
			}
			sc := s.getSchemaClient(cluster)
			if sc == nil {
				return mcp.NewToolResultError("Schema Registry not configured"), nil
			}
			versions, err := sc.GetSubjectVersions(ctx, subject)
			if err != nil {
				return s.toolError("Failed to get schema versions", err), nil
			}
			timeline := evolution.VersionTimeline{
				Subject: subject,
			}
			for _, v := range versions {
				timeline.Versions = append(timeline.Versions, evolution.VersionEntry{
					Version: v,
					Status:  "active",
				})
			}
			// Add pending proposals
			proposals := s.schemaWorkflow.ListProposals("pending")
			for _, p := range proposals {
				if p.Subject == subject {
					timeline.Versions = append(timeline.Versions, evolution.VersionEntry{
						Version:   len(versions) + 1,
						Timestamp: p.ProposedAt,
						Status:    "proposed",
					})
				}
			}
			return marshalResult(timeline)

		default:
			return mcp.NewToolResultError("invalid action: must be review, apply, rollback, list, get, or timeline"), nil
		}
	})(ctx, request)
}
