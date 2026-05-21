package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/schemaplayground"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleSchemaPlayground(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_schema_playground",
		resourceType: auth.ResourceSchema,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")
		draftID := request.GetString("draft_id", "")

		if deny := validateStringLength("draft_id", draftID); deny != nil {
			return deny, nil
		}

		switch action {
		case "create":
			subject := request.GetString("subject", "")
			schemaType := schemaplayground.SchemaType(request.GetString("schema_type", "AVRO"))
			schema := request.GetString("schema", "")

			if subject == "" || schema == "" {
				return mcp.NewToolResultError("subject and schema are required"), nil
			}
			if deny := validateStringLength("subject", subject); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("schema", schema); deny != nil {
				return deny, nil
			}

			draft, err := s.schemaPlayground.CreateDraft(subject, schemaType, schema, agentID)
			if err != nil {
				return s.toolError("Failed to create draft", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_schema_playground", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "created", "draft": draft})

		case "update":
			if draftID == "" {
				return mcp.NewToolResultError("draft_id is required"), nil
			}
			schema := request.GetString("schema", "")
			if schema == "" {
				return mcp.NewToolResultError("schema is required"), nil
			}
			draft, err := s.schemaPlayground.UpdateDraft(draftID, schema)
			if err != nil {
				return s.toolError("Failed to update draft", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_schema_playground", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "updated", "draft": draft})

		case "test":
			if draftID == "" {
				return mcp.NewToolResultError("draft_id is required"), nil
			}
			msg := request.GetString("message", "")
			if msg == "" {
				return mcp.NewToolResultError("message is required"), nil
			}
			result, err := s.schemaPlayground.TestMessage(draftID, msg)
			if err != nil {
				return s.toolError("Test failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_schema_playground", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"test_result": result})

		case "validate":
			if draftID == "" {
				return mcp.NewToolResultError("draft_id is required"), nil
			}
			against := request.GetString("against_schema", "")
			if against == "" {
				return mcp.NewToolResultError("against_schema is required"), nil
			}
			compat := request.GetString("compatibility", "BACKWARD")
			validation, err := s.schemaPlayground.ValidateCompatibility(draftID, against, compat)
			if err != nil {
				return s.toolError("Validation failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_schema_playground", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"validation": validation})

		case "evolve":
			if draftID == "" {
				return mcp.NewToolResultError("draft_id is required"), nil
			}
			current := request.GetString("against_schema", "")
			if current == "" {
				return mcp.NewToolResultError("against_schema (current schema) is required"), nil
			}
			impact, err := s.schemaPlayground.SimulateEvolution(draftID, current)
			if err != nil {
				return s.toolError("Evolution simulation failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_schema_playground", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"evolution_impact": impact})

		case "promote":
			if draftID == "" {
				return mcp.NewToolResultError("draft_id is required"), nil
			}
			draft, err := s.schemaPlayground.PromoteDraft(draftID)
			if err != nil {
				return s.toolError("Promotion failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_schema_playground", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "promoted", "draft": draft})

		case "get":
			if draftID == "" {
				return mcp.NewToolResultError("draft_id is required"), nil
			}
			draft, err := s.schemaPlayground.GetDraft(draftID)
			if err != nil {
				return s.toolError("Draft not found", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_schema_playground", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"draft": draft})

		default:
			statusFilter := request.GetString("status_filter", "")
			drafts := s.schemaPlayground.ListDrafts(statusFilter)
			s.logAudit(ctx, agentID, "kafka_schema_playground", args, "", "success", len(drafts), nil, start)
			return marshalResult(map[string]any{"drafts": drafts, "total": len(drafts)})
		}
	})(ctx, request)
}
