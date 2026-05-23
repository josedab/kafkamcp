package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/schemaadvisor"
)

func (s *Server) handleSchemaAdvisorPredict(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_schema_advisor_predict",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		subject := request.GetString("subject", "")
		schema := request.GetString("schema", "")
		schemaType := request.GetString("schema_type", "AVRO")
		compat := request.GetString("compatibility", "BACKWARD")

		if subject == "" || schema == "" {
			return mcp.NewToolResultError("subject and schema are required"), nil
		}
		if deny := validateStringLength("subject", subject); deny != nil {
			return deny, nil
		}

		compatLevel := schemaadvisor.CompatibilityLevel(compat)
		prediction := s.schemaAdvisor.PredictCompatibility(subject, schema, schemaType, compatLevel)

		s.logAudit(ctx, agentID, "kafka_schema_advisor_predict", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{
			"prediction": prediction,
		})
	})(ctx, request)
}

func (s *Server) handleSchemaAdvisorMigrate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_schema_advisor_migrate",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		subject := request.GetString("subject", "")
		if subject == "" {
			return mcp.NewToolResultError("subject is required"), nil
		}

		fromVersion := int(request.GetFloat("from_version", 1))
		toVersion := int(request.GetFloat("to_version", 2))

		plan, err := s.schemaAdvisor.GenerateMigrationPlan(subject, fromVersion, toVersion)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_schema_advisor_migrate", args, "", "error", 0, err, start)
			return s.toolError("Failed to generate migration plan", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_schema_advisor_migrate", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{"migration_plan": plan})
	})(ctx, request)
}

func (s *Server) handleSchemaAdvisorAnalyze(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_schema_advisor_analyze",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		subject := request.GetString("subject", "")

		if subject == "" {
			subjects := s.schemaAdvisor.ListSubjects()
			s.logAudit(ctx, agentID, "kafka_schema_advisor_analyze", args, "", "success", len(subjects), nil, start)
			return marshalResult(map[string]any{"subjects": subjects, "total": len(subjects)})
		}

		advice, err := s.schemaAdvisor.AnalyzeEvolution(subject)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_schema_advisor_analyze", args, "", "error", 0, err, start)
			return s.toolError("Failed to analyze evolution", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_schema_advisor_analyze", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{"advice": advice})
	})(ctx, request)
}

func (s *Server) handleSchemaAdvisorIntelligent(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_schema_advisor_intelligent",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		subject := request.GetString("subject", "")
		if subject == "" {
			return mcp.NewToolResultError("subject is required"), nil
		}

		intel, err := s.schemaAdvisor.AnalyzeIntelligent(subject)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_schema_advisor_intelligent", args, "", "error", 0, err, start)
			return s.toolError("Failed to perform intelligent analysis", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_schema_advisor_intelligent", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{"intelligence": intel})
	})(ctx, request)
}
