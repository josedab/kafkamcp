package server

import (
	"context"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
)

func (s *Server) handleNLPipelineBuild(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_nl_pipeline_build",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		description := request.GetString("description", "")
		if description == "" {
			return mcp.NewToolResultError("description is required"), nil
		}
		if deny := validateStringLength("description", description); deny != nil {
			return deny, nil
		}

		spec, err := s.nlPipelineBuilder.Generate(description, agentID)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_nl_pipeline_build", args, "", "error", 0, err, start)
			return s.toolError("Failed to build pipeline", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_nl_pipeline_build", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{
			"status":   "generated",
			"pipeline": spec,
			"message":  fmt.Sprintf("Pipeline %q generated with %d steps from: %s", spec.Name, len(spec.Steps), description),
		})
	})(ctx, request)
}

func (s *Server) handleNLPipelineManage(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_nl_pipeline_manage",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")
		pipelineID := request.GetString("pipeline_id", "")
		description := request.GetString("description", "")

		if deny := validateStringLength("pipeline_id", pipelineID); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("description", description); deny != nil {
			return deny, nil
		}

		switch action {
		case "list":
			pipelines := s.nlPipelineBuilder.ListPipelines()
			s.logAudit(ctx, agentID, "kafka_nl_pipeline_manage", args, "", "success", len(pipelines), nil, start)
			return marshalResult(map[string]any{"pipelines": pipelines, "total": len(pipelines)})

		case "get":
			if pipelineID == "" {
				return mcp.NewToolResultError("pipeline_id is required"), nil
			}
			spec, err := s.nlPipelineBuilder.GetPipeline(pipelineID)
			if err != nil {
				return s.toolError("Pipeline not found", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_nl_pipeline_manage", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"pipeline": spec})

		case "validate":
			if pipelineID == "" {
				return mcp.NewToolResultError("pipeline_id is required"), nil
			}
			issues, err := s.nlPipelineBuilder.Validate(pipelineID)
			if err != nil {
				return s.toolError("Validation failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_nl_pipeline_manage", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"valid": len(issues) == 0, "issues": issues})

		case "deploy":
			if pipelineID == "" {
				return mcp.NewToolResultError("pipeline_id is required"), nil
			}
			if err := s.nlPipelineBuilder.Deploy(pipelineID); err != nil {
				return s.toolError("Deploy failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_nl_pipeline_manage", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "deployed", "pipeline_id": pipelineID})

		case "delete":
			if pipelineID == "" {
				return mcp.NewToolResultError("pipeline_id is required"), nil
			}
			if err := s.nlPipelineBuilder.DeletePipeline(pipelineID); err != nil {
				return s.toolError("Delete failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_nl_pipeline_manage", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "deleted", "pipeline_id": pipelineID})

		case "parse":
			if description == "" {
				return mcp.NewToolResultError("description is required for parse action"), nil
			}
			result := s.nlPipelineBuilder.Parse(description)
			s.logAudit(ctx, agentID, "kafka_nl_pipeline_manage", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"parse_result": result})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; use: list, get, validate, deploy, delete, parse", action)), nil
		}
	})(ctx, request)
}
