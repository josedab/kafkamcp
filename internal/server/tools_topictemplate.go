package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleTopicTemplate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_topic_template", resourceType: auth.ResourceTopic, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		if action == "" {
			return mcp.NewToolResultError("action is required"), nil
		}

		switch action {
		case "parse":
			content := request.GetString("template", "")
			if content == "" {
				return mcp.NewToolResultError("template content is required"), nil
			}
			if deny := validateStringLength("template", content); deny != nil {
				return deny, nil
			}
			format := request.GetString("format", "json")
			tmpl, err := s.topicTemplateEngine.ParseTemplate(content, format)
			if err != nil {
				return s.toolError("Failed to parse template", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_topic_template", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "valid", "template": tmpl})

		case "save":
			content := request.GetString("template", "")
			if content == "" {
				return mcp.NewToolResultError("template content is required"), nil
			}
			if deny := validateStringLength("template", content); deny != nil {
				return deny, nil
			}
			format := request.GetString("format", "json")
			tmpl, err := s.topicTemplateEngine.ParseTemplate(content, format)
			if err != nil {
				return s.toolError("Failed to parse template", err), nil
			}
			if err := s.topicTemplateEngine.SaveTemplate(tmpl, agentID); err != nil {
				return s.toolError("Failed to save template", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_topic_template", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "saved", "name": tmpl.Metadata.Name})

		case "get":
			name := request.GetString("name", "")
			if name == "" {
				return mcp.NewToolResultError("name is required"), nil
			}
			tmpl, err := s.topicTemplateEngine.GetTemplate(name)
			if err != nil {
				return s.toolError("Template not found", err), nil
			}
			return marshalResult(map[string]any{"template": tmpl})

		case "list":
			templates := s.topicTemplateEngine.ListTemplates()
			s.logAudit(ctx, agentID, "kafka_topic_template", args, "", "success", len(templates), nil, start)
			return marshalResult(map[string]any{"templates": templates, "total": len(templates)})

		case "delete":
			name := request.GetString("name", "")
			if name == "" {
				return mcp.NewToolResultError("name is required"), nil
			}
			if err := s.topicTemplateEngine.DeleteTemplate(name); err != nil {
				return s.toolError("Failed to delete template", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_topic_template", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "deleted", "name": name})

		case "diff", "plan":
			name := request.GetString("name", "")
			if name == "" {
				return mcp.NewToolResultError("name is required"), nil
			}
			cluster := request.GetString("cluster", "")
			tmpl, err := s.topicTemplateEngine.GetTemplate(name)
			if err != nil {
				return s.toolError("Template not found", err), nil
			}

			actualExists := false
			var actualPartitions int32
			var actualReplFactor int16
			var actualConfigs map[string]string

			detail, err := s.kafkaManager.DescribeTopic(ctx, cluster, tmpl.Spec.TopicName)
			if err == nil && detail != nil {
				actualExists = true
				actualPartitions = int32(len(detail.Partitions))
				actualReplFactor = int16(detail.ReplicationFactor)
				actualConfigs = detail.Configs
			}

			diff := s.topicTemplateEngine.ComputeDiff(tmpl, actualExists, actualPartitions, actualReplFactor, actualConfigs)

			if action == "plan" {
				plan := s.topicTemplateEngine.ComputePlan(diff)
				s.logAudit(ctx, agentID, "kafka_topic_template", args, cluster, "success", len(plan.Changes), nil, start)
				return marshalResult(plan)
			}
			s.logAudit(ctx, agentID, "kafka_topic_template", args, cluster, "success", len(diff.Changes), nil, start)
			return marshalResult(diff)

		case "export":
			name := request.GetString("name", "")
			if name == "" {
				return mcp.NewToolResultError("name is required"), nil
			}
			format := request.GetString("format", "yaml")
			tmpl, err := s.topicTemplateEngine.GetTemplate(name)
			if err != nil {
				return s.toolError("Template not found", err), nil
			}
			output, err := s.topicTemplateEngine.ExportTemplate(tmpl, format)
			if err != nil {
				return s.toolError("Failed to export template", err), nil
			}
			return marshalResult(map[string]any{"format": format, "content": output})

		default:
			return mcp.NewToolResultError("action must be: parse, save, get, list, delete, diff, plan, export"), nil
		}
	})(ctx, request)
}
