package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/operator"
)

func (s *Server) registerOperatorTools() {
	if s.operatorController == nil {
		return
	}

	s.addTool(mcp.NewTool("k8s_operator_manage",
		withCategory("ops"),
		mcp.WithDescription("Manage KafkaMCP Kubernetes operator instances. Actions: create, get, list, update, delete, scale, manifest, crd."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: create, get, list, update, delete, scale, manifest, crd")),
		mcp.WithString("name", mcp.Description("Instance name")),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace. Default: default")),
		mcp.WithNumber("replicas", mcp.Description("Number of replicas")),
		mcp.WithString("image", mcp.Description("Container image")),
		mcp.WithString("version", mcp.Description("Image version tag")),
		mcp.WithString("transport", mcp.Description("MCP transport: stdio, sse, streamable-http")),
		mcp.WithNumber("port", mcp.Description("Service port")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "k8s_operator_manage",
		resourceType: "cluster",
		action:       "admin",
	}, s.handleOperatorManage))
}

func (s *Server) handleOperatorManage(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	action, _ := args["action"].(string)
	name, _ := args["name"].(string)
	namespace, _ := args["namespace"].(string)
	if namespace == "" {
		namespace = "default"
	}

	switch action {
	case "create":
		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}
		spec := operator.KafkaMCPSpec{}
		if r, ok := args["replicas"].(float64); ok {
			spec.Replicas = int(r)
		}
		if img, ok := args["image"].(string); ok {
			spec.Image = img
		}
		if ver, ok := args["version"].(string); ok {
			spec.Version = ver
		}
		if t, ok := args["transport"].(string); ok {
			spec.Transport = t
		}
		if p, ok := args["port"].(float64); ok {
			spec.Port = int(p)
		}

		inst, err := s.operatorController.CreateInstance(name, namespace, spec)
		if err != nil {
			return s.toolError("failed to create operator instance", err), nil
		}
		s.logAudit(ctx, agentID, "k8s_operator_manage", args, "", "success", 1, nil, start)
		return marshalResult(inst)

	case "get":
		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}
		inst, err := s.operatorController.GetInstance(name, namespace)
		if err != nil {
			return s.toolError("operator instance not found", err), nil
		}
		s.logAudit(ctx, agentID, "k8s_operator_manage", args, "", "success", 1, nil, start)
		return marshalResult(inst)

	case "list":
		instances := s.operatorController.ListInstances(namespace)
		s.logAudit(ctx, agentID, "k8s_operator_manage", args, "", "success", len(instances), nil, start)
		return marshalResult(map[string]any{"instances": instances, "total": len(instances)})

	case "delete":
		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}
		err := s.operatorController.DeleteInstance(name, namespace)
		if err != nil {
			return s.toolError("failed to delete operator instance", err), nil
		}
		s.logAudit(ctx, agentID, "k8s_operator_manage", args, "", "success", 1, nil, start)
		return marshalResult(map[string]string{"status": "deleted"})

	case "scale":
		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}
		replicas, ok := args["replicas"].(float64)
		if !ok {
			return mcp.NewToolResultError("replicas is required"), nil
		}
		inst, err := s.operatorController.ScaleInstance(name, namespace, int(replicas))
		if err != nil {
			return s.toolError("failed to scale operator instance", err), nil
		}
		s.logAudit(ctx, agentID, "k8s_operator_manage", args, "", "success", 1, nil, start)
		return marshalResult(inst)

	case "manifest":
		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}
		inst, err := s.operatorController.GetInstance(name, namespace)
		if err != nil {
			return s.toolError("operator instance not found", err), nil
		}
		manifest := s.operatorController.GenerateDeploymentManifest(inst)
		s.logAudit(ctx, agentID, "k8s_operator_manage", args, "", "success", 1, nil, start)
		return mcp.NewToolResultText(manifest), nil

	case "crd":
		crd := operator.GenerateCRD()
		s.logAudit(ctx, agentID, "k8s_operator_manage", args, "", "success", 1, nil, start)
		return mcp.NewToolResultText(crd), nil

	default:
		return mcp.NewToolResultError("action must be: create, get, list, update, delete, scale, manifest, or crd"), nil
	}
}
