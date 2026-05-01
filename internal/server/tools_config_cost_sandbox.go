package server

import (
	"context"
	"encoding/json"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/sandbox"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleConfigHistory(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	resource, _ := args["resource"].(string)
	resourceID, _ := args["resource_id"].(string)

	if resource == "" || resourceID == "" {
		return mcp.NewToolResultError("resource and resource_id are required"), nil
	}
	if deny := validateStringLength("resource", resource); deny != nil {
		return deny, nil
	}
	if deny := validateStringLength("resource_id", resourceID); deny != nil {
		return deny, nil
	}

	limit := 50
	if v, ok := args["limit"].(float64); ok && v > 0 {
		limit = int(v)
	}
	if limit > MaxDLQMessages {
		limit = MaxDLQMessages
	}

	auditReport, _ := args["audit_report"].(bool)

	if auditReport {
		report, err := s.configVCSEngine.AuditReport(resource, resourceID, time.Time{})
		if err != nil {
			return s.toolError("failed to generate audit report", err), nil
		}
		s.logAudit(agentID, "kafka_config_history", args, "", "success", report.TotalChanges, nil, start)
		return marshalResult(report)
	}

	history := s.configVCSEngine.History(resource, resourceID, limit)
	s.logAudit(agentID, "kafka_config_history", args, "", "success", len(history), nil, start)
	return marshalResult(map[string]any{"history": history, "total": len(history)})
}

func (s *Server) handleConfigRollback(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	resource, _ := args["resource"].(string)
	resourceID, _ := args["resource_id"].(string)
	reason, _ := args["reason"].(string)

	if resource == "" || resourceID == "" {
		return mcp.NewToolResultError("resource and resource_id are required"), nil
	}
	if deny := validateStringLength("resource", resource); deny != nil {
		return deny, nil
	}
	if deny := validateStringLength("resource_id", resourceID); deny != nil {
		return deny, nil
	}
	if deny := validateStringLength("reason", reason); deny != nil {
		return deny, nil
	}

	targetVersion := 0
	if v, ok := args["target_version"].(float64); ok {
		targetVersion = int(v)
	}
	if targetVersion < 1 {
		return mcp.NewToolResultError("target_version must be >= 1"), nil
	}

	result, err := s.configVCSEngine.Rollback(resource, resourceID, targetVersion, agentID, reason)
	if err != nil {
		return s.toolError("config rollback failed", err), nil
	}

	s.logAudit(agentID, "kafka_config_rollback", args, "", "success", 1, nil, start)
	return marshalResult(result)
}

func (s *Server) handleConfigCompare(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	resource, _ := args["resource"].(string)
	resourceID, _ := args["resource_id"].(string)

	if resource == "" || resourceID == "" {
		return mcp.NewToolResultError("resource and resource_id are required"), nil
	}
	if deny := validateStringLength("resource", resource); deny != nil {
		return deny, nil
	}
	if deny := validateStringLength("resource_id", resourceID); deny != nil {
		return deny, nil
	}

	var version1, version2 int
	if v, ok := args["version1"].(float64); ok {
		version1 = int(v)
	}
	if v, ok := args["version2"].(float64); ok {
		version2 = int(v)
	}

	if version1 < 1 || version2 < 1 {
		return mcp.NewToolResultError("version1 and version2 must be >= 1"), nil
	}

	result, err := s.configVCSEngine.Compare(resource, resourceID, version1, version2)
	if err != nil {
		return s.toolError("config comparison failed", err), nil
	}

	s.logAudit(agentID, "kafka_config_compare", args, "", "success", len(result.Differences), nil, start)
	return marshalResult(result)
}

func (s *Server) handleCostLive(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	resourceType, _ := args["resource_type"].(string)
	resourceName, _ := args["resource_name"].(string)

	if resourceType != "" && resourceName != "" {
		cost, err := s.costAttrEngine.GetLiveCost(resourceType, resourceName)
		if err != nil {
			return s.toolError("failed to retrieve cost data", err), nil
		}
		s.logAudit(agentID, "kafka_cost_live", args, "", "success", 1, nil, start)
		return marshalResult(cost)
	}

	costs := s.costAttrEngine.GetAllLiveCosts()
	s.logAudit(agentID, "kafka_cost_live", args, "", "success", len(costs), nil, start)
	return marshalResult(map[string]any{"costs": costs, "total": len(costs)})
}

func (s *Server) handleCostAllocate(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	action, _ := args["action"].(string)

	switch action {
	case "create":
		name, _ := args["name"].(string)
		owner, _ := args["owner"].(string)
		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}

		var resources []string
		if raw, ok := args["resources"]; ok {
			data, err := json.Marshal(raw)
			if err != nil {
				return s.toolError("failed to process resources", err), nil
			}
			if err := json.Unmarshal(data, &resources); err != nil {
				return s.toolError("failed to process resources", err), nil
			}
		}

		var tags map[string]string
		if raw, ok := args["tags"].(map[string]any); ok {
			tags = make(map[string]string)
			for k, v := range raw {
				if s, ok := v.(string); ok {
					tags[k] = s
				}
			}
		}

		alloc, err := s.costAttrEngine.CreateAllocation(name, owner, resources, tags)
		if err != nil {
			return s.toolError("failed to create cost allocation", err), nil
		}
		s.logAudit(agentID, "kafka_cost_allocate", args, "", "success", 1, nil, start)
		return marshalResult(alloc)

	case "list":
		allocs := s.costAttrEngine.GetAllocations()
		s.logAudit(agentID, "kafka_cost_allocate", args, "", "success", len(allocs), nil, start)
		return marshalResult(map[string]any{"allocations": allocs, "total": len(allocs)})

	default:
		return mcp.NewToolResultError("action must be: create or list"), nil
	}
}

func (s *Server) handleCostBudget(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	action, _ := args["action"].(string)

	switch action {
	case "create":
		name, _ := args["name"].(string)
		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}

		monthlyLimit := 0.0
		if v, ok := args["monthly_limit"].(float64); ok {
			monthlyLimit = v
		}

		var resources []string
		if raw, ok := args["resources"]; ok {
			data, err := json.Marshal(raw)
			if err != nil {
				return s.toolError("failed to process resources", err), nil
			}
			if err := json.Unmarshal(data, &resources); err != nil {
				return s.toolError("failed to process resources", err), nil
			}
		}

		var thresholds []float64
		if raw, ok := args["alert_thresholds"]; ok {
			data, err := json.Marshal(raw)
			if err != nil {
				return s.toolError("failed to process alert thresholds", err), nil
			}
			if err := json.Unmarshal(data, &thresholds); err != nil {
				return s.toolError("failed to process alert thresholds", err), nil
			}
		}

		budget, err := s.costAttrEngine.CreateBudget(name, agentID, monthlyLimit, resources, thresholds)
		if err != nil {
			return s.toolError("failed to create budget", err), nil
		}
		s.logAudit(agentID, "kafka_cost_budget", args, "", "success", 1, nil, start)
		return marshalResult(budget)

	case "list":
		budgets := s.costAttrEngine.GetBudgets()
		s.logAudit(agentID, "kafka_cost_budget", args, "", "success", len(budgets), nil, start)
		return marshalResult(map[string]any{"budgets": budgets, "total": len(budgets)})

	default:
		return mcp.NewToolResultError("action must be: create or list"), nil
	}
}

func (s *Server) handleSandboxCreate(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	name, _ := args["name"].(string)
	description, _ := args["description"].(string)

	if name == "" {
		return mcp.NewToolResultError("name is required"), nil
	}
	if deny := validateStringLength("name", name); deny != nil {
		return deny, nil
	}
	if deny := validateStringLength("description", description); deny != nil {
		return deny, nil
	}

	var durationSec int64
	if v, ok := args["duration_sec"].(float64); ok {
		durationSec = int64(v)
	}

	var quotas *sandbox.ResourceQuotas
	if raw, ok := args["quotas"]; ok {
		quotas = &sandbox.ResourceQuotas{}
		data, err := json.Marshal(raw)
		if err != nil {
			return s.toolError("failed to process quotas", err), nil
		}
		if err := json.Unmarshal(data, quotas); err != nil {
			return s.toolError("failed to process quotas", err), nil
		}
		clampSandboxQuotas(quotas)
	}

	sb, err := s.sandboxEngine.Create(name, description, agentID, durationSec, quotas)
	if err != nil {
		return s.toolError("failed to create sandbox", err), nil
	}

	s.logAudit(agentID, "kafka_sandbox_create", args, "", "success", 1, nil, start)
	return marshalResult(sb)
}

func (s *Server) handleSandboxExec(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	sandboxID, _ := args["sandbox_id"].(string)
	tool, _ := args["tool"].(string)

	if sandboxID == "" || tool == "" {
		return mcp.NewToolResultError("sandbox_id and tool are required"), nil
	}
	if deny := validateStringLength("sandbox_id", sandboxID); deny != nil {
		return deny, nil
	}
	if deny := validateStringLength("tool", tool); deny != nil {
		return deny, nil
	}

	toolArgs, _ := args["args"].(map[string]any)

	execStart := time.Now()

	// Record the operation (actual tool execution would be delegated in production)
	op, err := s.sandboxEngine.Exec(sandboxID, tool, toolArgs, map[string]string{"status": "simulated"}, "", time.Since(execStart))
	if err != nil {
		return s.toolError("sandbox execution failed", err), nil
	}

	s.logAudit(agentID, "kafka_sandbox_exec", args, "", "success", 1, nil, start)
	return marshalResult(op)
}

func (s *Server) handleSandboxManage(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	action, _ := args["action"].(string)
	sandboxID, _ := args["sandbox_id"].(string)

	if deny := validateStringLength("sandbox_id", sandboxID); deny != nil {
		return deny, nil
	}

	switch action {
	case "destroy":
		if sandboxID == "" {
			return mcp.NewToolResultError("sandbox_id is required"), nil
		}
		if err := s.sandboxEngine.Destroy(sandboxID); err != nil {
			return s.toolError("failed to destroy sandbox", err), nil
		}
		s.logAudit(agentID, "kafka_sandbox_destroy", args, "", "success", 1, nil, start)
		return marshalResult(map[string]string{"status": "destroyed"})

	case "list":
		sandboxes := s.sandboxEngine.List(agentID)
		s.logAudit(agentID, "kafka_sandbox_destroy", args, "", "success", len(sandboxes), nil, start)
		return marshalResult(map[string]any{"sandboxes": sandboxes, "total": len(sandboxes)})

	case "get":
		if sandboxID == "" {
			return mcp.NewToolResultError("sandbox_id is required"), nil
		}
		sb, err := s.sandboxEngine.Get(sandboxID)
		if err != nil {
			return s.toolError("sandbox not found", err), nil
		}
		s.logAudit(agentID, "kafka_sandbox_destroy", args, "", "success", 1, nil, start)
		return marshalResult(sb)

	case "promotion_report":
		if sandboxID == "" {
			return mcp.NewToolResultError("sandbox_id is required"), nil
		}
		report, err := s.sandboxEngine.GeneratePromotionReport(sandboxID)
		if err != nil {
			return s.toolError("failed to generate promotion report", err), nil
		}
		s.logAudit(agentID, "kafka_sandbox_destroy", args, "", "success", 1, nil, start)
		return marshalResult(report)

	default:
		return mcp.NewToolResultError("action must be: destroy, list, get, or promotion_report"), nil
	}
}

// clampSandboxQuotas enforces upper bounds on user-supplied quota values.
func clampSandboxQuotas(q *sandbox.ResourceQuotas) {
	const (
		maxTopics       = 50
		maxPartitions   = 12
		maxMessages     = 100000
		maxStorageBytes = 1 << 30 // 1 GB
		maxDurationSec  = 86400   // 24 hours
	)
	if q.MaxTopics > maxTopics {
		q.MaxTopics = maxTopics
	}
	if q.MaxPartitions > maxPartitions {
		q.MaxPartitions = maxPartitions
	}
	if q.MaxMessages > int64(maxMessages) {
		q.MaxMessages = int64(maxMessages)
	}
	if q.MaxStorageBytes > int64(maxStorageBytes) {
		q.MaxStorageBytes = int64(maxStorageBytes)
	}
	if q.MaxDurationSec > int64(maxDurationSec) {
		q.MaxDurationSec = int64(maxDurationSec)
	}
}
