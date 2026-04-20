package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/contracts"
	"github.com/kafkamcp/kafkamcp/internal/forecast"
	"github.com/kafkamcp/kafkamcp/internal/lagdiag"
	"github.com/kafkamcp/kafkamcp/internal/lineage"
	"github.com/kafkamcp/kafkamcp/internal/migration"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleDataContract(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	args := request.GetArguments()

	if deny := s.checkRateLimit(agentID, "kafka_validate_contract"); deny != nil {
		return deny, nil
	}

	action := request.GetString("action", "")
	topic := request.GetString("topic", "")
	cluster := request.GetString("cluster", "")

	switch action {
	case "register":
		contractData, ok := args["contract"]
		if !ok {
			return mcp.NewToolResultError("contract is required for register action"), nil
		}
		data, err := json.Marshal(contractData)
		if err != nil {
			return s.toolError("invalid contract", err), nil
		}
		var c contracts.Contract
		if err := json.Unmarshal(data, &c); err != nil {
			return s.toolError("invalid contract format", err), nil
		}
		if err := s.contractEngine.RegisterContract(c); err != nil {
			return s.toolError("failed to register contract", err), nil
		}
		s.logAudit(agentID, "kafka_validate_contract", args, cluster, "success", 1, nil, start)
		return marshalResult(map[string]any{"status": "registered", "contract": c.Name, "topic": c.Topic})

	case "validate":
		if topic == "" {
			return mcp.NewToolResultError("topic is required for validate action"), nil
		}
		msgData, ok := args["message"]
		if !ok {
			return mcp.NewToolResultError("message is required for validate action"), nil
		}
		msgMap, ok := msgData.(map[string]any)
		if !ok {
			return mcp.NewToolResultError("message must be a JSON object"), nil
		}
		result := s.contractEngine.ValidateMessage(topic, msgMap)
		s.logAudit(agentID, "kafka_validate_contract", args, cluster, "success", 1, nil, start)
		return marshalResult(result)

	case "get":
		if topic == "" {
			return mcp.NewToolResultError("topic is required for get action"), nil
		}
		c, err := s.contractEngine.GetContract(topic)
		if err != nil {
			return s.toolError("contract not found", err), nil
		}
		s.logAudit(agentID, "kafka_validate_contract", args, cluster, "success", 1, nil, start)
		return marshalResult(c)

	case "list":
		result := s.contractEngine.ListContracts()
		s.logAudit(agentID, "kafka_validate_contract", args, cluster, "success", len(result), nil, start)
		return marshalResult(map[string]any{"contracts": result, "total": len(result)})

	case "compliance":
		if topic == "" {
			return mcp.NewToolResultError("topic is required for compliance action"), nil
		}
		report, err := s.contractEngine.GetComplianceReport(topic)
		if err != nil {
			return s.toolError("compliance report failed", err), nil
		}
		s.logAudit(agentID, "kafka_validate_contract", args, cluster, "success", 1, nil, start)
		return marshalResult(report)

	default:
		return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: validate, register, get, list, compliance", action)), nil
	}
}

func (s *Server) handleDiagnoseLag(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	args := request.GetArguments()

	if deny := s.checkRateLimit(agentID, "kafka_diagnose_lag"); deny != nil {
		return deny, nil
	}

	groupID := request.GetString("group_id", "")
	cluster := request.GetString("cluster", "")

	if groupID == "" {
		return mcp.NewToolResultError("group_id is required"), nil
	}

	if deny := s.checkAuth(agentID, "kafka_diagnose_lag", auth.ResourceConsumerGroup, groupID, auth.ActionRead); deny != nil {
		return deny, nil
	}

	// Get current group details for lag info
	groupDetail, err := s.kafkaManager.DescribeConsumerGroup(ctx, cluster, groupID)
	if err != nil {
		s.logAudit(agentID, "kafka_diagnose_lag", args, cluster, "error", 0, err, start)
		return s.toolError("failed to describe consumer group", err), nil
	}

	// Build partition lag snapshots
	var partitionLags []lagdiag.LagSnapshot
	for _, offset := range groupDetail.Offsets {
		partitionLags = append(partitionLags, lagdiag.LagSnapshot{
			GroupID:        groupID,
			Topic:          offset.Topic,
			Partition:      offset.Partition,
			Lag:            offset.Lag,
			ConsumerOffset: offset.CurrentOffset,
			LogEndOffset:   offset.LogEndOffset,
		})
	}

	diagnosis := s.lagAnalyzer.Diagnose(groupID, groupDetail.TotalLag, partitionLags)

	s.logAudit(agentID, "kafka_diagnose_lag", args, cluster, "success", 1, nil, start)
	return marshalResult(diagnosis)
}

func (s *Server) handleMigrationPlan(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	args := request.GetArguments()

	if deny := s.checkRateLimit(agentID, "kafka_migration_plan"); deny != nil {
		return deny, nil
	}

	action := request.GetString("action", "")
	cluster := request.GetString("cluster", "")

	switch action {
	case "plan":
		sourceCluster := request.GetString("source_cluster", "")
		targetCluster := request.GetString("target_cluster", "")
		if sourceCluster == "" || targetCluster == "" {
			return mcp.NewToolResultError("source_cluster and target_cluster are required"), nil
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
}

func (s *Server) handleTraceLineage(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	args := request.GetArguments()

	if deny := s.checkRateLimit(agentID, "kafka_trace_lineage"); deny != nil {
		return deny, nil
	}

	action := request.GetString("action", "")
	topic := request.GetString("topic", "")
	cluster := request.GetString("cluster", "")

	switch action {
	case "trace":
		if topic == "" {
			return mcp.NewToolResultError("topic is required for trace action"), nil
		}

		// Auto-discover lineage from cluster metadata
		graph, err := s.kafkaManager.GetTopicGraph(ctx, cluster)
		if err == nil {
			for _, node := range graph.Nodes {
				s.lineageTracker.AddNode(lineage.LineageNode{
					ID: node.ID, Name: node.Name, Type: node.Type,
				})
			}
			for _, edge := range graph.Edges {
				s.lineageTracker.AddEdge(lineage.LineageEdge{
					Source: edge.Source, Target: edge.Target, Type: edge.Type,
				})
			}
		}

		result := s.lineageTracker.TraceLineage(topic)
		s.logAudit(agentID, "kafka_trace_lineage", args, cluster, "success", 1, nil, start)
		return marshalResult(result)

	case "annotate_pii":
		if topic == "" {
			return mcp.NewToolResultError("topic is required"), nil
		}
		piiType := request.GetString("pii_type", "email")
		field := request.GetString("field", "")
		if field == "" {
			return mcp.NewToolResultError("field is required for annotate_pii"), nil
		}
		s.lineageTracker.AnnotatePII(lineage.PIIAnnotation{
			Topic: topic, Field: field, PIIType: piiType, DetectedBy: "manual",
		})
		s.logAudit(agentID, "kafka_trace_lineage", args, cluster, "success", 1, nil, start)
		return marshalResult(map[string]any{"status": "annotated", "topic": topic, "field": field, "pii_type": piiType})

	case "detect_pii":
		if topic == "" {
			return mcp.NewToolResultError("topic is required"), nil
		}
		// Auto-detect from topic metadata
		detected := s.lineageTracker.DetectPIIPatterns(topic, nil)
		for _, pii := range detected {
			s.lineageTracker.AnnotatePII(pii)
		}
		s.logAudit(agentID, "kafka_trace_lineage", args, cluster, "success", len(detected), nil, start)
		return marshalResult(map[string]any{"topic": topic, "detected": detected, "count": len(detected)})

	case "compliance_report":
		framework := request.GetString("framework", "GDPR")
		report := s.lineageTracker.GenerateComplianceReport(framework)
		s.logAudit(agentID, "kafka_trace_lineage", args, cluster, "success", 1, nil, start)
		return marshalResult(report)

	default:
		return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: trace, annotate_pii, detect_pii, compliance_report", action)), nil
	}
}

func (s *Server) handleGroupHealth(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	args := request.GetArguments()

	if deny := s.checkRateLimit(agentID, "kafka_group_health"); deny != nil {
		return deny, nil
	}

	groupID := request.GetString("group_id", "")
	cluster := request.GetString("cluster", "")

	if groupID == "" {
		return mcp.NewToolResultError("group_id is required"), nil
	}

	if deny := s.checkAuth(agentID, "kafka_group_health", auth.ResourceConsumerGroup, groupID, auth.ActionRead); deny != nil {
		return deny, nil
	}

	groupDetail, err := s.kafkaManager.DescribeConsumerGroup(ctx, cluster, groupID)
	if err != nil {
		s.logAudit(agentID, "kafka_group_health", args, cluster, "error", 0, err, start)
		return s.toolError("failed to describe consumer group", err), nil
	}

	health := s.selfHealMon.EvaluateHealth(
		groupID,
		groupDetail.TotalLag,
		len(groupDetail.Members),
		0, // commit rate not directly available
		0, // rebalance count not directly available
	)

	s.logAudit(agentID, "kafka_group_health", args, cluster, "success", 1, nil, start)
	return marshalResult(health)
}

func (s *Server) handlePersona(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	args := request.GetArguments()

	if deny := s.checkRateLimit(agentID, "kafka_persona"); deny != nil {
		return deny, nil
	}

	action := request.GetString("action", "")
	personaName := request.GetString("persona", "")

	switch action {
	case "list":
		personas := s.personaEngine.ListPersonas()
		active := s.personaEngine.GetActive()
		var activeName string
		if active != nil {
			activeName = active.Name
		}
		s.logAudit(agentID, "kafka_persona", args, "", "success", len(personas), nil, start)
		return marshalResult(map[string]any{"personas": personas, "active": activeName, "total": len(personas)})

	case "get":
		if personaName == "" {
			return mcp.NewToolResultError("persona name is required"), nil
		}
		p, err := s.personaEngine.GetPersona(personaName)
		if err != nil {
			return s.toolError("persona not found", err), nil
		}
		s.logAudit(agentID, "kafka_persona", args, "", "success", 1, nil, start)
		return marshalResult(p)

	case "activate":
		if personaName == "" {
			return mcp.NewToolResultError("persona name is required"), nil
		}
		if err := s.personaEngine.SetActive(personaName); err != nil {
			return s.toolError("persona activation failed", err), nil
		}
		s.logAudit(agentID, "kafka_persona", args, "", "success", 1, nil, start)
		return marshalResult(map[string]any{"status": "activated", "persona": personaName})

	default:
		return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: list, get, activate", action)), nil
	}
}

func (s *Server) handleForecast(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	args := request.GetArguments()

	if deny := s.checkRateLimit(agentID, "kafka_forecast"); deny != nil {
		return deny, nil
	}

	topic := request.GetString("topic", "")
	metricName := request.GetString("metric", "throughput")
	horizonDays := intFromAny(args["horizon_days"], 7)
	cluster := request.GetString("cluster", "")

	if topic == "" {
		return mcp.NewToolResultError("topic is required"), nil
	}

	if deny := s.checkAuth(agentID, "kafka_forecast", auth.ResourceTopic, topic, auth.ActionRead); deny != nil {
		return deny, nil
	}

	// Record current metrics for the topic
	detail, err := s.kafkaManager.DescribeTopic(ctx, cluster, topic)
	if err != nil {
		s.logAudit(agentID, "kafka_forecast", args, cluster, "error", 0, err, start)
		return s.toolError("failed to describe topic", err), nil
	}

	throughput := estimateThroughput(detail)
	s.forecaster.RecordMetric(forecast.Metric{
		Topic:      topic,
		MetricName: metricName,
		Value:      throughput,
		Timestamp:  time.Now(),
	})

	result, err := s.forecaster.ForecastTopic(topic, metricName, horizonDays)
	if err != nil {
		s.logAudit(agentID, "kafka_forecast", args, cluster, "error", 0, err, start)
		return s.toolError("forecast failed", err), nil
	}

	s.logAudit(agentID, "kafka_forecast", args, cluster, "success", 1, nil, start)
	return marshalResult(result)
}
