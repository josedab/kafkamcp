package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/incident"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerIncidentCorrelationTools() {
	if s.incidentCorrelator == nil {
		return
	}

	s.addTool(mcp.NewTool("kafka_incident_list",
		withCategory("observability"),
		mcp.WithDescription("List correlated incidents across Kafka clusters with root cause analysis."),
		mcp.WithNumber("limit", mcp.Description("Max incidents. Default: 10")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_incident_list",
		resourceType: "cluster",
		action:       "read",
	}, s.handleIncidentList))

	s.addTool(mcp.NewTool("kafka_incident_report",
		withCategory("observability"),
		mcp.WithDescription("Generate a structured incident report with timeline, blast radius, root cause, and remediation steps."),
		mcp.WithString("incident_id", mcp.Required(), mcp.Description("Incident ID")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_incident_report",
		resourceType: "cluster",
		action:       "read",
	}, s.handleIncidentReport))

	s.addTool(mcp.NewTool("kafka_incident_ingest",
		withCategory("observability"),
		mcp.WithDescription("Ingest an anomaly event for cross-cluster correlation."),
		mcp.WithString("cluster", mcp.Required(), mcp.Description("Cluster name")),
		mcp.WithString("topic", mcp.Description("Affected topic")),
		mcp.WithString("type", mcp.Required(), mcp.Description("Anomaly type: consumer_lag, throughput_drop, error_spike, broker_down")),
		mcp.WithString("severity", mcp.Description("Severity: info, warning, critical. Default: warning")),
		mcp.WithNumber("value", mcp.Description("Metric value")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_incident_ingest",
		resourceType: "cluster",
		action:       "write",
	}, s.handleIncidentIngest))

	s.addTool(mcp.NewTool("kafka_incident_stats",
		withCategory("observability"),
		mcp.WithDescription("Get incident correlation engine statistics."),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_incident_stats",
		resourceType: "cluster",
		action:       "read",
	}, s.handleIncidentStats))
}

func (s *Server) handleIncidentList(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	limit := 10
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}
	if limit > MaxDLQMessages {
		limit = MaxDLQMessages
	}

	incidents := s.incidentCorrelator.GetIncidents(limit)
	s.logAudit(agentID, "kafka_incident_list", args, "", "success", len(incidents), nil, start)
	return marshalResult(map[string]any{
		"incidents": incidents,
		"total":     len(incidents),
	})
}

func (s *Server) handleIncidentReport(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	incidentID, _ := args["incident_id"].(string)
	if incidentID == "" {
		return mcp.NewToolResultError("incident_id is required"), nil
	}

	report, err := s.incidentCorrelator.GenerateReport(incidentID)
	if err != nil {
		s.logAudit(agentID, "kafka_incident_report", args, "", "error", 0, err, start)
		return s.toolError("failed to generate incident report", err), nil
	}

	s.logAudit(agentID, "kafka_incident_report", args, "", "success", 1, nil, start)
	return marshalResult(report)
}

func (s *Server) handleIncidentIngest(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	cluster, _ := args["cluster"].(string)
	anomalyType, _ := args["type"].(string)
	if cluster == "" || anomalyType == "" {
		return mcp.NewToolResultError("cluster and type are required"), nil
	}

	severity := incident.SeverityWarning
	if s, ok := args["severity"].(string); ok && s != "" {
		severity = incident.Severity(s)
	}

	event := incident.AnomalyEvent{
		Cluster:   cluster,
		Type:      anomalyType,
		Severity:  severity,
		Timestamp: time.Now(),
	}
	if topic, ok := args["topic"].(string); ok {
		event.Topic = topic
	}
	if val, ok := args["value"].(float64); ok {
		event.Value = val
	}

	result := s.incidentCorrelator.IngestEvent(event)

	response := map[string]any{
		"status": "ingested",
	}
	if result != nil {
		response["incident_created"] = true
		response["incident_id"] = result.ID
		response["severity"] = result.Severity
		response["blast_radius"] = result.BlastRadius
	}

	s.logAudit(agentID, "kafka_incident_ingest", args, "", "success", 1, nil, start)
	return marshalResult(response)
}

func (s *Server) handleIncidentStats(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	stats := s.incidentCorrelator.Stats()
	s.logAudit(agentID, "kafka_incident_stats", args, "", "success", 1, nil, start)
	return marshalResult(stats)
}
