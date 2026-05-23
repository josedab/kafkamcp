package telemetry

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// InteractionRecord captures a single agent interaction with Kafka.
type InteractionRecord struct {
	ID          string         `json:"id"`
	Timestamp   time.Time      `json:"timestamp"`
	AgentID     string         `json:"agent_id"`
	Tool        string         `json:"tool"`
	Cluster     string         `json:"cluster"`
	Status      string         `json:"status"` // success, error
	LatencyMs   int64          `json:"latency_ms"`
	ResultCount int            `json:"result_count"`
	Parameters  map[string]any `json:"parameters,omitempty"`
	ErrorType   string         `json:"error_type,omitempty"`
	SessionID   string         `json:"session_id,omitempty"`
}

// DataLakeStats provides statistics about the telemetry data lake.
type DataLakeStats struct {
	TotalRecords    int            `json:"total_records"`
	UniqueAgents    int            `json:"unique_agents"`
	UniqueTools     int            `json:"unique_tools"`
	UniqueClusters  int            `json:"unique_clusters"`
	OldestRecord    string         `json:"oldest_record,omitempty"`
	NewestRecord    string         `json:"newest_record,omitempty"`
	RecordsByAgent  map[string]int `json:"records_by_agent"`
	RecordsByTool   map[string]int `json:"records_by_tool"`
	RecordsByStatus map[string]int `json:"records_by_status"`
	AvgLatencyMs    float64        `json:"avg_latency_ms"`
}

// AgentBehaviorReport summarizes an agent's interaction patterns.
type AgentBehaviorReport struct {
	AgentID       string         `json:"agent_id"`
	TotalCalls    int            `json:"total_calls"`
	SuccessRate   float64        `json:"success_rate"`
	AvgLatencyMs  float64        `json:"avg_latency_ms"`
	P95LatencyMs  int64          `json:"p95_latency_ms"`
	TopTools      []ToolUsage    `json:"top_tools"`
	TopClusters   []ClusterUsage `json:"top_clusters"`
	ErrorPatterns []ErrorPattern `json:"error_patterns"`
	ActivePeriods []string       `json:"active_periods"`
	FirstSeen     string         `json:"first_seen"`
	LastSeen      string         `json:"last_seen"`
}

// ToolUsage tracks usage count for a tool.
type ToolUsage struct {
	Tool  string `json:"tool"`
	Count int    `json:"count"`
}

// ClusterUsage tracks usage count for a cluster.
type ClusterUsage struct {
	Cluster string `json:"cluster"`
	Count   int    `json:"count"`
}

// ErrorPattern captures recurring error patterns.
type ErrorPattern struct {
	ErrorType string `json:"error_type"`
	Count     int    `json:"count"`
	LastSeen  string `json:"last_seen"`
}

// ExportFormat defines the output format for data lake exports.
type ExportFormat string

// FormatJSON and related values define constants for this package.
const (
	FormatJSON  ExportFormat = "json"
	FormatJSONL ExportFormat = "jsonl"
	FormatCSV   ExportFormat = "csv"
)

// ExportOptions configures data lake export.
type ExportOptions struct {
	Format    ExportFormat `json:"format"`
	AgentID   string       `json:"agent_id,omitempty"`
	Tool      string       `json:"tool,omitempty"`
	Cluster   string       `json:"cluster,omitempty"`
	StartTime *time.Time   `json:"start_time,omitempty"`
	EndTime   *time.Time   `json:"end_time,omitempty"`
	Limit     int          `json:"limit"`
}

// ExportResult contains the exported data.
type ExportResult struct {
	Format      ExportFormat `json:"format"`
	RecordCount int          `json:"record_count"`
	Data        string       `json:"data"`
	SizeBytes   int          `json:"size_bytes"`
	ExportedAt  string       `json:"exported_at"`
}

// DataLake stores and manages agent interaction telemetry.
type DataLake struct {
	mu         sync.RWMutex
	records    []InteractionRecord
	maxRecords int
	nextID     int64
}

// NewDataLake creates a new telemetry data lake.
func NewDataLake(maxRecords int) *DataLake {
	if maxRecords <= 0 {
		maxRecords = 100000
	}
	return &DataLake{
		records:    make([]InteractionRecord, 0, 1024),
		maxRecords: maxRecords,
	}
}

// Record adds an interaction record to the data lake.
func (dl *DataLake) Record(agentID, tool, cluster, status string, latencyMs int64, resultCount int, params map[string]any, errorType, sessionID string) string {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	dl.nextID++
	id := fmt.Sprintf("rec_%d", dl.nextID)

	record := InteractionRecord{
		ID:          id,
		Timestamp:   time.Now().UTC(),
		AgentID:     agentID,
		Tool:        tool,
		Cluster:     cluster,
		Status:      status,
		LatencyMs:   latencyMs,
		ResultCount: resultCount,
		Parameters:  params,
		ErrorType:   errorType,
		SessionID:   sessionID,
	}

	dl.records = append(dl.records, record)

	// Ring buffer eviction
	if len(dl.records) > dl.maxRecords {
		dl.records = dl.records[len(dl.records)-dl.maxRecords:]
	}

	return id
}

// GetStats returns aggregate statistics about the data lake.
func (dl *DataLake) GetStats() *DataLakeStats {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	stats := &DataLakeStats{
		TotalRecords:    len(dl.records),
		RecordsByAgent:  make(map[string]int),
		RecordsByTool:   make(map[string]int),
		RecordsByStatus: make(map[string]int),
	}

	agents := make(map[string]bool)
	tools := make(map[string]bool)
	clusters := make(map[string]bool)
	var totalLatency int64

	for i, r := range dl.records {
		agents[r.AgentID] = true
		tools[r.Tool] = true
		if r.Cluster != "" {
			clusters[r.Cluster] = true
		}
		stats.RecordsByAgent[r.AgentID]++
		stats.RecordsByTool[r.Tool]++
		stats.RecordsByStatus[r.Status]++
		totalLatency += r.LatencyMs

		if i == 0 {
			stats.OldestRecord = r.Timestamp.Format(time.RFC3339)
		}
		stats.NewestRecord = r.Timestamp.Format(time.RFC3339)
	}

	stats.UniqueAgents = len(agents)
	stats.UniqueTools = len(tools)
	stats.UniqueClusters = len(clusters)
	if stats.TotalRecords > 0 {
		stats.AvgLatencyMs = float64(totalLatency) / float64(stats.TotalRecords)
	}

	return stats
}

// AgentReport generates a behavior report for a specific agent.
func (dl *DataLake) AgentReport(agentID string) *AgentBehaviorReport {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	report := &AgentBehaviorReport{
		AgentID: agentID,
	}

	toolCounts := make(map[string]int)
	clusterCounts := make(map[string]int)
	errorCounts := make(map[string]string) // errorType -> last seen
	errorFreq := make(map[string]int)
	var latencies []int64
	var successCount int

	for _, r := range dl.records {
		if r.AgentID != agentID {
			continue
		}
		report.TotalCalls++
		toolCounts[r.Tool]++
		if r.Cluster != "" {
			clusterCounts[r.Cluster]++
		}
		latencies = append(latencies, r.LatencyMs)
		if r.Status == "success" {
			successCount++
		}
		if r.ErrorType != "" {
			errorCounts[r.ErrorType] = r.Timestamp.Format(time.RFC3339)
			errorFreq[r.ErrorType]++
		}
		if report.FirstSeen == "" {
			report.FirstSeen = r.Timestamp.Format(time.RFC3339)
		}
		report.LastSeen = r.Timestamp.Format(time.RFC3339)
	}

	if report.TotalCalls == 0 {
		return report
	}

	report.SuccessRate = float64(successCount) / float64(report.TotalCalls) * 100

	// Average and P95 latency
	var totalLat int64
	for _, l := range latencies {
		totalLat += l
	}
	report.AvgLatencyMs = float64(totalLat) / float64(len(latencies))

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p95Idx := int(float64(len(latencies)) * 0.95)
	if p95Idx >= len(latencies) {
		p95Idx = len(latencies) - 1
	}
	report.P95LatencyMs = latencies[p95Idx]

	// Top tools
	for tool, count := range toolCounts {
		report.TopTools = append(report.TopTools, ToolUsage{Tool: tool, Count: count})
	}
	sort.Slice(report.TopTools, func(i, j int) bool { return report.TopTools[i].Count > report.TopTools[j].Count })
	if len(report.TopTools) > 10 {
		report.TopTools = report.TopTools[:10]
	}

	// Top clusters
	for cluster, count := range clusterCounts {
		report.TopClusters = append(report.TopClusters, ClusterUsage{Cluster: cluster, Count: count})
	}

	// Error patterns
	for errType, lastSeen := range errorCounts {
		report.ErrorPatterns = append(report.ErrorPatterns, ErrorPattern{
			ErrorType: errType, Count: errorFreq[errType], LastSeen: lastSeen,
		})
	}

	return report
}

// Export exports telemetry data in the specified format.
func (dl *DataLake) Export(opts ExportOptions) (*ExportResult, error) {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	if opts.Format == "" {
		opts.Format = FormatJSON
	}
	if opts.Limit <= 0 {
		opts.Limit = 10000
	}
	if opts.Limit > 100000 {
		opts.Limit = 100000
	}

	var filtered []InteractionRecord
	for _, r := range dl.records {
		if opts.AgentID != "" && r.AgentID != opts.AgentID {
			continue
		}
		if opts.Tool != "" && r.Tool != opts.Tool {
			continue
		}
		if opts.Cluster != "" && r.Cluster != opts.Cluster {
			continue
		}
		if opts.StartTime != nil && r.Timestamp.Before(*opts.StartTime) {
			continue
		}
		if opts.EndTime != nil && r.Timestamp.After(*opts.EndTime) {
			continue
		}
		filtered = append(filtered, r)
		if len(filtered) >= opts.Limit {
			break
		}
	}

	var data string
	switch opts.Format {
	case FormatJSON:
		b, err := json.MarshalIndent(filtered, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("marshaling JSON: %w", err)
		}
		data = string(b)
	case FormatJSONL:
		for _, r := range filtered {
			b, err := json.Marshal(r)
			if err != nil {
				return nil, fmt.Errorf("marshaling record %s: %w", r.ID, err)
			}
			data += string(b) + "\n"
		}
	case FormatCSV:
		data = "id,timestamp,agent_id,tool,cluster,status,latency_ms,result_count,error_type\n"
		for _, r := range filtered {
			data += fmt.Sprintf("%s,%s,%s,%s,%s,%s,%d,%d,%s\n",
				r.ID, r.Timestamp.Format(time.RFC3339), r.AgentID, r.Tool, r.Cluster, r.Status, r.LatencyMs, r.ResultCount, r.ErrorType)
		}
	default:
		return nil, fmt.Errorf("unsupported format: %s", opts.Format)
	}

	return &ExportResult{
		Format:      opts.Format,
		RecordCount: len(filtered),
		Data:        data,
		SizeBytes:   len(data),
		ExportedAt:  time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// Query returns records matching the given filters.
func (dl *DataLake) Query(agentID, tool, cluster, status string, limit int) []InteractionRecord {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	if limit <= 0 {
		limit = 100
	}

	var result []InteractionRecord
	// Iterate in reverse for newest first
	for i := len(dl.records) - 1; i >= 0 && len(result) < limit; i-- {
		r := dl.records[i]
		if agentID != "" && r.AgentID != agentID {
			continue
		}
		if tool != "" && r.Tool != tool {
			continue
		}
		if cluster != "" && r.Cluster != cluster {
			continue
		}
		if status != "" && r.Status != status {
			continue
		}
		result = append(result, r)
	}
	return result
}
