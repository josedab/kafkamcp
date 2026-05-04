package telemetry

import (
	"testing"
)

func TestDataLake_Record(t *testing.T) {
	t.Parallel()
	dl := NewDataLake(1000)
	id := dl.Record("agent-1", "kafka_list_topics", "cluster-1", "success", 50, 10, nil, "", "")
	if id == "" {
		t.Fatal("expected non-empty ID")
	}
}

func TestDataLake_Stats(t *testing.T) {
	t.Parallel()
	dl := NewDataLake(1000)
	dl.Record("agent-1", "kafka_list_topics", "cluster-1", "success", 50, 10, nil, "", "")
	dl.Record("agent-2", "kafka_produce", "cluster-1", "success", 100, 1, nil, "", "")
	dl.Record("agent-1", "kafka_consume", "cluster-2", "error", 200, 0, nil, "timeout", "")

	stats := dl.GetStats()
	if stats.TotalRecords != 3 {
		t.Errorf("expected 3 records, got %d", stats.TotalRecords)
	}
	if stats.UniqueAgents != 2 {
		t.Errorf("expected 2 agents, got %d", stats.UniqueAgents)
	}
	if stats.UniqueTools != 3 {
		t.Errorf("expected 3 tools, got %d", stats.UniqueTools)
	}
	if stats.RecordsByStatus["success"] != 2 {
		t.Errorf("expected 2 success, got %d", stats.RecordsByStatus["success"])
	}
}

func TestDataLake_AgentReport(t *testing.T) {
	t.Parallel()
	dl := NewDataLake(1000)
	dl.Record("agent-1", "kafka_list_topics", "c1", "success", 50, 10, nil, "", "")
	dl.Record("agent-1", "kafka_produce", "c1", "success", 100, 1, nil, "", "")
	dl.Record("agent-1", "kafka_consume", "c1", "error", 500, 0, nil, "timeout", "")

	report := dl.AgentReport("agent-1")
	if report.TotalCalls != 3 {
		t.Errorf("expected 3 calls, got %d", report.TotalCalls)
	}
	if report.SuccessRate < 60 || report.SuccessRate > 70 {
		t.Errorf("expected ~66.7%% success rate, got %.1f%%", report.SuccessRate)
	}
	if len(report.TopTools) != 3 {
		t.Errorf("expected 3 tools, got %d", len(report.TopTools))
	}
	if len(report.ErrorPatterns) != 1 {
		t.Errorf("expected 1 error pattern, got %d", len(report.ErrorPatterns))
	}
}

func TestDataLake_ExportJSON(t *testing.T) {
	t.Parallel()
	dl := NewDataLake(1000)
	dl.Record("agent-1", "kafka_list_topics", "c1", "success", 50, 10, nil, "", "")

	result, err := dl.Export(ExportOptions{Format: FormatJSON})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RecordCount != 1 {
		t.Errorf("expected 1 record, got %d", result.RecordCount)
	}
	if result.SizeBytes == 0 {
		t.Error("expected non-zero size")
	}
}

func TestDataLake_ExportCSV(t *testing.T) {
	t.Parallel()
	dl := NewDataLake(1000)
	dl.Record("agent-1", "tool1", "c1", "success", 50, 10, nil, "", "")
	dl.Record("agent-2", "tool2", "c2", "error", 100, 0, nil, "timeout", "")

	result, err := dl.Export(ExportOptions{Format: FormatCSV})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RecordCount != 2 {
		t.Errorf("expected 2 records, got %d", result.RecordCount)
	}
}

func TestDataLake_ExportJSONL(t *testing.T) {
	t.Parallel()
	dl := NewDataLake(1000)
	dl.Record("agent-1", "tool1", "c1", "success", 50, 5, nil, "", "")

	result, err := dl.Export(ExportOptions{Format: FormatJSONL})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RecordCount != 1 {
		t.Errorf("expected 1 record, got %d", result.RecordCount)
	}
}

func TestDataLake_ExportFiltered(t *testing.T) {
	t.Parallel()
	dl := NewDataLake(1000)
	dl.Record("agent-1", "tool1", "c1", "success", 50, 10, nil, "", "")
	dl.Record("agent-2", "tool1", "c1", "success", 100, 5, nil, "", "")

	result, err := dl.Export(ExportOptions{Format: FormatJSON, AgentID: "agent-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RecordCount != 1 {
		t.Errorf("expected 1 filtered record, got %d", result.RecordCount)
	}
}

func TestDataLake_Query(t *testing.T) {
	t.Parallel()
	dl := NewDataLake(1000)
	dl.Record("agent-1", "tool1", "c1", "success", 50, 10, nil, "", "")
	dl.Record("agent-1", "tool2", "c1", "error", 100, 0, nil, "timeout", "")
	dl.Record("agent-2", "tool1", "c2", "success", 75, 5, nil, "", "")

	results := dl.Query("agent-1", "", "", "", 10)
	if len(results) != 2 {
		t.Errorf("expected 2 results for agent-1, got %d", len(results))
	}

	results = dl.Query("", "tool1", "", "", 10)
	if len(results) != 2 {
		t.Errorf("expected 2 results for tool1, got %d", len(results))
	}
}

func TestDataLake_RingBuffer(t *testing.T) {
	t.Parallel()
	dl := NewDataLake(5)
	for i := 0; i < 10; i++ {
		dl.Record("agent-1", "tool1", "c1", "success", 50, 1, nil, "", "")
	}
	stats := dl.GetStats()
	if stats.TotalRecords != 5 {
		t.Errorf("expected 5 records after eviction, got %d", stats.TotalRecords)
	}
}
