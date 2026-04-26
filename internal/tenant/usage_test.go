package tenant

import (
	"encoding/csv"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestUsageTrackerRecord(t *testing.T) {
	ut := NewUsageTracker()

	rec := UsageRecord{
		TenantID: "t1",
		AgentID:  "agent-1",
		ToolName: "produce",
		Cluster:  "cluster-a",
		Messages: 10,
		Bytes:    1024,
	}
	ut.Record(rec)

	ut.mu.RLock()
	defer ut.mu.RUnlock()

	if len(ut.records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(ut.records))
	}
	if ut.records[0].TenantID != "t1" {
		t.Errorf("expected tenant t1, got %s", ut.records[0].TenantID)
	}
	if ut.records[0].Timestamp.IsZero() {
		t.Error("expected timestamp to be set automatically")
	}
}

func TestUsageTrackerRecord_ExplicitTimestamp(t *testing.T) {
	ut := NewUsageTracker()
	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	ut.Record(UsageRecord{TenantID: "t1", ToolName: "consume", Timestamp: ts})

	ut.mu.RLock()
	defer ut.mu.RUnlock()
	if !ut.records[0].Timestamp.Equal(ts) {
		t.Errorf("expected explicit timestamp preserved, got %v", ut.records[0].Timestamp)
	}
}

func TestUsageTrackerRecord_MaxRecordsCap(t *testing.T) {
	ut := NewUsageTracker()
	ut.maxRecords = 10
	for i := 0; i < 15; i++ {
		ut.Record(UsageRecord{TenantID: "t1", Messages: int64(i)})
	}
	ut.mu.RLock()
	defer ut.mu.RUnlock()
	if len(ut.records) != 10 {
		t.Errorf("expected max 10 records, got %d", len(ut.records))
	}
	// oldest should have been dropped, first record should have Messages=5
	if ut.records[0].Messages != 5 {
		t.Errorf("expected first record Messages=5 after trim, got %d", ut.records[0].Messages)
	}
}

func TestGenerateRollup(t *testing.T) {
	ut := NewUsageTracker()
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 30, 23, 59, 59, 0, time.UTC)
	mid := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	ut.Record(UsageRecord{TenantID: "t1", AgentID: "a1", ToolName: "produce", Messages: 10, Bytes: 100, Timestamp: mid})
	ut.Record(UsageRecord{TenantID: "t1", AgentID: "a2", ToolName: "consume", Messages: 20, Bytes: 200, Timestamp: mid})
	ut.Record(UsageRecord{TenantID: "t1", AgentID: "a1", ToolName: "produce", Messages: 5, Bytes: 50, Timestamp: mid})
	// different tenant, should be excluded
	ut.Record(UsageRecord{TenantID: "t2", AgentID: "a3", ToolName: "produce", Messages: 99, Bytes: 999, Timestamp: mid})

	rollup := ut.GenerateRollup("t1", "monthly", start, end)

	if rollup.TenantID != "t1" {
		t.Errorf("expected tenant t1, got %s", rollup.TenantID)
	}
	if rollup.TotalToolCalls != 3 {
		t.Errorf("expected 3 tool calls, got %d", rollup.TotalToolCalls)
	}
	if rollup.TotalMessages != 35 {
		t.Errorf("expected 35 messages, got %d", rollup.TotalMessages)
	}
	if rollup.TotalBytes != 350 {
		t.Errorf("expected 350 bytes, got %d", rollup.TotalBytes)
	}
	if rollup.UniqueAgents != 2 {
		t.Errorf("expected 2 unique agents, got %d", rollup.UniqueAgents)
	}
	if rollup.Period != "monthly" {
		t.Errorf("expected period 'monthly', got %s", rollup.Period)
	}
}

func TestGenerateRollup_Empty(t *testing.T) {
	ut := NewUsageTracker()
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 30, 23, 59, 59, 0, time.UTC)

	rollup := ut.GenerateRollup("t1", "daily", start, end)

	if rollup.TotalToolCalls != 0 {
		t.Errorf("expected 0 tool calls, got %d", rollup.TotalToolCalls)
	}
	if rollup.UniqueAgents != 0 {
		t.Errorf("expected 0 unique agents, got %d", rollup.UniqueAgents)
	}
	if rollup.ByTool == nil {
		t.Error("expected ByTool map to be initialized")
	}
	if rollup.ByAgent == nil {
		t.Error("expected ByAgent map to be initialized")
	}
}

func TestGenerateRollup_ByToolAndByAgent(t *testing.T) {
	ut := NewUsageTracker()
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 30, 23, 59, 59, 0, time.UTC)
	mid := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	ut.Record(UsageRecord{TenantID: "t1", AgentID: "a1", ToolName: "produce", Timestamp: mid})
	ut.Record(UsageRecord{TenantID: "t1", AgentID: "a1", ToolName: "produce", Timestamp: mid})
	ut.Record(UsageRecord{TenantID: "t1", AgentID: "a2", ToolName: "consume", Timestamp: mid})

	rollup := ut.GenerateRollup("t1", "monthly", start, end)

	if rollup.ByTool["produce"] != 2 {
		t.Errorf("expected ByTool[produce]=2, got %d", rollup.ByTool["produce"])
	}
	if rollup.ByTool["consume"] != 1 {
		t.Errorf("expected ByTool[consume]=1, got %d", rollup.ByTool["consume"])
	}
	if rollup.ByAgent["a1"] != 2 {
		t.Errorf("expected ByAgent[a1]=2, got %d", rollup.ByAgent["a1"])
	}
	if rollup.ByAgent["a2"] != 1 {
		t.Errorf("expected ByAgent[a2]=1, got %d", rollup.ByAgent["a2"])
	}
}

func TestGenerateRollup_OutOfRange(t *testing.T) {
	ut := NewUsageTracker()
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 30, 23, 59, 59, 0, time.UTC)

	// record before range
	ut.Record(UsageRecord{TenantID: "t1", ToolName: "produce", Timestamp: time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)})
	// record after range
	ut.Record(UsageRecord{TenantID: "t1", ToolName: "produce", Timestamp: time.Date(2024, 7, 15, 0, 0, 0, 0, time.UTC)})

	rollup := ut.GenerateRollup("t1", "monthly", start, end)
	if rollup.TotalToolCalls != 0 {
		t.Errorf("expected 0 tool calls for out-of-range records, got %d", rollup.TotalToolCalls)
	}
}

func TestGenerateChargebackReport_ProPlan(t *testing.T) {
	ut := NewUsageTracker()
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 30, 23, 59, 59, 0, time.UTC)
	mid := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	tenant := &Tenant{ID: "t1", Name: "Pro Corp", Plan: "pro"}

	ut.Record(UsageRecord{TenantID: "t1", AgentID: "a1", ToolName: "produce", Messages: 1000, Bytes: 1024 * 1024 * 1024, Timestamp: mid})

	report := ut.GenerateChargebackReport(tenant, "monthly", start, end)

	if report.TenantID != "t1" {
		t.Errorf("expected tenant t1, got %s", report.TenantID)
	}
	if report.TenantName != "Pro Corp" {
		t.Errorf("expected name 'Pro Corp', got %s", report.TenantName)
	}
	if report.Plan != "pro" {
		t.Errorf("expected plan 'pro', got %s", report.Plan)
	}

	// Pro pricing: CostPerToolCall=0.001, CostPerMessage=0.0001, CostPerGBStorage=0.10, MonthlyBase=49.0
	// 1 tool call * 0.001 = 0.001
	if report.Cost.ToolCallCost < 0.0009 || report.Cost.ToolCallCost > 0.0011 {
		t.Errorf("expected tool call cost ~0.001, got %f", report.Cost.ToolCallCost)
	}
	// 1000 messages * 0.0001 = 0.1
	if report.Cost.MessageCost < 0.099 || report.Cost.MessageCost > 0.101 {
		t.Errorf("expected message cost ~0.1, got %f", report.Cost.MessageCost)
	}
	// 1GB * 0.10 = 0.10
	if report.Cost.StorageCost < 0.099 || report.Cost.StorageCost > 0.101 {
		t.Errorf("expected storage cost ~0.10, got %f", report.Cost.StorageCost)
	}
	if report.Cost.Currency != "USD" {
		t.Errorf("expected currency USD, got %s", report.Cost.Currency)
	}
	// TotalCost = 0.001 + 0.1 + 0.1 + 49.0 = 49.201
	if report.Cost.TotalCost < 49.0 {
		t.Errorf("expected total cost > 49.0 (includes monthly base), got %f", report.Cost.TotalCost)
	}

	// Budget: limit = 49*2 = 98
	if report.BudgetStatus.BudgetLimit != 98.0 {
		t.Errorf("expected budget limit 98.0, got %f", report.BudgetStatus.BudgetLimit)
	}
	if report.Forecast.ForecastPeriod != "monthly" {
		t.Errorf("expected forecast period 'monthly', got %s", report.Forecast.ForecastPeriod)
	}
}

func TestGenerateChargebackReport_FreePlan(t *testing.T) {
	ut := NewUsageTracker()
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 30, 23, 59, 59, 0, time.UTC)
	mid := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	tenant := &Tenant{ID: "t2", Name: "Free Corp", Plan: "free"}

	ut.Record(UsageRecord{TenantID: "t2", AgentID: "a1", ToolName: "produce", Messages: 500, Bytes: 512, Timestamp: mid})

	report := ut.GenerateChargebackReport(tenant, "monthly", start, end)

	// Free pricing: all costs are 0, MonthlyBase=0
	if report.Cost.ToolCallCost != 0 {
		t.Errorf("expected 0 tool call cost for free plan, got %f", report.Cost.ToolCallCost)
	}
	if report.Cost.MessageCost != 0 {
		t.Errorf("expected 0 message cost for free plan, got %f", report.Cost.MessageCost)
	}
	if report.Cost.StorageCost != 0 {
		t.Errorf("expected 0 storage cost for free plan, got %f", report.Cost.StorageCost)
	}
	// TotalCost = 0 + 0 + 0 + 0 (monthly base) = 0
	if report.Cost.TotalCost != 0 {
		t.Errorf("expected 0 total cost for free plan, got %f", report.Cost.TotalCost)
	}
	// Budget default for free: 100
	if report.BudgetStatus.BudgetLimit != 100 {
		t.Errorf("expected budget limit 100 for free plan, got %f", report.BudgetStatus.BudgetLimit)
	}
	if !report.BudgetStatus.OnTrack {
		t.Error("expected free plan with zero cost to be on track")
	}
}

func TestGenerateChargebackReport_UnknownPlan(t *testing.T) {
	ut := NewUsageTracker()
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 30, 23, 59, 59, 0, time.UTC)

	tenant := &Tenant{ID: "t3", Name: "Unknown Corp", Plan: "custom"}
	report := ut.GenerateChargebackReport(tenant, "monthly", start, end)

	// Unknown plans fall back to free pricing
	if report.Cost.TotalCost != 0 {
		t.Errorf("expected 0 total cost for unknown plan (free fallback), got %f", report.Cost.TotalCost)
	}
}

func TestExportCSV(t *testing.T) {
	ut := NewUsageTracker()
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 30, 23, 59, 59, 0, time.UTC)
	ts1 := time.Date(2024, 6, 10, 8, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 6, 20, 16, 0, 0, 0, time.UTC)

	ut.Record(UsageRecord{TenantID: "t1", AgentID: "a1", ToolName: "produce", Cluster: "c1", Messages: 10, Bytes: 100, Timestamp: ts1})
	ut.Record(UsageRecord{TenantID: "t1", AgentID: "a2", ToolName: "consume", Cluster: "c2", Messages: 20, Bytes: 200, Timestamp: ts2})
	// different tenant, should be excluded
	ut.Record(UsageRecord{TenantID: "t2", AgentID: "a3", ToolName: "produce", Cluster: "c1", Messages: 99, Bytes: 999, Timestamp: ts1})

	result, err := ut.ExportCSV("t1", start, end)
	if err != nil {
		t.Fatalf("ExportCSV returned error: %v", err)
	}

	r := csv.NewReader(strings.NewReader(result))
	rows, err2 := r.ReadAll()
	if err2 != nil {
		t.Fatalf("failed to parse CSV: %v", err2)
	}

	// header + 2 data rows
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows (1 header + 2 data), got %d", len(rows))
	}

	// verify header
	expectedHeader := []string{"timestamp", "tenant_id", "agent_id", "tool_name", "cluster", "messages", "bytes"}
	for i, h := range expectedHeader {
		if rows[0][i] != h {
			t.Errorf("header[%d]: expected %q, got %q", i, h, rows[0][i])
		}
	}

	// verify first data row
	if rows[1][1] != "t1" {
		t.Errorf("expected tenant_id t1, got %s", rows[1][1])
	}
	if rows[1][3] != "produce" {
		t.Errorf("expected tool_name produce, got %s", rows[1][3])
	}
	if rows[1][5] != "10" {
		t.Errorf("expected messages 10, got %s", rows[1][5])
	}
}

func TestExportCSV_Empty(t *testing.T) {
	ut := NewUsageTracker()
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 30, 23, 59, 59, 0, time.UTC)

	result, err := ut.ExportCSV("t1", start, end)
	if err != nil {
		t.Fatalf("ExportCSV returned error: %v", err)
	}

	r := csv.NewReader(strings.NewReader(result))
	rows, err2 := r.ReadAll()
	if err2 != nil {
		t.Fatalf("failed to parse CSV: %v", err2)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row (header only), got %d", len(rows))
	}
}

func TestExportJSON(t *testing.T) {
	ut := NewUsageTracker()
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 30, 23, 59, 59, 0, time.UTC)
	ts1 := time.Date(2024, 6, 10, 8, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 6, 20, 16, 0, 0, 0, time.UTC)

	ut.Record(UsageRecord{TenantID: "t1", AgentID: "a1", ToolName: "produce", Cluster: "c1", Messages: 10, Bytes: 100, Timestamp: ts1})
	ut.Record(UsageRecord{TenantID: "t1", AgentID: "a2", ToolName: "consume", Cluster: "c2", Messages: 20, Bytes: 200, Timestamp: ts2})
	// different tenant
	ut.Record(UsageRecord{TenantID: "t2", AgentID: "a3", ToolName: "produce", Messages: 99, Timestamp: ts1})

	result, err := ut.ExportJSON("t1", start, end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var records []UsageRecord
	if err := json.Unmarshal([]byte(result), &records); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	// should be sorted by timestamp
	if !records[0].Timestamp.Before(records[1].Timestamp) {
		t.Error("expected records sorted by timestamp ascending")
	}
	if records[0].ToolName != "produce" {
		t.Errorf("expected first record tool 'produce', got %s", records[0].ToolName)
	}
	if records[1].ToolName != "consume" {
		t.Errorf("expected second record tool 'consume', got %s", records[1].ToolName)
	}
}

func TestExportJSON_Empty(t *testing.T) {
	ut := NewUsageTracker()
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 30, 23, 59, 59, 0, time.UTC)

	result, err := ut.ExportJSON("t1", start, end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "null" {
		// json.MarshalIndent of nil slice returns "null"
		t.Errorf("expected 'null' for empty export, got %s", result)
	}
}
