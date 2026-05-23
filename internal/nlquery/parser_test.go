package nlquery

import (
	"log/slog"
	"testing"
)

func newTestEngine() *Engine {
	return NewEngine(slog.Default())
}

func TestParseAverage(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	plan, err := e.Parse("average latency in events", []string{"events"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Intent != "average" {
		t.Errorf("expected intent 'average', got %q", plan.Intent)
	}
	if plan.Aggregation == nil || plan.Aggregation.Function != "avg" {
		t.Error("expected avg aggregation")
	}
	if plan.Aggregation != nil && plan.Aggregation.Field != "latency" {
		t.Errorf("expected field 'latency', got %q", plan.Aggregation.Field)
	}
}

func TestParseGroupBy(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	plan, err := e.Parse("group messages by region", []string{"messages"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Intent != "group_by" {
		t.Errorf("expected intent 'group_by', got %q", plan.Intent)
	}
	if plan.Aggregation == nil || plan.Aggregation.GroupBy != "region" {
		t.Error("expected group_by with field 'region'")
	}
}

func TestParseTopN(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	plan, err := e.Parse("top 5 topics by throughput", []string{"metrics"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Intent != "top_n" {
		t.Errorf("expected intent 'top_n', got %q", plan.Intent)
	}
}

func TestParseFilterErrors(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	plan, err := e.Parse("show errors in app-logs", []string{"app-logs"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Intent != "filter_errors" {
		t.Errorf("expected intent 'filter_errors', got %q", plan.Intent)
	}
	if len(plan.Filters) == 0 {
		t.Fatal("expected at least one filter")
	}
	if plan.Filters[0].Field != "level" || plan.Filters[0].Value != "error" {
		t.Errorf("expected level=error filter, got %+v", plan.Filters[0])
	}
}

func TestParseFilterFailed(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	plan, err := e.Parse("list all failed jobs", []string{"jobs"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Intent != "filter_failed" {
		t.Errorf("expected intent 'filter_failed', got %q", plan.Intent)
	}
	found := false
	for _, f := range plan.Filters {
		if f.Field == "status" && f.Value == "failed" {
			found = true
		}
	}
	if !found {
		t.Error("expected status=failed filter")
	}
}

func TestParseDefaultLimit(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	plan, err := e.Parse("show me orders", []string{"orders"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Limit != 100 {
		t.Errorf("expected default limit 100, got %d", plan.Limit)
	}
}

func TestParseDefaultIntent(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	plan, err := e.Parse("something unknown", []string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Intent != "search" {
		t.Errorf("expected default intent 'search', got %q", plan.Intent)
	}
}

func TestParseRawQueryPreserved(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	query := "show me all orders"
	plan, err := e.Parse(query, []string{"orders"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.RawQuery != query {
		t.Errorf("expected raw query %q, got %q", query, plan.RawQuery)
	}
}

func TestParseDuration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		amount, unit string
		wantMinutes  int
	}{
		{"1", "minute", 1},
		{"5", "minute", 5},
		{"1", "hour", 60},
		{"2", "hour", 120},
		{"1", "day", 1440},
		{"1", "week", 10080},
		{"1", "unknown", 60}, // defaults to hour
	}
	for _, tt := range tests {
		dur := parseDuration(tt.amount, tt.unit)
		gotMinutes := int(dur.Minutes())
		if gotMinutes != tt.wantMinutes {
			t.Errorf("parseDuration(%q, %q) = %d minutes, want %d", tt.amount, tt.unit, gotMinutes, tt.wantMinutes)
		}
	}
}

func TestFuzzyFieldMatchExact(t *testing.T) {
	t.Parallel()
	fields := map[string]string{"status": "string", "order_status": "string"}
	if got := fuzzyFieldMatch("status", fields); got != "status" {
		t.Errorf("exact match failed: got %q", got)
	}
}

func TestFuzzyFieldMatchCaseInsensitive(t *testing.T) {
	t.Parallel()
	fields := map[string]string{"OrderStatus": "string"}
	if got := fuzzyFieldMatch("orderstatus", fields); got != "OrderStatus" {
		t.Errorf("case-insensitive match failed: got %q", got)
	}
}

func TestFuzzyFieldMatchSubstring(t *testing.T) {
	t.Parallel()
	fields := map[string]string{"customer_email": "string"}
	if got := fuzzyFieldMatch("email", fields); got != "customer_email" {
		t.Errorf("substring match failed: got %q", got)
	}
}

func TestFuzzyFieldMatchNoMatch(t *testing.T) {
	t.Parallel()
	fields := map[string]string{"order_id": "string"}
	if got := fuzzyFieldMatch("xyz", fields); got != "" {
		t.Errorf("expected no match, got %q", got)
	}
}

func TestResolveTopicsDirectMatch(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	topics := []string{"user-events", "order-events"}
	matched := e.resolveTopics("show me order-events", topics)
	found := false
	for _, m := range matched {
		if m == "order-events" {
			found = true
		}
	}
	if !found {
		t.Error("expected direct match for 'order-events'")
	}
}

func TestResolveTopicsFuzzyPartialMatch(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	topics := []string{"user-events", "payment-transactions"}
	matched := e.resolveTopics("show me payment data", topics)
	found := false
	for _, m := range matched {
		if m == "payment-transactions" {
			found = true
		}
	}
	if !found {
		t.Error("expected fuzzy match for 'payment-transactions'")
	}
}

func TestResolveTopicsNoMatch(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	topics := []string{"user-events"}
	matched := e.resolveTopics("nothing here", topics)
	if len(matched) != 0 {
		t.Errorf("expected no matches, got %v", matched)
	}
}

func TestResolveFieldsSchemaResolution(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	e.RegisterSchema(SchemaInfo{
		Topic:  "orders",
		Fields: map[string]string{"order_status": "string", "total_amount": "float"},
	})
	plan := &QueryPlan{
		Topics:  []string{"orders"},
		Filters: []Filter{{Field: "status", Operator: "eq", Value: "failed"}},
	}
	e.resolveFields(plan)
	if plan.Filters[0].Field != "order_status" {
		t.Errorf("expected field resolution to 'order_status', got %q", plan.Filters[0].Field)
	}
}

func TestSemanticParseMaxLength(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	longQuery := make([]byte, 2001)
	for i := range longQuery {
		longQuery[i] = 'a'
	}
	_, err := e.SemanticParse(string(longQuery), nil)
	if err == nil {
		t.Error("expected error for query exceeding 2000 chars")
	}
}

func TestSemanticParseMultipleIntents(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	tests := []struct {
		query      string
		wantIntent string
	}{
		{"count messages in orders", "count"},
		{"average latency in events", "average"},
		{"sum of bytes in logs", "sum"},
		{"maximum throughput in metrics", "max"},
		{"minimum latency in events", "min"},
		{"breakdown events by region", "group_by"},
		{"show where status = failed", "filter"},
		{"compare orders versus payments", "compare"},
	}

	for _, tt := range tests {
		t.Run(tt.wantIntent, func(t *testing.T) {
			plan, err := e.SemanticParse(tt.query, []string{"orders", "events", "logs", "metrics", "payments"})
			if err != nil {
				t.Fatalf("SemanticParse failed: %v", err)
			}
			if plan.Intent != tt.wantIntent {
				t.Errorf("for query %q: expected intent %q, got %q", tt.query, tt.wantIntent, plan.Intent)
			}
		})
	}
}

func TestSemanticParseWithBooleanFilters(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	plan, err := e.SemanticParse("show where status = failed and region = us-east-1", []string{"logs"})
	if err != nil {
		t.Fatalf("SemanticParse failed: %v", err)
	}
	if len(plan.Filters) < 1 {
		t.Errorf("expected at least 1 filter, got %d", len(plan.Filters))
	}
}

func TestExtractLimitEdgeCases(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	if l := e.extractLimit("top 0 results"); l != 0 {
		t.Errorf("expected 0 for top 0, got %d", l)
	}
	if l := e.extractLimit("top 10001 results"); l != 0 {
		t.Errorf("expected 0 for top 10001 (exceeds max), got %d", l)
	}
	if l := e.extractLimit("first 1 result"); l != 1 {
		t.Errorf("expected 1, got %d", l)
	}
	if l := e.extractLimit("limit 10000 results"); l != 10000 {
		t.Errorf("expected 10000, got %d", l)
	}
}

func TestExtractTimeRangeVariants(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	tests := []struct {
		query   string
		wantNil bool
		wantDur string
	}{
		{"from the last 5 minutes", false, "5 minutes"},
		{"over the last 1 week", false, "1 weeks"},
		{"from last 2 days", false, "2 days"},
		{"since yesterday", false, ""},
		{"today", false, ""},
		{"random text", true, ""},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			tr := e.extractTimeRange(tt.query)
			if tt.wantNil && tr != nil {
				t.Errorf("expected nil time range for %q", tt.query)
			}
			if !tt.wantNil && tr == nil {
				t.Errorf("expected time range for %q", tt.query)
			}
			if !tt.wantNil && tr != nil && tt.wantDur != "" && tr.Duration != tt.wantDur {
				t.Errorf("expected duration %q, got %q", tt.wantDur, tr.Duration)
			}
		})
	}
}

func TestNormalizeOperatorAll(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"=": "eq", "is": "eq", "equals": "eq",
		"!=": "neq", ">": "gt", "<": "lt",
		">=": "gte", "<=": "lte",
		"contains": "contains", "like": "contains",
		"xyz": "eq",
	}
	for input, want := range tests {
		if got := normalizeOperator(input); got != want {
			t.Errorf("normalizeOperator(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestIsCommonWordTable(t *testing.T) {
	t.Parallel()
	common := []string{"show", "find", "get", "list", "me", "all", "the", "in", "from", "on"}
	for _, w := range common {
		if !isCommonWord(w) {
			t.Errorf("expected %q to be common", w)
		}
	}
	notCommon := []string{"status", "error", "region", "kafka"}
	for _, w := range notCommon {
		if isCommonWord(w) {
			t.Errorf("expected %q to NOT be common", w)
		}
	}
}
