package nlquery

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

func coverageTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func newCoverageEngine() *Engine {
	return NewEngine(coverageTestLogger())
}

// --- SemanticParse ---

func TestSemanticParseSimpleIntent(t *testing.T) {
	e := newCoverageEngine()
	plan, err := e.SemanticParse("count messages in orders", []string{"orders"})
	if err != nil {
		t.Fatalf("SemanticParse failed: %v", err)
	}
	if plan.Intent != "count" {
		t.Errorf("expected count intent, got %s", plan.Intent)
	}
}

func TestSemanticParseComplexBoolean(t *testing.T) {
	e := newCoverageEngine()
	plan, err := e.SemanticParse("show where status = failed and region = us-east-1", []string{"logs"})
	if err != nil {
		t.Fatalf("SemanticParse failed: %v", err)
	}
	if len(plan.Filters) == 0 {
		t.Error("expected filters to be extracted")
	}
}

func TestSemanticParseEmpty(t *testing.T) {
	e := newCoverageEngine()
	_, err := e.SemanticParse("", nil)
	if err == nil {
		t.Error("expected error for empty query")
	}
}

func TestSemanticParseTooLong(t *testing.T) {
	e := newCoverageEngine()
	longQuery := ""
	for i := 0; i < 2001; i++ {
		longQuery += "a"
	}
	_, err := e.SemanticParse(longQuery, nil)
	if err == nil {
		t.Error("expected error for query > 2000 chars")
	}
}

func TestSemanticParseFallbackToRegex(t *testing.T) {
	e := newCoverageEngine()
	plan, err := e.SemanticParse("show me all errors in logs", []string{"logs"})
	if err != nil {
		t.Fatalf("SemanticParse failed: %v", err)
	}
	if plan.Intent == "" {
		t.Error("expected intent to be set")
	}
}

func TestSemanticParseWithTimeRange(t *testing.T) {
	e := newCoverageEngine()
	plan, err := e.SemanticParse("show errors in the last 2 hours", []string{"logs"})
	if err != nil {
		t.Fatalf("SemanticParse failed: %v", err)
	}
	if plan.TimeRange == nil {
		t.Error("expected time range to be extracted")
	}
}

func TestSemanticParseWithLimit(t *testing.T) {
	e := newCoverageEngine()
	plan, err := e.SemanticParse("top 10 errors by count", []string{"logs"})
	if err != nil {
		t.Fatalf("SemanticParse failed: %v", err)
	}
	if plan.Limit != 10 {
		t.Errorf("expected limit 10, got %d", plan.Limit)
	}
}

func TestSemanticParseWithSort(t *testing.T) {
	e := newCoverageEngine()
	plan, err := e.SemanticParse("show messages sort by timestamp desc", []string{"logs"})
	if err != nil {
		t.Fatalf("SemanticParse failed: %v", err)
	}
	if plan.SortBy == "" {
		t.Error("expected sort to be extracted")
	}
}

// --- detectIntent ---

func TestDetectIntentCount(t *testing.T) {
	e := newCoverageEngine()
	if e.detectIntent("count messages") != "count" {
		t.Error("expected count")
	}
}

func TestDetectIntentAverage(t *testing.T) {
	e := newCoverageEngine()
	if e.detectIntent("average latency") != "average" {
		t.Error("expected average")
	}
}

func TestDetectIntentMax(t *testing.T) {
	e := newCoverageEngine()
	if e.detectIntent("maximum throughput") != "max" {
		t.Error("expected max")
	}
}

func TestDetectIntentMin(t *testing.T) {
	e := newCoverageEngine()
	if e.detectIntent("minimum latency") != "min" {
		t.Error("expected min")
	}
}

func TestDetectIntentUnknown(t *testing.T) {
	e := newCoverageEngine()
	intent := e.detectIntent("xyzzy foobar")
	if intent != "search" {
		t.Errorf("expected search for unknown, got %s", intent)
	}
}

func TestDetectIntentJoin(t *testing.T) {
	e := newCoverageEngine()
	if e.detectIntent("join orders and users") != "join" {
		t.Error("expected join")
	}
}

func TestDetectIntentTrend(t *testing.T) {
	e := newCoverageEngine()
	if e.detectIntent("show trend over time") != "trend" {
		t.Error("expected trend")
	}
}

// --- extractBooleanFilters ---

func TestExtractBooleanFiltersWhereClause(t *testing.T) {
	e := newCoverageEngine()
	filters := e.extractBooleanFilters("where status = active and region = us-east-1")
	if len(filters) < 1 {
		t.Error("expected at least 1 filter")
	}
}

func TestExtractBooleanFiltersNoFilters(t *testing.T) {
	e := newCoverageEngine()
	filters := e.extractBooleanFilters("show me everything")
	if len(filters) != 0 {
		t.Errorf("expected 0 filters, got %d", len(filters))
	}
}

func TestExtractBooleanFiltersFailed(t *testing.T) {
	e := newCoverageEngine()
	filters := e.extractBooleanFilters("show me failed requests")
	found := false
	for _, f := range filters {
		if f.Field == "status" && f.Value == "failed" {
			found = true
		}
	}
	if !found {
		t.Error("expected status=failed filter")
	}
}

func TestExtractBooleanFiltersError(t *testing.T) {
	e := newCoverageEngine()
	filters := e.extractBooleanFilters("show me error logs")
	found := false
	for _, f := range filters {
		if f.Field == "level" && f.Value == "error" {
			found = true
		}
	}
	if !found {
		t.Error("expected level=error filter")
	}
}

func TestExtractBooleanFiltersEqPattern(t *testing.T) {
	e := newCoverageEngine()
	filters := e.extractBooleanFilters("status is active")
	if len(filters) == 0 {
		t.Error("expected filter from eq pattern")
	}
}

// --- extractAggregation ---

func TestExtractAggregationCount(t *testing.T) {
	e := newCoverageEngine()
	agg := e.extractAggregation("count messages")
	if agg == nil || agg.Function != "count" {
		t.Error("expected count aggregation")
	}
}

func TestExtractAggregationAvg(t *testing.T) {
	e := newCoverageEngine()
	agg := e.extractAggregation("average latency")
	if agg == nil || agg.Function != "avg" {
		t.Error("expected avg aggregation")
	}
}

func TestExtractAggregationSum(t *testing.T) {
	e := newCoverageEngine()
	agg := e.extractAggregation("sum of bytes")
	if agg == nil || agg.Function != "sum" {
		t.Error("expected sum aggregation")
	}
}

func TestExtractAggregationMax(t *testing.T) {
	e := newCoverageEngine()
	agg := e.extractAggregation("maximum throughput")
	if agg == nil || agg.Function != "max" {
		t.Error("expected max aggregation")
	}
}

func TestExtractAggregationMin(t *testing.T) {
	e := newCoverageEngine()
	agg := e.extractAggregation("minimum latency")
	if agg == nil || agg.Function != "min" {
		t.Error("expected min aggregation")
	}
}

func TestExtractAggregationNone(t *testing.T) {
	e := newCoverageEngine()
	agg := e.extractAggregation("show me everything")
	if agg != nil {
		t.Error("expected nil aggregation")
	}
}

func TestExtractAggregationGroupBy(t *testing.T) {
	e := newCoverageEngine()
	agg := e.extractAggregation("group events by region")
	if agg == nil || agg.Function != "group_by" {
		t.Error("expected group_by aggregation")
	}
}

func TestExtractAggregationWithGroupBy(t *testing.T) {
	e := newCoverageEngine()
	agg := e.extractAggregation("count messages by topic")
	if agg == nil {
		t.Fatal("expected aggregation")
	}
	if agg.GroupBy == "" {
		t.Error("expected group by field")
	}
}

// --- extractTimeRange ---

func TestExtractTimeRangeLastN(t *testing.T) {
	e := newCoverageEngine()
	tr := e.extractTimeRange("in the last 3 hours")
	if tr == nil {
		t.Fatal("expected time range")
	}
	if tr.Duration != "3 hours" {
		t.Errorf("expected '3 hours', got %s", tr.Duration)
	}
}

func TestExtractTimeRangeYesterday(t *testing.T) {
	e := newCoverageEngine()
	tr := e.extractTimeRange("since yesterday")
	if tr == nil {
		t.Fatal("expected time range")
	}
}

func TestExtractTimeRangeToday(t *testing.T) {
	e := newCoverageEngine()
	tr := e.extractTimeRange("today")
	if tr == nil {
		t.Fatal("expected time range")
	}
}

func TestExtractTimeRangeNone(t *testing.T) {
	e := newCoverageEngine()
	tr := e.extractTimeRange("show me stuff")
	if tr != nil {
		t.Error("expected nil time range")
	}
}

// --- extractLimit ---

func TestExtractLimitValid(t *testing.T) {
	e := newCoverageEngine()
	l := e.extractLimit("top 25 results")
	if l != 25 {
		t.Errorf("expected 25, got %d", l)
	}
}

func TestExtractLimitNone(t *testing.T) {
	e := newCoverageEngine()
	l := e.extractLimit("show me stuff")
	if l != 0 {
		t.Errorf("expected 0, got %d", l)
	}
}

// --- extractSort ---

func TestExtractSortDesc(t *testing.T) {
	e := newCoverageEngine()
	s := e.extractSort("sort by timestamp desc")
	if s != "timestamp:desc" {
		t.Errorf("expected timestamp:desc, got %s", s)
	}
}

func TestExtractSortAsc(t *testing.T) {
	e := newCoverageEngine()
	s := e.extractSort("order by name asc")
	if s != "name:asc" {
		t.Errorf("expected name:asc, got %s", s)
	}
}

func TestExtractSortNone(t *testing.T) {
	e := newCoverageEngine()
	s := e.extractSort("show me stuff")
	if s != "" {
		t.Errorf("expected empty, got %s", s)
	}
}

// --- CacheResult / GetCached ---

func TestCacheResultAndGet(t *testing.T) {
	e := newCoverageEngine()
	result := ExecutionResult{TotalCount: 42}
	e.CacheResult("test query", result)

	cached, ok := e.GetCached("test query")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if cached.TotalCount != 42 {
		t.Errorf("expected 42, got %d", cached.TotalCount)
	}
	if !cached.FromCache {
		t.Error("expected FromCache=true")
	}
}

func TestGetCachedMiss(t *testing.T) {
	e := newCoverageEngine()
	_, ok := e.GetCached("nonexistent")
	if ok {
		t.Error("expected cache miss")
	}
}

func TestGetCachedExpired(t *testing.T) {
	e := newCoverageEngine()
	e.cacheTTL = 1 * time.Millisecond
	e.CacheResult("expiring", ExecutionResult{TotalCount: 1})

	time.Sleep(5 * time.Millisecond)
	_, ok := e.GetCached("expiring")
	if ok {
		t.Error("expected cache miss for expired entry")
	}
}

func TestCacheResultCaseInsensitive(t *testing.T) {
	e := newCoverageEngine()
	e.CacheResult("My Query", ExecutionResult{TotalCount: 10})

	cached, ok := e.GetCached("  my query  ")
	if !ok {
		t.Fatal("expected cache hit (case-insensitive + trimmed)")
	}
	if cached.TotalCount != 10 {
		t.Errorf("expected 10, got %d", cached.TotalCount)
	}
}

// --- RecordHistory / GetHistory ---

func TestRecordAndGetHistory(t *testing.T) {
	e := newCoverageEngine()
	e.RecordHistory("q1", QueryPlan{Intent: "search"}, 10, 100)
	e.RecordHistory("q2", QueryPlan{Intent: "count"}, 5, 50)

	history := e.GetHistory(2)
	if len(history) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(history))
	}
	// Most recent first
	if history[0].Query != "q2" {
		t.Errorf("expected q2 first, got %s", history[0].Query)
	}
}

func TestGetHistoryLimitBounds(t *testing.T) {
	e := newCoverageEngine()
	e.RecordHistory("q1", QueryPlan{}, 0, 0)

	// Negative limit should return all
	history := e.GetHistory(-1)
	if len(history) != 1 {
		t.Errorf("expected 1, got %d", len(history))
	}

	// 0 limit should return all
	history = e.GetHistory(0)
	if len(history) != 1 {
		t.Errorf("expected 1, got %d", len(history))
	}

	// Limit > len should return all
	history = e.GetHistory(100)
	if len(history) != 1 {
		t.Errorf("expected 1, got %d", len(history))
	}
}

func TestRecordHistoryMaxEntries(t *testing.T) {
	e := newCoverageEngine()
	e.maxHistory = 3

	for i := 0; i < 5; i++ {
		e.RecordHistory("q", QueryPlan{}, 0, 0)
	}

	history := e.GetHistory(100)
	if len(history) != 3 {
		t.Errorf("expected max 3 entries, got %d", len(history))
	}
}

// --- Suggest ---

func TestSuggestFromHistory(t *testing.T) {
	e := newCoverageEngine()
	e.RecordHistory("show me errors in logs", QueryPlan{}, 0, 0)

	suggestions := e.Suggest("show", []string{})
	if len(suggestions) == 0 {
		t.Error("expected suggestions from history")
	}
}

func TestSuggestFromTemplates(t *testing.T) {
	e := newCoverageEngine()

	suggestions := e.Suggest("show", []string{"orders"})
	if len(suggestions) == 0 {
		t.Error("expected template suggestions")
	}
}

func TestSuggestDeduplication(t *testing.T) {
	e := newCoverageEngine()
	e.RecordHistory("count messages in orders", QueryPlan{}, 0, 0)
	e.RecordHistory("count messages in orders", QueryPlan{}, 0, 0)

	suggestions := e.Suggest("count", []string{"orders"})
	seen := make(map[string]bool)
	for _, s := range suggestions {
		if seen[s] {
			t.Errorf("duplicate suggestion: %s", s)
		}
		seen[s] = true
	}
}

func TestSuggestEmptyPrefix(t *testing.T) {
	e := newCoverageEngine()
	e.RecordHistory("test query", QueryPlan{}, 0, 0)

	suggestions := e.Suggest("", []string{"orders"})
	if len(suggestions) == 0 {
		t.Error("expected suggestions for empty prefix")
	}
}

// --- normalizeOperator ---

func TestNormalizeOperatorVariants(t *testing.T) {
	tests := []struct {
		input, expected string
	}{
		{"=", "eq"}, {"is", "eq"}, {"equals", "eq"},
		{"!=", "neq"}, {">", "gt"}, {"<", "lt"},
		{">=", "gte"}, {"<=", "lte"},
		{"contains", "contains"}, {"like", "contains"},
		{"unknown", "eq"},
	}
	for _, tt := range tests {
		got := normalizeOperator(tt.input)
		if got != tt.expected {
			t.Errorf("normalizeOperator(%q) = %s, want %s", tt.input, got, tt.expected)
		}
	}
}

// --- isCommonWord ---

func TestIsCommonWord(t *testing.T) {
	if !isCommonWord("show") {
		t.Error("expected 'show' to be common")
	}
	if isCommonWord("status") {
		t.Error("expected 'status' to not be common")
	}
}
