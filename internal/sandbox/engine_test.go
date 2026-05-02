package sandbox

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestCreateAndDestroy(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	sb, err := e.Create("test-sandbox", "Testing", "agent-1", 3600, nil)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if sb.Status != "active" {
		t.Errorf("expected active, got %s", sb.Status)
	}

	err = e.Destroy(sb.ID)
	if err != nil {
		t.Fatalf("Destroy failed: %v", err)
	}

	destroyed, _ := e.Get(sb.ID)
	if destroyed.Status != "destroyed" {
		t.Errorf("expected destroyed, got %s", destroyed.Status)
	}
}

func TestExecTracking(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	sb, _ := e.Create("exec-test", "", "agent-1", 3600, nil)

	op, err := e.Exec(sb.ID, "kafka_create_topic", map[string]any{"topic": "test-topic"}, nil, "", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}
	if op.Tool != "kafka_create_topic" {
		t.Errorf("expected kafka_create_topic, got %s", op.Tool)
	}

	got, _ := e.Get(sb.ID)
	if got.Usage.TopicCount != 1 {
		t.Errorf("expected 1 topic count, got %d", got.Usage.TopicCount)
	}
}

func TestQuotaEnforcement(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	sb, _ := e.Create("quota-test", "", "agent-1", 3600, &ResourceQuotas{
		MaxTopics:     1,
		MaxPartitions: 3,
		MaxMessages:   100,
	})

	// First topic OK
	e.Exec(sb.ID, "kafka_create_topic", map[string]any{"topic": "t1"}, nil, "", 0)

	// Second topic should fail quota
	_, err := e.Exec(sb.ID, "kafka_create_topic", map[string]any{"topic": "t2"}, nil, "", 0)
	if err == nil {
		t.Error("expected quota exceeded error")
	}
}

func TestList(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	e.Create("sb1", "", "agent-1", 3600, nil)
	e.Create("sb2", "", "agent-2", 3600, nil)

	list := e.List("agent-1")
	if len(list) != 1 {
		t.Errorf("expected 1 sandbox for agent-1, got %d", len(list))
	}

	allList := e.List("")
	if len(allList) != 2 {
		t.Errorf("expected 2 sandboxes total, got %d", len(allList))
	}
}

func TestPromotionReport(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	sb, _ := e.Create("promo-test", "", "agent-1", 3600, nil)
	e.Exec(sb.ID, "kafka_produce", nil, nil, "", 0)
	e.Exec(sb.ID, "kafka_consume", nil, nil, "", 0)

	report, err := e.GeneratePromotionReport(sb.ID)
	if err != nil {
		t.Fatalf("PromotionReport failed: %v", err)
	}
	if !report.Ready {
		t.Error("expected sandbox to be ready for promotion")
	}
	if report.Usage.OperationCount != 2 {
		t.Errorf("expected 2 operations, got %d", report.Usage.OperationCount)
	}
}
