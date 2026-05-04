package gateway

import (
	"log/slog"
	"testing"
	"time"
)

func TestEventTranslator_CreateRule(t *testing.T) {
	t.Parallel()
	mgr := NewManager(slog.Default())
	et := NewEventTranslator(mgr, slog.Default())

	rule, err := et.CreateRule("kafka-to-pulsar", "kafka-main", "orders", "pulsar-backend", "orders-mirror", TransformConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rule.Status != "active" {
		t.Errorf("expected active, got %s", rule.Status)
	}
}

func TestEventTranslator_CreateRule_Validation(t *testing.T) {
	t.Parallel()
	mgr := NewManager(slog.Default())
	et := NewEventTranslator(mgr, slog.Default())

	_, err := et.CreateRule("", "src", "t1", "tgt", "t2", TransformConfig{})
	if err == nil {
		t.Fatal("expected error for empty name")
	}

	_, err = et.CreateRule("rule", "src", "same-topic", "src", "same-topic", TransformConfig{})
	if err == nil {
		t.Fatal("expected error for same source and target")
	}
}

func TestEventTranslator_ListRules(t *testing.T) {
	t.Parallel()
	mgr := NewManager(slog.Default())
	et := NewEventTranslator(mgr, slog.Default())

	et.CreateRule("r1", "src", "t1", "tgt", "t2", TransformConfig{})
	et.CreateRule("r2", "src", "t3", "tgt", "t4", TransformConfig{})

	rules := et.ListRules("")
	if len(rules) != 2 {
		t.Errorf("expected 2 rules, got %d", len(rules))
	}
}

func TestEventTranslator_PauseResume(t *testing.T) {
	t.Parallel()
	mgr := NewManager(slog.Default())
	et := NewEventTranslator(mgr, slog.Default())

	rule, _ := et.CreateRule("r1", "src", "t1", "tgt", "t2", TransformConfig{})

	et.PauseRule(rule.ID)
	paused, _ := et.GetRule(rule.ID)
	if paused.Status != "paused" {
		t.Errorf("expected paused, got %s", paused.Status)
	}

	et.ResumeRule(rule.ID)
	resumed, _ := et.GetRule(rule.ID)
	if resumed.Status != "active" {
		t.Errorf("expected active, got %s", resumed.Status)
	}
}

func TestEventTranslator_DeleteRule(t *testing.T) {
	t.Parallel()
	mgr := NewManager(slog.Default())
	et := NewEventTranslator(mgr, slog.Default())

	rule, _ := et.CreateRule("r1", "src", "t1", "tgt", "t2", TransformConfig{})
	err := et.DeleteRule(rule.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(et.ListRules("")) != 0 {
		t.Error("expected 0 rules after delete")
	}
}

func TestEventTranslator_Transform(t *testing.T) {
	t.Parallel()
	mgr := NewManager(slog.Default())
	et := NewEventTranslator(mgr, slog.Default())

	msg := Message{
		Key:     "order-123",
		Value:   []byte(`{"amount":100}`),
		Headers: map[string]string{"source": "kafka", "trace-id": "abc"},
		Topic:   "orders",
	}

	transform := TransformConfig{
		RenameHeaders: map[string]string{"trace-id": "X-Trace-ID"},
		AddHeaders:    map[string]string{"translated-by": "kafkamcp"},
		StripHeaders:  []string{"source"},
		KeyTransform:  "prefix",
		KeyPrefix:     "kafka_",
	}

	result := et.TranslateMessage(msg, transform)
	if result.Key != "kafka_order-123" {
		t.Errorf("expected kafka_order-123, got %s", result.Key)
	}
	if _, ok := result.Headers["source"]; ok {
		t.Error("expected source header to be stripped")
	}
	if result.Headers["X-Trace-ID"] != "abc" {
		t.Errorf("expected renamed header X-Trace-ID=abc, got %s", result.Headers["X-Trace-ID"])
	}
	if result.Headers["translated-by"] != "kafkamcp" {
		t.Error("expected translated-by header")
	}
}

func TestEventTranslator_DryRun(t *testing.T) {
	t.Parallel()
	mgr := NewManager(slog.Default())
	et := NewEventTranslator(mgr, slog.Default())

	msg := Message{Key: "key1", Headers: map[string]string{"h1": "v1"}, Timestamp: time.Now()}
	transform := TransformConfig{AddHeaders: map[string]string{"h2": "v2"}}

	result := et.DryRun(msg, transform)
	if result["changes"] == nil {
		t.Error("expected changes in dry run result")
	}
}

func TestEventTranslator_Stats(t *testing.T) {
	t.Parallel()
	mgr := NewManager(slog.Default())
	et := NewEventTranslator(mgr, slog.Default())

	et.CreateRule("r1", "src", "t1", "tgt", "t2", TransformConfig{})
	et.CreateRule("r2", "src", "t3", "tgt", "t4", TransformConfig{})

	stats := et.GetStats()
	if stats.TotalRules != 2 {
		t.Errorf("expected 2 rules, got %d", stats.TotalRules)
	}
	if stats.ActiveRules != 2 {
		t.Errorf("expected 2 active, got %d", stats.ActiveRules)
	}
}

func TestDescribeProtocolMapping(t *testing.T) {
	t.Parallel()
	mapping := DescribeProtocolMapping(ProtocolKafka, ProtocolRabbitMQ)
	if mapping["source_protocol"] != ProtocolKafka {
		t.Errorf("expected kafka source")
	}
	notes, ok := mapping["notes"].([]string)
	if !ok || len(notes) == 0 {
		t.Error("expected translation notes")
	}
}
