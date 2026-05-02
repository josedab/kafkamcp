package cdc

import (
	"log/slog"
	"testing"
)

func newTestEngine() *Engine {
	return NewEngine(slog.Default())
}

func TestCreateBridge(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	sink := SinkConfig{Type: SinkPostgres, Table: "users"}
	b, err := e.CreateBridge("test-bridge", "users-topic", "agent-1", sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	if b.ID == "" {
		t.Error("expected non-empty ID")
	}
	if b.Status != "active" {
		t.Errorf("expected status active, got %s", b.Status)
	}
}

func TestCreateBridgeValidation(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	if _, err := e.CreateBridge("", "topic", "a", SinkConfig{Type: SinkPostgres}, nil); err == nil {
		t.Error("expected error for empty name")
	}
	if _, err := e.CreateBridge("b", "", "a", SinkConfig{Type: SinkPostgres}, nil); err == nil {
		t.Error("expected error for empty source topic")
	}
	if _, err := e.CreateBridge("b", "t", "a", SinkConfig{Type: "invalid"}, nil); err == nil {
		t.Error("expected error for invalid sink type")
	}
}

func TestProcessEvent(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	sink := SinkConfig{Type: SinkElastic, Index: "events"}
	b, _ := e.CreateBridge("test", "events-topic", "agent", sink, nil)

	// INSERT event
	ev, err := e.ProcessEvent(b.ID, "key1", `{"name":"alice","age":30}`, 0, 1, "events-topic")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Operation != OpInsert {
		t.Errorf("expected INSERT, got %s", ev.Operation)
	}
	if ev.After["name"] != "alice" {
		t.Error("expected after.name = alice")
	}

	// Debezium-style UPDATE
	ev, err = e.ProcessEvent(b.ID, "key1", `{"op":"u","before":{"name":"alice"},"after":{"name":"bob"}}`, 0, 2, "events-topic")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Operation != OpUpdate {
		t.Errorf("expected UPDATE, got %s", ev.Operation)
	}

	// DELETE (tombstone)
	ev, err = e.ProcessEvent(b.ID, "key1", "", 0, 3, "events-topic")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Operation != OpDelete {
		t.Errorf("expected DELETE, got %s", ev.Operation)
	}

	// Check stats
	got, _ := e.GetBridge(b.ID)
	if got.Stats.RecordsProcessed != 3 {
		t.Errorf("expected 3 records processed, got %d", got.Stats.RecordsProcessed)
	}
	if got.Stats.Inserts != 1 {
		t.Errorf("expected 1 insert, got %d", got.Stats.Inserts)
	}
}

func TestProcessEventWithTransform(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	transform := &TransformSpec{
		FieldMapping: map[string]string{"name": "user_name", "age": "user_age"},
		IncludeMetadata: true,
	}
	sink := SinkConfig{Type: SinkPostgres, Table: "users"}
	b, _ := e.CreateBridge("test", "topic", "agent", sink, transform)

	ev, err := e.ProcessEvent(b.ID, "k1", `{"name":"alice","age":30,"extra":"x"}`, 0, 1, "topic")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := ev.After["user_name"]; !ok {
		t.Error("expected field mapping to apply")
	}
	if _, ok := ev.After["extra"]; ok {
		t.Error("expected unmapped field to be excluded")
	}
	if _, ok := ev.After["_cdc_processed_at"]; !ok {
		t.Error("expected metadata field")
	}
}

func TestBridgeLifecycle(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	sink := SinkConfig{Type: SinkRedis}
	b, _ := e.CreateBridge("test", "topic", "agent", sink, nil)

	// List
	bridges := e.ListBridges()
	if len(bridges) != 1 {
		t.Errorf("expected 1 bridge, got %d", len(bridges))
	}

	// Update status
	if err := e.UpdateBridgeStatus(b.ID, "paused"); err != nil {
		t.Fatal(err)
	}
	got, _ := e.GetBridge(b.ID)
	if got.Status != "paused" {
		t.Errorf("expected paused, got %s", got.Status)
	}

	// Invalid status
	if err := e.UpdateBridgeStatus(b.ID, "invalid"); err == nil {
		t.Error("expected error for invalid status")
	}

	// Delete
	if err := e.DeleteBridge(b.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.GetBridge(b.ID); err == nil {
		t.Error("expected error after delete")
	}
}

func TestGetRecentEvents(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	sink := SinkConfig{Type: SinkS3, Bucket: "test"}
	b, _ := e.CreateBridge("test", "topic", "agent", sink, nil)

	for i := 0; i < 5; i++ {
		e.ProcessEvent(b.ID, "k", `{"i":1}`, 0, int64(i), "topic")
	}

	events, err := e.GetRecentEvents(b.ID, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Errorf("expected 3 events, got %d", len(events))
	}
}

func TestProcessEventBridgeNotActive(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	sink := SinkConfig{Type: SinkPostgres}
	b, _ := e.CreateBridge("test", "topic", "agent", sink, nil)
	e.UpdateBridgeStatus(b.ID, "paused")

	_, err := e.ProcessEvent(b.ID, "k", `{}`, 0, 1, "topic")
	if err == nil {
		t.Error("expected error for paused bridge")
	}
}

func TestFlattenMap(t *testing.T) {
	t.Parallel()
	input := map[string]any{
		"a": 1,
		"b": map[string]any{
			"c": 2,
			"d": map[string]any{"e": 3},
		},
	}
	result := flattenMap("", input)
	if result["a"] != 1 {
		t.Error("expected a=1")
	}
	if result["b.c"] != 2 {
		t.Error("expected b.c=2")
	}
	if result["b.d.e"] != 3 {
		t.Error("expected b.d.e=3")
	}
}
