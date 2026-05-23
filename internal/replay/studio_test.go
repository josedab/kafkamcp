package replay

import (
	"log/slog"
	"testing"
)

func TestRecording(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	rec := e.CreateRecording("test-recording", "orders", "cluster-a", 100)
	if rec.Status != RecordingActive {
		t.Errorf("expected active, got %s", rec.Status)
	}

	err := e.AddRecordedMessage(rec.ID, RecordedMessage{
		Partition: 0, Offset: 1, Key: "order-1",
		Value: `{"id":"order-1","amount":99.99,"email":"user@test.com"}`,
		Size:  50,
	})
	if err != nil {
		t.Fatalf("AddRecordedMessage: %v", err)
	}

	err = e.AddRecordedMessage(rec.ID, RecordedMessage{
		Partition: 0, Offset: 2, Key: "order-2",
		Value: `{"id":"order-2","amount":150.00}`,
		Size:  30,
	})
	if err != nil {
		t.Fatalf("AddRecordedMessage: %v", err)
	}

	stopped, err := e.StopRecording(rec.ID)
	if err != nil {
		t.Fatalf("StopRecording: %v", err)
	}
	if stopped.Status != RecordingStopped {
		t.Errorf("expected stopped, got %s", stopped.Status)
	}
	if len(stopped.Messages) != 2 {
		t.Errorf("expected 2 messages, got %d", len(stopped.Messages))
	}
}

func TestReplaySession(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	rec := e.CreateRecording("test", "orders", "cluster", 100)
	_ = e.AddRecordedMessage(rec.ID, RecordedMessage{
		Partition: 0, Offset: 1, Value: `{"id":"1","email":"test@test.com"}`, Size: 30,
	})
	_, _ = e.StopRecording(rec.ID)

	assertions := []Assertion{
		{Name: "has-messages", Type: "message_count", Operator: "gt", Expected: "0"},
		{Name: "has-email", Type: "field_exists", Field: "email"},
		{Name: "no-errors", Type: "no_errors", Expected: "true"},
	}

	session, err := e.CreateReplaySession("test-replay", rec.ID, 1.0, "", assertions)
	if err != nil {
		t.Fatalf("CreateReplaySession: %v", err)
	}

	session, err = e.RunReplaySession(session.ID)
	if err != nil {
		t.Fatalf("RunReplaySession: %v", err)
	}
	if session.Status != "completed" {
		t.Errorf("expected completed, got %s", session.Status)
	}
	if len(session.AssertionResults) != 3 {
		t.Errorf("expected 3 assertion results, got %d", len(session.AssertionResults))
	}
	for _, ar := range session.AssertionResults {
		if !ar.Passed {
			t.Errorf("assertion %q failed: %s", ar.Name, ar.Message)
		}
	}
}

func TestRegressionSuite(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	rec := e.CreateRecording("regression-data", "events", "cluster", 100)
	_ = e.AddRecordedMessage(rec.ID, RecordedMessage{
		Partition: 0, Offset: 1, Value: `{"type":"click","user":"123"}`, Size: 25,
	})
	_, _ = e.StopRecording(rec.ID)

	suite, err := e.CreateRegressionSuite("event-tests", "Basic event regression tests", rec.ID, []Assertion{
		{Name: "count-check", Type: "message_count", Operator: "eq", Expected: "1"},
		{Name: "has-type", Type: "field_value", Field: "type", Expected: "click"},
	})
	if err != nil {
		t.Fatalf("CreateRegressionSuite: %v", err)
	}

	result, err := e.RunRegressionSuite(suite.ID)
	if err != nil {
		t.Fatalf("RunRegressionSuite: %v", err)
	}
	if !result.Passed {
		t.Error("expected regression suite to pass")
	}
	if result.TotalTests != 2 {
		t.Errorf("expected 2 tests, got %d", result.TotalTests)
	}
}

func TestTimeDilation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	rec := e.CreateRecording("td-test", "topic", "cluster", 10)
	_ = e.AddRecordedMessage(rec.ID, RecordedMessage{Value: "test", Size: 4})
	_, _ = e.StopRecording(rec.ID)

	// Fast replay (0.5x = 2x faster)
	session, err := e.CreateReplaySession("fast-replay", rec.ID, 0.5, "", nil)
	if err != nil {
		t.Fatalf("CreateReplaySession: %v", err)
	}
	if session.TimeDilation != 0.5 {
		t.Errorf("expected dilation 0.5, got %f", session.TimeDilation)
	}

	// Clamp at max
	session2, _ := e.CreateReplaySession("clamped", rec.ID, 100.0, "", nil)
	if session2.TimeDilation != 10.0 {
		t.Errorf("expected clamped dilation 10.0, got %f", session2.TimeDilation)
	}
}

func TestCheckOperatorNumeric(t *testing.T) {
	t.Parallel()

	// Numeric gt: "10" > "2" should be true (not string comparison where "10" < "2")
	if !checkOperator("10", "gt", "2") {
		t.Error("10 > 2 should be true")
	}
	if checkOperator("2", "gt", "10") {
		t.Error("2 > 10 should be false")
	}
	if !checkOperator("5", "gte", "5") {
		t.Error("5 >= 5 should be true")
	}
	if !checkOperator("100", "lt", "200") {
		t.Error("100 < 200 should be true")
	}
	if !checkOperator("5", "lte", "5") {
		t.Error("5 <= 5 should be true")
	}
	if !checkOperator("a", "ne", "b") {
		t.Error("a != b should be true")
	}
	if checkOperator("a", "ne", "a") {
		t.Error("a != a should be false")
	}
}

func TestReplayActiveRecordingCannotReplay(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	rec := e.CreateRecording("active", "topic", "cluster", 100)
	// Don't stop — still active
	_, err := e.CreateReplaySession("test", rec.ID, 1.0, "", nil)
	if err == nil {
		t.Error("expected error creating replay from active recording")
	}
}

func TestRecordingMaxMessages(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	rec := e.CreateRecording("small", "topic", "cluster", 2)
	_ = e.AddRecordedMessage(rec.ID, RecordedMessage{Value: "msg1", Size: 4})
	_ = e.AddRecordedMessage(rec.ID, RecordedMessage{Value: "msg2", Size: 4})

	// Third message should trigger auto-completion
	err := e.AddRecordedMessage(rec.ID, RecordedMessage{Value: "msg3", Size: 4})
	if err == nil {
		t.Error("expected error when exceeding max messages")
	}

	got, _ := e.GetRecording(rec.ID)
	if got.Status != RecordingCompleted {
		t.Errorf("expected completed status, got %s", got.Status)
	}
}

func TestListRecordingsEmpty(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	recs := e.ListRecordings("")
	if len(recs) != 0 {
		t.Errorf("expected 0 recordings, got %d", len(recs))
	}
}
