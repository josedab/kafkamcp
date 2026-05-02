package taskqueue

import (
	"testing"
	"time"
)

// --- Heartbeat ---

func TestHeartbeatValid(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Publish("q", "task1", "a1", PriorityMedium, 3, nil, 0)
	task, _ := e.Claim("worker", "q")

	err := e.Heartbeat("worker", task.ID)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	got, _ := e.GetTask(task.ID)
	if got.Status != "running" {
		t.Errorf("expected running after heartbeat, got %s", got.Status)
	}
}

func TestHeartbeatNonExistent(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	err := e.Heartbeat("worker", "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent task")
	}
}

func TestHeartbeatNotClaimedByAgent(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Publish("q", "task1", "a1", PriorityMedium, 3, nil, 0)
	task, _ := e.Claim("worker1", "q")

	err := e.Heartbeat("worker2", task.ID)
	if err == nil {
		t.Error("expected error for wrong agent")
	}
}

// --- ReassignStale ---

func TestReassignStaleTask(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.heartbeatTTL = 1 * time.Millisecond

	e.Publish("q", "task1", "a1", PriorityMedium, 3, nil, 0)
	task, _ := e.Claim("worker", "q")

	// Wait for heartbeat to expire
	time.Sleep(5 * time.Millisecond)

	reassigned := e.ReassignStale()
	if reassigned != 1 {
		t.Errorf("expected 1 reassigned, got %d", reassigned)
	}

	got, _ := e.GetTask(task.ID)
	if got.Status != "pending" {
		t.Errorf("expected pending after reassign, got %s", got.Status)
	}
	if got.ClaimedBy != "" {
		t.Errorf("expected empty ClaimedBy, got %s", got.ClaimedBy)
	}
}

func TestReassignStaleNone(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Publish("q", "task1", "a1", PriorityMedium, 3, nil, 0)

	reassigned := e.ReassignStale()
	if reassigned != 0 {
		t.Errorf("expected 0 reassigned, got %d", reassigned)
	}
}

func TestReassignStaleRunningTask(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.heartbeatTTL = 1 * time.Millisecond

	e.Publish("q", "task1", "a1", PriorityMedium, 3, nil, 0)
	task, _ := e.Claim("worker", "q")
	e.Heartbeat("worker", task.ID)

	time.Sleep(5 * time.Millisecond)

	reassigned := e.ReassignStale()
	if reassigned != 1 {
		t.Errorf("expected 1 stale running task reassigned, got %d", reassigned)
	}
}

// --- GetTask ---

func TestGetTaskExisting(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	task, _ := e.Publish("q", "task1", "a1", PriorityMedium, 3, nil, 0)

	got, err := e.GetTask(task.ID)
	if err != nil {
		t.Fatalf("GetTask failed: %v", err)
	}
	if got.Type != "task1" {
		t.Errorf("expected task1, got %s", got.Type)
	}
}

func TestGetTaskNonExistent(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.GetTask("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent task")
	}
}

// --- ListTasks ---

func TestListTasksByStatus(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Publish("q", "t1", "a1", PriorityMedium, 3, nil, 0)
	e.Publish("q", "t2", "a1", PriorityMedium, 3, nil, 0)
	e.Claim("worker", "q")

	tasks := e.ListTasks("q", "pending", 50)
	if len(tasks) != 1 {
		t.Errorf("expected 1 pending, got %d", len(tasks))
	}
}

func TestListTasksByQueue(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Publish("q1", "t1", "a1", PriorityMedium, 3, nil, 0)
	e.Publish("q2", "t2", "a1", PriorityMedium, 3, nil, 0)

	tasks := e.ListTasks("q1", "", 50)
	if len(tasks) != 1 {
		t.Errorf("expected 1, got %d", len(tasks))
	}
}

func TestListTasksEmpty(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	tasks := e.ListTasks("", "", 50)
	if len(tasks) != 0 {
		t.Errorf("expected 0, got %d", len(tasks))
	}
}

func TestListTasksLimit(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	for i := 0; i < 5; i++ {
		e.Publish("q", "t", "a1", PriorityMedium, 3, nil, 0)
	}

	tasks := e.ListTasks("q", "", 2)
	if len(tasks) != 2 {
		t.Errorf("expected 2, got %d", len(tasks))
	}
}

func TestListTasksDefaultLimit(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Publish("q", "t", "a1", PriorityMedium, 3, nil, 0)

	tasks := e.ListTasks("q", "", 0)
	if len(tasks) != 1 {
		t.Errorf("expected 1, got %d", len(tasks))
	}
}

// --- Stats edge cases ---

func TestStatsAllPending(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Publish("q", "t1", "a1", PriorityMedium, 3, nil, 0)
	e.Publish("q", "t2", "a1", PriorityMedium, 3, nil, 0)

	stats := e.Stats("q")
	if len(stats) != 1 {
		t.Fatalf("expected 1 queue, got %d", len(stats))
	}
	if stats[0].Pending != 2 {
		t.Errorf("expected 2 pending, got %d", stats[0].Pending)
	}
	// No division by zero for 0 completed
	if stats[0].AvgRunMs != 0 {
		t.Errorf("expected 0 avg run ms, got %d", stats[0].AvgRunMs)
	}
}

func TestStatsCompleted(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Publish("q", "t1", "a1", PriorityMedium, 3, nil, 0)
	task, _ := e.Claim("worker", "q")
	e.Complete("worker", task.ID, "completed", "done", "")

	stats := e.Stats("q")
	if stats[0].Completed != 1 {
		t.Errorf("expected 1 completed, got %d", stats[0].Completed)
	}
}

func TestStatsEmpty(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	stats := e.Stats("")
	if len(stats) != 0 {
		t.Errorf("expected 0, got %d", len(stats))
	}
}

func TestStatsAllQueues(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Publish("q1", "t1", "a1", PriorityMedium, 3, nil, 0)
	e.Publish("q2", "t2", "a1", PriorityMedium, 3, nil, 0)

	stats := e.Stats("")
	if len(stats) != 2 {
		t.Errorf("expected 2, got %d", len(stats))
	}
}

// --- Complete edge cases ---

func TestCompleteFailedRetryThenDeadLetter(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Publish("q", "t1", "a1", PriorityMedium, 1, nil, 0) // maxRetries=1

	task, _ := e.Claim("worker", "q")
	result, _ := e.Complete("worker", task.ID, "failed", nil, "err")
	if result.Status != "dead_letter" {
		t.Errorf("expected dead_letter with maxRetries=1, got %s", result.Status)
	}
}

func TestCompleteInvalidStatus(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Publish("q", "t1", "a1", PriorityMedium, 3, nil, 0)
	task, _ := e.Claim("worker", "q")

	_, err := e.Complete("worker", task.ID, "invalid", nil, "")
	if err == nil {
		t.Error("expected error for invalid status")
	}
}

func TestCompleteWrongAgent(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Publish("q", "t1", "a1", PriorityMedium, 3, nil, 0)
	task, _ := e.Claim("worker1", "q")

	_, err := e.Complete("worker2", task.ID, "completed", nil, "")
	if err == nil {
		t.Error("expected error for wrong agent")
	}
}

func TestCompleteEmptyTaskID(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.Complete("worker", "", "completed", nil, "")
	if err == nil {
		t.Error("expected error for empty task_id")
	}
}

func TestCompleteNonExistent(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.Complete("worker", "nonexistent", "completed", nil, "")
	if err == nil {
		t.Error("expected error for nonexistent task")
	}
}

// --- Publish edge cases ---

func TestPublishEmptyQueue(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.Publish("", "task", "agent", PriorityMedium, 3, nil, 0)
	if err == nil {
		t.Error("expected error for empty queue")
	}
}

func TestPublishWithDeadline(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	task, err := e.Publish("q", "task", "agent", PriorityMedium, 3, nil, 60)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if task.Deadline == nil {
		t.Error("expected deadline to be set")
	}
}

func TestPublishInvalidPriority(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	task, err := e.Publish("q", "task", "agent", -1, 3, nil, 0)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if task.Priority != PriorityMedium {
		t.Errorf("expected medium priority for invalid, got %d", task.Priority)
	}
}

func TestPublishNegativeRetries(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	task, err := e.Publish("q", "task", "agent", PriorityMedium, -5, nil, 0)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if task.MaxRetries != 3 {
		t.Errorf("expected 3 retries for negative, got %d", task.MaxRetries)
	}
}

// --- Claim edge cases ---

func TestClaimEmptyQueue(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.Claim("worker", "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent queue")
	}
}

func TestClaimNoPendingTasks(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Publish("q", "t1", "a1", PriorityMedium, 3, nil, 0)
	e.Claim("worker", "q")

	_, err := e.Claim("worker2", "q")
	if err == nil {
		t.Error("expected error when no pending tasks")
	}
}

func TestClaimMissingAgentID(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.Claim("", "q")
	if err == nil {
		t.Error("expected error for empty agent_id")
	}
}
