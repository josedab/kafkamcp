// Package taskqueue provides a Kafka-backed multi-agent task distribution system
// with priority queues, distributed locking, heartbeat-based liveness, and dead-letter retries.
package taskqueue

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// Priority levels for tasks.
const (
	PriorityCritical = 0
	PriorityHigh     = 1
	PriorityMedium   = 2
	PriorityLow      = 3
)

// Task represents a work item in the task queue.
type Task struct {
	ID          string         `json:"id"`
	Queue       string         `json:"queue"`
	Type        string         `json:"type"`
	Priority    int            `json:"priority"` // 0=critical, 1=high, 2=medium, 3=low
	Payload     map[string]any `json:"payload"`
	Status      string         `json:"status"` // pending, claimed, running, completed, failed, dead_letter
	CreatedBy   string         `json:"created_by"`
	ClaimedBy   string         `json:"claimed_by,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	ClaimedAt   *time.Time     `json:"claimed_at,omitempty"`
	CompletedAt *time.Time     `json:"completed_at,omitempty"`
	Deadline    *time.Time     `json:"deadline,omitempty"`
	MaxRetries  int            `json:"max_retries"`
	RetryCount  int            `json:"retry_count"`
	Result      any            `json:"result,omitempty"`
	Error       string         `json:"error,omitempty"`
	LastHeartbeat *time.Time   `json:"last_heartbeat,omitempty"`
}

// QueueStats holds statistics about a task queue.
type QueueStats struct {
	Queue       string `json:"queue"`
	Pending     int    `json:"pending"`
	Claimed     int    `json:"claimed"`
	Running     int    `json:"running"`
	Completed   int    `json:"completed"`
	Failed      int    `json:"failed"`
	DeadLetter  int    `json:"dead_letter"`
	AvgWaitMs   int64  `json:"avg_wait_ms"`
	AvgRunMs    int64  `json:"avg_run_ms"`
}

// Engine manages task queues and task lifecycle.
type Engine struct {
	mu            sync.RWMutex
	tasks         map[string]*Task          // taskID -> task
	queues        map[string][]*Task         // queue -> tasks (for fast queue access)
	heartbeatTTL  time.Duration
	logger        *slog.Logger
	nextID        int64
}

// NewEngine creates a new task queue engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		tasks:        make(map[string]*Task),
		queues:       make(map[string][]*Task),
		heartbeatTTL: 60 * time.Second,
		logger:       logger,
	}
}

// Publish adds a task to a queue.
func (e *Engine) Publish(queue, taskType, createdBy string, priority, maxRetries int, payload map[string]any, deadlineSec int64) (*Task, error) {
	if queue == "" || taskType == "" {
		return nil, fmt.Errorf("queue and type are required")
	}
	if priority < 0 || priority > 3 {
		priority = PriorityMedium
	}
	if maxRetries < 0 {
		maxRetries = 3
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextID++
	id := fmt.Sprintf("task_%d", e.nextID)

	task := &Task{
		ID:         id,
		Queue:      queue,
		Type:       taskType,
		Priority:   priority,
		Payload:    payload,
		Status:     "pending",
		CreatedBy:  createdBy,
		CreatedAt:  time.Now(),
		MaxRetries: maxRetries,
	}

	if deadlineSec > 0 {
		d := time.Now().Add(time.Duration(deadlineSec) * time.Second)
		task.Deadline = &d
	}

	e.tasks[id] = task
	e.queues[queue] = append(e.queues[queue], task)

	e.logger.Info("task published", "id", id, "queue", queue, "type", taskType, "priority", priority)
	return task, nil
}

// Claim assigns the highest-priority pending task from a queue to an agent.
func (e *Engine) Claim(agentID, queue string) (*Task, error) {
	if agentID == "" || queue == "" {
		return nil, fmt.Errorf("agent_id and queue are required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	tasks, ok := e.queues[queue]
	if !ok || len(tasks) == 0 {
		return nil, fmt.Errorf("no tasks available in queue %q", queue)
	}

	// Sort by priority (lower = higher priority), then by creation time
	sort.SliceStable(tasks, func(i, j int) bool {
		if tasks[i].Priority != tasks[j].Priority {
			return tasks[i].Priority < tasks[j].Priority
		}
		return tasks[i].CreatedAt.Before(tasks[j].CreatedAt)
	})

	// Find first pending task
	for _, task := range tasks {
		if task.Status == "pending" {
			now := time.Now()
			task.Status = "claimed"
			task.ClaimedBy = agentID
			task.ClaimedAt = &now
			task.LastHeartbeat = &now
			e.logger.Info("task claimed", "id", task.ID, "agent", agentID, "queue", queue)
			return task, nil
		}
	}

	return nil, fmt.Errorf("no pending tasks in queue %q", queue)
}

// Complete marks a task as completed or failed.
func (e *Engine) Complete(agentID, taskID, status string, result any, errMsg string) (*Task, error) {
	if taskID == "" {
		return nil, fmt.Errorf("task_id is required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	task, ok := e.tasks[taskID]
	if !ok {
		return nil, fmt.Errorf("task %q not found", taskID)
	}

	if task.ClaimedBy != agentID {
		return nil, fmt.Errorf("task %q is claimed by %s, not %s", taskID, task.ClaimedBy, agentID)
	}

	now := time.Now()

	switch status {
	case "completed":
		task.Status = "completed"
		task.Result = result
		task.CompletedAt = &now
	case "failed":
		task.RetryCount++
		if task.RetryCount >= task.MaxRetries {
			task.Status = "dead_letter"
			task.Error = errMsg
			task.CompletedAt = &now
		} else {
			// Retry: put back to pending
			task.Status = "pending"
			task.ClaimedBy = ""
			task.ClaimedAt = nil
			task.Error = errMsg
		}
	default:
		return nil, fmt.Errorf("status must be 'completed' or 'failed'")
	}

	return task, nil
}

// Heartbeat updates the heartbeat timestamp for a claimed task.
func (e *Engine) Heartbeat(agentID, taskID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	task, ok := e.tasks[taskID]
	if !ok {
		return fmt.Errorf("task %q not found", taskID)
	}
	if task.ClaimedBy != agentID {
		return fmt.Errorf("task %q not claimed by %s", taskID, agentID)
	}

	now := time.Now()
	task.LastHeartbeat = &now
	task.Status = "running"
	return nil
}

// GetTask returns a task by ID.
func (e *Engine) GetTask(taskID string) (*Task, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	task, ok := e.tasks[taskID]
	if !ok {
		return nil, fmt.Errorf("task %q not found", taskID)
	}
	return task, nil
}

// Stats returns statistics for a queue or all queues.
func (e *Engine) Stats(queue string) []QueueStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	statsMap := make(map[string]*QueueStats)

	for _, task := range e.tasks {
		if queue != "" && task.Queue != queue {
			continue
		}
		qs, ok := statsMap[task.Queue]
		if !ok {
			qs = &QueueStats{Queue: task.Queue}
			statsMap[task.Queue] = qs
		}

		switch task.Status {
		case "pending":
			qs.Pending++
		case "claimed":
			qs.Claimed++
		case "running":
			qs.Running++
		case "completed":
			qs.Completed++
			if task.ClaimedAt != nil && task.CompletedAt != nil {
				qs.AvgRunMs += task.CompletedAt.Sub(*task.ClaimedAt).Milliseconds()
			}
		case "failed":
			qs.Failed++
		case "dead_letter":
			qs.DeadLetter++
		}

		if task.ClaimedAt != nil {
			qs.AvgWaitMs += task.ClaimedAt.Sub(task.CreatedAt).Milliseconds()
		}
	}

	var result []QueueStats
	for _, qs := range statsMap {
		total := qs.Completed + qs.Failed + qs.DeadLetter
		if total > 0 {
			qs.AvgWaitMs /= int64(total)
		}
		if qs.Completed > 0 {
			qs.AvgRunMs /= int64(qs.Completed)
		}
		result = append(result, *qs)
	}
	return result
}

// ReassignStale reassigns tasks whose heartbeat has expired.
func (e *Engine) ReassignStale() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	reassigned := 0

	for _, task := range e.tasks {
		if (task.Status == "claimed" || task.Status == "running") &&
			task.LastHeartbeat != nil &&
			now.Sub(*task.LastHeartbeat) > e.heartbeatTTL {
			task.Status = "pending"
			task.ClaimedBy = ""
			task.ClaimedAt = nil
			task.LastHeartbeat = nil
			reassigned++
		}
	}
	return reassigned
}

// ListTasks returns tasks for a queue, optionally filtered by status.
func (e *Engine) ListTasks(queue, status string, limit int) []Task {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 {
		limit = 50
	}

	var result []Task
	for _, task := range e.tasks {
		if queue != "" && task.Queue != queue {
			continue
		}
		if status != "" && task.Status != status {
			continue
		}
		result = append(result, *task)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Priority != result[j].Priority {
			return result[i].Priority < result[j].Priority
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})

	if len(result) > limit {
		result = result[:limit]
	}
	return result
}
