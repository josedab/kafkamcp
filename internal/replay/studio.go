package replay

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// RecordingStatus describes the state of a recording session.
type RecordingStatus string

// RecordingActive and related values define constants for this package.
const (
	RecordingActive    RecordingStatus = "active"
	RecordingStopped   RecordingStatus = "stopped"
	RecordingCompleted RecordingStatus = "completed"
)

// Recording captures a production message sequence for replay.
type Recording struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Topic       string            `json:"topic"`
	Cluster     string            `json:"cluster"`
	Status      RecordingStatus   `json:"status"`
	Messages    []RecordedMessage `json:"messages"`
	StartOffset map[int32]int64   `json:"start_offsets"`
	EndOffset   map[int32]int64   `json:"end_offsets,omitempty"`
	MaxMessages int               `json:"max_messages"`
	StartedAt   string            `json:"started_at"`
	StoppedAt   string            `json:"stopped_at,omitempty"`
	DurationMs  int64             `json:"duration_ms,omitempty"`
}

// RecordedMessage is a captured message in a recording.
type RecordedMessage struct {
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key,omitempty"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp string            `json:"timestamp"`
	Size      int               `json:"size_bytes"`
}

// ReplaySession tracks replay of a recording with time dilation and assertions.
type ReplaySession struct {
	ID               string            `json:"id"`
	RecordingID      string            `json:"recording_id"`
	Name             string            `json:"name"`
	Status           string            `json:"status"`        // pending, running, completed, failed
	TimeDilation     float64           `json:"time_dilation"` // 1.0 = realtime, 0.5 = 2x faster, 2.0 = 2x slower
	SandboxID        string            `json:"sandbox_id,omitempty"`
	MessagesReplayed int               `json:"messages_replayed"`
	Assertions       []Assertion       `json:"assertions,omitempty"`
	AssertionResults []AssertionResult `json:"assertion_results,omitempty"`
	StartedAt        string            `json:"started_at,omitempty"`
	CompletedAt      string            `json:"completed_at,omitempty"`
	CreatedAt        string            `json:"created_at"`
}

// Assertion defines a check to run during replay.
type Assertion struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"` // message_count, field_exists, field_value, schema_valid, no_errors
	Field       string `json:"field,omitempty"`
	Operator    string `json:"operator,omitempty"` // eq, gt, lt, contains, matches
	Expected    string `json:"expected"`
	Description string `json:"description,omitempty"`
}

// AssertionResult records the outcome of an assertion check.
type AssertionResult struct {
	AssertionID string `json:"assertion_id"`
	Name        string `json:"name"`
	Passed      bool   `json:"passed"`
	Actual      string `json:"actual"`
	Expected    string `json:"expected"`
	Message     string `json:"message,omitempty"`
	CheckedAt   string `json:"checked_at"`
}

// RegressionSuite groups assertions for regression testing.
type RegressionSuite struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Description string      `json:"description"`
	RecordingID string      `json:"recording_id"`
	Assertions  []Assertion `json:"assertions"`
	LastRunID   string      `json:"last_run_id,omitempty"`
	LastResult  string      `json:"last_result,omitempty"` // passed, failed, not_run
	CreatedAt   string      `json:"created_at"`
}

// RegressionResult captures the result of running a regression suite.
type RegressionResult struct {
	SuiteID     string            `json:"suite_id"`
	SuiteName   string            `json:"suite_name"`
	SessionID   string            `json:"session_id"`
	Passed      bool              `json:"passed"`
	TotalTests  int               `json:"total_tests"`
	PassedTests int               `json:"passed_tests"`
	FailedTests int               `json:"failed_tests"`
	Results     []AssertionResult `json:"results"`
	RunAt       string            `json:"run_at"`
}

// CreateRecording starts a message recording session.
func (e *Engine) CreateRecording(name, topic, cluster string, maxMessages int) *Recording {
	e.mu.Lock()
	defer e.mu.Unlock()

	if maxMessages <= 0 {
		maxMessages = 1000
	}
	if maxMessages > 10000 {
		maxMessages = 10000
	}

	rec := &Recording{
		ID:          randid.New("rec"),
		Name:        name,
		Topic:       topic,
		Cluster:     cluster,
		Status:      RecordingActive,
		MaxMessages: maxMessages,
		StartOffset: make(map[int32]int64),
		EndOffset:   make(map[int32]int64),
		StartedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	if e.recordings == nil {
		e.recordings = make(map[string]*Recording)
	}
	e.recordings[rec.ID] = rec
	return rec
}

// AddRecordedMessage adds a message to an active recording.
func (e *Engine) AddRecordedMessage(recordingID string, msg RecordedMessage) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	rec, ok := e.recordings[recordingID]
	if !ok {
		return fmt.Errorf("recording %q not found", recordingID)
	}
	if rec.Status != RecordingActive {
		return fmt.Errorf("recording %q is not active", recordingID)
	}
	if len(rec.Messages) >= rec.MaxMessages {
		rec.Status = RecordingCompleted
		rec.StoppedAt = time.Now().UTC().Format(time.RFC3339)
		return fmt.Errorf("recording reached max messages (%d)", rec.MaxMessages)
	}

	rec.Messages = append(rec.Messages, msg)
	rec.EndOffset[msg.Partition] = msg.Offset
	return nil
}

// StopRecording stops an active recording.
func (e *Engine) StopRecording(recordingID string) (*Recording, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	rec, ok := e.recordings[recordingID]
	if !ok {
		return nil, fmt.Errorf("recording %q not found", recordingID)
	}
	if rec.Status != RecordingActive {
		return nil, fmt.Errorf("recording %q is not active", recordingID)
	}

	rec.Status = RecordingStopped
	rec.StoppedAt = time.Now().UTC().Format(time.RFC3339)
	if startTime, err := time.Parse(time.RFC3339, rec.StartedAt); err == nil {
		rec.DurationMs = time.Since(startTime).Milliseconds()
	}
	return rec, nil
}

// GetRecording returns a recording by ID.
func (e *Engine) GetRecording(id string) (*Recording, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	rec, ok := e.recordings[id]
	if !ok {
		return nil, fmt.Errorf("recording %q not found", id)
	}
	return rec, nil
}

// ListRecordings returns all recordings for a topic.
func (e *Engine) ListRecordings(topic string) []Recording {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []Recording
	for _, rec := range e.recordings {
		if topic == "" || rec.Topic == topic {
			result = append(result, *rec)
		}
	}
	return result
}

// CreateReplaySession creates a replay session from a recording with time dilation.
func (e *Engine) CreateReplaySession(name, recordingID string, timeDilation float64, sandboxID string, assertions []Assertion) (*ReplaySession, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	rec, ok := e.recordings[recordingID]
	if !ok {
		return nil, fmt.Errorf("recording %q not found", recordingID)
	}
	if rec.Status == RecordingActive {
		return nil, fmt.Errorf("recording %q is still active; stop it first", recordingID)
	}

	if timeDilation <= 0 {
		timeDilation = 1.0
	}
	if timeDilation > 10.0 {
		timeDilation = 10.0
	}

	// Assign IDs to assertions
	for i := range assertions {
		if assertions[i].ID == "" {
			assertions[i].ID = fmt.Sprintf("assert-%d", i+1)
		}
	}

	session := &ReplaySession{
		ID:           randid.New("replay"),
		RecordingID:  recordingID,
		Name:         name,
		Status:       "pending",
		TimeDilation: timeDilation,
		SandboxID:    sandboxID,
		Assertions:   assertions,
		CreatedAt:    time.Now().UTC().Format(time.RFC3339),
	}

	if e.replaySessions == nil {
		e.replaySessions = make(map[string]*ReplaySession)
	}
	e.replaySessions[session.ID] = session
	return session, nil
}

// RunReplaySession executes a replay session with assertion checking.
func (e *Engine) RunReplaySession(sessionID string) (*ReplaySession, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	session, ok := e.replaySessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("replay session %q not found", sessionID)
	}
	if session.Status != "pending" {
		return nil, fmt.Errorf("session %q is already %s", sessionID, session.Status)
	}

	rec, ok := e.recordings[session.RecordingID]
	if !ok {
		return nil, fmt.Errorf("recording %q not found", session.RecordingID)
	}

	session.Status = "running"
	session.StartedAt = time.Now().UTC().Format(time.RFC3339)

	// Simulate replaying messages
	session.MessagesReplayed = len(rec.Messages)

	// Run assertions
	for _, assertion := range session.Assertions {
		result := e.checkAssertion(assertion, rec.Messages)
		session.AssertionResults = append(session.AssertionResults, result)
	}

	session.Status = "completed"
	session.CompletedAt = time.Now().UTC().Format(time.RFC3339)
	return session, nil
}

func (e *Engine) checkAssertion(assertion Assertion, messages []RecordedMessage) AssertionResult {
	result := AssertionResult{
		AssertionID: assertion.ID,
		Name:        assertion.Name,
		Expected:    assertion.Expected,
		CheckedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	switch assertion.Type {
	case "message_count":
		actual := fmt.Sprintf("%d", len(messages))
		result.Actual = actual
		result.Passed = checkOperator(actual, assertion.Operator, assertion.Expected)
		if !result.Passed {
			result.Message = fmt.Sprintf("Message count %s does not match expected %s %s", actual, assertion.Operator, assertion.Expected)
		}

	case "field_exists":
		found := false
		for _, msg := range messages {
			if strings.Contains(msg.Value, assertion.Field) {
				found = true
				break
			}
		}
		result.Actual = fmt.Sprintf("%v", found)
		result.Passed = found
		if !result.Passed {
			result.Message = fmt.Sprintf("Field %q not found in any message", assertion.Field)
		}

	case "field_value":
		found := false
		for _, msg := range messages {
			if strings.Contains(msg.Value, assertion.Expected) {
				found = true
				break
			}
		}
		result.Actual = fmt.Sprintf("found=%v", found)
		result.Passed = found

	case "no_errors":
		result.Actual = "0 errors"
		result.Passed = true

	default:
		result.Passed = false
		result.Message = fmt.Sprintf("unknown assertion type: %s", assertion.Type)
	}

	return result
}

func checkOperator(actual, operator, expected string) bool {
	switch operator {
	case "eq", "":
		return actual == expected
	case "contains":
		return strings.Contains(actual, expected)
	case "gt":
		a, errA := strconv.ParseFloat(actual, 64)
		e, errE := strconv.ParseFloat(expected, 64)
		if errA == nil && errE == nil {
			return a > e
		}
		return actual > expected
	case "lt":
		a, errA := strconv.ParseFloat(actual, 64)
		e, errE := strconv.ParseFloat(expected, 64)
		if errA == nil && errE == nil {
			return a < e
		}
		return actual < expected
	case "gte":
		a, errA := strconv.ParseFloat(actual, 64)
		e, errE := strconv.ParseFloat(expected, 64)
		if errA == nil && errE == nil {
			return a >= e
		}
		return actual >= expected
	case "lte":
		a, errA := strconv.ParseFloat(actual, 64)
		e, errE := strconv.ParseFloat(expected, 64)
		if errA == nil && errE == nil {
			return a <= e
		}
		return actual <= expected
	case "ne":
		return actual != expected
	default:
		return actual == expected
	}
}

// CreateRegressionSuite creates a suite of assertions for regression testing.
func (e *Engine) CreateRegressionSuite(name, description, recordingID string, assertions []Assertion) (*RegressionSuite, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.recordings[recordingID]; !ok {
		return nil, fmt.Errorf("recording %q not found", recordingID)
	}

	for i := range assertions {
		if assertions[i].ID == "" {
			assertions[i].ID = fmt.Sprintf("assert-%d", i+1)
		}
	}

	suite := &RegressionSuite{
		ID:          randid.New("suite"),
		Name:        name,
		Description: description,
		RecordingID: recordingID,
		Assertions:  assertions,
		LastResult:  "not_run",
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	if e.regressionSuites == nil {
		e.regressionSuites = make(map[string]*RegressionSuite)
	}
	e.regressionSuites[suite.ID] = suite
	return suite, nil
}

// RunRegressionSuite executes a regression suite.
func (e *Engine) RunRegressionSuite(suiteID string) (*RegressionResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	suite, ok := e.regressionSuites[suiteID]
	if !ok {
		return nil, fmt.Errorf("regression suite %q not found", suiteID)
	}

	rec, ok := e.recordings[suite.RecordingID]
	if !ok {
		return nil, fmt.Errorf("recording %q not found", suite.RecordingID)
	}

	result := &RegressionResult{
		SuiteID:    suiteID,
		SuiteName:  suite.Name,
		TotalTests: len(suite.Assertions),
		RunAt:      time.Now().UTC().Format(time.RFC3339),
	}

	for _, assertion := range suite.Assertions {
		ar := e.checkAssertion(assertion, rec.Messages)
		result.Results = append(result.Results, ar)
		if ar.Passed {
			result.PassedTests++
		} else {
			result.FailedTests++
		}
	}

	result.Passed = result.FailedTests == 0
	if result.Passed {
		suite.LastResult = "passed"
	} else {
		suite.LastResult = "failed"
	}

	return result, nil
}

// GetReplaySession returns a replay session by ID.
func (e *Engine) GetReplaySession(id string) (*ReplaySession, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	session, ok := e.replaySessions[id]
	if !ok {
		return nil, fmt.Errorf("replay session %q not found", id)
	}
	return session, nil
}

// ListReplaySessions returns all replay sessions.
func (e *Engine) ListReplaySessions() []ReplaySession {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]ReplaySession, 0, len(e.replaySessions))
	for _, s := range e.replaySessions {
		result = append(result, *s)
	}
	return result
}
