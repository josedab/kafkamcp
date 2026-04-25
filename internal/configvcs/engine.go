// Package configvcs provides Git-like version control for Kafka and KafkaMCP
// configurations with full diff history, rollback, and audit trail.
package configvcs

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
)

// ConfigChange represents a versioned configuration change.
type ConfigChange struct {
	ID          string         `json:"id"`
	Resource    string         `json:"resource"`    // topic, acl, schema, policy, cluster
	ResourceID  string         `json:"resource_id"` // e.g., topic name
	Action      string         `json:"action"`      // create, update, delete
	Version     int            `json:"version"`
	Before      map[string]any `json:"before,omitempty"`
	After       map[string]any `json:"after,omitempty"`
	Diff        []DiffEntry    `json:"diff,omitempty"`
	Author      string         `json:"author"`
	Message     string         `json:"message,omitempty"`
	Timestamp   time.Time      `json:"timestamp"`
	Tags        []string       `json:"tags,omitempty"`
}

// DiffEntry represents a single field change in a configuration diff.
type DiffEntry struct {
	Field    string `json:"field"`
	OldValue any    `json:"old_value,omitempty"`
	NewValue any    `json:"new_value,omitempty"`
	Type     string `json:"type"` // added, removed, modified
}

// CompareResult holds the comparison between two versions.
type CompareResult struct {
	Resource    string      `json:"resource"`
	ResourceID  string      `json:"resource_id"`
	Version1    int         `json:"version_1"`
	Version2    int         `json:"version_2"`
	Differences []DiffEntry `json:"differences"`
	Summary     string      `json:"summary"`
}

// RollbackResult holds the result of a rollback operation.
type RollbackResult struct {
	Resource     string         `json:"resource"`
	ResourceID   string         `json:"resource_id"`
	FromVersion  int            `json:"from_version"`
	ToVersion    int            `json:"to_version"`
	RestoredConfig map[string]any `json:"restored_config"`
	NewVersion   int            `json:"new_version"`
	ChangesCount int            `json:"changes_count"`
}

// AuditReport provides SOC2-ready audit trail.
type AuditReport struct {
	Resource     string         `json:"resource"`
	ResourceID   string         `json:"resource_id"`
	TotalChanges int            `json:"total_changes"`
	Changes      []ConfigChange `json:"changes"`
	Authors      []string       `json:"authors"`
	DateRange    string         `json:"date_range"`
}

// Engine manages configuration version control.
type Engine struct {
	mu       sync.RWMutex
	changes  map[string][]*ConfigChange // resource:resourceID -> ordered list of changes
	logger   *slog.Logger
	nextID   int64
}

// NewEngine creates a new config VCS engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		changes: make(map[string][]*ConfigChange),
		logger:  logger,
	}
}

func changeKey(resource, resourceID string) string {
	return resource + ":" + resourceID
}

// RecordChange records a configuration change.
func (e *Engine) RecordChange(resource, resourceID, action, author, message string, before, after map[string]any, tags []string) (*ConfigChange, error) {
	if resource == "" || resourceID == "" {
		return nil, fmt.Errorf("resource and resource_id are required")
	}
	if action == "" {
		action = "update"
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	key := changeKey(resource, resourceID)
	existing := e.changes[key]
	version := len(existing) + 1

	// Calculate diff
	var diff []DiffEntry
	if before != nil && after != nil {
		diff = computeDiff(before, after)
	} else if after != nil {
		for k, v := range after {
			diff = append(diff, DiffEntry{Field: k, NewValue: v, Type: "added"})
		}
	} else if before != nil {
		for k, v := range before {
			diff = append(diff, DiffEntry{Field: k, OldValue: v, Type: "removed"})
		}
	}

	e.nextID++
	change := &ConfigChange{
		ID:         fmt.Sprintf("cfg_%d", e.nextID),
		Resource:   resource,
		ResourceID: resourceID,
		Action:     action,
		Version:    version,
		Before:     before,
		After:      after,
		Diff:       diff,
		Author:     author,
		Message:    message,
		Timestamp:  time.Now(),
		Tags:       tags,
	}

	e.changes[key] = append(e.changes[key], change)
	e.logger.Info("config change recorded", "resource", resource, "id", resourceID, "version", version, "author", author)
	return change, nil
}

// History returns the change history for a resource.
func (e *Engine) History(resource, resourceID string, limit int) []ConfigChange {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 {
		limit = 50
	}

	key := changeKey(resource, resourceID)
	changes := e.changes[key]

	var result []ConfigChange
	// Return in reverse chronological order
	for i := len(changes) - 1; i >= 0; i-- {
		result = append(result, *changes[i])
		if len(result) >= limit {
			break
		}
	}
	return result
}

// Compare compares two versions of a resource configuration.
func (e *Engine) Compare(resource, resourceID string, version1, version2 int) (*CompareResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	key := changeKey(resource, resourceID)
	changes := e.changes[key]

	if len(changes) == 0 {
		return nil, fmt.Errorf("no history for %s:%s", resource, resourceID)
	}
	if version1 < 1 || version1 > len(changes) {
		return nil, fmt.Errorf("version %d not found (range: 1-%d)", version1, len(changes))
	}
	if version2 < 1 || version2 > len(changes) {
		return nil, fmt.Errorf("version %d not found (range: 1-%d)", version2, len(changes))
	}

	config1 := changes[version1-1].After
	config2 := changes[version2-1].After

	if config1 == nil {
		config1 = changes[version1-1].Before
	}
	if config2 == nil {
		config2 = changes[version2-1].Before
	}

	diffs := computeDiff(config1, config2)

	summary := fmt.Sprintf("%d field(s) changed between v%d and v%d", len(diffs), version1, version2)

	return &CompareResult{
		Resource:    resource,
		ResourceID:  resourceID,
		Version1:    version1,
		Version2:    version2,
		Differences: diffs,
		Summary:     summary,
	}, nil
}

// Rollback creates a new version that restores a previous configuration.
func (e *Engine) Rollback(resource, resourceID string, targetVersion int, author, reason string) (*RollbackResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	key := changeKey(resource, resourceID)
	changes := e.changes[key]

	if len(changes) == 0 {
		return nil, fmt.Errorf("no history for %s:%s", resource, resourceID)
	}
	if targetVersion < 1 || targetVersion > len(changes) {
		return nil, fmt.Errorf("version %d not found (range: 1-%d)", targetVersion, len(changes))
	}

	targetConfig := changes[targetVersion-1].After
	if targetConfig == nil {
		targetConfig = changes[targetVersion-1].Before
	}

	currentConfig := changes[len(changes)-1].After
	if currentConfig == nil {
		currentConfig = changes[len(changes)-1].Before
	}

	diff := computeDiff(currentConfig, targetConfig)
	newVersion := len(changes) + 1

	e.nextID++
	rollbackChange := &ConfigChange{
		ID:         fmt.Sprintf("cfg_%d", e.nextID),
		Resource:   resource,
		ResourceID: resourceID,
		Action:     "rollback",
		Version:    newVersion,
		Before:     currentConfig,
		After:      targetConfig,
		Diff:       diff,
		Author:     author,
		Message:    fmt.Sprintf("Rollback to v%d: %s", targetVersion, reason),
		Timestamp:  time.Now(),
		Tags:       []string{"rollback"},
	}

	e.changes[key] = append(e.changes[key], rollbackChange)

	return &RollbackResult{
		Resource:       resource,
		ResourceID:     resourceID,
		FromVersion:    len(changes),
		ToVersion:      targetVersion,
		RestoredConfig: targetConfig,
		NewVersion:     newVersion,
		ChangesCount:   len(diff),
	}, nil
}

// AuditReport generates a compliance audit report.
func (e *Engine) AuditReport(resource, resourceID string, since time.Time) (*AuditReport, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	report := &AuditReport{
		Resource:   resource,
		ResourceID: resourceID,
	}

	authorSet := make(map[string]bool)

	for key, changes := range e.changes {
		if resource != "" {
			if !strings.HasPrefix(key, resource+":") {
				continue
			}
		}
		if resourceID != "" {
			if key != changeKey(resource, resourceID) {
				continue
			}
		}

		for _, change := range changes {
			if !since.IsZero() && change.Timestamp.Before(since) {
				continue
			}
			report.Changes = append(report.Changes, *change)
			authorSet[change.Author] = true
		}
	}

	for author := range authorSet {
		report.Authors = append(report.Authors, author)
	}

	sort.Slice(report.Changes, func(i, j int) bool {
		return report.Changes[i].Timestamp.After(report.Changes[j].Timestamp)
	})

	report.TotalChanges = len(report.Changes)
	if len(report.Changes) > 0 {
		report.DateRange = fmt.Sprintf("%s to %s",
			report.Changes[len(report.Changes)-1].Timestamp.Format(time.RFC3339),
			report.Changes[0].Timestamp.Format(time.RFC3339))
	}

	return report, nil
}

func computeDiff(before, after map[string]any) []DiffEntry {
	var diffs []DiffEntry

	// Find modified and removed fields
	for k, oldVal := range before {
		newVal, exists := after[k]
		if !exists {
			diffs = append(diffs, DiffEntry{Field: k, OldValue: oldVal, Type: "removed"})
		} else if !jsonEqual(oldVal, newVal) {
			diffs = append(diffs, DiffEntry{Field: k, OldValue: oldVal, NewValue: newVal, Type: "modified"})
		}
	}

	// Find added fields
	for k, newVal := range after {
		if _, exists := before[k]; !exists {
			diffs = append(diffs, DiffEntry{Field: k, NewValue: newVal, Type: "added"})
		}
	}

	sort.Slice(diffs, func(i, j int) bool {
		return diffs[i].Field < diffs[j].Field
	})

	return diffs
}

func jsonEqual(a, b any) bool {
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	return string(aj) == string(bj)
}
