package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// TranslationRule defines how events are translated between platforms.
type TranslationRule struct {
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	Description    string            `json:"description,omitempty"`
	SourceBackend  string            `json:"source_backend"`
	SourceTopic    string            `json:"source_topic"`
	TargetBackend  string            `json:"target_backend"`
	TargetTopic    string            `json:"target_topic"`
	Transform      TransformConfig   `json:"transform,omitempty"`
	Status         string            `json:"status"` // active, paused, error
	CreatedAt      time.Time         `json:"created_at"`
}

// TransformConfig defines message transformation rules.
type TransformConfig struct {
	RenameHeaders map[string]string `json:"rename_headers,omitempty"`
	AddHeaders    map[string]string `json:"add_headers,omitempty"`
	StripHeaders  []string          `json:"strip_headers,omitempty"`
	KeyTransform  string            `json:"key_transform,omitempty"` // passthrough, hash, prefix
	KeyPrefix     string            `json:"key_prefix,omitempty"`
}

// TranslationResult is the outcome of a translation operation.
type TranslationResult struct {
	RuleID          string `json:"rule_id"`
	SourceBackend   string `json:"source_backend"`
	SourceTopic     string `json:"source_topic"`
	TargetBackend   string `json:"target_backend"`
	TargetTopic     string `json:"target_topic"`
	MessagesRead    int    `json:"messages_read"`
	MessagesWritten int    `json:"messages_written"`
	Errors          int    `json:"errors"`
	Duration        string `json:"duration"`
	Timestamp       string `json:"timestamp"`
}

// TranslationStats tracks aggregate translation statistics.
type TranslationStats struct {
	TotalRules      int                      `json:"total_rules"`
	ActiveRules     int                      `json:"active_rules"`
	TotalTranslated int64                    `json:"total_translated"`
	TotalErrors     int64                    `json:"total_errors"`
	RuleStats       map[string]RuleStats     `json:"rule_stats"`
	BackendPairs    []BackendPairStats       `json:"backend_pairs"`
}

// RuleStats tracks per-rule statistics.
type RuleStats struct {
	Translated int64  `json:"translated"`
	Errors     int64  `json:"errors"`
	LastRun    string `json:"last_run"`
}

// BackendPairStats tracks translations between backend pairs.
type BackendPairStats struct {
	Source     string `json:"source"`
	Target     string `json:"target"`
	RuleCount  int    `json:"rule_count"`
	Translated int64  `json:"translated"`
}

// EventTranslator manages cross-platform event translation.
type EventTranslator struct {
	mu      sync.RWMutex
	rules   map[string]*TranslationRule
	stats   map[string]*RuleStats
	manager *Manager
	logger  *slog.Logger
	nextID  int64
}

// NewEventTranslator creates a new event translator.
func NewEventTranslator(manager *Manager, logger *slog.Logger) *EventTranslator {
	return &EventTranslator{
		rules:   make(map[string]*TranslationRule),
		stats:   make(map[string]*RuleStats),
		manager: manager,
		logger:  logger,
	}
}

// CreateRule creates a new translation rule.
func (et *EventTranslator) CreateRule(name, sourceBackend, sourceTopic, targetBackend, targetTopic string, transform TransformConfig) (*TranslationRule, error) {
	if name == "" {
		return nil, fmt.Errorf("rule name is required")
	}
	if sourceBackend == "" || targetBackend == "" {
		return nil, fmt.Errorf("source and target backends are required")
	}
	if sourceTopic == "" || targetTopic == "" {
		return nil, fmt.Errorf("source and target topics are required")
	}
	if sourceBackend == targetBackend && sourceTopic == targetTopic {
		return nil, fmt.Errorf("source and target cannot be the same")
	}

	et.mu.Lock()
	defer et.mu.Unlock()

	et.nextID++
	id := fmt.Sprintf("rule_%d", et.nextID)

	rule := &TranslationRule{
		ID:            id,
		Name:          name,
		SourceBackend: sourceBackend,
		SourceTopic:   sourceTopic,
		TargetBackend: targetBackend,
		TargetTopic:   targetTopic,
		Transform:     transform,
		Status:        "active",
		CreatedAt:     time.Now(),
	}

	et.rules[id] = rule
	et.stats[id] = &RuleStats{}
	et.logger.Info("translation rule created", "id", id, "source", sourceBackend+"/"+sourceTopic, "target", targetBackend+"/"+targetTopic)
	return rule, nil
}

// ExecuteRule runs a translation rule, reading from source and writing to target.
func (et *EventTranslator) ExecuteRule(ctx context.Context, ruleID string, limit int) (*TranslationResult, error) {
	et.mu.RLock()
	rule, ok := et.rules[ruleID]
	if !ok {
		et.mu.RUnlock()
		return nil, fmt.Errorf("rule %q not found", ruleID)
	}
	if rule.Status != "active" {
		et.mu.RUnlock()
		return nil, fmt.Errorf("rule %q is not active (status: %s)", ruleID, rule.Status)
	}
	et.mu.RUnlock()

	if limit <= 0 {
		limit = 100
	}

	start := time.Now()
	result := &TranslationResult{
		RuleID:        ruleID,
		SourceBackend: rule.SourceBackend,
		SourceTopic:   rule.SourceTopic,
		TargetBackend: rule.TargetBackend,
		TargetTopic:   rule.TargetTopic,
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
	}

	// Read from source
	messages, err := et.manager.Consume(ctx, rule.SourceBackend, ConsumeOptions{
		Topic:     rule.SourceTopic,
		Limit:     limit,
		TimeoutMs: 5000,
	})
	if err != nil {
		result.Errors = 1
		et.recordStats(ruleID, 0, 1)
		return result, fmt.Errorf("consuming from source: %w", err)
	}
	result.MessagesRead = len(messages)

	// Transform and write to target
	for _, msg := range messages {
		transformed := et.applyTransform(msg, rule.Transform)
		_, err := et.manager.Produce(ctx, rule.TargetBackend, ProduceRequest{
			Topic:   rule.TargetTopic,
			Key:     transformed.Key,
			Value:   transformed.Value,
			Headers: transformed.Headers,
		})
		if err != nil {
			result.Errors++
			continue
		}
		result.MessagesWritten++
	}

	result.Duration = time.Since(start).String()
	et.recordStats(ruleID, int64(result.MessagesWritten), int64(result.Errors))
	return result, nil
}

// TranslateMessage translates a single message according to a rule's transform config.
func (et *EventTranslator) TranslateMessage(msg Message, transform TransformConfig) Message {
	return et.applyTransform(msg, transform)
}

// ListRules returns all translation rules.
func (et *EventTranslator) ListRules(status string) []TranslationRule {
	et.mu.RLock()
	defer et.mu.RUnlock()

	var result []TranslationRule
	for _, rule := range et.rules {
		if status == "" || rule.Status == status {
			result = append(result, *rule)
		}
	}
	return result
}

// GetRule returns a specific rule.
func (et *EventTranslator) GetRule(ruleID string) (*TranslationRule, error) {
	et.mu.RLock()
	defer et.mu.RUnlock()
	rule, ok := et.rules[ruleID]
	if !ok {
		return nil, fmt.Errorf("rule %q not found", ruleID)
	}
	return rule, nil
}

// PauseRule pauses a translation rule.
func (et *EventTranslator) PauseRule(ruleID string) error {
	et.mu.Lock()
	defer et.mu.Unlock()
	rule, ok := et.rules[ruleID]
	if !ok {
		return fmt.Errorf("rule %q not found", ruleID)
	}
	rule.Status = "paused"
	return nil
}

// ResumeRule resumes a paused rule.
func (et *EventTranslator) ResumeRule(ruleID string) error {
	et.mu.Lock()
	defer et.mu.Unlock()
	rule, ok := et.rules[ruleID]
	if !ok {
		return fmt.Errorf("rule %q not found", ruleID)
	}
	rule.Status = "active"
	return nil
}

// DeleteRule removes a translation rule.
func (et *EventTranslator) DeleteRule(ruleID string) error {
	et.mu.Lock()
	defer et.mu.Unlock()
	if _, ok := et.rules[ruleID]; !ok {
		return fmt.Errorf("rule %q not found", ruleID)
	}
	delete(et.rules, ruleID)
	delete(et.stats, ruleID)
	return nil
}

// GetStats returns aggregate translation statistics.
func (et *EventTranslator) GetStats() *TranslationStats {
	et.mu.RLock()
	defer et.mu.RUnlock()

	stats := &TranslationStats{
		TotalRules: len(et.rules),
		RuleStats:  make(map[string]RuleStats),
	}

	pairMap := make(map[string]*BackendPairStats)
	for id, rule := range et.rules {
		if rule.Status == "active" {
			stats.ActiveRules++
		}
		if rs, ok := et.stats[id]; ok {
			stats.TotalTranslated += rs.Translated
			stats.TotalErrors += rs.Errors
			stats.RuleStats[id] = *rs
		}
		pairKey := rule.SourceBackend + "->" + rule.TargetBackend
		if pair, ok := pairMap[pairKey]; ok {
			pair.RuleCount++
			if rs, ok := et.stats[id]; ok {
				pair.Translated += rs.Translated
			}
		} else {
			pair := &BackendPairStats{Source: rule.SourceBackend, Target: rule.TargetBackend, RuleCount: 1}
			if rs, ok := et.stats[id]; ok {
				pair.Translated = rs.Translated
			}
			pairMap[pairKey] = pair
		}
	}

	for _, pair := range pairMap {
		stats.BackendPairs = append(stats.BackendPairs, *pair)
	}

	return stats
}

// DryRun simulates a translation without actually writing to the target.
func (et *EventTranslator) DryRun(msg Message, transform TransformConfig) map[string]any {
	transformed := et.applyTransform(msg, transform)
	return map[string]any{
		"original": map[string]any{
			"key":     msg.Key,
			"headers": msg.Headers,
			"topic":   msg.Topic,
		},
		"transformed": map[string]any{
			"key":     transformed.Key,
			"headers": transformed.Headers,
			"topic":   transformed.Topic,
		},
		"changes": et.describeChanges(msg, transformed),
	}
}

func (et *EventTranslator) applyTransform(msg Message, transform TransformConfig) Message {
	result := Message{
		ID:        msg.ID,
		Topic:     msg.Topic,
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   make(map[string]string),
		Timestamp: msg.Timestamp,
		Protocol:  msg.Protocol,
	}

	// Copy headers
	for k, v := range msg.Headers {
		result.Headers[k] = v
	}

	// Rename headers
	for old, new := range transform.RenameHeaders {
		if val, ok := result.Headers[old]; ok {
			result.Headers[new] = val
			delete(result.Headers, old)
		}
	}

	// Add headers
	for k, v := range transform.AddHeaders {
		result.Headers[k] = v
	}

	// Strip headers
	for _, h := range transform.StripHeaders {
		delete(result.Headers, h)
	}

	// Key transform
	switch transform.KeyTransform {
	case "prefix":
		if transform.KeyPrefix != "" {
			result.Key = transform.KeyPrefix + result.Key
		}
	case "passthrough", "":
		// Keep as-is
	}

	return result
}

func (et *EventTranslator) recordStats(ruleID string, translated, errors int64) {
	et.mu.Lock()
	defer et.mu.Unlock()
	if rs, ok := et.stats[ruleID]; ok {
		rs.Translated += translated
		rs.Errors += errors
		rs.LastRun = time.Now().UTC().Format(time.RFC3339)
	}
}

func (et *EventTranslator) describeChanges(original, transformed Message) []string {
	var changes []string
	if original.Key != transformed.Key {
		changes = append(changes, fmt.Sprintf("key: %q -> %q", original.Key, transformed.Key))
	}
	for k := range original.Headers {
		if _, ok := transformed.Headers[k]; !ok {
			changes = append(changes, fmt.Sprintf("header removed: %s", k))
		}
	}
	for k, v := range transformed.Headers {
		if origV, ok := original.Headers[k]; !ok {
			changes = append(changes, fmt.Sprintf("header added: %s=%s", k, v))
		} else if origV != v {
			changes = append(changes, fmt.Sprintf("header changed: %s: %q -> %q", k, origV, v))
		}
	}
	if len(changes) == 0 {
		changes = append(changes, "no changes")
	}
	return changes
}

// DescribeProtocolMapping returns information about protocol-specific mappings.
func DescribeProtocolMapping(source, target Protocol) map[string]any {
	mapping := map[string]any{
		"source_protocol": source,
		"target_protocol": target,
	}

	concepts := map[Protocol]map[string]string{
		ProtocolKafka:     {"topic": "topic", "partition": "partition", "offset": "offset", "group": "consumer_group"},
		ProtocolPulsar:    {"topic": "topic", "partition": "partition", "offset": "message_id", "group": "subscription"},
		ProtocolRabbitMQ:  {"topic": "exchange/queue", "partition": "n/a", "offset": "delivery_tag", "group": "consumer_tag"},
		ProtocolNATS:      {"topic": "subject", "partition": "n/a", "offset": "sequence", "group": "queue_group"},
		ProtocolKinesis:   {"topic": "stream", "partition": "shard", "offset": "sequence_number", "group": "consumer_group"},
		ProtocolEventHubs: {"topic": "event_hub", "partition": "partition", "offset": "offset", "group": "consumer_group"},
	}

	if srcConcepts, ok := concepts[source]; ok {
		mapping["source_concepts"] = srcConcepts
	}
	if tgtConcepts, ok := concepts[target]; ok {
		mapping["target_concepts"] = tgtConcepts
	}

	var notes []string
	if source == ProtocolKafka && target == ProtocolRabbitMQ {
		notes = append(notes, "Kafka partitions have no RabbitMQ equivalent — messages will be distributed across consumers")
	}
	if source == ProtocolRabbitMQ && target == ProtocolKafka {
		notes = append(notes, "RabbitMQ queue semantics differ from Kafka topic semantics — messages become append-only")
	}
	if (source == ProtocolKafka || source == ProtocolPulsar) && target == ProtocolNATS {
		notes = append(notes, "NATS JetStream provides persistence; core NATS is at-most-once delivery")
	}
	mapping["notes"] = notes

	return mapping
}
