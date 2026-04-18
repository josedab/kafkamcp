package kafka

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// TimeTravelSnapshot represents a point-in-time state of a compacted topic.
type TimeTravelSnapshot struct {
	Topic      string            `json:"topic"`
	Timestamp  string            `json:"timestamp"`
	KeyCount   int               `json:"key_count"`
	State      map[string]any    `json:"state"` // key -> last value before timestamp
	ScanInfo   TimeTravelScanInfo `json:"scan_info"`
}

// TimeTravelScanInfo provides metadata about the scan operation.
type TimeTravelScanInfo struct {
	MessagesScanned int    `json:"messages_scanned"`
	Duration        string `json:"duration_ms"`
}

// TimeTravelDiffResult represents changes between two timestamps.
type TimeTravelDiffResult struct {
	Topic        string              `json:"topic"`
	From         string              `json:"timestamp_from"`
	To           string              `json:"timestamp_to"`
	TotalChanges int                 `json:"total_changes"`
	Added        map[string]any      `json:"added"`   // keys that appeared
	Modified     []TimeTravelModified `json:"modified"` // keys that changed value
	Deleted      []string            `json:"deleted"`  // keys that disappeared
}

// TimeTravelModified represents a modified key between two snapshots.
type TimeTravelModified struct {
	Key      string `json:"key"`
	OldValue any    `json:"old_value"`
	NewValue any    `json:"new_value"`
}

// TimeTravelSnapshot reconstructs topic state at a point in time.
func (m *Manager) TimeTravelSnapshot(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
	ts, err := parseTimestamp(timestamp)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp: %w", err)
	}

	startTime := time.Now()

	// Consume from earliest up to the given timestamp
	msgs, err := m.Consume(ctx, cluster, topic, -1, "earliest", maxKeys*2, 30000, "auto")
	if err != nil {
		return nil, fmt.Errorf("consuming for snapshot: %w", err)
	}

	// Build state: last value per key before timestamp
	state := make(map[string]any)
	scanned := 0
	for _, msg := range msgs {
		msgTime, parseErr := time.Parse("2006-01-02T15:04:05.000Z", msg.Timestamp)
		if parseErr != nil {
			continue
		}
		if msgTime.After(ts) {
			break
		}
		scanned++
		key := fmt.Sprintf("%v", msg.Key)
		if key == "" || key == "<nil>" {
			continue
		}
		if msg.Value == nil {
			delete(state, key) // tombstone
		} else {
			state[key] = msg.Value
		}
		if len(state) >= maxKeys {
			break
		}
	}

	return &TimeTravelSnapshot{
		Topic:     topic,
		Timestamp: timestamp,
		KeyCount:  len(state),
		State:     state,
		ScanInfo: TimeTravelScanInfo{
			MessagesScanned: scanned,
			Duration:        fmt.Sprintf("%d", time.Since(startTime).Milliseconds()),
		},
	}, nil
}

// TimeTravelDiff computes the difference between topic state at two timestamps.
func (m *Manager) TimeTravelDiff(ctx context.Context, cluster, topic, tsFrom, tsTo string, maxKeys int) (*TimeTravelDiffResult, error) {
	fromSnapshot, err := m.TimeTravelSnapshot(ctx, cluster, topic, tsFrom, maxKeys)
	if err != nil {
		return nil, fmt.Errorf("building from-snapshot: %w", err)
	}

	toSnapshot, err := m.TimeTravelSnapshot(ctx, cluster, topic, tsTo, maxKeys)
	if err != nil {
		return nil, fmt.Errorf("building to-snapshot: %w", err)
	}

	result := &TimeTravelDiffResult{
		Topic: topic,
		From:  tsFrom,
		To:    tsTo,
		Added: make(map[string]any),
	}

	// Find added and modified
	for key, newVal := range toSnapshot.State {
		if oldVal, exists := fromSnapshot.State[key]; exists {
			if fmt.Sprintf("%v", oldVal) != fmt.Sprintf("%v", newVal) {
				result.Modified = append(result.Modified, TimeTravelModified{
					Key: key, OldValue: oldVal, NewValue: newVal,
				})
			}
		} else {
			result.Added[key] = newVal
		}
	}

	// Find deleted
	for key := range fromSnapshot.State {
		if _, exists := toSnapshot.State[key]; !exists {
			result.Deleted = append(result.Deleted, key)
		}
	}

	result.TotalChanges = len(result.Added) + len(result.Modified) + len(result.Deleted)
	return result, nil
}

// Recommendation represents an actionable insight about a topic or cluster.
type Recommendation struct {
	Type       string  `json:"type"`       // partition_hotspot, idle_topic, under_replicated, lag_trend, retention, compression
	Severity   string  `json:"severity"`   // critical, warning, info
	Topic      string  `json:"topic,omitempty"`
	Message    string  `json:"message"`
	Suggestion string  `json:"suggestion"`
	Confidence float64 `json:"confidence"` // 0.0 - 1.0
}

// RecommendationResult contains all recommendations.
type RecommendationResult struct {
	Recommendations []Recommendation `json:"recommendations"`
	TopicsAnalyzed  int              `json:"topics_analyzed"`
	Cluster         string           `json:"cluster"`
}

// GetRecommendations analyzes topics and provides optimization suggestions.
func (m *Manager) GetRecommendations(ctx context.Context, cluster, specificTopic string) (*RecommendationResult, error) {
	topics, err := m.ListTopics(ctx, cluster, false)
	if err != nil {
		return nil, err
	}

	if specificTopic != "" {
		var filtered []TopicSummary
		for _, t := range topics {
			if t.Name == specificTopic {
				filtered = append(filtered, t)
			}
		}
		topics = filtered
	}

	var recommendations []Recommendation

	for _, topic := range topics {
		detail, err := m.DescribeTopic(ctx, cluster, topic.Name)
		if err != nil {
			continue
		}

		// Check under-replicated partitions
		for _, p := range detail.Partitions {
			if len(p.ISR) < len(p.Replicas) {
				recommendations = append(recommendations, Recommendation{
					Type:       "under_replicated",
					Severity:   "critical",
					Topic:      topic.Name,
					Message:    fmt.Sprintf("Partition %d has %d ISR out of %d replicas", p.ID, len(p.ISR), len(p.Replicas)),
					Suggestion: "Check broker health. Under-replicated partitions risk data loss.",
					Confidence: 0.95,
				})
			}
		}

		// Check idle topics (zero messages)
		if topic.MessageCount == 0 {
			recommendations = append(recommendations, Recommendation{
				Type:       "idle_topic",
				Severity:   "info",
				Topic:      topic.Name,
				Message:    "Topic has zero messages",
				Suggestion: "Consider removing if unused, or verify producers are functioning.",
				Confidence: 0.8,
			})
		}

		// Partition hotspot detection
		if len(detail.Partitions) > 1 {
			var offsets []int64
			for _, p := range detail.Partitions {
				offsets = append(offsets, p.LatestOffset-p.EarliestOffset)
			}
			sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
			if len(offsets) > 0 && offsets[len(offsets)-1] > 0 {
				ratio := float64(offsets[0]) / float64(offsets[len(offsets)-1])
				if ratio < 0.1 && offsets[len(offsets)-1] > 1000 {
					recommendations = append(recommendations, Recommendation{
						Type:       "partition_hotspot",
						Severity:   "warning",
						Topic:      topic.Name,
						Message:    fmt.Sprintf("Partition skew detected: min/max ratio %.2f", ratio),
						Suggestion: "Consider using a better partitioning key or increasing partition count.",
						Confidence: 0.85,
					})
				}
			}
		}

		// Retention tuning suggestion
		if retMs, ok := detail.Configs["retention.ms"]; ok {
			if retMs == "-1" {
				recommendations = append(recommendations, Recommendation{
					Type:       "retention",
					Severity:   "info",
					Topic:      topic.Name,
					Message:    "Topic has infinite retention",
					Suggestion: "Set a finite retention period to manage disk usage.",
					Confidence: 0.7,
				})
			}
		}

		// Compression suggestion
		if compression, ok := detail.Configs["compression.type"]; ok {
			if compression == "none" || compression == "producer" {
				recommendations = append(recommendations, Recommendation{
					Type:       "compression",
					Severity:   "info",
					Topic:      topic.Name,
					Message:    fmt.Sprintf("Topic compression is '%s'", compression),
					Suggestion: "Consider enabling lz4 or zstd compression to reduce storage and network usage.",
					Confidence: 0.75,
				})
			}
		}

		// Partition count suggestion
		if topic.MessageCount > 100000 && topic.Partitions == 1 {
			recommendations = append(recommendations, Recommendation{
				Type:       "partition_count",
				Severity:   "warning",
				Topic:      topic.Name,
				Message:    fmt.Sprintf("High-volume topic (%d msgs) with only 1 partition", topic.MessageCount),
				Suggestion: "Increase partition count to improve parallelism and throughput.",
				Confidence: 0.85,
			})
		}
	}

	// Consumer lag trend detection
	groups, err := m.ListConsumerGroups(ctx, cluster)
	if err == nil {
		for _, g := range groups {
			if g.TotalLag > 10000 {
				recommendations = append(recommendations, Recommendation{
					Type:       "lag_trend",
					Severity:   "warning",
					Topic:      "",
					Message:    fmt.Sprintf("Consumer group '%s' has high lag: %d", g.GroupID, g.TotalLag),
					Suggestion: "Scale consumers or investigate processing bottlenecks.",
					Confidence: 0.9,
				})
			}
		}
	}

	return &RecommendationResult{
		Recommendations: recommendations,
		TopicsAnalyzed:  len(topics),
		Cluster:         m.resolveCluster(cluster),
	}, nil
}

func (m *Manager) resolveCluster(cluster string) string {
	if cluster == "" {
		return m.defClust
	}
	return cluster
}

// TraceResult represents the result of tracing a message across topics.
type TraceResult struct {
	CorrelationID string      `json:"correlation_id"`
	Hops          []TraceHop  `json:"hops"`
	TopicsSearched int        `json:"topics_searched"`
	TotalMessages  int        `json:"total_messages_found"`
	FlowGraph      []FlowEdge `json:"flow_graph"`
}

// TraceHop represents a single hop in a message's journey.
type TraceHop struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Timestamp string `json:"timestamp"`
	Key       any    `json:"key,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// FlowEdge represents an edge in the message flow graph.
type FlowEdge struct {
	FromTopic  string `json:"from_topic"`
	ToTopic    string `json:"to_topic"`
	LatencyMs  int64  `json:"latency_ms"`
}

// correlationHeaders lists common correlation ID header names.
var correlationHeaders = []string{
	"correlation-id", "correlationid", "correlation_id",
	"trace-id", "traceid", "trace_id",
	"x-request-id", "x-correlation-id", "x-trace-id",
	"request-id", "requestid", "request_id",
}

// TraceMessage traces a correlation ID across multiple topics.
func (m *Manager) TraceMessage(ctx context.Context, cluster, correlationID, headerName string, topics []string, maxScan int) (*TraceResult, error) {
	result := &TraceResult{
		CorrelationID: correlationID,
	}

	headerNames := correlationHeaders
	if headerName != "" {
		headerNames = []string{headerName}
	}

	for _, topic := range topics {
		msgs, _, err := m.SearchMessages(ctx, cluster, SearchRequest{
			Topic:   topic,
			MaxScan: maxScan,
			Limit:   100,
		})
		if err != nil {
			continue
		}

		for _, msg := range msgs {
			if msg.Headers == nil {
				continue
			}
			for _, hdr := range headerNames {
				if val, ok := msg.Headers[hdr]; ok && val == correlationID {
					result.Hops = append(result.Hops, TraceHop{
						Topic:     topic,
						Partition: msg.Partition,
						Offset:    msg.Offset,
						Timestamp: msg.Timestamp,
						Key:       msg.Key,
						Headers:   msg.Headers,
					})
					break
				}
			}
		}
		result.TopicsSearched++
	}

	// Sort by timestamp
	sort.Slice(result.Hops, func(i, j int) bool {
		return result.Hops[i].Timestamp < result.Hops[j].Timestamp
	})

	result.TotalMessages = len(result.Hops)

	// Build flow graph
	for i := 1; i < len(result.Hops); i++ {
		prev := result.Hops[i-1]
		curr := result.Hops[i]
		if prev.Topic != curr.Topic {
			var latencyMs int64
			if t1, err := time.Parse("2006-01-02T15:04:05.000Z", prev.Timestamp); err == nil {
				if t2, err := time.Parse("2006-01-02T15:04:05.000Z", curr.Timestamp); err == nil {
					latencyMs = t2.Sub(t1).Milliseconds()
				}
			}
			result.FlowGraph = append(result.FlowGraph, FlowEdge{
				FromTopic: prev.Topic,
				ToTopic:   curr.Topic,
				LatencyMs: latencyMs,
			})
		}
	}

	return result, nil
}

// ACLEntry represents a single Kafka ACL.
type ACLEntry struct {
	Principal    string `json:"principal"`
	Host         string `json:"host"`
	ResourceType string `json:"resource_type"`
	ResourceName string `json:"resource_name"`
	Operation    string `json:"operation"`
	Permission   string `json:"permission"` // Allow or Deny
	PatternType  string `json:"pattern_type"`
}

// ACLResult contains ACL listing and security analysis.
type ACLResult struct {
	ACLs            []ACLEntry         `json:"acls"`
	TotalACLs       int                `json:"total_acls"`
	SecurityAnalysis *SecurityAnalysis  `json:"security_analysis,omitempty"`
}

// SecurityAnalysis provides security insights about ACLs.
type SecurityAnalysis struct {
	OverPermissioned []SecurityFinding `json:"over_permissioned"`
	WildcardACLs     []SecurityFinding `json:"wildcard_acls"`
	Findings         int               `json:"total_findings"`
}

// SecurityFinding describes a single security finding.
type SecurityFinding struct {
	Principal string  `json:"principal"`
	Issue     string  `json:"issue"`
	Severity  string  `json:"severity"`
	Confidence float64 `json:"confidence"`
}

// ListACLs lists all Kafka ACLs with optional security analysis.
func (m *Manager) ListACLs(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*ACLResult, error) {
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return nil, err
	}

	// Build a permissive filter to list all ACLs
	b := kadm.NewACLs().
		AnyResource().
		Allow().
		Deny().
		AllowHosts().
		DenyHosts().
		Operations(kadm.OpAny).
		ResourcePatternType(kadm.ACLPatternAny)

	aclResults, err := admin.DescribeACLs(ctx, b)
	if err != nil {
		return nil, fmt.Errorf("describing ACLs: %w", err)
	}

	result := &ACLResult{}

	for _, aclResult := range aclResults {
		for _, acl := range aclResult.Described {
			entry := ACLEntry{
				Principal:    acl.Principal,
				Host:         acl.Host,
				ResourceType: acl.Type.String(),
				ResourceName: acl.Name,
				Operation:    acl.Operation.String(),
				Permission:   acl.Permission.String(),
				PatternType:  acl.Pattern.String(),
			}

			// Apply filters
			if principal != "" && !strings.Contains(entry.Principal, principal) {
				continue
			}
			if resourceType != "" && !strings.EqualFold(entry.ResourceType, resourceType) {
				continue
			}

			result.ACLs = append(result.ACLs, entry)
		}
	}

	result.TotalACLs = len(result.ACLs)

	if includeAnalysis {
		result.SecurityAnalysis = analyzeACLSecurity(result.ACLs)
	}

	return result, nil
}

func analyzeACLSecurity(acls []ACLEntry) *SecurityAnalysis {
	analysis := &SecurityAnalysis{}

	// Group by principal
	principalOps := make(map[string][]ACLEntry)
	for _, acl := range acls {
		principalOps[acl.Principal] = append(principalOps[acl.Principal], acl)
	}

	for principal, ops := range principalOps {
		// Check wildcard resource access
		for _, op := range ops {
			if op.ResourceName == "*" {
				analysis.WildcardACLs = append(analysis.WildcardACLs, SecurityFinding{
					Principal:  principal,
					Issue:      fmt.Sprintf("Wildcard %s access on %s resources", op.Operation, op.ResourceType),
					Severity:   "warning",
					Confidence: 0.9,
				})
			}
		}

		// Check over-permissioned (too many operations)
		if len(ops) > 10 {
			analysis.OverPermissioned = append(analysis.OverPermissioned, SecurityFinding{
				Principal:  principal,
				Issue:      fmt.Sprintf("Principal has %d ACL entries — consider consolidation", len(ops)),
				Severity:   "info",
				Confidence: 0.7,
			})
		}
	}

	analysis.Findings = len(analysis.OverPermissioned) + len(analysis.WildcardACLs)
	return analysis
}

// ConfigCompareResult contains cross-cluster config comparison results.
type ConfigCompareResult struct {
	SourceCluster string       `json:"source_cluster"`
	TargetCluster string       `json:"target_cluster"`
	Diffs         []ConfigDiff `json:"diffs"`
	MatchCount    int          `json:"matching_topics"`
	DiffCount     int          `json:"differing_topics"`
}

// ConfigDiff represents a config difference for a single topic.
type ConfigDiff struct {
	Topic   string                `json:"topic"`
	Diffs   []ConfigValueDiff     `json:"config_diffs"`
	OnlyIn  string                `json:"only_in,omitempty"` // source or target
}

// ConfigValueDiff represents a single config value difference.
type ConfigValueDiff struct {
	Key         string `json:"key"`
	SourceValue string `json:"source_value"`
	TargetValue string `json:"target_value"`
}

// CompareConfigs compares topic configs between two clusters.
func (m *Manager) CompareConfigs(ctx context.Context, sourceCluster, targetCluster, specificTopic string) (*ConfigCompareResult, error) {
	sourceTopics, err := m.ListTopics(ctx, sourceCluster, false)
	if err != nil {
		return nil, fmt.Errorf("listing source topics: %w", err)
	}
	targetTopics, err := m.ListTopics(ctx, targetCluster, false)
	if err != nil {
		return nil, fmt.Errorf("listing target topics: %w", err)
	}

	targetSet := make(map[string]bool)
	for _, t := range targetTopics {
		targetSet[t.Name] = true
	}

	result := &ConfigCompareResult{
		SourceCluster: sourceCluster,
		TargetCluster: targetCluster,
	}

	for _, st := range sourceTopics {
		if specificTopic != "" && st.Name != specificTopic {
			continue
		}
		if !targetSet[st.Name] {
			result.Diffs = append(result.Diffs, ConfigDiff{
				Topic: st.Name, OnlyIn: "source",
			})
			continue
		}

		sourceDetail, err := m.DescribeTopic(ctx, sourceCluster, st.Name)
		if err != nil {
			continue
		}
		targetDetail, err := m.DescribeTopic(ctx, targetCluster, st.Name)
		if err != nil {
			continue
		}

		var valueDiffs []ConfigValueDiff
		allKeys := make(map[string]bool)
		for k := range sourceDetail.Configs {
			allKeys[k] = true
		}
		for k := range targetDetail.Configs {
			allKeys[k] = true
		}

		for key := range allKeys {
			sv := sourceDetail.Configs[key]
			tv := targetDetail.Configs[key]
			if sv != tv {
				valueDiffs = append(valueDiffs, ConfigValueDiff{
					Key: key, SourceValue: sv, TargetValue: tv,
				})
			}
		}

		if len(valueDiffs) > 0 {
			result.Diffs = append(result.Diffs, ConfigDiff{
				Topic: st.Name, Diffs: valueDiffs,
			})
			result.DiffCount++
		} else {
			result.MatchCount++
		}
	}

	// Check for topics only in target
	sourceSet := make(map[string]bool)
	for _, t := range sourceTopics {
		sourceSet[t.Name] = true
	}
	for _, tt := range targetTopics {
		if specificTopic != "" && tt.Name != specificTopic {
			continue
		}
		if !sourceSet[tt.Name] {
			result.Diffs = append(result.Diffs, ConfigDiff{
				Topic: tt.Name, OnlyIn: "target",
			})
		}
	}

	return result, nil
}

// ConfigAuditResult contains config audit results against a template.
type ConfigAuditResult struct {
	Cluster    string            `json:"cluster"`
	Deviations []ConfigDeviation `json:"deviations"`
	Compliant  int               `json:"compliant_topics"`
	NonCompliant int             `json:"non_compliant_topics"`
}

// ConfigDeviation represents a deviation from the golden template.
type ConfigDeviation struct {
	Topic       string `json:"topic"`
	Key         string `json:"config_key"`
	Expected    string `json:"expected_value"`
	Actual      string `json:"actual_value"`
	Remediation string `json:"remediation_command"`
}

// AuditConfigs audits topic configs against a golden template.
func (m *Manager) AuditConfigs(ctx context.Context, cluster, specificTopic string, template map[string]string) (*ConfigAuditResult, error) {
	topics, err := m.ListTopics(ctx, cluster, false)
	if err != nil {
		return nil, err
	}

	result := &ConfigAuditResult{
		Cluster: m.resolveCluster(cluster),
	}

	for _, topic := range topics {
		if specificTopic != "" && topic.Name != specificTopic {
			continue
		}

		detail, err := m.DescribeTopic(ctx, cluster, topic.Name)
		if err != nil {
			continue
		}

		hasDeviation := false
		for key, expected := range template {
			actual := detail.Configs[key]
			if actual != expected {
				hasDeviation = true
				result.Deviations = append(result.Deviations, ConfigDeviation{
					Topic:    topic.Name,
					Key:      key,
					Expected: expected,
					Actual:   actual,
					Remediation: fmt.Sprintf("kafka-configs --alter --entity-type topics --entity-name %s --add-config %s=%s",
						topic.Name, key, expected),
				})
			}
		}

		if hasDeviation {
			result.NonCompliant++
		} else {
			result.Compliant++
		}
	}

	return result, nil
}
