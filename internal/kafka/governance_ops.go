package kafka

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
)

// TraceResult represents the result of tracing a message across topics.
type TraceResult struct {
	CorrelationID  string     `json:"correlation_id"`
	Hops           []TraceHop `json:"hops"`
	TopicsSearched int        `json:"topics_searched"`
	TotalMessages  int        `json:"total_messages_found"`
	FlowGraph      []FlowEdge `json:"flow_graph"`
}

// TraceHop represents a single hop in a message's journey.
type TraceHop struct {
	Topic     string            `json:"topic"`
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Timestamp string            `json:"timestamp"`
	Key       any               `json:"key,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// FlowEdge represents an edge in the message flow graph.
type FlowEdge struct {
	FromTopic string `json:"from_topic"`
	ToTopic   string `json:"to_topic"`
	LatencyMs int64  `json:"latency_ms"`
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
			Topic:         topic,
			ValueContains: correlationID,
			MaxScan:       maxScan,
			Limit:         100,
		})
		if err != nil {
			continue
		}
		result.TopicsSearched++

		for _, msg := range msgs {
			for _, hn := range headerNames {
				if val, ok := msg.Headers[hn]; ok && val == correlationID {
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
	}

	sort.Slice(result.Hops, func(i, j int) bool {
		return result.Hops[i].Timestamp < result.Hops[j].Timestamp
	})

	result.TotalMessages = len(result.Hops)

	for i := 1; i < len(result.Hops); i++ {
		prev := result.Hops[i-1]
		curr := result.Hops[i]
		if prev.Topic != curr.Topic {
			var latencyMs int64
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
	Permission   string `json:"permission"`
	PatternType  string `json:"pattern_type"`
}

// ACLResult contains ACL listing and security analysis.
type ACLResult struct {
	ACLs             []ACLEntry        `json:"acls"`
	TotalACLs        int               `json:"total_acls"`
	SecurityAnalysis *SecurityAnalysis `json:"security_analysis,omitempty"`
}

// SecurityAnalysis provides security insights about ACLs.
type SecurityAnalysis struct {
	OverPermissioned []SecurityFinding `json:"over_permissioned"`
	WildcardACLs     []SecurityFinding `json:"wildcard_acls"`
	Findings         int               `json:"total_findings"`
}

// SecurityFinding describes a single security finding.
type SecurityFinding struct {
	Principal  string  `json:"principal"`
	Issue      string  `json:"issue"`
	Severity   string  `json:"severity"`
	Confidence float64 `json:"confidence"`
}

// ListACLs lists all Kafka ACLs with optional security analysis.
func (m *Manager) ListACLs(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*ACLResult, error) {
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return nil, err
	}

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

	principalOps := make(map[string][]ACLEntry)
	for _, acl := range acls {
		principalOps[acl.Principal] = append(principalOps[acl.Principal], acl)
	}

	for principal, ops := range principalOps {
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
	Topic  string            `json:"topic"`
	Diffs  []ConfigValueDiff `json:"config_diffs"`
	OnlyIn string            `json:"only_in,omitempty"`
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
	Cluster      string            `json:"cluster"`
	Deviations   []ConfigDeviation `json:"deviations"`
	Compliant    int               `json:"compliant_topics"`
	NonCompliant int               `json:"non_compliant_topics"`
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
