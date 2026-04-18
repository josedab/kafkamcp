package lineage

import (
	"regexp"
	"strings"
	"sync"
	"time"
)

// LineageGraph represents the full lineage graph for a topic.
type LineageGraph struct {
	Nodes     []LineageNode   `json:"nodes"`
	Edges     []LineageEdge   `json:"edges"`
	PIIFields []PIIAnnotation `json:"pii_fields"`
}

// LineageNode represents a node in the lineage graph.
type LineageNode struct {
	ID       string         `json:"id"`
	Name     string         `json:"name"`
	Type     string         `json:"type"` // topic, producer, consumer, schema
	Metadata map[string]any `json:"metadata,omitempty"`
}

// LineageEdge represents a directed edge in the lineage graph.
type LineageEdge struct {
	Source  string `json:"source"`
	Target string `json:"target"`
	Type   string `json:"type"` // produces, consumes, transforms
	Latency string `json:"latency,omitempty"`
}

// PIIAnnotation marks a field in a topic as containing PII.
type PIIAnnotation struct {
	Topic      string `json:"topic"`
	Field      string `json:"field"`
	PIIType    string `json:"pii_type"`    // email, phone, ssn, name, address, ip
	DetectedBy string `json:"detected_by"` // pattern, annotation
}

// ComplianceReport is a compliance report for a given framework.
type ComplianceReport struct {
	Framework      string           `json:"framework"` // GDPR, CCPA
	PIITopics      []PIITopicReport `json:"pii_topics"`
	TotalPIIFields int              `json:"total_pii_fields"`
	Timestamp      string           `json:"timestamp"`
}

// PIITopicReport summarizes PII exposure for a single topic.
type PIITopicReport struct {
	Topic        string          `json:"topic"`
	Fields       []PIIAnnotation `json:"fields"`
	Consumers    []string        `json:"consumers"`
	DeletionPath string          `json:"deletion_path"`
}

// Tracker tracks topic data lineage.
type Tracker struct {
	mu        sync.RWMutex
	nodes     map[string]LineageNode
	edges     []LineageEdge
	piiFields []PIIAnnotation
}

// NewTracker creates a new Tracker.
func NewTracker() *Tracker {
	return &Tracker{
		nodes: make(map[string]LineageNode),
	}
}

// AddNode adds a node to the lineage graph.
func (t *Tracker) AddNode(node LineageNode) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[node.ID] = node
}

// AddEdge adds an edge to the lineage graph.
func (t *Tracker) AddEdge(edge LineageEdge) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.edges = append(t.edges, edge)
}

// AnnotatePII records a PII annotation.
func (t *Tracker) AnnotatePII(annotation PIIAnnotation) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.piiFields = append(t.piiFields, annotation)
}

// TraceLineage traces all upstream and downstream nodes for a topic.
func (t *Tracker) TraceLineage(topic string) *LineageGraph {
	t.mu.RLock()
	defer t.mu.RUnlock()

	visited := make(map[string]bool)
	var nodes []LineageNode
	var edges []LineageEdge

	// Collect all connected nodes via BFS in both directions.
	queue := []string{topic}
	visited[topic] = true

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if node, ok := t.nodes[current]; ok {
			nodes = append(nodes, node)
		}

		for _, e := range t.edges {
			if e.Source == current || e.Target == current {
				edges = appendEdgeUnique(edges, e)
				next := e.Target
				if e.Target == current {
					next = e.Source
				}
				if !visited[next] {
					visited[next] = true
					queue = append(queue, next)
				}
			}
		}
	}

	var pii []PIIAnnotation
	for _, p := range t.piiFields {
		if visited[p.Topic] {
			pii = append(pii, p)
		}
	}

	return &LineageGraph{
		Nodes:     nodes,
		Edges:     edges,
		PIIFields: pii,
	}
}

func appendEdgeUnique(edges []LineageEdge, e LineageEdge) []LineageEdge {
	for _, existing := range edges {
		if existing.Source == e.Source && existing.Target == e.Target && existing.Type == e.Type {
			return edges
		}
	}
	return append(edges, e)
}

// GetUpstream returns nodes that produce to this topic.
func (t *Tracker) GetUpstream(topic string) []LineageNode {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var result []LineageNode
	seen := make(map[string]bool)
	for _, e := range t.edges {
		if e.Target == topic {
			if node, ok := t.nodes[e.Source]; ok && !seen[node.ID] {
				seen[node.ID] = true
				result = append(result, node)
			}
		}
	}
	return result
}

// GetDownstream returns nodes that consume from this topic.
func (t *Tracker) GetDownstream(topic string) []LineageNode {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var result []LineageNode
	seen := make(map[string]bool)
	for _, e := range t.edges {
		if e.Source == topic {
			if node, ok := t.nodes[e.Target]; ok && !seen[node.ID] {
				seen[node.ID] = true
				result = append(result, node)
			}
		}
	}
	return result
}

// GenerateComplianceReport generates a GDPR/CCPA compliance report.
func (t *Tracker) GenerateComplianceReport(framework string) *ComplianceReport {
	t.mu.RLock()
	defer t.mu.RUnlock()

	topicPII := make(map[string][]PIIAnnotation)
	for _, p := range t.piiFields {
		topicPII[p.Topic] = append(topicPII[p.Topic], p)
	}

	var reports []PIITopicReport
	totalFields := 0
	for topic, fields := range topicPII {
		var consumers []string
		seen := make(map[string]bool)
		for _, e := range t.edges {
			if e.Source == topic {
				if node, ok := t.nodes[e.Target]; ok && !seen[node.ID] {
					seen[node.ID] = true
					consumers = append(consumers, node.Name)
				}
			}
		}

		deletionPath := buildDeletionPath(topic, consumers)
		reports = append(reports, PIITopicReport{
			Topic:        topic,
			Fields:       fields,
			Consumers:    consumers,
			DeletionPath: deletionPath,
		})
		totalFields += len(fields)
	}

	return &ComplianceReport{
		Framework:      framework,
		PIITopics:      reports,
		TotalPIIFields: totalFields,
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
	}
}

func buildDeletionPath(topic string, consumers []string) string {
	if len(consumers) == 0 {
		return topic
	}
	return topic + " -> " + strings.Join(consumers, ", ")
}

// piiPatterns maps PII types to field-name and value patterns.
var piiPatterns = map[string]struct {
	fieldPatterns []string
	valuePattern  *regexp.Regexp
}{
	"email": {
		fieldPatterns: []string{"email", "e_mail", "email_address"},
		valuePattern:  regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`),
	},
	"phone": {
		fieldPatterns: []string{"phone", "telephone", "mobile", "cell", "phone_number"},
		valuePattern:  regexp.MustCompile(`^\+?[0-9\-\s\(\)]{7,}$`),
	},
	"ssn": {
		fieldPatterns: []string{"ssn", "social_security", "social_security_number"},
		valuePattern:  regexp.MustCompile(`^\d{3}-?\d{2}-?\d{4}$`),
	},
	"name": {
		fieldPatterns: []string{"first_name", "last_name", "full_name", "firstname", "lastname", "fullname", "surname"},
		valuePattern:  nil,
	},
	"address": {
		fieldPatterns: []string{"address", "street", "city", "zip", "zipcode", "zip_code", "postal"},
		valuePattern:  nil,
	},
	"ip": {
		fieldPatterns: []string{"ip", "ip_address", "ipaddress", "client_ip", "remote_addr"},
		valuePattern:  regexp.MustCompile(`^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$`),
	},
}

// DetectPIIPatterns auto-detects PII from field names and sample values.
func (t *Tracker) DetectPIIPatterns(topic string, sampleValues map[string][]string) []PIIAnnotation {
	var annotations []PIIAnnotation
	seen := make(map[string]bool)

	for field, values := range sampleValues {
		normalized := strings.ToLower(field)

		for piiType, patterns := range piiPatterns {
			key := field + ":" + piiType
			if seen[key] {
				continue
			}

			// Check field name match.
			for _, fp := range patterns.fieldPatterns {
				if strings.Contains(normalized, fp) {
					seen[key] = true
					annotations = append(annotations, PIIAnnotation{
						Topic:      topic,
						Field:      field,
						PIIType:    piiType,
						DetectedBy: "pattern",
					})
					break
				}
			}
			if seen[key] {
				continue
			}

			// Check value pattern match.
			if patterns.valuePattern != nil {
				for _, v := range values {
					if patterns.valuePattern.MatchString(v) {
						seen[key] = true
						annotations = append(annotations, PIIAnnotation{
							Topic:      topic,
							Field:      field,
							PIIType:    piiType,
							DetectedBy: "pattern",
						})
						break
					}
				}
			}
		}
	}

	return annotations
}
