package contracts

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// ProducerConsumerPair defines a producer-consumer relationship for a topic.
type ProducerConsumerPair struct {
	Topic        string   `json:"topic"`
	ProducerID   string   `json:"producer_id"`
	ConsumerIDs  []string `json:"consumer_ids"`
	RegisteredAt string   `json:"registered_at"`
}

// CompatibilityIssue describes a schema/contract incompatibility.
type CompatibilityIssue struct {
	Topic       string `json:"topic"`
	ProducerID  string `json:"producer_id"`
	ConsumerID  string `json:"consumer_id"`
	IssueType   string `json:"issue_type"` // missing_field, type_mismatch, new_required_field, removed_field
	Field       string `json:"field"`
	Description string `json:"description"`
	Severity    string `json:"severity"` // error, warning, info
	DetectedAt  string `json:"detected_at"`
}

// ContractTestResult is the outcome of a contract compatibility test.
type ContractTestResult struct {
	Topic       string               `json:"topic"`
	ProducerID  string               `json:"producer_id"`
	ConsumerIDs []string             `json:"consumer_ids"`
	Compatible  bool                 `json:"compatible"`
	Issues      []CompatibilityIssue `json:"issues"`
	TestedAt    string               `json:"tested_at"`
	Duration    string               `json:"duration"`
}

// ContractTestSuite defines a reusable suite of contract tests.
type ContractTestSuite struct {
	ID        string             `json:"id"`
	Name      string             `json:"name"`
	Topic     string             `json:"topic"`
	TestCases []ContractTestCase `json:"test_cases"`
	CreatedAt string             `json:"created_at"`
}

// ContractTestCase is a single test case within a suite.
type ContractTestCase struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Input       map[string]any `json:"input"`
	ExpectValid bool           `json:"expect_valid"`
	ExpectRules []string       `json:"expect_rules,omitempty"`
}

// ContractTestSuiteResult is the result of running a test suite.
type ContractTestSuiteResult struct {
	SuiteID   string           `json:"suite_id"`
	SuiteName string           `json:"suite_name"`
	Topic     string           `json:"topic"`
	Passed    int              `json:"passed"`
	Failed    int              `json:"failed"`
	Total     int              `json:"total"`
	Results   []TestCaseResult `json:"results"`
	RunAt     string           `json:"run_at"`
}

// TestCaseResult is the outcome of a single test case.
type TestCaseResult struct {
	Name       string   `json:"name"`
	Passed     bool     `json:"passed"`
	Expected   string   `json:"expected"`
	Actual     string   `json:"actual"`
	Violations []string `json:"violations,omitempty"`
}

// ContractTester manages producer/consumer contract testing.
type ContractTester struct {
	mu         sync.RWMutex
	pairs      map[string]*ProducerConsumerPair // key: topic
	schemas    map[string]map[string][]string   // topic -> producerID -> field names
	testSuites map[string]*ContractTestSuite
	nextID     int64
}

// NewContractTester creates a new contract tester.
func NewContractTester() *ContractTester {
	return &ContractTester{
		pairs:      make(map[string]*ProducerConsumerPair),
		schemas:    make(map[string]map[string][]string),
		testSuites: make(map[string]*ContractTestSuite),
	}
}

// RegisterPair registers a producer-consumer relationship for testing.
func (ct *ContractTester) RegisterPair(topic, producerID string, consumerIDs []string) *ProducerConsumerPair {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	pair := &ProducerConsumerPair{
		Topic:        topic,
		ProducerID:   producerID,
		ConsumerIDs:  consumerIDs,
		RegisteredAt: time.Now().UTC().Format(time.RFC3339),
	}
	ct.pairs[topic] = pair
	return pair
}

// RegisterProducerSchema records the fields a producer writes to a topic.
func (ct *ContractTester) RegisterProducerSchema(topic, producerID string, fields []string) {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	if ct.schemas[topic] == nil {
		ct.schemas[topic] = make(map[string][]string)
	}
	ct.schemas[topic][producerID] = fields
}

// RegisterConsumerSchema records the fields a consumer expects from a topic.
func (ct *ContractTester) RegisterConsumerSchema(topic, consumerID string, fields []string) {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	if ct.schemas[topic] == nil {
		ct.schemas[topic] = make(map[string][]string)
	}
	ct.schemas[topic][consumerID] = fields
}

// TestCompatibility checks if producer fields satisfy all consumer expectations.
func (ct *ContractTester) TestCompatibility(topic string) *ContractTestResult {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	start := time.Now()
	result := &ContractTestResult{
		Topic:      topic,
		Compatible: true,
		TestedAt:   start.UTC().Format(time.RFC3339),
	}

	pair, ok := ct.pairs[topic]
	if !ok {
		result.Issues = append(result.Issues, CompatibilityIssue{
			Topic: topic, IssueType: "no_pair", Description: "No producer-consumer pair registered",
			Severity: "warning", DetectedAt: time.Now().UTC().Format(time.RFC3339),
		})
		result.Duration = time.Since(start).String()
		return result
	}

	result.ProducerID = pair.ProducerID
	result.ConsumerIDs = pair.ConsumerIDs

	topicSchemas := ct.schemas[topic]
	if topicSchemas == nil {
		result.Duration = time.Since(start).String()
		return result
	}

	producerFields := topicSchemas[pair.ProducerID]
	producerFieldSet := make(map[string]bool)
	for _, f := range producerFields {
		producerFieldSet[f] = true
	}

	for _, consumerID := range pair.ConsumerIDs {
		consumerFields := topicSchemas[consumerID]
		for _, field := range consumerFields {
			if !producerFieldSet[field] {
				result.Compatible = false
				result.Issues = append(result.Issues, CompatibilityIssue{
					Topic:       topic,
					ProducerID:  pair.ProducerID,
					ConsumerID:  consumerID,
					IssueType:   "missing_field",
					Field:       field,
					Description: fmt.Sprintf("Consumer %q expects field %q but producer %q does not provide it", consumerID, field, pair.ProducerID),
					Severity:    "error",
					DetectedAt:  time.Now().UTC().Format(time.RFC3339),
				})
			}
		}
	}

	result.Duration = time.Since(start).String()
	return result
}

// CreateTestSuite creates a reusable contract test suite.
func (ct *ContractTester) CreateTestSuite(name, topic string, testCases []ContractTestCase) *ContractTestSuite {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	ct.nextID++
	suite := &ContractTestSuite{
		ID:        fmt.Sprintf("suite_%d", ct.nextID),
		Name:      name,
		Topic:     topic,
		TestCases: testCases,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	ct.testSuites[suite.ID] = suite
	return suite
}

// RunTestSuite executes all test cases in a suite against the contract engine.
func (ct *ContractTester) RunTestSuite(suiteID string, engine *Engine) (*ContractTestSuiteResult, error) {
	ct.mu.RLock()
	suite, ok := ct.testSuites[suiteID]
	ct.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("test suite %q not found", suiteID)
	}

	result := &ContractTestSuiteResult{
		SuiteID:   suite.ID,
		SuiteName: suite.Name,
		Topic:     suite.Topic,
		Total:     len(suite.TestCases),
		RunAt:     time.Now().UTC().Format(time.RFC3339),
	}

	for _, tc := range suite.TestCases {
		validation := engine.ValidateMessage(suite.Topic, tc.Input)
		tcResult := TestCaseResult{Name: tc.Name}

		if validation == nil {
			tcResult.Passed = tc.ExpectValid
			tcResult.Expected = fmt.Sprintf("valid=%v", tc.ExpectValid)
			tcResult.Actual = "no contract found"
		} else {
			tcResult.Expected = fmt.Sprintf("valid=%v", tc.ExpectValid)
			tcResult.Actual = fmt.Sprintf("valid=%v", validation.Valid)
			tcResult.Passed = validation.Valid == tc.ExpectValid

			// Check expected violation rules
			if len(tc.ExpectRules) > 0 {
				violatedRules := make(map[string]bool)
				for _, v := range validation.Violations {
					violatedRules[v.Rule] = true
					tcResult.Violations = append(tcResult.Violations, v.Message)
				}
				for _, expectedRule := range tc.ExpectRules {
					if !violatedRules[expectedRule] {
						tcResult.Passed = false
						tcResult.Violations = append(tcResult.Violations,
							fmt.Sprintf("expected rule %q to be violated but it wasn't", expectedRule))
					}
				}
			}
		}

		if tcResult.Passed {
			result.Passed++
		} else {
			result.Failed++
		}
		result.Results = append(result.Results, tcResult)
	}

	return result, nil
}

// ListTestSuites returns all registered test suites.
func (ct *ContractTester) ListTestSuites() []ContractTestSuite {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	var result []ContractTestSuite
	for _, s := range ct.testSuites {
		result = append(result, *s)
	}
	return result
}

// ListPairs returns all registered producer-consumer pairs.
func (ct *ContractTester) ListPairs() []ProducerConsumerPair {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	var result []ProducerConsumerPair
	for _, p := range ct.pairs {
		result = append(result, *p)
	}
	return result
}

// DetectBreakingChanges compares two sets of fields and reports breaking changes.
func DetectBreakingChanges(oldFields, newFields []string) []CompatibilityIssue {
	oldSet := make(map[string]bool)
	for _, f := range oldFields {
		oldSet[f] = true
	}
	newSet := make(map[string]bool)
	for _, f := range newFields {
		newSet[f] = true
	}

	var issues []CompatibilityIssue
	for _, f := range oldFields {
		if !newSet[f] {
			issues = append(issues, CompatibilityIssue{
				IssueType:   "removed_field",
				Field:       f,
				Description: fmt.Sprintf("Field %q was removed — consumers may break", f),
				Severity:    "error",
				DetectedAt:  time.Now().UTC().Format(time.RFC3339),
			})
		}
	}
	for _, f := range newFields {
		if !oldSet[f] {
			issues = append(issues, CompatibilityIssue{
				IssueType:   "new_field",
				Field:       f,
				Description: fmt.Sprintf("New field %q added — consumers should handle gracefully", f),
				Severity:    "info",
				DetectedAt:  time.Now().UTC().Format(time.RFC3339),
			})
		}
	}

	// Check for renamed fields (heuristic: same position, different name)
	for _, removed := range issues {
		if removed.IssueType != "removed_field" {
			continue
		}
		for j, added := range issues {
			if added.IssueType != "new_field" {
				continue
			}
			if strings.HasPrefix(added.Field, removed.Field[:min(3, len(removed.Field))]) {
				issues[j].Severity = "warning"
				issues[j].Description += fmt.Sprintf(" (possibly renamed from %q)", removed.Field)
			}
		}
	}

	return issues
}
