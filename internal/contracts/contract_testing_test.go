package contracts

import (
	"testing"
)

func TestContractTester_Compatibility_Pass(t *testing.T) {
	t.Parallel()
	ct := NewContractTester()
	ct.RegisterPair("orders", "order-service", []string{"billing-service", "shipping-service"})
	ct.RegisterProducerSchema("orders", "order-service", []string{"order_id", "amount", "customer_id", "status"})
	ct.RegisterConsumerSchema("orders", "billing-service", []string{"order_id", "amount"})
	ct.RegisterConsumerSchema("orders", "shipping-service", []string{"order_id", "customer_id"})

	result := ct.TestCompatibility("orders")
	if !result.Compatible {
		t.Errorf("expected compatible, got issues: %v", result.Issues)
	}
}

func TestContractTester_Compatibility_Fail(t *testing.T) {
	t.Parallel()
	ct := NewContractTester()
	ct.RegisterPair("orders", "order-service", []string{"billing-service"})
	ct.RegisterProducerSchema("orders", "order-service", []string{"order_id", "amount"})
	ct.RegisterConsumerSchema("orders", "billing-service", []string{"order_id", "amount", "tax_rate"})

	result := ct.TestCompatibility("orders")
	if result.Compatible {
		t.Error("expected incompatible due to missing tax_rate")
	}
	if len(result.Issues) != 1 {
		t.Errorf("expected 1 issue, got %d", len(result.Issues))
	}
	if result.Issues[0].Field != "tax_rate" {
		t.Errorf("expected tax_rate issue, got %s", result.Issues[0].Field)
	}
}

func TestContractTester_TestSuite(t *testing.T) {
	t.Parallel()
	engine := NewEngine()
	engine.RegisterContract(Contract{
		Name: "order-contract", Version: "1.0", Topic: "orders", Owner: "team-a",
		Quality: []QualityRule{
			{Field: "order_id", Rule: "not_null", Severity: "error"},
			{Field: "status", Rule: "enum", Value: "pending,shipped,delivered", Severity: "error"},
		},
	})

	ct := NewContractTester()
	suite := ct.CreateTestSuite("order-validation", "orders", []ContractTestCase{
		{Name: "valid order", Input: map[string]any{"order_id": "123", "status": "pending"}, ExpectValid: true},
		{Name: "missing order_id", Input: map[string]any{"status": "pending"}, ExpectValid: false, ExpectRules: []string{"not_null"}},
		{Name: "invalid status", Input: map[string]any{"order_id": "123", "status": "cancelled"}, ExpectValid: false, ExpectRules: []string{"enum"}},
	})

	result, err := ct.RunTestSuite(suite.ID, engine)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Failed != 0 {
		for _, r := range result.Results {
			if !r.Passed {
				t.Errorf("test case %q failed: expected=%s actual=%s", r.Name, r.Expected, r.Actual)
			}
		}
	}
	if result.Passed != 3 {
		t.Errorf("expected 3 passed, got %d", result.Passed)
	}
}

func TestContractTester_DetectBreakingChanges(t *testing.T) {
	t.Parallel()
	issues := DetectBreakingChanges(
		[]string{"order_id", "amount", "customer_id"},
		[]string{"order_id", "amount", "tax_rate"},
	)

	hasRemoved := false
	hasNew := false
	for _, issue := range issues {
		if issue.IssueType == "removed_field" && issue.Field == "customer_id" {
			hasRemoved = true
		}
		if issue.IssueType == "new_field" && issue.Field == "tax_rate" {
			hasNew = true
		}
	}
	if !hasRemoved {
		t.Error("expected removed_field issue for customer_id")
	}
	if !hasNew {
		t.Error("expected new_field issue for tax_rate")
	}
}

func TestContractTester_ListPairs(t *testing.T) {
	t.Parallel()
	ct := NewContractTester()
	ct.RegisterPair("t1", "p1", []string{"c1"})
	ct.RegisterPair("t2", "p2", []string{"c2", "c3"})

	pairs := ct.ListPairs()
	if len(pairs) != 2 {
		t.Errorf("expected 2 pairs, got %d", len(pairs))
	}
}

func TestContractTester_ListTestSuites(t *testing.T) {
	t.Parallel()
	ct := NewContractTester()
	ct.CreateTestSuite("s1", "t1", nil)
	ct.CreateTestSuite("s2", "t2", nil)

	suites := ct.ListTestSuites()
	if len(suites) != 2 {
		t.Errorf("expected 2 suites, got %d", len(suites))
	}
}
