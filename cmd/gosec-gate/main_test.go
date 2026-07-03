package main

import (
	"strings"
	"testing"
)

func TestEvaluateReport(t *testing.T) {
	t.Parallel()
	report := gosecReport{Issues: []gosecIssue{
		{Severity: "HIGH", Confidence: "MEDIUM", RuleID: "G115"},
		{Severity: "MEDIUM", Confidence: "HIGH", RuleID: "G204"},
	}}
	if err := evaluateReport(report); err != nil {
		t.Fatalf("advisory findings should pass: %v", err)
	}

	report.Issues = append(report.Issues, gosecIssue{
		Severity:   "HIGH",
		Confidence: "HIGH",
		RuleID:     "G702",
		File:       "main.go",
		Line:       "10",
	})
	if err := evaluateReport(report); err == nil || !strings.Contains(err.Error(), "1 blocking") {
		t.Fatalf("blocking finding error = %v", err)
	}
}

func TestEvaluateReportRejectsAnalysisErrors(t *testing.T) {
	t.Parallel()
	err := evaluateReport(gosecReport{
		GolangErrors: map[string]any{"package": []string{"build failed"}},
	})
	if err == nil {
		t.Fatal("analysis errors must fail the gate")
	}
}
