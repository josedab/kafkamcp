// Command gosec-gate runs the pinned gosec analyzer and fails only for
// high-severity, high-confidence findings. The complete JSON report remains
// available for advisory review.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

const defaultGosecVersion = "v2.28.0"

type gosecIssue struct {
	Severity   string `json:"severity"`
	Confidence string `json:"confidence"`
	RuleID     string `json:"rule_id"`
	File       string `json:"file"`
	Line       string `json:"line"`
	Details    string `json:"details"`
}

type gosecReport struct {
	Issues       []gosecIssue   `json:"Issues"`
	GolangErrors map[string]any `json:"Golang errors"`
}

func main() {
	var outputPath string
	flag.StringVar(&outputPath, "out", "", "keep the complete gosec JSON report at this path")
	flag.Parse()

	if err := run(outputPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(outputPath string) error {
	reportPath := outputPath
	removeReport := false
	if reportPath == "" {
		file, err := os.CreateTemp("", "kafkamcp-gosec-*.json")
		if err != nil {
			return fmt.Errorf("create temporary gosec report: %w", err)
		}
		reportPath = file.Name()
		if err := file.Close(); err != nil {
			return fmt.Errorf("close temporary gosec report: %w", err)
		}
		removeReport = true
	}
	if removeReport {
		defer func() { _ = os.Remove(reportPath) }()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	// #nosec G702 -- the Go executable, pinned module version, and analyzer
	// arguments are fixed; reportPath is passed as one argv value without a shell.
	cmd := exec.CommandContext(ctx, "go", "run",
		"github.com/securego/gosec/v2/cmd/gosec@"+defaultGosecVersion,
		"-quiet",
		"-fmt", "json",
		"-out", reportPath,
		"./...",
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	if ctx.Err() != nil {
		return fmt.Errorf("gosec timed out: %w", ctx.Err())
	}
	var exitErr *exec.ExitError
	if runErr != nil && !errors.As(runErr, &exitErr) {
		return fmt.Errorf("run gosec: %w", runErr)
	}
	if exitErr != nil && exitErr.ExitCode() != 1 {
		return fmt.Errorf("gosec exited with code %d: %s", exitErr.ExitCode(), strings.TrimSpace(stderr.String()))
	}

	data, err := os.ReadFile(reportPath)
	if err != nil {
		return fmt.Errorf("read gosec report: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	var report gosecReport
	if err := json.Unmarshal(data, &report); err != nil {
		return fmt.Errorf("parse gosec report: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return evaluateReport(report)
}

func evaluateReport(report gosecReport) error {
	if len(report.GolangErrors) > 0 {
		return fmt.Errorf("gosec could not analyze %d package(s); inspect the JSON report", len(report.GolangErrors))
	}
	var blocking []gosecIssue
	for _, issue := range report.Issues {
		if strings.EqualFold(issue.Severity, "HIGH") &&
			strings.EqualFold(issue.Confidence, "HIGH") {
			blocking = append(blocking, issue)
		}
	}
	fmt.Printf("gosec findings: %d total, %d high-severity/high-confidence blocking\n", len(report.Issues), len(blocking))
	if len(blocking) == 0 {
		return nil
	}
	for _, issue := range blocking {
		fmt.Fprintf(os.Stderr, "%s:%s: %s %s: %s\n",
			issue.File, issue.Line, issue.RuleID, issue.Severity, issue.Details)
	}
	return fmt.Errorf("gosec production gate failed with %d blocking finding(s)", len(blocking))
}
