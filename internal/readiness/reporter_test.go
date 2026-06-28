package readiness

import (
	"context"
	"strings"
	"testing"
	"time"
)

// stubRunner lets tests control command exit codes and output without running real commands.
type stubRunner struct {
	responses map[string]cmdResponse
}

type deadlineRunner struct {
	deadlines []time.Time
}

func (r *deadlineRunner) Run(ctx context.Context, _ string, _ ...string) (string, int, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return "", 1, &pathError{path: "missing deadline"}
	}
	r.deadlines = append(r.deadlines, deadline)
	return "", 0, nil
}

func TestReporter_AppliesTimeoutPerGate(t *testing.T) {
	runner := &deadlineRunner{}
	reporter := NewReporter(runner, minimalFS("/repo"), "/repo", WithTimeout(time.Minute))
	_ = reporter.Evaluate(context.Background())
	if len(runner.deadlines) < 2 {
		t.Fatalf("captured %d command deadlines, want at least 2", len(runner.deadlines))
	}
	if !runner.deadlines[len(runner.deadlines)-1].After(runner.deadlines[0]) {
		t.Fatal("later gates must receive a fresh timeout window")
	}
}

type cmdResponse struct {
	output   string
	exitCode int
	err      error
}

func (s *stubRunner) Run(_ context.Context, name string, args ...string) (string, int, error) {
	key := name
	if len(args) > 0 {
		key = name + " " + args[0]
	}
	if r, ok := s.responses[key]; ok {
		return r.output, r.exitCode, r.err
	}
	// Default: command "succeeds" with no output.
	return "", 0, nil
}

// stubFS lets tests control file reads.
type stubFS struct {
	files map[string][]byte
}

func (s *stubFS) ReadFile(path string) ([]byte, error) {
	if data, ok := s.files[path]; ok {
		return data, nil
	}
	return nil, &pathError{path: path}
}

type pathError struct{ path string }

func (e *pathError) Error() string { return "file not found: " + e.path }

// allPassRunner returns a runner where every command succeeds.
func allPassRunner() *stubRunner {
	return &stubRunner{
		responses: map[string]cmdResponse{
			"gofmt -l":             {output: "", exitCode: 0},
			"go vet":               {output: "", exitCode: 0},
			"golangci-lint run":    {output: "", exitCode: 0},
			"go test":              {output: "ok  github.com/josedab/kafkamcp/...", exitCode: 0},
			"go tool":              {output: "total:\t(statements)\t75.0%", exitCode: 0},
			"govulncheck ./...":    {output: "", exitCode: 0},
			"go run":               {output: "gosec findings: 0 total, 0 blocking", exitCode: 0},
			"goreleaser --version": {output: "goreleaser version 2.17.0", exitCode: 0},
			"goreleaser check":     {output: "", exitCode: 0},
			"git diff":             {output: "", exitCode: 0},
			"go build":             {output: "", exitCode: 0},
		},
	}
}

func TestReporter_GosecBlocksReadiness(t *testing.T) {
	runner := allPassRunner()
	runner.responses["go run"] = cmdResponse{
		output:   "gosec production gate failed with 1 blocking finding",
		exitCode: 1,
	}
	report := NewReporter(runner, minimalFS("/repo"), "/repo").Evaluate(context.Background())
	if report.Overall != StatusFail {
		t.Fatalf("gosec failure must block readiness, got %s", report.Overall)
	}
}

func minimalFS(projectDir string) *stubFS {
	return &stubFS{
		files: map[string][]byte{
			projectDir + "/.goreleaser.yml":                     []byte("version: 2\nsboms:\nsigns:\ndocker_signs:\n"),
			projectDir + "/docs/tool-reference.md":              []byte("# tool reference"),
			projectDir + "/website/docs/api-reference/tools.md": []byte("# tool reference"),
			projectDir + "/docs/OPERATIONS.md":                  []byte("# Operations"),
			projectDir + "/docs/RELEASE.md":                     []byte("# Release"),
			projectDir + "/docs/ROADMAP.md":                     []byte("# Roadmap"),
			projectDir + "/docs/KIP-1318.md":                    []byte("# KIP-1318"),
			projectDir + "/docs/COMPATIBILITY.md":               []byte("# Compatibility"),
			projectDir + "/docs/API_STABILITY.md":               []byte("# API stability"),
			projectDir + "/docs/PRODUCTION_CHECKLIST.md":        []byte("# Production checklist"),
			projectDir + "/.github/workflows/release.yml":       []byte("uses: actions/attest-build-provenance@v3\npush-to-registry: true\n"),
		},
	}
}

func TestReporter_AllPass(t *testing.T) {
	runner := allPassRunner()
	fs := minimalFS("/repo")
	r := NewReporter(runner, fs, "/repo")
	rep := r.Evaluate(context.Background())

	// Manual gates should never auto-pass
	for _, g := range rep.Gates {
		if g.Kind == GateManual && g.Status == StatusPass {
			t.Errorf("manual gate %q auto-passed: must always be MANUAL", g.Name)
		}
	}

	// Mandatory gates should all pass given all-pass runner
	for _, g := range rep.Gates {
		if g.Kind == GateMandatory && (g.Status == StatusFail) {
			t.Errorf("mandatory gate %q failed unexpectedly: %s", g.Name, g.Message)
		}
	}
}

func TestReporter_MandatoryFailed_ExitSignal(t *testing.T) {
	runner := &stubRunner{
		responses: map[string]cmdResponse{
			// go test fails
			"go test":  {output: "FAIL github.com/josedab/kafkamcp/internal/kafka", exitCode: 1},
			"go tool":  {output: "total:\t\t\t\t55.0%", exitCode: 0},
			"gofmt -l": {output: "", exitCode: 0},
			"go vet":   {output: "", exitCode: 0},
		},
	}

	fs := minimalFS("/repo")
	r := NewReporter(runner, fs, "/repo")
	rep := r.Evaluate(context.Background())

	if rep.Overall != StatusFail {
		t.Errorf("expected overall FAIL, got %s", rep.Overall)
	}
	failed := rep.MandatoryFailed()
	if len(failed) == 0 {
		t.Error("expected mandatory failures, got none")
	}
}

func TestReporter_MandatoryWarningStillBlocksReadiness(t *testing.T) {
	runner := allPassRunner()
	runner.responses["golangci-lint run"] = cmdResponse{err: &pathError{path: "golangci-lint"}}
	report := NewReporter(runner, minimalFS("/repo"), "/repo").Evaluate(context.Background())
	if report.Overall != StatusFail {
		t.Fatalf("mandatory degraded gate must fail readiness, got %s", report.Overall)
	}
}

func TestReporter_ManualGatesNeverAutoPass(t *testing.T) {
	// Even with all-pass runner, manual gates must stay MANUAL
	runner := allPassRunner()
	fs := minimalFS("/repo")
	r := NewReporter(runner, fs, "/repo")
	rep := r.Evaluate(context.Background())

	manualNames := []string{
		"elapsed reliability window",
		"customer adoption",
		"paid pilot customers",
		"published case studies",
	}
	for _, prefix := range manualNames {
		found := false
		for _, g := range rep.Gates {
			if strings.HasPrefix(g.Name, prefix) {
				found = true
				if g.Status != StatusManual {
					t.Errorf("gate %q should always be MANUAL, got %s", g.Name, g.Status)
				}
			}
		}
		if !found {
			t.Errorf("manual gate with prefix %q not found in report", prefix)
		}
	}
}

func TestReporter_GorelaserMissing(t *testing.T) {
	runner := allPassRunner()
	fs := &stubFS{files: map[string][]byte{
		"/repo/docs/tool-reference.md": []byte("# tool reference"),
		// .goreleaser.yml intentionally absent
	}}
	r := NewReporter(runner, fs, "/repo")
	rep := r.Evaluate(context.Background())

	for _, g := range rep.Gates {
		if strings.Contains(g.Name, ".goreleaser.yml") ||
			strings.Contains(g.Name, "SBOM") || strings.Contains(g.Name, "signing") {
			if g.Status != StatusFail && g.Status != StatusWarn {
				t.Errorf("gate %q: expected FAIL or WARN when goreleaser missing, got %s", g.Name, g.Status)
			}
		}
	}
}

func TestReport_MandatoryFailed(t *testing.T) {
	rep := &Report{
		Gates: []GateResult{
			{Name: "a", Kind: GateMandatory, Status: StatusFail},
			{Name: "b", Kind: GateMandatory, Status: StatusPass},
			{Name: "c", Kind: GateAdvisory, Status: StatusFail},
			{Name: "d", Kind: GateManual, Status: StatusManual},
		},
	}
	failed := rep.MandatoryFailed()
	if len(failed) != 1 || failed[0].Name != "a" {
		t.Errorf("MandatoryFailed: expected [a], got %+v", failed)
	}
}

func TestWriteHuman(t *testing.T) {
	rep := &Report{
		Timestamp: "2026-01-01T00:00:00Z",
		Overall:   StatusFail,
		Gates: []GateResult{
			{Name: "unit tests", Kind: GateMandatory, Status: StatusFail, Message: "tests failed", Remediation: "run make test"},
			{Name: "go vet", Kind: GateMandatory, Status: StatusPass, Message: "no vet issues"},
			{Name: "elapsed reliability window", Kind: GateManual, Status: StatusManual, Message: "requires human verification"},
		},
	}
	var sb strings.Builder
	if err := WriteHuman(&sb, rep); err != nil {
		t.Fatalf("WriteHuman: %v", err)
	}
	out := sb.String()

	if !strings.Contains(out, "KafkaMCP v1 Readiness Report") {
		t.Error("missing header in human output")
	}
	if !strings.Contains(out, "unit tests") {
		t.Error("missing gate name in human output")
	}
	if !strings.Contains(out, "BLOCKING") {
		t.Error("missing BLOCKING marker in human output with failed mandatory gate")
	}
	if !strings.Contains(out, "Remediation: run make test") {
		t.Error("missing remediation in human output")
	}
}

func TestWriteJSON(t *testing.T) {
	rep := &Report{
		Timestamp: "2026-01-01T00:00:00Z",
		Overall:   StatusPass,
		Gates: []GateResult{
			{Name: "go vet", Kind: GateMandatory, Status: StatusPass, Message: "no vet issues"},
		},
	}
	var sb strings.Builder
	if err := WriteJSON(&sb, rep); err != nil {
		t.Fatalf("WriteJSON error: %v", err)
	}
	out := sb.String()
	if !strings.Contains(out, `"overall"`) {
		t.Error("missing 'overall' in JSON output")
	}
	if !strings.Contains(out, `"PASS"`) {
		t.Error("missing 'PASS' in JSON output")
	}
}

func TestExtractTotalCoverage(t *testing.T) {
	cases := []struct {
		input    string
		expected float64
	}{
		{"total:\t(statements)\t75.3%", 75.3},
		{"total:\t(statements)\t82.0%", 82.0},
		{"something else\ntotal:\t(statements)\t65.1%", 65.1},
		{"no coverage here", -1},
	}
	for _, c := range cases {
		got := extractTotalCoverage(c.input)
		if got != c.expected {
			t.Errorf("extractTotalCoverage(%q) = %v, want %v", c.input, got, c.expected)
		}
	}
}

func TestStatusIcon(t *testing.T) {
	cases := map[Status]string{
		StatusPass:   "✓",
		StatusFail:   "✗",
		StatusWarn:   "!",
		StatusManual: "?",
		StatusSkip:   "-",
	}
	for status, icon := range cases {
		if got := statusIcon(status); got != icon {
			t.Errorf("statusIcon(%s) = %q, want %q", status, got, icon)
		}
	}
}

func TestFilterLines(t *testing.T) {
	lines := []string{"ok pkg/a", "FAIL pkg/b", "FAIL pkg/c", "ok pkg/d"}
	got := filterLines(lines, func(l string) bool { return strings.HasPrefix(l, "FAIL") })
	if len(got) != 2 {
		t.Errorf("filterLines: expected 2, got %d: %v", len(got), got)
	}
}
