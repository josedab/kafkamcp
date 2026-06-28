// Package readiness implements an objective v1 readiness reporter for KafkaMCP.
// It checks a set of gates — mandatory code-verifiable checks plus explicit
// manual/external gates — and produces human-readable or JSON output.
//
// Inject CmdRunner and FSReader to make all checks testable without requiring
// the full build toolchain present.
package readiness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

// Status represents the outcome of a single gate evaluation.
type Status string

const (
	// StatusPass means the gate completed successfully.
	StatusPass Status = "PASS"
	// StatusFail means the gate failed.
	StatusFail Status = "FAIL"
	// StatusWarn means the gate produced a non-blocking warning.
	StatusWarn Status = "WARN"
	// StatusManual means the gate requires external human verification.
	StatusManual Status = "MANUAL" // requires human verification; never auto-passes
	// StatusSkip means the caller excluded the gate.
	StatusSkip Status = "SKIP" // gate was excluded by the caller
)

const (
	govulncheckVersion = "v1.6.0"
	goreleaserVersion  = "v2.17.0"
)

// GateKind classifies whether a failing gate blocks a v1 release.
type GateKind string

const (
	// GateMandatory blocks readiness when it fails.
	GateMandatory GateKind = "mandatory" // exit non-zero when FAIL
	// GateAdvisory reports a warning without blocking readiness.
	GateAdvisory GateKind = "advisory" // WARN on failure but do not block
	// GateManual can only be verified outside the repository.
	GateManual GateKind = "manual" // always MANUAL; human must verify externally
)

// GateResult is the outcome of evaluating one readiness gate.
type GateResult struct {
	Name        string   `json:"name"`
	Kind        GateKind `json:"kind"`
	Status      Status   `json:"status"`
	Message     string   `json:"message"`
	Remediation string   `json:"remediation,omitempty"`
}

// Report is the full set of gate results for a readiness run.
type Report struct {
	Timestamp string       `json:"timestamp"`
	Overall   Status       `json:"overall"`
	Gates     []GateResult `json:"gates"`
}

// MandatoryFailed returns the gates that are mandatory and failed.
func (r *Report) MandatoryFailed() []GateResult {
	var failed []GateResult
	for _, g := range r.Gates {
		if g.Kind == GateMandatory && g.Status != StatusPass {
			failed = append(failed, g)
		}
	}
	return failed
}

// CmdRunner executes an external command and returns its combined output and
// exit code. A non-zero exit code is not an error; the caller inspects it.
type CmdRunner interface {
	Run(ctx context.Context, name string, args ...string) (output string, exitCode int, err error)
}

// FSReader reads a file from the filesystem.
type FSReader interface {
	ReadFile(path string) ([]byte, error)
}

// RealCmdRunner runs real OS commands.
type RealCmdRunner struct{}

// Run implements CmdRunner using os/exec.
func (r *RealCmdRunner) Run(ctx context.Context, name string, args ...string) (string, int, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Run()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return buf.String(), exitErr.ExitCode(), nil
		}
		return buf.String(), -1, err
	}
	return buf.String(), 0, nil
}

// RealFSReader reads real files from disk.
type RealFSReader struct {
	ReadFileFunc func(string) ([]byte, error)
}

// ReadFile implements FSReader.
func (r *RealFSReader) ReadFile(path string) ([]byte, error) {
	return r.ReadFileFunc(path)
}

// Reporter evaluates all readiness gates and returns a Report.
type Reporter struct {
	runner     CmdRunner
	fs         FSReader
	projectDir string // root of the repository for relative path checks
	timeout    time.Duration
}

// Option configures a Reporter.
type Option func(*Reporter)

// WithTimeout sets the timeout for each individual readiness gate (default:
// 15 minutes). The full report may take longer because gates run sequentially.
func WithTimeout(d time.Duration) Option {
	return func(r *Reporter) { r.timeout = d }
}

// NewReporter creates a Reporter using the provided runner and fs reader.
func NewReporter(runner CmdRunner, fs FSReader, projectDir string, opts ...Option) *Reporter {
	r := &Reporter{
		runner:     runner,
		fs:         fs,
		projectDir: projectDir,
		timeout:    15 * time.Minute,
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Evaluate runs all gates and returns the Report.
func (r *Reporter) Evaluate(ctx context.Context) *Report {
	var results []GateResult
	for _, def := range r.gateDefinitions() {
		gateCtx, cancel := context.WithTimeout(ctx, r.timeout)
		result := def.evaluate(gateCtx, r)
		cancel()
		results = append(results, result)
	}

	overall := StatusPass
	for _, g := range results {
		if g.Kind == GateMandatory && g.Status != StatusPass {
			overall = StatusFail
			break
		}
		if g.Status == StatusManual && overall == StatusPass {
			overall = StatusWarn
		}
	}

	return &Report{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Overall:   overall,
		Gates:     results,
	}
}

// WriteHuman writes a human-readable report to w.
func WriteHuman(w io.Writer, rep *Report) error {
	var output strings.Builder
	output.WriteString("=== KafkaMCP v1 Readiness Report ===\n")
	output.WriteString(fmt.Sprintf("Generated: %s\n\n", rep.Timestamp))

	mandatory := []GateResult{}
	advisory := []GateResult{}
	manual := []GateResult{}
	for _, g := range rep.Gates {
		switch g.Kind {
		case GateMandatory:
			mandatory = append(mandatory, g)
		case GateAdvisory:
			advisory = append(advisory, g)
		case GateManual:
			manual = append(manual, g)
		}
	}

	printSection := func(title string, gates []GateResult) {
		if len(gates) == 0 {
			return
		}
		output.WriteString(fmt.Sprintf("--- %s ---\n", title))
		for _, g := range gates {
			icon := statusIcon(g.Status)
			output.WriteString(fmt.Sprintf("  %s  %-55s %s\n", icon, g.Name, g.Status))
			if g.Message != "" {
				output.WriteString(fmt.Sprintf("       %s\n", g.Message))
			}
			if g.Remediation != "" && g.Status != StatusPass {
				output.WriteString(fmt.Sprintf("       Remediation: %s\n", g.Remediation))
			}
		}
		output.WriteString("\n")
	}

	printSection("Mandatory gates (block release on FAIL)", mandatory)
	printSection("Advisory gates (warning only)", advisory)
	printSection("Manual / external gates (human verification required)", manual)

	output.WriteString(fmt.Sprintf("Overall: %s\n", rep.Overall))
	if failed := rep.MandatoryFailed(); len(failed) > 0 {
		output.WriteString("\n")
		output.WriteString(fmt.Sprintf("BLOCKING: %d mandatory gate(s) failed:\n", len(failed)))
		for _, g := range failed {
			output.WriteString(fmt.Sprintf("  - %s: %s\n", g.Name, g.Message))
		}
	}
	_, err := io.WriteString(w, output.String())
	return err
}

// WriteJSON writes a JSON report to w.
func WriteJSON(w io.Writer, rep *Report) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(rep)
}

func statusIcon(s Status) string {
	switch s {
	case StatusPass:
		return "✓"
	case StatusFail:
		return "✗"
	case StatusWarn:
		return "!"
	case StatusManual:
		return "?"
	case StatusSkip:
		return "-"
	default:
		return " "
	}
}

// gateDefinition is an internal gate evaluator.
type gateDefinition struct {
	name        string
	kind        GateKind
	remediation string
	evaluate    func(ctx context.Context, r *Reporter) GateResult
}

// gateDefinitions returns all gate definitions in evaluation order.
func (r *Reporter) gateDefinitions() []gateDefinition {
	return []gateDefinition{
		r.gateFormat(),
		r.gateVet(),
		r.gateLint(),
		r.gateTests(),
		r.gateCoverage(),
		r.gateVulncheck(),
		r.gateGosec(),
		r.gateGeneratedDocs(),
		r.gateSchemaSnapshot(),
		r.gateToolSnapshot(),
		r.gateIntegrationReadiness(),
		r.gateReleaseSBOM(),
		r.gateReleaseSigning(),
		r.gateReleaseAttestation(),
		r.gateGorelaserConfig(),
		r.gateProductionDocs(),
		r.gateReliabilityWindow(),
		r.gateCustomerCount(),
		r.gatePaidPilots(),
		r.gateCaseStudies(),
	}
}

// --- Code-verifiable mandatory gates ---

func (r *Reporter) gateFormat() gateDefinition {
	return gateDefinition{
		name:        "code-format (gofmt+goimports)",
		kind:        GateMandatory,
		remediation: "run 'make fmt' to auto-format",
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			name := "code-format (gofmt+goimports)"
			out, code, err := rep.runner.Run(ctx, "gofmt", "-l", ".")
			if err != nil {
				return failGate(name, GateMandatory, fmt.Sprintf("gofmt unavailable: %v", err), "install Go toolchain")
			}
			unformatted := strings.TrimSpace(out)
			if code != 0 || unformatted != "" {
				return failGate(name, GateMandatory, fmt.Sprintf("unformatted files: %s", unformatted), "run 'make fmt'")
			}
			return passGate(name, GateMandatory, "all files formatted")
		},
	}
}

func (r *Reporter) gateVet() gateDefinition {
	return gateDefinition{
		name:        "go vet",
		kind:        GateMandatory,
		remediation: "fix reported issues",
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			name := "go vet"
			out, code, err := rep.runner.Run(ctx, "go", "vet", "./...")
			if err != nil {
				return failGate(name, GateMandatory, fmt.Sprintf("go vet unavailable: %v", err), "install Go toolchain")
			}
			if code != 0 {
				msg := strings.TrimSpace(out)
				if msg == "" {
					msg = "vet reported issues (non-zero exit)"
				}
				return failGate(name, GateMandatory, msg, "fix reported issues")
			}
			return passGate(name, GateMandatory, "no vet issues")
		},
	}
}

func (r *Reporter) gateLint() gateDefinition {
	return gateDefinition{
		name:        "golangci-lint",
		kind:        GateMandatory,
		remediation: "run 'make lint' and fix reported issues",
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			name := "golangci-lint"
			out, code, err := rep.runner.Run(ctx, "golangci-lint", "run", "--allow-parallel-runners", "--timeout=15m", "./...")
			if err != nil {
				return warnGate(name, GateMandatory, "golangci-lint not installed; skipping lint gate",
					"install golangci-lint: see Makefile GOLANGCI_LINT_VERSION")
			}
			if code != 0 {
				message := strings.TrimSpace(out)
				if message == "" {
					message = "lint issues detected"
				}
				return failGate(name, GateMandatory, message, "run 'make lint' and fix reported issues")
			}
			return passGate(name, GateMandatory, "no lint issues")
		},
	}
}

func (r *Reporter) gateTests() gateDefinition {
	return gateDefinition{
		name:        "unit tests (-race)",
		kind:        GateMandatory,
		remediation: "run 'make test' and fix failing tests",
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			name := "unit tests (-race)"
			out, code, err := rep.runner.Run(ctx, "go", "test", "-race", "./...")
			if err != nil {
				return failGate(name, GateMandatory, fmt.Sprintf("go test unavailable: %v", err), "install Go toolchain")
			}
			if code != 0 {
				lines := strings.Split(strings.TrimSpace(out), "\n")
				failLines := filterLines(lines, func(l string) bool {
					return strings.HasPrefix(l, "FAIL") || strings.Contains(l, "--- FAIL")
				})
				msg := "tests failed"
				if len(failLines) > 0 {
					msg = strings.Join(failLines, "; ")
				}
				return failGate(name, GateMandatory, msg, "run 'make test' and fix failing tests")
			}
			return passGate(name, GateMandatory, "all tests passed")
		},
	}
}

func (r *Reporter) gateCoverage() gateDefinition {
	const minCoverage = 70.0
	return gateDefinition{
		name:        fmt.Sprintf("test coverage >= %.0f%%", minCoverage),
		kind:        GateMandatory,
		remediation: "add tests to raise coverage above 70%",
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			name := fmt.Sprintf("test coverage >= %.0f%%", minCoverage)
			_, code, err := rep.runner.Run(ctx, "go", "test", "-coverprofile=coverage.out", "./...")
			if err != nil {
				return failGate(name, GateMandatory, fmt.Sprintf("go test unavailable: %v", err), "install Go toolchain")
			}
			if code != 0 {
				return failGate(name, GateMandatory, "tests failed; cannot measure coverage", "fix tests first")
			}
			out2, _, _ := rep.runner.Run(ctx, "go", "tool", "cover", "-func=coverage.out")
			pct := extractTotalCoverage(out2)
			if pct < 0 {
				return warnGate(name, GateMandatory, "could not parse coverage output", "check coverage.out manually")
			}
			if pct < minCoverage {
				return failGate(name, GateMandatory,
					fmt.Sprintf("coverage %.1f%% is below %.0f%% threshold", pct, minCoverage),
					"add tests to raise coverage")
			}
			return passGate(name, GateMandatory, fmt.Sprintf("coverage %.1f%%", pct))
		},
	}
}

func (r *Reporter) gateVulncheck() gateDefinition {
	return gateDefinition{
		name:        "govulncheck (no called vulnerabilities)",
		kind:        GateMandatory,
		remediation: "upgrade affected modules: govulncheck will list them",
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			name := "govulncheck (no called vulnerabilities)"
			_, code, err := rep.runner.Run(ctx, "govulncheck", "./...")
			if err != nil {
				_, code2, err2 := rep.runner.Run(ctx, "go", "run",
					"golang.org/x/vuln/cmd/govulncheck@"+govulncheckVersion, "./...")
				if err2 != nil {
					return failGate(name, GateMandatory,
						"govulncheck not installed and the pinned fallback failed",
						"install govulncheck: go install golang.org/x/vuln/cmd/govulncheck@"+govulncheckVersion)
				}
				return buildVulnResult(name, code2)
			}
			return buildVulnResult(name, code)
		},
	}
}

func buildVulnResult(name string, code int) GateResult {
	if code != 0 {
		return failGate(name, GateMandatory, "vulnerabilities found in called code",
			"upgrade affected modules (govulncheck output shows which)")
	}
	return passGate(name, GateMandatory, "no called vulnerabilities")
}

func (r *Reporter) gateGosec() gateDefinition {
	return gateDefinition{
		name:        "gosec production gate",
		kind:        GateMandatory,
		remediation: "run 'make security-static' and fix HIGH/HIGH findings",
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			name := "gosec production gate"
			out, code, err := rep.runner.Run(ctx, "go", "run", "./cmd/gosec-gate")
			if err != nil || code != 0 {
				message := strings.TrimSpace(out)
				if message == "" {
					message = "gosec production gate failed"
				}
				return failGate(name, GateMandatory, message,
					"run 'make security-static' and fix HIGH/HIGH findings")
			}
			return passGate(name, GateMandatory, "no high-severity/high-confidence findings")
		},
	}
}

func (r *Reporter) gateGeneratedDocs() gateDefinition {
	return gateDefinition{
		name:        "generated docs up to date",
		kind:        GateMandatory,
		remediation: "run 'make generate' and commit the result",
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			name := "generated docs up to date"
			paths := []string{
				rep.projectDir + "/docs/tool-reference.md",
				rep.projectDir + "/website/docs/api-reference/tools.md",
			}
			before := make([][]byte, len(paths))
			for i, path := range paths {
				data, err := rep.fs.ReadFile(path)
				if err != nil {
					return failGate(name, GateMandatory, path+" not found", "run 'make generate'")
				}
				before[i] = data
			}
			_, generationCode, generationErr := rep.runner.Run(ctx, "go", "run", "./cmd/gen-tool-docs")
			if generationErr != nil || generationCode != 0 {
				return failGate(name, GateMandatory, "tool documentation generation failed", "run 'make generate' and fix the generator error")
			}
			for i, path := range paths {
				after, err := rep.fs.ReadFile(path)
				if err != nil {
					return failGate(name, GateMandatory, path+" disappeared during generation", "run 'make generate'")
				}
				if !bytes.Equal(before[i], after) {
					return failGate(name, GateMandatory, path+" was stale and changed during generation", "commit the regenerated files")
				}
			}
			return passGate(name, GateMandatory, "generated tool references were already up to date")
		},
	}
}

func (r *Reporter) gateSchemaSnapshot() gateDefinition {
	return gateDefinition{
		name:        "config-schema contract snapshot",
		kind:        GateMandatory,
		remediation: "run 'go test ./internal/config/ -run TestSchemaContract' to see the diff, then update testdata/schema_contract.json",
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			name := "config-schema contract snapshot"
			_, code, err := rep.runner.Run(ctx, "go", "test",
				"./internal/config/", "-run", "TestSchemaContract", "-count=1")
			if err != nil {
				return failGate(name, GateMandatory, fmt.Sprintf("go test unavailable: %v", err), "install Go toolchain")
			}
			if code != 0 {
				return failGate(name, GateMandatory,
					"config schema contract snapshot mismatch — schema changed without updating the snapshot",
					"run 'go test ./internal/config/ -run TestSchemaContract -update' to refresh the snapshot")
			}
			return passGate(name, GateMandatory, "schema contract snapshot matches")
		},
	}
}

func (r *Reporter) gateToolSnapshot() gateDefinition {
	return gateDefinition{
		name:        "stable tool/resource snapshot",
		kind:        GateMandatory,
		remediation: "run 'go test ./internal/server/ -run TestStableCoreReadSnapshot' to investigate",
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			name := "stable tool/resource snapshot"
			_, code, err := rep.runner.Run(ctx, "go", "test",
				"./internal/server/", "-run", "TestStableCoreReadSnapshot", "-count=1")
			if err != nil {
				return failGate(name, GateMandatory, fmt.Sprintf("go test unavailable: %v", err), "install Go toolchain")
			}
			if code != 0 {
				return failGate(name, GateMandatory,
					"tool/resource snapshot mismatch",
					"run 'go test ./internal/server/ -run TestStableCoreReadSnapshot' to investigate")
			}
			return passGate(name, GateMandatory, "tool/resource snapshot matches")
		},
	}
}

func (r *Reporter) gateIntegrationReadiness() gateDefinition {
	return gateDefinition{
		name:        "integration test build (compilation check)",
		kind:        GateMandatory,
		remediation: "fix compilation errors in integration test files",
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			name := "integration test build (compilation check)"
			_, code, err := rep.runner.Run(ctx, "go", "test", "-tags=integration", "-run", "^$", "./...")
			if err != nil {
				return failGate(name, GateMandatory, fmt.Sprintf("go test unavailable: %v", err), "install Go toolchain")
			}
			if code != 0 {
				return failGate(name, GateMandatory, "integration-tagged code does not compile",
					"fix compilation errors in files tagged //go:build integration")
			}
			return passGate(name, GateMandatory, "integration-tagged code compiles")
		},
	}
}

// --- Release configuration gates ---

func (r *Reporter) gateReleaseSBOM() gateDefinition {
	return gateDefinition{
		name:        "release config: SBOM generation",
		kind:        GateMandatory,
		remediation: "add 'sboms:' section to .goreleaser.yml",
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			name := "release config: SBOM generation"
			data, err := rep.fs.ReadFile(rep.projectDir + "/.goreleaser.yml")
			if err != nil {
				return failGate(name, GateMandatory, ".goreleaser.yml not found", "create .goreleaser.yml with SBOM config")
			}
			content := string(data)
			if !strings.Contains(content, "sboms:") {
				return failGate(name, GateMandatory, ".goreleaser.yml is missing sboms: section",
					"add 'sboms:' section to .goreleaser.yml")
			}
			return passGate(name, GateMandatory, "sboms: section present in .goreleaser.yml")
		},
	}
}

func (r *Reporter) gateReleaseSigning() gateDefinition {
	return gateDefinition{
		name:        "release config: artifact signing",
		kind:        GateMandatory,
		remediation: "add 'signs:' section to .goreleaser.yml (cosign recommended)",
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			name := "release config: artifact signing"
			data, err := rep.fs.ReadFile(rep.projectDir + "/.goreleaser.yml")
			if err != nil {
				return failGate(name, GateMandatory, ".goreleaser.yml not found", "create .goreleaser.yml")
			}
			content := string(data)
			if !strings.Contains(content, "signs:") || !strings.Contains(content, "docker_signs:") {
				return failGate(name, GateMandatory, ".goreleaser.yml must sign checksums and container images",
					"configure both 'signs:' and 'docker_signs:' using cosign keyless signing")
			}
			return passGate(name, GateMandatory, "checksum and container signing are configured")
		},
	}
}

func (r *Reporter) gateReleaseAttestation() gateDefinition {
	return gateDefinition{
		name:        "release config: provenance attestation",
		kind:        GateMandatory,
		remediation: "add 'before.hooks' running 'cosign attest' or use GitHub SLSA provenance action",
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			name := "release config: provenance attestation"
			data, err := rep.fs.ReadFile(rep.projectDir + "/.goreleaser.yml")
			if err != nil {
				return failGate(name, GateMandatory, ".goreleaser.yml not found", "create .goreleaser.yml")
			}
			content := string(data)
			hasAttest := strings.Contains(content, "attest") || strings.Contains(content, "slsa") ||
				strings.Contains(content, "provenance")
			if !hasAttest {
				// Check GitHub workflow as fallback
				data2, err2 := rep.fs.ReadFile(rep.projectDir + "/.github/workflows/release.yml")
				if err2 == nil && strings.Contains(string(data2), "attest-build-provenance") &&
					strings.Contains(string(data2), "push-to-registry: true") {
					return passGate(name, GateMandatory, "binary and container provenance attestations found in release workflow")
				}
				return failGate(name, GateMandatory,
					"binary and container provenance attestations are not fully configured",
					"add cosign attestation or SLSA provenance to release pipeline")
			}
			return passGate(name, GateMandatory, "provenance attestation configured")
		},
	}
}

func (r *Reporter) gateGorelaserConfig() gateDefinition {
	return gateDefinition{
		name:        "release config: .goreleaser.yml exists and is parseable",
		kind:        GateMandatory,
		remediation: "create or fix .goreleaser.yml",
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			name := "release config: .goreleaser.yml exists and is parseable"
			data, err := rep.fs.ReadFile(rep.projectDir + "/.goreleaser.yml")
			if err != nil {
				return failGate(name, GateMandatory, ".goreleaser.yml not found", "create .goreleaser.yml")
			}
			if len(data) < 10 {
				return failGate(name, GateMandatory, ".goreleaser.yml appears empty", "populate .goreleaser.yml")
			}
			versionOut, versionCode, versionErr := rep.runner.Run(ctx, "goreleaser", "--version")
			useInstalled := versionErr == nil && versionCode == 0 &&
				strings.Contains(versionOut, strings.TrimPrefix(goreleaserVersion, "v"))
			var code int
			var runErr error
			if useInstalled {
				_, code, runErr = rep.runner.Run(ctx, "goreleaser", "check")
			} else {
				_, code, runErr = rep.runner.Run(ctx, "go", "run",
					"github.com/goreleaser/goreleaser/v2@"+goreleaserVersion, "check")
			}
			if runErr != nil || code != 0 {
				return failGate(name, GateMandatory, ".goreleaser.yml failed the pinned GoReleaser check",
					"run 'go run github.com/goreleaser/goreleaser/v2@"+goreleaserVersion+" check'")
			}
			return passGate(name, GateMandatory, ".goreleaser.yml passes the pinned GoReleaser check")
		},
	}
}

func (r *Reporter) gateProductionDocs() gateDefinition {
	required := []string{
		"docs/OPERATIONS.md",
		"docs/RELEASE.md",
		"docs/ROADMAP.md",
		"docs/KIP-1318.md",
		"docs/COMPATIBILITY.md",
		"docs/API_STABILITY.md",
		"docs/PRODUCTION_CHECKLIST.md",
	}
	return gateDefinition{
		name:        "production operations and compatibility documentation",
		kind:        GateMandatory,
		remediation: "add the missing production documentation",
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			_ = ctx
			var missing []string
			for _, path := range required {
				if _, err := rep.fs.ReadFile(rep.projectDir + "/" + path); err != nil {
					missing = append(missing, path)
				}
			}
			if len(missing) > 0 {
				return failGate("production operations and compatibility documentation", GateMandatory,
					"missing: "+strings.Join(missing, ", "), "add the listed documents")
			}
			return passGate("production operations and compatibility documentation", GateMandatory,
				"required production documents are present")
		},
	}
}

// --- Manual / external gates (can never auto-pass) ---

func (r *Reporter) gateReliabilityWindow() gateDefinition {
	return gateDefinition{
		name: "elapsed reliability window (≥30 days without P0 regression)",
		kind: GateManual,
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			return GateResult{
				Name:        "elapsed reliability window (≥30 days without P0 regression)",
				Kind:        GateManual,
				Status:      StatusManual,
				Message:     "requires human attestation: no P0 regression for 30+ consecutive days",
				Remediation: "document the reliability window start date and last P0 incident in release notes",
			}
		},
	}
}

func (r *Reporter) gateCustomerCount() gateDefinition {
	return gateDefinition{
		name: "customer adoption (≥N active production users)",
		kind: GateManual,
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			return GateResult{
				Name:        "customer adoption (≥N active production users)",
				Kind:        GateManual,
				Status:      StatusManual,
				Message:     "requires human attestation: customer count cannot be verified from code",
				Remediation: "document active production user count in release notes before tagging v1",
			}
		},
	}
}

func (r *Reporter) gatePaidPilots() gateDefinition {
	return gateDefinition{
		name: "paid pilot customers",
		kind: GateManual,
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			return GateResult{
				Name:        "paid pilot customers",
				Kind:        GateManual,
				Status:      StatusManual,
				Message:     "requires human attestation: paid pilot status is a commercial gate, not code-verifiable",
				Remediation: "document signed pilot agreements before tagging v1",
			}
		},
	}
}

func (r *Reporter) gateCaseStudies() gateDefinition {
	return gateDefinition{
		name: "published case studies / testimonials",
		kind: GateManual,
		evaluate: func(ctx context.Context, rep *Reporter) GateResult {
			return GateResult{
				Name:        "published case studies / testimonials",
				Kind:        GateManual,
				Status:      StatusManual,
				Message:     "requires human attestation: case study publication is external and not code-verifiable",
				Remediation: "publish at least one customer case study or testimonial before claiming v1",
			}
		},
	}
}

// --- helpers ---

func passGate(name string, kind GateKind, msg string) GateResult {
	return GateResult{Name: name, Kind: kind, Status: StatusPass, Message: msg}
}

func failGate(name string, kind GateKind, msg, remediation string) GateResult {
	return GateResult{Name: name, Kind: kind, Status: StatusFail, Message: msg, Remediation: remediation}
}

func warnGate(name string, kind GateKind, msg, remediation string) GateResult {
	return GateResult{Name: name, Kind: kind, Status: StatusWarn, Message: msg, Remediation: remediation}
}

func filterLines(lines []string, pred func(string) bool) []string {
	var out []string
	for _, l := range lines {
		if pred(l) {
			out = append(out, l)
		}
	}
	return out
}

// extractTotalCoverage parses the "total: ... X.X%" line from go tool cover output.
func extractTotalCoverage(output string) float64 {
	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, "total:") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				pctStr := strings.TrimSuffix(parts[len(parts)-1], "%")
				var pct float64
				if _, err := fmt.Sscanf(pctStr, "%f", &pct); err == nil {
					return pct
				}
			}
		}
	}
	return -1
}
