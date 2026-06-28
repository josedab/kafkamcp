// Package doctor implements a non-destructive readiness check ("doctor") for a
// KafkaMCP deployment. It proves that configuration, identity, policy, masking,
// approval, persistence, and (via injected probes) Kafka and schema-registry
// connectivity are correctly set up, and emits both a human-readable summary and
// machine-readable JSON with actionable remedies. It never mutates any cluster.
package doctor

import (
	"context"
	"fmt"
	"strings"

	"github.com/josedab/kafkamcp/internal/config"
	"github.com/josedab/kafkamcp/internal/masking"
)

// Status is a per-check outcome.
type Status string

// Check outcomes.
const (
	StatusOK   Status = "ok"
	StatusWarn Status = "warn"
	StatusFail Status = "fail"
	StatusSkip Status = "skip"
)

// Check is a single readiness check result.
type Check struct {
	Name   string `json:"name"`
	Status Status `json:"status"`
	Detail string `json:"detail"`
	Remedy string `json:"remedy,omitempty"`
}

// Report is the full doctor result.
type Report struct {
	OK     bool    `json:"ok"`
	Checks []Check `json:"checks"`
}

// Probe is an injected connectivity check (Kafka, schema registry). It should be
// read-only. A nil error means healthy.
type Probe func(ctx context.Context) error

// Options carries injected probes so the pure config checks stay testable.
type Options struct {
	// KafkaProbe verifies broker connectivity (e.g. a metadata request).
	KafkaProbe Probe
	// SchemaProbe verifies schema-registry connectivity.
	SchemaProbe Probe
}

// Run executes all readiness checks and returns a Report. Report.OK is false if
// any check failed (warnings do not fail the report).
func Run(ctx context.Context, cfg *config.Config, opts Options) Report {
	r := Report{OK: true}
	add := func(c Check) {
		if c.Status == StatusFail {
			r.OK = false
		}
		r.Checks = append(r.Checks, c)
	}

	// 1. Configuration is loaded and valid (reaching here implies it parsed).
	add(Check{Name: "config", Status: StatusOK, Detail: "configuration loaded and schema-valid"})

	// 2. Transport security posture (fail-closed for network transports).
	if err := cfg.TransportSecurityError(); err != nil {
		add(Check{Name: "transport-security", Status: StatusFail, Detail: err.Error(),
			Remedy: "configure a cryptographic identity provider and default-deny, or set the unsafe dev override for local use only"})
	} else if cfg.NetworkTransport() && cfg.UnsafeNetworkOverrideEnabled() {
		add(Check{Name: "transport-security", Status: StatusWarn,
			Detail: "network transport running with the unsafe override (no enforced identity/default-deny)",
			Remedy: "remove server.dangerously_allow_unauthenticated_network before production"})
	} else {
		add(Check{Name: "transport-security", Status: StatusOK, Detail: fmt.Sprintf("transport=%s posture OK", cfg.Server.Transport)})
	}

	// 3. Identity provider.
	if cfg.Policies.Identity.IsCryptographic() {
		add(Check{Name: "identity", Status: StatusOK, Detail: "cryptographic identity provider: " + cfg.Policies.Identity.Provider})
	} else if cfg.NetworkTransport() {
		add(Check{Name: "identity", Status: StatusWarn, Detail: "no cryptographic identity provider on a network transport",
			Remedy: "set policies.identity.provider to static, oidc, or mtls"})
	} else {
		add(Check{Name: "identity", Status: StatusOK, Detail: "stdio transport; header-asserted identity is acceptable locally"})
	}

	// 4. Authorization policy.
	if cfg.Policies.DefaultDeny {
		add(Check{Name: "policy", Status: StatusOK, Detail: fmt.Sprintf("default_deny=true (%d agent rules)", len(cfg.Policies.Agents))})
	} else {
		add(Check{Name: "policy", Status: StatusWarn, Detail: "default_deny=false (open access)",
			Remedy: "set policies.default_deny: true for production"})
	}

	// 5. Masking key readiness.
	if cfg.Masking.Enabled {
		tok, err := masking.TokenizerFromEnv(cfg.Masking.KeyEnv)
		switch {
		case err != nil:
			add(Check{Name: "masking-key", Status: StatusFail, Detail: "masking enabled but the configured key is invalid: " + err.Error(),
				Remedy: "provide a >=16-byte key in the configured environment variable"})
		case tok.Configured():
			add(Check{Name: "masking-key", Status: StatusOK, Detail: "masking enabled with a valid HMAC key"})
		default:
			add(Check{Name: "masking-key", Status: StatusWarn, Detail: "masking enabled but no HMAC key set; hash/tokenize actions will fail closed (fully redact)",
				Remedy: "set the " + keyEnvName(cfg.Masking.KeyEnv) + " environment variable"})
		}
	} else {
		add(Check{Name: "masking-key", Status: StatusSkip, Detail: "masking disabled"})
	}

	// 6. Approval enforcement.
	if cfg.Policies.Approval.EnforcementEnabled(cfg.Policies.DefaultDeny) {
		add(Check{Name: "approval", Status: StatusOK, Detail: "write/destructive tools require short-lived approval grants"})
	} else {
		add(Check{Name: "approval", Status: StatusWarn, Detail: "approval enforcement is OFF; write tools are unguarded",
			Remedy: "set policies.approval.enabled: true (or enable default_deny)"})
	}

	// 7. Persistence.
	switch strings.ToLower(strings.TrimSpace(cfg.Server.Persistence.Backend)) {
	case "sqlite", "sqlite3":
		if strings.TrimSpace(cfg.Server.Persistence.Path) == "" {
			add(Check{Name: "persistence", Status: StatusFail, Detail: "backend=sqlite but no persistence.path set",
				Remedy: "set server.persistence.path to a writable file path"})
		} else {
			add(Check{Name: "persistence", Status: StatusOK, Detail: "sqlite backend at " + cfg.Server.Persistence.Path})
		}
	default:
		add(Check{Name: "persistence", Status: StatusWarn, Detail: "in-memory backend; state and approval grants are lost on restart",
			Remedy: "set server.persistence.backend: sqlite to persist supported engines"})
	}

	// 8. Tool surface.
	packs := cfg.Server.ToolPacks
	if len(packs) == 0 {
		packs = []string{config.DefaultToolPack}
	}
	deprecated := []string{}
	for _, p := range packs {
		if _, ok := config.DeprecatedToolPacks[strings.ToLower(strings.TrimSpace(p))]; ok {
			deprecated = append(deprecated, p)
		}
	}
	if len(deprecated) > 0 {
		add(Check{Name: "tool-packs", Status: StatusWarn, Detail: "using deprecated pack(s): " + strings.Join(deprecated, ", "),
			Remedy: "migrate to core-read / ops-write / governance / experimental"})
	} else {
		add(Check{Name: "tool-packs", Status: StatusOK, Detail: "active packs: " + strings.Join(packs, ", ")})
	}

	// 9. Kafka connectivity (injected).
	runProbe(ctx, add, "kafka-connectivity", opts.KafkaProbe,
		"verify clusters[].bootstrap_servers and network reachability")

	// 10. Schema registry connectivity (injected, optional).
	if opts.SchemaProbe != nil {
		runProbe(ctx, add, "schema-registry", opts.SchemaProbe,
			"verify clusters[].schema_registry.url and credentials")
	} else {
		add(Check{Name: "schema-registry", Status: StatusSkip, Detail: "no schema registry configured"})
	}

	return r
}

func runProbe(ctx context.Context, add func(Check), name string, probe Probe, remedy string) {
	if probe == nil {
		add(Check{Name: name, Status: StatusSkip, Detail: "not checked"})
		return
	}
	if err := probe(ctx); err != nil {
		add(Check{Name: name, Status: StatusFail, Detail: err.Error(), Remedy: remedy})
		return
	}
	add(Check{Name: name, Status: StatusOK, Detail: "reachable"})
}

func keyEnvName(configured string) string {
	if strings.TrimSpace(configured) == "" {
		return masking.DefaultKeyEnv
	}
	return configured
}

// Summary renders a human-readable multi-line summary of the report.
func (r Report) Summary() string {
	var b strings.Builder
	symbols := map[Status]string{StatusOK: "OK ", StatusWarn: "!! ", StatusFail: "XX ", StatusSkip: "-- "}
	for _, c := range r.Checks {
		fmt.Fprintf(&b, "  [%s] %-20s %s\n", symbols[c.Status], c.Name, c.Detail)
		if c.Remedy != "" && c.Status != StatusOK {
			fmt.Fprintf(&b, "        remedy: %s\n", c.Remedy)
		}
	}
	if r.OK {
		b.WriteString("\nDoctor: PASS (no failures)\n")
	} else {
		b.WriteString("\nDoctor: FAIL (one or more checks failed)\n")
	}
	return b.String()
}
