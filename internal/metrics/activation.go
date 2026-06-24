package metrics

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ActivationMetrics is an opt-in, privacy-preserving set of local Prometheus
// metrics that measure whether an installation is successfully activated and
// exercising the core secure workflows. It is exposed on the local metrics
// endpoint only — there is NO phone-home and no external transmission.
//
// All labels are deliberately low-cardinality and bounded (pack names, write
// tool names, and fixed outcome enums). Raw agent identifiers and resource names
// are never used as labels; the active-verified-agents gauge counts distinct
// agents behind a hash without exposing the identifiers.
type ActivationMetrics struct {
	enabled bool

	SetupTotal            *prometheus.CounterVec // outcome=success|failure
	ActiveVerifiedAgents  prometheus.Gauge
	ToolPackUseTotal      *prometheus.CounterVec // pack
	DeniedUnsafeCalls     *prometheus.CounterVec // tool (bounded write set)
	ApprovalOutcomesTotal *prometheus.CounterVec // outcome=request|approve|use|deny|expire|revoke
	CoreWorkflowTotal     *prometheus.CounterVec // workflow

	agentMu          sync.Mutex
	agentHashes      map[string]struct{}
	agentWindowStart time.Time
	agentWindow      time.Duration
	maxAgentHashes   int
	now              func() time.Time
}

// NewActivationMetrics registers the activation metrics on reg. When enabled is
// false, the returned value is a no-op recorder (metrics are not registered) so
// operators must explicitly opt in.
func NewActivationMetrics(reg *prometheus.Registry, enabled bool) *ActivationMetrics {
	am := &ActivationMetrics{
		enabled:        enabled,
		agentHashes:    make(map[string]struct{}),
		agentWindow:    24 * time.Hour,
		maxAgentHashes: 10000,
		now:            time.Now,
	}
	if !enabled || reg == nil {
		return am
	}
	f := promauto.With(reg)
	am.SetupTotal = f.NewCounterVec(prometheus.CounterOpts{
		Name: "kafkamcp_activation_setup_total",
		Help: "Setup / cluster-validation outcomes (opt-in activation metric).",
	}, []string{"outcome"})
	am.ActiveVerifiedAgents = f.NewGauge(prometheus.GaugeOpts{
		Name: "kafkamcp_activation_active_verified_agents",
		Help: "Distinct cryptographically-verified agents seen (opt-in; no identifiers exposed).",
	})
	am.ToolPackUseTotal = f.NewCounterVec(prometheus.CounterOpts{
		Name: "kafkamcp_activation_tool_pack_use_total",
		Help: "Active tool-pack selections at startup (opt-in activation metric).",
	}, []string{"pack"})
	am.DeniedUnsafeCalls = f.NewCounterVec(prometheus.CounterOpts{
		Name: "kafkamcp_activation_denied_unsafe_calls_total",
		Help: "Write/destructive tool calls denied for lack of a valid approval grant (opt-in).",
	}, []string{"tool"})
	am.ApprovalOutcomesTotal = f.NewCounterVec(prometheus.CounterOpts{
		Name: "kafkamcp_activation_approval_outcomes_total",
		Help: "Approval-grant lifecycle outcomes (opt-in activation metric).",
	}, []string{"outcome"})
	am.CoreWorkflowTotal = f.NewCounterVec(prometheus.CounterOpts{
		Name: "kafkamcp_activation_core_workflow_total",
		Help: "Completions of core secure workflows (opt-in activation metric).",
	}, []string{"workflow"})
	return am
}

// Enabled reports whether activation metrics are active.
func (a *ActivationMetrics) Enabled() bool { return a != nil && a.enabled }

// RecordSetup records a setup/validation outcome ("success" or "failure").
func (a *ActivationMetrics) RecordSetup(outcome string) {
	if !a.Enabled() {
		return
	}
	if outcome != "success" && outcome != "failure" {
		outcome = "other"
	}
	a.SetupTotal.WithLabelValues(outcome).Inc()
}

// RecordToolPack records an active tool pack.
func (a *ActivationMetrics) RecordToolPack(pack string) {
	if !a.Enabled() {
		return
	}
	switch pack {
	case "core-read", "ops-write", "governance", "experimental", "essential", "ops":
	default:
		pack = "other"
	}
	a.ToolPackUseTotal.WithLabelValues(pack).Inc()
}

// RecordDeniedUnsafe records a denied write/destructive call. The tool name is
// bounded (the write set) so it is safe as a label.
func (a *ActivationMetrics) RecordDeniedUnsafe(tool string) {
	if !a.Enabled() {
		return
	}
	switch tool {
	case "kafka_produce", "kafka_create_topic", "kafka_delete_topic", "kafka_alter_topic",
		"kafka_reset_offsets", "kafka_acl_manage", "kafka_create_connector",
		"kafka_update_connector", "kafka_delete_connector", "kafka_restart_connector":
	default:
		tool = "other"
	}
	a.DeniedUnsafeCalls.WithLabelValues(tool).Inc()
}

// RecordApprovalOutcome records an approval lifecycle event outcome.
func (a *ActivationMetrics) RecordApprovalOutcome(outcome string) {
	if !a.Enabled() {
		return
	}
	switch outcome {
	case "request", "approve", "use", "deny", "expire", "revoke":
	default:
		outcome = "other"
	}
	a.ApprovalOutcomesTotal.WithLabelValues(outcome).Inc()
}

// RecordCoreWorkflow records completion of a named core workflow.
func (a *ActivationMetrics) RecordCoreWorkflow(workflow string) {
	if !a.Enabled() {
		return
	}
	switch workflow {
	case "approved_write", "incident_triage", "cluster_validation":
	default:
		workflow = "other"
	}
	a.CoreWorkflowTotal.WithLabelValues(workflow).Inc()
}

// ObserveVerifiedAgent records a verified agent, hashing the identifier before
// retaining it in memory. The identifier is never exported as a label.
func (a *ActivationMetrics) ObserveVerifiedAgent(agentID string) {
	if !a.Enabled() || agentID == "" {
		return
	}
	sum := sha256.Sum256([]byte("kafkamcp-activation-agent-v1\x00" + agentID))
	agentHash := hex.EncodeToString(sum[:])
	a.agentMu.Lock()
	now := a.now()
	if a.agentWindowStart.IsZero() || now.Sub(a.agentWindowStart) >= a.agentWindow {
		clear(a.agentHashes)
		a.agentWindowStart = now
		a.ActiveVerifiedAgents.Set(0)
	}
	if _, ok := a.agentHashes[agentHash]; !ok {
		if len(a.agentHashes) >= a.maxAgentHashes {
			a.agentMu.Unlock()
			return
		}
		a.agentHashes[agentHash] = struct{}{}
		a.ActiveVerifiedAgents.Set(float64(len(a.agentHashes)))
	}
	a.agentMu.Unlock()
}
