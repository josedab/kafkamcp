package server

import (
	"sort"
	"strings"
)

// Built-in tool pack names. Operators select one or more in
// ServerConfig.ToolPacks; the special pack "all" disables filtering.
const (
	PackEssential    = "essential"
	PackOps          = "ops"
	PackAnalytics    = "analytics"
	PackGovernance   = "governance"
	PackStreaming    = "streaming"
	PackExperimental = "experimental"
	PackAll          = "all"
)

// essentialPackTools is the canonical list of MCP tools that ship in the
// default ("essential") pack. The bar for inclusion is intentionally high:
// only the tools an AI agent absolutely needs to inspect, produce, consume,
// search, audit and reason about a Kafka deployment without venturing into
// niche operational territory.
//
// Aim: ≤50 tools so LLM tool-routing quality stays high.
var essentialPackTools = stringSet(
	// --- Cluster, topic & schema inspection ---
	"kafka_list_topics",
	"kafka_describe_topic",
	"kafka_create_topic",
	"kafka_delete_topic",
	"kafka_alter_topic",
	"kafka_list_schemas",
	"kafka_get_schema",
	"kafka_check_schema_compatibility",

	// --- Read / write (schema-aware) ---
	"kafka_consume",
	"kafka_produce",
	"kafka_search",
	"kafka_aggregate",
	"kafka_join",

	// --- Consumer groups ---
	"kafka_list_consumer_groups",
	"kafka_describe_consumer_group",
	"kafka_reset_offsets",
	"kafka_diagnose_lag",

	// --- ACLs / security (read) ---
	"kafka_acl_manage",

	// --- Observability essentials ---
	"kafka_metrics",
	"kafka_health",
	"kafka_anomalies",

	// --- Cluster discovery ---
	"kafka_discover_clusters",
	"kafka_discover_tools",

	// --- Lineage & data quality fundamentals ---
	"kafka_lineage_query",
	"kafka_messages_quality",

	// --- Audit / governance read-side ---
	"kafka_audit_query",
	"kafka_policy_check",
	"kafka_masking_status",

	// --- Cost & capacity quick-look ---
	"kafka_cost_analyze",
	"kafka_capacity_forecast",

	// --- Connectors (read-only) ---
	"kafka_list_connectors",
	"kafka_describe_connector",
	"kafka_connector_health",

	// --- DLQ triage ---
	"kafka_analyze_dlq",

	// --- Replay & time-travel basics ---
	"kafka_replay_to",
	"kafka_time_travel",

	// --- NL/chat onboarding ---
	"kafka_nl_query",
	"kafka_onboarding",
)

// opsPackTools layers heavy operational tooling on top of essentials:
// connectors, scaling, replication, chaos *status* (not inject), etc. It
// excludes the experimental write-side and analytics surfaces.
var opsPackTools = unionStringSets(essentialPackTools, stringSet(
	"kafka_create_connector",
	"kafka_update_connector",
	"kafka_delete_connector",
	"kafka_restart_connector",
	"kafka_connect_bridge",
	"kafka_alter_topic_config",
	"kafka_reassign_partitions",
	"kafka_capacity_plan",
	"kafka_capacity_forecast",
	"kafka_replication_topology",
	"kafka_replication_status",
	"kafka_selfheal_status",
	"kafka_chaos_status",
	"kafka_failover_readiness",
	"kafka_benchmark",
	"kafka_runbook",
	"kafka_agentic_runbook",
	"kafka_perf_guard",
))

// analyticsPackTools surfaces forecasting, anomaly detection, cost
// attribution, lineage, telemetry, time-series analytics, etc.
var analyticsPackTools = unionStringSets(essentialPackTools, stringSet(
	"kafka_anomaly_ingest",
	"kafka_anomalies",
	"kafka_capacity_forecast",
	"kafka_correlation_analyze",
	"kafka_correlation_endpoints",
	"kafka_correlation_ingest",
	"kafka_cost_allocate",
	"kafka_cost_analyze",
	"kafka_cost_budget",
	"kafka_cost_live",
	"kafka_cost_waste",
	"kafka_cost_whatif",
	"kafka_finops_budget",
	"kafka_finops_record_cost",
	"kafka_cross_lineage",
	"kafka_advisor_report",
	"kafka_advisor_scan",
	"kafka_metrics",
	"kafka_observability",
	"kafka_timeseries_query",
))

// governancePackTools focuses on policy, compliance, masking, contracts,
// audit, encryption and the schema-evolution workflow.
var governancePackTools = unionStringSets(essentialPackTools, stringSet(
	"kafka_acl_manage",
	"kafka_audit_query",
	"kafka_compliance_audit_report",
	"kafka_compliance_report",
	"kafka_compliance_scan",
	"kafka_compliance_violations",
	"kafka_contract_test",
	"kafka_contract_violations",
	"kafka_encryption_configure",
	"kafka_encryption_key",
	"kafka_encryption_process",
	"kafka_masking_status",
	"kafka_policy_check",
	"kafka_governance_portal",
	"kafka_dataquality_score",
	"kafka_messages_quality",
))

// streamingPackTools layers richer streaming/processing tools on top of
// essentials: stream SQL, CDC, materialized views, etc.
var streamingPackTools = unionStringSets(essentialPackTools, stringSet(
	"kafka_streamsql",
	"kafka_streamsql_optimize",
	"kafka_cdc_create_bridge",
	"kafka_cdc_list_bridges",
	"kafka_cdc_manage_bridge",
	"kafka_cdc_process",
	"kafka_view_create",
	"kafka_view_list",
	"kafka_view_query",
	"kafka_event_bus",
	"kafka_event_translate",
))

// experimentalPackTools contains the simulation/stub tools. They are also
// gated at runtime via the experimental.* config flags — this pack just
// makes them addressable when an operator opts in deliberately.
var experimentalPackTools = stringSet(
	"kafka_chaos_inject",
	"kafka_chaos_stop",
	"kafka_chaos_gymnasium",
	"kafka_wasm_load",
	"kafka_wasm_execute",
	"kafka_wasm_manage",
	"kafka_proxy",
	"kafka_autopilot_execute",
	"kafka_autopilot_rollback",
	"kafka_migration_plan",
	"kafka_controlplane_instance",
	"kafka_controlplane_org",
	"kafka_controlplane_usage",
	"kafka_plugin_install",
)

// packTools returns the union of tool names enabled by the named packs.
// "all" is a shortcut that disables filtering entirely (handled by the
// caller — packTools returns nil for "all").
func packTools(packs []string) map[string]struct{} {
	if len(packs) == 0 {
		// Empty config → default to essential.
		return cloneStringSet(essentialPackTools)
	}
	result := make(map[string]struct{})
	for _, p := range packs {
		switch strings.ToLower(strings.TrimSpace(p)) {
		case PackAll:
			return nil // sentinel: no filtering
		case PackEssential, "":
			mergeStringSet(result, essentialPackTools)
		case PackOps:
			mergeStringSet(result, opsPackTools)
		case PackAnalytics:
			mergeStringSet(result, analyticsPackTools)
		case PackGovernance:
			mergeStringSet(result, governancePackTools)
		case PackStreaming:
			mergeStringSet(result, streamingPackTools)
		case PackExperimental:
			mergeStringSet(result, experimentalPackTools)
		}
	}
	return result
}

// ToolPackFilter decides whether a tool should be registered with the MCP
// server based on the active set of tool packs.
//
// Rules:
//
//   - Packs == ["all"] (or contains "all") → register everything.
//   - Otherwise, a tool is registered iff:
//   - it is explicitly listed in one of the active packs, OR
//   - it has tier == essential (always included), OR
//   - it has tier == experimental AND PackExperimental is active.
type ToolPackFilter struct {
	packs    []string // normalized
	allowed  map[string]struct{}
	disabled bool // true when packs include "all"
}

// NewToolPackFilter builds a filter from the configured pack list. An
// unknown pack name is ignored (a warning is logged at startup elsewhere).
func NewToolPackFilter(packs []string) *ToolPackFilter {
	normalised := normalisePacks(packs)
	allowed := packTools(normalised)
	return &ToolPackFilter{
		packs:    normalised,
		allowed:  allowed,
		disabled: allowed == nil,
	}
}

// Packs returns the normalised, ordered list of active packs.
func (f *ToolPackFilter) Packs() []string {
	out := make([]string, len(f.packs))
	copy(out, f.packs)
	return out
}

// AllAllowed returns true when filtering is disabled (e.g. packs == "all").
func (f *ToolPackFilter) AllAllowed() bool {
	if f == nil {
		return true
	}
	return f.disabled
}

// ShouldRegister decides whether a tool with the given name and tier is in
// the active pack set. A nil filter (no config) registers everything.
func (f *ToolPackFilter) ShouldRegister(name, tier string) bool {
	if f == nil || f.disabled {
		return true
	}
	if name == "" {
		// Defensive: never register an unnamed tool.
		return false
	}
	if tier == "" {
		tier = TierAdvanced
	}
	if tier == TierEssential {
		return true
	}
	if tier == TierExperimental {
		// Experimental tools must be opted into via the experimental pack.
		if _, ok := f.allowed[name]; ok {
			return true
		}
		for _, p := range f.packs {
			if p == PackExperimental {
				return true
			}
		}
		return false
	}
	_, ok := f.allowed[name]
	return ok
}

// AllowedCount returns the size of the allow-list (or -1 when filtering is
// disabled). Useful for startup logging and tests.
func (f *ToolPackFilter) AllowedCount() int {
	if f == nil || f.disabled {
		return -1
	}
	return len(f.allowed)
}

// AllowedNames returns the sorted list of explicitly-allowed tool names.
// Returns nil when filtering is disabled.
func (f *ToolPackFilter) AllowedNames() []string {
	if f == nil || f.disabled {
		return nil
	}
	out := make([]string, 0, len(f.allowed))
	for n := range f.allowed {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// --- helpers ---

func stringSet(items ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(items))
	for _, s := range items {
		m[s] = struct{}{}
	}
	return m
}

func cloneStringSet(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for k := range in {
		out[k] = struct{}{}
	}
	return out
}

func mergeStringSet(dst, src map[string]struct{}) {
	for k := range src {
		dst[k] = struct{}{}
	}
}

func unionStringSets(a, b map[string]struct{}) map[string]struct{} {
	out := cloneStringSet(a)
	mergeStringSet(out, b)
	return out
}

func normalisePacks(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, p := range in {
		p = strings.ToLower(strings.TrimSpace(p))
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	if len(out) == 0 {
		out = []string{PackEssential}
	}
	return out
}
