package server

import (
	"sort"
	"strings"

	"github.com/josedab/kafkamcp/internal/auth"
)

// Built-in tool pack names. Operators select one or more in
// ServerConfig.ToolPacks.
//
// The curated, secure-by-default surface:
//
//   - PackCoreRead ("core-read"): the DEFAULT. <=15 strictly read-only tools
//     covering cluster/topic/schema discovery, bounded consume/search,
//     consumer-group/lag/DLQ diagnosis, health/metrics and capability
//     discovery. Never contains produce/create/alter/delete/reset/ACL mutation.
//   - PackOpsWrite ("ops-write"): mutating/destructive operations. Every tool
//     here is routed through the approval-grant enforcement path.
//   - PackGovernance ("governance"): policy, audit, masking, compliance,
//     contracts (read-side governance).
//
// INVARIANT: KafkaMCP ships NO simulated or placeholder tools. Every registered
// tool performs real behavior. Future incomplete work must stay out of these
// runtime packs until it has a real implementation and integration tests. There
// is deliberately no "experimental" pack. The supported surface is exactly the
// union of core-read (15 stable) + ops-write + governance (14 beta) = 29 tools,
// enforced by TestSupportedSurfaceIsExactly29.
//
// The previous analytics, streaming and all packs were intentionally removed.
// They exposed a broad legacy surface whose mutation and production semantics
// could not be guaranteed centrally. Legacy tools remain in the source tree for
// compatibility work, but are not addressable from a production pack.
const (
	PackCoreRead   = "core-read"
	PackOpsWrite   = "ops-write"
	PackGovernance = "governance"

	// Deprecated aliases retained for backward compatibility. They emit
	// migration guidance at startup (see DeprecatedPackTarget).
	PackEssential = "essential" // -> core-read
	PackOps       = "ops"       // -> core-read + ops-write
)

// coreReadTools is the curated read-only core. It MUST stay strictly read-only
// and at most 15 tools. Every entry here is also required to be non-mutating in
// writeToolSet (see TestCoreReadHasNoWriteTools).
var coreReadTools = stringSet(
	// Cluster / topic / schema discovery.
	"kafka_list_topics",
	"kafka_describe_topic",
	"kafka_list_schemas",
	"kafka_get_schema",
	"kafka_check_schema_compatibility",
	// Bounded consume / search (read-only).
	"kafka_consume",
	"kafka_search",
	// Consumer-group / lag / DLQ diagnosis.
	"kafka_list_consumer_groups",
	"kafka_describe_consumer_group",
	"kafka_diagnose_lag",
	"kafka_analyze_dlq",
	// Health / metrics.
	"kafka_health",
	"kafka_metrics",
	// Cluster & capability discovery.
	"kafka_discover_clusters",
	"kafka_discover_tools",
)

// writeToolSet is the AUTHORITATIVE set of mutating/destructive tools. It is the
// single source of truth used BOTH to populate the ops-write pack AND to decide
// which tools must pass approval-grant enforcement (see approval wiring). No
// read-only tool may appear here, and nothing here may appear in coreReadTools.
type writeToolPolicy struct {
	ResourceType auth.ResourceType
	Action       auth.Action
	ResourceKeys []string
}

// writeToolPolicies is the single source of truth for the supported production
// mutation surface. A tool cannot be exposed by ops-write or execute without
// approval unless it appears here.
var writeToolPolicies = map[string]writeToolPolicy{
	"kafka_produce":           {auth.ResourceTopic, auth.ActionWrite, []string{"topic"}},
	"kafka_create_topic":      {auth.ResourceTopic, auth.ActionCreate, []string{"topic", "name"}},
	"kafka_delete_topic":      {auth.ResourceTopic, auth.ActionDelete, []string{"topic", "name"}},
	"kafka_alter_topic":       {auth.ResourceTopic, auth.ActionAdmin, []string{"topic", "name"}},
	"kafka_reset_offsets":     {auth.ResourceConsumerGroup, auth.ActionReset, []string{"group_id", "group"}},
	"kafka_acl_manage":        {auth.ResourceCluster, auth.ActionAdmin, []string{"resource", "topic", "name"}},
	"kafka_create_connector":  {auth.ResourceCluster, auth.ActionAdmin, []string{"name", "connector"}},
	"kafka_update_connector":  {auth.ResourceCluster, auth.ActionAdmin, []string{"name", "connector"}},
	"kafka_delete_connector":  {auth.ResourceCluster, auth.ActionAdmin, []string{"name", "connector"}},
	"kafka_restart_connector": {auth.ResourceCluster, auth.ActionAdmin, []string{"name", "connector"}},
}

var writeToolSet = func() map[string]struct{} {
	out := make(map[string]struct{}, len(writeToolPolicies))
	for name := range writeToolPolicies {
		out[name] = struct{}{}
	}
	return out
}()

// idempotentWriteToolSet contains mutations for which KafkaMCP can safely
// suppress a retry after a confirmed successful response.
var idempotentWriteToolSet = stringSet(
	"kafka_create_topic",
	"kafka_delete_topic",
	"kafka_create_connector",
	"kafka_update_connector",
	"kafka_delete_connector",
	"kafka_acl_manage",
)

// opsWritePackTools is the authoritative write set plus the approval lifecycle
// tool (kafka_approval), which operators need to obtain grants. kafka_approval
// itself is NOT a write tool (it does not mutate Kafka) so it does not require
// an approval grant.
var opsWritePackTools = unionStringSets(writeToolSet, stringSet("kafka_approval"))

// governancePackTools focuses on policy, compliance, masking, contracts, audit
// and the schema-evolution workflow (read-side governance).
var governancePackTools = stringSet(
	"kafka_approval",
	"kafka_governance_decision",
	"kafka_acl_manage",
	"kafka_policy_evaluate",
	"kafka_policy_simulate",
)

// experimentalPackTools was removed along with the simulated chaos/WASM/proxy/
// migration tools. No non-production pack is accepted any longer.

// IsWriteTool reports whether the named tool performs a mutating/destructive
// operation and must therefore pass approval-grant enforcement. This is the
// single source of truth shared by pack gating and approval enforcement.
func IsWriteTool(name string) bool {
	_, ok := writeToolSet[name]
	return ok
}

func policyForWriteTool(name string) (writeToolPolicy, bool) {
	policy, ok := writeToolPolicies[name]
	return policy, ok
}

// WriteToolNames returns the sorted list of mutating/destructive tools.
func WriteToolNames() []string {
	return sortedKeys(writeToolSet)
}

func isIdempotentWriteTool(name string) bool {
	_, ok := idempotentWriteToolSet[name]
	return ok
}

// DeprecatedPackTarget returns the replacement pack list for a deprecated pack
// name and true, or (nil, false) if the name is not deprecated.
func DeprecatedPackTarget(name string) ([]string, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case PackEssential:
		return []string{PackCoreRead}, true
	case PackOps:
		return []string{PackCoreRead, PackOpsWrite}, true
	default:
		return nil, false
	}
}

// resolvePack returns the tool set for a single (already-normalised) pack name,
// resolving deprecated aliases.
func resolvePack(pack string) map[string]struct{} {
	switch pack {
	case PackCoreRead, PackEssential, "":
		// essential is a deprecated alias for core-read.
		return coreReadTools
	case PackOps:
		// deprecated alias: core-read + ops-write.
		return unionStringSets(coreReadTools, opsWritePackTools)
	case PackOpsWrite:
		return opsWritePackTools
	case PackGovernance:
		return governancePackTools
	default:
		return map[string]struct{}{}
	}
}

// ToolPackFilter decides whether a tool should be registered with the MCP
// server based on the active set of tool packs.
//
// A tool is registered only when it appears in the union of explicitly active
// packs. There is deliberately no wildcard pack.
type ToolPackFilter struct {
	packs   []string
	allowed map[string]struct{}
}

// NewToolPackFilter builds a filter from the configured pack list. An unknown
// pack name contributes no tools (a warning is logged at startup elsewhere).
func NewToolPackFilter(packs []string) *ToolPackFilter {
	normalised := normalisePacks(packs)

	allowed := make(map[string]struct{})
	for _, p := range normalised {
		mergeStringSet(allowed, resolvePack(p))
	}

	return &ToolPackFilter{
		packs:   normalised,
		allowed: allowed,
	}
}

// Packs returns the normalised, ordered list of active packs.
func (f *ToolPackFilter) Packs() []string {
	out := make([]string, len(f.packs))
	copy(out, f.packs)
	return out
}

// AllAllowed is retained for compatibility with startup logging. Production
// filters never allow a wildcard; a nil filter is the only allow-all case.
func (f *ToolPackFilter) AllAllowed() bool {
	return f == nil
}

// ShouldRegister decides whether a tool is in the active pack set. A nil
// filter (no config) registers everything.
func (f *ToolPackFilter) ShouldRegister(name string, _ ...string) bool {
	if f == nil {
		return true
	}
	if name == "" {
		// Defensive: never register an unnamed tool.
		return false
	}
	_, ok := f.allowed[name]
	return ok
}

func toolStability(name string) string {
	if _, ok := coreReadTools[name]; ok {
		return "stable"
	}
	return "beta"
}

// AllowedCount returns the size of the allow-list.
func (f *ToolPackFilter) AllowedCount() int {
	if f == nil {
		return -1
	}
	return len(f.allowed)
}

// AllowedNames returns the sorted list of explicitly-allowed tool names.
func (f *ToolPackFilter) AllowedNames() []string {
	if f == nil {
		return nil
	}
	return sortedKeys(f.allowed)
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

func sortedKeys(in map[string]struct{}) []string {
	out := make([]string, 0, len(in))
	for k := range in {
		out = append(out, k)
	}
	sort.Strings(out)
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
		out = []string{PackCoreRead}
	}
	return out
}
