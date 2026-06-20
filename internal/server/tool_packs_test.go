package server

import (
	"io"
	"log/slog"
	"testing"

	"github.com/josedab/kafkamcp/internal/config"
	"github.com/josedab/kafkamcp/internal/kafka"
)

// TestSupportedSurfaceIsExactly29 pins the supported production surface after
// the removal of the simulated experimental pack: the union of the core-read,
// ops-write and governance packs must be exactly 29 tools (15 stable core-read
// plus 14 beta), and must never contain a simulated/placeholder tool.
func TestSupportedSurfaceIsExactly29(t *testing.T) {
	union := map[string]struct{}{}
	for _, pack := range []map[string]struct{}{coreReadTools, opsWritePackTools, governancePackTools} {
		for name := range pack {
			union[name] = struct{}{}
		}
	}
	if len(coreReadTools) != 15 {
		t.Fatalf("stable core-read must be 15 tools, got %d", len(coreReadTools))
	}
	if len(union) != 29 {
		t.Fatalf("supported surface must be exactly 29 tools, got %d: %v", len(union), sortedKeys(union))
	}
	for _, banned := range []string{
		"kafka_chaos_inject", "kafka_chaos_stop", "kafka_chaos_gymnasium",
		"kafka_wasm_load", "kafka_wasm_execute", "kafka_wasm_manage",
		"kafka_proxy", "kafka_migration_plan",
	} {
		if _, ok := union[banned]; ok {
			t.Fatalf("simulated tool %q must not be part of the supported surface", banned)
		}
	}
}

func TestToolStability(t *testing.T) {
	if got := toolStability("kafka_list_topics"); got != "stable" {
		t.Fatalf("core-read stability = %q, want stable", got)
	}
	if got := toolStability("kafka_produce"); got != "beta" {
		t.Fatalf("write-tool stability = %q, want beta", got)
	}
}

func TestNewToolPackFilter_DefaultsToCoreRead(t *testing.T) {
	f := NewToolPackFilter(nil)
	if f.AllAllowed() {
		t.Fatalf("expected filter with default packs to filter tools, got AllAllowed=true")
	}
	packs := f.Packs()
	if len(packs) != 1 || packs[0] != PackCoreRead {
		t.Fatalf("expected [core-read], got %v", packs)
	}
	if f.AllowedCount() <= 0 {
		t.Fatalf("expected non-zero allowed count")
	}
}

// TestCoreReadSize enforces the product invariant: the default read-only core
// must contain at most 15 tools.
func TestCoreReadSize(t *testing.T) {
	if got := len(coreReadTools); got > 15 {
		t.Fatalf("core-read must contain at most 15 tools, got %d", got)
	}
	if got := len(coreReadTools); got == 0 {
		t.Fatal("core-read must not be empty")
	}
}

// TestCoreReadHasNoWriteTools enforces that the default pack is strictly
// read-only: no core-read tool may appear in the authoritative write set.
func TestCoreReadHasNoWriteTools(t *testing.T) {
	for name := range coreReadTools {
		if IsWriteTool(name) {
			t.Errorf("core-read must be strictly read-only, but %q is a write tool", name)
		}
	}
	// Spot-check the specific mutation verbs the product decision forbids.
	forbidden := []string{
		"kafka_produce", "kafka_create_topic", "kafka_alter_topic",
		"kafka_delete_topic", "kafka_reset_offsets", "kafka_acl_manage",
	}
	for _, name := range forbidden {
		if _, ok := coreReadTools[name]; ok {
			t.Errorf("core-read must not contain mutating tool %q", name)
		}
	}
}

// TestWriteAndCoreDisjoint enforces the two sets never overlap.
func TestWriteAndCoreDisjoint(t *testing.T) {
	for name := range writeToolSet {
		if _, ok := coreReadTools[name]; ok {
			t.Errorf("tool %q is in both writeToolSet and coreReadTools", name)
		}
	}
}

func TestToolPackFilter_NoWildcardPack(t *testing.T) {
	f := NewToolPackFilter([]string{"all"})
	if f.AllAllowed() {
		t.Fatal("the legacy wildcard must not enable allow-all registration")
	}
	if f.AllowedCount() != 0 {
		t.Fatalf("unknown/removed pack must expose no tools, got %d", f.AllowedCount())
	}
}

func TestToolPackFilter_CoreReadFiltersUnlisted(t *testing.T) {
	f := NewToolPackFilter([]string{"core-read"})
	if !f.ShouldRegister("kafka_list_topics") {
		t.Fatalf("kafka_list_topics is in core-read and should register")
	}
	if f.ShouldRegister("kafka_obscure_advanced") {
		t.Fatalf("expected unlisted advanced tool to be filtered out")
	}
	if f.ShouldRegister("kafka_delete_topic") {
		t.Fatalf("core-read must not register write tools")
	}
}

func TestToolPackFilter_OpsWriteRequiresOptIn(t *testing.T) {
	coreOnly := NewToolPackFilter([]string{"core-read"})
	if coreOnly.ShouldRegister("kafka_produce") {
		t.Fatalf("write tool must NOT be registered in core-read-only pack")
	}
	withWrite := NewToolPackFilter([]string{"core-read", "ops-write"})
	for _, name := range WriteToolNames() {
		if !withWrite.ShouldRegister(name) {
			t.Fatalf("ops-write pack must register write tool %q", name)
		}
	}
}

func TestToolPackFilter_ExperimentalPackNoLongerAccepted(t *testing.T) {
	// The experimental pack and all simulated tools were removed. An
	// "experimental" pack name must resolve to no tools, and legacy simulated
	// tool names must never be registerable under any pack.
	f := NewToolPackFilter([]string{"core-read", "experimental"})
	for _, name := range []string{
		"kafka_chaos_inject", "kafka_chaos_stop", "kafka_chaos_gymnasium",
		"kafka_wasm_load", "kafka_wasm_execute", "kafka_wasm_manage",
		"kafka_proxy", "kafka_migration_plan",
	} {
		if f.ShouldRegister(name) {
			t.Fatalf("removed simulated tool %q must not be registerable", name)
		}
	}
	// A pack list of only "experimental" resolves to the default core-read.
	expOnly := NewToolPackFilter([]string{"experimental"})
	if expOnly.ShouldRegister("kafka_chaos_inject") {
		t.Fatalf("experimental pack must not expose any simulated tools")
	}
}

func TestToolPackFilter_GovernancePackHasGovTools(t *testing.T) {
	f := NewToolPackFilter([]string{"governance"})
	for _, name := range []string{"kafka_approval", "kafka_governance_decision", "kafka_acl_manage", "kafka_policy_evaluate", "kafka_policy_simulate"} {
		if !f.ShouldRegister(name) {
			t.Fatalf("governance pack should include %q", name)
		}
	}
}

func TestDeprecatedPackTarget(t *testing.T) {
	if target, ok := DeprecatedPackTarget("essential"); !ok || len(target) != 1 || target[0] != PackCoreRead {
		t.Fatalf("essential should map to [core-read], got %v (ok=%v)", target, ok)
	}
	if target, ok := DeprecatedPackTarget("ops"); !ok || len(target) != 2 {
		t.Fatalf("ops should map to [core-read, ops-write], got %v (ok=%v)", target, ok)
	}
	if _, ok := DeprecatedPackTarget("core-read"); ok {
		t.Fatal("core-read is not deprecated")
	}
}

// Deprecated "essential" alias resolves to core-read tools.
func TestDeprecatedEssentialAliasResolvesToCoreRead(t *testing.T) {
	f := NewToolPackFilter([]string{"essential"})
	if !f.ShouldRegister("kafka_list_topics") {
		t.Fatalf("essential alias should include core-read tools")
	}
	if f.ShouldRegister("kafka_produce") {
		t.Fatalf("essential alias must be read-only (no write tools)")
	}
}

func TestToolPackFilter_NormalisationAndDeduplication(t *testing.T) {
	f := NewToolPackFilter([]string{" Core-Read ", "core-read", "OPS-WRITE"})
	packs := f.Packs()
	if len(packs) != 2 {
		t.Fatalf("expected 2 packs after normalization, got %v", packs)
	}
	if packs[0] != "core-read" || packs[1] != "ops-write" {
		t.Fatalf("expected normalised order [core-read, ops-write], got %v", packs)
	}
}

func TestToolPackFilter_NilSafety(t *testing.T) {
	var f *ToolPackFilter
	if !f.AllAllowed() {
		t.Fatalf("nil filter should allow everything")
	}
	if !f.ShouldRegister("anything") {
		t.Fatalf("nil filter must register everything")
	}
	if f.AllowedCount() != -1 {
		t.Fatalf("nil filter AllowedCount should be -1")
	}
	if f.AllowedNames() != nil {
		t.Fatalf("nil filter AllowedNames should be nil")
	}
}

func TestToolPackFilter_AllowedNamesSortedAndStable(t *testing.T) {
	f := NewToolPackFilter([]string{"core-read"})
	names := f.AllowedNames()
	for i := 1; i < len(names); i++ {
		if names[i-1] >= names[i] {
			t.Fatalf("expected sorted names, got %v", names)
		}
	}
}

func TestOpsWritePackRegistersEverySupportedMutation(t *testing.T) {
	t.Setenv(config.ApprovalSigningKeyEnv, "0123456789abcdef0123456789abcdef")
	cfg := &config.Config{
		Server: config.ServerConfig{
			Transport: "streamable-http",
			ToolPacks: []string{config.PackOpsWrite},
		},
		Clusters: []config.ClusterConfig{{
			Name:             "local",
			BootstrapServers: "localhost:9092",
			Default:          true,
		}},
		Policies: config.PolicyConfig{
			DefaultDeny: true,
			Identity: config.IdentityConfig{
				Provider: "static",
				Static: config.StaticIdentityConfig{Tokens: []config.StaticTokenConfig{{
					Token: "0123456789abcdef", AgentID: "operator",
				}}},
			},
			Agents: []config.AgentPolicy{{
				ID:   "operator",
				Role: "admin",
			}},
		},
		SchemaCache: config.CacheConfig{MaxEntries: 10, TTLSeconds: 60},
		Audit:       config.AuditConfig{Enabled: false, MaxEntries: 10},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := New(cfg, &kafka.MockKafkaManager{}, logger, "test")
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("close server: %v", err)
		}
	}()

	registered := s.RegisteredTools()
	for _, name := range WriteToolNames() {
		stability, ok := registered[name]
		if !ok {
			t.Errorf("ops-write tool %q is not registered; remove it from the policy set or wire the real tool", name)
		} else if stability != "beta" {
			t.Errorf("ops-write tool %q stability = %q, want beta", name, stability)
		}
	}
	if _, ok := registered["kafka_approval"]; !ok {
		t.Fatal("ops-write must expose the approval lifecycle tool")
	}
}
