package plugin

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func testSlogLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// --- mock helpers ---

type mockLifecyclePlugin struct {
	name    string
	version string
	desc    string
	closed  bool
	hooks   []LifecycleHook
	hookLog []LifecycleHook
	hookErr error
}

func (p *mockLifecyclePlugin) Name() string                                   { return p.name }
func (p *mockLifecyclePlugin) Version() string                                { return p.version }
func (p *mockLifecyclePlugin) Description() string                            { return p.desc }
func (p *mockLifecyclePlugin) Init(_ context.Context, _ map[string]any) error { return nil }
func (p *mockLifecyclePlugin) Close() error                                   { p.closed = true; return nil }
func (p *mockLifecyclePlugin) SupportedHooks() []LifecycleHook                { return p.hooks }
func (p *mockLifecyclePlugin) OnHook(_ context.Context, hook LifecycleHook, _ map[string]any) error {
	p.hookLog = append(p.hookLog, hook)
	return p.hookErr
}

type mockLogger struct{}

func (l *mockLogger) Info(msg string, args ...any) {}
func (l *mockLogger) Warn(msg string, args ...any) {}

// --- SecuritySandbox tests ---

func TestNewSecuritySandbox(t *testing.T) {
	t.Parallel()
	sb := NewSecuritySandbox()
	if sb == nil {
		t.Fatal("expected non-nil sandbox")
	}
	if !sb.denyByDefault {
		t.Error("expected denyByDefault to be true")
	}
}

func TestSecuritySandbox_GrantAndCheckToolAccess(t *testing.T) {
	t.Parallel()
	sb := NewSecuritySandbox()

	// Before grant: denied
	if sb.CheckToolAccess("plugin1", "tool_a") {
		t.Error("expected tool access denied before grant")
	}

	sb.Grant("plugin1", PluginCapabilities{Tools: []string{"tool_a", "tool_b"}})

	if !sb.CheckToolAccess("plugin1", "tool_a") {
		t.Error("expected tool_a access granted")
	}
	if !sb.CheckToolAccess("plugin1", "tool_b") {
		t.Error("expected tool_b access granted")
	}
	if sb.CheckToolAccess("plugin1", "tool_c") {
		t.Error("expected tool_c access denied")
	}
}

func TestSecuritySandbox_WildcardToolAccess(t *testing.T) {
	t.Parallel()
	sb := NewSecuritySandbox()
	sb.Grant("plugin1", PluginCapabilities{Tools: []string{"*"}})

	if !sb.CheckToolAccess("plugin1", "anything") {
		t.Error("expected wildcard tool access")
	}
}

func TestSecuritySandbox_Revoke(t *testing.T) {
	t.Parallel()
	sb := NewSecuritySandbox()
	sb.Grant("plugin1", PluginCapabilities{Tools: []string{"tool_a"}, Network: true})

	if !sb.CheckToolAccess("plugin1", "tool_a") {
		t.Fatal("expected access before revoke")
	}

	sb.Revoke("plugin1")

	if sb.CheckToolAccess("plugin1", "tool_a") {
		t.Error("expected tool access denied after revoke")
	}
	if sb.CheckNetworkAccess("plugin1") {
		t.Error("expected network access denied after revoke")
	}
}

func TestSecuritySandbox_CheckNetworkAccess(t *testing.T) {
	t.Parallel()
	sb := NewSecuritySandbox()
	sb.Grant("net-plugin", PluginCapabilities{Network: true})
	sb.Grant("no-net", PluginCapabilities{Network: false})

	if !sb.CheckNetworkAccess("net-plugin") {
		t.Error("expected network access granted")
	}
	if sb.CheckNetworkAccess("no-net") {
		t.Error("expected network access denied")
	}
	if sb.CheckNetworkAccess("unknown") {
		t.Error("expected network access denied for unknown plugin")
	}
}

func TestSecuritySandbox_CheckFileSystemAccess(t *testing.T) {
	t.Parallel()
	sb := NewSecuritySandbox()
	sb.Grant("fs-plugin", PluginCapabilities{FileSystem: true})
	sb.Grant("no-fs", PluginCapabilities{FileSystem: false})

	if !sb.CheckFileSystemAccess("fs-plugin") {
		t.Error("expected filesystem access granted")
	}
	if sb.CheckFileSystemAccess("no-fs") {
		t.Error("expected filesystem access denied")
	}
	if sb.CheckFileSystemAccess("unknown") {
		t.Error("expected filesystem access denied for unknown plugin")
	}
}

func TestSecuritySandbox_DenyByDefaultFalse(t *testing.T) {
	t.Parallel()
	sb := &SecuritySandbox{
		allowed:       make(map[string]*PluginCapabilities),
		denyByDefault: false,
	}
	// When denyByDefault is false, all checks return true
	if !sb.CheckToolAccess("any", "any") {
		t.Error("expected tool access allowed when denyByDefault=false")
	}
	if !sb.CheckNetworkAccess("any") {
		t.Error("expected network access allowed when denyByDefault=false")
	}
	if !sb.CheckFileSystemAccess("any") {
		t.Error("expected filesystem access allowed when denyByDefault=false")
	}
}

func TestSecuritySandbox_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	sb := NewSecuritySandbox()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			name := "plugin"
			sb.Grant(name, PluginCapabilities{Tools: []string{"t1"}, Network: true})
			sb.CheckToolAccess(name, "t1")
			sb.CheckNetworkAccess(name)
			sb.CheckFileSystemAccess(name)
			sb.Revoke(name)
		}(i)
	}
	wg.Wait()
}

// --- SidecarManager tests ---

func TestSidecarManager_RegisterAndList(t *testing.T) {
	t.Parallel()
	sm := NewSidecarManager(&mockLogger{})
	s := sm.RegisterSidecar("py-plugin", "localhost:50051")
	if s.Name != "py-plugin" || s.Address != "localhost:50051" || s.Status != "connected" {
		t.Errorf("unexpected sidecar: %+v", s)
	}

	list := sm.ListSidecars()
	if len(list) != 1 {
		t.Fatalf("expected 1 sidecar, got %d", len(list))
	}
}

func TestSidecarManager_GetSidecar(t *testing.T) {
	t.Parallel()
	sm := NewSidecarManager(&mockLogger{})
	sm.RegisterSidecar("s1", "localhost:50051")

	s, err := sm.GetSidecar("s1")
	if err != nil || s.Name != "s1" {
		t.Errorf("expected sidecar s1, got err=%v", err)
	}

	_, err = sm.GetSidecar("missing")
	if err == nil {
		t.Error("expected error for missing sidecar")
	}
}

func TestSidecarManager_Unregister(t *testing.T) {
	t.Parallel()
	sm := NewSidecarManager(&mockLogger{})
	sm.RegisterSidecar("s1", "localhost:50051")

	if err := sm.UnregisterSidecar("s1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sm.ListSidecars()) != 0 {
		t.Error("expected empty sidecar list after unregister")
	}

	if err := sm.UnregisterSidecar("missing"); err == nil {
		t.Error("expected error for missing sidecar")
	}
}

func TestSidecarManager_NilLogger(t *testing.T) {
	t.Parallel()
	sm := NewSidecarManager(nil)
	s := sm.RegisterSidecar("s1", "localhost:50051")
	if s == nil {
		t.Fatal("expected non-nil sidecar even with nil logger")
	}
}

// --- Manifest validation tests ---

func TestValidateManifest_Valid(t *testing.T) {
	t.Parallel()
	m := &Manifest{Name: "my-plugin", Version: "1.0.0", Type: "native"}
	if err := validateManifest(m); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.APIVersion != "v1" {
		t.Error("expected default APIVersion v1")
	}
}

func TestValidateManifest_MissingName(t *testing.T) {
	t.Parallel()
	m := &Manifest{Version: "1.0.0"}
	if err := validateManifest(m); err == nil {
		t.Error("expected error for missing name")
	}
}

func TestValidateManifest_MissingVersion(t *testing.T) {
	t.Parallel()
	m := &Manifest{Name: "test"}
	if err := validateManifest(m); err == nil {
		t.Error("expected error for missing version")
	}
}

func TestValidateManifest_InvalidType(t *testing.T) {
	t.Parallel()
	m := &Manifest{Name: "test", Version: "1.0.0", Type: "invalid"}
	if err := validateManifest(m); err == nil {
		t.Error("expected error for invalid type")
	}
}

func TestValidateManifest_NameWithSpaces(t *testing.T) {
	t.Parallel()
	m := &Manifest{Name: "bad name", Version: "1.0.0"}
	if err := validateManifest(m); err == nil {
		t.Error("expected error for name with spaces")
	}
}

func TestValidateManifest_NameWithSlashes(t *testing.T) {
	t.Parallel()
	m := &Manifest{Name: "bad/name", Version: "1.0.0"}
	if err := validateManifest(m); err == nil {
		t.Error("expected error for name with slashes")
	}
}

func TestValidateManifest_DefaultType(t *testing.T) {
	t.Parallel()
	m := &Manifest{Name: "test", Version: "1.0.0"}
	if err := validateManifest(m); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.Type != "native" {
		t.Errorf("expected default type 'native', got %q", m.Type)
	}
}

func TestValidateManifest_AllValidTypes(t *testing.T) {
	t.Parallel()
	for _, tp := range []string{"native", "grpc", "wasm"} {
		m := &Manifest{Name: "test", Version: "1.0.0", Type: tp}
		if err := validateManifest(m); err != nil {
			t.Errorf("type %q should be valid: %v", tp, err)
		}
	}
}

// --- LoadManifest tests ---

func TestLoadManifest_ValidFile(t *testing.T) {
	t.Parallel()
	manifest := Manifest{
		APIVersion: "v1",
		Name:       "test-plugin",
		Version:    "1.0.0",
		Type:       "native",
	}
	data, _ := json.Marshal(manifest)
	path := filepath.Join(t.TempDir(), "manifest.json")
	_ = os.WriteFile(path, data, 0644)

	m, err := LoadManifest(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.Name != "test-plugin" {
		t.Errorf("expected name test-plugin, got %s", m.Name)
	}
}

func TestLoadManifest_FileNotFound(t *testing.T) {
	t.Parallel()
	_, err := LoadManifest("/nonexistent/path.json")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestLoadManifest_InvalidJSON(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "bad.json")
	_ = os.WriteFile(path, []byte("not json"), 0644)

	_, err := LoadManifest(path)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestLoadManifest_FailsValidation(t *testing.T) {
	t.Parallel()
	data, _ := json.Marshal(map[string]string{"version": "1.0.0"})
	path := filepath.Join(t.TempDir(), "incomplete.json")
	_ = os.WriteFile(path, data, 0644)

	_, err := LoadManifest(path)
	if err == nil {
		t.Error("expected validation error for missing name")
	}
}

// --- ExtendedRegistry tests ---

func TestExtendedRegistry_RegisterWithManifest(t *testing.T) {
	t.Parallel()
	reg := NewRegistry(testSlogLogger())
	er := NewExtendedRegistry(reg)

	p := &mockLifecyclePlugin{
		name:    "test-lp",
		version: "1.0.0",
		hooks:   []LifecycleHook{HookInit, HookRegisterTools},
	}
	manifest := &Manifest{
		Name:    "test-lp",
		Version: "1.0.0",
		Type:    "native",
		Capabilities: PluginCapabilities{
			Tools:   []string{"tool1"},
			Network: true,
		},
	}

	err := er.RegisterWithManifest(context.Background(), p, manifest, map[string]any{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check sandbox capabilities were granted
	if !er.Sandbox().CheckToolAccess("test-lp", "tool1") {
		t.Error("expected sandbox tool access granted")
	}
	if !er.Sandbox().CheckNetworkAccess("test-lp") {
		t.Error("expected sandbox network access granted")
	}

	// Check lifecycle hooks were called
	if len(p.hookLog) != 2 {
		t.Fatalf("expected 2 hooks called, got %d", len(p.hookLog))
	}
	if p.hookLog[0] != HookInit || p.hookLog[1] != HookRegisterTools {
		t.Errorf("unexpected hook order: %v", p.hookLog)
	}

	// Check manifest stored
	if m := er.GetManifest("test-lp"); m == nil || m.Name != "test-lp" {
		t.Error("expected manifest to be stored")
	}
}

func TestExtendedRegistry_RegisterWithManifest_MissingRequiredConfig(t *testing.T) {
	t.Parallel()
	reg := NewRegistry(testSlogLogger())
	er := NewExtendedRegistry(reg)

	p := &mockLifecyclePlugin{name: "cfg-plugin", version: "1.0.0"}
	manifest := &Manifest{
		Name:    "cfg-plugin",
		Version: "1.0.0",
		Config: map[string]ConfigParam{
			"api_key": {Type: "string", Required: true},
		},
	}

	err := er.RegisterWithManifest(context.Background(), p, manifest, map[string]any{})
	if err == nil {
		t.Error("expected error for missing required config")
	}
}

func TestExtendedRegistry_RegisterWithManifest_RequiredConfigWithDefault(t *testing.T) {
	t.Parallel()
	reg := NewRegistry(testSlogLogger())
	er := NewExtendedRegistry(reg)

	p := &mockLifecyclePlugin{name: "cfg-plugin2", version: "1.0.0"}
	manifest := &Manifest{
		Name:    "cfg-plugin2",
		Version: "1.0.0",
		Config: map[string]ConfigParam{
			"timeout": {Type: "int", Required: true, Default: 30},
		},
	}

	config := map[string]any{}
	err := er.RegisterWithManifest(context.Background(), p, manifest, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config["timeout"] != 30 {
		t.Errorf("expected default value 30 for timeout, got %v", config["timeout"])
	}
}

func TestExtendedRegistry_UnregisterWithLifecycle(t *testing.T) {
	t.Parallel()
	reg := NewRegistry(testSlogLogger())
	er := NewExtendedRegistry(reg)

	p := &mockLifecyclePlugin{
		name:    "unreg-plugin",
		version: "1.0.0",
		hooks:   []LifecycleHook{HookShutdown},
	}
	manifest := &Manifest{
		Name:         "unreg-plugin",
		Version:      "1.0.0",
		Capabilities: PluginCapabilities{Tools: []string{"t1"}},
	}

	_ = er.RegisterWithManifest(context.Background(), p, manifest, map[string]any{})

	err := er.UnregisterWithLifecycle(context.Background(), "unreg-plugin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Shutdown hook should have been called
	found := false
	for _, h := range p.hookLog {
		if h == HookShutdown {
			found = true
		}
	}
	if !found {
		t.Error("expected shutdown hook to be called")
	}

	// Sandbox should be revoked
	if er.Sandbox().CheckToolAccess("unreg-plugin", "t1") {
		t.Error("expected sandbox revoked after unregister")
	}

	// Manifest should be removed
	if er.GetManifest("unreg-plugin") != nil {
		t.Error("expected manifest removed after unregister")
	}
}

func TestExtendedRegistry_UnregisterNonexistent(t *testing.T) {
	t.Parallel()
	reg := NewRegistry(testSlogLogger())
	er := NewExtendedRegistry(reg)

	err := er.UnregisterWithLifecycle(context.Background(), "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent plugin")
	}
}

func TestExtendedRegistry_Sidecars(t *testing.T) {
	t.Parallel()
	reg := NewRegistry(testSlogLogger())
	er := NewExtendedRegistry(reg)

	sm := er.Sidecars()
	if sm == nil {
		t.Fatal("expected non-nil sidecar manager")
	}
	sm.RegisterSidecar("s1", "localhost:50051")
	if len(sm.ListSidecars()) != 1 {
		t.Error("expected 1 sidecar via registry")
	}
}

func TestExtendedRegistry_GetManifestMissing(t *testing.T) {
	t.Parallel()
	reg := NewRegistry(testSlogLogger())
	er := NewExtendedRegistry(reg)
	if m := er.GetManifest("missing"); m != nil {
		t.Error("expected nil manifest for unregistered plugin")
	}
}
