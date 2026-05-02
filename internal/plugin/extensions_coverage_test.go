package plugin

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
)

// Additional tests for extensions.go — covering RegisterWithManifest register-failure
// cleanup path and concurrent plugin registration.

type mockFailInitPlugin struct {
	name    string
	version string
}

func (p *mockFailInitPlugin) Name() string                                     { return p.name }
func (p *mockFailInitPlugin) Version() string                                  { return p.version }
func (p *mockFailInitPlugin) Description() string                              { return "fail init" }
func (p *mockFailInitPlugin) Init(_ context.Context, _ map[string]any) error   { return fmt.Errorf("init exploded") }
func (p *mockFailInitPlugin) Close() error                                     { return nil }

func TestExtendedRegistry_RegisterWithManifest_InitFailureRevokesCapabilities(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	reg := NewRegistry(logger)
	er := NewExtendedRegistry(reg)

	plugin := &mockFailInitPlugin{name: "fail-plugin", version: "1.0"}
	manifest := &Manifest{
		Name:       "fail-plugin",
		Version:    "1.0",
		APIVersion: "v1",
		Type:       "native",
		Capabilities: PluginCapabilities{
			Tools:   []string{"tool-a"},
			Network: true,
		},
	}

	err := er.RegisterWithManifest(context.Background(), plugin, manifest, map[string]any{})
	if err == nil {
		t.Fatal("expected error from plugin init failure")
	}

	// Verify sandbox capabilities were revoked after failure
	if er.Sandbox().CheckToolAccess("fail-plugin", "tool-a") {
		t.Error("expected tool access to be revoked after register failure")
	}
	if er.Sandbox().CheckNetworkAccess("fail-plugin") {
		t.Error("expected network access to be revoked after register failure")
	}
}

func TestExtendedRegistry_RegisterWithManifest_LifecycleHookError(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	reg := NewRegistry(logger)
	er := NewExtendedRegistry(reg)

	plugin := &mockLifecyclePlugin{
		name:    "hook-fail-plugin",
		version: "1.0",
		hooks:   []LifecycleHook{HookInit, HookRegisterTools},
		hookErr: fmt.Errorf("hook failed"),
	}
	manifest := &Manifest{
		Name:       "hook-fail-plugin",
		Version:    "1.0",
		APIVersion: "v1",
		Type:       "native",
	}

	// Hook errors are logged but don't prevent registration
	err := er.RegisterWithManifest(context.Background(), plugin, manifest, map[string]any{})
	if err != nil {
		t.Fatalf("unexpected error: %v (hook errors should be logged, not fatal)", err)
	}

	// Verify hooks were still called
	if len(plugin.hookLog) != 2 {
		t.Errorf("expected 2 hook calls (init + register_tools), got %d", len(plugin.hookLog))
	}

	// Plugin should still be registered
	m := er.GetManifest("hook-fail-plugin")
	if m == nil {
		t.Error("expected manifest to be stored despite hook errors")
	}
}

func TestExtendedRegistry_RegisterWithManifest_NonLifecyclePlugin(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	reg := NewRegistry(logger)
	er := NewExtendedRegistry(reg)

	// Simple plugin (not LifecyclePlugin) — lifecycle hooks should be skipped
	p := &successInitPlugin{name: "simple-ok", version: "1.0"}

	manifest := &Manifest{
		Name:       "simple-ok",
		Version:    "1.0",
		APIVersion: "v1",
		Type:       "native",
	}

	err := er.RegisterWithManifest(context.Background(), p, manifest, map[string]any{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

type successInitPlugin struct {
	name    string
	version string
}

func (p *successInitPlugin) Name() string                                     { return p.name }
func (p *successInitPlugin) Version() string                                  { return p.version }
func (p *successInitPlugin) Description() string                              { return "test" }
func (p *successInitPlugin) Init(_ context.Context, _ map[string]any) error   { return nil }
func (p *successInitPlugin) Close() error                                     { return nil }

func TestExtendedRegistry_ConcurrentRegistration(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	reg := NewRegistry(logger)
	er := NewExtendedRegistry(reg)

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("plugin-%d", idx)
			p := &successInitPlugin{name: name, version: "1.0"}
			manifest := &Manifest{
				Name:       name,
				Version:    "1.0",
				APIVersion: "v1",
				Type:       "native",
				Capabilities: PluginCapabilities{
					Tools: []string{fmt.Sprintf("tool-%d", idx)},
				},
			}
			if err := er.RegisterWithManifest(context.Background(), p, manifest, map[string]any{}); err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent registration error: %v", err)
	}

	// Verify all 10 plugins were registered
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("plugin-%d", i)
		if m := er.GetManifest(name); m == nil {
			t.Errorf("plugin %q manifest not found after concurrent registration", name)
		}
	}
}

func TestExtendedRegistry_UnregisterWithLifecycle_ShutdownHookCalled(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	reg := NewRegistry(logger)
	er := NewExtendedRegistry(reg)

	plugin := &mockLifecyclePlugin{
		name:    "shutting-down",
		version: "1.0",
		hooks:   []LifecycleHook{HookInit, HookShutdown},
	}
	manifest := &Manifest{
		Name:       "shutting-down",
		Version:    "1.0",
		APIVersion: "v1",
		Type:       "native",
	}

	err := er.RegisterWithManifest(context.Background(), plugin, manifest, map[string]any{})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	err = er.UnregisterWithLifecycle(context.Background(), "shutting-down")
	if err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	// Verify shutdown hook was called
	hasShutdown := false
	for _, h := range plugin.hookLog {
		if h == HookShutdown {
			hasShutdown = true
		}
	}
	if !hasShutdown {
		t.Error("expected shutdown hook to be called during unregister")
	}

	// Verify manifest is removed
	if m := er.GetManifest("shutting-down"); m != nil {
		t.Error("expected manifest to be removed after unregister")
	}
}

func TestSidecarManager_ConcurrentRegisterAndList(t *testing.T) {
	t.Parallel()
	sm := NewSidecarManager(&mockLogger{})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sm.RegisterSidecar(fmt.Sprintf("sidecar-%d", idx), fmt.Sprintf("localhost:%d", 50000+idx))
		}(i)
	}
	wg.Wait()

	sidecars := sm.ListSidecars()
	if len(sidecars) != 20 {
		t.Errorf("expected 20 sidecars, got %d", len(sidecars))
	}
}
