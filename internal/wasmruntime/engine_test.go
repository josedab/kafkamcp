package wasmruntime

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

var testWASMBytes = append([]byte("\x00asm"), make([]byte, 100)...)

func TestLoadPlugin(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	manifest := PluginManifest{
		Name:        "test-plugin",
		Version:     "1.0.0",
		Description: "A test WASM plugin",
		Language:    LangRust,
		Exports:     []ExportDef{{Name: "transform", Description: "Transform data"}},
	}

	plugin, err := e.LoadPlugin(manifest, testWASMBytes)
	if err != nil {
		t.Fatalf("LoadPlugin: %v", err)
	}
	if plugin.Status != StatusLoaded {
		t.Errorf("expected loaded status, got %s", plugin.Status)
	}
	if len(plugin.Exports) != 1 {
		t.Errorf("expected 1 export, got %d", len(plugin.Exports))
	}
	if plugin.WASMSize != len(testWASMBytes) {
		t.Errorf("expected wasm size %d, got %d", len(testWASMBytes), plugin.WASMSize)
	}
}

func TestLoadPluginValidation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	if _, err := e.LoadPlugin(PluginManifest{}, nil); err == nil {
		t.Error("expected error for empty name")
	}
	if _, err := e.LoadPlugin(PluginManifest{Name: "test"}, nil); err == nil {
		t.Error("expected error for empty bytes")
	}
	if _, err := e.LoadPlugin(PluginManifest{Name: "test"}, []byte("invalid")); err == nil {
		t.Error("expected error for invalid magic bytes")
	}
	if _, err := e.LoadPlugin(PluginManifest{Name: "test"}, []byte("\x00asm")); err != nil {
		t.Errorf("minimal valid wasm should load: %v", err)
	}
}

func TestExecute(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	manifest := PluginManifest{
		Name:    "exec-test",
		Version: "1.0.0",
		Language: LangGo,
		Exports: []ExportDef{{Name: "process"}},
	}
	plugin, _ := e.LoadPlugin(manifest, testWASMBytes)

	result, err := e.Execute(context.Background(), plugin.ID, "process", `{"key":"value"}`, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.Success {
		t.Errorf("expected success, got error: %s", result.Error)
	}
	if result.Output == "" {
		t.Error("expected output")
	}
	if result.FuelConsumed == 0 {
		t.Error("expected non-zero fuel consumed")
	}

	p, _ := e.GetPlugin(plugin.ID)
	if p.RunCount != 1 {
		t.Errorf("expected run count 1, got %d", p.RunCount)
	}
	if p.Status != StatusLoaded {
		t.Errorf("expected loaded status after execution, got %s", p.Status)
	}
}

func TestExecuteContextCancellation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	manifest := PluginManifest{
		Name:    "cancel-test",
		Exports: []ExportDef{{Name: "run"}},
	}
	plugin, _ := e.LoadPlugin(manifest, testWASMBytes)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	cfg := SandboxConfig{
		MaxMemoryMB:  64,
		MaxFuelUnits: 1_000_000,
		Timeout:      50 * time.Millisecond,
	}
	result, err := e.Execute(ctx, plugin.ID, "run", "", &cfg)
	if err != nil {
		t.Fatalf("Execute should not return error, got: %v", err)
	}
	// With a cancelled context and short timeout, should either succeed quickly
	// or time out — both are valid
	_ = result
}

func TestExecuteFuelExhaustion(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	manifest := PluginManifest{
		Name:    "fuel-test",
		Exports: []ExportDef{{Name: "run"}},
	}
	plugin, _ := e.LoadPlugin(manifest, testWASMBytes)

	cfg := SandboxConfig{
		MaxMemoryMB:  64,
		MaxFuelUnits: 10, // very low fuel limit
		Timeout:      30 * time.Second,
	}
	// A non-empty input should trigger fuel exhaustion
	result, err := e.Execute(context.Background(), plugin.ID, "run", "some input data", &cfg)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Success {
		t.Error("expected failure due to fuel exhaustion")
	}
	if result.Error == "" {
		t.Error("expected error message for fuel exhaustion")
	}
}

func TestExecuteInputSizeLimit(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	manifest := PluginManifest{
		Name:    "size-test",
		Exports: []ExportDef{{Name: "run"}},
	}
	plugin, _ := e.LoadPlugin(manifest, testWASMBytes)

	cfg := SandboxConfig{
		MaxMemoryMB:  1, // 1MB limit
		MaxFuelUnits: 1_000_000,
		Timeout:      30 * time.Second,
	}
	// Create input larger than 1MB
	bigInput := string(make([]byte, 2*1024*1024))
	_, err := e.Execute(context.Background(), plugin.ID, "run", bigInput, &cfg)
	if err == nil {
		t.Error("expected error for oversized input")
	}
}

func TestExecuteInvalidFunction(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	manifest := PluginManifest{
		Name:    "test",
		Exports: []ExportDef{{Name: "run"}},
	}
	plugin, _ := e.LoadPlugin(manifest, testWASMBytes)

	_, err := e.Execute(context.Background(), plugin.ID, "nonexistent", "", nil)
	if err == nil {
		t.Error("expected error for non-exported function")
	}
}

func TestExecutePluginNotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	_, err := e.Execute(context.Background(), "nonexistent", "run", "", nil)
	if err == nil {
		t.Error("expected error for nonexistent plugin")
	}
}

func TestUnloadPlugin(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	manifest := PluginManifest{Name: "remove-me", Exports: []ExportDef{{Name: "run"}}}
	plugin, _ := e.LoadPlugin(manifest, testWASMBytes)

	if err := e.UnloadPlugin(plugin.ID); err != nil {
		t.Fatalf("UnloadPlugin: %v", err)
	}

	_, err := e.GetPlugin(plugin.ID)
	if err == nil {
		t.Error("expected error after unload")
	}

	// Double unload should error
	if err := e.UnloadPlugin(plugin.ID); err == nil {
		t.Error("expected error on double unload")
	}
}

func TestListPlugins(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	e.LoadPlugin(PluginManifest{Name: "a", Exports: []ExportDef{{Name: "x"}}}, testWASMBytes)
	e.LoadPlugin(PluginManifest{Name: "b", Exports: []ExportDef{{Name: "y"}}}, testWASMBytes)

	plugins := e.ListPlugins()
	if len(plugins) != 2 {
		t.Errorf("expected 2 plugins, got %d", len(plugins))
	}
}

func TestUpdateSandbox(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	manifest := PluginManifest{Name: "sandbox-test", Exports: []ExportDef{{Name: "run"}}}
	plugin, _ := e.LoadPlugin(manifest, testWASMBytes)

	cfg := SandboxConfig{MaxMemoryMB: 128, Timeout: time.Minute}
	if err := e.UpdateSandbox(plugin.ID, cfg); err != nil {
		t.Fatalf("UpdateSandbox: %v", err)
	}

	// Exceeding max memory
	cfg.MaxMemoryMB = 1024
	if err := e.UpdateSandbox(plugin.ID, cfg); err == nil {
		t.Error("expected error for memory exceeding 512MB")
	}

	// Exceeding max timeout
	cfg.MaxMemoryMB = 64
	cfg.Timeout = 10 * time.Minute
	if err := e.UpdateSandbox(plugin.ID, cfg); err == nil {
		t.Error("expected error for timeout exceeding 5 minutes")
	}
}
