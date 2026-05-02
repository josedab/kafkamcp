package plugin

import (
	"context"
	"testing"
)

// testPlugin is a simple plugin for testing.
type testPlugin struct {
	name    string
	version string
	closed  bool
}

func (p *testPlugin) Name() string        { return p.name }
func (p *testPlugin) Version() string     { return p.version }
func (p *testPlugin) Description() string { return "test plugin" }
func (p *testPlugin) Init(_ context.Context, _ map[string]any) error {
	return nil
}
func (p *testPlugin) Close() error {
	p.closed = true
	return nil
}

type testToolPlugin struct {
	testPlugin
}

func (p *testToolPlugin) Tools() []ToolDefinition {
	return []ToolDefinition{
		{Name: "test_tool", Description: "A test tool"},
	}
}

func TestRegisterAndList(t *testing.T) {
	t.Parallel()
	r := NewRegistry(nil)
	defer r.Close()

	p := &testPlugin{name: "test-plugin", version: "1.0.0"}
	err := r.Register(context.Background(), p, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	plugins := r.List()
	if len(plugins) != 1 {
		t.Fatalf("expected 1 plugin, got %d", len(plugins))
	}
	if plugins[0].Name != "test-plugin" {
		t.Errorf("expected name 'test-plugin', got %q", plugins[0].Name)
	}
	if plugins[0].Status != "loaded" {
		t.Errorf("expected status 'loaded', got %q", plugins[0].Status)
	}
}

func TestRegisterDuplicate(t *testing.T) {
	t.Parallel()
	r := NewRegistry(nil)
	defer r.Close()

	p := &testPlugin{name: "dup", version: "1.0.0"}
	_ = r.Register(context.Background(), p, nil)
	err := r.Register(context.Background(), p, nil)
	if err == nil {
		t.Fatal("expected error for duplicate registration")
	}
}

func TestUnregister(t *testing.T) {
	t.Parallel()
	r := NewRegistry(nil)
	p := &testPlugin{name: "removable", version: "1.0.0"}
	_ = r.Register(context.Background(), p, nil)

	err := r.Unregister("removable")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.closed {
		t.Error("expected plugin Close() to be called")
	}
	if r.Count() != 0 {
		t.Errorf("expected 0 plugins, got %d", r.Count())
	}
}

func TestUnregisterNotFound(t *testing.T) {
	t.Parallel()
	r := NewRegistry(nil)
	err := r.Unregister("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent plugin")
	}
}

func TestToolProvider(t *testing.T) {
	t.Parallel()
	r := NewRegistry(nil)
	defer r.Close()

	p := &testToolPlugin{testPlugin: testPlugin{name: "tool-plugin", version: "2.0.0"}}
	err := r.Register(context.Background(), p, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	info, err := r.Get("tool-plugin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(info.Tools) != 1 {
		t.Fatalf("expected 1 tool, got %d", len(info.Tools))
	}
	if info.Tools[0].Name != "test_tool" {
		t.Errorf("expected tool name 'test_tool', got %q", info.Tools[0].Name)
	}
}

func TestRegisterNil(t *testing.T) {
	t.Parallel()
	r := NewRegistry(nil)
	err := r.Register(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("expected error for nil plugin")
	}
}
