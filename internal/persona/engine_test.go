package persona

import (
	"testing"
)

func TestNewEngineHasBuiltins(t *testing.T) {
	e := NewEngine()
	personas := e.ListPersonas()
	if len(personas) != 5 {
		t.Fatalf("expected 5 built-in personas, got %d", len(personas))
	}

	expected := []string{"sre", "data-engineer", "compliance", "developer", "admin"}
	for _, name := range expected {
		if _, err := e.GetPersona(name); err != nil {
			t.Errorf("expected built-in persona %q: %v", name, err)
		}
	}
}

func TestGetPersonaNotFound(t *testing.T) {
	e := NewEngine()
	_, err := e.GetPersona("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent persona")
	}
}

func TestSetActiveAndGetActive(t *testing.T) {
	e := NewEngine()

	// Default active is developer.
	active := e.GetActive()
	if active.Name != "developer" {
		t.Fatalf("expected default active persona 'developer', got %q", active.Name)
	}

	if err := e.SetActive("admin"); err != nil {
		t.Fatalf("SetActive failed: %v", err)
	}
	active = e.GetActive()
	if active.Name != "admin" {
		t.Fatalf("expected active persona 'admin', got %q", active.Name)
	}
}

func TestSetActiveNotFound(t *testing.T) {
	e := NewEngine()
	if err := e.SetActive("nonexistent"); err == nil {
		t.Fatal("expected error for nonexistent persona")
	}
}

func TestIsToolAllowed(t *testing.T) {
	e := NewEngine()

	// Admin has wildcard access.
	if !e.IsToolAllowed("admin", "anything") {
		t.Error("admin should allow any tool")
	}

	// Developer allowed tools.
	if !e.IsToolAllowed("developer", "consume") {
		t.Error("developer should allow 'consume'")
	}
	if !e.IsToolAllowed("developer", "produce") {
		t.Error("developer should allow 'produce'")
	}

	// Developer denied tools.
	if e.IsToolAllowed("developer", "topic_delete") {
		t.Error("developer should deny 'topic_delete'")
	}
	if e.IsToolAllowed("developer", "acl_create") {
		t.Error("developer should deny 'acl_create'")
	}

	// Developer disallowed tools (not in allowed list).
	if e.IsToolAllowed("developer", "alerts") {
		t.Error("developer should not allow 'alerts'")
	}

	// Compliance denied tools override allowed.
	if e.IsToolAllowed("compliance", "produce") {
		t.Error("compliance should deny 'produce'")
	}

	// Unknown persona.
	if e.IsToolAllowed("nonexistent", "consume") {
		t.Error("unknown persona should deny all tools")
	}
}

func TestApplyDefaults(t *testing.T) {
	e := NewEngine()

	args := map[string]any{"custom_key": "custom_value"}
	merged := e.ApplyDefaults("developer", args)

	// Should contain persona defaults.
	if merged["output_format"] != "json" {
		t.Errorf("expected output_format 'json', got %v", merged["output_format"])
	}
	if merged["max_messages"] != 100 {
		t.Errorf("expected max_messages 100, got %v", merged["max_messages"])
	}

	// User args take precedence.
	if merged["custom_key"] != "custom_value" {
		t.Errorf("expected custom_key preserved, got %v", merged["custom_key"])
	}

	// Override a default.
	args2 := map[string]any{"output_format": "yaml"}
	merged2 := e.ApplyDefaults("developer", args2)
	if merged2["output_format"] != "yaml" {
		t.Errorf("expected user override 'yaml', got %v", merged2["output_format"])
	}
}

func TestApplyDefaultsUnknownPersona(t *testing.T) {
	e := NewEngine()
	args := map[string]any{"key": "value"}
	merged := e.ApplyDefaults("nonexistent", args)
	if merged["key"] != "value" {
		t.Error("expected args returned unchanged for unknown persona")
	}
}

func TestListPersonasContent(t *testing.T) {
	e := NewEngine()
	personas := e.ListPersonas()

	found := make(map[string]bool)
	for _, p := range personas {
		found[p.Name] = true
		if p.DisplayName == "" {
			t.Errorf("persona %q has empty DisplayName", p.Name)
		}
		if p.Description == "" {
			t.Errorf("persona %q has empty Description", p.Name)
		}
		if p.SystemPrompt == "" {
			t.Errorf("persona %q has empty SystemPrompt", p.Name)
		}
		if len(p.AllowedTools) == 0 {
			t.Errorf("persona %q has no AllowedTools", p.Name)
		}
	}

	for _, name := range []string{"sre", "data-engineer", "compliance", "developer", "admin"} {
		if !found[name] {
			t.Errorf("missing persona %q in ListPersonas", name)
		}
	}
}

func TestPersonaFields(t *testing.T) {
	e := NewEngine()

	sre, _ := e.GetPersona("sre")
	if sre.Role != "sre" {
		t.Errorf("sre role: got %q", sre.Role)
	}
	if len(sre.Tags) == 0 {
		t.Error("sre should have tags")
	}

	compliance, _ := e.GetPersona("compliance")
	if len(compliance.DeniedTools) == 0 {
		t.Error("compliance should have denied tools")
	}
	if compliance.Defaults["mask_pii"] != true {
		t.Error("compliance should have mask_pii default")
	}
}

func TestApplyDefaultsNilArgs(t *testing.T) {
	e := NewEngine()
	merged := e.ApplyDefaults("developer", nil)
	if merged["output_format"] != "json" {
		t.Errorf("expected defaults applied to nil args, got %v", merged["output_format"])
	}
}
