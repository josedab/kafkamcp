package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExtractBlockNames(t *testing.T) {
	t.Parallel()

	src := `prefix coreReadTools = stringSet(
		"kafka_list_topics",
		"kafka_describe_topic",
	)
	suffix`

	got := extractBlockNames(src, "coreReadTools = stringSet(", "\n\t)")
	if strings.Join(got, ",") != "kafka_list_topics,kafka_describe_topic" {
		t.Fatalf("extractBlockNames() = %v", got)
	}
	if got := extractBlockNames(src, "missing", "\n\t)"); got != nil {
		t.Fatalf("missing marker returned %v, want nil", got)
	}
	if got := extractBlockNames(src, "coreReadTools = stringSet(", "missing"); got != nil {
		t.Fatalf("missing terminator returned %v, want nil", got)
	}
}

func TestExtractMapKeyNames(t *testing.T) {
	t.Parallel()

	src := `writeToolPolicies = map[string]writeToolPolicy{
		"kafka_create_topic": {},
		"kafka_delete_topic": {},
	}
	suffix`

	got := extractMapKeyNames(src, "writeToolPolicies = map[string]writeToolPolicy{", "\n\t}")
	if strings.Join(got, ",") != "kafka_create_topic,kafka_delete_topic" {
		t.Fatalf("extractMapKeyNames() = %v", got)
	}
	if got := extractMapKeyNames(src, "missing", "\n\t}"); got != nil {
		t.Fatalf("missing marker returned %v, want nil", got)
	}
	if got := extractMapKeyNames(src, "writeToolPolicies = map[string]writeToolPolicy{", "missing"); got != nil {
		t.Fatalf("missing terminator returned %v, want nil", got)
	}
}

func TestParseFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "register_example.go")
	content := `package server

func register() {
	s.addTool(mcp.NewTool("kafka_example",
		withCategory("streaming"),
		mcp.WithDescription("Example tool"),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithNumber("limit", mcp.Description("Maximum records")),
	), s.handleExample)

	d.addTool(mcp.NewTool("kafka_second",
		mcp.WithBoolean("enabled", mcp.Description("Whether to enable it")),
	), d.handleSecond)
}
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	tools, err := parseFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 2 {
		t.Fatalf("parseFile() returned %d tools, want 2", len(tools))
	}

	first := tools[0]
	if first.Name != "kafka_example" || first.Description != "Example tool" ||
		first.Category != "streaming" || first.SourceFile != "register_example.go" ||
		first.Tier != "archived" {
		t.Fatalf("unexpected first tool: %+v", first)
	}
	if len(first.Params) != 2 {
		t.Fatalf("first tool has %d parameters, want 2", len(first.Params))
	}
	if !first.Params[0].Required || first.Params[0].Type != "string" ||
		first.Params[0].Name != "topic" {
		t.Fatalf("unexpected required parameter: %+v", first.Params[0])
	}
	if first.Params[1].Required || first.Params[1].Type != "number" {
		t.Fatalf("unexpected optional parameter: %+v", first.Params[1])
	}

	if _, err := parseFile(filepath.Join(t.TempDir(), "missing.go")); err == nil {
		t.Fatal("parseFile() error = nil for missing file")
	}
}

func TestRenderMarkdown(t *testing.T) {
	t.Parallel()

	longDescription := strings.Repeat("x", 90)
	tools := []toolDef{
		{
			Name:        "kafka_list_topics",
			Description: longDescription,
			SourceFile:  "register_p0.go",
			Tier:        "stable",
			Category:    "streaming",
			Packs:       []string{"core-read"},
			Params: []toolParam{{
				Name:        "filter",
				Type:        "string",
				Description: "Topic filter",
			}},
		},
		{
			Name:        "kafka_create_topic",
			Description: "Create a topic",
			SourceFile:  "register_p0.go",
			Tier:        "beta",
			Packs:       []string{"governance", "ops-write"},
			Params: []toolParam{{
				Name:        "topic",
				Type:        "string",
				Required:    true,
				Description: "Topic name",
			}},
		},
	}

	got := renderMarkdown(tools)
	for _, want := range []string{
		"# KafkaMCP Tool Reference",
		"**2 tools**",
		"| `stable` | 1 |",
		"| `beta` | 1 |",
		"## Stable (1 tools)",
		"## Beta (1 tools)",
		"### `kafka_create_topic`",
		"**Tool packs:** `governance`, `ops-write`",
		"| `topic` | string | ✓ | Topic name |",
		"...",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("renderMarkdown() missing %q", want)
		}
	}
}

func TestToolHelpers(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		"kafka_create_topic",
		"kafka_delete_topic",
		"kafka_create_connector",
		"kafka_update_connector",
		"kafka_delete_connector",
	} {
		if !isIdempotentTool(name) {
			t.Errorf("isIdempotentTool(%q) = false", name)
		}
	}
	if isIdempotentTool("kafka_produce") {
		t.Error("isIdempotentTool(kafka_produce) = true")
	}

	if got := escapeMDX("map { key: value }"); got != "map &#123; key: value &#125;" {
		t.Fatalf("escapeMDX() = %q", got)
	}
}

func TestRenderContract(t *testing.T) {
	t.Parallel()

	data, err := renderContract([]toolDef{{
		Name:        "kafka_list_topics",
		Description: "List topics",
		SourceFile:  "register_p0.go",
		Tier:        "stable",
		Category:    "streaming",
		Packs:       []string{"core-read"},
		Params: []toolParam{{
			Name: "filter", Type: "string", Description: "Topic filter",
		}},
	}})
	if err != nil {
		t.Fatalf("renderContract: %v", err)
	}
	var contract toolContract
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatalf("unmarshal contract: %v", err)
	}
	if contract.Version != 1 || len(contract.Tools) != 1 ||
		contract.Tools[0].Name != "kafka_list_topics" ||
		contract.Tools[0].Params[0].Name != "filter" {
		t.Fatalf("contract = %#v", contract)
	}
}
