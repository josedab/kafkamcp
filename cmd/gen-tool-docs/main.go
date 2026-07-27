// Command gen-tool-docs extracts MCP tool definitions from register_*.go files
// and generates a markdown reference document at docs/tool-reference.md.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

type toolParam struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Description string `json:"description"`
}

type toolDef struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Params      []toolParam `json:"parameters,omitempty"`
	SourceFile  string      `json:"source_file"`
	Tier        string      `json:"stability"`
	Category    string      `json:"category,omitempty"`
	Packs       []string    `json:"packs"`
}

type toolContract struct {
	Version int       `json:"version"`
	Tools   []toolDef `json:"tools"`
}

var (
	toolNameRe = regexp.MustCompile(`mcp\.NewTool\("([^"]+)"`)
	descRe     = regexp.MustCompile(`mcp\.WithDescription\("([^"]+)"\)`)
	paramRe    = regexp.MustCompile(`(?m)mcp\.With(String|Number|Boolean|Object)\("([^"]+)",[^\n]*?mcp\.Description\("([^"]+)"\)[^\n]*\)`)
	requiredRe = regexp.MustCompile(`mcp\.Required\(\)`)
	categoryRe = regexp.MustCompile(`withCategory\("([^"]+)"\)`)
	// Extracts every quoted string from a Go var = stringSet(... ) block.
	// Used to learn which tools are in essentialPackTools without importing
	// the server package (which would create an import cycle with main).
	packEntryRe = regexp.MustCompile(`"([a-z][a-z0-9_]+)"`)
	mapKeyRe    = regexp.MustCompile(`(?m)^\s*"([a-z][a-z0-9_]+)"\s*:`)
)

type surfaceClass struct {
	Stability string
	Packs     []string
	Write     bool
}

// toolSurfaceFromSource parses the production pack allow-lists. Tools not in
// this surface are archived implementation code and are intentionally omitted
// from the public tool reference.
func toolSurfaceFromSource() (map[string]surfaceClass, error) {
	path := filepath.Join("internal", "server", "tool_packs.go")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	src := string(data)
	out := make(map[string]surfaceClass)
	add := func(name, stability, pack string, write bool) {
		current := out[name]
		if current.Stability == "" || stability == "stable" {
			current.Stability = stability
		}
		current.Write = current.Write || write
		for _, existing := range current.Packs {
			if existing == pack {
				out[name] = current
				return
			}
		}
		current.Packs = append(current.Packs, pack)
		sort.Strings(current.Packs)
		out[name] = current
	}
	for _, name := range extractBlockNames(src, "coreReadTools = stringSet(", "\n)") {
		add(name, "stable", "core-read", false)
	}
	for _, name := range extractMapKeyNames(src, "writeToolPolicies = map[string]writeToolPolicy{", "\n}") {
		add(name, "beta", "ops-write", true)
	}
	add("kafka_approval", "beta", "ops-write", false)
	for _, name := range extractBlockNames(src, "governancePackTools = stringSet(", "\n)") {
		add(name, "beta", "governance", false)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no production tool packs found in %s", path)
	}
	return out, nil
}

func extractMapKeyNames(src, marker, terminator string) []string {
	start := strings.Index(src, marker)
	if start == -1 {
		return nil
	}
	rest := src[start:]
	end := strings.Index(rest, terminator)
	if end == -1 {
		return nil
	}
	var names []string
	for _, match := range mapKeyRe.FindAllStringSubmatch(rest[:end], -1) {
		names = append(names, match[1])
	}
	return names
}

func extractBlockNames(src, marker, terminator string) []string {
	start := strings.Index(src, marker)
	if start == -1 {
		return nil
	}
	rest := src[start:]
	end := strings.Index(rest, terminator)
	if end == -1 {
		return nil
	}
	var names []string
	for _, match := range packEntryRe.FindAllStringSubmatch(rest[:end], -1) {
		names = append(names, match[1])
	}
	return names
}

func main() {
	registerDir := filepath.Join("internal", "server")
	outputFile := filepath.Join("docs", "tool-reference.md")
	websiteOutputFile := filepath.Join("website", "docs", "api-reference", "tools.md")
	contractOutputFile := filepath.Join("sdk", "tool-contract.json")

	surface, err := toolSurfaceFromSource()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading production tool surface: %v\n", err)
		os.Exit(1)
	}

	files, err := filepath.Glob(filepath.Join(registerDir, "register_*.go"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error finding register files: %v\n", err)
		os.Exit(1)
	}
	sort.Strings(files)

	var parsedTools []toolDef
	for _, f := range files {
		parsed, err := parseFile(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: error parsing %s: %v\n", f, err)
			continue
		}
		parsedTools = append(parsedTools, parsed...)
	}

	seen := make(map[string]string)
	var tools []toolDef
	for _, tool := range parsedTools {
		class, ok := surface[tool.Name]
		if !ok {
			continue
		}
		if previous, duplicate := seen[tool.Name]; duplicate {
			fmt.Fprintf(os.Stderr, "duplicate tool %q in %s and %s\n", tool.Name, previous, tool.SourceFile)
			os.Exit(1)
		}
		seen[tool.Name] = tool.SourceFile
		tool.Tier = class.Stability
		tool.Packs = class.Packs
		if class.Write {
			tool.Params = append(tool.Params, toolParam{
				Name:        "approval_token",
				Type:        "string",
				Description: "Short-lived token issued by kafka_approval for this exact request.",
			})
			if isIdempotentTool(tool.Name) {
				tool.Params = append(tool.Params, toolParam{
					Name:        "idempotency_key",
					Type:        "string",
					Description: "Optional retry key for safely suppressing a duplicate successful mutation.",
				})
			}
		}
		tools = append(tools, tool)
	}
	for name := range surface {
		if _, ok := seen[name]; !ok {
			fmt.Fprintf(os.Stderr, "production pack references unregistered tool %q\n", name)
			os.Exit(1)
		}
	}

	sort.Slice(tools, func(i, j int) bool {
		return tools[i].Name < tools[j].Name
	})

	markdown := renderMarkdown(tools)
	if err := os.WriteFile(outputFile, []byte(markdown), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "error writing output: %v\n", err)
		os.Exit(1)
	}
	websiteMarkdown := fmt.Sprintf(`---
sidebar_position: 1
title: "API Reference: Tools"
toc_max_heading_level: 3
---

KafkaMCP exposes %d MCP tools across the supported production surface.

`, len(tools)) + strings.TrimRight(escapeMDX(markdown), "\n") + "\n"
	if err := os.WriteFile(websiteOutputFile, []byte(websiteMarkdown), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "error writing website output: %v\n", err)
		os.Exit(1)
	}
	contract, err := renderContract(tools)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error rendering tool contract: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(contractOutputFile, contract, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "error writing tool contract: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Generated %s, %s, and %s with %d tools\n",
		outputFile, websiteOutputFile, contractOutputFile, len(tools))
}

func parseFile(path string) ([]toolDef, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	content := string(data)
	sourceFile := filepath.Base(path)

	// Split on mcp.NewTool to isolate each tool block
	blocks := strings.Split(content, "mcp.NewTool(")
	var tools []toolDef

	for i := 1; i < len(blocks); i++ {
		block := blocks[i]

		// Find closing of the AddTool call (approximate: find "), s." or "), d." pattern)
		endIdx := strings.Index(block, "), s.")
		if endIdx == -1 {
			endIdx = strings.Index(block, "), d.")
		}
		if endIdx == -1 {
			endIdx = len(block)
		}
		block = block[:endIdx]

		nameMatch := toolNameRe.FindStringSubmatch("mcp.NewTool(" + block)
		if nameMatch == nil {
			continue
		}

		td := toolDef{
			Name:       nameMatch[1],
			SourceFile: sourceFile,
			Tier:       "archived",
		}

		if m := descRe.FindStringSubmatch(block); m != nil {
			td.Description = m[1]
		}

		if m := categoryRe.FindStringSubmatch(block); m != nil {
			td.Category = m[1]
		}

		// Extract parameters
		paramMatches := paramRe.FindAllStringSubmatch(block, -1)
		for _, pm := range paramMatches {
			p := toolParam{
				Type:        strings.ToLower(pm[1]),
				Name:        pm[2],
				Description: pm[3],
				Required:    requiredRe.MatchString(pm[0]),
			}
			td.Params = append(td.Params, p)
		}

		tools = append(tools, td)
	}

	return tools, nil
}

func isIdempotentTool(name string) bool {
	switch name {
	case "kafka_create_topic", "kafka_delete_topic", "kafka_create_connector",
		"kafka_update_connector", "kafka_delete_connector":
		return true
	default:
		return false
	}
}

func escapeMDX(markdown string) string {
	return strings.NewReplacer(
		"{", "&#123;",
		"}", "&#125;",
	).Replace(markdown)
}

func renderMarkdown(tools []toolDef) string {
	var sb strings.Builder

	sb.WriteString("# KafkaMCP Tool Reference\n\n")
	sb.WriteString(fmt.Sprintf("> Auto-generated on %s from `internal/server/register_*.go`.\n", time.Now().UTC().Format("2006-01-02")))
	sb.WriteString("> Do not edit manually. Run `make generate` to regenerate.\n\n")

	// Tier summary
	tierBuckets := map[string][]toolDef{}
	for _, t := range tools {
		tier := t.Tier
		if tier == "" {
			tier = "beta"
		}
		tierBuckets[tier] = append(tierBuckets[tier], t)
	}
	tierOrder := []string{"stable", "beta"}

	sb.WriteString(fmt.Sprintf("## Summary\n\n**%d tools** are available across the supported production surface.\n\n", len(tools)))
	sb.WriteString("The default `core-read` pack contains at most 15 read-only tools. The guarded `ops-write` and `governance` packs require secure network identity, default-deny authorization, and signed approval records. `ops-write` adds real mutations and optional idempotency keys; `governance` adds policy and decision controls. No simulated or placeholder tools are shipped.\n\n")
	sb.WriteString("| Stability | Count | Tool packs |\n|---|---:|---|\n")
	for _, tier := range tierOrder {
		count := len(tierBuckets[tier])
		var packs string
		switch tier {
		case "stable":
			packs = "`core-read` (default)"
		case "beta":
			packs = "`ops-write`, `governance`"
		}
		sb.WriteString(fmt.Sprintf("| `%s` | %d | %s |\n", tier, count, packs))
	}
	sb.WriteString("\n")

	// Master index
	sb.WriteString("| Tool | Stability | Packs | Category | Description |\n|---|---|---|---|---|\n")
	for _, t := range tools {
		desc := t.Description
		if len(desc) > 80 {
			desc = desc[:77] + "..."
		}
		sb.WriteString(fmt.Sprintf("| [`%s`](#%s) | %s | %s | %s | %s |\n", t.Name, t.Name, t.Tier, strings.Join(t.Packs, ", "), t.Category, desc))
	}
	sb.WriteString("\n---\n\n")

	// Per-tier sections
	for _, tier := range tierOrder {
		bucket := tierBuckets[tier]
		if len(bucket) == 0 {
			continue
		}
		sb.WriteString(fmt.Sprintf("## %s (%d tools)\n\n", strings.ToUpper(tier[:1])+tier[1:], len(bucket)))
		switch tier {
		case "stable":
			sb.WriteString("The read-only default surface. Stable names and schemas follow the published compatibility policy.\n\n")
		case "beta":
			sb.WriteString("Real functionality that must be opted into. Mutations require policy authorization and an approval grant.\n\n")
		}
		for _, t := range bucket {
			sb.WriteString(fmt.Sprintf("- [`%s`](#%s) — %s\n", t.Name, t.Name, t.Description))
		}
		sb.WriteString("\n")
	}

	// Tool details
	sb.WriteString("## Tools\n\n")
	for _, t := range tools {
		sb.WriteString(fmt.Sprintf("### `%s`\n\n", t.Name))
		if t.Description != "" {
			sb.WriteString(t.Description + "\n\n")
		}
		sb.WriteString(fmt.Sprintf("**Source:** `%s`\n\n", t.SourceFile))
		sb.WriteString(fmt.Sprintf("**Stability:** `%s`\n\n", t.Tier))
		sb.WriteString(fmt.Sprintf("**Tool packs:** `%s`", strings.Join(t.Packs, "`, `")))
		if t.Category != "" {
			sb.WriteString(fmt.Sprintf("\n\n**Category:** `%s`", t.Category))
		}
		sb.WriteString("\n\n")

		if len(t.Params) > 0 {
			sb.WriteString("**Parameters:**\n\n")
			sb.WriteString("| Name | Type | Required | Description |\n|------|------|----------|-------------|\n")
			for _, p := range t.Params {
				req := ""
				if p.Required {
					req = "✓"
				}
				sb.WriteString(fmt.Sprintf("| `%s` | %s | %s | %s |\n", p.Name, p.Type, req, p.Description))
			}
			sb.WriteString("\n")
		}
	}

	return sb.String()
}

func renderContract(tools []toolDef) ([]byte, error) {
	data, err := json.MarshalIndent(toolContract{Version: 1, Tools: tools}, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}
