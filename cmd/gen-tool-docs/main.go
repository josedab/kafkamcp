// Command gen-tool-docs extracts MCP tool definitions from register_*.go files
// and generates a markdown reference document at docs/tool-reference.md.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

type toolParam struct {
	Name        string
	Type        string
	Required    bool
	Description string
}

type toolDef struct {
	Name        string
	Description string
	Params      []toolParam
	SourceFile  string
	Tier        string
	Category    string
}

var (
	toolNameRe = regexp.MustCompile(`mcp\.NewTool\("([^"]+)"`)
	descRe     = regexp.MustCompile(`mcp\.WithDescription\("([^"]+)"\)`)
	paramRe    = regexp.MustCompile(`mcp\.With(String|Number|Boolean|Object)\("([^"]+)"[^)]*mcp\.Description\("([^"]+)"\)`)
	requiredRe = regexp.MustCompile(`mcp\.Required\(\)`)
	tierRe     = regexp.MustCompile(`withTier\(\s*(?:Tier(\w+)|"(\w+)")\s*\)`)
	categoryRe = regexp.MustCompile(`withCategory\("([^"]+)"\)`)
	// Extracts every quoted string from a Go var = stringSet(... ) block.
	// Used to learn which tools are in essentialPackTools without importing
	// the server package (which would create an import cycle with main).
	packEntryRe = regexp.MustCompile(`"([a-z][a-z0-9_]+)"`)
)

const defaultTier = "advanced"

// essentialNamesFromSource parses internal/server/tool_packs.go to extract
// the names listed in essentialPackTools. The generator uses this list to
// promote those tools to the "essential" tier in the rendered docs even
// when the register_*.go site does not call withTier explicitly.
func essentialNamesFromSource() (map[string]struct{}, error) {
	path := filepath.Join("internal", "server", "tool_packs.go")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	src := string(data)
	start := strings.Index(src, "essentialPackTools = stringSet(")
	if start == -1 {
		return nil, fmt.Errorf("could not find essentialPackTools in %s", path)
	}
	rest := src[start:]
	end := strings.Index(rest, "\n)")
	if end == -1 {
		return nil, fmt.Errorf("could not find closing paren for essentialPackTools")
	}
	block := rest[:end]
	out := make(map[string]struct{})
	for _, m := range packEntryRe.FindAllStringSubmatch(block, -1) {
		out[m[1]] = struct{}{}
	}
	return out, nil
}

func main() {
	registerDir := filepath.Join("internal", "server")
	outputFile := filepath.Join("docs", "tool-reference.md")

	essentialNames, err := essentialNamesFromSource()
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not load essentialPackTools: %v\n", err)
		essentialNames = map[string]struct{}{}
	}

	files, err := filepath.Glob(filepath.Join(registerDir, "register_*.go"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error finding register files: %v\n", err)
		os.Exit(1)
	}
	sort.Strings(files)

	var tools []toolDef
	for _, f := range files {
		parsed, err := parseFile(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: error parsing %s: %v\n", f, err)
			continue
		}
		tools = append(tools, parsed...)
	}

	// Promote essential tools by name when not already tagged.
	for i := range tools {
		if tools[i].Tier != defaultTier {
			continue
		}
		if _, ok := essentialNames[tools[i].Name]; ok {
			tools[i].Tier = "essential"
		}
	}

	sort.Slice(tools, func(i, j int) bool {
		return tools[i].Name < tools[j].Name
	})

	if err := writeMarkdown(outputFile, tools); err != nil {
		fmt.Fprintf(os.Stderr, "error writing output: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Generated %s with %d tools\n", outputFile, len(tools))
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
			Tier:       defaultTier,
		}

		if m := descRe.FindStringSubmatch(block); m != nil {
			td.Description = m[1]
		}

		if m := tierRe.FindStringSubmatch(block); m != nil {
			tier := strings.ToLower(m[1])
			if tier == "" {
				tier = strings.ToLower(m[2])
			}
			if tier != "" {
				td.Tier = tier
			}
		}

		if m := categoryRe.FindStringSubmatch(block); m != nil {
			td.Category = m[1]
		}

		// Extract parameters
		paramMatches := paramRe.FindAllStringSubmatch(block, -1)
		for _, pm := range paramMatches {
			idx := strings.Index(block, pm[0])
			if idx == -1 {
				continue
			}
			// Check if mcp.Required() appears near this parameter
			start := max(0, idx-50)
			end := min(len(block), idx+len(pm[0])+20)
			surrounding := block[start:end]
			p := toolParam{
				Type:        strings.ToLower(pm[1]),
				Name:        pm[2],
				Description: pm[3],
				Required:    requiredRe.MatchString(surrounding),
			}
			td.Params = append(td.Params, p)
		}

		tools = append(tools, td)
	}

	return tools, nil
}

func writeMarkdown(path string, tools []toolDef) error {
	var sb strings.Builder

	sb.WriteString("# KafkaMCP Tool Reference\n\n")
	sb.WriteString(fmt.Sprintf("> Auto-generated on %s from `internal/server/register_*.go`.\n", time.Now().UTC().Format("2006-01-02")))
	sb.WriteString("> Do not edit manually. Run `make generate` to regenerate.\n\n")

	// Tier summary
	tierBuckets := map[string][]toolDef{}
	for _, t := range tools {
		tier := t.Tier
		if tier == "" {
			tier = defaultTier
		}
		tierBuckets[tier] = append(tierBuckets[tier], t)
	}
	tierOrder := []string{"essential", "advanced", "experimental"}

	sb.WriteString(fmt.Sprintf("## Summary\n\n**%d tools** available across the full catalog.\n\n", len(tools)))
	sb.WriteString("Tools are organized into **tiers** (essential / advanced / experimental) and surfaced to MCP clients through **tool packs** configured via `server.tool_packs` in `kafkamcp.yaml`. The default pack is `essential`, which keeps the advertised surface ≤50 tools so LLM tool-routing quality stays high.\n\n")
	sb.WriteString("| Tier | Count | When registered |\n|------|-------|------------------|\n")
	for _, tier := range tierOrder {
		count := len(tierBuckets[tier])
		var when string
		switch tier {
		case "essential":
			when = "Always (every pack except `experimental`-only)"
		case "advanced":
			when = "When the matching pack (`ops`, `analytics`, `governance`, `streaming`, `all`) is active"
		case "experimental":
			when = "Only when `experimental` (or `all`) pack is active *and* the matching `experimental.*` flag is set"
		}
		sb.WriteString(fmt.Sprintf("| `%s` | %d | %s |\n", tier, count, when))
	}
	sb.WriteString("\n")

	// Master index
	sb.WriteString("| Tool | Tier | Category | Description |\n|------|------|----------|-------------|\n")
	for _, t := range tools {
		desc := t.Description
		if len(desc) > 80 {
			desc = desc[:77] + "..."
		}
		sb.WriteString(fmt.Sprintf("| [`%s`](#%s) | %s | %s | %s |\n", t.Name, t.Name, t.Tier, t.Category, desc))
	}
	sb.WriteString("\n---\n\n")

	// Per-tier sections
	for _, tier := range tierOrder {
		bucket := tierBuckets[tier]
		if len(bucket) == 0 {
			continue
		}
		sb.WriteString(fmt.Sprintf("## %s tier (%d tools)\n\n", strings.Title(tier), len(bucket))) //nolint:staticcheck // deprecated but readable
		switch tier {
		case "essential":
			sb.WriteString("These tools are always registered. They cover the minimum viable agent workflow: cluster/topic/schema inspection, schema-aware produce/consume, search, audit, consumer groups, and basic cost/lineage queries.\n\n")
		case "advanced":
			sb.WriteString("Registered when the matching tool pack (`ops`, `analytics`, `governance`, `streaming`, `all`) is active in `server.tool_packs`.\n\n")
		case "experimental":
			sb.WriteString("Backed by simulations or stub implementations. Registered only when the `experimental` (or `all`) tool pack is active **and** the matching `experimental.*` configuration flag is set.\n\n")
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
		sb.WriteString(fmt.Sprintf("**Source:** `%s`  \n", t.SourceFile))
		sb.WriteString(fmt.Sprintf("**Tier:** `%s`", t.Tier))
		if t.Category != "" {
			sb.WriteString(fmt.Sprintf("  \n**Category:** `%s`", t.Category))
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

	return os.WriteFile(path, []byte(sb.String()), 0644)
}
