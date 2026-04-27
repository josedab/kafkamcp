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
}

var (
	toolNameRe = regexp.MustCompile(`mcp\.NewTool\("([^"]+)"`)
	descRe     = regexp.MustCompile(`mcp\.WithDescription\("([^"]+)"\)`)
	paramRe    = regexp.MustCompile(`mcp\.With(String|Number|Boolean|Object)\("([^"]+)"[^)]*mcp\.Description\("([^"]+)"\)`)
	requiredRe = regexp.MustCompile(`mcp\.Required\(\)`)
)

func main() {
	registerDir := filepath.Join("internal", "server")
	outputFile := filepath.Join("docs", "tool-reference.md")

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
		}

		if m := descRe.FindStringSubmatch(block); m != nil {
			td.Description = m[1]
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
	sb.WriteString("> Do not edit manually. Run `go generate ./internal/server/` to regenerate.\n\n")

	// Table of contents
	sb.WriteString(fmt.Sprintf("## Summary\n\n**%d tools** available.\n\n", len(tools)))
	sb.WriteString("| Tool | Description |\n|------|-------------|\n")
	for _, t := range tools {
		desc := t.Description
		if len(desc) > 80 {
			desc = desc[:77] + "..."
		}
		sb.WriteString(fmt.Sprintf("| [`%s`](#%s) | %s |\n", t.Name, strings.ReplaceAll(t.Name, "_", "_"), desc))
	}
	sb.WriteString("\n---\n\n")

	// Tool details
	sb.WriteString("## Tools\n\n")
	for _, t := range tools {
		sb.WriteString(fmt.Sprintf("### `%s`\n\n", t.Name))
		if t.Description != "" {
			sb.WriteString(t.Description + "\n\n")
		}
		sb.WriteString(fmt.Sprintf("**Source:** `%s`\n\n", t.SourceFile))

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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
