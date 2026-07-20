package schema

import (
	"fmt"
	"strings"
)

// generateFieldSummary creates a human-readable field summary from a schema.
func generateFieldSummary(schema any, schemaType string) string {
	if schemaType != "AVRO" {
		return fmt.Sprintf("(%s schema)", schemaType)
	}

	schemaMap, ok := schema.(map[string]any)
	if !ok {
		return ""
	}

	name, ok := schemaMap["name"].(string)
	if !ok {
		name = ""
	}
	fields, ok := schemaMap["fields"].([]any)
	if !ok {
		return name
	}

	var fieldDescs []string
	for _, f := range fields {
		field, ok := f.(map[string]any)
		if !ok {
			continue
		}
		fieldName, ok := field["name"].(string)
		if !ok {
			continue
		}
		fieldType := formatAvroType(field["type"], 0)

		desc := fieldName + ": " + fieldType
		if def, ok := field["default"]; ok {
			desc += fmt.Sprintf(" (default: %v)", def)
		}
		fieldDescs = append(fieldDescs, desc)
	}

	return fmt.Sprintf("%s { %s }", name, strings.Join(fieldDescs, ", "))
}

const maxAvroTypeDepth = 20

func formatAvroType(t any, depth int) string {
	if depth >= maxAvroTypeDepth {
		return "..."
	}
	switch v := t.(type) {
	case string:
		return v
	case map[string]any:
		if typ, ok := v["type"].(string); ok {
			if logicalType, ok := v["logicalType"].(string); ok {
				return logicalType
			}
			if typ == "array" {
				items := formatAvroType(v["items"], depth+1)
				return "array<" + items + ">"
			}
			if typ == "map" {
				values := formatAvroType(v["values"], depth+1)
				return "map<string, " + values + ">"
			}
			return typ
		}
		return "complex"
	case []any:
		if len(v) == 2 {
			for _, item := range v {
				if s, ok := item.(string); ok && s == "null" {
					continue
				}
				return formatAvroType(item, depth+1) + "?"
			}
		}
		types := make([]string, len(v))
		for i, item := range v {
			types[i] = formatAvroType(item, depth+1)
		}
		return "union<" + strings.Join(types, ", ") + ">"
	default:
		return fmt.Sprintf("%v", t)
	}
}
