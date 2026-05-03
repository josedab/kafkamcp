package nlquery

import (
	"fmt"
	"regexp"
	"strings"
)

// OperationalCommand represents a parsed operational Kafka command.
type OperationalCommand struct {
	Action     string         `json:"action"`     // create_topic, delete_topic, describe_topic, reset_offsets, scale_partitions, set_config, list_groups, describe_group
	Resource   string         `json:"resource"`   // topic, consumer_group, schema, cluster
	Name       string         `json:"name"`       // resource name
	Cluster    string         `json:"cluster"`    // optional cluster name
	Parameters map[string]any `json:"parameters"` // action-specific parameters
	Confidence float64        `json:"confidence"` // 0-1
	RawInput   string         `json:"raw_input"`
}

type opPattern struct {
	pattern *regexp.Regexp
	action  string
	extract func(matches []string) OperationalCommand
}

// ParseOperational parses a natural language string into an operational command.
func (e *Engine) ParseOperational(input string) (*OperationalCommand, error) {
	if input == "" {
		return nil, fmt.Errorf("input cannot be empty")
	}
	if len(input) > 2000 {
		return nil, fmt.Errorf("input too long (max 2000 characters)")
	}

	inputLower := strings.ToLower(strings.TrimSpace(input))

	patterns := buildOpPatterns()
	for _, p := range patterns {
		matches := p.pattern.FindStringSubmatch(inputLower)
		if matches != nil {
			cmd := p.extract(matches)
			cmd.Action = p.action
			cmd.RawInput = input
			if cmd.Parameters == nil {
				cmd.Parameters = make(map[string]any)
			}

			// Extract cluster from "on cluster X" or "in cluster X"
			clusterPattern := regexp.MustCompile(`(?:on|in)\s+cluster\s+(\S+)`)
			if m := clusterPattern.FindStringSubmatch(inputLower); m != nil {
				cmd.Cluster = m[1]
			}

			return &cmd, nil
		}
	}

	// Fallback: try keyword-based detection
	cmd := e.keywordDetect(inputLower, input)
	if cmd != nil {
		return cmd, nil
	}

	return nil, fmt.Errorf("could not parse operational command from: %q", input)
}

func buildOpPatterns() []opPattern {
	return []opPattern{
		// Create topic
		{
			pattern: regexp.MustCompile(`(?:create|add|make)\s+(?:a\s+)?(?:new\s+)?topic\s+(?:called\s+|named\s+)?(\S+)(?:\s+with\s+(\d+)\s+partitions?)?(?:\s+(?:and\s+)?(?:replication\s+(?:factor\s+)?(?:of\s+)?)?(\d+)\s+replicas?)?`),
			action:  "create_topic",
			extract: func(m []string) OperationalCommand {
				cmd := OperationalCommand{
					Resource:   "topic",
					Name:       m[1],
					Confidence: 0.9,
					Parameters: make(map[string]any),
				}
				if m[2] != "" {
					var n int
					fmt.Sscanf(m[2], "%d", &n)
					if n > 0 {
						cmd.Parameters["partitions"] = n
					}
				}
				if m[3] != "" {
					var n int
					fmt.Sscanf(m[3], "%d", &n)
					if n > 0 {
						cmd.Parameters["replication_factor"] = n
					}
				}
				return cmd
			},
		},
		// Delete topic
		{
			pattern: regexp.MustCompile(`(?:delete|remove|drop)\s+(?:the\s+)?topic\s+(\S+)`),
			action:  "delete_topic",
			extract: func(m []string) OperationalCommand {
				return OperationalCommand{
					Resource:   "topic",
					Name:       m[1],
					Confidence: 0.9,
				}
			},
		},
		// Describe topic
		{
			pattern: regexp.MustCompile(`(?:describe|show|info|details?)\s+(?:of\s+|about\s+|for\s+)?(?:the\s+)?topic\s+(\S+)`),
			action:  "describe_topic",
			extract: func(m []string) OperationalCommand {
				return OperationalCommand{
					Resource:   "topic",
					Name:       m[1],
					Confidence: 0.85,
				}
			},
		},
		// Scale partitions
		{
			pattern: regexp.MustCompile(`(?:scale|increase|add|set)\s+(?:the\s+)?(?:number\s+of\s+)?partitions?\s+(?:for|on|of)\s+(\S+)\s+to\s+(\d+)`),
			action:  "scale_partitions",
			extract: func(m []string) OperationalCommand {
				var n int
				fmt.Sscanf(m[2], "%d", &n)
				return OperationalCommand{
					Resource:   "topic",
					Name:       m[1],
					Confidence: 0.85,
					Parameters: map[string]any{"partitions": n},
				}
			},
		},
		// Reset offsets
		{
			pattern: regexp.MustCompile(`reset\s+(?:the\s+)?offsets?\s+(?:for|of)\s+(?:consumer\s+)?(?:group\s+)?(\S+)(?:\s+(?:on|for|to)\s+(?:topic\s+)?(\S+))?(?:\s+to\s+(earliest|latest|timestamp))?`),
			action:  "reset_offsets",
			extract: func(m []string) OperationalCommand {
				cmd := OperationalCommand{
					Resource:   "consumer_group",
					Name:       m[1],
					Confidence: 0.85,
					Parameters: make(map[string]any),
				}
				if m[2] != "" {
					cmd.Parameters["topic"] = m[2]
				}
				target := "earliest"
				if m[3] != "" {
					target = m[3]
				}
				cmd.Parameters["target"] = target
				return cmd
			},
		},
		// Describe consumer group
		{
			pattern: regexp.MustCompile(`(?:describe|show|info|details?)\s+(?:of\s+|about\s+|for\s+)?(?:the\s+)?(?:consumer\s+)?group\s+(\S+)`),
			action:  "describe_group",
			extract: func(m []string) OperationalCommand {
				return OperationalCommand{
					Resource:   "consumer_group",
					Name:       m[1],
					Confidence: 0.85,
				}
			},
		},
		// List topics
		{
			pattern: regexp.MustCompile(`(?:list|show|get)\s+(?:all\s+)?(?:the\s+)?topics`),
			action:  "list_topics",
			extract: func(m []string) OperationalCommand {
				return OperationalCommand{
					Resource:   "topic",
					Confidence: 0.9,
				}
			},
		},
		// List consumer groups
		{
			pattern: regexp.MustCompile(`(?:list|show|get)\s+(?:all\s+)?(?:the\s+)?(?:consumer\s+)?groups`),
			action:  "list_groups",
			extract: func(m []string) OperationalCommand {
				return OperationalCommand{
					Resource:   "consumer_group",
					Confidence: 0.9,
				}
			},
		},
		// Set topic config
		{
			pattern: regexp.MustCompile(`set\s+(?:the\s+)?(?:config|configuration|property)\s+(\S+)\s*(?:=|to)\s*(\S+)\s+(?:on|for)\s+(?:topic\s+)?(\S+)`),
			action:  "set_config",
			extract: func(m []string) OperationalCommand {
				return OperationalCommand{
					Resource:   "topic",
					Name:       m[3],
					Confidence: 0.8,
					Parameters: map[string]any{
						"config_key":   m[1],
						"config_value": m[2],
					},
				}
			},
		},
		// Produce message
		{
			pattern: regexp.MustCompile(`(?:send|produce|publish|write)\s+(?:a\s+)?message\s+(?:to\s+)?(?:topic\s+)?(\S+)(?:\s+(?:with\s+)?(?:key\s+)?(\S+))?`),
			action:  "produce_message",
			extract: func(m []string) OperationalCommand {
				cmd := OperationalCommand{
					Resource:   "topic",
					Name:       m[1],
					Confidence: 0.8,
					Parameters: make(map[string]any),
				}
				if m[2] != "" {
					cmd.Parameters["key"] = m[2]
				}
				return cmd
			},
		},
	}
}

func (e *Engine) keywordDetect(inputLower, rawInput string) *OperationalCommand {
	// Topic name extraction helper
	extractName := func(input, keyword string) string {
		idx := strings.Index(input, keyword)
		if idx < 0 || idx+len(keyword) >= len(input) {
			return ""
		}
		after := input[idx+len(keyword):]
		parts := strings.Fields(after)
		for _, p := range parts {
			p = strings.Trim(p, ".,;!?'\"")
			if len(p) > 1 && !isCommonWord(p) && p != "topic" && p != "group" && p != "consumer" {
				return p
			}
		}
		return ""
	}

	switch {
	case strings.Contains(inputLower, "create") && strings.Contains(inputLower, "topic"):
		name := extractName(inputLower, "topic")
		return &OperationalCommand{
			Action:     "create_topic",
			Resource:   "topic",
			Name:       name,
			Confidence: 0.6,
			RawInput:   rawInput,
			Parameters: make(map[string]any),
		}
	case strings.Contains(inputLower, "delete") && strings.Contains(inputLower, "topic"):
		name := extractName(inputLower, "topic")
		return &OperationalCommand{
			Action:     "delete_topic",
			Resource:   "topic",
			Name:       name,
			Confidence: 0.6,
			RawInput:   rawInput,
			Parameters: make(map[string]any),
		}
	case strings.Contains(inputLower, "reset") && strings.Contains(inputLower, "offset"):
		name := extractName(inputLower, "offset")
		return &OperationalCommand{
			Action:     "reset_offsets",
			Resource:   "consumer_group",
			Name:       name,
			Confidence: 0.5,
			RawInput:   rawInput,
			Parameters: map[string]any{"target": "earliest"},
		}
	case strings.Contains(inputLower, "list") && strings.Contains(inputLower, "topic"):
		return &OperationalCommand{
			Action:     "list_topics",
			Resource:   "topic",
			Confidence: 0.7,
			RawInput:   rawInput,
			Parameters: make(map[string]any),
		}
	}

	return nil
}
