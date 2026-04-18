package persona

import (
	"fmt"
	"sync"
)

// Persona defines a declarative agent persona with tool access and defaults.
type Persona struct {
	Name           string
	DisplayName    string
	Description    string
	Role           string
	AllowedTools   []string
	DeniedTools    []string
	DefaultCluster string
	SystemPrompt   string
	Defaults       map[string]any
	Tags           []string
}

// PersonaConfig holds the active persona selection and all registered personas.
type PersonaConfig struct {
	Active   string
	Personas map[string]Persona
}

// Engine manages persona registration, lookup, and tool-access checks.
type Engine struct {
	mu       sync.RWMutex
	personas map[string]*Persona
	active   string
}

// NewEngine creates an Engine pre-loaded with built-in personas.
func NewEngine() *Engine {
	e := &Engine{
		personas: make(map[string]*Persona),
	}
	e.RegisterBuiltins()
	return e
}

// RegisterBuiltins registers the five built-in personas.
func (e *Engine) RegisterBuiltins() {
	builtins := []Persona{
		{
			Name:        "sre",
			DisplayName: "SRE / DevOps",
			Description: "Site-reliability and operations persona focused on cluster health, monitoring, and alerting.",
			Role:        "sre",
			AllowedTools: []string{
				"cluster_status", "broker_status", "consumer_group_health",
				"lag_analysis", "alerts", "metrics", "healthcheck",
				"partition_status", "rebalance_status",
			},
			SystemPrompt: "You are an SRE assistant. Focus on monitoring, alerting, consumer group health, and lag analysis. Prioritize cluster stability and uptime.",
			Defaults: map[string]any{
				"output_format": "table",
				"lag_threshold": 1000,
			},
			Tags: []string{"operations", "monitoring", "reliability"},
		},
		{
			Name:        "data-engineer",
			DisplayName: "Data Engineer",
			Description: "Data engineering persona for topic management, schema design, data quality, and aggregation.",
			Role:        "data-engineer",
			AllowedTools: []string{
				"topics", "schemas", "data_quality", "aggregation",
				"schema_registry", "topic_config", "partitions",
				"consume", "produce",
			},
			SystemPrompt: "You are a data engineering assistant. Focus on topics, schemas, data quality, and aggregation pipelines. Ensure data integrity and efficient partitioning.",
			Defaults: map[string]any{
				"output_format":   "json",
				"schema_validate": true,
			},
			Tags: []string{"data", "schemas", "pipelines"},
		},
		{
			Name:        "compliance",
			DisplayName: "Compliance Officer",
			Description: "Compliance persona for audit trails, PII masking, and schema governance.",
			Role:        "compliance",
			AllowedTools: []string{
				"audit", "pii_masking", "schema_governance",
				"access_logs", "data_classification", "retention_policies",
			},
			DeniedTools: []string{
				"produce", "topic_delete", "acl_delete",
			},
			SystemPrompt: "You are a compliance assistant. Focus on audit trails, PII masking, schema governance, and data classification. Never allow destructive operations.",
			Defaults: map[string]any{
				"mask_pii":      true,
				"audit_logging": true,
			},
			Tags: []string{"compliance", "audit", "governance"},
		},
		{
			Name:        "developer",
			DisplayName: "Developer",
			Description: "Developer persona for consuming, producing, playground experimentation, and search.",
			Role:        "developer",
			AllowedTools: []string{
				"consume", "produce", "playground", "search",
				"topic_describe", "schema_describe", "consumer_group_describe",
			},
			DeniedTools: []string{
				"topic_delete", "acl_create", "acl_delete",
				"cluster_config", "broker_config",
			},
			SystemPrompt: "You are a developer assistant. Help with consuming and producing messages, playground experimentation, and searching topics. Avoid administrative operations.",
			Defaults: map[string]any{
				"output_format": "json",
				"max_messages":  100,
			},
			Tags: []string{"development", "testing", "integration"},
		},
		{
			Name:        "admin",
			DisplayName: "Administrator",
			Description: "Full-access administrator persona with all tools including create, delete, and ACL management.",
			Role:        "admin",
			AllowedTools: []string{
				"*",
			},
			SystemPrompt: "You are a Kafka administrator with full access. You can create and delete topics, manage ACLs, configure brokers, and perform all administrative operations. Exercise caution with destructive actions.",
			Defaults: map[string]any{
				"output_format":  "table",
				"confirm_delete": true,
			},
			Tags: []string{"admin", "management", "full-access"},
		},
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range builtins {
		p := builtins[i]
		e.personas[p.Name] = &p
	}
	if e.active == "" {
		e.active = "developer"
	}
}

// GetPersona returns the persona with the given name or an error if not found.
func (e *Engine) GetPersona(name string) (*Persona, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	p, ok := e.personas[name]
	if !ok {
		return nil, fmt.Errorf("persona %q not found", name)
	}
	return p, nil
}

// ListPersonas returns a slice of all registered personas.
func (e *Engine) ListPersonas() []Persona {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]Persona, 0, len(e.personas))
	for _, p := range e.personas {
		out = append(out, *p)
	}
	return out
}

// SetActive sets the active persona by name.
func (e *Engine) SetActive(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.personas[name]; !ok {
		return fmt.Errorf("persona %q not found", name)
	}
	e.active = name
	return nil
}

// GetActive returns the currently active persona.
func (e *Engine) GetActive() *Persona {
	e.mu.RLock()
	defer e.mu.RUnlock()
	p := e.personas[e.active]
	return p
}

// IsToolAllowed checks whether the given tool is permitted for the persona.
func (e *Engine) IsToolAllowed(personaName, toolName string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	p, ok := e.personas[personaName]
	if !ok {
		return false
	}

	// Check denied list first.
	for _, d := range p.DeniedTools {
		if d == toolName {
			return false
		}
	}

	// Check allowed list.
	for _, a := range p.AllowedTools {
		if a == "*" || a == toolName {
			return true
		}
	}
	return false
}

// ApplyDefaults merges persona defaults into args. Existing keys in args take precedence.
func (e *Engine) ApplyDefaults(personaName string, args map[string]any) map[string]any {
	e.mu.RLock()
	defer e.mu.RUnlock()
	p, ok := e.personas[personaName]
	if !ok {
		return args
	}

	merged := make(map[string]any, len(p.Defaults)+len(args))
	for k, v := range p.Defaults {
		merged[k] = v
	}
	for k, v := range args {
		merged[k] = v
	}
	return merged
}
