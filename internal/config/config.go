package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the top-level KafkaMCP configuration.
type Config struct {
	Server      ServerConfig   `yaml:"server" json:"server"`
	Clusters    []ClusterConfig `yaml:"clusters" json:"clusters"`
	Policies    PolicyConfig   `yaml:"policies" json:"policies"`
	SchemaCache CacheConfig    `yaml:"schema_cache" json:"schema_cache"`
	Audit       AuditConfig    `yaml:"audit" json:"audit"`
	Masking     MaskingConfig  `yaml:"masking,omitempty" json:"masking,omitempty"`
}

// ServerConfig holds network transport and TLS settings for the MCP server.
type ServerConfig struct {
	Transport      string `yaml:"transport" json:"transport"`             // Protocol: "stdio", "sse", or "streamable-http"
	Host           string `yaml:"host" json:"host"`                       // Bind address (default "0.0.0.0")
	Port           int    `yaml:"port" json:"port"`                       // Listen port (default 3100)
	LogLevel       string `yaml:"log_level" json:"log_level"`             // Log verbosity: "debug", "info", "warn", "error"
	MaxMessageSize int    `yaml:"max_message_size" json:"max_message_size"` // Max MCP message size in bytes (default 1 MB)
	TLSCert        string `yaml:"tls_cert,omitempty" json:"tls_cert,omitempty"` // Path to TLS certificate file
	TLSKey         string `yaml:"tls_key,omitempty" json:"tls_key,omitempty"`   // Path to TLS private key file
}

// Address returns the "host:port" listen address, using defaults if not set.
func (s ServerConfig) Address() string {
	host := s.Host
	if host == "" {
		host = "0.0.0.0"
	}
	port := s.Port
	if port == 0 {
		port = 3100
	}
	return fmt.Sprintf("%s:%d", host, port)
}

// ClusterConfig describes a single Kafka cluster connection including broker
// addresses, authentication, Schema Registry, and Kafka Connect settings.
type ClusterConfig struct {
	Name             string               `yaml:"name" json:"name"`                                         // Unique cluster identifier
	BootstrapServers string               `yaml:"bootstrap_servers" json:"bootstrap_servers"`                // Comma-separated broker addresses
	SecurityProtocol string               `yaml:"security_protocol" json:"security_protocol"`                // e.g. "PLAINTEXT", "SSL", "SASL_SSL"
	SASLMechanism    string               `yaml:"sasl_mechanism,omitempty" json:"sasl_mechanism,omitempty"`   // e.g. "PLAIN", "SCRAM-SHA-256"
	SASLUsername     string               `yaml:"sasl_username,omitempty" json:"sasl_username,omitempty"`     // SASL username (required when mechanism is set)
	SASLPassword     string               `yaml:"sasl_password,omitempty" json:"sasl_password,omitempty"`     // SASL password (required when mechanism is set)
	SSLCALocation    string               `yaml:"ssl_ca_location,omitempty" json:"ssl_ca_location,omitempty"` // Path to CA certificate bundle
	SchemaRegistry   SchemaRegistryConfig `yaml:"schema_registry,omitempty" json:"schema_registry,omitempty"` // Optional Schema Registry connection
	ConnectURL       string               `yaml:"connect_url,omitempty" json:"connect_url,omitempty"`         // Kafka Connect REST API base URL
	ConnectTLS       ConnectTLSConfig     `yaml:"connect_tls,omitempty" json:"connect_tls,omitempty"`         // TLS settings for Kafka Connect
	Default          bool                 `yaml:"default,omitempty" json:"default,omitempty"`                 // Mark as the default cluster
}

// SchemaRegistryConfig holds the URL and optional basic-auth credentials for a
// Confluent-compatible Schema Registry.
type SchemaRegistryConfig struct {
	URL  string    `yaml:"url" json:"url"`
	Auth BasicAuth `yaml:"auth,omitempty" json:"auth,omitempty"`
}

// ConnectTLSConfig holds TLS settings for the Kafka Connect REST API.
type ConnectTLSConfig struct {
	CAFile             string `yaml:"ca_file,omitempty" json:"ca_file,omitempty"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify,omitempty" json:"insecure_skip_verify,omitempty"`
}

// BasicAuth holds username/password credentials for HTTP basic authentication.
type BasicAuth struct {
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
}

// CacheConfig controls the in-memory schema cache size and TTL.
type CacheConfig struct {
	MaxEntries int `yaml:"max_entries" json:"max_entries"` // Maximum cached schemas (default 10000)
	TTLSeconds int `yaml:"ttl_seconds" json:"ttl_seconds"` // Entry time-to-live in seconds (default 300)
}

// PolicyConfig defines the access-control policy applied to MCP agents.
type PolicyConfig struct {
	DefaultDeny bool          `yaml:"default_deny" json:"default_deny"` // When true, deny access unless explicitly allowed
	Agents      []AgentPolicy `yaml:"agents" json:"agents"`             // Per-agent permission definitions
}

// AgentPolicy specifies the permissions and rate limits for a single MCP agent.
type AgentPolicy struct {
	ID             string              `yaml:"id" json:"id"`
	Role           string              `yaml:"role" json:"role"`
	Topics         []TopicPermission   `yaml:"topics" json:"topics"`
	ConsumerGroups GroupPermission      `yaml:"consumer_groups" json:"consumer_groups"`
	Schemas        SchemaPermission    `yaml:"schemas" json:"schemas"`
	RateLimit      RateLimitConfig     `yaml:"rate_limit" json:"rate_limit"`
}

// TopicPermission maps a glob pattern to the set of allowed operations on
// matching topics (e.g. "read", "write", "create", "delete").
type TopicPermission struct {
	Pattern     string   `yaml:"pattern" json:"pattern"`
	Permissions []string `yaml:"permissions" json:"permissions"`
}

// GroupPermission lists the allowed operations on consumer groups.
type GroupPermission struct {
	Permissions []string `yaml:"permissions" json:"permissions"`
}

// SchemaPermission lists the allowed operations on Schema Registry subjects.
type SchemaPermission struct {
	Permissions []string `yaml:"permissions" json:"permissions"`
}

// RateLimitConfig controls the maximum request rate for an agent.
type RateLimitConfig struct {
	RequestsPerMinute int `yaml:"requests_per_minute" json:"requests_per_minute"` // Token-bucket refill rate
}

// AuditConfig controls the MCP audit log (file path, buffer size, and toggle).
type AuditConfig struct {
	Enabled    bool   `yaml:"enabled" json:"enabled"`         // Enable audit logging
	MaxEntries int    `yaml:"max_entries" json:"max_entries"` // Max in-memory audit entries (default 10000)
	LogFile    string `yaml:"log_file" json:"log_file"`       // Path to audit log file
}

// MaskingConfig defines data masking and PII redaction rules.
type MaskingConfig struct {
	Enabled bool          `yaml:"enabled" json:"enabled"`
	Rules   []MaskingRule `yaml:"rules" json:"rules"`
}

// MaskingRule defines a single masking rule for a field pattern.
type MaskingRule struct {
	Name      string `yaml:"name" json:"name"`           // Rule identifier
	Type      string `yaml:"type" json:"type"`           // Rule type: email, ssn, credit_card, phone, regex, ner
	FieldPath string `yaml:"field_path" json:"field_path"` // JSON path pattern (e.g., "$.user.email", "**email**")
	Pattern   string `yaml:"pattern,omitempty" json:"pattern,omitempty"` // Custom regex pattern (for type=regex)
	Replace   string `yaml:"replace,omitempty" json:"replace,omitempty"` // Replacement string. Default: "***REDACTED***"
	Agents    []string `yaml:"agents,omitempty" json:"agents,omitempty"` // Apply only to these agents. Empty = all.
}

var envVarPattern = regexp.MustCompile(`\$\{([^}]+)\}`)

// expandEnvVars replaces ${VAR} patterns with environment variable values.
// Returns an error if a referenced variable is not set.
// Skips variables found within YAML comment lines (lines starting with #).
func expandEnvVars(data []byte) ([]byte, error) {
	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}

		matches := envVarPattern.FindAllStringSubmatch(line, -1)
		for _, match := range matches {
			fullMatch := match[0]
			varName := match[1]

			parts := strings.SplitN(varName, ":-", 2)
			envName := parts[0]

			value, exists := os.LookupEnv(envName)
			if !exists {
				if len(parts) == 2 {
					value = parts[1]
				} else {
					return nil, fmt.Errorf("required environment variable %q is not set", envName)
				}
			}
			lines[i] = strings.ReplaceAll(lines[i], fullMatch, value)
		}
	}

	return []byte(strings.Join(lines, "\n")), nil
}

// Load reads and parses a KafkaMCP config file with env var expansion.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	// Warn if config file is world-readable (may contain credentials)
	if info, err := os.Stat(path); err == nil {
		mode := info.Mode().Perm()
		if mode&0044 != 0 {
			fmt.Fprintf(os.Stderr, "WARNING: config file %q has permissive permissions (%04o); consider chmod 600 if it contains credentials\n", path, mode)
		}
	}

	expanded, err := expandEnvVars(data)
	if err != nil {
		return nil, fmt.Errorf("expanding environment variables: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(expanded, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	cfg.applyDefaults()
	return &cfg, nil
}

func (c *Config) validate() error {
	if len(c.Clusters) == 0 {
		return fmt.Errorf("at least one cluster must be configured")
	}

	seen := make(map[string]bool)
	for i, cluster := range c.Clusters {
		if cluster.Name == "" {
			return fmt.Errorf("cluster[%d]: name is required", i)
		}
		if seen[cluster.Name] {
			return fmt.Errorf("cluster[%d]: duplicate cluster name %q", i, cluster.Name)
		}
		seen[cluster.Name] = true
		if cluster.BootstrapServers == "" {
			return fmt.Errorf("cluster[%d] (%s): bootstrap_servers is required", i, cluster.Name)
		}
		// SASL consistency: if SASL mechanism is set, username and password are required
		if cluster.SASLMechanism != "" && (cluster.SASLUsername == "" || cluster.SASLPassword == "") {
			return fmt.Errorf("cluster[%d] (%s): sasl_username and sasl_password are required when sasl_mechanism is set", i, cluster.Name)
		}
	}

	transport := c.Server.Transport
	if transport == "" {
		transport = "stdio"
	}
	validTransports := map[string]bool{"stdio": true, "sse": true, "streamable-http": true}
	if !validTransports[transport] {
		return fmt.Errorf("invalid transport %q; must be one of: stdio, sse, streamable-http", transport)
	}

	return nil
}

func (c *Config) applyDefaults() {
	if c.Server.Transport == "" {
		c.Server.Transport = "stdio"
	}
	if c.Server.LogLevel == "" {
		c.Server.LogLevel = "info"
	}
	if c.Server.Port == 0 {
		c.Server.Port = 3100
	}
	if c.Server.MaxMessageSize == 0 {
		c.Server.MaxMessageSize = 1048576 // 1MB default
	}
	if c.SchemaCache.MaxEntries == 0 {
		c.SchemaCache.MaxEntries = 10000
	}
	if c.SchemaCache.TTLSeconds == 0 {
		c.SchemaCache.TTLSeconds = 300
	}
	if c.Audit.MaxEntries == 0 {
		c.Audit.MaxEntries = 10000
	}

	// Ensure at least one cluster is marked as default
	hasDefault := false
	for _, cl := range c.Clusters {
		if cl.Default {
			hasDefault = true
			break
		}
	}
	if !hasDefault && len(c.Clusters) > 0 {
		c.Clusters[0].Default = true
	}
}

// DefaultCluster returns the default cluster configuration.
func (c *Config) DefaultCluster() *ClusterConfig {
	for i := range c.Clusters {
		if c.Clusters[i].Default {
			return &c.Clusters[i]
		}
	}
	if len(c.Clusters) > 0 {
		return &c.Clusters[0]
	}
	return nil
}

// FindCluster returns the cluster configuration by name, or the default if name is empty.
func (c *Config) FindCluster(name string) (*ClusterConfig, error) {
	if name == "" {
		cl := c.DefaultCluster()
		if cl == nil {
			return nil, fmt.Errorf("no default cluster configured")
		}
		return cl, nil
	}
	for i := range c.Clusters {
		if c.Clusters[i].Name == name {
			return &c.Clusters[i], nil
		}
	}
	return nil, fmt.Errorf("cluster %q not found", name)
}
