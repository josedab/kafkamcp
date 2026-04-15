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
}

type ServerConfig struct {
	Transport      string `yaml:"transport" json:"transport"`
	Host           string `yaml:"host" json:"host"`
	Port           int    `yaml:"port" json:"port"`
	LogLevel       string `yaml:"log_level" json:"log_level"`
	MaxMessageSize int    `yaml:"max_message_size" json:"max_message_size"`
	TLSCert        string `yaml:"tls_cert,omitempty" json:"tls_cert,omitempty"`
	TLSKey         string `yaml:"tls_key,omitempty" json:"tls_key,omitempty"`
}

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

type ClusterConfig struct {
	Name             string               `yaml:"name" json:"name"`
	BootstrapServers string               `yaml:"bootstrap_servers" json:"bootstrap_servers"`
	SecurityProtocol string               `yaml:"security_protocol" json:"security_protocol"`
	SASLMechanism    string               `yaml:"sasl_mechanism,omitempty" json:"sasl_mechanism,omitempty"`
	SASLUsername     string               `yaml:"sasl_username,omitempty" json:"sasl_username,omitempty"`
	SASLPassword     string               `yaml:"sasl_password,omitempty" json:"sasl_password,omitempty"`
	SSLCALocation    string               `yaml:"ssl_ca_location,omitempty" json:"ssl_ca_location,omitempty"`
	SchemaRegistry   SchemaRegistryConfig `yaml:"schema_registry,omitempty" json:"schema_registry,omitempty"`
	ConnectURL       string               `yaml:"connect_url,omitempty" json:"connect_url,omitempty"`
	ConnectTLS       ConnectTLSConfig     `yaml:"connect_tls,omitempty" json:"connect_tls,omitempty"`
	Default          bool                 `yaml:"default,omitempty" json:"default,omitempty"`
}

type SchemaRegistryConfig struct {
	URL  string    `yaml:"url" json:"url"`
	Auth BasicAuth `yaml:"auth,omitempty" json:"auth,omitempty"`
}

// ConnectTLSConfig holds TLS settings for the Kafka Connect REST API.
type ConnectTLSConfig struct {
	CAFile             string `yaml:"ca_file,omitempty" json:"ca_file,omitempty"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify,omitempty" json:"insecure_skip_verify,omitempty"`
}

type BasicAuth struct {
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
}

type CacheConfig struct {
	MaxEntries int `yaml:"max_entries" json:"max_entries"`
	TTLSeconds int `yaml:"ttl_seconds" json:"ttl_seconds"`
}

type PolicyConfig struct {
	DefaultDeny bool          `yaml:"default_deny" json:"default_deny"`
	Agents      []AgentPolicy `yaml:"agents" json:"agents"`
}

type AgentPolicy struct {
	ID             string              `yaml:"id" json:"id"`
	Role           string              `yaml:"role" json:"role"`
	Topics         []TopicPermission   `yaml:"topics" json:"topics"`
	ConsumerGroups GroupPermission      `yaml:"consumer_groups" json:"consumer_groups"`
	Schemas        SchemaPermission    `yaml:"schemas" json:"schemas"`
	RateLimit      RateLimitConfig     `yaml:"rate_limit" json:"rate_limit"`
}

type TopicPermission struct {
	Pattern     string   `yaml:"pattern" json:"pattern"`
	Permissions []string `yaml:"permissions" json:"permissions"`
}

type GroupPermission struct {
	Permissions []string `yaml:"permissions" json:"permissions"`
}

type SchemaPermission struct {
	Permissions []string `yaml:"permissions" json:"permissions"`
}

type RateLimitConfig struct {
	RequestsPerMinute int `yaml:"requests_per_minute" json:"requests_per_minute"`
}

type AuditConfig struct {
	Enabled    bool   `yaml:"enabled" json:"enabled"`
	MaxEntries int    `yaml:"max_entries" json:"max_entries"`
	LogFile    string `yaml:"log_file" json:"log_file"`
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
