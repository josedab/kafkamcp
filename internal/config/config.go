package config

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

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
			slog.Warn("config file has permissive permissions", "path", path, "mode", fmt.Sprintf("%04o", mode))
		}
	}

	expanded, err := expandEnvVars(data)
	if err != nil {
		return nil, fmt.Errorf("expanding environment variables: %w", err)
	}

	var cfg Config
	decoder := yaml.NewDecoder(bytes.NewReader(expanded))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	cfg.applyDefaults()
	return &cfg, nil
}

func (c *Config) validate() error {
	if err := c.validateClusters(); err != nil {
		return err
	}
	if err := c.validateTransport(); err != nil {
		return err
	}
	if err := c.validateToolPacksAndApproval(); err != nil {
		return err
	}
	if err := c.validatePersistence(); err != nil {
		return err
	}
	if err := c.validateIdentity(); err != nil {
		return err
	}

	if err := c.TransportSecurityError(); err != nil {
		return err
	}
	return nil
}

func (c *Config) validateIdentity() error {
	switch strings.ToLower(strings.TrimSpace(c.Policies.Identity.Provider)) {
	case "", "none", "disabled":
		// no-op
	case "static":
		if len(c.Policies.Identity.Static.Tokens) == 0 {
			return fmt.Errorf("policies.identity.static.tokens is required when provider=static")
		}
		for i, t := range c.Policies.Identity.Static.Tokens {
			if strings.TrimSpace(t.Token) == "" {
				return fmt.Errorf("policies.identity.static.tokens[%d].token must not be empty", i)
			}
			if strings.TrimSpace(t.AgentID) == "" {
				return fmt.Errorf("policies.identity.static.tokens[%d].agent_id must not be empty", i)
			}
		}
	case "oidc":
		if strings.TrimSpace(c.Policies.Identity.OIDC.IssuerURL) == "" {
			return fmt.Errorf("policies.identity.oidc.issuer_url is required when provider=oidc")
		}
		if strings.TrimSpace(c.Policies.Identity.OIDC.ClientID) == "" {
			return fmt.Errorf("policies.identity.oidc.client_id is required when provider=oidc")
		}
	case "mtls":
		// mtls validation is intentionally lenient: a deployment can
		// turn on the provider before populating CABundle (it will
		// then trust the system pool), and AllowedSPIFFEIDs /
		// AllowedTrustDomains are optional. The TLS transport layer
		// will refuse to start without the server.tls_cert/key pair,
		// so a misconfigured deployment fails at server start, not
		// silently at request time.
	default:
		return fmt.Errorf("invalid policies.identity.provider %q; must be one of: none, static, oidc, mtls", c.Policies.Identity.Provider)
	}
	return nil
}

func (c *Config) validatePersistence() error {
	backend := strings.ToLower(strings.TrimSpace(c.Server.Persistence.Backend))
	if backend == "" {
		return nil
	}
	validBackends := map[string]bool{"inmem": true, "memory": true, "in-memory": true, "sqlite": true, "sqlite3": true}
	if !validBackends[backend] {
		return fmt.Errorf("invalid server.persistence.backend %q; must be one of: inmem, sqlite", c.Server.Persistence.Backend)
	}
	if (backend == "sqlite" || backend == "sqlite3") && strings.TrimSpace(c.Server.Persistence.Path) == "" {
		return fmt.Errorf("server.persistence.path is required when server.persistence.backend=%q", c.Server.Persistence.Backend)
	}
	return nil
}

func (c *Config) validateToolPacksAndApproval() error {
	if len(c.Server.ToolPacks) > 0 {
		for i, p := range c.Server.ToolPacks {
			norm := strings.ToLower(strings.TrimSpace(p))
			if !ValidToolPacks[norm] {
				return fmt.Errorf("server.tool_packs[%d] %q is invalid; must be one of: core-read, ops-write, governance, experimental (deprecated: essential, ops)", i, p)
			}
		}
	}

	guardedPacksEnabled := HasToolPack(c.Server.ToolPacks, PackOpsWrite) ||
		HasToolPack(c.Server.ToolPacks, PackGovernance)
	if guardedPacksEnabled {
		if c.Policies.Approval.Enabled != nil && !*c.Policies.Approval.Enabled {
			return fmt.Errorf("policies.approval.enabled cannot be false while a guarded tool pack is enabled")
		}
		if !c.NetworkTransport() {
			return fmt.Errorf("ops-write and governance require sse or streamable-http so callers can use cryptographically verified identities; stdio is core-read only")
		}
		if err := c.TransportSecurityError(); err != nil {
			return fmt.Errorf("guarded tool packs require a secure network posture: %w", err)
		}
		keyEnv := c.Policies.Approval.EffectiveSigningKeyEnv()
		key := os.Getenv(keyEnv)
		if len(key) < 32 {
			return fmt.Errorf("%s must contain at least 32 bytes when ops-write or governance is enabled so approval decisions are tamper-evident", keyEnv)
		}
		for i, previousEnv := range c.Policies.Approval.PreviousSigningKeyEnvs {
			previousEnv = strings.TrimSpace(previousEnv)
			if previousEnv == "" {
				return fmt.Errorf("policies.approval.previous_signing_key_envs[%d] must not be empty", i)
			}
			if len(os.Getenv(previousEnv)) < 32 {
				return fmt.Errorf("%s must contain at least 32 bytes while configured as a previous approval signing key", previousEnv)
			}
		}
	}
	if c.Policies.Approval.TTLSeconds < 0 {
		return fmt.Errorf("policies.approval.ttl_seconds must be non-negative")
	}
	if c.Policies.Approval.MaxTTLSeconds < 0 || c.Policies.Approval.MaxTTLSeconds > 3600 {
		return fmt.Errorf("policies.approval.max_ttl_seconds must be between 0 and 3600")
	}
	if retention := c.Policies.Approval.TerminalRetentionSeconds; retention < 0 || retention > 2592000 {
		return fmt.Errorf("policies.approval.terminal_retention_seconds must be between 0 and 2592000")
	}
	return nil
}

func (c *Config) validateTransport() error {
	transport := c.Server.Transport
	if transport == "" {
		transport = "stdio"
	}
	validTransports := map[string]bool{"stdio": true, "sse": true, "streamable-http": true}
	if !validTransports[transport] {
		return fmt.Errorf("invalid transport %q; must be one of: stdio, sse, streamable-http", transport)
	}
	serverPort := c.Server.Port
	if serverPort == 0 {
		serverPort = 3100
	}
	if serverPort < 1 || serverPort > 65535 {
		return fmt.Errorf("server.port must be between 1 and 65535")
	}
	metricsPort := c.Server.MetricsPort
	if metricsPort == 0 {
		if serverPort == 65535 {
			return fmt.Errorf("server.metrics_port is required when server.port is 65535")
		}
		metricsPort = serverPort + 1
	}
	if metricsPort < 1 || metricsPort > 65535 {
		return fmt.Errorf("server.metrics_port must be between 1 and 65535")
	}
	if c.NetworkTransport() && metricsPort == serverPort &&
		effectiveHost(c.Server.MetricsHost, "127.0.0.1") == effectiveHost(c.Server.Host, "0.0.0.0") {
		return fmt.Errorf("server.metrics_port must differ from server.port when both listeners use the same host")
	}
	return nil
}

func (c *Config) validateClusters() error {
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
		protocol := strings.ToUpper(strings.TrimSpace(cluster.SecurityProtocol))
		if protocol == "" {
			protocol = "PLAINTEXT"
		}
		switch protocol {
		case "PLAINTEXT", "SSL", "SASL_SSL", "SASL_PLAINTEXT":
		default:
			return fmt.Errorf("cluster[%d] (%s): invalid security_protocol %q; must be one of: PLAINTEXT, SSL, SASL_SSL, SASL_PLAINTEXT", i, cluster.Name, cluster.SecurityProtocol)
		}
		saslMechanism := strings.ToUpper(strings.TrimSpace(cluster.SASLMechanism))
		// SASL consistency: if SASL mechanism is set, username and password are required
		if saslMechanism != "" && (cluster.SASLUsername == "" || cluster.SASLPassword == "") {
			return fmt.Errorf("cluster[%d] (%s): sasl_username and sasl_password are required when sasl_mechanism is set", i, cluster.Name)
		}
		if strings.HasPrefix(protocol, "SASL_") && saslMechanism == "" {
			return fmt.Errorf("cluster[%d] (%s): sasl_mechanism is required when security_protocol=%s", i, cluster.Name, protocol)
		}
		if saslMechanism != "" && !strings.HasPrefix(protocol, "SASL_") {
			return fmt.Errorf("cluster[%d] (%s): security_protocol must be SASL_SSL or SASL_PLAINTEXT when sasl_mechanism is set", i, cluster.Name)
		}
		if err := validateTLSVersion(cluster.TLSMinVersion); err != nil {
			return fmt.Errorf("cluster[%d] (%s): tls_min_version: %w", i, cluster.Name, err)
		}
		if err := validateTLSVersion(cluster.ConnectTLS.MinVersion); err != nil {
			return fmt.Errorf("cluster[%d] (%s): connect_tls.min_version: %w", i, cluster.Name, err)
		}
		// InsecureSkipVerify requires explicit env gate
		if cluster.ConnectTLS.InsecureSkipVerify && os.Getenv("KAFKAMCP_ALLOW_INSECURE") != "true" {
			return fmt.Errorf("cluster[%d] (%s): insecure_skip_verify requires KAFKAMCP_ALLOW_INSECURE=true environment variable", i, cluster.Name)
		}
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
	if c.Server.MetricsHost == "" {
		c.Server.MetricsHost = "127.0.0.1"
	}
	if c.Server.MetricsPort == 0 {
		c.Server.MetricsPort = c.Server.Port + 1
	}
	if c.Server.MaxMessageSize == 0 {
		c.Server.MaxMessageSize = 1048576 // 1MB default
	}
	if len(c.Server.ToolPacks) == 0 {
		// Default to the curated read-only core (<=15 tools). Mutating and
		// experimental surfaces must be opted into explicitly.
		c.Server.ToolPacks = []string{DefaultToolPack}
	}
	if c.Server.Persistence.Backend == "" {
		// Preserve the historical behavior: in-memory state, lost on
		// restart. Operators opt into durability by setting
		// server.persistence.backend: sqlite.
		c.Server.Persistence.Backend = "inmem"
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
	for i := range c.Clusters {
		if c.Clusters[i].SecurityProtocol == "" {
			c.Clusters[i].SecurityProtocol = "PLAINTEXT"
		}
		if c.Clusters[i].TLSMinVersion == "" {
			c.Clusters[i].TLSMinVersion = "1.2"
		}
		if c.Clusters[i].ConnectTLS.MinVersion == "" {
			c.Clusters[i].ConnectTLS.MinVersion = c.Clusters[i].TLSMinVersion
		}
	}
	if !hasDefault && len(c.Clusters) > 0 {
		c.Clusters[0].Default = true
	}
}

func validateTLSVersion(version string) error {
	switch strings.TrimSpace(version) {
	case "", "1.2", "1.3":
		return nil
	default:
		return fmt.Errorf("must be one of: 1.2, 1.3")
	}
}

func effectiveHost(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
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

// UnsafeNetworkOverrideEnv is the environment variable that, when set to a
// truthy value, disables the fail-closed network-transport security guard. It
// mirrors ServerConfig.DangerouslyAllowUnauthenticatedNetwork and exists ONLY
// for local development.
const UnsafeNetworkOverrideEnv = "KAFKAMCP_UNSAFE_ALLOW_UNAUTHENTICATED_NETWORK"

// NetworkTransport reports whether the configured transport exposes a network
// listener (anything other than stdio).
func (c *Config) NetworkTransport() bool {
	t := strings.ToLower(strings.TrimSpace(c.Server.Transport))
	return t != "" && t != "stdio"
}

// UnsafeNetworkOverrideEnabled reports whether the unsafe development override
// that disables the network-transport security guard is active, via either the
// config field or the KAFKAMCP_UNSAFE_ALLOW_UNAUTHENTICATED_NETWORK env var.
func (c *Config) UnsafeNetworkOverrideEnabled() bool {
	if c.Server.DangerouslyAllowUnauthenticatedNetwork {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(os.Getenv(UnsafeNetworkOverrideEnv))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// TransportSecurityError enforces the fail-closed posture for network
// transports. It returns a non-nil error when a network transport (sse,
// streamable-http) is configured without BOTH a cryptographic identity provider
// and default-deny authorization, unless the unsafe development override is
// enabled. stdio transports always return nil so local use stays frictionless.
func (c *Config) TransportSecurityError() error {
	if !c.NetworkTransport() {
		return nil
	}
	if c.UnsafeNetworkOverrideEnabled() {
		return nil
	}
	var missing []string
	if !c.Policies.Identity.IsCryptographic() {
		missing = append(missing, fmt.Sprintf(
			"policies.identity.provider must be one of static, oidc, mtls (got %q)",
			c.Policies.Identity.Provider))
	}
	if c.Policies.Identity.HeaderFallbackEnabled() {
		missing = append(missing, "policies.identity.allow_header_fallback must be false")
	}
	if !c.Policies.DefaultDeny {
		missing = append(missing, "policies.default_deny must be true")
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf(
		"refusing to start %q network transport without a secure posture: %s. "+
			"Configure a cryptographic identity provider and default-deny authorization, "+
			"or set server.dangerously_allow_unauthenticated_network: true "+
			"(env %s=true) for LOCAL DEVELOPMENT ONLY",
		c.Server.Transport, strings.Join(missing, "; "), UnsafeNetworkOverrideEnv)
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
