package config

import (
	"bytes"
	"fmt"
	"log/slog"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the top-level KafkaMCP configuration.
type Config struct {
	Server      ServerConfig    `yaml:"server" json:"server"`
	Clusters    []ClusterConfig `yaml:"clusters" json:"clusters"`
	Policies    PolicyConfig    `yaml:"policies" json:"policies"`
	SchemaCache CacheConfig     `yaml:"schema_cache" json:"schema_cache"`
	Audit       AuditConfig     `yaml:"audit" json:"audit"`
	Masking     MaskingConfig   `yaml:"masking,omitempty" json:"masking,omitempty"`
	Telemetry   TelemetryConfig `yaml:"telemetry,omitempty" json:"telemetry,omitempty"`
}

// ServerConfig holds network transport and TLS settings for the MCP server.
type ServerConfig struct {
	Transport      string            `yaml:"transport" json:"transport"`                           // Protocol: "stdio", "sse", or "streamable-http"
	Host           string            `yaml:"host" json:"host"`                                     // Bind address (default "0.0.0.0")
	Port           int               `yaml:"port" json:"port"`                                     // Listen port (default 3100)
	MetricsHost    string            `yaml:"metrics_host,omitempty" json:"metrics_host,omitempty"` // Metrics/health bind address (default "127.0.0.1")
	MetricsPort    int               `yaml:"metrics_port,omitempty" json:"metrics_port,omitempty"` // Metrics/health listen port (default server.port + 1)
	LogLevel       string            `yaml:"log_level" json:"log_level"`                           // Log verbosity: "debug", "info", "warn", "error"
	MaxMessageSize int               `yaml:"max_message_size" json:"max_message_size"`             // Max MCP message size in bytes (default 1 MB)
	TLSCert        string            `yaml:"tls_cert,omitempty" json:"tls_cert,omitempty"`         // Path to TLS certificate file
	TLSKey         string            `yaml:"tls_key,omitempty" json:"tls_key,omitempty"`           // Path to TLS private key file
	ToolPacks      []string          `yaml:"tool_packs,omitempty" json:"tool_packs,omitempty"`     // Curated surface: core-read, ops-write, governance, experimental (deprecated aliases: essential, ops). Default: ["core-read"].
	Persistence    PersistenceConfig `yaml:"persistence,omitempty" json:"persistence,omitempty"`   // Where engine state (agent memory, materialized views, ...) is stored.
	// DangerouslyAllowUnauthenticatedNetwork disables the fail-closed guard that
	// otherwise prevents network transports (sse, streamable-http) from starting
	// without a cryptographic identity provider AND default-deny authorization.
	// It exists ONLY for local development and MUST NOT be used in production.
	// It can also be enabled via KAFKAMCP_UNSAFE_ALLOW_UNAUTHENTICATED_NETWORK=true.
	DangerouslyAllowUnauthenticatedNetwork bool `yaml:"dangerously_allow_unauthenticated_network,omitempty" json:"dangerously_allow_unauthenticated_network,omitempty"`
}

// PersistenceConfig selects the storage backend used by KafkaMCP's
// persistent engines (agent memory, materialized views, rate-limit
// budgets). When omitted, an in-memory backend is used and all state is
// lost on restart.
type PersistenceConfig struct {
	Backend string `yaml:"backend,omitempty" json:"backend,omitempty"` // "inmem" (default) or "sqlite"
	Path    string `yaml:"path,omitempty" json:"path,omitempty"`       // On-disk path for sqlite backend
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
	return net.JoinHostPort(host, strconv.Itoa(port))
}

// MetricsAddress returns the metrics/health listen address. It defaults to
// loopback for direct binary deployments; container orchestrators must opt in
// to a pod-reachable address such as 0.0.0.0.
func (s ServerConfig) MetricsAddress() string {
	host := s.MetricsHost
	if host == "" {
		host = "127.0.0.1"
	}
	port := s.MetricsPort
	if port == 0 {
		port = s.Port
		if port == 0 {
			port = 3100
		}
		port++
	}
	return net.JoinHostPort(host, strconv.Itoa(port))
}

// ClusterConfig describes a single Kafka cluster connection including broker
// addresses, authentication, Schema Registry, and Kafka Connect settings.
type ClusterConfig struct {
	Name             string               `yaml:"name" json:"name"`                                           // Unique cluster identifier
	BootstrapServers string               `yaml:"bootstrap_servers" json:"bootstrap_servers"`                 // Comma-separated broker addresses
	SecurityProtocol string               `yaml:"security_protocol" json:"security_protocol"`                 // e.g. "PLAINTEXT", "SSL", "SASL_SSL"
	SASLMechanism    string               `yaml:"sasl_mechanism,omitempty" json:"sasl_mechanism,omitempty"`   // e.g. "PLAIN", "SCRAM-SHA-256"
	SASLUsername     string               `yaml:"sasl_username,omitempty" json:"-"`                           // SASL username (required when mechanism is set)
	SASLPassword     string               `yaml:"sasl_password,omitempty" json:"-"`                           // SASL password (required when mechanism is set)
	SSLCALocation    string               `yaml:"ssl_ca_location,omitempty" json:"ssl_ca_location,omitempty"` // Path to CA certificate bundle
	TLSMinVersion    string               `yaml:"tls_min_version,omitempty" json:"tls_min_version,omitempty"` // Minimum broker TLS version: "1.2" (default) or "1.3"
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
	MinVersion         string `yaml:"min_version,omitempty" json:"min_version,omitempty"` // Defaults to the cluster tls_min_version
}

// BasicAuth holds username/password credentials for HTTP basic authentication.
type BasicAuth struct {
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"-"`
}

// CacheConfig controls the in-memory schema cache size and TTL.
type CacheConfig struct {
	MaxEntries int `yaml:"max_entries" json:"max_entries"` // Maximum cached schemas (default 10000)
	TTLSeconds int `yaml:"ttl_seconds" json:"ttl_seconds"` // Entry time-to-live in seconds (default 300)
}

// PolicyConfig defines the access-control policy applied to MCP agents.
type PolicyConfig struct {
	DefaultDeny              bool           `yaml:"default_deny" json:"default_deny"`                                                     // When true, deny access unless explicitly allowed
	Agents                   []AgentPolicy  `yaml:"agents" json:"agents"`                                                                 // Per-agent permission definitions
	GlobalRateLimitPerMinute int            `yaml:"global_rate_limit_per_minute,omitempty" json:"global_rate_limit_per_minute,omitempty"` // Shared cap across all agents (0 = unlimited)
	Identity                 IdentityConfig `yaml:"identity,omitempty" json:"identity,omitempty"`                                         // Cryptographic identity verification for the X-Agent-ID claim.
	Approval                 ApprovalConfig `yaml:"approval,omitempty" json:"approval,omitempty"`                                         // Short-lived approval grants gating write/destructive tools.
}

// ApprovalConfig controls the short-lived, single-use approval grants that gate
// every mutating/destructive ("ops-write") tool. Read-only tools are never
// affected. When Enabled is unset, enforcement defaults to the value of
// DefaultDeny so production installs (default_deny=true) require approvals while
// permissive local stdio stays frictionless.
type ApprovalConfig struct {
	// Enabled toggles enforcement. Unset -> follows Policies.DefaultDeny.
	Enabled *bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	// TTLSeconds is the default grant lifetime. 0 -> 300s (5 minutes).
	TTLSeconds int `yaml:"ttl_seconds,omitempty" json:"ttl_seconds,omitempty"`
	// MaxTTLSeconds caps the grant lifetime. 0 or too large -> 3600s (1 hour).
	MaxTTLSeconds int `yaml:"max_ttl_seconds,omitempty" json:"max_ttl_seconds,omitempty"`
	// SingleUse consumes a grant on first use. Unset -> true.
	SingleUse *bool `yaml:"single_use,omitempty" json:"single_use,omitempty"`
	// AllowSelfApproval permits the requesting identity to approve its own
	// grant. It defaults to false so production approval is a two-person action.
	AllowSelfApproval bool `yaml:"allow_self_approval,omitempty" json:"allow_self_approval,omitempty"`
	// SigningKeyEnv names the environment variable holding the HMAC key used to
	// make approval decision records tamper-evident. The key itself is never
	// serialized. Defaults to KAFKAMCP_APPROVAL_SIGNING_KEY.
	SigningKeyEnv string `yaml:"signing_key_env,omitempty" json:"signing_key_env,omitempty"`
	// PreviousSigningKeyEnvs lists old HMAC keys accepted while rotating the
	// signing key. New records are always signed with SigningKeyEnv.
	PreviousSigningKeyEnvs []string `yaml:"previous_signing_key_envs,omitempty" json:"previous_signing_key_envs,omitempty"`
	// TerminalRetentionSeconds controls how long used, revoked, expired, and
	// abandoned grants remain queryable. 0 -> 7 days.
	TerminalRetentionSeconds int `yaml:"terminal_retention_seconds,omitempty" json:"terminal_retention_seconds,omitempty"`
}

// EnforcementEnabled reports whether approval enforcement is active given the
// surrounding default-deny posture.
func (a ApprovalConfig) EnforcementEnabled(defaultDeny bool) bool {
	if a.Enabled != nil {
		return *a.Enabled
	}
	return defaultDeny
}

// ApprovalSigningKeyEnv is the default environment variable containing the
// HMAC-SHA-256 key for approval decision records.
const ApprovalSigningKeyEnv = "KAFKAMCP_APPROVAL_SIGNING_KEY"

// EffectiveSigningKeyEnv returns the configured approval decision signing-key
// environment variable.
func (a ApprovalConfig) EffectiveSigningKeyEnv() string {
	if strings.TrimSpace(a.SigningKeyEnv) == "" {
		return ApprovalSigningKeyEnv
	}
	return strings.TrimSpace(a.SigningKeyEnv)
}

// IdentityConfig configures how KafkaMCP verifies the claimed agent
// identity carried in each request. The default ("none") preserves the
// historical X-Agent-ID-only behavior; "static" and "oidc" let
// deployments cryptographically verify the caller before the policy
// engine matches.
type IdentityConfig struct {
	// Provider selects the verification method:
	//   - "" / "none" (default): trust X-Agent-ID header
	//   - "static": verify Authorization: Bearer <token> against an
	//     inline token table
	//   - "oidc": verify Authorization: Bearer <jwt> against an
	//     OIDC issuer's JWKS
	Provider string `yaml:"provider,omitempty" json:"provider,omitempty"`

	// AllowHeaderFallback controls whether requests that fail
	// provider verification (typically because they carry no
	// credentials) may fall back to the legacy X-Agent-ID /
	// X-Client-ID headers. The pointer-bool encoding lets us
	// distinguish "explicitly false" from "unset"; when unset, the
	// default is false for any cryptographic provider
	// (static/oidc/mtls) and true for "none" so existing
	// deployments are unaffected. Set explicitly when you want to
	// override the default — for example, a controlled migration
	// from header-asserted to OIDC where both must temporarily
	// work.
	AllowHeaderFallback *bool `yaml:"allow_header_fallback,omitempty" json:"allow_header_fallback,omitempty"`

	// Static is consulted when Provider == "static".
	Static StaticIdentityConfig `yaml:"static,omitempty" json:"static,omitempty"`

	// OIDC is consulted when Provider == "oidc".
	OIDC OIDCIdentityConfig `yaml:"oidc,omitempty" json:"oidc,omitempty"`

	// MTLS is consulted when Provider == "mtls".
	MTLS MTLSIdentityConfig `yaml:"mtls,omitempty" json:"mtls,omitempty"`
}

// HeaderFallbackEnabled reports whether legacy X-Agent-ID header
// fallback is permitted after provider verification fails. The default
// is true only when Provider is "" or "none"; for cryptographic
// providers (static/oidc/mtls) the default is false so an attacker
// cannot bypass the provider by simply omitting the bearer token.
func (c IdentityConfig) HeaderFallbackEnabled() bool {
	if c.AllowHeaderFallback != nil {
		return *c.AllowHeaderFallback
	}
	switch c.Provider {
	case "", "none":
		return true
	default:
		return false
	}
}

// IsCryptographic reports whether the identity provider verifies agent identity
// with an authenticated credential (static shared-secret token, OIDC bearer
// token, or mTLS client certificate) rather than trusting a plaintext
// X-Agent-ID header. Network transports require this to be true unless the
// unsafe development override is enabled.
func (c IdentityConfig) IsCryptographic() bool {
	switch strings.ToLower(strings.TrimSpace(c.Provider)) {
	case "static", "oidc", "mtls":
		return true
	default:
		return false
	}
}

// StaticIdentityConfig describes the inline shared-secret token table.
type StaticIdentityConfig struct {
	Tokens []StaticTokenConfig `yaml:"tokens,omitempty" json:"tokens,omitempty"`
}

// StaticTokenConfig binds a shared-secret bearer token to an agent
// identifier and optional groups.
type StaticTokenConfig struct {
	Token   string   `yaml:"token" json:"-"`                           // Secret bearer token (env-expanded)
	AgentID string   `yaml:"agent_id" json:"agent_id"`                 // Policy-engine identifier
	Groups  []string `yaml:"groups,omitempty" json:"groups,omitempty"` // Optional group labels surfaced on audit logs
}

// OIDCIdentityConfig configures the OIDC bearer-token verifier.
type OIDCIdentityConfig struct {
	IssuerURL    string `yaml:"issuer_url" json:"issuer_url"`                                       // OIDC issuer (discovery base)
	ClientID     string `yaml:"client_id" json:"client_id"`                                         // Expected audience
	AgentIDClaim string `yaml:"agent_id_claim,omitempty" json:"agent_id_claim,omitempty"`           // Defaults to "sub"
	GroupsClaim  string `yaml:"groups_claim,omitempty" json:"groups_claim,omitempty"`               // Defaults to "groups"
	EmailClaim   string `yaml:"email_claim,omitempty" json:"email_claim,omitempty"`                 // Defaults to "email"
	SkipExpiry   bool   `yaml:"skip_expiry_check,omitempty" json:"skip_expiry_check,omitempty"`     // Tests only
	SkipAudience bool   `yaml:"skip_audience_check,omitempty" json:"skip_audience_check,omitempty"` // Tests only
}

// MTLSIdentityConfig configures the mTLS identity provider. The TLS
// handshake itself (chain validation, expiry) is configured on the
// server's transport: this block governs which identities derived
// from the verified peer cert are accepted by KafkaMCP.
type MTLSIdentityConfig struct {
	// CABundle is the path to the PEM file of CAs that KafkaMCP will
	// trust to sign client certificates. When set, the HTTP transport
	// switches to RequireAndVerifyClientCert. When empty, KafkaMCP
	// uses the system trust store and still requires a client cert.
	CABundle string `yaml:"ca_bundle,omitempty" json:"ca_bundle,omitempty"`

	// AllowedSPIFFEIDs, when non-empty, restricts accepted SPIFFE IDs
	// to the listed values.
	AllowedSPIFFEIDs []string `yaml:"allowed_spiffe_ids,omitempty" json:"allowed_spiffe_ids,omitempty"`

	// AllowedTrustDomains, when non-empty, restricts the accepted
	// SPIFFE trust domains (e.g. "example.org").
	AllowedTrustDomains []string `yaml:"allowed_trust_domains,omitempty" json:"allowed_trust_domains,omitempty"`

	// GroupsFromOUs maps the cert Subject.OrganizationalUnit values
	// into the identity Groups field.
	GroupsFromOUs bool `yaml:"groups_from_ous,omitempty" json:"groups_from_ous,omitempty"`

	// AllowCNFallback controls whether the Subject CN may be used as
	// the agent ID when the cert has no URI SANs. Defaults to true.
	AllowCNFallback bool `yaml:"allow_cn_fallback,omitempty" json:"allow_cn_fallback,omitempty"`
}

// AgentPolicy specifies the permissions and rate limits for a single MCP agent.
type AgentPolicy struct {
	ID             string            `yaml:"id" json:"id"`
	Role           string            `yaml:"role" json:"role"`
	Topics         []TopicPermission `yaml:"topics" json:"topics"`
	ConsumerGroups GroupPermission   `yaml:"consumer_groups" json:"consumer_groups"`
	Schemas        SchemaPermission  `yaml:"schemas" json:"schemas"`
	RateLimit      RateLimitConfig   `yaml:"rate_limit" json:"rate_limit"`
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
	// KeyEnv names the environment variable that holds the HMAC-SHA-256 key used
	// for the "hash" and "tokenize" masking actions. The secret itself is never
	// stored in serialized config. When empty, KAFKAMCP_MASKING_KEY is consulted.
	// If no key is present, hash/tokenize actions fail closed (fully redact).
	KeyEnv string `yaml:"key_env,omitempty" json:"key_env,omitempty"`
}

// MaskingRule defines a single masking rule for a field pattern.
type MaskingRule struct {
	Name      string   `yaml:"name" json:"name"`                           // Rule identifier
	Type      string   `yaml:"type" json:"type"`                           // Rule type: email, ssn, credit_card, phone, regex, ner
	FieldPath string   `yaml:"field_path" json:"field_path"`               // JSON path pattern (e.g., "$.user.email", "**email**")
	Pattern   string   `yaml:"pattern,omitempty" json:"pattern,omitempty"` // Custom regex pattern (for type=regex)
	Replace   string   `yaml:"replace,omitempty" json:"replace,omitempty"` // Replacement string. Default: "***REDACTED***"
	Agents    []string `yaml:"agents,omitempty" json:"agents,omitempty"`   // Apply only to these agents. Empty = all.
}

// TelemetryConfig holds OpenTelemetry configuration for tracing.
type TelemetryConfig struct {
	Enabled     bool    `yaml:"enabled" json:"enabled"`
	Endpoint    string  `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`         // OTLP endpoint (e.g., "localhost:4318")
	Insecure    bool    `yaml:"insecure,omitempty" json:"insecure,omitempty"`         // Use HTTP instead of HTTPS
	ServiceName string  `yaml:"service_name,omitempty" json:"service_name,omitempty"` // Service name for traces
	SampleRate  float64 `yaml:"sample_rate,omitempty" json:"sample_rate,omitempty"`   // 0.0-1.0, default 1.0
	// ActivationMetrics opts into extra, privacy-preserving local activation
	// metrics (setup/validation success, active verified agents, tool-pack use,
	// denied unsafe calls, approval outcomes, core workflow completions). These
	// are exposed on the local Prometheus endpoint only — there is NO phone-home.
	// Labels are deliberately low-cardinality and never include raw agent or
	// resource identifiers. Default: off.
	ActivationMetrics bool `yaml:"activation_metrics,omitempty" json:"activation_metrics,omitempty"`
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
