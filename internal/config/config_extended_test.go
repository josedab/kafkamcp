package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestServerAddress(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  ServerConfig
		want string
	}{
		{"defaults", ServerConfig{}, "0.0.0.0:3100"},
		{"custom_host", ServerConfig{Host: "127.0.0.1"}, "127.0.0.1:3100"},
		{"custom_port", ServerConfig{Port: 8080}, "0.0.0.0:8080"},
		{"both_set", ServerConfig{Host: "10.0.0.1", Port: 9090}, "10.0.0.1:9090"},
		{"ipv6", ServerConfig{Host: "::1", Port: 9090}, "[::1]:9090"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.cfg.Address()
			if got != tt.want {
				t.Errorf("Address() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestServerMetricsAddress(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  ServerConfig
		want string
	}{
		{"defaults", ServerConfig{}, "127.0.0.1:3101"},
		{"custom_server_port", ServerConfig{Port: 8080}, "127.0.0.1:8081"},
		{"custom_host", ServerConfig{MetricsHost: "0.0.0.0"}, "0.0.0.0:3101"},
		{"custom_port", ServerConfig{MetricsPort: 9400}, "127.0.0.1:9400"},
		{"both_set", ServerConfig{MetricsHost: "10.0.0.1", MetricsPort: 9400}, "10.0.0.1:9400"},
		{"ipv6", ServerConfig{MetricsHost: "::1", MetricsPort: 9400}, "[::1]:9400"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.cfg.MetricsAddress(); got != tt.want {
				t.Errorf("MetricsAddress() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDefaultCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		cfg      Config
		wantName string
		wantNil  bool
	}{
		{
			name:    "no_clusters",
			cfg:     Config{},
			wantNil: true,
		},
		{
			name: "first_is_default",
			cfg: Config{Clusters: []ClusterConfig{
				{Name: "first", BootstrapServers: "a:9092", Default: true},
				{Name: "second", BootstrapServers: "b:9092"},
			}},
			wantName: "first",
		},
		{
			name: "second_is_default",
			cfg: Config{Clusters: []ClusterConfig{
				{Name: "first", BootstrapServers: "a:9092"},
				{Name: "second", BootstrapServers: "b:9092", Default: true},
			}},
			wantName: "second",
		},
		{
			name: "none_marked_returns_first",
			cfg: Config{Clusters: []ClusterConfig{
				{Name: "only", BootstrapServers: "a:9092"},
			}},
			wantName: "only",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.cfg.DefaultCluster()
			if tt.wantNil {
				if got != nil {
					t.Errorf("expected nil, got %+v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("expected non-nil cluster")
			}
			if got.Name != tt.wantName {
				t.Errorf("DefaultCluster().Name = %q, want %q", got.Name, tt.wantName)
			}
		})
	}
}

func TestFindCluster(t *testing.T) {
	t.Parallel()
	cfg := Config{Clusters: []ClusterConfig{
		{Name: "prod", BootstrapServers: "prod:9092", Default: true},
		{Name: "staging", BootstrapServers: "staging:9092"},
	}}

	tests := []struct {
		name     string
		search   string
		wantName string
		wantErr  bool
	}{
		{"find_by_name", "staging", "staging", false},
		{"find_default_empty_name", "", "prod", false},
		{"not_found", "dev", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := cfg.FindCluster(tt.search)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Name != tt.wantName {
				t.Errorf("FindCluster(%q).Name = %q, want %q", tt.search, got.Name, tt.wantName)
			}
		})
	}
}

func TestFindClusterNoClusters(t *testing.T) {
	t.Parallel()
	cfg := Config{}
	_, err := cfg.FindCluster("")
	if err == nil {
		t.Fatal("expected error for empty config with no clusters")
	}
}

func TestApplyDefaultsSetsFirstClusterAsDefault(t *testing.T) {
	t.Parallel()
	cfg := Config{Clusters: []ClusterConfig{
		{Name: "a", BootstrapServers: "a:9092"},
		{Name: "b", BootstrapServers: "b:9092"},
	}}
	cfg.applyDefaults()

	if !cfg.Clusters[0].Default {
		t.Error("expected first cluster to be marked as default")
	}
	if cfg.Clusters[1].Default {
		t.Error("second cluster should not be default")
	}
}

func TestApplyDefaultsPreservesExistingDefault(t *testing.T) {
	t.Parallel()
	cfg := Config{Clusters: []ClusterConfig{
		{Name: "a", BootstrapServers: "a:9092"},
		{Name: "b", BootstrapServers: "b:9092", Default: true},
	}}
	cfg.applyDefaults()

	if cfg.Clusters[0].Default {
		t.Error("first cluster should not be default when second is already default")
	}
	if !cfg.Clusters[1].Default {
		t.Error("second cluster should remain default")
	}
}

func TestApplyDefaultsMaxMessageSize(t *testing.T) {
	t.Parallel()
	cfg := Config{Clusters: []ClusterConfig{{Name: "t", BootstrapServers: "l:9092"}}}
	cfg.applyDefaults()

	if cfg.Server.MaxMessageSize != 1048576 {
		t.Errorf("MaxMessageSize = %d, want 1048576", cfg.Server.MaxMessageSize)
	}
	if cfg.Server.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want %q", cfg.Server.LogLevel, "info")
	}
	if cfg.Server.MetricsHost != "127.0.0.1" || cfg.Server.MetricsPort != 3101 {
		t.Errorf("metrics address = %s, want 127.0.0.1:3101", cfg.Server.MetricsAddress())
	}
	if cfg.Clusters[0].SecurityProtocol != "PLAINTEXT" {
		t.Errorf("SecurityProtocol = %q, want PLAINTEXT", cfg.Clusters[0].SecurityProtocol)
	}
	if cfg.Clusters[0].TLSMinVersion != "1.2" || cfg.Clusters[0].ConnectTLS.MinVersion != "1.2" {
		t.Errorf("TLS defaults = broker %q connect %q, want 1.2", cfg.Clusters[0].TLSMinVersion, cfg.Clusters[0].ConnectTLS.MinVersion)
	}
	if cfg.Audit.MaxEntries != 10000 {
		t.Errorf("Audit.MaxEntries = %d, want 10000", cfg.Audit.MaxEntries)
	}
	if cfg.SchemaCache.TTLSeconds != 300 {
		t.Errorf("SchemaCache.TTLSeconds = %d, want 300", cfg.SchemaCache.TTLSeconds)
	}
}

func TestValidateTransports(t *testing.T) {
	t.Parallel()
	base := Config{Clusters: []ClusterConfig{{Name: "t", BootstrapServers: "l:9092"}}}

	validTransports := []string{"stdio", "sse", "streamable-http", ""}
	for _, tr := range validTransports {
		cfg := base
		cfg.Server.Transport = tr
		if tr == "sse" || tr == "streamable-http" {
			cfg.Server.DangerouslyAllowUnauthenticatedNetwork = true
		}
		if err := cfg.validate(); err != nil {
			t.Errorf("transport %q should be valid, got error: %v", tr, err)
		}
	}

	invalidTransports := []string{"grpc", "websocket", "http"}
	for _, tr := range invalidTransports {
		cfg := base
		cfg.Server.Transport = tr
		if err := cfg.validate(); err == nil {
			t.Errorf("transport %q should be invalid", tr)
		}
	}
}

func TestValidateClusterNameRequired(t *testing.T) {
	t.Parallel()
	cfg := Config{Clusters: []ClusterConfig{{BootstrapServers: "l:9092"}}}
	err := cfg.validate()
	if err == nil {
		t.Fatal("expected error for missing cluster name")
	}
}

func TestValidateSASLPartialCredentials(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		cluster ClusterConfig
		wantErr bool
	}{
		{
			name:    "mechanism_without_username",
			cluster: ClusterConfig{Name: "t", BootstrapServers: "l:9092", SASLMechanism: "PLAIN", SASLPassword: "pass"},
			wantErr: true,
		},
		{
			name:    "mechanism_without_password",
			cluster: ClusterConfig{Name: "t", BootstrapServers: "l:9092", SASLMechanism: "PLAIN", SASLUsername: "user"},
			wantErr: true,
		},
		{
			name:    "no_mechanism_no_creds_ok",
			cluster: ClusterConfig{Name: "t", BootstrapServers: "l:9092"},
			wantErr: false,
		},
		{
			name: "complete_sasl_config_ok",
			cluster: ClusterConfig{
				Name:             "t",
				BootstrapServers: "l:9092",
				SecurityProtocol: "SASL_SSL",
				SASLMechanism:    "SCRAM-SHA-512",
				SASLUsername:     "user",
				SASLPassword:     "pass",
			},
			wantErr: false,
		},
		{
			name: "sasl_protocol_requires_mechanism",
			cluster: ClusterConfig{
				Name:             "t",
				BootstrapServers: "l:9092",
				SecurityProtocol: "SASL_SSL",
			},
			wantErr: true,
		},
		{
			name: "mechanism_requires_sasl_protocol",
			cluster: ClusterConfig{
				Name:             "t",
				BootstrapServers: "l:9092",
				SecurityProtocol: "SSL",
				SASLMechanism:    "PLAIN",
				SASLUsername:     "user",
				SASLPassword:     "pass",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{Clusters: []ClusterConfig{tt.cluster}}
			err := cfg.validate()
			if tt.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateTLSVersions(t *testing.T) {
	t.Parallel()
	for _, version := range []string{"", "1.2", "1.3"} {
		cfg := Config{Clusters: []ClusterConfig{{
			Name:             "t",
			BootstrapServers: "l:9092",
			TLSMinVersion:    version,
			ConnectTLS:       ConnectTLSConfig{MinVersion: version},
		}}}
		if err := cfg.validate(); err != nil {
			t.Errorf("TLS version %q should be valid: %v", version, err)
		}
	}
	for _, version := range []string{"1.0", "1.1", "latest"} {
		cfg := Config{Clusters: []ClusterConfig{{
			Name:             "t",
			BootstrapServers: "l:9092",
			TLSMinVersion:    version,
		}}}
		if err := cfg.validate(); err == nil {
			t.Errorf("TLS version %q should be rejected", version)
		}
	}
}

func TestValidateMetricsPort(t *testing.T) {
	t.Parallel()
	base := Config{Clusters: []ClusterConfig{{Name: "t", BootstrapServers: "l:9092"}}}

	cfg := base
	cfg.Server.Port = 65535
	if err := cfg.validate(); err == nil {
		t.Fatal("server.port=65535 without explicit metrics_port should fail")
	}

	cfg = base
	cfg.Server.Transport = "sse"
	cfg.Server.DangerouslyAllowUnauthenticatedNetwork = true
	cfg.Server.Host = "127.0.0.1"
	cfg.Server.Port = 3100
	cfg.Server.MetricsHost = "127.0.0.1"
	cfg.Server.MetricsPort = 3100
	if err := cfg.validate(); err == nil {
		t.Fatal("same host and port for API and metrics should fail")
	}
}

func TestExpandEnvVarsCommentSkip(t *testing.T) {
	t.Parallel()
	input := "# ${SHOULD_NOT_EXPAND}\nvalue: plain"
	got, err := expandEnvVars([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != input {
		t.Errorf("got %q, want %q (comments should be unchanged)", string(got), input)
	}
}

func TestExpandEnvVarsSetVarOverridesDefault(t *testing.T) {
	_ = os.Setenv("TEST_EXPAND_OVERRIDE", "real_value")
	defer func() { _ = os.Unsetenv("TEST_EXPAND_OVERRIDE") }()

	input := "val: ${TEST_EXPAND_OVERRIDE:-fallback}"
	got, err := expandEnvVars([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "val: real_value"
	if string(got) != want {
		t.Errorf("got %q, want %q", string(got), want)
	}
}

func TestLoadValidConfigFile(t *testing.T) {
	t.Parallel()
	content := `
clusters:
  - name: test-cluster
    bootstrap_servers: localhost:9092
server:
  transport: stdio
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Clusters[0].Name != "test-cluster" {
		t.Errorf("cluster name = %q, want %q", cfg.Clusters[0].Name, "test-cluster")
	}
	if cfg.Server.Port != 3100 {
		t.Errorf("port = %d, want 3100 (default)", cfg.Server.Port)
	}
}

func TestLoadMissingFile(t *testing.T) {
	t.Parallel()
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadInvalidYAML(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte(":::not yaml:::"), 0600); err != nil {
		t.Fatal(err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestLoadValidationFailure(t *testing.T) {
	t.Parallel()
	content := `
clusters: []
`
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for empty clusters")
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	content := `
server:
  transport: stdio
  dashboard:
    api_key: removed
clusters:
  - name: local
    bootstrap_servers: localhost:9092
`
	path := filepath.Join(t.TempDir(), "unknown.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("unknown config field should be rejected")
	}
	if !strings.Contains(err.Error(), "field dashboard not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadWithEnvVars(t *testing.T) {
	_ = os.Setenv("TEST_BOOTSTRAP", "kafka:9092")
	defer func() { _ = os.Unsetenv("TEST_BOOTSTRAP") }()

	content := `
clusters:
  - name: env-cluster
    bootstrap_servers: ${TEST_BOOTSTRAP}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "env.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Clusters[0].BootstrapServers != "kafka:9092" {
		t.Errorf("bootstrap_servers = %q, want %q", cfg.Clusters[0].BootstrapServers, "kafka:9092")
	}
}
