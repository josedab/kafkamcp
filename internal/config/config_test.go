package config

import (
	"os"
	"testing"
)

func TestExpandEnvVars(t *testing.T) {
	_ = os.Setenv("TEST_KAFKA_USER", "admin")
	_ = os.Setenv("TEST_KAFKA_PASS", "secret123")
	defer func() { _ = os.Unsetenv("TEST_KAFKA_USER") }()
	defer func() { _ = os.Unsetenv("TEST_KAFKA_PASS") }()

	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:  "simple expansion",
			input: "username: ${TEST_KAFKA_USER}",
			want:  "username: admin",
		},
		{
			name:  "multiple expansions",
			input: "user: ${TEST_KAFKA_USER}, pass: ${TEST_KAFKA_PASS}",
			want:  "user: admin, pass: secret123",
		},
		{
			name:  "with default value",
			input: "host: ${NONEXISTENT_VAR:-localhost}",
			want:  "host: localhost",
		},
		{
			name:    "missing required var",
			input:   "host: ${DEFINITELY_NOT_SET}",
			wantErr: true,
		},
		{
			name:  "no vars",
			input: "host: localhost",
			want:  "host: localhost",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := expandEnvVars([]byte(tt.input))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("got %q, want %q", string(got), tt.want)
			}
		})
	}
}

func TestConfigValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name:    "no clusters",
			cfg:     Config{},
			wantErr: true,
		},
		{
			name: "missing bootstrap_servers",
			cfg: Config{
				Clusters: []ClusterConfig{{Name: "test"}},
			},
			wantErr: true,
		},
		{
			name: "invalid transport",
			cfg: Config{
				Server:   ServerConfig{Transport: "grpc"},
				Clusters: []ClusterConfig{{Name: "test", BootstrapServers: "localhost:9092"}},
			},
			wantErr: true,
		},
		{
			name: "duplicate cluster names",
			cfg: Config{
				Clusters: []ClusterConfig{
					{Name: "prod", BootstrapServers: "localhost:9092"},
					{Name: "prod", BootstrapServers: "localhost:9093"},
				},
			},
			wantErr: true,
		},
		{
			name: "SASL mechanism without credentials",
			cfg: Config{
				Clusters: []ClusterConfig{{Name: "test", BootstrapServers: "localhost:9092", SASLMechanism: "PLAIN"}},
			},
			wantErr: true,
		},
		{
			name: "valid minimal config",
			cfg: Config{
				Clusters: []ClusterConfig{{Name: "test", BootstrapServers: "localhost:9092"}},
			},
			wantErr: false,
		},
		{
			name: "valid SASL config",
			cfg: Config{
				Clusters: []ClusterConfig{{Name: "test", BootstrapServers: "localhost:9092", SASLMechanism: "PLAIN", SASLUsername: "user", SASLPassword: "pass"}},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.cfg.validate()
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Clusters: []ClusterConfig{
			{Name: "test", BootstrapServers: "localhost:9092"},
		},
	}
	cfg.applyDefaults()

	if cfg.Server.Transport != "stdio" {
		t.Errorf("expected default transport 'stdio', got %q", cfg.Server.Transport)
	}
	if cfg.Server.Port != 3100 {
		t.Errorf("expected default port 3100, got %d", cfg.Server.Port)
	}
	if cfg.SchemaCache.MaxEntries != 10000 {
		t.Errorf("expected default max_entries 10000, got %d", cfg.SchemaCache.MaxEntries)
	}
	if !cfg.Clusters[0].Default {
		t.Error("expected first cluster to be marked as default")
	}
}
