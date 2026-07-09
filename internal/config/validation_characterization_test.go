package config

import (
	"reflect"
	"testing"
)

func TestValidateCharacterization_ErrorOrder(t *testing.T) {
	disabled := false
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "clusters before transport",
			cfg:  Config{Server: ServerConfig{Transport: "grpc"}},
			want: "at least one cluster must be configured",
		},
		{
			name: "cluster fields before transport",
			cfg: Config{
				Server: ServerConfig{Transport: "grpc"},
				Clusters: []ClusterConfig{{
					Name: "local", BootstrapServers: "localhost:9092", SecurityProtocol: "bad",
				}},
			},
			want: `cluster[0] (local): invalid security_protocol "bad"; must be one of: PLAINTEXT, SSL, SASL_SSL, SASL_PLAINTEXT`,
		},
		{
			name: "transport before tool packs",
			cfg: Config{
				Server: ServerConfig{Transport: "grpc", ToolPacks: []string{"bad"}},
				Clusters: []ClusterConfig{{
					Name: "local", BootstrapServers: "localhost:9092",
				}},
			},
			want: `invalid transport "grpc"; must be one of: stdio, sse, streamable-http`,
		},
		{
			name: "tool packs before approval values",
			cfg: Config{
				Server: ServerConfig{Transport: "stdio", ToolPacks: []string{"bad"}},
				Clusters: []ClusterConfig{{
					Name: "local", BootstrapServers: "localhost:9092",
				}},
				Policies: PolicyConfig{Approval: ApprovalConfig{TTLSeconds: -1}},
			},
			want: `server.tool_packs[0] "bad" is invalid; must be one of: core-read, ops-write, governance, experimental (deprecated: essential, ops)`,
		},
		{
			name: "guarded approval posture before persistence",
			cfg: Config{
				Server: ServerConfig{
					Transport:   "streamable-http",
					ToolPacks:   []string{PackOpsWrite},
					Persistence: PersistenceConfig{Backend: "bogus"},
				},
				Clusters: []ClusterConfig{{
					Name: "local", BootstrapServers: "localhost:9092",
				}},
				Policies: PolicyConfig{
					DefaultDeny: true,
					Identity: IdentityConfig{
						Provider: "static",
						Static: StaticIdentityConfig{Tokens: []StaticTokenConfig{{
							Token: "token", AgentID: "operator",
						}}},
					},
					Approval: ApprovalConfig{Enabled: &disabled},
				},
			},
			want: "policies.approval.enabled cannot be false while a guarded tool pack is enabled",
		},
		{
			name: "approval values before persistence",
			cfg: Config{
				Server: ServerConfig{
					Transport:   "stdio",
					Persistence: PersistenceConfig{Backend: "bogus"},
				},
				Clusters: []ClusterConfig{{
					Name: "local", BootstrapServers: "localhost:9092",
				}},
				Policies: PolicyConfig{Approval: ApprovalConfig{TTLSeconds: -1}},
			},
			want: "policies.approval.ttl_seconds must be non-negative",
		},
		{
			name: "persistence before identity",
			cfg: Config{
				Server: ServerConfig{
					Transport:   "stdio",
					Persistence: PersistenceConfig{Backend: "bogus"},
				},
				Clusters: []ClusterConfig{{
					Name: "local", BootstrapServers: "localhost:9092",
				}},
				Policies: PolicyConfig{Identity: IdentityConfig{Provider: "bogus"}},
			},
			want: `invalid server.persistence.backend "bogus"; must be one of: inmem, sqlite`,
		},
		{
			name: "identity before final network posture",
			cfg: Config{
				Server: ServerConfig{Transport: "streamable-http"},
				Clusters: []ClusterConfig{{
					Name: "local", BootstrapServers: "localhost:9092",
				}},
				Policies: PolicyConfig{Identity: IdentityConfig{Provider: "bogus"}},
			},
			want: `invalid policies.identity.provider "bogus"; must be one of: none, static, oidc, mtls`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if err == nil {
				t.Fatal("validate() returned nil")
			}
			if err.Error() != tt.want {
				t.Fatalf("validate() error = %q, want %q", err, tt.want)
			}
		})
	}
}

func TestApplyDefaultsCharacterization(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Server: ServerConfig{Port: 3200},
		Clusters: []ClusterConfig{
			{Name: "first", BootstrapServers: "first:9092"},
			{Name: "second", BootstrapServers: "second:9092", TLSMinVersion: "1.3"},
		},
	}
	cfg.applyDefaults()

	if cfg.Server.Transport != "stdio" ||
		cfg.Server.LogLevel != "info" ||
		cfg.Server.Port != 3200 ||
		cfg.Server.MetricsHost != "127.0.0.1" ||
		cfg.Server.MetricsPort != 3201 ||
		cfg.Server.MaxMessageSize != 1048576 {
		t.Fatalf("server defaults = %#v", cfg.Server)
	}
	if !reflect.DeepEqual(cfg.Server.ToolPacks, []string{PackCoreRead}) {
		t.Fatalf("tool packs = %v, want [%s]", cfg.Server.ToolPacks, PackCoreRead)
	}
	if cfg.Server.Persistence.Backend != "inmem" {
		t.Fatalf("persistence backend = %q, want inmem", cfg.Server.Persistence.Backend)
	}
	if cfg.SchemaCache.MaxEntries != 10000 || cfg.SchemaCache.TTLSeconds != 300 {
		t.Fatalf("schema cache defaults = %#v", cfg.SchemaCache)
	}
	if cfg.Audit.MaxEntries != 10000 {
		t.Fatalf("audit max entries = %d, want 10000", cfg.Audit.MaxEntries)
	}
	if !cfg.Clusters[0].Default || cfg.Clusters[1].Default {
		t.Fatalf("cluster defaults = %#v", cfg.Clusters)
	}
	if cfg.Clusters[0].SecurityProtocol != "PLAINTEXT" ||
		cfg.Clusters[0].TLSMinVersion != "1.2" ||
		cfg.Clusters[0].ConnectTLS.MinVersion != "1.2" {
		t.Fatalf("first cluster defaults = %#v", cfg.Clusters[0])
	}
	if cfg.Clusters[1].SecurityProtocol != "PLAINTEXT" ||
		cfg.Clusters[1].TLSMinVersion != "1.3" ||
		cfg.Clusters[1].ConnectTLS.MinVersion != "1.3" {
		t.Fatalf("second cluster defaults = %#v", cfg.Clusters[1])
	}
}
