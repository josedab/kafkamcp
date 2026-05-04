package server

import (
	"fmt"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/schema"
)

func TestResolveClusterName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cluster string
		want    string
	}{
		{
			name:    "empty string returns default",
			cluster: "",
			want:    "default",
		},
		{
			name:    "valid cluster returns itself",
			cluster: "prod",
			want:    "prod",
		},
		{
			name:    "invalid cluster falls back to default",
			cluster: "nonexistent",
			want:    "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := newTestServer(t)
			s.kafkaManager = &kafka.MockKafkaManager{
				DefaultClusterNameFn: func() string { return "default" },
				GetClusterConfigFn: func(name string) (*config.ClusterConfig, error) {
					if name == "prod" {
						return &config.ClusterConfig{Name: "prod"}, nil
					}
					return nil, fmt.Errorf("cluster %q not found", name)
				},
			}

			got := s.resolveClusterName(tt.cluster)
			if got != tt.want {
				t.Errorf("resolveClusterName(%q) = %q, want %q", tt.cluster, got, tt.want)
			}
		})
	}
}

func TestGetSchemaClient(t *testing.T) {
	t.Parallel()

	defaultClient := &schema.Client{}
	prodClient := &schema.Client{}

	tests := []struct {
		name    string
		cluster string
		clients map[string]*schema.Client
		want    *schema.Client
	}{
		{
			name:    "empty cluster returns default client",
			cluster: "",
			clients: map[string]*schema.Client{"default": defaultClient, "prod": prodClient},
			want:    defaultClient,
		},
		{
			name:    "named cluster returns that client",
			cluster: "prod",
			clients: map[string]*schema.Client{"default": defaultClient, "prod": prodClient},
			want:    prodClient,
		},
		{
			name:    "unknown cluster falls back to default",
			cluster: "staging",
			clients: map[string]*schema.Client{"default": defaultClient},
			want:    defaultClient,
		},
		{
			name:    "no clients returns nil",
			cluster: "",
			clients: map[string]*schema.Client{},
			want:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := newTestServer(t)
			s.kafkaManager = &kafka.MockKafkaManager{
				DefaultClusterNameFn: func() string { return "default" },
			}
			s.schemaClients = tt.clients

			got := s.getSchemaClient(tt.cluster)
			if got != tt.want {
				t.Errorf("getSchemaClient(%q) = %v, want %v", tt.cluster, got, tt.want)
			}
		})
	}
}
