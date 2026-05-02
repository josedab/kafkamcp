package clouddeploy

import (
	"log/slog"
	"strings"
	"testing"
)

func TestGenerateAWS(t *testing.T) {
	t.Parallel()
	g := NewGenerator(slog.Default())

	cfg := DeploymentConfig{
		Provider:         ProviderAWS,
		Service:          ServiceMSK,
		ClusterName:      "prod-cluster",
		BootstrapServers: "b-1.msk.us-east-1.amazonaws.com:9092",
	}

	templates, err := g.Generate(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(templates) < 4 {
		t.Errorf("expected at least 4 templates, got %d", len(templates))
	}

	var hasDocker, hasK8s, hasCLI, hasConfig bool
	for _, tmpl := range templates {
		switch tmpl.Type {
		case "docker-compose":
			hasDocker = true
			if !strings.Contains(tmpl.Content, "kafkamcp") {
				t.Error("docker-compose should reference kafkamcp")
			}
		case "kubernetes":
			hasK8s = true
			if !strings.Contains(tmpl.Content, "prod-cluster") {
				t.Error("k8s manifest should contain cluster name")
			}
		case "cli":
			hasCLI = true
			if !strings.Contains(tmpl.Content, "aws") {
				t.Error("CLI commands should contain aws")
			}
		case "config":
			hasConfig = true
			if !strings.Contains(tmpl.Content, "SASL_SSL") {
				t.Error("MSK config should use SASL_SSL")
			}
		}
	}

	if !hasDocker || !hasK8s || !hasCLI || !hasConfig {
		t.Errorf("missing templates: docker=%v k8s=%v cli=%v config=%v", hasDocker, hasK8s, hasCLI, hasConfig)
	}
}

func TestGenerateGCP(t *testing.T) {
	t.Parallel()
	g := NewGenerator(slog.Default())

	templates, err := g.Generate(DeploymentConfig{
		Provider:         ProviderGCP,
		Service:          ServiceGCPKafka,
		BootstrapServers: "kafka.gcp.example.com:9092",
	})
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, tmpl := range templates {
		if tmpl.Type == "cli" && strings.Contains(tmpl.Content, "gcloud") {
			found = true
		}
	}
	if !found {
		t.Error("expected GCP CLI commands")
	}
}

func TestGenerateAzure(t *testing.T) {
	t.Parallel()
	g := NewGenerator(slog.Default())

	templates, err := g.Generate(DeploymentConfig{
		Provider:         ProviderAzure,
		Service:          ServiceEventHubs,
		BootstrapServers: "eh.servicebus.windows.net:9093",
	})
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, tmpl := range templates {
		if tmpl.Type == "cli" && strings.Contains(tmpl.Content, "az container") {
			found = true
		}
	}
	if !found {
		t.Error("expected Azure CLI commands")
	}
}

func TestGenerateDefaults(t *testing.T) {
	t.Parallel()
	g := NewGenerator(slog.Default())

	templates, err := g.Generate(DeploymentConfig{Provider: ProviderAWS, Service: ServiceMSK})
	if err != nil {
		t.Fatal(err)
	}

	for _, tmpl := range templates {
		if tmpl.Type == "kubernetes" && !strings.Contains(tmpl.Content, "replicas: 1") {
			t.Error("expected default 1 replica")
		}
	}
}

func TestListTemplates(t *testing.T) {
	t.Parallel()
	g := NewGenerator(slog.Default())

	g.Generate(DeploymentConfig{Provider: ProviderAWS, Service: ServiceMSK})
	templates := g.ListTemplates()
	if len(templates) == 0 {
		t.Error("expected templates after generation")
	}

	tmpl, err := g.GetTemplate(templates[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if tmpl.Content == "" {
		t.Error("expected non-empty content")
	}
}
