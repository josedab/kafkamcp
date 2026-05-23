package clouddeploy

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// CloudProvider represents a cloud platform.
type CloudProvider string

// ProviderAWS and related values define constants for this package.
const (
	ProviderAWS       CloudProvider = "aws"
	ProviderGCP       CloudProvider = "gcp"
	ProviderAzure     CloudProvider = "azure"
	ProviderConfluent CloudProvider = "confluent"
)

// KafkaService represents the cloud Kafka service.
type KafkaService string

// ServiceMSK and related values define constants for this package.
const (
	ServiceMSK            KafkaService = "aws-msk"
	ServiceConfluentCloud KafkaService = "confluent-cloud"
	ServiceEventHubs      KafkaService = "azure-event-hubs"
	ServiceGCPKafka       KafkaService = "gcp-managed-kafka"
)

// DeploymentConfig holds input for deployment template generation.
type DeploymentConfig struct {
	Provider         CloudProvider     `json:"provider"`
	Service          KafkaService      `json:"service"`
	Region           string            `json:"region"`
	ClusterName      string            `json:"cluster_name"`
	BootstrapServers string            `json:"bootstrap_servers,omitempty"`
	SecurityProtocol string            `json:"security_protocol,omitempty"`
	SASLMechanism    string            `json:"sasl_mechanism,omitempty"`
	Namespace        string            `json:"namespace,omitempty"`
	Replicas         int               `json:"replicas,omitempty"`
	Transport        string            `json:"transport,omitempty"` // stdio, sse, streamable-http
	Tags             map[string]string `json:"tags,omitempty"`
}

// DeploymentTemplate is a generated deployment artifact.
type DeploymentTemplate struct {
	ID        string            `json:"id"`
	Provider  CloudProvider     `json:"provider"`
	Service   KafkaService      `json:"service"`
	Type      string            `json:"type"` // docker-compose, kubernetes, terraform, cloudformation, cli
	Content   string            `json:"content"`
	Files     map[string]string `json:"files,omitempty"`
	Commands  []string          `json:"commands,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
}

// Generator creates deployment templates.
type Generator struct {
	mu        sync.RWMutex
	templates map[string]*DeploymentTemplate
	logger    *slog.Logger
	nextID    int64
}

// NewGenerator creates a new deployment generator.
func NewGenerator(logger *slog.Logger) *Generator {
	return &Generator{
		templates: make(map[string]*DeploymentTemplate),
		logger:    logger,
	}
}

// Generate creates deployment templates for the given config.
func (g *Generator) Generate(cfg DeploymentConfig) ([]DeploymentTemplate, error) {
	if cfg.ClusterName == "" {
		cfg.ClusterName = "kafkamcp"
	}
	if cfg.Region == "" {
		cfg.Region = defaultRegion(cfg.Provider)
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "kafkamcp"
	}
	if cfg.Replicas <= 0 {
		cfg.Replicas = 1
	}
	if cfg.Transport == "" {
		cfg.Transport = "streamable-http"
	}
	if cfg.SecurityProtocol == "" {
		cfg.SecurityProtocol = defaultSecurityProtocol(cfg.Service)
	}

	var templates []DeploymentTemplate

	// Docker Compose
	dockerCompose := g.generateDockerCompose(cfg)
	templates = append(templates, dockerCompose)

	// Kubernetes manifest
	k8s := g.generateKubernetes(cfg)
	templates = append(templates, k8s)

	// Provider-specific IaC
	switch cfg.Provider {
	case ProviderAWS:
		templates = append(templates, g.generateAWSCommands(cfg))
	case ProviderGCP:
		templates = append(templates, g.generateGCPCommands(cfg))
	case ProviderAzure:
		templates = append(templates, g.generateAzureCommands(cfg))
	}

	// Config file
	configTemplate := g.generateConfigFile(cfg)
	templates = append(templates, configTemplate)

	g.mu.Lock()
	for i := range templates {
		g.nextID++
		templates[i].ID = fmt.Sprintf("deploy_%d", g.nextID)
		templates[i].CreatedAt = time.Now()
		g.templates[templates[i].ID] = &templates[i]
	}
	g.mu.Unlock()

	return templates, nil
}

func (g *Generator) generateDockerCompose(cfg DeploymentConfig) DeploymentTemplate {
	env := buildEnvVars(cfg)
	var envLines []string
	for k, v := range env {
		envLines = append(envLines, fmt.Sprintf("      %s: %s", k, v))
	}

	content := fmt.Sprintf(`version: '3.8'
services:
  kafkamcp:
    image: ghcr.io/kafkamcp/kafkamcp:latest
    container_name: kafkamcp
    ports:
      - "3100:3100"
      - "3101:3101"
    environment:
%s
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:3101/health"]
      interval: 30s
      timeout: 10s
      retries: 3
`, strings.Join(envLines, "\n"))

	return DeploymentTemplate{
		Provider: cfg.Provider,
		Service:  cfg.Service,
		Type:     "docker-compose",
		Content:  content,
	}
}

func (g *Generator) generateKubernetes(cfg DeploymentConfig) DeploymentTemplate {
	env := buildEnvVars(cfg)
	var envYAML []string
	for k, v := range env {
		envYAML = append(envYAML, fmt.Sprintf("          - name: %s\n            value: %q", k, v))
	}

	content := fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
  namespace: %s
  labels:
    app: kafkamcp
spec:
  replicas: %d
  selector:
    matchLabels:
      app: kafkamcp
  template:
    metadata:
      labels:
        app: kafkamcp
    spec:
      containers:
        - name: kafkamcp
          image: ghcr.io/kafkamcp/kafkamcp:latest
          ports:
            - containerPort: 3100
            - containerPort: 3101
          env:
%s
          livenessProbe:
            httpGet:
              path: /health
              port: 3101
            initialDelaySeconds: 10
            periodSeconds: 30
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: 500m
              memory: 256Mi
---
apiVersion: v1
kind: Service
metadata:
  name: %s
  namespace: %s
spec:
  selector:
    app: kafkamcp
  ports:
    - port: 3100
      targetPort: 3100
      name: mcp
    - port: 3101
      targetPort: 3101
      name: metrics
  type: ClusterIP
`, cfg.ClusterName, cfg.Namespace, cfg.Replicas, strings.Join(envYAML, "\n"), cfg.ClusterName, cfg.Namespace)

	return DeploymentTemplate{
		Provider: cfg.Provider,
		Service:  cfg.Service,
		Type:     "kubernetes",
		Content:  content,
	}
}

func (g *Generator) generateAWSCommands(cfg DeploymentConfig) DeploymentTemplate {
	commands := []string{
		fmt.Sprintf("# AWS MSK + KafkaMCP deployment for region %s", cfg.Region),
		fmt.Sprintf("aws ecs create-cluster --cluster-name %s --region %s", cfg.ClusterName, cfg.Region),
		"# Register task definition",
		fmt.Sprintf("aws ecs register-task-definition --family kafkamcp --container-definitions '[{\"name\":\"kafkamcp\",\"image\":\"ghcr.io/kafkamcp/kafkamcp:latest\",\"portMappings\":[{\"containerPort\":3100},{\"containerPort\":3101}],\"environment\":[{\"name\":\"KAFKAMCP_BOOTSTRAP_SERVERS\",\"value\":\"%s\"}]}]' --region %s", cfg.BootstrapServers, cfg.Region),
		"# Create service",
		fmt.Sprintf("aws ecs create-service --cluster %s --service-name kafkamcp --task-definition kafkamcp --desired-count %d --region %s", cfg.ClusterName, cfg.Replicas, cfg.Region),
	}

	return DeploymentTemplate{
		Provider: cfg.Provider,
		Service:  cfg.Service,
		Type:     "cli",
		Content:  strings.Join(commands, "\n"),
		Commands: commands,
	}
}

func (g *Generator) generateGCPCommands(cfg DeploymentConfig) DeploymentTemplate {
	commands := []string{
		fmt.Sprintf("# GCP Cloud Run + KafkaMCP deployment for region %s", cfg.Region),
		fmt.Sprintf("gcloud run deploy kafkamcp --image ghcr.io/kafkamcp/kafkamcp:latest --region %s --port 3100 --allow-unauthenticated --set-env-vars KAFKAMCP_BOOTSTRAP_SERVERS=%s,KAFKAMCP_TRANSPORT=%s", cfg.Region, cfg.BootstrapServers, cfg.Transport),
	}

	return DeploymentTemplate{
		Provider: cfg.Provider,
		Service:  cfg.Service,
		Type:     "cli",
		Content:  strings.Join(commands, "\n"),
		Commands: commands,
	}
}

func (g *Generator) generateAzureCommands(cfg DeploymentConfig) DeploymentTemplate {
	commands := []string{
		fmt.Sprintf("# Azure Container Instances + KafkaMCP for region %s", cfg.Region),
		fmt.Sprintf("az group create --name %s-rg --location %s", cfg.ClusterName, cfg.Region),
		fmt.Sprintf("az container create --resource-group %s-rg --name kafkamcp --image ghcr.io/kafkamcp/kafkamcp:latest --ports 3100 3101 --environment-variables KAFKAMCP_BOOTSTRAP_SERVERS=%s KAFKAMCP_TRANSPORT=%s", cfg.ClusterName, cfg.BootstrapServers, cfg.Transport),
	}

	return DeploymentTemplate{
		Provider: cfg.Provider,
		Service:  cfg.Service,
		Type:     "cli",
		Content:  strings.Join(commands, "\n"),
		Commands: commands,
	}
}

func (g *Generator) generateConfigFile(cfg DeploymentConfig) DeploymentTemplate {
	content := fmt.Sprintf(`server:
  transport: %s
  host: 0.0.0.0
  port: 3100
  log_level: info

clusters:
  - name: %s
    bootstrap_servers: %s
    security_protocol: %s
    sasl_mechanism: %s
`, cfg.Transport, cfg.ClusterName, cfg.BootstrapServers, cfg.SecurityProtocol, cfg.SASLMechanism)

	return DeploymentTemplate{
		Provider: cfg.Provider,
		Service:  cfg.Service,
		Type:     "config",
		Content:  content,
	}
}

// GetTemplate returns a template by ID.
func (g *Generator) GetTemplate(id string) (*DeploymentTemplate, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	t, ok := g.templates[id]
	if !ok {
		return nil, fmt.Errorf("template %q not found", id)
	}
	return t, nil
}

// ListTemplates returns all generated templates.
func (g *Generator) ListTemplates() []DeploymentTemplate {
	g.mu.RLock()
	defer g.mu.RUnlock()

	result := make([]DeploymentTemplate, 0, len(g.templates))
	for _, t := range g.templates {
		result = append(result, *t)
	}
	return result
}

func buildEnvVars(cfg DeploymentConfig) map[string]string {
	env := map[string]string{
		"KAFKAMCP_TRANSPORT":         cfg.Transport,
		"KAFKAMCP_BOOTSTRAP_SERVERS": cfg.BootstrapServers,
		"KAFKAMCP_LOG_LEVEL":         "info",
	}
	if cfg.SecurityProtocol != "" {
		env["KAFKAMCP_SECURITY_PROTOCOL"] = cfg.SecurityProtocol
	}
	if cfg.SASLMechanism != "" {
		env["KAFKAMCP_SASL_MECHANISM"] = cfg.SASLMechanism
	}
	return env
}

func defaultRegion(provider CloudProvider) string {
	switch provider {
	case ProviderAWS:
		return "us-east-1"
	case ProviderGCP:
		return "us-central1"
	case ProviderAzure:
		return "eastus"
	default:
		return "us-east-1"
	}
}

func defaultSecurityProtocol(service KafkaService) string {
	switch service {
	case ServiceMSK:
		return "SASL_SSL"
	case ServiceConfluentCloud:
		return "SASL_SSL"
	case ServiceEventHubs:
		return "SASL_SSL"
	default:
		return "PLAINTEXT"
	}
}
