// Package operator provides a Kubernetes operator for managing KafkaMCP
// instances via Custom Resource Definitions (CRDs).
package operator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Controller is the KafkaMCP Kubernetes operator controller.
type Controller struct {
	mu          sync.RWMutex
	instances   map[string]*KafkaMCPInstance
	logger      *slog.Logger
	reconcileCh chan string
}

// KafkaMCPInstance represents a managed KafkaMCP deployment.
type KafkaMCPInstance struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace"`
	Spec        KafkaMCPSpec      `json:"spec"`
	Status      KafkaMCPStatus    `json:"status"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// KafkaMCPSpec is the desired state for a KafkaMCP instance.
type KafkaMCPSpec struct {
	Replicas    int             `json:"replicas" yaml:"replicas"`
	Image       string          `json:"image" yaml:"image"`
	Version     string          `json:"version" yaml:"version"`
	Transport   string          `json:"transport" yaml:"transport"`
	Port        int             `json:"port" yaml:"port"`
	Clusters    []ClusterRef    `json:"clusters" yaml:"clusters"`
	Resources   ResourceSpec    `json:"resources" yaml:"resources"`
	AutoScale   *AutoScaleSpec  `json:"auto_scale,omitempty" yaml:"auto_scale,omitempty"`
	HealthCheck HealthCheckSpec `json:"health_check" yaml:"health_check"`
	Policies    PolicySpec      `json:"policies,omitempty" yaml:"policies,omitempty"`
	Telemetry   TelemetrySpec   `json:"telemetry,omitempty" yaml:"telemetry,omitempty"`
}

// ClusterRef references a Kafka cluster to connect to.
type ClusterRef struct {
	Name             string `json:"name" yaml:"name"`
	BootstrapServers string `json:"bootstrap_servers" yaml:"bootstrap_servers"`
	SecretRef        string `json:"secret_ref,omitempty" yaml:"secret_ref,omitempty"`
	Default          bool   `json:"default,omitempty" yaml:"default,omitempty"`
}

// ResourceSpec defines compute resources.
type ResourceSpec struct {
	CPURequest    string `json:"cpu_request" yaml:"cpu_request"`
	CPULimit      string `json:"cpu_limit" yaml:"cpu_limit"`
	MemoryRequest string `json:"memory_request" yaml:"memory_request"`
	MemoryLimit   string `json:"memory_limit" yaml:"memory_limit"`
}

// AutoScaleSpec defines horizontal auto-scaling parameters.
type AutoScaleSpec struct {
	Enabled      bool `json:"enabled" yaml:"enabled"`
	MinReplicas  int  `json:"min_replicas" yaml:"min_replicas"`
	MaxReplicas  int  `json:"max_replicas" yaml:"max_replicas"`
	TargetCPU    int  `json:"target_cpu_percent" yaml:"target_cpu_percent"`
	TargetMemory int  `json:"target_memory_percent,omitempty" yaml:"target_memory_percent,omitempty"`
}

// HealthCheckSpec defines liveness and readiness probes.
type HealthCheckSpec struct {
	Enabled          bool `json:"enabled" yaml:"enabled"`
	IntervalSeconds  int  `json:"interval_seconds" yaml:"interval_seconds"`
	TimeoutSeconds   int  `json:"timeout_seconds" yaml:"timeout_seconds"`
	FailureThreshold int  `json:"failure_threshold" yaml:"failure_threshold"`
}

// PolicySpec defines RBAC policies for the operator.
type PolicySpec struct {
	DefaultDeny bool   `json:"default_deny" yaml:"default_deny"`
	ConfigMap   string `json:"config_map,omitempty" yaml:"config_map,omitempty"`
}

// TelemetrySpec defines observability configuration.
type TelemetrySpec struct {
	MetricsEnabled bool   `json:"metrics_enabled" yaml:"metrics_enabled"`
	TracingEnabled bool   `json:"tracing_enabled" yaml:"tracing_enabled"`
	OTLPEndpoint   string `json:"otlp_endpoint,omitempty" yaml:"otlp_endpoint,omitempty"`
}

// KafkaMCPStatus represents the observed state of a KafkaMCP instance.
type KafkaMCPStatus struct {
	Phase           string            `json:"phase"` // Pending, Running, Failed, Scaling, Updating
	ReadyReplicas   int               `json:"ready_replicas"`
	DesiredReplicas int               `json:"desired_replicas"`
	Conditions      []StatusCondition `json:"conditions"`
	LastReconcile   time.Time         `json:"last_reconcile"`
	ObservedGen     int64             `json:"observed_generation"`
	Message         string            `json:"message,omitempty"`
}

// StatusCondition represents a condition of the KafkaMCP instance.
type StatusCondition struct {
	Type    string    `json:"type"`
	Status  string    `json:"status"` // True, False, Unknown
	Reason  string    `json:"reason"`
	Message string    `json:"message"`
	Updated time.Time `json:"last_transition_time"`
}

// NewController creates a new operator controller.
func NewController(logger *slog.Logger) *Controller {
	return &Controller{
		instances:   make(map[string]*KafkaMCPInstance),
		logger:      logger,
		reconcileCh: make(chan string, 100),
	}
}

// CreateInstance creates a new KafkaMCP instance.
func (c *Controller) CreateInstance(name, namespace string, spec KafkaMCPSpec) (*KafkaMCPInstance, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := namespace + "/" + name
	if _, exists := c.instances[key]; exists {
		return nil, fmt.Errorf("instance %q already exists", key)
	}

	// Apply defaults
	if spec.Replicas <= 0 {
		spec.Replicas = 1
	}
	if spec.Image == "" {
		spec.Image = "kafkamcp/kafkamcp"
	}
	if spec.Version == "" {
		spec.Version = "latest"
	}
	if spec.Transport == "" {
		spec.Transport = "sse"
	}
	if spec.Port == 0 {
		spec.Port = 3100
	}
	if spec.Resources.CPURequest == "" {
		spec.Resources.CPURequest = "100m"
	}
	if spec.Resources.MemoryRequest == "" {
		spec.Resources.MemoryRequest = "128Mi"
	}
	if spec.Resources.CPULimit == "" {
		spec.Resources.CPULimit = "500m"
	}
	if spec.Resources.MemoryLimit == "" {
		spec.Resources.MemoryLimit = "512Mi"
	}
	if spec.HealthCheck.IntervalSeconds == 0 {
		spec.HealthCheck.IntervalSeconds = 10
		spec.HealthCheck.TimeoutSeconds = 5
		spec.HealthCheck.FailureThreshold = 3
		spec.HealthCheck.Enabled = true
	}

	instance := &KafkaMCPInstance{
		Name:      name,
		Namespace: namespace,
		Spec:      spec,
		Status: KafkaMCPStatus{
			Phase:           "Pending",
			DesiredReplicas: spec.Replicas,
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	c.instances[key] = instance
	c.logger.Info("instance created", "name", key)

	// Trigger reconciliation
	c.reconcile(instance)
	return instance, nil
}

// GetInstance returns an instance by name and namespace.
func (c *Controller) GetInstance(name, namespace string) (*KafkaMCPInstance, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := namespace + "/" + name
	inst, ok := c.instances[key]
	if !ok {
		return nil, fmt.Errorf("instance %q not found", key)
	}
	return inst, nil
}

// ListInstances returns all instances, optionally filtered by namespace.
func (c *Controller) ListInstances(namespace string) []KafkaMCPInstance {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result []KafkaMCPInstance
	for _, inst := range c.instances {
		if namespace == "" || inst.Namespace == namespace {
			result = append(result, *inst)
		}
	}
	return result
}

// UpdateInstance updates an instance's spec and triggers reconciliation.
func (c *Controller) UpdateInstance(name, namespace string, spec KafkaMCPSpec) (*KafkaMCPInstance, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := namespace + "/" + name
	inst, ok := c.instances[key]
	if !ok {
		return nil, fmt.Errorf("instance %q not found", key)
	}

	inst.Spec = spec
	inst.UpdatedAt = time.Now()
	inst.Status.Phase = "Updating"

	c.reconcile(inst)
	return inst, nil
}

// DeleteInstance removes an instance.
func (c *Controller) DeleteInstance(name, namespace string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := namespace + "/" + name
	if _, ok := c.instances[key]; !ok {
		return fmt.Errorf("instance %q not found", key)
	}

	delete(c.instances, key)
	c.logger.Info("instance deleted", "name", key)
	return nil
}

// ScaleInstance changes the replica count.
func (c *Controller) ScaleInstance(name, namespace string, replicas int) (*KafkaMCPInstance, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := namespace + "/" + name
	inst, ok := c.instances[key]
	if !ok {
		return nil, fmt.Errorf("instance %q not found", key)
	}

	if replicas < 0 || replicas > 100 {
		return nil, fmt.Errorf("replicas must be between 0 and 100")
	}

	inst.Spec.Replicas = replicas
	inst.Status.DesiredReplicas = replicas
	inst.Status.Phase = "Scaling"
	inst.UpdatedAt = time.Now()

	c.reconcile(inst)
	return inst, nil
}

// reconcile simulates the Kubernetes reconciliation loop.
func (c *Controller) reconcile(inst *KafkaMCPInstance) {
	now := time.Now()

	inst.Status.ReadyReplicas = inst.Spec.Replicas
	inst.Status.DesiredReplicas = inst.Spec.Replicas
	inst.Status.Phase = "Running"
	inst.Status.LastReconcile = now
	inst.Status.ObservedGen++

	inst.Status.Conditions = []StatusCondition{
		{
			Type:    "Available",
			Status:  "True",
			Reason:  "DeploymentAvailable",
			Message: fmt.Sprintf("%d/%d replicas ready", inst.Status.ReadyReplicas, inst.Status.DesiredReplicas),
			Updated: now,
		},
		{
			Type:    "KafkaConnected",
			Status:  "True",
			Reason:  "ClustersReachable",
			Message: fmt.Sprintf("%d cluster(s) configured", len(inst.Spec.Clusters)),
			Updated: now,
		},
	}

	if inst.Spec.HealthCheck.Enabled {
		inst.Status.Conditions = append(inst.Status.Conditions, StatusCondition{
			Type:    "HealthCheckConfigured",
			Status:  "True",
			Reason:  "ProbesConfigured",
			Message: fmt.Sprintf("interval=%ds, timeout=%ds", inst.Spec.HealthCheck.IntervalSeconds, inst.Spec.HealthCheck.TimeoutSeconds),
			Updated: now,
		})
	}

	c.logger.Info("reconciled instance", "name", inst.Namespace+"/"+inst.Name, "phase", inst.Status.Phase)
}

// GenerateCRD returns the CRD YAML for KafkaMCP.
func GenerateCRD() string {
	return `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: kafkamcps.kafkamcp.io
spec:
  group: kafkamcp.io
  versions:
    - name: v1alpha1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              properties:
                replicas:
                  type: integer
                  minimum: 0
                  maximum: 100
                  default: 1
                image:
                  type: string
                  default: "kafkamcp/kafkamcp"
                version:
                  type: string
                  default: "latest"
                transport:
                  type: string
                  enum: ["stdio", "sse", "streamable-http"]
                  default: "sse"
                port:
                  type: integer
                  default: 3100
                clusters:
                  type: array
                  items:
                    type: object
                    properties:
                      name:
                        type: string
                      bootstrap_servers:
                        type: string
                      secret_ref:
                        type: string
                      default:
                        type: boolean
                resources:
                  type: object
                  properties:
                    cpu_request:
                      type: string
                    cpu_limit:
                      type: string
                    memory_request:
                      type: string
                    memory_limit:
                      type: string
                auto_scale:
                  type: object
                  properties:
                    enabled:
                      type: boolean
                    min_replicas:
                      type: integer
                    max_replicas:
                      type: integer
                    target_cpu_percent:
                      type: integer
                health_check:
                  type: object
                  properties:
                    enabled:
                      type: boolean
                    interval_seconds:
                      type: integer
                    timeout_seconds:
                      type: integer
                    failure_threshold:
                      type: integer
            status:
              type: object
              properties:
                phase:
                  type: string
                ready_replicas:
                  type: integer
                desired_replicas:
                  type: integer
      subresources:
        status: {}
        scale:
          specReplicasPath: .spec.replicas
          statusReplicasPath: .status.ready_replicas
  scope: Namespaced
  names:
    plural: kafkamcps
    singular: kafkamcp
    kind: KafkaMCP
    shortNames:
      - kmcp`
}

// GenerateDeploymentManifest creates a Kubernetes deployment manifest for an instance.
func (c *Controller) GenerateDeploymentManifest(inst *KafkaMCPInstance) string {
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
  namespace: %s
  labels:
    app: kafkamcp
    instance: %s
spec:
  replicas: %d
  selector:
    matchLabels:
      app: kafkamcp
      instance: %s
  template:
    metadata:
      labels:
        app: kafkamcp
        instance: %s
    spec:
      containers:
        - name: kafkamcp
          image: %s:%s
          ports:
            - containerPort: %d
          resources:
            requests:
              cpu: %s
              memory: %s
            limits:
              cpu: %s
              memory: %s
          livenessProbe:
            httpGet:
              path: /healthz
              port: %d
            initialDelaySeconds: 10
            periodSeconds: %d
            timeoutSeconds: %d
            failureThreshold: %d
          readinessProbe:
            httpGet:
              path: /healthz
              port: %d
            initialDelaySeconds: 5
            periodSeconds: 5
`,
		inst.Name, inst.Namespace, inst.Name,
		inst.Spec.Replicas,
		inst.Name, inst.Name,
		inst.Spec.Image, inst.Spec.Version,
		inst.Spec.Port,
		inst.Spec.Resources.CPURequest, inst.Spec.Resources.MemoryRequest,
		inst.Spec.Resources.CPULimit, inst.Spec.Resources.MemoryLimit,
		inst.Spec.Port,
		inst.Spec.HealthCheck.IntervalSeconds,
		inst.Spec.HealthCheck.TimeoutSeconds,
		inst.Spec.HealthCheck.FailureThreshold,
		inst.Spec.Port,
	)
}

// Run starts the operator control loop (for use in cmd/operator/).
func (c *Controller) Run(ctx context.Context) error {
	c.logger.Info("starting KafkaMCP operator")

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("operator shutting down")
			return nil
		case <-ticker.C:
			c.mu.RLock()
			for _, inst := range c.instances {
				c.reconcile(inst)
			}
			c.mu.RUnlock()
		}
	}
}
