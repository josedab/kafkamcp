package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// LifecycleHook defines lifecycle events plugins can respond to.
type LifecycleHook string

const (
	HookInit         LifecycleHook = "init"
	HookRegisterTools LifecycleHook = "register_tools"
	HookShutdown     LifecycleHook = "shutdown"
	HookHealthCheck  LifecycleHook = "health_check"
	HookConfigReload LifecycleHook = "config_reload"
)

// LifecyclePlugin extends Plugin with lifecycle hook support.
type LifecyclePlugin interface {
	Plugin
	// OnHook is called when a lifecycle event occurs.
	OnHook(ctx context.Context, hook LifecycleHook, data map[string]any) error
	// SupportedHooks returns the hooks this plugin listens to.
	SupportedHooks() []LifecycleHook
}

// Manifest describes a plugin's metadata, capabilities, and security requirements.
type Manifest struct {
	APIVersion  string            `json:"api_version" yaml:"api_version"`
	Name        string            `json:"name" yaml:"name"`
	Version     string            `json:"version" yaml:"version"`
	Description string            `json:"description" yaml:"description"`
	Author      string            `json:"author,omitempty" yaml:"author,omitempty"`
	License     string            `json:"license,omitempty" yaml:"license,omitempty"`
	Homepage    string            `json:"homepage,omitempty" yaml:"homepage,omitempty"`
	Entrypoint  string            `json:"entrypoint" yaml:"entrypoint"`
	Type        string            `json:"type" yaml:"type"` // "native", "grpc", "wasm"
	Capabilities PluginCapabilities `json:"capabilities" yaml:"capabilities"`
	Config      map[string]ConfigParam `json:"config,omitempty" yaml:"config,omitempty"`
	Dependencies []string          `json:"dependencies,omitempty" yaml:"dependencies,omitempty"`
}

// PluginCapabilities declares what a plugin needs access to.
type PluginCapabilities struct {
	Tools     []string `json:"tools,omitempty" yaml:"tools,omitempty"`         // Tools this plugin provides
	Resources []string `json:"resources,omitempty" yaml:"resources,omitempty"` // Resources this plugin provides
	Hooks     []string `json:"hooks,omitempty" yaml:"hooks,omitempty"`         // Lifecycle hooks to subscribe to
	Network   bool     `json:"network,omitempty" yaml:"network,omitempty"`     // Whether plugin needs network access
	FileSystem bool    `json:"filesystem,omitempty" yaml:"filesystem,omitempty"` // Whether plugin needs filesystem access
}

// ConfigParam describes a configuration parameter for a plugin.
type ConfigParam struct {
	Type        string `json:"type" yaml:"type"`         // "string", "int", "bool", "secret"
	Description string `json:"description" yaml:"description"`
	Default     any    `json:"default,omitempty" yaml:"default,omitempty"`
	Required    bool   `json:"required" yaml:"required"`
}

// SecuritySandbox enforces capability-based security for plugins.
type SecuritySandbox struct {
	mu           sync.RWMutex
	allowed      map[string]*PluginCapabilities // plugin name -> allowed caps
	denyByDefault bool
}

// NewSecuritySandbox creates a new security sandbox with deny-by-default policy.
func NewSecuritySandbox() *SecuritySandbox {
	return &SecuritySandbox{
		allowed:      make(map[string]*PluginCapabilities),
		denyByDefault: true,
	}
}

// Grant grants capabilities to a plugin based on its manifest.
func (s *SecuritySandbox) Grant(pluginName string, caps PluginCapabilities) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allowed[pluginName] = &caps
}

// Revoke removes all capabilities from a plugin.
func (s *SecuritySandbox) Revoke(pluginName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.allowed, pluginName)
}

// CheckToolAccess verifies a plugin is allowed to provide a specific tool.
func (s *SecuritySandbox) CheckToolAccess(pluginName, toolName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.denyByDefault {
		return true
	}

	caps, ok := s.allowed[pluginName]
	if !ok {
		return false
	}

	for _, t := range caps.Tools {
		if t == toolName || t == "*" {
			return true
		}
	}
	return false
}

// CheckNetworkAccess verifies a plugin has network access rights.
func (s *SecuritySandbox) CheckNetworkAccess(pluginName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.denyByDefault {
		return true
	}

	caps, ok := s.allowed[pluginName]
	if !ok {
		return false
	}
	return caps.Network
}

// CheckFileSystemAccess verifies a plugin has filesystem access rights.
func (s *SecuritySandbox) CheckFileSystemAccess(pluginName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.denyByDefault {
		return true
	}

	caps, ok := s.allowed[pluginName]
	if !ok {
		return false
	}
	return caps.FileSystem
}

// GRPCSidecar defines the interface for communicating with language-agnostic plugin sidecars.
type GRPCSidecar struct {
	Name       string `json:"name"`
	Address    string `json:"address"` // e.g., "localhost:50051"
	Status     string `json:"status"`  // "connected", "disconnected", "error"
	StartedAt  time.Time `json:"started_at"`
	HealthURL  string `json:"health_url,omitempty"`
}

// SidecarManager manages gRPC-based plugin sidecars.
type SidecarManager struct {
	mu       sync.RWMutex
	sidecars map[string]*GRPCSidecar
	logger   interface{ Info(string, ...any); Warn(string, ...any) }
}

// NewSidecarManager creates a new sidecar manager.
func NewSidecarManager(logger interface{ Info(string, ...any); Warn(string, ...any) }) *SidecarManager {
	return &SidecarManager{
		sidecars: make(map[string]*GRPCSidecar),
		logger:   logger,
	}
}

// RegisterSidecar registers a gRPC plugin sidecar.
func (sm *SidecarManager) RegisterSidecar(name, address string) *GRPCSidecar {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sidecar := &GRPCSidecar{
		Name:      name,
		Address:   address,
		Status:    "connected",
		StartedAt: time.Now(),
	}
	sm.sidecars[name] = sidecar

	if sm.logger != nil {
		sm.logger.Info("gRPC sidecar registered", "name", name, "address", address)
	}
	return sidecar
}

// UnregisterSidecar removes a gRPC plugin sidecar.
func (sm *SidecarManager) UnregisterSidecar(name string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, ok := sm.sidecars[name]; !ok {
		return fmt.Errorf("sidecar %q not found", name)
	}

	delete(sm.sidecars, name)
	return nil
}

// ListSidecars returns all registered sidecars.
func (sm *SidecarManager) ListSidecars() []GRPCSidecar {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make([]GRPCSidecar, 0, len(sm.sidecars))
	for _, s := range sm.sidecars {
		result = append(result, *s)
	}
	return result
}

// GetSidecar returns a specific sidecar.
func (sm *SidecarManager) GetSidecar(name string) (*GRPCSidecar, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	s, ok := sm.sidecars[name]
	if !ok {
		return nil, fmt.Errorf("sidecar %q not found", name)
	}
	return s, nil
}

// LoadManifest loads a plugin manifest from a JSON file.
func LoadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading manifest %s: %w", path, err)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parsing manifest %s: %w", path, err)
	}

	if err := validateManifest(&manifest); err != nil {
		return nil, fmt.Errorf("invalid manifest %s: %w", path, err)
	}

	return &manifest, nil
}

func validateManifest(m *Manifest) error {
	if m.Name == "" {
		return fmt.Errorf("name is required")
	}
	if m.Version == "" {
		return fmt.Errorf("version is required")
	}
	if m.APIVersion == "" {
		m.APIVersion = "v1"
	}
	if m.Type == "" {
		m.Type = "native"
	}
	validTypes := map[string]bool{"native": true, "grpc": true, "wasm": true}
	if !validTypes[m.Type] {
		return fmt.Errorf("invalid plugin type %q; must be: native, grpc, wasm", m.Type)
	}

	// Validate plugin name format
	if strings.Contains(m.Name, " ") || strings.Contains(m.Name, "/") {
		return fmt.Errorf("plugin name %q must not contain spaces or slashes", m.Name)
	}

	return nil
}

// ExtendedRegistry wraps Registry with lifecycle and sidecar support.
type ExtendedRegistry struct {
	*Registry
	sandbox        *SecuritySandbox
	sidecarManager *SidecarManager
	manifests      map[string]*Manifest
	mu             sync.RWMutex
}

// NewExtendedRegistry creates an extended plugin registry.
func NewExtendedRegistry(registry *Registry) *ExtendedRegistry {
	return &ExtendedRegistry{
		Registry:       registry,
		sandbox:        NewSecuritySandbox(),
		sidecarManager: NewSidecarManager(registry.logger),
		manifests:      make(map[string]*Manifest),
	}
}

// RegisterWithManifest loads and registers a plugin using its manifest.
func (er *ExtendedRegistry) RegisterWithManifest(ctx context.Context, p Plugin, manifest *Manifest, config map[string]any) error {
	// Validate required config params
	for key, param := range manifest.Config {
		if param.Required {
			if _, ok := config[key]; !ok {
				if param.Default != nil {
					config[key] = param.Default
				} else {
					return fmt.Errorf("required config parameter %q not provided for plugin %q", key, manifest.Name)
				}
			}
		}
	}

	// Grant sandbox capabilities
	er.sandbox.Grant(manifest.Name, manifest.Capabilities)

	// Register the plugin
	if err := er.Registry.Register(ctx, p, config); err != nil {
		er.sandbox.Revoke(manifest.Name)
		return err
	}

	er.mu.Lock()
	er.manifests[manifest.Name] = manifest
	er.mu.Unlock()

	// Trigger lifecycle hooks
	if lp, ok := p.(LifecyclePlugin); ok {
		if err := lp.OnHook(ctx, HookInit, config); err != nil {
			er.Registry.logger.Warn("lifecycle hook failed", "plugin", manifest.Name, "hook", HookInit, "error", err)
		}
		if err := lp.OnHook(ctx, HookRegisterTools, nil); err != nil {
			er.Registry.logger.Warn("lifecycle hook failed", "plugin", manifest.Name, "hook", HookRegisterTools, "error", err)
		}
	}

	return nil
}

// UnregisterWithLifecycle unregisters a plugin with lifecycle hook support.
func (er *ExtendedRegistry) UnregisterWithLifecycle(ctx context.Context, name string) error {
	// Get plugin before unregistering
	er.Registry.mu.RLock()
	entry, exists := er.Registry.plugins[name]
	er.Registry.mu.RUnlock()

	if exists {
		if lp, ok := entry.plugin.(LifecyclePlugin); ok {
			if err := lp.OnHook(ctx, HookShutdown, nil); err != nil {
				er.Registry.logger.Warn("shutdown hook failed", "plugin", name, "error", err)
			}
		}
	}

	err := er.Registry.Unregister(name)
	er.sandbox.Revoke(name)

	er.mu.Lock()
	delete(er.manifests, name)
	er.mu.Unlock()

	return err
}

// GetManifest returns the manifest for a registered plugin.
func (er *ExtendedRegistry) GetManifest(name string) *Manifest {
	er.mu.RLock()
	defer er.mu.RUnlock()
	return er.manifests[name]
}

// Sandbox returns the security sandbox.
func (er *ExtendedRegistry) Sandbox() *SecuritySandbox {
	return er.sandbox
}

// Sidecars returns the sidecar manager.
func (er *ExtendedRegistry) Sidecars() *SidecarManager {
	return er.sidecarManager
}
