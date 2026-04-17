package plugin

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// Plugin defines the interface that all KafkaMCP plugins must implement.
type Plugin interface {
	// Name returns the unique plugin identifier.
	Name() string
	// Version returns the plugin version string.
	Version() string
	// Description returns a human-readable description.
	Description() string
	// Init is called when the plugin is loaded.
	Init(ctx context.Context, config map[string]any) error
	// Close is called when the plugin is unloaded.
	Close() error
}

// ToolProvider is an optional interface for plugins that provide MCP tools.
type ToolProvider interface {
	// Tools returns the list of tool definitions this plugin provides.
	Tools() []ToolDefinition
}

// ResourceProvider is an optional interface for plugins that provide MCP resources.
type ResourceProvider interface {
	// Resources returns the list of resource definitions this plugin provides.
	Resources() []ResourceDefinition
}

// ToolDefinition describes a tool provided by a plugin.
type ToolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters,omitempty"`
}

// ResourceDefinition describes a resource provided by a plugin.
type ResourceDefinition struct {
	URI         string `json:"uri"`
	Name        string `json:"name"`
	Description string `json:"description"`
	MIMEType    string `json:"mime_type"`
}

// PluginInfo contains metadata about a registered plugin.
type PluginInfo struct {
	Name        string             `json:"name"`
	Version     string             `json:"version"`
	Description string             `json:"description"`
	Status      string             `json:"status"` // "loaded", "error", "unloaded"
	LoadedAt    string             `json:"loaded_at"`
	Tools       []ToolDefinition   `json:"tools,omitempty"`
	Resources   []ResourceDefinition `json:"resources,omitempty"`
}

// Registry manages plugin registration, lifecycle, and discovery.
type Registry struct {
	mu      sync.RWMutex
	plugins map[string]*pluginEntry
	logger  *slog.Logger
}

type pluginEntry struct {
	plugin Plugin
	info   PluginInfo
}

// NewRegistry creates a new plugin registry.
func NewRegistry(logger *slog.Logger) *Registry {
	return &Registry{
		plugins: make(map[string]*pluginEntry),
		logger:  logger,
	}
}

// Register loads and initializes a plugin.
func (r *Registry) Register(ctx context.Context, p Plugin, config map[string]any) error {
	if p == nil {
		return fmt.Errorf("plugin cannot be nil")
	}

	name := p.Name()
	if name == "" {
		return fmt.Errorf("plugin name cannot be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.plugins[name]; exists {
		return fmt.Errorf("plugin %q already registered", name)
	}

	info := PluginInfo{
		Name:        name,
		Version:     p.Version(),
		Description: p.Description(),
		Status:      "loaded",
		LoadedAt:    time.Now().UTC().Format(time.RFC3339),
	}

	if tp, ok := p.(ToolProvider); ok {
		info.Tools = tp.Tools()
	}
	if rp, ok := p.(ResourceProvider); ok {
		info.Resources = rp.Resources()
	}

	if err := p.Init(ctx, config); err != nil {
		info.Status = "error"
		r.plugins[name] = &pluginEntry{plugin: p, info: info}
		return fmt.Errorf("initializing plugin %q: %w", name, err)
	}

	r.plugins[name] = &pluginEntry{plugin: p, info: info}
	if r.logger != nil {
		r.logger.Info("plugin registered", "name", name, "version", p.Version())
	}
	return nil
}

// Unregister shuts down and removes a plugin.
func (r *Registry) Unregister(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.plugins[name]
	if !ok {
		return fmt.Errorf("plugin %q not found", name)
	}

	if err := entry.plugin.Close(); err != nil {
		if r.logger != nil {
			r.logger.Warn("error closing plugin", "name", name, "error", err)
		}
	}

	entry.info.Status = "unloaded"
	delete(r.plugins, name)
	return nil
}

// Get returns plugin info by name.
func (r *Registry) Get(name string) (*PluginInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, ok := r.plugins[name]
	if !ok {
		return nil, fmt.Errorf("plugin %q not found", name)
	}
	return &entry.info, nil
}

// List returns all registered plugins sorted by name.
func (r *Registry) List() []PluginInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	infos := make([]PluginInfo, 0, len(r.plugins))
	for _, entry := range r.plugins {
		infos = append(infos, entry.info)
	}

	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Name < infos[j].Name
	})
	return infos
}

// Count returns the number of registered plugins.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.plugins)
}

// Close shuts down all registered plugins.
func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for name, entry := range r.plugins {
		if err := entry.plugin.Close(); err != nil && r.logger != nil {
			r.logger.Warn("error closing plugin", "name", name, "error", err)
		}
		delete(r.plugins, name)
	}
}
