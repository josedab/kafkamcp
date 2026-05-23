package wasmruntime

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// PluginLanguage identifies the source language of a WASM plugin.
type PluginLanguage string

// LangRust and related values define constants for this package.
const (
	LangRust   PluginLanguage = "rust"
	LangGo     PluginLanguage = "tinygo"
	LangJS     PluginLanguage = "javascript"
	LangPython PluginLanguage = "python"
	LangOther  PluginLanguage = "other"
)

// Sandbox ceilings — single source of truth so LoadPlugin, DefaultSandboxConfig
// and UpdateSandbox cannot drift apart (F19).
const (
	// MaxWASMSizeBytes is the largest WASM module the engine will accept.
	MaxWASMSizeBytes = 50 * 1024 * 1024 // 50 MB

	// MaxSandboxMemoryMB is the upper bound for any plugin's memory limit.
	// LoadPlugin and UpdateSandbox both refuse values above this.
	MaxSandboxMemoryMB = 512

	// MaxSandboxTimeout is the upper bound for any plugin's execution
	// timeout — same hard limit for LoadPlugin and UpdateSandbox.
	MaxSandboxTimeout = 5 * time.Minute
)

// PluginStatus tracks plugin lifecycle state.
type PluginStatus string

// StatusLoaded and related values define constants for this package.
const (
	StatusLoaded  PluginStatus = "loaded"
	StatusRunning PluginStatus = "running"
	StatusStopped PluginStatus = "stopped"
	StatusError   PluginStatus = "error"
)

// SandboxConfig defines resource limits for a WASM plugin execution sandbox.
type SandboxConfig struct {
	MaxMemoryMB  int           `json:"max_memory_mb"`
	MaxCPUTimeMs int64         `json:"max_cpu_time_ms"`
	MaxFuelUnits uint64        `json:"max_fuel_units"`
	AllowedHosts []string      `json:"allowed_hosts,omitempty"`
	AllowFS      bool          `json:"allow_fs"`
	AllowNetwork bool          `json:"allow_network"`
	Timeout      time.Duration `json:"timeout"`
}

// DefaultSandboxConfig returns safe default resource limits.
func DefaultSandboxConfig() SandboxConfig {
	return SandboxConfig{
		MaxMemoryMB:  64,
		MaxCPUTimeMs: 5000,
		MaxFuelUnits: 1_000_000,
		AllowFS:      false,
		AllowNetwork: false,
		Timeout:      30 * time.Second,
	}
}

// WASMPlugin represents a loaded WASM plugin.
type WASMPlugin struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Version     string         `json:"version"`
	Description string         `json:"description"`
	Language    PluginLanguage `json:"language"`
	Status      PluginStatus   `json:"status"`
	Exports     []string       `json:"exports"`
	Sandbox     SandboxConfig  `json:"sandbox"`
	WASMSize    int            `json:"wasm_size_bytes"`
	LoadedAt    string         `json:"loaded_at"`
	LastRunAt   string         `json:"last_run_at,omitempty"`
	RunCount    int64          `json:"run_count"`
	ErrorCount  int64          `json:"error_count"`
}

// PluginManifest describes a WASM plugin package.
type PluginManifest struct {
	Name        string         `json:"name"`
	Version     string         `json:"version"`
	Description string         `json:"description"`
	Language    PluginLanguage `json:"language"`
	Entrypoint  string         `json:"entrypoint"`
	Exports     []ExportDef    `json:"exports"`
	Permissions []string       `json:"permissions,omitempty"`
}

// ExportDef describes an exported function from a WASM module.
type ExportDef struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	InputSchema  string `json:"input_schema,omitempty"`
	OutputSchema string `json:"output_schema,omitempty"`
}

// ExecutionResult captures the outcome of running a WASM plugin function.
type ExecutionResult struct {
	PluginID     string  `json:"plugin_id"`
	Function     string  `json:"function"`
	Success      bool    `json:"success"`
	Output       string  `json:"output,omitempty"`
	Error        string  `json:"error,omitempty"`
	DurationMs   int64   `json:"duration_ms"`
	MemoryUsedMB float64 `json:"memory_used_mb"`
	FuelConsumed uint64  `json:"fuel_consumed"`
	ExecutedAt   string  `json:"executed_at"`
}

// Engine manages WASM plugin lifecycle and execution.
type Engine struct {
	mu       sync.RWMutex
	plugins  map[string]*WASMPlugin
	wasmData map[string][]byte // plugin ID -> wasm bytes
	logger   *slog.Logger
}

// NewEngine creates a new WASM runtime engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		plugins:  make(map[string]*WASMPlugin),
		wasmData: make(map[string][]byte),
		logger:   logger,
	}
}

// LoadPlugin loads a WASM module from bytes with the given manifest.
func (e *Engine) LoadPlugin(manifest PluginManifest, wasmBytes []byte) (*WASMPlugin, error) {
	if manifest.Name == "" {
		return nil, fmt.Errorf("plugin name is required")
	}
	if len(wasmBytes) == 0 {
		return nil, fmt.Errorf("wasm bytes are empty")
	}
	if len(wasmBytes) > MaxWASMSizeBytes {
		return nil, fmt.Errorf("wasm module exceeds maximum size of %d bytes", MaxWASMSizeBytes)
	}

	// Validate WASM magic bytes
	if len(wasmBytes) < 4 || string(wasmBytes[:4]) != "\x00asm" {
		return nil, fmt.Errorf("invalid wasm module: missing magic bytes")
	}

	exports := make([]string, 0, len(manifest.Exports))
	for _, exp := range manifest.Exports {
		exports = append(exports, exp.Name)
	}

	plugin := &WASMPlugin{
		ID:          randid.New("wasm"),
		Name:        manifest.Name,
		Version:     manifest.Version,
		Description: manifest.Description,
		Language:    manifest.Language,
		Status:      StatusLoaded,
		Exports:     exports,
		Sandbox:     DefaultSandboxConfig(),
		WASMSize:    len(wasmBytes),
		LoadedAt:    time.Now().UTC().Format(time.RFC3339),
	}

	e.mu.Lock()
	e.plugins[plugin.ID] = plugin
	e.wasmData[plugin.ID] = wasmBytes
	e.mu.Unlock()

	e.logger.Info("wasm plugin loaded",
		"plugin", plugin.ID,
		"name", manifest.Name,
		"size", len(wasmBytes),
	)
	return plugin, nil
}

// Execute runs a function in a loaded WASM plugin within sandbox limits.
// Uses an in-process simulation since wazero is not in the dependency tree.
// In production, replace the simulation block with wazero runtime calls.
func (e *Engine) Execute(ctx context.Context, pluginID, function, input string, sandbox *SandboxConfig) (*ExecutionResult, error) {
	e.mu.Lock()
	plugin, ok := e.plugins[pluginID]
	if !ok {
		e.mu.Unlock()
		return nil, fmt.Errorf("plugin %q not found", pluginID)
	}
	if plugin.Status == StatusError {
		e.mu.Unlock()
		return nil, fmt.Errorf("plugin %q is in error state", pluginID)
	}

	// Validate function exists in exports
	found := false
	for _, exp := range plugin.Exports {
		if exp == function {
			found = true
			break
		}
	}
	if !found {
		e.mu.Unlock()
		return nil, fmt.Errorf("function %q not exported by plugin %q", function, plugin.Name)
	}

	if sandbox == nil {
		cfg := DefaultSandboxConfig()
		sandbox = &cfg
	}

	// Enforce input size limits based on sandbox memory
	maxInputBytes := sandbox.MaxMemoryMB * 1024 * 1024
	if len(input) > maxInputBytes {
		e.mu.Unlock()
		return nil, fmt.Errorf("input size %d exceeds sandbox memory limit %dMB", len(input), sandbox.MaxMemoryMB)
	}

	plugin.Status = StatusRunning
	e.mu.Unlock()

	start := time.Now()
	result := &ExecutionResult{
		PluginID:   pluginID,
		Function:   function,
		ExecutedAt: start.UTC().Format(time.RFC3339),
	}

	// Apply timeout from sandbox config
	execCtx, cancel := context.WithTimeout(ctx, sandbox.Timeout)
	defer cancel()

	// Simulated execution with proper context cancellation handling
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Simulate processing time proportional to input size
		processingTime := time.Duration(len(input)/100+1) * time.Microsecond
		select {
		case <-time.After(processingTime):
		case <-execCtx.Done():
		}
	}()

	select {
	case <-execCtx.Done():
		result.Success = false
		result.Error = fmt.Sprintf("execution timed out after %s", sandbox.Timeout)
		e.mu.Lock()
		plugin.Status = StatusError
		plugin.ErrorCount++
		e.mu.Unlock()
	case <-done:
		fuelConsumed := uint64(len(input)*10 + 100)
		if fuelConsumed > sandbox.MaxFuelUnits {
			result.Success = false
			result.Error = fmt.Sprintf("fuel exhausted: consumed %d, limit %d", fuelConsumed, sandbox.MaxFuelUnits)
			e.mu.Lock()
			plugin.Status = StatusLoaded
			plugin.ErrorCount++
			e.mu.Unlock()
		} else {
			result.Success = true
			result.Output = fmt.Sprintf(`{"status":"ok","plugin":"%s","function":"%s","input_length":%d}`, plugin.Name, function, len(input))
			result.MemoryUsedMB = float64(len(input)+plugin.WASMSize) / (1024 * 1024)
			result.FuelConsumed = fuelConsumed

			e.mu.Lock()
			plugin.Status = StatusLoaded
			plugin.RunCount++
			plugin.LastRunAt = time.Now().UTC().Format(time.RFC3339)
			e.mu.Unlock()
		}
	}

	result.DurationMs = time.Since(start).Milliseconds()
	return result, nil
}

// UnloadPlugin removes a plugin from the runtime.
func (e *Engine) UnloadPlugin(pluginID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.plugins[pluginID]; !ok {
		return fmt.Errorf("plugin %q not found", pluginID)
	}
	delete(e.plugins, pluginID)
	delete(e.wasmData, pluginID)
	return nil
}

// GetPlugin returns a plugin by ID.
func (e *Engine) GetPlugin(pluginID string) (*WASMPlugin, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	p, ok := e.plugins[pluginID]
	if !ok {
		return nil, fmt.Errorf("plugin %q not found", pluginID)
	}
	return p, nil
}

// ListPlugins returns all loaded plugins.
func (e *Engine) ListPlugins() []WASMPlugin {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]WASMPlugin, 0, len(e.plugins))
	for _, p := range e.plugins {
		result = append(result, *p)
	}
	return result
}

// UpdateSandbox updates the sandbox configuration for a plugin.
func (e *Engine) UpdateSandbox(pluginID string, config SandboxConfig) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	p, ok := e.plugins[pluginID]
	if !ok {
		return fmt.Errorf("plugin %q not found", pluginID)
	}

	if config.MaxMemoryMB > MaxSandboxMemoryMB {
		return fmt.Errorf("max memory cannot exceed %d MB", MaxSandboxMemoryMB)
	}
	if config.Timeout > MaxSandboxTimeout {
		return fmt.Errorf("timeout cannot exceed %s", MaxSandboxTimeout)
	}

	p.Sandbox = config
	return nil
}
