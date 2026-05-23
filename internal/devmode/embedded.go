package devmode

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"
)

// BrokerType identifies the embedded broker implementation.
type BrokerType string

// BrokerKRaft and related values define constants for this package.
const (
	BrokerKRaft    BrokerType = "kraft"
	BrokerRedpanda BrokerType = "redpanda"
)

// EmbeddedBrokerConfig configures the embedded broker.
type EmbeddedBrokerConfig struct {
	Type       BrokerType `json:"type" yaml:"type"`
	DataDir    string     `json:"data_dir" yaml:"data_dir"`
	Port       int        `json:"port" yaml:"port"`
	AutoPort   bool       `json:"auto_port" yaml:"auto_port"`
	LogDir     string     `json:"log_dir" yaml:"log_dir"`
	BinaryPath string     `json:"binary_path,omitempty" yaml:"binary_path,omitempty"`
}

// DefaultEmbeddedBrokerConfig returns sensible defaults.
func DefaultEmbeddedBrokerConfig() EmbeddedBrokerConfig {
	homeDir, _ := os.UserHomeDir()
	return EmbeddedBrokerConfig{
		Type:     BrokerKRaft,
		DataDir:  filepath.Join(homeDir, ".kafkamcp", "data"),
		LogDir:   filepath.Join(homeDir, ".kafkamcp", "logs"),
		Port:     0, // auto-allocate
		AutoPort: true,
	}
}

// EmbeddedBrokerStatus represents the broker state.
type EmbeddedBrokerStatus struct {
	Running   bool       `json:"running"`
	Type      BrokerType `json:"type"`
	Address   string     `json:"address,omitempty"`
	Port      int        `json:"port,omitempty"`
	PID       int        `json:"pid,omitempty"`
	DataDir   string     `json:"data_dir"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	Uptime    string     `json:"uptime,omitempty"`
	DiskUsage string     `json:"disk_usage,omitempty"`
}

// EmbeddedBroker manages a local Kafka broker subprocess.
type EmbeddedBroker struct {
	config    EmbeddedBrokerConfig
	cmd       *exec.Cmd
	port      int
	startedAt time.Time
	running   bool
}

// NewEmbeddedBroker creates a new embedded broker manager.
func NewEmbeddedBroker(cfg EmbeddedBrokerConfig) *EmbeddedBroker {
	return &EmbeddedBroker{
		config: cfg,
	}
}

// Status returns the current broker status.
func (eb *EmbeddedBroker) Status() EmbeddedBrokerStatus {
	status := EmbeddedBrokerStatus{
		Running: eb.running,
		Type:    eb.config.Type,
		DataDir: eb.config.DataDir,
	}
	if eb.running {
		status.Address = fmt.Sprintf("localhost:%d", eb.port)
		status.Port = eb.port
		started := eb.startedAt
		status.StartedAt = &started
		status.Uptime = time.Since(eb.startedAt).Round(time.Second).String()
		if eb.cmd != nil && eb.cmd.Process != nil {
			status.PID = eb.cmd.Process.Pid
		}
	}
	status.DiskUsage = estimateDiskUsage(eb.config.DataDir)
	return status
}

// AllocatePort finds a free TCP port.
func AllocatePort() (int, error) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, fmt.Errorf("failed to allocate port: %w", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port, nil
}

// DetectBrokerBinary searches for Kafka or Redpanda binaries.
func DetectBrokerBinary() (BrokerType, string, error) {
	// Check for redpanda first (lighter weight)
	if path, err := exec.LookPath("redpanda"); err == nil {
		return BrokerRedpanda, path, nil
	}

	// Check for Kafka KRaft
	kafkaDirs := []string{
		"/opt/kafka/bin/kafka-server-start.sh",
		"/usr/local/kafka/bin/kafka-server-start.sh",
	}
	homeDir, _ := os.UserHomeDir()
	kafkaDirs = append(kafkaDirs, filepath.Join(homeDir, ".kafkamcp", "kafka", "bin", "kafka-server-start.sh"))

	for _, dir := range kafkaDirs {
		if _, err := os.Stat(dir); err == nil {
			return BrokerKRaft, dir, nil
		}
	}

	return "", "", fmt.Errorf("no Kafka or Redpanda binary found; run 'kafkamcp dev --download' to install")
}

// IDEConfig holds auto-configuration for IDE integrations.
type IDEConfig struct {
	IDE          string            `json:"ide"`
	ConfigPath   string            `json:"config_path"`
	Settings     map[string]string `json:"settings"`
	Instructions []string          `json:"instructions"`
}

// GenerateIDEConfigs generates auto-configuration for supported IDEs.
func GenerateIDEConfigs(brokerAddr string) []IDEConfig {
	configs := []IDEConfig{
		{
			IDE:        "vscode",
			ConfigPath: ".vscode/settings.json",
			Settings: map[string]string{
				"kafkamcp.brokerAddress": brokerAddr,
				"kafkamcp.transport":     "stdio",
			},
			Instructions: []string{
				"1. Install the KafkaMCP VS Code extension",
				fmt.Sprintf("2. Set kafkamcp.brokerAddress to %q in settings", brokerAddr),
				"3. Open the MCP panel and start chatting",
			},
		},
		{
			IDE:        "cursor",
			ConfigPath: ".cursor/mcp.json",
			Settings: map[string]string{
				"mcpServers.kafkamcp.command": "kafkamcp",
				"mcpServers.kafkamcp.args":    fmt.Sprintf("serve --broker %s", brokerAddr),
			},
			Instructions: []string{
				"1. Add KafkaMCP to .cursor/mcp.json",
				fmt.Sprintf("2. Configure broker address: %s", brokerAddr),
				"3. Restart Cursor to activate",
			},
		},
		{
			IDE:        "claude-desktop",
			ConfigPath: claudeConfigPath(),
			Settings: map[string]string{
				"mcpServers.kafkamcp.command": "kafkamcp",
				"mcpServers.kafkamcp.args":    fmt.Sprintf("serve --broker %s --transport stdio", brokerAddr),
			},
			Instructions: []string{
				"1. Add to Claude Desktop config (claude_desktop_config.json)",
				fmt.Sprintf("2. Set broker address to %s", brokerAddr),
				"3. Restart Claude Desktop",
			},
		},
	}
	return configs
}

func claudeConfigPath() string {
	homeDir, _ := os.UserHomeDir()
	switch runtime.GOOS {
	case "darwin":
		return filepath.Join(homeDir, "Library", "Application Support", "Claude", "claude_desktop_config.json")
	case "linux":
		return filepath.Join(homeDir, ".config", "claude", "claude_desktop_config.json")
	default:
		return filepath.Join(homeDir, "AppData", "Roaming", "Claude", "claude_desktop_config.json")
	}
}

// DevSession represents an active dev mode session.
type DevSession struct {
	ID         string      `json:"id"`
	BrokerAddr string      `json:"broker_addr"`
	Status     string      `json:"status"` // starting, ready, stopped
	Topics     []string    `json:"topics"`
	IDEConfigs []IDEConfig `json:"ide_configs"`
	StartedAt  time.Time   `json:"started_at"`
	QuickStart []string    `json:"quick_start"`
}

// StartDevSession creates a new dev mode session configuration.
func (e *Engine) StartDevSession(brokerAddr string) *DevSession {
	if brokerAddr == "" {
		brokerAddr = "localhost:9092"
	}

	topicNames := make([]string, len(e.topics))
	for i, t := range e.topics {
		topicNames[i] = t.Name
	}

	return &DevSession{
		ID:         fmt.Sprintf("dev_%d", time.Now().UnixNano()),
		BrokerAddr: brokerAddr,
		Status:     "ready",
		Topics:     topicNames,
		IDEConfigs: GenerateIDEConfigs(brokerAddr),
		StartedAt:  time.Now(),
		QuickStart: []string{
			fmt.Sprintf("Kafka broker: %s", brokerAddr),
			fmt.Sprintf("Sample topics: %d pre-populated", len(e.topics)),
			"Run: kafka_list_topics to see topics",
			"Run: kafka_consume topic=orders limit=5 to read sample data",
			"Run: kafka_dev_tutorial action=start id=getting-started",
		},
	}
}

func estimateDiskUsage(dir string) string {
	var size int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		size += info.Size()
		return nil
	})
	if size < 1024 {
		return fmt.Sprintf("%d B", size)
	}
	if size < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(size)/1024)
	}
	return fmt.Sprintf("%.1f MB", float64(size)/(1024*1024))
}
