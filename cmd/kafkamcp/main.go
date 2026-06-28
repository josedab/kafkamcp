package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/josedab/kafkamcp/internal/app"
	"github.com/josedab/kafkamcp/internal/config"
)

var (
	version  = "dev"
	cfgFile  string
	logLevel string
	debug    bool
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "kafkamcp",
		Short: "KafkaMCP — MCP server for Apache Kafka",
		Long: `KafkaMCP is a secure, vendor-neutral MCP gateway for Apache Kafka.

The default core-read pack exposes at most 15 read-only tools for topics,
messages, schemas, consumer groups, lag, DLQs, health, and discovery.
Mutating ops-write tools require a secure network transport, verified identity,
default-deny policy, and a short-lived two-person approval grant.`,
		RunE: runServer,
	}

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "kafkamcp.yaml", "Path to configuration file")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "", "Override log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().BoolVar(&debug, "debug", false, "Enable debug logging (shorthand for --log-level debug)")

	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("kafkamcp %s\n", version)
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   "validate",
		Short: "Validate the configuration file",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := config.Load(cfgFile)
			if err != nil {
				return fmt.Errorf("configuration validation failed: %w", err)
			}
			fmt.Println("Configuration is valid.")
			return nil
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   "validate-config [file]",
		Short: "Validate a configuration file (accepts path as argument or --config flag)",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := cfgFile
			if len(args) > 0 {
				path = args[0]
			}
			cfg, err := config.Load(path)
			if err != nil {
				return fmt.Errorf("configuration validation failed: %w", err)
			}
			fmt.Printf("Configuration %s is valid.\n", path)
			fmt.Printf("  Transport: %s\n", cfg.Server.Transport)
			fmt.Printf("  Clusters:  %d\n", len(cfg.Clusters))
			for i, c := range cfg.Clusters {
				fmt.Printf("    [%d] %s (%s)\n", i, c.Name, c.BootstrapServers)
			}
			if cfg.Policies.DefaultDeny {
				fmt.Printf("  Policy:    default_deny=true (%d agent rules)\n", len(cfg.Policies.Agents))
			} else {
				fmt.Printf("  Policy:    default_deny=false (open access)\n")
			}
			if cfg.Masking.Enabled {
				fmt.Printf("  Masking:   enabled (%d rules)\n", len(cfg.Masking.Rules))
			}
			return nil
		},
	})

	rootCmd.AddCommand(newDoctorCmd())

	initCmd := &cobra.Command{
		Use:   "init",
		Short: "Generate a starter configuration file",
		Long:  `Generate a minimal kafkamcp.yaml to stdout (or to a file with --output).`,
		RunE:  runConfigInit,
	}
	initCmd.Flags().StringP("output", "o", "", "Write config to file instead of stdout")
	initCmd.Flags().String("transport", "stdio", "Transport type (stdio, sse, streamable-http)")
	initCmd.Flags().String("bootstrap-servers", "localhost:9092", "Kafka bootstrap servers")
	initCmd.Flags().String("schema-registry", "", "Schema Registry URL (optional)")
	initCmd.Flags().Bool("claude-desktop", false, "Output a claude_desktop_config.json snippet for Claude Desktop integration")
	rootCmd.AddCommand(initCmd)

	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the MCP server (alias for root command)",
		Long:  `Start the KafkaMCP server. This is equivalent to running kafkamcp without a subcommand.`,
		RunE:  runServer,
	}
	rootCmd.AddCommand(serveCmd)

	devCmd := &cobra.Command{
		Use:   "dev",
		Short: "Start KafkaMCP for local read-only development",
		Long: `Start KafkaMCP with the configured core-read surface.

For a deterministic lag, DLQ, and schema-drift scenario, run
'make incident-demo-up incident-demo-setup' before starting this command.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("KafkaMCP development mode (core-read)")
			fmt.Println("Use 'make incident-demo-up incident-demo-setup' to seed the local triage scenario.")
			return runServer(cmd, args)
		},
	}
	rootCmd.AddCommand(devCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runConfigInit(cmd *cobra.Command, args []string) error {
	transport, err := cmd.Flags().GetString("transport")
	if err != nil {
		return fmt.Errorf("reading flag: %w", err)
	}
	bootstrap, err := cmd.Flags().GetString("bootstrap-servers")
	if err != nil {
		return fmt.Errorf("reading flag: %w", err)
	}
	schemaReg, err := cmd.Flags().GetString("schema-registry")
	if err != nil {
		return fmt.Errorf("reading flag: %w", err)
	}
	output, err := cmd.Flags().GetString("output")
	if err != nil {
		return fmt.Errorf("reading flag: %w", err)
	}
	claudeDesktop, err := cmd.Flags().GetBool("claude-desktop")
	if err != nil {
		return fmt.Errorf("reading flag: %w", err)
	}

	// Handle --claude-desktop: output Claude Desktop config JSON
	if claudeDesktop {
		return runClaudeDesktopInit(cmd, bootstrap, output)
	}

	switch transport {
	case "stdio", "sse", "streamable-http":
	default:
		return fmt.Errorf("unsupported transport %q; must be: stdio, sse, streamable-http", transport)
	}

	var schemaCfg string
	if schemaReg != "" {
		schemaCfg = fmt.Sprintf("\n    schema_registry:\n      url: %s", schemaReg)
	}

	var securityCfg string
	if transport != "stdio" {
		securityCfg = `

policies:
  default_deny: true
  identity:
    provider: static
    static:
      tokens:
        - token: ${KAFKAMCP_AGENT_TOKEN}
          agent_id: local-operator
  agents:
    - id: local-operator
      role: admin
      topics:
        - pattern: "*"
          permissions: [read]
      consumer_groups:
        permissions: [read, describe]
      schemas:
        permissions: [read]
`
	}

	cfg := fmt.Sprintf(`# KafkaMCP configuration — generated by kafkamcp init
# Validate with: kafkamcp validate --config <file>
server:
  transport: %s
  host: 0.0.0.0
  port: 3100
  log_level: info

clusters:
  - name: local
    bootstrap_servers: %s%s
%s`, transport, bootstrap, schemaCfg, securityCfg)

	if output != "" {
		if err := os.WriteFile(output, []byte(cfg), 0o600); err != nil {
			return fmt.Errorf("writing config to %s: %w", output, err)
		}
		fmt.Fprintf(os.Stderr, "Config written to %s\n", output)
		return nil
	}

	fmt.Print(cfg)
	return nil
}

func runClaudeDesktopInit(cmd *cobra.Command, bootstrap, output string) error {
	// Find the kafkamcp binary path
	binaryPath, err := os.Executable()
	if err != nil {
		binaryPath = "kafkamcp"
	}

	claudeConfig := map[string]any{
		"mcpServers": map[string]any{
			"kafkamcp": map[string]any{
				"command": binaryPath,
				"args":    []string{"--config", "kafkamcp.yaml"},
				"env": map[string]string{
					"KAFKA_BROKERS": bootstrap,
				},
			},
		},
	}

	data, err := json.MarshalIndent(claudeConfig, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	if output != "" {
		if err := os.WriteFile(output, data, 0o600); err != nil {
			return fmt.Errorf("writing config to %s: %w", output, err)
		}
		fmt.Fprintf(os.Stderr, "Claude Desktop config written to %s\n", output)
		return nil
	}

	fmt.Println(string(data))
	return nil
}

func runServer(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("loading configuration: %w", err)
	}

	app.ApplyOverrides(cfg, logLevel, debug)
	logger := app.NewLogger(cfg, os.Stderr)
	logger.Info("starting KafkaMCP", "version", version, "transport", cfg.Server.Transport)

	return app.Run(cmd.Context(), cfg, version, logger, app.DefaultDeps())
}
