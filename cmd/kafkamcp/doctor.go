package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/josedab/kafkamcp/internal/config"
	"github.com/josedab/kafkamcp/internal/doctor"
	"github.com/josedab/kafkamcp/internal/kafka"
	"github.com/josedab/kafkamcp/internal/schema"
)

// newDoctorCmd builds the non-destructive readiness ("doctor") command.
func newDoctorCmd() *cobra.Command {
	var jsonOut bool
	var skipConnectivity bool
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Run non-destructive readiness checks (config, identity, policy, connectivity, tool readiness)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(cfgFile)
			if err != nil {
				return fmt.Errorf("configuration validation failed: %w", err)
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
			defer cancel()

			opts := doctor.Options{}
			if !skipConnectivity {
				opts.KafkaProbe, opts.SchemaProbe = buildProbes(cfg)
			}

			report := doctor.Run(ctx, cfg, opts)

			if jsonOut {
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				if err := enc.Encode(report); err != nil {
					return err
				}
			} else {
				fmt.Println("KafkaMCP doctor")
				fmt.Print(report.Summary())
			}
			if !report.OK {
				return fmt.Errorf("doctor checks failed")
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Emit machine-readable JSON")
	cmd.Flags().BoolVar(&skipConnectivity, "skip-connectivity", false, "Skip Kafka/schema-registry connectivity probes")
	return cmd
}

// buildProbes constructs read-only connectivity probes. Building the Kafka
// manager may itself fail (e.g. broker unreachable); that failure is surfaced as
// the kafka-connectivity check rather than aborting the whole doctor run.
func buildProbes(cfg *config.Config) (doctor.Probe, doctor.Probe) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	var kafkaProbe doctor.Probe
	mgr, err := kafka.NewManager(cfg, logger)
	if err != nil {
		kafkaProbe = func(context.Context) error { return err }
	} else {
		kafkaProbe = func(ctx context.Context) error {
			defer mgr.Close()
			return mgr.Ping(ctx)
		}
	}

	var schemaProbe doctor.Probe
	for i := range cfg.Clusters {
		cl := cfg.Clusters[i]
		if cl.SchemaRegistry.URL == "" {
			continue
		}
		sc, serr := schema.NewClient(cl.SchemaRegistry.URL, cl.SchemaRegistry.Auth.Username,
			cl.SchemaRegistry.Auth.Password, cfg.SchemaCache.MaxEntries, cfg.SchemaCache.TTLSeconds)
		if serr != nil {
			schemaProbe = func(context.Context) error { return serr }
			break
		}
		schemaProbe = func(ctx context.Context) error {
			_, e := sc.ListSubjects(ctx)
			return e
		}
		break
	}
	return kafkaProbe, schemaProbe
}
