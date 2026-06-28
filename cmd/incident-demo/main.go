// Command incident-demo sets up, verifies, and tears down a prefixed Kafka
// scenario for incident-triage demonstrations.
//
// Usage:
//
//	incident-demo setup    # create topics and populate with demo data
//	incident-demo verify   # check topics and message counts
//	incident-demo cleanup  # delete only demo-prefixed topics
//
// Flags:
//
//	-brokers   kafka bootstrap servers (default: localhost:9092)
//	-prefix    topic prefix (default: incident-demo.)
//	-json      emit machine-readable JSON output
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

func main() {
	os.Exit(run())
}

func run() int {
	brokers := flag.String("brokers", "localhost:9092", "Kafka bootstrap servers (comma-separated)")
	prefix := flag.String("prefix", "incident-demo.", "Topic prefix; cleanup is restricted to this prefix")
	jsonOut := flag.Bool("json", false, "output results as JSON")
	verbose := flag.Bool("v", false, "verbose logging")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: incident-demo [flags] <command>\n\n")
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  setup    Create demo topics and populate with scenario data\n")
		fmt.Fprintf(os.Stderr, "  verify   Check that topics and messages exist\n")
		fmt.Fprintf(os.Stderr, "  cleanup  Delete all topics matching the prefix\n")
		fmt.Fprintf(os.Stderr, "\nFlags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "incident-demo: command required (setup|verify|cleanup)")
		flag.Usage()
		return 2
	}
	command := strings.ToLower(args[0])

	logLevel := slog.LevelInfo
	if *verbose {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))

	if err := ValidatePrefix(*prefix); err != nil {
		fmt.Fprintf(os.Stderr, "incident-demo: invalid prefix: %v\n", err)
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	s, err := New(Config{
		Prefix:           *prefix,
		BootstrapServers: *brokers,
		Logger:           logger,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "incident-demo: cannot connect to Kafka: %v\n", err)
		return 1
	}
	defer s.Close()

	switch command {
	case "setup":
		if err := s.Setup(ctx); err != nil {
			emitError(*jsonOut, "setup failed", err)
			return 1
		}
		emitSuccess(*jsonOut, "setup", map[string]any{
			"prefix":  s.Prefix(),
			"brokers": *brokers,
			"message": "Demo scenario is ready. Run 'incident-demo verify' to confirm.",
		})

	case "verify":
		result, err := s.Verify(ctx)
		if err != nil {
			emitError(*jsonOut, "verify failed", err)
			return 1
		}
		if *jsonOut {
			printJSON(result)
		} else {
			printVerifyResult(result)
		}
		if !result.OK {
			return 1
		}

	case "cleanup":
		deleted, err := s.Cleanup(ctx)
		if err != nil {
			emitError(*jsonOut, "cleanup failed", err)
			return 1
		}
		emitSuccess(*jsonOut, "cleanup", map[string]any{
			"deleted": deleted,
			"count":   len(deleted),
			"prefix":  s.Prefix(),
		})

	default:
		fmt.Fprintf(os.Stderr, "incident-demo: unknown command %q (want: setup|verify|cleanup)\n", command)
		flag.Usage()
		return 2
	}
	return 0
}

func emitError(jsonMode bool, msg string, err error) {
	if jsonMode {
		printJSON(map[string]any{
			"ok":    false,
			"error": fmt.Sprintf("%s: %v", msg, err),
		})
	} else {
		fmt.Fprintf(os.Stderr, "incident-demo ERROR: %s: %v\n", msg, err)
	}
}

func emitSuccess(jsonMode bool, op string, data map[string]any) {
	if jsonMode {
		data["ok"] = true
		data["operation"] = op
		printJSON(data)
	} else {
		fmt.Printf("incident-demo %s: OK\n", op)
		for k, v := range data {
			if k != "ok" && k != "operation" {
				fmt.Printf("  %s: %v\n", k, v)
			}
		}
	}
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		fmt.Fprintf(os.Stderr, "json encode error: %v\n", err)
	}
}

func printVerifyResult(r *VerifyResult) {
	fmt.Printf("incident-demo verify: prefix=%s\n", r.Prefix)
	if r.OK {
		fmt.Println("  Status: OK — topics, scenario records, and consumer lag are present")
	} else {
		fmt.Printf("  Status: FAIL — missing_topics=%d total_lag=%d\n", len(r.Missing), r.TotalLag)
	}
	fmt.Printf("\n  Lag scenario:\n    group=%s total_lag=%d\n", r.LagGroup, r.TotalLag)
	fmt.Printf("\n  Topics present (%d):\n", len(r.Present))
	for _, t := range r.Present {
		fmt.Printf("    %-45s  partitions=%d\n", t.Name, t.Partitions)
	}
	if len(r.Missing) > 0 {
		fmt.Printf("\n  Topics missing (%d):\n", len(r.Missing))
		for _, name := range r.Missing {
			fmt.Printf("    %s\n", name)
		}
	}
	if len(r.MessageCounts) > 0 {
		fmt.Printf("\n  Message counts (approximate end offsets):\n")
		for _, mc := range r.MessageCounts {
			fmt.Printf("    %-45s  messages~%d\n", mc.Topic, mc.Count)
		}
	}
}
