// Command readiness evaluates the KafkaMCP v1 readiness gates and exits
// non-zero when any mandatory gate fails.
//
// Usage:
//
//	readiness            # human-readable report (default)
//	readiness -json      # JSON report
//	readiness -dir /path # specify project root (default: current directory)
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	"github.com/josedab/kafkamcp/internal/readiness"
)

func main() {
	jsonOut := flag.Bool("json", false, "output report as JSON")
	projectDir := flag.String("dir", "", "project root directory (default: current working directory)")
	timeout := flag.Duration("timeout", 15*time.Minute, "per-gate evaluation timeout")
	flag.Parse()

	dir := *projectDir
	if dir == "" {
		var err error
		dir, err = os.Getwd()
		if err != nil {
			fmt.Fprintf(os.Stderr, "readiness: could not determine working directory: %v\n", err)
			os.Exit(2)
		}
	}
	if err := os.Chdir(dir); err != nil {
		fmt.Fprintf(os.Stderr, "readiness: could not enter project directory: %v\n", err)
		os.Exit(2)
	}
	dir = "."

	runner := &realRunner{}
	fs := &osFS{}
	reporter := readiness.NewReporter(runner, fs, dir,
		readiness.WithTimeout(*timeout),
	)

	ctx := context.Background()
	report := reporter.Evaluate(ctx)

	var w io.Writer = os.Stdout
	if *jsonOut {
		if err := readiness.WriteJSON(w, report); err != nil {
			fmt.Fprintf(os.Stderr, "readiness: JSON encode error: %v\n", err)
			os.Exit(2)
		}
	} else {
		if err := readiness.WriteHuman(w, report); err != nil {
			fmt.Fprintf(os.Stderr, "readiness: output error: %v\n", err)
			os.Exit(2)
		}
	}

	if len(report.MandatoryFailed()) > 0 {
		os.Exit(1)
	}
}

type realRunner struct{}

func (r *realRunner) Run(ctx context.Context, name string, args ...string) (string, int, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return string(out), exitErr.ExitCode(), nil
		}
		return string(out), -1, err
	}
	return string(out), 0, nil
}

type osFS struct{}

func (f *osFS) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}
