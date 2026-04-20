// Example Go SDK agent that interacts with KafkaMCP.
//
// Usage:
//
//	# Start KafkaMCP with stdio transport (default)
//	kafkamcp --config kafkamcp.yaml
//
//	# Run this example (requires a running KafkaMCP server)
//	go run ./examples/go-sdk/
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/kafkamcp/kafkamcp/sdk/go/kafkamcpsdk"
)

// stdioTransport implements kafkamcpsdk.Transport over a child process stdin/stdout.
type stdioTransport struct {
	cmd    *exec.Cmd
	enc    *json.Encoder
	dec    *json.Decoder
	cancel context.CancelFunc
}

func newStdioTransport(ctx context.Context, command string, args ...string) (*stdioTransport, error) {
	ctx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Stderr = os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start kafkamcp: %w", err)
	}

	return &stdioTransport{
		cmd:    cmd,
		enc:    json.NewEncoder(stdin),
		dec:    json.NewDecoder(stdout),
		cancel: cancel,
	}, nil
}

func (t *stdioTransport) CallTool(_ context.Context, name string, args map[string]any) (json.RawMessage, error) {
	req := map[string]any{"method": "tools/call", "params": map[string]any{"name": name, "arguments": args}}
	if err := t.enc.Encode(req); err != nil {
		return nil, err
	}
	var resp json.RawMessage
	if err := t.dec.Decode(&resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (t *stdioTransport) ReadResource(_ context.Context, uri string) (json.RawMessage, error) {
	req := map[string]any{"method": "resources/read", "params": map[string]any{"uri": uri}}
	if err := t.enc.Encode(req); err != nil {
		return nil, err
	}
	var resp json.RawMessage
	if err := t.dec.Decode(&resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (t *stdioTransport) Close() error {
	t.cancel()
	return t.cmd.Wait()
}

func main() {
	configPath := "kafkamcp.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	fmt.Println("KafkaMCP Go SDK Example")
	fmt.Printf("Config: %s\n\n", configPath)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	transport, err := newStdioTransport(ctx, "kafkamcp", "--config", configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to KafkaMCP: %v\n", err)
		fmt.Fprintln(os.Stderr, "Hint: ensure kafkamcp is installed and Kafka is running (make docker-dev)")
		os.Exit(1)
	}
	defer transport.Close()

	client := kafkamcpsdk.New(transport,
		kafkamcpsdk.WithAgentID("go-sdk-example"),
		kafkamcpsdk.WithTimeout(15*time.Second),
	)

	// List all topics
	fmt.Println("Listing topics...")
	topics, err := client.ListTopics(ctx, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "ListTopics error: %v\n", err)
		os.Exit(1)
	}

	if len(topics) == 0 {
		fmt.Println("No topics found.")
	} else {
		fmt.Printf("Found %d topic(s):\n", len(topics))
		for _, t := range topics {
			fmt.Printf("  %-30s  partitions=%d  replication=%d\n", t.Name, t.Partitions, t.ReplicationFactor)
		}
	}
}
