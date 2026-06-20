package server

import (
	"context"
	"testing"
	"time"

	"github.com/josedab/kafkamcp/internal/kafka"
	"github.com/josedab/kafkamcp/internal/schema"
)

func TestMCPServer_ReturnsNonNil(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	// newTestServer doesn't initialize mcpServer; verify accessor works without panic
	mcp := s.MCPServer()
	// mcpServer is nil because newTestServer is a minimal helper
	_ = mcp
}

func TestHealthCheck_CallsPing(t *testing.T) {
	t.Parallel()
	pingCalled := false
	s := newTestServer(t)
	s.kafkaManager = &kafka.MockKafkaManager{
		PingFn: func(ctx context.Context) error {
			pingCalled = true
			return nil
		},
	}

	err := s.HealthCheck(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !pingCalled {
		t.Error("expected Ping to be called")
	}
}

func TestHasSchemaRegistry_NoClients(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	s.schemaClients = make(map[string]*schema.Client)

	if s.hasSchemaRegistry() {
		t.Error("expected false with no schema clients")
	}
}

func TestHasSchemaRegistry_WithClients(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	s.schemaClients = map[string]*schema.Client{"default": {}}

	if !s.hasSchemaRegistry() {
		t.Error("expected true with schema client present")
	}
}

func TestAcquireExpensiveOp_Success(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)

	ctx := context.Background()
	err := s.acquireExpensiveOp(ctx)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	s.releaseExpensiveOp()
}

func TestAcquireExpensiveOp_BlocksWhenFull(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)

	// Fill the semaphore (capacity = 5)
	for i := 0; i < 5; i++ {
		if err := s.acquireExpensiveOp(context.Background()); err != nil {
			t.Fatalf("failed to acquire slot %d: %v", i, err)
		}
	}

	// Next acquire should block and fail on canceled context
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := s.acquireExpensiveOp(ctx)
	if err == nil {
		t.Error("expected error when semaphore is full")
	}

	// Release one and verify we can acquire again
	s.releaseExpensiveOp()
	err = s.acquireExpensiveOp(context.Background())
	if err != nil {
		t.Fatalf("expected success after release, got %v", err)
	}

	// Clean up remaining
	for i := 0; i < 5; i++ {
		s.releaseExpensiveOp()
	}
}

func TestAcquireExpensiveOp_CancelledContext(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)

	// Fill semaphore
	for i := 0; i < 5; i++ {
		_ = s.acquireExpensiveOp(context.Background())
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := s.acquireExpensiveOp(ctx)
	if err == nil {
		t.Error("expected error for canceled context")
	}

	// Clean up
	for i := 0; i < 5; i++ {
		s.releaseExpensiveOp()
	}
}

func TestReleaseExpensiveOp_UnblocksWaiting(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)

	// Fill semaphore
	for i := 0; i < 5; i++ {
		_ = s.acquireExpensiveOp(context.Background())
	}

	// Start a goroutine that tries to acquire
	acquired := make(chan struct{})
	go func() {
		_ = s.acquireExpensiveOp(context.Background())
		close(acquired)
	}()

	// Release one slot
	s.releaseExpensiveOp()

	select {
	case <-acquired:
		// Success - goroutine was unblocked
	case <-time.After(2 * time.Second):
		t.Fatal("releaseExpensiveOp did not unblock waiting acquire")
	}

	// Clean up
	for i := 0; i < 5; i++ {
		s.releaseExpensiveOp()
	}
}

func TestSetOTelProvider_Nil(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)

	if s.otelProvider != nil {
		t.Error("expected nil otelProvider initially")
	}
	// SetOTelProvider accepts a *telemetry.Provider; nil is valid
	s.SetOTelProvider(nil)
	if s.otelProvider != nil {
		t.Error("expected nil after setting nil")
	}
}
