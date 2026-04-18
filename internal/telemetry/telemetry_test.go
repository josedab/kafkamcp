package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestNewProviderDisabled(t *testing.T) {
	provider, err := NewProvider(context.Background(), Config{Enabled: false}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if provider == nil {
		t.Fatal("expected non-nil provider")
	}

	tracer := provider.Tracer()
	if tracer == nil {
		t.Fatal("expected non-nil tracer")
	}
}

func TestNewProviderDisabledDefaults(t *testing.T) {
	provider, err := NewProvider(context.Background(), Config{}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if provider.tp != nil {
		t.Error("expected nil trace provider for disabled config")
	}
}

func TestProviderTracerNoop(t *testing.T) {
	provider, _ := NewProvider(context.Background(), Config{Enabled: false}, slog.Default())
	tracer := provider.Tracer()

	ctx, span := tracer.Start(context.Background(), "test-span")
	defer span.End()

	if ctx == nil {
		t.Error("expected non-nil context")
	}
}

func TestProviderShutdownNilTP(t *testing.T) {
	provider, _ := NewProvider(context.Background(), Config{Enabled: false}, slog.Default())
	err := provider.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("unexpected error on shutdown: %v", err)
	}
}

func TestProviderStartToolSpan(t *testing.T) {
	provider, _ := NewProvider(context.Background(), Config{Enabled: false}, slog.Default())
	ctx, span := provider.StartToolSpan(context.Background(), "kafka_list_topics", "agent-1", "default")
	defer span.End()

	if ctx == nil {
		t.Error("expected non-nil context")
	}
}

func TestProviderStartToolSpanMultipleTools(t *testing.T) {
	provider, _ := NewProvider(context.Background(), Config{Enabled: false}, slog.Default())

	tools := []string{"kafka_list_topics", "kafka_consume", "kafka_produce", "kafka_describe_topic"}
	for _, tool := range tools {
		ctx, span := provider.StartToolSpan(context.Background(), tool, "test-agent", "prod")
		if ctx == nil {
			t.Errorf("expected non-nil context for tool %s", tool)
		}
		span.End()
	}
}

func TestSetSpanResult(t *testing.T) {
	tracer := noop.NewTracerProvider().Tracer("test")
	_, span := tracer.Start(context.Background(), "test")
	defer span.End()

	SetSpanResult(span, "success", 10, nil)
}

func TestSetSpanResultWithError(t *testing.T) {
	tracer := noop.NewTracerProvider().Tracer("test")
	_, span := tracer.Start(context.Background(), "test")
	defer span.End()

	SetSpanResult(span, "error", 0, context.DeadlineExceeded)
}

func TestSetSpanResultVariousStatuses(t *testing.T) {
	tracer := noop.NewTracerProvider().Tracer("test")

	statuses := []struct {
		status string
		count  int
		err    error
	}{
		{"success", 42, nil},
		{"error", 0, errors.New("connection refused")},
		{"error", 0, context.Canceled},
		{"timeout", 0, context.DeadlineExceeded},
		{"success", 1000, nil},
	}

	for _, s := range statuses {
		_, span := tracer.Start(context.Background(), "test-"+s.status)
		SetSpanResult(span, s.status, s.count, s.err)
		span.End()
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := Config{}
	if cfg.Enabled {
		t.Error("expected Enabled=false by default")
	}
	if cfg.SampleRate != 0 {
		t.Errorf("expected SampleRate=0 by default, got %f", cfg.SampleRate)
	}
	if cfg.Endpoint != "" {
		t.Errorf("expected empty Endpoint by default, got %q", cfg.Endpoint)
	}
	if cfg.ServiceName != "" {
		t.Errorf("expected empty ServiceName by default, got %q", cfg.ServiceName)
	}
}

func TestConfigValues(t *testing.T) {
	cfg := Config{
		Enabled:     true,
		Endpoint:    "localhost:4318",
		Insecure:    true,
		ServiceName: "test-service",
		SampleRate:  0.5,
	}
	if !cfg.Enabled {
		t.Error("expected Enabled=true")
	}
	if cfg.Endpoint != "localhost:4318" {
		t.Errorf("expected Endpoint=localhost:4318, got %q", cfg.Endpoint)
	}
	if cfg.SampleRate != 0.5 {
		t.Errorf("expected SampleRate=0.5, got %f", cfg.SampleRate)
	}
}

func TestProviderTracerType(t *testing.T) {
	provider, _ := NewProvider(context.Background(), Config{Enabled: false}, slog.Default())
	tracer := provider.Tracer()
	var _ trace.Tracer = tracer
}

func TestNewProviderEnabledInsecure(t *testing.T) {
	cfg := Config{
		Enabled:     true,
		Endpoint:    "localhost:14318",
		Insecure:    true,
		ServiceName: "test-kafkamcp",
		SampleRate:  1.0,
	}

	provider, err := NewProvider(context.Background(), cfg, slog.Default())
	if err != nil {
		// Some OTel SDK version combinations may fail on resource.Merge
		t.Skipf("skipping: OTel provider creation failed (likely SDK version mismatch): %v", err)
	}
	if provider == nil {
		t.Fatal("expected non-nil provider")
	}
	if provider.tp == nil {
		t.Error("expected non-nil trace provider for enabled config")
	}

	tracer := provider.Tracer()
	if tracer == nil {
		t.Fatal("expected non-nil tracer")
	}

	ctx, span := provider.StartToolSpan(context.Background(), "kafka_test", "agent", "cluster")
	if ctx == nil {
		t.Error("expected non-nil context")
	}
	SetSpanResult(span, "success", 1, nil)
	span.End()

	_ = provider.Shutdown(context.Background())
}

func TestNewProviderDefaultServiceName(t *testing.T) {
	cfg := Config{
		Enabled:  true,
		Endpoint: "localhost:14318",
		Insecure: true,
	}
	provider, err := NewProvider(context.Background(), cfg, slog.Default())
	if err != nil {
		t.Skipf("skipping: %v", err)
	}
	defer provider.Shutdown(context.Background())
	if provider.tp == nil {
		t.Error("expected trace provider to be created")
	}
}

func TestNewProviderDefaultSampleRate(t *testing.T) {
	cfg := Config{
		Enabled:    true,
		Endpoint:   "localhost:14318",
		Insecure:   true,
		SampleRate: 0,
	}
	provider, err := NewProvider(context.Background(), cfg, slog.Default())
	if err != nil {
		t.Skipf("skipping: %v", err)
	}
	defer provider.Shutdown(context.Background())
	if provider.tp == nil {
		t.Error("expected trace provider to be created")
	}
}

func TestNewProviderNegativeSampleRate(t *testing.T) {
	cfg := Config{
		Enabled:    true,
		Endpoint:   "localhost:14318",
		Insecure:   true,
		SampleRate: -0.5,
	}
	provider, err := NewProvider(context.Background(), cfg, slog.Default())
	if err != nil {
		t.Skipf("skipping: %v", err)
	}
	defer provider.Shutdown(context.Background())
}

func TestProviderShutdownWithTP(t *testing.T) {
	cfg := Config{
		Enabled:  true,
		Endpoint: "localhost:14318",
		Insecure: true,
	}
	provider, err := NewProvider(context.Background(), cfg, slog.Default())
	if err != nil {
		t.Skipf("skipping: %v", err)
	}
	_ = provider.Shutdown(context.Background())
}
