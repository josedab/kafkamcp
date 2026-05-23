package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"
)

// ===================== NewProvider: disabled config returns noop =====================

func TestNewProvider_DisabledReturnsNoop(t *testing.T) {
	t.Parallel()
	p, err := NewProvider(context.Background(), Config{Enabled: false}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.tp != nil {
		t.Error("expected nil TracerProvider for disabled config")
	}
	if p.tracer == nil {
		t.Error("expected non-nil noop tracer")
	}
}

// ===================== NewProvider: sample rate variants =====================

func TestNewProvider_SampleRateZero(t *testing.T) {
	t.Parallel()
	cfg := Config{Enabled: true, Endpoint: "localhost:14318", Insecure: true, SampleRate: 0}
	p, err := NewProvider(context.Background(), cfg, slog.Default())
	if err != nil {
		t.Skipf("skipping: %v", err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	if p.tp == nil {
		t.Error("expected non-nil TracerProvider (sample rate 0 should default to 1.0)")
	}
}

func TestNewProvider_SampleRateHalf(t *testing.T) {
	t.Parallel()
	cfg := Config{Enabled: true, Endpoint: "localhost:14318", Insecure: true, SampleRate: 0.5}
	p, err := NewProvider(context.Background(), cfg, slog.Default())
	if err != nil {
		t.Skipf("skipping: %v", err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	if p.tp == nil {
		t.Error("expected non-nil TracerProvider")
	}
}

func TestNewProvider_SampleRateFull(t *testing.T) {
	t.Parallel()
	cfg := Config{Enabled: true, Endpoint: "localhost:14318", Insecure: true, SampleRate: 1.0}
	p, err := NewProvider(context.Background(), cfg, slog.Default())
	if err != nil {
		t.Skipf("skipping: %v", err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	if p.tp == nil {
		t.Error("expected non-nil TracerProvider")
	}
}

// ===================== Shutdown: nil TracerProvider =====================

func TestShutdown_NilTP(t *testing.T) {
	t.Parallel()
	p := &Provider{
		tracer: noop.NewTracerProvider().Tracer("test"),
		logger: slog.Default(),
	}
	err := p.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("expected nil error for nil TP, got: %v", err)
	}
}

// ===================== Shutdown: with TracerProvider =====================

func TestShutdown_WithTP(t *testing.T) {
	t.Parallel()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	p := &Provider{
		tp:     tp,
		tracer: tp.Tracer("test"),
		logger: slog.Default(),
	}
	err := p.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ===================== StartToolSpan: attributes =====================

func TestStartToolSpan_Attributes(t *testing.T) {
	t.Parallel()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	p := &Provider{
		tp:     tp,
		tracer: tp.Tracer("kafkamcp"),
		logger: slog.Default(),
	}
	defer func() { _ = p.Shutdown(context.Background()) }()

	ctx, span := p.StartToolSpan(context.Background(), "kafka_list_topics", "agent-1", "prod")
	if ctx == nil {
		t.Error("expected non-nil context")
	}
	span.End()

	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected at least one span")
	}
	s := spans[0]
	if s.Name != "tool/kafka_list_topics" {
		t.Errorf("expected span name 'tool/kafka_list_topics', got %q", s.Name)
	}

	attrMap := make(map[string]string)
	for _, attr := range s.Attributes {
		attrMap[string(attr.Key)] = attr.Value.AsString()
	}
	if attrMap["mcp.tool"] != "kafka_list_topics" {
		t.Errorf("expected mcp.tool='kafka_list_topics', got %q", attrMap["mcp.tool"])
	}
	if attrMap["mcp.agent_id"] != "agent-1" {
		t.Errorf("expected mcp.agent_id='agent-1', got %q", attrMap["mcp.agent_id"])
	}
	if attrMap["kafka.cluster"] != "prod" {
		t.Errorf("expected kafka.cluster='prod', got %q", attrMap["kafka.cluster"])
	}
}

// ===================== SetSpanResult =====================

func TestSetSpanResult_Success(t *testing.T) {
	t.Parallel()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "test-op")
	SetSpanResult(span, "success", 42, nil)
	span.End()

	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected at least one span")
	}
	attrMap := make(map[string]any)
	for _, attr := range spans[0].Attributes {
		attrMap[string(attr.Key)] = attr.Value.AsInterface()
	}
	if attrMap["mcp.status"] != "success" {
		t.Errorf("expected mcp.status='success', got %v", attrMap["mcp.status"])
	}
	if attrMap["mcp.result_count"] != int64(42) {
		t.Errorf("expected mcp.result_count=42, got %v", attrMap["mcp.result_count"])
	}
}

func TestSetSpanResult_WithError(t *testing.T) {
	t.Parallel()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("test").Start(context.Background(), "test-op")
	testErr := errors.New("connection refused")
	SetSpanResult(span, "error", 0, testErr)
	span.End()

	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected at least one span")
	}
	if len(spans[0].Events) == 0 {
		t.Error("expected error event recorded")
	}
}

// ===================== Provider.Tracer() =====================

func TestProvider_Tracer_NotNil(t *testing.T) {
	t.Parallel()
	p, _ := NewProvider(context.Background(), Config{Enabled: false}, slog.Default())
	if p.Tracer() == nil {
		t.Error("expected non-nil tracer")
	}
}
