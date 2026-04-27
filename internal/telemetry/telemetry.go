// Package telemetry provides OpenTelemetry integration for KafkaMCP.
// It wraps tool handlers with trace spans and supports configurable OTLP export.
package telemetry

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Config holds OpenTelemetry configuration.
type Config struct {
	Enabled      bool   `yaml:"enabled" json:"enabled"`
	Endpoint     string `yaml:"endpoint" json:"endpoint"`         // OTLP endpoint (e.g., "localhost:4318")
	Insecure     bool   `yaml:"insecure" json:"insecure"`         // Use HTTP instead of HTTPS
	ServiceName  string `yaml:"service_name" json:"service_name"` // Service name for traces
	SampleRate   float64 `yaml:"sample_rate" json:"sample_rate"`  // 0.0-1.0, default 1.0
}

// Provider wraps the OTel trace provider and exposes a tracer.
type Provider struct {
	tp     *sdktrace.TracerProvider
	tracer trace.Tracer
	logger *slog.Logger
}

// NewProvider creates an OTel trace provider with OTLP HTTP exporter.
func NewProvider(ctx context.Context, cfg Config, logger *slog.Logger) (*Provider, error) {
	if !cfg.Enabled {
		return &Provider{
			tracer: trace.NewNoopTracerProvider().Tracer("kafkamcp"),
			logger: logger,
		}, nil
	}

	serviceName := cfg.ServiceName
	if serviceName == "" {
		serviceName = "kafkamcp"
	}

	sampleRate := cfg.SampleRate
	if sampleRate <= 0 {
		sampleRate = 1.0
	}

	opts := []otlptracehttp.Option{}
	if cfg.Endpoint != "" {
		opts = append(opts, otlptracehttp.WithEndpoint(cfg.Endpoint))
	}
	if cfg.Insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}

	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating OTLP exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("1.0.0"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(sampleRate)),
	)

	otel.SetTracerProvider(tp)
	tracer := tp.Tracer("kafkamcp")

	logger.Info("OpenTelemetry tracing enabled",
		"endpoint", cfg.Endpoint,
		"service", serviceName,
		"sample_rate", sampleRate,
	)

	return &Provider{
		tp:     tp,
		tracer: tracer,
		logger: logger,
	}, nil
}

// Tracer returns the OTel tracer.
func (p *Provider) Tracer() trace.Tracer {
	return p.tracer
}

// Shutdown gracefully shuts down the trace provider.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p.tp != nil {
		return p.tp.Shutdown(ctx)
	}
	return nil
}

// StartToolSpan creates a span for a tool invocation with standard attributes.
func (p *Provider) StartToolSpan(ctx context.Context, toolName, agentID, cluster string) (context.Context, trace.Span) {
	return p.tracer.Start(ctx, fmt.Sprintf("tool/%s", toolName),
		trace.WithAttributes(
			attribute.String("mcp.tool", toolName),
			attribute.String("mcp.agent_id", agentID),
			attribute.String("kafka.cluster", cluster),
		),
	)
}

// SetSpanResult sets success/error attributes on a span.
func SetSpanResult(span trace.Span, status string, count int, err error) {
	span.SetAttributes(
		attribute.String("mcp.status", status),
		attribute.Int("mcp.result_count", count),
	)
	if err != nil {
		span.RecordError(err)
	}
}
