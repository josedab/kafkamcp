package app

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/josedab/kafkamcp/internal/config"
)

// NewMetricsServer builds—but does not start—the Prometheus metrics HTTP server.
// It binds to server.metrics_host:server.metrics_port, exposes /metrics,
// /livez, /healthz, and /ready, and wraps the mux with security headers.
// Metrics are rate-limited; health probes are not, so monitoring load cannot
// make Kubernetes falsely conclude the process is dead.
//
// The caller is responsible for starting the server and arranging shutdown.
func NewMetricsServer(cfg *config.Config, srv AppServer, logger *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	limiter := NewRateLimiter(10, 20) // 10 req/s, burst of 20
	metricsHandler := RateLimitMiddleware(
		promhttp.HandlerFor(srv.Metrics().Registry, promhttp.HandlerOpts{}),
		limiter,
	)
	mux.Handle("/metrics", metricsHandler)
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("alive"))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		checkCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := srv.HealthCheck(checkCtx); err != nil {
			logger.Error("health check failed", "error", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("unhealthy"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		checkCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := srv.ReadinessCheck(checkCtx); err != nil {
			logger.Warn("readiness check failed", "error", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready: " + err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

	handler := SecurityHeadersMiddleware(mux)

	return &http.Server{
		Addr:              cfg.Server.MetricsAddress(),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}
