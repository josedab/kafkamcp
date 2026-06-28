package app_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/josedab/kafkamcp/internal/app"
	"github.com/josedab/kafkamcp/internal/config"
)

func TestNewMetricsServer_ListensOnPortPlusOne(t *testing.T) {
	cfg := &config.Config{Server: config.ServerConfig{Port: 3200}}
	srv := app.NewMetricsServer(cfg, &fakeAppServer{}, noopLogger())
	if srv == nil {
		t.Fatal("NewMetricsServer returned nil")
	}
	if srv.Addr != "127.0.0.1:3201" {
		t.Errorf("expected addr 127.0.0.1:3201, got %q", srv.Addr)
	}
}

func TestNewMetricsServer_DefaultPort(t *testing.T) {
	cfg := &config.Config{} // Port == 0 → defaults to 3100, metrics on 3101
	srv := app.NewMetricsServer(cfg, &fakeAppServer{}, noopLogger())
	if srv.Addr != "127.0.0.1:3101" {
		t.Errorf("expected addr 127.0.0.1:3101 (default port+1), got %q", srv.Addr)
	}
}

func TestNewMetricsServer_UsesConfiguredAddress(t *testing.T) {
	cfg := &config.Config{Server: config.ServerConfig{
		Port:        9000,
		MetricsHost: "0.0.0.0",
		MetricsPort: 9400,
	}}
	srv := app.NewMetricsServer(cfg, &fakeAppServer{}, noopLogger())
	if srv.Addr != "0.0.0.0:9400" {
		t.Fatalf("expected configured metrics address, got %q", srv.Addr)
	}
}

func TestMetricsServer_AllInterfacesReachableViaNonLoopbackAddress(t *testing.T) {
	var podIP net.IP
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	for _, addr := range addrs {
		ip, _, err := net.ParseCIDR(addr.String())
		if err == nil && ip.To4() != nil && !ip.IsLoopback() {
			podIP = ip
			break
		}
	}
	if podIP == nil {
		t.Skip("no non-loopback IPv4 address available")
	}

	probe, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	_ = probe.Close()

	cfg := &config.Config{Server: config.ServerConfig{
		MetricsHost: "0.0.0.0",
		MetricsPort: port,
	}}
	srv := app.NewMetricsServer(cfg, &fakeAppServer{}, noopLogger())
	listener, err := net.Listen("tcp4", srv.Addr)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	client := &http.Client{
		Timeout: time.Second,
		Transport: &http.Transport{
			Proxy: nil,
		},
	}
	url := fmt.Sprintf("http://%s/livez", net.JoinHostPort(podIP.String(), strconv.Itoa(port)))
	resp, err := client.Get(url)
	if err != nil {
		t.Skipf("environment does not permit dialing its non-loopback address: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s status = %d, want 200", url, resp.StatusCode)
	}
}

func TestNewMetricsServer_HandlerRegistersEndpoints(t *testing.T) {
	cfg := &config.Config{Server: config.ServerConfig{Port: 9000}}
	srv := app.NewMetricsServer(cfg, &fakeAppServer{}, noopLogger())

	for _, path := range []string{"/metrics", "/livez", "/healthz", "/ready"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", path, nil)
		srv.Handler.ServeHTTP(rec, req)
		if rec.Code == http.StatusNotFound {
			t.Errorf("path %s not registered (got 404)", path)
		}
	}
}

func TestMetricsServer_LivenessDoesNotDependOnKafka(t *testing.T) {
	cfg := &config.Config{Server: config.ServerConfig{Port: 9001}}
	srv := app.NewMetricsServer(cfg, &fakeAppServer{
		healthErr:    errors.New("kafka unavailable"),
		readinessErr: errors.New("not ready"),
	}, noopLogger())

	for path, want := range map[string]int{
		"/livez":   http.StatusOK,
		"/healthz": http.StatusServiceUnavailable,
		"/ready":   http.StatusServiceUnavailable,
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		srv.Handler.ServeHTTP(rec, req)
		if rec.Code != want {
			t.Errorf("%s status = %d, want %d", path, rec.Code, want)
		}
	}
}

func TestNewMetricsServer_TimeoutsConfigured(t *testing.T) {
	cfg := &config.Config{Server: config.ServerConfig{Port: 9002}}
	srv := app.NewMetricsServer(cfg, &fakeAppServer{}, noopLogger())
	if srv.ReadHeaderTimeout != 10*time.Second {
		t.Errorf("ReadHeaderTimeout: got %v, want 10s", srv.ReadHeaderTimeout)
	}
	if srv.ReadTimeout != 30*time.Second {
		t.Errorf("ReadTimeout: got %v, want 30s", srv.ReadTimeout)
	}
	if srv.WriteTimeout != 60*time.Second {
		t.Errorf("WriteTimeout: got %v, want 60s", srv.WriteTimeout)
	}
}

func TestMetricsServer_ShutdownCompletesPromptly(t *testing.T) {
	cfg := &config.Config{Server: config.ServerConfig{Port: 9003}}
	srv := app.NewMetricsServer(cfg, &fakeAppServer{}, noopLogger())

	// Shutdown on an unstarted *http.Server completes immediately.
	done := make(chan error, 1)
	go func() {
		ctx, cancel := newShortCtx(t, 5*time.Second)
		defer cancel()
		done <- srv.Shutdown(ctx)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Shutdown error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Shutdown did not complete within 2 s")
	}
}
