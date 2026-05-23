package kafkaproxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"
)

func TestNewProxy(t *testing.T) {
	t.Parallel()
	cfg := DefaultProxyConfig()
	p := NewProxy(cfg, slog.Default())
	if p == nil {
		t.Fatal("NewProxy returned nil")
	}
	if p.config.ListenAddr != ":19092" {
		t.Errorf("ListenAddr = %q, want :19092", p.config.ListenAddr)
	}
}

func TestProxy_StartStop(t *testing.T) {
	t.Parallel()
	cfg := DefaultProxyConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	p := NewProxy(cfg, slog.Default())

	ctx := context.Background()
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	addr := p.ListenAddr()
	if addr == "" || addr == "127.0.0.1:0" {
		t.Fatal("expected resolved listen address")
	}

	// Verify we can connect
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	_ = conn.Close()

	if err := p.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestProxy_Stats_Initial(t *testing.T) {
	t.Parallel()
	cfg := DefaultProxyConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	p := NewProxy(cfg, slog.Default())

	stats := p.Stats()
	if stats.ActiveConnections != 0 {
		t.Errorf("ActiveConnections = %d, want 0", stats.ActiveConnections)
	}
	if stats.TotalConnections != 0 {
		t.Errorf("TotalConnections = %d, want 0", stats.TotalConnections)
	}
}

func TestProxy_Stats_AfterConnections(t *testing.T) {
	t.Parallel()
	cfg := DefaultProxyConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.ReadTimeout = 1 * time.Second
	p := NewProxy(cfg, slog.Default())

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = p.Stop() }()

	addr := p.ListenAddr()

	// Make 3 connections, then close them
	for i := 0; i < 3; i++ {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			t.Fatalf("Dial %d: %v", i, err)
		}
		_ = conn.Close()
	}

	// Wait a bit for connections to be handled
	time.Sleep(200 * time.Millisecond)

	stats := p.Stats()
	if stats.TotalConnections < 3 {
		t.Errorf("TotalConnections = %d, want >= 3", stats.TotalConnections)
	}
}

func TestProxy_Connections_Empty(t *testing.T) {
	t.Parallel()
	cfg := DefaultProxyConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	p := NewProxy(cfg, slog.Default())

	conns := p.Connections()
	if len(conns) != 0 {
		t.Errorf("Connections = %d, want 0", len(conns))
	}
}

func TestProxy_ListenAddr_BeforeStart(t *testing.T) {
	t.Parallel()
	cfg := DefaultProxyConfig()
	cfg.ListenAddr = ":12345"
	p := NewProxy(cfg, slog.Default())

	if got := p.ListenAddr(); got != ":12345" {
		t.Errorf("ListenAddr before start = %q, want :12345", got)
	}
}

// buildKafkaRequest builds a valid Kafka wire protocol request.
func buildKafkaRequest(apiKey int16, apiVersion int16, correlationID int32, clientID string, body []byte) []byte {
	var buf bytes.Buffer
	headerSize := 2 + 2 + 4 + 2 + len(clientID)
	totalSize := headerSize + len(body)
	_ = binary.Write(&buf, binary.BigEndian, int32(totalSize))
	_ = binary.Write(&buf, binary.BigEndian, apiKey)
	_ = binary.Write(&buf, binary.BigEndian, apiVersion)
	_ = binary.Write(&buf, binary.BigEndian, correlationID)
	_ = binary.Write(&buf, binary.BigEndian, int16(len(clientID)))
	buf.WriteString(clientID)
	buf.Write(body)
	return buf.Bytes()
}

func TestProxy_HandleConnection_APIVersions(t *testing.T) {
	t.Parallel()
	cfg := DefaultProxyConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.ReadTimeout = 2 * time.Second
	p := NewProxy(cfg, slog.Default())

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = p.Stop() }()

	conn, err := net.DialTimeout("tcp", p.ListenAddr(), 2*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Send APIVersions request
	req := buildKafkaRequest(APIKeyAPIVersions, 0, 42, "test-client", nil)
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Read response
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var respSize int32
	if err := binary.Read(conn, binary.BigEndian, &respSize); err != nil {
		t.Fatalf("Read response size: %v", err)
	}
	if respSize < 4 {
		t.Fatalf("response size too small: %d", respSize)
	}

	respBody := make([]byte, respSize)
	if _, err := conn.Read(respBody); err != nil {
		t.Fatalf("Read response: %v", err)
	}

	// Check correlation ID
	corrID := int32(binary.BigEndian.Uint32(respBody[0:4]))
	if corrID != 42 {
		t.Errorf("CorrelationID = %d, want 42", corrID)
	}
}

func TestProxy_HandleConnection_Produce(t *testing.T) {
	t.Parallel()
	cfg := DefaultProxyConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.ReadTimeout = 2 * time.Second
	p := NewProxy(cfg, slog.Default())

	var translatedTool string
	p.OnTranslated = func(result *TranslationResult) ([]byte, error) {
		translatedTool = result.Tool
		return []byte{0, 0, 0, 0}, nil
	}

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = p.Stop() }()

	conn, err := net.DialTimeout("tcp", p.ListenAddr(), 2*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Send Produce request with a topic name
	topicData := append([]byte{0, 5}, []byte("hello")...)
	req := buildKafkaRequest(APIKeyProduce, 0, 100, "producer-1", topicData)
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Read response
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var respSize int32
	if err := binary.Read(conn, binary.BigEndian, &respSize); err != nil {
		t.Fatalf("Read response size: %v", err)
	}

	if translatedTool != "kafka_produce" {
		t.Errorf("translated tool = %q, want kafka_produce", translatedTool)
	}

	// Verify stats
	stats := p.Stats()
	if stats.TotalRequests < 1 {
		t.Errorf("TotalRequests = %d, want >= 1", stats.TotalRequests)
	}
}

func TestProxy_HandleConnection_MalformedData(t *testing.T) {
	t.Parallel()
	cfg := DefaultProxyConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.ReadTimeout = 1 * time.Second
	p := NewProxy(cfg, slog.Default())

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = p.Stop() }()

	conn, err := net.DialTimeout("tcp", p.ListenAddr(), 2*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	// Send garbage data
	_, _ = conn.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	time.Sleep(200 * time.Millisecond)
	_ = conn.Close()
}

func TestProxy_HandleConnection_ConnectionReset(t *testing.T) {
	t.Parallel()
	cfg := DefaultProxyConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.ReadTimeout = 1 * time.Second
	p := NewProxy(cfg, slog.Default())

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = p.Stop() }()

	conn, err := net.DialTimeout("tcp", p.ListenAddr(), 2*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	// Immediately close without sending data
	_ = conn.Close()
	time.Sleep(200 * time.Millisecond)
}

func TestProxy_AcceptLoop_ContextCancellation(t *testing.T) {
	t.Parallel()
	cfg := DefaultProxyConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	p := NewProxy(cfg, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cancel()
	if err := p.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestProxy_ConcurrentConnections(t *testing.T) {
	t.Parallel()
	cfg := DefaultProxyConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.ReadTimeout = 2 * time.Second
	cfg.MaxConnections = 100
	p := NewProxy(cfg, slog.Default())

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = p.Stop() }()

	addr := p.ListenAddr()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				return
			}
			defer func() { _ = conn.Close() }()

			req := buildKafkaRequest(APIKeyAPIVersions, 0, int32(idx), "client", nil)
			_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			_, _ = conn.Write(req)

			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			resp := make([]byte, 256)
			_, _ = conn.Read(resp)
		}(i)
	}
	wg.Wait()

	stats := p.Stats()
	if stats.TotalConnections < 10 {
		t.Errorf("TotalConnections = %d, want >= 10", stats.TotalConnections)
	}
}

func TestEncodeErrorResponse(t *testing.T) {
	t.Parallel()
	resp := encodeErrorResponse(APIKeyProduce)
	if len(resp) != 4 {
		t.Errorf("response length = %d, want 4", len(resp))
	}
	// Error code should be 1
	errCode := binary.BigEndian.Uint16(resp[2:4])
	if errCode != 1 {
		t.Errorf("error code = %d, want 1", errCode)
	}
}

func TestEncodeMinimalResponse(t *testing.T) {
	t.Parallel()
	resp := encodeMinimalResponse(APIKeyFetch)
	if len(resp) != 6 {
		t.Errorf("response length = %d, want 6", len(resp))
	}
	// Error code should be 0 (bytes 4-5)
	errCode := binary.BigEndian.Uint16(resp[4:6])
	if errCode != 0 {
		t.Errorf("error code = %d, want 0", errCode)
	}
}

func TestProxy_Start_InvalidAddr(t *testing.T) {
	t.Parallel()
	cfg := DefaultProxyConfig()
	cfg.ListenAddr = "invalid-addr-99999:99999"
	p := NewProxy(cfg, slog.Default())

	err := p.Start(context.Background())
	if err == nil {
		_ = p.Stop()
		t.Fatal("expected error for invalid listen address")
	}
}

func TestProxy_Stop_AfterStart(t *testing.T) {
	t.Parallel()
	cfg := DefaultProxyConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	p := NewProxy(cfg, slog.Default())

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := p.Start(context.Background()); err != nil {
			return
		}
		_ = p.Stop()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop timed out")
	}
}
