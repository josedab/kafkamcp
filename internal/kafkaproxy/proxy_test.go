package kafkaproxy

import (
	"bytes"
	"encoding/binary"
	"log/slog"
	"testing"
)

func TestAPIKeyName(t *testing.T) {
	tests := []struct {
		key  int16
		want string
	}{
		{APIKeyProduce, "Produce"},
		{APIKeyFetch, "Fetch"},
		{APIKeyMetadata, "Metadata"},
		{APIKeyAPIVersions, "ApiVersions"},
		{99, "Unknown(99)"},
	}
	for _, tt := range tests {
		if got := APIKeyName(tt.key); got != tt.want {
			t.Errorf("APIKeyName(%d) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestReadRequestHeader(t *testing.T) {
	// Build a minimal Kafka request: size(4) + api_key(2) + api_version(2) + correlation_id(4) + client_id
	var buf bytes.Buffer
	clientID := "test-client"
	bodySize := 2 + 2 + 4 + 2 + len(clientID)
	_ = binary.Write(&buf, binary.BigEndian, int32(bodySize))
	_ = binary.Write(&buf, binary.BigEndian, APIKeyMetadata)       // api_key
	_ = binary.Write(&buf, binary.BigEndian, int16(9))             // api_version
	_ = binary.Write(&buf, binary.BigEndian, int32(42))            // correlation_id
	_ = binary.Write(&buf, binary.BigEndian, int16(len(clientID))) // client_id length
	buf.WriteString(clientID)

	hdr, _, err := ReadRequestHeader(&buf)
	if err != nil {
		t.Fatalf("ReadRequestHeader: %v", err)
	}
	if hdr.APIKey != APIKeyMetadata {
		t.Errorf("APIKey = %d, want %d", hdr.APIKey, APIKeyMetadata)
	}
	if hdr.CorrelationID != 42 {
		t.Errorf("CorrelationID = %d, want 42", hdr.CorrelationID)
	}
	if hdr.ClientID != clientID {
		t.Errorf("ClientID = %q, want %q", hdr.ClientID, clientID)
	}
}

func TestWriteResponse(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte{0, 0, 0, 1}
	if err := WriteResponse(&buf, 42, payload); err != nil {
		t.Fatalf("WriteResponse: %v", err)
	}
	// Expect: size(4) + correlation_id(4) + payload(4) = 12 bytes total
	if buf.Len() != 12 {
		t.Errorf("response length = %d, want 12", buf.Len())
	}
}

func TestTranslator(t *testing.T) {
	logger := slog.Default()
	tr := NewTranslator(DefaultTranslatorConfig(), logger)

	tests := []struct {
		name     string
		apiKey   int16
		wantTool string
		wantXlat bool
	}{
		{"produce", APIKeyProduce, "kafka_produce", true},
		{"fetch", APIKeyFetch, "kafka_consume", true},
		{"metadata", APIKeyMetadata, "kafka_list_topics", true},
		{"api_versions", APIKeyAPIVersions, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hdr := &RequestHeader{APIKey: tt.apiKey, ClientID: "test"}
			result, err := tr.Translate(hdr, nil)
			if err != nil {
				t.Fatalf("Translate: %v", err)
			}
			if result.Translated != tt.wantXlat {
				t.Errorf("Translated = %v, want %v", result.Translated, tt.wantXlat)
			}
			if result.Tool != tt.wantTool {
				t.Errorf("Tool = %q, want %q", result.Tool, tt.wantTool)
			}
		})
	}
}

func TestTranslatorPolicyDeny(t *testing.T) {
	logger := slog.Default()
	cfg := TranslatorConfig{
		Policies: []Policy{
			{Name: "deny-produce", APIKeys: []int16{APIKeyProduce}, Action: PolicyDeny},
		},
	}
	tr := NewTranslator(cfg, logger)

	hdr := &RequestHeader{APIKey: APIKeyProduce, ClientID: "test"}
	_, err := tr.Translate(hdr, nil)
	if err == nil {
		t.Fatal("expected error for denied request")
	}

	stats := tr.Stats()
	if stats.DeniedByPolicy != 1 {
		t.Errorf("DeniedByPolicy = %d, want 1", stats.DeniedByPolicy)
	}
}

func TestEncodeAPIVersionsResponse(t *testing.T) {
	resp := EncodeAPIVersionsResponse()
	if len(resp) == 0 {
		t.Fatal("empty APIVersions response")
	}
	// Error code should be 0
	errCode := binary.BigEndian.Uint16(resp[0:2])
	if errCode != 0 {
		t.Errorf("error code = %d, want 0", errCode)
	}
}

func TestEncodeMetadataResponse(t *testing.T) {
	topics := []TopicMetadata{
		{Name: "test-topic", PartitionCount: 3},
	}
	resp := EncodeMetadataResponse("localhost", 9092, topics)
	if len(resp) == 0 {
		t.Fatal("empty Metadata response")
	}
}

func TestDefaultProxyConfig(t *testing.T) {
	cfg := DefaultProxyConfig()
	if cfg.ListenAddr != ":19092" {
		t.Errorf("ListenAddr = %q, want :19092", cfg.ListenAddr)
	}
	if cfg.MaxConnections != 1000 {
		t.Errorf("MaxConnections = %d, want 1000", cfg.MaxConnections)
	}
}

func TestExtractNullableString(t *testing.T) {
	tests := []struct {
		name   string
		data   []byte
		offset int
		want   string
	}{
		{"valid", append([]byte{0, 5}, []byte("hello")...), 0, "hello"},
		{"empty", []byte{0, 0}, 0, ""},
		{"too short", []byte{0}, 0, ""},
		{"null", []byte{0xFF, 0xFF}, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractNullableString(tt.data, tt.offset); got != tt.want {
				t.Errorf("extractNullableString = %q, want %q", got, tt.want)
			}
		})
	}
}
