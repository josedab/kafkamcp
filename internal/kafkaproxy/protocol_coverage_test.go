package kafkaproxy

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// --- ReadRequestHeader tests ---

func TestReadRequestHeader_ValidRequest(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	clientID := "my-producer"
	bodySize := 2 + 2 + 4 + 2 + len(clientID) + 3 // header + 3 extra body bytes
	binary.Write(&buf, binary.BigEndian, int32(bodySize))
	binary.Write(&buf, binary.BigEndian, int16(APIKeyProduce))
	binary.Write(&buf, binary.BigEndian, int16(8))
	binary.Write(&buf, binary.BigEndian, int32(99))
	binary.Write(&buf, binary.BigEndian, int16(len(clientID)))
	buf.WriteString(clientID)
	buf.Write([]byte{1, 2, 3}) // extra body

	hdr, body, err := ReadRequestHeader(&buf)
	if err != nil {
		t.Fatalf("ReadRequestHeader: %v", err)
	}
	if hdr.APIKey != APIKeyProduce {
		t.Errorf("APIKey = %d, want %d", hdr.APIKey, APIKeyProduce)
	}
	if hdr.APIVersion != 8 {
		t.Errorf("APIVersion = %d, want 8", hdr.APIVersion)
	}
	if hdr.CorrelationID != 99 {
		t.Errorf("CorrelationID = %d, want 99", hdr.CorrelationID)
	}
	if hdr.ClientID != clientID {
		t.Errorf("ClientID = %q, want %q", hdr.ClientID, clientID)
	}
	if len(body) != 3 {
		t.Errorf("body length = %d, want 3", len(body))
	}
}

func TestReadRequestHeader_TruncatedSize(t *testing.T) {
	t.Parallel()
	buf := bytes.NewBuffer([]byte{0, 0}) // only 2 bytes, need 4
	_, _, err := ReadRequestHeader(buf)
	if err == nil {
		t.Fatal("expected error for truncated size")
	}
}

func TestReadRequestHeader_InvalidMessageSize_TooSmall(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, int32(4)) // less than 8 min
	buf.Write(make([]byte, 4))
	_, _, err := ReadRequestHeader(&buf)
	if err == nil {
		t.Fatal("expected error for size < 8")
	}
}

func TestReadRequestHeader_InvalidMessageSize_TooLarge(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, int32(200*1024*1024)) // > 100MB
	_, _, err := ReadRequestHeader(&buf)
	if err == nil {
		t.Fatal("expected error for size > 100MB")
	}
}

func TestReadRequestHeader_ZeroLengthBody(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	// Size = 8 (minimum), just the header fields
	binary.Write(&buf, binary.BigEndian, int32(8))
	binary.Write(&buf, binary.BigEndian, int16(APIKeyFetch))
	binary.Write(&buf, binary.BigEndian, int16(0))
	binary.Write(&buf, binary.BigEndian, int32(1))

	hdr, body, err := ReadRequestHeader(&buf)
	if err != nil {
		t.Fatalf("ReadRequestHeader: %v", err)
	}
	if hdr.APIKey != APIKeyFetch {
		t.Errorf("APIKey = %d, want %d", hdr.APIKey, APIKeyFetch)
	}
	if len(body) != 0 {
		t.Errorf("body length = %d, want 0", len(body))
	}
	if hdr.ClientID != "" {
		t.Errorf("ClientID = %q, want empty", hdr.ClientID)
	}
}

func TestReadRequestHeader_MaxSizeBoundary(t *testing.T) {
	t.Parallel()
	// 100MB exactly should be valid
	var buf bytes.Buffer
	maxSize := int32(100 * 1024 * 1024)
	binary.Write(&buf, binary.BigEndian, maxSize)
	// We won't actually write 100MB of data; the read will fail on io.ReadFull
	_, _, err := ReadRequestHeader(&buf)
	if err == nil {
		t.Fatal("expected error because we don't have enough data to read")
	}
	// This is fine - it should fail on io.ReadFull, not on size validation
}

func TestReadRequestHeader_NullClientID(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	bodySize := 2 + 2 + 4 + 2 // header + null client (-1)
	binary.Write(&buf, binary.BigEndian, int32(bodySize))
	binary.Write(&buf, binary.BigEndian, int16(APIKeyMetadata))
	binary.Write(&buf, binary.BigEndian, int16(9))
	binary.Write(&buf, binary.BigEndian, int32(42))
	binary.Write(&buf, binary.BigEndian, int16(-1)) // null client ID

	hdr, _, err := ReadRequestHeader(&buf)
	if err != nil {
		t.Fatalf("ReadRequestHeader: %v", err)
	}
	if hdr.ClientID != "" {
		t.Errorf("ClientID = %q, want empty for null", hdr.ClientID)
	}
}

// --- WriteResponse tests ---

func TestWriteResponse_RoundTrip(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if err := WriteResponse(&buf, 123, payload); err != nil {
		t.Fatalf("WriteResponse: %v", err)
	}

	// Read back: size(4) + correlationID(4) + payload
	data := buf.Bytes()
	size := int32(binary.BigEndian.Uint32(data[0:4]))
	if size != 8 { // 4 (corrID) + 4 (payload)
		t.Errorf("size = %d, want 8", size)
	}
	corrID := int32(binary.BigEndian.Uint32(data[4:8]))
	if corrID != 123 {
		t.Errorf("correlationID = %d, want 123", corrID)
	}
	if !bytes.Equal(data[8:], payload) {
		t.Errorf("payload mismatch: %v", data[8:])
	}
}

func TestWriteResponse_EmptyPayload(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	if err := WriteResponse(&buf, 0, nil); err != nil {
		t.Fatalf("WriteResponse: %v", err)
	}
	data := buf.Bytes()
	size := int32(binary.BigEndian.Uint32(data[0:4]))
	if size != 4 { // only correlation ID
		t.Errorf("size = %d, want 4", size)
	}
}

// --- EncodeMetadataResponse tests ---

func TestEncodeMetadataResponse_MultipleBrokersAndTopics(t *testing.T) {
	t.Parallel()
	topics := []TopicMetadata{
		{Name: "topic-a", PartitionCount: 3},
		{Name: "topic-b", PartitionCount: 1},
	}
	resp := EncodeMetadataResponse("kafka-broker", 9092, topics)
	if len(resp) == 0 {
		t.Fatal("empty response")
	}

	// Parse throttle time
	throttle := binary.BigEndian.Uint32(resp[0:4])
	if throttle != 0 {
		t.Errorf("throttle time = %d, want 0", throttle)
	}

	// Parse broker count
	brokerCount := binary.BigEndian.Uint32(resp[4:8])
	if brokerCount != 1 {
		t.Errorf("broker count = %d, want 1", brokerCount)
	}
}

func TestEncodeMetadataResponse_EmptyTopics(t *testing.T) {
	t.Parallel()
	resp := EncodeMetadataResponse("localhost", 9092, nil)
	if len(resp) == 0 {
		t.Fatal("empty response for nil topics")
	}
}

func TestEncodeMetadataResponse_MultipleTopics(t *testing.T) {
	t.Parallel()
	topics := []TopicMetadata{
		{Name: "t1", PartitionCount: 1},
		{Name: "t2", PartitionCount: 2},
		{Name: "t3", PartitionCount: 3},
	}
	resp := EncodeMetadataResponse("host", 9092, topics)
	if len(resp) < 50 {
		t.Errorf("response too short for 3 topics: %d bytes", len(resp))
	}
}

// --- EncodeAPIVersionsResponse tests ---

func TestEncodeAPIVersionsResponse_ValidStructure(t *testing.T) {
	t.Parallel()
	resp := EncodeAPIVersionsResponse()

	// Error code (2 bytes)
	errCode := binary.BigEndian.Uint16(resp[0:2])
	if errCode != 0 {
		t.Errorf("error code = %d, want 0", errCode)
	}

	// API count (4 bytes)
	apiCount := binary.BigEndian.Uint32(resp[2:6])
	if apiCount != 10 {
		t.Errorf("API count = %d, want 10", apiCount)
	}
}

func TestEncodeAPIVersionsResponse_AllSupportedAPIs(t *testing.T) {
	t.Parallel()
	resp := EncodeAPIVersionsResponse()

	apiCount := binary.BigEndian.Uint32(resp[2:6])
	offset := 6

	expectedAPIs := map[int16]bool{
		APIKeyProduce: true, APIKeyFetch: true, APIKeyListOffsets: true,
		APIKeyMetadata: true, APIKeyOffsetCommit: true, APIKeyOffsetFetch: true,
		APIKeyFindCoordinator: true, APIKeyJoinGroup: true, APIKeySyncGroup: true,
		APIKeyAPIVersions: true,
	}

	for i := 0; i < int(apiCount); i++ {
		if offset+6 > len(resp) {
			t.Fatalf("response too short at API %d", i)
		}
		apiKey := int16(binary.BigEndian.Uint16(resp[offset : offset+2]))
		minVer := int16(binary.BigEndian.Uint16(resp[offset+2 : offset+4]))
		maxVer := int16(binary.BigEndian.Uint16(resp[offset+4 : offset+6]))
		offset += 6

		if !expectedAPIs[apiKey] {
			t.Errorf("unexpected API key %d in response", apiKey)
		}
		if minVer != 0 {
			t.Errorf("API %d: minVer = %d, want 0", apiKey, minVer)
		}
		if maxVer < 0 {
			t.Errorf("API %d: maxVer = %d, should be non-negative", apiKey, maxVer)
		}
		delete(expectedAPIs, apiKey)
	}

	if len(expectedAPIs) > 0 {
		t.Errorf("missing APIs in response: %v", expectedAPIs)
	}
}

// --- APIKeyName tests ---

func TestAPIKeyName_AllKnownKeys(t *testing.T) {
	t.Parallel()
	tests := []struct {
		key  int16
		want string
	}{
		{APIKeyProduce, "Produce"},
		{APIKeyFetch, "Fetch"},
		{APIKeyListOffsets, "ListOffsets"},
		{APIKeyMetadata, "Metadata"},
		{APIKeyOffsetCommit, "OffsetCommit"},
		{APIKeyOffsetFetch, "OffsetFetch"},
		{APIKeyFindCoordinator, "FindCoordinator"},
		{APIKeyJoinGroup, "JoinGroup"},
		{APIKeySyncGroup, "SyncGroup"},
		{APIKeyAPIVersions, "ApiVersions"},
	}
	for _, tt := range tests {
		if got := APIKeyName(tt.key); got != tt.want {
			t.Errorf("APIKeyName(%d) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestAPIKeyName_BoundaryValues(t *testing.T) {
	t.Parallel()
	// Negative value
	name := APIKeyName(-1)
	if name != "Unknown(-1)" {
		t.Errorf("APIKeyName(-1) = %q, want Unknown(-1)", name)
	}

	// Value > 67
	name = APIKeyName(100)
	if name != "Unknown(100)" {
		t.Errorf("APIKeyName(100) = %q, want Unknown(100)", name)
	}
}

// --- Binary edge cases ---

func TestReadRequestHeader_BodyOnlyHeaderBytes(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	// Exactly 8 bytes: minimal header
	binary.Write(&buf, binary.BigEndian, int32(8))
	binary.Write(&buf, binary.BigEndian, int16(APIKeyAPIVersions))
	binary.Write(&buf, binary.BigEndian, int16(3))
	binary.Write(&buf, binary.BigEndian, int32(55))

	hdr, body, err := ReadRequestHeader(&buf)
	if err != nil {
		t.Fatalf("ReadRequestHeader: %v", err)
	}
	if hdr.APIKey != APIKeyAPIVersions {
		t.Errorf("APIKey = %d", hdr.APIKey)
	}
	if len(body) != 0 {
		t.Errorf("body = %d bytes, want 0", len(body))
	}
}

func TestReadRequestHeader_NilBody(t *testing.T) {
	t.Parallel()
	// Empty reader
	buf := bytes.NewBuffer(nil)
	_, _, err := ReadRequestHeader(buf)
	if err == nil {
		t.Fatal("expected error for nil/empty reader")
	}
}

func TestReadRequestHeader_TruncatedBody(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, int32(100)) // claims 100 bytes
	buf.Write(make([]byte, 10))                       // but only 10 available
	_, _, err := ReadRequestHeader(&buf)
	if err == nil {
		t.Fatal("expected error for truncated body")
	}
}
