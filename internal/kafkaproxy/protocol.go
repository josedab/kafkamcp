package kafkaproxy

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Kafka API keys for the top 10 most-used APIs.
const (
	APIKeyProduce         int16 = 0
	APIKeyFetch           int16 = 1
	APIKeyListOffsets     int16 = 2
	APIKeyMetadata        int16 = 3
	APIKeyOffsetCommit    int16 = 8
	APIKeyOffsetFetch     int16 = 9
	APIKeyFindCoordinator int16 = 10
	APIKeyJoinGroup       int16 = 11
	APIKeySyncGroup       int16 = 14
	APIKeyAPIVersions     int16 = 18
)

// APIKeyName returns a human-readable name for a Kafka API key.
func APIKeyName(apiKey int16) string {
	switch apiKey {
	case APIKeyProduce:
		return "Produce"
	case APIKeyFetch:
		return "Fetch"
	case APIKeyListOffsets:
		return "ListOffsets"
	case APIKeyMetadata:
		return "Metadata"
	case APIKeyOffsetCommit:
		return "OffsetCommit"
	case APIKeyOffsetFetch:
		return "OffsetFetch"
	case APIKeyFindCoordinator:
		return "FindCoordinator"
	case APIKeyJoinGroup:
		return "JoinGroup"
	case APIKeySyncGroup:
		return "SyncGroup"
	case APIKeyAPIVersions:
		return "ApiVersions"
	default:
		return fmt.Sprintf("Unknown(%d)", apiKey)
	}
}

// RequestHeader represents a parsed Kafka request header (v0-v2).
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
	Size          int32
}

// ResponseHeader represents a Kafka response header.
type ResponseHeader struct {
	CorrelationID int32
}

// ReadRequestHeader reads a Kafka request header from the wire.
// The Kafka protocol sends a 4-byte size prefix followed by the request header.
func ReadRequestHeader(r io.Reader) (*RequestHeader, []byte, error) {
	// Read message size (4 bytes, big-endian)
	var size int32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return nil, nil, fmt.Errorf("reading message size: %w", err)
	}
	if size < 8 || size > 100*1024*1024 {
		return nil, nil, fmt.Errorf("invalid message size: %d", size)
	}

	// Read the entire message body
	body := make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, nil, fmt.Errorf("reading message body: %w", err)
	}

	if len(body) < 8 {
		return nil, nil, fmt.Errorf("message too short for header: %d bytes", len(body))
	}

	hdr := &RequestHeader{
		Size:          size,
		APIKey:        int16(binary.BigEndian.Uint16(body[0:2])),
		APIVersion:    int16(binary.BigEndian.Uint16(body[2:4])),
		CorrelationID: int32(binary.BigEndian.Uint32(body[4:8])),
	}

	offset := 8
	// Parse ClientID (nullable string: 2-byte length, then bytes)
	if len(body) > offset+2 {
		clientIDLen := int16(binary.BigEndian.Uint16(body[offset : offset+2]))
		offset += 2
		if clientIDLen > 0 && len(body) >= offset+int(clientIDLen) {
			hdr.ClientID = string(body[offset : offset+int(clientIDLen)])
			offset += int(clientIDLen)
		}
	}

	return hdr, body[offset:], nil
}

// WriteResponse writes a Kafka response with the correlation ID and payload.
func WriteResponse(w io.Writer, correlationID int32, payload []byte) error {
	// Size = 4 bytes correlation ID + payload length
	size := int32(4 + len(payload))
	if err := binary.Write(w, binary.BigEndian, size); err != nil {
		return fmt.Errorf("writing response size: %w", err)
	}
	if err := binary.Write(w, binary.BigEndian, correlationID); err != nil {
		return fmt.Errorf("writing correlation ID: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("writing response payload: %w", err)
	}
	return nil
}

// EncodeMetadataResponse builds a minimal Metadata response with broker and topic info.
func EncodeMetadataResponse(brokerHost string, brokerPort int32, topics []TopicMetadata) []byte {
	buf := make([]byte, 0, 256)

	// Throttle time (4 bytes)
	buf = binary.BigEndian.AppendUint32(buf, 0)
	// Broker count
	buf = binary.BigEndian.AppendUint32(buf, 1)
	// Broker: node_id=0
	buf = binary.BigEndian.AppendUint32(buf, 0)
	// Broker host
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(brokerHost)))
	buf = append(buf, []byte(brokerHost)...)
	// Broker port
	buf = binary.BigEndian.AppendUint32(buf, uint32(brokerPort))
	// Rack (nullable: -1)
	buf = binary.BigEndian.AppendUint16(buf, 0xFFFF)

	// Cluster ID (nullable)
	buf = binary.BigEndian.AppendUint16(buf, 0xFFFF)
	// Controller ID
	buf = binary.BigEndian.AppendUint32(buf, 0)

	// Topics
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(topics)))
	for _, t := range topics {
		// Error code
		buf = binary.BigEndian.AppendUint16(buf, 0)
		// Topic name
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(t.Name)))
		buf = append(buf, []byte(t.Name)...)
		// Is internal
		buf = append(buf, 0)
		// Partitions
		buf = binary.BigEndian.AppendUint32(buf, uint32(t.PartitionCount))
		for p := 0; p < t.PartitionCount; p++ {
			// Error code, partition index, leader, replicas, ISR
			buf = binary.BigEndian.AppendUint16(buf, 0)
			buf = binary.BigEndian.AppendUint32(buf, uint32(p))
			buf = binary.BigEndian.AppendUint32(buf, 0)
			// Replica count=1, replica=0
			buf = binary.BigEndian.AppendUint32(buf, 1)
			buf = binary.BigEndian.AppendUint32(buf, 0)
			// ISR count=1, ISR=0
			buf = binary.BigEndian.AppendUint32(buf, 1)
			buf = binary.BigEndian.AppendUint32(buf, 0)
		}
	}

	return buf
}

// EncodeAPIVersionsResponse builds an ApiVersions response listing supported APIs.
func EncodeAPIVersionsResponse() []byte {
	type apiVersion struct {
		key    int16
		minVer int16
		maxVer int16
	}
	supported := []apiVersion{
		{APIKeyProduce, 0, 8},
		{APIKeyFetch, 0, 11},
		{APIKeyListOffsets, 0, 5},
		{APIKeyMetadata, 0, 9},
		{APIKeyOffsetCommit, 0, 8},
		{APIKeyOffsetFetch, 0, 7},
		{APIKeyFindCoordinator, 0, 3},
		{APIKeyJoinGroup, 0, 7},
		{APIKeySyncGroup, 0, 5},
		{APIKeyAPIVersions, 0, 3},
	}

	buf := make([]byte, 0, 128)
	// Error code = 0
	buf = binary.BigEndian.AppendUint16(buf, 0)
	// API count
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(supported)))
	for _, v := range supported {
		buf = binary.BigEndian.AppendUint16(buf, uint16(v.key))
		buf = binary.BigEndian.AppendUint16(buf, uint16(v.minVer))
		buf = binary.BigEndian.AppendUint16(buf, uint16(v.maxVer))
	}
	// Throttle time
	buf = binary.BigEndian.AppendUint32(buf, 0)
	return buf
}

// TopicMetadata holds topic info for metadata responses.
type TopicMetadata struct {
	Name           string `json:"name"`
	PartitionCount int    `json:"partition_count"`
}
