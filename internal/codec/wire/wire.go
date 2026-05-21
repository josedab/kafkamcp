// Package wire implements the Confluent Schema Registry "wire format"
// used by all official Avro / Protobuf / JSON-Schema clients
// interoperable with kafka-avro-serializer.
//
// The wire format is documented at
// https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
//
//	0                   1                   2                   3
//	0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|              Magic              |        4-byte schema ID       |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                            payload                           ...
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//
// The magic byte is always 0x00; any other value indicates a non-wire
// payload (raw JSON, raw bytes, ...). Schema IDs are big-endian
// unsigned 32-bit integers — they are positive 31-bit numbers in
// practice but the protocol uses the full 32-bit width.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Magic is the leading byte of every Confluent-wire-format message.
const Magic byte = 0x00

// HeaderLen is the byte length of the wire-format prefix
// (1 magic byte + 4 schema-id bytes).
const HeaderLen = 5

// Sentinel errors returned by [Decode].
var (
	// ErrShortPayload is returned when the supplied buffer is too short
	// to contain a Confluent wire-format header.
	ErrShortPayload = errors.New("wire: payload shorter than 5 bytes")

	// ErrBadMagic is returned when the first byte is not 0x00.
	ErrBadMagic = errors.New("wire: leading byte is not the magic 0x00")
)

// Encode wraps payload bytes in the Confluent wire format and returns
// the resulting byte slice. It always allocates a new slice; the input
// is not modified. SchemaID is encoded as a big-endian unsigned 32-bit
// integer regardless of the language/platform of the caller.
func Encode(schemaID uint32, payload []byte) []byte {
	out := make([]byte, HeaderLen+len(payload))
	out[0] = Magic
	binary.BigEndian.PutUint32(out[1:HeaderLen], schemaID)
	copy(out[HeaderLen:], payload)
	return out
}

// Decode peels the Confluent wire-format header off data and returns
// the schema ID plus a slice into the payload. The returned payload
// slice shares the underlying array with data — callers that retain
// the slice across mutations should copy it. Returns [ErrShortPayload]
// or [ErrBadMagic] when data is not a valid wire-format message.
func Decode(data []byte) (schemaID uint32, payload []byte, err error) {
	if len(data) < HeaderLen {
		return 0, nil, fmt.Errorf("%w: have %d bytes", ErrShortPayload, len(data))
	}
	if data[0] != Magic {
		return 0, nil, fmt.Errorf("%w: got 0x%02x", ErrBadMagic, data[0])
	}
	schemaID = binary.BigEndian.Uint32(data[1:HeaderLen])
	payload = data[HeaderLen:]
	return schemaID, payload, nil
}

// IsWireFormat reports whether data has a valid Confluent wire-format
// header. Useful for content-sniffing in mixed-format consumers.
func IsWireFormat(data []byte) bool {
	return len(data) >= HeaderLen && data[0] == Magic
}
