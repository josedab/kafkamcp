package wire

import (
	"bytes"
	"errors"
	"testing"
)

func TestEncodeRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		schemaID uint32
		payload  []byte
	}{
		{"empty payload", 1, nil},
		{"single byte", 0x12345678, []byte{0xff}},
		{"typical avro", 42, []byte{0x06, 'a', 'b', 'c'}},
		{"max schema id", 0xFFFFFFFF, []byte("hello, kafka")},
		{"zero schema id", 0, []byte{1, 2, 3, 4, 5}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			encoded := Encode(tc.schemaID, tc.payload)

			if len(encoded) != HeaderLen+len(tc.payload) {
				t.Fatalf("encoded length = %d, want %d", len(encoded), HeaderLen+len(tc.payload))
			}
			if encoded[0] != Magic {
				t.Fatalf("magic byte = 0x%02x, want 0x00", encoded[0])
			}
			if !IsWireFormat(encoded) {
				t.Fatalf("IsWireFormat returned false for valid encoded payload")
			}

			gotID, gotPayload, err := Decode(encoded)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}
			if gotID != tc.schemaID {
				t.Errorf("schemaID = %d, want %d", gotID, tc.schemaID)
			}
			if !bytes.Equal(gotPayload, tc.payload) {
				t.Errorf("payload = %v, want %v", gotPayload, tc.payload)
			}
		})
	}
}

func TestEncodeBigEndian(t *testing.T) {
	t.Parallel()
	// SchemaID 1 must encode to bytes [0x00 0x00 0x00 0x00 0x01], regardless
	// of host endianness. This guards against accidental host-endian
	// regressions.
	encoded := Encode(1, nil)
	want := []byte{0x00, 0x00, 0x00, 0x00, 0x01}
	if !bytes.Equal(encoded, want) {
		t.Fatalf("Encode(1, nil) = %v, want %v", encoded, want)
	}

	encoded = Encode(0x01020304, nil)
	want = []byte{0x00, 0x01, 0x02, 0x03, 0x04}
	if !bytes.Equal(encoded, want) {
		t.Fatalf("Encode(0x01020304, nil) = %v, want %v", encoded, want)
	}
}

func TestDecodeErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		data []byte
		want error
	}{
		{"empty", nil, ErrShortPayload},
		{"four bytes", []byte{0x00, 0x00, 0x00, 0x01}, ErrShortPayload},
		{"bad magic", []byte{0x01, 0x00, 0x00, 0x00, 0x01}, ErrBadMagic},
		{"json-leading-byte", []byte("{}"), ErrShortPayload},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := Decode(tc.data)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want errors.Is(%v)", err, tc.want)
			}
		})
	}
}

func TestIsWireFormat(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		data []byte
		want bool
	}{
		{"too short", []byte{0x00, 0x00}, false},
		{"valid", []byte{0x00, 0x00, 0x00, 0x00, 0x01}, true},
		{"valid with payload", []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0xaa, 0xbb}, true},
		{"bad magic", []byte{0x01, 0x00, 0x00, 0x00, 0x01}, false},
		{"empty", nil, false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsWireFormat(tc.data); got != tc.want {
				t.Fatalf("IsWireFormat(%v) = %v, want %v", tc.data, got, tc.want)
			}
		})
	}
}

func TestDecodePayloadAliasing(t *testing.T) {
	t.Parallel()
	// The returned payload slice intentionally shares storage with the
	// input. This lets hot paths avoid an allocation. Document the
	// behaviour with a test so future refactors don't regress it.
	in := []byte{0x00, 0x00, 0x00, 0x00, 0x07, 'x', 'y', 'z'}
	_, payload, err := Decode(in)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(payload) == 0 {
		t.Fatalf("expected payload, got empty")
	}
	// Mutating the source should be visible through the returned slice.
	in[HeaderLen] = 'Q'
	if payload[0] != 'Q' {
		t.Fatalf("payload[0] = %c, want Q (aliased slice not honoured)", payload[0])
	}
}
