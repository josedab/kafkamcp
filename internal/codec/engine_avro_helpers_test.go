package codec

import (
	"github.com/kafkamcp/kafkamcp/internal/codec/avro"
)

// newTestAvroCodec returns a fresh avro.Codec for tests.
func newTestAvroCodec() *avro.Codec {
	return avro.New()
}
