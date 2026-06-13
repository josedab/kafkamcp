package server

import (
	"github.com/josedab/kafkamcp/internal/kafka"
	"github.com/josedab/kafkamcp/internal/masking"
)

// MaskingEngine returns the configured PII masking engine. It is non-nil for
// servers constructed via New().
func (s *Server) MaskingEngine() *masking.Engine { return s.maskingEngine }

// maskMessages applies the configured masking engine to the Key and Value of
// every message in place, for the given agent. It intentionally does NOT touch
// Kafka metadata (partition, offset, timestamp, headers) or schema fields —
// only the message payload carried in Key/Value, including nested decoded JSON
// objects and arrays.
//
// It fails closed: if any value cannot be masked safely, it returns an error and
// the caller MUST NOT return the (potentially unmasked) messages to the client.
// When masking is disabled or no engine is configured, messages pass through
// unchanged.
func (s *Server) maskMessages(agentID string, msgs []kafka.Message) error {
	if s == nil || s.maskingEngine == nil || !s.maskingEngine.Enabled() {
		return nil
	}
	for i := range msgs {
		if msgs[i].Key != nil {
			mk, err := s.maskingEngine.MaskValueChecked(agentID, msgs[i].Key)
			if err != nil {
				return err
			}
			msgs[i].Key = mk
		}
		if msgs[i].Value != nil {
			mv, err := s.maskingEngine.MaskValueChecked(agentID, msgs[i].Value)
			if err != nil {
				return err
			}
			msgs[i].Value = mv
		}
	}
	return nil
}
