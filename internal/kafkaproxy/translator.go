package kafkaproxy

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// TranslationResult holds the result of translating a Kafka request to an MCP tool call.
type TranslationResult struct {
	Tool       string         `json:"tool"`
	Args       map[string]any `json:"args"`
	APIKey     int16          `json:"api_key"`
	APIName    string         `json:"api_name"`
	ClientID   string         `json:"client_id"`
	Latency    time.Duration  `json:"latency"`
	Translated bool           `json:"translated"`
}

// PolicyAction defines what happens to a translated request.
type PolicyAction string

// PolicyAllow and related values define constants for this package.
const (
	PolicyAllow PolicyAction = "allow"
	PolicyDeny  PolicyAction = "deny"
	PolicyAudit PolicyAction = "audit"
)

// Policy defines an enforcement rule for the proxy.
type Policy struct {
	Name      string       `json:"name" yaml:"name"`
	APIKeys   []int16      `json:"api_keys" yaml:"api_keys"`
	ClientIDs []string     `json:"client_ids,omitempty" yaml:"client_ids,omitempty"`
	Action    PolicyAction `json:"action" yaml:"action"`
	Priority  int          `json:"priority" yaml:"priority"`
}

// TranslatorConfig configures the translation layer.
type TranslatorConfig struct {
	EnableAudit   bool     `json:"enable_audit" yaml:"enable_audit"`
	EnableMasking bool     `json:"enable_masking" yaml:"enable_masking"`
	Policies      []Policy `json:"policies,omitempty" yaml:"policies,omitempty"`
}

// DefaultTranslatorConfig returns sensible defaults.
func DefaultTranslatorConfig() TranslatorConfig {
	return TranslatorConfig{
		EnableAudit:   true,
		EnableMasking: false,
	}
}

// Translator maps Kafka API requests to MCP tool invocations.
type Translator struct {
	config TranslatorConfig
	stats  TranslationStats
	mu     sync.RWMutex
	logger *slog.Logger
}

// TranslationStats tracks translation metrics.
type TranslationStats struct {
	TotalRequests   int64            `json:"total_requests"`
	TranslatedOK    int64            `json:"translated_ok"`
	TranslateErrors int64            `json:"translate_errors"`
	Passthrough     int64            `json:"passthrough"`
	DeniedByPolicy  int64            `json:"denied_by_policy"`
	ByAPIKey        map[int16]int64  `json:"by_api_key"`
	ByClient        map[string]int64 `json:"by_client"`
}

// NewTranslator creates a new Kafka-to-MCP translator.
func NewTranslator(cfg TranslatorConfig, logger *slog.Logger) *Translator {
	return &Translator{
		config: cfg,
		stats: TranslationStats{
			ByAPIKey: make(map[int16]int64),
			ByClient: make(map[string]int64),
		},
		logger: logger,
	}
}

// Translate converts a Kafka request header and body into an MCP tool call.
func (t *Translator) Translate(hdr *RequestHeader, body []byte) (*TranslationResult, error) {
	t.mu.Lock()
	t.stats.TotalRequests++
	t.stats.ByAPIKey[hdr.APIKey]++
	if hdr.ClientID != "" {
		t.stats.ByClient[hdr.ClientID]++
	}
	t.mu.Unlock()

	start := time.Now()

	// Check policies
	if action := t.evaluatePolicy(hdr); action == PolicyDeny {
		t.mu.Lock()
		t.stats.DeniedByPolicy++
		t.mu.Unlock()
		return nil, fmt.Errorf("request denied by policy for API %s from client %s",
			APIKeyName(hdr.APIKey), hdr.ClientID)
	}

	result := &TranslationResult{
		APIKey:   hdr.APIKey,
		APIName:  APIKeyName(hdr.APIKey),
		ClientID: hdr.ClientID,
	}

	switch hdr.APIKey {
	case APIKeyProduce:
		result.Tool = "kafka_produce"
		result.Args = t.translateProduce(body)
		result.Translated = true

	case APIKeyFetch:
		result.Tool = "kafka_consume"
		result.Args = t.translateFetch(body)
		result.Translated = true

	case APIKeyMetadata:
		result.Tool = "kafka_list_topics"
		result.Args = t.translateMetadata(body)
		result.Translated = true

	case APIKeyFindCoordinator:
		result.Tool = "kafka_describe_consumer_group"
		result.Args = t.translateFindCoordinator(body)
		result.Translated = true

	case APIKeyListOffsets:
		result.Tool = "kafka_list_offsets"
		result.Args = t.translateListOffsets(body)
		result.Translated = true

	case APIKeyOffsetCommit:
		result.Tool = "kafka_commit_offsets"
		result.Args = map[string]any{"action": "commit"}
		result.Translated = true

	case APIKeyOffsetFetch:
		result.Tool = "kafka_fetch_offsets"
		result.Args = t.translateOffsetFetch(body)
		result.Translated = true

	case APIKeyJoinGroup:
		result.Tool = "kafka_consumer_group_join"
		result.Args = t.translateJoinGroup(body)
		result.Translated = true

	case APIKeySyncGroup:
		result.Tool = "kafka_consumer_group_sync"
		result.Args = map[string]any{"action": "sync"}
		result.Translated = true

	case APIKeyAPIVersions:
		// Handled directly by the proxy, no MCP translation needed
		result.Translated = false
		t.mu.Lock()
		t.stats.Passthrough++
		t.mu.Unlock()

	default:
		result.Translated = false
		t.mu.Lock()
		t.stats.Passthrough++
		t.mu.Unlock()
	}

	result.Latency = time.Since(start)
	if result.Translated {
		t.mu.Lock()
		t.stats.TranslatedOK++
		t.mu.Unlock()
	}

	return result, nil
}

// Stats returns current translation statistics.
func (t *Translator) Stats() TranslationStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	cp := t.stats
	cp.ByAPIKey = make(map[int16]int64, len(t.stats.ByAPIKey))
	for k, v := range t.stats.ByAPIKey {
		cp.ByAPIKey[k] = v
	}
	cp.ByClient = make(map[string]int64, len(t.stats.ByClient))
	for k, v := range t.stats.ByClient {
		cp.ByClient[k] = v
	}
	return cp
}

func (t *Translator) evaluatePolicy(hdr *RequestHeader) PolicyAction {
	for _, p := range t.config.Policies {
		matched := false
		for _, k := range p.APIKeys {
			if k == hdr.APIKey {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		if len(p.ClientIDs) > 0 {
			clientMatch := false
			for _, c := range p.ClientIDs {
				if c == hdr.ClientID {
					clientMatch = true
					break
				}
			}
			if !clientMatch {
				continue
			}
		}
		return p.Action
	}
	return PolicyAllow
}

// Translation helpers extract topic/group names from wire format bodies.

func (t *Translator) translateProduce(body []byte) map[string]any {
	args := map[string]any{"action": "produce"}
	if topic := extractNullableString(body, 0); topic != "" {
		args["topic"] = topic
	}
	return args
}

func (t *Translator) translateFetch(body []byte) map[string]any {
	args := map[string]any{"action": "consume"}
	// Fetch request: replica_id(4) + max_wait(4) + min_bytes(4) + topics...
	if len(body) > 16 {
		if topic := extractNullableString(body, 12); topic != "" {
			args["topic"] = topic
		}
	}
	return args
}

func (t *Translator) translateMetadata(body []byte) map[string]any {
	args := map[string]any{}
	if len(body) >= 4 {
		topicCount := int32(binary.BigEndian.Uint32(body[0:4]))
		if topicCount == 0 || topicCount == -1 {
			// All topics
			return args
		}
		if topicCount == 1 && len(body) > 6 {
			if topic := extractNullableString(body, 4); topic != "" {
				args["filter"] = topic
			}
		}
	}
	return args
}

func (t *Translator) translateFindCoordinator(body []byte) map[string]any {
	args := map[string]any{"action": "describe"}
	if group := extractNullableString(body, 0); group != "" {
		args["group_id"] = group
	}
	return args
}

func (t *Translator) translateListOffsets(body []byte) map[string]any {
	args := map[string]any{"action": "list_offsets"}
	// replica_id(4) + isolation_level(1) + topic_count(4) + topic...
	if len(body) > 9 {
		if topic := extractNullableString(body, 9); topic != "" {
			args["topic"] = topic
		}
	}
	return args
}

func (t *Translator) translateOffsetFetch(body []byte) map[string]any {
	args := map[string]any{"action": "fetch_offsets"}
	if group := extractNullableString(body, 0); group != "" {
		args["group_id"] = group
	}
	return args
}

func (t *Translator) translateJoinGroup(body []byte) map[string]any {
	args := map[string]any{"action": "join_group"}
	if group := extractNullableString(body, 0); group != "" {
		args["group_id"] = group
	}
	return args
}

// extractNullableString reads a Kafka nullable string (2-byte length prefix).
func extractNullableString(data []byte, offset int) string {
	if len(data) < offset+2 {
		return ""
	}
	length := int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	if length <= 0 || len(data) < offset+2+int(length) {
		return ""
	}
	return string(data[offset+2 : offset+2+int(length)])
}
