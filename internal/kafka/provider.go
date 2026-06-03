package kafka

// ProviderKind identifies the implementation supplying Kafka operations.
type ProviderKind string

const (
	// ProviderNative executes operations directly through franz-go.
	ProviderNative ProviderKind = "native"
	// ProviderDelegated is reserved for adapters that translate the stable
	// KafkaMCP contract to another MCP provider, such as a future KIP-1318
	// server.
	ProviderDelegated ProviderKind = "delegated"
)

// ProviderInfo describes an operation provider without coupling the server to a
// vendor-specific implementation.
type ProviderInfo struct {
	Kind         ProviderKind `json:"kind"`
	Name         string       `json:"name"`
	Capabilities []string     `json:"capabilities,omitempty"`
}

// OperationProvider is the vendor-neutral boundary behind KafkaMCP's
// governance layer. Implementations satisfy the existing KafkaManager contract,
// so a future KIP-1318 adapter can translate operations without changing policy,
// approval, masking, audit, or MCP handler code.
type OperationProvider interface {
	KafkaManager
	ProviderInfo() ProviderInfo
}

type nativeOperationProvider struct {
	KafkaManager
}

// NewNativeOperationProvider wraps the existing franz-go-backed manager in the
// provider boundary.
func NewNativeOperationProvider(manager KafkaManager) OperationProvider {
	return &nativeOperationProvider{KafkaManager: manager}
}

// ProviderInfo describes the native franz-go implementation.
func (p *nativeOperationProvider) ProviderInfo() ProviderInfo {
	return ProviderInfo{
		Kind: ProviderNative,
		Name: "franz-go",
		Capabilities: []string{
			"topics",
			"messages",
			"consumer-groups",
			"cluster-metadata",
		},
	}
}
