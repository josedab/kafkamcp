package server

// Validation limits for user-supplied parameters.
const (
	// MaxPartitions is the upper bound for topic partition count.
	MaxPartitions = 10000

	// MaxReplicationFactor is the upper bound for topic replication factor.
	MaxReplicationFactor = 10

	// MaxSchemaSize is the maximum allowed schema body size in bytes (1 MB).
	MaxSchemaSize = 1 << 20

	// MaxDLQMessages is the upper bound for DLQ analysis message count.
	MaxDLQMessages = 10000

	// MaxSearchScan is the upper bound for search scan message count.
	MaxSearchScan = 100000

	// DefaultSearchScan is the default number of messages to scan during search.
	DefaultSearchScan = 10000

	// MaxProduceMessages is the maximum messages per produce call.
	MaxProduceMessages = 100

	// MaxConsumeLimit is the maximum messages per consume call.
	MaxConsumeLimit = 1000

	// MaxConsumeTimeout is the maximum consume timeout in milliseconds.
	MaxConsumeTimeout = 60000

	// MaxJoinLimit is the upper bound for join result count.
	MaxJoinLimit = 10000

	// maxInputLength is the maximum allowed length for user-supplied string parameters.
	maxInputLength = 1024
)
