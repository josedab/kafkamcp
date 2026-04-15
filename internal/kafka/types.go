package kafka

// TopicSummary contains summarized information about a Kafka topic.
type TopicSummary struct {
	Name             string `json:"name"`
	Partitions       int    `json:"partitions"`
	ReplicationFactor int   `json:"replication_factor"`
	RetentionMs      int64  `json:"retention_ms,omitempty"`
	IsInternal       bool   `json:"is_internal"`
	MessageCount     int64  `json:"message_count_estimate"`
	SchemaSubject    string `json:"schema_subject,omitempty"`
}

// TopicDetail contains full topic metadata.
type TopicDetail struct {
	Name             string            `json:"name"`
	Partitions       []PartitionDetail `json:"partitions"`
	ReplicationFactor int              `json:"replication_factor"`
	Configs          map[string]string `json:"configs"`
	IsInternal       bool              `json:"is_internal"`
	MessageCount     int64             `json:"message_count_estimate"`
}

// PartitionDetail describes a single topic partition.
type PartitionDetail struct {
	ID             int32   `json:"id"`
	Leader         int32   `json:"leader"`
	Replicas       []int32 `json:"replicas"`
	ISR            []int32 `json:"isr"`
	EarliestOffset int64   `json:"earliest_offset"`
	LatestOffset   int64   `json:"latest_offset"`
}

// ConsumerGroupSummary is a summary of a consumer group.
type ConsumerGroupSummary struct {
	GroupID       string `json:"group_id"`
	State         string `json:"state"`
	MemberCount   int    `json:"member_count"`
	Protocol      string `json:"protocol"`
	TotalLag      int64  `json:"total_lag"`
}

// ConsumerGroupDetail is full consumer group metadata.
type ConsumerGroupDetail struct {
	GroupID      string              `json:"group_id"`
	State        string              `json:"state"`
	ProtocolType string              `json:"protocol_type"`
	Protocol     string              `json:"protocol"`
	Coordinator  BrokerInfo          `json:"coordinator"`
	Members      []GroupMemberDetail `json:"members"`
	Offsets      []GroupOffsetDetail `json:"offsets"`
	TotalLag     int64               `json:"total_lag"`
}

// BrokerInfo describes a Kafka broker.
type BrokerInfo struct {
	ID   int32  `json:"id"`
	Host string `json:"host"`
	Port int32  `json:"port"`
	Rack string `json:"rack,omitempty"`
}

// GroupMemberDetail describes a member of a consumer group.
type GroupMemberDetail struct {
	MemberID   string            `json:"member_id"`
	ClientID   string            `json:"client_id"`
	Host       string            `json:"host"`
	Assignment []TopicPartitions `json:"assignment"`
}

// TopicPartitions maps a topic to its assigned partitions.
type TopicPartitions struct {
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

// GroupOffsetDetail describes offsets for a partition within a consumer group.
type GroupOffsetDetail struct {
	Topic         string `json:"topic"`
	Partition     int32  `json:"partition"`
	CurrentOffset int64  `json:"current_offset"`
	LogEndOffset  int64  `json:"log_end_offset"`
	Lag           int64  `json:"lag"`
}

// ClusterMetadata contains cluster-level information.
type ClusterMetadata struct {
	ClusterID      string       `json:"cluster_id"`
	Controller     BrokerInfo   `json:"controller"`
	Brokers        []BrokerInfo `json:"brokers"`
	BrokerCount    int          `json:"broker_count"`
	TopicCount     int          `json:"topic_count"`
	PartitionCount int          `json:"partition_count"`
	KraftMode      bool         `json:"kraft_mode"`
}

// Message represents a consumed Kafka message.
type Message struct {
	Partition    int32             `json:"partition"`
	Offset       int64             `json:"offset"`
	Timestamp    string            `json:"timestamp"`
	Key          any               `json:"key"`
	Value        any               `json:"value"`
	Headers      map[string]string `json:"headers,omitempty"`
}

// DeliveryResult is the result of producing a message.
type DeliveryResult struct {
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Timestamp string `json:"timestamp"`
	Status    string `json:"status"`
}
