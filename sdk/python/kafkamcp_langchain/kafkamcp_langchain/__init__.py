"""KafkaMCP LangChain integration — pre-built tool wrappers for Kafka operations."""

from kafkamcp_langchain.tools import (
    KafkaConsumeToolkit,
    KafkaProduceToolkit,
    KafkaTopicToolkit,
    KafkaSchemaToolkit,
    KafkaMCPToolkit,
)

__version__ = "0.1.0"
__all__ = [
    "KafkaConsumeToolkit",
    "KafkaProduceToolkit",
    "KafkaTopicToolkit",
    "KafkaSchemaToolkit",
    "KafkaMCPToolkit",
]
