"""KafkaMCP CrewAI integration — Kafka-aware AI agent crews."""

from kafkamcp_crewai.tools import (
    KafkaConsumeTool,
    KafkaProduceTool,
    KafkaListTopicsTool,
    KafkaSearchTool,
    KafkaWatchTool,
    create_kafka_tools,
)

__version__ = "0.1.0"
__all__ = [
    "KafkaConsumeTool",
    "KafkaProduceTool",
    "KafkaListTopicsTool",
    "KafkaSearchTool",
    "KafkaWatchTool",
    "create_kafka_tools",
]
