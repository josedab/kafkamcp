# CrewAI Agent Example

Demonstrates a multi-agent CrewAI crew that collaborates on Kafka operations using KafkaMCP tools.

## Prerequisites

- Python 3.10+
- `kafkamcp` binary installed and on `PATH`
- A running Kafka broker (e.g., `make docker-dev` from the repo root)
- An OpenAI API key

## Setup

```bash
# Install Python dependencies
cd examples/crewai-agent
pip install -r requirements.txt
pip install -e ../../sdk/python/kafkamcp_crewai

# Set your OpenAI API key
export OPENAI_API_KEY=your-key

# Optional: point to a custom config
export KAFKAMCP_CONFIG=/path/to/kafkamcp.yaml
```

## Usage

```bash
python crew.py
```

## Expected Output

```
KafkaMCP CrewAI Example
Starting crew execution...

[Monitor] Checking cluster health...
[Analyst] Analyzing orders topic for failure patterns...
[DLQ Specialist] Analyzing DLQ failure patterns...

Crew result:
  ...
```

## How It Works

The crew consists of three specialized agents:

1. **Kafka Monitor** — checks cluster health and detects anomalies
2. **Data Analyst** — analyzes message patterns and generates insights
3. **DLQ Specialist** — categorizes dead-letter queue failures and recommends next steps

`StdioMCPClient` keeps the asynchronous MCP session on a worker event loop so
CrewAI's synchronous tools can invoke it safely. The default wrapper set is
read-only; guarded mutation wrappers must be added explicitly.
