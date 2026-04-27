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
pip install -r requirements.txt

# Set your OpenAI API key
export OPENAI_API_KEY=your-key
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
[DLQ Specialist] Checking DLQ topics for retryable messages...

Crew result:
  ...
```

## How It Works

The crew consists of three specialized agents:

1. **Kafka Monitor** — checks cluster health and detects anomalies
2. **Data Analyst** — analyzes message patterns and generates insights
3. **DLQ Remediation Specialist** — identifies retryable dead-letter queue messages

Each agent uses KafkaMCP tools (provided via `kafkamcp-crewai`) to interact with Kafka. The crew executes tasks sequentially and produces a combined report.
