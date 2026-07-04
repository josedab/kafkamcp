# KafkaMCP Blog Content Strategy

## Post Outline — Prioritized by Impact

### Category 1: Problem & Narrative Posts

| # | Title | Thesis | Target Reader | Core Argument | Date |
|---|-------|--------|---------------|---------------|------|
| **1** | "Your AI Agents Are Blind to Kafka" | Teams building AI agents on Kafka repeatedly maintain bespoke glue code, creating a hidden reliability and ownership cost. | Senior data engineers, ML platform leads who operate Kafka daily | The consumer → REST → tool wrapper pattern adds layers that are difficult to govern, audit, and keep schema-aware. A narrow, secure MCP gateway removes those layers. | **Launch day** |
| **3** | "The Schema Blindness Problem: Why Your Agent Hallucinates Field Names" | Agents that consume Kafka without schema awareness generate invalid queries, produce malformed messages, and hallucinate fields that don't exist. | Data engineers using Avro/Protobuf schemas with Kafka | Schema blindness is not a minor inconvenience — it makes agents produce wrong results. Schema-aware tooling is a prerequisite, not a nice-to-have. | Week 6 |
| **5** | "What 2 AM Incident Triage Looks Like Without Agent-Kafka Access" | SREs manually running kafka-console-consumer during incidents is a solved problem — if your agent can read consumer group lag and DLQ messages itself. | SREs and incident responders operating Kafka infrastructure | Autonomous incident triage agents need three Kafka capabilities: consumer group inspection, message sampling, and DLQ analysis. Without them, humans stay in the loop for purely mechanical diagnostic steps. | Week 10 |

### Category 2: Technical Deep-Dives

| # | Title | Thesis | Target Reader | Core Argument | Date |
|---|-------|--------|---------------|---------------|------|
| **4** | "Building a Secure Kafka MCP Gateway in Go" | A walkthrough of KafkaMCP's curated tool packs, verified identity, approval middleware, and franz-go provider boundary. | Go developers and infra engineers evaluating the codebase | The architecture deliberately trades headline tool count for predictable routing, least privilege, and auditable mutations. | Week 8 |
| **6** | "MCP Resources vs MCP Tools: When to Use Which for Infrastructure Access" | MCP tools and resources serve different purposes — tools for actions, resources for context — and getting the boundary wrong makes your MCP server awkward to use. | Developers building MCP servers for any infrastructure | KafkaMCP's 37 supported tools handle bounded operations while nine stable resources provide read-only context. The separation keeps discovery clear and governance consistent. | Week 12 |

### Category 3: Differentiation & Positioning Posts

| # | Title | Thesis | Target Reader | Core Argument | Date |
|---|-------|--------|---------------|---------------|------|
| **2** | "Connect Claude to Your Kafka Cluster in 5 Minutes" | A zero-to-working tutorial that proves the setup claim and makes the product tangible immediately. | Any developer with Kafka and Claude Desktop who wants to try it | You can go from zero to "Claude reads my Kafka topics" in under 5 minutes with a single binary and 8 lines of YAML. Here's exactly how. | Week 2 |
| **7** | "KafkaMCP vs Kafka REST Proxy: Why MCP-Native Matters" | Kafka REST Proxy is a fine HTTP adapter, but retrofitting MCP onto a REST API produces a worse result than building MCP-first — and here's the architectural proof. | Teams evaluating whether to wrap REST Proxy with MCP tools or use a purpose-built MCP server | MCP-native design means agents discover tools automatically, parameters are typed, schemas surface as resources, and access control is per-agent. A REST-to-MCP shim gives you none of that, and adding it after the fact is more work than the initial integration. | Week 14 |

---

## Publication Schedule

| Week | Post | Category |
|------|------|----------|
| Launch day | #1 "Your AI Agents Are Blind to Kafka" | Problem |
| Week 2 | #2 "Connect Claude to Your Kafka Cluster in 5 Minutes" | Differentiation |
| Week 6 | #3 "The Schema Blindness Problem" | Problem |
| Week 8 | #4 "Building a 22-Tool MCP Server in Go" | Technical |
| Week 10 | #5 "What 2 AM Incident Triage Looks Like" | Problem |
| Week 12 | #6 "MCP Resources vs MCP Tools" | Technical |
| Week 14 | #7 "KafkaMCP vs Kafka REST Proxy" | Differentiation |
