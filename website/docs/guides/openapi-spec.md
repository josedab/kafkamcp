---
sidebar_position: 9
title: OpenAPI Spec Generation
---

# OpenAPI Spec Generation

KafkaMCP includes a built-in MCP tool called `kafka_openapi_spec` that auto-generates an [OpenAPI 3.0.3](https://spec.openapis.org/oas/v3.0.3) specification from all registered MCP tools. This is useful for SDK code generation, API documentation, and integrating KafkaMCP with REST-based toolchains.

## Usage

Call the `kafka_openapi_spec` tool through any MCP client:

```
Tool: kafka_openapi_spec
Args: { "format": "json" }
```

The tool returns a complete OpenAPI 3.0.3 document with:

- **Paths** generated from all registered MCP tools
- **Request/response schemas** derived from tool parameter definitions
- **Security scheme** using the `X-Agent-ID` header
- **Server URL** pointing to the local KafkaMCP instance

### Supported Formats

| Format | Description |
|--------|-------------|
| `json` | JSON-formatted OpenAPI spec (default) |
| `yaml` | YAML-formatted OpenAPI spec |

## Example: Generate and Save

Using the Go SDK:

```go
result, _ := client.CallTool(ctx, "kafka_openapi_spec", map[string]any{
    "format": "json",
})
os.WriteFile("openapi.json", []byte(result), 0644)
```

Using the TypeScript SDK:

```typescript
const spec = await transport.callTool("kafka_openapi_spec", { format: "json" });
```

Using curl (SSE transport):

```bash
# Via the MCP HTTP endpoint
curl -X POST http://localhost:3100/mcp \
  -H "Content-Type: application/json" \
  -d '{"tool": "kafka_openapi_spec", "arguments": {"format": "json"}}'
```

## Use Cases

- **SDK generation** — Feed the spec into [OpenAPI Generator](https://openapi-generator.tech/) to create typed clients in any language
- **API documentation** — Import into Swagger UI, Redoc, or Stoplight for interactive API docs
- **Testing** — Use with Postman or other API testing tools that support OpenAPI import
- **CI validation** — Snapshot the spec in CI to detect breaking API changes

## Authorization

The `kafka_openapi_spec` tool requires `read` permission on the `cluster` resource type. Ensure your agent's policy includes cluster read access:

```yaml
policies:
  agents:
    - id: "my-agent"
      topics:
        - pattern: "*"
          permissions: [read]
```

## See Also

- [API Reference: Tools](../api-reference/tools.md) — Full list of MCP tools
- [Configuration](../configuration.md) — Server and policy configuration
