# ADR-010: Protobuf Wire-Format Codec

- Status: **Superseded by the supported-surface cleanup**
- Date: 2026-05-04
- Supersedes: —
- Superseded by: the 29-tool supported-product boundary
- Related: [ADR-008 Avro wire-format codec](./008-avro-wire-format-codec.md)

> Historical: the standalone codec tool family and `internal/codec` package
> were removed. Supported produce/consume operations remain, but KafkaMCP no
> longer exposes a public Protobuf codec engine/tool.

## Context

[ADR-008](./008-avro-wire-format-codec.md) replaced the
JSON-in-wire-format placeholder for `FormatAvro` with a real
hamba/avro v2 codec, making KafkaMCP's Avro output interoperable with
the Confluent Schema Registry ecosystem. The same placeholder still
existed for `FormatProtobuf`:

```go
func (e *Engine) encodeProtobuf(value any, schemaID int) ([]byte, error) {
    if !e.allowJSONFallback {
        return nil, ErrNoProtobufCodec
    }
    // … emits 0x00 || JSON(value) — NOT valid protobuf binary.
}
```

Protobuf is the second-most-common Kafka serialization format in
production today (after JSON, ahead of Avro for newer microservices
stacks). Continuing to ship JSON-in-protobuf-wire-format means every
KafkaMCP-produced message that claims `FormatProtobuf` is unreadable
by every standards-compliant consumer — which is the same correctness
problem ADR-008 set out to solve for Avro.

## Decision

Add `internal/codec/protobuf` — a thin wrapper around two pure-Go
libraries:

- **`github.com/bufbuild/protocompile`** parses `.proto` schema text
  into a `protoreflect.FileDescriptor` without invoking the external
  `protoc` binary.
- **`google.golang.org/protobuf/types/dynamicpb`** instantiates a
  `proto.Message` from any FileDescriptor at runtime, so we don't need
  code-generated Go types per registered schema.

Together they give us schema-text → wire-bytes serialisation with no
build-time `protoc` step, matching the operator's expectation that a
Schema Registry alone is enough to roundtrip a message.

### Confluent message-index encoding

Confluent's Protobuf wire format prepends a "message index" before
the protobuf payload so the consumer can decode against the right
message type in a multi-message `.proto` file. The index is a
zigzag-encoded varint array:

| Index    | Bytes                                |
|----------|--------------------------------------|
| `nil`    | `0x00` (optimisation for index 0)    |
| `{0}`    | `0x00`                                |
| `{N}`    | `varint(1) varint(zigzag(N))`         |
| `{a,b…}` | `varint(len) varint(zigzag(a)) …`     |

The `encodeMessageIndex` / `decodeMessageIndex` helpers in
`protobuf.go` implement exactly this, and round-trip tests cover the
zero-byte optimisation, single-element and multi-element paths.

### Why dynamicpb, not protoc-generated types

Two reasons:

1. **No build step.** The Schema Registry returns `.proto` text at
   runtime; we don't know the schemas at compile time. Code generation
   would require shelling out to `protoc` (a CGo dependency on most
   platforms) at server start — exactly what we tried to avoid for
   Avro.
2. **Schema evolution.** dynamicpb instances react to schema changes
   without rebuilding; a single Codec can hold an unbounded number of
   compiled FileDescriptors keyed by schema text.

### Why not bufbuild/connect-go or vtprotobuf

Both target code-generated types only. They're faster for known-shape
messages but inapplicable to the dynamic-schema use case.

### Schema text caching

A `sync.RWMutex`-guarded map (`map[string]*schemaEntry`) caches the
compiled FileDescriptor + the flat declaration-order message list per
unique schema text. This mirrors the Avro codec's caching strategy
(ADR-008) and keeps the steady-state cost of a hot schema down to a
single map lookup.

### Cross-file imports

Standard well-known types (`google/protobuf/timestamp.proto` etc.)
work via `protocompile.WithStandardImports`. **User-defined cross-file
imports are NOT supported**: every schema must be self-contained
because the Schema Registry API also returns a single `.proto` text
per schema ID. Imports beyond the well-knowns surface as a clear
`ErrSchemaParse`.

### Engine integration

`codec.NewEngineWithProtobuf(provider, codec)` mirrors
`NewEngineWithAvro`. The default platform initializer
(`server/init_platform.go`) registers a fresh `protobuf.New()`
alongside the existing `avro.New()` so out-of-the-box deployments get
real wire-bytes for both formats:

```go
codecEng := codec.NewEngineWithAvro(nil, codecavro.New())
codecEng.SetProtobufCodec(codecprotobuf.New())
```

Strict-mode encoding (no `AllowJSONFallback`) returns
`ErrNoProtobufCodec` only when the registered codec is nil — matching
the Avro pattern.

## Consequences

### Positive

- KafkaMCP-produced Protobuf messages are byte-for-byte
  Schema-Registry-interoperable; Java, Python, Go, .NET consumers
  read them with no special handling.
- The CLI's `kafka_codec_encode` / `kafka_codec_decode` tools now
  produce / consume real protobuf binary, closing the largest
  remaining "we look correct but aren't" gap in the codec stack.
- The same `protobuf.Codec` can be re-used from non-MCP code paths
  (sandbox runs, contract tests, schema-validation tooling).

### Negative

- **+1 transitive dep:** `github.com/bufbuild/protocompile` (and its
  transitive `golang.org/x/sync`). Both are Buf's, both are pure-Go,
  both are MIT-licensed — but the supply-chain surface area grows by
  one module.
- **No cross-file imports** beyond well-known types. Documented in
  the package doc; the registry-driven workflow doesn't need them.
- **Generic map[string]any only.** Callers who want strongly typed
  Go structs need to use the code-generated types directly; KafkaMCP
  intentionally stays in the schema-driven generic world.

## Alternatives considered

- **Keep JSON-in-protobuf-wire-format placeholder.** Rejected — same
  correctness argument as ADR-008.
- **Shell out to `protoc`.** Rejected — adds a CGo / external-binary
  dependency, defeats the pure-Go-only constraint that ADR-006 and
  ADR-008 already committed to.
- **Use `kreuzwerker/m3o` or `jhump/protoreflect`.** Both viable, but
  protocompile is the actively maintained Buf-owned successor to
  jhump/protoreflect and ships its own pure-Go parser.
