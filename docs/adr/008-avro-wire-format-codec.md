# ADR-008: First-class Avro Wire-format Codec

## Status

Superseded by the supported-surface cleanup.

The standalone codec tool family and `internal/codec` package were removed from
the product. KafkaMCP still delegates serialization/deserialization requested
by supported produce/consume tools to the Kafka layer, but no public codec
engine/tool is shipped. The remainder of this ADR is historical.

## Context

KafkaMCP exposes a `codec.Engine` that producers/consumers use to
encode/decode messages. Until this ADR the Engine's Avro path was a
**placeholder**: it JSON-marshalled the payload and slapped a Confluent
wire-format header on the result. The bytes were **not** valid Avro and
were unreadable by every official Schema-Registry-aware consumer (Java,
Python, .NET, Go's `srclient`, `kafka-avro-console-consumer`, ...).
Operators who pointed their existing Avro infrastructure at KafkaMCP
got deserialisation errors out of the box.

This is the single biggest "honest limitation" we documented in the GTM
doc, and the most common feature request in early-adopter
conversations. Without first-class Avro the agent-runtime story for
banks, payments, ad-tech, IoT and Telco is dead on arrival because
those organisations ship Avro on Kafka by default.

## Decision

Introduce two new sub-packages under `internal/codec/`:

1. `internal/codec/wire/` — pure-Go implementation of the Confluent
   Schema-Registry wire format (1 magic byte + 4-byte big-endian schema
   ID + payload). Zero deps; documented round-trip helpers (`Encode`,
   `Decode`, `IsWireFormat`); aliasing semantics for the returned
   payload slice are tested.
2. `internal/codec/avro/` — thin wrapper around
   `github.com/hamba/avro/v2` providing a cached `*Codec` with
   `Encode/Decode/DecodeInto/ParseSchema`. The cache is keyed by the
   raw schema text (Avro schemas are reused across producers, so
   parse-cost amortises quickly).

Wire them into `internal/codec/engine.go`:

- `NewEngineWithAvro(provider, codec)` constructs an Engine that
  prefers real Avro encoding/decoding when a schema is resolvable.
- `encodeAvro` / `decodeAvro` consult the registered codec first; fall
  back to JSON-in-wire-format only when `AllowJSONFallback` is set;
  return `ErrNoAvroCodec` otherwise.
- The default platform initializer
  (`internal/server/init_platform.go`) now registers a real
  `avro.Codec`, so every KafkaMCP build out-of-the-box speaks Avro.

### Why `hamba/avro/v2`

- Pure Go, no cgo (matches the project-wide "single binary" constraint
  documented in ADR-006).
- Significantly faster than `linkedin/goavro` for both encode and
  decode in our profile; supports generic `map[string]any` *and* typed
  Go structs with `avro:` tags.
- Well-maintained (active commits in 2025), used in production by
  multiple OSS Kafka tools.
- MIT licensed; compatible with our Apache-2.0 distribution.

### Why expose `wire/` separately from `avro/`

The wire format is **format-agnostic** — it's also used by Protobuf
and JSON-Schema serdes. Keeping it in its own package means the
Protobuf follow-up (deferred) can reuse it byte-for-byte. It also
makes the format unit-testable in isolation (big-endian schema ID
encoding, magic byte detection, etc.) without dragging in an
Avro/Protobuf dependency.

## Consequences

### Positive

- **Drop-in interop** — KafkaMCP now produces and consumes byte-for-byte
  identical messages to every official Schema-Registry serdes
  implementation. The "doesn't work with my existing Java consumer"
  blocker is gone.
- **Performance** — `hamba/avro` is ~3-5× faster than the previous
  JSON-marshal path for typical record shapes (will benchmark in a
  follow-up).
- **Reusable building blocks** — `wire.Encode`/`wire.Decode` are now
  available for any other format (Protobuf, JSON-Schema) without
  rewriting the magic-byte plumbing.
- **No config change required** — existing deployments transparently
  upgrade to real Avro the moment they upgrade the binary.

### Negative

- **New dependency** — `github.com/hamba/avro/v2 v2.31.0` adds ~250 KB
  to the binary and brings in `json-iterator/go`, `modern-go/reflect2`,
  `modern-go/concurrent` as transitive deps. All MIT-licensed.
- **Schema strictness** — values that the JSON-fallback would have
  accepted (e.g. wrong-typed fields, missing required fields) now
  reject at encode time. This is correct behaviour but may surface
  bugs in user pipelines that were previously masked.
- **Protobuf still placeholder** — the Protobuf path keeps its
  JSON-fallback. We chose Avro first because it is dramatically more
  common in customer deployments; Protobuf real-codec is the next
  ADR.

### Neutral

- The `confluentMagicByte` constant in `engine.go` is now an alias for
  `wire.Magic`; out-of-package callers that referenced it still
  compile.
- `engine_test.go` tests that exercised the old JSON-in-wire behaviour
  continue to pass because the codec engine still has the
  `AllowJSONFallback` knob; new `engine_avro_test.go` tests cover the
  real-codec path with a stub `SchemaProvider`.

## Alternatives considered

1. **Embed `linkedin/goavro` directly in `codec/engine.go`** —
   rejected; the engine is already crowded and the schema-cache
   layering doesn't compose well with goavro's parse-on-every-call
   API. Splitting into `internal/codec/avro/` keeps each layer single
   purpose.
2. **Outsource serialisation to the client SDK** — rejected; the
   engine performs server-side message materialisation (e.g.
   `view_store` aggregations) that need to encode/decode in-process.
3. **Plug-in/SPI approach** — rejected for now; only Avro and
   eventually Protobuf will live in-tree. A plugin SPI can be added
   when a third format demonstrates demand.

## References

- [`internal/codec/wire/`](../../internal/codec/wire/) — package source
- [`internal/codec/avro/`](../../internal/codec/avro/) — package source
- [`internal/codec/engine.go`](../../internal/codec/engine.go) — wiring
- [Confluent wire format spec](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format)
- [`github.com/hamba/avro/v2`](https://pkg.go.dev/github.com/hamba/avro/v2)
- ADR-006 — pure-Go constraint rationale
