// Package protobuf is a thin wrapper around
// github.com/bufbuild/protocompile + google.golang.org/protobuf that
// gives KafkaMCP a first-class Confluent-Schema-Registry-interoperable
// Protobuf encoder/decoder.
//
// # Background
//
// Confluent's Protobuf wire format prepends a "message index" before
// the protobuf payload so the consumer knows which message type in a
// multi-message .proto file to decode against. The index is a varint
// array, zigzag-encoded:
//
//   - `[]int{0}`  → 1 byte 0x00     (the message at index 0; the
//     overwhelming common case)
//   - `[]int{N}`  → varint(1) varint(zigzag(N))
//   - `[]int{a,b}`→ varint(2) varint(zigzag(a)) varint(zigzag(b))
//
// See: https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-protobuf.html#wire-format
//
// # Design
//
// .proto schemas are parsed once via protocompile's pure-Go parser and
// cached by schema text. The compiled descriptor is then used to
// instantiate google.golang.org/protobuf/types/dynamicpb messages so
// we don't need code-generated Go types per schema. This matches the
// behaviour of confluent-kafka-go's GenericDeserializer.
//
// Cross-file imports are intentionally NOT supported: every schema
// must be self-contained because the Schema Registry API also returns
// a single .proto text per schema ID. Imports beyond the standard
// well-known types surface as a clear parse error.
//
// # Concurrency
//
// [Codec] is safe for concurrent use; the cache is guarded by
// sync.RWMutex and parsed descriptors are immutable.
package protobuf

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ErrSchemaParse is returned when a .proto schema string cannot be
// compiled by protocompile.
var ErrSchemaParse = errors.New("protobuf: schema parse failed")

// ErrMessageIndex is returned when a message index points to a
// non-existent message in the compiled schema.
var ErrMessageIndex = errors.New("protobuf: invalid message index")

// ErrEmptySchema is returned when the supplied schema declares no
// messages.
var ErrEmptySchema = errors.New("protobuf: schema declares no message types")

// schemaEntry caches a compiled file descriptor and the flat list of
// declared message descriptors in declaration order — Confluent
// message indices reference this exact ordering, including nested
// types via depth-first traversal.
type schemaEntry struct {
	file     protoreflect.FileDescriptor
	messages []protoreflect.MessageDescriptor
}

// Codec is a cached Protobuf encoder/decoder. The zero value is ready
// to use; consider [New] for the documented constructor.
type Codec struct {
	mu    sync.RWMutex
	cache map[string]*schemaEntry
}

// New returns a Protobuf codec with an empty parsed-schema cache.
func New() *Codec {
	return &Codec{cache: make(map[string]*schemaEntry)}
}

// CacheSize returns the number of parsed schemas held by the codec.
func (c *Codec) CacheSize() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}

// parse compiles schemaStr and indexes the resulting message
// descriptors in declaration order (Confluent uses this exact order
// for the message-index varint array).
func (c *Codec) parse(schemaStr string) (*schemaEntry, error) {
	c.mu.RLock()
	if e, ok := c.cache[schemaStr]; ok {
		c.mu.RUnlock()
		return e, nil
	}
	c.mu.RUnlock()

	const fakeFile = "schema.proto"
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(
			&protocompile.SourceResolver{
				Accessor: protocompile.SourceAccessorFromMap(map[string]string{
					fakeFile: schemaStr,
				}),
			},
		),
	}
	files, err := compiler.Compile(context.Background(), fakeFile)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSchemaParse, err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("%w: compiler returned no files", ErrSchemaParse)
	}
	entry := &schemaEntry{file: files[0]}
	entry.messages = collectMessages(files[0])
	if len(entry.messages) == 0 {
		return nil, ErrEmptySchema
	}

	c.mu.Lock()
	if c.cache == nil {
		c.cache = make(map[string]*schemaEntry)
	}
	c.cache[schemaStr] = entry
	c.mu.Unlock()
	return entry, nil
}

// collectMessages traverses the FileDescriptor in declaration order
// (top-level messages first, then their nested types depth-first)
// matching the Confluent / protoc index convention.
func collectMessages(file protoreflect.FileDescriptor) []protoreflect.MessageDescriptor {
	var out []protoreflect.MessageDescriptor
	msgs := file.Messages()
	for i := 0; i < msgs.Len(); i++ {
		walkMessages(msgs.Get(i), &out)
	}
	return out
}

func walkMessages(md protoreflect.MessageDescriptor, out *[]protoreflect.MessageDescriptor) {
	*out = append(*out, md)
	nested := md.Messages()
	for i := 0; i < nested.Len(); i++ {
		walkMessages(nested.Get(i), out)
	}
}

// resolveMessage returns the message descriptor at the given
// Confluent index list. An empty / nil index defaults to [0].
func (c *Codec) resolveMessage(entry *schemaEntry, idx []int) (protoreflect.MessageDescriptor, error) {
	if len(idx) == 0 {
		return entry.messages[0], nil
	}
	cursor := idx[0]
	if cursor < 0 || cursor >= len(entry.messages) {
		return nil, fmt.Errorf("%w: top-level index %d out of range [0, %d)", ErrMessageIndex, cursor, len(entry.messages))
	}
	md := entry.messages[cursor]
	for depth := 1; depth < len(idx); depth++ {
		nested := md.Messages()
		next := idx[depth]
		if next < 0 || next >= nested.Len() {
			return nil, fmt.Errorf("%w: nested index %d at depth %d out of range [0, %d)", ErrMessageIndex, next, depth, nested.Len())
		}
		md = nested.Get(next)
	}
	return md, nil
}

// Encode serialises value (a map[string]any) into Confluent's
// protobuf wire-format payload — message-index prefix followed by the
// binary protobuf payload. The returned bytes are MEANT to be wrapped
// by the Confluent magic byte + schema id (see internal/codec/wire).
// When msgIndex is nil or {0} the optimised single-zero-byte prefix
// is emitted.
func (c *Codec) Encode(schemaStr string, msgIndex []int, value any) ([]byte, error) {
	entry, err := c.parse(schemaStr)
	if err != nil {
		return nil, err
	}
	md, err := c.resolveMessage(entry, msgIndex)
	if err != nil {
		return nil, err
	}

	msg := dynamicpb.NewMessage(md)
	if err := populateMessage(msg, value); err != nil {
		return nil, fmt.Errorf("protobuf: populate %s: %w", md.FullName(), err)
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("protobuf: marshal %s: %w", md.FullName(), err)
	}
	prefix := encodeMessageIndex(msgIndex)
	out := make([]byte, 0, len(prefix)+len(payload))
	out = append(out, prefix...)
	out = append(out, payload...)
	return out, nil
}

// Decode parses Confluent's protobuf wire format (without the magic
// byte + schema-id, which the caller is expected to have stripped):
// it strips the message-index varint array, picks the matching
// descriptor, and unmarshals into a generic map[string]any.
func (c *Codec) Decode(schemaStr string, data []byte) (msgIndex []int, value any, err error) {
	entry, err := c.parse(schemaStr)
	if err != nil {
		return nil, nil, err
	}
	idx, payload, err := decodeMessageIndex(data)
	if err != nil {
		return nil, nil, err
	}
	md, err := c.resolveMessage(entry, idx)
	if err != nil {
		return nil, nil, err
	}
	msg := dynamicpb.NewMessage(md)
	if err := proto.Unmarshal(payload, msg); err != nil {
		return nil, nil, fmt.Errorf("protobuf: unmarshal %s: %w", md.FullName(), err)
	}
	return idx, messageToMap(msg), nil
}

// ParseSchema validates that schemaStr is a syntactically valid
// .proto schema without serialising anything.
func (c *Codec) ParseSchema(schemaStr string) error {
	_, err := c.parse(schemaStr)
	return err
}

// --- message-index codec ------------------------------------------------

// encodeMessageIndex emits Confluent's varint-zigzag message-index
// prefix.
//   - nil or {0} → single 0x00 byte (optimisation).
//   - {N}        → varint(1) varint(zigzag(N))
//   - {a,b,...}  → varint(len) varint(zigzag(a)) varint(zigzag(b))...
func encodeMessageIndex(idx []int) []byte {
	if len(idx) == 0 || (len(idx) == 1 && idx[0] == 0) {
		return []byte{0x00}
	}
	buf := make([]byte, 0, 8)
	buf = binary.AppendVarint(buf, int64(len(idx)))
	for _, v := range idx {
		buf = binary.AppendVarint(buf, int64(v))
	}
	return buf
}

// decodeMessageIndex strips the Confluent message-index prefix and
// returns the resolved []int (defaulting to {0} for the
// single-zero-byte optimisation).
func decodeMessageIndex(data []byte) ([]int, []byte, error) {
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("%w: empty payload", ErrMessageIndex)
	}
	if data[0] == 0x00 {
		return []int{0}, data[1:], nil
	}
	count, n := binary.Varint(data)
	if n <= 0 {
		return nil, nil, fmt.Errorf("%w: cannot decode index length", ErrMessageIndex)
	}
	if count < 0 || count > 1024 {
		return nil, nil, fmt.Errorf("%w: implausible index length %d", ErrMessageIndex, count)
	}
	idx := make([]int, 0, count)
	cursor := n
	for i := int64(0); i < count; i++ {
		v, nn := binary.Varint(data[cursor:])
		if nn <= 0 {
			return nil, nil, fmt.Errorf("%w: cannot decode index entry %d", ErrMessageIndex, i)
		}
		idx = append(idx, int(v))
		cursor += nn
	}
	return idx, data[cursor:], nil
}

// --- generic <-> dynamicpb -----------------------------------------------

// populateMessage walks value (expected map[string]any) and assigns
// matching fields on msg. Unknown fields are silently ignored —
// matches confluent-kafka-go's forgiving stance for forward-compat.
func populateMessage(msg *dynamicpb.Message, value any) error {
	asMap, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("protobuf: expected map[string]any value, got %T", value)
	}
	desc := msg.Descriptor()
	fields := desc.Fields()
	for name, val := range asMap {
		fd := fields.ByJSONName(name)
		if fd == nil {
			fd = fields.ByName(protoreflect.Name(name))
		}
		if fd == nil {
			continue
		}
		pv, err := toProtoValue(msg, fd, val)
		if err != nil {
			return fmt.Errorf("field %q: %w", name, err)
		}
		if pv.IsValid() {
			msg.Set(fd, pv)
		}
	}
	return nil
}

func toProtoValue(parent *dynamicpb.Message, fd protoreflect.FieldDescriptor, v any) (protoreflect.Value, error) {
	if v == nil {
		return protoreflect.Value{}, nil
	}
	if fd.IsList() {
		list := parent.Mutable(fd).List()
		arr, ok := v.([]any)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected []any for repeated field, got %T", v)
		}
		for i, item := range arr {
			pv, err := scalarOrMessageToValue(fd, item)
			if err != nil {
				return protoreflect.Value{}, fmt.Errorf("item %d: %w", i, err)
			}
			list.Append(pv)
		}
		return protoreflect.ValueOfList(list), nil
	}
	return scalarOrMessageToValue(fd, v)
}

func scalarOrMessageToValue(fd protoreflect.FieldDescriptor, v any) (protoreflect.Value, error) {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		b, ok := v.(bool)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected bool, got %T", v)
		}
		return protoreflect.ValueOfBool(b), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		i, err := toInt64(v)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt32(int32(i)), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		i, err := toInt64(v)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt64(i), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		i, err := toInt64(v)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfUint32(uint32(i)), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		i, err := toInt64(v)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfUint64(uint64(i)), nil
	case protoreflect.FloatKind:
		f, err := toFloat64(v)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfFloat32(float32(f)), nil
	case protoreflect.DoubleKind:
		f, err := toFloat64(v)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfFloat64(f), nil
	case protoreflect.StringKind:
		s, ok := v.(string)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected string, got %T", v)
		}
		return protoreflect.ValueOfString(s), nil
	case protoreflect.BytesKind:
		switch x := v.(type) {
		case []byte:
			return protoreflect.ValueOfBytes(x), nil
		case string:
			return protoreflect.ValueOfBytes([]byte(x)), nil
		default:
			return protoreflect.Value{}, fmt.Errorf("expected []byte/string, got %T", v)
		}
	case protoreflect.EnumKind:
		switch x := v.(type) {
		case string:
			vd := fd.Enum().Values().ByName(protoreflect.Name(x))
			if vd == nil {
				return protoreflect.Value{}, fmt.Errorf("unknown enum value %q", x)
			}
			return protoreflect.ValueOfEnum(vd.Number()), nil
		default:
			i, err := toInt64(v)
			if err != nil {
				return protoreflect.Value{}, err
			}
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(i)), nil
		}
	case protoreflect.MessageKind, protoreflect.GroupKind:
		nestedMsg := dynamicpb.NewMessage(fd.Message())
		if err := populateMessage(nestedMsg, v); err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfMessage(nestedMsg), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported field kind %s", fd.Kind())
	}
}

// messageToMap converts a dynamicpb message back to a generic
// map[string]any using JSON field names where available.
func messageToMap(msg *dynamicpb.Message) map[string]any {
	out := map[string]any{}
	desc := msg.Descriptor()
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if !msg.Has(fd) {
			continue
		}
		out[fd.JSONName()] = valueToAny(fd, msg.Get(fd))
	}
	return out
}

func valueToAny(fd protoreflect.FieldDescriptor, v protoreflect.Value) any {
	if fd.IsList() {
		list := v.List()
		arr := make([]any, list.Len())
		for i := 0; i < list.Len(); i++ {
			arr[i] = scalarValueToAny(fd, list.Get(i))
		}
		return arr
	}
	return scalarValueToAny(fd, v)
}

func scalarValueToAny(fd protoreflect.FieldDescriptor, v protoreflect.Value) any {
	switch fd.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		if nested, ok := v.Message().Interface().(*dynamicpb.Message); ok {
			return messageToMap(nested)
		}
		return v.Interface()
	case protoreflect.EnumKind:
		ed := fd.Enum().Values().ByNumber(v.Enum())
		if ed != nil {
			return string(ed.Name())
		}
		return int32(v.Enum())
	case protoreflect.BytesKind:
		return v.Bytes()
	default:
		return v.Interface()
	}
}

// toInt64 coerces common numeric Go types (including float64 from
// JSON decoding) to int64.
func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case uint:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		return int64(x), nil
	case float32:
		return int64(x), nil
	case float64:
		return int64(x), nil
	default:
		return 0, fmt.Errorf("cannot coerce %T to integer", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float32:
		return float64(x), nil
	case float64:
		return x, nil
	case int:
		return float64(x), nil
	case int32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	default:
		return 0, fmt.Errorf("cannot coerce %T to float", v)
	}
}
