# Schema Decoders

Kafka topics carry binary data without attaching schema metadata to every byte. EventLens includes decoders to convert wire bytes into structured Python dictionaries and readable JSON.

## Supported Encodings

1. **JSON**: Parses standard UTF-8 JSON objects and arrays. Handles embedded timestamp strings and nested records.
2. **Google Protocol Buffers (Proto3)**: Parses dynamic messages directly from `.proto` source files or compiled descriptor sets without needing pre-generated Python files.
3. **Confluent Schema Registry Wire Format**: Identifies the 5-byte header prefix (magic byte `0x00` followed by a 4-byte big-endian schema ID) and extracts embedded Avro or JSON payloads.
4. **Raw Fallback**: If a payload cannot be decoded as a structured format, EventLens provides a printable ASCII representation and a hexadecimal dump.

## Dynamic Protobuf Reflection

When working with Protobuf, EventLens uses the `google.protobuf.descriptor_pool` API to construct descriptors at runtime:

```python
from eventlens.decoders.proto_decoder import ProtoDecoder

decoder = ProtoDecoder(
    proto_file_path="contracts/order_events.proto",
    message_type="OrderCreatedProto"
)

decoded = decoder.decode(raw_bytes)
print(decoded.data)
```

This approach lets you point EventLens to existing `.proto` files in your repositories without building intermediate wheel packages.

## Auto-Detection Strategy

The `DecoderRegistry` applies a sequence of heuristics to determine payload type:

1. Check for Confluent magic byte `0x00`. If present, parse the schema identifier.
2. Attempt UTF-8 decoding and JSON parsing. If the payload is valid JSON, tag it as `json`.
3. If an explicit Protobuf schema is provided through CLI or configuration, attempt Protobuf decoding.
4. Fall back to raw string or hex inspection.
