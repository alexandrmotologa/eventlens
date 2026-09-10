"""Unit tests for JSON, Protobuf, and Avro payload decoders."""

from __future__ import annotations

import struct
from pathlib import Path

from eventlens.decoders import AvroDecoder, DecoderRegistry, JsonDecoder, ProtoDecoder


def test_json_decoder_valid() -> None:
    decoder = JsonDecoder()
    payload = b'{"order_id": "ord_100", "total": 49.99, "currency": "USD"}'

    assert decoder.can_decode(payload) is True
    decoded = decoder.decode(payload)
    assert decoded.format == "json"
    assert decoded.is_structured is True
    assert decoded.data["order_id"] == "ord_100"
    assert decoded.data["total"] == 49.99


def test_json_decoder_invalid() -> None:
    decoder = JsonDecoder()
    assert decoder.can_decode(b"not json at all") is False
    assert decoder.can_decode(b"{corrupted json:") is False


def test_avro_decoder_confluent_format() -> None:
    decoder = AvroDecoder()
    schema_id = 42
    payload_inner = b'{"customer": "cust_1"}'
    wire_bytes = struct.pack(">bI", 0x00, schema_id) + payload_inner

    assert decoder.can_decode(wire_bytes) is True
    decoded = decoder.decode(wire_bytes)
    assert decoded.format == "avro"
    assert decoded.data["_schema_id"] == 42
    assert decoded.data["customer"] == "cust_1"


def test_avro_decoder_rejects_non_magic_bytes() -> None:
    decoder = AvroDecoder()
    assert decoder.can_decode(b"\x01\x00\x00\x00*payload") is False
    assert decoder.can_decode(b"short") is False


def test_proto_decoder_with_fixture() -> None:
    fixture_path = Path(__file__).parent.parent / "fixtures" / "sample_order.proto"
    decoder = ProtoDecoder(proto_file_path=fixture_path, message_type="OrderCreatedProto")

    # Serialize a message using the dynamically created class
    cls = decoder.message_classes["OrderCreatedProto"]
    msg = cls(
        event_id="evt_001",
        order_id="ord_999",
        customer_id="cust_88",
        currency="EUR",
        total_amount="120.50",
        timestamp=1700000000,
        sequence_number=1,
    )
    raw_bytes = msg.SerializeToString()

    assert decoder.can_decode(raw_bytes) is True
    decoded = decoder.decode(raw_bytes)
    assert decoded.format == "protobuf"
    assert decoded.data["order_id"] == "ord_999"
    assert decoded.data["currency"] == "EUR"
    assert decoded.data["total_amount"] == "120.50"


def test_proto_decoder_schemaless_fallback() -> None:
    decoder = ProtoDecoder()  # No schema registered
    # Manually pack varint tag 1 = 123
    # tag 1, wire type 0 (varint) -> (1 << 3) | 0 = 0x08
    raw_bytes = b"\x08\x7b"
    headers = {"content-type": "application/x-protobuf"}

    assert decoder.can_decode(raw_bytes, headers=headers) is True
    decoded = decoder.decode(raw_bytes, headers=headers)
    assert decoded.format == "protobuf"
    assert decoded.data["tag_1"] == 123


def test_decoder_registry_auto_detect() -> None:
    registry = DecoderRegistry()

    # JSON auto detection
    json_bytes = b'{"status": "CONFIRMED"}'
    res_json = registry.decode(json_bytes)
    assert res_json.format == "json"
    assert res_json.data["status"] == "CONFIRMED"

    # Avro auto detection
    avro_wire = struct.pack(">bI", 0x00, 101) + b'{"status": "AVRO_EVENT"}'
    res_avro = registry.decode(avro_wire)
    assert res_avro.format == "avro"
    assert res_avro.data["_schema_id"] == 101

    # Text fallback
    text_bytes = b"Hello raw Kafka world"
    res_text = registry.decode(text_bytes)
    assert res_text.format == "text"
    assert res_text.data == "Hello raw Kafka world"

    # None handling
    res_none = registry.decode(None)
    assert res_none.format == "null"
