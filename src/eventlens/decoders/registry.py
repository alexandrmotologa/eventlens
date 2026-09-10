"""Decoder registry and auto-detection engine."""

from __future__ import annotations

from eventlens.decoders.avro_decoder import AvroDecoder
from eventlens.decoders.base import BaseDecoder, DecodedPayload
from eventlens.decoders.json_decoder import JsonDecoder
from eventlens.decoders.proto_decoder import ProtoDecoder


class RawDecoder(BaseDecoder):
    """Fallback decoder converting raw bytes to string or hex."""

    @property
    def name(self) -> str:
        return "raw"

    def can_decode(self, raw: bytes, headers: dict[str, str] | None = None) -> bool:
        return True

    def decode(self, raw: bytes, headers: dict[str, str] | None = None) -> DecodedPayload:
        if not raw:
            return DecodedPayload(format=self.name, data="", raw_bytes=raw, is_structured=False)
        try:
            text = raw.decode("utf-8")
            return DecodedPayload(format="text", data=text, raw_bytes=raw, is_structured=False)
        except UnicodeDecodeError:
            return DecodedPayload(
                format="binary",
                data=f"0x{raw.hex()}",
                raw_bytes=raw,
                is_structured=False,
            )


class DecoderRegistry:
    """Manages decoders and applies auto-detection heuristics."""

    def __init__(
        self,
        proto_file_path: str | None = None,
        proto_message_type: str | None = None,
        schema_registry_url: str | None = None,
    ) -> None:
        self.decoders: list[BaseDecoder] = []

        # Order matters:
        # 1. Avro (strictly checked via Confluent magic byte 0x00)
        self.avro_decoder = AvroDecoder(schema_registry_url=schema_registry_url)
        self.decoders.append(self.avro_decoder)

        # 2. JSON (strictly checked via valid UTF-8 object/array syntax)
        self.json_decoder = JsonDecoder()
        self.decoders.append(self.json_decoder)

        # 3. Protobuf (if proto file or content provided or wire heuristics)
        self.proto_decoder = ProtoDecoder(
            proto_file_path=proto_file_path,
            message_type=proto_message_type,
        )
        self.decoders.append(self.proto_decoder)

        # 4. Fallback raw decoder
        self.raw_decoder = RawDecoder()
        self.decoders.append(self.raw_decoder)

    def set_proto_schema(self, proto_path: str, message_type: str | None = None) -> None:
        """Dynamically load or update Protobuf schema."""
        self.proto_decoder.load_schema(proto_file_path=proto_path)
        if message_type:
            self.proto_decoder.message_type = message_type

    def decode(self, raw: bytes | None, headers: dict[str, str] | None = None) -> DecodedPayload:
        """Auto-detect and decode payload bytes."""
        if raw is None:
            return DecodedPayload(format="null", data=None, raw_bytes=b"", is_structured=False)

        headers = headers or {}

        # If user explicitly supplied proto schema and headers hint protobuf, try proto first
        ct = headers.get("content-type", "") or headers.get("Content-Type", "")
        if "protobuf" in ct.lower() and self.proto_decoder.can_decode(raw, headers):
            return self.proto_decoder.decode(raw, headers)

        for decoder in self.decoders:
            if decoder.can_decode(raw, headers):
                decoded = decoder.decode(raw, headers)
                if not decoded.error:
                    return decoded

        return self.raw_decoder.decode(raw, headers)
