"""Confluent Schema Registry wire-format Avro decoder."""

from __future__ import annotations

import struct
from typing import Any

from eventlens.decoders.base import BaseDecoder, DecodedPayload


class AvroDecoder(BaseDecoder):
    """Decodes Confluent Schema Registry wire-format frames (0x00 + schema ID + payload)."""

    def __init__(self, schema_registry_url: str | None = None) -> None:
        self.schema_registry_url = schema_registry_url

    @property
    def name(self) -> str:
        return "avro"

    def can_decode(self, raw: bytes, headers: dict[str, str] | None = None) -> bool:
        """Check if message starts with Confluent magic byte 0x00 and is at least 5 bytes."""
        if not raw or len(raw) < 5:
            return False
        return raw[0] == 0x00

    def decode(self, raw: bytes, headers: dict[str, str] | None = None) -> DecodedPayload:
        """Extract schema ID and parse payload."""
        if len(raw) < 5 or raw[0] != 0x00:
            return DecodedPayload(
                format=self.name,
                data=None,
                raw_bytes=raw,
                is_structured=False,
                error="Invalid Confluent wire format header",
            )

        schema_id = struct.unpack(">I", raw[1:5])[0]
        payload_bytes = raw[5:]

        # Try parsing payload as embedded JSON first
        try:
            import json

            text = payload_bytes.decode("utf-8")
            data = json.loads(text)
            return DecodedPayload(
                format=self.name,
                data={"_schema_id": schema_id, **(data if isinstance(data, dict) else {"content": data})},
                raw_bytes=raw,
                is_structured=True,
            )
        except Exception:
            # Binary Avro payload representation
            data: dict[str, Any] = {
                "_schema_id": schema_id,
                "_wire_format": "confluent-avro",
                "payload_hex": payload_bytes.hex(),
                "payload_length_bytes": len(payload_bytes),
            }
            return DecodedPayload(
                format=self.name,
                data=data,
                raw_bytes=raw,
                is_structured=True,
            )
