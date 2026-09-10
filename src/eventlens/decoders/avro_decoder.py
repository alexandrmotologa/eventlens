"""Confluent Schema Registry wire-format Avro decoder."""

from __future__ import annotations

import struct
from typing import Any

from eventlens.decoders.base import BaseDecoder, DecodedPayload
from eventlens.decoders.schema_registry import SchemaRegistryClient


class AvroDecoder(BaseDecoder):
    """Decodes Confluent Schema Registry wire-format frames (0x00 + schema ID + payload)."""

    def __init__(
        self,
        schema_registry_url: str | None = None,
        registry_client: SchemaRegistryClient | None = None,
    ) -> None:
        self.schema_registry_url = schema_registry_url
        self.registry_client = registry_client or (
            SchemaRegistryClient(base_url=schema_registry_url) if schema_registry_url else None
        )

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

        # Lookup schema in registry client if available
        resolved_schema = None
        if self.registry_client:
            resolved_schema = self.registry_client.get_cached_schema(schema_id)

        # Try parsing payload as embedded JSON first
        try:
            import json

            text = payload_bytes.decode("utf-8")
            data = json.loads(text)
            out_data: dict[str, Any] = {
                "_schema_id": schema_id,
                **(data if isinstance(data, dict) else {"content": data}),
            }
            if resolved_schema:
                out_data["_schema_definition"] = resolved_schema.get("schema")
                out_data["_schema_type"] = resolved_schema.get("schemaType", "AVRO")

            return DecodedPayload(
                format=self.name,
                data=out_data,
                raw_bytes=raw,
                is_structured=True,
            )
        except Exception:
            # Binary Avro payload representation with schema metadata
            data_bin: dict[str, Any] = {
                "_schema_id": schema_id,
                "_wire_format": "confluent-avro",
                "payload_hex": payload_bytes.hex(),
                "payload_length_bytes": len(payload_bytes),
            }
            if resolved_schema:
                data_bin["_schema_definition"] = resolved_schema.get("schema")
                data_bin["_schema_type"] = resolved_schema.get("schemaType", "AVRO")

            return DecodedPayload(
                format=self.name,
                data=data_bin,
                raw_bytes=raw,
                is_structured=True,
            )
