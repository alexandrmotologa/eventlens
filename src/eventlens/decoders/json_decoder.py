"""JSON payload decoder for EventLens."""

from __future__ import annotations

import json
from typing import Any

from eventlens.decoders.base import BaseDecoder, DecodedPayload


class JsonDecoder(BaseDecoder):
    """Decodes UTF-8 JSON payloads into Python objects."""

    @property
    def name(self) -> str:
        return "json"

    def can_decode(self, raw: bytes, headers: dict[str, str] | None = None) -> bool:
        """Check if bytes look like a JSON object or array."""
        if not raw:
            return False

        stripped = raw.strip()
        if not (
            (stripped.startswith(b"{") and stripped.endswith(b"}"))
            or (stripped.startswith(b"[") and stripped.endswith(b"]"))
        ):
            return False

        try:
            text = raw.decode("utf-8")
            json.loads(text)
            return True
        except (UnicodeDecodeError, json.JSONDecodeError):
            return False

    def decode(self, raw: bytes, headers: dict[str, str] | None = None) -> DecodedPayload:
        """Parse raw bytes as JSON."""
        try:
            text = raw.decode("utf-8")
            data: Any = json.loads(text)
            return DecodedPayload(
                format=self.name,
                data=data,
                raw_bytes=raw,
                is_structured=True,
            )
        except Exception as err:
            return DecodedPayload(
                format=self.name,
                data=None,
                raw_bytes=raw,
                is_structured=False,
                error=f"JSON decode failed: {err}",
            )
