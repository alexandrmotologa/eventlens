"""Abstract base decoder for EventLens."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel


class DecodedPayload(BaseModel):
    """Result of payload decoding."""

    format: str
    data: Any
    raw_bytes: bytes | None = None
    is_structured: bool = False
    error: str | None = None

    def as_json_string(self, indent: int = 2) -> str:
        """Render decoded data as a formatted JSON string if possible."""
        import json

        if isinstance(self.data, (dict, list)):
            return json.dumps(self.data, indent=indent, default=str)
        if isinstance(self.data, str):
            return self.data
        return str(self.data)


class BaseDecoder(ABC):
    """Abstract interface for payload decoders."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the decoder format."""
        pass

    @abstractmethod
    def can_decode(self, raw: bytes, headers: dict[str, str] | None = None) -> bool:
        """Check if raw bytes can be processed by this decoder."""
        pass

    @abstractmethod
    def decode(self, raw: bytes, headers: dict[str, str] | None = None) -> DecodedPayload:
        """Decode raw bytes into a DecodedPayload."""
        pass
