"""Configuration management for EventLens."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class EventLensConfig(BaseModel):
    """EventLens runtime and Kafka connection settings."""

    bootstrap_servers: str = Field(default="localhost:9092", description="Comma-separated Kafka broker addresses")
    client_id: str = Field(default="eventlens-debugger", description="Kafka client identifier")
    group_id_prefix: str = Field(default="eventlens-debug", description="Prefix for ephemeral consumer group IDs")
    security_protocol: str = Field(
        default="PLAINTEXT", description="Security protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL"
    )
    sasl_mechanism: str | None = Field(default=None, description="SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512")
    sasl_username: str | None = None
    sasl_password: str | None = None
    ssl_cafile: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None

    request_timeout_ms: int = Field(default=15000, description="Kafka request timeout in ms")
    default_poll_timeout_ms: int = Field(default=1000, description="Poll timeout in ms")
    auto_offset_reset: str = Field(default="latest", description="Offset reset strategy")

    # Decoders settings
    default_proto_dir: str | None = None
    schema_registry_url: str | None = None

    @classmethod
    def from_file(cls, path: str | Path) -> EventLensConfig:
        """Load configuration from a JSON file."""
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"Config file not found: {file_path}")

        content = file_path.read_text(encoding="utf-8")
        data: dict[str, Any] = json.loads(content)
        return cls(**data)

    def to_aiokafka_consumer_kwargs(self) -> dict[str, Any]:
        """Convert config to aiokafka consumer kwargs."""
        kwargs: dict[str, Any] = {
            "bootstrap_servers": self.bootstrap_servers,
            "client_id": self.client_id,
            "security_protocol": self.security_protocol,
            "request_timeout_ms": self.request_timeout_ms,
            "auto_offset_reset": self.auto_offset_reset,
            "enable_auto_commit": False,
        }

        if self.sasl_mechanism:
            kwargs["sasl_mechanism"] = self.sasl_mechanism
        if self.sasl_username and self.sasl_password:
            kwargs["sasl_plain_username"] = self.sasl_username
            kwargs["sasl_plain_password"] = self.sasl_password

        return kwargs

    def to_aiokafka_producer_kwargs(self) -> dict[str, Any]:
        """Convert config to aiokafka producer kwargs."""
        kwargs: dict[str, Any] = {
            "bootstrap_servers": self.bootstrap_servers,
            "client_id": f"{self.client_id}-producer",
            "security_protocol": self.security_protocol,
            "request_timeout_ms": self.request_timeout_ms,
        }

        if self.sasl_mechanism:
            kwargs["sasl_mechanism"] = self.sasl_mechanism
        if self.sasl_username and self.sasl_password:
            kwargs["sasl_plain_username"] = self.sasl_username
            kwargs["sasl_plain_password"] = self.sasl_password

        return kwargs
