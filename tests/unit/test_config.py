"""Unit tests for configuration loading and validation."""

from __future__ import annotations

import json
from pathlib import Path

from eventlens.config import EventLensConfig


def test_default_config() -> None:
    cfg = EventLensConfig()
    assert cfg.bootstrap_servers == "localhost:9092"
    assert cfg.security_protocol == "PLAINTEXT"
    assert cfg.client_id == "eventlens-debugger"


def test_aiokafka_kwargs() -> None:
    cfg = EventLensConfig(
        bootstrap_servers="kafka-broker:9094",
        sasl_mechanism="PLAIN",
        sasl_username="admin",
        sasl_password="secret-password",
    )

    kwargs = cfg.to_aiokafka_consumer_kwargs()
    assert kwargs["bootstrap_servers"] == "kafka-broker:9094"
    assert kwargs["sasl_mechanism"] == "PLAIN"
    assert kwargs["sasl_plain_username"] == "admin"
    assert kwargs["sasl_plain_password"] == "secret-password"


def test_load_from_json_file(tmp_path: Path) -> None:
    file = tmp_path / "config.json"
    data = {
        "bootstrap_servers": "remote.kafka.internal:9092",
        "client_id": "custom-lens",
        "default_proto_dir": "/opt/proto",
    }
    file.write_text(json.dumps(data), encoding="utf-8")

    cfg = EventLensConfig.from_file(file)
    assert cfg.bootstrap_servers == "remote.kafka.internal:9092"
    assert cfg.client_id == "custom-lens"
    assert cfg.default_proto_dir == "/opt/proto"
