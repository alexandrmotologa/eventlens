"""Unit tests for DLQ failure heuristics and redrive logic."""

from __future__ import annotations

import pytest

from eventlens.engine.dlq_engine import DLQEngine
from eventlens.models import KafkaRecord


def test_diagnose_json_syntax_error() -> None:
    rec = KafkaRecord(
        topic="order.events.dlq",
        partition=0,
        offset=501,
        headers={
            "x-exception-message": "JsonParseException: Unexpected character '}' at offset 42",
            "x-original-topic": "order.events",
            "x-original-partition": "2",
            "x-original-offset": "100",
        },
    )

    diag = DLQEngine.diagnose_record(rec)
    assert diag.failure_category == "Deserialization Syntax Error"
    assert diag.original_topic == "order.events"
    assert diag.original_partition == 2
    assert diag.original_offset == 100
    assert "JSON" in diag.suggested_action


def test_diagnose_validation_failure() -> None:
    rec = KafkaRecord(
        topic="order.events.dlq",
        partition=1,
        offset=999,
        headers={
            "x-exception-message": "ConstraintViolationException: total_amount must be positive",
            "x-original-topic": "order.events",
        },
    )

    diag = DLQEngine.diagnose_record(rec)
    assert diag.failure_category == "Schema Validation Failure"
    assert "constraint" in diag.suggested_action.lower()


def test_diagnose_timeout_outage() -> None:
    rec = KafkaRecord(
        topic="order.events.dlq",
        partition=3,
        offset=202,
        headers={
            "x-exception-message": "TimeoutException: downstream connection timed out",
            "x-original-topic": "order.events",
        },
    )

    diag = DLQEngine.diagnose_record(rec)
    assert diag.failure_category == "Downstream Service Outage"
    assert "safe to redrive" in diag.suggested_action.lower()


@pytest.mark.asyncio
async def test_dlq_redrive_missing_target_fails() -> None:
    engine = DLQEngine()
    rec = KafkaRecord(
        topic="order.events.dlq",
        partition=0,
        offset=12,
        headers={},  # Missing x-original-topic
    )

    result = await engine.redrive(record=rec)
    assert result.success is False
    assert "unspecified" in result.error.lower()
