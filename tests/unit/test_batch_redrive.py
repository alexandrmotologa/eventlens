"""Unit tests for batch DLQ redrive and dry-run execution."""

from __future__ import annotations

import pytest

from eventlens.engine.dlq_engine import DLQEngine
from eventlens.models import KafkaRecord


@pytest.fixture
def sample_dlq_records() -> list[KafkaRecord]:
    return [
        KafkaRecord(
            topic="order.events.dlq",
            partition=0,
            offset=10,
            decoded_payload={"order_id": "ord_01", "retryable": True, "total": 50.0},
            headers={
                "x-exception-message": "TimeoutException: downstream connection timed out",
                "x-original-topic": "order.events",
            },
        ),
        KafkaRecord(
            topic="order.events.dlq",
            partition=0,
            offset=11,
            decoded_payload={"order_id": "ord_02", "retryable": False, "total": 150.0},
            headers={
                "x-exception-message": "JsonParseException: invalid character at pos 3",
                "x-original-topic": "order.events",
            },
        ),
        KafkaRecord(
            topic="order.events.dlq",
            partition=1,
            offset=12,
            decoded_payload={"order_id": "ord_03", "retryable": True, "total": 200.0},
            headers={
                "x-exception-message": "ConnectionRefusedError: service 503 unavailable",
                "x-original-topic": "order.events",
            },
        ),
        KafkaRecord(
            topic="order.events.dlq",
            partition=1,
            offset=13,
            decoded_payload={"order_id": "ord_04", "retryable": True, "total": 80.0},
            headers={},  # Missing x-original-topic
        ),
    ]


@pytest.mark.asyncio
async def test_batch_redrive_dry_run(sample_dlq_records: list[KafkaRecord]) -> None:
    engine = DLQEngine()
    summary = await engine.redrive_batch(
        records=sample_dlq_records,
        dry_run=True,
    )

    assert summary.dry_run is True
    assert summary.total_scanned == 4
    # ord_04 has no target and no x-original-topic, so 3 match
    assert summary.matched_count == 3
    assert summary.skipped_count == 1
    assert summary.redriven_count == 3
    assert len(summary.results) == 3
    assert all(r.success for r in summary.results)


@pytest.mark.asyncio
async def test_batch_redrive_category_filter(sample_dlq_records: list[KafkaRecord]) -> None:
    engine = DLQEngine()
    summary = await engine.redrive_batch(
        records=sample_dlq_records,
        category="Outage",  # Matches "Downstream Service Outage"
        dry_run=True,
    )

    # 2 outage records (ord_01 and ord_03)
    assert summary.matched_count == 2
    assert summary.skipped_count == 2
    assert summary.redriven_count == 2


@pytest.mark.asyncio
async def test_batch_redrive_jmespath_filter(sample_dlq_records: list[KafkaRecord]) -> None:
    engine = DLQEngine()
    summary = await engine.redrive_batch(
        records=sample_dlq_records,
        filter_expr="payload.total > `100.0`",
        dry_run=True,
    )

    # ord_02 (150.0) and ord_03 (200.0) match
    assert summary.matched_count == 2
    assert summary.redriven_count == 2


@pytest.mark.asyncio
async def test_batch_redrive_limit(sample_dlq_records: list[KafkaRecord]) -> None:
    engine = DLQEngine()
    summary = await engine.redrive_batch(
        records=sample_dlq_records,
        target_topic="order.events.replayed",
        limit=1,
        dry_run=True,
    )

    assert summary.matched_count == 1
    assert summary.redriven_count == 1
    assert summary.target_topic == "order.events.replayed"
