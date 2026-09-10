"""Unit tests for distributed event tracing and lifecycle correlation graph."""

from __future__ import annotations

from eventlens.engine.tracer import EventTracer
from eventlens.models import KafkaRecord


def test_record_matches_correlation_headers() -> None:
    rec = KafkaRecord(
        topic="order.events",
        partition=0,
        offset=100,
        headers={"x-correlation-id": "req-99128-abc"},
    )
    assert EventTracer.record_matches_correlation(rec, "req-99128-abc")
    assert EventTracer.record_matches_correlation(rec, "99128")
    assert not EventTracer.record_matches_correlation(rec, "unknown-id")


def test_record_matches_correlation_payload() -> None:
    rec = KafkaRecord(
        topic="payment.events",
        partition=1,
        offset=200,
        decoded_payload={"order_id": "ord_5521", "amount": 49.99},
    )
    assert EventTracer.record_matches_correlation(rec, "ord_5521")
    assert not EventTracer.record_matches_correlation(rec, "ord_9999")


def test_build_trace_graph_hops_and_latency() -> None:
    records = [
        KafkaRecord(
            topic="order.events",
            partition=0,
            offset=10,
            timestamp=1000,
            key="ord_123",
            decoded_payload={"event_type": "OrderPlaced", "order_id": "ord_123"},
        ),
        KafkaRecord(
            topic="unrelated.topic",
            partition=0,
            offset=55,
            timestamp=1050,
            key="other_key",
        ),
        KafkaRecord(
            topic="payment.events",
            partition=1,
            offset=20,
            timestamp=1200,
            key="ord_123",
            decoded_payload={"event_type": "PaymentAuthorized", "order_id": "ord_123"},
        ),
        KafkaRecord(
            topic="order.events.dlq",
            partition=0,
            offset=5,
            timestamp=1350,
            key="ord_123",
            decoded_payload={"order_id": "ord_123"},
            is_dlq=True,
            error_message="InventoryAllocationException: Out of stock",
        ),
    ]

    graph = EventTracer.build_trace_graph(correlation_id="ord_123", records=records)

    assert graph.correlation_id == "ord_123"
    assert len(graph.hops) == 3
    assert graph.has_dlq is True
    assert graph.total_latency_ms == 350.0

    # Hop 1
    assert graph.hops[0].topic == "order.events"
    assert graph.hops[0].latency_from_prev_ms is None
    assert graph.hops[0].status == "OK"

    # Hop 2
    assert graph.hops[1].topic == "payment.events"
    assert graph.hops[1].latency_from_prev_ms == 200.0

    # Hop 3 (DLQ)
    assert graph.hops[2].topic == "order.events.dlq"
    assert graph.hops[2].latency_from_prev_ms == 150.0
    assert graph.hops[2].is_dlq is True
    assert graph.hops[2].status == "DLQ_POISON"
