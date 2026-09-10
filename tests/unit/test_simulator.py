"""Unit tests for the mock stream simulator."""

from __future__ import annotations

from eventlens.engine.simulator import MockStreamSimulator


def test_simulator_generates_order_events() -> None:
    sim = MockStreamSimulator(topic="order.events", failure_rate=0.0)
    rec = sim.generate_record(force_dlq=False)

    assert rec.topic == "order.events"
    assert rec.is_dlq is False
    assert rec.payload_format == "json"
    assert "order_id" in rec.decoded_payload
    assert "currency" in rec.decoded_payload


def test_simulator_generates_dlq_poison_pills() -> None:
    sim = MockStreamSimulator(topic="order.events")
    rec = sim.generate_record(force_dlq=True)

    assert rec.is_dlq is True
    assert rec.topic == "order.events.dlq"
    assert "x-exception-message" in rec.headers
    assert rec.error_message is not None


def test_simulator_mock_lag() -> None:
    sim = MockStreamSimulator()
    lag = sim.generate_mock_lag("order.events", "test-group")

    assert lag.group_id == "test-group"
    assert len(lag.partitions) == 4
    for p in lag.partitions:
        assert p.log_end_offset >= p.current_offset
        assert p.lag == (p.log_end_offset - p.current_offset)
