"""Distributed event tracing engine across multiple Kafka topics."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from eventlens.config import EventLensConfig
from eventlens.models import KafkaRecord


class TraceHop(BaseModel):
    """A single step in a distributed event lifecycle."""

    topic: str
    partition: int
    offset: int
    timestamp: int | None = None
    event_type: str | None = None
    key: str | None = None
    latency_from_prev_ms: float | None = None
    is_dlq: bool = False
    status: str = "OK"
    error_message: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    payload: Any = None


class TraceGraph(BaseModel):
    """Correlated event lifecycle graph."""

    correlation_id: str
    hops: list[TraceHop] = Field(default_factory=list)
    total_latency_ms: float | None = None
    has_dlq: bool = False


class EventTracer:
    """Finds and correlates events across topics matching correlation IDs or entity keys."""

    def __init__(self, config: EventLensConfig | None = None) -> None:
        self.config = config or EventLensConfig()

    @classmethod
    def record_matches_correlation(cls, record: KafkaRecord, correlation_id: str) -> bool:
        """Check if record matches target correlation ID across headers, key, or payload."""
        cid = correlation_id.lower()

        # 1. Match key
        if record.key and cid in record.key.lower():
            return True

        # 2. Match standard headers
        for h_key in (
            "x-correlation-id",
            "x-trace-id",
            "correlation_id",
            "traceparent",
            "x-b3-traceid",
        ):
            val = record.headers.get(h_key, "")
            if val and cid in val.lower():
                return True

        # 3. Match in payload properties
        if isinstance(record.decoded_payload, dict):
            for prop in ("order_id", "event_id", "transaction_id", "correlation_id", "customer_id"):
                val = str(record.decoded_payload.get(prop, "")).lower()
                if val and cid in val:
                    return True

        return False

    @classmethod
    def build_trace_graph(cls, correlation_id: str, records: list[KafkaRecord]) -> TraceGraph:
        """Build ordered chronological trace graph from matching records."""
        matching = [r for r in records if cls.record_matches_correlation(r, correlation_id)]

        # Sort by timestamp, fallback to offset
        matching.sort(key=lambda r: (r.timestamp or 0, r.offset))

        hops: list[TraceHop] = []
        prev_ts: int | None = None
        has_dlq = False

        for r in matching:
            latency: float | None = None
            if prev_ts is not None and r.timestamp is not None:
                latency = max(0.0, float(r.timestamp - prev_ts))
            prev_ts = r.timestamp

            evt_type = None
            if isinstance(r.decoded_payload, dict):
                evt_type = r.decoded_payload.get("event_type")

            if r.is_dlq:
                has_dlq = True
                status = "DLQ_POISON"
            else:
                status = "OK"

            hops.append(
                TraceHop(
                    topic=r.topic,
                    partition=r.partition,
                    offset=r.offset,
                    timestamp=r.timestamp,
                    event_type=evt_type,
                    key=r.key,
                    latency_from_prev_ms=latency,
                    is_dlq=r.is_dlq,
                    status=status,
                    error_message=r.error_message,
                    headers=r.headers,
                    payload=r.decoded_payload or r.value,
                )
            )

        total_latency = None
        if len(hops) >= 2 and hops[0].timestamp and hops[-1].timestamp:
            total_latency = float(hops[-1].timestamp - hops[0].timestamp)

        return TraceGraph(
            correlation_id=correlation_id,
            hops=hops,
            total_latency_ms=total_latency,
            has_dlq=has_dlq,
        )
