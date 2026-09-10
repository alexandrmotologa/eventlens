"""Core data models for EventLens."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class KafkaRecord(BaseModel):
    """Represents a consumed or simulated Kafka record."""

    topic: str
    partition: int
    offset: int
    timestamp: int | None = None
    key: str | None = None
    value: str | None = None
    raw_key: bytes | None = Field(default=None, repr=False)
    raw_value: bytes | None = Field(default=None, repr=False)
    headers: dict[str, str] = Field(default_factory=dict)

    # Decoded representation
    decoded_key: Any = None
    decoded_payload: Any = None
    payload_format: str = "raw"
    is_dlq: bool = False
    error_message: str | None = None

    def to_summary_dict(self) -> dict[str, Any]:
        """Convert record to a human-readable dictionary."""
        return {
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
            "timestamp": self.timestamp,
            "key": self.key,
            "format": self.payload_format,
            "is_dlq": self.is_dlq,
            "headers": self.headers,
            "payload": self.decoded_payload or self.value,
        }


class PartitionLag(BaseModel):
    """Lag status for a specific partition."""

    topic: str
    partition: int
    log_end_offset: int
    current_offset: int
    lag: int


class ConsumerGroupLag(BaseModel):
    """Aggregated lag information for a consumer group."""

    group_id: str
    topic: str
    partitions: list[PartitionLag]
    total_lag: int


class DLQDiagnostic(BaseModel):
    """Heuristic diagnostic result for a dead-lettered message."""

    failure_category: str
    root_cause: str
    suggested_action: str
    original_topic: str | None = None
    original_partition: int | None = None
    original_offset: int | None = None
    exception_type: str | None = None
    exception_message: str | None = None
    stack_trace: str | None = None


class RedriveResult(BaseModel):
    """Result of redriving a dead-lettered message."""

    success: bool
    source_topic: str
    target_topic: str
    source_offset: int
    produced_partition: int | None = None
    produced_offset: int | None = None
    error: str | None = None


class BatchRedriveSummary(BaseModel):
    """Summary of batch redrive execution."""

    source_topic: str
    target_topic: str
    total_scanned: int
    matched_count: int
    redriven_count: int
    skipped_count: int
    failed_count: int
    dry_run: bool
    results: list[RedriveResult] = Field(default_factory=list)
