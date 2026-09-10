"""Traffic recording engine for capturing Kafka streams into .lens session files."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import AsyncGenerator

from eventlens.models import KafkaRecord


class TrafficRecorder:
    """Captures Kafka records and serializes them into timestamped session files."""

    def __init__(self, output_path: str | Path) -> None:
        self.output_path = Path(output_path)
        self.recorded_count = 0
        self.start_time: float | None = None
        self.end_time: float | None = None

    async def record_stream(
        self,
        stream: AsyncGenerator[KafkaRecord, None],
        max_messages: int | None = None,
        duration_seconds: float | None = None,
        stop_event: asyncio.Event | None = None,
    ) -> int:
        """Consume stream and append records to NDJSON file."""
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.start_time = time.time()
        self.recorded_count = 0

        with open(self.output_path, "w", encoding="utf-8") as f:
            async for record in stream:
                if stop_event and stop_event.is_set():
                    break

                now = time.time()
                if duration_seconds is not None and (now - self.start_time) >= duration_seconds:
                    break

                entry = {
                    "time_recorded_ms": int(now * 1000),
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "timestamp": record.timestamp,
                    "key": record.key,
                    "value": record.value,
                    "headers": record.headers,
                    "payload_format": record.payload_format,
                    "is_dlq": record.is_dlq,
                }
                f.write(json.dumps(entry, default=str) + "\n")
                f.flush()
                self.recorded_count += 1

                if max_messages is not None and self.recorded_count >= max_messages:
                    break

        self.end_time = time.time()
        return self.recorded_count
