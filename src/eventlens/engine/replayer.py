"""Traffic replay engine for reproducing recorded .lens sessions into target Kafka topics."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import AsyncGenerator

from eventlens.config import EventLensConfig
from eventlens.engine.producer import AsyncProducer
from eventlens.models import KafkaRecord


class TrafficReplayer:
    """Reads recorded .lens session files and replays them into target topics with rate scaling."""

    def __init__(
        self,
        session_path: str | Path,
        config: EventLensConfig | None = None,
    ) -> None:
        self.session_path = Path(session_path)
        if not self.session_path.exists():
            raise FileNotFoundError(f"Recorded session file not found: {self.session_path}")
        self.config = config or EventLensConfig()
        self.producer = AsyncProducer(config=self.config)

    def load_records(self) -> list[dict]:
        """Load all recorded records into memory."""
        records: list[dict] = []
        with open(self.session_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    async def stream_replay(
        self,
        target_topic: str | None = None,
        speed: float = 1.0,
        loop: bool = False,
        dry_run: bool = False,
        stop_event: asyncio.Event | None = None,
    ) -> AsyncGenerator[tuple[KafkaRecord, bool], None]:
        """Yield (record, is_replayed) and publish to Kafka if not dry_run."""
        records = self.load_records()
        if not records:
            return

        while True:
            prev_time: int | None = None

            for item in records:
                if stop_event and stop_event.is_set():
                    return

                rec_time = item.get("time_recorded_ms")
                if prev_time is not None and rec_time is not None and speed > 0:
                    delta_ms = max(0, rec_time - prev_time)
                    delay_sec = (delta_ms / 1000.0) / speed
                    # Cap maximum sleep to 5 seconds to prevent huge stalls
                    delay_sec = min(5.0, delay_sec)
                    if delay_sec > 0:
                        await asyncio.sleep(delay_sec)

                prev_time = rec_time

                dest_topic = target_topic or item["topic"]
                headers = item.get("headers") or {}
                headers["x-replayed-by"] = "eventlens"
                headers["x-replay-source-file"] = self.session_path.name

                raw_val = item.get("value") or ""
                key = item.get("key")

                reconstructed = KafkaRecord(
                    topic=dest_topic,
                    partition=item.get("partition", 0),
                    offset=item.get("offset", 0),
                    timestamp=item.get("timestamp"),
                    key=key,
                    value=raw_val,
                    headers=headers,
                    payload_format=item.get("payload_format", "raw"),
                    is_dlq=item.get("is_dlq", False),
                )

                if not dry_run:
                    await self.producer.send(
                        topic=dest_topic,
                        value=raw_val,
                        key=key,
                        headers=headers,
                    )

                yield reconstructed, not dry_run

            if not loop:
                break
