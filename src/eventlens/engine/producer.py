"""Asynchronous Kafka producer for publishing and redriving messages."""

from __future__ import annotations

import json
import logging
from typing import Any

from aiokafka import AIOKafkaProducer

from eventlens.config import EventLensConfig
from eventlens.models import KafkaRecord, RedriveResult

logger = logging.getLogger(__name__)

DLQ_ERROR_HEADERS = {
    "x-exception-message",
    "x-exception-stacktrace",
    "x-original-topic",
    "x-original-partition",
    "x-original-offset",
    "deadletter.reason",
    "x-death",
}


class AsyncProducer:
    """Publishes messages to Kafka topics."""

    def __init__(self, config: EventLensConfig | None = None) -> None:
        self.config = config or EventLensConfig()
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        """Start the underlying aiokafka producer."""
        kwargs = self.config.to_aiokafka_producer_kwargs()
        self._producer = AIOKafkaProducer(**kwargs)
        await self._producer.start()

    async def stop(self) -> None:
        """Stop the producer."""
        if self._producer:
            try:
                await self._producer.stop()
            except Exception as err:
                logger.warning(f"Error closing Kafka producer: {err}")
            finally:
                self._producer = None

    async def send(
        self,
        topic: str,
        value: Any,
        key: str | bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, int]:
        """Send message and return (partition, offset)."""
        if not self._producer:
            await self.start()

        assert self._producer is not None

        # Prepare payload bytes
        if isinstance(value, bytes):
            val_bytes = value
        elif isinstance(value, (dict, list)):
            val_bytes = json.dumps(value).encode("utf-8")
        elif isinstance(value, str):
            val_bytes = value.encode("utf-8")
        else:
            val_bytes = str(value).encode("utf-8")

        # Prepare key bytes
        key_bytes: bytes | None = None
        if isinstance(key, bytes):
            key_bytes = key
        elif isinstance(key, str):
            key_bytes = key.encode("utf-8")

        # Prepare headers
        raw_headers: list[tuple[str, bytes]] = []
        if headers:
            for k, v in headers.items():
                raw_headers.append((k, str(v).encode("utf-8")))

        record_meta = await self._producer.send_and_wait(
            topic=topic,
            value=val_bytes,
            key=key_bytes,
            headers=raw_headers if raw_headers else None,
        )

        return record_meta.partition, record_meta.offset

    async def redrive_record(
        self,
        record: KafkaRecord,
        target_topic: str,
        patched_payload: Any | None = None,
        strip_dlq_headers: bool = True,
    ) -> RedriveResult:
        """Republish a dead-lettered message to a target topic."""
        try:
            # Determine payload to send
            val_to_send = (
                patched_payload
                if patched_payload is not None
                else (record.raw_value or record.decoded_payload or record.value)
            )

            # Filter headers
            headers_to_send: dict[str, str] = {}
            for k, v in record.headers.items():
                if strip_dlq_headers and k.lower() in DLQ_ERROR_HEADERS:
                    continue
                headers_to_send[k] = v

            # Add provenance header
            headers_to_send["x-redriven-by"] = "eventlens"
            headers_to_send["x-redrive-source-topic"] = record.topic
            headers_to_send["x-redrive-source-offset"] = str(record.offset)

            part, off = await self.send(
                topic=target_topic,
                value=val_to_send,
                key=record.raw_key or record.key,
                headers=headers_to_send,
            )

            return RedriveResult(
                success=True,
                source_topic=record.topic,
                target_topic=target_topic,
                source_offset=record.offset,
                produced_partition=part,
                produced_offset=off,
            )
        except Exception as err:
            logger.error(f"Redrive failed: {err}")
            return RedriveResult(
                success=False,
                source_topic=record.topic,
                target_topic=target_topic,
                source_offset=record.offset,
                error=str(err),
            )
