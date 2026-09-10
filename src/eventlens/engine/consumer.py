"""Asynchronous Kafka consumer wrapper using aiokafka."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncGenerator

from aiokafka import AIOKafkaConsumer, TopicPartition

from eventlens.config import EventLensConfig
from eventlens.decoders.registry import DecoderRegistry
from eventlens.models import KafkaRecord

logger = logging.getLogger(__name__)


class AsyncConsumer:
    """Consumes Kafka messages asynchronously and decodes payloads."""

    def __init__(
        self,
        topic: str,
        config: EventLensConfig | None = None,
        decoder_registry: DecoderRegistry | None = None,
        partition: int | None = None,
        from_beginning: bool = False,
        tail_n: int | None = None,
    ) -> None:
        self.topic = topic
        self.config = config or EventLensConfig()
        self.decoder_registry = decoder_registry or DecoderRegistry()
        self.partition = partition
        self.from_beginning = from_beginning
        self.tail_n = tail_n

        self._consumer: AIOKafkaConsumer | None = None
        self._is_running = False

    async def start(self) -> None:
        """Initialize and start the underlying aiokafka consumer."""
        kwargs = self.config.to_aiokafka_consumer_kwargs()

        if self.from_beginning:
            kwargs["auto_offset_reset"] = "earliest"
        else:
            kwargs["auto_offset_reset"] = "latest"

        self._consumer = AIOKafkaConsumer(**kwargs)
        await self._consumer.start()
        self._is_running = True

        # Assign or subscribe
        if self.partition is not None:
            tp = TopicPartition(self.topic, self.partition)
            self._consumer.assign([tp])
            if self.from_beginning:
                await self._consumer.seek_to_beginning(tp)
            elif self.tail_n is not None:
                await self._seek_tail_n([tp], self.tail_n)
        else:
            self._consumer.subscribe([self.topic])
            # Wait for partitions assignment if tailing N
            if self.tail_n is not None:
                # Poll briefly to trigger partition assignment
                await asyncio.sleep(0.5)
                assigned = self._consumer.assignment()
                if assigned:
                    await self._seek_tail_n(list(assigned), self.tail_n)

    async def _seek_tail_n(self, topic_partitions: list[TopicPartition], n: int) -> None:
        """Seek consumer to last N messages before end offset."""
        if not self._consumer:
            return

        end_offsets = await self._consumer.end_offsets(topic_partitions)
        beginning_offsets = await self._consumer.beginning_offsets(topic_partitions)

        for tp in topic_partitions:
            end_off = end_offsets.get(tp, 0)
            beg_off = beginning_offsets.get(tp, 0)
            target = max(beg_off, end_off - n)
            self._consumer.seek(tp, target)

    async def stop(self) -> None:
        """Gracefully stop consumer."""
        self._is_running = False
        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception as err:
                logger.warning(f"Error closing Kafka consumer: {err}")
            finally:
                self._consumer = None

    async def stream_records(
        self,
        max_messages: int | None = None,
        stop_event: asyncio.Event | None = None,
    ) -> AsyncGenerator[KafkaRecord, None]:
        """Yield decoded Kafka records continuously."""
        if not self._consumer:
            await self.start()

        assert self._consumer is not None
        count = 0

        try:
            while self._is_running:
                if stop_event and stop_event.is_set():
                    break

                try:
                    # Poll records with a short timeout to check cancellation
                    batch = await self._consumer.getmany(
                        timeout_ms=self.config.default_poll_timeout_ms,
                        max_records=100,
                    )
                except asyncio.CancelledError:
                    break
                except Exception as err:
                    logger.error(f"Error polling Kafka messages: {err}")
                    await asyncio.sleep(1.0)
                    continue

                for tp, messages in batch.items():
                    for msg in messages:
                        record = self._convert_record(msg)
                        yield record
                        count += 1
                        if max_messages is not None and count >= max_messages:
                            return

        finally:
            await self.stop()

    def _convert_record(self, raw_msg: Any) -> KafkaRecord:
        """Transform raw aiokafka record to normalized KafkaRecord with decoded payload."""
        # Parse headers
        headers_dict: dict[str, str] = {}
        if raw_msg.headers:
            for k, v in raw_msg.headers:
                try:
                    headers_dict[k] = v.decode("utf-8") if v else ""
                except Exception:
                    headers_dict[k] = str(v)

        # Parse key
        key_str: str | None = None
        if raw_msg.key:
            try:
                key_str = raw_msg.key.decode("utf-8")
            except UnicodeDecodeError:
                key_str = f"0x{raw_msg.key.hex()}"

        # Raw value string preview
        val_str: str | None = None
        if raw_msg.value:
            try:
                val_str = raw_msg.value.decode("utf-8")
            except UnicodeDecodeError:
                val_str = f"0x{raw_msg.value.hex()}"

        # Decode payload using decoder registry
        decoded = self.decoder_registry.decode(raw_msg.value, headers_dict)

        # Check DLQ characteristics
        is_dlq = (
            self.topic.lower().endswith(".dlq")
            or "dlq" in self.topic.lower()
            or "x-exception-message" in headers_dict
            or "x-original-topic" in headers_dict
            or "deadletter.reason" in headers_dict
        )

        error_msg = headers_dict.get("x-exception-message") or headers_dict.get("deadletter.reason") or decoded.error

        return KafkaRecord(
            topic=raw_msg.topic,
            partition=raw_msg.partition,
            offset=raw_msg.offset,
            timestamp=raw_msg.timestamp,
            key=key_str,
            value=val_str,
            raw_key=raw_msg.key,
            raw_value=raw_msg.value,
            headers=headers_dict,
            decoded_key=key_str,
            decoded_payload=decoded.data,
            payload_format=decoded.format,
            is_dlq=is_dlq,
            error_message=error_msg,
        )
