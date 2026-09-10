"""Kafka consumer group and partition lag tracker."""

from __future__ import annotations

import logging

from aiokafka import AIOKafkaConsumer, TopicPartition

from eventlens.config import EventLensConfig
from eventlens.models import ConsumerGroupLag, PartitionLag

logger = logging.getLogger(__name__)


class LagTracker:
    """Calculates partition watermarks, consumer group committed offsets, and lag."""

    def __init__(self, config: EventLensConfig | None = None) -> None:
        self.config = config or EventLensConfig()

    async def get_topic_partitions(self, topic: str) -> list[int]:
        """Fetch partition IDs for a topic."""
        consumer = AIOKafkaConsumer(**self.config.to_aiokafka_consumer_kwargs())
        await consumer.start()
        try:
            partitions = await consumer.partitions_for_topic(topic)
            return sorted(list(partitions)) if partitions else []
        finally:
            await consumer.stop()

    async def get_topic_lag(self, topic: str, group_id: str | None = None) -> ConsumerGroupLag:
        """Calculate lag for a consumer group across topic partitions."""
        kwargs = self.config.to_aiokafka_consumer_kwargs()
        if group_id:
            kwargs["group_id"] = group_id

        consumer = AIOKafkaConsumer(**kwargs)
        await consumer.start()
        try:
            partition_ids = await consumer.partitions_for_topic(topic)
            if not partition_ids:
                return ConsumerGroupLag(
                    group_id=group_id or "unassigned",
                    topic=topic,
                    partitions=[],
                    total_lag=0,
                )

            tps = [TopicPartition(topic, p) for p in sorted(partition_ids)]

            # Fetch log end offsets (high watermarks)
            end_offsets = await consumer.end_offsets(tps)

            # Fetch committed offsets for group if group provided
            committed_map: dict[TopicPartition, int] = {}
            if group_id:
                for tp in tps:
                    comm = await consumer.committed(tp)
                    committed_map[tp] = comm if comm is not None else 0

            partition_lags: list[PartitionLag] = []
            total_lag = 0

            for tp in tps:
                high_watermark = end_offsets.get(tp, 0)
                current = committed_map.get(tp, high_watermark)
                lag = max(0, high_watermark - current)
                total_lag += lag

                partition_lags.append(
                    PartitionLag(
                        topic=topic,
                        partition=tp.partition,
                        log_end_offset=high_watermark,
                        current_offset=current,
                        lag=lag,
                    )
                )

            return ConsumerGroupLag(
                group_id=group_id or "none",
                topic=topic,
                partitions=partition_lags,
                total_lag=total_lag,
            )
        finally:
            await consumer.stop()
