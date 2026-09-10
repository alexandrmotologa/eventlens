"""Offline mock stream simulator for demonstration and automated testing."""

from __future__ import annotations

import asyncio
import json
import random
import time
from typing import AsyncGenerator

from eventlens.models import ConsumerGroupLag, KafkaRecord, PartitionLag

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD"]
CUSTOMERS = ["cust_9011", "cust_4822", "cust_1109", "cust_3320", "cust_7718"]
CITIES = ["New York", "Berlin", "London", "Tokyo", "Bucharest", "San Francisco"]

SAMPLE_POISON_PILLS = [
    {
        "payload": '{"order_id": "ord_err_99", "currency": "USD", "total_amount": 149.99, "items": [{"id": "item_1", "qty": -5}]}',
        "key": "ord_err_99",
        "headers": {
            "x-exception-message": "ValidationException: Quantity must be greater than zero, found -5",
            "x-exception-stacktrace": "com.engine.order.domain.OrderValidator.validateItems(OrderValidator.java:72)\n\tat com.engine.order.listener.OrderListener.onMessage(OrderListener.java:38)",
            "x-original-topic": "order.events",
            "x-original-partition": "0",
            "x-original-offset": "4102",
        },
    },
    {
        "payload": '{"order_id": "ord_bad_json", "customer_id": "cust_4822", "currency": "EUR", "total_amount": 89.00, corrupted_field: }',
        "key": "ord_bad_json",
        "headers": {
            "x-exception-message": "JsonParseException: Unexpected character 'c' at offset 94",
            "x-exception-stacktrace": "com.fasterxml.jackson.core.JsonParseException: Unexpected character 'c'\n\tat com.engine.order.deserializer.JacksonDecoder.decode(JacksonDecoder.java:54)",
            "x-original-topic": "order.events",
            "x-original-partition": "1",
            "x-original-offset": "8931",
        },
    },
    {
        "payload": '{"order_id": "ord_timeout_12", "customer_id": "cust_1109", "currency": "USD", "total_amount": 540.00}',
        "key": "ord_timeout_12",
        "headers": {
            "x-exception-message": "TimeoutException: Payment gateway payment-gateway-east.internal timed out after 5000ms",
            "x-exception-stacktrace": "java.util.concurrent.TimeoutException: HTTP POST request timed out\n\tat com.engine.payment.client.GatewayClient.execute(GatewayClient.java:112)",
            "x-original-topic": "order.events",
            "x-original-partition": "2",
            "x-original-offset": "12055",
        },
    },
]


class MockStreamSimulator:
    """Generates synthetic Kafka records for live tailing, filtering, and DLQ demos."""

    def __init__(self, topic: str = "order.events", failure_rate: float = 0.15) -> None:
        self.topic = topic
        self.failure_rate = failure_rate
        self._current_offset = 1000
        self._sequence = 1

    def generate_record(self, force_dlq: bool = False) -> KafkaRecord:
        """Generate a single realistic event record."""
        is_dlq = force_dlq or (random.random() < self.failure_rate)
        now_ms = int(time.time() * 1000)
        self._current_offset += 1
        self._sequence += 1
        partition = random.choice([0, 1, 2, 3])

        if is_dlq:
            pill = random.choice(SAMPLE_POISON_PILLS)
            key = pill["key"]
            raw_text = pill["payload"]
            headers = dict(pill["headers"])

            # Try parsing if valid JSON, else keep as text
            try:
                decoded_data = json.loads(raw_text)
                fmt = "json"
            except Exception:
                decoded_data = raw_text
                fmt = "raw"

            return KafkaRecord(
                topic=f"{self.topic}.dlq" if not self.topic.endswith(".dlq") else self.topic,
                partition=partition,
                offset=self._current_offset,
                timestamp=now_ms,
                key=key,
                value=raw_text,
                raw_key=key.encode("utf-8"),
                raw_value=raw_text.encode("utf-8"),
                headers=headers,
                decoded_key=key,
                decoded_payload=decoded_data,
                payload_format=fmt,
                is_dlq=True,
                error_message=headers.get("x-exception-message"),
            )

        # Normal business event
        order_id = f"ord_{random.randint(1000, 9999)}"
        event_types = ["OrderCreated", "OrderValidated", "OrderPaid", "OrderCompleted"]
        weights = [0.45, 0.25, 0.20, 0.10]
        event_type = random.choices(event_types, weights=weights)[0]

        data = {
            "event_id": f"evt_{random.randint(100000, 999999)}",
            "order_id": order_id,
            "event_type": event_type,
            "customer_id": random.choice(CUSTOMERS),
            "currency": random.choice(CURRENCIES),
            "total_amount": f"{round(random.uniform(15.0, 850.0), 2):.2f}",
            "destination_city": random.choice(CITIES),
            "sequence_number": self._sequence,
            "timestamp": now_ms,
            "status": "PROCESSED" if event_type != "OrderCancelled" else "CANCELLED",
        }

        payload_json = json.dumps(data)
        return KafkaRecord(
            topic=self.topic,
            partition=partition,
            offset=self._current_offset,
            timestamp=now_ms,
            key=order_id,
            value=payload_json,
            raw_key=order_id.encode("utf-8"),
            raw_value=payload_json.encode("utf-8"),
            headers={"content-type": "application/json", "producer": "mock-simulator"},
            decoded_key=order_id,
            decoded_payload=data,
            payload_format="json",
            is_dlq=False,
        )

    async def stream_records(
        self,
        rate_per_second: float = 2.0,
        max_messages: int | None = None,
        stop_event: asyncio.Event | None = None,
    ) -> AsyncGenerator[KafkaRecord, None]:
        """Yield simulated records continuously with rate limiting."""
        delay = 1.0 / max(0.1, rate_per_second)
        count = 0

        while True:
            if stop_event and stop_event.is_set():
                break

            record = self.generate_record()
            yield record
            count += 1

            if max_messages is not None and count >= max_messages:
                break

            await asyncio.sleep(delay)

    def generate_mock_lag(self, topic: str, group_id: str = "order-processor-group") -> ConsumerGroupLag:
        """Generate dynamic mock lag metrics for 4 partitions."""
        partitions: list[PartitionLag] = []
        total_lag = 0

        for p in range(4):
            high = 15000 + random.randint(100, 500)
            lag = random.choice([0, 2, 14, 45, 128, 5]) if p != 2 else random.randint(80, 240)
            current = high - lag
            total_lag += lag
            partitions.append(
                PartitionLag(
                    topic=topic,
                    partition=p,
                    log_end_offset=high,
                    current_offset=current,
                    lag=lag,
                )
            )

        return ConsumerGroupLag(
            group_id=group_id,
            topic=topic,
            partitions=partitions,
            total_lag=total_lag,
        )
