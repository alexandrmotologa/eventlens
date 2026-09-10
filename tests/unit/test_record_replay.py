"""Unit tests for traffic recording (.lens session files) and replay engine."""

from __future__ import annotations

from pathlib import Path
from typing import AsyncGenerator

import pytest

from eventlens.engine.recorder import TrafficRecorder
from eventlens.engine.replayer import TrafficReplayer
from eventlens.models import KafkaRecord


async def sample_stream() -> AsyncGenerator[KafkaRecord, None]:
    for i in range(5):
        yield KafkaRecord(
            topic="test.topic",
            partition=0,
            offset=i,
            timestamp=1700000000000 + (i * 100),
            key=f"key_{i}",
            value=f'{{"count": {i}}}',
            headers={"x-source": "unit-test"},
            payload_format="json",
        )


@pytest.mark.asyncio
async def test_recorder_writes_session_file(tmp_path: Path) -> None:
    session_file = tmp_path / "traffic_session.lens"
    recorder = TrafficRecorder(output_path=session_file)

    count = await recorder.record_stream(
        stream=sample_stream(),
        max_messages=3,
    )

    assert count == 3
    assert session_file.exists()

    lines = session_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 3
    assert "test.topic" in lines[0]
    assert "unit-test" in lines[0]


@pytest.mark.asyncio
async def test_replayer_dry_run(tmp_path: Path) -> None:
    session_file = tmp_path / "traffic_session.lens"
    recorder = TrafficRecorder(output_path=session_file)
    await recorder.record_stream(stream=sample_stream(), max_messages=4)

    replayer = TrafficReplayer(session_path=session_file)
    replayed_records = []

    # High speed to run tests instantaneously
    async for rec, published in replayer.stream_replay(speed=1000.0, dry_run=True):
        replayed_records.append((rec, published))

    assert len(replayed_records) == 4
    for rec, published in replayed_records:
        assert published is False
        assert rec.headers.get("x-replayed-by") == "eventlens"
        assert rec.headers.get("x-replay-source-file") == session_file.name


@pytest.mark.asyncio
async def test_replayer_target_topic_override(tmp_path: Path) -> None:
    session_file = tmp_path / "traffic_session.lens"
    recorder = TrafficRecorder(output_path=session_file)
    await recorder.record_stream(stream=sample_stream(), max_messages=2)

    replayer = TrafficReplayer(session_path=session_file)
    async for rec, _ in replayer.stream_replay(target_topic="shadow.orders", speed=500.0, dry_run=True):
        assert rec.topic == "shadow.orders"
