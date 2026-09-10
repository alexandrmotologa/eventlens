"""FastAPI server with WebSocket streaming for EventLens Web UI."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from eventlens.config import EventLensConfig
from eventlens.decoders.registry import DecoderRegistry
from eventlens.engine.consumer import AsyncConsumer
from eventlens.engine.dlq_engine import DLQEngine
from eventlens.engine.lag_tracker import LagTracker
from eventlens.engine.simulator import MockStreamSimulator
from eventlens.models import KafkaRecord, RedriveResult

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


class RedriveRequest(BaseModel):
    source_topic: str
    target_topic: str
    offset: int
    partition: int = 0
    patched_payload: Any = None
    headers: dict[str, str] = {}


def create_app(
    topic: str = "order.events",
    broker: str = "localhost:9092",
    demo: bool = True,
) -> FastAPI:
    """Factory creating the FastAPI instance configured for topic and broker."""
    app = FastAPI(title="EventLens Web Studio", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    config = EventLensConfig(bootstrap_servers=broker)
    dlq_engine = DLQEngine(config=config)
    decoders = DecoderRegistry()
    recent_records: list[dict[str, Any]] = []
    recent_dlq: list[dict[str, Any]] = []

    active_connections: list[WebSocket] = []

    async def broadcast_record(rec_dict: dict[str, Any]) -> None:
        recent_records.append(rec_dict)
        if len(recent_records) > 200:
            recent_records.pop(0)

        if rec_dict.get("is_dlq"):
            recent_dlq.append(rec_dict)
            if len(recent_dlq) > 100:
                recent_dlq.pop(0)

        payload_str = json.dumps({"type": "record", "data": rec_dict})
        disconnected: list[WebSocket] = []
        for ws in active_connections:
            try:
                await ws.send_text(payload_str)
            except Exception:
                disconnected.append(ws)

        for ws in disconnected:
            if ws in active_connections:
                active_connections.remove(ws)

    # Background stream task
    async def stream_worker() -> None:
        if demo:
            sim = MockStreamSimulator(topic=topic)
            async for record in sim.stream_records(rate_per_second=2.0):
                await broadcast_record(record.to_summary_dict())
        else:
            consumer = AsyncConsumer(
                topic=topic,
                config=config,
                decoder_registry=decoders,
                from_beginning=False,
            )
            try:
                async for record in consumer.stream_records():
                    await broadcast_record(record.to_summary_dict())
            except Exception as err:
                logger.error(f"Kafka consumer stream error: {err}")

    @app.on_event("startup")
    async def startup_event() -> None:
        asyncio.create_task(stream_worker())

    @app.get("/api/status")
    async def get_status() -> dict[str, Any]:
        return {
            "topic": topic,
            "broker": broker,
            "demo": demo,
            "connected_clients": len(active_connections),
            "cached_events": len(recent_records),
            "cached_dlq": len(recent_dlq),
        }

    @app.get("/api/lag")
    async def get_lag(group: str | None = None) -> dict[str, Any]:
        if demo:
            sim = MockStreamSimulator()
            lag_data = sim.generate_mock_lag(topic=topic, group_id=group or "demo-web-group")
            return lag_data.model_dump()
        else:
            tracker = LagTracker(config=config)
            try:
                lag_data = await tracker.get_topic_lag(topic=topic, group_id=group)
                return lag_data.model_dump()
            except Exception as err:
                raise HTTPException(status_code=500, detail=str(err))

    @app.get("/api/dlq")
    async def get_dlq_records() -> list[dict[str, Any]]:
        # Return diagnosed DLQ records
        result: list[dict[str, Any]] = []
        for r in recent_dlq:
            rec = KafkaRecord(**r)
            diag = DLQEngine.diagnose_record(rec)
            item = r.copy()
            item["diagnostic"] = diag.model_dump()
            result.append(item)
        return result

    @app.post("/api/dlq/redrive")
    async def redrive_message(req: RedriveRequest) -> dict[str, Any]:
        rec = KafkaRecord(
            topic=req.source_topic,
            partition=req.partition,
            offset=req.offset,
            value=json.dumps(req.patched_payload) if req.patched_payload else "{}",
            decoded_payload=req.patched_payload,
            headers=req.headers,
        )
        res: RedriveResult = await dlq_engine.redrive(
            record=rec,
            target_topic=req.target_topic,
            patched_payload=req.patched_payload,
        )
        return res.model_dump()

    @app.websocket("/ws/events")
    async def websocket_events(websocket: WebSocket) -> None:
        await websocket.accept()
        active_connections.append(websocket)
        try:
            # Send initial backlog
            for r in recent_records[-50:]:
                await websocket.send_text(json.dumps({"type": "record", "data": r}))

            while True:
                await websocket.receive_text()
                # Handle client ping / filter command if needed
                pass
        except WebSocketDisconnect:
            if websocket in active_connections:
                active_connections.remove(websocket)
        except Exception:
            if websocket in active_connections:
                active_connections.remove(websocket)

    # Static assets
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

        @app.get("/")
        async def serve_index() -> FileResponse:
            return FileResponse(STATIC_DIR / "index.html")

    return app
