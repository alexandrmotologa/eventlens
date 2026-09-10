"""FastAPI server with WebSocket streaming, trace, diff, and export for EventLens Web UI."""

from __future__ import annotations

import asyncio
import io
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from eventlens.config import EventLensConfig
from eventlens.decoders.registry import DecoderRegistry
from eventlens.engine.consumer import AsyncConsumer
from eventlens.engine.diff import compare_payloads
from eventlens.engine.dlq_engine import DLQEngine
from eventlens.engine.lag_tracker import LagTracker
from eventlens.engine.producer import AsyncProducer
from eventlens.engine.simulator import MockStreamSimulator
from eventlens.engine.tracer import EventTracer
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


class ProduceRequest(BaseModel):
    topic: str
    key: str | None = None
    payload: Any
    headers: dict[str, str] = {}


class DiffRequest(BaseModel):
    payload_a: Any
    payload_b: Any


def create_app(
    topic: str = "order.events",
    broker: str = "localhost:9092",
    demo: bool = True,
) -> FastAPI:
    config = EventLensConfig(bootstrap_servers=broker)
    dlq_engine = DLQEngine(config=config)
    decoders = DecoderRegistry()
    producer = AsyncProducer(config=config)
    recent_records: list[dict[str, Any]] = []
    recent_dlq: list[dict[str, Any]] = []

    active_connections: list[WebSocket] = []

    async def broadcast_record(rec_dict: dict[str, Any]) -> None:
        recent_records.append(rec_dict)
        if len(recent_records) > 300:
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

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        task = asyncio.create_task(stream_worker())
        try:
            yield
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    app = FastAPI(title="EventLens Web Studio", version="0.1.0", lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

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

    @app.post("/api/produce")
    async def produce_message(req: ProduceRequest) -> dict[str, Any]:
        if demo:
            now_ms = int(asyncio.get_event_loop().time() * 1000)
            mock_rec = {
                "topic": req.topic,
                "partition": 0,
                "offset": 9999,
                "timestamp": now_ms,
                "key": req.key or "demo_key",
                "value": json.dumps(req.payload) if isinstance(req.payload, dict) else str(req.payload),
                "headers": req.headers,
                "payload_format": "json",
                "is_dlq": False,
                "payload": req.payload,
            }
            await broadcast_record(mock_rec)
            return {"success": True, "partition": 0, "offset": 9999, "demo": True}
        else:
            part, off = await producer.send(
                topic=req.topic,
                value=req.payload,
                key=req.key,
                headers=req.headers,
            )
            return {"success": True, "partition": part, "offset": off, "demo": False}

    @app.get("/api/trace")
    async def trace_lifecycle(correlation_id: str) -> dict[str, Any]:
        records = [KafkaRecord(**r) for r in recent_records]
        if demo and not any(EventTracer.record_matches_correlation(r, correlation_id) for r in records):
            # Generate sample trace for demo
            import time

            now_ms = int(time.time() * 1000)
            records = [
                KafkaRecord(
                    topic="order.events",
                    partition=0,
                    offset=1001,
                    timestamp=now_ms - 320,
                    key=correlation_id,
                    decoded_payload={"event_type": "OrderCreated", "order_id": correlation_id},
                    headers={"x-correlation-id": correlation_id},
                ),
                KafkaRecord(
                    topic="payment.events",
                    partition=1,
                    offset=405,
                    timestamp=now_ms - 150,
                    key=correlation_id,
                    decoded_payload={"event_type": "OrderPaid", "order_id": correlation_id, "amount": 149.99},
                    headers={"x-correlation-id": correlation_id},
                ),
                KafkaRecord(
                    topic="order.events.dlq",
                    partition=0,
                    offset=88,
                    timestamp=now_ms,
                    key=correlation_id,
                    decoded_payload={"order_id": correlation_id, "error": "InventoryAllocationException"},
                    headers={"x-correlation-id": correlation_id, "x-exception-message": "SKU out of stock"},
                    is_dlq=True,
                    error_message="SKU out of stock",
                ),
            ]

        graph = EventTracer.build_trace_graph(correlation_id=correlation_id, records=records)
        return graph.model_dump()

    @app.post("/api/diff")
    async def diff_payloads(req: DiffRequest) -> dict[str, Any]:
        diff_res = compare_payloads(req.payload_a, req.payload_b)
        return diff_res.model_dump()

    @app.get("/api/export")
    async def export_events(format: str = "json", dlq_only: bool = False) -> Response:
        source_data = recent_dlq if dlq_only else recent_records

        if format == "csv":
            import csv

            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(["Topic", "Partition", "Offset", "Key", "Format", "Timestamp", "Is_DLQ", "Payload"])
            for r in source_data:
                payload_str = (
                    json.dumps(r.get("payload", ""))
                    if isinstance(r.get("payload"), dict)
                    else str(r.get("payload", ""))
                )
                writer.writerow(
                    [
                        r.get("topic"),
                        r.get("partition"),
                        r.get("offset"),
                        r.get("key"),
                        r.get("format"),
                        r.get("timestamp"),
                        r.get("is_dlq"),
                        payload_str,
                    ]
                )
            return Response(
                content=output.getvalue(),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=eventlens_export.csv"},
            )

        elif format == "ndjson":
            ndjson_str = "\n".join(json.dumps(r, default=str) for r in source_data)
            return Response(
                content=ndjson_str,
                media_type="application/x-ndjson",
                headers={"Content-Disposition": "attachment; filename=eventlens_export.ndjson"},
            )

        else:
            json_str = json.dumps(source_data, indent=2, default=str)
            return Response(
                content=json_str,
                media_type="application/json",
                headers={"Content-Disposition": "attachment; filename=eventlens_export.json"},
            )

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
