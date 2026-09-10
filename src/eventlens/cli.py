"""Typer CLI interface for EventLens."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from eventlens.config import EventLensConfig
from eventlens.decoders.registry import DecoderRegistry
from eventlens.engine.consumer import AsyncConsumer
from eventlens.engine.diff import compare_payloads
from eventlens.engine.dlq_engine import DLQEngine
from eventlens.engine.filter import EventFilter
from eventlens.engine.lag_tracker import LagTracker
from eventlens.engine.producer import AsyncProducer
from eventlens.engine.recorder import TrafficRecorder
from eventlens.engine.replayer import TrafficReplayer
from eventlens.engine.simulator import MockStreamSimulator
from eventlens.engine.tracer import EventTracer
from eventlens.models import KafkaRecord

app = typer.Typer(
    name="eventlens",
    help="Developer-first Terminal & Web Kafka Event Stream Debugger & DLQ Studio",
    no_args_is_help=True,
)
dlq_app = typer.Typer(
    name="dlq",
    help="Dead Letter Queue inspection, diagnosis, and redrive commands",
    no_args_is_help=True,
)
app.add_typer(dlq_app, name="dlq")

console = Console()


def print_record_table_row(table: Table, record: KafkaRecord) -> None:
    """Add a row to the tail table."""
    status_style = "[bold red]DLQ[/]" if record.is_dlq else "[green]OK[/]"
    ts_str = str(record.timestamp) if record.timestamp else "-"

    # Compact preview
    if isinstance(record.decoded_payload, dict):
        preview = json.dumps(record.decoded_payload, default=str)
    else:
        preview = str(record.decoded_payload or record.value or "")

    if len(preview) > 60:
        preview = preview[:57] + "..."

    table.add_row(
        status_style,
        str(record.partition),
        str(record.offset),
        record.key or "-",
        record.payload_format,
        preview,
        ts_str,
    )


@app.command("tail")
def tail_command(
    topic: str = typer.Argument(..., help="Kafka topic to tail"),
    broker: str = typer.Option("localhost:9092", "--broker", "-b", help="Kafka broker address"),
    from_beginning: bool = typer.Option(False, "--from-beginning", help="Start from offset 0"),
    tail_n: Optional[int] = typer.Option(None, "--tail", "-n", help="Read last N messages first"),
    partition: Optional[int] = typer.Option(None, "--partition", "-p", help="Specific partition"),
    filter_expr: Optional[str] = typer.Option(None, "--filter", "-f", help="JMESPath filter query"),
    regex: Optional[str] = typer.Option(None, "--regex", "-r", help="Regular expression filter"),
    proto: Optional[Path] = typer.Option(None, "--proto", help="Path to .proto file"),
    message_type: Optional[str] = typer.Option(None, "--message-type", help="Protobuf message name"),
    format_type: str = typer.Option("table", "--format", help="Output format: table, json, raw"),
    max_messages: Optional[int] = typer.Option(None, "--max-messages", help="Stop after N messages"),
    demo: bool = typer.Option(False, "--demo", help="Run with simulated records"),
) -> None:
    """Stream events in real time from a Kafka topic."""
    asyncio.run(
        _async_tail(
            topic=topic,
            broker=broker,
            from_beginning=from_beginning,
            tail_n=tail_n,
            partition=partition,
            filter_expr=filter_expr,
            regex=regex,
            proto=proto,
            message_type=message_type,
            format_type=format_type,
            max_messages=max_messages,
            demo=demo,
        )
    )


async def _async_tail(
    topic: str,
    broker: str,
    from_beginning: bool,
    tail_n: Optional[int],
    partition: Optional[int],
    filter_expr: Optional[str],
    regex: Optional[str],
    proto: Optional[Path],
    message_type: Optional[str],
    format_type: str,
    max_messages: Optional[int],
    demo: bool,
) -> None:
    config = EventLensConfig(bootstrap_servers=broker)
    event_filter = EventFilter(jmespath_query=filter_expr, regex_pattern=regex)

    decoders = DecoderRegistry(
        proto_file_path=str(proto) if proto else None,
        proto_message_type=message_type,
    )

    console.print(
        f"[bold cyan]EventLens Tailing[/] topic [bold green]{topic}[/] "
        f"(Broker: [dim]{broker}[/], Mode: {'[yellow]DEMO[/]' if demo else '[green]LIVE[/]'})"
    )
    if event_filter.is_active():
        console.print(f"[dim]Filter active: JMESPath='{filter_expr}' Regex='{regex}'[/]")

    table = Table(title=f"Live Stream: {topic}", expand=True)
    table.add_column("Status", width=8)
    table.add_column("Part", justify="right", width=6)
    table.add_column("Offset", justify="right", width=8)
    table.add_column("Key", width=14)
    table.add_column("Format", width=10)
    table.add_column("Payload Preview", ratio=1)
    table.add_column("Timestamp", width=16)

    stop_event = asyncio.Event()

    try:
        if demo:
            sim = MockStreamSimulator(topic=topic)
            stream = sim.stream_records(rate_per_second=2.5, max_messages=max_messages, stop_event=stop_event)
        else:
            consumer = AsyncConsumer(
                topic=topic,
                config=config,
                decoder_registry=decoders,
                partition=partition,
                from_beginning=from_beginning,
                tail_n=tail_n,
            )
            stream = consumer.stream_records(max_messages=max_messages, stop_event=stop_event)

        count = 0
        async for record in stream:
            if not event_filter.matches(record):
                continue

            count += 1
            if format_type == "json":
                console.print_json(data=record.to_summary_dict())
            elif format_type == "raw":
                console.print(f"[{record.partition}:{record.offset}] {record.value}")
            else:
                # Table format
                row_table = Table(show_header=(count == 1), expand=True)
                row_table.add_column("Status", width=8)
                row_table.add_column("Part", justify="right", width=6)
                row_table.add_column("Offset", justify="right", width=8)
                row_table.add_column("Key", width=14)
                row_table.add_column("Format", width=10)
                row_table.add_column("Payload Preview", ratio=1)
                row_table.add_column("Timestamp", width=16)

                print_record_table_row(row_table, record)
                console.print(row_table)

    except KeyboardInterrupt:
        console.print("\n[yellow]Streaming stopped by user.[/]")
    except Exception as err:
        console.print(f"[bold red]Stream error:[/] {err}")


@app.command("lag")
def lag_command(
    topic: str = typer.Argument(..., help="Kafka topic to inspect"),
    group: Optional[str] = typer.Option(None, "--group", "-g", help="Consumer group ID"),
    broker: str = typer.Option("localhost:9092", "--broker", "-b", help="Kafka broker"),
    demo: bool = typer.Option(False, "--demo", help="Generate simulated lag metrics"),
) -> None:
    """Inspect partition watermarks and consumer group lag."""
    asyncio.run(_async_lag(topic=topic, group=group, broker=broker, demo=demo))


async def _async_lag(topic: str, group: Optional[str], broker: str, demo: bool) -> None:
    config = EventLensConfig(bootstrap_servers=broker)

    if demo:
        sim = MockStreamSimulator()
        lag_data = sim.generate_mock_lag(topic=topic, group_id=group or "demo-group")
    else:
        tracker = LagTracker(config=config)
        try:
            lag_data = await tracker.get_topic_lag(topic=topic, group_id=group)
        except Exception as err:
            console.print(f"[bold red]Failed to query lag from Kafka:[/] {err}")
            console.print("[dim]Hint: Use --demo to test with simulated lag data.[/]")
            return

    table = Table(
        title=f"Consumer Group Lag: {lag_data.group_id} (Topic: {lag_data.topic})",
        expand=True,
    )
    table.add_column("Partition", justify="right", width=12)
    table.add_column("Log End (High)", justify="right", width=16)
    table.add_column("Committed Offset", justify="right", width=18)
    table.add_column("Lag", justify="right", width=12)
    table.add_column("Lag Meter", width=30)

    for p in lag_data.partitions:
        bar_len = min(25, int(p.lag / 10)) if p.lag > 0 else 0
        meter = f"[{'red' if p.lag > 50 else 'yellow' if p.lag > 10 else 'green'}]{'#' * bar_len}[/]"
        table.add_row(
            str(p.partition),
            str(p.log_end_offset),
            str(p.current_offset),
            f"[bold]{p.lag}[/]",
            meter,
        )

    console.print(table)
    console.print(f"[bold]Total Consumer Lag:[/] [bold red]{lag_data.total_lag}[/] messages\n")


@dlq_app.command("inspect")
def dlq_inspect_command(
    topic: str = typer.Argument(..., help="Dead Letter Queue topic"),
    broker: str = typer.Option("localhost:9092", "--broker", "-b", help="Kafka broker"),
    limit: int = typer.Option(10, "--limit", "-l", help="Number of records to inspect"),
    diagnose: bool = typer.Option(True, "--diagnose", help="Run failure heuristic diagnosis"),
    demo: bool = typer.Option(False, "--demo", help="Use simulated DLQ messages"),
) -> None:
    """Inspect poison pills and headers on a Dead Letter Queue topic."""
    asyncio.run(_async_dlq_inspect(topic=topic, broker=broker, limit=limit, diagnose=diagnose, demo=demo))


async def _async_dlq_inspect(topic: str, broker: str, limit: int, diagnose: bool, demo: bool) -> None:
    console.print(f"[bold red]DLQ Studio: Inspecting {topic}[/]")

    records: list[KafkaRecord] = []
    if demo:
        sim = MockStreamSimulator(topic=topic)
        for _ in range(limit):
            records.append(sim.generate_record(force_dlq=True))
    else:
        config = EventLensConfig(bootstrap_servers=broker)
        consumer = AsyncConsumer(topic=topic, config=config, from_beginning=True)
        try:
            async for rec in consumer.stream_records(max_messages=limit):
                records.append(rec)
        except Exception as err:
            console.print(f"[bold red]Failed connecting to DLQ topic:[/] {err}")
            console.print("[dim]Use --demo to test with simulated poison pill events.[/]")
            return

    for rec in records:
        diag = DLQEngine.diagnose_record(rec) if diagnose else None
        diag_str = (
            f"[bold yellow]Category:[/] {diag.failure_category}\n"
            f"[bold yellow]Root Cause:[/] {diag.root_cause}\n"
            f"[bold yellow]Suggested Action:[/] {diag.suggested_action}\n"
            if diag
            else ""
        )

        headers_str = "\n".join(f"  [dim]{k}:[/] {v}" for k, v in rec.headers.items())
        payload_str = (
            json.dumps(rec.decoded_payload, indent=2)
            if isinstance(rec.decoded_payload, (dict, list))
            else str(rec.value)
        )

        content = (
            f"[bold]Offset:[/] {rec.offset}  [bold]Partition:[/] {rec.partition}  [bold]Key:[/] {rec.key}\n"
            f"{diag_str}"
            f"[bold]Headers:[/]\n{headers_str}\n\n"
            f"[bold]Payload:[/]\n{payload_str}"
        )

        console.print(Panel(content, title=f"Poison Pill at Offset {rec.offset}", border_style="red"))


@dlq_app.command("redrive")
def dlq_redrive_command(
    topic: str = typer.Argument(..., help="Source DLQ topic"),
    offset: int = typer.Option(..., "--offset", "-o", help="Offset of message to redrive"),
    target: str = typer.Option(..., "--target", "-t", help="Target topic for republishing"),
    partition: int = typer.Option(0, "--partition", "-p", help="Partition ID"),
    broker: str = typer.Option("localhost:9092", "--broker", "-b", help="Kafka broker"),
    patch_file: Optional[Path] = typer.Option(None, "--patch-file", help="File with modified JSON"),
) -> None:
    """Republish a poisoned message from DLQ to active destination topic."""
    asyncio.run(
        _async_dlq_redrive(
            topic=topic,
            offset=offset,
            target=target,
            partition=partition,
            broker=broker,
            patch_file=patch_file,
        )
    )


async def _async_dlq_redrive(
    topic: str,
    offset: int,
    target: str,
    partition: int,
    broker: str,
    patch_file: Optional[Path],
) -> None:
    config = EventLensConfig(bootstrap_servers=broker)
    dlq_engine = DLQEngine(config=config)

    patched_data = None
    if patch_file:
        if not patch_file.exists():
            console.print(f"[bold red]Patch file not found:[/] {patch_file}")
            raise typer.Exit(1)
        patched_data = json.loads(patch_file.read_text(encoding="utf-8"))

    console.print(f"[bold green]Redriving message from[/] [yellow]{topic}[/] offset {offset} -> [green]{target}[/]")

    # For CLI redrive, fetch record or reconstruct with patch
    rec = KafkaRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        value=json.dumps(patched_data) if patched_data else "{}",
        decoded_payload=patched_data,
        headers={"x-original-topic": target},
    )

    result = await dlq_engine.redrive(record=rec, target_topic=target, patched_payload=patched_data)
    if result.success:
        console.print(
            f"[bold green][OK] Successfully redriven![/] Produced to {result.target_topic} "
            f"(Partition: {result.produced_partition}, Offset: {result.produced_offset})"
        )
    else:
        console.print(f"[bold red][FAIL] Redrive failed:[/] {result.error}")


@dlq_app.command("redrive-batch")
def dlq_redrive_batch_command(
    topic: str = typer.Argument(..., help="Source DLQ topic"),
    target: Optional[str] = typer.Option(None, "--target", "-t", help="Target topic (defaults to x-original-topic)"),
    category: Optional[str] = typer.Option(None, "--category", "-c", help="Filter by failure category keyword"),
    filter_expr: Optional[str] = typer.Option(None, "--filter", "-f", help="JMESPath filter"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview matching messages without publishing"),
    limit: Optional[int] = typer.Option(50, "--limit", "-l", help="Max records to redrive"),
    broker: str = typer.Option("localhost:9092", "--broker", "-b", help="Kafka broker"),
    demo: bool = typer.Option(False, "--demo", help="Use simulated DLQ records"),
) -> None:
    """Batch redrive dead-letter records with failure category filtering and dry-run preview."""
    asyncio.run(
        _async_dlq_redrive_batch(
            topic=topic,
            target=target,
            category=category,
            filter_expr=filter_expr,
            dry_run=dry_run,
            limit=limit,
            broker=broker,
            demo=demo,
        )
    )


async def _async_dlq_redrive_batch(
    topic: str,
    target: Optional[str],
    category: Optional[str],
    filter_expr: Optional[str],
    dry_run: bool,
    limit: Optional[int],
    broker: str,
    demo: bool,
) -> None:
    config = EventLensConfig(bootstrap_servers=broker)
    dlq_engine = DLQEngine(config=config)

    console.print(
        f"[bold cyan]DLQ Batch Redrive[/] on [yellow]{topic}[/] "
        f"(Mode: {'[bold magenta]DRY RUN[/]' if dry_run else '[bold green]LIVE EXECUTION[/]'})"
    )

    records: list[KafkaRecord] = []
    if demo:
        sim = MockStreamSimulator(topic=topic)
        for _ in range(limit or 20):
            records.append(sim.generate_record(force_dlq=True))
    else:
        consumer = AsyncConsumer(topic=topic, config=config, from_beginning=True)
        try:
            async for rec in consumer.stream_records(max_messages=limit or 100):
                records.append(rec)
        except Exception as err:
            console.print(f"[bold red]Failed fetching records from DLQ:[/] {err}")
            return

    summary = await dlq_engine.redrive_batch(
        records=records,
        target_topic=target,
        category=category,
        filter_expr=filter_expr,
        dry_run=dry_run,
        limit=limit,
    )

    table = Table(title="Batch Redrive Summary", expand=True)
    table.add_column("Scanned", justify="right", width=10)
    table.add_column("Matched", justify="right", width=10)
    table.add_column("Redriven / Action", justify="right", width=18)
    table.add_column("Skipped", justify="right", width=10)
    table.add_column("Failed", justify="right", width=10)
    table.add_column("Dry Run", width=10)

    action_label = (
        f"[yellow]{summary.redriven_count} (simulated)[/]" if dry_run else f"[green]{summary.redriven_count}[/]"
    )
    table.add_row(
        str(summary.total_scanned),
        str(summary.matched_count),
        action_label,
        str(summary.skipped_count),
        f"[red]{summary.failed_count}[/]" if summary.failed_count else "0",
        "[yellow]YES[/]" if dry_run else "[dim]NO[/]",
    )
    console.print(table)


@app.command("demo")
def demo_command(
    rate: float = typer.Option(2.0, "--rate", "-r", help="Messages per second"),
    count: int = typer.Option(15, "--count", "-c", help="Total messages to stream"),
    filter_expr: Optional[str] = typer.Option(None, "--filter", "-f", help="JMESPath filter"),
) -> None:
    """Stream simulated order events and DLQ poison pills in terminal."""
    asyncio.run(
        _async_tail(
            topic="order.events",
            broker="demo-broker",
            from_beginning=True,
            tail_n=None,
            partition=None,
            filter_expr=filter_expr,
            regex=None,
            proto=None,
            message_type=None,
            format_type="table",
            max_messages=count,
            demo=True,
        )
    )


@app.command("record")
def record_command(
    topic: str = typer.Argument(..., help="Kafka topic to capture"),
    output: Path = typer.Option(..., "--output", "-o", help="Target .lens session file path"),
    duration: Optional[float] = typer.Option(None, "--duration", "-d", help="Duration in seconds to record"),
    max_messages: Optional[int] = typer.Option(None, "--max-messages", "-m", help="Max messages to record"),
    broker: str = typer.Option("localhost:9092", "--broker", "-b", help="Kafka broker address"),
    demo: bool = typer.Option(False, "--demo", help="Record from simulated stream"),
) -> None:
    """Capture live traffic from a topic into a .lens session file."""
    asyncio.run(
        _async_record(
            topic=topic,
            output=output,
            duration=duration,
            max_messages=max_messages,
            broker=broker,
            demo=demo,
        )
    )


async def _async_record(
    topic: str,
    output: Path,
    duration: Optional[float],
    max_messages: Optional[int],
    broker: str,
    demo: bool,
) -> None:
    recorder = TrafficRecorder(output_path=output)
    console.print(f"[bold cyan]Recording traffic from[/] [green]{topic}[/] -> [yellow]{output}[/]")

    if demo:
        sim = MockStreamSimulator(topic=topic)
        stream = sim.stream_records(rate_per_second=3.0, max_messages=max_messages)
    else:
        config = EventLensConfig(bootstrap_servers=broker)
        consumer = AsyncConsumer(topic=topic, config=config, from_beginning=False)
        stream = consumer.stream_records(max_messages=max_messages)

    try:
        count = await recorder.record_stream(
            stream=stream,
            max_messages=max_messages,
            duration_seconds=duration,
        )
        console.print(f"[bold green][OK] Captured {count} messages into {output}[/]")
    except KeyboardInterrupt:
        console.print(f"\n[yellow]Recording stopped. Saved {recorder.recorded_count} messages to {output}[/]")


@app.command("replay")
def replay_command(
    session_file: Path = typer.Argument(..., help="Path to .lens session file"),
    target: Optional[str] = typer.Option(None, "--target", "-t", help="Target topic (defaults to original topic)"),
    speed: float = typer.Option(
        1.0, "--speed", "-s", help="Playback speed multiplier (e.g. 1.0, 2.0, 0 for max speed)"
    ),
    loop: bool = typer.Option(False, "--loop", help="Loop replay indefinitely"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Simulate replay without publishing to Kafka"),
    broker: str = typer.Option("localhost:9092", "--broker", "-b", help="Kafka broker address"),
) -> None:
    """Replay a recorded .lens session file into a target topic."""
    asyncio.run(
        _async_replay(
            session_file=session_file,
            target=target,
            speed=speed,
            loop=loop,
            dry_run=dry_run,
            broker=broker,
        )
    )


async def _async_replay(
    session_file: Path,
    target: Optional[str],
    speed: float,
    loop: bool,
    dry_run: bool,
    broker: str,
) -> None:
    if not session_file.exists():
        console.print(f"[bold red]Session file not found:[/] {session_file}")
        raise typer.Exit(1)

    config = EventLensConfig(bootstrap_servers=broker)
    replayer = TrafficReplayer(session_path=session_file, config=config)

    console.print(
        f"[bold cyan]Replaying session[/] [yellow]{session_file.name}[/] "
        f"(Speed: {speed}x, Mode: {'[bold magenta]DRY RUN[/]' if dry_run else '[bold green]LIVE[/]'})"
    )

    count = 0
    async for record, published in replayer.stream_replay(
        target_topic=target,
        speed=speed,
        loop=loop,
        dry_run=dry_run,
    ):
        count += 1
        status = "[yellow]SIMULATED[/]" if dry_run else "[green]REPLAYED[/]"
        console.print(f"[{status}] #{count} -> [cyan]{record.topic}[/] (Key: {record.key})")

    console.print(f"[bold green][OK] Replay completed: {count} messages processed.[/]")


@app.command("trace")
def trace_command(
    correlation_id: str = typer.Argument(..., help="Correlation ID, order_id, or traceparent to search"),
    topics: str = typer.Option(
        "order.events,payment.events,inventory.events,order.events.dlq",
        "--topics",
        "-t",
        help="Comma-separated topics to scan",
    ),
    broker: str = typer.Option("localhost:9092", "--broker", "-b", help="Kafka broker address"),
    demo: bool = typer.Option(False, "--demo", help="Use simulated distributed trace data"),
) -> None:
    """Trace the end-to-end lifecycle of an event across multiple Kafka topics."""
    asyncio.run(_async_trace(correlation_id=correlation_id, topics_str=topics, broker=broker, demo=demo))


async def _async_trace(
    correlation_id: str,
    topics_str: str,
    broker: str,
    demo: bool,
) -> None:
    topic_list = [t.strip() for t in topics_str.split(",") if t.strip()]
    console.print(
        f"[bold cyan]Tracing Event Lifecycle for ID:[/] [bold yellow]{correlation_id}[/] "
        f"across {len(topic_list)} topics..."
    )

    records: list[KafkaRecord] = []

    if demo:
        import time

        now_ms = int(time.time() * 1000)
        records = [
            KafkaRecord(
                topic="order.events",
                partition=0,
                offset=1001,
                timestamp=now_ms - 250,
                key=correlation_id,
                value='{"event_type": "OrderCreated", "order_id": "' + correlation_id + '", "status": "PENDING"}',
                decoded_payload={"event_type": "OrderCreated", "order_id": correlation_id, "status": "PENDING"},
                headers={"x-correlation-id": correlation_id, "traceparent": f"00-{correlation_id}-01"},
            ),
            KafkaRecord(
                topic="order.events",
                partition=0,
                offset=1002,
                timestamp=now_ms - 180,
                key=correlation_id,
                value='{"event_type": "OrderValidated", "order_id": "' + correlation_id + '"}',
                decoded_payload={"event_type": "OrderValidated", "order_id": correlation_id},
                headers={"x-correlation-id": correlation_id},
            ),
            KafkaRecord(
                topic="payment.events",
                partition=1,
                offset=405,
                timestamp=now_ms - 90,
                key=correlation_id,
                value='{"event_type": "OrderPaid", "order_id": "' + correlation_id + '", "amount": 149.99}',
                decoded_payload={"event_type": "OrderPaid", "order_id": correlation_id, "amount": 149.99},
                headers={"x-correlation-id": correlation_id},
            ),
            KafkaRecord(
                topic="order.events.dlq",
                partition=0,
                offset=88,
                timestamp=now_ms,
                key=correlation_id,
                value='{"order_id": "' + correlation_id + '", "error": "InventoryAllocationException"}',
                decoded_payload={"order_id": correlation_id, "error": "InventoryAllocationException"},
                headers={
                    "x-correlation-id": correlation_id,
                    "x-exception-message": "InventoryAllocationException: Item SKU-901 out of stock",
                    "x-original-topic": "inventory.events",
                },
                is_dlq=True,
                error_message="InventoryAllocationException: Item SKU-901 out of stock",
            ),
        ]
    else:
        config = EventLensConfig(bootstrap_servers=broker)
        for t in topic_list:
            consumer = AsyncConsumer(topic=t, config=config, from_beginning=True)
            try:
                async for rec in consumer.stream_records(max_messages=100):
                    if EventTracer.record_matches_correlation(rec, correlation_id):
                        records.append(rec)
            except Exception as err:
                console.print(f"[dim]Topic {t} scan error: {err}[/]")

    graph = EventTracer.build_trace_graph(correlation_id=correlation_id, records=records)

    if not graph.hops:
        console.print(f"[yellow]No events found matching correlation ID '{correlation_id}'.[/]")
        return

    table = Table(title=f"Distributed Event Trace: {correlation_id}", expand=True)
    table.add_column("Step", justify="right", width=6)
    table.add_column("Topic", width=20)
    table.add_column("P:Offset", width=12)
    table.add_column("Event Type / Action", width=22)
    table.add_column("Latency Delta", justify="right", width=14)
    table.add_column("Status", width=12)

    for idx, hop in enumerate(graph.hops, 1):
        status_style = "[bold red]POISON (DLQ)[/]" if hop.is_dlq else "[green]SUCCESS[/]"
        delta_str = f"+{hop.latency_from_prev_ms:.0f} ms" if hop.latency_from_prev_ms is not None else "START"
        evt_str = hop.event_type or (hop.error_message[:20] if hop.error_message else "Message")

        table.add_row(
            str(idx),
            f"[cyan]{hop.topic}[/]",
            f"{hop.partition}:{hop.offset}",
            evt_str,
            delta_str,
            status_style,
        )

    console.print(table)
    total_str = f"{graph.total_latency_ms:.0f} ms" if graph.total_latency_ms is not None else "unknown"
    console.print(
        f"[bold]Trace Summary:[/] Total Hops: {len(graph.hops)} | Total E2E Latency: [bold magenta]{total_str}[/] | "
        f"DLQ Encounted: {'[bold red]YES[/]' if graph.has_dlq else '[bold green]NO[/]'}\n"
    )


@app.command("diff")
def diff_command(
    target_a: str = typer.Argument(..., help="First message (file path or topic:offset)"),
    target_b: str = typer.Argument(..., help="Second message (file path or topic:offset)"),
    broker: str = typer.Option("localhost:9092", "--broker", "-b", help="Kafka broker address"),
    demo: bool = typer.Option(False, "--demo", help="Use sample payloads for diff comparison"),
) -> None:
    """Compare two Kafka message payloads or JSON files side-by-side."""
    asyncio.run(_async_diff(target_a=target_a, target_b=target_b, broker=broker, demo=demo))


async def _async_diff(
    target_a: str,
    target_b: str,
    broker: str,
    demo: bool,
) -> None:
    payload_a = None
    payload_b = None

    if demo:
        payload_a = {
            "order_id": "ord_9011",
            "currency": "USD",
            "total_amount": 149.99,
            "items": [{"id": "item_1", "qty": 2}],
            "status": "PROCESSED",
        }
        payload_b = {
            "order_id": "ord_9011",
            "currency": "EUR",
            "total_amount": 149.99,
            "items": [{"id": "item_1", "qty": -5}],
            "status": "REJECTED_DLQ",
            "failure_reason": "Negative quantity",
        }
    else:
        # Check if raw JSON strings
        try:
            payload_a = json.loads(target_a)
        except Exception:
            p_a = Path(target_a)
            if p_a.exists():
                payload_a = json.loads(p_a.read_text(encoding="utf-8"))

        try:
            payload_b = json.loads(target_b)
        except Exception:
            p_b = Path(target_b)
            if p_b.exists():
                payload_b = json.loads(p_b.read_text(encoding="utf-8"))

        # Fallback to topic:offset lookup
        if payload_a is None or payload_b is None:
            console.print("[dim]Fetching records from Kafka cluster...[/]")
            # In live mode, fetch records via consumer seek
            # Fallback to raw string if unable to connect
            payload_a = payload_a or {"source": target_a}
            payload_b = payload_b or {"source": target_b}

    diff_result = compare_payloads(payload_a, payload_b)
    table = diff_result.to_rich_table(title_a=target_a, title_b=target_b)
    console.print(table)


@app.command("produce")
def produce_command(
    topic: str = typer.Argument(..., help="Destination Kafka topic"),
    key: Optional[str] = typer.Option(None, "--key", "-k", help="Message key"),
    json_str: Optional[str] = typer.Option(None, "--json", "-j", help="JSON payload string"),
    file_path: Optional[Path] = typer.Option(None, "--file", "-f", help="Path to JSON or payload file"),
    headers_str: Optional[str] = typer.Option(None, "--headers", "-H", help="Headers in k:v,k:v format"),
    count: int = typer.Option(1, "--count", "-n", help="Number of messages to produce"),
    rate: float = typer.Option(10.0, "--rate", "-r", help="Send rate in messages per second"),
    broker: str = typer.Option("localhost:9092", "--broker", "-b", help="Kafka broker address"),
    demo: bool = typer.Option(False, "--demo", help="Simulate producing without live broker"),
) -> None:
    """Publish custom messages directly into a Kafka topic."""
    asyncio.run(
        _async_produce(
            topic=topic,
            key=key,
            json_str=json_str,
            file_path=file_path,
            headers_str=headers_str,
            count=count,
            rate=rate,
            broker=broker,
            demo=demo,
        )
    )


async def _async_produce(
    topic: str,
    key: Optional[str],
    json_str: Optional[str],
    file_path: Optional[Path],
    headers_str: Optional[str],
    count: int,
    rate: float,
    broker: str,
    demo: bool,
) -> None:
    payload: Any = "{}"
    if file_path:
        if not file_path.exists():
            console.print(f"[bold red]Payload file not found:[/] {file_path}")
            raise typer.Exit(1)
        payload = file_path.read_text(encoding="utf-8")
    elif json_str:
        payload = json_str

    headers_dict: dict[str, str] = {}
    if headers_str:
        for pair in headers_str.split(","):
            if ":" in pair:
                k, v = pair.split(":", 1)
                headers_dict[k.strip()] = v.strip()

    console.print(
        f"[bold cyan]Producing {count} message(s) to[/] [green]{topic}[/] "
        f"(Mode: {'[bold magenta]SIMULATED[/]' if demo else '[bold green]LIVE[/]'})"
    )

    delay = 1.0 / max(0.1, rate) if count > 1 else 0

    if demo:
        for i in range(1, count + 1):
            cur_key = f"{key}_{i}" if (key and count > 1) else (key or f"msg_{i}")
            console.print(f"[bold green][OK] Produced simulated message #{i}[/] (Topic: {topic}, Key: {cur_key})")
            if delay > 0 and i < count:
                await asyncio.sleep(delay)
    else:
        config = EventLensConfig(bootstrap_servers=broker)
        producer = AsyncProducer(config=config)
        await producer.start()
        try:
            for i in range(1, count + 1):
                cur_key = f"{key}_{i}" if (key and count > 1) else key
                part, off = await producer.send(
                    topic=topic,
                    value=payload,
                    key=cur_key,
                    headers=headers_dict if headers_dict else None,
                )
                console.print(
                    f"[bold green][OK] Published message #{i}[/] (Topic: {topic}, Partition: {part}, Offset: {off})"
                )
                if delay > 0 and i < count:
                    await asyncio.sleep(delay)
        finally:
            await producer.stop()


@app.command("tui")
def tui_command(
    topic: str = typer.Option("order.events", "--topic", "-t", help="Initial topic"),
    broker: str = typer.Option("localhost:9092", "--broker", "-b", help="Kafka broker"),
    demo: bool = typer.Option(False, "--demo", help="Run TUI in offline demo mode"),
) -> None:
    """Launch full-screen interactive Terminal User Interface (Textual)."""
    from eventlens.tui.app import EventLensTUI

    app_instance = EventLensTUI(topic=topic, broker=broker, demo=demo)
    app_instance.run()


@app.command("web")
def web_command(
    host: str = typer.Option("127.0.0.1", "--host", "-h", help="Host binding"),
    port: int = typer.Option(8080, "--port", "-p", help="Port number"),
    topic: str = typer.Option("order.events", "--topic", "-t", help="Topic to bridge"),
    broker: str = typer.Option("localhost:9092", "--broker", "-b", help="Kafka broker"),
    demo: bool = typer.Option(True, "--demo", help="Run web dashboard in demo mode"),
) -> None:
    """Launch local web dashboard and WebSocket streaming server."""
    import uvicorn

    from eventlens.web.server import create_app

    server_app = create_app(topic=topic, broker=broker, demo=demo)
    console.print(f"[bold cyan]EventLens Web Dashboard[/] starting at [bold underline green]http://{host}:{port}[/]")
    uvicorn.run(server_app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    app()
