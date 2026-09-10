"""Typer CLI interface for EventLens."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from eventlens.config import EventLensConfig
from eventlens.decoders.registry import DecoderRegistry
from eventlens.engine.consumer import AsyncConsumer
from eventlens.engine.dlq_engine import DLQEngine
from eventlens.engine.filter import EventFilter
from eventlens.engine.lag_tracker import LagTracker
from eventlens.engine.simulator import MockStreamSimulator
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
