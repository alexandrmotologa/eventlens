"""Main Textual application for EventLens."""

from __future__ import annotations

import asyncio
import json
import time

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import Footer, TabbedContent, TabPane

from eventlens.config import EventLensConfig
from eventlens.decoders.registry import DecoderRegistry
from eventlens.engine.consumer import AsyncConsumer
from eventlens.engine.dlq_engine import DLQEngine
from eventlens.engine.lag_tracker import LagTracker
from eventlens.engine.simulator import MockStreamSimulator
from eventlens.tui.screens.dlq_screen import DLQScreen
from eventlens.tui.screens.lag_screen import LagScreen
from eventlens.tui.screens.stream_screen import StreamScreen
from eventlens.tui.widgets.header_bar import HeaderBar


class EventLensTUI(App[None]):
    """EventLens Full-Screen Interactive Terminal User Interface."""

    TITLE = "EventLens - Kafka Stream Debugger & DLQ Studio"
    CSS = """
    Screen {
        background: #11111b;
        color: #cdd6f4;
    }
    TabbedContent {
        height: 1fr;
    }
    TabPane {
        padding: 0;
    }
    """

    BINDINGS = [
        Binding("space", "toggle_pause", "Pause/Resume", show=True),
        Binding("ctrl+r", "trigger_redrive", "Redrive DLQ", show=True),
        Binding("c", "copy_payload", "Copy JSON", show=True),
        Binding("q", "quit", "Quit", show=True),
    ]

    def __init__(
        self,
        topic: str = "order.events",
        broker: str = "localhost:9092",
        demo: bool = False,
    ) -> None:
        super().__init__()
        self.target_topic = topic
        self.broker = broker
        self.is_demo = demo
        self.is_paused = False

        self.config = EventLensConfig(bootstrap_servers=broker)
        self.dlq_engine = DLQEngine(config=self.config)
        self.decoders = DecoderRegistry()

        self._event_count = 0
        self._last_time = time.time()
        self._stop_event = asyncio.Event()

    def compose(self) -> ComposeResult:
        yield HeaderBar(topic=self.target_topic, broker=self.broker, demo=self.is_demo)
        with TabbedContent(initial="tab-streams"):
            with TabPane("Live Streams", id="tab-streams"):
                yield StreamScreen()
            with TabPane("DLQ Studio", id="tab-dlq"):
                yield DLQScreen(dlq_engine=self.dlq_engine)
            with TabPane("Lag Monitor", id="tab-lag"):
                yield LagScreen()
        yield Footer()

    async def on_mount(self) -> None:
        """Start background ingestion task when application boots."""
        self.run_worker(self._ingest_loop, exclusive=True)
        self.run_worker(self._lag_polling_loop, exclusive=False)

    async def _ingest_loop(self) -> None:
        """Background worker consuming live or simulated records."""
        stream_screen = self.query_one(StreamScreen)
        dlq_screen = self.query_one(DLQScreen)
        header_bar = self.query_one(HeaderBar)

        if self.is_demo:
            sim = MockStreamSimulator(topic=self.target_topic)
            stream = sim.stream_records(rate_per_second=2.5, stop_event=self._stop_event)
        else:
            consumer = AsyncConsumer(
                topic=self.target_topic,
                config=self.config,
                decoder_registry=self.decoders,
                from_beginning=False,
            )
            stream = consumer.stream_records(stop_event=self._stop_event)

        try:
            async for record in stream:
                while self.is_paused:
                    await asyncio.sleep(0.2)
                    if self._stop_event.is_set():
                        return

                self._event_count += 1
                stream_screen.add_record(record)
                if record.is_dlq:
                    dlq_screen.add_dlq_record(record)

                now = time.time()
                elapsed = now - self._last_time
                if elapsed >= 1.0:
                    header_bar.events_per_sec = self._event_count / elapsed
                    self._event_count = 0
                    self._last_time = now

        except asyncio.CancelledError:
            pass
        except Exception as err:
            self.notify(f"Stream error: {err}", severity="error")

    async def _lag_polling_loop(self) -> None:
        """Periodically poll consumer lag."""
        lag_screen = self.query_one(LagScreen)
        tracker = LagTracker(config=self.config)
        sim = MockStreamSimulator()

        while not self._stop_event.is_set():
            try:
                if self.is_demo:
                    lag_data = sim.generate_mock_lag(topic=self.target_topic)
                else:
                    lag_data = await tracker.get_topic_lag(topic=self.target_topic)

                lag_screen.update_lag(lag_data)
            except Exception:
                pass

            await asyncio.sleep(4.0)

    def action_toggle_pause(self) -> None:
        """Toggle stream pause state."""
        self.is_paused = not self.is_paused
        header_bar = self.query_one(HeaderBar)
        header_bar.is_paused = self.is_paused
        self.notify("Stream paused" if self.is_paused else "Stream resumed")

    async def action_trigger_redrive(self) -> None:
        """Trigger DLQ redrive action on current screen."""
        try:
            dlq_screen = self.query_one(DLQScreen)
            await dlq_screen.action_redrive()
        except Exception as err:
            self.notify(f"Redrive action error: {err}", severity="error")

    def action_copy_payload(self) -> None:
        """Copy active record JSON payload to system clipboard."""
        stream_screen = self.query_one(StreamScreen)
        table = stream_screen.query_one("#records-table")
        if table.cursor_row is not None and table.cursor_row < len(stream_screen.records):
            rec = stream_screen.records[table.cursor_row]
            payload_str = (
                json.dumps(rec.decoded_payload, indent=2)
                if isinstance(rec.decoded_payload, (dict, list))
                else str(rec.value or "")
            )
            # Textual clipboard copy
            try:
                self.copy_to_clipboard(payload_str)
                self.notify(f"Copied offset {rec.offset} payload to clipboard")
            except Exception:
                self.notify("Clipboard unavailable in current terminal", severity="warning")
