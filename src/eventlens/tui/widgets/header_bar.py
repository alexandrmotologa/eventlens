"""Header bar widget for EventLens TUI."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Static


class HeaderBar(Widget):
    """Displays connection status, active topic, and message throughput."""

    DEFAULT_CSS = """
    HeaderBar {
        dock: top;
        height: 3;
        background: #1e1e2e;
        color: #cdd6f4;
        border-bottom: solid #45475a;
        padding: 0 1;
    }
    .header-title {
        text-style: bold;
        color: #89b4fa;
    }
    .header-info {
        color: #a6adc8;
    }
    .header-status-ok {
        color: #a6e3a1;
        text-style: bold;
    }
    .header-status-demo {
        color: #f9e2af;
        text-style: bold;
    }
    """

    topic: reactive[str] = reactive("order.events")
    broker: reactive[str] = reactive("localhost:9092")
    is_demo: reactive[bool] = reactive(False)
    is_paused: reactive[bool] = reactive(False)
    events_per_sec: reactive[float] = reactive(0.0)

    def __init__(
        self,
        topic: str = "order.events",
        broker: str = "localhost:9092",
        demo: bool = False,
    ) -> None:
        super().__init__()
        self.topic = topic
        self.broker = broker
        self.is_demo = demo

    def compose(self) -> ComposeResult:
        mode_text = "[DEMO MODE]" if self.is_demo else f"Broker: {self.broker}"
        status_badge = "[PAUSED]" if self.is_paused else "[STREAMING]"
        yield Static(
            f"[bold cyan]EVENTLENS[/] | Topic: [bold green]{self.topic}[/] | "
            f"[dim]{mode_text}[/] | Status: [bold yellow]{status_badge}[/] | "
            f"Throughput: [bold magenta]{self.events_per_sec:.1f}[/] eps",
            id="header-content",
        )

    def watch_is_paused(self, new_val: bool) -> None:
        self.update_content()

    def watch_events_per_sec(self, new_val: float) -> None:
        self.update_content()

    def update_content(self) -> None:
        if not self.is_mounted:
            return
        try:
            mode_text = "[DEMO MODE]" if self.is_demo else f"Broker: {self.broker}"
            status_badge = "[PAUSED]" if self.is_paused else "[STREAMING]"
            static = self.query_one("#header-content", Static)
            static.update(
                f"[bold cyan]EVENTLENS[/] | Topic: [bold green]{self.topic}[/] | "
                f"[dim]{mode_text}[/] | Status: [bold {'yellow' if self.is_paused else 'green'}]{status_badge}[/] | "
                f"Throughput: [bold magenta]{self.events_per_sec:.1f}[/] eps"
            )
        except Exception:
            pass
