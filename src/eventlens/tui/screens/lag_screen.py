"""Partition lag and consumer group monitoring screen for Textual TUI."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.widget import Widget
from textual.widgets import DataTable, Label, Static

from eventlens.models import ConsumerGroupLag


class LagScreen(Widget):
    """Visualizes partition offsets, high watermarks, and committed consumer lag."""

    DEFAULT_CSS = """
    LagScreen {
        layout: vertical;
        padding: 1;
        height: 100%;
    }
    #lag-summary-box {
        height: 4;
        background: #1e1e2e;
        border: solid #89b4fa;
        padding: 1;
        margin-bottom: 1;
    }
    DataTable {
        height: 1fr;
    }
    """

    def compose(self) -> ComposeResult:
        yield Static("[bold cyan]Consumer Group Lag Summary[/]", id="lag-summary-box")
        yield Label("[bold green]Partition Distribution and Lag Gauges[/]")
        yield DataTable(id="lag-table", cursor_type="row")

    def on_mount(self) -> None:
        table = self.query_one("#lag-table", DataTable)
        table.add_columns("Partition", "Log End (High)", "Committed", "Lag", "Lag Meter")

    def update_lag(self, lag_data: ConsumerGroupLag) -> None:
        """Refresh lag statistics."""
        summary = self.query_one("#lag-summary-box", Static)
        summary.update(
            f"[bold cyan]Group:[/] [white]{lag_data.group_id}[/] | "
            f"[bold cyan]Topic:[/] [white]{lag_data.topic}[/] | "
            f"[bold cyan]Total Lag:[/] [bold {'red' if lag_data.total_lag > 50 else 'green'}]{lag_data.total_lag}[/] messages"
        )

        table = self.query_one("#lag-table", DataTable)
        table.clear()

        for p in lag_data.partitions:
            bar_len = min(25, int(p.lag / 10)) if p.lag > 0 else 0
            color = "red" if p.lag > 50 else "yellow" if p.lag > 10 else "green"
            meter = f"[{color}]{'#' * bar_len}[/]"

            table.add_row(
                str(p.partition),
                str(p.log_end_offset),
                str(p.current_offset),
                str(p.lag),
                meter,
            )
