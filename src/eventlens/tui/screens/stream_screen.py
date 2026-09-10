"""Live event streaming screen for Textual TUI."""

from __future__ import annotations

import json

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import DataTable, Input, Label, Static

from eventlens.engine.filter import EventFilter
from eventlens.models import KafkaRecord
from eventlens.tui.widgets.event_tree import EventTree


class StreamScreen(Widget):
    """Live streaming interface with table feed, filter, and inspector."""

    DEFAULT_CSS = """
    StreamScreen {
        layout: vertical;
        height: 100%;
    }
    #filter-container {
        height: 3;
        dock: top;
        background: #181825;
        padding: 0 1;
        border-bottom: solid #313244;
    }
    #main-split {
        height: 1fr;
    }
    #table-panel {
        width: 60%;
        height: 100%;
        border-right: solid #313244;
    }
    #inspector-panel {
        width: 40%;
        height: 100%;
        background: #11111b;
        padding: 1;
    }
    #inspector-headers {
        height: 8;
        border-bottom: solid #313244;
        overflow-y: scroll;
    }
    #inspector-tree-container {
        height: 1fr;
    }
    DataTable {
        height: 100%;
    }
    """

    filter_query: reactive[str] = reactive("")

    def __init__(self) -> None:
        super().__init__()
        self.records: list[KafkaRecord] = []
        self.filter_engine = EventFilter()

    def compose(self) -> ComposeResult:
        with Horizontal(id="filter-container"):
            yield Label("Filter (JMESPath): ", classes="filter-label")
            yield Input(placeholder="e.g. currency == 'USD' && total_amount > '50'", id="filter-input")

        with Horizontal(id="main-split"):
            with Vertical(id="table-panel"):
                yield DataTable(id="records-table", cursor_type="row")
            with Vertical(id="inspector-panel"):
                yield Label("[bold cyan]Message Headers[/]", id="headers-title")
                yield Static("No message selected", id="inspector-headers")
                yield Label("[bold cyan]Payload Inspector[/]", id="payload-title")
                with Vertical(id="inspector-tree-container"):
                    yield EventTree("Payload", id="payload-tree")

    def on_mount(self) -> None:
        table = self.query_one("#records-table", DataTable)
        table.add_columns("Status", "Part", "Offset", "Key", "Format", "Preview")

    def add_record(self, record: KafkaRecord) -> None:
        """Process incoming record and append to table if matches filter."""
        self.records.append(record)
        if len(self.records) > 1000:
            self.records.pop(0)

        if not self.filter_engine.matches(record):
            return

        table = self.query_one("#records-table", DataTable)
        status = "[red]DLQ[/]" if record.is_dlq else "[green]OK[/]"

        preview = (
            json.dumps(record.decoded_payload, default=str)
            if isinstance(record.decoded_payload, (dict, list))
            else str(record.value or "")
        )
        if len(preview) > 40:
            preview = preview[:37] + "..."

        table.add_row(
            status,
            str(record.partition),
            str(record.offset),
            record.key or "-",
            record.payload_format,
            preview,
            key=str(len(self.records) - 1),
        )

    def on_input_changed(self, event: Input.Changed) -> None:
        """Update live filter when query text changes."""
        query = event.value.strip()
        self.filter_engine = EventFilter(jmespath_query=query if query else None)
        self._rebuild_table()

    def _rebuild_table(self) -> None:
        table = self.query_one("#records-table", DataTable)
        table.clear()
        for idx, rec in enumerate(self.records):
            if self.filter_engine.matches(rec):
                status = "[red]DLQ[/]" if rec.is_dlq else "[green]OK[/]"
                preview = (
                    json.dumps(rec.decoded_payload, default=str)
                    if isinstance(rec.decoded_payload, (dict, list))
                    else str(rec.value or "")
                )
                if len(preview) > 40:
                    preview = preview[:37] + "..."
                table.add_row(
                    status,
                    str(rec.partition),
                    str(rec.offset),
                    rec.key or "-",
                    rec.payload_format,
                    preview,
                    key=str(idx),
                )

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Display selected message headers and JSON tree."""
        row_key = event.row_key.value
        if not row_key or not row_key.isdigit():
            return
        idx = int(row_key)
        if idx < 0 or idx >= len(self.records):
            return

        rec = self.records[idx]

        # Update headers
        headers_static = self.query_one("#inspector-headers", Static)
        if rec.headers:
            h_text = "\n".join(f"[bold dim]{k}:[/] {v}" for k, v in rec.headers.items())
            headers_static.update(h_text)
        else:
            headers_static.update("[dim](No headers)[/]")

        # Update payload tree
        tree = self.query_one("#payload-tree", EventTree)
        if isinstance(rec.decoded_payload, (dict, list)):
            tree.load_data(f"Record {rec.partition}:{rec.offset}", rec.decoded_payload)
        else:
            tree.load_data(f"Record {rec.partition}:{rec.offset}", {"raw": rec.value})
