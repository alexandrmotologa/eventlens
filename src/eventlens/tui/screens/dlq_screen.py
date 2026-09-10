"""Dead Letter Queue triage and redrive screen for Textual TUI."""

from __future__ import annotations

import json

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widget import Widget
from textual.widgets import Button, DataTable, Input, Label, Static, TextArea

from eventlens.engine.dlq_engine import DLQEngine
from eventlens.models import KafkaRecord


class DLQScreen(Widget):
    """Interactive DLQ inspector, failure diagnostic view, and payload editor."""

    DEFAULT_CSS = """
    DLQScreen {
        layout: vertical;
        height: 100%;
    }
    #dlq-main-layout {
        height: 1fr;
    }
    #dlq-table-pane {
        width: 35%;
        height: 100%;
        border-right: solid #313244;
    }
    #dlq-detail-pane {
        width: 65%;
        height: 100%;
        padding: 1;
        background: #11111b;
    }
    #diagnostic-box {
        height: 10;
        background: #1e1e2e;
        border: solid #f38ba8;
        padding: 1;
        margin-bottom: 1;
    }
    #editor-box {
        height: 1fr;
    }
    #redrive-bar {
        height: 3;
        dock: bottom;
        background: #181825;
        padding: 0 1;
    }
    #target-topic-input {
        width: 30;
    }
    #redrive-btn {
        margin-left: 2;
    }
    #status-label {
        margin-left: 2;
        color: #a6e3a1;
        text-style: bold;
    }
    """

    def __init__(self, dlq_engine: DLQEngine | None = None) -> None:
        super().__init__()
        self.dlq_engine = dlq_engine or DLQEngine()
        self.poison_pills: list[KafkaRecord] = []
        self.current_record: KafkaRecord | None = None

    def compose(self) -> ComposeResult:
        with Horizontal(id="dlq-main-layout"):
            with Vertical(id="dlq-table-pane"):
                yield Label("[bold red]Dead Letter Queue Messages[/]")
                yield DataTable(id="dlq-table", cursor_type="row")

            with Vertical(id="dlq-detail-pane"):
                yield Label("[bold yellow]Heuristic Failure Diagnosis[/]")
                yield Static("Select a poisoned message to diagnose.", id="diagnostic-box")

                yield Label("[bold cyan]In-Terminal Payload Editor (Patch and Redrive)[/]")
                with Vertical(id="editor-box"):
                    yield TextArea("", id="payload-editor")

                with Horizontal(id="redrive-bar"):
                    yield Label("Target Topic: ")
                    yield Input(placeholder="e.g. order.events", id="target-topic-input")
                    yield Button("Redrive (Ctrl+R)", id="redrive-btn", variant="error")
                    yield Label("", id="status-label")

    def on_mount(self) -> None:
        table = self.query_one("#dlq-table", DataTable)
        table.add_columns("Off", "Key", "Exception")

    def add_dlq_record(self, record: KafkaRecord) -> None:
        """Add a poisoned message to the list and table."""
        self.poison_pills.append(record)
        table = self.query_one("#dlq-table", DataTable)
        err = record.error_message or "Unknown failure"
        if len(err) > 30:
            err = err[:27] + "..."
        table.add_row(str(record.offset), record.key or "-", err, key=str(len(self.poison_pills) - 1))

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        row_key = event.row_key.value
        if not row_key or not row_key.isdigit():
            return
        idx = int(row_key)
        if idx < 0 or idx >= len(self.poison_pills):
            return

        rec = self.poison_pills[idx]
        self.current_record = rec

        # Run diagnosis
        diag = DLQEngine.diagnose_record(rec)
        diag_box = self.query_one("#diagnostic-box", Static)
        diag_box.update(
            f"[bold red]Category:[/] {diag.failure_category}\n"
            f"[bold yellow]Root Cause:[/] {diag.root_cause}\n"
            f"[bold green]Suggested Action:[/] {diag.suggested_action}\n"
            f"[dim]Original Topic:[/] {diag.original_topic or 'unknown'}"
        )

        # Set default target topic
        target_input = self.query_one("#target-topic-input", Input)
        if diag.original_topic:
            target_input.value = diag.original_topic

        # Populate editor
        editor = self.query_one("#payload-editor", TextArea)
        if isinstance(rec.decoded_payload, (dict, list)):
            formatted = json.dumps(rec.decoded_payload, indent=2)
        else:
            formatted = str(rec.value or "")
        editor.text = formatted

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "redrive-btn":
            await self.action_redrive()

    async def action_redrive(self) -> None:
        """Execute redrive with current edited payload."""
        status_lbl = self.query_one("#status-label", Label)
        if not self.current_record:
            status_lbl.update("[bold red]No message selected[/]")
            return

        target_topic = self.query_one("#target-topic-input", Input).value.strip()
        if not target_topic:
            status_lbl.update("[bold red]Specify target topic[/]")
            return

        editor = self.query_one("#payload-editor", TextArea)
        edited_text = editor.text.strip()

        try:
            patched_payload = json.loads(edited_text)
        except Exception:
            patched_payload = edited_text

        result = await self.dlq_engine.redrive(
            record=self.current_record,
            target_topic=target_topic,
            patched_payload=patched_payload,
        )

        if result.success:
            status_lbl.update(
                f"[bold green][OK] Redriven to {target_topic} (Part: {result.produced_partition}, Off: {result.produced_offset})[/]"
            )
        else:
            status_lbl.update(f"[bold red][FAIL] Redrive failed: {result.error}[/]")
