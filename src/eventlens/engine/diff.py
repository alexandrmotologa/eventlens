"""Structural payload diffing engine for comparing Kafka messages."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, Field, computed_field
from rich.table import Table


class PayloadDiff(BaseModel):
    """Represents differences between two payloads."""

    added: dict[str, Any] = Field(default_factory=dict)
    removed: dict[str, Any] = Field(default_factory=dict)
    changed: dict[str, tuple[Any, Any]] = Field(default_factory=dict)
    identical: bool = True

    @computed_field
    @property
    def added_count(self) -> int:
        return len(self.added)

    @computed_field
    @property
    def removed_count(self) -> int:
        return len(self.removed)

    @computed_field
    @property
    def modified_count(self) -> int:
        return len(self.changed)

    @computed_field
    @property
    def items(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for k, v in self.removed.items():
            rows.append({"path": k, "diff_type": "removed", "old_value": v, "new_value": None})
        for k, v in self.added.items():
            rows.append({"path": k, "diff_type": "added", "old_value": None, "new_value": v})
        for k, (v1, v2) in self.changed.items():
            rows.append({"path": k, "diff_type": "modified", "old_value": v1, "new_value": v2})
        return rows

    def to_rich_table(self, title_a: str = "Message A", title_b: str = "Message B") -> Table:
        """Render diff as a formatted Rich table."""
        table = Table(title=f"Payload Comparison: {title_a} vs {title_b}", expand=True)
        table.add_column("Property Path", width=25)
        table.add_column("Change", justify="center", width=12)
        table.add_column(title_a, ratio=1)
        table.add_column(title_b, ratio=1)

        if self.identical:
            table.add_row("[dim]All properties[/]", "[green]IDENTICAL[/]", "[dim]-[/]", "[dim]-[/]")
            return table

        for k, v in self.removed.items():
            table.add_row(f"[bold red]{k}[/]", "[red]- REMOVED[/]", str(v), "[dim]-[/]")

        for k, v in self.added.items():
            table.add_row(f"[bold green]{k}[/]", "[green]+ ADDED[/]", "[dim]-[/]", str(v))

        for k, (v1, v2) in self.changed.items():
            table.add_row(f"[bold yellow]{k}[/]", "[yellow]~ MODIFIED[/]", f"[red]{v1}[/]", f"[green]{v2}[/]")

        return table


def compare_payloads(
    payload_a: Any,
    payload_b: Any,
    prefix: str = "",
) -> PayloadDiff:
    """Recursively compare two dictionaries or JSON objects."""
    # Convert strings to dicts if valid JSON
    if isinstance(payload_a, str):
        try:
            payload_a = json.loads(payload_a)
        except Exception:
            pass

    if isinstance(payload_b, str):
        try:
            payload_b = json.loads(payload_b)
        except Exception:
            pass

    if not isinstance(payload_a, dict) or not isinstance(payload_b, dict):
        if payload_a == payload_b:
            return PayloadDiff(identical=True)
        return PayloadDiff(
            changed={prefix or "value": (payload_a, payload_b)},
            identical=False,
        )

    added: dict[str, Any] = {}
    removed: dict[str, Any] = {}
    changed: dict[str, tuple[Any, Any]] = {}

    keys_a = set(payload_a.keys())
    keys_b = set(payload_b.keys())

    for k in keys_a - keys_b:
        p = f"{prefix}.{k}" if prefix else k
        removed[p] = payload_a[k]

    for k in keys_b - keys_a:
        p = f"{prefix}.{k}" if prefix else k
        added[p] = payload_b[k]

    for k in keys_a & keys_b:
        p = f"{prefix}.{k}" if prefix else k
        val_a = payload_a[k]
        val_b = payload_b[k]

        if isinstance(val_a, dict) and isinstance(val_b, dict):
            nested = compare_payloads(val_a, val_b, prefix=p)
            added.update(nested.added)
            removed.update(nested.removed)
            changed.update(nested.changed)
        elif val_a != val_b:
            changed[p] = (val_a, val_b)

    identical = not bool(added or removed or changed)
    return PayloadDiff(added=added, removed=removed, changed=changed, identical=identical)
