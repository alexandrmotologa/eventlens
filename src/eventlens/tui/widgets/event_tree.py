"""Collapsible tree widget for exploring JSON and Protobuf structures."""

from __future__ import annotations

from typing import Any

from rich.text import Text
from textual.widgets import Tree


class EventTree(Tree[Any]):
    """Renders nested dictionaries and lists in an interactive collapsible tree."""

    DEFAULT_CSS = """
    EventTree {
        background: #181825;
        border: solid #313244;
        padding: 1;
        scrollbar-color: #585b70;
    }
    """

    def load_data(self, label: str, data: Any) -> None:
        """Clear tree and populate with new object graph."""
        self.clear()
        self.root.set_label(Text(label, style="bold cyan"))
        self._populate_node(self.root, data)
        self.root.expand()

    def _populate_node(self, node: Any, data: Any) -> None:
        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(v, (dict, list)):
                    child = node.add(Text(f"{k}", style="bold yellow"), data=v)
                    self._populate_node(child, v)
                    child.expand()
                else:
                    val_style = "green" if isinstance(v, (int, float, bool)) else "white"
                    node.add_leaf(Text.assemble((f"{k}: ", "dim"), (f"{v}", val_style)))
        elif isinstance(data, list):
            for idx, item in enumerate(data):
                if isinstance(item, (dict, list)):
                    child = node.add(Text(f"[{idx}]", style="bold magenta"), data=item)
                    self._populate_node(child, item)
                    child.expand()
                else:
                    node.add_leaf(Text.assemble((f"[{idx}]: ", "dim"), (f"{item}", "white")))
        else:
            node.add_leaf(Text(str(data), style="white"))
