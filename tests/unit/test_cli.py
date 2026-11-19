"""Unit tests for the Typer CLI in EventLens."""

from __future__ import annotations

from typer.testing import CliRunner

from eventlens import __version__
from eventlens.cli import app

runner = CliRunner()


def test_cli_help() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Developer-first Terminal & Web Kafka Event Stream Debugger" in result.stdout


def test_cli_version_command() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert f"EventLens version {__version__}" in result.stdout


def test_cli_version_flags() -> None:
    for flag in ["--version", "-v"]:
        result = runner.invoke(app, [flag])
        assert result.exit_code == 0
        assert f"EventLens version {__version__}" in result.stdout
