from __future__ import annotations

from typer.testing import CliRunner

from testifier_audit.cli import app


def test_cli_help() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "run-all" in result.stdout
    assert "prepare-rarity-baselines" in result.stdout
