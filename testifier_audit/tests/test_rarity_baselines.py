from __future__ import annotations

from pathlib import Path

import pandas as pd
from typer.testing import CliRunner
import yaml

from testifier_audit.cli import app
from testifier_audit.io.rarity_baselines import normalize_frequency_baseline


def test_normalize_frequency_baseline_aggregates_names() -> None:
    raw = pd.DataFrame(
        {
            "First Name": ["Jane", "JANE", "John", "X Æ A-12", ""],
            "Count": [100, 25, 125, 2, 999],
        }
    )
    normalized, used_name_col, used_value_col = normalize_frequency_baseline(
        table=raw,
        name_column="First Name",
        value_column="Count",
        min_weight=1.0,
    )

    assert used_name_col == "First Name"
    assert used_value_col == "Count"
    assert list(normalized.columns) == ["name", "count", "probability"]
    assert "JANE" in set(normalized["name"])
    assert "JOHN" in set(normalized["name"])
    assert abs(float(normalized["probability"].sum()) - 1.0) < 1e-9
    jane_count = float(normalized.loc[normalized["name"] == "JANE", "count"].iloc[0])
    assert jane_count == 125.0


def test_prepare_rarity_baselines_command_writes_outputs(tmp_path: Path) -> None:
    first_raw = tmp_path / "first_raw.csv"
    first_raw.write_text(
        "\n".join(
            [
                "name,count",
                "Jane,100",
                "John,150",
            ]
        ),
        encoding="utf-8",
    )
    last_raw = tmp_path / "last_raw.csv"
    last_raw.write_text(
        "\n".join(
            [
                "surname,frequency",
                "Doe,200",
                "Smith,300",
            ]
        ),
        encoding="utf-8",
    )

    out_dir = tmp_path / "lookups"
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "prepare-rarity-baselines",
            "--out-dir",
            str(out_dir),
            "--first-raw",
            str(first_raw),
            "--last-raw",
            str(last_raw),
        ],
    )

    assert result.exit_code == 0, result.stdout
    first_output = out_dir / "first_name_frequency.csv"
    last_output = out_dir / "last_name_frequency.csv"
    assert first_output.exists()
    assert last_output.exists()
    first_table = pd.read_csv(first_output)
    last_table = pd.read_csv(last_output)
    assert list(first_table.columns) == ["name", "count", "probability"]
    assert list(last_table.columns) == ["name", "count", "probability"]
    assert abs(float(first_table["probability"].sum()) - 1.0) < 1e-9
    assert abs(float(last_table["probability"].sum()) - 1.0) < 1e-9


def test_prepare_rarity_baselines_can_update_config(tmp_path: Path) -> None:
    first_raw = tmp_path / "first_raw.csv"
    first_raw.write_text("name,count\nJane,100\nJohn,150\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "columns": {
                    "id": "id",
                    "name": "name",
                    "organization": "organization",
                    "position": "position",
                    "time_signed_in": "time_signed_in",
                }
            }
        ),
        encoding="utf-8",
    )

    out_dir = tmp_path / "lookups"
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "prepare-rarity-baselines",
            "--out-dir",
            str(out_dir),
            "--first-raw",
            str(first_raw),
            "--write-config",
            str(config_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert cfg["rarity"]["enabled"] is True
    assert cfg["rarity"]["first_name_frequency_path"] == "lookups/first_name_frequency.csv"


def test_prepare_rarity_baselines_profile_applies_default_min_weight(tmp_path: Path) -> None:
    first_raw = tmp_path / "first_raw.csv"
    first_raw.write_text(
        "\n".join(
            [
                "name,count",
                "Jane,3",
                "John,10",
            ]
        ),
        encoding="utf-8",
    )

    out_dir = tmp_path / "lookups"
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "prepare-rarity-baselines",
            "--out-dir",
            str(out_dir),
            "--first-raw",
            str(first_raw),
            "--first-profile",
            "ssa_first",
            "--min-weight",
            "0",
        ],
    )

    assert result.exit_code == 0, result.stdout
    table = pd.read_csv(out_dir / "first_name_frequency.csv")
    assert "JANE" not in set(table["name"])
    assert "JOHN" in set(table["name"])
