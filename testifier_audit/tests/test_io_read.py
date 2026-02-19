from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from testifier_audit.config import AppConfig
from testifier_audit.io import read as read_module
from testifier_audit.io.read import load_records, load_table


def _config_with_custom_columns() -> AppConfig:
    return AppConfig.model_validate(
        {
            "columns": {
                "id": "Count",
                "name": "Name",
                "organization": "Organization",
                "position": "Position",
                "time_signed_in": "Time Signed In",
            }
        }
    )


def test_load_records_normalizes_source_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "records.csv"
    csv_path.write_text(
        "\n".join(
            [
                "Count,Name,Organization,Position,Time Signed In",
                '1,"Doe, Jane",,Pro,2/3/2026 5:07 PM',
            ]
        ),
        encoding="utf-8",
    )

    loaded = load_records(csv_path=csv_path, config=_config_with_custom_columns())
    assert list(loaded.columns) == ["id", "name", "organization", "position", "time_signed_in"]
    assert loaded.loc[0, "name"] == "Doe, Jane"


def test_load_records_raises_when_normalized_output_is_missing_required_column(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "records.csv"
    csv_path.write_text(
        "Count,Name,Organization,Position,Time Signed In\n1,Doe,Org,Pro,2/3/2026 5:07 PM\n",
        encoding="utf-8",
    )

    def _bad_normalize(df: pd.DataFrame, columns: object) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "id": [1],
                "name": ["Doe"],
                "organization": ["Org"],
                "position": ["Pro"],
            }
        )

    monkeypatch.setattr(read_module, "normalize_columns", _bad_normalize)

    with pytest.raises(ValueError, match="time_signed_in"):
        load_records(csv_path=csv_path, config=_config_with_custom_columns())


def test_load_table_supports_csv_and_parquet_and_rejects_unknown_types(tmp_path: Path) -> None:
    csv_path = tmp_path / "table.csv"
    parquet_path = tmp_path / "table.parquet"
    text_path = tmp_path / "table.txt"

    frame = pd.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    frame.to_csv(csv_path, index=False)
    frame.to_parquet(parquet_path, index=False)
    text_path.write_text("not-a-table", encoding="utf-8")

    loaded_csv = load_table(csv_path)
    loaded_parquet = load_table(parquet_path)

    assert list(loaded_csv.columns) == ["x", "y"]
    assert list(loaded_parquet.columns) == ["x", "y"]
    with pytest.raises(ValueError, match="Unsupported table file type"):
        load_table(text_path)
