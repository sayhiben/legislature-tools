from __future__ import annotations

from pathlib import Path

import pandas as pd

from testifier_audit.config import AppConfig
from testifier_audit.io.schema import normalize_columns


REQUIRED_COLUMNS = ["id", "name", "organization", "position", "time_signed_in"]


def load_records(csv_path: Path, config: AppConfig) -> pd.DataFrame:
    """Load CSV and return normalized canonical columns."""
    # utf-8-sig strips BOM-prefixed headers commonly found in exported CSV files.
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    normalized = normalize_columns(df=df, columns=config.columns)
    for column in REQUIRED_COLUMNS:
        if column not in normalized.columns:
            raise ValueError(f"Normalized data missing column: {column}")
    return normalized


def load_table(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    if path.suffix == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported table file type: {path.suffix}")
