from __future__ import annotations

from pathlib import Path

import pandas as pd

from testifier_audit.config import AppConfig
from testifier_audit.io.schema import normalize_columns
from testifier_audit.io.submissions_postgres import load_submission_records_from_postgres

REQUIRED_COLUMNS = ["id", "name", "organization", "position", "time_signed_in"]


def _validate_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    for column in REQUIRED_COLUMNS:
        if column not in df.columns:
            raise ValueError(f"Normalized data missing column: {column}")
    return df


def load_records(csv_path: Path | None, config: AppConfig) -> pd.DataFrame:
    """Load records from CSV or PostgreSQL and return canonical columns."""
    if config.input.mode == "postgres":
        if not config.input.db_url:
            raise ValueError("input.db_url must be set when input.mode is 'postgres'")
        frame = load_submission_records_from_postgres(
            db_url=config.input.db_url,
            table_name=config.input.submissions_table,
            source_file=config.input.source_file,
        )
        return _validate_required_columns(frame)

    if csv_path is None:
        raise ValueError("csv_path is required when input.mode is 'csv'")

    # utf-8-sig strips BOM-prefixed headers commonly found in exported CSV files.
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    normalized = normalize_columns(df=df, columns=config.columns)
    return _validate_required_columns(normalized)


def load_table(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    if path.suffix == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported table file type: {path.suffix}")
