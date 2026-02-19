from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from testifier_audit.config import ColumnsConfig


@dataclass(frozen=True)
class CanonicalColumns:
    id: str = "id"
    name: str = "name"
    organization: str = "organization"
    position: str = "position"
    time_signed_in: str = "time_signed_in"


def normalize_columns(df: pd.DataFrame, columns: ColumnsConfig) -> pd.DataFrame:
    """Rename source columns to canonical names used by detectors/pipeline."""
    rename_map = {
        columns.id: CanonicalColumns.id,
        columns.name: CanonicalColumns.name,
        columns.organization: CanonicalColumns.organization,
        columns.position: CanonicalColumns.position,
        columns.time_signed_in: CanonicalColumns.time_signed_in,
    }
    missing = [source for source in rename_map if source not in df.columns]
    if missing:
        missing_str = ", ".join(missing)
        raise ValueError(f"Missing required columns in CSV: {missing_str}")
    return df.rename(columns=rename_map)
