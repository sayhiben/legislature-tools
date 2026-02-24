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


def _resolve_source_column(
    *,
    df: pd.DataFrame,
    primary: str,
    fallback: str | None = None,
) -> str | None:
    if primary in df.columns:
        return primary
    if fallback is not None and fallback in df.columns:
        return fallback
    return None


def normalize_columns(df: pd.DataFrame, columns: ColumnsConfig) -> pd.DataFrame:
    """Rename source columns to canonical names used by detectors/pipeline."""
    resolved_sources = {
        CanonicalColumns.id: _resolve_source_column(df=df, primary=columns.id),
        CanonicalColumns.name: _resolve_source_column(df=df, primary=columns.name),
        CanonicalColumns.organization: _resolve_source_column(df=df, primary=columns.organization),
        CanonicalColumns.position: _resolve_source_column(df=df, primary=columns.position),
        CanonicalColumns.time_signed_in: _resolve_source_column(
            df=df,
            primary=columns.time_signed_in,
        ),
    }
    missing = [canonical for canonical, source in resolved_sources.items() if source is None]
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(f"Missing required columns in CSV: {missing_str}")

    rename_map = {source: canonical for canonical, source in resolved_sources.items() if source}
    return df.rename(columns=rename_map)
