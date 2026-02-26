from __future__ import annotations

import math
import re
from typing import Any

import pandas as pd

from testifier_audit.report.rendering.constants import _REPORT_MATCH_MODE_ALIASES
from testifier_audit.report.rendering.serialization import _serialize_value

def _slugify_path_component(value: Any) -> str:
    normalized = re.sub(r"[^a-z0-9_-]+", "-", str(value or "").strip().lower()).strip("-")
    return normalized or "analysis"


def _coerce_bucket_minutes(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return int(value) if value > 0 else None
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        rounded = int(round(value))
        return rounded if rounded > 0 else None
    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            return None
        try:
            parsed = float(trimmed)
        except ValueError:
            return None
        if not math.isfinite(parsed):
            return None
        rounded = int(round(parsed))
        return rounded if rounded > 0 else None
    return None


def _canonical_name_to_display_name(value: Any) -> str:
    canonical_name = str(value or "").strip()
    if not canonical_name:
        return ""
    if "|" not in canonical_name:
        return canonical_name
    last_name, first_name = canonical_name.split("|", 1)
    display_name = f"{last_name.strip()}, {first_name.strip()}".strip(", ").strip()
    return display_name if display_name else canonical_name


def _records_from_frame(
    frame: pd.DataFrame,
    columns: list[str],
    max_rows: int | None = None,
) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    selected = [column for column in columns if column in frame.columns]
    if not selected:
        return []
    working = frame[selected].copy()
    if max_rows is not None:
        working = working.head(max_rows)
    for column in working.columns:
        working[column] = working[column].map(_serialize_value)
    return working.to_dict(orient="records")


def _table_key(detector: str, table_name: str) -> str:
    return f"{detector}.{table_name}"


def _extract_bucket_options(*frames: pd.DataFrame) -> list[int]:
    options: set[int] = set()
    for frame in frames:
        if frame.empty or "bucket_minutes" not in frame.columns:
            continue
        numeric = pd.to_numeric(frame["bucket_minutes"], errors="coerce").dropna()
        for value in numeric.astype(int).tolist():
            if value > 0:
                options.add(int(value))
    return sorted(options)


def _with_expected_columns(frame: pd.DataFrame, expected: list[str]) -> pd.DataFrame:
    working = frame.copy()
    for column in expected:
        if column not in working.columns:
            working[column] = pd.NA
    return working


def _normalize_report_match_mode(value: Any, *, default: str = "strict") -> str:
    if value is None:
        return default
    if isinstance(value, str):
        raw = value.strip().lower()
    else:
        try:
            if pd.isna(value):
                return default
        except Exception:
            pass
        raw = str(value).strip().lower()
    if not raw:
        return default
    return _REPORT_MATCH_MODE_ALIASES.get(raw, raw if raw in {"strict", "loose"} else default)
