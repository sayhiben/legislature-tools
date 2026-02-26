from __future__ import annotations

import math
from typing import Any

import pandas as pd

from testifier_audit.report.rendering.constants import PACIFIC_TIMEZONE_NAME

def _to_pacific_timestamp(value: pd.Timestamp) -> pd.Timestamp:
    if value.tzinfo is None:
        localized = value.tz_localize(
            PACIFIC_TIMEZONE_NAME,
            nonexistent="shift_forward",
            ambiguous="NaT",
        )
    else:
        localized = value.tz_convert(PACIFIC_TIMEZONE_NAME)
    return localized


def _serialize_value(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        converted = _to_pacific_timestamp(value)
        if pd.isna(converted):
            return None
        return converted.isoformat()
    if isinstance(value, pd.Timedelta):
        return str(value)
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, pd.Timestamp):
        converted = _to_pacific_timestamp(value)
        if pd.isna(converted):
            return None
        return converted.isoformat()
    if isinstance(value, pd.Timedelta):
        return str(value)
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (int, str, bool)) or value is None:
        return value
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value
