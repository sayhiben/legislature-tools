from __future__ import annotations

from typing import Literal

import pandas as pd

DedupMode = Literal["raw", "exact_row_dedup", "side_by_side"]

DEDUP_MODES: tuple[DedupMode, ...] = ("raw", "exact_row_dedup", "side_by_side")
DEFAULT_DEDUP_MODE: DedupMode = "side_by_side"


def normalize_dedup_mode(mode: str | None, *, default: DedupMode = DEFAULT_DEDUP_MODE) -> DedupMode:
    if isinstance(mode, str):
        normalized = mode.strip().lower()
        if normalized in DEDUP_MODES:
            return normalized  # type: ignore[return-value]
    return default


def counts_columns_for_mode(mode: DedupMode) -> dict[str, str]:
    if mode == "exact_row_dedup":
        return {
            "n_total": "n_total_dedup",
            "n_pro": "n_pro_dedup",
            "n_con": "n_con_dedup",
            "dup_fraction": "dup_name_fraction_dedup",
        }
    return {
        "n_total": "n_total",
        "n_pro": "n_pro",
        "n_con": "n_con",
        "dup_fraction": "dup_name_fraction",
    }


def _numeric_series(
    frame: pd.DataFrame,
    column: str,
    *,
    default: float = 0.0,
    fallback: pd.Series | None = None,
) -> pd.Series:
    if column in frame.columns:
        raw = frame[column]
    elif fallback is not None:
        raw = fallback
    else:
        raw = pd.Series(default, index=frame.index, dtype=float)
    parsed = pd.to_numeric(raw, errors="coerce")
    if isinstance(parsed, pd.Series):
        return parsed.fillna(default)
    return pd.Series(parsed, index=frame.index, dtype=float).fillna(default)


def ensure_dedup_count_columns(counts: pd.DataFrame) -> pd.DataFrame:
    """Ensure minute-level count tables expose dedup-aware columns."""
    working = counts.copy()

    raw_total = _numeric_series(working, "n_total", default=0.0)
    raw_pro = _numeric_series(working, "n_pro", default=0.0)
    raw_con = _numeric_series(working, "n_con", default=0.0)
    unique_names = _numeric_series(working, "n_unique_names", default=0.0, fallback=raw_total)

    dedup_total = _numeric_series(working, "n_total_dedup", default=0.0, fallback=unique_names)
    dedup_total = dedup_total.clip(lower=0.0)
    dedup_pro = _numeric_series(working, "n_pro_dedup", default=0.0, fallback=raw_pro)
    dedup_con = _numeric_series(working, "n_con_dedup", default=0.0, fallback=raw_con)
    dedup_pro = dedup_pro.clip(lower=0.0)
    dedup_con = dedup_con.clip(lower=0.0)

    dedup_nonzero = dedup_total > 0
    dedup_dup_fraction = _numeric_series(working, "dup_name_fraction_dedup", default=0.0)
    dedup_dup_fraction = dedup_dup_fraction.where(dedup_nonzero)
    fallback_dup_fraction = (1.0 - (unique_names / dedup_total)).where(dedup_nonzero).fillna(0.0)
    dedup_dup_fraction = dedup_dup_fraction.fillna(fallback_dup_fraction).clip(lower=0.0, upper=1.0)

    working["n_total_dedup"] = dedup_total
    working["n_pro_dedup"] = dedup_pro
    working["n_con_dedup"] = dedup_con
    working["pro_rate_dedup"] = (dedup_pro / dedup_total).where(dedup_nonzero)
    working["con_rate_dedup"] = (dedup_con / dedup_total).where(dedup_nonzero)
    working["dup_name_fraction_dedup"] = dedup_dup_fraction
    working["dedup_drop_fraction"] = ((raw_total - dedup_total) / raw_total).where(raw_total > 0)
    working["dedup_multiplier"] = (raw_total / dedup_total).where(dedup_nonzero)
    return working
