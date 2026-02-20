from __future__ import annotations

import numpy as np
import pandas as pd

DEFAULT_LOW_POWER_MIN_TOTAL = 30


def _to_float_array(values: pd.Series | np.ndarray) -> np.ndarray:
    if isinstance(values, pd.Series):
        return pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    return np.asarray(values, dtype=float)


def wilson_interval(
    successes: pd.Series | np.ndarray,
    totals: pd.Series | np.ndarray,
    z: float = 1.96,
) -> tuple[np.ndarray, np.ndarray]:
    n = _to_float_array(totals)
    k = _to_float_array(successes)

    lower = np.full(n.shape, np.nan, dtype=float)
    upper = np.full(n.shape, np.nan, dtype=float)

    valid = np.isfinite(n) & np.isfinite(k) & (n > 0.0)
    if not np.any(valid):
        return lower, upper

    n_valid = n[valid]
    p_valid = np.clip(k[valid] / n_valid, 0.0, 1.0)
    z2 = z * z
    denom = 1.0 + (z2 / n_valid)
    center = (p_valid + (z2 / (2.0 * n_valid))) / denom
    half_width = z * np.sqrt((p_valid * (1.0 - p_valid) + (z2 / (4.0 * n_valid))) / n_valid) / denom

    lower[valid] = np.clip(center - half_width, 0.0, 1.0)
    upper[valid] = np.clip(center + half_width, 0.0, 1.0)
    return lower, upper


def wilson_half_width(
    successes: pd.Series | np.ndarray,
    totals: pd.Series | np.ndarray,
    z: float = 1.96,
) -> np.ndarray:
    lower, upper = wilson_interval(successes=successes, totals=totals, z=z)
    return (upper - lower) / 2.0


def low_power_mask(
    totals: pd.Series | np.ndarray,
    min_total: int = DEFAULT_LOW_POWER_MIN_TOTAL,
) -> np.ndarray:
    threshold = float(max(1, int(min_total)))
    n = _to_float_array(totals)
    return (~np.isfinite(n)) | (n < threshold)
