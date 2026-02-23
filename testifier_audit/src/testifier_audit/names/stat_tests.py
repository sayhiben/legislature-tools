from __future__ import annotations

import math

import numpy as np
import pandas as pd
from scipy.stats import fisher_exact

from testifier_audit.proportion_stats import wilson_interval


def benjamini_hochberg(p_values: pd.Series | list[float]) -> pd.Series:
    if isinstance(p_values, list):
        p_series = pd.Series(p_values, dtype=float)
    else:
        p_series = pd.to_numeric(p_values, errors="coerce").astype(float)
    if p_series.empty:
        return pd.Series(dtype=float)

    order = p_series.sort_values(kind="mergesort").index
    ordered = p_series.loc[order].to_numpy(dtype=float)
    n = float(len(ordered))
    q_values = np.full(len(ordered), np.nan, dtype=float)
    running = 1.0
    for i in range(len(ordered) - 1, -1, -1):
        rank = float(i + 1)
        p = ordered[i]
        if not np.isfinite(p):
            continue
        candidate = min(1.0, p * (n / rank))
        running = min(running, candidate)
        q_values[i] = running
    out = pd.Series(index=order, data=q_values, dtype=float).reindex(p_series.index)
    return out


def empirical_p_value_one_sided(null_values: np.ndarray, observed: float) -> float:
    if null_values.size == 0 or not math.isfinite(observed):
        return 1.0
    valid = null_values[np.isfinite(null_values)]
    if valid.size == 0:
        return 1.0
    ge_count = int(np.sum(valid >= observed))
    return float((ge_count + 1) / (valid.size + 1))


def bootstrap_rate_difference(
    *,
    successes_a: int,
    total_a: int,
    successes_b: int,
    total_b: int,
    n_boot: int,
    rng: np.random.Generator,
) -> tuple[float, float, float]:
    if total_a <= 0 or total_b <= 0:
        return 0.0, 0.0, 0.0
    p_a = successes_a / total_a
    p_b = successes_b / total_b
    observed = p_a - p_b
    if n_boot <= 0:
        return observed, observed, observed

    samples_a = rng.binomial(total_a, p_a, size=n_boot) / float(total_a)
    samples_b = rng.binomial(total_b, p_b, size=n_boot) / float(total_b)
    deltas = samples_a - samples_b
    return (
        observed,
        float(np.quantile(deltas, 0.025)),
        float(np.quantile(deltas, 0.975)),
    )


def fisher_pairwise_rate_test(
    *,
    successes_left: int,
    total_left: int,
    successes_right: int,
    total_right: int,
) -> dict[str, float]:
    failures_left = max(int(total_left) - int(successes_left), 0)
    failures_right = max(int(total_right) - int(successes_right), 0)
    odds_ratio, p_value = fisher_exact(
        [[int(successes_left), failures_left], [int(successes_right), failures_right]]
    )
    left_low, left_high = wilson_interval(
        successes=pd.Series([int(successes_left)]),
        totals=pd.Series([int(total_left)]),
    )
    right_low, right_high = wilson_interval(
        successes=pd.Series([int(successes_right)]),
        totals=pd.Series([int(total_right)]),
    )
    left_rate = (float(successes_left) / float(total_left)) if total_left else 0.0
    right_rate = (float(successes_right) / float(total_right)) if total_right else 0.0
    return {
        "left_rate": left_rate,
        "right_rate": right_rate,
        "rate_difference": left_rate - right_rate,
        "odds_ratio": float(odds_ratio) if np.isfinite(odds_ratio) else 0.0,
        "p_value": float(p_value) if np.isfinite(p_value) else 1.0,
        "left_wilson_low": float(left_low[0]),
        "left_wilson_high": float(left_high[0]),
        "right_wilson_low": float(right_low[0]),
        "right_wilson_high": float(right_high[0]),
    }
