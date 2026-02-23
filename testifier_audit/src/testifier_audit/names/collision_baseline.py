from __future__ import annotations

import math

import numpy as np
import pandas as pd

from testifier_audit.names.stat_tests import empirical_p_value_one_sided


def duplicate_metrics_from_counts(counts: np.ndarray) -> dict[str, float]:
    values = counts.astype(float)
    n_rows = float(values.sum())
    if n_rows <= 0:
        return {
            "n_rows": 0.0,
            "n_unique_names": 0.0,
            "duplicate_rows": 0.0,
            "duplicate_row_rate": 0.0,
            "duplicate_pairs": 0.0,
            "n_names_ge2": 0.0,
            "n_names_ge3": 0.0,
            "n_names_ge5": 0.0,
            "n_names_ge10": 0.0,
            "max_count": 0.0,
        }
    duplicate_mask = values >= 2.0
    duplicate_rows = float(values[duplicate_mask].sum())
    duplicate_pairs = float((values * np.maximum(values - 1.0, 0.0) / 2.0).sum())
    return {
        "n_rows": n_rows,
        "n_unique_names": float(len(values)),
        "duplicate_rows": duplicate_rows,
        "duplicate_row_rate": duplicate_rows / n_rows,
        "duplicate_pairs": duplicate_pairs,
        "n_names_ge2": float(np.sum(values >= 2.0)),
        "n_names_ge3": float(np.sum(values >= 3.0)),
        "n_names_ge5": float(np.sum(values >= 5.0)),
        "n_names_ge10": float(np.sum(values >= 10.0)),
        "max_count": float(values.max()) if values.size else 0.0,
    }


def _normalize_probabilities(name_counts: pd.Series) -> np.ndarray:
    counts = pd.to_numeric(name_counts, errors="coerce").fillna(0.0).astype(float)
    counts = counts[counts > 0.0]
    if counts.empty:
        return np.asarray([], dtype=float)
    total = float(counts.sum())
    if not math.isfinite(total) or total <= 0.0:
        return np.asarray([], dtype=float)
    probs = counts.to_numpy(dtype=float) / total
    probs = probs[np.isfinite(probs) & (probs > 0.0)]
    if probs.size == 0:
        return np.asarray([], dtype=float)
    probs = probs / probs.sum()
    return probs


def simulate_duplicate_null(
    *,
    n_rows: int,
    population_name_counts: pd.Series,
    draws: int,
    rng: np.random.Generator,
    chunk_size: int = 500,
) -> pd.DataFrame:
    if n_rows <= 0 or draws <= 0:
        return pd.DataFrame()
    probabilities = _normalize_probabilities(population_name_counts)
    if probabilities.size == 0:
        return pd.DataFrame()

    out_rows: list[dict[str, float]] = []
    remaining = int(draws)
    while remaining > 0:
        batch = min(int(chunk_size), remaining)
        samples = rng.multinomial(int(n_rows), probabilities, size=batch)
        values = samples.astype(float)
        duplicate_mask = values >= 2.0
        duplicate_rows = np.where(duplicate_mask, values, 0.0).sum(axis=1)
        duplicate_pairs = (values * np.maximum(values - 1.0, 0.0) / 2.0).sum(axis=1)
        n_names_ge2 = duplicate_mask.sum(axis=1)
        n_names_ge3 = (values >= 3.0).sum(axis=1)
        n_names_ge5 = (values >= 5.0).sum(axis=1)
        n_names_ge10 = (values >= 10.0).sum(axis=1)
        max_count = values.max(axis=1)
        for idx in range(batch):
            out_rows.append(
                {
                    "duplicate_rows": float(duplicate_rows[idx]),
                    "duplicate_row_rate": float(duplicate_rows[idx] / float(n_rows)),
                    "duplicate_pairs": float(duplicate_pairs[idx]),
                    "n_names_ge2": float(n_names_ge2[idx]),
                    "n_names_ge3": float(n_names_ge3[idx]),
                    "n_names_ge5": float(n_names_ge5[idx]),
                    "n_names_ge10": float(n_names_ge10[idx]),
                    "max_count": float(max_count[idx]),
                }
            )
        remaining -= batch
    return pd.DataFrame(out_rows)


def summarize_observed_vs_null(
    *,
    observed: dict[str, float],
    null_samples: pd.DataFrame,
    metric_fields: tuple[str, ...] = ("duplicate_rows", "duplicate_row_rate", "duplicate_pairs"),
) -> pd.DataFrame:
    rows: list[dict[str, float | str]] = []
    for metric in metric_fields:
        observed_value = float(observed.get(metric, 0.0))
        if null_samples.empty or metric not in null_samples.columns:
            rows.append(
                {
                    "metric": metric,
                    "observed_value": observed_value,
                    "expected_mean": 0.0,
                    "expected_p05": 0.0,
                    "expected_p50": 0.0,
                    "expected_p95": 0.0,
                    "excess_over_expected": observed_value,
                    "p_value_one_sided": 1.0,
                }
            )
            continue
        values = pd.to_numeric(null_samples[metric], errors="coerce").dropna().to_numpy(dtype=float)
        if values.size == 0:
            rows.append(
                {
                    "metric": metric,
                    "observed_value": observed_value,
                    "expected_mean": 0.0,
                    "expected_p05": 0.0,
                    "expected_p50": 0.0,
                    "expected_p95": 0.0,
                    "excess_over_expected": observed_value,
                    "p_value_one_sided": 1.0,
                }
            )
            continue
        expected_mean = float(np.mean(values))
        rows.append(
            {
                "metric": metric,
                "observed_value": observed_value,
                "expected_mean": expected_mean,
                "expected_p05": float(np.quantile(values, 0.05)),
                "expected_p50": float(np.quantile(values, 0.50)),
                "expected_p95": float(np.quantile(values, 0.95)),
                "excess_over_expected": observed_value - expected_mean,
                "p_value_one_sided": empirical_p_value_one_sided(values, observed_value),
            }
        )
    return pd.DataFrame(rows)
