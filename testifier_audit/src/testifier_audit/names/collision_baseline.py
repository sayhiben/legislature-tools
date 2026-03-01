from __future__ import annotations

import math
import os
from concurrent.futures import ProcessPoolExecutor
from time import perf_counter
from typing import Literal

import numpy as np
import pandas as pd
from scipy.special import gammaln

from testifier_audit.names.stat_tests import empirical_p_value_one_sided
from testifier_audit.profiling import record_runtime_counter, record_runtime_timing

CollisionMetric = Literal["pairs", "excess_rows", "repeated_group_rows"]

COLLISION_METRICS: tuple[CollisionMetric, ...] = (
    "pairs",
    "excess_rows",
    "repeated_group_rows",
)

_COLLISION_NULL_PARALLEL_MIN_DRAWS = 512
_COLLISION_NULL_PARALLEL_MIN_ROWS = 200
_COLLISION_NULL_PARALLEL_CHUNK_SIZE = 128
_COLLISION_NULL_PARALLEL_MAX_WORKERS = 8
_COLLISION_NULL_MIN_DRAWS = 128
_COLLISION_NULL_TARGET_P_MCSE = 0.01
_COLLISION_NULL_DECISION_P_THRESHOLD = 0.05
_COLLISION_NULL_DECISION_CONFIDENCE = 0.95

_CONFIDENCE_Z = {
    0.90: 1.6448536269514722,
    0.95: 1.959963984540054,
    0.99: 2.5758293035489004,
}


def _safe_float(value: float) -> float:
    if not math.isfinite(value):
        return 0.0
    return float(value)


def _clamp_non_negative(value: float) -> float:
    if not math.isfinite(value):
        return 0.0
    return float(max(value, 0.0))


def _safe_int(value: float) -> int:
    if not math.isfinite(value):
        return 0
    return int(max(int(value), 0))


def _normalized_probability_array(probabilities: np.ndarray) -> np.ndarray:
    probs = np.asarray(probabilities, dtype=float).reshape(-1)
    probs = probs[np.isfinite(probs) & (probs > 0.0)]
    if probs.size == 0:
        return np.asarray([], dtype=float)
    total = float(probs.sum())
    if not math.isfinite(total) or total <= 0.0:
        return np.asarray([], dtype=float)
    return probs / total


def _normalize_metric_list(metrics: tuple[str, ...] | list[str] | None) -> tuple[CollisionMetric, ...]:
    if not metrics:
        return COLLISION_METRICS
    out: list[CollisionMetric] = []
    for raw in metrics:
        metric = str(raw or "").strip().lower()
        if metric not in COLLISION_METRICS:
            continue
        if metric not in out:
            out.append(metric)  # type: ignore[arg-type]
    return tuple(out) if out else COLLISION_METRICS


def _log_choose(n: int | np.ndarray, k: int | np.ndarray) -> np.ndarray:
    n_array = np.asarray(n, dtype=float)
    k_array = np.asarray(k, dtype=float)
    out = gammaln(n_array + 1.0) - gammaln(k_array + 1.0) - gammaln(n_array - k_array + 1.0)
    invalid = (k_array < 0.0) | (k_array > n_array)
    out = np.asarray(out, dtype=float)
    if out.ndim == 0:
        if bool(np.asarray(invalid).item()):
            return np.asarray(-np.inf, dtype=float)
        return out
    out[np.asarray(invalid, dtype=bool)] = -np.inf
    return out


def histogram_from_name_counts(name_counts: pd.Series) -> pd.DataFrame:
    counts = pd.to_numeric(name_counts, errors="coerce").dropna().astype(int)
    counts = counts[counts > 0]
    if counts.empty:
        return pd.DataFrame(columns=["name_count", "n_keys", "N"])
    hist = (
        counts.value_counts(dropna=False)
        .rename_axis("name_count")
        .rename("n_keys")
        .reset_index()
        .sort_values("name_count")
    )
    hist["name_count"] = pd.to_numeric(hist["name_count"], errors="coerce").fillna(0).astype(int)
    hist["n_keys"] = pd.to_numeric(hist["n_keys"], errors="coerce").fillna(0).astype(int)
    hist = hist[(hist["name_count"] > 0) & (hist["n_keys"] > 0)].copy()
    n_population = int((hist["name_count"] * hist["n_keys"]).sum())
    hist["N"] = n_population
    return hist.reset_index(drop=True)


def _extract_histogram_arrays(
    histogram: pd.DataFrame,
    *,
    n_population: int | None = None,
) -> tuple[np.ndarray, np.ndarray, int]:
    if histogram.empty:
        return np.asarray([], dtype=float), np.asarray([], dtype=float), 0
    required = {"name_count", "n_keys"}
    missing = required - set(histogram.columns)
    if missing:
        raise ValueError(f"Histogram missing required columns: {sorted(missing)}")
    name_count = pd.to_numeric(histogram["name_count"], errors="coerce").fillna(0).to_numpy(dtype=float)
    n_keys = pd.to_numeric(histogram["n_keys"], errors="coerce").fillna(0).to_numpy(dtype=float)
    valid = (name_count > 0.0) & (n_keys > 0.0) & np.isfinite(name_count) & np.isfinite(n_keys)
    if not np.any(valid):
        return np.asarray([], dtype=float), np.asarray([], dtype=float), 0
    name_count = name_count[valid]
    n_keys = n_keys[valid]
    if n_population is None:
        if "N" in histogram.columns:
            candidate = pd.to_numeric(histogram["N"], errors="coerce").dropna()
            if not candidate.empty:
                n_population = int(max(candidate.max(), 0))
    if n_population is None or int(n_population) <= 0:
        n_population = int(np.sum(name_count * n_keys))
    return name_count, n_keys, int(max(int(n_population), 0))


def collision_metrics_from_counts(counts: np.ndarray) -> dict[str, float]:
    values = counts.astype(float)
    values = values[np.isfinite(values) & (values >= 0.0)]
    n_rows = float(values.sum())
    n_unique = float(values.size)
    if n_rows <= 0.0:
        return {
            "n_rows": 0.0,
            "n_unique_names": 0.0,
            "pairs": 0.0,
            "excess_rows": 0.0,
            "repeated_group_rows": 0.0,
        }
    pairs = float((values * np.maximum(values - 1.0, 0.0) / 2.0).sum())
    excess_rows = float(np.maximum(values - 1.0, 0.0).sum())
    repeated_group_rows = float(values[values >= 2.0].sum())
    return {
        "n_rows": n_rows,
        "n_unique_names": n_unique,
        "pairs": _clamp_non_negative(pairs),
        "excess_rows": _clamp_non_negative(excess_rows),
        "repeated_group_rows": _clamp_non_negative(repeated_group_rows),
    }


def expected_collision_metrics(
    *,
    n_rows: int,
    histogram: pd.DataFrame,
    baseline_model: Literal["multinomial", "hypergeometric"] = "multinomial",
    n_population: int | None = None,
) -> dict[str, float]:
    n = int(max(int(n_rows), 0))
    name_count, n_keys, n_pop = _extract_histogram_arrays(histogram, n_population=n_population)
    if n <= 0 or n_pop <= 0 or name_count.size == 0:
        return {"pairs": 0.0, "excess_rows": 0.0, "repeated_group_rows": 0.0}

    if baseline_model == "hypergeometric" and n > n_pop:
        raise ValueError(f"Hypergeometric baseline requires n <= N (got n={n}, N={n_pop}).")

    p = name_count / float(n_pop)
    if baseline_model == "multinomial":
        if n <= 1:
            return {"pairs": 0.0, "excess_rows": 0.0, "repeated_group_rows": 0.0}
        prob_present = 1.0 - np.power(1.0 - p, n)
        expected_unique = float(np.sum(n_keys * prob_present))
        prob_singleton = n * p * np.power(1.0 - p, n - 1)
        expected_singletons = float(np.sum(n_keys * prob_singleton))
        pair_probability = float(np.sum(n_keys * (p**2)))
        expected_pairs = float((n * (n - 1) / 2.0) * pair_probability)
        expected_excess = float(n - expected_unique)
        expected_repeated = float(n - expected_singletons)
    else:
        if n <= 1:
            return {"pairs": 0.0, "excess_rows": 0.0, "repeated_group_rows": 0.0}
        log_den = _log_choose(n_pop, n)
        log_p0 = _log_choose((n_pop - name_count).astype(int), n) - log_den
        p0 = np.exp(np.clip(log_p0, a_min=-745.0, a_max=0.0))
        log_p1 = np.log(np.clip(name_count, 1.0, None)) + _log_choose(
            (n_pop - name_count).astype(int), n - 1
        ) - log_den
        p1 = np.exp(np.clip(log_p1, a_min=-745.0, a_max=0.0))
        expected_unique = float(np.sum(n_keys * (1.0 - p0)))
        expected_singletons = float(np.sum(n_keys * p1))
        if n_pop >= 2:
            same_name_prob = float(
                np.sum(n_keys * (name_count * np.maximum(name_count - 1.0, 0.0)))
                / float(n_pop * (n_pop - 1))
            )
        else:
            same_name_prob = 0.0
        expected_pairs = float((n * (n - 1) / 2.0) * same_name_prob)
        expected_excess = float(n - expected_unique)
        expected_repeated = float(n - expected_singletons)

    if n <= 1:
        expected_pairs = 0.0
        expected_excess = 0.0
        expected_repeated = 0.0

    expected_pairs = _clamp_non_negative(expected_pairs)
    expected_excess = _clamp_non_negative(expected_excess)
    expected_repeated = _clamp_non_negative(expected_repeated)

    expected_excess = min(expected_excess, float(max(n - 1, 0)))
    expected_repeated = min(expected_repeated, float(n))

    return {
        "pairs": expected_pairs,
        "excess_rows": expected_excess,
        "repeated_group_rows": expected_repeated,
    }


def expected_collision_metrics_from_probabilities(
    *,
    n_rows: int,
    probabilities: np.ndarray,
) -> dict[str, float]:
    n = int(max(int(n_rows), 0))
    p = _normalized_probability_array(probabilities)
    if n <= 0 or p.size == 0:
        return {"pairs": 0.0, "excess_rows": 0.0, "repeated_group_rows": 0.0}
    if n <= 1:
        return {"pairs": 0.0, "excess_rows": 0.0, "repeated_group_rows": 0.0}

    expected_pairs = float((n * (n - 1) / 2.0) * np.sum(p**2))
    expected_unique = float(np.sum(1.0 - np.power(1.0 - p, n)))
    expected_excess = float(n - expected_unique)
    expected_singletons = float(np.sum(n * p * np.power(1.0 - p, n - 1)))
    expected_repeated = float(n - expected_singletons)

    expected_pairs = _clamp_non_negative(expected_pairs)
    expected_excess = _clamp_non_negative(expected_excess)
    expected_repeated = _clamp_non_negative(expected_repeated)
    expected_excess = min(expected_excess, float(max(n - 1, 0)))
    expected_repeated = min(expected_repeated, float(n))
    return {
        "pairs": expected_pairs,
        "excess_rows": expected_excess,
        "repeated_group_rows": expected_repeated,
    }


def histogram_from_probabilities(
    *,
    probabilities: np.ndarray,
    n_population: int,
    max_classes: int = 25_000,
) -> pd.DataFrame:
    p = _normalized_probability_array(probabilities)
    n_pop = int(max(int(n_population), 0))
    if p.size == 0 or n_pop <= 0:
        return pd.DataFrame(columns=["name_count", "n_keys", "N"])

    expected_counts = p * float(n_pop)
    rounded_counts = np.maximum(np.rint(expected_counts).astype(int), 0)
    if rounded_counts.sum() == 0:
        return pd.DataFrame(columns=["name_count", "n_keys", "N"])

    class_frame = (
        pd.Series(rounded_counts, name="name_count")
        .value_counts(dropna=False)
        .rename_axis("name_count")
        .rename("n_keys")
        .reset_index()
    )
    class_frame["name_count"] = pd.to_numeric(class_frame["name_count"], errors="coerce").fillna(0).astype(int)
    class_frame["n_keys"] = pd.to_numeric(class_frame["n_keys"], errors="coerce").fillna(0).astype(int)
    class_frame = class_frame[(class_frame["name_count"] > 0) & (class_frame["n_keys"] > 0)].copy()
    if class_frame.empty:
        return pd.DataFrame(columns=["name_count", "n_keys", "N"])
    if len(class_frame) > int(max_classes):
        class_frame = class_frame.sort_values("name_count").tail(int(max_classes)).copy()
    n_population_hist = int((class_frame["name_count"] * class_frame["n_keys"]).sum())
    if n_population_hist <= 0:
        return pd.DataFrame(columns=["name_count", "n_keys", "N"])
    class_frame["N"] = n_population_hist
    return class_frame.sort_values("name_count").reset_index(drop=True)


def _simulate_one_draw_from_histogram(
    *,
    class_draw_counts: np.ndarray,
    n_keys: np.ndarray,
    rng: np.random.Generator,
) -> tuple[float, float, float]:
    if class_draw_counts.size == 0:
        return 0.0, 0.0, 0.0

    pairs = 0.0
    excess_rows = 0.0
    repeated_rows = 0.0
    for draw_count, n_key_bucket in zip(class_draw_counts, n_keys, strict=False):
        m = int(draw_count)
        h = int(n_key_bucket)
        # A class needs at least two sampled rows to contribute to any collision metric.
        if m <= 1 or h <= 0:
            continue
        if h == 1:
            count = float(m)
            pairs += count * (count - 1.0) / 2.0
            excess_rows += count - 1.0
            repeated_rows += count
            continue
        if m == 2:
            if rng.random() < (1.0 / float(h)):
                pairs += 1.0
                excess_rows += 1.0
                repeated_rows += 2.0
            continue
        if m == 3:
            h_float = float(h)
            p_all_same = 1.0 / (h_float * h_float)
            if h >= 3:
                p_all_distinct = ((h_float - 1.0) * (h_float - 2.0)) / (h_float * h_float)
            else:
                p_all_distinct = 0.0
            p_one_pair = max(0.0, min(1.0 - p_all_same - p_all_distinct, 1.0))
            u = float(rng.random())
            if u < p_all_same:
                pairs += 3.0
                excess_rows += 2.0
                repeated_rows += 3.0
            elif u < (p_all_same + p_one_pair):
                pairs += 1.0
                excess_rows += 1.0
                repeated_rows += 2.0
            continue
        key_ids = rng.integers(0, h, size=m, endpoint=False)
        if h <= 10_000:
            occupancy = np.bincount(key_ids, minlength=h)
            occupancy = occupancy[occupancy > 0].astype(float, copy=False)
        else:
            _, occupancy = np.unique(key_ids, return_counts=True)
            occupancy = occupancy.astype(float)
        over_one = np.maximum(occupancy - 1.0, 0.0)
        pairs += float((occupancy * over_one / 2.0).sum())
        excess_rows += float(over_one.sum())
        repeated_rows += float(occupancy[occupancy >= 2.0].sum())
    return _clamp_non_negative(pairs), _clamp_non_negative(excess_rows), _clamp_non_negative(repeated_rows)


def _draw_chunk_plan(total_draws: int, chunk_size: int) -> list[int]:
    draws = int(max(int(total_draws), 0))
    if draws <= 0:
        return []
    normalized_chunk_size = int(max(int(chunk_size), 1))
    plan: list[int] = []
    remaining = draws
    while remaining > 0:
        draw_count = min(normalized_chunk_size, remaining)
        plan.append(int(draw_count))
        remaining -= draw_count
    return plan


def _z_for_confidence(confidence_level: float) -> float:
    if not math.isfinite(confidence_level):
        return _CONFIDENCE_Z[0.95]
    level = float(min(max(confidence_level, 0.0), 1.0))
    return float(_CONFIDENCE_Z.get(level, _CONFIDENCE_Z[0.95]))


def _wilson_interval(
    *,
    successes: int,
    trials: int,
    confidence_level: float,
) -> tuple[float, float]:
    n = int(max(int(trials), 0))
    if n <= 0:
        return (0.0, 1.0)
    k = int(min(max(int(successes), 0), n))
    p = float(k) / float(n)
    z = _z_for_confidence(confidence_level)
    z2_over_n = (z * z) / float(n)
    denominator = 1.0 + z2_over_n
    center = (p + (z2_over_n / 2.0)) / denominator
    half_width = (
        z
        * math.sqrt((p * (1.0 - p) / float(n)) + ((z * z) / (4.0 * float(n) * float(n))))
        / denominator
    )
    low = max(0.0, min(1.0, center - half_width))
    high = max(0.0, min(1.0, center + half_width))
    return (float(low), float(high))


def _tail_probability_precision(
    *,
    null_values: np.ndarray,
    observed: float,
    decision_p_threshold: float = _COLLISION_NULL_DECISION_P_THRESHOLD,
    confidence_level: float = _COLLISION_NULL_DECISION_CONFIDENCE,
) -> dict[str, float | int | bool]:
    values = np.asarray(null_values, dtype=float)
    values = values[np.isfinite(values)]
    n_draws = int(values.size)
    if n_draws <= 0 or not math.isfinite(observed):
        return {
            "n_draws": int(n_draws),
            "exceedances": 0,
            "p_value_estimate": 1.0,
            "p_value_mcse": np.nan,
            "p_value_ci_low": np.nan,
            "p_value_ci_high": np.nan,
            "quantile_resolution": np.nan,
            "ci_separated_from_threshold": False,
        }
    exceedances = int(np.sum(values >= float(observed)))
    p_value = float((exceedances + 1) / (n_draws + 1))
    mcse = float(math.sqrt(max(p_value * (1.0 - p_value), 0.0) / float(n_draws)))
    ci_low, ci_high = _wilson_interval(
        successes=exceedances,
        trials=n_draws,
        confidence_level=confidence_level,
    )
    threshold = float(min(max(decision_p_threshold, 0.0), 1.0))
    separated = bool(ci_high < threshold or ci_low > threshold)
    return {
        "n_draws": int(n_draws),
        "exceedances": int(exceedances),
        "p_value_estimate": p_value,
        "p_value_mcse": mcse,
        "p_value_ci_low": float(ci_low),
        "p_value_ci_high": float(ci_high),
        "quantile_resolution": float(1.0 / float(n_draws + 1)),
        "ci_separated_from_threshold": separated,
    }


def _target_precision_draw_count(
    *,
    draws_requested: int,
    max_draws: int,
    min_draws: int,
    target_p_mcse: float,
    decision_p_threshold: float,
) -> tuple[int, int]:
    requested = int(max(int(draws_requested), 0))
    hard_cap = int(max(int(max_draws), 0))
    if requested <= 0 or hard_cap <= 0:
        return (0, 0)
    draw_cap = int(min(requested, hard_cap))
    if draw_cap <= 0:
        return (0, 0)
    minimum = int(min(draw_cap, max(int(min_draws), 0)))
    if minimum <= 0:
        minimum = int(min(draw_cap, _COLLISION_NULL_MIN_DRAWS))

    target = float(target_p_mcse)
    if not math.isfinite(target) or target <= 0.0:
        return (draw_cap, minimum)
    threshold = float(min(max(decision_p_threshold, 0.0), 1.0))
    variance = threshold * (1.0 - threshold)
    required = int(math.ceil(variance / (target * target))) if variance > 0.0 else minimum
    required = int(max(required, minimum))
    return (int(min(draw_cap, required)), minimum)


def _simulate_collision_chunk(
    args: tuple[int, np.ndarray, np.ndarray, int, np.ndarray],
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    n_rows, class_prob, n_key_buckets, draw_count, seed_state = args
    n = int(max(int(n_rows), 0))
    draws = int(max(int(draw_count), 0))
    if n <= 0 or draws <= 0:
        empty = np.asarray([], dtype=float)
        return empty, empty, empty
    rng = np.random.default_rng(np.random.SeedSequence(seed_state))
    pairs = np.zeros(draws, dtype=float)
    excess_rows = np.zeros(draws, dtype=float)
    repeated_rows = np.zeros(draws, dtype=float)
    for draw_idx in range(draws):
        class_draw_counts = rng.multinomial(n, class_prob)
        pairs_i, excess_rows_i, repeated_rows_i = _simulate_one_draw_from_histogram(
            class_draw_counts=class_draw_counts,
            n_keys=n_key_buckets,
            rng=rng,
        )
        pairs[draw_idx] = pairs_i
        excess_rows[draw_idx] = excess_rows_i
        repeated_rows[draw_idx] = repeated_rows_i
    return pairs, excess_rows, repeated_rows


def simulate_collision_null_from_histogram(
    *,
    n_rows: int,
    histogram: pd.DataFrame,
    draws: int,
    rng: np.random.Generator,
    baseline_model: Literal["multinomial", "hypergeometric"] = "multinomial",
    n_population: int | None = None,
    max_draws: int = 1000,
    min_draws: int = _COLLISION_NULL_MIN_DRAWS,
    target_p_mcse: float = float("nan"),
    decision_p_threshold: float = _COLLISION_NULL_DECISION_P_THRESHOLD,
    decision_confidence_level: float = _COLLISION_NULL_DECISION_CONFIDENCE,
    tail_observed: dict[str, float] | None = None,
) -> pd.DataFrame:
    started = perf_counter()
    samples = pd.DataFrame(columns=["pairs", "excess_rows", "repeated_group_rows"])
    n = int(max(int(n_rows), 0))
    n_draws = int(max(int(draws), 0))
    target_draws = 0
    minimum_draws = 0
    draws_effective = 0
    parallel_enabled = False
    parallel_worker_count = 1
    parallel_chunk_count = 0
    parallel_fallback = False
    stop_reason = "not_started"
    try:
        if n <= 0 or n_draws <= 0:
            return pd.DataFrame(columns=["pairs", "excess_rows", "repeated_group_rows"])

        name_count, n_keys, n_pop = _extract_histogram_arrays(histogram, n_population=n_population)
        if n_pop <= 0 or name_count.size == 0:
            return pd.DataFrame(columns=["pairs", "excess_rows", "repeated_group_rows"])

        if baseline_model != "multinomial":
            # Hypergeometric mode currently uses analytic expectations only.
            return pd.DataFrame(columns=["pairs", "excess_rows", "repeated_group_rows"])

        target_draws, minimum_draws = _target_precision_draw_count(
            draws_requested=n_draws,
            max_draws=max_draws,
            min_draws=min_draws,
            target_p_mcse=target_p_mcse,
            decision_p_threshold=decision_p_threshold,
        )
        if target_draws <= 0:
            return pd.DataFrame(columns=["pairs", "excess_rows", "repeated_group_rows"])

        class_prob = (name_count * n_keys) / float(n_pop)
        prob_total = float(class_prob.sum())
        if not math.isfinite(prob_total) or prob_total <= 0.0:
            return pd.DataFrame(columns=["pairs", "excess_rows", "repeated_group_rows"])
        class_prob = class_prob / prob_total

        n_key_buckets = np.asarray(np.rint(n_keys), dtype=np.int64)
        draw_plan = _draw_chunk_plan(
            total_draws=target_draws,
            chunk_size=_COLLISION_NULL_PARALLEL_CHUNK_SIZE,
        )
        if not draw_plan:
            return pd.DataFrame(columns=["pairs", "excess_rows", "repeated_group_rows"])
        parallel_chunk_count = int(len(draw_plan))
        available_cpus = int(max(int(os.cpu_count() or 1), 1))
        parallel_worker_cap = int(
            min(
                _COLLISION_NULL_PARALLEL_MAX_WORKERS,
                available_cpus,
                parallel_chunk_count,
            )
        )
        parallel_worker_count = int(max(parallel_worker_cap, 1))
        parallel_enabled = bool(
            n >= _COLLISION_NULL_PARALLEL_MIN_ROWS
            and target_draws >= _COLLISION_NULL_PARALLEL_MIN_DRAWS
            and parallel_worker_count > 1
        )

        observed_by_metric: dict[str, float] = {}
        for metric, value in (tail_observed or {}).items():
            metric_name = str(metric or "").strip().lower()
            if metric_name not in COLLISION_METRICS:
                continue
            numeric_value = float(value)
            if not math.isfinite(numeric_value):
                continue
            observed_by_metric[metric_name] = numeric_value

        seed_value = int(rng.integers(0, np.iinfo(np.uint64).max, dtype=np.uint64))
        seed_sequence = np.random.SeedSequence(seed_value)
        child_sequences = seed_sequence.spawn(parallel_chunk_count)
        chunk_tasks: list[tuple[int, np.ndarray, np.ndarray, int, np.ndarray]] = [
            (
                int(n),
                class_prob,
                n_key_buckets,
                int(draw_count),
                child_sequences[idx].generate_state(4, dtype=np.uint64),
            )
            for idx, draw_count in enumerate(draw_plan)
        ]

        chunk_results: list[tuple[np.ndarray, np.ndarray, np.ndarray]] = []
        if observed_by_metric:
            stop_reason = "max_draws_reached"
            exceedance_counts = {metric: 0 for metric in observed_by_metric}
            for task in chunk_tasks:
                chunk = _simulate_collision_chunk(task)
                chunk_results.append(chunk)
                chunk_size = int(len(chunk[0]))
                draws_effective += chunk_size
                if chunk_size <= 0:
                    continue
                if "pairs" in exceedance_counts:
                    exceedance_counts["pairs"] += int(np.sum(chunk[0] >= observed_by_metric["pairs"]))
                if "excess_rows" in exceedance_counts:
                    exceedance_counts["excess_rows"] += int(
                        np.sum(chunk[1] >= observed_by_metric["excess_rows"])
                    )
                if "repeated_group_rows" in exceedance_counts:
                    exceedance_counts["repeated_group_rows"] += int(
                        np.sum(chunk[2] >= observed_by_metric["repeated_group_rows"])
                    )
                if draws_effective < minimum_draws:
                    continue
                resolved = True
                for metric_name in observed_by_metric:
                    k = int(exceedance_counts.get(metric_name, 0))
                    p_estimate = float((k + 1) / (draws_effective + 1))
                    mcse = float(
                        math.sqrt(max(p_estimate * (1.0 - p_estimate), 0.0) / float(draws_effective))
                    )
                    ci_low, ci_high = _wilson_interval(
                        successes=k,
                        trials=draws_effective,
                        confidence_level=decision_confidence_level,
                    )
                    threshold = float(min(max(decision_p_threshold, 0.0), 1.0))
                    ci_separated = bool(ci_high < threshold or ci_low > threshold)
                    mcse_resolved = bool(
                        math.isfinite(target_p_mcse) and target_p_mcse > 0.0 and mcse <= target_p_mcse
                    )
                    if not (mcse_resolved or ci_separated):
                        resolved = False
                        break
                if resolved:
                    stop_reason = "precision_resolved"
                    break
            parallel_enabled = False
            parallel_worker_count = 1
            parallel_chunk_count = int(len(chunk_results))
        else:
            if parallel_enabled:
                try:
                    with ProcessPoolExecutor(max_workers=parallel_worker_count) as executor:
                        chunk_results = list(executor.map(_simulate_collision_chunk, chunk_tasks))
                except Exception:
                    parallel_fallback = True
                    chunk_results = [_simulate_collision_chunk(task) for task in chunk_tasks]
            else:
                chunk_results = [_simulate_collision_chunk(task) for task in chunk_tasks]
            draws_effective = int(sum(len(chunk[0]) for chunk in chunk_results))
            stop_reason = "target_draws_reached"

        if not chunk_results:
            return pd.DataFrame(columns=["pairs", "excess_rows", "repeated_group_rows"])
        pairs = np.concatenate([chunk[0] for chunk in chunk_results], axis=0)
        excess_rows = np.concatenate([chunk[1] for chunk in chunk_results], axis=0)
        repeated_rows = np.concatenate([chunk[2] for chunk in chunk_results], axis=0)
        samples = pd.DataFrame(
            {
                "pairs": pairs,
                "excess_rows": excess_rows,
                "repeated_group_rows": repeated_rows,
            }
        )
        samples.attrs["monte_carlo_precision"] = {
            "draws_requested": int(n_draws),
            "draws_target": int(target_draws),
            "draws_effective": int(draws_effective),
            "min_draws": int(minimum_draws),
            "target_p_mcse": float(target_p_mcse),
            "decision_p_threshold": float(decision_p_threshold),
            "decision_confidence_level": float(decision_confidence_level),
            "stopped_early": bool(draws_effective < target_draws),
            "stop_reason": str(stop_reason),
        }
        return samples
    finally:
        record_runtime_timing(
            "simulation.collision_null_from_histogram",
            (perf_counter() - started) * 1000.0,
        )
        record_runtime_counter("simulation.collision_null_from_histogram.calls", 1)
        record_runtime_counter(
            "simulation.collision_null_from_histogram.n_rows",
            max(int(n_rows), 0),
        )
        record_runtime_counter(
            "simulation.collision_null_from_histogram.draws_requested",
            max(int(draws), 0),
        )
        record_runtime_counter(
            "simulation.collision_null_from_histogram.draws_effective",
            int(max(int(draws_effective), 0)),
        )
        record_runtime_counter(
            "simulation.collision_null_from_histogram.draws_target",
            int(max(int(target_draws), 0)),
        )
        record_runtime_counter(
            "simulation.collision_null_from_histogram.output_samples",
            int(len(samples)),
        )
        record_runtime_counter(
            "simulation.collision_null_from_histogram.parallel_enabled",
            1 if parallel_enabled else 0,
        )
        record_runtime_counter(
            "simulation.collision_null_from_histogram.parallel_workers",
            int(parallel_worker_count),
        )
        record_runtime_counter(
            "simulation.collision_null_from_histogram.parallel_chunks",
            int(parallel_chunk_count),
        )
        record_runtime_counter(
            "simulation.collision_null_from_histogram.parallel_fallback",
            1 if parallel_fallback else 0,
        )


def summarize_collision_observed_vs_null(
    *,
    observed: dict[str, float],
    expected: dict[str, float],
    null_samples: pd.DataFrame,
    metrics: tuple[str, ...] | list[str] | None = None,
    decision_p_threshold: float = _COLLISION_NULL_DECISION_P_THRESHOLD,
    decision_confidence_level: float = _COLLISION_NULL_DECISION_CONFIDENCE,
) -> pd.DataFrame:
    metric_list = _normalize_metric_list(metrics)
    rows: list[dict[str, float | str]] = []
    for metric in metric_list:
        observed_value = _safe_float(float(observed.get(metric, 0.0)))
        expected_value = _safe_float(float(expected.get(metric, 0.0)))
        if null_samples.empty or metric not in null_samples.columns:
            rows.append(
                {
                    "metric": metric,
                    "observed": observed_value,
                    "expected": expected_value,
                    "expected_p05": expected_value,
                    "expected_p50": expected_value,
                    "expected_p95": expected_value,
                    "z_score": 0.0,
                    "p_value": 1.0,
                    "monte_carlo_draws_effective": 0,
                    "monte_carlo_quantile_resolution": np.nan,
                    "monte_carlo_p_value_mcse": np.nan,
                    "monte_carlo_p_value_ci_low": np.nan,
                    "monte_carlo_p_value_ci_high": np.nan,
                    "monte_carlo_p_value_ci_separated_from_threshold": False,
                }
            )
            continue
        values = pd.to_numeric(null_samples[metric], errors="coerce").dropna().to_numpy(dtype=float)
        if values.size == 0:
            rows.append(
                {
                    "metric": metric,
                    "observed": observed_value,
                    "expected": expected_value,
                    "expected_p05": expected_value,
                    "expected_p50": expected_value,
                    "expected_p95": expected_value,
                    "z_score": 0.0,
                    "p_value": 1.0,
                    "monte_carlo_draws_effective": 0,
                    "monte_carlo_quantile_resolution": np.nan,
                    "monte_carlo_p_value_mcse": np.nan,
                    "monte_carlo_p_value_ci_low": np.nan,
                    "monte_carlo_p_value_ci_high": np.nan,
                    "monte_carlo_p_value_ci_separated_from_threshold": False,
                }
            )
            continue
        mean_value = float(np.mean(values))
        std_value = float(np.std(values, ddof=1)) if values.size > 1 else 0.0
        z_score = (observed_value - mean_value) / std_value if std_value > 0 else 0.0
        precision = _tail_probability_precision(
            null_values=values,
            observed=observed_value,
            decision_p_threshold=decision_p_threshold,
            confidence_level=decision_confidence_level,
        )
        rows.append(
            {
                "metric": metric,
                "observed": observed_value,
                "expected": expected_value if math.isfinite(expected_value) else mean_value,
                "expected_p05": float(np.quantile(values, 0.05)),
                "expected_p50": float(np.quantile(values, 0.50)),
                "expected_p95": float(np.quantile(values, 0.95)),
                "z_score": _safe_float(z_score),
                "p_value": float(precision["p_value_estimate"]),
                "monte_carlo_draws_effective": int(precision["n_draws"]),
                "monte_carlo_quantile_resolution": float(precision["quantile_resolution"]),
                "monte_carlo_p_value_mcse": float(precision["p_value_mcse"]),
                "monte_carlo_p_value_ci_low": float(precision["p_value_ci_low"]),
                "monte_carlo_p_value_ci_high": float(precision["p_value_ci_high"]),
                "monte_carlo_p_value_ci_separated_from_threshold": bool(
                    precision["ci_separated_from_threshold"]
                ),
            }
        )
    return pd.DataFrame(rows)


# Legacy compatibility wrappers (kept while report contracts migrate).
def duplicate_metrics_from_counts(counts: np.ndarray) -> dict[str, float]:
    out = collision_metrics_from_counts(counts)
    n_rows = out["n_rows"]
    repeated = out["repeated_group_rows"]
    values = counts.astype(float)
    values = values[np.isfinite(values) & (values >= 0.0)]
    return {
        "n_rows": n_rows,
        "n_unique_names": out["n_unique_names"],
        "duplicate_rows": repeated,
        "duplicate_row_rate": (repeated / n_rows) if n_rows > 0 else 0.0,
        "duplicate_pairs": out["pairs"],
        "n_names_ge2": float(np.sum(values >= 2.0)),
        "n_names_ge3": float(np.sum(values >= 3.0)),
        "n_names_ge5": float(np.sum(values >= 5.0)),
        "n_names_ge10": float(np.sum(values >= 10.0)),
        "max_count": float(values.max()) if values.size else 0.0,
    }


def simulate_duplicate_null(
    *,
    n_rows: int,
    population_name_counts: pd.Series,
    draws: int,
    rng: np.random.Generator,
    chunk_size: int = 500,
) -> pd.DataFrame:
    del chunk_size  # compatibility-only argument.
    hist = histogram_from_name_counts(population_name_counts)
    samples = simulate_collision_null_from_histogram(
        n_rows=n_rows,
        histogram=hist,
        draws=draws,
        rng=rng,
        baseline_model="multinomial",
    )
    if samples.empty:
        return pd.DataFrame()
    samples["duplicate_rows"] = samples["repeated_group_rows"]
    if int(n_rows) > 0:
        samples["duplicate_row_rate"] = samples["duplicate_rows"] / float(int(n_rows))
    else:
        samples["duplicate_row_rate"] = 0.0
    samples["duplicate_pairs"] = samples["pairs"]
    samples["n_names_ge2"] = np.nan
    samples["n_names_ge3"] = np.nan
    samples["n_names_ge5"] = np.nan
    samples["n_names_ge10"] = np.nan
    samples["max_count"] = np.nan
    return samples[
        [
            "duplicate_rows",
            "duplicate_row_rate",
            "duplicate_pairs",
            "n_names_ge2",
            "n_names_ge3",
            "n_names_ge5",
            "n_names_ge10",
            "max_count",
        ]
    ].copy()


def summarize_observed_vs_null(
    *,
    observed: dict[str, float],
    null_samples: pd.DataFrame,
    metric_fields: tuple[str, ...] = ("duplicate_rows", "duplicate_row_rate", "duplicate_pairs"),
) -> pd.DataFrame:
    rows: list[dict[str, float | str]] = []
    for metric in metric_fields:
        observed_value = _safe_float(float(observed.get(metric, 0.0)))
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
