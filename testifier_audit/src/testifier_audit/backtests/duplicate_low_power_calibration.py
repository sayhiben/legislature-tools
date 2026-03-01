from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
import logging
import math
from typing import Any

import numpy as np
import pandas as pd

from testifier_audit.names.collision_baseline import (
    collision_metrics_from_counts,
    expected_collision_metrics_from_probabilities,
    histogram_from_probabilities,
    simulate_collision_null_from_histogram,
    summarize_collision_observed_vs_null,
)
from testifier_audit.names.stat_tests import benjamini_hochberg, binomial_tail_p_value

LOGGER = logging.getLogger(__name__)

CALIBRATION_VERSION = "duplicate_low_power_calibration_v1"
PRIMARY_METRIC = "repeated_group_rows"
SCOPE_FAMILY = "scope"
BUCKET_FAMILY = "bucket"
POSITION_FAMILY = "position"


@dataclass(frozen=True, slots=True)
class CalibrationScenario:
    scenario_id: str
    scenario_group: str
    n_rows: int
    n_base_names: int
    zipf_alpha: float
    span_minutes: int = 360
    scope_injection_fraction: float = 0.0
    temporal_burst_fraction: float = 0.0
    temporal_burst_minutes: int = 10
    position_bias_fraction: float = 0.0
    aliasing_rate: float = 0.0
    match_coverage: float = 1.0
    missing_timestamp_rate: float = 0.0
    stratification_skew: float = 0.0
    truth_scope_anomaly: bool = False
    truth_bucket_anomaly: bool = False
    truth_position_anomaly: bool = False
    truth_per_name_anomaly: bool = False


@dataclass(frozen=True, slots=True)
class FamilyOperatingTargets:
    max_fpr: float
    min_power: float
    min_support_rate: float
    max_fdr: float | None = None
    min_ci_coverage: float | None = None
    min_secondary_power: float | None = None


@dataclass(frozen=True, slots=True)
class CalibrationTargets:
    scope: FamilyOperatingTargets
    bucket: FamilyOperatingTargets
    position: FamilyOperatingTargets


@dataclass(frozen=True, slots=True)
class CalibrationRecommendations:
    low_power_min_unique_names: int
    low_power_min_expected_duplicates: float
    low_power_min_unique_names_scope: int
    low_power_min_expected_duplicates_scope: float
    low_power_min_unique_names_bucket: int
    low_power_min_expected_duplicates_bucket: float
    low_power_min_unique_names_position: int
    low_power_min_expected_duplicates_position: float


@dataclass(frozen=True, slots=True)
class CalibrationArtifacts:
    case_summary: pd.DataFrame
    bucket_details: pd.DataFrame
    threshold_grid: pd.DataFrame
    recommendations: CalibrationRecommendations
    benchmark_summary: dict[str, Any]


def stable_seed(*parts: object) -> int:
    token = "|".join(str(part or "").strip() for part in parts)
    digest = __import__("hashlib").sha1(token.encode("utf-8")).hexdigest()
    return int(digest[:16], 16) % (2**32)


def default_scenarios() -> tuple[CalibrationScenario, ...]:
    return (
        CalibrationScenario(
            scenario_id="null_clean_state",
            scenario_group="null",
            n_rows=420,
            n_base_names=260,
            zipf_alpha=1.05,
        ),
        CalibrationScenario(
            scenario_id="null_homonym_heavy",
            scenario_group="null",
            n_rows=420,
            n_base_names=140,
            zipf_alpha=1.55,
        ),
        CalibrationScenario(
            scenario_id="null_match_coverage_loss",
            scenario_group="null",
            n_rows=260,
            n_base_names=190,
            zipf_alpha=1.15,
            match_coverage=0.58,
            missing_timestamp_rate=0.18,
        ),
        CalibrationScenario(
            scenario_id="null_normalization_aliasing",
            scenario_group="null",
            n_rows=420,
            n_base_names=230,
            zipf_alpha=1.10,
            aliasing_rate=0.35,
        ),
        CalibrationScenario(
            scenario_id="null_geo_conditioned_county",
            scenario_group="null",
            n_rows=380,
            n_base_names=170,
            zipf_alpha=1.30,
        ),
        CalibrationScenario(
            scenario_id="null_stratification_error",
            scenario_group="null",
            n_rows=420,
            n_base_names=240,
            zipf_alpha=1.08,
            stratification_skew=0.25,
        ),
        CalibrationScenario(
            scenario_id="anomaly_scope_repeated_mild",
            scenario_group="scope_anomaly",
            n_rows=420,
            n_base_names=230,
            zipf_alpha=1.08,
            scope_injection_fraction=0.08,
            truth_scope_anomaly=True,
            truth_per_name_anomaly=True,
        ),
        CalibrationScenario(
            scenario_id="anomaly_scope_repeated_strong",
            scenario_group="scope_anomaly",
            n_rows=420,
            n_base_names=230,
            zipf_alpha=1.08,
            scope_injection_fraction=0.14,
            truth_scope_anomaly=True,
            truth_per_name_anomaly=True,
        ),
        CalibrationScenario(
            scenario_id="anomaly_temporal_burst",
            scenario_group="bucket_anomaly",
            n_rows=420,
            n_base_names=230,
            zipf_alpha=1.08,
            scope_injection_fraction=0.05,
            temporal_burst_fraction=0.20,
            temporal_burst_minutes=5,
            truth_bucket_anomaly=True,
            truth_per_name_anomaly=True,
        ),
        CalibrationScenario(
            scenario_id="anomaly_position_concentration",
            scenario_group="position_anomaly",
            n_rows=420,
            n_base_names=230,
            zipf_alpha=1.08,
            position_bias_fraction=0.35,
            truth_position_anomaly=True,
        ),
    )


def default_targets() -> CalibrationTargets:
    return CalibrationTargets(
        scope=FamilyOperatingTargets(
            max_fpr=0.10,
            min_power=0.70,
            min_support_rate=0.55,
            max_fdr=0.15,
            min_ci_coverage=0.85,
            min_secondary_power=0.60,
        ),
        bucket=FamilyOperatingTargets(
            max_fpr=0.12,
            min_power=0.65,
            min_support_rate=0.50,
        ),
        position=FamilyOperatingTargets(
            max_fpr=0.12,
            min_power=0.60,
            min_support_rate=0.45,
        ),
    )


def default_threshold_candidates() -> pd.DataFrame:
    unique_candidates = [10, 15, 20, 25, 30, 35, 40]
    expected_candidates = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0]
    rows: list[dict[str, float | int]] = []
    for min_unique in unique_candidates:
        for min_expected in expected_candidates:
            rows.append(
                {
                    "min_unique_names": int(min_unique),
                    "min_expected_duplicates": float(min_expected),
                }
            )
    return pd.DataFrame(rows)


def _zipf_probabilities(n_names: int, alpha: float) -> np.ndarray:
    n = int(max(int(n_names), 2))
    exponent = float(max(float(alpha), 0.01))
    ranks = np.arange(1, n + 1, dtype=float)
    weights = 1.0 / np.power(ranks, exponent)
    total = float(weights.sum())
    if not math.isfinite(total) or total <= 0.0:
        return np.full(n, 1.0 / float(n), dtype=float)
    return (weights / total).astype(float)


def _expand_alias_vocab(
    *,
    base_probs: np.ndarray,
    aliasing_rate: float,
    rng: np.random.Generator,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    n_base = int(base_probs.size)
    alias_target = int(round(float(max(aliasing_rate, 0.0)) * float(n_base)))
    alias_target = max(min(alias_target, n_base), 0)
    alias_indices: set[int] = set()
    if alias_target > 0:
        alias_indices = set(
            rng.choice(np.arange(n_base, dtype=int), size=alias_target, replace=False).tolist()
        )

    strict_tokens: list[str] = []
    strict_probs: list[float] = []
    nickname_tokens: list[str] = []
    for idx in range(n_base):
        base_prob = float(base_probs[idx])
        nickname_key = f"name_{idx}"
        if idx in alias_indices:
            strict_tokens.append(f"name_{idx}_a")
            strict_probs.append(base_prob / 2.0)
            nickname_tokens.append(nickname_key)
            strict_tokens.append(f"name_{idx}_b")
            strict_probs.append(base_prob / 2.0)
            nickname_tokens.append(nickname_key)
        else:
            strict_tokens.append(f"name_{idx}")
            strict_probs.append(base_prob)
            nickname_tokens.append(nickname_key)

    strict_probabilities = np.asarray(strict_probs, dtype=float)
    strict_probabilities = strict_probabilities / float(strict_probabilities.sum())
    return (
        np.asarray(strict_tokens, dtype=object),
        strict_probabilities,
        np.asarray(nickname_tokens, dtype=object),
    )


def _shifted_probability_mix(base_probs: np.ndarray, skew: float) -> np.ndarray:
    skew_value = float(min(max(float(skew), 0.0), 0.95))
    if skew_value <= 0.0:
        return base_probs.copy()
    shifted = np.roll(base_probs, 7)
    mixed = (1.0 - skew_value) * base_probs + skew_value * shifted
    mixed = np.clip(mixed, a_min=0.0, a_max=None)
    mixed = mixed / float(mixed.sum())
    return mixed


def _probability_map(keys: np.ndarray, probs: np.ndarray) -> dict[str, float]:
    frame = pd.DataFrame(
        {
            "key": pd.Series(keys, dtype=object).astype(str),
            "prob": pd.Series(probs, dtype=float),
        }
    )
    grouped = frame.groupby("key", dropna=False)["prob"].sum()
    values = grouped.to_dict()
    total = float(sum(values.values()))
    if not math.isfinite(total) or total <= 0.0:
        return {}
    return {str(key): float(value / total) for key, value in values.items() if float(value) > 0.0}


def _counts_from_series(values: pd.Series) -> np.ndarray:
    counts = values.value_counts(dropna=False).to_numpy(dtype=float)
    return counts[np.isfinite(counts) & (counts >= 0.0)]


def _scope_metrics(
    *,
    strict_keys: pd.Series,
    strict_baseline_probs: np.ndarray,
    scope_draws: int,
    rng: np.random.Generator,
    null_cache: dict[tuple[int, str], pd.DataFrame],
) -> dict[str, float | bool | int]:
    n_rows = int(len(strict_keys))
    counts = _counts_from_series(strict_keys)
    observed_metrics = collision_metrics_from_counts(counts)
    expected_metrics = expected_collision_metrics_from_probabilities(
        n_rows=n_rows,
        probabilities=strict_baseline_probs,
    )
    histogram = histogram_from_probabilities(
        probabilities=strict_baseline_probs,
        n_population=max(250_000, int(n_rows) * 250),
    )
    cache_key = (int(n_rows), "scope")
    null_samples = null_cache.get(cache_key)
    if null_samples is None:
        null_samples = simulate_collision_null_from_histogram(
            n_rows=n_rows,
            histogram=histogram,
            draws=int(max(int(scope_draws), 16)),
            rng=rng,
            baseline_model="multinomial",
            max_draws=int(max(int(scope_draws), 16)),
            min_draws=int(max(int(scope_draws), 16)),
            target_p_mcse=float("nan"),
        )
        null_cache[cache_key] = null_samples
    summary = summarize_collision_observed_vs_null(
        observed=observed_metrics,
        expected=expected_metrics,
        null_samples=null_samples,
        metrics=[PRIMARY_METRIC],
    )
    if summary.empty:
        return {
            "scope_n_unique": int(pd.Series(strict_keys).nunique(dropna=False)),
            "scope_expected": 0.0,
            "scope_p_value": 1.0,
            "scope_significant": False,
            "scope_ci_contains_observed": True,
            "scope_observed": 0.0,
            "scope_expected_p05": 0.0,
            "scope_expected_p95": 0.0,
        }
    row = summary.iloc[0]
    observed = float(row.get("observed", 0.0))
    expected = float(row.get("expected", 0.0))
    p05 = float(row.get("expected_p05", expected))
    p95 = float(row.get("expected_p95", expected))
    p_value = float(row.get("p_value", 1.0))
    return {
        "scope_n_unique": int(pd.Series(strict_keys).nunique(dropna=False)),
        "scope_expected": expected,
        "scope_p_value": p_value,
        "scope_significant": bool(p_value <= 0.05),
        "scope_ci_contains_observed": bool((observed >= p05) and (observed <= p95)),
        "scope_observed": observed,
        "scope_expected_p05": p05,
        "scope_expected_p95": p95,
    }


def _bucket_metrics(
    *,
    case_id: str,
    strict_keys: pd.Series,
    minute_offsets: pd.Series,
    strict_baseline_probs: np.ndarray,
    bucket_minutes: int,
    bucket_draws: int,
    rng: np.random.Generator,
    null_cache: dict[tuple[int, str], pd.DataFrame],
    injected_bucket_cutoff: int | None,
) -> pd.DataFrame:
    if strict_keys.empty:
        return pd.DataFrame()
    bucket = int(max(int(bucket_minutes), 1))
    minute_numeric = pd.to_numeric(minute_offsets, errors="coerce")
    valid_mask = minute_numeric.notna() & (minute_numeric >= 0)
    if not bool(valid_mask.any()):
        return pd.DataFrame()

    working = pd.DataFrame(
        {
            "name_key": strict_keys[valid_mask].astype(str),
            "minute_offset": minute_numeric[valid_mask].astype(int),
        }
    )
    working["bucket_start"] = (working["minute_offset"] // bucket) * bucket

    rows: list[dict[str, Any]] = []
    histogram = histogram_from_probabilities(
        probabilities=strict_baseline_probs,
        n_population=max(250_000, int(len(working)) * 200),
    )
    for bucket_start, group in working.groupby("bucket_start", dropna=False):
        n_bucket = int(len(group))
        if n_bucket <= 0:
            continue
        counts = _counts_from_series(group["name_key"])
        observed_metrics = collision_metrics_from_counts(counts)
        expected_metrics = expected_collision_metrics_from_probabilities(
            n_rows=n_bucket,
            probabilities=strict_baseline_probs,
        )
        cache_key = (int(n_bucket), "bucket")
        null_samples = null_cache.get(cache_key)
        if null_samples is None:
            null_samples = simulate_collision_null_from_histogram(
                n_rows=n_bucket,
                histogram=histogram,
                draws=int(max(int(bucket_draws), 16)),
                rng=rng,
                baseline_model="multinomial",
                max_draws=int(max(int(bucket_draws), 16)),
                min_draws=int(max(int(bucket_draws), 16)),
                target_p_mcse=float("nan"),
            )
            null_cache[cache_key] = null_samples
        summary = summarize_collision_observed_vs_null(
            observed=observed_metrics,
            expected=expected_metrics,
            null_samples=null_samples,
            metrics=[PRIMARY_METRIC],
        )
        p_value = 1.0 if summary.empty else float(summary.iloc[0]["p_value"])
        rows.append(
            {
                "case_id": str(case_id),
                "bucket_start": int(bucket_start),
                "n_rows": int(n_bucket),
                "n_unique_names": int(group["name_key"].nunique(dropna=False)),
                "expected_duplicate_rows": float(expected_metrics.get(PRIMARY_METRIC, 0.0)),
                "p_value": float(p_value),
                "is_injected_bucket": bool(
                    injected_bucket_cutoff is not None and int(bucket_start) < int(injected_bucket_cutoff)
                ),
            }
        )

    bucket_frame = pd.DataFrame(rows)
    if bucket_frame.empty:
        return bucket_frame
    bucket_frame["q_value"] = benjamini_hochberg(bucket_frame["p_value"]).astype(float)
    bucket_frame["is_significant"] = bucket_frame["q_value"] <= 0.10
    return bucket_frame


def _position_metrics(
    *,
    strict_keys: pd.Series,
    positions: pd.Series,
    strict_baseline_probs: np.ndarray,
    permutations: int,
    rng: np.random.Generator,
) -> dict[str, float | int | bool]:
    frame = pd.DataFrame(
        {
            "name_key": strict_keys.astype(str),
            "position": positions.astype(str),
        }
    )
    frame = frame[frame["position"].isin({"Pro", "Con"})].copy()
    if frame.empty:
        return {
            "position_left_n_unique": 0,
            "position_right_n_unique": 0,
            "position_left_expected": 0.0,
            "position_right_expected": 0.0,
            "position_p_value": 1.0,
            "position_rate_diff": 0.0,
        }

    left = frame[frame["position"] == "Pro"]
    right = frame[frame["position"] == "Con"]
    left_counts = _counts_from_series(left["name_key"])
    right_counts = _counts_from_series(right["name_key"])

    left_observed = collision_metrics_from_counts(left_counts)
    right_observed = collision_metrics_from_counts(right_counts)
    left_expected = expected_collision_metrics_from_probabilities(
        n_rows=int(len(left)),
        probabilities=strict_baseline_probs,
    )
    right_expected = expected_collision_metrics_from_probabilities(
        n_rows=int(len(right)),
        probabilities=strict_baseline_probs,
    )
    left_rate = (
        float(left_observed.get(PRIMARY_METRIC, 0.0)) / float(max(int(len(left)), 1))
        if len(left) > 0
        else 0.0
    )
    right_rate = (
        float(right_observed.get(PRIMARY_METRIC, 0.0)) / float(max(int(len(right)), 1))
        if len(right) > 0
        else 0.0
    )
    observed_diff = float(left_rate - right_rate)

    labels = frame["position"].to_numpy(dtype=object)
    names = frame["name_key"].to_numpy(dtype=object)
    null_abs_diff = np.zeros(int(max(int(permutations), 1)), dtype=float)
    for idx in range(int(max(int(permutations), 1))):
        permuted = labels[rng.permutation(len(labels))]
        perm_frame = pd.DataFrame({"name_key": names, "position": permuted})
        perm_left = perm_frame[perm_frame["position"] == "Pro"]
        perm_right = perm_frame[perm_frame["position"] == "Con"]
        left_counts_perm = _counts_from_series(perm_left["name_key"])
        right_counts_perm = _counts_from_series(perm_right["name_key"])
        left_metric_perm = collision_metrics_from_counts(left_counts_perm)
        right_metric_perm = collision_metrics_from_counts(right_counts_perm)
        left_rate_perm = (
            float(left_metric_perm.get(PRIMARY_METRIC, 0.0)) / float(max(int(len(perm_left)), 1))
            if len(perm_left) > 0
            else 0.0
        )
        right_rate_perm = (
            float(right_metric_perm.get(PRIMARY_METRIC, 0.0)) / float(max(int(len(perm_right)), 1))
            if len(perm_right) > 0
            else 0.0
        )
        null_abs_diff[idx] = abs(float(left_rate_perm - right_rate_perm))

    p_value = float((np.sum(null_abs_diff >= abs(observed_diff)) + 1.0) / (len(null_abs_diff) + 1.0))
    return {
        "position_left_n_unique": int(left["name_key"].nunique(dropna=False)),
        "position_right_n_unique": int(right["name_key"].nunique(dropna=False)),
        "position_left_expected": float(left_expected.get(PRIMARY_METRIC, 0.0)),
        "position_right_expected": float(right_expected.get(PRIMARY_METRIC, 0.0)),
        "position_p_value": float(p_value),
        "position_rate_diff": observed_diff,
    }


def _per_name_metrics(
    *,
    strict_keys: pd.Series,
    strict_probability_map: dict[str, float],
    injected_key: str,
) -> dict[str, float | int | bool]:
    if strict_keys.empty:
        return {
            "per_name_total_discoveries": 0,
            "per_name_false_discoveries": 0,
            "per_name_true_discoveries": 0,
            "per_name_injected_detected": False,
        }
    n_rows = int(len(strict_keys))
    counts = strict_keys.value_counts(dropna=False)
    rows: list[dict[str, object]] = []
    for key, observed_count in counts.items():
        key_token = str(key)
        p = float(strict_probability_map.get(key_token, 0.0))
        p_value = binomial_tail_p_value(
            observed_successes=int(observed_count),
            total_trials=n_rows,
            success_probability=p,
        )
        rows.append(
            {
                "name_key": key_token,
                "observed_count": int(observed_count),
                "p_value": float(p_value),
                "is_true_anomaly": bool(injected_key and key_token == injected_key),
            }
        )
    per_name = pd.DataFrame(rows)
    if per_name.empty:
        return {
            "per_name_total_discoveries": 0,
            "per_name_false_discoveries": 0,
            "per_name_true_discoveries": 0,
            "per_name_injected_detected": False,
        }
    per_name["q_value"] = benjamini_hochberg(per_name["p_value"]).astype(float)
    discoveries = per_name[per_name["q_value"] <= 0.10].copy()
    total_discoveries = int(len(discoveries))
    true_discoveries = int(discoveries["is_true_anomaly"].astype(bool).sum())
    false_discoveries = int(max(total_discoveries - true_discoveries, 0))
    injected_detected = bool(
        injected_key and bool((discoveries["name_key"] == str(injected_key)).any())
    )
    return {
        "per_name_total_discoveries": total_discoveries,
        "per_name_false_discoveries": false_discoveries,
        "per_name_true_discoveries": true_discoveries,
        "per_name_injected_detected": injected_detected,
    }


def _simulate_case(
    *,
    scenario: CalibrationScenario,
    replicate: int,
    seed: int,
    bucket_minutes: int,
    scope_draws: int,
    bucket_draws: int,
    position_permutations: int,
) -> tuple[dict[str, Any], pd.DataFrame]:
    case_id = f"{scenario.scenario_id}__r{int(replicate):03d}"
    case_seed = stable_seed(seed, scenario.scenario_id, replicate)
    rng = np.random.default_rng(case_seed)

    base_probs = _zipf_probabilities(
        n_names=scenario.n_base_names,
        alpha=scenario.zipf_alpha,
    )
    strict_vocab, strict_baseline_probs, nickname_vocab = _expand_alias_vocab(
        base_probs=base_probs,
        aliasing_rate=scenario.aliasing_rate,
        rng=rng,
    )
    strict_actual_probs = _shifted_probability_mix(strict_baseline_probs, scenario.stratification_skew)
    strict_indices = rng.choice(
        np.arange(strict_vocab.size, dtype=int),
        size=int(scenario.n_rows),
        replace=True,
        p=strict_actual_probs,
    )

    strict_keys = pd.Series(strict_vocab[strict_indices], dtype=str)
    nickname_keys = pd.Series(nickname_vocab[strict_indices], dtype=str)

    minute_offsets = pd.Series(
        rng.integers(0, int(max(scenario.span_minutes, 1)), size=int(scenario.n_rows), endpoint=False),
        dtype=int,
    )
    positions = pd.Series(
        rng.choice(["Pro", "Con", "Other"], size=int(scenario.n_rows), p=[0.45, 0.45, 0.10]),
        dtype=str,
    )
    is_matched = pd.Series(
        rng.random(int(scenario.n_rows)) <= float(min(max(scenario.match_coverage, 0.0), 1.0)),
        dtype=bool,
    )

    injected_key = str(strict_vocab[0]) if strict_vocab.size > 0 else ""
    injected_bucket_cutoff: int | None = None

    scope_injection_count = int(round(float(max(scenario.scope_injection_fraction, 0.0)) * float(len(strict_keys))))
    if scope_injection_count > 0 and injected_key:
        scope_injection_count = min(scope_injection_count, int(len(strict_keys)))
        injection_indices = rng.choice(
            np.arange(int(len(strict_keys)), dtype=int),
            size=int(scope_injection_count),
            replace=False,
        )
        strict_keys.iloc[injection_indices] = injected_key

    if scenario.temporal_burst_fraction > 0.0 and injected_key:
        temporal_count = int(round(float(scenario.temporal_burst_fraction) * float(len(strict_keys))))
        temporal_count = min(max(temporal_count, 0), int(len(strict_keys)))
        if temporal_count > 0:
            anomaly_pool = np.flatnonzero(strict_keys.to_numpy(dtype=str) == injected_key)
            if anomaly_pool.size == 0:
                anomaly_pool = np.arange(int(len(strict_keys)), dtype=int)
            chosen = rng.choice(
                anomaly_pool,
                size=int(min(temporal_count, anomaly_pool.size)),
                replace=False,
            )
            burst_span = int(max(int(scenario.temporal_burst_minutes), 1))
            minute_offsets.iloc[chosen] = rng.integers(
                low=0,
                high=burst_span,
                size=int(len(chosen)),
                endpoint=False,
            )
            injected_bucket_cutoff = int((burst_span // int(max(bucket_minutes, 1)) + 1) * int(max(bucket_minutes, 1)))

    if scenario.position_bias_fraction > 0.0:
        bias_count = int(round(float(scenario.position_bias_fraction) * float(len(strict_keys))))
        bias_count = min(max(bias_count, 0), int(len(strict_keys)))
        if bias_count > 0:
            duplicate_pool = np.flatnonzero(strict_keys.to_numpy(dtype=str) == injected_key)
            if duplicate_pool.size == 0:
                duplicate_pool = np.arange(int(len(strict_keys)), dtype=int)
            chosen = rng.choice(
                duplicate_pool,
                size=int(min(bias_count, duplicate_pool.size)),
                replace=False,
            )
            positions.iloc[chosen] = "Pro"

    if scenario.missing_timestamp_rate > 0.0:
        missing_count = int(round(float(scenario.missing_timestamp_rate) * float(len(minute_offsets))))
        missing_count = min(max(missing_count, 0), int(len(minute_offsets)))
        if missing_count > 0:
            missing_indices = rng.choice(
                np.arange(int(len(minute_offsets)), dtype=int),
                size=int(missing_count),
                replace=False,
            )
            minute_offsets.iloc[missing_indices] = -1

    strict_probability_map = _probability_map(strict_vocab, strict_baseline_probs)
    scope_null_cache: dict[tuple[int, str], pd.DataFrame] = {}
    bucket_null_cache: dict[tuple[int, str], pd.DataFrame] = {}

    scope_stats = _scope_metrics(
        strict_keys=strict_keys,
        strict_baseline_probs=strict_baseline_probs,
        scope_draws=scope_draws,
        rng=rng,
        null_cache=scope_null_cache,
    )
    per_name_stats = _per_name_metrics(
        strict_keys=strict_keys,
        strict_probability_map=strict_probability_map,
        injected_key=(injected_key if scenario.truth_per_name_anomaly else ""),
    )
    bucket_stats = _bucket_metrics(
        case_id=case_id,
        strict_keys=strict_keys,
        minute_offsets=minute_offsets,
        strict_baseline_probs=strict_baseline_probs,
        bucket_minutes=bucket_minutes,
        bucket_draws=bucket_draws,
        rng=rng,
        null_cache=bucket_null_cache,
        injected_bucket_cutoff=injected_bucket_cutoff if scenario.truth_bucket_anomaly else None,
    )
    position_stats = _position_metrics(
        strict_keys=strict_keys,
        positions=positions,
        strict_baseline_probs=strict_baseline_probs,
        permutations=position_permutations,
        rng=rng,
    )

    bucket_any_significant = bool(
        (bucket_stats.get("is_significant", pd.Series(dtype=bool)).astype(bool).any())
        if not bucket_stats.empty
        else False
    )

    case_row: dict[str, Any] = {
        "case_id": case_id,
        "scenario_id": scenario.scenario_id,
        "scenario_group": scenario.scenario_group,
        "replicate": int(replicate),
        "seed": int(case_seed),
        "n_rows": int(len(strict_keys)),
        "strict_vocab_size": int(pd.Series(strict_keys).nunique(dropna=False)),
        "nickname_vocab_size": int(pd.Series(nickname_keys).nunique(dropna=False)),
        "aliasing_rate": float(scenario.aliasing_rate),
        "match_coverage": float(scenario.match_coverage),
        "missing_timestamp_rate": float(scenario.missing_timestamp_rate),
        "truth_scope_anomaly": bool(scenario.truth_scope_anomaly),
        "truth_bucket_anomaly": bool(scenario.truth_bucket_anomaly),
        "truth_position_anomaly": bool(scenario.truth_position_anomaly),
        "truth_per_name_anomaly": bool(scenario.truth_per_name_anomaly),
        "bucket_any_significant": bool(bucket_any_significant),
        **scope_stats,
        **per_name_stats,
        **position_stats,
    }
    return case_row, bucket_stats


def _safe_rate(numerator: float, denominator: float) -> float:
    den = float(denominator)
    if not math.isfinite(den) or den <= 0.0:
        return float("nan")
    return float(float(numerator) / den)


def _evaluate_scope_thresholds(
    *,
    case_summary: pd.DataFrame,
    candidates: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for candidate in candidates.itertuples(index=False):
        min_unique = int(candidate.min_unique_names)
        min_expected = float(candidate.min_expected_duplicates)
        tested = (
            (pd.to_numeric(case_summary["scope_n_unique"], errors="coerce") >= float(min_unique))
            & (pd.to_numeric(case_summary["scope_expected"], errors="coerce") >= float(min_expected))
        )
        null_mask = ~case_summary["truth_scope_anomaly"].astype(bool)
        alt_mask = case_summary["truth_scope_anomaly"].astype(bool)
        tested_null = tested & null_mask
        tested_alt = tested & alt_mask

        significant = case_summary["scope_significant"].astype(bool)
        fpr = _safe_rate(float((significant & tested_null).sum()), float(tested_null.sum()))
        power = _safe_rate(float((significant & tested_alt).sum()), float(tested_alt.sum()))
        support = _safe_rate(float(tested.sum()), float(len(case_summary)))
        ci_coverage = _safe_rate(
            float((case_summary["scope_ci_contains_observed"].astype(bool) & tested_null).sum()),
            float(tested_null.sum()),
        )

        tested_per_name = tested
        per_name_total = float(
            pd.to_numeric(
                case_summary.loc[tested_per_name, "per_name_total_discoveries"],
                errors="coerce",
            )
            .fillna(0)
            .sum()
        )
        per_name_false = float(
            pd.to_numeric(
                case_summary.loc[tested_per_name, "per_name_false_discoveries"],
                errors="coerce",
            )
            .fillna(0)
            .sum()
        )
        per_name_fdr = _safe_rate(per_name_false, per_name_total)

        per_name_alt = tested_per_name & case_summary["truth_per_name_anomaly"].astype(bool)
        per_name_power = _safe_rate(
            float(
                (
                    case_summary["per_name_injected_detected"].astype(bool)
                    & per_name_alt
                ).sum()
            ),
            float(per_name_alt.sum()),
        )

        rows.append(
            {
                "family": SCOPE_FAMILY,
                "min_unique_names": int(min_unique),
                "min_expected_duplicates": float(min_expected),
                "fpr": fpr,
                "power": power,
                "support_rate": support,
                "ci_coverage": ci_coverage,
                "secondary_fdr": per_name_fdr,
                "secondary_power": per_name_power,
            }
        )

    return pd.DataFrame(rows)


def _evaluate_bucket_thresholds(
    *,
    case_summary: pd.DataFrame,
    bucket_details: pd.DataFrame,
    candidates: pd.DataFrame,
) -> pd.DataFrame:
    if bucket_details.empty:
        return pd.DataFrame(columns=["family", "min_unique_names", "min_expected_duplicates", "fpr", "power", "support_rate"])

    rows: list[dict[str, Any]] = []
    truth_by_case = case_summary.set_index("case_id")["truth_bucket_anomaly"].astype(bool).to_dict()
    for candidate in candidates.itertuples(index=False):
        min_unique = int(candidate.min_unique_names)
        min_expected = float(candidate.min_expected_duplicates)

        working = bucket_details.copy()
        working["eligible"] = (
            (pd.to_numeric(working["n_unique_names"], errors="coerce") >= float(min_unique))
            & (pd.to_numeric(working["expected_duplicate_rows"], errors="coerce") >= float(min_expected))
        )
        working["tested_significant"] = working["eligible"] & working["is_significant"].astype(bool)

        per_case = (
            working.groupby("case_id", dropna=False)
            .agg(
                case_tested=("eligible", "any"),
                case_significant=("tested_significant", "any"),
                eligible_share=("eligible", "mean"),
                injected_bucket_hit=(
                    "tested_significant",
                    lambda values: bool(
                        (
                            values
                            & working.loc[values.index, "is_injected_bucket"].astype(bool)
                        ).any()
                    ),
                ),
            )
            .reset_index()
        )
        per_case["truth_bucket_anomaly"] = per_case["case_id"].map(
            lambda token: bool(truth_by_case.get(str(token), False))
        )

        tested_null = per_case["case_tested"].astype(bool) & (~per_case["truth_bucket_anomaly"])
        tested_alt = per_case["case_tested"].astype(bool) & per_case["truth_bucket_anomaly"]
        fpr = _safe_rate(
            float((per_case["case_significant"].astype(bool) & tested_null).sum()),
            float(tested_null.sum()),
        )
        power = _safe_rate(
            float((per_case["case_significant"].astype(bool) & tested_alt).sum()),
            float(tested_alt.sum()),
        )
        support = _safe_rate(float(per_case["case_tested"].astype(bool).sum()), float(len(per_case)))
        rows.append(
            {
                "family": BUCKET_FAMILY,
                "min_unique_names": int(min_unique),
                "min_expected_duplicates": float(min_expected),
                "fpr": fpr,
                "power": power,
                "support_rate": support,
                "ci_coverage": np.nan,
                "secondary_fdr": np.nan,
                "secondary_power": np.nan,
            }
        )

    return pd.DataFrame(rows)


def _evaluate_position_thresholds(
    *,
    case_summary: pd.DataFrame,
    candidates: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for candidate in candidates.itertuples(index=False):
        min_unique = int(candidate.min_unique_names)
        min_expected = float(candidate.min_expected_duplicates)
        left_unique = pd.to_numeric(case_summary["position_left_n_unique"], errors="coerce")
        right_unique = pd.to_numeric(case_summary["position_right_n_unique"], errors="coerce")
        left_expected = pd.to_numeric(case_summary["position_left_expected"], errors="coerce")
        right_expected = pd.to_numeric(case_summary["position_right_expected"], errors="coerce")

        tested = (
            (left_unique >= float(min_unique))
            & (right_unique >= float(min_unique))
            & (left_expected >= float(min_expected))
            & (right_expected >= float(min_expected))
        )
        significant = pd.to_numeric(case_summary["position_p_value"], errors="coerce") <= 0.05
        null_mask = ~case_summary["truth_position_anomaly"].astype(bool)
        alt_mask = case_summary["truth_position_anomaly"].astype(bool)
        tested_null = tested & null_mask
        tested_alt = tested & alt_mask

        fpr = _safe_rate(float((significant & tested_null).sum()), float(tested_null.sum()))
        power = _safe_rate(float((significant & tested_alt).sum()), float(tested_alt.sum()))
        support = _safe_rate(float(tested.sum()), float(len(case_summary)))
        rows.append(
            {
                "family": POSITION_FAMILY,
                "min_unique_names": int(min_unique),
                "min_expected_duplicates": float(min_expected),
                "fpr": fpr,
                "power": power,
                "support_rate": support,
                "ci_coverage": np.nan,
                "secondary_fdr": np.nan,
                "secondary_power": np.nan,
            }
        )

    return pd.DataFrame(rows)


def _recommend_for_family(
    *,
    family_rows: pd.DataFrame,
    targets: FamilyOperatingTargets,
) -> pd.Series:
    working = family_rows.copy()
    if working.empty:
        raise ValueError("family_rows cannot be empty when selecting recommendation")

    def _metric(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series, errors="coerce")

    working["fpr"] = _metric(working["fpr"])
    working["power"] = _metric(working["power"])
    working["support_rate"] = _metric(working["support_rate"])

    meets = (
        working["fpr"].le(float(targets.max_fpr))
        & working["power"].ge(float(targets.min_power))
        & working["support_rate"].ge(float(targets.min_support_rate))
    )
    if targets.max_fdr is not None and "secondary_fdr" in working.columns:
        secondary_fdr = _metric(working["secondary_fdr"])
        meets = meets & (secondary_fdr.le(float(targets.max_fdr)) | secondary_fdr.isna())
    if targets.min_ci_coverage is not None and "ci_coverage" in working.columns:
        coverage = _metric(working["ci_coverage"])
        meets = meets & (coverage.ge(float(targets.min_ci_coverage)) | coverage.isna())
    if targets.min_secondary_power is not None and "secondary_power" in working.columns:
        secondary_power = _metric(working["secondary_power"])
        meets = meets & (secondary_power.ge(float(targets.min_secondary_power)) | secondary_power.isna())

    working["meets_targets"] = meets.astype(bool)
    eligible = working[working["meets_targets"]].copy()

    sort_columns = [
        "meets_targets",
        "power",
        "support_rate",
        "fpr",
    ]
    ascending = [False, False, False, True]
    if "secondary_power" in working.columns:
        sort_columns.append("secondary_power")
        ascending.append(False)
    if "secondary_fdr" in working.columns:
        sort_columns.append("secondary_fdr")
        ascending.append(True)
    if "ci_coverage" in working.columns:
        sort_columns.append("ci_coverage")
        ascending.append(False)
    sort_columns.extend(["min_unique_names", "min_expected_duplicates"])
    ascending.extend([True, True])

    selected_pool = eligible if not eligible.empty else working
    selected = selected_pool.sort_values(sort_columns, ascending=ascending).iloc[0].copy()
    return selected


def _build_recommendations(
    *,
    threshold_grid: pd.DataFrame,
    targets: CalibrationTargets,
) -> CalibrationRecommendations:
    scope_row = _recommend_for_family(
        family_rows=threshold_grid[threshold_grid["family"] == SCOPE_FAMILY],
        targets=targets.scope,
    )
    bucket_row = _recommend_for_family(
        family_rows=threshold_grid[threshold_grid["family"] == BUCKET_FAMILY],
        targets=targets.bucket,
    )
    position_row = _recommend_for_family(
        family_rows=threshold_grid[threshold_grid["family"] == POSITION_FAMILY],
        targets=targets.position,
    )

    return CalibrationRecommendations(
        low_power_min_unique_names=int(scope_row["min_unique_names"]),
        low_power_min_expected_duplicates=float(scope_row["min_expected_duplicates"]),
        low_power_min_unique_names_scope=int(scope_row["min_unique_names"]),
        low_power_min_expected_duplicates_scope=float(scope_row["min_expected_duplicates"]),
        low_power_min_unique_names_bucket=int(bucket_row["min_unique_names"]),
        low_power_min_expected_duplicates_bucket=float(bucket_row["min_expected_duplicates"]),
        low_power_min_unique_names_position=int(position_row["min_unique_names"]),
        low_power_min_expected_duplicates_position=float(position_row["min_expected_duplicates"]),
    )


def run_duplicate_low_power_calibration(
    *,
    scenarios: Sequence[CalibrationScenario] | None = None,
    scenario_replicates: int = 24,
    seed: int = 6346,
    bucket_minutes: int = 30,
    scope_draws: int = 256,
    bucket_draws: int = 128,
    position_permutations: int = 400,
    candidates: pd.DataFrame | None = None,
    targets: CalibrationTargets | None = None,
) -> CalibrationArtifacts:
    scenario_list = tuple(scenarios) if scenarios is not None else default_scenarios()
    candidate_grid = candidates.copy() if candidates is not None else default_threshold_candidates()
    operating_targets = targets if targets is not None else default_targets()

    case_rows: list[dict[str, Any]] = []
    bucket_rows: list[pd.DataFrame] = []

    for scenario in scenario_list:
        for replicate in range(int(max(int(scenario_replicates), 1))):
            case_row, case_bucket_rows = _simulate_case(
                scenario=scenario,
                replicate=replicate,
                seed=seed,
                bucket_minutes=bucket_minutes,
                scope_draws=scope_draws,
                bucket_draws=bucket_draws,
                position_permutations=position_permutations,
            )
            case_rows.append(case_row)
            if not case_bucket_rows.empty:
                bucket_rows.append(case_bucket_rows)

    case_summary = pd.DataFrame(case_rows)
    bucket_details = (
        pd.concat(bucket_rows, ignore_index=True)
        if bucket_rows
        else pd.DataFrame(
            columns=[
                "case_id",
                "bucket_start",
                "n_rows",
                "n_unique_names",
                "expected_duplicate_rows",
                "p_value",
                "q_value",
                "is_significant",
                "is_injected_bucket",
            ]
        )
    )

    threshold_frames = [
        _evaluate_scope_thresholds(case_summary=case_summary, candidates=candidate_grid),
        _evaluate_bucket_thresholds(
            case_summary=case_summary,
            bucket_details=bucket_details,
            candidates=candidate_grid,
        ),
        _evaluate_position_thresholds(case_summary=case_summary, candidates=candidate_grid),
    ]
    threshold_grid = pd.concat(threshold_frames, ignore_index=True)
    recommendations = _build_recommendations(
        threshold_grid=threshold_grid,
        targets=operating_targets,
    )

    benchmark_summary: dict[str, Any] = {
        "calibration_version": CALIBRATION_VERSION,
        "seed": int(seed),
        "scenario_replicates": int(scenario_replicates),
        "bucket_minutes": int(bucket_minutes),
        "scope_draws": int(scope_draws),
        "bucket_draws": int(bucket_draws),
        "position_permutations": int(position_permutations),
        "targets": {
            "scope": asdict(operating_targets.scope),
            "bucket": asdict(operating_targets.bucket),
            "position": asdict(operating_targets.position),
        },
        "recommendations": asdict(recommendations),
        "n_cases": int(len(case_summary)),
        "n_bucket_rows": int(len(bucket_details)),
        "n_threshold_rows": int(len(threshold_grid)),
        "scenario_ids": [scenario.scenario_id for scenario in scenario_list],
    }

    for family, target in (
        (SCOPE_FAMILY, operating_targets.scope),
        (BUCKET_FAMILY, operating_targets.bucket),
        (POSITION_FAMILY, operating_targets.position),
    ):
        family_grid = threshold_grid[threshold_grid["family"] == family].copy()
        selected = _recommend_for_family(family_rows=family_grid, targets=target)
        benchmark_summary[f"{family}_selection"] = {
            "min_unique_names": int(selected["min_unique_names"]),
            "min_expected_duplicates": float(selected["min_expected_duplicates"]),
            "fpr": float(selected.get("fpr", float("nan"))),
            "power": float(selected.get("power", float("nan"))),
            "support_rate": float(selected.get("support_rate", float("nan"))),
            "ci_coverage": float(selected.get("ci_coverage", float("nan"))),
            "secondary_fdr": float(selected.get("secondary_fdr", float("nan"))),
            "secondary_power": float(selected.get("secondary_power", float("nan"))),
            "meets_targets": bool(selected.get("meets_targets", False)),
        }

    LOGGER.info(
        "duplicate low-power calibration complete cases=%s bucket_rows=%s threshold_rows=%s",
        len(case_summary),
        len(bucket_details),
        len(threshold_grid),
    )

    return CalibrationArtifacts(
        case_summary=case_summary,
        bucket_details=bucket_details,
        threshold_grid=threshold_grid,
        recommendations=recommendations,
        benchmark_summary=benchmark_summary,
    )


def build_calibration_report_markdown(
    *,
    artifacts: CalibrationArtifacts,
    scenarios: Sequence[CalibrationScenario],
    targets: CalibrationTargets,
) -> str:
    lines: list[str] = []
    lines.append("# DUP-021 Duplicate Low-Power Calibration Report")
    lines.append("")
    lines.append(f"Calibration version: `{CALIBRATION_VERSION}`")
    lines.append("")

    lines.append("## Operating Targets")
    lines.append(
        "- Scope: "
        + f"FPR <= {targets.scope.max_fpr:.2f}, power >= {targets.scope.min_power:.2f}, "
        + f"support >= {targets.scope.min_support_rate:.2f}, per-name FDR <= {targets.scope.max_fdr:.2f}, "
        + f"CI coverage >= {targets.scope.min_ci_coverage:.2f}"
    )
    lines.append(
        "- Bucket: "
        + f"FPR <= {targets.bucket.max_fpr:.2f}, power >= {targets.bucket.min_power:.2f}, "
        + f"support >= {targets.bucket.min_support_rate:.2f}"
    )
    lines.append(
        "- Position: "
        + f"FPR <= {targets.position.max_fpr:.2f}, power >= {targets.position.min_power:.2f}, "
        + f"support >= {targets.position.min_support_rate:.2f}"
    )
    lines.append("")

    lines.append("## Scenario Coverage")
    for scenario in scenarios:
        lines.append(
            "- "
            + f"{scenario.scenario_id}: group={scenario.scenario_group}, rows={scenario.n_rows}, "
            + f"zipf_alpha={scenario.zipf_alpha:.2f}, aliasing={scenario.aliasing_rate:.2f}, "
            + f"scope_inject={scenario.scope_injection_fraction:.2f}, temporal_burst={scenario.temporal_burst_fraction:.2f}, "
            + f"position_bias={scenario.position_bias_fraction:.2f}, match_coverage={scenario.match_coverage:.2f}, "
            + f"stratification_skew={scenario.stratification_skew:.2f}"
        )
    lines.append("")

    lines.append("## Recommended Thresholds")
    rec = artifacts.recommendations
    lines.append(
        "- Global/scope low-power thresholds: "
        + f"`low_power_min_unique_names={rec.low_power_min_unique_names}`, "
        + f"`low_power_min_expected_duplicates={rec.low_power_min_expected_duplicates:.1f}`"
    )
    lines.append(
        "- Bucket family thresholds: "
        + f"`low_power_min_unique_names_bucket={rec.low_power_min_unique_names_bucket}`, "
        + f"`low_power_min_expected_duplicates_bucket={rec.low_power_min_expected_duplicates_bucket:.1f}`"
    )
    lines.append(
        "- Position family thresholds: "
        + f"`low_power_min_unique_names_position={rec.low_power_min_unique_names_position}`, "
        + f"`low_power_min_expected_duplicates_position={rec.low_power_min_expected_duplicates_position:.1f}`"
    )
    lines.append("")

    lines.append("## Selected Operating Characteristics")
    for family in (SCOPE_FAMILY, BUCKET_FAMILY, POSITION_FAMILY):
        selected = artifacts.benchmark_summary.get(f"{family}_selection", {})
        lines.append(
            "- "
            + f"{family}: meets_targets={bool(selected.get('meets_targets', False))}, "
            + f"FPR={float(selected.get('fpr', float('nan'))):.3f}, "
            + f"power={float(selected.get('power', float('nan'))):.3f}, "
            + f"support={float(selected.get('support_rate', float('nan'))):.3f}, "
            + f"secondary_fdr={float(selected.get('secondary_fdr', float('nan'))):.3f}, "
            + f"secondary_power={float(selected.get('secondary_power', float('nan'))):.3f}"
        )
    lines.append("")

    lines.append("## Notes")
    lines.append(
        "- Low-power thresholds are now selected against explicit operating targets, not heuristic constants."
    )
    lines.append(
        "- Family-specific threshold overrides are emitted for bucket and position workflows while preserving "
        "the existing global thresholds for scope/per-name gating."
    )
    lines.append(
        "- This harness is deterministic under fixed seeds and intended for CI smoke + periodic full calibration runs."
    )
    lines.append("")
    return "\n".join(lines)
