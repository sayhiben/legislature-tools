from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from hashlib import sha1
from math import sqrt
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

DEFAULT_STATE_VALUE = "WA"
DEFAULT_NAME_KEY_TYPE = "full_name_key"

PROBABILITY_COLUMNS: tuple[str, ...] = (
    "name_key",
    "name_key_type",
    "count",
    "probability",
    "denominator",
    "geo_level",
    "geo_value",
    "baseline_variant",
    "vrdb_version",
    "normalization_version",
)

BACKOFF_COLUMNS: tuple[str, ...] = (
    "baseline_variant",
    "requested_geo_level",
    "requested_geo_value",
    "effective_geo_level",
    "effective_geo_value",
    "fallback_steps",
    "backoff_reason",
    "effective_denominator",
)


@dataclass(frozen=True, slots=True)
class BaselineScenario:
    scenario_id: str
    baseline_variant: str
    requested_geo_level: str
    requested_geo_value: str
    normalization_mode: str = "default"


@dataclass(frozen=True, slots=True)
class SyntheticScenario:
    scenario_id: str
    n_rows: int
    span_minutes: int
    injection_fraction: float = 0.0
    injection_burst_minutes: int = 0


def _safe_text(value: object) -> str:
    return str(value or "").strip()


def stable_seed(*parts: object) -> int:
    payload = "|".join(_safe_text(part) for part in parts)
    digest = sha1(payload.encode("utf-8")).hexdigest()
    return int(digest[:16], 16) % (2**32)


def required_geo_targets(
    *,
    requested_geo_level: str,
    requested_geo_value: str,
    fallback_state_value: str = DEFAULT_STATE_VALUE,
) -> set[tuple[str, str]]:
    level = _safe_text(requested_geo_level).lower()
    value = _safe_text(requested_geo_value).upper()
    state_value = _safe_text(fallback_state_value).upper() or DEFAULT_STATE_VALUE
    targets: set[tuple[str, str]] = {("state", state_value)}

    if level == "state":
        targets = {("state", value or state_value)}
    elif level == "county":
        if value:
            targets.add(("county", value))
    elif level == "city":
        if value:
            targets.add(("city", value))
            county_code = value.split("|", 1)[0].strip().upper()
            if county_code:
                targets.add(("county", county_code))
    elif level and value:
        targets.add((level, value))
    return targets


def required_geo_target_keys(
    *,
    scenarios: Iterable[BaselineScenario],
    fallback_state_value: str = DEFAULT_STATE_VALUE,
) -> set[str]:
    keys: set[str] = set()
    for scenario in scenarios:
        targets = required_geo_targets(
            requested_geo_level=scenario.requested_geo_level,
            requested_geo_value=scenario.requested_geo_value,
            fallback_state_value=fallback_state_value,
        )
        keys.update(f"{level}|{value}" for level, value in targets)
    return keys


def _normalize_probability_rows(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    for column in PROBABILITY_COLUMNS:
        if column not in working.columns:
            working[column] = "" if column not in {"count", "probability", "denominator"} else 0

    working["name_key"] = working["name_key"].fillna("").astype(str)
    working["name_key_type"] = working["name_key_type"].fillna("").astype(str)
    working["baseline_variant"] = working["baseline_variant"].fillna("").astype(str)
    working["geo_level"] = working["geo_level"].fillna("").astype(str).str.lower()
    working["geo_value"] = working["geo_value"].fillna("").astype(str).str.upper()
    working["count"] = pd.to_numeric(working["count"], errors="coerce").fillna(0).astype(int)
    working["probability"] = pd.to_numeric(working["probability"], errors="coerce").fillna(0.0)
    working["denominator"] = pd.to_numeric(working["denominator"], errors="coerce").fillna(0).astype(int)
    return working


def filter_probability_rows(
    *,
    probability_rows: pd.DataFrame,
    baseline_variants: set[str],
    name_key_type: str,
    geo_target_keys: set[str],
) -> pd.DataFrame:
    if probability_rows.empty:
        return pd.DataFrame(columns=list(PROBABILITY_COLUMNS))

    working = _normalize_probability_rows(probability_rows)
    filtered = working[
        (working["baseline_variant"].isin({variant for variant in baseline_variants if variant}))
        & (working["name_key_type"] == _safe_text(name_key_type))
        & ((working["geo_level"] + "|" + working["geo_value"]).isin(geo_target_keys))
    ]
    if filtered.empty:
        return pd.DataFrame(columns=list(PROBABILITY_COLUMNS))
    return filtered.loc[:, list(PROBABILITY_COLUMNS)].reset_index(drop=True)


def load_probability_rows_for_scenarios(
    *,
    probability_path: Path,
    scenarios: Sequence[BaselineScenario],
    name_key_type: str = DEFAULT_NAME_KEY_TYPE,
    fallback_state_value: str = DEFAULT_STATE_VALUE,
) -> pd.DataFrame:
    if not probability_path.exists():
        raise FileNotFoundError(f"Probability artifact not found: {probability_path}")

    baseline_variants = {scenario.baseline_variant for scenario in scenarios if _safe_text(scenario.baseline_variant)}
    geo_target_keys = required_geo_target_keys(
        scenarios=scenarios,
        fallback_state_value=fallback_state_value,
    )

    if probability_path.suffix.lower() == ".parquet":
        frame = pd.read_parquet(probability_path, columns=list(PROBABILITY_COLUMNS))
        return filter_probability_rows(
            probability_rows=frame,
            baseline_variants=baseline_variants,
            name_key_type=name_key_type,
            geo_target_keys=geo_target_keys,
        )

    chunks: list[pd.DataFrame] = []
    for chunk in pd.read_csv(probability_path, usecols=list(PROBABILITY_COLUMNS), chunksize=250_000):
        keep = filter_probability_rows(
            probability_rows=chunk,
            baseline_variants=baseline_variants,
            name_key_type=name_key_type,
            geo_target_keys=geo_target_keys,
        )
        if not keep.empty:
            chunks.append(keep)
    if not chunks:
        return pd.DataFrame(columns=list(PROBABILITY_COLUMNS))
    return pd.concat(chunks, ignore_index=True)


def load_backoff_rows(
    *,
    backoff_path: Path,
    baseline_variants: Iterable[str],
) -> pd.DataFrame:
    if not backoff_path.exists():
        raise FileNotFoundError(f"Backoff artifact not found: {backoff_path}")

    use_baselines = {variant for variant in baseline_variants if _safe_text(variant)}
    if backoff_path.suffix.lower() == ".parquet":
        frame = pd.read_parquet(backoff_path, columns=list(BACKOFF_COLUMNS))
    else:
        frame = pd.read_csv(backoff_path, usecols=list(BACKOFF_COLUMNS))

    working = frame.copy()
    for column in BACKOFF_COLUMNS:
        if column not in working.columns:
            working[column] = "" if column not in {"fallback_steps", "effective_denominator"} else 0
    text_columns = set(BACKOFF_COLUMNS) - {"fallback_steps", "effective_denominator"}
    for column in text_columns:
        working[column] = working[column].fillna("").astype(str)
    working["fallback_steps"] = pd.to_numeric(working["fallback_steps"], errors="coerce").fillna(0).astype(int)
    working["effective_denominator"] = (
        pd.to_numeric(working["effective_denominator"], errors="coerce").fillna(0).astype(int)
    )
    if use_baselines:
        working = working[working["baseline_variant"].isin(use_baselines)]
    return working.loc[:, list(BACKOFF_COLUMNS)].reset_index(drop=True)


def attach_bucket_fields(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    if "slice_id" not in frame.columns:
        return frame
    working = frame.copy()
    match = working["slice_id"].fillna("").astype(str).str.extract(r"^.+::bucket_(\d+)m:(.+)$", expand=True)
    working["bucket_minutes"] = pd.to_numeric(match[0], errors="coerce").fillna(0).astype(int)
    working["bucket_start"] = pd.to_datetime(match[1], format="%Y-%m-%dT%H:%M:%S%z", errors="coerce")
    return working


def slice_rows_for_case(
    *,
    case_id: str,
    frame: pd.DataFrame,
    bucket_minutes: Sequence[int],
    baseline_variant: str,
    requested_geo_level: str,
    requested_geo_value: str,
    name_column: str = DEFAULT_NAME_KEY_TYPE,
    name_key_type: str = DEFAULT_NAME_KEY_TYPE,
) -> pd.DataFrame:
    working = frame.copy()
    if name_column not in working.columns:
        raise ValueError(f"frame must include {name_column}")
    if "timestamp" not in working.columns:
        raise ValueError("frame must include timestamp")

    working[name_column] = working[name_column].fillna("").astype(str).str.strip()
    timestamps = pd.to_datetime(working["timestamp"], errors="coerce", utc=True)
    working = working[(working[name_column] != "") & timestamps.notna()].copy()
    if working.empty:
        return pd.DataFrame()
    working["timestamp"] = timestamps.loc[working.index]

    case_token = _safe_text(case_id)
    if not case_token:
        raise ValueError("case_id must be non-empty")

    rows: list[pd.DataFrame] = []
    common_columns: dict[str, Any] = {
        "case_id": case_token,
        "baseline_variant": _safe_text(baseline_variant),
        "name_key_type": _safe_text(name_key_type) or DEFAULT_NAME_KEY_TYPE,
        "requested_geo_level": _safe_text(requested_geo_level).lower(),
        "requested_geo_value": _safe_text(requested_geo_value).upper(),
    }
    full = pd.DataFrame(
        {
            **common_columns,
            "slice_id": f"{case_token}::full_hearing",
            "slice_type": "full_hearing",
            "name_key": working[name_column].astype(str),
        }
    )
    rows.append(full)

    for bucket in sorted({int(value) for value in bucket_minutes if int(value) > 0}):
        bucket_start = pd.to_datetime(working["timestamp"], errors="coerce", utc=True).dt.floor(
            f"{int(bucket)}min"
        )
        bucket_frame = pd.DataFrame(
            {
                **common_columns,
                "slice_id": (
                    case_token
                    + "::bucket_"
                    + str(int(bucket))
                    + "m:"
                    + bucket_start.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
                ),
                "slice_type": f"bucket_{int(bucket)}m",
                "name_key": working[name_column].astype(str),
            }
        )
        rows.append(bucket_frame)
    return pd.concat(rows, ignore_index=True)


def case_id_from_slice_id(slice_id: object) -> str:
    token = _safe_text(slice_id)
    if "::" not in token:
        return ""
    return token.split("::", 1)[0]


def summarize_case_metrics(
    *,
    metrics_rows: pd.DataFrame,
    case_id: str,
    family: str,
    scenario_id: str,
    baseline_variant: str,
    requested_geo_level: str,
    requested_geo_value: str,
    normalization_mode: str,
    tail_alpha: float,
    small_bucket_minutes: int,
) -> dict[str, Any]:
    case_token = _safe_text(case_id)
    if not case_token:
        raise ValueError("case_id must be non-empty")

    working = metrics_rows.copy()
    if working.empty:
        return {
            "case_id": case_token,
            "family": family,
            "scenario_id": scenario_id,
            "baseline_variant": baseline_variant,
            "requested_geo_level": requested_geo_level,
            "requested_geo_value": requested_geo_value,
            "normalization_mode": normalization_mode,
            "has_metrics": False,
        }
    working["case_id"] = working.get("slice_id", pd.Series(dtype=str)).map(case_id_from_slice_id)
    case_metrics = working[working["case_id"] == case_token].copy()
    if case_metrics.empty:
        return {
            "case_id": case_token,
            "family": family,
            "scenario_id": scenario_id,
            "baseline_variant": baseline_variant,
            "requested_geo_level": requested_geo_level,
            "requested_geo_value": requested_geo_value,
            "normalization_mode": normalization_mode,
            "has_metrics": False,
        }

    case_metrics = attach_bucket_fields(case_metrics)
    case_metrics["tail_prob_pairs"] = pd.to_numeric(
        case_metrics.get("tail_prob_pairs", pd.Series(dtype=float)),
        errors="coerce",
    )
    case_metrics["tail_prob_max_name"] = pd.to_numeric(
        case_metrics.get("tail_prob_max_name", pd.Series(dtype=float)),
        errors="coerce",
    )
    case_metrics["observed_pairs"] = pd.to_numeric(
        case_metrics.get("observed_pairs", pd.Series(dtype=float)),
        errors="coerce",
    )
    case_metrics["expected_pairs_mean"] = pd.to_numeric(
        case_metrics.get("expected_pairs_mean", pd.Series(dtype=float)),
        errors="coerce",
    )
    case_metrics["observed_max_name_count"] = pd.to_numeric(
        case_metrics.get("observed_max_name_count", pd.Series(dtype=float)),
        errors="coerce",
    )
    case_metrics["expected_max_name_count_mean"] = pd.to_numeric(
        case_metrics.get("expected_max_name_count_mean", pd.Series(dtype=float)),
        errors="coerce",
    )
    case_metrics["fallback_steps"] = pd.to_numeric(
        case_metrics.get("fallback_steps", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0).astype(int)

    full_metrics = case_metrics[case_metrics["bucket_minutes"] <= 0]
    full_row = full_metrics.iloc[0] if not full_metrics.empty else case_metrics.iloc[0]
    bucket_metrics = case_metrics[case_metrics["bucket_minutes"] > 0].copy()
    small_bucket_rows = bucket_metrics[bucket_metrics["bucket_minutes"] == int(small_bucket_minutes)]

    expected_pairs = float(full_row.get("expected_pairs_mean", np.nan))
    expected_max = float(full_row.get("expected_max_name_count_mean", np.nan))
    full_pairs_ratio = (
        float(full_row.get("observed_pairs", np.nan)) / expected_pairs
        if np.isfinite(expected_pairs) and expected_pairs > 0.0
        else float("nan")
    )
    full_max_ratio = (
        float(full_row.get("observed_max_name_count", np.nan)) / expected_max
        if np.isfinite(expected_max) and expected_max > 0.0
        else float("nan")
    )

    def _alert_share(frame: pd.DataFrame) -> float:
        if frame.empty:
            return float("nan")
        return float(
            pd.to_numeric(frame.get("tail_prob_pairs", pd.Series(dtype=float)), errors="coerce")
            .le(float(tail_alpha))
            .mean()
        )

    inferential_status = _safe_text(full_row.get("inferential_status", ""))
    inferential_reason = _safe_text(full_row.get("inferential_reason", ""))
    low_power_share = (
        float(
            bucket_metrics.get("inferential_status", pd.Series(dtype=str))
            .astype(str)
            .str.lower()
            .ne("inferential")
            .mean()
        )
        if not bucket_metrics.empty
        else float("nan")
    )
    backoff_reason = ""
    if "backoff_reason" in bucket_metrics.columns and not bucket_metrics.empty:
        mode = bucket_metrics["backoff_reason"].fillna("").astype(str).value_counts(dropna=False)
        if not mode.empty:
            backoff_reason = str(mode.index[0] or "")

    return {
        "case_id": case_token,
        "family": _safe_text(family),
        "scenario_id": _safe_text(scenario_id),
        "baseline_variant": _safe_text(baseline_variant),
        "requested_geo_level": _safe_text(requested_geo_level),
        "requested_geo_value": _safe_text(requested_geo_value),
        "normalization_mode": _safe_text(normalization_mode),
        "has_metrics": True,
        "n_slices_total": int(len(case_metrics)),
        "n_bucket_slices": int(len(bucket_metrics)),
        "n_small_bucket_slices": int(len(small_bucket_rows)),
        "full_tail_prob_pairs": float(full_row.get("tail_prob_pairs", np.nan)),
        "full_tail_prob_max_name": float(full_row.get("tail_prob_max_name", np.nan)),
        "full_observed_pairs": float(full_row.get("observed_pairs", np.nan)),
        "full_expected_pairs_mean": float(full_row.get("expected_pairs_mean", np.nan)),
        "full_pairs_ratio": full_pairs_ratio,
        "full_observed_max_name_count": float(full_row.get("observed_max_name_count", np.nan)),
        "full_expected_max_name_count_mean": float(
            full_row.get("expected_max_name_count_mean", np.nan)
        ),
        "full_max_ratio": full_max_ratio,
        "full_inferential_status": inferential_status,
        "full_inferential_reason": inferential_reason,
        "effective_geo_level": _safe_text(full_row.get("effective_geo_level", "")),
        "effective_geo_value": _safe_text(full_row.get("effective_geo_value", "")),
        "fallback_steps_max": int(case_metrics["fallback_steps"].max()),
        "backoff_reason_mode": backoff_reason,
        "small_bucket_alert_share": _alert_share(small_bucket_rows),
        "bucket_alert_share": _alert_share(bucket_metrics),
        "bucket_low_power_share": low_power_share,
        "vrdb_version": _safe_text(full_row.get("vrdb_version", "")),
        "normalization_version": _safe_text(full_row.get("normalization_version", "")),
    }


def case_stats_from_frame(*, case_id: str, frame: pd.DataFrame, name_column: str = DEFAULT_NAME_KEY_TYPE) -> dict[str, Any]:
    case_token = _safe_text(case_id)
    if not case_token:
        raise ValueError("case_id must be non-empty")
    if name_column not in frame.columns:
        raise ValueError(f"frame must include {name_column}")

    names = frame[name_column].fillna("").astype(str).str.strip()
    names = names[names != ""]
    n_rows = int(len(names))
    if n_rows <= 1:
        return {
            "case_id": case_token,
            "n_rows": n_rows,
            "n_unique_names": int(names.nunique(dropna=False)),
            "observed_pairs": 0.0,
            "duplicate_pairs_ratio": 0.0,
        }

    counts = names.value_counts(dropna=False).to_numpy(dtype=float)
    observed_pairs = float(np.sum((counts * np.maximum(counts - 1.0, 0.0)) / 2.0))
    n_possible_pairs = float(n_rows * (n_rows - 1) / 2)
    ratio = observed_pairs / n_possible_pairs if n_possible_pairs > 0 else 0.0
    return {
        "case_id": case_token,
        "n_rows": n_rows,
        "n_unique_names": int(names.nunique(dropna=False)),
        "observed_pairs": observed_pairs,
        "duplicate_pairs_ratio": float(ratio),
    }


def select_historical_case_families(
    *,
    case_stats: pd.DataFrame,
    normal_count: int,
    suspect_count: int,
    min_rows: int,
    force_suspect_case_ids: Sequence[str] = (),
) -> pd.DataFrame:
    if case_stats.empty:
        return pd.DataFrame(columns=["case_id", "family"])

    working = case_stats.copy()
    working["case_id"] = working["case_id"].fillna("").astype(str)
    working["n_rows"] = pd.to_numeric(working.get("n_rows", 0), errors="coerce").fillna(0).astype(int)
    working["duplicate_pairs_ratio"] = pd.to_numeric(
        working.get("duplicate_pairs_ratio", 0.0),
        errors="coerce",
    ).fillna(0.0)

    eligible = working[(working["case_id"] != "") & (working["n_rows"] >= int(max(min_rows, 1)))].copy()
    if eligible.empty:
        return pd.DataFrame(columns=["case_id", "family"])
    eligible = eligible.sort_values(["duplicate_pairs_ratio", "n_rows", "case_id"]).reset_index(drop=True)

    force_suspect = {_safe_text(case_id) for case_id in force_suspect_case_ids if _safe_text(case_id)}
    force_suspect = force_suspect & set(eligible["case_id"].tolist())

    suspect_ids: list[str] = []
    if force_suspect:
        suspect_ids.extend(sorted(force_suspect))

    remaining_for_suspect = eligible[~eligible["case_id"].isin(set(suspect_ids))]
    needed_suspect = max(int(suspect_count) - len(suspect_ids), 0)
    if needed_suspect > 0 and not remaining_for_suspect.empty:
        suspect_tail = remaining_for_suspect.tail(needed_suspect)
        suspect_ids.extend(suspect_tail["case_id"].astype(str).tolist())

    suspect_set = set(suspect_ids)
    normal_pool = eligible[~eligible["case_id"].isin(suspect_set)].copy()
    if normal_pool.empty:
        normal_ids: list[str] = []
    else:
        low = int(np.floor(0.20 * len(normal_pool)))
        high = int(np.ceil(0.80 * len(normal_pool)))
        central = normal_pool.iloc[low:high].copy()
        if central.empty:
            central = normal_pool
        target = int(max(normal_count, 0))
        if target <= 0:
            normal_ids = []
        elif len(central) <= target:
            normal_ids = central["case_id"].astype(str).tolist()
        else:
            indices = np.linspace(0, len(central) - 1, num=target)
            selected = sorted({int(round(value)) for value in indices})
            normal_ids = central.iloc[selected]["case_id"].astype(str).tolist()

    rows = [{"case_id": case_id, "family": "historical_normal"} for case_id in normal_ids]
    rows.extend({"case_id": case_id, "family": "historical_suspect"} for case_id in suspect_ids)
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["case_id", "family"])
    return out.drop_duplicates(subset=["case_id"], keep="first").reset_index(drop=True)


def split_case_ids(
    *,
    case_ids: Sequence[str],
    seed: int,
    holdout_fraction: float,
) -> tuple[list[str], list[str]]:
    tokens = sorted({_safe_text(case_id) for case_id in case_ids if _safe_text(case_id)})
    if not tokens:
        return [], []
    if len(tokens) == 1:
        return tokens, []

    fraction = float(max(min(float(holdout_fraction), 0.95), 0.05))
    holdout_size = int(round(len(tokens) * fraction))
    holdout_size = max(min(holdout_size, len(tokens) - 1), 1)

    rng = np.random.default_rng(int(seed))
    shuffled = np.asarray(tokens, dtype=object)[rng.permutation(len(tokens))]
    holdout = sorted(str(value) for value in shuffled[:holdout_size].tolist())
    calibration = sorted(str(value) for value in shuffled[holdout_size:].tolist())
    return calibration, holdout


def wilson_interval(
    *,
    successes: int,
    trials: int,
    z_score: float = 1.96,
) -> tuple[float, float]:
    n = int(max(trials, 0))
    if n == 0:
        return float("nan"), float("nan")

    k = int(min(max(successes, 0), n))
    z = float(max(z_score, 0.0))
    p_hat = float(k) / float(n)
    denominator = 1.0 + ((z * z) / float(n))
    centre = (p_hat + ((z * z) / (2.0 * float(n)))) / denominator
    radius = (z / denominator) * sqrt((p_hat * (1.0 - p_hat) / float(n)) + ((z * z) / (4.0 * float(n) * float(n))))
    lower = max(0.0, centre - radius)
    upper = min(1.0, centre + radius)
    return lower, upper


def threshold_feasibility_scan(
    *,
    holdout_normal_tail_probs: Sequence[float],
    synthetic_injected_tail_probs: Sequence[float],
    max_holdout_normal_alert_rate: float,
    min_synthetic_injected_alert_rate: float,
    candidate_thresholds: Sequence[float] | None = None,
) -> dict[str, Any]:
    normal = np.asarray([float(value) for value in holdout_normal_tail_probs], dtype=float)
    injected = np.asarray([float(value) for value in synthetic_injected_tail_probs], dtype=float)
    normal = normal[np.isfinite(normal)]
    injected = injected[np.isfinite(injected)]

    if normal.size == 0 or injected.size == 0:
        return {
            "feasible": False,
            "feasible_count": 0,
            "feasible_min_threshold": float("nan"),
            "feasible_max_threshold": float("nan"),
        }

    if candidate_thresholds is None:
        thresholds = np.unique(np.concatenate([normal, injected]))
    else:
        thresholds = np.asarray([float(value) for value in candidate_thresholds], dtype=float)
        thresholds = thresholds[np.isfinite(thresholds)]
        if thresholds.size == 0:
            thresholds = np.unique(np.concatenate([normal, injected]))

    thresholds = np.sort(np.unique(thresholds))
    max_normal = float(max_holdout_normal_alert_rate)
    min_injected = float(min_synthetic_injected_alert_rate)
    feasible: list[float] = []
    for threshold in thresholds:
        holdout_normal_rate = float(np.mean(normal <= threshold))
        synthetic_injected_rate = float(np.mean(injected <= threshold))
        if holdout_normal_rate <= max_normal and synthetic_injected_rate >= min_injected:
            feasible.append(float(threshold))

    if not feasible:
        return {
            "feasible": False,
            "feasible_count": 0,
            "feasible_min_threshold": float("nan"),
            "feasible_max_threshold": float("nan"),
        }
    return {
        "feasible": True,
        "feasible_count": int(len(feasible)),
        "feasible_min_threshold": float(min(feasible)),
        "feasible_max_threshold": float(max(feasible)),
    }


def synthetic_case_frame(
    *,
    probability_rows: pd.DataFrame,
    n_rows: int,
    case_seed: int,
    start_timestamp: pd.Timestamp,
    span_minutes: int,
    injection_name_key: str = "",
    injection_fraction: float = 0.0,
    injection_burst_minutes: int = 0,
) -> pd.DataFrame:
    if n_rows <= 0:
        return pd.DataFrame(columns=["full_name_key", "timestamp"])
    if probability_rows.empty:
        raise ValueError("probability_rows must be non-empty")

    working = probability_rows.copy()
    working["name_key"] = working.get("name_key", pd.Series(dtype=str)).fillna("").astype(str)
    working["probability"] = pd.to_numeric(
        working.get("probability", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    working = working[(working["name_key"] != "") & (working["probability"] > 0.0)].copy()
    if working.empty:
        raise ValueError("probability_rows has no positive-probability name_key rows")

    rng = np.random.default_rng(int(case_seed))
    names = working["name_key"].to_numpy(dtype=object)
    probs = working["probability"].to_numpy(dtype=float)
    probs = probs / float(probs.sum())
    sampled_names = rng.choice(names, size=int(n_rows), replace=True, p=probs).astype(object)

    span = int(max(int(span_minutes), 1))
    minute_offsets = rng.integers(low=0, high=span, size=int(n_rows), endpoint=False)

    requested_injection = int(round(float(max(injection_fraction, 0.0)) * float(n_rows)))
    if requested_injection > 0 and _safe_text(injection_name_key):
        injection_count = min(requested_injection, int(n_rows))
        injection_indices = rng.choice(
            np.arange(int(n_rows), dtype=int),
            size=int(injection_count),
            replace=False,
        )
        sampled_names[injection_indices] = _safe_text(injection_name_key)
        burst_span = int(max(int(injection_burst_minutes), 1))
        minute_offsets[injection_indices] = rng.integers(
            low=0,
            high=min(span, burst_span),
            size=int(injection_count),
            endpoint=False,
        )

    timestamps = pd.to_datetime(start_timestamp, utc=True) + pd.to_timedelta(
        minute_offsets,
        unit="m",
    )
    return pd.DataFrame(
        {
            "full_name_key": sampled_names.astype(str),
            "timestamp": pd.to_datetime(timestamps, utc=True),
        }
    )
