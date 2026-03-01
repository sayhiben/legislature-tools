from __future__ import annotations

import logging
from dataclasses import dataclass
from hashlib import sha1
from math import comb
from pathlib import Path

import numpy as np
import pandas as pd

from testifier_audit.names.collision_baseline import (
    collision_metrics_from_counts,
    expected_collision_metrics,
    simulate_collision_null_from_histogram,
)
from testifier_audit.names.stat_tests import empirical_p_value_one_sided

logger = logging.getLogger(__name__)

DEFAULT_EVIDENCE_FAMILY = "vrdb_collision_null"
DEFAULT_BASELINE_VARIANT = "all_registrants"
DEFAULT_NAME_KEY_TYPE = "full_name_key"
DEFAULT_GEO_LEVEL = "state"
DEFAULT_GEO_VALUE = "WA"

_REQUIRED_PROBABILITY_COLUMNS = {
    "name_key",
    "name_key_type",
    "count",
    "probability",
    "denominator",
    "geo_level",
    "geo_value",
    "baseline_variant",
}
_REQUIRED_BACKOFF_COLUMNS = {
    "baseline_variant",
    "requested_geo_level",
    "requested_geo_value",
    "effective_geo_level",
    "effective_geo_value",
}


@dataclass(frozen=True)
class _BaselineContext:
    baseline_variant: str
    name_key_type: str
    geo_level: str
    geo_value: str
    denominator: int
    histogram: pd.DataFrame
    probability_by_name: pd.Series
    vrdb_version: str
    normalization_version: str


def _safe_text(value: object) -> str:
    return str(value or "").strip()


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        candidate = float(value)
    except (TypeError, ValueError):
        return float(default)
    return candidate if np.isfinite(candidate) else float(default)


def _safe_int(value: object, default: int = 0) -> int:
    return int(max(int(_safe_float(value, float(default))), 0))


def _wilson_interval(successes: int, trials: int) -> tuple[float, float]:
    n = int(max(int(trials), 0))
    if n <= 0:
        return (0.0, 1.0)
    k = int(min(max(int(successes), 0), n))
    p = float(k) / float(n)
    z = 1.959963984540054
    z2_over_n = (z * z) / float(n)
    denominator = 1.0 + z2_over_n
    center = (p + (z2_over_n / 2.0)) / denominator
    half = (
        z
        * np.sqrt((p * (1.0 - p) / float(n)) + ((z * z) / (4.0 * float(n) * float(n))))
        / denominator
    )
    low = max(0.0, min(1.0, center - half))
    high = max(0.0, min(1.0, center + half))
    return (float(low), float(high))


def _stable_seed(*parts: object) -> int:
    payload = "|".join(_safe_text(value) for value in parts)
    digest = sha1(payload.encode("utf-8")).hexdigest()
    return int(digest[:16], 16) % (2**32)


def _normalize_probability_rows(probability_rows: pd.DataFrame) -> pd.DataFrame:
    missing = sorted(_REQUIRED_PROBABILITY_COLUMNS - set(probability_rows.columns))
    if missing:
        raise ValueError(f"Probability rows missing required columns: {', '.join(missing)}")

    frame = probability_rows.copy()
    frame["name_key"] = frame["name_key"].map(_safe_text)
    frame["name_key_type"] = frame["name_key_type"].map(_safe_text)
    frame["geo_level"] = frame["geo_level"].map(_safe_text)
    frame["geo_value"] = frame["geo_value"].map(_safe_text)
    frame["baseline_variant"] = frame["baseline_variant"].map(_safe_text)
    frame["count"] = pd.to_numeric(frame["count"], errors="coerce").fillna(0).astype(int)
    frame["probability"] = pd.to_numeric(frame["probability"], errors="coerce").fillna(0.0)
    frame["denominator"] = pd.to_numeric(frame["denominator"], errors="coerce").fillna(0).astype(int)

    if "vrdb_version" not in frame.columns:
        frame["vrdb_version"] = ""
    if "normalization_version" not in frame.columns:
        frame["normalization_version"] = ""
    frame["vrdb_version"] = frame["vrdb_version"].map(_safe_text)
    frame["normalization_version"] = frame["normalization_version"].map(_safe_text)

    frame = frame[
        (frame["name_key"] != "")
        & (frame["name_key_type"] != "")
        & (frame["baseline_variant"] != "")
        & (frame["geo_level"] != "")
        & (frame["count"] > 0)
        & (frame["denominator"] > 0)
        & (frame["probability"] > 0.0)
    ].copy()

    # Normalize probabilities per (variant, key-type, geography) so MC and analytic
    # expectations remain consistent even if the source rows were lightly rounded.
    grouped = frame.groupby(["baseline_variant", "name_key_type", "geo_level", "geo_value"], dropna=False)
    frame["probability"] = grouped["probability"].transform(
        lambda values: values / float(max(float(values.sum()), 1e-12))
    )
    return frame.reset_index(drop=True)


def _normalize_backoff_rows(backoff_rows: pd.DataFrame | None) -> pd.DataFrame:
    if backoff_rows is None:
        return pd.DataFrame(columns=sorted(_REQUIRED_BACKOFF_COLUMNS))
    missing = sorted(_REQUIRED_BACKOFF_COLUMNS - set(backoff_rows.columns))
    if missing:
        raise ValueError(f"Backoff rows missing required columns: {', '.join(missing)}")
    frame = backoff_rows.copy()
    for column in _REQUIRED_BACKOFF_COLUMNS:
        frame[column] = frame[column].map(_safe_text)
    if "fallback_steps" not in frame.columns:
        frame["fallback_steps"] = 0
    if "backoff_reason" not in frame.columns:
        frame["backoff_reason"] = ""
    if "effective_denominator" not in frame.columns:
        frame["effective_denominator"] = 0
    frame["fallback_steps"] = pd.to_numeric(frame["fallback_steps"], errors="coerce").fillna(0).astype(int)
    frame["effective_denominator"] = (
        pd.to_numeric(frame["effective_denominator"], errors="coerce").fillna(0).astype(int)
    )
    frame["backoff_reason"] = frame["backoff_reason"].map(_safe_text)
    frame = frame[
        (frame["baseline_variant"] != "")
        & (frame["requested_geo_level"] != "")
        & (frame["effective_geo_level"] != "")
    ].copy()
    return frame.sort_values(
        ["baseline_variant", "requested_geo_level", "requested_geo_value", "fallback_steps"],
        ascending=[True, True, True, True],
    ).reset_index(drop=True)


def _resolve_effective_geo(
    *,
    backoff_rows: pd.DataFrame,
    baseline_variant: str,
    requested_geo_level: str,
    requested_geo_value: str,
    fallback_geo_value: str,
) -> dict[str, object]:
    if backoff_rows.empty:
        return {
            "requested_geo_level": requested_geo_level,
            "requested_geo_value": requested_geo_value,
            "effective_geo_level": requested_geo_level,
            "effective_geo_value": requested_geo_value,
            "fallback_steps": 0,
            "backoff_reason": "not_provided",
            "effective_denominator": 0,
        }

    match = backoff_rows[
        (backoff_rows["baseline_variant"] == baseline_variant)
        & (backoff_rows["requested_geo_level"] == requested_geo_level)
        & (backoff_rows["requested_geo_value"] == requested_geo_value)
    ]
    if match.empty:
        if requested_geo_level == DEFAULT_GEO_LEVEL:
            return {
                "requested_geo_level": requested_geo_level,
                "requested_geo_value": requested_geo_value,
                "effective_geo_level": requested_geo_level,
                "effective_geo_value": requested_geo_value,
                "fallback_steps": 0,
                "backoff_reason": "missing_backoff_row",
                "effective_denominator": 0,
            }
        state_match = backoff_rows[
            (backoff_rows["baseline_variant"] == baseline_variant)
            & (backoff_rows["requested_geo_level"] == DEFAULT_GEO_LEVEL)
            & (backoff_rows["requested_geo_value"] == fallback_geo_value)
        ]
        if not state_match.empty:
            row = state_match.iloc[0]
            return {
                "requested_geo_level": requested_geo_level,
                "requested_geo_value": requested_geo_value,
                "effective_geo_level": _safe_text(row.get("effective_geo_level", DEFAULT_GEO_LEVEL)),
                "effective_geo_value": _safe_text(row.get("effective_geo_value", fallback_geo_value)),
                "fallback_steps": 2,
                "backoff_reason": "missing_backoff_row",
                "effective_denominator": _safe_int(row.get("effective_denominator", 0)),
            }
        return {
            "requested_geo_level": requested_geo_level,
            "requested_geo_value": requested_geo_value,
            "effective_geo_level": DEFAULT_GEO_LEVEL,
            "effective_geo_value": fallback_geo_value,
            "fallback_steps": 2,
            "backoff_reason": "missing_backoff_row",
            "effective_denominator": 0,
        }

    row = match.iloc[0]
    return {
        "requested_geo_level": _safe_text(row.get("requested_geo_level", requested_geo_level)),
        "requested_geo_value": _safe_text(row.get("requested_geo_value", requested_geo_value)),
        "effective_geo_level": _safe_text(row.get("effective_geo_level", requested_geo_level)),
        "effective_geo_value": _safe_text(row.get("effective_geo_value", requested_geo_value)),
        "fallback_steps": _safe_int(row.get("fallback_steps", 0)),
        "backoff_reason": _safe_text(row.get("backoff_reason", "")),
        "effective_denominator": _safe_int(row.get("effective_denominator", 0)),
    }


def _build_baseline_context(
    *,
    probability_rows: pd.DataFrame,
    baseline_variant: str,
    name_key_type: str,
    geo_level: str,
    geo_value: str,
) -> _BaselineContext:
    filtered = probability_rows[
        (probability_rows["baseline_variant"] == baseline_variant)
        & (probability_rows["name_key_type"] == name_key_type)
        & (probability_rows["geo_level"] == geo_level)
        & (probability_rows["geo_value"] == geo_value)
    ].copy()
    if filtered.empty:
        raise ValueError(
            "No VRDB probability rows available for "
            f"variant={baseline_variant}, key_type={name_key_type}, geo={geo_level}:{geo_value}"
        )

    denominator_candidates = filtered["denominator"].dropna().astype(int)
    denominator = int(max(int(denominator_candidates.max()), 0)) if not denominator_candidates.empty else 0
    if denominator <= 0:
        denominator = int(max(int(filtered["count"].sum()), 0))

    histogram = (
        filtered["count"]
        .value_counts(dropna=False)
        .rename_axis("name_count")
        .rename("n_keys")
        .reset_index()
        .sort_values("name_count")
        .reset_index(drop=True)
    )
    histogram["name_count"] = pd.to_numeric(histogram["name_count"], errors="coerce").fillna(0).astype(int)
    histogram["n_keys"] = pd.to_numeric(histogram["n_keys"], errors="coerce").fillna(0).astype(int)
    histogram = histogram[(histogram["name_count"] > 0) & (histogram["n_keys"] > 0)].copy()
    histogram["N"] = int(max(denominator, 0))

    probability_by_name = (
        filtered[["name_key", "probability"]]
        .dropna(subset=["name_key"]) 
        .drop_duplicates(subset=["name_key"], keep="first")
        .set_index("name_key")["probability"]
        .astype(float)
    )

    vrdb_versions = sorted({value for value in filtered["vrdb_version"].tolist() if _safe_text(value)})
    normalization_versions = sorted(
        {value for value in filtered["normalization_version"].tolist() if _safe_text(value)}
    )

    return _BaselineContext(
        baseline_variant=baseline_variant,
        name_key_type=name_key_type,
        geo_level=geo_level,
        geo_value=geo_value,
        denominator=denominator,
        histogram=histogram,
        probability_by_name=probability_by_name,
        vrdb_version=vrdb_versions[0] if vrdb_versions else "",
        normalization_version=normalization_versions[0] if normalization_versions else "",
    )


def _simulate_max_name_counts(
    *,
    n_rows: int,
    histogram: pd.DataFrame,
    draws: int,
    rng: np.random.Generator,
    max_categories: int,
) -> np.ndarray:
    if n_rows <= 0 or draws <= 0:
        return np.asarray([], dtype=float)
    if histogram.empty:
        return np.asarray([], dtype=float)

    counts = pd.to_numeric(histogram.get("name_count"), errors="coerce").fillna(0).astype(int).to_numpy()
    n_keys = pd.to_numeric(histogram.get("n_keys"), errors="coerce").fillna(0).astype(int).to_numpy()
    denominator_values = pd.to_numeric(histogram.get("N"), errors="coerce").dropna()
    denominator = int(max(int(denominator_values.max()), 0)) if not denominator_values.empty else 0
    if denominator <= 0:
        return np.asarray([], dtype=float)

    category_count = int(np.sum(n_keys))
    if category_count <= 0 or category_count > int(max_categories):
        return np.asarray([], dtype=float)

    probabilities = np.repeat(counts.astype(float) / float(denominator), n_keys)
    probabilities = probabilities[probabilities > 0.0]
    if probabilities.size == 0:
        return np.asarray([], dtype=float)
    probabilities = probabilities / float(probabilities.sum())

    max_counts = np.zeros(int(draws), dtype=float)
    for idx in range(int(draws)):
        draw_counts = rng.multinomial(int(n_rows), probabilities)
        max_counts[idx] = float(draw_counts.max()) if draw_counts.size else 0.0
    return max_counts


def _empty_slice_row(
    *,
    slice_id: str,
    slice_type: str,
    baseline_variant: str,
    name_key_type: str,
    requested_geo_level: str,
    requested_geo_value: str,
    effective_geo_level: str,
    effective_geo_value: str,
    reason: str,
) -> dict[str, object]:
    return {
        "evidence_family": DEFAULT_EVIDENCE_FAMILY,
        "slice_id": slice_id,
        "slice_type": slice_type,
        "name_key_type": name_key_type,
        "baseline_variant": baseline_variant,
        "requested_geo_level": requested_geo_level,
        "requested_geo_value": requested_geo_value,
        "effective_geo_level": effective_geo_level,
        "effective_geo_value": effective_geo_value,
        "fallback_steps": 0,
        "backoff_reason": reason,
        "n_rows": 0,
        "n_unique_names": 0,
        "observed_pairs": 0.0,
        "observed_max_name_count": 0.0,
        "expected_pairs_analytic": 0.0,
        "expected_pairs_mean": 0.0,
        "expected_pairs_median": 0.0,
        "expected_pairs_p95": 0.0,
        "expected_pairs_p99": 0.0,
        "tail_prob_pairs": 1.0,
        "tail_prob_pairs_mcse": np.nan,
        "tail_prob_pairs_ci_low": np.nan,
        "tail_prob_pairs_ci_high": np.nan,
        "monte_carlo_quantile_resolution": np.nan,
        "expected_max_name_count_mean": np.nan,
        "expected_max_name_count_p95": np.nan,
        "expected_max_name_count_p99": np.nan,
        "tail_prob_max_name": np.nan,
        "max_count_reference_available": False,
        "max_count_reference_reason": "not_computed",
        "inferential_status": "unavailable",
        "inferential_reason": reason,
        "monte_carlo_draws_requested": 0,
        "monte_carlo_draws_effective": 0,
        "effective_denominator": 0,
        "vrdb_version": "",
        "normalization_version": "",
    }


def _expected_name_rows(
    *,
    slice_id: str,
    slice_type: str,
    observed_counts: pd.Series,
    n_rows: int,
    probability_by_name: pd.Series,
    top_name_limit: int,
    baseline_variant: str,
    name_key_type: str,
    effective_geo_level: str,
    effective_geo_value: str,
) -> list[dict[str, object]]:
    if observed_counts.empty or n_rows <= 0 or int(top_name_limit) <= 0:
        return []

    ranked = observed_counts.sort_values(ascending=False)
    if int(top_name_limit) > 0:
        ranked = ranked.head(int(top_name_limit))

    rows: list[dict[str, object]] = []
    for name_key, observed_value in ranked.items():
        probability = _safe_float(probability_by_name.get(name_key, 0.0), 0.0)
        expected_count = float(n_rows) * probability
        observed_count = int(max(int(observed_value), 0))
        rows.append(
            {
                "evidence_family": DEFAULT_EVIDENCE_FAMILY,
                "slice_id": slice_id,
                "slice_type": slice_type,
                "name_key": _safe_text(name_key),
                "name_key_type": name_key_type,
                "baseline_variant": baseline_variant,
                "effective_geo_level": effective_geo_level,
                "effective_geo_value": effective_geo_value,
                "observed_count": observed_count,
                "expected_count": expected_count,
                "overrun_count": float(observed_count) - expected_count,
                "expected_share": probability,
            }
        )
    return rows


def _expected_sum_p2(histogram: pd.DataFrame) -> float:
    if histogram.empty:
        return 0.0
    name_count = pd.to_numeric(histogram.get("name_count"), errors="coerce").fillna(0).astype(float)
    n_keys = pd.to_numeric(histogram.get("n_keys"), errors="coerce").fillna(0).astype(float)
    denominator_values = pd.to_numeric(histogram.get("N"), errors="coerce").dropna()
    denominator = float(denominator_values.max()) if not denominator_values.empty else 0.0
    if denominator <= 0.0:
        return 0.0
    return float(np.sum(n_keys * (name_count / denominator) ** 2))


def compute_vrdb_collision_null_for_slices(
    *,
    slice_rows: pd.DataFrame,
    probability_rows: pd.DataFrame,
    backoff_rows: pd.DataFrame | None = None,
    slice_id_column: str = "slice_id",
    slice_type_column: str = "slice_type",
    name_key_column: str = "name_key",
    baseline_variant_column: str = "baseline_variant",
    name_key_type_column: str = "name_key_type",
    requested_geo_level_column: str = "requested_geo_level",
    requested_geo_value_column: str = "requested_geo_value",
    default_baseline_variant: str = DEFAULT_BASELINE_VARIANT,
    default_name_key_type: str = DEFAULT_NAME_KEY_TYPE,
    default_geo_level: str = DEFAULT_GEO_LEVEL,
    default_geo_value: str = DEFAULT_GEO_VALUE,
    monte_carlo_draws: int = 2_000,
    random_seed: int = 42,
    top_name_limit: int = 25,
    min_rows_for_inference: int = 25,
    min_expected_pairs_for_inference: float = 5.0,
    max_categories_for_max_count_reference: int = 20_000,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if slice_rows.empty:
        return pd.DataFrame(), pd.DataFrame()
    required_slice_columns = {slice_id_column, name_key_column}
    missing = sorted(required_slice_columns - set(slice_rows.columns))
    if missing:
        raise ValueError(f"Slice rows missing required columns: {', '.join(missing)}")

    cleaned_probability_rows = _normalize_probability_rows(probability_rows)
    cleaned_backoff_rows = _normalize_backoff_rows(backoff_rows)

    working = slice_rows.copy()
    for column in (
        slice_id_column,
        slice_type_column,
        name_key_column,
        baseline_variant_column,
        name_key_type_column,
        requested_geo_level_column,
        requested_geo_value_column,
    ):
        if column not in working.columns:
            working[column] = ""
        working[column] = working[column].map(_safe_text)

    output_rows: list[dict[str, object]] = []
    expected_name_rows: list[dict[str, object]] = []
    context_cache: dict[tuple[str, str, str, str], _BaselineContext] = {}

    grouped = working.groupby(slice_id_column, dropna=False)
    for slice_id, group in grouped:
        slice_id_value = _safe_text(slice_id)
        slice_type = _safe_text(group[slice_type_column].iloc[0]) or "slice"
        baseline_variant = _safe_text(group[baseline_variant_column].iloc[0]) or _safe_text(
            default_baseline_variant
        )
        name_key_type = _safe_text(group[name_key_type_column].iloc[0]) or _safe_text(default_name_key_type)
        requested_geo_level = _safe_text(group[requested_geo_level_column].iloc[0]) or _safe_text(
            default_geo_level
        )
        requested_geo_value = _safe_text(group[requested_geo_value_column].iloc[0]) or _safe_text(
            default_geo_value
        )

        geo_resolution = _resolve_effective_geo(
            backoff_rows=cleaned_backoff_rows,
            baseline_variant=baseline_variant,
            requested_geo_level=requested_geo_level,
            requested_geo_value=requested_geo_value,
            fallback_geo_value=_safe_text(default_geo_value),
        )
        effective_geo_level = _safe_text(geo_resolution.get("effective_geo_level", default_geo_level))
        effective_geo_value = _safe_text(geo_resolution.get("effective_geo_value", default_geo_value))

        context_key = (baseline_variant, name_key_type, effective_geo_level, effective_geo_value)
        context = context_cache.get(context_key)
        if context is None:
            try:
                context = _build_baseline_context(
                    probability_rows=cleaned_probability_rows,
                    baseline_variant=baseline_variant,
                    name_key_type=name_key_type,
                    geo_level=effective_geo_level,
                    geo_value=effective_geo_value,
                )
            except ValueError as exc:
                logger.warning(
                    "VRDB collision-null slice %s unavailable: %s",
                    slice_id_value,
                    exc,
                )
                output_rows.append(
                    _empty_slice_row(
                        slice_id=slice_id_value,
                        slice_type=slice_type,
                        baseline_variant=baseline_variant,
                        name_key_type=name_key_type,
                        requested_geo_level=requested_geo_level,
                        requested_geo_value=requested_geo_value,
                        effective_geo_level=effective_geo_level,
                        effective_geo_value=effective_geo_value,
                        reason="missing_baseline_context",
                    )
                )
                continue
            context_cache[context_key] = context

        valid_names = group[name_key_column].map(_safe_text)
        valid_names = valid_names[valid_names != ""]
        observed_counts = valid_names.value_counts(dropna=False)
        observed_metrics = collision_metrics_from_counts(observed_counts.to_numpy(dtype=float))
        n_rows = int(max(int(observed_metrics.get("n_rows", 0.0)), 0))
        n_unique_names = int(max(int(observed_metrics.get("n_unique_names", 0.0)), 0))
        observed_pairs = float(max(float(observed_metrics.get("pairs", 0.0)), 0.0))
        observed_max_name_count = float(observed_counts.max()) if not observed_counts.empty else 0.0

        if n_rows <= 0:
            output_rows.append(
                _empty_slice_row(
                    slice_id=slice_id_value,
                    slice_type=slice_type,
                    baseline_variant=baseline_variant,
                    name_key_type=name_key_type,
                    requested_geo_level=requested_geo_level,
                    requested_geo_value=requested_geo_value,
                    effective_geo_level=effective_geo_level,
                    effective_geo_value=effective_geo_value,
                    reason="empty_slice",
                )
            )
            continue

        expected_metrics = expected_collision_metrics(
            n_rows=n_rows,
            histogram=context.histogram,
            baseline_model="multinomial",
        )
        expected_pairs_analytic = float(max(float(expected_metrics.get("pairs", 0.0)), 0.0))
        sum_p2 = _expected_sum_p2(context.histogram)
        closed_form_expected_pairs = float(comb(n_rows, 2) * sum_p2) if n_rows >= 2 else 0.0

        seed = _stable_seed(
            random_seed,
            slice_id_value,
            baseline_variant,
            name_key_type,
            effective_geo_level,
            effective_geo_value,
        )
        rng = np.random.default_rng(seed)
        null_samples = simulate_collision_null_from_histogram(
            n_rows=n_rows,
            histogram=context.histogram,
            draws=int(max(int(monte_carlo_draws), 0)),
            max_draws=int(max(int(monte_carlo_draws), 0)),
            rng=rng,
            baseline_model="multinomial",
        )
        pairs_samples = pd.to_numeric(null_samples.get("pairs"), errors="coerce").dropna().to_numpy(dtype=float)
        draws_effective = int(len(pairs_samples))

        if draws_effective > 0:
            exceedances = int(np.sum(pairs_samples >= observed_pairs))
            tail_prob_pairs = float((exceedances + 1) / (draws_effective + 1))
            tail_prob_pairs_mcse = float(
                np.sqrt(
                    max(tail_prob_pairs * (1.0 - tail_prob_pairs), 0.0)
                    / float(max(draws_effective, 1))
                )
            )
            tail_prob_pairs_ci_low, tail_prob_pairs_ci_high = _wilson_interval(
                exceedances,
                draws_effective,
            )
            monte_carlo_quantile_resolution = float(1.0 / float(draws_effective + 1))
            expected_pairs_mean = float(np.mean(pairs_samples))
            expected_pairs_median = float(np.quantile(pairs_samples, 0.50))
            expected_pairs_p95 = float(np.quantile(pairs_samples, 0.95))
            expected_pairs_p99 = float(np.quantile(pairs_samples, 0.99))
        else:
            expected_pairs_mean = expected_pairs_analytic
            expected_pairs_median = expected_pairs_analytic
            expected_pairs_p95 = expected_pairs_analytic
            expected_pairs_p99 = expected_pairs_analytic
            tail_prob_pairs = 1.0
            tail_prob_pairs_mcse = np.nan
            tail_prob_pairs_ci_low = np.nan
            tail_prob_pairs_ci_high = np.nan
            monte_carlo_quantile_resolution = np.nan

        # Max-repeat references can be expensive for very large category sets, so we
        # compute them only when the baseline category count is manageable.
        max_samples = _simulate_max_name_counts(
            n_rows=n_rows,
            histogram=context.histogram,
            draws=int(max(int(monte_carlo_draws), 0)),
            rng=np.random.default_rng(seed ^ 0xA5A5A5A5),
            max_categories=int(max_categories_for_max_count_reference),
        )
        if max_samples.size > 0:
            expected_max_mean = float(np.mean(max_samples))
            expected_max_p95 = float(np.quantile(max_samples, 0.95))
            expected_max_p99 = float(np.quantile(max_samples, 0.99))
            tail_prob_max = float(empirical_p_value_one_sided(max_samples, observed_max_name_count))
            max_reference_available = True
            max_reference_reason = "computed"
        else:
            expected_max_mean = np.nan
            expected_max_p95 = np.nan
            expected_max_p99 = np.nan
            tail_prob_max = np.nan
            max_reference_available = False
            max_reference_reason = "category_limit"

        low_power = bool(
            n_rows < int(max(min_rows_for_inference, 1))
            or expected_pairs_analytic < float(max(min_expected_pairs_for_inference, 0.0))
        )
        inferential_status = "descriptive_only" if low_power else "inferential"
        inferential_reason = "low_power_support" if low_power else "reference_model_inference_available"

        output_rows.append(
            {
                "evidence_family": DEFAULT_EVIDENCE_FAMILY,
                "slice_id": slice_id_value,
                "slice_type": slice_type,
                "name_key_type": name_key_type,
                "baseline_variant": baseline_variant,
                "requested_geo_level": requested_geo_level,
                "requested_geo_value": requested_geo_value,
                "effective_geo_level": effective_geo_level,
                "effective_geo_value": effective_geo_value,
                "fallback_steps": _safe_int(geo_resolution.get("fallback_steps", 0)),
                "backoff_reason": _safe_text(geo_resolution.get("backoff_reason", "")),
                "n_rows": n_rows,
                "n_unique_names": n_unique_names,
                "observed_pairs": observed_pairs,
                "observed_max_name_count": observed_max_name_count,
                "expected_pairs_analytic": expected_pairs_analytic,
                "expected_pairs_closed_form": closed_form_expected_pairs,
                "expected_pairs_mean": expected_pairs_mean,
                "expected_pairs_median": expected_pairs_median,
                "expected_pairs_p95": expected_pairs_p95,
                "expected_pairs_p99": expected_pairs_p99,
                "tail_prob_pairs": tail_prob_pairs,
                "tail_prob_pairs_mcse": tail_prob_pairs_mcse,
                "tail_prob_pairs_ci_low": tail_prob_pairs_ci_low,
                "tail_prob_pairs_ci_high": tail_prob_pairs_ci_high,
                "monte_carlo_quantile_resolution": monte_carlo_quantile_resolution,
                "expected_max_name_count_mean": expected_max_mean,
                "expected_max_name_count_p95": expected_max_p95,
                "expected_max_name_count_p99": expected_max_p99,
                "tail_prob_max_name": tail_prob_max,
                "max_count_reference_available": max_reference_available,
                "max_count_reference_reason": max_reference_reason,
                "inferential_status": inferential_status,
                "inferential_reason": inferential_reason,
                "monte_carlo_draws_requested": int(max(int(monte_carlo_draws), 0)),
                "monte_carlo_draws_effective": draws_effective,
                "effective_denominator": context.denominator,
                "vrdb_version": context.vrdb_version,
                "normalization_version": context.normalization_version,
            }
        )

        expected_name_rows.extend(
            _expected_name_rows(
                slice_id=slice_id_value,
                slice_type=slice_type,
                observed_counts=observed_counts,
                n_rows=n_rows,
                probability_by_name=context.probability_by_name,
                top_name_limit=int(top_name_limit),
                baseline_variant=baseline_variant,
                name_key_type=name_key_type,
                effective_geo_level=effective_geo_level,
                effective_geo_value=effective_geo_value,
            )
        )

        logger.info(
            "VRDB collision-null slice=%s n=%s observed_pairs=%.3f expected_pairs=%.3f geo=%s:%s variant=%s",
            slice_id_value,
            n_rows,
            observed_pairs,
            expected_pairs_analytic,
            effective_geo_level,
            effective_geo_value,
            baseline_variant,
        )

    metrics_frame = pd.DataFrame(output_rows)
    expected_names_frame = pd.DataFrame(expected_name_rows)
    if not metrics_frame.empty:
        metrics_frame = metrics_frame.sort_values(["slice_type", "slice_id"]).reset_index(drop=True)
    if not expected_names_frame.empty:
        expected_names_frame = expected_names_frame.sort_values(
            ["slice_type", "slice_id", "observed_count", "name_key"],
            ascending=[True, True, False, True],
        ).reset_index(drop=True)
    return metrics_frame, expected_names_frame


def load_vrdb_probability_artifacts(
    *,
    probability_csv_path: Path,
    backoff_csv_path: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    probability_rows = pd.read_csv(probability_csv_path)
    backoff_rows = pd.read_csv(backoff_csv_path)
    return probability_rows, backoff_rows


def write_vrdb_collision_null_tables(
    *,
    metrics_rows: pd.DataFrame,
    expected_name_rows: pd.DataFrame,
    metrics_csv_path: Path,
    expected_names_csv_path: Path,
) -> tuple[Path, Path]:
    metrics_csv_path.parent.mkdir(parents=True, exist_ok=True)
    expected_names_csv_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_rows.to_csv(metrics_csv_path, index=False, float_format="%.12g")
    expected_name_rows.to_csv(expected_names_csv_path, index=False, float_format="%.12g")
    return metrics_csv_path, expected_names_csv_path
