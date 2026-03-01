from __future__ import annotations

import json
import logging
import math
from pathlib import Path
from time import perf_counter
from typing import Any

import numpy as np
import pandas as pd

from testifier_audit.detectors.base import DetectorResult
from testifier_audit.features.dedup import DEDUP_MODES, DEFAULT_DEDUP_MODE, normalize_dedup_mode
from testifier_audit.io.hearing_metadata import HearingMetadata
from testifier_audit.proportion_stats import wilson_interval
from testifier_audit.report.analysis_registry import (
    analysis_status as analysis_registry_status,
)
from testifier_audit.report.analysis_registry import (
    configured_analysis_ids as registry_configured_analysis_ids,
)
from testifier_audit.report.analysis_registry import (
    default_analysis_definitions as registry_analysis_definitions,
)
from testifier_audit.report.analysis_registry import (
    focus_mode_for_analysis_ids as registry_focus_mode_for_analysis_ids,
)
from testifier_audit.report.contracts import default_color_semantics
from testifier_audit.report.global_baselines import (
    default_cross_hearing_loo_payload,
    normalize_leave_one_out_baseline_payload,
)
from testifier_audit.report.help_registry import (
    build_methodology_content,
    default_evidence_taxonomy,
    default_theme_options,
)
from testifier_audit.report.quality_builder import build_data_quality_panel
from testifier_audit.report.rendering.constants import (
    BASELINE_PROFILE_BUCKET_MINUTES,
    PACIFIC_TIMEZONE_NAME,
    _VOTER_LINKAGE_POSITION_CHART_COLUMNS,
)
from testifier_audit.report.rendering.data_sources import (
    _load_table_map_from_disk,
    _load_table_map_from_results,
)
from testifier_audit.report.rendering.hearing_context import _build_hearing_context_panel
from testifier_audit.report.rendering.help_docs import (
    _build_analysis_help_docs,
    _build_chart_help_docs,
    _default_chart_legend_docs,
    _detailed_what_to_look_for_by_analysis,
    _fallback_chart_legend_doc,
)
from testifier_audit.report.rendering.payload.common import (
    _canonical_name_to_display_name,
    _extract_bucket_options,
    _normalize_report_match_mode,
    _records_from_frame,
    _table_key,
    _with_expected_columns,
)
from testifier_audit.report.rendering.serialization import _json_safe
from testifier_audit.report.rendering.table_previews import _load_summaries_from_disk
from testifier_audit.report.triage_builder import build_investigation_views

LOGGER = logging.getLogger(__name__)

_VOLUME_ADAPTIVE_BUCKET_THRESHOLDS: tuple[tuple[int, int], ...] = (
    (25, 1440),
    (80, 720),
    (200, 480),
    (500, 240),
    (1000, 120),
    (2500, 60),
)
_MIN_DEFAULT_BUCKET_MINUTES = 30

_DUPLICATE_DEFAULT_ESTIMAND_PRIMARY = "name-key collision burden relative to reference baseline"
_DUPLICATE_DEFAULT_NON_GOALS = (
    "cannot infer identity, intent, IP-based behavior, or per-person duplication from the public dataset"
)
_DUPLICATE_DEFAULT_BASELINE_SEMANTICS = "reference model; not the data-generating process"
_DUPLICATE_DEFAULT_CLAIM_CLASS = "collision_signal"
_DUPLICATE_DEFAULT_INFERENTIAL_GATING = (
    "Low-power rows/windows are descriptive-only; inferential claims require supportable rows and "
    "an inferential status other than descriptive-only or unavailable."
)
_DUPLICATE_INFERENTIAL_REASON_UNMATCHED_SCOPE_BASELINE_UNSUPPORTED = (
    "unmatched_scope_registry_baseline_unsupported"
)
_DUPLICATE_CHART_IDS: tuple[str, ...] = (
    "duplicates_exact_bucket_concentration",
    "duplicates_exact_metric_diagnostics",
    "duplicates_exact_per_name_anomalies",
    "duplicates_exact_top_name_timing_exact",
    "duplicates_exact_position_bucket_deviance",
    "duplicates_exact_null_distribution",
    "duplicates_exact_swing_impact",
    "duplicates_exact_top_names",
    "duplicates_exact_position_switch",
)
_DUPLICATE_TABLE_NAMES: tuple[str, ...] = (
    "collision_methods",
    "collision_overview",
    "collision_by_bucket",
    "collision_by_bucket_position",
    "collision_stratification_sensitivity",
    "duplicate_metrics_overview",
    "duplicate_by_bucket",
    "position_duplicate_metrics",
    "position_concentration_tests",
    "null_distribution",
    "swing_impact_scenarios",
    "top_repeated_names",
    "per_name_tests",
    "per_name_display",
    "per_name_duplicates_by_mode",
    "per_name_submission_timing_by_mode",
    "top_name_timing_by_mode",
    "temporal_burst_signals",
    "hypothesis_families",
)
_EVIDENCE_MATRIX_SIGNAL_SCORE: dict[str, int] = {
    "normal": 0,
    "any": 1,
    "high": 2,
}
_EVIDENCE_MATRIX_SCENARIOS: tuple[dict[str, str], ...] = (
    {
        "scenario_id": "vrdb_high_duplicate_normal",
        "scenario_label": "VRDB high + duplicate normal",
        "disagreement_kind": "discordant",
        "duplicate_signal_level": "normal",
        "vrdb_signal_level": "high",
        "behavioral_signal_level": "any",
        "interpretation": (
            "VRDB collision-null evidence is elevated while duplicate burden remains normal. "
            "Treat this as a string-collision null concern, not as suppression of other families."
        ),
    },
    {
        "scenario_id": "duplicate_high_vrdb_normal",
        "scenario_label": "Duplicate high + VRDB normal",
        "disagreement_kind": "discordant",
        "duplicate_signal_level": "high",
        "vrdb_signal_level": "normal",
        "behavioral_signal_level": "any",
        "interpretation": (
            "Duplicate collision burden is elevated without VRDB-null elevation. "
            "Interpret as duplicate concentration above report baseline, not as VRDB anomaly."
        ),
    },
    {
        "scenario_id": "both_name_families_high",
        "scenario_label": "Duplicate high + VRDB high",
        "disagreement_kind": "concordant",
        "duplicate_signal_level": "high",
        "vrdb_signal_level": "high",
        "behavioral_signal_level": "any",
        "interpretation": (
            "Both name-evidence families are elevated in the same windows. "
            "Concordance raises follow-up priority."
        ),
    },
    {
        "scenario_id": "name_families_normal_behavioral_high",
        "scenario_label": "Name families normal + behavioral high",
        "disagreement_kind": "behavioral_primary",
        "duplicate_signal_level": "normal",
        "vrdb_signal_level": "normal",
        "behavioral_signal_level": "high",
        "interpretation": (
            "Name-evidence families are normal while behavioral timing alerts are elevated. "
            "Use behavioral explanations as the primary hypothesis."
        ),
    },
)
_DUPLICATE_EVIDENCE_MATRIX_POLICY_RULES: tuple[str, ...] = (
    "VRDB collision evidence may increase concern, but does not suppress other flags.",
    "Existing timing/content/metadata evidence may increase concern, but does not erase a VRDB extreme.",
    "Agreement across evidence families strengthens follow-up priority.",
    "Disagreement narrows the question being answered; it does not imply one method failed.",
    "No composite score in v1; review separate evidence-family columns.",
)


def _build_duplicate_evidence_matrix_frames(
    *,
    dup_exact_bucket: pd.DataFrame,
    vrdb_collision_bucket: pd.DataFrame,
    off_hours_window_control: pd.DataFrame,
    primary_scope: str,
    primary_match_mode: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    matrix_columns = [
        "bucket_minutes",
        "scenario_id",
        "scenario_label",
        "scenario_order",
        "disagreement_kind",
        "family_id",
        "family_label",
        "family_order",
        "signal_level",
        "signal_score",
        "signal_label",
        "window_count",
        "window_share",
        "first_bucket_start",
        "last_bucket_start",
        "interpretation",
        "policy_note",
    ]
    summary_columns = [
        "bucket_minutes",
        "scenario_id",
        "scenario_label",
        "scenario_order",
        "disagreement_kind",
        "window_count",
        "window_share",
        "first_bucket_start",
        "last_bucket_start",
        "interpretation",
        "policy_note",
    ]
    if dup_exact_bucket.empty or vrdb_collision_bucket.empty:
        return (
            _with_expected_columns(pd.DataFrame(), matrix_columns),
            _with_expected_columns(pd.DataFrame(), summary_columns),
        )

    dup_rows = dup_exact_bucket.copy()
    dup_rows["metric"] = dup_rows.get("metric", pd.Series(dtype=str)).fillna("").astype(str)
    dup_rows["scope"] = dup_rows.get("scope", pd.Series(dtype=str)).fillna("").astype(str)
    dup_rows["match_mode"] = (
        dup_rows.get("match_mode", pd.Series(dtype=str))
        .fillna(primary_match_mode)
        .map(lambda value: _normalize_report_match_mode(value, default=primary_match_mode))
        .astype(str)
    )
    dup_rows["bucket_start"] = pd.to_datetime(
        dup_rows.get("bucket_start", pd.Series(dtype=object)),
        errors="coerce",
    )
    dup_rows["bucket_minutes"] = pd.to_numeric(
        dup_rows.get("bucket_minutes", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0).astype(int)
    dup_rows = dup_rows[
        (dup_rows["metric"] == "rows_anywhere")
        & (dup_rows["scope"] == primary_scope)
        & (dup_rows["match_mode"] == primary_match_mode)
        & (dup_rows["bucket_minutes"] > 0)
    ].copy()
    if dup_rows.empty:
        dup_rows = dup_exact_bucket.copy()
        dup_rows["metric"] = dup_rows.get("metric", pd.Series(dtype=str)).fillna("").astype(str)
        dup_rows["bucket_start"] = pd.to_datetime(
            dup_rows.get("bucket_start", pd.Series(dtype=object)),
            errors="coerce",
        )
        dup_rows["bucket_minutes"] = pd.to_numeric(
            dup_rows.get("bucket_minutes", pd.Series(dtype=float)),
            errors="coerce",
        ).fillna(0).astype(int)
        dup_rows = dup_rows[
            (dup_rows["metric"] == "rows_anywhere") & (dup_rows["bucket_minutes"] > 0)
        ].copy()
    if dup_rows.empty:
        return (
            _with_expected_columns(pd.DataFrame(), matrix_columns),
            _with_expected_columns(pd.DataFrame(), summary_columns),
        )
    dup_rows["duplicate_rows"] = pd.to_numeric(
        dup_rows.get("duplicate_rows", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    dup_rows["expected_duplicate_rows"] = pd.to_numeric(
        dup_rows.get("expected_duplicate_rows", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    dup_rows["duplicate_excess"] = pd.to_numeric(
        dup_rows.get("excess_duplicate_rows", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(dup_rows["duplicate_rows"] - dup_rows["expected_duplicate_rows"])
    dup_rows["duplicate_signal_high"] = dup_rows["duplicate_excess"] > 0.0
    dup_rows = dup_rows.dropna(subset=["bucket_start"])
    dup_signal = (
        dup_rows.groupby(["bucket_minutes", "bucket_start"], dropna=False)
        .agg(
            duplicate_signal_high=("duplicate_signal_high", "max"),
            duplicate_observed=("duplicate_rows", "mean"),
            duplicate_expected=("expected_duplicate_rows", "mean"),
            duplicate_excess=("duplicate_excess", "mean"),
        )
        .reset_index()
    )
    if dup_signal.empty:
        return (
            _with_expected_columns(pd.DataFrame(), matrix_columns),
            _with_expected_columns(pd.DataFrame(), summary_columns),
        )

    vrdb_rows = vrdb_collision_bucket.copy()
    vrdb_rows["bucket_start"] = pd.to_datetime(
        vrdb_rows.get("bucket_start", pd.Series(dtype=object)),
        errors="coerce",
    )
    vrdb_rows["bucket_minutes"] = pd.to_numeric(
        vrdb_rows.get("bucket_minutes", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0).astype(int)
    vrdb_rows = vrdb_rows[vrdb_rows["bucket_minutes"] > 0].dropna(subset=["bucket_start"]).copy()
    vrdb_rows["observed_pairs"] = pd.to_numeric(
        vrdb_rows.get("observed_pairs", pd.Series(dtype=float)),
        errors="coerce",
    )
    vrdb_rows["expected_pairs_p95"] = pd.to_numeric(
        vrdb_rows.get("expected_pairs_p95", pd.Series(dtype=float)),
        errors="coerce",
    )
    vrdb_rows["tail_prob_pairs"] = pd.to_numeric(
        vrdb_rows.get("tail_prob_pairs", pd.Series(dtype=float)),
        errors="coerce",
    )
    vrdb_rows["vrdb_signal_high"] = (
        (
            vrdb_rows["observed_pairs"].notna()
            & vrdb_rows["expected_pairs_p95"].notna()
            & (vrdb_rows["observed_pairs"] > vrdb_rows["expected_pairs_p95"])
        )
        | (
            vrdb_rows["tail_prob_pairs"].notna()
            & (vrdb_rows["tail_prob_pairs"] <= 0.05)
        )
    )
    vrdb_signal = (
        vrdb_rows.groupby(["bucket_minutes", "bucket_start"], dropna=False)
        .agg(
            vrdb_signal_high=("vrdb_signal_high", "max"),
            vrdb_observed_pairs=("observed_pairs", "mean"),
            vrdb_expected_pairs_p95=("expected_pairs_p95", "mean"),
            vrdb_min_tail_prob=("tail_prob_pairs", "min"),
        )
        .reset_index()
    )
    if vrdb_signal.empty:
        return (
            _with_expected_columns(pd.DataFrame(), matrix_columns),
            _with_expected_columns(pd.DataFrame(), summary_columns),
        )

    aligned = dup_signal.merge(
        vrdb_signal,
        on=["bucket_minutes", "bucket_start"],
        how="inner",
    )
    if aligned.empty:
        LOGGER.info(
            "Duplicate evidence matrix: no aligned duplicate/VRDB bucket windows; matrix disabled."
        )
        return (
            _with_expected_columns(pd.DataFrame(), matrix_columns),
            _with_expected_columns(pd.DataFrame(), summary_columns),
        )

    behavioral_signal = pd.DataFrame(
        columns=["bucket_minutes", "bucket_start", "behavioral_signal_high"]
    )
    if not off_hours_window_control.empty:
        off_hours_rows = off_hours_window_control.copy()
        off_hours_rows["bucket_start"] = pd.to_datetime(
            off_hours_rows.get("bucket_start", pd.Series(dtype=object)),
            errors="coerce",
        )
        off_hours_rows["bucket_minutes"] = pd.to_numeric(
            off_hours_rows.get("bucket_minutes", pd.Series(dtype=float)),
            errors="coerce",
        ).fillna(0).astype(int)
        off_hours_rows = off_hours_rows[
            off_hours_rows["bucket_minutes"] > 0
        ].dropna(subset=["bucket_start"])
        if not off_hours_rows.empty:
            robust_alert = pd.to_numeric(
                off_hours_rows.get(
                    "is_primary_two_sided_alert_window",
                    pd.Series(dtype=float),
                ),
                errors="coerce",
            ).fillna(0.0)
            fallback_alert = pd.to_numeric(
                off_hours_rows.get("is_primary_alert_window", pd.Series(dtype=float)),
                errors="coerce",
            ).fillna(0.0)
            eligibility_alert = pd.to_numeric(
                off_hours_rows.get("is_alert_off_hours_window", pd.Series(dtype=float)),
                errors="coerce",
            ).fillna(0.0)
            off_hours_rows["behavioral_signal_high"] = (
                robust_alert.gt(0.0) | fallback_alert.gt(0.0) | eligibility_alert.gt(0.0)
            )
            behavioral_signal = (
                off_hours_rows.groupby(["bucket_minutes", "bucket_start"], dropna=False)
                .agg(behavioral_signal_high=("behavioral_signal_high", "max"))
                .reset_index()
            )

    aligned = aligned.merge(
        behavioral_signal,
        on=["bucket_minutes", "bucket_start"],
        how="left",
    )
    aligned["behavioral_signal_high"] = (
        aligned.get("behavioral_signal_high", pd.Series(dtype=bool))
        .fillna(False)
        .astype(bool)
    )

    scenario_rows: list[dict[str, Any]] = []
    for bucket_minutes, bucket_frame in aligned.groupby("bucket_minutes", dropna=False):
        total_windows = int(len(bucket_frame))
        if total_windows <= 0:
            continue
        for scenario_order, scenario in enumerate(_EVIDENCE_MATRIX_SCENARIOS, start=1):
            scenario_id = str(scenario["scenario_id"])
            if scenario_id == "vrdb_high_duplicate_normal":
                mask = bucket_frame["vrdb_signal_high"] & (~bucket_frame["duplicate_signal_high"])
            elif scenario_id == "duplicate_high_vrdb_normal":
                mask = bucket_frame["duplicate_signal_high"] & (~bucket_frame["vrdb_signal_high"])
            elif scenario_id == "both_name_families_high":
                mask = bucket_frame["duplicate_signal_high"] & bucket_frame["vrdb_signal_high"]
            else:
                mask = (
                    (~bucket_frame["duplicate_signal_high"])
                    & (~bucket_frame["vrdb_signal_high"])
                    & bucket_frame["behavioral_signal_high"]
                )
            matching = bucket_frame.loc[mask].copy()
            window_count = int(len(matching))
            first_bucket_start = matching["bucket_start"].min() if window_count else pd.NaT
            last_bucket_start = matching["bucket_start"].max() if window_count else pd.NaT
            scenario_rows.append(
                {
                    "bucket_minutes": int(bucket_minutes),
                    "scenario_id": scenario_id,
                    "scenario_label": scenario["scenario_label"],
                    "scenario_order": scenario_order,
                    "disagreement_kind": scenario["disagreement_kind"],
                    "window_count": window_count,
                    "window_share": (
                        float(window_count) / float(total_windows) if total_windows else 0.0
                    ),
                    "first_bucket_start": first_bucket_start,
                    "last_bucket_start": last_bucket_start,
                    "interpretation": scenario["interpretation"],
                    "policy_note": (
                        "No composite score. Evidence families remain additive and separately interpreted."
                    ),
                    "duplicate_signal_level": scenario["duplicate_signal_level"],
                    "vrdb_signal_level": scenario["vrdb_signal_level"],
                    "behavioral_signal_level": scenario["behavioral_signal_level"],
                }
            )

    scenario_summary = _with_expected_columns(pd.DataFrame(scenario_rows), summary_columns)
    if scenario_summary.empty:
        return (
            _with_expected_columns(pd.DataFrame(), matrix_columns),
            _with_expected_columns(pd.DataFrame(), summary_columns),
        )

    family_rows: list[dict[str, Any]] = []
    family_config = (
        ("duplicate_signal_level", "duplicate_collision", "Duplicate collision", 1),
        ("vrdb_signal_level", "vrdb_collision", "VRDB collision-null", 2),
        ("behavioral_signal_level", "behavioral_timing", "Behavioral timing", 3),
    )
    for row in scenario_summary.itertuples(index=False):
        for signal_field, family_id, family_label, family_order in family_config:
            signal_level = str(getattr(row, signal_field, "any")).strip().lower() or "any"
            signal_score = _EVIDENCE_MATRIX_SIGNAL_SCORE.get(signal_level, 1)
            signal_label = (
                "High"
                if signal_level == "high"
                else "Normal"
                if signal_level == "normal"
                else "Any"
            )
            family_rows.append(
                {
                    "bucket_minutes": int(getattr(row, "bucket_minutes", 0)),
                    "scenario_id": str(getattr(row, "scenario_id", "")),
                    "scenario_label": str(getattr(row, "scenario_label", "")),
                    "scenario_order": int(getattr(row, "scenario_order", 0)),
                    "disagreement_kind": str(getattr(row, "disagreement_kind", "")),
                    "family_id": family_id,
                    "family_label": family_label,
                    "family_order": family_order,
                    "signal_level": signal_level,
                    "signal_score": signal_score,
                    "signal_label": signal_label,
                    "window_count": int(getattr(row, "window_count", 0)),
                    "window_share": float(getattr(row, "window_share", 0.0)),
                    "first_bucket_start": getattr(row, "first_bucket_start", pd.NaT),
                    "last_bucket_start": getattr(row, "last_bucket_start", pd.NaT),
                    "interpretation": str(getattr(row, "interpretation", "")),
                    "policy_note": str(getattr(row, "policy_note", "")),
                }
            )
    matrix_cells = _with_expected_columns(pd.DataFrame(family_rows), matrix_columns)
    if not matrix_cells.empty:
        LOGGER.info(
            "Duplicate evidence matrix built: bucket_variants=%s scenario_rows=%s",
            int(matrix_cells["bucket_minutes"].nunique()),
            int(len(scenario_summary)),
        )
    return matrix_cells, scenario_summary


def _normalized_optional_string(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value)


def _duplicate_baseline_label_for_source(source: str) -> str:
    source_norm = _normalized_optional_string(source).strip().lower()
    if source_norm in {"vrdb_full_histogram", "vrdb_full_keys"}:
        return "Statewide registry reference baseline"
    if source_norm == "historical_hearing_loo":
        return "Historical hearing leave-one-out baseline"
    if source_norm == "hearing_empirical":
        return "Same-hearing empirical baseline"
    if source_norm:
        return source_norm.replace("_", " ")
    return "Reference baseline"


def _duplicate_inferential_status_for_source(source: str) -> str:
    source_norm = _normalized_optional_string(source).strip().lower()
    if source_norm == "hearing_empirical":
        return "descriptive_only"
    return "reference_model_inference"


def _duplicate_default_inferential_reason_for_status(status: str) -> str:
    status_norm = _normalized_optional_string(status).strip().lower()
    if status_norm == "descriptive_only":
        return "self_referential_baseline"
    if status_norm == "unavailable":
        return "analytic_only_no_null_samples"
    if status_norm in {"reference_model_inference", "tested"}:
        return "reference_model_inference_available"
    return "status_not_specified"


def _duplicate_inferential_reason_label(reason: str) -> str:
    reason_norm = _normalized_optional_string(reason).strip().lower()
    if reason_norm == _DUPLICATE_INFERENTIAL_REASON_UNMATCHED_SCOPE_BASELINE_UNSUPPORTED:
        return (
            "Unmatched-only scope is descriptive-only under registry baselines until a "
            "dedicated unmatched reference baseline is implemented."
        )
    if not reason_norm:
        return ""
    return reason_norm.replace("_", " ")


def _duplicate_default_scope_reason_for_status(scope_status: str) -> str:
    status_norm = _normalized_optional_string(scope_status).strip().lower()
    if status_norm == "available":
        return "available"
    return "unavailable_no_rows_after_filtering"


def _duplicate_default_scope_status_for_reason(scope_reason: str) -> str:
    reason_norm = _normalized_optional_string(scope_reason).strip().lower()
    if reason_norm in {"", "available"}:
        return "available"
    return "unavailable"


def _duplicate_match_mode_role(mode: str, *, inferential_key_mode: str) -> str:
    normalized_mode = _normalize_report_match_mode(mode, default="strict")
    normalized_key_mode = _normalize_report_match_mode(inferential_key_mode, default="strict")
    if normalized_mode == normalized_key_mode:
        return "primary_inferential"
    return "sensitivity_only"


def _duplicate_match_mode_label(mode: str, *, inferential_key_mode: str) -> str:
    normalized_mode = _normalize_report_match_mode(mode, default="strict")
    if normalized_mode == "strict":
        base = "Strict"
    elif normalized_mode == "loose":
        base = "Loose (nickname)"
    else:
        base = normalized_mode.replace("_", " ")
    role = _duplicate_match_mode_role(normalized_mode, inferential_key_mode=inferential_key_mode)
    suffix = "Primary inferential key" if role == "primary_inferential" else "Sensitivity view"
    return f"{base} ({suffix})"


def _duplicate_inferential_supported_mask(
    frame: pd.DataFrame,
    *,
    status_column: str = "inferential_status",
    fallback_status_column: str | None = "inference_status",
) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=bool)
    if status_column in frame.columns:
        status_series = frame[status_column].fillna("").astype(str)
    elif fallback_status_column and fallback_status_column in frame.columns:
        status_series = frame[fallback_status_column].fillna("").astype(str)
    else:
        return pd.Series(False, index=frame.index, dtype=bool)
    status_norm = status_series.str.strip().str.lower()
    return status_norm.isin({"reference_model_inference", "tested"})


def _duplicate_scope_available_mask(frame: pd.DataFrame) -> pd.Series:
    if frame.empty or "scope_status" not in frame.columns:
        return pd.Series(dtype=bool)
    status_norm = frame["scope_status"].fillna("").astype(str).str.strip().str.lower()
    return status_norm == "available"


def _duplicate_detector_baseline_family_for_source(source: str) -> str:
    source_norm = _normalized_optional_string(source).strip().lower()
    if source_norm in {"vrdb_full_histogram", "vrdb_full_keys"}:
        return "vrdb_collision_null"
    if source_norm == "hearing_empirical":
        return "detector_self_referential"
    if source_norm:
        return "detector_collision_null"
    return "detector_collision_null"


def _duplicate_detector_baseline_family_label(family: str) -> str:
    family_norm = _normalized_optional_string(family).strip().lower()
    if family_norm == "vrdb_collision_null":
        return "VRDB collision-null baseline"
    if family_norm == "detector_self_referential":
        return "Detector self-referential baseline"
    if family_norm == "detector_collision_null":
        return "Detector collision baseline"
    return "Detector baseline"


def _log_choose_scalar(n: int, k: int) -> float:
    if k < 0 or k > n:
        return float("-inf")
    return math.lgamma(float(n) + 1.0) - math.lgamma(float(k) + 1.0) - math.lgamma(
        float(n - k) + 1.0
    )


def _expected_distinct_names_from_occupancy(
    *,
    n_rows: int,
    n_population: int,
    count_values: np.ndarray,
    name_frequencies: np.ndarray,
    cache: dict[int, float],
) -> float:
    n = int(max(int(n_rows), 0))
    n_pop = int(max(int(n_population), 0))
    if n <= 0 or n_pop <= 0:
        return 0.0
    if count_values.size == 0 or name_frequencies.size == 0:
        return 0.0
    n = min(n, n_pop)
    cached = cache.get(n)
    if cached is not None:
        return float(cached)
    log_den = _log_choose_scalar(n_pop, n)
    if not math.isfinite(log_den):
        return 0.0
    expected = 0.0
    for raw_count, raw_frequency in zip(count_values, name_frequencies, strict=False):
        count = int(max(int(raw_count), 0))
        frequency = float(raw_frequency)
        if count <= 0 or not math.isfinite(frequency) or frequency <= 0.0:
            continue
        absent_population = n_pop - count
        if absent_population < 0:
            present_probability = 1.0
        elif n > absent_population:
            present_probability = 1.0
        else:
            log_absent = _log_choose_scalar(absent_population, n) - log_den
            absent_probability = (
                math.exp(log_absent) if math.isfinite(log_absent) and log_absent > -745.0 else 0.0
            )
            present_probability = 1.0 - absent_probability
        expected += frequency * max(0.0, min(1.0, present_probability))
    expected = float(max(expected, 0.0))
    cache[n] = expected
    return expected


def _total_submissions_from_counts_per_minute(counts_per_minute: pd.DataFrame) -> int | None:
    if counts_per_minute.empty:
        return None
    totals = pd.to_numeric(
        counts_per_minute.get("n_total", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    total_submissions = int(round(float(totals.sum())))
    return total_submissions if total_submissions > 0 else None


def _bucket_at_or_above_target(options: list[int], *, target_minutes: int) -> int | None:
    if not options:
        return None
    above = [value for value in options if value >= target_minutes]
    if above:
        return above[0]
    return options[-1]


def _select_default_bucket_minutes(
    options: list[int],
    *,
    total_submissions: int | None,
) -> int | None:
    if not options:
        return None
    normalized = sorted({int(value) for value in options if int(value) > 0})
    if not normalized:
        return None
    if total_submissions is None:
        return (
            _MIN_DEFAULT_BUCKET_MINUTES
            if _MIN_DEFAULT_BUCKET_MINUTES in normalized
            else normalized[0]
        )
    target_minutes = _MIN_DEFAULT_BUCKET_MINUTES
    for submission_upper_bound, candidate_minutes in _VOLUME_ADAPTIVE_BUCKET_THRESHOLDS:
        if total_submissions <= submission_upper_bound:
            target_minutes = candidate_minutes
            break
    resolved = _bucket_at_or_above_target(normalized, target_minutes=target_minutes)
    if resolved is not None:
        return resolved
    return normalized[0]


def _load_cross_hearing_baseline_payload(out_dir: Path | None) -> dict[str, Any]:
    if out_dir is None:
        return normalize_leave_one_out_baseline_payload(default_cross_hearing_loo_payload())
    summary_path = out_dir / "summary" / "cross_hearing_baseline_loo.json"
    if not summary_path.exists():
        normalized = normalize_leave_one_out_baseline_payload(default_cross_hearing_loo_payload())
        normalized["source_path"] = str(summary_path)
        return normalized
    try:
        with summary_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        normalized = normalize_leave_one_out_baseline_payload(default_cross_hearing_loo_payload())
        normalized["source_path"] = str(summary_path)
        normalized["reason"] = "invalid_baseline_payload"
        return normalized
    normalized = normalize_leave_one_out_baseline_payload(payload if isinstance(payload, dict) else None)
    normalized["source_path"] = str(summary_path)
    return normalized


def _normalize_voter_position_label(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "Other"
    normalized = raw.lower()
    if normalized == "unknown":
        return "Other"
    if normalized == "other":
        return "Other"
    if normalized == "pro":
        return "Pro"
    if normalized == "con":
        return "Con"
    return raw


def _build_voter_position_bounds_fallback(
    voter_position_rows: pd.DataFrame,
    voter_position_unique: pd.DataFrame,
) -> pd.DataFrame:
    expected_columns = [
        "match_mode",
        "unit",
        "position_normalized",
        "n_total_lower",
        "n_total_upper",
        "matched_rate_lower",
        "matched_rate_upper",
        "matched_rate_span",
        "unmatched_rate_lower",
        "unmatched_rate_upper",
        "unmatched_rate_span",
        "inference_status",
    ]
    source_frames: list[pd.DataFrame] = []
    if not voter_position_rows.empty:
        source_frames.append(
            voter_position_rows.assign(unit="rows")[
                ["match_mode", "unit", "position_normalized", "n_total", "matched_rate", "unmatched_rate"]
            ]
        )
    if not voter_position_unique.empty:
        source_frames.append(
            voter_position_unique.assign(unit="unique_names")[
                ["match_mode", "unit", "position_normalized", "n_total", "matched_rate", "unmatched_rate"]
            ]
        )
    if not source_frames:
        return _with_expected_columns(pd.DataFrame(), expected_columns)

    combined = pd.concat(source_frames, ignore_index=True)
    combined["n_total"] = pd.to_numeric(combined["n_total"], errors="coerce")
    combined["matched_rate"] = pd.to_numeric(combined["matched_rate"], errors="coerce")
    combined["unmatched_rate"] = pd.to_numeric(combined["unmatched_rate"], errors="coerce")

    aggregated = (
        combined.groupby(["match_mode", "position_normalized"], dropna=False)
        .agg(
            n_total_lower=("n_total", "min"),
            n_total_upper=("n_total", "max"),
            matched_rate_lower=("matched_rate", "min"),
            matched_rate_upper=("matched_rate", "max"),
            unmatched_rate_lower=("unmatched_rate", "min"),
            unmatched_rate_upper=("unmatched_rate", "max"),
        )
        .reset_index()
    )
    aggregated["unit"] = "rows_vs_unique"
    aggregated["matched_rate_span"] = (
        pd.to_numeric(aggregated["matched_rate_upper"], errors="coerce")
        - pd.to_numeric(aggregated["matched_rate_lower"], errors="coerce")
    ).clip(lower=0.0)
    aggregated["unmatched_rate_span"] = (
        pd.to_numeric(aggregated["unmatched_rate_upper"], errors="coerce")
        - pd.to_numeric(aggregated["unmatched_rate_lower"], errors="coerce")
    ).clip(lower=0.0)
    aggregated["inference_status"] = "derived_from_rows_and_unique"
    return _with_expected_columns(aggregated, expected_columns)


def _build_interactive_chart_payload_v2(
    table_map: dict[str, pd.DataFrame],
    detector_summaries: dict[str, dict[str, Any]],
    *,
    cross_hearing_baseline: dict[str, Any] | None = None,
    default_dedup_mode: str | None = None,
    min_cell_n_for_rates: int = 25,
    hearing_metadata: HearingMetadata | None = None,
) -> dict[str, Any]:
    payload_started = perf_counter()
    counts_per_minute = _with_expected_columns(
        table_map.get("artifacts.counts_per_minute", pd.DataFrame()),
        [
            "minute_bucket",
            "n_total",
            "n_pro",
            "n_con",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "n_unique_names",
            "unique_ratio",
            "threshold_unique_ratio",
        ],
    )

    bursts_significant = _with_expected_columns(
        table_map.get(_table_key("bursts", "burst_significant_windows"), pd.DataFrame()),
        [
            "start_minute",
            "end_minute",
            "window_minutes",
            "bucket_minutes",
            "observed_count",
            "expected_count",
            "rate_ratio",
            "n_pro",
            "n_con",
            "pro_rate",
            "baseline_pro_rate",
            "delta_pro_rate",
            "abs_delta_pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "q_value",
            "is_significant",
        ],
    )
    bursts_tests = _with_expected_columns(
        table_map.get(_table_key("bursts", "burst_window_tests"), pd.DataFrame()),
        [
            "window_minutes",
            "bucket_minutes",
            "rate_ratio",
            "pro_rate",
            "baseline_pro_rate",
            "delta_pro_rate",
            "abs_delta_pro_rate",
            "is_low_power",
            "is_significant",
        ],
    )
    bursts_null = _with_expected_columns(
        table_map.get(_table_key("bursts", "burst_null_distribution"), pd.DataFrame()),
        ["window_minutes", "bucket_minutes", "iteration", "max_window_count"],
    )

    off_hours_hourly = _with_expected_columns(
        table_map.get(_table_key("off_hours", "hourly_distribution"), pd.DataFrame()),
        [
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
    )
    off_hours_date_hour = _with_expected_columns(
        table_map.get(_table_key("off_hours", "date_hour_distribution"), pd.DataFrame()),
        [
            "date",
            "day_of_week",
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "n_known",
            "n_unknown",
            "n_off_hours",
            "off_hours_fraction",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
    )
    off_hours_date_hour_primary_residual = _with_expected_columns(
        table_map.get(
            _table_key("off_hours", "date_hour_primary_residual_distribution"),
            pd.DataFrame(),
        ),
        [
            "bucket_minutes",
            "date",
            "day_of_week",
            "hour",
            "n_windows",
            "n_windows_alert_eligible",
            "n_windows_tested",
            "n_windows_low_power",
            "n_windows_primary_alert",
            "primary_alert_fraction_tested",
            "n_total",
            "n_known",
            "n_known_tested",
            "n_pro",
            "n_con",
            "off_hours_fraction",
            "pro_rate",
            "expected_pro_rate_primary",
            "delta_pro_rate_primary",
            "z_score_primary",
            "z_score_primary_median",
            "z_score_primary_abs_max",
            "is_low_power",
        ],
    )
    off_hours_window_control = _with_expected_columns(
        table_map.get(_table_key("off_hours", "window_control_profile"), pd.DataFrame()),
        [
            "bucket_start",
            "bucket_minutes",
            "event_date_key",
            "day_of_week",
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "n_known",
            "n_unknown",
            "n_off_hours",
            "off_hours_fraction",
            "is_off_hours_window",
            "is_pure_off_hours_window",
            "is_alert_off_hours_window",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "expected_pro_rate_day",
            "expected_pro_rate_model",
            "expected_pro_rate_primary",
            "expected_pro_rate_global",
            "baseline_source",
            "model_baseline_source",
            "primary_baseline_source",
            "is_model_baseline_available",
            "model_fit_method",
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_converged",
            "model_fit_aic",
            "model_fit_used_harmonics",
            "control_low_95_day",
            "control_high_95_day",
            "control_low_998_day",
            "control_high_998_day",
            "control_low_95_model",
            "control_high_95_model",
            "control_low_998_model",
            "control_high_998_model",
            "control_low_95_primary",
            "control_high_95_primary",
            "control_low_998_primary",
            "control_high_998_primary",
            "control_low_95_global",
            "control_high_95_global",
            "control_low_998_global",
            "control_high_998_global",
            "z_score_day",
            "z_score_model",
            "z_score_primary",
            "delta_pro_rate_day",
            "delta_pro_rate_model",
            "delta_pro_rate_primary",
            "p_value_day",
            "p_value_day_two_sided",
            "p_value_day_lower",
            "p_value_day_upper",
            "p_value_model",
            "p_value_model_two_sided",
            "p_value_model_lower",
            "p_value_model_upper",
            "p_value_primary",
            "p_value_primary_two_sided",
            "p_value_primary_lower",
            "p_value_primary_upper",
            "q_value_day",
            "q_value_day_lower",
            "q_value_day_upper",
            "q_value_day_two_sided",
            "q_value_model",
            "q_value_model_lower",
            "q_value_model_upper",
            "q_value_model_two_sided",
            "q_value_primary",
            "q_value_primary_lower",
            "q_value_primary_upper",
            "q_value_primary_two_sided",
            "is_significant_day",
            "is_significant_day_lower",
            "is_significant_day_upper",
            "is_significant_day_two_sided",
            "is_significant_model",
            "is_significant_model_lower",
            "is_significant_model_upper",
            "is_significant_model_two_sided",
            "is_significant_primary",
            "is_significant_primary_lower",
            "is_significant_primary_upper",
            "is_significant_primary_two_sided",
            "is_material_primary_shift",
            "is_material_primary_lower_shift",
            "is_material_primary_upper_shift",
            "is_primary_alert_window",
            "is_primary_lower_alert_window",
            "is_primary_upper_alert_window",
            "is_primary_two_sided_alert_window",
            "is_primary_spc_998_two_sided",
            "is_primary_fdr_two_sided",
            "is_primary_any_flag_channel",
            "is_primary_both_flag_channels",
            "is_below_day_control_95",
            "is_below_day_control_998",
            "is_above_day_control_95",
            "is_above_day_control_998",
            "is_below_model_control_95",
            "is_below_model_control_998",
            "is_above_model_control_95",
            "is_above_model_control_998",
            "is_below_primary_control_95",
            "is_below_primary_control_998",
            "is_above_primary_control_95",
            "is_above_primary_control_998",
            "is_outside_day_control_95",
            "is_outside_day_control_998",
            "is_outside_model_control_95",
            "is_outside_model_control_998",
            "is_outside_primary_control_95",
            "is_outside_primary_control_998",
            "is_below_global_control_95",
            "is_below_global_control_998",
        ],
    )
    off_hours_summary = _with_expected_columns(
        table_map.get(_table_key("off_hours", "off_hours_summary"), pd.DataFrame()),
        [
            "off_hours",
            "on_hours",
            "off_hours_ratio",
            "off_hours_pro_rate",
            "on_hours_pro_rate",
            "off_hours_pro_rate_wilson_low",
            "off_hours_pro_rate_wilson_high",
            "on_hours_pro_rate_wilson_low",
            "on_hours_pro_rate_wilson_high",
            "chi_square_p_value",
            "off_hours_is_low_power",
            "on_hours_is_low_power",
            "primary_bucket_minutes",
            "primary_baseline_method",
            "alert_off_hours_min_fraction",
            "primary_alert_min_abs_delta",
            "off_hours_windows_alert_eligible",
            "off_hours_windows_alert_eligible_low_power",
            "off_hours_windows_alert_eligible_tested_fraction",
            "off_hours_windows_alert_eligible_low_power_fraction",
            "off_hours_windows_tested",
            "off_hours_windows_below_day_control_95",
            "off_hours_windows_below_day_control_998",
            "off_hours_windows_below_model_control_95",
            "off_hours_windows_below_model_control_998",
            "off_hours_windows_below_primary_control_95",
            "off_hours_windows_below_primary_control_998",
            "off_hours_windows_above_primary_control_95",
            "off_hours_windows_above_primary_control_998",
            "off_hours_windows_significant_day",
            "off_hours_windows_significant_model",
            "off_hours_windows_significant_primary",
            "off_hours_windows_significant_primary_upper",
            "off_hours_windows_significant_primary_two_sided",
            "off_hours_windows_primary_spc_998_any",
            "off_hours_windows_primary_fdr_two_sided",
            "off_hours_windows_primary_flag_any",
            "off_hours_windows_primary_flag_both",
            "off_hours_windows_primary_spc_998_any_fraction",
            "off_hours_windows_primary_fdr_two_sided_fraction",
            "off_hours_windows_primary_flag_any_fraction",
            "off_hours_windows_primary_flag_both_fraction",
            "off_hours_windows_primary_alert",
            "off_hours_windows_primary_alert_fraction",
            "off_hours_primary_alert_run_count",
            "off_hours_primary_alert_max_run_windows",
            "off_hours_primary_alert_max_run_minutes",
            "off_hours_min_day_z",
            "off_hours_max_abs_day_z",
            "off_hours_min_model_z",
            "off_hours_max_abs_model_z",
            "off_hours_min_primary_z",
            "off_hours_max_abs_primary_z",
            "off_hours_min_primary_delta",
            "off_hours_max_abs_primary_delta",
            "off_hours_windows_model_available",
            "global_daytime_pro_rate",
            "day_adjusted_fdr_alpha",
            "model_fit_min_rows",
            "model_hour_harmonics",
            "primary_model_fit_method",
            "primary_model_fit_rows",
            "primary_model_fit_unique_days",
            "primary_model_fit_unique_hours",
            "primary_model_fit_converged",
            "primary_model_fit_aic",
        ],
    )
    off_hours_flag_channels = _with_expected_columns(
        table_map.get(_table_key("off_hours", "flag_channel_summary"), pd.DataFrame()),
        [
            "rank",
            "channel",
            "channel_label",
            "count",
            "share_of_tested",
        ],
    )

    dup_exact_methods = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "collision_methods"), pd.DataFrame()),
        [
            "scope",
            "baseline_source",
            "baseline_label",
            "baseline_model",
            "uncertainty_model",
            "n_used",
            "N_used",
            "scope_status",
            "scope_reason",
            "metric_primary",
            "metrics_reported",
            "baseline_degraded",
            "fallback_policy",
            "collision_key_mode",
            "inferential_key_mode",
            "low_power_min_unique_names_scope",
            "low_power_min_expected_duplicates_scope",
            "low_power_min_unique_names_bucket",
            "low_power_min_expected_duplicates_bucket",
            "low_power_min_unique_names_position",
            "low_power_min_expected_duplicates_position",
            "normalization_version",
            "normalization_version_hash",
            "stratification",
            "stratification_weight_source",
            "stratification_leakage_control",
            "stratification_weight_uncertainty",
            "stratification_endogeneity_uncontrolled",
            "historical_reference_channel",
            "historical_reference_report_count",
            "historical_reference_reports_loaded",
            "historical_reference_missing_table_count",
            "historical_reference_excluded_target",
            "historical_reference_target_report_id",
            "historical_reference_reason",
            "historical_reference_loo_source_path",
            "censored",
            "claim_class",
            "inferential_status",
            "inferential_reason",
            "family_id",
            "adjustment_method",
            "n_tests",
            "n_tests_in_family",
            "eligible_by_gate",
            "gate_reason",
            "adjusted_p_value",
            "is_significant",
            "estimand_primary",
            "non_goals",
            "baseline_semantics",
            "rng_root_seed",
            "rng_root_stream_id",
            "rng_stream_scope_collision",
            "rng_stream_scope_stratified_collision",
            "rng_stream_bucket_collision",
            "rng_stream_bucket_stratified_collision",
            "rng_stream_position_interval",
            "rng_stream_position_permutation",
            "rng_stream_position_cluster_bootstrap",
            "rng_stream_temporal_permutation",
        ],
    )
    if not dup_exact_methods.empty:
        dup_exact_methods["baseline_source"] = (
            dup_exact_methods.get("baseline_source", pd.Series(dtype=str)).fillna("").astype(str)
        )
        dup_exact_methods["baseline_label"] = (
            dup_exact_methods.get("baseline_label", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .where(
                dup_exact_methods.get("baseline_label", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
                .str.len()
                > 0,
                dup_exact_methods["baseline_source"].map(_duplicate_baseline_label_for_source),
            )
        )
        dup_exact_methods["inferential_status"] = (
            dup_exact_methods.get("inferential_status", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .where(
                dup_exact_methods.get("inferential_status", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
                .str.len()
                > 0,
                dup_exact_methods["baseline_source"].map(_duplicate_inferential_status_for_source),
            )
        )
        dup_exact_methods["inferential_reason"] = (
            dup_exact_methods.get("inferential_reason", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .where(
                dup_exact_methods.get("inferential_reason", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
                .str.len()
                > 0,
                dup_exact_methods["inferential_status"].map(
                    _duplicate_default_inferential_reason_for_status
                ),
            )
        )
        dup_exact_methods["inferential_key_mode"] = (
            dup_exact_methods.get("inferential_key_mode", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .where(
                dup_exact_methods.get("inferential_key_mode", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
                .str.len()
                > 0,
                dup_exact_methods.get("collision_key_mode", pd.Series(dtype=str))
                .fillna("")
                .astype(str),
            )
            .replace("", "strict")
            .map(lambda value: _normalize_report_match_mode(value, default="strict"))
        )
        dup_exact_methods["scope_status"] = (
            dup_exact_methods.get("scope_status", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .where(
                dup_exact_methods.get("scope_status", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
                .str.len()
                > 0,
                dup_exact_methods.get("scope_reason", pd.Series(dtype=str)).map(
                    _duplicate_default_scope_status_for_reason
                ),
            )
            .replace("", "available")
        )
        dup_exact_methods["scope_reason"] = (
            dup_exact_methods.get("scope_reason", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .where(
                dup_exact_methods.get("scope_reason", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
                .str.len()
                > 0,
                dup_exact_methods["scope_status"].map(_duplicate_default_scope_reason_for_status),
            )
        )
    dup_exact_summary_raw = detector_summaries.get("duplicates_exact", {})
    dup_exact_summary = dup_exact_summary_raw if isinstance(dup_exact_summary_raw, dict) else {}
    dup_exact_statistical_contract_raw = dup_exact_summary.get("statistical_contract", {})
    dup_exact_statistical_contract = (
        dup_exact_statistical_contract_raw
        if isinstance(dup_exact_statistical_contract_raw, dict)
        else {}
    )
    dup_estimand_primary = str(
        dup_exact_statistical_contract.get("estimand_primary")
        or dup_exact_summary.get("estimand_primary")
        or _DUPLICATE_DEFAULT_ESTIMAND_PRIMARY
    )
    dup_non_goals = str(
        dup_exact_statistical_contract.get("non_goals")
        or dup_exact_summary.get("non_goals")
        or _DUPLICATE_DEFAULT_NON_GOALS
    )
    dup_baseline_semantics = str(
        dup_exact_statistical_contract.get("baseline_semantics")
        or dup_exact_summary.get("baseline_semantics")
        or _DUPLICATE_DEFAULT_BASELINE_SEMANTICS
    )
    dup_claim_class = str(
        dup_exact_summary.get("claim_class") or _DUPLICATE_DEFAULT_CLAIM_CLASS
    )
    primary_dup_scope = (
        str(dup_exact_methods["scope"].iloc[0]).strip() if not dup_exact_methods.empty else "full_hearing"
    )
    primary_dup_metric = (
        str(dup_exact_methods["metric_primary"].iloc[0]).strip()
        if not dup_exact_methods.empty
        else "repeated_group_rows"
    )
    primary_dup_unit = "rows_anywhere"
    primary_dup_baseline_source = (
        str(dup_exact_methods.get("baseline_source", pd.Series(dtype=str)).iloc[0]).strip()
        if not dup_exact_methods.empty
        else str(dup_exact_summary.get("baseline_source") or "").strip()
    )
    primary_dup_baseline_label = (
        str(dup_exact_methods.get("baseline_label", pd.Series(dtype=str)).iloc[0]).strip()
        if not dup_exact_methods.empty
        else str(dup_exact_summary.get("baseline_label") or "").strip()
    )
    if not primary_dup_baseline_label:
        primary_dup_baseline_label = _duplicate_baseline_label_for_source(primary_dup_baseline_source)
    primary_dup_inferential_status = (
        str(dup_exact_methods.get("inferential_status", pd.Series(dtype=str)).iloc[0]).strip()
        if not dup_exact_methods.empty
        else str(dup_exact_summary.get("inferential_status") or "").strip()
    )
    if not primary_dup_inferential_status:
        primary_dup_inferential_status = _duplicate_inferential_status_for_source(
            primary_dup_baseline_source
        )
    primary_dup_inferential_reason = (
        str(dup_exact_methods.get("inferential_reason", pd.Series(dtype=str)).iloc[0]).strip()
        if not dup_exact_methods.empty
        else str(dup_exact_summary.get("inferential_reason") or "").strip()
    )
    if not primary_dup_inferential_reason:
        primary_dup_inferential_reason = _duplicate_default_inferential_reason_for_status(
            primary_dup_inferential_status
        )
    primary_dup_scope_status = (
        str(dup_exact_methods.get("scope_status", pd.Series(dtype=str)).iloc[0]).strip()
        if not dup_exact_methods.empty
        else str(dup_exact_summary.get("scope_status") or "").strip()
    )
    if not primary_dup_scope_status:
        primary_dup_scope_status = "available"
    primary_dup_scope_reason = (
        str(dup_exact_methods.get("scope_reason", pd.Series(dtype=str)).iloc[0]).strip()
        if not dup_exact_methods.empty
        else str(dup_exact_summary.get("scope_reason") or "").strip()
    )
    if not primary_dup_scope_reason:
        primary_dup_scope_reason = _duplicate_default_scope_reason_for_status(
            primary_dup_scope_status
        )

    dup_exact_collision_overview = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "collision_overview"), pd.DataFrame()),
        [
            "scope",
            "scope_status",
            "scope_reason",
            "metric",
            "observed",
            "expected",
            "expected_p05",
            "expected_p50",
            "expected_p95",
            "z_score",
            "p_value",
            "monte_carlo_draws_effective",
            "monte_carlo_quantile_resolution",
            "monte_carlo_p_value_mcse",
            "monte_carlo_p_value_ci_low",
            "monte_carlo_p_value_ci_high",
            "n_used",
            "N_used",
            "baseline_source",
            "baseline_label",
            "inferential_status",
            "inferential_reason",
            "family_id",
            "adjustment_method",
            "n_tests",
            "n_tests_in_family",
            "eligible_by_gate",
            "gate_reason",
            "adjusted_p_value",
            "is_significant",
            "claim_class",
        ],
    )
    duplicate_metric_options = ["rows_anywhere", "names_anywhere"]
    primary_dup_match_mode = _normalize_report_match_mode(
        (
            dup_exact_methods.get("inferential_key_mode", pd.Series(dtype=str)).iloc[0]
            if not dup_exact_methods.empty
            else dup_exact_summary.get("inferential_key_mode")
        )
        or (
            dup_exact_methods.get("collision_key_mode", pd.Series(dtype=str)).iloc[0]
            if not dup_exact_methods.empty
            else "strict"
        ),
        default="strict",
    )
    primary_dup_inferential_key_mode = primary_dup_match_mode
    primary_dup_inferential_key_label = _duplicate_match_mode_label(
        primary_dup_inferential_key_mode,
        inferential_key_mode=primary_dup_inferential_key_mode,
    )
    duplicate_match_mode_options: list[str] = []
    dup_scope_metadata_columns = [
        "scope",
        "scope_status",
        "scope_reason",
        "baseline_source",
        "baseline_label",
        "inferential_status",
        "inferential_reason",
        "inferential_key_mode",
        "claim_class",
    ]
    if not dup_exact_methods.empty:
        dup_scope_metadata = (
            dup_exact_methods[dup_scope_metadata_columns]
            .dropna(subset=["scope"])
            .drop_duplicates(subset=["scope"], keep="first")
            .copy()
        )
    else:
        dup_scope_metadata = pd.DataFrame(
            [
                {
                    "scope": primary_dup_scope,
                    "scope_status": primary_dup_scope_status,
                    "scope_reason": primary_dup_scope_reason,
                    "baseline_source": primary_dup_baseline_source,
                    "baseline_label": primary_dup_baseline_label,
                    "inferential_status": primary_dup_inferential_status,
                    "inferential_reason": primary_dup_inferential_reason,
                    "inferential_key_mode": primary_dup_inferential_key_mode,
                    "claim_class": dup_claim_class,
                }
            ]
        )
    if dup_scope_metadata.empty:
        dup_scope_metadata = pd.DataFrame(
            [
                {
                    "scope": primary_dup_scope,
                    "scope_status": primary_dup_scope_status,
                    "scope_reason": primary_dup_scope_reason,
                    "baseline_source": primary_dup_baseline_source,
                    "baseline_label": primary_dup_baseline_label,
                    "inferential_status": primary_dup_inferential_status,
                    "inferential_reason": primary_dup_inferential_reason,
                    "inferential_key_mode": primary_dup_inferential_key_mode,
                    "claim_class": dup_claim_class,
                }
            ]
        )
    dup_scope_metadata["scope"] = (
        dup_scope_metadata["scope"].fillna("").astype(str).replace("", primary_dup_scope)
    )
    dup_scope_metadata["scope_status"] = (
        dup_scope_metadata.get("scope_status", pd.Series(dtype=str))
        .fillna("")
        .astype(str)
        .where(
            dup_scope_metadata.get("scope_status", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .str.len()
            > 0,
            dup_scope_metadata.get("scope_reason", pd.Series(dtype=str)).map(
                _duplicate_default_scope_status_for_reason
            ),
        )
        .replace("", "available")
    )
    dup_scope_metadata["scope_reason"] = (
        dup_scope_metadata.get("scope_reason", pd.Series(dtype=str))
        .fillna("")
        .astype(str)
        .where(
            dup_scope_metadata.get("scope_reason", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .str.len()
            > 0,
            dup_scope_metadata["scope_status"].map(_duplicate_default_scope_reason_for_status),
        )
    )
    dup_scope_metadata["inferential_key_mode"] = (
        dup_scope_metadata.get("inferential_key_mode", pd.Series(dtype=str))
        .fillna("")
        .astype(str)
        .replace("", primary_dup_inferential_key_mode)
        .map(lambda value: _normalize_report_match_mode(value, default=primary_dup_inferential_key_mode))
    )
    duplicate_scope_availability = _records_from_frame(
        dup_scope_metadata[["scope", "scope_status", "scope_reason"]],
        columns=["scope", "scope_status", "scope_reason"],
        max_rows=20,
    )
    available_scope_options = sorted(
        {
            str(value).strip()
            for value in dup_scope_metadata.loc[
                _duplicate_scope_available_mask(dup_scope_metadata), "scope"
            ].tolist()
            if str(value).strip()
        }
    )
    duplicate_scope_options = (
        available_scope_options if available_scope_options else [primary_dup_scope]
    )
    primary_dup_scope_control = (
        primary_dup_scope
        if primary_dup_scope in duplicate_scope_options
        else duplicate_scope_options[0]
    )

    def _attach_duplicate_scope_metadata(frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame
        working = frame.copy()
        if "scope" not in working.columns:
            working["scope"] = primary_dup_scope
        working["scope"] = working["scope"].fillna("").astype(str).replace("", primary_dup_scope)
        metadata = dup_scope_metadata.copy()
        metadata["scope"] = metadata["scope"].fillna("").astype(str).replace("", primary_dup_scope)
        working = working.merge(
            metadata,
            on="scope",
            how="left",
            suffixes=("", "_scope_meta"),
        )
        for column in (
            "scope_status",
            "scope_reason",
            "baseline_source",
            "baseline_label",
            "inferential_status",
            "inferential_reason",
            "inferential_key_mode",
            "claim_class",
        ):
            fallback_column = f"{column}_scope_meta"
            if fallback_column not in working.columns:
                continue
            if column not in working.columns:
                working[column] = working[fallback_column]
            else:
                working[column] = (
                    working[column]
                    .fillna("")
                    .astype(str)
                    .where(
                        working[column].fillna("").astype(str).str.len() > 0,
                        working[fallback_column].fillna("").astype(str),
                    )
                )
            working = working.drop(columns=[fallback_column])
        if "baseline_source" in working.columns:
            working["baseline_source"] = working["baseline_source"].fillna("").astype(str)
        else:
            working["baseline_source"] = primary_dup_baseline_source
        if "scope_status" in working.columns:
            working["scope_status"] = (
                working["scope_status"]
                .fillna("")
                .astype(str)
                .where(
                    working["scope_status"].fillna("").astype(str).str.len() > 0,
                    working.get("scope_reason", pd.Series(dtype=str)).map(
                        _duplicate_default_scope_status_for_reason
                    ),
                )
                .replace("", "available")
            )
        else:
            working["scope_status"] = "available"
        if "scope_reason" in working.columns:
            working["scope_reason"] = (
                working["scope_reason"]
                .fillna("")
                .astype(str)
                .where(
                    working["scope_reason"].fillna("").astype(str).str.len() > 0,
                    working["scope_status"].map(_duplicate_default_scope_reason_for_status),
                )
            )
        else:
            working["scope_reason"] = working["scope_status"].map(
                _duplicate_default_scope_reason_for_status
            )
        if "baseline_label" in working.columns:
            working["baseline_label"] = (
                working["baseline_label"]
                .fillna("")
                .astype(str)
                .where(
                    working["baseline_label"].fillna("").astype(str).str.len() > 0,
                    working["baseline_source"].map(_duplicate_baseline_label_for_source),
                )
            )
        else:
            working["baseline_label"] = working["baseline_source"].map(
                _duplicate_baseline_label_for_source
            )
        if "inferential_status" in working.columns:
            working["inferential_status"] = (
                working["inferential_status"]
                .fillna("")
                .astype(str)
                .where(
                    working["inferential_status"].fillna("").astype(str).str.len() > 0,
                    working["baseline_source"].map(_duplicate_inferential_status_for_source),
                )
            )
        else:
            working["inferential_status"] = working["baseline_source"].map(
                _duplicate_inferential_status_for_source
            )
        if "inferential_reason" in working.columns:
            working["inferential_reason"] = (
                working["inferential_reason"]
                .fillna("")
                .astype(str)
                .where(
                    working["inferential_reason"].fillna("").astype(str).str.len() > 0,
                    working["inferential_status"].map(
                        _duplicate_default_inferential_reason_for_status
                    ),
                )
            )
        else:
            working["inferential_reason"] = working["inferential_status"].map(
                _duplicate_default_inferential_reason_for_status
            )
        if "inferential_key_mode" in working.columns:
            working["inferential_key_mode"] = (
                working["inferential_key_mode"]
                .fillna("")
                .astype(str)
                .replace("", primary_dup_inferential_key_mode)
                .map(
                    lambda value: _normalize_report_match_mode(
                        value,
                        default=primary_dup_inferential_key_mode,
                    )
                )
            )
        else:
            working["inferential_key_mode"] = primary_dup_inferential_key_mode
        if "claim_class" in working.columns:
            working["claim_class"] = (
                working["claim_class"]
                .fillna("")
                .astype(str)
                .where(working["claim_class"].fillna("").astype(str).str.len() > 0, dup_claim_class)
            )
        else:
            working["claim_class"] = dup_claim_class
        return working

    dup_exact_collision_overview = _attach_duplicate_scope_metadata(dup_exact_collision_overview)
    non_inferential_overview = ~_duplicate_inferential_supported_mask(
        dup_exact_collision_overview
    )
    if bool(non_inferential_overview.any()):
        for inferential_column in (
            "expected_p05",
            "expected_p50",
            "expected_p95",
            "z_score",
            "p_value",
            "monte_carlo_quantile_resolution",
            "monte_carlo_p_value_mcse",
            "monte_carlo_p_value_ci_low",
            "monte_carlo_p_value_ci_high",
        ):
            if inferential_column in dup_exact_collision_overview.columns:
                dup_exact_collision_overview.loc[non_inferential_overview, inferential_column] = np.nan
    dup_exact_metric_diagnostics = dup_exact_collision_overview[
        dup_exact_collision_overview["scope"].astype(str).str.len() > 0
    ].copy()

    dup_exact_collision_bucket = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "collision_by_bucket"), pd.DataFrame()),
        [
            "scope",
            "scope_status",
            "scope_reason",
            "metric",
            "bucket_start",
            "bucket_minutes",
            "n_bucket",
            "n_used",
            "N_used",
            "n_unique_names",
            "n_pro",
            "n_con",
            "observed",
            "expected",
            "expected_p05",
            "expected_p95",
            "z_score",
            "p_value",
            "monte_carlo_draws_effective",
            "monte_carlo_quantile_resolution",
            "monte_carlo_p_value_mcse",
            "monte_carlo_p_value_ci_low",
            "monte_carlo_p_value_ci_high",
            "excess",
            "baseline_model",
            "baseline_source",
            "baseline_label",
            "baseline_degraded",
            "is_low_power",
            "inference_status",
            "inferential_status",
            "inferential_reason",
            "family_id",
            "adjustment_method",
            "n_tests",
            "n_tests_in_family",
            "eligible_by_gate",
            "gate_reason",
            "adjusted_p_value",
            "is_significant",
            "claim_class",
        ],
    )
    dup_exact_collision_bucket = _attach_duplicate_scope_metadata(dup_exact_collision_bucket)
    non_inferential_bucket = ~_duplicate_inferential_supported_mask(dup_exact_collision_bucket)
    if bool(non_inferential_bucket.any()):
        for inferential_column in (
            "expected_p05",
            "expected_p95",
            "z_score",
            "p_value",
            "monte_carlo_quantile_resolution",
            "monte_carlo_p_value_mcse",
            "monte_carlo_p_value_ci_low",
            "monte_carlo_p_value_ci_high",
        ):
            if inferential_column in dup_exact_collision_bucket.columns:
                dup_exact_collision_bucket.loc[non_inferential_bucket, inferential_column] = np.nan
    dup_exact_per_name_by_mode = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "per_name_duplicates_by_mode"), pd.DataFrame()),
        [
            "scope",
            "scope_status",
            "scope_reason",
            "match_mode",
            "match_label",
            "match_definition",
            "match_mode_role",
            "inferential_key_mode",
            "display_name",
            "canonical_name",
            "name_key",
            "observed_count",
            "total_repeated_rows",
            "n_pro",
            "n_con",
            "first_seen",
            "last_seen",
            "time_span_minutes",
        ],
    )
    if not dup_exact_per_name_by_mode.empty:
        dup_exact_per_name_by_mode["scope"] = (
            dup_exact_per_name_by_mode.get("scope", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .replace("", primary_dup_scope)
        )
        dup_exact_per_name_by_mode["match_mode"] = (
            dup_exact_per_name_by_mode["match_mode"]
            .map(lambda value: _normalize_report_match_mode(value, default="strict"))
            .astype(str)
        )
        dup_exact_per_name_by_mode["inferential_key_mode"] = (
            dup_exact_per_name_by_mode.get("inferential_key_mode", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .replace("", primary_dup_inferential_key_mode)
            .map(lambda value: _normalize_report_match_mode(value, default=primary_dup_inferential_key_mode))
        )
        dup_exact_per_name_by_mode["match_mode_role"] = (
            dup_exact_per_name_by_mode.get("match_mode_role", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .where(
                dup_exact_per_name_by_mode.get("match_mode_role", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
                .str.len()
                > 0,
                dup_exact_per_name_by_mode["match_mode"].map(
                    lambda mode: _duplicate_match_mode_role(
                        mode,
                        inferential_key_mode=primary_dup_inferential_key_mode,
                    )
                ),
            )
        )
        dup_exact_per_name_by_mode["name_key"] = (
            dup_exact_per_name_by_mode.get("name_key", pd.Series(dtype=str))
            .fillna(dup_exact_per_name_by_mode.get("canonical_name", pd.Series(dtype=str)))
            .fillna(dup_exact_per_name_by_mode.get("display_name", pd.Series(dtype=str)))
            .astype(str)
            .str.strip()
        )
    dup_exact_per_name_by_mode = _attach_duplicate_scope_metadata(dup_exact_per_name_by_mode)
    dup_exact_per_name_timing_by_mode = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "per_name_submission_timing_by_mode"), pd.DataFrame()),
        [
            "scope",
            "scope_status",
            "scope_reason",
            "match_mode",
            "match_label",
            "match_definition",
            "match_mode_role",
            "inferential_key_mode",
            "canonical_name",
            "name_key",
            "display_name",
            "bucket_start",
            "position_normalized",
        ],
    )
    if not dup_exact_per_name_timing_by_mode.empty:
        dup_exact_per_name_timing_by_mode["scope"] = (
            dup_exact_per_name_timing_by_mode.get("scope", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .replace("", primary_dup_scope)
        )
        dup_exact_per_name_timing_by_mode["match_mode"] = (
            dup_exact_per_name_timing_by_mode["match_mode"]
            .map(lambda value: _normalize_report_match_mode(value, default="strict"))
            .astype(str)
        )
        dup_exact_per_name_timing_by_mode["inferential_key_mode"] = (
            dup_exact_per_name_timing_by_mode.get("inferential_key_mode", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .replace("", primary_dup_inferential_key_mode)
            .map(lambda value: _normalize_report_match_mode(value, default=primary_dup_inferential_key_mode))
        )
        dup_exact_per_name_timing_by_mode["match_mode_role"] = (
            dup_exact_per_name_timing_by_mode.get("match_mode_role", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .where(
                dup_exact_per_name_timing_by_mode.get("match_mode_role", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
                .str.len()
                > 0,
                dup_exact_per_name_timing_by_mode["match_mode"].map(
                    lambda mode: _duplicate_match_mode_role(
                        mode,
                        inferential_key_mode=primary_dup_inferential_key_mode,
                    )
                ),
            )
        )
        dup_exact_per_name_timing_by_mode["name_key"] = (
            dup_exact_per_name_timing_by_mode.get("name_key", pd.Series(dtype=str))
            .fillna(dup_exact_per_name_timing_by_mode.get("canonical_name", pd.Series(dtype=str)))
            .fillna(dup_exact_per_name_timing_by_mode.get("display_name", pd.Series(dtype=str)))
            .astype(str)
            .str.strip()
        )
        dup_exact_per_name_timing_by_mode["bucket_start"] = pd.to_datetime(
            dup_exact_per_name_timing_by_mode["bucket_start"], errors="coerce"
        )
        dup_exact_per_name_timing_by_mode = dup_exact_per_name_timing_by_mode.dropna(
            subset=["bucket_start"]
        )
    dup_exact_per_name_timing_by_mode = _attach_duplicate_scope_metadata(
        dup_exact_per_name_timing_by_mode
    )

    # Bucket skeleton comes from collision tables; semantic values are replaced below.
    dup_exact_bucket_skeleton = pd.DataFrame()
    if not dup_exact_collision_bucket.empty:
        working_collision_bucket = dup_exact_collision_bucket.copy()
        metric_column = working_collision_bucket.get("metric", pd.Series("", index=working_collision_bucket.index))
        primary_mask = metric_column.astype(str) == str(primary_dup_metric)
        if bool(primary_mask.any()):
            working_collision_bucket = working_collision_bucket.loc[primary_mask].copy()
        dup_exact_bucket_skeleton = working_collision_bucket.rename(
            columns={
                "n_bucket": "n_rows",
                "observed": "legacy_observed_rows",
                "expected": "legacy_expected_rows",
                "excess": "legacy_deviation_rows",
            }
        )
        dup_exact_bucket_skeleton["scope"] = (
            dup_exact_bucket_skeleton.get("scope", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .replace("", primary_dup_scope)
        )
        dup_exact_bucket_skeleton["bucket_start"] = pd.to_datetime(
            dup_exact_bucket_skeleton.get("bucket_start"), errors="coerce"
        )
        dup_exact_bucket_skeleton = (
            dup_exact_bucket_skeleton.dropna(subset=["bucket_start"])
            .sort_values(["scope", "bucket_minutes", "bucket_start"])
            .drop_duplicates(subset=["scope", "bucket_minutes", "bucket_start"], keep="first")
        )
    if dup_exact_bucket_skeleton.empty:
        dup_exact_bucket_skeleton = _with_expected_columns(
            table_map.get(_table_key("duplicates_exact", "duplicate_by_bucket"), pd.DataFrame()),
            [
                "scope",
                "scope_status",
                "scope_reason",
                "bucket_start",
                "bucket_minutes",
                "n_rows",
                "n_unique_names",
                "n_pro",
                "n_con",
                "duplicate_rows",
                "expected_duplicate_rows",
                "excess_duplicate_rows",
            ],
        )
        if not dup_exact_bucket_skeleton.empty:
            dup_exact_bucket_skeleton = dup_exact_bucket_skeleton.rename(
                columns={
                    "duplicate_rows": "legacy_observed_rows",
                    "expected_duplicate_rows": "legacy_expected_rows",
                    "excess_duplicate_rows": "legacy_deviation_rows",
                }
            )
            dup_exact_bucket_skeleton["scope"] = (
                dup_exact_bucket_skeleton.get("scope", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
                .replace("", primary_dup_scope)
            )
            dup_exact_bucket_skeleton["bucket_start"] = pd.to_datetime(
                dup_exact_bucket_skeleton.get("bucket_start"), errors="coerce"
            )
            dup_exact_bucket_skeleton = dup_exact_bucket_skeleton.dropna(subset=["bucket_start"])
        else:
            legacy_dup_exact_bucket = _with_expected_columns(
                table_map.get(_table_key("duplicates_exact", "repeated_same_bucket"), pd.DataFrame()),
                ["bucket_start", "bucket_minutes", "n", "n_pro", "n_con"],
            )
            if not legacy_dup_exact_bucket.empty:
                dup_exact_bucket_skeleton = (
                    legacy_dup_exact_bucket.groupby(["bucket_start", "bucket_minutes"], dropna=False)
                    .agg(
                        n_rows=("n", "sum"),
                        n_pro=("n_pro", "sum"),
                        n_con=("n_con", "sum"),
                        legacy_observed_rows=("n", "sum"),
                    )
                    .reset_index()
                )
                dup_exact_bucket_skeleton["scope"] = primary_dup_scope
                dup_exact_bucket_skeleton["n_unique_names"] = pd.NA
                dup_exact_bucket_skeleton["legacy_expected_rows"] = pd.NA
                dup_exact_bucket_skeleton["legacy_deviation_rows"] = dup_exact_bucket_skeleton[
                    "legacy_observed_rows"
                ]

    dup_exact_bucket = pd.DataFrame()
    if not dup_exact_bucket_skeleton.empty:
        dup_exact_bucket_skeleton["n_rows"] = pd.to_numeric(
            dup_exact_bucket_skeleton.get("n_rows", pd.Series(dtype=float)),
            errors="coerce",
        ).fillna(0.0)
        for column in ("legacy_observed_rows", "legacy_expected_rows", "legacy_deviation_rows"):
            dup_exact_bucket_skeleton[column] = pd.to_numeric(
                dup_exact_bucket_skeleton.get(column, pd.Series(dtype=float)),
                errors="coerce",
            )
        dup_exact_bucket_skeleton["bucket_minutes"] = pd.to_numeric(
            dup_exact_bucket_skeleton.get("bucket_minutes", pd.Series(dtype=float)),
            errors="coerce",
        ).fillna(0).astype(int)
        for column in ("n_used", "N_used"):
            dup_exact_bucket_skeleton[column] = pd.to_numeric(
                dup_exact_bucket_skeleton.get(column, pd.Series(dtype=float)),
                errors="coerce",
            )

        scope_mode_pairs = pd.DataFrame(columns=["scope", "match_mode"])
        if not dup_exact_per_name_by_mode.empty:
            scope_mode_pairs = pd.concat(
                [
                    scope_mode_pairs,
                    dup_exact_per_name_by_mode[["scope", "match_mode"]].drop_duplicates(),
                ],
                ignore_index=True,
            )
        if not dup_exact_per_name_timing_by_mode.empty:
            scope_mode_pairs = pd.concat(
                [
                    scope_mode_pairs,
                    dup_exact_per_name_timing_by_mode[["scope", "match_mode"]].drop_duplicates(),
                ],
                ignore_index=True,
            )
        if scope_mode_pairs.empty:
            scope_mode_pairs = (
                dup_exact_bucket_skeleton[["scope"]]
                .drop_duplicates()
                .assign(match_mode=primary_dup_match_mode)
            )
        else:
            scope_mode_pairs = (
                scope_mode_pairs.drop_duplicates().sort_values(["scope", "match_mode"])
            )
            missing_scope_rows = (
                dup_exact_bucket_skeleton[["scope"]]
                .drop_duplicates()
                .merge(scope_mode_pairs[["scope"]].drop_duplicates(), on="scope", how="left", indicator=True)
            )
            missing_scope_rows = missing_scope_rows[missing_scope_rows["_merge"] == "left_only"][
                ["scope"]
            ]
            if not missing_scope_rows.empty:
                missing_scope_rows = missing_scope_rows.assign(match_mode=primary_dup_match_mode)
                scope_mode_pairs = pd.concat(
                    [scope_mode_pairs, missing_scope_rows], ignore_index=True
                ).drop_duplicates()

        dup_exact_bucket = dup_exact_bucket_skeleton.merge(
            scope_mode_pairs,
            on="scope",
            how="left",
        )
        dup_exact_bucket["match_mode"] = (
            dup_exact_bucket.get("match_mode", pd.Series(dtype=str))
            .fillna(primary_dup_match_mode)
            .map(lambda value: _normalize_report_match_mode(value, default="strict"))
            .astype(str)
        )
        dup_exact_bucket["inferential_key_mode"] = (
            dup_exact_bucket.get("inferential_key_mode", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .replace("", primary_dup_inferential_key_mode)
            .map(lambda value: _normalize_report_match_mode(value, default=primary_dup_inferential_key_mode))
        )
        dup_exact_bucket["match_mode_role"] = dup_exact_bucket["match_mode"].map(
            lambda mode: _duplicate_match_mode_role(
                mode,
                inferential_key_mode=primary_dup_inferential_key_mode,
            )
        )

        dup_exact_timing_bucket = pd.DataFrame()
        if not dup_exact_per_name_timing_by_mode.empty:
            timing_group_source = dup_exact_per_name_timing_by_mode[
                dup_exact_per_name_timing_by_mode["name_key"].astype(str).str.len() > 0
            ][["scope", "match_mode", "name_key", "bucket_start"]].copy()
            bucket_minute_values = sorted(
                {
                    int(value)
                    for value in pd.to_numeric(
                        dup_exact_bucket.get("bucket_minutes", pd.Series(dtype=float)),
                        errors="coerce",
                    ).dropna()
                    if int(value) > 0
                }
            )
            timing_bucket_frames: list[pd.DataFrame] = []
            for bucket_minutes in bucket_minute_values:
                timed = timing_group_source.assign(
                    bucket_minutes=int(bucket_minutes),
                    bucket_start=timing_group_source["bucket_start"].dt.floor(f"{int(bucket_minutes)}min"),
                ).dropna(subset=["bucket_start"])
                if timed.empty:
                    continue
                grouped = (
                    timed.groupby(
                        ["scope", "match_mode", "bucket_minutes", "bucket_start"],
                        dropna=False,
                    )
                    .agg(
                        unit_observed_rows=("name_key", "size"),
                        unit_observed_names=("name_key", "nunique"),
                    )
                    .reset_index()
                )
                timing_bucket_frames.append(grouped)
            if timing_bucket_frames:
                dup_exact_timing_bucket = pd.concat(timing_bucket_frames, ignore_index=True)

        if not dup_exact_timing_bucket.empty:
            dup_exact_bucket = dup_exact_bucket.merge(
                dup_exact_timing_bucket,
                on=["scope", "match_mode", "bucket_minutes", "bucket_start"],
                how="left",
            )
        else:
            dup_exact_bucket["unit_observed_rows"] = pd.NA
            dup_exact_bucket["unit_observed_names"] = pd.NA

        dup_exact_global_totals = pd.DataFrame(
            columns=[
                "scope",
                "match_mode",
                "global_duplicated_rows",
                "global_duplicated_names",
            ]
        )
        dup_exact_name_multiplicities = pd.DataFrame(
            columns=["scope", "match_mode", "name_key", "name_count"]
        )
        if not dup_exact_per_name_by_mode.empty:
            total_repeated_rows = pd.to_numeric(
                dup_exact_per_name_by_mode.get(
                    "total_repeated_rows",
                    dup_exact_per_name_by_mode.get("observed_count", pd.Series(dtype=float)),
                ),
                errors="coerce",
            ).fillna(0.0)
            per_name_global = dup_exact_per_name_by_mode.assign(
                _global_duplicated_rows=total_repeated_rows,
                _name_key=dup_exact_per_name_by_mode["name_key"].astype(str).str.strip(),
            )
            per_name_global = per_name_global[per_name_global["_name_key"].str.len() > 0]
            if not per_name_global.empty:
                dup_exact_name_multiplicities = (
                    per_name_global[
                        ["scope", "match_mode", "_name_key", "_global_duplicated_rows"]
                    ]
                    .rename(
                        columns={
                            "_name_key": "name_key",
                            "_global_duplicated_rows": "name_count",
                        }
                    )
                    .copy()
                )
                dup_exact_global_totals = (
                    per_name_global.groupby(["scope", "match_mode"], dropna=False)
                    .agg(
                        global_duplicated_rows=("_global_duplicated_rows", "sum"),
                        global_duplicated_names=("_name_key", "nunique"),
                    )
                    .reset_index()
                )
        if not dup_exact_per_name_timing_by_mode.empty:
            timing_global = (
                dup_exact_per_name_timing_by_mode[
                    dup_exact_per_name_timing_by_mode["name_key"].astype(str).str.len() > 0
                ]
                .groupby(["scope", "match_mode"], dropna=False)
                .agg(
                    global_duplicated_rows_timing=("name_key", "size"),
                    global_duplicated_names_timing=("name_key", "nunique"),
                )
                .reset_index()
            )
            timing_name_multiplicities = (
                dup_exact_per_name_timing_by_mode[
                    dup_exact_per_name_timing_by_mode["name_key"].astype(str).str.len() > 0
                ]
                .groupby(["scope", "match_mode", "name_key"], dropna=False)
                .size()
                .rename("name_count_timing")
                .reset_index()
            )
            if dup_exact_name_multiplicities.empty:
                dup_exact_name_multiplicities = timing_name_multiplicities.rename(
                    columns={"name_count_timing": "name_count"}
                )
            elif not timing_name_multiplicities.empty:
                dup_exact_name_multiplicities = (
                    dup_exact_name_multiplicities.merge(
                        timing_name_multiplicities,
                        on=["scope", "match_mode", "name_key"],
                        how="outer",
                    )
                    .assign(
                        name_count=lambda frame: pd.to_numeric(
                            frame.get("name_count", pd.Series(dtype=float)),
                            errors="coerce",
                        ).fillna(
                            pd.to_numeric(
                                frame.get("name_count_timing", pd.Series(dtype=float)),
                                errors="coerce",
                            )
                        )
                    )
                    .drop(columns=["name_count_timing"], errors="ignore")
                )
            if dup_exact_global_totals.empty:
                dup_exact_global_totals = timing_global.rename(
                    columns={
                        "global_duplicated_rows_timing": "global_duplicated_rows",
                        "global_duplicated_names_timing": "global_duplicated_names",
                    }
                )
            else:
                dup_exact_global_totals = dup_exact_global_totals.merge(
                    timing_global,
                    on=["scope", "match_mode"],
                    how="outer",
                )
                dup_exact_global_totals["global_duplicated_rows"] = pd.to_numeric(
                    dup_exact_global_totals.get("global_duplicated_rows", pd.Series(dtype=float)),
                    errors="coerce",
                ).fillna(
                    pd.to_numeric(
                        dup_exact_global_totals.get(
                            "global_duplicated_rows_timing", pd.Series(dtype=float)
                        ),
                        errors="coerce",
                    )
                )
                dup_exact_global_totals["global_duplicated_names"] = pd.to_numeric(
                    dup_exact_global_totals.get("global_duplicated_names", pd.Series(dtype=float)),
                    errors="coerce",
                ).fillna(
                    pd.to_numeric(
                        dup_exact_global_totals.get(
                            "global_duplicated_names_timing", pd.Series(dtype=float)
                        ),
                        errors="coerce",
                    )
                )
                dup_exact_global_totals = dup_exact_global_totals[
                    ["scope", "match_mode", "global_duplicated_rows", "global_duplicated_names"]
                ]

        if not dup_exact_global_totals.empty:
            dup_exact_bucket = dup_exact_bucket.merge(
                dup_exact_global_totals,
                on=["scope", "match_mode"],
                how="left",
            )
        else:
            dup_exact_bucket["global_duplicated_rows"] = pd.NA
            dup_exact_bucket["global_duplicated_names"] = pd.NA

        scope_rows_from_overview = (
            dup_exact_collision_overview[
                dup_exact_collision_overview.get("metric", pd.Series(dtype=str)).astype(str)
                == str(primary_dup_metric)
            ][["scope", "observed"]]
            .dropna(subset=["scope"])
            .drop_duplicates(subset=["scope"], keep="first")
            .set_index("scope")["observed"]
            .to_dict()
            if not dup_exact_collision_overview.empty
            else {}
        )
        dup_exact_bucket["global_duplicated_rows"] = pd.to_numeric(
            dup_exact_bucket.get("global_duplicated_rows", pd.Series(dtype=float)),
            errors="coerce",
        )
        if scope_rows_from_overview:
            dup_exact_bucket["global_duplicated_rows"] = dup_exact_bucket[
                "global_duplicated_rows"
            ].fillna(
                dup_exact_bucket.get("scope", pd.Series(dtype=str)).map(scope_rows_from_overview)
            )
        global_rows_available = dup_exact_bucket["global_duplicated_rows"].notna()
        dup_exact_bucket["global_duplicated_rows"] = dup_exact_bucket[
            "global_duplicated_rows"
        ].fillna(0.0)
        dup_exact_bucket["global_duplicated_names"] = pd.to_numeric(
            dup_exact_bucket.get("global_duplicated_names", pd.Series(dtype=float)),
            errors="coerce",
        )
        global_names_available = dup_exact_bucket["global_duplicated_names"].notna()
        dup_exact_bucket["global_duplicated_names"] = dup_exact_bucket[
            "global_duplicated_names"
        ].fillna(0.0)

        scope_n_used_map: dict[str, float] = {}
        if not dup_exact_methods.empty:
            methods_scope = dup_exact_methods[["scope", "n_used"]].copy()
            methods_scope["scope"] = (
                methods_scope["scope"].fillna("").astype(str).replace("", primary_dup_scope)
            )
            methods_scope["n_used"] = pd.to_numeric(methods_scope["n_used"], errors="coerce")
            methods_scope = methods_scope.dropna(subset=["n_used"])
            methods_scope = methods_scope[methods_scope["n_used"] > 0]
            if not methods_scope.empty:
                scope_n_used_map.update(
                    methods_scope.drop_duplicates(subset=["scope"], keep="first")
                    .set_index("scope")["n_used"]
                    .to_dict()
                )
        if "n_used" in dup_exact_bucket.columns:
            bucket_scope = dup_exact_bucket[["scope", "n_used"]].copy()
            bucket_scope["scope"] = (
                bucket_scope["scope"].fillna("").astype(str).replace("", primary_dup_scope)
            )
            bucket_scope["n_used"] = pd.to_numeric(bucket_scope["n_used"], errors="coerce")
            bucket_scope = bucket_scope.dropna(subset=["n_used"])
            bucket_scope = bucket_scope[bucket_scope["n_used"] > 0]
            if not bucket_scope.empty:
                for scope_value, n_used_value in (
                    bucket_scope.drop_duplicates(subset=["scope"], keep="first")
                    .set_index("scope")["n_used"]
                    .to_dict()
                    .items()
                ):
                    scope_n_used_map.setdefault(str(scope_value), float(n_used_value))

        total_rows_in_scope = pd.to_numeric(
            dup_exact_bucket.get("n_used", pd.Series(dtype=float)),
            errors="coerce",
        )
        if scope_n_used_map:
            total_rows_in_scope = total_rows_in_scope.fillna(
                dup_exact_bucket.get("scope", pd.Series(dtype=str)).map(scope_n_used_map)
            )
        scope_bucket_volume = (
            dup_exact_bucket.groupby("scope", dropna=False)["n_rows"].sum(min_count=1).to_dict()
        )
        total_rows_in_scope = total_rows_in_scope.fillna(
            dup_exact_bucket.get("scope", pd.Series(dtype=str)).map(scope_bucket_volume)
        ).fillna(0.0)
        total_rows_in_scope = pd.to_numeric(total_rows_in_scope, errors="coerce").fillna(0.0)

        dup_exact_bucket["unit_observed_rows"] = pd.to_numeric(
            dup_exact_bucket.get("unit_observed_rows", pd.Series(dtype=float)),
            errors="coerce",
        ).fillna(
            pd.to_numeric(
                dup_exact_bucket.get("legacy_observed_rows", pd.Series(dtype=float)),
                errors="coerce",
            )
        )
        dup_exact_bucket["unit_observed_rows"] = dup_exact_bucket["unit_observed_rows"].fillna(0.0)
        dup_exact_bucket["unit_observed_names"] = pd.to_numeric(
            dup_exact_bucket.get("unit_observed_names", pd.Series(dtype=float)),
            errors="coerce",
        ).fillna(0.0)

        n_rows_numeric = pd.to_numeric(
            dup_exact_bucket.get("n_rows", pd.Series(dtype=float)),
            errors="coerce",
        ).fillna(0.0)
        global_rows_numeric = pd.to_numeric(
            dup_exact_bucket.get("global_duplicated_rows", pd.Series(dtype=float)),
            errors="coerce",
        ).fillna(0.0)
        global_names_numeric = pd.to_numeric(
            dup_exact_bucket.get("global_duplicated_names", pd.Series(dtype=float)),
            errors="coerce",
        ).fillna(0.0)

        expected_rows = pd.Series(np.nan, index=dup_exact_bucket.index, dtype=float)
        expected_names = pd.Series(np.nan, index=dup_exact_bucket.index, dtype=float)
        expected_names_method = pd.Series("", index=dup_exact_bucket.index, dtype=str)
        valid_total_mask = total_rows_in_scope > 0
        valid_rows_mask = valid_total_mask & global_rows_available
        valid_names_mask = valid_total_mask & global_names_available
        if bool(valid_rows_mask.any()):
            expected_rows.loc[valid_rows_mask] = (
                n_rows_numeric.loc[valid_rows_mask]
                * global_rows_numeric.loc[valid_rows_mask]
                / total_rows_in_scope.loc[valid_rows_mask]
            )

        occupancy_profiles: dict[tuple[str, str], dict[str, np.ndarray]] = {}
        if not dup_exact_name_multiplicities.empty:
            multiplicities = dup_exact_name_multiplicities.copy()
            multiplicities["scope"] = (
                multiplicities.get("scope", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
                .replace("", primary_dup_scope)
            )
            multiplicities["match_mode"] = (
                multiplicities.get("match_mode", pd.Series(dtype=str))
                .fillna(primary_dup_match_mode)
                .map(lambda value: _normalize_report_match_mode(value, default="strict"))
                .astype(str)
            )
            multiplicities["name_key"] = (
                multiplicities.get("name_key", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
            )
            multiplicities = multiplicities[multiplicities["name_key"].str.len() > 0]
            multiplicities["name_count"] = pd.to_numeric(
                multiplicities.get("name_count", pd.Series(dtype=float)),
                errors="coerce",
            ).fillna(0.0)
            multiplicities = multiplicities[multiplicities["name_count"] > 0.0]
            if not multiplicities.empty:
                # Occupancy baseline profile: duplicated-name multiplicity distribution per scope/match mode.
                for (scope_value, match_mode_value), group in multiplicities.groupby(
                    ["scope", "match_mode"], dropna=False
                ):
                    frequency = (
                        group["name_count"]
                        .round()
                        .astype(int)
                        .value_counts(dropna=False)
                        .sort_index()
                    )
                    if frequency.empty:
                        continue
                    occupancy_profiles[(str(scope_value), str(match_mode_value))] = {
                        "count_values": frequency.index.to_numpy(dtype=int, copy=False),
                        "name_frequencies": frequency.to_numpy(dtype=float, copy=False),
                    }
                if occupancy_profiles:
                    LOGGER.debug(
                        "duplicates_exact payload occupancy profiles prepared: %s",
                        len(occupancy_profiles),
                    )

        if bool(valid_names_mask.any()):
            occupancy_cache: dict[tuple[str, str, int], dict[int, float]] = {}
            occupancy_inputs = pd.DataFrame(
                {
                    "scope": dup_exact_bucket.get("scope", pd.Series(dtype=str)).fillna("").astype(str),
                    "match_mode": (
                        dup_exact_bucket.get("match_mode", pd.Series(dtype=str))
                        .fillna(primary_dup_match_mode)
                        .map(lambda value: _normalize_report_match_mode(value, default="strict"))
                        .astype(str)
                    ),
                    "n_bucket": pd.to_numeric(n_rows_numeric, errors="coerce").fillna(0.0),
                    "n_population": pd.to_numeric(total_rows_in_scope, errors="coerce").fillna(0.0),
                },
                index=dup_exact_bucket.index,
            ).loc[valid_names_mask]
            if not occupancy_inputs.empty and occupancy_profiles:
                occupancy_inputs["n_bucket_int"] = (
                    occupancy_inputs["n_bucket"].round().astype(int).clip(lower=0)
                )
                occupancy_inputs["n_population_int"] = (
                    occupancy_inputs["n_population"].round().astype(int).clip(lower=0)
                )
                occupancy_inputs = occupancy_inputs[occupancy_inputs["n_population_int"] > 0]
                for (
                    scope_value,
                    match_mode_value,
                    n_population_value,
                    n_bucket_value,
                ), group in occupancy_inputs.groupby(
                    ["scope", "match_mode", "n_population_int", "n_bucket_int"], dropna=False
                ):
                    profile = occupancy_profiles.get((str(scope_value), str(match_mode_value)))
                    if profile is None:
                        continue
                    cache_key = (str(scope_value), str(match_mode_value), int(n_population_value))
                    per_n_cache = occupancy_cache.setdefault(cache_key, {})
                    expected_value = _expected_distinct_names_from_occupancy(
                        n_rows=int(n_bucket_value),
                        n_population=int(n_population_value),
                        count_values=profile["count_values"],
                        name_frequencies=profile["name_frequencies"],
                        cache=per_n_cache,
                    )
                    expected_names.loc[group.index] = expected_value
                    expected_names_method.loc[group.index] = "occupancy_without_replacement"
            unresolved_names_mask = valid_names_mask & expected_names.isna()
            if bool(unresolved_names_mask.any()):
                expected_names.loc[unresolved_names_mask] = (
                    n_rows_numeric.loc[unresolved_names_mask]
                    * global_names_numeric.loc[unresolved_names_mask]
                    / total_rows_in_scope.loc[unresolved_names_mask]
                )
                expected_names_method.loc[unresolved_names_mask] = (
                    "row_share_fallback_missing_multiplicity"
                )

        expected_rows = expected_rows.fillna(
            pd.to_numeric(
                dup_exact_bucket.get("legacy_expected_rows", pd.Series(dtype=float)),
                errors="coerce",
            )
        ).fillna(0.0)
        expected_names = expected_names.fillna(0.0)
        expected_names_method = expected_names_method.where(
            expected_names_method.str.len() > 0, "row_share_fallback_missing_multiplicity"
        )

        dup_exact_bucket["unit_expected_rows"] = expected_rows
        dup_exact_bucket["unit_expected_names"] = expected_names
        dup_exact_bucket["unit_expected_names_method"] = expected_names_method
        dup_exact_bucket["unit_deviation_rows"] = (
            dup_exact_bucket["unit_observed_rows"] - dup_exact_bucket["unit_expected_rows"]
        )
        dup_exact_bucket["unit_deviation_names"] = (
            dup_exact_bucket["unit_observed_names"] - dup_exact_bucket["unit_expected_names"]
        )

        rows_unit = dup_exact_bucket.copy()
        rows_unit["metric"] = "rows_anywhere"
        rows_unit["duplicate_rows"] = rows_unit["unit_observed_rows"]
        rows_unit["expected_duplicate_rows"] = rows_unit["unit_expected_rows"]
        rows_unit["excess_duplicate_rows"] = rows_unit["unit_deviation_rows"]
        rows_unit["report_baseline_family"] = "report_proportional_share"
        rows_unit["report_baseline_label"] = "Report-layer proportional-share baseline"
        rows_unit["report_baseline_method"] = "row_volume_share"
        rows_unit["report_baseline_method_label"] = (
            "Report expected rows use row-volume share "
            "(bucket rows * hearing duplicated-row share)."
        )
        rows_unit["duplicate_row_rate"] = (
            rows_unit["duplicate_rows"] / rows_unit["n_rows"]
        ).where(rows_unit["n_rows"] > 0, 0.0)

        names_unit = dup_exact_bucket.copy()
        names_unit["metric"] = "names_anywhere"
        names_unit["duplicate_rows"] = names_unit["unit_observed_names"]
        names_unit["expected_duplicate_rows"] = names_unit["unit_expected_names"]
        names_unit["excess_duplicate_rows"] = names_unit["unit_deviation_names"]
        names_unit["report_baseline_family"] = names_unit["unit_expected_names_method"].map(
            lambda value: (
                "report_occupancy_multiplicity"
                if str(value).strip().lower() == "occupancy_without_replacement"
                else "report_proportional_share_fallback"
            )
        )
        names_unit["report_baseline_label"] = names_unit["report_baseline_family"].map(
            lambda family: (
                "Report-layer occupancy baseline"
                if family == "report_occupancy_multiplicity"
                else "Report-layer proportional-share fallback baseline"
            )
        )
        names_unit["report_baseline_method"] = names_unit["unit_expected_names_method"]
        names_unit["report_baseline_method_label"] = names_unit["unit_expected_names_method"].map(
            lambda method: (
                "Report expected names use occupancy "
                "(sum over names: 1 - C(N-c, n) / C(N, n))."
                if str(method).strip().lower() == "occupancy_without_replacement"
                else "Report expected names fell back to row-volume share "
                "(multiplicity profile unavailable)."
            )
        )
        names_unit["duplicate_row_rate"] = (
            names_unit["duplicate_rows"] / names_unit["n_rows"]
        ).where(names_unit["n_rows"] > 0, 0.0)

        dup_exact_bucket = pd.concat([rows_unit, names_unit], ignore_index=True, sort=False)
        dup_exact_bucket = _attach_duplicate_scope_metadata(dup_exact_bucket)
        dup_exact_bucket["detector_baseline_family"] = (
            dup_exact_bucket.get("baseline_source", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .map(_duplicate_detector_baseline_family_for_source)
        )
        dup_exact_bucket["detector_baseline_family_label"] = dup_exact_bucket[
            "detector_baseline_family"
        ].map(_duplicate_detector_baseline_family_label)
        dup_exact_bucket["detector_baseline_label"] = (
            dup_exact_bucket.get("baseline_label", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
        )
        dup_exact_bucket["detector_baseline_label"] = dup_exact_bucket[
            "detector_baseline_label"
        ].where(
            dup_exact_bucket["detector_baseline_label"].str.len() > 0,
            dup_exact_bucket.get("baseline_source", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .map(_duplicate_baseline_label_for_source),
        )

        numeric_columns = [
            "n_rows",
            "duplicate_rows",
            "duplicate_row_rate",
            "expected_duplicate_rows",
            "excess_duplicate_rows",
            "unit_observed_rows",
            "unit_expected_rows",
            "unit_deviation_rows",
            "unit_observed_names",
            "unit_expected_names",
            "unit_deviation_names",
        ]
        for column in numeric_columns:
            dup_exact_bucket[column] = pd.to_numeric(
                dup_exact_bucket.get(column, pd.Series(dtype=float)),
                errors="coerce",
            ).fillna(0.0)

    dup_exact_per_name_anomalies = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "per_name_anomalies"), pd.DataFrame()),
        [
            "scope",
            "scope_status",
            "scope_reason",
            "inferential_status",
            "inferential_reason",
            "family_id",
            "adjustment_method",
            "n_tests",
            "n_tests_in_family",
            "eligible_by_gate",
            "gate_reason",
            "adjusted_p_value",
            "match_mode",
            "display_name",
            "canonical_name",
            "n",
            "n_pro",
            "n_con",
            "first_seen",
            "last_seen",
            "time_span_minutes",
            "expected_count",
            "p_value",
            "q_value",
            "is_significant",
            "within_5m_pairs",
            "within_15m_pairs",
            "temporal_p_value_within_5m",
            "temporal_p_value_min_gap",
        ],
    )
    if not dup_exact_per_name_anomalies.empty:
        dup_exact_per_name_anomalies["match_mode"] = dup_exact_per_name_anomalies["match_mode"].map(
            lambda value: _normalize_report_match_mode(value, default="strict")
        )
        dup_exact_per_name_anomalies["scope"] = (
            dup_exact_per_name_anomalies.get("scope", pd.Series(dtype=str)).fillna("").astype(str)
        )
        if not (dup_exact_per_name_anomalies["scope"].astype(str).str.len() > 0).any():
            dup_exact_per_name_anomalies["scope"] = primary_dup_scope
    dup_exact_per_name_anomalies = _attach_duplicate_scope_metadata(dup_exact_per_name_anomalies)

    if not dup_exact_per_name_by_mode.empty:
        dup_exact_per_name = dup_exact_per_name_by_mode.rename(
            columns={
                "observed_count": "n",
            }
        ).copy()
        if not dup_exact_per_name_anomalies.empty:
            anomaly_columns = [
                "scope",
                "inferential_status",
                "inferential_reason",
                "family_id",
                "adjustment_method",
                "n_tests",
                "n_tests_in_family",
                "eligible_by_gate",
                "gate_reason",
                "adjusted_p_value",
                "match_mode",
                "canonical_name",
                "display_name",
                "expected_count",
                "p_value",
                "q_value",
                "is_significant",
                "within_5m_pairs",
                "within_15m_pairs",
                "temporal_p_value_within_5m",
                "temporal_p_value_min_gap",
            ]
            anomaly_subset = dup_exact_per_name_anomalies[anomaly_columns].copy()
            dup_exact_per_name = dup_exact_per_name.merge(
                anomaly_subset,
                on=["scope", "match_mode", "canonical_name"],
                how="left",
                suffixes=("", "_anomaly"),
            )
            if "display_name_anomaly" in dup_exact_per_name.columns:
                dup_exact_per_name["display_name"] = dup_exact_per_name["display_name"].where(
                    dup_exact_per_name["display_name"].astype(str).str.strip() != "",
                    dup_exact_per_name["display_name_anomaly"],
                )
                dup_exact_per_name = dup_exact_per_name.drop(columns=["display_name_anomaly"])
    elif not dup_exact_per_name_anomalies.empty:
        dup_exact_per_name = dup_exact_per_name_anomalies.copy()
    else:
        per_name_display = _with_expected_columns(
            table_map.get(_table_key("duplicates_exact", "per_name_display"), pd.DataFrame()),
            [
                "scope",
                "scope_status",
                "scope_reason",
                "inferential_status",
                "inferential_reason",
                "family_id",
                "adjustment_method",
                "n_tests",
                "n_tests_in_family",
                "eligible_by_gate",
                "gate_reason",
                "adjusted_p_value",
                "display_name",
                "canonical_name",
                "observed_count",
                "n_pro",
                "n_con",
                "time_span_minutes",
                "expected_count",
                "p_value",
                "q_value",
                "is_significant",
            ],
        )
        if not per_name_display.empty:
            per_name_display = per_name_display[
                per_name_display["scope"].astype(str) == primary_dup_scope
            ].copy()
            dup_exact_per_name = per_name_display.rename(columns={"observed_count": "n"})
            dup_exact_per_name["match_mode"] = "strict"
        else:
            dup_exact_per_name = _with_expected_columns(
                table_map.get(_table_key("duplicates_exact", "top_repeated_names"), pd.DataFrame()),
                ["display_name", "canonical_name", "n", "n_pro", "n_con", "time_span_minutes"],
            )
            dup_exact_per_name["scope"] = primary_dup_scope
            dup_exact_per_name["match_mode"] = "strict"
    if "scope" not in dup_exact_per_name.columns:
        dup_exact_per_name["scope"] = primary_dup_scope
    dup_exact_per_name["scope"] = dup_exact_per_name["scope"].fillna("").astype(str).replace("", primary_dup_scope)
    dup_exact_per_name["match_mode"] = dup_exact_per_name.get(
        "match_mode", pd.Series("strict", index=dup_exact_per_name.index)
    ).map(lambda value: _normalize_report_match_mode(value, default="strict"))
    dup_exact_per_name["inferential_key_mode"] = (
        dup_exact_per_name.get("inferential_key_mode", pd.Series(dtype=str))
        .fillna("")
        .astype(str)
        .replace("", primary_dup_inferential_key_mode)
        .map(lambda value: _normalize_report_match_mode(value, default=primary_dup_inferential_key_mode))
    )
    dup_exact_per_name["match_mode_role"] = dup_exact_per_name["match_mode"].map(
        lambda mode: _duplicate_match_mode_role(
            mode,
            inferential_key_mode=primary_dup_inferential_key_mode,
        )
    )
    dup_exact_per_name["expected_count"] = dup_exact_per_name.get("expected_count", pd.Series(dtype=float))
    dup_exact_per_name["p_value"] = dup_exact_per_name.get("p_value", pd.Series(dtype=float)).fillna(pd.NA)
    dup_exact_per_name["q_value"] = dup_exact_per_name.get("q_value", pd.Series(dtype=float)).fillna(pd.NA)
    is_significant_series = (
        dup_exact_per_name["is_significant"]
        if "is_significant" in dup_exact_per_name.columns
        else pd.Series(pd.NA, index=dup_exact_per_name.index, dtype="object")
    )
    dup_exact_per_name["is_significant"] = (
        pd.to_numeric(is_significant_series, errors="coerce").fillna(0).astype(bool)
    )
    dup_exact_per_name["is_significant"] = dup_exact_per_name["is_significant"].astype("object")
    dup_exact_per_name["within_5m_pairs"] = pd.to_numeric(
        dup_exact_per_name.get(
            "within_5m_pairs",
            pd.Series(pd.NA, index=dup_exact_per_name.index),
        ),
        errors="coerce",
    ).fillna(0)
    dup_exact_per_name["within_15m_pairs"] = pd.to_numeric(
        dup_exact_per_name.get(
            "within_15m_pairs",
            pd.Series(pd.NA, index=dup_exact_per_name.index),
        ),
        errors="coerce",
    ).fillna(0)
    dup_exact_per_name["temporal_p_value_within_5m"] = dup_exact_per_name.get(
        "temporal_p_value_within_5m", pd.Series(pd.NA, index=dup_exact_per_name.index)
    )
    dup_exact_per_name["temporal_p_value_min_gap"] = dup_exact_per_name.get(
        "temporal_p_value_min_gap", pd.Series(pd.NA, index=dup_exact_per_name.index)
    )
    dup_exact_per_name = _attach_duplicate_scope_metadata(dup_exact_per_name)
    non_inferential_per_name = ~_duplicate_inferential_supported_mask(dup_exact_per_name)
    if bool(non_inferential_per_name.any()):
        for inferential_column in (
            "p_value",
            "q_value",
            "is_significant",
            "temporal_p_value_within_5m",
            "temporal_p_value_min_gap",
        ):
            if inferential_column in dup_exact_per_name.columns:
                dup_exact_per_name.loc[non_inferential_per_name, inferential_column] = pd.NA
    dup_exact_top_name_timing = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "top_name_timing_by_mode"), pd.DataFrame()),
        [
            "scope",
            "match_mode",
            "match_label",
            "match_definition",
            "match_mode_role",
            "inferential_key_mode",
            "rank",
            "name_key",
            "display_name",
            "total_repeated_rows",
            "bucket_start",
            "bucket_minutes",
            "duplicate_rows",
            "n_pro",
            "n_con",
            "n_other",
            "first_seen",
            "last_seen",
        ],
    )
    if not dup_exact_top_name_timing.empty:
        dup_exact_top_name_timing["match_mode"] = dup_exact_top_name_timing["match_mode"].map(
            lambda value: _normalize_report_match_mode(value, default="strict")
        )
        dup_exact_top_name_timing["inferential_key_mode"] = (
            dup_exact_top_name_timing.get("inferential_key_mode", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .replace("", primary_dup_inferential_key_mode)
            .map(lambda value: _normalize_report_match_mode(value, default=primary_dup_inferential_key_mode))
        )
        dup_exact_top_name_timing["match_mode_role"] = (
            dup_exact_top_name_timing.get("match_mode_role", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .where(
                dup_exact_top_name_timing.get("match_mode_role", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
                .str.len()
                > 0,
                dup_exact_top_name_timing["match_mode"].map(
                    lambda mode: _duplicate_match_mode_role(
                        mode,
                        inferential_key_mode=primary_dup_inferential_key_mode,
                    )
                ),
            )
        )
        dup_exact_top_name_timing["scope"] = (
            dup_exact_top_name_timing.get("scope", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .replace("", primary_dup_scope)
        )
    dup_exact_top_name_timing = _attach_duplicate_scope_metadata(dup_exact_top_name_timing)
    duplicate_match_mode_options = sorted(
        {
            _normalize_report_match_mode(value, default="strict")
            for value in [
                *dup_exact_top_name_timing.get("match_mode", pd.Series(dtype=str)).tolist(),
                *dup_exact_per_name.get("match_mode", pd.Series(dtype=str)).tolist(),
                *dup_exact_per_name_by_mode.get("match_mode", pd.Series(dtype=str)).tolist(),
                *dup_exact_per_name_timing_by_mode.get("match_mode", pd.Series(dtype=str)).tolist(),
                *dup_exact_bucket.get("match_mode", pd.Series(dtype=str)).tolist(),
            ]
            if str(value).strip()
        }
    )
    duplicate_match_mode_options = [
        value for value in duplicate_match_mode_options if value in {"strict", "loose"}
    ]
    if not duplicate_match_mode_options:
        duplicate_match_mode_options = ["strict"]
    if primary_dup_match_mode not in duplicate_match_mode_options:
        primary_dup_match_mode = (
            "strict" if "strict" in duplicate_match_mode_options else duplicate_match_mode_options[0]
        )
    duplicate_match_mode_policy = [
        {
            "match_mode": mode,
            "match_mode_label": _duplicate_match_mode_label(
                mode,
                inferential_key_mode=primary_dup_inferential_key_mode,
            ),
            "match_mode_role": _duplicate_match_mode_role(
                mode,
                inferential_key_mode=primary_dup_inferential_key_mode,
            ),
            "inferential_enabled": bool(mode == primary_dup_inferential_key_mode),
        }
        for mode in duplicate_match_mode_options
    ]

    dup_exact_bucket_position = _with_expected_columns(
        table_map.get(
            _table_key("duplicates_exact", "collision_by_bucket_position"),
            pd.DataFrame(),
        ),
        [
            "scope",
            "scope_status",
            "scope_reason",
            "metric",
            "bucket_start",
            "bucket_minutes",
            "position_normalized",
            "n_bucket_position",
            "n_unique_names",
            "observed",
            "expected",
            "excess",
            "deviance",
            "deviance_ratio",
            "lambda_side",
            "shrink_k",
            "prior_level",
            "is_low_power",
            "inference_status",
            "baseline_source",
            "baseline_label",
            "inferential_status",
            "inferential_reason",
            "claim_class",
        ],
    )
    dup_exact_bucket_position = _attach_duplicate_scope_metadata(dup_exact_bucket_position)
    dup_exact_null_distribution = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "null_distribution"), pd.DataFrame()),
        [
            "iteration",
            "duplicate_rows",
            "duplicate_row_rate",
            "duplicate_pairs",
            "n_names_ge2",
            "n_names_ge3",
            "n_names_ge5",
            "n_names_ge10",
            "max_count",
        ],
    )
    if not dup_exact_null_distribution.empty:
        dup_exact_null_distribution["baseline_source"] = primary_dup_baseline_source
        dup_exact_null_distribution["baseline_label"] = primary_dup_baseline_label
        dup_exact_null_distribution["inferential_status"] = primary_dup_inferential_status
        dup_exact_null_distribution["inferential_reason"] = primary_dup_inferential_reason
        dup_exact_null_distribution["inferential_key_mode"] = primary_dup_inferential_key_mode
        dup_exact_null_distribution["claim_class"] = dup_claim_class
    dup_exact_swing_impact = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "swing_impact_scenarios"), pd.DataFrame()),
        [
            "scenario",
            "n_pro_effective",
            "n_con_effective",
            "pro_share",
        ],
    )
    if not dup_exact_swing_impact.empty:
        dup_exact_swing_impact["baseline_source"] = primary_dup_baseline_source
        dup_exact_swing_impact["baseline_label"] = primary_dup_baseline_label
        dup_exact_swing_impact["inferential_status"] = primary_dup_inferential_status
        dup_exact_swing_impact["inferential_reason"] = primary_dup_inferential_reason
        dup_exact_swing_impact["inferential_key_mode"] = primary_dup_inferential_key_mode
        dup_exact_swing_impact["claim_class"] = dup_claim_class
    dup_exact_hypothesis_families = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "hypothesis_families"), pd.DataFrame()),
        [
            "scope",
            "family_id",
            "family_label",
            "family_order",
            "adjustment_method",
            "n_tests",
            "n_significant",
            "eligible_by_gate",
            "gate_reason",
            "adjusted_p_value",
        ],
    )
    if dup_exact_hypothesis_families.empty:
        summary_family_rows = dup_exact_summary.get("hypothesis_families", [])
        if isinstance(summary_family_rows, list) and summary_family_rows:
            dup_exact_hypothesis_families = _with_expected_columns(
                pd.DataFrame(summary_family_rows),
                [
                    "scope",
                    "family_id",
                    "family_label",
                    "family_order",
                    "adjustment_method",
                    "n_tests",
                    "n_significant",
                    "eligible_by_gate",
                    "gate_reason",
                    "adjusted_p_value",
                ],
            )
    dup_exact_hypothesis_family_totals = _with_expected_columns(
        pd.DataFrame(dup_exact_summary.get("hypothesis_family_totals", []))
        if isinstance(dup_exact_summary.get("hypothesis_family_totals", []), list)
        else pd.DataFrame(),
        [
            "family_id",
            "family_label",
            "family_order",
            "adjustment_method",
            "n_tests",
            "n_significant",
            "n_scopes",
        ],
    )
    if dup_exact_hypothesis_family_totals.empty and not dup_exact_hypothesis_families.empty:
        dup_exact_hypothesis_family_totals = (
            dup_exact_hypothesis_families.groupby(
                ["family_id", "family_label", "family_order", "adjustment_method"],
                dropna=False,
            )
            .agg(
                n_tests=("n_tests", "sum"),
                n_significant=("n_significant", "sum"),
                n_scopes=("scope", "nunique"),
            )
            .reset_index()
        )

    vrdb_collision_metrics = _with_expected_columns(
        table_map.get(_table_key("vrdb_collision_evidence", "slice_metrics"), pd.DataFrame()),
        [
            "evidence_family",
            "slice_id",
            "slice_type",
            "bucket_start",
            "bucket_minutes",
            "n_rows",
            "n_unique_names",
            "baseline_variant",
            "requested_geo_level",
            "requested_geo_value",
            "effective_geo_level",
            "effective_geo_value",
            "fallback_steps",
            "backoff_reason",
            "observed_pairs",
            "expected_pairs_analytic",
            "expected_pairs_mean",
            "expected_pairs_median",
            "expected_pairs_p95",
            "expected_pairs_p99",
            "tail_prob_pairs",
            "observed_max_name_count",
            "expected_max_name_count_mean",
            "expected_max_name_count_p95",
            "expected_max_name_count_p99",
            "tail_prob_max_name",
            "max_count_reference_available",
            "max_count_reference_reason",
            "inferential_status",
            "inferential_reason",
            "effective_denominator",
            "vrdb_version",
            "normalization_version",
            "rng_root_seed",
            "rng_slice_seed",
            "rng_root_stream_id",
            "rng_stream_pairs",
            "rng_stream_max_name",
            "top_overrun_names",
        ],
    )
    if not vrdb_collision_metrics.empty:
        vrdb_collision_metrics["bucket_minutes"] = pd.to_numeric(
            vrdb_collision_metrics.get("bucket_minutes", pd.Series(dtype=float)),
            errors="coerce",
        ).fillna(0).astype(int)
        vrdb_collision_metrics["bucket_start"] = pd.to_datetime(
            vrdb_collision_metrics.get("bucket_start", pd.Series(dtype=object)),
            errors="coerce",
        )
    vrdb_collision_overrun = _with_expected_columns(
        table_map.get(_table_key("vrdb_collision_evidence", "top_overrun_names"), pd.DataFrame()),
        [
            "slice_id",
            "slice_type",
            "bucket_start",
            "bucket_minutes",
            "name_key",
            "observed_count",
            "expected_count",
            "overrun_count",
            "expected_share",
            "baseline_variant",
            "effective_geo_level",
            "effective_geo_value",
            "rank",
        ],
    )
    if not vrdb_collision_overrun.empty:
        vrdb_collision_overrun["bucket_minutes"] = pd.to_numeric(
            vrdb_collision_overrun.get("bucket_minutes", pd.Series(dtype=float)),
            errors="coerce",
        ).fillna(0).astype(int)
        vrdb_collision_overrun["bucket_start"] = pd.to_datetime(
            vrdb_collision_overrun.get("bucket_start", pd.Series(dtype=object)),
            errors="coerce",
        )
        vrdb_collision_overrun["overrun_count"] = pd.to_numeric(
            vrdb_collision_overrun.get("overrun_count", pd.Series(dtype=float)),
            errors="coerce",
        ).fillna(0.0)
        vrdb_collision_overrun = vrdb_collision_overrun.sort_values(
            ["slice_id", "overrun_count", "observed_count", "name_key"],
            ascending=[True, False, False, True],
        )
    vrdb_collision_bucket = vrdb_collision_metrics[
        pd.to_numeric(
            vrdb_collision_metrics.get("bucket_minutes", pd.Series(dtype=float)),
            errors="coerce",
        ).fillna(0.0)
        > 0.0
    ].copy()
    vrdb_collision_full = vrdb_collision_metrics[
        pd.to_numeric(
            vrdb_collision_metrics.get("bucket_minutes", pd.Series(dtype=float)),
            errors="coerce",
        ).fillna(0.0)
        <= 0.0
    ].copy()

    org_blank_rates = _with_expected_columns(
        table_map.get(
            _table_key("org_anomalies", "organization_blank_rate_by_bucket"), pd.DataFrame()
        ),
        [
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "n_pro",
            "n_con",
            "n_unknown",
            "blank_org_rate",
            "blank_org_rate_wilson_low",
            "blank_org_rate_wilson_high",
            "pro_blank_org_rate",
            "pro_blank_org_rate_wilson_low",
            "pro_blank_org_rate_wilson_high",
            "con_blank_org_rate",
            "con_blank_org_rate_wilson_low",
            "con_blank_org_rate_wilson_high",
            "unknown_blank_org_rate",
            "unknown_blank_org_rate_wilson_low",
            "unknown_blank_org_rate_wilson_high",
            "is_low_power",
            "pro_is_low_power",
            "con_is_low_power",
            "unknown_is_low_power",
        ],
    )
    org_position_rates = _with_expected_columns(
        table_map.get(
            _table_key("org_anomalies", "organization_blank_rate_by_bucket_position"),
            pd.DataFrame(),
        ),
        [
            "bucket_start",
            "bucket_minutes",
            "position_normalized",
            "n_total",
            "blank_org_rate",
            "blank_org_rate_wilson_low",
            "blank_org_rate_wilson_high",
            "is_low_power",
        ],
    )

    voter_bucket = _with_expected_columns(
        table_map.get(_table_key("voter_registry_match", "match_by_bucket"), pd.DataFrame()),
        [
            "match_mode",
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "n_matched_unique",
            "n_matched_ambiguous",
            "n_unmatched",
            "matched_rate",
            "unmatched_rate",
            "matched_rate_wilson_low",
            "matched_rate_wilson_high",
            "unmatched_rate_wilson_low",
            "unmatched_rate_wilson_high",
            "is_low_power",
            "n_pro",
            "n_con",
        ],
    )
    voter_position_rows = _with_expected_columns(
        table_map.get(_table_key("voter_registry_match", "linkage_by_position_rows"), pd.DataFrame()),
        [
            "match_mode",
            "position_normalized",
            "n_total",
            "n_matched_unique",
            "n_matched_ambiguous",
            "n_unmatched",
            "matched_rate",
            "unmatched_rate",
            "matched_rate_wilson_low",
            "matched_rate_wilson_high",
            "unmatched_rate_wilson_low",
            "unmatched_rate_wilson_high",
            "is_low_power",
        ],
    )
    voter_position_unique = _with_expected_columns(
        table_map.get(_table_key("voter_registry_match", "linkage_by_position_unique"), pd.DataFrame()),
        [
            "match_mode",
            "position_normalized",
            "n_total",
            "n_matched_unique",
            "n_matched_ambiguous",
            "n_unmatched",
            "matched_rate",
            "unmatched_rate",
            "matched_rate_wilson_low",
            "matched_rate_wilson_high",
            "unmatched_rate_wilson_low",
            "unmatched_rate_wilson_high",
            "is_low_power",
        ],
    )
    voter_pairwise = _with_expected_columns(
        table_map.get(_table_key("voter_registry_match", "position_pairwise_tests"), pd.DataFrame()),
        [
            "match_mode",
            "unit",
            "position_left",
            "position_right",
            "left_n_total",
            "left_n_unmatched",
            "left_unmatched_rate",
            "right_n_total",
            "right_n_unmatched",
            "right_unmatched_rate",
            "rate_difference",
            "odds_ratio",
            "p_value",
            "alpha",
            "is_significant",
            "inference_status",
        ],
    )
    voter_sensitivity_modes = _with_expected_columns(
        table_map.get(_table_key("voter_registry_match", "sensitivity_modes"), pd.DataFrame()),
        [
            "mode",
            "match_mode",
            "n_rows",
            "n_unmatched_rows",
            "unmatched_rate_rows",
            "n_unique_names",
            "n_unmatched_unique",
            "unmatched_rate_unique",
        ],
    )
    voter_unmatched = _with_expected_columns(
        table_map.get(_table_key("voter_registry_match", "unmatched_names"), pd.DataFrame()),
        [
            "match_mode",
            "display_name",
            "canonical_name",
            "n_rows",
            "n_pro",
            "n_con",
            "first_seen",
            "last_seen",
            "top_caveat",
            "best_similarity_score",
            "candidate_pool_size",
        ],
    )
    voter_bucket_position = _with_expected_columns(
        table_map.get(
            _table_key("voter_registry_match", "match_by_bucket_position"),
            pd.DataFrame(),
        ),
        [
            "match_mode",
            "bucket_start",
            "bucket_minutes",
            "position_normalized",
            "n_total",
            "n_matched_unique",
            "n_matched_ambiguous",
            "n_unmatched",
            "matched_rate",
            "unmatched_rate",
            "matched_rate_wilson_low",
            "matched_rate_wilson_high",
            "unmatched_rate_wilson_low",
            "unmatched_rate_wilson_high",
            "is_low_power",
        ],
    )
    voter_position_bounds = _with_expected_columns(
        table_map.get(_table_key("voter_registry_match", "position_bounds"), pd.DataFrame()),
        [
            "match_mode",
            "unit",
            "position_normalized",
            "n_total_lower",
            "n_total_upper",
            "matched_rate_lower",
            "matched_rate_upper",
            "matched_rate_span",
            "unmatched_rate_lower",
            "unmatched_rate_upper",
            "unmatched_rate_span",
            "inference_status",
        ],
    )

    # Compatibility aliases used by shared chart helpers/front-end logic.
    for frame in (voter_bucket, voter_position_rows, voter_position_unique, voter_bucket_position):
        if not frame.empty:
            if "match_rate" not in frame.columns and "matched_rate" in frame.columns:
                frame["match_rate"] = frame["matched_rate"]
            if "match_rate_wilson_low" not in frame.columns and "matched_rate_wilson_low" in frame.columns:
                frame["match_rate_wilson_low"] = frame["matched_rate_wilson_low"]
            if "match_rate_wilson_high" not in frame.columns and "matched_rate_wilson_high" in frame.columns:
                frame["match_rate_wilson_high"] = frame["matched_rate_wilson_high"]

    if not voter_sensitivity_modes.empty and "match_mode" in voter_sensitivity_modes.columns:
        voter_sensitivity_modes["match_mode"] = voter_sensitivity_modes["match_mode"].map(
            lambda value: _normalize_report_match_mode(value, default="loose")
        )
    elif not voter_sensitivity_modes.empty and "mode" in voter_sensitivity_modes.columns:
        voter_sensitivity_modes["match_mode"] = voter_sensitivity_modes["mode"].map(
            lambda value: _normalize_report_match_mode(value, default="loose")
        )
    for frame in (
        voter_bucket,
        voter_position_rows,
        voter_position_unique,
        voter_pairwise,
        voter_unmatched,
        voter_bucket_position,
        voter_position_bounds,
    ):
        if frame.empty:
            continue
        if "match_mode" not in frame.columns:
            frame["match_mode"] = "loose"
        frame["match_mode"] = frame["match_mode"].map(
            lambda value: _normalize_report_match_mode(value, default="loose")
        )
    for frame in (
        voter_position_rows,
        voter_position_unique,
        voter_bucket_position,
        voter_position_bounds,
    ):
        if frame.empty or "position_normalized" not in frame.columns:
            continue
        frame["position_normalized"] = frame["position_normalized"].map(_normalize_voter_position_label)

    if voter_position_bounds.empty:
        voter_position_bounds = _build_voter_position_bounds_fallback(
            voter_position_rows=voter_position_rows,
            voter_position_unique=voter_position_unique,
        )

    voter_match_mode_options = sorted(
        {
            _normalize_report_match_mode(value, default="loose")
            for value in [
                *voter_bucket.get("match_mode", pd.Series(dtype=str)).tolist(),
                *voter_position_rows.get("match_mode", pd.Series(dtype=str)).tolist(),
                *voter_position_unique.get("match_mode", pd.Series(dtype=str)).tolist(),
                *voter_pairwise.get("match_mode", pd.Series(dtype=str)).tolist(),
                *voter_unmatched.get("match_mode", pd.Series(dtype=str)).tolist(),
                *voter_bucket_position.get("match_mode", pd.Series(dtype=str)).tolist(),
                *voter_position_bounds.get("match_mode", pd.Series(dtype=str)).tolist(),
                *voter_sensitivity_modes.get("match_mode", pd.Series(dtype=str)).tolist(),
            ]
            if str(value).strip()
        }
    )
    voter_match_mode_options = [value for value in voter_match_mode_options if value in {"strict", "loose"}]
    if not voter_match_mode_options:
        voter_match_mode_options = ["loose"]
    voter_summary = detector_summaries.get("voter_registry_match", {})
    voter_default_mode = _normalize_report_match_mode(
        voter_summary.get("match_mode_default")
        or voter_summary.get("primary_match_mode")
        or "loose",
        default="loose",
    )
    if voter_default_mode not in voter_match_mode_options:
        voter_default_mode = "loose" if "loose" in voter_match_mode_options else voter_match_mode_options[0]

    if "n_records" not in voter_unmatched.columns and "n_rows" in voter_unmatched.columns:
        voter_unmatched["n_records"] = voter_unmatched["n_rows"]
    if "display_name" not in voter_unmatched.columns:
        voter_unmatched["display_name"] = ""
    voter_unmatched["display_name"] = voter_unmatched["display_name"].fillna("").astype(str)
    if "canonical_name" in voter_unmatched.columns:
        canonical_display_names = (
            voter_unmatched["canonical_name"].fillna("").astype(str).map(_canonical_name_to_display_name)
        )
        voter_unmatched["display_name"] = voter_unmatched["display_name"].where(
            voter_unmatched["display_name"].str.strip() != "",
            canonical_display_names,
        )

    for frame, column in [
        (counts_per_minute, "minute_bucket"),
        (bursts_significant, "start_minute"),
        (bursts_tests, "start_minute"),
        (off_hours_window_control, "bucket_start"),
        (dup_exact_bucket, "bucket_start"),
        (dup_exact_per_name, "first_seen"),
        (dup_exact_per_name, "last_seen"),
        (org_blank_rates, "bucket_start"),
        (org_position_rates, "bucket_start"),
        (voter_bucket, "bucket_start"),
        (voter_bucket_position, "bucket_start"),
        (voter_unmatched, "first_seen"),
        (voter_unmatched, "last_seen"),
    ]:
        if not frame.empty and column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")

    for frame in [bursts_significant, bursts_tests, bursts_null]:
        if (
            frame.empty
            or "window_minutes" not in frame.columns
            or "bucket_minutes" not in frame.columns
        ):
            continue
        window_minutes = pd.to_numeric(frame["window_minutes"], errors="coerce")
        bucket_minutes = pd.to_numeric(frame["bucket_minutes"], errors="coerce")
        frame["bucket_minutes"] = bucket_minutes.where(bucket_minutes.notna(), window_minutes)

    if not off_hours_window_control.empty:
        def _off_hours_bool(column_name: str) -> pd.Series:
            if column_name not in off_hours_window_control.columns:
                return pd.Series(False, index=off_hours_window_control.index, dtype=bool)
            return (
                pd.to_numeric(off_hours_window_control[column_name], errors="coerce")
                .fillna(0)
                .astype(int)
                .astype(bool)
            )

        lower_alert = _off_hours_bool("is_primary_alert_window")
        upper_alert = (
            _off_hours_bool("is_alert_off_hours_window")
            & (~_off_hours_bool("is_low_power"))
            & _off_hours_bool("is_above_primary_control_998")
            & (
                _off_hours_bool("is_significant_primary_upper")
                | _off_hours_bool("is_significant_primary_two_sided")
                | _off_hours_bool("is_primary_fdr_two_sided")
            )
            & _off_hours_bool("is_material_primary_upper_shift")
        )
        off_hours_window_control["is_primary_alert_window"] = lower_alert
        off_hours_window_control["is_primary_lower_alert_window"] = lower_alert
        off_hours_window_control["is_primary_upper_alert_window"] = upper_alert
        off_hours_window_control["is_primary_two_sided_alert_window"] = (
            lower_alert | upper_alert
        )

    global_match_rate = float("nan")
    if not voter_bucket.empty:
        voter_bucket["n_total"] = pd.to_numeric(
            voter_bucket.get("n_total"), errors="coerce"
        ).fillna(0)
        voter_bucket["n_matched_unique"] = pd.to_numeric(
            voter_bucket.get("n_matched_unique"), errors="coerce"
        ).fillna(0)
        voter_bucket["n_matched_ambiguous"] = pd.to_numeric(
            voter_bucket.get("n_matched_ambiguous"), errors="coerce"
        ).fillna(0)
        voter_bucket["matched_rate"] = pd.to_numeric(
            voter_bucket.get("matched_rate"), errors="coerce"
        )
        voter_bucket["n_matched"] = (
            voter_bucket["n_matched_unique"] + voter_bucket["n_matched_ambiguous"]
        )

        if not voter_bucket_position.empty:
            position_frame = voter_bucket_position.copy()
            position_frame["match_mode"] = position_frame.get(
                "match_mode",
                pd.Series(pd.NA, index=position_frame.index),
            ).map(lambda value: _normalize_report_match_mode(value, default="loose"))
            position_frame["bucket_start"] = pd.to_datetime(
                position_frame.get("bucket_start"), errors="coerce"
            )
            position_frame["bucket_minutes"] = pd.to_numeric(
                position_frame.get("bucket_minutes"), errors="coerce"
            )
            position_frame["position_key"] = (
                position_frame.get("position_normalized", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
                .str.strip()
                .str.lower()
            )
            for position_key, prefix in (("pro", "pro"), ("con", "con")):
                position_subset = position_frame[
                    position_frame["position_key"] == position_key
                ].copy()
                if position_subset.empty:
                    continue
                position_subset = position_subset[
                    [
                        "match_mode",
                        "bucket_start",
                        "bucket_minutes",
                        "matched_rate",
                        "matched_rate_wilson_low",
                        "matched_rate_wilson_high",
                    ]
                ].rename(
                    columns={
                        "matched_rate": f"matched_rate_{prefix}",
                        "matched_rate_wilson_low": f"matched_rate_{prefix}_wilson_low",
                        "matched_rate_wilson_high": f"matched_rate_{prefix}_wilson_high",
                    }
                )
                voter_bucket = voter_bucket.merge(
                    position_subset,
                    on=["match_mode", "bucket_start", "bucket_minutes"],
                    how="left",
                )
        for position_column in [
            "matched_rate_pro",
            "matched_rate_pro_wilson_low",
            "matched_rate_pro_wilson_high",
            "matched_rate_con",
            "matched_rate_con_wilson_low",
            "matched_rate_con_wilson_high",
        ]:
            if position_column not in voter_bucket.columns:
                voter_bucket[position_column] = np.nan

        n_total_all = float(pd.to_numeric(voter_bucket["n_total"], errors="coerce").sum())
        n_matched_all = float(
            pd.to_numeric(voter_bucket["n_matched"], errors="coerce").sum()
        )
        global_match_rate = (
            (n_matched_all / n_total_all) if n_total_all > 0 else float("nan")
        )
        voter_bucket["expected_match_rate_global"] = global_match_rate
        n_series = pd.to_numeric(voter_bucket["n_total"], errors="coerce").replace(0, np.nan)
        variance = global_match_rate * (1.0 - global_match_rate)
        std_error = np.sqrt(variance / n_series) if np.isfinite(global_match_rate) else np.nan
        voter_bucket["control_low_95_match_global"] = np.clip(
            global_match_rate - 1.96 * std_error, 0.0, 1.0
        )
        voter_bucket["control_high_95_match_global"] = np.clip(
            global_match_rate + 1.96 * std_error, 0.0, 1.0
        )
        voter_bucket["control_low_998_match_global"] = np.clip(
            global_match_rate - 3.0 * std_error, 0.0, 1.0
        )
        voter_bucket["control_high_998_match_global"] = np.clip(
            global_match_rate + 3.0 * std_error, 0.0, 1.0
        )
        voter_bucket["match_rate_delta_global"] = (
            pd.to_numeric(voter_bucket["matched_rate"], errors="coerce")
            - pd.to_numeric(voter_bucket["expected_match_rate_global"], errors="coerce")
        )
        voter_low_power = (
            pd.to_numeric(voter_bucket.get("is_low_power"), errors="coerce")
            .fillna(0)
            .astype(int)
            .astype(bool)
        )
        voter_bucket["is_match_rate_alert_lower"] = (
            (~voter_low_power)
            & pd.to_numeric(voter_bucket["matched_rate"], errors="coerce").notna()
            & (
                pd.to_numeric(voter_bucket["matched_rate"], errors="coerce")
                < pd.to_numeric(voter_bucket["control_low_998_match_global"], errors="coerce")
            )
        )
        voter_bucket["is_match_rate_alert_upper"] = (
            (~voter_low_power)
            & pd.to_numeric(voter_bucket["matched_rate"], errors="coerce").notna()
            & (
                pd.to_numeric(voter_bucket["matched_rate"], errors="coerce")
                > pd.to_numeric(voter_bucket["control_high_998_match_global"], errors="coerce")
            )
        )
        voter_bucket["is_match_rate_alert_any"] = (
            voter_bucket["is_match_rate_alert_lower"].astype(bool)
            | voter_bucket["is_match_rate_alert_upper"].astype(bool)
        )
    expected_unmatched_rate_global = (
        (1.0 - global_match_rate) if np.isfinite(global_match_rate) else float("nan")
    )
    for position_frame in (voter_position_rows, voter_position_unique):
        if position_frame.empty:
            continue
        position_frame["expected_match_rate_global"] = global_match_rate
        position_frame["expected_unmatched_rate_global"] = expected_unmatched_rate_global

    burst_events = pd.DataFrame()
    if not bursts_significant.empty:
        burst_windows = bursts_significant.copy()
        burst_windows["start_minute"] = pd.to_datetime(
            burst_windows.get("start_minute"), errors="coerce"
        )
        burst_windows["end_minute"] = pd.to_datetime(
            burst_windows.get("end_minute"), errors="coerce"
        )
        burst_windows = burst_windows.dropna(subset=["start_minute", "end_minute"]).copy()
        for column in (
            "window_minutes",
            "bucket_minutes",
            "observed_count",
            "expected_count",
            "rate_ratio",
            "n_pro",
            "n_con",
            "baseline_pro_rate",
            "q_value",
        ):
            burst_windows[column] = pd.to_numeric(
                burst_windows.get(column),
                errors="coerce",
            )
        burst_windows["bucket_minutes"] = burst_windows["bucket_minutes"].where(
            burst_windows["bucket_minutes"].notna(),
            burst_windows["window_minutes"],
        )
        burst_windows["duration_minutes"] = (
            (
                (burst_windows["end_minute"] - burst_windows["start_minute"])
                / pd.Timedelta(minutes=1)
            )
            .fillna(0.0)
            .clip(lower=0.0)
            + 1.0
        )
        burst_windows["excess_count"] = (
            burst_windows["observed_count"] - burst_windows["expected_count"]
        )
        burst_windows["expected_pro_count"] = (
            burst_windows["expected_count"] * burst_windows["baseline_pro_rate"].fillna(0.0)
        )
        burst_windows["expected_con_count"] = (
            burst_windows["expected_count"]
            * (1.0 - burst_windows["baseline_pro_rate"].fillna(0.0))
        )
        burst_windows["pro_impact_count"] = (
            burst_windows["n_pro"] - burst_windows["expected_pro_count"]
        )
        burst_windows["con_impact_count"] = (
            burst_windows["n_con"] - burst_windows["expected_con_count"]
        )
        burst_windows["net_position_impact"] = (
            burst_windows["pro_impact_count"] - burst_windows["con_impact_count"]
        )
        burst_windows["dominant_impact_count"] = np.maximum(
            burst_windows["pro_impact_count"].abs(),
            burst_windows["con_impact_count"].abs(),
        )

        def _burst_impacted_positions(row: pd.Series) -> str:
            pro_impact = float(pd.to_numeric(row.get("pro_impact_count"), errors="coerce") or 0.0)
            con_impact = float(pd.to_numeric(row.get("con_impact_count"), errors="coerce") or 0.0)
            pro_material = abs(pro_impact) >= 0.5
            con_material = abs(con_impact) >= 0.5
            if pro_material and con_material:
                return "Pro & Con"
            if pro_material:
                return "Pro"
            if con_material:
                return "Con"
            return "Mixed"

        def _burst_dominant_position(row: pd.Series) -> str:
            pro_impact = abs(float(pd.to_numeric(row.get("pro_impact_count"), errors="coerce") or 0.0))
            con_impact = abs(float(pd.to_numeric(row.get("con_impact_count"), errors="coerce") or 0.0))
            if pro_impact > con_impact:
                return "Pro"
            if con_impact > pro_impact:
                return "Con"
            return "Mixed"

        burst_windows["impacted_positions"] = burst_windows.apply(
            _burst_impacted_positions,
            axis=1,
        )
        burst_windows["dominant_position"] = burst_windows.apply(
            _burst_dominant_position,
            axis=1,
        )

        if not burst_windows.empty:
            burst_windows = burst_windows.sort_values(
                ["start_minute", "end_minute", "rate_ratio"],
                ascending=[True, True, False],
                na_position="last",
            ).reset_index(drop=True)

            merged_events: list[dict[str, Any]] = []
            current_rows: list[pd.Series] = []
            current_start: pd.Timestamp | None = None
            current_end: pd.Timestamp | None = None

            def _flush_burst_event() -> None:
                nonlocal current_rows, current_start, current_end
                if not current_rows or current_start is None or current_end is None:
                    current_rows = []
                    current_start = None
                    current_end = None
                    return
                cluster = pd.DataFrame(current_rows)
                if cluster.empty:
                    current_rows = []
                    current_start = None
                    current_end = None
                    return
                representative_idx = pd.to_numeric(
                    cluster.get("excess_count"),
                    errors="coerce",
                ).fillna(-np.inf).idxmax()
                representative = cluster.loc[representative_idx]
                duration_minutes = (
                    ((current_end - current_start) / pd.Timedelta(minutes=1)) + 1.0
                )
                q_values = pd.to_numeric(cluster.get("q_value"), errors="coerce")
                merged_events.append(
                    {
                        "start_minute": current_start,
                        "end_minute": current_end,
                        "duration_minutes": max(float(duration_minutes), 1.0),
                        "windows_merged": int(len(cluster)),
                        "window_minutes": pd.to_numeric(
                            representative.get("window_minutes"),
                            errors="coerce",
                        ),
                        "bucket_minutes": pd.to_numeric(
                            representative.get("bucket_minutes"),
                            errors="coerce",
                        ),
                        "observed_count": pd.to_numeric(
                            representative.get("observed_count"),
                            errors="coerce",
                        ),
                        "expected_count": pd.to_numeric(
                            representative.get("expected_count"),
                            errors="coerce",
                        ),
                        "excess_count": pd.to_numeric(
                            representative.get("excess_count"),
                            errors="coerce",
                        ),
                        "rate_ratio": pd.to_numeric(
                            representative.get("rate_ratio"),
                            errors="coerce",
                        ),
                        "n_pro": pd.to_numeric(representative.get("n_pro"), errors="coerce"),
                        "n_con": pd.to_numeric(representative.get("n_con"), errors="coerce"),
                        "pro_impact_count": pd.to_numeric(
                            representative.get("pro_impact_count"),
                            errors="coerce",
                        ),
                        "con_impact_count": pd.to_numeric(
                            representative.get("con_impact_count"),
                            errors="coerce",
                        ),
                        "net_position_impact": pd.to_numeric(
                            representative.get("net_position_impact"),
                            errors="coerce",
                        ),
                        "impacted_positions": str(
                            representative.get("impacted_positions") or "Mixed"
                        ),
                        "dominant_position": str(
                            representative.get("dominant_position") or "Mixed"
                        ),
                        "dominant_impact_count": pd.to_numeric(
                            representative.get("dominant_impact_count"),
                            errors="coerce",
                        ),
                        "is_low_power": bool(
                            pd.to_numeric(cluster.get("is_low_power"), errors="coerce")
                            .fillna(0)
                            .astype(int)
                            .astype(bool)
                            .any()
                        ),
                        "q_value": float(q_values.min())
                        if q_values.notna().any()
                        else np.nan,
                    }
                )
                current_rows = []
                current_start = None
                current_end = None

            for row in burst_windows.to_dict(orient="records"):
                row_start = pd.to_datetime(row.get("start_minute"), errors="coerce")
                row_end = pd.to_datetime(row.get("end_minute"), errors="coerce")
                if pd.isna(row_start) or pd.isna(row_end):
                    continue
                row_start_ts = pd.Timestamp(row_start)
                row_end_ts = pd.Timestamp(row_end)
                if current_start is None or current_end is None:
                    current_start = row_start_ts
                    current_end = row_end_ts
                    current_rows = [pd.Series(row)]
                    continue
                if row_start_ts <= (current_end + pd.Timedelta(minutes=1)):
                    current_end = max(current_end, row_end_ts)
                    current_rows.append(pd.Series(row))
                    continue
                _flush_burst_event()
                current_start = row_start_ts
                current_end = row_end_ts
                current_rows = [pd.Series(row)]
            _flush_burst_event()

            if merged_events:
                burst_events = pd.DataFrame(merged_events)
            else:
                burst_events = burst_windows.copy()

    charts: dict[str, list[dict[str, Any]]] = {}

    bursts_chart_source = (
        burst_events.copy() if not burst_events.empty else bursts_significant.copy()
    )
    bursts_chart_source = _with_expected_columns(
        bursts_chart_source,
        [
            "start_minute",
            "end_minute",
            "duration_minutes",
            "windows_merged",
            "window_minutes",
            "bucket_minutes",
            "observed_count",
            "expected_count",
            "excess_count",
            "rate_ratio",
            "n_pro",
            "n_con",
            "pro_impact_count",
            "con_impact_count",
            "net_position_impact",
            "impacted_positions",
            "dominant_position",
            "dominant_impact_count",
            "q_value",
            "is_significant",
            "is_low_power",
        ],
    )
    bursts_chart_source = bursts_chart_source.sort_values(
        ["start_minute", "duration_minutes"],
        ascending=[True, False],
        na_position="last",
    )
    charts["bursts_hero_timeline"] = _records_from_frame(
        bursts_chart_source,
        columns=[
            "start_minute",
            "end_minute",
            "duration_minutes",
            "windows_merged",
            "window_minutes",
            "bucket_minutes",
            "observed_count",
            "expected_count",
            "excess_count",
            "rate_ratio",
            "dominant_position",
            "dominant_impact_count",
            "impacted_positions",
            "q_value",
            "is_significant",
            "is_low_power",
        ],
        max_rows=25_000,
    )
    burst_duration_timeline = bursts_chart_source.copy()
    charts["bursts_significance_by_window"] = _records_from_frame(
        burst_duration_timeline,
        columns=[
            "start_minute",
            "end_minute",
            "duration_minutes",
            "windows_merged",
            "window_minutes",
            "bucket_minutes",
            "observed_count",
            "expected_count",
            "excess_count",
            "rate_ratio",
            "dominant_position",
            "dominant_impact_count",
            "is_low_power",
        ],
        max_rows=25_000,
    )
    charts["bursts_composition_shift"] = _records_from_frame(
        bursts_chart_source,
        columns=[
            "start_minute",
            "end_minute",
            "duration_minutes",
            "windows_merged",
            "window_minutes",
            "bucket_minutes",
            "observed_count",
            "expected_count",
            "excess_count",
            "pro_impact_count",
            "con_impact_count",
            "net_position_impact",
            "dominant_position",
            "dominant_impact_count",
            "impacted_positions",
            "is_low_power",
            "q_value",
        ],
        max_rows=25_000,
    )
    charts["bursts_null_distribution"] = _records_from_frame(
        bursts_null.sort_values(["window_minutes", "iteration"]),
        columns=["window_minutes", "bucket_minutes", "iteration", "max_window_count"],
        max_rows=25_000,
    )

    charts["off_hours_hourly_profile"] = _records_from_frame(
        off_hours_hourly.sort_values("hour"),
        columns=[
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
        max_rows=500,
    )
    charts["off_hours_control_timeline"] = _records_from_frame(
        off_hours_window_control.sort_values(["bucket_minutes", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "event_date_key",
            "day_of_week",
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "n_known",
            "n_unknown",
            "n_off_hours",
            "off_hours_fraction",
            "is_off_hours_window",
            "is_pure_off_hours_window",
            "is_alert_off_hours_window",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "expected_pro_rate_day",
            "expected_pro_rate_model",
            "expected_pro_rate_primary",
            "expected_pro_rate_global",
            "baseline_source",
            "model_baseline_source",
            "primary_baseline_source",
            "is_model_baseline_available",
            "model_fit_method",
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_used_harmonics",
            "control_low_95_day",
            "control_high_95_day",
            "control_low_998_day",
            "control_high_998_day",
            "control_low_95_model",
            "control_high_95_model",
            "control_low_998_model",
            "control_high_998_model",
            "control_low_95_primary",
            "control_high_95_primary",
            "control_low_998_primary",
            "control_high_998_primary",
            "control_low_95_global",
            "control_high_95_global",
            "control_low_998_global",
            "control_high_998_global",
            "z_score_day",
            "z_score_model",
            "z_score_primary",
            "delta_pro_rate_day",
            "delta_pro_rate_model",
            "delta_pro_rate_primary",
            "p_value_day",
            "p_value_day_two_sided",
            "p_value_day_lower",
            "p_value_day_upper",
            "p_value_model",
            "p_value_model_two_sided",
            "p_value_model_lower",
            "p_value_model_upper",
            "p_value_primary",
            "p_value_primary_two_sided",
            "p_value_primary_lower",
            "p_value_primary_upper",
            "q_value_day",
            "q_value_day_lower",
            "q_value_day_upper",
            "q_value_day_two_sided",
            "q_value_model",
            "q_value_model_lower",
            "q_value_model_upper",
            "q_value_model_two_sided",
            "q_value_primary",
            "q_value_primary_lower",
            "q_value_primary_upper",
            "q_value_primary_two_sided",
            "is_significant_day",
            "is_significant_day_lower",
            "is_significant_day_upper",
            "is_significant_day_two_sided",
            "is_significant_model",
            "is_significant_model_lower",
            "is_significant_model_upper",
            "is_significant_model_two_sided",
            "is_significant_primary",
            "is_significant_primary_lower",
            "is_significant_primary_upper",
            "is_significant_primary_two_sided",
            "is_material_primary_shift",
            "is_material_primary_lower_shift",
            "is_material_primary_upper_shift",
            "is_primary_alert_window",
            "is_primary_lower_alert_window",
            "is_primary_upper_alert_window",
            "is_primary_two_sided_alert_window",
            "is_primary_spc_998_two_sided",
            "is_primary_fdr_two_sided",
            "is_primary_any_flag_channel",
            "is_primary_both_flag_channels",
            "is_below_day_control_95",
            "is_below_day_control_998",
            "is_above_day_control_95",
            "is_above_day_control_998",
            "is_below_model_control_95",
            "is_below_model_control_998",
            "is_above_model_control_95",
            "is_above_model_control_998",
            "is_below_primary_control_95",
            "is_below_primary_control_998",
            "is_above_primary_control_95",
            "is_above_primary_control_998",
            "is_outside_day_control_95",
            "is_outside_day_control_998",
            "is_outside_model_control_95",
            "is_outside_model_control_998",
            "is_outside_primary_control_95",
            "is_outside_primary_control_998",
            "is_below_global_control_95",
            "is_below_global_control_998",
        ],
        max_rows=100_000,
    )
    charts["off_hours_funnel_plot"] = _records_from_frame(
        off_hours_window_control.sort_values(["bucket_minutes", "n_known", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "n_known",
            "n_pro",
            "n_con",
            "off_hours_fraction",
            "is_off_hours_window",
            "is_pure_off_hours_window",
            "is_alert_off_hours_window",
            "pro_rate",
            "is_low_power",
            "expected_pro_rate_day",
            "expected_pro_rate_model",
            "expected_pro_rate_primary",
            "expected_pro_rate_global",
            "model_fit_method",
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_used_harmonics",
            "control_low_95_day",
            "control_high_95_day",
            "control_low_998_day",
            "control_high_998_day",
            "control_low_95_model",
            "control_high_95_model",
            "control_low_998_model",
            "control_high_998_model",
            "control_low_95_primary",
            "control_high_95_primary",
            "control_low_998_primary",
            "control_high_998_primary",
            "control_low_95_global",
            "control_high_95_global",
            "control_low_998_global",
            "control_high_998_global",
            "z_score_day",
            "z_score_model",
            "z_score_primary",
            "p_value_day",
            "p_value_day_two_sided",
            "p_value_model",
            "p_value_model_two_sided",
            "p_value_primary",
            "p_value_primary_two_sided",
            "q_value_day",
            "q_value_day_two_sided",
            "q_value_model",
            "q_value_model_two_sided",
            "q_value_primary",
            "q_value_primary_two_sided",
            "is_significant_day",
            "is_significant_day_two_sided",
            "is_significant_model",
            "is_significant_model_two_sided",
            "is_significant_primary",
            "is_significant_primary_lower",
            "is_significant_primary_upper",
            "is_significant_primary_two_sided",
            "is_material_primary_shift",
            "is_material_primary_lower_shift",
            "is_material_primary_upper_shift",
            "is_primary_alert_window",
            "is_primary_lower_alert_window",
            "is_primary_upper_alert_window",
            "is_primary_two_sided_alert_window",
            "is_primary_spc_998_two_sided",
            "is_primary_fdr_two_sided",
            "is_primary_any_flag_channel",
            "is_primary_both_flag_channels",
            "is_below_day_control_95",
            "is_below_day_control_998",
            "is_below_model_control_95",
            "is_below_model_control_998",
            "is_below_primary_control_95",
            "is_below_primary_control_998",
            "is_above_primary_control_95",
            "is_above_primary_control_998",
            "is_below_global_control_95",
            "is_below_global_control_998",
        ],
        max_rows=100_000,
    )
    overview_position_volume = off_hours_window_control.copy()
    if not overview_position_volume.empty:
        n_total = pd.to_numeric(overview_position_volume["n_total"], errors="coerce").fillna(0.0)
        n_pro = pd.to_numeric(overview_position_volume["n_pro"], errors="coerce").fillna(0.0)
        n_con = pd.to_numeric(overview_position_volume["n_con"], errors="coerce").fillna(0.0)
        n_unknown = pd.to_numeric(
            overview_position_volume["n_unknown"], errors="coerce"
        ).fillna(0.0)
        residual_other = (n_total - n_pro - n_con).clip(lower=0.0)
        n_other_position = n_unknown.where(n_unknown > 0, residual_other)
        overview_position_volume["n_other_position"] = n_other_position

        overview_position_volume["pro_share_total"] = (n_pro / n_total).where(n_total > 0.0)
        overview_position_volume["con_share_total"] = (n_con / n_total).where(n_total > 0.0)
        overview_position_volume["other_share_total"] = (n_other_position / n_total).where(
            n_total > 0.0
        )

        pro_share_low, pro_share_high = wilson_interval(successes=n_pro, totals=n_total)
        con_share_low, con_share_high = wilson_interval(successes=n_con, totals=n_total)
        other_share_low, other_share_high = wilson_interval(
            successes=n_other_position,
            totals=n_total,
        )
        overview_position_volume["pro_share_total_wilson_low"] = pro_share_low
        overview_position_volume["pro_share_total_wilson_high"] = pro_share_high
        overview_position_volume["con_share_total_wilson_low"] = con_share_low
        overview_position_volume["con_share_total_wilson_high"] = con_share_high
        overview_position_volume["other_share_total_wilson_low"] = other_share_low
        overview_position_volume["other_share_total_wilson_high"] = other_share_high

        overview_position_volume["n_pro_wilson_low"] = (
            pd.Series(pro_share_low, index=overview_position_volume.index) * n_total
        )
        overview_position_volume["n_pro_wilson_high"] = (
            pd.Series(pro_share_high, index=overview_position_volume.index) * n_total
        )
        overview_position_volume["n_con_wilson_low"] = (
            pd.Series(con_share_low, index=overview_position_volume.index) * n_total
        )
        overview_position_volume["n_con_wilson_high"] = (
            pd.Series(con_share_high, index=overview_position_volume.index) * n_total
        )
        overview_position_volume["n_other_position_wilson_low"] = (
            pd.Series(other_share_low, index=overview_position_volume.index) * n_total
        )
        overview_position_volume["n_other_position_wilson_high"] = (
            pd.Series(other_share_high, index=overview_position_volume.index) * n_total
        )
    charts["overview_position_volume_by_bucket"] = _records_from_frame(
        overview_position_volume.sort_values(["bucket_minutes", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "n_pro",
            "n_con",
            "n_other_position",
            "pro_share_total",
            "pro_share_total_wilson_low",
            "pro_share_total_wilson_high",
            "con_share_total",
            "con_share_total_wilson_low",
            "con_share_total_wilson_high",
            "other_share_total",
            "other_share_total_wilson_low",
            "other_share_total_wilson_high",
            "n_pro_wilson_low",
            "n_pro_wilson_high",
            "n_con_wilson_low",
            "n_con_wilson_high",
            "n_other_position_wilson_low",
            "n_other_position_wilson_high",
            "n_unknown",
            "n_known",
            "is_off_hours_window",
            "is_alert_off_hours_window",
            "is_low_power",
        ],
        max_rows=100_000,
    )
    off_hours_residual_timeline = off_hours_window_control.copy()
    if not off_hours_residual_timeline.empty:
        off_hours_residual_timeline["z_ref_zero"] = 0.0
        off_hours_residual_timeline["z_ref_pos3"] = 3.0
        off_hours_residual_timeline["z_ref_neg3"] = -3.0
    charts["off_hours_primary_residual_timeline"] = _records_from_frame(
        off_hours_residual_timeline.sort_values(["bucket_minutes", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "n_known",
            "n_pro",
            "n_con",
            "is_off_hours_window",
            "is_alert_off_hours_window",
            "is_low_power",
            "pro_rate",
            "expected_pro_rate_primary",
            "delta_pro_rate_primary",
            "z_score_primary",
            "z_score_day",
            "z_ref_zero",
            "z_ref_pos3",
            "z_ref_neg3",
            "p_value_primary",
            "p_value_primary_two_sided",
            "q_value_primary",
            "q_value_primary_two_sided",
            "is_primary_spc_998_two_sided",
            "is_primary_fdr_two_sided",
            "is_primary_any_flag_channel",
            "is_primary_both_flag_channels",
            "is_primary_alert_window",
            "is_primary_lower_alert_window",
            "is_primary_upper_alert_window",
            "is_primary_two_sided_alert_window",
            "primary_baseline_source",
            "is_model_baseline_available",
            "model_fit_method",
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_used_harmonics",
        ],
        max_rows=100_000,
    )
    charts["off_hours_primary_flag_channels"] = _records_from_frame(
        off_hours_flag_channels.sort_values(["rank", "channel"]),
        columns=[
            "rank",
            "channel",
            "channel_label",
            "count",
            "share_of_tested",
        ],
        max_rows=50,
    )
    charts["off_hours_date_hour_pro_heatmap"] = _records_from_frame(
        off_hours_date_hour.sort_values(["date", "hour"]),
        columns=[
            "date",
            "day_of_week",
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "n_known",
            "n_unknown",
            "n_off_hours",
            "off_hours_fraction",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
        max_rows=20_000,
    )
    charts["off_hours_date_hour_primary_residual_heatmap"] = _records_from_frame(
        off_hours_date_hour_primary_residual.sort_values(["bucket_minutes", "date", "hour"]),
        columns=[
            "bucket_minutes",
            "date",
            "day_of_week",
            "hour",
            "n_windows",
            "n_windows_alert_eligible",
            "n_windows_tested",
            "n_windows_low_power",
            "n_windows_primary_alert",
            "primary_alert_fraction_tested",
            "n_total",
            "n_known",
            "n_known_tested",
            "n_pro",
            "n_con",
            "off_hours_fraction",
            "pro_rate",
            "expected_pro_rate_primary",
            "delta_pro_rate_primary",
            "z_score_primary",
            "z_score_primary_median",
            "z_score_primary_abs_max",
            "is_low_power",
        ],
        max_rows=20_000,
    )
    charts["off_hours_date_hour_volume_heatmap"] = _records_from_frame(
        off_hours_date_hour.sort_values(["date", "hour"]),
        columns=[
            "date",
            "day_of_week",
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "n_known",
            "n_unknown",
            "n_off_hours",
            "off_hours_fraction",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
        max_rows=20_000,
    )
    charts["off_hours_summary_compare"] = _records_from_frame(
        off_hours_summary,
        columns=[
            "off_hours",
            "on_hours",
            "off_hours_ratio",
            "off_hours_pro_rate",
            "on_hours_pro_rate",
            "off_hours_pro_rate_wilson_low",
            "off_hours_pro_rate_wilson_high",
            "on_hours_pro_rate_wilson_low",
            "on_hours_pro_rate_wilson_high",
            "chi_square_p_value",
            "off_hours_is_low_power",
            "on_hours_is_low_power",
            "primary_bucket_minutes",
            "primary_baseline_method",
            "alert_off_hours_min_fraction",
            "primary_alert_min_abs_delta",
            "off_hours_windows_alert_eligible",
            "off_hours_windows_alert_eligible_low_power",
            "off_hours_windows_alert_eligible_tested_fraction",
            "off_hours_windows_alert_eligible_low_power_fraction",
            "off_hours_windows_tested",
            "off_hours_windows_below_day_control_95",
            "off_hours_windows_below_day_control_998",
            "off_hours_windows_below_model_control_95",
            "off_hours_windows_below_model_control_998",
            "off_hours_windows_below_primary_control_95",
            "off_hours_windows_below_primary_control_998",
            "off_hours_windows_above_primary_control_95",
            "off_hours_windows_above_primary_control_998",
            "off_hours_windows_significant_day",
            "off_hours_windows_significant_model",
            "off_hours_windows_significant_primary",
            "off_hours_windows_significant_primary_upper",
            "off_hours_windows_significant_primary_two_sided",
            "off_hours_windows_primary_spc_998_any",
            "off_hours_windows_primary_fdr_two_sided",
            "off_hours_windows_primary_flag_any",
            "off_hours_windows_primary_flag_both",
            "off_hours_windows_primary_spc_998_any_fraction",
            "off_hours_windows_primary_fdr_two_sided_fraction",
            "off_hours_windows_primary_flag_any_fraction",
            "off_hours_windows_primary_flag_both_fraction",
            "off_hours_windows_primary_alert",
            "off_hours_windows_primary_alert_fraction",
            "off_hours_primary_alert_run_count",
            "off_hours_primary_alert_max_run_windows",
            "off_hours_primary_alert_max_run_minutes",
            "off_hours_min_day_z",
            "off_hours_max_abs_day_z",
            "off_hours_min_model_z",
            "off_hours_max_abs_model_z",
            "off_hours_min_primary_z",
            "off_hours_max_abs_primary_z",
            "off_hours_min_primary_delta",
            "off_hours_max_abs_primary_delta",
            "off_hours_windows_model_available",
            "global_daytime_pro_rate",
            "day_adjusted_fdr_alpha",
            "model_fit_min_rows",
            "model_hour_harmonics",
            "primary_model_fit_method",
            "primary_model_fit_rows",
            "primary_model_fit_unique_days",
            "primary_model_fit_unique_hours",
            "primary_model_fit_converged",
            "primary_model_fit_aic",
        ],
        max_rows=10,
    )

    dup_exact_bucket_sorted = dup_exact_bucket.copy()
    duplicate_bucket_sort_columns = [
        column
        for column in ("scope", "match_mode", "metric", "bucket_minutes", "bucket_start")
        if column in dup_exact_bucket_sorted.columns
    ]
    if duplicate_bucket_sort_columns:
        dup_exact_bucket_sorted = dup_exact_bucket_sorted.sort_values(duplicate_bucket_sort_columns)
    charts["duplicates_exact_bucket_concentration"] = _records_from_frame(
        dup_exact_bucket_sorted,
        columns=[
            "bucket_start",
            "bucket_minutes",
            "scope",
            "match_mode",
            "match_mode_role",
            "inferential_key_mode",
            "metric",
            "n_rows",
            "n_unique_names",
            "duplicate_rows",
            "duplicate_row_rate",
            "expected_duplicate_rows",
            "excess_duplicate_rows",
            "unit_observed_rows",
            "unit_expected_rows",
            "unit_deviation_rows",
            "unit_observed_names",
            "unit_expected_names",
            "unit_expected_names_method",
            "unit_deviation_names",
            "report_baseline_family",
            "report_baseline_label",
            "report_baseline_method",
            "report_baseline_method_label",
            "detector_baseline_family",
            "detector_baseline_family_label",
            "detector_baseline_label",
            "n_used",
            "N_used",
            "baseline_model",
            "baseline_source",
            "baseline_label",
            "inferential_status",
            "inferential_reason",
            "family_id",
            "adjustment_method",
            "n_tests",
            "n_tests_in_family",
            "eligible_by_gate",
            "gate_reason",
            "adjusted_p_value",
            "is_significant",
            "claim_class",
            "baseline_degraded",
            "n_pro",
            "n_con",
        ],
        max_rows=100_000,
    )
    charts["duplicates_exact_metric_diagnostics"] = _records_from_frame(
        dup_exact_metric_diagnostics.sort_values(["scope", "metric"]),
        columns=[
            "scope",
            "metric",
            "observed",
            "expected",
            "expected_p05",
            "expected_p50",
            "expected_p95",
            "z_score",
            "p_value",
            "n_used",
            "N_used",
            "baseline_source",
            "baseline_label",
            "inferential_status",
            "inferential_reason",
            "inferential_key_mode",
            "family_id",
            "adjustment_method",
            "n_tests",
            "n_tests_in_family",
            "eligible_by_gate",
            "gate_reason",
            "adjusted_p_value",
            "is_significant",
            "claim_class",
        ],
        max_rows=50,
    )
    dup_exact_per_name_chart = dup_exact_per_name.copy()
    dup_exact_per_name_chart["n"] = pd.to_numeric(
        dup_exact_per_name_chart.get("n", pd.Series(0, index=dup_exact_per_name_chart.index)),
        errors="coerce",
    ).fillna(0)
    dup_exact_per_name_chart["n_pro"] = pd.to_numeric(
        dup_exact_per_name_chart.get("n_pro", pd.Series(0, index=dup_exact_per_name_chart.index)),
        errors="coerce",
    ).fillna(0)
    dup_exact_per_name_chart["n_con"] = pd.to_numeric(
        dup_exact_per_name_chart.get("n_con", pd.Series(0, index=dup_exact_per_name_chart.index)),
        errors="coerce",
    ).fillna(0)

    if not dup_exact_per_name_chart.empty:
        dup_exact_per_name_chart["position_series"] = np.where(
            (dup_exact_per_name_chart["n_pro"] > 0) & (dup_exact_per_name_chart["n_con"] > 0),
            "Mixed",
            np.where(dup_exact_per_name_chart["n_pro"] > 0, "Pro", "Con"),
        )
        dup_exact_per_name_chart["position_count"] = (
            pd.to_numeric(dup_exact_per_name_chart["n_pro"], errors="coerce").fillna(0)
            + pd.to_numeric(dup_exact_per_name_chart["n_con"], errors="coerce").fillna(0)
        )
        dup_exact_per_name_chart = dup_exact_per_name_chart.sort_values(
            ["scope", "match_mode", "position_count", "q_value", "p_value", "n"],
            ascending=[True, True, False, True, True, False],
        )
    if not dup_exact_per_name_chart.empty:
        group_fields = [
            field for field in ("scope", "match_mode") if field in dup_exact_per_name_chart.columns
        ]
        if group_fields:
            dup_exact_per_name_chart = dup_exact_per_name_chart.groupby(
                group_fields, dropna=False, group_keys=False
            ).head(100)
        else:
            dup_exact_per_name_chart = dup_exact_per_name_chart.head(100)
    charts["duplicates_exact_per_name_anomalies"] = _records_from_frame(
        dup_exact_per_name_chart,
        columns=[
            "scope",
            "match_mode",
            "match_mode_role",
            "inferential_key_mode",
            "display_name",
            "canonical_name",
            "n",
            "n_pro",
            "n_con",
            "first_seen",
            "last_seen",
            "time_span_minutes",
            "expected_count",
            "p_value",
            "q_value",
            "is_significant",
            "within_5m_pairs",
            "within_15m_pairs",
            "temporal_p_value_within_5m",
            "temporal_p_value_min_gap",
            "position_series",
            "position_count",
            "baseline_source",
            "baseline_label",
            "inferential_status",
            "inferential_reason",
            "family_id",
            "adjustment_method",
            "n_tests",
            "n_tests_in_family",
            "eligible_by_gate",
            "gate_reason",
            "adjusted_p_value",
            "claim_class",
        ],
        max_rows=100_000,
    )
    top_name_timing_sorted = dup_exact_top_name_timing.sort_values(
        ["match_mode", "rank", "bucket_minutes", "bucket_start", "name_key"]
    )
    top_name_timing_rows = _records_from_frame(
        top_name_timing_sorted,
        columns=[
            "scope",
            "match_mode",
            "match_label",
            "match_definition",
            "match_mode_role",
            "inferential_key_mode",
            "rank",
            "name_key",
            "display_name",
            "total_repeated_rows",
            "bucket_start",
            "bucket_minutes",
            "duplicate_rows",
            "n_pro",
            "n_con",
            "n_other",
            "first_seen",
            "last_seen",
            "baseline_source",
            "baseline_label",
            "inferential_status",
            "inferential_reason",
            "claim_class",
        ],
        max_rows=100_000,
    )
    top_name_timing_rank_rows = _records_from_frame(
        top_name_timing_sorted.drop_duplicates(
            subset=["scope", "match_mode", "rank", "name_key"],
            keep="first",
        ),
        columns=[
            "scope",
            "match_mode",
            "match_label",
            "match_definition",
            "match_mode_role",
            "inferential_key_mode",
            "rank",
            "name_key",
            "display_name",
            "total_repeated_rows",
            "baseline_source",
            "baseline_label",
            "inferential_status",
            "inferential_reason",
            "claim_class",
        ],
        max_rows=100_000,
    )
    charts["duplicates_exact_top_name_timing_exact"] = top_name_timing_rows + [
        {**row, "row_kind": "name_rank"} for row in top_name_timing_rank_rows
    ]
    charts["duplicates_exact_position_bucket_deviance"] = _records_from_frame(
        dup_exact_bucket_position.sort_values(
            ["scope", "bucket_minutes", "bucket_start", "position_normalized"]
        ),
        columns=[
            "scope",
            "metric",
            "bucket_start",
            "bucket_minutes",
            "position_normalized",
            "n_bucket_position",
            "n_unique_names",
            "observed",
            "expected",
            "excess",
            "deviance",
            "deviance_ratio",
            "lambda_side",
            "shrink_k",
            "prior_level",
            "is_low_power",
            "inference_status",
            "baseline_source",
            "baseline_label",
            "inferential_status",
            "inferential_reason",
            "claim_class",
        ],
        max_rows=100_000,
    )
    charts["duplicates_exact_null_distribution"] = _records_from_frame(
        dup_exact_null_distribution.sort_values("iteration"),
        columns=[
            "iteration",
            "duplicate_rows",
            "duplicate_row_rate",
            "duplicate_pairs",
            "n_names_ge2",
            "n_names_ge3",
            "n_names_ge5",
            "n_names_ge10",
            "max_count",
            "baseline_source",
            "baseline_label",
            "inferential_status",
            "inferential_reason",
            "inferential_key_mode",
            "claim_class",
        ],
        max_rows=25_000,
    )
    charts["duplicates_exact_swing_impact"] = _records_from_frame(
        dup_exact_swing_impact,
        columns=[
            "scenario",
            "n_pro_effective",
            "n_con_effective",
            "pro_share",
            "baseline_source",
            "baseline_label",
            "inferential_status",
            "inferential_reason",
            "inferential_key_mode",
            "claim_class",
        ],
        max_rows=20,
    )

    # Compatibility aliases retained during contract migration.
    charts["duplicates_exact_top_names"] = charts["duplicates_exact_per_name_anomalies"]
    charts["duplicates_exact_position_switch"] = charts["duplicates_exact_per_name_anomalies"]

    charts["vrdb_collision_evidence_pairs"] = _records_from_frame(
        vrdb_collision_bucket.sort_values(["bucket_minutes", "bucket_start", "slice_id"]),
        columns=[
            "slice_id",
            "slice_type",
            "bucket_start",
            "bucket_minutes",
            "n_rows",
            "n_unique_names",
            "baseline_variant",
            "requested_geo_level",
            "requested_geo_value",
            "effective_geo_level",
            "effective_geo_value",
            "observed_pairs",
            "expected_pairs_analytic",
            "expected_pairs_mean",
            "expected_pairs_median",
            "expected_pairs_p95",
            "expected_pairs_p99",
            "tail_prob_pairs",
            "inferential_status",
            "inferential_reason",
            "top_overrun_names",
            "normalization_version",
            "vrdb_version",
            "rng_root_seed",
            "rng_slice_seed",
            "rng_root_stream_id",
            "rng_stream_pairs",
            "rng_stream_max_name",
        ],
        max_rows=100_000,
    )
    charts["vrdb_collision_evidence_max_name_count"] = _records_from_frame(
        vrdb_collision_bucket.sort_values(["bucket_minutes", "bucket_start", "slice_id"]),
        columns=[
            "slice_id",
            "slice_type",
            "bucket_start",
            "bucket_minutes",
            "n_rows",
            "baseline_variant",
            "effective_geo_level",
            "effective_geo_value",
            "observed_max_name_count",
            "expected_max_name_count_mean",
            "expected_max_name_count_p95",
            "expected_max_name_count_p99",
            "tail_prob_max_name",
            "max_count_reference_available",
            "max_count_reference_reason",
            "inferential_status",
            "inferential_reason",
            "normalization_version",
            "vrdb_version",
            "rng_root_seed",
            "rng_slice_seed",
            "rng_root_stream_id",
            "rng_stream_pairs",
            "rng_stream_max_name",
        ],
        max_rows=100_000,
    )
    vrdb_overrun_chart = vrdb_collision_overrun.copy()
    if not vrdb_overrun_chart.empty and not vrdb_collision_full.empty:
        full_slice_ids = {
            str(value).strip()
            for value in vrdb_collision_full.get("slice_id", pd.Series(dtype=str)).tolist()
            if str(value).strip()
        }
        full_overrun = vrdb_overrun_chart[
            vrdb_overrun_chart.get("slice_id", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .isin(full_slice_ids)
        ]
        if not full_overrun.empty:
            vrdb_overrun_chart = full_overrun
    charts["vrdb_collision_evidence_overrun_names"] = _records_from_frame(
        vrdb_overrun_chart.head(100),
        columns=[
            "slice_id",
            "slice_type",
            "bucket_start",
            "bucket_minutes",
            "name_key",
            "observed_count",
            "expected_count",
            "overrun_count",
            "expected_share",
            "rank",
            "baseline_variant",
            "effective_geo_level",
            "effective_geo_value",
        ],
        max_rows=100,
    )
    duplicate_evidence_matrix_cells, duplicate_evidence_matrix_summary = (
        _build_duplicate_evidence_matrix_frames(
            dup_exact_bucket=dup_exact_bucket,
            vrdb_collision_bucket=vrdb_collision_bucket,
            off_hours_window_control=off_hours_window_control,
            primary_scope=primary_dup_scope_control,
            primary_match_mode=primary_dup_match_mode,
        )
    )
    charts["duplicate_evidence_matrix_overview"] = _records_from_frame(
        duplicate_evidence_matrix_cells.sort_values(
            ["bucket_minutes", "scenario_order", "family_order", "scenario_id", "family_id"]
        ),
        columns=[
            "bucket_minutes",
            "scenario_id",
            "scenario_label",
            "scenario_order",
            "disagreement_kind",
            "family_id",
            "family_label",
            "family_order",
            "signal_level",
            "signal_score",
            "signal_label",
            "window_count",
            "window_share",
            "first_bucket_start",
            "last_bucket_start",
            "interpretation",
            "policy_note",
        ],
        max_rows=5_000,
    )
    charts["duplicate_evidence_matrix_scenario_counts"] = _records_from_frame(
        duplicate_evidence_matrix_summary.sort_values(
            ["bucket_minutes", "scenario_order", "scenario_id"]
        ),
        columns=[
            "bucket_minutes",
            "scenario_id",
            "scenario_label",
            "scenario_order",
            "disagreement_kind",
            "window_count",
            "window_share",
            "first_bucket_start",
            "last_bucket_start",
            "interpretation",
            "policy_note",
        ],
        max_rows=2_000,
    )

    charts["org_anomalies_blank_rate"] = _records_from_frame(
        org_blank_rates.sort_values(["bucket_minutes", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "n_pro",
            "n_con",
            "blank_org_rate",
            "blank_org_rate_wilson_low",
            "blank_org_rate_wilson_high",
            "pro_blank_org_rate",
            "con_blank_org_rate",
            "is_low_power",
            "pro_is_low_power",
            "con_is_low_power",
        ],
        max_rows=25_000,
    )
    charts["org_anomalies_position_rates"] = _records_from_frame(
        org_blank_rates.sort_values(["bucket_minutes", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "n_pro",
            "n_con",
            "n_unknown",
            "pro_blank_org_rate",
            "pro_blank_org_rate_wilson_low",
            "pro_blank_org_rate_wilson_high",
            "pro_is_low_power",
            "con_blank_org_rate",
            "con_blank_org_rate_wilson_low",
            "con_blank_org_rate_wilson_high",
            "con_is_low_power",
            "unknown_blank_org_rate",
            "unknown_blank_org_rate_wilson_low",
            "unknown_blank_org_rate_wilson_high",
            "unknown_is_low_power",
        ],
        max_rows=25_000,
    )

    charts["voter_registry_match_rates"] = _records_from_frame(
        voter_bucket.sort_values(["match_mode", "bucket_minutes", "bucket_start"]),
        columns=[
            "match_mode",
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "n_matched_unique",
            "n_matched_ambiguous",
            "n_unmatched",
            "matched_rate",
            "unmatched_rate",
            "match_rate_wilson_low",
            "match_rate_wilson_high",
            "unmatched_rate_wilson_low",
            "unmatched_rate_wilson_high",
            "matched_rate_pro",
            "matched_rate_pro_wilson_low",
            "matched_rate_pro_wilson_high",
            "matched_rate_con",
            "matched_rate_con_wilson_low",
            "matched_rate_con_wilson_high",
            "expected_match_rate_global",
            "control_low_95_match_global",
            "control_high_95_match_global",
            "control_low_998_match_global",
            "control_high_998_match_global",
            "match_rate_delta_global",
            "is_match_rate_alert_lower",
            "is_match_rate_alert_upper",
            "is_match_rate_alert_any",
            "is_low_power",
            "n_pro",
            "n_con",
        ],
        max_rows=25_000,
    )
    charts["voter_registry_linkage_by_position_rows"] = _records_from_frame(
        voter_position_rows.sort_values(["match_mode", "position_normalized"]),
        columns=list(_VOTER_LINKAGE_POSITION_CHART_COLUMNS)
        + ["expected_match_rate_global", "expected_unmatched_rate_global"],
        max_rows=100,
    )
    charts["voter_registry_linkage_by_position_unique"] = _records_from_frame(
        voter_position_unique.sort_values(["match_mode", "position_normalized"]),
        columns=list(_VOTER_LINKAGE_POSITION_CHART_COLUMNS)
        + ["expected_match_rate_global", "expected_unmatched_rate_global"],
        max_rows=100,
    )
    voter_unmatched_top = voter_unmatched.sort_values(
        ["match_mode", "n_records", "display_name"],
        ascending=[True, False, True],
    )
    if not voter_unmatched_top.empty and "match_mode" in voter_unmatched_top.columns:
        voter_unmatched_top = voter_unmatched_top.groupby(
            "match_mode", dropna=False, group_keys=False
        ).head(100)
    charts["voter_registry_unmatched_names"] = _records_from_frame(
        voter_unmatched_top,
        columns=[
            "match_mode",
            "display_name",
            "canonical_name",
            "n_records",
            "n_pro",
            "n_con",
            "first_seen",
            "last_seen",
            "top_caveat",
            "best_similarity_score",
            "candidate_pool_size",
        ],
        max_rows=1000,
    )
    charts["voter_registry_pairwise_tests"] = _records_from_frame(
        voter_pairwise.assign(
            pair_label=(
                voter_pairwise["unit"].astype(str)
                + ": "
                + voter_pairwise["position_left"].astype(str)
                + " vs "
                + voter_pairwise["position_right"].astype(str)
            )
        ).sort_values(["match_mode", "unit", "p_value", "pair_label"]),
        columns=[
            "match_mode",
            "unit",
            "pair_label",
            "position_left",
            "position_right",
            "left_n_total",
            "left_n_unmatched",
            "left_unmatched_rate",
            "right_n_total",
            "right_n_unmatched",
            "right_unmatched_rate",
            "rate_difference",
            "odds_ratio",
            "p_value",
            "alpha",
            "is_significant",
            "inference_status",
        ],
        max_rows=250,
    )
    charts["voter_registry_sensitivity_modes"] = _records_from_frame(
        voter_sensitivity_modes.sort_values("mode"),
        columns=[
            "mode",
            "match_mode",
            "n_rows",
            "n_unmatched_rows",
            "unmatched_rate_rows",
            "n_unique_names",
            "n_unmatched_unique",
            "unmatched_rate_unique",
        ],
        max_rows=20,
    )
    charts["voter_registry_position_bounds"] = _records_from_frame(
        voter_position_bounds.sort_values(["match_mode", "unit", "position_normalized"]),
        columns=[
            "match_mode",
            "unit",
            "position_normalized",
            "n_total_lower",
            "n_total_upper",
            "matched_rate_lower",
            "matched_rate_upper",
            "matched_rate_span",
            "unmatched_rate_lower",
            "unmatched_rate_upper",
            "unmatched_rate_span",
            "inference_status",
        ],
        max_rows=5_000,
    )
    charts["voter_registry_position_buckets"] = _records_from_frame(
        voter_bucket_position.sort_values(
            ["match_mode", "bucket_minutes", "bucket_start", "position_normalized"]
        ),
        columns=[
            "match_mode",
            "bucket_start",
            "bucket_minutes",
            "position_normalized",
            "n_total",
            "n_matched_unique",
            "n_matched_ambiguous",
            "n_unmatched",
            "matched_rate",
            "unmatched_rate",
            "match_rate_wilson_low",
            "match_rate_wilson_high",
            "unmatched_rate_wilson_low",
            "unmatched_rate_wilson_high",
            "is_low_power",
        ],
        max_rows=25_000,
    )
    # Compatibility aliases retained during contract migration.
    charts["voter_registry_match_by_position"] = charts["voter_registry_linkage_by_position_rows"]
    charts["voter_registry_match_tiers"] = charts["voter_registry_sensitivity_modes"]

    analysis_definitions = registry_analysis_definitions()
    cross_hearing_payload = normalize_leave_one_out_baseline_payload(
        cross_hearing_baseline if isinstance(cross_hearing_baseline, dict) else None
    )
    look_for_details = _detailed_what_to_look_for_by_analysis()
    analysis_help_docs = _build_analysis_help_docs(
        analysis_definitions=analysis_definitions,
        detailed_look_for=look_for_details,
    )
    chart_legend_docs = _default_chart_legend_docs()
    for chart_id in charts.keys():
        if chart_id not in chart_legend_docs:
            chart_legend_docs[chart_id] = _fallback_chart_legend_doc(chart_id)
    chart_help_docs = _build_chart_help_docs(chart_legend_docs=chart_legend_docs)
    analysis_catalog: list[dict[str, Any]] = []

    bucket_map: dict[str, list[int]] = {
        "bursts": _extract_bucket_options(bursts_significant, bursts_tests),
        "off_hours": _extract_bucket_options(off_hours_window_control),
        "duplicates_exact": _extract_bucket_options(dup_exact_bucket),
        "vrdb_collision_evidence": _extract_bucket_options(
            vrdb_collision_bucket,
            vrdb_collision_overrun,
        ),
        "duplicate_evidence_matrix": _extract_bucket_options(
            duplicate_evidence_matrix_summary,
            duplicate_evidence_matrix_cells,
        ),
        "org_anomalies": _extract_bucket_options(org_blank_rates, org_position_rates),
        "voter_registry_match": _extract_bucket_options(voter_bucket, voter_bucket_position),
    }
    standard_buckets = [int(value) for value in BASELINE_PROFILE_BUCKET_MINUTES]
    for definition in registry_analysis_definitions():
        analysis_id = str(definition["id"])
        current = {int(value) for value in bucket_map.get(analysis_id, []) if int(value) > 0}
        bucket_map[analysis_id] = sorted(current.union(standard_buckets))

    for definition in analysis_definitions:
        status, reason = analysis_registry_status(
            detector=definition.get("detector"),
            charts=charts,
            hero_chart_id=str(definition["hero_chart_id"]),
            detail_chart_ids=list(definition["detail_chart_ids"]),
            detector_summaries=detector_summaries,
        )
        analysis_catalog.append(
            {
                "id": definition["id"],
                "title": definition["title"],
                "detector": definition.get("detector"),
                "status": status,
                "reason": reason,
                "hero_chart_id": definition["hero_chart_id"],
                "detail_chart_ids": definition["detail_chart_ids"],
                "bucket_options": bucket_map.get(definition["id"], []),
                "group": definition.get("group", "detector_analysis"),
                "priority": int(definition.get("priority", 50)),
                "how_to_read": definition["how_to_read"],
                "what_to_look_for": definition["what_to_look_for"],
                "what_to_look_for_details": look_for_details.get(str(definition["id"]), []),
                "common_benign_causes": definition["common_benign_causes"],
                "expected_metric_keys": list(definition.get("expected_metric_keys") or []),
                "help_sections": analysis_help_docs.get(str(definition["id"]), {}),
            }
        )

    analysis_allowlist = registry_configured_analysis_ids()
    if analysis_allowlist:
        allowset = set(analysis_allowlist)
        analysis_catalog = [
            analysis
            for analysis in analysis_catalog
            if str(analysis.get("id") or "").strip() in allowset
        ]
        allowlist_order = {analysis_id: index for index, analysis_id in enumerate(analysis_allowlist)}
        analysis_catalog.sort(
            key=lambda analysis: allowlist_order.get(str(analysis.get("id") or ""), len(allowlist_order))
        )
    visible_analysis_ids = [str(analysis.get("id") or "").strip() for analysis in analysis_catalog]
    visible_analysis_id_set = set(visible_analysis_ids)
    focus_analysis_ids = [
        analysis_id for analysis_id in analysis_allowlist if analysis_id in visible_analysis_id_set
    ]
    focus_mode = registry_focus_mode_for_analysis_ids(focus_analysis_ids)
    analysis_metric_map = {
        str(analysis.get("id") or ""): [
            str(metric_key)
            for metric_key in analysis.get("expected_metric_keys", [])
            if isinstance(metric_key, str) and metric_key
        ]
        for analysis in analysis_catalog
        if str(analysis.get("id") or "")
    }
    cross_hearing_payload["analysis_metric_map"] = analysis_metric_map
    visible_chart_ids = {
        str(chart_id)
        for analysis in analysis_catalog
        for chart_id in [analysis.get("hero_chart_id"), *(analysis.get("detail_chart_ids") or [])]
        if isinstance(chart_id, str) and chart_id
    }
    supplemental_chart_ids = {
        "off_hours_hourly_profile",
        "off_hours_summary_compare",
        "off_hours_date_hour_pro_heatmap",
        "off_hours_date_hour_primary_residual_heatmap",
        "off_hours_date_hour_volume_heatmap",
        "overview_position_volume_by_bucket",
        "duplicates_exact_null_distribution",
        "duplicates_exact_top_names",
        "duplicates_exact_position_switch",
        "voter_registry_position_buckets",
        "voter_registry_match_by_position",
        "voter_registry_match_tiers",
    }
    retained_chart_ids = visible_chart_ids | supplemental_chart_ids
    charts = {
        chart_id: rows
        for chart_id, rows in charts.items()
        if chart_id in retained_chart_ids
    }
    chart_legend_docs = {
        chart_id: legend
        for chart_id, legend in chart_legend_docs.items()
        if chart_id in retained_chart_ids
    }
    chart_help_docs = {
        chart_id: help_doc
        for chart_id, help_doc in chart_help_docs.items()
        if chart_id in retained_chart_ids
    }

    global_bucket_options = sorted(
        {
            value
            for analysis in analysis_catalog
            for value in analysis.get("bucket_options", [])
            if isinstance(value, int)
        }
    )
    preferred_global = [
        value for value in BASELINE_PROFILE_BUCKET_MINUTES if value in global_bucket_options
    ]
    if preferred_global:
        global_bucket_options = preferred_global
    total_submissions = _total_submissions_from_counts_per_minute(counts_per_minute)
    default_bucket_minutes = _select_default_bucket_minutes(
        global_bucket_options,
        total_submissions=total_submissions,
    )

    absolute_time_chart_ids = [
        "bursts_hero_timeline",
        "bursts_significance_by_window",
        "bursts_composition_shift",
        "overview_position_volume_by_bucket",
        "off_hours_control_timeline",
        "off_hours_primary_residual_timeline",
        "duplicates_exact_bucket_concentration",
        "duplicates_exact_position_bucket_deviance",
        "vrdb_collision_evidence_pairs",
        "vrdb_collision_evidence_max_name_count",
        "org_anomalies_blank_rate",
        "org_anomalies_position_rates",
        "voter_registry_match_rates",
        "voter_registry_position_buckets",
    ]
    absolute_time_chart_ids = [
        chart_id for chart_id in absolute_time_chart_ids if charts.get(chart_id)
    ]

    resolved_default_dedup_mode = normalize_dedup_mode(
        default_dedup_mode,
        default=DEFAULT_DEDUP_MODE,
    )
    triage_views = build_investigation_views(table_map=table_map)
    investigation = triage_views.get(resolved_default_dedup_mode, triage_views.get("raw", {}))
    triage_summary = investigation.get("triage_summary", {})
    data_quality_panel = build_data_quality_panel(
        table_map=table_map,
        triage_views=triage_views,
        min_cell_n_for_rates=min_cell_n_for_rates,
    )
    hearing_context_panel = _build_hearing_context_panel(
        counts_per_minute,
        hearing_metadata=hearing_metadata,
        min_cell_n_for_rates=min_cell_n_for_rates,
    )

    timezone_name = PACIFIC_TIMEZONE_NAME
    process_markers = hearing_context_panel.get("process_markers", [])
    evidence_taxonomy = default_evidence_taxonomy()
    methodology = build_methodology_content(evidence_taxonomy=evidence_taxonomy)
    duplicate_evidence_matrix_policy = {
        "title": "Cross-family disagreement handling",
        "rules": list(_DUPLICATE_EVIDENCE_MATRIX_POLICY_RULES),
    }
    methodology["definitions"].append(
        {
            "term": "Cross-family evidence matrix",
            "definition": (
                "Side-by-side interpretation layer for duplicate collision, VRDB "
                "collision-null, and behavioral timing signals. It preserves disagreement "
                "without collapsing families into one composite score."
            ),
        }
    )
    methodology["caveats"].append(
        "Disagreement across evidence families does not mean one method failed; it narrows the question."
    )
    methodology["interpretation_guidance"].append(
        "Use the evidence matrix to triage concordant versus discordant scenarios before escalation."
    )
    if "dup_exact_methods" in locals() and isinstance(dup_exact_methods, pd.DataFrame) and not dup_exact_methods.empty:
        baseline_models = sorted(
            {
                str(value)
                for value in dup_exact_methods.get("baseline_model", pd.Series(dtype=str)).tolist()
                if str(value).strip()
            }
        )
        baseline_sources = sorted(
            {
                str(value)
                for value in dup_exact_methods.get("baseline_source", pd.Series(dtype=str)).tolist()
                if str(value).strip()
            }
        )
        degraded = bool(
            pd.to_numeric(
                dup_exact_methods.get("baseline_degraded", pd.Series(dtype=float)),
                errors="coerce",
            )
            .fillna(0.0)
            .astype(float)
            .gt(0.0)
            .any()
        )
        methodology["definitions"].append(
            {
                "term": "Duplicate baseline runtime",
                "definition": (
                    "Duplicate-collision expectations were generated from runtime-selected "
                    f"sources/models: sources={','.join(baseline_sources) or 'unknown'}, "
                    f"models={','.join(baseline_models) or 'unknown'}."
                ),
            }
        )
        if degraded:
            methodology["caveats"].append(
                "Duplicate-collision baseline degraded during runtime; review methods metadata before inference."
            )
        unavailable_scope_rows = dup_exact_methods[
            dup_exact_methods.get("scope_status", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            != "available"
        ].copy()
        if not unavailable_scope_rows.empty:
            for row in unavailable_scope_rows.drop_duplicates(subset=["scope"], keep="first").itertuples(
                index=False
            ):
                scope_name = str(getattr(row, "scope", "")).strip() or "unknown_scope"
                scope_reason = str(getattr(row, "scope_reason", "")).strip() or "unavailable"
                methodology["caveats"].append(
                    "Duplicate-collision scope unavailable during runtime: "
                    f"{scope_name} ({scope_reason})."
                )
        unmatched_scope_rows = dup_exact_methods[
            dup_exact_methods.get("scope", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .eq("unmatched_only")
            & dup_exact_methods.get("inferential_reason", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .eq(_DUPLICATE_INFERENTIAL_REASON_UNMATCHED_SCOPE_BASELINE_UNSUPPORTED)
        ].copy()
        if not unmatched_scope_rows.empty:
            methodology["caveats"].append(
                _duplicate_inferential_reason_label(
                    _DUPLICATE_INFERENTIAL_REASON_UNMATCHED_SCOPE_BASELINE_UNSUPPORTED
                )
            )
        if "dup_exact_bucket" in locals() and isinstance(dup_exact_bucket, pd.DataFrame):
            names_methods = (
                dup_exact_bucket.get("unit_expected_names_method", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
                .str.strip()
                .str.lower()
            )
            uses_occupancy = bool((names_methods == "occupancy_without_replacement").any())
            if uses_occupancy:
                methodology["caveats"].append(
                    "Duplicate bucket expected lines are report-layer baselines: "
                    "rows use proportional-share scaling, names use occupancy from "
                    "hearing multiplicities. Detector diagnostics/tables use the detector "
                    "collision baseline listed in methods (VRDB sources denote sidecar collision-null baselines)."
                )
            else:
                methodology["caveats"].append(
                    "Duplicate names expected lines used row-share fallback because occupancy "
                    "multiplicity profiles were unavailable in this payload build."
                )
        methodology["caveats"].append(
            "Duplicate match modes are not interchangeable inferentially: strict is the primary "
            "inferential key and loose nickname matching is sensitivity-only."
        )
        methodology["duplicate_runtime"] = _records_from_frame(
            dup_exact_methods,
            columns=[
                "scope",
                "scope_status",
                "scope_reason",
                "baseline_source",
                "baseline_label",
                "baseline_model",
                "uncertainty_model",
                "n_used",
                "N_used",
                "metric_primary",
                "baseline_degraded",
                "fallback_policy",
                "collision_key_mode",
                "inferential_key_mode",
                "low_power_min_unique_names_scope",
                "low_power_min_expected_duplicates_scope",
                "low_power_min_unique_names_bucket",
                "low_power_min_expected_duplicates_bucket",
                "low_power_min_unique_names_position",
                "low_power_min_expected_duplicates_position",
                "normalization_version",
                "normalization_version_hash",
                "stratification",
                "stratification_weight_source",
                "stratification_leakage_control",
                "stratification_weight_uncertainty",
                "stratification_endogeneity_uncontrolled",
                "historical_reference_channel",
                "historical_reference_report_count",
                "historical_reference_reports_loaded",
                "historical_reference_missing_table_count",
                "historical_reference_excluded_target",
                "historical_reference_target_report_id",
                "historical_reference_reason",
                "historical_reference_loo_source_path",
                "inferential_status",
                "inferential_reason",
                "family_id",
                "adjustment_method",
                "n_tests",
                "n_tests_in_family",
                "eligible_by_gate",
                "gate_reason",
                "adjusted_p_value",
                "is_significant",
                "claim_class",
                "estimand_primary",
                "non_goals",
                "baseline_semantics",
                "rng_root_seed",
                "rng_root_stream_id",
                "rng_stream_scope_collision",
                "rng_stream_scope_stratified_collision",
                "rng_stream_bucket_collision",
                "rng_stream_bucket_stratified_collision",
                "rng_stream_position_interval",
                "rng_stream_position_permutation",
                "rng_stream_position_cluster_bootstrap",
                "rng_stream_temporal_permutation",
            ],
            max_rows=20,
        )
    duplicate_hypothesis_families = _records_from_frame(
        dup_exact_hypothesis_family_totals.sort_values(["family_order", "family_id"]),
        columns=[
            "family_id",
            "family_label",
            "family_order",
            "adjustment_method",
            "n_tests",
            "n_significant",
            "n_scopes",
        ],
        max_rows=50,
    )
    if not duplicate_hypothesis_families:
        duplicate_hypothesis_families = _records_from_frame(
            dup_exact_hypothesis_families.sort_values(["scope", "family_order", "family_id"]),
            columns=[
                "scope",
                "family_id",
                "family_label",
                "family_order",
                "adjustment_method",
                "n_tests",
                "n_significant",
                "eligible_by_gate",
                "gate_reason",
                "adjusted_p_value",
            ],
            max_rows=250,
        )
    if duplicate_hypothesis_families:
        methodology["duplicate_hypothesis_families"] = duplicate_hypothesis_families
    duplicate_low_power_rows = bool(
        pd.to_numeric(
            dup_exact_collision_bucket.get("is_low_power", pd.Series(dtype=float)),
            errors="coerce",
        )
        .fillna(0.0)
        .astype(float)
        .gt(0.0)
        .any()
        or pd.to_numeric(
            dup_exact_bucket_position.get("is_low_power", pd.Series(dtype=float)),
            errors="coerce",
        )
        .fillna(0.0)
        .astype(float)
        .gt(0.0)
        .any()
    )
    duplicate_gating_text = _DUPLICATE_DEFAULT_INFERENTIAL_GATING
    if duplicate_low_power_rows:
        duplicate_gating_text += " This run includes low-power flags in collision outputs."
    if duplicate_hypothesis_families:
        multiplicity_fragments: list[str] = []
        for row in duplicate_hypothesis_families:
            family_id = str(row.get("family_id", "")).strip()
            n_tests_raw = pd.to_numeric(row.get("n_tests"), errors="coerce")
            n_tests = int(n_tests_raw) if pd.notna(n_tests_raw) else 0
            method = str(row.get("adjustment_method", "")).strip().replace("_", " ")
            if not family_id or not method:
                continue
            multiplicity_fragments.append(f"{family_id}: n={n_tests} ({method})")
        if multiplicity_fragments:
            duplicate_gating_text += " Multiplicity families: " + "; ".join(
                multiplicity_fragments
            ) + "."

    duplicate_chart_declarations = {
        chart_id: {
            "baseline_source": primary_dup_baseline_source,
            "baseline_label": primary_dup_baseline_label,
            "inferential_status": primary_dup_inferential_status,
            "inferential_reason": primary_dup_inferential_reason,
            "inferential_key_mode": primary_dup_inferential_key_mode,
            "inferential_key_label": primary_dup_inferential_key_label,
            "match_mode_policy": duplicate_match_mode_policy,
            "gating": duplicate_gating_text,
            "hypothesis_families": duplicate_hypothesis_families,
        }
        for chart_id in _DUPLICATE_CHART_IDS
    }
    duplicate_table_declarations = {
        table_name: {
            "baseline_source": primary_dup_baseline_source,
            "baseline_label": primary_dup_baseline_label,
            "inferential_status": primary_dup_inferential_status,
            "inferential_reason": primary_dup_inferential_reason,
            "inferential_key_mode": primary_dup_inferential_key_mode,
            "inferential_key_label": primary_dup_inferential_key_label,
            "match_mode_policy": duplicate_match_mode_policy,
            "gating": duplicate_gating_text,
            "hypothesis_families": duplicate_hypothesis_families,
        }
        for table_name in _DUPLICATE_TABLE_NAMES
    }
    duplicate_statistical_contract = {
        "estimand_primary": dup_estimand_primary,
        "non_goals": dup_non_goals,
        "baseline_semantics": dup_baseline_semantics,
        "claim_class": dup_claim_class,
        "baseline_source": primary_dup_baseline_source,
        "baseline_label": primary_dup_baseline_label,
        "inferential_status": primary_dup_inferential_status,
        "inferential_reason": primary_dup_inferential_reason,
        "inferential_key_mode": primary_dup_inferential_key_mode,
        "inferential_key_label": primary_dup_inferential_key_label,
        "match_mode_policy": duplicate_match_mode_policy,
        "gating": duplicate_gating_text,
        "hypothesis_families": duplicate_hypothesis_families,
        "interpretation_callout": (
            "Treat this detector as a collision-burden screen: values above reference-baseline "
            "expectation are follow-up signals, not proof about specific people."
        ),
        "limitations_callout": dup_non_goals,
        "can_conclude": [
            "Whether duplicate-name collision burden is higher or lower than reference-baseline expectation.",
            "Whether collision burden patterns persist across adjacent windows.",
            "Whether outputs are inferential, descriptive-only, or unavailable in this run.",
        ],
        "cannot_conclude": [
            "Identity or intent of any person.",
            "IP/device coordination or per-person duplicate behavior from this public dataset alone.",
            "Definitive manipulation claims without corroborating evidence outside this detector.",
        ],
        "chart_declarations": duplicate_chart_declarations,
        "table_declarations": duplicate_table_declarations,
    }
    theme_options = default_theme_options()
    color_semantics = default_color_semantics()

    payload = {
        "version": 4,
        "analysis_catalog": analysis_catalog,
        "charts": charts,
        "chart_legend_docs": chart_legend_docs,
        "chart_help_docs": chart_help_docs,
        "cross_hearing_baseline": cross_hearing_payload,
        "triage_views": triage_views,
        "triage_summary": triage_summary,
        "data_quality_panel": data_quality_panel,
        "hearing_context_panel": hearing_context_panel,
        "controls": {
            "default_bucket_minutes": default_bucket_minutes,
            "global_bucket_options": global_bucket_options,
            "zoom_sync_groups": {"absolute_time": absolute_time_chart_ids},
            "evidence_taxonomy": evidence_taxonomy,
            "methodology": methodology,
            "theme_options": theme_options,
            "default_theme": "light",
            "color_semantics": color_semantics,
            "dedup_modes": list(DEDUP_MODES),
            "default_dedup_mode": resolved_default_dedup_mode,
            "duplicate_collision_scope_default": primary_dup_scope_control,
            "duplicate_collision_metric_default": primary_dup_unit,
            "duplicate_collision_scope_options": duplicate_scope_options,
            "duplicate_collision_scope_availability": duplicate_scope_availability,
            "duplicate_collision_metric_options": duplicate_metric_options,
            "duplicate_match_mode_default": primary_dup_match_mode,
            "duplicate_match_mode_options": duplicate_match_mode_options,
            "duplicate_inferential_key_mode": primary_dup_inferential_key_mode,
            "duplicate_inferential_key_label": primary_dup_inferential_key_label,
            "duplicate_match_mode_policy": duplicate_match_mode_policy,
            "duplicate_statistical_contract": duplicate_statistical_contract,
            "duplicate_evidence_matrix_policy": duplicate_evidence_matrix_policy,
            "voter_match_mode_default": voter_default_mode,
            "voter_match_mode_options": voter_match_mode_options,
            "timezone": timezone_name,
            "timezone_label": timezone_name,
            "process_markers": process_markers,
            "focus_mode": focus_mode,
            "focus_analysis_ids": focus_analysis_ids,
        },
    }
    payload = _json_safe(payload)
    payload_build_ms = round((perf_counter() - payload_started) * 1000.0, 3)
    payload_json_bytes = len(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    )
    controls = payload.get("controls")
    if isinstance(controls, dict):
        controls["runtime"] = {
            "payload_build_ms": payload_build_ms,
            "payload_json_bytes": payload_json_bytes,
        }
    return payload


def _build_interactive_chart_payload(
    counts_per_minute: pd.DataFrame,
    org_blank_rates: pd.DataFrame,
    voter_match_by_bucket: pd.DataFrame,
) -> dict[str, Any]:
    placeholder_table_map = {
        "artifacts.counts_per_minute": counts_per_minute,
        _table_key("org_anomalies", "organization_blank_rate_by_bucket"): org_blank_rates,
        _table_key("voter_registry_match", "match_by_bucket"): voter_match_by_bucket,
    }
    return _build_interactive_chart_payload_v2(
        table_map=placeholder_table_map,
        detector_summaries={},
    )


def _interactive_chart_payload_from_results(
    results: dict[str, DetectorResult],
    artifacts: dict[str, pd.DataFrame],
    *,
    out_dir: Path | None = None,
    default_dedup_mode: str | None = None,
    min_cell_n_for_rates: int = 25,
    hearing_metadata: HearingMetadata | None = None,
) -> dict[str, Any]:
    table_map = _load_table_map_from_results(results=results, artifacts=artifacts)
    detector_summaries = {name: result.summary for name, result in sorted(results.items())}
    cross_hearing_baseline = _load_cross_hearing_baseline_payload(out_dir)
    return _build_interactive_chart_payload_v2(
        table_map=table_map,
        detector_summaries=detector_summaries,
        cross_hearing_baseline=cross_hearing_baseline,
        default_dedup_mode=default_dedup_mode,
        min_cell_n_for_rates=min_cell_n_for_rates,
        hearing_metadata=hearing_metadata,
    )


def _interactive_chart_payload_from_disk(
    out_dir: Path,
    *,
    default_dedup_mode: str | None = None,
    min_cell_n_for_rates: int = 25,
    hearing_metadata: HearingMetadata | None = None,
) -> dict[str, Any]:
    table_map = _load_table_map_from_disk(out_dir=out_dir)
    detector_summaries = _load_summaries_from_disk(out_dir)
    cross_hearing_baseline = _load_cross_hearing_baseline_payload(out_dir)
    return _build_interactive_chart_payload_v2(
        table_map=table_map,
        detector_summaries=detector_summaries,
        cross_hearing_baseline=cross_hearing_baseline,
        default_dedup_mode=default_dedup_mode,
        min_cell_n_for_rates=min_cell_n_for_rates,
        hearing_metadata=hearing_metadata,
    )
