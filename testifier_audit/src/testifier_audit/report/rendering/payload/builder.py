from __future__ import annotations

import json
import logging
from pathlib import Path
from time import perf_counter
from typing import Any

import numpy as np
import pandas as pd

from testifier_audit.detectors.base import DetectorResult
from testifier_audit.features.dedup import DEDUP_MODES, DEFAULT_DEDUP_MODE, normalize_dedup_mode
from testifier_audit.io.hearing_metadata import HearingMetadata
from testifier_audit.proportion_stats import (
    DEFAULT_LOW_POWER_MIN_TOTAL,
    low_power_mask,
    wilson_interval,
)
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

def _build_bucketed_baseline_profiles(
    counts_per_minute: pd.DataFrame,
    bucket_minutes: list[int] | None = None,
) -> pd.DataFrame:
    expected = [
        "minute_bucket",
        "bucket_minutes",
        "n_total",
        "n_pro",
        "n_con",
        "pro_rate",
        "pro_rate_wilson_low",
        "pro_rate_wilson_high",
        "is_low_power",
    ]
    if counts_per_minute.empty or "minute_bucket" not in counts_per_minute.columns:
        return _with_expected_columns(pd.DataFrame(), expected)

    windows = sorted(
        {
            int(value)
            for value in (bucket_minutes or BASELINE_PROFILE_BUCKET_MINUTES)
            if int(value) > 0
        }
    )
    if not windows:
        return _with_expected_columns(pd.DataFrame(), expected)

    working = counts_per_minute.copy()
    working["minute_bucket"] = pd.to_datetime(working["minute_bucket"], errors="coerce")
    working = working.dropna(subset=["minute_bucket"])
    if working.empty:
        return _with_expected_columns(pd.DataFrame(), expected)

    for column in ["n_total", "n_pro", "n_con"]:
        if column not in working.columns:
            working[column] = 0
        working[column] = pd.to_numeric(working[column], errors="coerce").fillna(0.0)

    bucketed: list[pd.DataFrame] = []
    for minutes in windows:
        grouped = (
            working.assign(bucket_start=working["minute_bucket"].dt.floor(f"{int(minutes)}min"))
            .groupby("bucket_start", dropna=True)
            .agg(
                n_total=("n_total", "sum"),
                n_pro=("n_pro", "sum"),
                n_con=("n_con", "sum"),
            )
            .reset_index()
            .rename(columns={"bucket_start": "minute_bucket"})
            .sort_values("minute_bucket")
        )
        if grouped.empty:
            continue

        grouped["bucket_minutes"] = int(minutes)
        grouped["pro_rate"] = (grouped["n_pro"] / grouped["n_total"]).where(grouped["n_total"] > 0)
        grouped["pro_rate_wilson_low"], grouped["pro_rate_wilson_high"] = wilson_interval(
            successes=grouped["n_pro"],
            totals=grouped["n_total"],
        )
        grouped["is_low_power"] = low_power_mask(
            totals=grouped["n_total"],
            min_total=DEFAULT_LOW_POWER_MIN_TOTAL,
        )
        bucketed.append(grouped)

    if not bucketed:
        return _with_expected_columns(pd.DataFrame(), expected)

    combined = pd.concat(bucketed, ignore_index=True).sort_values(
        ["bucket_minutes", "minute_bucket"]
    )
    return _with_expected_columns(combined, expected)


def _build_bucketed_day_hour_profiles(
    baseline_bucket_profiles: pd.DataFrame,
    counts_per_hour: pd.DataFrame,
) -> pd.DataFrame:
    expected = [
        "bucket_minutes",
        "day_of_week",
        "hour",
        "n_total",
        "pro_rate",
        "pro_rate_wilson_low",
        "pro_rate_wilson_high",
        "is_low_power",
    ]

    if baseline_bucket_profiles.empty:
        if counts_per_hour.empty:
            return _with_expected_columns(pd.DataFrame(), expected)
        fallback = counts_per_hour.copy()
        fallback["bucket_minutes"] = 1
        return _with_expected_columns(fallback, expected)

    working = baseline_bucket_profiles.copy()
    if "minute_bucket" not in working.columns:
        return _with_expected_columns(pd.DataFrame(), expected)
    working["minute_bucket"] = pd.to_datetime(working["minute_bucket"], errors="coerce")
    working = working.dropna(subset=["minute_bucket"])
    if working.empty:
        return _with_expected_columns(pd.DataFrame(), expected)

    for column in ["n_total", "n_pro"]:
        if column not in working.columns:
            working[column] = 0
        working[column] = pd.to_numeric(working[column], errors="coerce").fillna(0.0)

    working["day_of_week"] = working["minute_bucket"].dt.day_name()
    working["hour"] = working["minute_bucket"].dt.hour

    grouped = (
        working.groupby(["bucket_minutes", "day_of_week", "hour"], dropna=True)
        .agg(
            n_total=("n_total", "sum"),
            n_pro=("n_pro", "sum"),
        )
        .reset_index()
        .sort_values(["bucket_minutes", "day_of_week", "hour"])
    )
    grouped["pro_rate"] = (grouped["n_pro"] / grouped["n_total"]).where(grouped["n_total"] > 0)
    grouped["pro_rate_wilson_low"], grouped["pro_rate_wilson_high"] = wilson_interval(
        successes=grouped["n_pro"],
        totals=grouped["n_total"],
    )
    grouped["is_low_power"] = low_power_mask(
        totals=grouped["n_total"],
        min_total=DEFAULT_LOW_POWER_MIN_TOTAL,
    )
    return _with_expected_columns(grouped, expected)


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
    counts_per_hour = _with_expected_columns(
        table_map.get("artifacts.counts_per_hour", pd.DataFrame()),
        [
            "day_of_week",
            "hour",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
    )
    name_frequency = _with_expected_columns(
        table_map.get("artifacts.name_frequency", pd.DataFrame()),
        ["display_name", "canonical_name", "n", "n_pro", "n_con", "time_span_minutes"],
    )
    name_text_features = _with_expected_columns(
        table_map.get("artifacts.name_text_features", pd.DataFrame()),
        ["name_length"],
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

    time_bucket_profiles = _with_expected_columns(
        table_map.get(_table_key("procon_swings", "time_bucket_profiles"), pd.DataFrame()),
        [
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "pro_rate",
            "baseline_pro_rate",
            "stable_lower",
            "stable_upper",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_flagged",
            "is_low_power",
        ],
    )
    day_bucket_profiles = _with_expected_columns(
        table_map.get(_table_key("procon_swings", "day_bucket_profiles"), pd.DataFrame()),
        [
            "date",
            "bucket_minutes",
            "slot_start_minute",
            "delta_from_slot_pro_rate",
            "n_total",
            "is_slot_outlier",
            "is_low_power",
        ],
    )
    pro_rate_by_hour = _with_expected_columns(
        table_map.get(_table_key("procon_swings", "pro_rate_by_hour"), pd.DataFrame()),
        [
            "day_of_week",
            "hour",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
    )
    time_of_day_profiles = _with_expected_columns(
        table_map.get(
            _table_key("procon_swings", "time_of_day_bucket_profiles"),
            pd.DataFrame(),
        ),
        [
            "bucket_minutes",
            "slot_start_minute",
            "n_total",
            "pro_rate",
            "baseline_pro_rate",
            "stable_lower",
            "stable_upper",
            "is_flagged",
            "is_low_power",
        ],
    )
    procon_direction_runs = _with_expected_columns(
        table_map.get(_table_key("procon_swings", "direction_runs"), pd.DataFrame()),
        [
            "bucket_minutes",
            "run_id",
            "run_direction",
            "start_bucket",
            "end_bucket",
            "run_length_buckets",
            "total_n",
            "support_n",
            "mean_abs_delta_pro_rate",
            "max_abs_delta_pro_rate",
            "n_flagged_buckets",
            "n_low_power_buckets",
            "flagged_ratio",
            "low_power_ratio",
            "is_long_run",
        ],
    )
    swing_null = _with_expected_columns(
        table_map.get(_table_key("procon_swings", "swing_null_distribution"), pd.DataFrame()),
        ["window_minutes", "iteration", "max_abs_delta_pro_rate"],
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
            "baseline_model",
            "uncertainty_model",
            "n_used",
            "N_used",
            "metric_primary",
            "metrics_reported",
            "baseline_degraded",
            "fallback_policy",
            "collision_key_mode",
            "normalization_version_hash",
            "stratification",
            "censored",
        ],
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
    duplicate_scope_options = sorted(
        {
            str(value).strip()
            for value in dup_exact_methods.get("scope", pd.Series(dtype=str)).tolist()
            if str(value).strip()
        }
    )
    if not duplicate_scope_options:
        duplicate_scope_options = [primary_dup_scope]

    dup_exact_collision_overview = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "collision_overview"), pd.DataFrame()),
        [
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
        ],
    )
    duplicate_metric_options = ["rows_anywhere", "names_anywhere"]
    primary_dup_match_mode = (
        _normalize_report_match_mode(
            dup_exact_methods.get("collision_key_mode", pd.Series(dtype=str)).iloc[0]
            if not dup_exact_methods.empty
            else "strict",
            default="strict",
        )
    )
    duplicate_match_mode_options: list[str] = []
    dup_exact_metric_diagnostics = dup_exact_collision_overview[
        dup_exact_collision_overview["scope"].astype(str).str.len() > 0
    ].copy()

    dup_exact_collision_bucket = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "collision_by_bucket"), pd.DataFrame()),
        [
            "scope",
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
            "excess",
            "baseline_model",
            "baseline_source",
            "baseline_degraded",
            "is_low_power",
            "inference_status",
        ],
    )
    dup_exact_per_name_by_mode = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "per_name_duplicates_by_mode"), pd.DataFrame()),
        [
            "scope",
            "match_mode",
            "match_label",
            "match_definition",
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
        dup_exact_per_name_by_mode["name_key"] = (
            dup_exact_per_name_by_mode.get("name_key", pd.Series(dtype=str))
            .fillna(dup_exact_per_name_by_mode.get("canonical_name", pd.Series(dtype=str)))
            .fillna(dup_exact_per_name_by_mode.get("display_name", pd.Series(dtype=str)))
            .astype(str)
            .str.strip()
        )
    dup_exact_per_name_timing_by_mode = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "per_name_submission_timing_by_mode"), pd.DataFrame()),
        [
            "scope",
            "match_mode",
            "match_label",
            "match_definition",
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
        valid_total_mask = total_rows_in_scope > 0
        valid_rows_mask = valid_total_mask & global_rows_available
        valid_names_mask = valid_total_mask & global_names_available
        if bool(valid_rows_mask.any()):
            expected_rows.loc[valid_rows_mask] = (
                n_rows_numeric.loc[valid_rows_mask]
                * global_rows_numeric.loc[valid_rows_mask]
                / total_rows_in_scope.loc[valid_rows_mask]
            )
        if bool(valid_names_mask.any()):
            expected_names.loc[valid_names_mask] = (
                n_rows_numeric.loc[valid_names_mask]
                * global_names_numeric.loc[valid_names_mask]
                / total_rows_in_scope.loc[valid_names_mask]
            )

        expected_rows = expected_rows.fillna(
            pd.to_numeric(
                dup_exact_bucket.get("legacy_expected_rows", pd.Series(dtype=float)),
                errors="coerce",
            )
        ).fillna(0.0)
        expected_names = expected_names.fillna(0.0)

        dup_exact_bucket["unit_expected_rows"] = expected_rows
        dup_exact_bucket["unit_expected_names"] = expected_names
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
        rows_unit["duplicate_row_rate"] = (
            rows_unit["duplicate_rows"] / rows_unit["n_rows"]
        ).where(rows_unit["n_rows"] > 0, 0.0)

        names_unit = dup_exact_bucket.copy()
        names_unit["metric"] = "names_anywhere"
        names_unit["duplicate_rows"] = names_unit["unit_observed_names"]
        names_unit["expected_duplicate_rows"] = names_unit["unit_expected_names"]
        names_unit["excess_duplicate_rows"] = names_unit["unit_deviation_names"]
        names_unit["duplicate_row_rate"] = (
            names_unit["duplicate_rows"] / names_unit["n_rows"]
        ).where(names_unit["n_rows"] > 0, 0.0)

        dup_exact_bucket = pd.concat([rows_unit, names_unit], ignore_index=True, sort=False)

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

    if not dup_exact_per_name_by_mode.empty:
        dup_exact_per_name = dup_exact_per_name_by_mode.rename(
            columns={
                "observed_count": "n",
            }
        ).copy()
        if not dup_exact_per_name_anomalies.empty:
            anomaly_columns = [
                "scope",
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
    dup_exact_top_name_timing = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "top_name_timing_by_mode"), pd.DataFrame()),
        [
            "scope",
            "match_mode",
            "match_label",
            "match_definition",
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
        dup_exact_top_name_timing["scope"] = (
            dup_exact_top_name_timing.get("scope", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .replace("", primary_dup_scope)
        )
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

    dup_exact_bucket_position = _with_expected_columns(
        table_map.get(
            _table_key("duplicates_exact", "collision_by_bucket_position"),
            pd.DataFrame(),
        ),
        [
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
        ],
    )
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
    dup_exact_swing_impact = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "swing_impact_scenarios"), pd.DataFrame()),
        [
            "scenario",
            "n_pro_effective",
            "n_con_effective",
            "pro_share",
        ],
    )

    org_blank_rates = _with_expected_columns(
        table_map.get(
            _table_key("org_anomalies", "organization_blank_rate_by_bucket"), pd.DataFrame()
        ),
        [
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "blank_org_rate",
            "blank_org_rate_wilson_low",
            "blank_org_rate_wilson_high",
            "pro_blank_org_rate",
            "con_blank_org_rate",
            "is_low_power",
            "pro_is_low_power",
            "con_is_low_power",
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
        (time_bucket_profiles, "bucket_start"),
        (procon_direction_runs, "start_bucket"),
        (procon_direction_runs, "end_bucket"),
        (day_bucket_profiles, "date"),
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

    baseline_bucket_profiles = _build_bucketed_baseline_profiles(
        counts_per_minute=counts_per_minute,
        bucket_minutes=BASELINE_PROFILE_BUCKET_MINUTES,
    )
    baseline_day_hour_profiles = _build_bucketed_day_hour_profiles(
        baseline_bucket_profiles=baseline_bucket_profiles,
        counts_per_hour=counts_per_hour,
    )

    charts: dict[str, list[dict[str, Any]]] = {}

    charts["baseline_volume_pro_rate"] = _records_from_frame(
        baseline_bucket_profiles.sort_values(["bucket_minutes", "minute_bucket"]),
        columns=[
            "minute_bucket",
            "bucket_minutes",
            "n_total",
            "n_pro",
            "n_con",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
        max_rows=25_000,
    )
    charts["baseline_day_hour_volume"] = _records_from_frame(
        baseline_day_hour_profiles.sort_values(["bucket_minutes", "day_of_week", "hour"]),
        columns=[
            "bucket_minutes",
            "day_of_week",
            "hour",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
        max_rows=500,
    )
    charts["baseline_top_names"] = _records_from_frame(
        name_frequency.sort_values("n", ascending=False),
        columns=["display_name", "canonical_name", "n", "n_pro", "n_con", "time_span_minutes"],
        max_rows=200,
    )
    if not name_text_features.empty and "name_length" in name_text_features.columns:
        length_dist = (
            pd.to_numeric(name_text_features["name_length"], errors="coerce")
            .dropna()
            .astype(int)
            .value_counts()
            .sort_index()
            .rename_axis("name_length")
            .reset_index(name="n_names")
        )
    else:
        length_dist = pd.DataFrame()
    charts["baseline_name_length_distribution"] = _records_from_frame(
        length_dist,
        columns=["name_length", "n_names"],
        max_rows=200,
    )

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

    charts["procon_swings_hero_bucket_trend"] = _records_from_frame(
        time_bucket_profiles.sort_values(["bucket_minutes", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "pro_rate",
            "baseline_pro_rate",
            "stable_lower",
            "stable_upper",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_flagged",
            "is_low_power",
        ],
        max_rows=25_000,
    )
    charts["procon_swings_shift_heatmap"] = _records_from_frame(
        day_bucket_profiles.sort_values(["bucket_minutes", "date", "slot_start_minute"]),
        columns=[
            "date",
            "bucket_minutes",
            "slot_start_minute",
            "delta_from_slot_pro_rate",
            "n_total",
            "is_slot_outlier",
            "is_low_power",
        ],
        max_rows=25_000,
    )
    charts["procon_swings_day_hour_heatmap"] = _records_from_frame(
        pro_rate_by_hour.sort_values(["day_of_week", "hour"]),
        columns=[
            "day_of_week",
            "hour",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
        max_rows=1_000,
    )
    charts["procon_swings_time_of_day_profile"] = _records_from_frame(
        time_of_day_profiles.sort_values(["bucket_minutes", "slot_start_minute"]),
        columns=[
            "bucket_minutes",
            "slot_start_minute",
            "n_total",
            "pro_rate",
            "baseline_pro_rate",
            "stable_lower",
            "stable_upper",
            "is_flagged",
            "is_low_power",
        ],
        max_rows=25_000,
    )
    charts["procon_swings_direction_runs"] = _records_from_frame(
        procon_direction_runs.sort_values(["bucket_minutes", "start_bucket"]),
        columns=[
            "bucket_minutes",
            "run_id",
            "run_direction",
            "start_bucket",
            "end_bucket",
            "run_length_buckets",
            "support_n",
            "mean_abs_delta_pro_rate",
            "max_abs_delta_pro_rate",
            "n_flagged_buckets",
            "n_low_power_buckets",
            "flagged_ratio",
            "low_power_ratio",
            "is_long_run",
        ],
        max_rows=10_000,
    )
    charts["procon_swings_null_distribution"] = _records_from_frame(
        swing_null.sort_values(["window_minutes", "iteration"]),
        columns=["window_minutes", "iteration", "max_abs_delta_pro_rate"],
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
        overview_position_volume["n_other_position"] = n_unknown.where(
            n_unknown > 0, residual_other
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
            "unit_deviation_names",
            "n_used",
            "N_used",
            "baseline_model",
            "baseline_source",
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
            "rank",
            "name_key",
            "display_name",
            "total_repeated_rows",
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
        ],
        max_rows=20,
    )

    # Compatibility aliases retained during contract migration.
    charts["duplicates_exact_top_names"] = charts["duplicates_exact_per_name_anomalies"]
    charts["duplicates_exact_position_switch"] = charts["duplicates_exact_per_name_anomalies"]

    charts["org_anomalies_blank_rate"] = _records_from_frame(
        org_blank_rates.sort_values(["bucket_minutes", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_total",
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
        org_position_rates.sort_values(["bucket_minutes", "bucket_start", "position_normalized"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "position_normalized",
            "n_total",
            "blank_org_rate",
            "blank_org_rate_wilson_low",
            "blank_org_rate_wilson_high",
            "is_low_power",
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
        "baseline_profile": _extract_bucket_options(
            baseline_bucket_profiles,
            baseline_day_hour_profiles,
        ),
        "bursts": _extract_bucket_options(bursts_significant, bursts_tests),
        "procon_swings": _extract_bucket_options(
            time_bucket_profiles, day_bucket_profiles, time_of_day_profiles, procon_direction_runs
        ),
        "off_hours": _extract_bucket_options(off_hours_window_control),
        "duplicates_exact": _extract_bucket_options(dup_exact_bucket),
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
        value for value in (1, 5, 15, 30, 60, 120, 240) if value in global_bucket_options
    ]
    if preferred_global:
        global_bucket_options = preferred_global

    absolute_time_chart_ids = [
        "baseline_volume_pro_rate",
        "bursts_hero_timeline",
        "bursts_significance_by_window",
        "bursts_composition_shift",
        "procon_swings_hero_bucket_trend",
        "overview_position_volume_by_bucket",
        "off_hours_control_timeline",
        "off_hours_primary_residual_timeline",
        "duplicates_exact_bucket_concentration",
        "duplicates_exact_position_bucket_deviance",
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
        methodology["duplicate_runtime"] = _records_from_frame(
            dup_exact_methods,
            columns=[
                "scope",
                "baseline_source",
                "baseline_model",
                "uncertainty_model",
                "n_used",
                "N_used",
                "metric_primary",
                "baseline_degraded",
                "fallback_policy",
                "collision_key_mode",
                "stratification",
            ],
            max_rows=20,
        )
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
            "default_bucket_minutes": 30
            if 30 in global_bucket_options
            else (global_bucket_options[0] if global_bucket_options else None),
            "global_bucket_options": global_bucket_options,
            "zoom_sync_groups": {"absolute_time": absolute_time_chart_ids},
            "evidence_taxonomy": evidence_taxonomy,
            "methodology": methodology,
            "theme_options": theme_options,
            "default_theme": "light",
            "color_semantics": color_semantics,
            "dedup_modes": list(DEDUP_MODES),
            "default_dedup_mode": resolved_default_dedup_mode,
            "duplicate_collision_scope_default": primary_dup_scope,
            "duplicate_collision_metric_default": primary_dup_unit,
            "duplicate_collision_scope_options": duplicate_scope_options,
            "duplicate_collision_metric_options": duplicate_metric_options,
            "duplicate_match_mode_default": primary_dup_match_mode,
            "duplicate_match_mode_options": duplicate_match_mode_options,
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
    time_bucket_profiles: pd.DataFrame,
    day_bucket_profiles: pd.DataFrame,
    org_blank_rates: pd.DataFrame,
    voter_match_by_bucket: pd.DataFrame,
) -> dict[str, Any]:
    placeholder_table_map = {
        "artifacts.counts_per_minute": counts_per_minute,
        _table_key("procon_swings", "time_bucket_profiles"): time_bucket_profiles,
        _table_key("procon_swings", "day_bucket_profiles"): day_bucket_profiles,
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
