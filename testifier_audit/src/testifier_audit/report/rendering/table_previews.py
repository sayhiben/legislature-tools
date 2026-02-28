from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

from testifier_audit.detectors.base import DetectorResult
from testifier_audit.report.rendering.constants import (
    _DUPLICATES_EXACT_FULL_PREVIEW_TABLES,
    _VOTER_LINKAGE_POSITION_PREVIEW_COLUMNS,
)
from testifier_audit.report.rendering.payload.common import (
    _canonical_name_to_display_name,
    _with_expected_columns,
)
from testifier_audit.report.rendering.serialization import _serialize_value

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pq = None


_EXCLUDED_REPORT_PREVIEW_TABLES: dict[str, frozenset[str]] = {
    "org_anomalies": frozenset(
        {
            "organization_blank_rate_by_bucket",
            "organization_blank_rate_by_bucket_position",
            "organization_blank_rate_summary",
        }
    )
}


def _include_detector_table_preview(detector_name: str, table_name: str) -> bool:
    return table_name not in _EXCLUDED_REPORT_PREVIEW_TABLES.get(detector_name, frozenset())


def _preview_columns_for_detector_table(
    detector_name: str,
    table_name: str,
) -> list[str] | None:
    if detector_name == "voter_registry_match" and table_name in {
        "linkage_by_position_rows",
        "linkage_by_position_unique",
    }:
        return list(_VOTER_LINKAGE_POSITION_PREVIEW_COLUMNS)
    if detector_name == "voter_registry_match" and table_name == "unmatched_names":
        return [
            "display_name",
            "n_rows",
            "n_pro",
            "n_con",
            "top_caveat",
            "best_similarity_score",
            "candidate_pool_size",
        ]
    if detector_name == "duplicates_exact":
        preview_columns: dict[str, list[str]] = {
            "per_name_tests": [
                "scope",
                "canonical_name",
                "display_name",
                "observed_count",
                "n_pro",
                "n_con",
                "time_span_minutes",
            ],
            "per_name_display": [
                "scope",
                "canonical_name",
                "display_name",
                "observed_count",
                "n_pro",
                "n_con",
                "time_span_minutes",
            ],
            "per_name_anomalies": [
                "scope",
                "canonical_name",
                "display_name",
                "n",
                "n_pro",
                "n_con",
                "time_span_minutes",
            ],
            "repeated_same_bucket": [
                "canonical_name",
                "bucket_start",
                "bucket_minutes",
                "n",
                "n_pro",
                "n_con",
                "n_unknown",
                "bucket_end",
            ],
            "per_name_submission_timing_by_mode": [
                "scope",
                "match_mode",
                "canonical_name",
                "name_key",
                "display_name",
                "bucket_start",
                "position_normalized",
            ],
        }
        return preview_columns.get(table_name)
    if detector_name != "off_hours":
        return None
    preview_columns: dict[str, list[str]] = {
        "off_hours_summary": [
            "off_hours",
            "on_hours",
            "off_hours_ratio",
            "off_hours_pro_rate",
            "on_hours_pro_rate",
            "primary_bucket_minutes",
            "primary_baseline_method",
            "alert_off_hours_min_fraction",
            "primary_alert_min_abs_delta",
            "off_hours_windows_alert_eligible",
            "off_hours_windows_alert_eligible_low_power",
            "off_hours_windows_alert_eligible_tested_fraction",
            "off_hours_windows_alert_eligible_low_power_fraction",
            "off_hours_windows_tested",
            "off_hours_windows_below_primary_control_998",
            "off_hours_windows_above_primary_control_998",
            "off_hours_windows_significant_primary",
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
            "off_hours_primary_alert_max_run_minutes",
            "off_hours_windows_model_available",
            "off_hours_min_primary_delta",
            "off_hours_min_primary_z",
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
        "window_control_profile": [
            "bucket_start",
            "bucket_minutes",
            "is_alert_off_hours_window",
            "is_off_hours_window",
            "is_pure_off_hours_window",
            "n_total",
            "n_known",
            "n_pro",
            "n_con",
            "pro_rate",
            "is_low_power",
            "expected_pro_rate_primary",
            "delta_pro_rate_primary",
            "control_low_95_primary",
            "control_low_998_primary",
            "control_high_95_primary",
            "control_high_998_primary",
            "z_score_primary",
            "q_value_primary",
            "is_significant_primary",
            "is_below_primary_control_998",
            "is_above_primary_control_998",
            "is_material_primary_lower_shift",
            "is_primary_alert_window",
            "is_primary_spc_998_two_sided",
            "is_primary_fdr_two_sided",
            "is_primary_any_flag_channel",
            "is_primary_both_flag_channels",
        ],
        "model_fit_diagnostics": [
            "bucket_minutes",
            "model_fit_method",
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_converged",
            "model_fit_aic",
            "model_fit_used_harmonics",
            "model_fit_window_count",
            "model_fit_available_windows",
            "model_fit_available_fraction",
        ],
        "flag_channel_summary": [
            "rank",
            "channel",
            "channel_label",
            "count",
            "share_of_tested",
        ],
        "flagged_window_diagnostics": [
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "n_known",
            "n_pro",
            "n_con",
            "pro_rate",
            "expected_pro_rate_primary",
            "delta_pro_rate_primary",
            "z_score_primary",
            "p_value_primary_two_sided",
            "q_value_primary_two_sided",
            "is_primary_spc_998_two_sided",
            "is_primary_fdr_two_sided",
            "is_primary_any_flag_channel",
            "is_primary_both_flag_channels",
            "is_primary_alert_window",
            "model_fit_method",
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_used_harmonics",
        ],
        "date_hour_distribution": [
            "date",
            "day_of_week",
            "hour",
            "n_total",
            "n_known",
            "n_pro",
            "n_con",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "n_off_hours",
            "off_hours_fraction",
        ],
        "date_hour_primary_residual_distribution": [
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
        "hour_of_week_distribution": [
            "day_of_week",
            "day_of_week_index",
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "n_off_hours",
            "off_hours_fraction",
        ],
        "hourly_distribution": [
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
    }
    return preview_columns.get(table_name)


def _preview_row_limit_for_detector_table(
    detector_name: str,
    table_name: str,
    *,
    default_max_rows: int,
) -> int | None:
    if detector_name == "voter_registry_match" and table_name == "unmatched_names":
        return 50
    if (
        detector_name == "duplicates_exact"
        and table_name in _DUPLICATES_EXACT_FULL_PREVIEW_TABLES
    ):
        return None
    return default_max_rows


def _prepare_table_for_preview(
    detector_name: str,
    table_name: str,
    table: pd.DataFrame,
) -> pd.DataFrame:
    if table.empty:
        return table
    if detector_name == "duplicates_exact":
        prepared = table.copy()
        if table_name in {"per_name_tests", "per_name_display", "per_name_anomalies"}:
            count_column = (
                "observed_count"
                if "observed_count" in prepared.columns
                else "n"
                if "n" in prepared.columns
                else None
            )
            if count_column is not None:
                counts = pd.to_numeric(prepared[count_column], errors="coerce")
                prepared = prepared[counts >= 2].copy()
            sort_columns: list[str] = []
            ascending: list[bool] = []
            if count_column is not None and count_column in prepared.columns:
                sort_columns.append(count_column)
                ascending.append(False)
            if "display_name" in prepared.columns:
                sort_columns.append("display_name")
                ascending.append(True)
            if "canonical_name" in prepared.columns:
                sort_columns.append("canonical_name")
                ascending.append(True)
            if sort_columns:
                prepared = prepared.sort_values(sort_columns, ascending=ascending)
            return prepared
        if table_name == "per_name_duplicates_by_mode":
            count_column = (
                "observed_count"
                if "observed_count" in prepared.columns
                else "total_repeated_rows"
                if "total_repeated_rows" in prepared.columns
                else "n"
                if "n" in prepared.columns
                else None
            )
            sort_columns: list[str] = []
            ascending: list[bool] = []
            for column in ("scope", "match_mode"):
                if column in prepared.columns:
                    sort_columns.append(column)
                    ascending.append(True)
            if count_column is not None and count_column in prepared.columns:
                sort_columns.append(count_column)
                ascending.append(False)
            if "display_name" in prepared.columns:
                sort_columns.append("display_name")
                ascending.append(True)
            if sort_columns:
                prepared = prepared.sort_values(sort_columns, ascending=ascending)
            return prepared
        if table_name == "repeated_same_bucket":
            sort_columns = [
                column
                for column in ("bucket_minutes", "bucket_start", "canonical_name")
                if column in prepared.columns
            ]
            if sort_columns:
                prepared = prepared.sort_values(sort_columns)
            return prepared
        if table_name == "per_name_submission_timing_by_mode":
            sort_columns = [
                column
                for column in ("scope", "match_mode", "name_key", "bucket_start")
                if column in prepared.columns
            ]
            if sort_columns:
                prepared = prepared.sort_values(sort_columns)
            return prepared
        return prepared

    if detector_name != "voter_registry_match":
        return table

    def _normalize_voter_position_label(value: Any) -> str:
        raw = str(value or "").strip()
        if not raw:
            return "Other"
        normalized = raw.lower()
        if normalized == "pro":
            return "Pro"
        if normalized == "con":
            return "Con"
        if normalized in {"unknown", "other"}:
            return "Other"
        return raw

    prepared = table.copy()
    if table_name in {"linkage_by_position_rows", "linkage_by_position_unique"}:
        if "matched_rate" not in prepared.columns and "match_rate" in prepared.columns:
            prepared["matched_rate"] = prepared["match_rate"]
        if (
            "matched_rate_wilson_low" not in prepared.columns
            and "match_rate_wilson_low" in prepared.columns
        ):
            prepared["matched_rate_wilson_low"] = prepared["match_rate_wilson_low"]
        if (
            "matched_rate_wilson_high" not in prepared.columns
            and "match_rate_wilson_high" in prepared.columns
        ):
            prepared["matched_rate_wilson_high"] = prepared["match_rate_wilson_high"]
        prepared = _with_expected_columns(prepared, list(_VOTER_LINKAGE_POSITION_PREVIEW_COLUMNS))
        prepared["match_mode"] = prepared["match_mode"].fillna("").astype(str).replace("", "loose")
        default_unit = "rows" if table_name == "linkage_by_position_rows" else "unique_names"
        prepared["unit"] = prepared["unit"].fillna("").astype(str).replace("", default_unit)
        prepared["position_normalized"] = prepared["position_normalized"].map(
            _normalize_voter_position_label
        )
        sort_columns = [column for column in ("match_mode", "position_normalized") if column in prepared.columns]
        if sort_columns:
            prepared = prepared.sort_values(sort_columns)
        return prepared

    if table_name != "unmatched_names":
        return table

    if "canonical_name" in prepared.columns:
        canonical_display_names = (
            prepared["canonical_name"].fillna("").astype(str).map(_canonical_name_to_display_name)
        )
    else:
        canonical_display_names = pd.Series("", index=prepared.index, dtype=str)

    if "display_name" not in prepared.columns:
        prepared["display_name"] = canonical_display_names
    else:
        prepared["display_name"] = prepared["display_name"].fillna("").astype(str)
        prepared["display_name"] = prepared["display_name"].where(
            prepared["display_name"].str.strip() != "",
            canonical_display_names,
        )
    return prepared


def _table_preview(
    df: pd.DataFrame,
    max_rows: int | None = 12,
    *,
    columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    limited = df.copy() if max_rows is None else df.head(max_rows).copy()
    if columns:
        selected_columns = [column for column in columns if column in limited.columns]
        if selected_columns:
            limited = limited[selected_columns]
    for column in limited.columns:
        limited[column] = limited[column].map(_serialize_value)
    return limited.to_dict(orient="records")


def _load_summaries_from_disk(out_dir: Path) -> dict[str, dict[str, Any]]:
    summary_dir = out_dir / "summary"
    if not summary_dir.exists():
        return {}

    summaries: dict[str, dict[str, Any]] = {}
    for path in sorted(summary_dir.glob("*.json")):
        with path.open("r", encoding="utf-8") as handle:
            summaries[path.stem] = json.load(handle)
    return summaries


def _artifact_rows_from_disk(out_dir: Path) -> dict[str, int]:
    artifacts_dir = out_dir / "artifacts"
    if not artifacts_dir.exists():
        return {}

    rows: dict[str, int] = {}
    for path in sorted(artifacts_dir.iterdir()):
        if path.suffix == ".parquet":
            if pq is not None:
                rows[path.stem] = int(pq.ParquetFile(path).metadata.num_rows)
        elif path.suffix == ".csv":
            with path.open("r", encoding="utf-8") as handle:
                line_count = sum(1 for _ in handle)
            rows[path.stem] = max(line_count - 1, 0)
    return rows


def _table_previews_from_results(
    results: dict[str, DetectorResult],
    max_rows: int = 12,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    previews: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for detector_name, result in sorted(results.items()):
        detector_tables: dict[str, list[dict[str, Any]]] = {}
        for table_name, table in sorted(result.tables.items()):
            if not _include_detector_table_preview(detector_name, table_name):
                continue
            if table.empty:
                continue
            table_max_rows = _preview_row_limit_for_detector_table(
                detector_name,
                table_name,
                default_max_rows=max_rows,
            )
            table = _prepare_table_for_preview(detector_name, table_name, table)
            detector_tables[table_name] = _table_preview(
                table,
                max_rows=table_max_rows,
                columns=_preview_columns_for_detector_table(detector_name, table_name),
            )
        if detector_tables:
            previews[detector_name] = detector_tables
    return previews


def _load_table_previews_from_disk(
    out_dir: Path,
    max_rows: int = 12,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    tables_dir = out_dir / "tables"
    if not tables_dir.exists():
        return {}

    previews: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(dict)
    for path in sorted(tables_dir.iterdir()):
        if "__" not in path.stem:
            continue
        detector_name, table_name = path.stem.split("__", 1)
        if not _include_detector_table_preview(detector_name, table_name):
            continue
        table_max_rows = _preview_row_limit_for_detector_table(
            detector_name,
            table_name,
            default_max_rows=max_rows,
        )

        table: pd.DataFrame
        try:
            if path.suffix == ".csv":
                table = (
                    pd.read_csv(path)
                    if table_max_rows is None
                    else pd.read_csv(path, nrows=table_max_rows)
                )
            elif path.suffix == ".parquet":
                table = (
                    pd.read_parquet(path)
                    if table_max_rows is None
                    else pd.read_parquet(path).head(table_max_rows)
                )
            else:
                continue
        except Exception:
            continue

        if table.empty:
            continue
        table = _prepare_table_for_preview(detector_name, table_name, table)
        previews[detector_name][table_name] = _table_preview(
            table,
            max_rows=table_max_rows,
            columns=_preview_columns_for_detector_table(detector_name, table_name),
        )

    return dict(previews)


def _load_frame_from_candidates(candidates: list[Path]) -> pd.DataFrame:
    for path in candidates:
        if not path.exists():
            continue
        try:
            if path.suffix == ".parquet":
                return pd.read_parquet(path)
            if path.suffix == ".csv":
                return pd.read_csv(path)
        except Exception:
            continue
    return pd.DataFrame()
