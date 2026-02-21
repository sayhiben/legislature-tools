from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape

from testifier_audit.detectors.base import DetectorResult
from testifier_audit.proportion_stats import (
    DEFAULT_LOW_POWER_MIN_TOTAL,
    low_power_mask,
    wilson_interval,
)
from testifier_audit.report.analysis_registry import (
    analysis_status as analysis_registry_status,
)
from testifier_audit.report.analysis_registry import (
    default_analysis_definitions as registry_analysis_definitions,
)

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pq = None


BASELINE_PROFILE_BUCKET_MINUTES = [1, 5, 15, 30, 60, 120, 240]

_COLUMN_DESCRIPTION_OVERRIDES: dict[str, str] = {
    "artifact": "Artifact/table identifier written by the pipeline.",
    "rows": "Number of rows available for that artifact/table in this run.",
    "metric": "Detector metric or score family represented by the row.",
    "window": "Window label or index used by that detector output.",
    "window_minutes": "Window size in minutes used to aggregate records.",
    "bucket_minutes": "Bucket size in minutes for this time-series point.",
    "bucket_start": "UTC timestamp marking the start of the aggregation bucket.",
    "minute_bucket": "UTC timestamp rounded to the bucket minute boundary.",
    "start_minute": "UTC minute where the evaluated window starts.",
    "end_minute": "UTC minute where the evaluated window ends.",
    "change_minute": "UTC minute where a structural changepoint was detected.",
    "change_hour": "Hour of day (0-23) for changepoint timing summaries.",
    "change_index": "Sequential changepoint identifier in detector output order.",
    "day_of_week": "Weekday label derived from the event timestamp.",
    "date": "Calendar date (UTC) associated with the aggregated slot.",
    "hour": "Hour of day (0-23) used for hour-level aggregation.",
    "slot_start_minute": "Minute offset from midnight for the day/time slot.",
    "minute_of_hour": "Minute within the hour (0-59) used in clock-face tests.",
    "position_normalized": "Normalized testimony position label (for example pro/con/other).",
    "display_name": "Raw name string as it appeared in submitted data.",
    "canonical_name": "Normalized name used for duplicate and match analysis.",
    "sample_name": "Representative example name for the cluster/score row.",
    "organization_clean": "Normalized organization text after cleanup rules.",
    "left_display_name": "First name in a near-duplicate similarity edge.",
    "right_display_name": "Second name in a near-duplicate similarity edge.",
    "block_key": "Blocking key used to limit candidate pairs for near-duplicate matching.",
    "cluster_id": "Identifier of a near-duplicate name cluster.",
    "cluster_size": "Number of unique names contained in the cluster.",
    "time_span_minutes": "Minutes between first and last observed record in this grouping.",
    "first_seen": "First observed UTC timestamp for this entity/group.",
    "last_seen": "Last observed UTC timestamp for this entity/group.",
    "n": "Count of records in the row's grouping.",
    "n_total": "Total records in the bucket/group (all positions combined).",
    "n_pro": "Count of records labeled pro within the bucket/group.",
    "n_con": "Count of records labeled con within the bucket/group.",
    "n_records": "Total raw records represented by this row.",
    "n_unique_names": "Count of distinct canonical names in the bucket/group.",
    "n_matches": "Count of records whose name matched the voter registry reference.",
    "n_unmatched": "Count of records with no voter-registry name match.",
    "n_windows": "Number of windows evaluated for that parameter setting.",
    "n_significant": "Number of windows passing the detector's significance threshold.",
    "n_changes": "Number of changepoints detected in that summarized bucket.",
    "n_clusters": "Number of near-duplicate clusters with that size.",
    "n_events": "Observed event count for the periodicity slot.",
    "expected_n_events_uniform": "Expected event count under a uniform minute-of-hour baseline.",
    "observed_count": "Observed submission count in the tested burst window.",
    "expected_count": "Expected submission count from the fitted baseline/null model.",
    "off_hours": "Count of records in configured off-hours period.",
    "on_hours": "Count of records in configured on-hours period.",
    "off_hours_ratio": "Fraction of all records submitted during off-hours.",
    "off_hours_pro_rate": "Pro share during off-hours windows.",
    "on_hours_pro_rate": "Pro share during on-hours windows.",
    "off_hours_pro_rate_wilson_low": "Lower Wilson bound for off-hours pro share.",
    "off_hours_pro_rate_wilson_high": "Upper Wilson bound for off-hours pro share.",
    "on_hours_pro_rate_wilson_low": "Lower Wilson bound for on-hours pro share.",
    "on_hours_pro_rate_wilson_high": "Upper Wilson bound for on-hours pro share.",
    "off_hours_is_low_power": "True when off-hours sample size is too small for stable inference.",
    "on_hours_is_low_power": "True when on-hours sample size is too small for stable inference.",
    "pro_rate": "Share of records that are pro in this row (0 to 1).",
    "baseline_pro_rate": "Reference pro share expected for this day/time context.",
    "stable_lower": "Lower bound of the expected stable pro-share band.",
    "stable_upper": "Upper bound of the expected stable pro-share band.",
    "delta_from_slot_pro_rate": "Difference between observed pro share and slot baseline.",
    "deviation_from_uniform": (
        "Difference between observed and uniform-expected periodic count/share."
    ),
    "rate_ratio": "Observed/expected rate ratio for burst testing.",
    "match_rate": "Share of records matched to voter registry (0 to 1).",
    "pro_match_rate": "Match rate for pro-position records only.",
    "con_match_rate": "Match rate for con-position records only.",
    "blank_org_rate": "Share of records with blank/null organization values.",
    "pro_blank_org_rate": "Blank organization share among pro records.",
    "con_blank_org_rate": "Blank organization share among con records.",
    "unique_ratio": "Distinct-name ratio: unique names divided by total records.",
    "threshold_unique_ratio": "Configured or modeled threshold used to flag unusual uniqueness.",
    "alphabetical_ratio": "Share of windows flagged as alphabetically ordered.",
    "avg_records_per_bucket": "Average number of records per evaluated bucket.",
    "is_alphabetical": "1/true when local ordering is alphabetical under detector rules.",
    "is_significant": "True when the hypothesis test passes configured significance thresholds.",
    "is_flagged": "Detector-level flag for windows considered anomalous/elevated.",
    "is_slot_outlier": "True when day/slot delta is an outlier versus peer slots.",
    "is_anomaly": "True when multivariate model scores this bucket as anomalous.",
    "is_model_eligible": "True when bucket has enough support/features for model scoring.",
    "is_changepoint": "True when timestamp coincides with a detected structural break.",
    "is_low_power": "True when sample size is too small for stable proportion inference.",
    "pro_is_low_power": "True when pro-side subgroup support is low.",
    "con_is_low_power": "True when con-side subgroup support is low.",
    "pro_rate_wilson_low": "Lower Wilson confidence bound for pro rate.",
    "pro_rate_wilson_high": "Upper Wilson confidence bound for pro rate.",
    "blank_org_rate_wilson_low": "Lower Wilson confidence bound for blank-org rate.",
    "blank_org_rate_wilson_high": "Upper Wilson confidence bound for blank-org rate.",
    "match_rate_wilson_low": "Lower Wilson confidence bound for registry match rate.",
    "match_rate_wilson_high": "Upper Wilson confidence bound for registry match rate.",
    "pro_match_rate_wilson_low": "Lower Wilson bound for pro-only registry match rate.",
    "pro_match_rate_wilson_high": "Upper Wilson bound for pro-only registry match rate.",
    "con_match_rate_wilson_low": "Lower Wilson bound for con-only registry match rate.",
    "con_match_rate_wilson_high": "Upper Wilson bound for con-only registry match rate.",
    "q_value": "Multiple-testing-adjusted p-value controlling false discovery rate.",
    "chi_square_p_value": "P-value from chi-square comparison between grouped distributions.",
    "autocorr": "Autocorrelation value at the specified lag.",
    "abs_autocorr": "Absolute autocorrelation magnitude (strength regardless of sign).",
    "lag_minutes": "Lag distance in minutes for autocorrelation analysis.",
    "period_minutes": "Cycle length in minutes derived from spectral analysis.",
    "frequency_per_minute": "Equivalent cycle frequency in events per minute.",
    "power": "Spectral power (relative strength) at that detected frequency.",
    "share": "Fraction of all events in the specified minute-of-hour bin.",
    "z_score_uniform": "Standardized deviation from uniform expectation.",
    "anomaly_score": "Model anomaly score; higher values indicate rarer feature combinations.",
    "anomaly_score_percentile": "Percentile rank of anomaly score within the run.",
    "composite_score": "Combined detector evidence score used for prioritization.",
    "evidence_count": "Number of detector signals contributing to this score.",
    "evidence_flags": "Comma-separated detector signal tags active in this bucket/window.",
    "flag": "Detector flag name counted in evidence composition.",
    "count": "Count of rows/windows/flags for the grouped label.",
    "burst_signal": "Binary indicator that burst detector contributed evidence.",
    "swing_signal": "Binary indicator that pro/con swing detector contributed evidence.",
    "changepoint_signal": "Binary indicator that changepoint detector contributed evidence.",
    "ml_anomaly_signal": (
        "Binary indicator that multivariate anomaly detector contributed evidence."
    ),
    "rarity_signal": "Binary indicator from rarity-focused detector components.",
    "unique_signal": "Binary indicator from unique-name ratio detector components.",
    "mean_before": "Mean value in the segment before the changepoint.",
    "mean_after": "Mean value in the segment after the changepoint.",
    "delta": "Signed difference (after - before) at the changepoint.",
    "abs_delta": "Absolute change magnitude at the changepoint.",
    "weirdness_score": "Name-string irregularity score; higher implies less typical structure.",
    "name_length": "Character length of the normalized name token/string.",
    "n_names": "Count of names in the associated histogram bin.",
    "non_alpha_fraction": "Fraction of characters that are non alphabetic.",
    "name_entropy": "Character-level entropy; higher values suggest more randomness.",
    "rarity_median": "Median rarity score among names in the bucket.",
    "rarity_p95": "95th percentile rarity score in the bucket.",
    "threshold": "Detector threshold used to flag bursts/excess concentration.",
    "iteration": "Null-simulation iteration index.",
    "max_window_count": "Maximum simulated count observed in that iteration/window setup.",
    "max_abs_delta_pro_rate": "Maximum absolute pro-rate delta observed in null simulation.",
    "similarity": "String similarity score for near-duplicate candidate pair.",
    "token": "Name token extracted during rarity/coverage diagnostics.",
    "value": "Detector-specific numeric value for the metric column.",
    "score": "Detector/model score for the row.",
}


def _template_env() -> Environment:
    templates_path = Path(__file__).resolve().parent / "templates"
    return Environment(
        loader=FileSystemLoader(str(templates_path)),
        autoescape=select_autoescape(enabled_extensions=("html", "xml")),
        trim_blocks=True,
        lstrip_blocks=True,
    )


def _serialize_value(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, pd.Timedelta):
        return str(value)
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, pd.Timedelta):
        return str(value)
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (int, str, bool)) or value is None:
        return value
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _table_preview(df: pd.DataFrame, max_rows: int = 12) -> list[dict[str, Any]]:
    limited = df.head(max_rows).copy()
    for column in limited.columns:
        limited[column] = limited[column].map(_serialize_value)
    return _json_safe(limited.to_dict(orient="records"))


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
            if table.empty:
                continue
            detector_tables[table_name] = _table_preview(table, max_rows=max_rows)
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

        table: pd.DataFrame
        try:
            if path.suffix == ".csv":
                table = pd.read_csv(path, nrows=max_rows)
            elif path.suffix == ".parquet":
                table = pd.read_parquet(path).head(max_rows)
            else:
                continue
        except Exception:
            continue

        if table.empty:
            continue
        previews[detector_name][table_name] = _table_preview(table, max_rows=max_rows)

    return dict(previews)


def _evidence_bundle_preview_from_results(
    results: dict[str, DetectorResult],
    max_rows: int = 25,
) -> list[dict[str, Any]]:
    composite = results.get("composite_score")
    if composite is None:
        return []
    table = composite.tables.get("evidence_bundle_windows")
    if table is None or table.empty:
        return []
    return _table_preview(table, max_rows=max_rows)


def _evidence_bundle_preview_from_disk(
    out_dir: Path,
    max_rows: int = 25,
) -> list[dict[str, Any]]:
    tables_dir = out_dir / "tables"
    if not tables_dir.exists():
        return []

    candidates = [
        tables_dir / "composite_score__evidence_bundle_windows.parquet",
        tables_dir / "composite_score__evidence_bundle_windows.csv",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            if path.suffix == ".parquet":
                table = pd.read_parquet(path).head(max_rows)
            else:
                table = pd.read_csv(path, nrows=max_rows)
        except Exception:
            continue
        if table.empty:
            return []
        return _table_preview(table, max_rows=max_rows)
    return []


def _rare_names_table_preview_from_results(
    results: dict[str, DetectorResult],
    table_name: str,
    max_rows: int = 25,
) -> list[dict[str, Any]]:
    rare_names = results.get("rare_names")
    if rare_names is None:
        return []
    table = rare_names.tables.get(table_name)
    if table is None or table.empty:
        return []
    return _table_preview(table, max_rows=max_rows)


def _rare_names_table_preview_from_disk(
    out_dir: Path,
    table_name: str,
    max_rows: int = 25,
) -> list[dict[str, Any]]:
    tables_dir = out_dir / "tables"
    if not tables_dir.exists():
        return []

    candidates = [
        tables_dir / f"rare_names__{table_name}.parquet",
        tables_dir / f"rare_names__{table_name}.csv",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            if path.suffix == ".parquet":
                table = pd.read_parquet(path).head(max_rows)
            else:
                table = pd.read_csv(path, nrows=max_rows)
        except Exception:
            continue
        if table.empty:
            return []
        return _table_preview(table, max_rows=max_rows)
    return []


def _periodicity_table_preview_from_results(
    results: dict[str, DetectorResult],
    table_name: str,
    max_rows: int = 25,
) -> list[dict[str, Any]]:
    periodicity = results.get("periodicity")
    if periodicity is None:
        return []
    table = periodicity.tables.get(table_name)
    if table is None or table.empty:
        return []
    return _table_preview(table, max_rows=max_rows)


def _periodicity_table_preview_from_disk(
    out_dir: Path,
    table_name: str,
    max_rows: int = 25,
) -> list[dict[str, Any]]:
    tables_dir = out_dir / "tables"
    if not tables_dir.exists():
        return []

    candidates = [
        tables_dir / f"periodicity__{table_name}.parquet",
        tables_dir / f"periodicity__{table_name}.csv",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            if path.suffix == ".parquet":
                table = pd.read_parquet(path).head(max_rows)
            else:
                table = pd.read_csv(path, nrows=max_rows)
        except Exception:
            continue
        if table.empty:
            return []
        return _table_preview(table, max_rows=max_rows)
    return []


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


def _records_from_frame(
    frame: pd.DataFrame,
    columns: list[str],
    max_rows: int | None = None,
) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    selected = [column for column in columns if column in frame.columns]
    if not selected:
        return []
    working = frame[selected].copy()
    if max_rows is not None:
        working = working.head(max_rows)
    for column in working.columns:
        working[column] = working[column].map(_serialize_value)
    return _json_safe(working.to_dict(orient="records"))


def _table_key(detector: str, table_name: str) -> str:
    return f"{detector}.{table_name}"


def _humanize_identifier(value: str) -> str:
    return " ".join(token for token in str(value).strip().replace("-", "_").split("_") if token)


def _default_column_description(column: str) -> str:
    label = _humanize_identifier(column)
    if not label:
        return "Column value from detector output."
    lower = str(column).lower()
    if lower.startswith("n_"):
        return f"Count of {_humanize_identifier(lower[2:])} in this row grouping."
    if lower.endswith("_rate"):
        return f"Proportion metric for {label}, on a 0 to 1 scale."
    if lower.endswith("_ratio"):
        return f"Ratio metric for {label}; compare against section baseline/threshold context."
    if lower.endswith("_wilson_low"):
        base = _humanize_identifier(lower.removesuffix("_wilson_low"))
        return f"Lower Wilson confidence bound for {base}."
    if lower.endswith("_wilson_high"):
        base = _humanize_identifier(lower.removesuffix("_wilson_high"))
        return f"Upper Wilson confidence bound for {base}."
    if lower.startswith("is_"):
        return f"Boolean indicator for {label}."
    if "minute" in lower or "hour" in lower or lower.endswith("_time") or lower.endswith("_date"):
        return f"Time coordinate for {label}."
    return f"Detector output field for {label}."


def _describe_column(column: str) -> str:
    normalized = str(column or "").strip()
    if not normalized:
        return "Column value from detector output."
    return _COLUMN_DESCRIPTION_OVERRIDES.get(normalized, _default_column_description(normalized))


def _table_column_docs_from_rows(rows: list[dict[str, Any]]) -> dict[str, str]:
    if not rows:
        return {}
    ordered_columns: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for column in row.keys():
            key = str(column)
            if key in seen:
                continue
            seen.add(key)
            ordered_columns.append(key)
    return {column: _describe_column(column) for column in ordered_columns}


def _build_table_column_docs(
    table_previews: dict[str, dict[str, list[dict[str, Any]]]],
    artifact_rows: dict[str, int],
    evidence_bundle_preview: list[dict[str, Any]],
    rarity_coverage_preview: list[dict[str, Any]],
    rarity_unmatched_first_preview: list[dict[str, Any]],
    rarity_unmatched_last_preview: list[dict[str, Any]],
    clockface_top_preview: list[dict[str, Any]],
) -> dict[str, dict[str, str]]:
    docs: dict[str, dict[str, str]] = {}
    for detector_name, detector_tables in sorted(table_previews.items()):
        for table_name, rows in sorted(detector_tables.items()):
            key = _table_key(detector_name, table_name)
            docs[key] = _table_column_docs_from_rows(rows)

    docs["artifacts.artifact_rows"] = _table_column_docs_from_rows(
        [
            {"artifact": artifact_name, "rows": row_count}
            for artifact_name, row_count in sorted(artifact_rows.items())
        ]
    )
    docs["composite_score.evidence_bundle_preview"] = _table_column_docs_from_rows(
        evidence_bundle_preview
    )
    docs["rare_names.rarity_coverage_preview"] = _table_column_docs_from_rows(
        rarity_coverage_preview
    )
    docs["rare_names.rarity_unmatched_first_preview"] = _table_column_docs_from_rows(
        rarity_unmatched_first_preview
    )
    docs["rare_names.rarity_unmatched_last_preview"] = _table_column_docs_from_rows(
        rarity_unmatched_last_preview
    )
    docs["periodicity.clockface_top_preview"] = _table_column_docs_from_rows(clockface_top_preview)

    return docs


def _build_table_help_docs(
    table_column_docs: dict[str, dict[str, str]],
) -> dict[str, dict[str, str]]:
    docs: dict[str, dict[str, str]] = {}
    for table_key, column_docs in sorted(table_column_docs.items()):
        column_names = list(column_docs.keys())
        has_rate = any(
            name.endswith("_rate") or "ratio" in name or "percentile" in name
            for name in column_names
        )
        has_counts = any(
            name.startswith("n_") or name in {"count", "rows", "n"}
            for name in column_names
        )
        has_time = any(
            token in name
            for name in column_names
            for token in ("minute", "hour", "bucket", "date", "time")
        )
        detector_label = _humanize_identifier(table_key.replace(".", " "))
        first_columns = ", ".join(column_names[:6]) if column_names else "no preview columns"

        value_context = []
        if has_rate:
            value_context.append("rate/proportion columns")
        if has_counts:
            value_context.append("volume/count columns")
        if has_time:
            value_context.append("time keys")
        context_text = ", ".join(value_context) if value_context else "detector-specific fields"

        docs[table_key] = {
            "what_is_this": (
                f"This table is a preview of {detector_label}. "
                "It exposes row-level values behind chart aggregates so you can inspect "
                "the exact buckets, categories, and flags that produced a visual signal. "
                "Use it when you need to answer which concrete records created a peak, "
                "dip, or anomaly marker."
            ),
            "why_it_matters": (
                "Tables are essential for auditability: they let you sort, filter, "
                "and verify whether visual anomalies are supported by real volume, "
                "consistent metadata, and non-sparse support. "
                "They also reveal false positives where a chart looks dramatic but "
                "underlying rows are low-power or internally contradictory."
            ),
            "how_to_interpret": (
                "Start with key columns and filter around flagged times/categories. "
                f"This table includes {context_text}. "
                "Read left-to-right from identifiers to volume/rate fields to flags, "
                "and compare adjacent rows to separate isolated outliers from "
                "persistent structure. Use the column glossary to avoid over-"
                "interpreting similarly named fields with different semantics."
            ),
            "what_to_look_for": (
                "Look for rows where multiple indicators move together (for example, "
                "high counts plus directional rates plus flags), and check whether "
                "those rows cluster in adjacent windows. "
                "Strong evidence usually appears as recurring patterns across nearby "
                "rows, not single extreme entries."
            ),
            "momentary_high_low": (
                "A single extreme row can be a genuine event or a sparse-data outlier. "
                "Momentary lows can be normal lulls; validate by checking neighboring "
                "rows and low-power indicators. "
                "Short-lived highs often map to reminders, queue releases, or reporting "
                "timing; short-lived lows often map to expected inactivity or ingest lag."
            ),
            "extended_high_low": (
                "Extended runs of high/low values across many rows are stronger signs "
                "of regime-level behavior. Persistent shifts that also align with "
                "chart-level signals are higher-confidence anomalies. "
                "Extended highs may indicate sustained mobilization or process skew; "
                "extended lows may indicate suppressed activity, missing data segments, "
                "or a stable low-intensity baseline."
            ),
            "column_highlight": (
                f"Primary columns in this preview: {first_columns}."
            ),
        }

    return docs


def _detailed_what_to_look_for_by_analysis() -> dict[str, list[str]]:
    return {
        "baseline_profile": [
            (
                "Short, isolated spikes in volume with no matching shift in pro rate "
                "or corroborating detector flags are often random campaign pulses "
                "rather than systemic manipulation."
            ),
            (
                "Extended level shifts (for example, 60-240 minutes) in both volume "
                "and composition, especially when Wilson bands tighten, suggest a "
                "meaningful regime change worth cross-checking against changepoints "
                "and composite evidence."
            ),
            (
                "Very low overnight volume can create dramatic percentage swings; "
                "prioritize windows where elevated rates persist after local volume "
                "recovers into daytime traffic."
            ),
        ],
        "bursts": [
            (
                "Single-window rate-ratio peaks can be benign; stronger signals are "
                "contiguous runs of elevated rate ratios that recur at multiple window "
                "sizes (for example 5m and 30m both elevated)."
            ),
            (
                "High observed counts with low q-values in sustained windows imply "
                "concentration beyond baseline expectation, especially when these "
                "bursts overlap with duplicate-name or swing anomalies."
            ),
            (
                "Suppressed or unusually flat burst activity can also be informative "
                "if baseline volume is high; a lack of natural variability may "
                "indicate synchronized intake behavior or batching."
            ),
        ],
        "procon_swings": [
            (
                "Brief pro-rate jumps with wide Wilson intervals typically indicate "
                "low-support noise; treat them as weak unless adjacent buckets move "
                "in the same direction with tighter intervals."
            ),
            (
                "Extended daytime streaks of positive or negative shifts (multiple "
                "contiguous buckets) can indicate directional mobilization, queueing "
                "effects, or operational gating; confirm with day/hour and "
                "time-of-day panels."
            ),
            (
                "Large off-hours directional blocks that reverse at wake-hour "
                "transitions may indicate temporally segmented participation behavior, "
                "including potential strategic timing by one side."
            ),
        ],
        "changepoints": [
            (
                "Look for clustered breakpoints across both volume and pro rate; "
                "multi-metric co-occurrence is usually more meaningful than a "
                "solitary break in one metric."
            ),
            (
                "Large absolute deltas with sustained post-break behavior "
                "(not immediate reversion) indicate structural transitions rather "
                "than transient spikes."
            ),
            (
                "Repeated changes at similar hours across days can reflect "
                "operational schedules; treat as lower risk unless change magnitudes "
                "are extreme and detector corroboration is strong."
            ),
        ],
        "off_hours": [
            (
                "A higher off-hours pro rate or off-hours volume share can be benign "
                "when support is low; prioritize differences that remain outside "
                "Wilson overlap and persist across days."
            ),
            (
                "Extended off-hours enrichment paired with normal daytime behavior "
                "can suggest time-targeted mobilization; compare with swing and "
                "periodicity detectors for repeated timing signatures."
            ),
            (
                "Unexpectedly low off-hours activity can also be anomalous in "
                "historically active datasets and may indicate ingestion gaps or "
                "narrowly timed campaign workflows."
            ),
        ],
        "duplicates_exact": [
            (
                "Short bursts of repeated names in tiny windows may occur during "
                "legitimate group actions; concern rises when concentration repeats "
                "across multiple larger buckets."
            ),
            (
                "Names that appear repeatedly while switching pro/con positions are "
                "higher-priority review targets because they indicate inconsistent "
                "stance representation under one canonical identity."
            ),
            (
                "Persistent duplicate concentration during otherwise stable baseline "
                "periods can imply scripted submissions or queue replay effects "
                "rather than organic participation."
            ),
        ],
        "duplicates_near": [
            (
                "Many small near-duplicate clusters are often benign "
                "typo/transliteration noise; stronger signals are rapid growth of "
                "large clusters in compressed time spans."
            ),
            (
                "High similarity edges among many distinct names during high-volume "
                "windows can indicate templated naming patterns or normalization "
                "collisions that warrant manual spot checks."
            ),
            (
                "Extended periods where cluster size and record counts rise together "
                "can point to coordinated intake streams, especially when aligned "
                "with burst and swing flags."
            ),
        ],
        "sortedness": [
            (
                "Single alphabetical spikes in small buckets can be accidental; "
                "repeated elevated alphabetical ratios across 15m-120m buckets "
                "suggest process-level ordering behavior."
            ),
            (
                "Sustained ordered streaks during high-volume windows are unusual "
                "for organic arrivals and may imply batch uploads, sorted lists, or "
                "deterministic queue processing."
            ),
            (
                "Low sortedness is expected for organic traffic, so abrupt "
                "transitions from unsorted to highly sorted and back are more "
                "informative than consistently modest ratios."
            ),
        ],
        "rare_names": [
            (
                "Short-lived unique-ratio increases during low volume can be "
                "misleading; investigate when unique-ratio elevation persists into "
                "higher-support windows."
            ),
            (
                "Concurrent rises in weirdness scores, singleton concentration, and "
                "rarity quantiles indicate novelty concentration beyond normal "
                "lexical drift."
            ),
            (
                "Extended rarity suppression (unusually low novelty) can also be "
                "noteworthy in broad public hearings and may suggest repeated "
                "template populations."
            ),
        ],
        "org_anomalies": [
            (
                "Blank-organization spikes in low-support windows are weak evidence; "
                "prioritize wide windows where blank rate rises and Wilson bands "
                "remain narrow."
            ),
            (
                "Divergence between pro and con blank-org rates over sustained "
                "periods can indicate side-specific form behavior, campaign guidance, "
                "or data-entry heterogeneity."
            ),
            (
                "Sharp blank-rate reversals around specific times may indicate UX "
                "changes, batch imports, or conditional form paths and should be "
                "checked against operational logs."
            ),
        ],
        "voter_registry_match": [
            (
                "Transient match-rate drops in very small buckets are expected; treat "
                "as notable only when low-power flags are absent and the drop "
                "persists across neighboring windows."
            ),
            (
                "Sustained side-specific divergence (pro vs con) with adequate volume "
                "may indicate composition shifts, normalization mismatch, or "
                "targeted non-registered participation."
            ),
            (
                "Rapid oscillation between high and low match rates can suggest "
                "mixed data sources or ingestion inconsistencies; cross-check "
                "unmatched-name concentration for diagnostics."
            ),
        ],
        "periodicity": [
            (
                "Minor periodic peaks are normal in outreach-driven datasets; "
                "stronger signals appear when clock-face concentration, "
                "autocorrelation peaks, and spectrum peaks align."
            ),
            (
                "Narrow high-power peaks at specific periods (for example near exact "
                "campaign cadence intervals) can indicate automation or tightly "
                "scheduled reminders."
            ),
            (
                "Extended suppression of expected periodic structure in otherwise "
                "campaign-heavy contexts may imply missing intervals or "
                "preprocessing artifacts."
            ),
        ],
        "multivariate_anomalies": [
            (
                "Single high anomaly buckets with low support can be model-noise; "
                "prioritize consecutive high-score windows with model eligibility and "
                "corroborating detector evidence."
            ),
            (
                "Joint excursions in volume, duplicate fraction, blank-org rate, and "
                "pro-rate shape are stronger than any one feature spike in isolation."
            ),
            (
                "Extended high-percentile stretches can indicate sustained "
                "behavioral mode changes; inspect top buckets and feature projection "
                "for which dimensions drive score elevation."
            ),
        ],
        "composite_score": [
            (
                "High composite windows are most useful when evidence-count is high "
                "and signals come from independent detectors rather than one "
                "detector repeated across scales."
            ),
            (
                "Short isolated composite spikes can still be benign; extended "
                "elevated runs with overlapping burst/swing/changepoint/ML evidence "
                "are higher-priority review candidates."
            ),
            (
                "Very low composite scores during known high-activity periods can "
                "reveal under-sensitive detector settings or data-quality gaps and "
                "should trigger configuration review."
            ),
        ],
    }


def _analysis_help_hints() -> dict[str, dict[str, str]]:
    return {
        "baseline_profile": {
            "primary_metric": "baseline volume and composition drift",
            "momentary_high": (
                "a short notice event, reminder blast, or temporary queue release"
            ),
            "momentary_low": (
                "normal minute-level quiet periods or ingest timing jitter"
            ),
            "extended_high": (
                "a sustained participation regime shift that can affect all downstream "
                "detectors"
            ),
            "extended_low": (
                "potential ingestion gaps, hearing lulls, or sustained reduced campaign "
                "activity"
            ),
        },
        "bursts": {
            "primary_metric": "observed-vs-expected burst intensity",
            "momentary_high": (
                "legitimate synchronized outreach or one-off reminder cascades"
            ),
            "momentary_low": (
                "normal random fluctuation when expected baseline is already elevated"
            ),
            "extended_high": (
                "repeated concentration windows that deserve correlation with duplicate "
                "and swing signals"
            ),
            "extended_low": (
                "suppressed variance that can indicate workflow smoothing or batching"
            ),
        },
        "procon_swings": {
            "primary_metric": (
                "directional pro/con ratio movement relative to expected bands"
            ),
            "momentary_high": "small-sample randomness, especially in low-power buckets",
            "momentary_low": (
                "brief balancing waves where opposite-side submissions cluster together"
            ),
            "extended_high": (
                "persistent directional mobilization or process-side skew in intake "
                "timing"
            ),
            "extended_low": (
                "prolonged suppression of one side that may indicate queueing or "
                "campaign fatigue"
            ),
        },
        "changepoints": {
            "primary_metric": "structural breaks in level or composition",
            "momentary_high": (
                "single regime boundaries caused by predictable hearing state "
                "transitions"
            ),
            "momentary_low": (
                "noisy micro-fluctuations that do not persist across adjacent windows"
            ),
            "extended_high": (
                "multi-break episodes indicating stable before/after behavioral regimes"
            ),
            "extended_low": "a relatively stationary process with fewer systemic shifts",
        },
        "off_hours": {
            "primary_metric": "overnight/off-hours participation and composition",
            "momentary_high": (
                "localized campaign pushes, timezone spillover, or delayed user "
                "activity"
            ),
            "momentary_low": "typical circadian troughs with naturally sparse submissions",
            "extended_high": (
                "systematic off-hours concentration that can indicate strategic timing"
            ),
            "extended_low": "consistently daytime-driven behavior and lower overnight engagement",
        },
        "duplicates_exact": {
            "primary_metric": "exact repeated-name concentration",
            "momentary_high": (
                "household/shared-name collisions or small coordinated batches"
            ),
            "momentary_low": "normal diversity of distinct names in organic intake",
            "extended_high": (
                "repeat-name patterns likely to influence authenticity and weighting "
                "assumptions"
            ),
            "extended_low": "healthy name diversity with limited exact repetition pressure",
        },
        "duplicates_near": {
            "primary_metric": "near-duplicate cluster growth and similarity structure",
            "momentary_high": "short typo/transliteration bursts around outreach windows",
            "momentary_low": "periods where naming variation is naturally broader",
            "extended_high": (
                "templated or normalization-colliding naming behavior over sustained "
                "windows"
            ),
            "extended_low": "low cluster cohesion and reduced near-duplicate pressure",
        },
        "sortedness": {
            "primary_metric": "alphabetical/ordered submission behavior",
            "momentary_high": (
                "small sorted snippets caused by chance or local administrative handling"
            ),
            "momentary_low": "expected unsorted arrivals from organic user behavior",
            "extended_high": "batch-oriented or deterministic ordering processes across windows",
            "extended_low": (
                "persistent organic ordering noise without process-level sorting artifacts"
            ),
        },
        "rare_names": {
            "primary_metric": "novelty, uniqueness, and rarity concentration",
            "momentary_high": (
                "brief novelty spikes from campaign expansion to new participants"
            ),
            "momentary_low": (
                "common-name clustering or temporary shrinkage in participant diversity"
            ),
            "extended_high": (
                "sustained lexical novelty requiring cross-check against lookup coverage"
            ),
            "extended_low": "repeated-name dominance or limited participant turnover",
        },
        "org_anomalies": {
            "primary_metric": "blank/null organization usage and split behavior",
            "momentary_high": "form UX friction or temporary omission guidance in outreach",
            "momentary_low": "short windows where organization prompts were more salient",
            "extended_high": (
                "systemic metadata sparsity that can bias affiliation interpretation"
            ),
            "extended_low": "more complete organization capture across participation streams",
        },
        "voter_registry_match": {
            "primary_metric": "name match coverage against voter registry reference",
            "momentary_high": "temporary concentration in highly matchable names",
            "momentary_low": "normal alias/normalization mismatch in sparse buckets",
            "extended_high": "stable overlap with known-voter naming patterns",
            "extended_low": (
                "persistent mismatch patterns requiring normalization and source review"
            ),
        },
        "periodicity": {
            "primary_metric": "recurring timing structure across minute and lag spaces",
            "momentary_high": "single reminder cycles or one-time timed campaign sends",
            "momentary_low": "flat/noisy slots where periodic patterns are not dominant",
            "extended_high": (
                "repeated cadence signatures that may indicate automation or strict "
                "scheduling"
            ),
            "extended_low": "weak periodic structure consistent with more organic arrival timing",
        },
        "multivariate_anomalies": {
            "primary_metric": "joint anomaly score across multiple behavioral features",
            "momentary_high": (
                "single-bucket feature coincidence without sustained corroboration"
            ),
            "momentary_low": "brief reversion to feature-space baseline",
            "extended_high": (
                "multi-feature regime changes needing manual validation and context "
                "checks"
            ),
            "extended_low": "feature combinations staying near historically typical mixtures",
        },
        "composite_score": {
            "primary_metric": "cross-detector evidence overlap and prioritization",
            "momentary_high": "short-lived detector agreement around a local event",
            "momentary_low": "isolated detector activity without consensus evidence",
            "extended_high": (
                "durable multi-detector agreement that should drive investigation "
                "priority"
            ),
            "extended_low": "broad detector disagreement suggesting mostly baseline behavior",
        },
    }


def _build_analysis_help_docs(
    analysis_definitions: list[dict[str, Any]],
    detailed_look_for: dict[str, list[str]],
) -> dict[str, dict[str, str]]:
    hints = _analysis_help_hints()
    docs: dict[str, dict[str, str]] = {}

    for definition in analysis_definitions:
        analysis_id = str(definition["id"])
        title = str(definition["title"])
        hint = hints.get(analysis_id, {})
        detail_points = detailed_look_for.get(analysis_id, [])
        detail_excerpt = " ".join(detail_points[:3]).strip()
        detail_suffix = (
            detail_excerpt
            if detail_excerpt
            else "Prioritize patterns that persist across adjacent windows and align "
            "with at least one independent detector signal."
        )

        primary_metric = hint.get("primary_metric", "this detector's primary signal")
        momentary_high = hint.get("momentary_high", "a local transient event")
        momentary_low = hint.get("momentary_low", "short-term random variation")
        extended_high = hint.get("extended_high", "a sustained process-level shift")
        extended_low = hint.get("extended_low", "a stable low-intensity regime")

        docs[analysis_id] = {
            "what_is_this": (
                f"{title} focuses on {primary_metric}. "
                "This section combines a hero chart, supporting charts, and tables to "
                "separate one-off noise from meaningful sustained behavior. "
                "Treat it as a detector notebook: start broad, then drill into "
                "specific windows with evidence context."
            ),
            "why_it_matters": (
                "This data matters because it changes how confident you should be in "
                "an anomaly narrative. Strong claims should come from persistent, "
                "well-supported patterns rather than isolated spikes. "
                "It also prevents both over-calling benign fluctuations and missing "
                "slow-burn anomalies that only emerge over longer runs."
            ),
            "how_to_interpret": (
                "Read the hero chart first for the dominant temporal structure, then "
                "use detail charts to test whether the signal repeats across scales, "
                "dayparts, or subgroup splits. Use tables to verify exact values and "
                "support counts behind flagged windows. "
                "When uncertainty bands or low-power markers are present, discount "
                "single-window jumps unless they recur with stronger support."
            ),
            "what_to_look_for": (
                f"{definition['what_to_look_for']} "
                f"{detail_suffix} "
                "Investigation priority should increase when multiple independent views "
                "tell the same story at the same time."
            ),
            "momentary_high_low": (
                "Momentary highs can indicate "
                f"{momentary_high}. Momentary lows can indicate {momentary_low}. "
                "Treat both cautiously when low-power flags are present. "
                "A practical rule: do not escalate on a single bucket unless a nearby "
                "table row and at least one companion chart support the same direction."
            ),
            "extended_high_low": (
                f"Extended highs can indicate {extended_high}. "
                f"Extended lows can indicate {extended_low}. "
                "Persistence across adjacent windows and corroborating detectors raises "
                "confidence that the shift is meaningful. "
                "Extended runs deserve timeline annotation and root-cause notes so later "
                "reviewers can separate operational context from suspicious behavior."
            ),
        }

    return docs


_SCATTER_CHART_IDS = {
    "multivariate_feature_projection",
    "multivariate_top_buckets",
}


def _chart_family(chart_id: str) -> str:
    chart_id_norm = str(chart_id or "")
    if chart_id_norm in _SCATTER_CHART_IDS:
        return "scatter"
    if "heatmap" in chart_id_norm:
        return "heatmap"
    if any(
        token in chart_id_norm
        for token in ("timeline", "rates", "ratio", "trend", "bucket", "profile")
    ):
        return "timeseries"
    return "categorical"


def _build_chart_help_docs(
    chart_legend_docs: dict[str, dict[str, Any]],
) -> dict[str, dict[str, str]]:
    docs: dict[str, dict[str, str]] = {}
    for chart_id, legend_doc in sorted(chart_legend_docs.items()):
        summary = str(legend_doc.get("summary") or "").strip()
        legend_items = legend_doc.get("items", [])
        labels = ", ".join(
            str(item.get("label", "")).strip() for item in legend_items if item
        )
        family = _chart_family(chart_id)

        if family == "heatmap":
            docs[chart_id] = {
                "what_is_this": (
                    f"{summary} This is a matrix view where color encodes magnitude "
                    "across paired axes such as date/hour or slot/day. "
                    "Each cell is a compact summary of one intersection, so the chart "
                    "is optimized for pattern shape over exact per-cell precision."
                ),
                "why_it_matters": (
                    "Heatmaps reveal spatially contiguous patterns that line charts can "
                    "hide, especially repeated daypart behavior and slot-level drift. "
                    "They are especially useful for finding regime-like blocks that "
                    "persist across many adjacent cells."
                ),
                "how_to_interpret": (
                    "Scan for contiguous blocks before focusing on single cells. "
                    "Compare high-intensity and low-intensity regions with bucket "
                    "support and related detector outputs. "
                    "Then check whether color transitions occur at meaningful "
                    "boundaries such as day changes, hearing windows, or slot shifts."
                ),
                "what_to_look_for": (
                    "Look for coherent blocks, repeated stripes, or abrupt regime "
                    "boundaries that persist across adjacent rows/columns. "
                    "Short isolated hot/cold cells are weaker evidence; long bands or "
                    "rectangles are stronger. "
                    f"Legend components: {labels}."
                ),
                "momentary_high_low": (
                    "A single hot/cold cell can reflect transient activity or low "
                    "support. Interpret isolated cells cautiously, especially if they "
                    "do not repeat in neighboring slots. "
                    "Momentary highs can map to one reminder wave; momentary lows can "
                    "map to ordinary quiet periods."
                ),
                "extended_high_low": (
                    "Extended hot/cold regions typically indicate sustained behavioral "
                    "mode shifts. Persistence across multiple dates/slots is stronger "
                    "evidence than one transition point. "
                    "Extended hot regions may indicate durable mobilization or process "
                    "bias; extended cold regions may indicate suppression or inactivity."
                ),
            }
            continue

        if family == "scatter":
            docs[chart_id] = {
                "what_is_this": (
                    f"{summary} This scatter plot maps each bucket as a point in "
                    "feature space, often with color and size as additional signals. "
                    "It is a relationship view, showing joint behavior rather than a "
                    "single metric over time."
                ),
                "why_it_matters": (
                    "Scatter views expose joint-feature structure, clusters, and "
                    "outliers that are not visible in one-dimensional summaries. "
                    "They help determine whether anomalies are isolated outliers or "
                    "part of a broader feature-space regime."
                ),
                "how_to_interpret": (
                    "Read axis meaning first, then evaluate whether outliers are "
                    "isolated or part of a cluster. Use color/size encodings to "
                    "understand confidence and support. "
                    "Cross-reference extreme points with time-based charts to determine "
                    "whether they are single events or repeated states."
                ),
                "what_to_look_for": (
                    "Look for detached point clouds, extreme tails, and dense anomaly "
                    "clusters that align with flagged windows. "
                    "A compact cluster far from baseline often carries more weight than "
                    "one far-away point with low support. "
                    f"Legend components: {labels}."
                ),
                "momentary_high_low": (
                    "A single extreme point may be a one-off event or model artifact. "
                    "Validate with timeline charts and table support counts. "
                    "Momentary lows are usually returns toward baseline and are often "
                    "benign unless paired with abrupt nearby outliers."
                ),
                "extended_high_low": (
                    "Large persistent outlier clusters imply broad feature-space drift. "
                    "Extended low-intensity clustering implies stable baseline behavior. "
                    "Sustained dual-cluster structure can indicate mixed populations or "
                    "alternating operational modes."
                ),
            }
            continue

        if family == "timeseries":
            docs[chart_id] = {
                "what_is_this": (
                    f"{summary} This time-aligned view shows how the measured signal "
                    "changes across chronological buckets. "
                    "It is the primary lens for identifying sequence, duration, and "
                    "coincidence with external events."
                ),
                "why_it_matters": (
                    "Time-series structure distinguishes transient spikes from sustained "
                    "regime changes and helps align detector evidence by timestamp. "
                    "Without duration context, it is easy to overreact to one-bucket "
                    "noise and miss broad shifts."
                ),
                "how_to_interpret": (
                    "Read left to right, compare volume with rate/score overlays, and "
                    "pay attention to uncertainty bounds and low-power markers where "
                    "available. "
                    "When zoomed in, verify whether local extremes persist across "
                    "neighboring buckets and remain visible at wider scales."
                ),
                "what_to_look_for": (
                    "Look for repeated peaks, troughs, trend breaks, and persistent "
                    "drifts across adjacent windows. "
                    "Patterns that recur at the same daypart across dates are usually "
                    "stronger than one isolated wave. "
                    f"Legend components: {labels}."
                ),
                "momentary_high_low": (
                    "Short highs/lows can reflect event timing, random variance, or "
                    "small-sample effects. Confirm with neighboring buckets before "
                    "treating them as material anomalies. "
                    "A momentary high near a known outreach time can be benign; a "
                    "momentary low during expected peak periods may indicate data lag."
                ),
                "extended_high_low": (
                    "Extended highs/lows are stronger indicators of behavioral shifts, "
                    "especially when they persist across multiple bucket sizes and "
                    "coincide with corroborating detector outputs. "
                    "Extended highs may indicate sustained mobilization or systematic "
                    "bias; extended lows may indicate prolonged inactivity or missing "
                    "segments."
                ),
            }
            continue

        docs[chart_id] = {
            "what_is_this": (
                f"{summary} This categorical/ranked chart compares values across "
                "labels, groups, or parameter settings. "
                "It emphasizes composition and concentration instead of chronology."
            ),
            "why_it_matters": (
                "Category comparisons show concentration, imbalance, and dominance "
                "patterns that can explain why timeline signals moved. "
                "They are often the fastest way to identify which subgroup is driving "
                "a detector outcome."
            ),
            "how_to_interpret": (
                "Sort by magnitude, compare head vs tail behavior, and relate category "
                "concentration to corresponding detector windows. "
                "Check both absolute values and relative spacing so you can distinguish "
                "true concentration from a uniformly low baseline."
            ),
            "what_to_look_for": (
                "Look for heavy concentration in a few categories, abrupt drop-offs, "
                "or rare categories with disproportionately high values. "
                "A long flat tail with one or two dominant bars often indicates a "
                "targeted driver worth validating in tables. "
                f"Legend components: {labels}."
            ),
            "momentary_high_low": (
                "A single dominant category may come from one campaign event or local "
                "data artifact. Check whether the dominance repeats over time. "
                "Momentary category suppression can also happen when total volume is "
                "temporarily low."
            ),
            "extended_high_low": (
                "Persistent dominance/absence across many categories can indicate "
                "structural participation effects rather than random variation. "
                "Extended concentration deserves follow-up to determine whether it is "
                "policy-driven outreach, operational process, or suspicious patterning."
            ),
        }

    return docs


def _fallback_chart_legend_doc(chart_id: str) -> dict[str, Any]:
    return {
        "summary": "Legend semantics for this chart.",
        "items": [
            {
                "label": "Primary series",
                "description": f"Main plotted signal for {chart_id.replace('_', ' ')}.",
            },
            {
                "label": "Axes",
                "description": (
                    "X-axis encodes time/category context; "
                    "Y-axis encodes magnitude or rate."
                ),
            },
        ],
    }


def _default_chart_legend_docs() -> dict[str, dict[str, Any]]:
    def timebar(
        *,
        summary: str,
        primary_label: str,
        primary_desc: str,
        include_wilson: bool = False,
        include_low_power: bool = True,
        flagged_label: str | None = None,
        flagged_desc: str | None = None,
        extra: list[dict[str, str]] | None = None,
        volume_label: str = "Volume",
        volume_desc: str = "Bars show record volume in each time bucket.",
    ) -> dict[str, Any]:
        items: list[dict[str, str]] = [
            {"label": volume_label, "description": volume_desc},
            {"label": primary_label, "description": primary_desc},
        ]
        if include_wilson:
            items.extend(
                [
                    {
                        "label": "Wilson low / Wilson high",
                        "description": (
                            "Confidence band for the proportion metric; "
                            "wider bands indicate higher uncertainty."
                        ),
                    }
                ]
            )
        if extra:
            items.extend(extra)
        if flagged_label and flagged_desc:
            items.append({"label": flagged_label, "description": flagged_desc})
        if include_low_power:
            items.append(
                {
                    "label": "Low-power",
                    "description": (
                        "Markers for buckets with insufficient support where rates "
                        "can swing from noise."
                    ),
                }
            )
        return {"summary": summary, "items": items}

    docs: dict[str, dict[str, Any]] = {
        "baseline_volume_pro_rate": timebar(
            summary="Baseline trend of submissions and pro share.",
            primary_label="Pro rate",
            primary_desc="Line shows pro-position share per bucket.",
            include_wilson=True,
        ),
        "baseline_day_hour_volume": {
            "summary": "Day/hour baseline heatmap.",
            "items": [
                {
                    "label": "Cell color",
                    "description": (
                        "Darker cells indicate higher submission volume for that "
                        "weekday/hour."
                    ),
                },
                {"label": "X/Y axes", "description": "X-axis is hour of day; Y-axis is weekday."},
            ],
        },
        "baseline_top_names": {
            "summary": "Top-frequency names.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Total submissions associated with each displayed name.",
                },
                {
                    "label": "X-axis names",
                    "description": "Most frequent names (trimmed to top slice for readability).",
                },
            ],
        },
        "baseline_name_length_distribution": {
            "summary": "Name-length histogram view.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Count of names with the corresponding character length.",
                },
                {"label": "X-axis", "description": "Normalized name length in characters."},
            ],
        },
        "bursts_hero_timeline": timebar(
            summary="Observed burst counts with burst intensity overlay.",
            primary_label="Rate ratio",
            primary_desc="Observed-to-expected count ratio per tested window.",
            include_wilson=False,
            include_low_power=False,
            volume_label="Observed count",
            volume_desc="Bars show observed submissions in each burst window.",
        ),
        "bursts_significance_by_window": {
            "summary": "Burst significance by window size.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Number of significant windows for each tested window size.",
                },
                {"label": "X-axis", "description": "Window size in minutes."},
            ],
        },
        "bursts_null_distribution": {
            "summary": "Burst null simulation output.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Maximum simulated count observed in each null iteration.",
                },
                {"label": "X-axis", "description": "Simulation iteration index."},
            ],
        },
        "procon_swings_hero_bucket_trend": timebar(
            summary="Pro-rate trend against baseline stability bands.",
            primary_label="Pro rate",
            primary_desc="Observed pro share in each bucket.",
            include_wilson=True,
            flagged_label="Flagged",
            flagged_desc="Buckets flagged by swing detector for abnormal deviation.",
            extra=[
                {
                    "label": "Baseline pro rate",
                    "description": "Expected day/time pro share baseline.",
                },
                {
                    "label": "Stable lower / stable upper",
                    "description": "Expected range around baseline for normal fluctuation.",
                },
            ],
        ),
        "procon_swings_shift_heatmap": {
            "summary": "Day/slot deviation heatmap.",
            "items": [
                {
                    "label": "Cell color",
                    "description": (
                        "Red cells are more pro-heavy than expected for that slot; "
                        "blue cells are more con-heavy."
                    ),
                },
                {
                    "label": "Slot outlier dots",
                    "description": "Highlighted cells that exceed detector outlier thresholds.",
                },
                {"label": "Axes", "description": "X-axis is slot-of-day; Y-axis is calendar date."},
            ],
        },
        "procon_swings_day_hour_heatmap": {
            "summary": "Average pro-rate by weekday/hour.",
            "items": [
                {"label": "Cell color", "description": "Darker cells indicate higher pro rate."},
                {"label": "Axes", "description": "X-axis is hour of day; Y-axis is weekday."},
            ],
        },
        "procon_swings_time_of_day_profile": {
            "summary": "Pro-rate profile by slot-of-day.",
            "items": [
                {"label": "Bar height", "description": "Pro share in that slot-of-day bucket."},
                {"label": "X-axis", "description": "Slot start minute from midnight."},
            ],
        },
        "procon_swings_null_distribution": {
            "summary": "Null distribution for swing extremes.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Maximum absolute pro-rate delta per null iteration.",
                },
                {"label": "X-axis", "description": "Simulation iteration index."},
            ],
        },
        "changepoints_hero_timeline": timebar(
            summary="Volume/pro-rate timeline with structural break markers.",
            primary_label="Pro rate",
            primary_desc="Observed pro share over time.",
            include_wilson=True,
            flagged_label="Flagged",
            flagged_desc="Detected changepoint locations.",
        ),
        "changepoints_magnitude": {
            "summary": "Changepoint magnitude ranking.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Absolute change magnitude at each detected break.",
                },
                {"label": "X-axis", "description": "Changepoint index/order."},
            ],
        },
        "changepoints_hour_hist": {
            "summary": "Changepoint timing histogram.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Number of changepoints occurring in each hour-of-day bin.",
                },
                {"label": "X-axis", "description": "Hour of day (0-23)."},
            ],
        },
        "off_hours_hourly_profile": {
            "summary": "Submission volume by hour-of-day.",
            "items": [
                {"label": "Bar height", "description": "Total submissions in each hourly bin."},
                {"label": "X-axis", "description": "Hour of day (0-23)."},
            ],
        },
        "off_hours_summary_compare": {
            "summary": "Off-hours vs on-hours summary.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Off-hours pro-rate statistic for each summary row.",
                },
                {"label": "X-axis", "description": "Off-hours count/context grouping key."},
            ],
        },
        "duplicates_exact_bucket_concentration": timebar(
            summary="Exact-duplicate concentration over time.",
            primary_label="Duplicate count",
            primary_desc="Bars show count of repeated canonical names in each bucket.",
            include_low_power=False,
            include_wilson=False,
            volume_label="Duplicate count",
            volume_desc="Exact duplicate occurrences per bucket.",
        ),
        "duplicates_exact_top_names": {
            "summary": "Most repeated exact names.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Total repeated occurrences for each display name.",
                },
                {"label": "X-axis", "description": "Top repeated display names."},
            ],
        },
        "duplicates_exact_position_switch": {
            "summary": "Repeated names with side switching.",
            "items": [
                {
                    "label": "Bar height",
                    "description": (
                        "Total records for repeated names appearing in multiple "
                        "positions."
                    ),
                },
                {"label": "X-axis", "description": "Display names exhibiting pro/con switching."},
            ],
        },
        "duplicates_near_cluster_timeline": timebar(
            summary="Near-duplicate cluster activity over time.",
            primary_label="Records",
            primary_desc="Line shows total records represented by active clusters.",
            include_low_power=False,
            include_wilson=False,
            volume_label="Cluster size",
            volume_desc="Bar height is cluster size at cluster first-seen time.",
        ),
        "duplicates_near_cluster_size": {
            "summary": "Cluster-size distribution.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Number of clusters observed at each cluster size.",
                },
                {"label": "X-axis", "description": "Cluster size (count of names)."},
            ],
        },
        "duplicates_near_similarity": {
            "summary": "Similarity levels among near-duplicate pairs.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Similarity score for candidate name pairs.",
                },
                {"label": "X-axis", "description": "Left-hand name label from each pair sample."},
            ],
        },
        "sortedness_bucket_ratio": timebar(
            summary="Ordering behavior across time buckets.",
            primary_label="Alphabetical indicator",
            primary_desc="Line values near 1 indicate alphabetical ordering for bucket windows.",
            include_low_power=False,
            include_wilson=False,
            volume_label="Records",
            volume_desc="Bar height is record count in each bucket.",
        ),
        "sortedness_bucket_summary": {
            "summary": "Sortedness summary by bucket size.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Alphabetical ordering ratio for each bucket size.",
                },
                {"label": "X-axis", "description": "Bucket size in minutes."},
            ],
        },
        "sortedness_minute_spikes": {
            "summary": "Minute-level ordering spikes.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Records seen in each minute-level ordering sample.",
                },
                {"label": "X-axis", "description": "Minute bucket timestamp."},
            ],
        },
        "rare_names_unique_ratio": timebar(
            summary="Name uniqueness over time.",
            primary_label="Unique ratio",
            primary_desc="Share of submissions with distinct canonical names per bucket.",
            include_low_power=True,
            include_wilson=False,
            extra=[
                {
                    "label": "Threshold unique ratio",
                    "description": "Reference threshold used for unique-ratio anomaly signaling.",
                }
            ],
        ),
        "rare_names_weird_scores": {
            "summary": "Highest weirdness-score names.",
            "items": [
                {
                    "label": "Bar height",
                    "description": (
                        "Weirdness score of sampled names; "
                        "higher indicates atypical string shape."
                    ),
                },
                {"label": "X-axis", "description": "Sample names sorted by weirdness."},
            ],
        },
        "rare_names_singletons": timebar(
            summary="Singleton name composition over time.",
            primary_label="Con count",
            primary_desc="Line shows con-side count among singleton records.",
            include_low_power=False,
            include_wilson=False,
            volume_label="Pro count",
            volume_desc="Bars show pro-side count among singleton records.",
        ),
        "rare_names_rarity_timeline": timebar(
            summary="Rarity-score timeline.",
            primary_label="Rarity median",
            primary_desc="Median rarity score in each bucket.",
            include_low_power=True,
            include_wilson=False,
            extra=[
                {
                    "label": "Rarity p95",
                    "description": "95th percentile rarity score to show tail behavior.",
                }
            ],
        ),
        "org_anomalies_blank_rate": timebar(
            summary="Blank organization-rate trend with position splits.",
            primary_label="Blank org rate",
            primary_desc="Overall blank/null organization share per bucket.",
            include_wilson=True,
            extra=[
                {
                    "label": "Pro blank org rate",
                    "description": "Blank-org share among pro records.",
                },
                {
                    "label": "Con blank org rate",
                    "description": "Blank-org share among con records.",
                },
            ],
        ),
        "org_anomalies_position_rates": timebar(
            summary="Per-position blank-org rates by time bucket.",
            primary_label="Blank org rate",
            primary_desc="Position-specific blank organization share.",
            include_wilson=True,
        ),
        "org_anomalies_bursts": {
            "summary": "Organization burst concentration.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Burst count for organization-related minute windows.",
                },
                {"label": "X-axis", "description": "Minute bucket of organization burst sample."},
            ],
        },
        "org_anomalies_top_orgs": {
            "summary": "Most common organization values.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Total records linked to each normalized organization value.",
                },
                {"label": "X-axis", "description": "Organization value labels."},
            ],
        },
        "voter_registry_match_rates": timebar(
            summary="Registry match-rate trend over time.",
            primary_label="Match rate",
            primary_desc="Overall voter-registry name match rate per bucket.",
            include_wilson=True,
            extra=[
                {
                    "label": "Pro match rate",
                    "description": "Match rate among pro-position records.",
                },
                {
                    "label": "Con match rate",
                    "description": "Match rate among con-position records.",
                },
            ],
        ),
        "voter_registry_match_by_position": {
            "summary": "Match rate by position grouping.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Registry match rate for each position label.",
                },
                {"label": "X-axis", "description": "Normalized position label."},
            ],
        },
        "voter_registry_unmatched_names": {
            "summary": "Most frequent unmatched names.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Count of unmatched records for each canonical name.",
                },
                {"label": "X-axis", "description": "Canonical unmatched name values."},
            ],
        },
        "voter_registry_position_buckets": timebar(
            summary="Position-specific match rates across time.",
            primary_label="Match rate",
            primary_desc="Per-position registry match share by bucket.",
            include_wilson=True,
        ),
        "periodicity_clockface": {
            "summary": "Clock-face minute concentration.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Observed event count at each minute-of-hour bin.",
                },
                {"label": "X-axis", "description": "Minute of hour (0-59)."},
            ],
        },
        "periodicity_autocorr": {
            "summary": "Autocorrelation by lag.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Autocorrelation coefficient at each lag in minutes.",
                },
                {"label": "X-axis", "description": "Lag length in minutes."},
            ],
        },
        "periodicity_spectrum": {
            "summary": "Top spectral periods.",
            "items": [
                {"label": "Bar height", "description": "Spectral power for each candidate period."},
                {"label": "X-axis", "description": "Detected period in minutes."},
            ],
        },
        "multivariate_score_timeline": timebar(
            summary="Multivariate anomaly score and support over time.",
            primary_label="Anomaly score",
            primary_desc="Combined feature-space anomaly score for each bucket.",
            include_wilson=False,
            include_low_power=True,
            extra=[
                {
                    "label": "Anomaly score percentile",
                    "description": "Percentile rank of anomaly score within this run.",
                }
            ],
        ),
        "multivariate_top_buckets": {
            "summary": "Top anomaly buckets (scatter).",
            "items": [
                {
                    "label": "Point position",
                    "description": "X-axis is bucket volume; Y-axis is anomaly score.",
                },
                {
                    "label": "Point color",
                    "description": "Color reflects anomaly-score percentile rank.",
                },
                {"label": "Point size", "description": "Bubble size scales with bucket volume."},
            ],
        },
        "multivariate_feature_projection": {
            "summary": "Feature projection scatter.",
            "items": [
                {
                    "label": "Point position",
                    "description": "X-axis is log volume; Y-axis is pro rate.",
                },
                {"label": "Point color", "description": "Color shows anomaly score intensity."},
                {"label": "Point size", "description": "Bubble size scales with bucket volume."},
            ],
        },
        "composite_score_timeline": timebar(
            summary="Composite risk score over time.",
            primary_label="Composite score",
            primary_desc="Aggregate score from multi-detector evidence overlap.",
            include_wilson=False,
            include_low_power=True,
        ),
        "composite_evidence_flags": {
            "summary": "Evidence-flag composition.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Count of windows containing each detector flag.",
                },
                {"label": "X-axis", "description": "Detector evidence flag token."},
            ],
        },
        "composite_high_priority": {
            "summary": "Highest-priority composite windows.",
            "items": [
                {"label": "Bar height", "description": "Composite score for top-ranked windows."},
                {"label": "X-axis", "description": "Window timestamp bucket."},
            ],
        },
    }
    return docs


def _extract_bucket_options(*frames: pd.DataFrame) -> list[int]:
    options: set[int] = set()
    for frame in frames:
        if frame.empty or "bucket_minutes" not in frame.columns:
            continue
        numeric = pd.to_numeric(frame["bucket_minutes"], errors="coerce").dropna()
        for value in numeric.astype(int).tolist():
            if value > 0:
                options.add(int(value))
    return sorted(options)


def _with_expected_columns(frame: pd.DataFrame, expected: list[str]) -> pd.DataFrame:
    working = frame.copy()
    for column in expected:
        if column not in working.columns:
            working[column] = pd.NA
    return working


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


def _load_table_map_from_results(
    results: dict[str, DetectorResult],
    artifacts: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    table_map: dict[str, pd.DataFrame] = {}
    for name, frame in artifacts.items():
        table_map[f"artifacts.{name}"] = frame.copy()
    for detector_name, result in results.items():
        for table_name, frame in result.tables.items():
            table_map[_table_key(detector_name, table_name)] = frame.copy()
    return table_map


def _load_table_map_from_disk(out_dir: Path) -> dict[str, pd.DataFrame]:
    table_map: dict[str, pd.DataFrame] = {}

    artifacts_dir = out_dir / "artifacts"
    if artifacts_dir.exists():
        artifact_candidates: dict[str, Path] = {}
        for path in artifacts_dir.iterdir():
            if path.suffix not in {".parquet", ".csv"}:
                continue
            existing = artifact_candidates.get(path.stem)
            if existing is None or (existing.suffix == ".csv" and path.suffix == ".parquet"):
                artifact_candidates[path.stem] = path
        for stem, path in sorted(artifact_candidates.items()):
            frame = _load_frame_from_candidates([path])
            table_map[f"artifacts.{stem}"] = frame

    tables_dir = out_dir / "tables"
    if tables_dir.exists():
        table_candidates: dict[str, Path] = {}
        for path in tables_dir.iterdir():
            if path.suffix not in {".parquet", ".csv"}:
                continue
            if "__" not in path.stem:
                continue
            detector_name, table_name = path.stem.split("__", 1)
            key = _table_key(detector_name, table_name)
            existing = table_candidates.get(key)
            if existing is None or (existing.suffix == ".csv" and path.suffix == ".parquet"):
                table_candidates[key] = path
        for key, path in sorted(table_candidates.items()):
            frame = _load_frame_from_candidates([path])
            table_map[key] = frame

    return table_map


def _build_interactive_chart_payload_v2(
    table_map: dict[str, pd.DataFrame],
    detector_summaries: dict[str, dict[str, Any]],
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
            "q_value",
            "is_significant",
        ],
    )
    bursts_tests = _with_expected_columns(
        table_map.get(_table_key("bursts", "burst_window_tests"), pd.DataFrame()),
        ["window_minutes", "bucket_minutes", "rate_ratio", "is_significant"],
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
    swing_null = _with_expected_columns(
        table_map.get(_table_key("procon_swings", "swing_null_distribution"), pd.DataFrame()),
        ["window_minutes", "iteration", "max_abs_delta_pro_rate"],
    )

    all_changepoints = _with_expected_columns(
        table_map.get(_table_key("changepoints", "all_changepoints"), pd.DataFrame()),
        [
            "metric",
            "change_index",
            "change_minute",
            "mean_before",
            "mean_after",
            "delta",
            "abs_delta",
        ],
    )
    volume_changepoints = _with_expected_columns(
        table_map.get(_table_key("changepoints", "volume_changepoints"), pd.DataFrame()),
        ["change_minute"],
    )
    pro_rate_changepoints = _with_expected_columns(
        table_map.get(_table_key("changepoints", "pro_rate_changepoints"), pd.DataFrame()),
        ["change_minute"],
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
        ],
    )

    dup_exact_bucket = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "repeated_same_bucket"), pd.DataFrame()),
        ["bucket_start", "bucket_minutes", "canonical_name", "n", "n_pro", "n_con"],
    )
    dup_exact_top = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "top_repeated_names"), pd.DataFrame()),
        ["display_name", "canonical_name", "n", "n_pro", "n_con", "time_span_minutes"],
    )
    dup_exact_switch = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "position_switching_names"), pd.DataFrame()),
        [
            "display_name",
            "canonical_name",
            "n",
            "n_pro",
            "n_con",
            "first_seen",
            "last_seen",
            "time_span_minutes",
        ],
    )

    dup_near_clusters = _with_expected_columns(
        table_map.get(_table_key("duplicates_near", "cluster_summary"), pd.DataFrame()),
        [
            "cluster_id",
            "first_seen",
            "last_seen",
            "cluster_size",
            "n_records",
            "n_pro",
            "n_con",
            "time_span_minutes",
        ],
    )
    dup_near_edges = _with_expected_columns(
        table_map.get(_table_key("duplicates_near", "similarity_edges"), pd.DataFrame()),
        ["similarity", "left_display_name", "right_display_name", "block_key"],
    )

    sorted_bucket = _with_expected_columns(
        table_map.get(_table_key("sortedness", "bucket_ordering"), pd.DataFrame()),
        ["bucket_start", "bucket_minutes", "n_records", "is_alphabetical"],
    )
    sorted_summary = _with_expected_columns(
        table_map.get(_table_key("sortedness", "bucket_ordering_summary"), pd.DataFrame()),
        ["bucket_minutes", "n_buckets", "avg_records_per_bucket", "alphabetical_ratio"],
    )
    sorted_minute = _with_expected_columns(
        table_map.get(_table_key("sortedness", "minute_ordering"), pd.DataFrame()),
        ["minute_bucket", "n_records", "is_alphabetical"],
    )

    rare_unique_ratio = _with_expected_columns(
        table_map.get(_table_key("rare_names", "unique_ratio_windows"), pd.DataFrame()),
        [
            "minute_bucket",
            "n_total",
            "n_unique_names",
            "unique_ratio",
            "threshold_unique_ratio",
            "is_low_power",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "bucket_minutes",
        ],
    )
    rare_weird = _with_expected_columns(
        table_map.get(_table_key("rare_names", "weird_names"), pd.DataFrame()),
        [
            "canonical_name",
            "sample_name",
            "weirdness_score",
            "name_length",
            "non_alpha_fraction",
            "name_entropy",
        ],
    )
    rare_singletons = _with_expected_columns(
        table_map.get(_table_key("rare_names", "singleton_names"), pd.DataFrame()),
        [
            "display_name",
            "canonical_name",
            "first_seen",
            "last_seen",
            "n_pro",
            "n_con",
            "time_span_minutes",
        ],
    )
    rare_rarity = _with_expected_columns(
        table_map.get(_table_key("rare_names", "rarity_by_minute"), pd.DataFrame()),
        [
            "minute_bucket",
            "n_total",
            "rarity_median",
            "rarity_p95",
            "is_low_power",
            "bucket_minutes",
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
    org_bursts = _with_expected_columns(
        table_map.get(_table_key("org_anomalies", "organization_minute_bursts"), pd.DataFrame()),
        ["minute_bucket", "organization_clean", "n", "threshold"],
    )
    org_counts = _with_expected_columns(
        table_map.get(_table_key("org_anomalies", "organization_counts"), pd.DataFrame()),
        ["organization_clean", "n", "n_pro", "n_con", "first_seen", "last_seen"],
    )

    voter_bucket = _with_expected_columns(
        table_map.get(_table_key("voter_registry_match", "match_by_bucket"), pd.DataFrame()),
        [
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "match_rate",
            "match_rate_wilson_low",
            "match_rate_wilson_high",
            "pro_match_rate",
            "pro_match_rate_wilson_low",
            "pro_match_rate_wilson_high",
            "con_match_rate",
            "con_match_rate_wilson_low",
            "con_match_rate_wilson_high",
            "is_low_power",
            "pro_is_low_power",
            "con_is_low_power",
        ],
    )
    voter_position = _with_expected_columns(
        table_map.get(_table_key("voter_registry_match", "match_by_position"), pd.DataFrame()),
        [
            "position_normalized",
            "n_total",
            "n_matches",
            "n_unmatched",
            "match_rate",
            "match_rate_wilson_low",
            "match_rate_wilson_high",
            "is_low_power",
        ],
    )
    voter_unmatched = _with_expected_columns(
        table_map.get(_table_key("voter_registry_match", "unmatched_names"), pd.DataFrame()),
        ["canonical_name", "n_records"],
    )
    voter_bucket_position = _with_expected_columns(
        table_map.get(
            _table_key("voter_registry_match", "match_by_bucket_position"),
            pd.DataFrame(),
        ),
        [
            "bucket_start",
            "bucket_minutes",
            "position_normalized",
            "n_total",
            "match_rate",
            "match_rate_wilson_low",
            "match_rate_wilson_high",
            "is_low_power",
        ],
    )

    periodic_clockface = _with_expected_columns(
        table_map.get(_table_key("periodicity", "clockface_distribution"), pd.DataFrame()),
        [
            "minute_of_hour",
            "n_events",
            "expected_n_events_uniform",
            "deviation_from_uniform",
            "share",
            "z_score_uniform",
        ],
    )
    periodic_autocorr = _with_expected_columns(
        table_map.get(_table_key("periodicity", "autocorr"), pd.DataFrame()),
        ["lag_minutes", "autocorr", "abs_autocorr", "q_value", "is_significant"],
    )
    periodic_spectrum = _with_expected_columns(
        table_map.get(_table_key("periodicity", "spectrum_top"), pd.DataFrame()),
        ["period_minutes", "frequency_per_minute", "power", "q_value", "is_significant"],
    )

    multivariate_scores = _with_expected_columns(
        table_map.get(
            _table_key("multivariate_anomalies", "bucket_anomaly_scores"), pd.DataFrame()
        ),
        [
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "dup_name_fraction_weighted",
            "blank_org_rate",
            "anomaly_score",
            "anomaly_score_percentile",
            "is_anomaly",
            "is_low_power",
            "is_model_eligible",
            "log_n_total",
        ],
    )
    multivariate_top = _with_expected_columns(
        table_map.get(_table_key("multivariate_anomalies", "top_bucket_anomalies"), pd.DataFrame()),
        [
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "pro_rate",
            "anomaly_score",
            "anomaly_score_percentile",
            "is_anomaly",
            "is_low_power",
        ],
    )

    composite_ranked = _with_expected_columns(
        table_map.get(_table_key("composite_score", "ranked_windows"), pd.DataFrame()),
        [
            "minute_bucket",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "composite_score",
            "evidence_count",
            "burst_signal",
            "swing_signal",
            "changepoint_signal",
            "ml_anomaly_signal",
        ],
    )
    composite_evidence = _with_expected_columns(
        table_map.get(_table_key("composite_score", "evidence_bundle_windows"), pd.DataFrame()),
        ["minute_bucket", "evidence_flags"],
    )
    composite_high = _with_expected_columns(
        table_map.get(_table_key("composite_score", "high_priority_windows"), pd.DataFrame()),
        [
            "minute_bucket",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "composite_score",
            "burst_signal",
            "swing_signal",
            "changepoint_signal",
            "ml_anomaly_signal",
            "rarity_signal",
            "unique_signal",
        ],
    )

    for frame, column in [
        (counts_per_minute, "minute_bucket"),
        (bursts_significant, "start_minute"),
        (bursts_tests, "start_minute"),
        (time_bucket_profiles, "bucket_start"),
        (day_bucket_profiles, "date"),
        (all_changepoints, "change_minute"),
        (volume_changepoints, "change_minute"),
        (pro_rate_changepoints, "change_minute"),
        (dup_exact_bucket, "bucket_start"),
        (dup_exact_switch, "first_seen"),
        (dup_near_clusters, "first_seen"),
        (sorted_bucket, "bucket_start"),
        (sorted_minute, "minute_bucket"),
        (rare_unique_ratio, "minute_bucket"),
        (rare_singletons, "first_seen"),
        (rare_rarity, "minute_bucket"),
        (org_blank_rates, "bucket_start"),
        (org_position_rates, "bucket_start"),
        (org_bursts, "minute_bucket"),
        (voter_bucket, "bucket_start"),
        (voter_bucket_position, "bucket_start"),
        (multivariate_scores, "bucket_start"),
        (multivariate_top, "bucket_start"),
        (composite_ranked, "minute_bucket"),
        (composite_evidence, "minute_bucket"),
        (composite_high, "minute_bucket"),
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

    charts["bursts_hero_timeline"] = _records_from_frame(
        bursts_significant.sort_values(["start_minute", "window_minutes"]),
        columns=[
            "start_minute",
            "end_minute",
            "window_minutes",
            "bucket_minutes",
            "observed_count",
            "expected_count",
            "rate_ratio",
            "q_value",
            "is_significant",
        ],
        max_rows=25_000,
    )
    if not bursts_tests.empty and "window_minutes" in bursts_tests.columns:
        burst_sig_summary = (
            bursts_tests.assign(
                is_significant_bool=bursts_tests.get("is_significant", False).astype(bool),
            )
            .groupby("window_minutes", dropna=False)
            .agg(
                n_windows=("window_minutes", "size"),
                n_significant=("is_significant_bool", "sum"),
                median_rate_ratio=("rate_ratio", "median"),
            )
            .reset_index()
            .sort_values("window_minutes")
        )
        burst_sig_summary["bucket_minutes"] = pd.to_numeric(
            burst_sig_summary["window_minutes"],
            errors="coerce",
        )
    else:
        burst_sig_summary = pd.DataFrame()
    charts["bursts_significance_by_window"] = _records_from_frame(
        burst_sig_summary,
        columns=[
            "window_minutes",
            "bucket_minutes",
            "n_windows",
            "n_significant",
            "median_rate_ratio",
        ],
        max_rows=100,
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
    charts["procon_swings_null_distribution"] = _records_from_frame(
        swing_null.sort_values(["window_minutes", "iteration"]),
        columns=["window_minutes", "iteration", "max_abs_delta_pro_rate"],
        max_rows=25_000,
    )

    if not counts_per_minute.empty:
        change_minutes = set(
            pd.to_datetime(
                all_changepoints.get("change_minute", pd.Series(dtype="datetime64[ns]")),
                errors="coerce",
            )
            .dropna()
            .map(_serialize_value)
            .tolist()
        )
        changepoint_timeline = counts_per_minute.copy()
        changepoint_timeline["minute_bucket_serialized"] = changepoint_timeline[
            "minute_bucket"
        ].map(_serialize_value)
        changepoint_timeline["is_changepoint"] = changepoint_timeline[
            "minute_bucket_serialized"
        ].isin(change_minutes)
    else:
        changepoint_timeline = _with_expected_columns(
            pd.DataFrame(),
            [
                "minute_bucket",
                "n_total",
                "pro_rate",
                "pro_rate_wilson_low",
                "pro_rate_wilson_high",
                "is_low_power",
                "is_changepoint",
            ],
        )
    charts["changepoints_hero_timeline"] = _records_from_frame(
        changepoint_timeline.sort_values("minute_bucket"),
        columns=[
            "minute_bucket",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "is_changepoint",
        ],
        max_rows=25_000,
    )
    charts["changepoints_magnitude"] = _records_from_frame(
        all_changepoints.sort_values("abs_delta", ascending=False),
        columns=[
            "metric",
            "change_index",
            "change_minute",
            "mean_before",
            "mean_after",
            "delta",
            "abs_delta",
        ],
        max_rows=5_000,
    )
    if not all_changepoints.empty and "change_minute" in all_changepoints.columns:
        change_hour_hist = (
            all_changepoints.assign(change_hour=all_changepoints["change_minute"].dt.hour)
            .dropna(subset=["change_hour"])
            .groupby("change_hour", dropna=False)
            .size()
            .rename("n_changes")
            .reset_index()
            .sort_values("change_hour")
        )
    else:
        change_hour_hist = pd.DataFrame()
    charts["changepoints_hour_hist"] = _records_from_frame(
        change_hour_hist,
        columns=["change_hour", "n_changes"],
        max_rows=500,
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
        ],
        max_rows=10,
    )

    charts["duplicates_exact_bucket_concentration"] = _records_from_frame(
        dup_exact_bucket.sort_values(["bucket_minutes", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "canonical_name",
            "n",
            "n_pro",
            "n_con",
        ],
        max_rows=25_000,
    )
    charts["duplicates_exact_top_names"] = _records_from_frame(
        dup_exact_top.sort_values("n", ascending=False),
        columns=["display_name", "canonical_name", "n", "n_pro", "n_con", "time_span_minutes"],
        max_rows=500,
    )
    charts["duplicates_exact_position_switch"] = _records_from_frame(
        dup_exact_switch.sort_values("n", ascending=False),
        columns=[
            "display_name",
            "canonical_name",
            "n",
            "n_pro",
            "n_con",
            "first_seen",
            "last_seen",
            "time_span_minutes",
        ],
        max_rows=500,
    )

    charts["duplicates_near_cluster_timeline"] = _records_from_frame(
        dup_near_clusters.sort_values("first_seen"),
        columns=[
            "cluster_id",
            "first_seen",
            "last_seen",
            "cluster_size",
            "n_records",
            "n_pro",
            "n_con",
            "time_span_minutes",
        ],
        max_rows=25_000,
    )
    if not dup_near_clusters.empty and "cluster_size" in dup_near_clusters.columns:
        cluster_size_summary = (
            dup_near_clusters.groupby("cluster_size", dropna=False)
            .size()
            .rename("n_clusters")
            .reset_index()
            .sort_values("cluster_size")
        )
    else:
        cluster_size_summary = pd.DataFrame()
    charts["duplicates_near_cluster_size"] = _records_from_frame(
        cluster_size_summary,
        columns=["cluster_size", "n_clusters"],
        max_rows=1_000,
    )
    charts["duplicates_near_similarity"] = _records_from_frame(
        dup_near_edges.sort_values("similarity", ascending=False),
        columns=["similarity", "left_display_name", "right_display_name", "block_key"],
        max_rows=5_000,
    )

    charts["sortedness_bucket_ratio"] = _records_from_frame(
        sorted_bucket.sort_values(["bucket_minutes", "bucket_start"]),
        columns=["bucket_start", "bucket_minutes", "n_records", "is_alphabetical"],
        max_rows=25_000,
    )
    charts["sortedness_bucket_summary"] = _records_from_frame(
        sorted_summary.sort_values("bucket_minutes"),
        columns=["bucket_minutes", "n_buckets", "avg_records_per_bucket", "alphabetical_ratio"],
        max_rows=500,
    )
    charts["sortedness_minute_spikes"] = _records_from_frame(
        sorted_minute.sort_values("minute_bucket"),
        columns=["minute_bucket", "n_records", "is_alphabetical"],
        max_rows=25_000,
    )

    charts["rare_names_unique_ratio"] = _records_from_frame(
        rare_unique_ratio.sort_values("minute_bucket"),
        columns=[
            "minute_bucket",
            "bucket_minutes",
            "n_total",
            "n_unique_names",
            "unique_ratio",
            "threshold_unique_ratio",
            "is_low_power",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
        ],
        max_rows=25_000,
    )
    charts["rare_names_weird_scores"] = _records_from_frame(
        rare_weird.sort_values("weirdness_score", ascending=False),
        columns=[
            "canonical_name",
            "sample_name",
            "weirdness_score",
            "name_length",
            "non_alpha_fraction",
            "name_entropy",
        ],
        max_rows=1_000,
    )
    charts["rare_names_singletons"] = _records_from_frame(
        rare_singletons.sort_values("first_seen"),
        columns=[
            "display_name",
            "canonical_name",
            "first_seen",
            "last_seen",
            "n_pro",
            "n_con",
            "time_span_minutes",
        ],
        max_rows=25_000,
    )
    charts["rare_names_rarity_timeline"] = _records_from_frame(
        rare_rarity.sort_values("minute_bucket"),
        columns=[
            "minute_bucket",
            "bucket_minutes",
            "n_total",
            "rarity_median",
            "rarity_p95",
            "is_low_power",
        ],
        max_rows=25_000,
    )

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
    charts["org_anomalies_bursts"] = _records_from_frame(
        org_bursts.sort_values("minute_bucket"),
        columns=["minute_bucket", "organization_clean", "n", "threshold"],
        max_rows=5_000,
    )
    charts["org_anomalies_top_orgs"] = _records_from_frame(
        org_counts.sort_values("n", ascending=False),
        columns=["organization_clean", "n", "n_pro", "n_con", "first_seen", "last_seen"],
        max_rows=1_000,
    )

    charts["voter_registry_match_rates"] = _records_from_frame(
        voter_bucket.sort_values(["bucket_minutes", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "match_rate",
            "match_rate_wilson_low",
            "match_rate_wilson_high",
            "pro_match_rate",
            "pro_match_rate_wilson_low",
            "pro_match_rate_wilson_high",
            "con_match_rate",
            "con_match_rate_wilson_low",
            "con_match_rate_wilson_high",
            "is_low_power",
            "pro_is_low_power",
            "con_is_low_power",
        ],
        max_rows=25_000,
    )
    charts["voter_registry_match_by_position"] = _records_from_frame(
        voter_position.sort_values("position_normalized"),
        columns=[
            "position_normalized",
            "n_total",
            "n_matches",
            "n_unmatched",
            "match_rate",
            "match_rate_wilson_low",
            "match_rate_wilson_high",
            "is_low_power",
        ],
        max_rows=100,
    )
    charts["voter_registry_unmatched_names"] = _records_from_frame(
        voter_unmatched.sort_values("n_records", ascending=False),
        columns=["canonical_name", "n_records"],
        max_rows=1_000,
    )
    charts["voter_registry_position_buckets"] = _records_from_frame(
        voter_bucket_position.sort_values(
            ["bucket_minutes", "bucket_start", "position_normalized"]
        ),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "position_normalized",
            "n_total",
            "match_rate",
            "match_rate_wilson_low",
            "match_rate_wilson_high",
            "is_low_power",
        ],
        max_rows=25_000,
    )

    charts["periodicity_clockface"] = _records_from_frame(
        periodic_clockface.sort_values("minute_of_hour"),
        columns=[
            "minute_of_hour",
            "n_events",
            "expected_n_events_uniform",
            "deviation_from_uniform",
            "share",
            "z_score_uniform",
        ],
        max_rows=500,
    )
    charts["periodicity_autocorr"] = _records_from_frame(
        periodic_autocorr.sort_values("lag_minutes"),
        columns=["lag_minutes", "autocorr", "abs_autocorr", "q_value", "is_significant"],
        max_rows=5_000,
    )
    charts["periodicity_spectrum"] = _records_from_frame(
        periodic_spectrum.sort_values("power", ascending=False),
        columns=["period_minutes", "frequency_per_minute", "power", "q_value", "is_significant"],
        max_rows=5_000,
    )

    charts["multivariate_score_timeline"] = _records_from_frame(
        multivariate_scores.sort_values("bucket_start"),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "dup_name_fraction_weighted",
            "blank_org_rate",
            "anomaly_score",
            "anomaly_score_percentile",
            "is_anomaly",
            "is_low_power",
            "is_model_eligible",
        ],
        max_rows=25_000,
    )
    charts["multivariate_top_buckets"] = _records_from_frame(
        multivariate_top.sort_values("anomaly_score", ascending=False),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "pro_rate",
            "anomaly_score",
            "anomaly_score_percentile",
            "is_anomaly",
            "is_low_power",
        ],
        max_rows=1_000,
    )
    charts["multivariate_feature_projection"] = _records_from_frame(
        multivariate_scores.sort_values("anomaly_score", ascending=False),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "log_n_total",
            "pro_rate",
            "dup_name_fraction_weighted",
            "blank_org_rate",
            "anomaly_score",
            "anomaly_score_percentile",
            "is_anomaly",
        ],
        max_rows=25_000,
    )

    charts["composite_score_timeline"] = _records_from_frame(
        composite_ranked.sort_values("minute_bucket"),
        columns=[
            "minute_bucket",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "composite_score",
            "evidence_count",
            "burst_signal",
            "swing_signal",
            "changepoint_signal",
            "ml_anomaly_signal",
        ],
        max_rows=25_000,
    )
    if not composite_evidence.empty and "evidence_flags" in composite_evidence.columns:
        flag_counts: dict[str, int] = defaultdict(int)
        for raw in composite_evidence["evidence_flags"].fillna("").astype(str).tolist():
            for token in [item.strip() for item in raw.split(",") if item.strip()]:
                flag_counts[token] += 1
        evidence_flag_table = pd.DataFrame(
            [
                {"flag": name, "count": count}
                for name, count in sorted(
                    flag_counts.items(), key=lambda item: item[1], reverse=True
                )
            ]
        )
    else:
        evidence_flag_table = pd.DataFrame()
    charts["composite_evidence_flags"] = _records_from_frame(
        evidence_flag_table,
        columns=["flag", "count"],
        max_rows=1_000,
    )
    charts["composite_high_priority"] = _records_from_frame(
        composite_high.sort_values("composite_score", ascending=False),
        columns=[
            "minute_bucket",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "composite_score",
            "burst_signal",
            "swing_signal",
            "changepoint_signal",
            "ml_anomaly_signal",
            "rarity_signal",
            "unique_signal",
        ],
        max_rows=5_000,
    )

    analysis_definitions = registry_analysis_definitions()
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
            time_bucket_profiles, day_bucket_profiles, time_of_day_profiles
        ),
        "changepoints": [],
        "off_hours": [],
        "duplicates_exact": _extract_bucket_options(dup_exact_bucket),
        "duplicates_near": [],
        "sortedness": _extract_bucket_options(sorted_bucket, sorted_summary),
        "rare_names": _extract_bucket_options(rare_unique_ratio, rare_rarity),
        "org_anomalies": _extract_bucket_options(org_blank_rates, org_position_rates),
        "voter_registry_match": _extract_bucket_options(voter_bucket, voter_bucket_position),
        "periodicity": [],
        "multivariate_anomalies": _extract_bucket_options(multivariate_scores),
        "composite_score": [],
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
                "how_to_read": definition["how_to_read"],
                "what_to_look_for": definition["what_to_look_for"],
                "what_to_look_for_details": look_for_details.get(str(definition["id"]), []),
                "common_benign_causes": definition["common_benign_causes"],
                "help_sections": analysis_help_docs.get(str(definition["id"]), {}),
            }
        )

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
        "procon_swings_hero_bucket_trend",
        "changepoints_hero_timeline",
        "duplicates_exact_bucket_concentration",
        "duplicates_near_cluster_timeline",
        "sortedness_bucket_ratio",
        "rare_names_unique_ratio",
        "org_anomalies_blank_rate",
        "org_anomalies_position_rates",
        "voter_registry_match_rates",
        "voter_registry_position_buckets",
        "multivariate_score_timeline",
        "composite_score_timeline",
    ]
    absolute_time_chart_ids = [
        chart_id for chart_id in absolute_time_chart_ids if charts.get(chart_id)
    ]

    payload = {
        "version": 2,
        "analysis_catalog": analysis_catalog,
        "charts": charts,
        "chart_legend_docs": chart_legend_docs,
        "chart_help_docs": chart_help_docs,
        "controls": {
            "default_bucket_minutes": 30
            if 30 in global_bucket_options
            else (global_bucket_options[0] if global_bucket_options else None),
            "global_bucket_options": global_bucket_options,
            "zoom_sync_groups": {"absolute_time": absolute_time_chart_ids},
            "timezone": "UTC",
            "timezone_label": "UTC",
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
    volume_changepoints: pd.DataFrame,
    pro_rate_changepoints: pd.DataFrame,
    time_bucket_profiles: pd.DataFrame,
    day_bucket_profiles: pd.DataFrame,
    org_blank_rates: pd.DataFrame,
    voter_match_by_bucket: pd.DataFrame,
) -> dict[str, Any]:
    placeholder_table_map = {
        "artifacts.counts_per_minute": counts_per_minute,
        _table_key("changepoints", "volume_changepoints"): volume_changepoints,
        _table_key("changepoints", "pro_rate_changepoints"): pro_rate_changepoints,
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
) -> dict[str, Any]:
    table_map = _load_table_map_from_results(results=results, artifacts=artifacts)
    detector_summaries = {name: result.summary for name, result in sorted(results.items())}
    return _build_interactive_chart_payload_v2(
        table_map=table_map,
        detector_summaries=detector_summaries,
    )


def _interactive_chart_payload_from_disk(out_dir: Path) -> dict[str, Any]:
    table_map = _load_table_map_from_disk(out_dir=out_dir)
    detector_summaries = _load_summaries_from_disk(out_dir)
    return _build_interactive_chart_payload_v2(
        table_map=table_map,
        detector_summaries=detector_summaries,
    )


def render_report(
    results: dict[str, DetectorResult],
    artifacts: dict[str, pd.DataFrame],
    out_dir: Path,
) -> Path:
    report_started = perf_counter()
    generated_at = datetime.now(timezone.utc).isoformat()
    env = _template_env()
    template = env.get_template("report.html.j2")

    detector_summaries = (
        {name: result.summary for name, result in sorted(results.items())}
        if results
        else _load_summaries_from_disk(out_dir)
    )
    artifact_rows = (
        {name: len(table) for name, table in sorted(artifacts.items())}
        if artifacts
        else _artifact_rows_from_disk(out_dir)
    )
    table_previews = (
        _table_previews_from_results(results)
        if results
        else _load_table_previews_from_disk(out_dir)
    )
    evidence_bundle_preview = (
        _evidence_bundle_preview_from_results(results)
        if results
        else _evidence_bundle_preview_from_disk(out_dir)
    )
    rarity_coverage_preview = (
        _rare_names_table_preview_from_results(
            results, table_name="rarity_lookup_coverage", max_rows=5
        )
        if results
        else _rare_names_table_preview_from_disk(
            out_dir, table_name="rarity_lookup_coverage", max_rows=5
        )
    )
    rarity_unmatched_first_preview = (
        _rare_names_table_preview_from_results(
            results, table_name="rarity_unmatched_first_tokens", max_rows=12
        )
        if results
        else _rare_names_table_preview_from_disk(
            out_dir, table_name="rarity_unmatched_first_tokens", max_rows=12
        )
    )
    rarity_unmatched_last_preview = (
        _rare_names_table_preview_from_results(
            results, table_name="rarity_unmatched_last_tokens", max_rows=12
        )
        if results
        else _rare_names_table_preview_from_disk(
            out_dir, table_name="rarity_unmatched_last_tokens", max_rows=12
        )
    )
    clockface_top_preview = (
        _periodicity_table_preview_from_results(
            results, table_name="clockface_top_minutes", max_rows=12
        )
        if results
        else _periodicity_table_preview_from_disk(
            out_dir, table_name="clockface_top_minutes", max_rows=12
        )
    )
    table_column_docs = _build_table_column_docs(
        table_previews=table_previews,
        artifact_rows=artifact_rows,
        evidence_bundle_preview=evidence_bundle_preview,
        rarity_coverage_preview=rarity_coverage_preview,
        rarity_unmatched_first_preview=rarity_unmatched_first_preview,
        rarity_unmatched_last_preview=rarity_unmatched_last_preview,
        clockface_top_preview=clockface_top_preview,
    )
    table_help_docs = _build_table_help_docs(table_column_docs=table_column_docs)
    interactive_started = perf_counter()
    interactive_charts = (
        _interactive_chart_payload_from_results(results=results, artifacts=artifacts)
        if results
        else _interactive_chart_payload_from_disk(out_dir=out_dir)
    )
    interactive_build_ms = round((perf_counter() - interactive_started) * 1000.0, 3)
    if isinstance(interactive_charts.get("controls"), dict):
        runtime_metrics = interactive_charts["controls"].get("runtime", {})
        if not isinstance(runtime_metrics, dict):
            runtime_metrics = {}
        runtime_metrics["interactive_payload_build_ms"] = interactive_build_ms
        interactive_charts["controls"]["runtime"] = runtime_metrics

    template_started = perf_counter()
    rendered = template.render(
        generated_at=generated_at,
        detector_summaries=_json_safe(detector_summaries),
        artifact_rows=_json_safe(artifact_rows),
        table_previews=_json_safe(table_previews),
        evidence_bundle_preview=_json_safe(evidence_bundle_preview),
        rarity_coverage_preview=_json_safe(rarity_coverage_preview),
        rarity_unmatched_first_preview=_json_safe(rarity_unmatched_first_preview),
        rarity_unmatched_last_preview=_json_safe(rarity_unmatched_last_preview),
        clockface_top_preview=_json_safe(clockface_top_preview),
        table_column_docs=_json_safe(table_column_docs),
        table_help_docs=_json_safe(table_help_docs),
        interactive_charts=_json_safe(interactive_charts),
        figure_files=sorted(path.name for path in (out_dir / "figures").glob("*")),
    )
    template_render_ms = round((perf_counter() - template_started) * 1000.0, 3)

    report_path = out_dir / "report.html"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    write_started = perf_counter()
    report_path.write_text(rendered, encoding="utf-8")
    report_write_ms = round((perf_counter() - write_started) * 1000.0, 3)

    runtime_metrics = {
        "generated_at": generated_at,
        "interactive_payload_build_ms": interactive_build_ms,
        "template_render_ms": template_render_ms,
        "report_write_ms": report_write_ms,
        "report_total_ms": round((perf_counter() - report_started) * 1000.0, 3),
        "report_html_bytes": int(report_path.stat().st_size),
    }
    runtime_path = out_dir / "artifacts" / "report_runtime.json"
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_path.write_text(
        json.dumps(_json_safe(runtime_metrics), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return report_path
