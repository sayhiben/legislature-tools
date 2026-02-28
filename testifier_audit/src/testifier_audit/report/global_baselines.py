from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import numpy as np

GLOBAL_BASELINES_FILENAME = "global_baselines.json"
LEAVE_ONE_OUT_BASELINE_FILENAME = "cross_hearing_baseline_loo.json"
FEATURE_VECTOR_SCHEMA_VERSION = 2
GLOBAL_BASELINES_SCHEMA_VERSION = 1
LEAVE_ONE_OUT_BASELINE_SCHEMA_VERSION = 2
_SUPPORT_MIN_REPORTS = 10
_SUPPORT_MIN_REPORTS_FOR_SEVERITY = 20

_COMPARATOR_METRIC_SPECS: tuple[tuple[str, str], ...] = (
    ("total_submissions", "Total submissions"),
    ("overall_pro_rate", "Overall pro rate"),
    ("window_high_share", "High-tier window share"),
    ("window_top_score", "Top window score"),
    ("window_top_abs_z", "Top window |z|"),
    ("window_top_dup_fraction", "Top window duplicate fraction"),
    ("top_name_max_records", "Top repeated-name records"),
    ("off_hours_ratio", "Off-hours submission ratio"),
    ("blank_org_ratio", "Blank organization rate"),
    ("blank_org_gap_pro_minus_con", "Blank organization gap (Pro minus Con)"),
    ("dedup_drop_fraction", "Dedup drop fraction"),
)

_LEAVE_ONE_OUT_CHANNEL_LABELS: dict[str, str] = {
    "cohort_loo": "Cohort LOO",
    "global_loo": "Global LOO",
}


@dataclass(frozen=True)
class ReportFeatureRecord:
    report_id: str
    feature_vector: dict[str, Any]
    summary_path: Path


def default_cross_hearing_baseline_payload() -> dict[str, Any]:
    return {
        "available": False,
        "reason": "global_baselines_unavailable",
        "report_count": 0,
        "metric_comparators": [],
        "top_name_cues": [],
    }


def _safe_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        candidate = float(value)
        return candidate if np.isfinite(candidate) else None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            candidate = float(stripped)
        except ValueError:
            return None
        return candidate if np.isfinite(candidate) else None
    return None


def _safe_int(value: Any, default: int = 0) -> int:
    parsed = _safe_float(value)
    return int(parsed) if parsed is not None else int(default)


def _safe_optional_int(value: Any) -> int | None:
    parsed = _safe_float(value)
    return int(parsed) if parsed is not None else None


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_rows(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, Mapping):
            rows.append(dict(item))
    return rows


def _percentile_rank(values: list[float], value: float) -> float | None:
    if not values:
        return None
    arr = np.asarray(values, dtype=float)
    if arr.size == 0:
        return None
    less = float(np.sum(arr < value))
    equal = float(np.sum(arr == value))
    return (less + 0.5 * equal) / float(arr.size)


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    arr = np.asarray(values, dtype=float)
    if arr.size == 0:
        return None
    return float(np.quantile(arr, q, method="linear"))


def _robust_zscore(values: list[float], value: float) -> float | None:
    if not values:
        return None
    arr = np.asarray(values, dtype=float)
    if arr.size == 0:
        return None
    median = float(np.median(arr))
    mad = float(np.median(np.abs(arr - median)))
    if mad <= 0.0 or not np.isfinite(mad):
        return None
    scale = 1.4826 * mad
    if scale <= 0.0:
        return None
    z = (float(value) - median) / scale
    return float(z) if np.isfinite(z) else None


def _empirical_two_sided_tail_p(values: list[float], value: float) -> float | None:
    if not values:
        return None
    arr = np.asarray(values, dtype=float)
    n = int(arr.size)
    if n <= 0:
        return None
    lower_tail = (float(np.sum(arr <= value)) + 1.0) / float(n + 1)
    upper_tail = (float(np.sum(arr >= value)) + 1.0) / float(n + 1)
    p = 2.0 * min(lower_tail, upper_tail)
    return float(min(1.0, max(0.0, p)))


def _first_non_null_float(*values: Any) -> float | None:
    for value in values:
        parsed = _safe_float(value)
        if parsed is not None:
            return parsed
    return None


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _normalize_name_rows(rows: list[dict[str, Any]], *, max_rows: int = 20) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        canonical_name = str(row.get("canonical_name") or "").strip()
        if not canonical_name:
            continue
        normalized.append(
            {
                "canonical_name": canonical_name,
                "display_name": str(row.get("display_name") or "").strip(),
                "n_records": _safe_int(row.get("n_records", row.get("n", row.get("count", 0)))),
                "n_pro": _safe_int(row.get("n_pro", 0)),
                "n_con": _safe_int(row.get("n_con", 0)),
            }
        )
    normalized.sort(
        key=lambda row: (
            -int(row.get("n_records", 0)),
            str(row.get("canonical_name", "")),
        )
    )
    return normalized[:max_rows]


def _normalize_cohort_text(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return text


def _cohort_token(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text


def _infer_chamber_from_report_id(report_id: str) -> str | None:
    head = str(report_id or "").strip().split("-", 1)[0].upper()
    if not head:
        return None
    if "HB" in head:
        return "House"
    if "SB" in head:
        return "Senate"
    return None


def _support_tier_for_n_reports(n_reports: int) -> str:
    if int(n_reports) < _SUPPORT_MIN_REPORTS:
        return "unavailable"
    if int(n_reports) < _SUPPORT_MIN_REPORTS_FOR_SEVERITY:
        return "descriptive_only"
    return "supported"


def _support_flags_for_n_reports(n_reports: int) -> dict[str, Any]:
    tier = _support_tier_for_n_reports(int(n_reports))
    return {
        "support_tier": tier,
        "descriptive_only": tier == "descriptive_only",
        "low_power": tier != "supported",
        "comparator_available": tier != "unavailable",
    }


def _empty_leave_one_out_channel(*, channel: str, label: str) -> dict[str, Any]:
    return {
        "channel": channel,
        "label": label,
        "available": False,
        "reason": "no_comparison_reports",
        "comparison_report_ids": [],
        "report_count": 0,
        "support_tier": "unavailable",
        "descriptive_only": False,
        "low_power": True,
        "metric_comparators": [],
        "top_name_cues": [],
    }


def default_cross_hearing_loo_payload() -> dict[str, Any]:
    channels = {
        channel: _empty_leave_one_out_channel(channel=channel, label=label)
        for channel, label in _LEAVE_ONE_OUT_CHANNEL_LABELS.items()
    }
    return {
        "schema_version": LEAVE_ONE_OUT_BASELINE_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "source": "leave_one_out",
        "target_report_id": "",
        "report_id": "",
        "excluded_report_ids": [],
        "available": False,
        "reason": "target_report_id_required",
        "selected_channel": "cohort_loo",
        "default_channel": "cohort_loo",
        "channel_options": [
            {"id": channel, "label": label} for channel, label in _LEAVE_ONE_OUT_CHANNEL_LABELS.items()
        ],
        "channels": channels,
        # Top-level compatibility aliases. These mirror the selected channel.
        "comparison_report_ids": [],
        "report_count": 0,
        "support_tier": "unavailable",
        "descriptive_only": False,
        "low_power": True,
        "metric_comparators": [],
        "top_name_cues": [],
        # Optional metadata used by the renderer for annotations.
        "cohort_strategy": "committee_chamber_then_chamber_then_global",
    }

def build_feature_vector(
    *,
    report_id: str,
    triage_summary: Mapping[str, Any],
    window_evidence_queue: list[dict[str, Any]],
    record_evidence_queue: list[dict[str, Any]],
    cluster_evidence_queue: list[dict[str, Any]],
    data_quality_panel: Mapping[str, Any],
    detector_summaries: Mapping[str, Mapping[str, Any]] | None = None,
    hearing_context_panel: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = _as_dict(triage_summary)
    window_rows = _as_rows(window_evidence_queue)
    record_rows = _as_rows(record_evidence_queue)
    cluster_rows = _as_rows(cluster_evidence_queue)
    quality = _as_dict(data_quality_panel)
    detector_summary_map = (
        {str(key): _as_dict(value) for key, value in detector_summaries.items()}
        if isinstance(detector_summaries, Mapping)
        else {}
    )
    context_panel = _as_dict(hearing_context_panel)
    context_source = _as_dict(context_panel.get("source"))
    queue_counts = _as_dict(summary.get("queue_counts"))
    tier_counts = _as_dict(summary.get("window_tier_counts"))

    window_scores = [
        score
        for score in (_safe_float(row.get("score")) for row in window_rows)
        if score is not None
    ]
    window_abs_z = [
        abs(z) for z in (_safe_float(row.get("z")) for row in window_rows) if z is not None
    ]
    window_dup_fraction = [
        value
        for value in (_safe_float(row.get("dup_fraction")) for row in window_rows)
        if value is not None
    ]
    window_q_values = [
        value
        for value in (_safe_float(row.get("q_value")) for row in window_rows)
        if value is not None and value >= 0.0
    ]

    top_name_rows = _normalize_name_rows(
        _as_rows(summary.get("top_repeated_names")) or _normalize_name_rows(record_rows)
    )
    top_name_max_records = max((row["n_records"] for row in top_name_rows), default=0) or None

    off_hours_summary = _as_dict(summary.get("off_hours_summary"))
    off_hours_detector_summary = _as_dict(detector_summary_map.get("off_hours"))
    org_anomalies_summary = _as_dict(detector_summary_map.get("org_anomalies"))
    duplicates_exact_summary = _as_dict(detector_summary_map.get("duplicates_exact"))
    off_hours_ratio = _first_non_null_float(
        off_hours_summary.get("off_hours_ratio"),
        off_hours_detector_summary.get("off_hours_ratio"),
    )

    raw_total = _first_non_null_float(
        summary.get("total_submissions_raw"),
        summary.get("total_submissions"),
        off_hours_summary.get("total"),
    )
    dedup_total = _first_non_null_float(
        summary.get("total_submissions_exact_row_dedup"),
        summary.get("total_submissions"),
    )
    dedup_drop_fraction = None
    if raw_total is not None and raw_total > 0 and dedup_total is not None:
        dedup_drop_fraction = max((raw_total - dedup_total) / raw_total, 0.0)
    if dedup_drop_fraction is None:
        dedup_drop_fraction = _safe_float(duplicates_exact_summary.get("dedup_drop_fraction"))

    blank_org_ratio = _first_non_null_float(
        org_anomalies_summary.get("blank_org_ratio"),
    )
    blank_org_gap_pro_minus_con = _first_non_null_float(
        org_anomalies_summary.get("blank_org_gap_pro_minus_con"),
    )

    window_high_count = _safe_optional_int(
        sum(1 for row in window_rows if str(row.get("evidence_tier") or "") == "high")
    )
    window_medium_count = _safe_optional_int(
        sum(1 for row in window_rows if str(row.get("evidence_tier") or "") == "medium")
    )
    window_watch_count = _safe_optional_int(
        sum(1 for row in window_rows if str(row.get("evidence_tier") or "") == "watch")
    )
    if not window_rows:
        window_high_count = _safe_optional_int(tier_counts.get("high"))
        window_medium_count = _safe_optional_int(tier_counts.get("medium"))
        window_watch_count = _safe_optional_int(tier_counts.get("watch"))

    window_queue_size = _safe_optional_int(len(window_rows))
    record_queue_size = _safe_optional_int(len(record_rows))
    cluster_queue_size = _safe_optional_int(len(cluster_rows))
    if not window_rows:
        window_queue_size = _safe_optional_int(queue_counts.get("window"))
    if not record_rows:
        record_queue_size = _safe_optional_int(queue_counts.get("record"))
    if not cluster_rows:
        cluster_queue_size = _safe_optional_int(queue_counts.get("cluster"))

    window_high_share = None
    if window_queue_size is not None and window_queue_size > 0 and window_high_count is not None:
        window_high_share = float(window_high_count) / float(window_queue_size)

    chamber = _normalize_cohort_text(
        summary.get("chamber")
        or context_source.get("chamber")
        or context_panel.get("chamber")
        or _infer_chamber_from_report_id(report_id)
    )
    committee_name = _normalize_cohort_text(
        summary.get("committee_name")
        or context_source.get("committee_name")
        or context_panel.get("committee_name")
    )

    metrics = {
        "total_submissions": _safe_optional_int(summary.get("total_submissions")),
        "overall_pro_rate": _safe_float(summary.get("overall_pro_rate")),
        "overall_con_rate": _safe_float(summary.get("overall_con_rate")),
        "window_queue_size": window_queue_size,
        "record_queue_size": record_queue_size,
        "cluster_queue_size": cluster_queue_size,
        "window_high_count": window_high_count,
        "window_medium_count": window_medium_count,
        "window_watch_count": window_watch_count,
        "window_high_share": window_high_share,
        "window_top_score": max(window_scores) if window_scores else None,
        "window_top_abs_z": max(window_abs_z) if window_abs_z else None,
        "window_top_dup_fraction": (
            max(window_dup_fraction)
            if window_dup_fraction
            else _safe_float(duplicates_exact_summary.get("duplicate_row_rate"))
        ),
        "window_min_q_value": min(window_q_values) if window_q_values else None,
        "top_name_max_records": top_name_max_records,
        "off_hours_ratio": off_hours_ratio,
        "blank_org_ratio": blank_org_ratio,
        "blank_org_gap_pro_minus_con": blank_org_gap_pro_minus_con,
        "dedup_drop_fraction": dedup_drop_fraction,
    }

    quality_metrics = _as_rows(
        quality.get("triage_raw_vs_dedup_metrics", quality.get("raw_vs_dedup_metrics", []))
    )
    quality_metric_count = int(sum(1 for row in quality_metrics if row.get("material_change")))

    return {
        "schema_version": FEATURE_VECTOR_SCHEMA_VERSION,
        "report_id": str(report_id),
        "generated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "lens": str(summary.get("lens") or "unknown"),
        "date_range_start": summary.get("date_range_start"),
        "date_range_end": summary.get("date_range_end"),
        "cohort": {
            "chamber": chamber,
            "committee_name": committee_name,
        },
        "chamber": chamber,
        "committee_name": committee_name,
        "metrics": metrics,
        "top_repeated_names": top_name_rows,
        "material_quality_metric_count": quality_metric_count,
        # Compatibility keys retained for previously emitted shape.
        "total_submissions": metrics["total_submissions"],
        "overall_pro_rate": metrics["overall_pro_rate"],
        "overall_con_rate": metrics["overall_con_rate"],
        "window_queue_size": metrics["window_queue_size"],
        "record_queue_size": metrics["record_queue_size"],
        "cluster_queue_size": metrics["cluster_queue_size"],
        "window_high_count": metrics["window_high_count"],
        "window_medium_count": metrics["window_medium_count"],
        "window_watch_count": metrics["window_watch_count"],
    }


def _normalize_feature_record(
    report_id: str,
    *,
    feature_vector: dict[str, Any],
    investigation_summary: dict[str, Any],
) -> dict[str, Any]:
    feature = dict(feature_vector)
    metrics = _as_dict(feature.get("metrics"))
    summary = _as_dict(investigation_summary)

    metrics.setdefault(
        "total_submissions",
        _safe_optional_int(feature.get("total_submissions", summary.get("total_submissions"))),
    )
    metrics.setdefault(
        "overall_pro_rate",
        _first_non_null_float(feature.get("overall_pro_rate"), summary.get("overall_pro_rate")),
    )
    metrics.setdefault(
        "overall_con_rate",
        _first_non_null_float(feature.get("overall_con_rate"), summary.get("overall_con_rate")),
    )
    metrics.setdefault(
        "window_queue_size",
        _safe_optional_int(
            feature.get("window_queue_size", _as_dict(summary.get("queue_counts")).get("window"))
        ),
    )
    metrics.setdefault(
        "record_queue_size",
        _safe_optional_int(
            feature.get("record_queue_size", _as_dict(summary.get("queue_counts")).get("record"))
        ),
    )
    metrics.setdefault(
        "cluster_queue_size",
        _safe_optional_int(
            feature.get("cluster_queue_size", _as_dict(summary.get("queue_counts")).get("cluster"))
        ),
    )
    metrics.setdefault(
        "window_high_count",
        _safe_optional_int(
            feature.get("window_high_count", _as_dict(summary.get("window_tier_counts")).get("high"))
        ),
    )

    if _safe_float(metrics.get("window_high_share")) is None:
        queue_size = _safe_float(metrics.get("window_queue_size"))
        high_count = _safe_float(metrics.get("window_high_count"))
        metrics["window_high_share"] = (
            (high_count / queue_size)
            if queue_size is not None and high_count is not None and queue_size > 0
            else None
        )

    top_names = _normalize_name_rows(
        _as_rows(feature.get("top_repeated_names")) or _as_rows(summary.get("top_repeated_names"))
    )

    if _safe_float(metrics.get("top_name_max_records")) is None:
        metrics["top_name_max_records"] = max((row["n_records"] for row in top_names), default=0) or None
    if _safe_float(metrics.get("off_hours_ratio")) is None:
        metrics["off_hours_ratio"] = _safe_float(
            _as_dict(summary.get("off_hours_summary")).get("off_hours_ratio")
        )

    if _safe_float(metrics.get("dedup_drop_fraction")) is None:
        raw_total = _first_non_null_float(
            summary.get("total_submissions_raw"),
            summary.get("total_submissions"),
            feature.get("total_submissions"),
        )
        dedup_total = _first_non_null_float(
            summary.get("total_submissions_exact_row_dedup"),
            summary.get("total_submissions"),
        )
        if raw_total is not None and raw_total > 0 and dedup_total is not None:
            metrics["dedup_drop_fraction"] = max((raw_total - dedup_total) / raw_total, 0.0)

    feature["schema_version"] = int(feature.get("schema_version") or FEATURE_VECTOR_SCHEMA_VERSION)
    feature["report_id"] = str(feature.get("report_id") or report_id)
    cohort = _as_dict(feature.get("cohort"))
    chamber = _normalize_cohort_text(
        cohort.get("chamber")
        or feature.get("chamber")
        or summary.get("chamber")
        or _infer_chamber_from_report_id(report_id)
    )
    committee_name = _normalize_cohort_text(
        cohort.get("committee_name")
        or feature.get("committee_name")
        or summary.get("committee_name")
    )
    feature["cohort"] = {
        "chamber": chamber,
        "committee_name": committee_name,
    }
    feature["chamber"] = chamber
    feature["committee_name"] = committee_name
    feature["metrics"] = metrics
    feature["top_repeated_names"] = top_names
    return feature


def collect_report_feature_records(reports_dir: Path) -> list[ReportFeatureRecord]:
    records: list[ReportFeatureRecord] = []
    if not reports_dir.exists():
        return records

    for report_dir in sorted(path for path in reports_dir.iterdir() if path.is_dir()):
        if report_dir.name.startswith("."):
            continue
        summary_dir = report_dir / "summary"
        if not summary_dir.exists():
            continue
        feature_path = summary_dir / "feature_vector.json"
        summary_path = summary_dir / "investigation_summary.json"
        feature_payload = _read_json(feature_path)
        summary_payload = _read_json(summary_path)
        if not feature_payload and not summary_payload:
            continue
        normalized = _normalize_feature_record(
            report_id=report_dir.name,
            feature_vector=feature_payload,
            investigation_summary=summary_payload,
        )
        records.append(
            ReportFeatureRecord(
                report_id=report_dir.name,
                feature_vector=normalized,
                summary_path=feature_path,
            )
        )

    return records


def _build_metric_distributions(records: list[ReportFeatureRecord]) -> dict[str, list[float]]:
    metrics_by_key: dict[str, list[float]] = {key: [] for key, _ in _COMPARATOR_METRIC_SPECS}
    for record in records:
        metrics = _as_dict(record.feature_vector.get("metrics"))
        for key, _label in _COMPARATOR_METRIC_SPECS:
            value = _safe_float(metrics.get(key, record.feature_vector.get(key)))
            if value is not None:
                metrics_by_key[key].append(value)
    return metrics_by_key


def _build_name_occurrence_index(records: list[ReportFeatureRecord]) -> dict[str, dict[str, Any]]:
    name_occurrences: dict[str, dict[str, Any]] = {}
    for record in records:
        for row in _normalize_name_rows(_as_rows(record.feature_vector.get("top_repeated_names"))):
            canonical_name = str(row.get("canonical_name") or "").strip()
            if not canonical_name:
                continue
            entry = name_occurrences.setdefault(
                canonical_name,
                {
                    "canonical_name": canonical_name,
                    "display_name": str(row.get("display_name") or ""),
                    "report_ids": set(),
                    "max_n_records": 0,
                },
            )
            entry["report_ids"].add(record.report_id)
            entry["max_n_records"] = max(
                int(entry.get("max_n_records") or 0),
                _safe_int(row.get("n_records"), default=0),
            )
    return name_occurrences


def _max_name_record_values(name_occurrences: Mapping[str, Mapping[str, Any]]) -> list[float]:
    values: list[float] = []
    for entry in name_occurrences.values():
        raw = entry.get("max_n_records")
        parsed = _safe_int(raw, default=0)
        if parsed > 0:
            values.append(float(parsed))
    return values


def _build_report_comparator_entry(
    *,
    report_id: str,
    feature_vector: Mapping[str, Any],
    metrics_by_key: Mapping[str, list[float]],
    name_occurrences: Mapping[str, Mapping[str, Any]],
    max_name_records_values: list[float],
    report_count: int,
) -> dict[str, Any]:
    feature = dict(feature_vector)
    metrics = _as_dict(feature.get("metrics"))

    metric_comparators: list[dict[str, Any]] = []
    for key, label in _COMPARATOR_METRIC_SPECS:
        value = _safe_float(metrics.get(key, feature.get(key)))
        distribution = list(metrics_by_key.get(key, []))
        if value is None:
            continue
        n_reports = len(distribution)
        support_flags = _support_flags_for_n_reports(n_reports)
        if not distribution:
            metric_comparators.append(
                {
                    "metric": key,
                    "label": label,
                    "value": value,
                    "observed": value,
                    "expected": None,
                    "delta": None,
                    "percentile": None,
                    "band_p10": None,
                    "band_p50": None,
                    "band_p90": None,
                    "robust_z": None,
                    "empirical_tail_p_two_sided": None,
                    "n_reports": 0,
                    **support_flags,
                }
            )
            continue

        expected = _quantile(distribution, 0.50)
        delta = (value - expected) if expected is not None else None
        metric_comparators.append(
            {
                "metric": key,
                "label": label,
                "value": value,
                "observed": value,
                "expected": expected,
                "delta": delta,
                "percentile": _percentile_rank(distribution, value),
                "band_p10": _quantile(distribution, 0.10),
                "band_p50": expected,
                "band_p90": _quantile(distribution, 0.90),
                "robust_z": _robust_zscore(distribution, value),
                "empirical_tail_p_two_sided": _empirical_two_sided_tail_p(distribution, value),
                "n_reports": n_reports,
                **support_flags,
            }
        )

    top_name_cues: list[dict[str, Any]] = []
    channel_support = _support_flags_for_n_reports(report_count)
    for row in _normalize_name_rows(_as_rows(feature.get("top_repeated_names"))):
        canonical_name = str(row.get("canonical_name") or "").strip()
        if not canonical_name:
            continue
        corpus = name_occurrences.get(canonical_name, {})
        report_ids = corpus.get("report_ids", set())
        report_name_count = int(len(report_ids))
        current_n_records = _safe_int(row.get("n_records"), default=0)
        max_n_records = _safe_int(corpus.get("max_n_records"), default=current_n_records)
        top_name_cues.append(
            {
                "canonical_name": canonical_name,
                "display_name": str(row.get("display_name") or ""),
                "current_n_records": current_n_records,
                "report_count": report_name_count,
                "report_share": (
                    (report_name_count / float(report_count)) if report_count > 0 else 0.0
                ),
                "max_n_records_across_reports": max_n_records,
                "max_n_records_percentile": _percentile_rank(
                    max_name_records_values,
                    float(max_n_records),
                ),
                "n_reports": int(report_count),
                **channel_support,
            }
        )

    return {
        "available": report_count > 0,
        "report_id": report_id,
        "report_count": int(report_count),
        **channel_support,
        "metric_comparators": metric_comparators,
        "top_name_cues": top_name_cues,
    }


def _record_cohort_values(record: ReportFeatureRecord) -> tuple[str | None, str | None]:
    feature = _as_dict(record.feature_vector)
    cohort = _as_dict(feature.get("cohort"))
    chamber = _normalize_cohort_text(
        cohort.get("chamber") or feature.get("chamber") or _infer_chamber_from_report_id(record.report_id)
    )
    committee_name = _normalize_cohort_text(
        cohort.get("committee_name") or feature.get("committee_name")
    )
    return chamber, committee_name


def _select_cohort_records(
    *,
    target_record: ReportFeatureRecord,
    comparison_records: list[ReportFeatureRecord],
) -> tuple[list[ReportFeatureRecord], dict[str, Any]]:
    target_chamber, target_committee = _record_cohort_values(target_record)
    chamber_token = _cohort_token(target_chamber)
    committee_token = _cohort_token(target_committee)
    metadata: dict[str, Any] = {
        "strategy": "committee_chamber_then_chamber_then_global",
        "target_chamber": target_chamber,
        "target_committee_name": target_committee,
        "selected_level": "global",
        "fallback_used": True,
    }

    if chamber_token and committee_token:
        same_committee_and_chamber = [
            record
            for record in comparison_records
            if _cohort_token(_record_cohort_values(record)[0]) == chamber_token
            and _cohort_token(_record_cohort_values(record)[1]) == committee_token
        ]
        if len(same_committee_and_chamber) >= _SUPPORT_MIN_REPORTS:
            metadata["selected_level"] = "committee_chamber"
            metadata["fallback_used"] = False
            metadata["report_count_by_level"] = {
                "committee_chamber": len(same_committee_and_chamber),
            }
            return same_committee_and_chamber, metadata
        metadata["report_count_committee_chamber"] = len(same_committee_and_chamber)

    if chamber_token:
        same_chamber = [
            record
            for record in comparison_records
            if _cohort_token(_record_cohort_values(record)[0]) == chamber_token
        ]
        if len(same_chamber) >= _SUPPORT_MIN_REPORTS:
            metadata["selected_level"] = "chamber"
            metadata["fallback_used"] = True
            metadata["report_count_by_level"] = {
                "chamber": len(same_chamber),
            }
            return same_chamber, metadata
        metadata["report_count_chamber"] = len(same_chamber)

    metadata["selected_level"] = "global"
    metadata["fallback_used"] = True
    metadata["report_count_by_level"] = {
        "global": len(comparison_records),
    }
    return comparison_records, metadata


def _build_leave_one_out_channel_payload(
    *,
    channel: str,
    label: str,
    report_id: str,
    feature_vector: Mapping[str, Any],
    comparison_records: list[ReportFeatureRecord],
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    channel_payload = _empty_leave_one_out_channel(channel=channel, label=label)
    channel_payload["comparison_report_ids"] = [record.report_id for record in comparison_records]
    channel_payload["report_count"] = int(len(comparison_records))
    channel_payload["metadata"] = dict(metadata) if isinstance(metadata, Mapping) else {}

    support_flags = _support_flags_for_n_reports(channel_payload["report_count"])
    channel_payload.update(
        {
            "support_tier": support_flags["support_tier"],
            "descriptive_only": support_flags["descriptive_only"],
            "low_power": support_flags["low_power"],
        }
    )
    if not support_flags["comparator_available"]:
        channel_payload["available"] = False
        channel_payload["reason"] = "insufficient_support"
        return channel_payload

    metrics_by_key = _build_metric_distributions(comparison_records)
    name_occurrences = _build_name_occurrence_index(comparison_records)
    max_name_records_values = _max_name_record_values(name_occurrences)
    comparator = _build_report_comparator_entry(
        report_id=report_id,
        feature_vector=feature_vector,
        metrics_by_key=metrics_by_key,
        name_occurrences=name_occurrences,
        max_name_records_values=max_name_records_values,
        report_count=len(comparison_records),
    )
    channel_payload.update(
        {
            "available": True,
            "reason": "",
            "metric_comparators": comparator.get("metric_comparators", []),
            "top_name_cues": comparator.get("top_name_cues", []),
        }
    )
    return channel_payload


def _choose_selected_channel(channels: Mapping[str, Mapping[str, Any]]) -> str:
    cohort = _as_dict(channels.get("cohort_loo"))
    global_loo = _as_dict(channels.get("global_loo"))
    if bool(cohort.get("available")):
        return "cohort_loo"
    if bool(global_loo.get("available")):
        return "global_loo"
    return "cohort_loo"


def _merge_selected_channel_aliases(payload: dict[str, Any]) -> dict[str, Any]:
    channels = _as_dict(payload.get("channels"))
    selected_channel = str(payload.get("selected_channel") or _choose_selected_channel(channels))
    selected = _as_dict(channels.get(selected_channel))
    if not selected:
        selected = _empty_leave_one_out_channel(
            channel=selected_channel,
            label=_LEAVE_ONE_OUT_CHANNEL_LABELS.get(selected_channel, selected_channel),
        )
    payload["selected_channel"] = selected_channel
    payload["available"] = bool(selected.get("available"))
    if payload["available"]:
        payload.pop("reason", None)
    else:
        payload["reason"] = str(payload.get("reason") or selected.get("reason") or "unavailable")
    payload["comparison_report_ids"] = list(selected.get("comparison_report_ids") or [])
    payload["report_count"] = int(selected.get("report_count") or 0)
    payload["support_tier"] = str(selected.get("support_tier") or "unavailable")
    payload["descriptive_only"] = bool(selected.get("descriptive_only"))
    payload["low_power"] = bool(selected.get("low_power", True))
    payload["metric_comparators"] = list(selected.get("metric_comparators") or [])
    payload["top_name_cues"] = list(selected.get("top_name_cues") or [])
    return payload


def normalize_leave_one_out_baseline_payload(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    normalized = default_cross_hearing_loo_payload()
    if isinstance(payload, Mapping):
        for key, value in dict(payload).items():
            if key == "channels":
                continue
            normalized[key] = value

    raw_channels = _as_dict(payload.get("channels") if isinstance(payload, Mapping) else None)
    channels: dict[str, Any] = {
        channel: _empty_leave_one_out_channel(channel=channel, label=label)
        for channel, label in _LEAVE_ONE_OUT_CHANNEL_LABELS.items()
    }
    for channel, label in _LEAVE_ONE_OUT_CHANNEL_LABELS.items():
        candidate = _as_dict(raw_channels.get(channel))
        if not candidate and channel == "global_loo" and isinstance(payload, Mapping):
            # Backward compatibility for prior single-channel payloads.
            candidate = {
                "available": bool(payload.get("available")),
                "reason": str(payload.get("reason") or ""),
                "comparison_report_ids": list(payload.get("comparison_report_ids") or []),
                "report_count": int(payload.get("report_count") or 0),
                "metric_comparators": list(payload.get("metric_comparators") or []),
                "top_name_cues": list(payload.get("top_name_cues") or []),
            }
        merged = _empty_leave_one_out_channel(channel=channel, label=label)
        merged.update(candidate)
        merged["channel"] = channel
        merged["label"] = label
        merged["comparison_report_ids"] = list(merged.get("comparison_report_ids") or [])
        merged["report_count"] = int(merged.get("report_count") or 0)
        merged["metric_comparators"] = list(merged.get("metric_comparators") or [])
        merged["top_name_cues"] = list(merged.get("top_name_cues") or [])
        support_flags = _support_flags_for_n_reports(merged["report_count"])
        provided_support_tier = (
            str(candidate.get("support_tier") or "").strip() if "support_tier" in candidate else ""
        )
        if provided_support_tier not in {"unavailable", "descriptive_only", "supported"}:
            provided_support_tier = support_flags["support_tier"]
        merged["support_tier"] = provided_support_tier
        if "descriptive_only" in candidate:
            merged["descriptive_only"] = bool(candidate.get("descriptive_only"))
        else:
            merged["descriptive_only"] = provided_support_tier == "descriptive_only"
        if "low_power" in candidate:
            merged["low_power"] = bool(candidate.get("low_power"))
        else:
            merged["low_power"] = provided_support_tier != "supported"
        if not merged.get("available") and not merged.get("reason"):
            merged["reason"] = "unavailable"
        channels[channel] = merged

    normalized["channels"] = channels
    normalized["channel_options"] = [
        {"id": channel, "label": label} for channel, label in _LEAVE_ONE_OUT_CHANNEL_LABELS.items()
    ]
    normalized["default_channel"] = str(normalized.get("default_channel") or "cohort_loo")
    explicit_selected_channel = (
        str(payload.get("selected_channel") or "").strip()
        if isinstance(payload, Mapping) and "selected_channel" in payload
        else ""
    )
    if explicit_selected_channel in channels:
        normalized["selected_channel"] = explicit_selected_channel
    else:
        normalized["selected_channel"] = _choose_selected_channel(channels)
    normalized["schema_version"] = int(
        normalized.get("schema_version") or LEAVE_ONE_OUT_BASELINE_SCHEMA_VERSION
    )
    normalized["target_report_id"] = str(normalized.get("target_report_id") or "")
    if normalized["target_report_id"] and str(normalized.get("reason") or "") == "target_report_id_required":
        normalized["reason"] = ""
    normalized["report_id"] = str(normalized.get("report_id") or normalized["target_report_id"])
    normalized["excluded_report_ids"] = list(normalized.get("excluded_report_ids") or [])
    return _merge_selected_channel_aliases(normalized)


def build_global_baselines(records: list[ReportFeatureRecord]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": GLOBAL_BASELINES_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "report_count": int(len(records)),
        "reports": [record.report_id for record in records],
        "by_report": {},
    }
    if not records:
        return payload

    metrics_by_key = _build_metric_distributions(records)
    name_occurrences = _build_name_occurrence_index(records)
    max_name_records_values = _max_name_record_values(name_occurrences)

    for record in records:
        payload["by_report"][record.report_id] = _build_report_comparator_entry(
            report_id=record.report_id,
            feature_vector=record.feature_vector,
            metrics_by_key=metrics_by_key,
            name_occurrences=name_occurrences,
            max_name_records_values=max_name_records_values,
            report_count=len(records),
        )

    return payload


def build_global_baselines_from_reports_dir(reports_dir: Path) -> dict[str, Any]:
    records = collect_report_feature_records(reports_dir)
    return build_global_baselines(records)


def build_leave_one_out_baseline(
    *,
    records: list[ReportFeatureRecord],
    target_report_id: str,
    excluded_report_ids: list[str] | None = None,
    cohort_strategy: str = "hierarchical",
) -> dict[str, Any]:
    target_id = str(target_report_id or "").strip()
    excluded = sorted(
        {
            str(report_id).strip()
            for report_id in (excluded_report_ids or [])
            if str(report_id or "").strip()
        }
    )
    payload = default_cross_hearing_loo_payload()
    payload.update(
        {
            "schema_version": LEAVE_ONE_OUT_BASELINE_SCHEMA_VERSION,
            "generated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
            "source": "leave_one_out",
            "target_report_id": target_id,
            "report_id": target_id,
            "excluded_report_ids": excluded,
            "cohort_strategy": (
                "committee_chamber_then_chamber_then_global"
                if str(cohort_strategy or "").strip().lower() == "hierarchical"
                else str(cohort_strategy or "").strip().lower() or "hierarchical"
            ),
        }
    )
    if not target_id:
        payload["reason"] = "target_report_id_required"
        return normalize_leave_one_out_baseline_payload(payload)

    record_by_id = {record.report_id: record for record in records}
    target_record = record_by_id.get(target_id)
    if target_record is None:
        payload["reason"] = "target_report_not_found"
        return normalize_leave_one_out_baseline_payload(payload)

    excluded_set = set(excluded)
    global_comparison_records = [
        record
        for record in records
        if record.report_id != target_id and record.report_id not in excluded_set
    ]
    if not global_comparison_records:
        payload["reason"] = "no_comparison_reports"
        return normalize_leave_one_out_baseline_payload(payload)

    payload.pop("reason", None)
    global_channel = _build_leave_one_out_channel_payload(
        channel="global_loo",
        label=_LEAVE_ONE_OUT_CHANNEL_LABELS["global_loo"],
        report_id=target_id,
        feature_vector=target_record.feature_vector,
        comparison_records=global_comparison_records,
        metadata={"selected_level": "global"},
    )
    cohort_records, cohort_metadata = _select_cohort_records(
        target_record=target_record,
        comparison_records=global_comparison_records,
    )
    cohort_channel = _build_leave_one_out_channel_payload(
        channel="cohort_loo",
        label=_LEAVE_ONE_OUT_CHANNEL_LABELS["cohort_loo"],
        report_id=target_id,
        feature_vector=target_record.feature_vector,
        comparison_records=cohort_records,
        metadata=cohort_metadata,
    )
    payload["channels"] = {
        "cohort_loo": cohort_channel,
        "global_loo": global_channel,
    }
    payload["selected_channel"] = _choose_selected_channel(payload["channels"])
    return normalize_leave_one_out_baseline_payload(payload)


def build_leave_one_out_baseline_from_reports_dir(
    *,
    reports_dir: Path,
    target_report_id: str,
    excluded_report_ids: list[str] | None = None,
    cohort_strategy: str = "hierarchical",
) -> dict[str, Any]:
    records = collect_report_feature_records(reports_dir)
    return build_leave_one_out_baseline(
        records=records,
        target_report_id=target_report_id,
        excluded_report_ids=excluded_report_ids,
        cohort_strategy=cohort_strategy,
    )


def write_leave_one_out_baselines_from_reports_dir(
    *,
    reports_dir: Path,
    target_report_ids: list[str] | None = None,
    excluded_report_ids: list[str] | None = None,
    cohort_strategy: str = "hierarchical",
) -> tuple[list[Path], list[dict[str, str]]]:
    """Build and write leave-one-out baselines for one or more report IDs in a single pass."""
    records = collect_report_feature_records(reports_dir)
    record_ids = {record.report_id for record in records}
    targets: list[str] = []
    seen_targets: set[str] = set()

    if target_report_ids is None:
        for record in records:
            target = str(record.report_id or "").strip()
            if not target or target in seen_targets:
                continue
            seen_targets.add(target)
            targets.append(target)
    else:
        for report_id in target_report_ids:
            target = str(report_id or "").strip()
            if not target or target in seen_targets:
                continue
            seen_targets.add(target)
            targets.append(target)

    written_paths: list[Path] = []
    failures: list[dict[str, str]] = []
    for target in targets:
        if target not in record_ids:
            failures.append(
                {
                    "report_id": target,
                    "reason": "target_report_not_found",
                }
            )
            continue
        payload = build_leave_one_out_baseline(
            records=records,
            target_report_id=target,
            excluded_report_ids=excluded_report_ids,
            cohort_strategy=cohort_strategy,
        )
        output_path = reports_dir / target / "summary" / LEAVE_ONE_OUT_BASELINE_FILENAME
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        written_paths.append(output_path)
    return written_paths, failures


def write_global_baselines(
    *,
    reports_dir: Path,
    payload: Mapping[str, Any],
    output_filename: str = GLOBAL_BASELINES_FILENAME,
) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    output_path = reports_dir / output_filename
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return output_path


def load_leave_one_out_baseline_payload(
    *,
    out_dir: Path,
) -> dict[str, Any]:
    summary_path = out_dir / "summary" / LEAVE_ONE_OUT_BASELINE_FILENAME
    payload = _read_json(summary_path)
    normalized = normalize_leave_one_out_baseline_payload(payload)
    normalized["source_path"] = str(summary_path)
    return normalized


def load_cross_hearing_baseline(
    *,
    out_dir: Path,
    report_id: str,
) -> dict[str, Any]:
    candidates = (
        out_dir / GLOBAL_BASELINES_FILENAME,
        out_dir.parent / GLOBAL_BASELINES_FILENAME,
    )
    for candidate in candidates:
        payload = _read_json(candidate)
        by_report = payload.get("by_report") if isinstance(payload, dict) else None
        if isinstance(by_report, dict):
            entry = by_report.get(report_id)
            if isinstance(entry, dict):
                merged = default_cross_hearing_baseline_payload()
                merged.update(entry)
                merged["available"] = True
                merged["report_count"] = int(payload.get("report_count") or merged["report_count"])
                merged["source_path"] = str(candidate)
                return merged
    return default_cross_hearing_baseline_payload()
