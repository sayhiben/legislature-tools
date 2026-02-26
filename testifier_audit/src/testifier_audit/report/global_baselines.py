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

_COMPARATOR_METRIC_SPECS: tuple[tuple[str, str], ...] = (
    ("total_submissions", "Total submissions"),
    ("overall_pro_rate", "Overall pro rate"),
    ("window_high_share", "High-tier window share"),
    ("window_top_score", "Top window score"),
    ("window_top_abs_z", "Top window |z|"),
    ("window_top_dup_fraction", "Top window duplicate fraction"),
    ("top_name_max_records", "Top repeated-name records"),
    ("off_hours_ratio", "Off-hours submission ratio"),
    ("dedup_drop_fraction", "Dedup drop fraction"),
)


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


def build_feature_vector(
    *,
    report_id: str,
    triage_summary: Mapping[str, Any],
    window_evidence_queue: list[dict[str, Any]],
    record_evidence_queue: list[dict[str, Any]],
    cluster_evidence_queue: list[dict[str, Any]],
    data_quality_panel: Mapping[str, Any],
) -> dict[str, Any]:
    summary = _as_dict(triage_summary)
    window_rows = _as_rows(window_evidence_queue)
    record_rows = _as_rows(record_evidence_queue)
    cluster_rows = _as_rows(cluster_evidence_queue)
    quality = _as_dict(data_quality_panel)

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

    top_name_max_records = max((row["n_records"] for row in top_name_rows), default=0)

    off_hours_summary = _as_dict(summary.get("off_hours_summary"))
    off_hours_ratio = _safe_float(off_hours_summary.get("off_hours_ratio"))

    raw_total = _first_non_null_float(
        summary.get("total_submissions_raw"),
        summary.get("total_submissions"),
    )
    dedup_total = _first_non_null_float(
        summary.get("total_submissions_exact_row_dedup"),
        summary.get("total_submissions"),
    )
    dedup_drop_fraction = None
    if raw_total is not None and raw_total > 0 and dedup_total is not None:
        dedup_drop_fraction = max((raw_total - dedup_total) / raw_total, 0.0)

    total_windows = max(len(window_rows), 1)
    window_high_count = int(
        sum(1 for row in window_rows if str(row.get("evidence_tier") or "") == "high")
    )
    window_medium_count = int(
        sum(1 for row in window_rows if str(row.get("evidence_tier") or "") == "medium")
    )
    window_watch_count = int(
        sum(1 for row in window_rows if str(row.get("evidence_tier") or "") == "watch")
    )

    metrics = {
        "total_submissions": _safe_int(summary.get("total_submissions", 0)),
        "overall_pro_rate": _safe_float(summary.get("overall_pro_rate")),
        "overall_con_rate": _safe_float(summary.get("overall_con_rate")),
        "window_queue_size": len(window_rows),
        "record_queue_size": len(record_rows),
        "cluster_queue_size": len(cluster_rows),
        "window_high_count": window_high_count,
        "window_medium_count": window_medium_count,
        "window_watch_count": window_watch_count,
        "window_high_share": window_high_count / float(total_windows),
        "window_top_score": max(window_scores) if window_scores else None,
        "window_top_abs_z": max(window_abs_z) if window_abs_z else None,
        "window_top_dup_fraction": max(window_dup_fraction) if window_dup_fraction else None,
        "window_min_q_value": min(window_q_values) if window_q_values else None,
        "top_name_max_records": top_name_max_records,
        "off_hours_ratio": off_hours_ratio,
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
        _safe_int(
            feature.get("total_submissions", summary.get("total_submissions", 0)),
            default=0,
        ),
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
        _safe_int(
            feature.get("window_queue_size", _as_dict(summary.get("queue_counts")).get("window", 0))
        ),
    )
    metrics.setdefault(
        "record_queue_size",
        _safe_int(
            feature.get("record_queue_size", _as_dict(summary.get("queue_counts")).get("record", 0))
        ),
    )
    metrics.setdefault(
        "cluster_queue_size",
        _safe_int(
            feature.get(
                "cluster_queue_size", _as_dict(summary.get("queue_counts")).get("cluster", 0)
            )
        ),
    )
    metrics.setdefault(
        "window_high_count",
        _safe_int(
            feature.get(
                "window_high_count", _as_dict(summary.get("window_tier_counts")).get("high", 0)
            )
        ),
    )

    if _safe_float(metrics.get("window_high_share")) is None:
        queue_size = _safe_float(metrics.get("window_queue_size")) or 0.0
        high_count = _safe_float(metrics.get("window_high_count")) or 0.0
        metrics["window_high_share"] = (high_count / queue_size) if queue_size > 0 else 0.0

    top_names = _normalize_name_rows(
        _as_rows(feature.get("top_repeated_names")) or _as_rows(summary.get("top_repeated_names"))
    )

    if _safe_float(metrics.get("top_name_max_records")) is None:
        metrics["top_name_max_records"] = max((row["n_records"] for row in top_names), default=0)
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
        if value is None or not distribution:
            continue
        metric_comparators.append(
            {
                "metric": key,
                "label": label,
                "value": value,
                "percentile": _percentile_rank(distribution, value),
                "band_p10": _quantile(distribution, 0.10),
                "band_p50": _quantile(distribution, 0.50),
                "band_p90": _quantile(distribution, 0.90),
                "n_reports": len(distribution),
            }
        )

    top_name_cues: list[dict[str, Any]] = []
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
            }
        )

    return {
        "available": True,
        "report_id": report_id,
        "report_count": int(report_count),
        "metric_comparators": metric_comparators,
        "top_name_cues": top_name_cues,
    }


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
) -> dict[str, Any]:
    target_id = str(target_report_id or "").strip()
    excluded = sorted(
        {
            str(report_id).strip()
            for report_id in (excluded_report_ids or [])
            if str(report_id or "").strip()
        }
    )
    payload: dict[str, Any] = {
        "schema_version": GLOBAL_BASELINES_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "available": False,
        "source": "leave_one_out",
        "target_report_id": target_id,
        "excluded_report_ids": excluded,
        "comparison_report_ids": [],
        "report_count": 0,
        "metric_comparators": [],
        "top_name_cues": [],
    }
    if not target_id:
        payload["reason"] = "target_report_id_required"
        return payload

    record_by_id = {record.report_id: record for record in records}
    target_record = record_by_id.get(target_id)
    if target_record is None:
        payload["reason"] = "target_report_not_found"
        return payload

    excluded_set = set(excluded)
    comparison_records = [
        record
        for record in records
        if record.report_id != target_id and record.report_id not in excluded_set
    ]
    payload["comparison_report_ids"] = [record.report_id for record in comparison_records]
    payload["report_count"] = int(len(comparison_records))
    if not comparison_records:
        payload["reason"] = "no_comparison_reports"
        return payload

    metrics_by_key = _build_metric_distributions(comparison_records)
    name_occurrences = _build_name_occurrence_index(comparison_records)
    max_name_records_values = _max_name_record_values(name_occurrences)
    comparator = _build_report_comparator_entry(
        report_id=target_id,
        feature_vector=target_record.feature_vector,
        metrics_by_key=metrics_by_key,
        name_occurrences=name_occurrences,
        max_name_records_values=max_name_records_values,
        report_count=len(comparison_records),
    )
    payload.update(comparator)
    payload["available"] = True
    payload.pop("reason", None)
    return payload


def build_leave_one_out_baseline_from_reports_dir(
    *,
    reports_dir: Path,
    target_report_id: str,
    excluded_report_ids: list[str] | None = None,
) -> dict[str, Any]:
    records = collect_report_feature_records(reports_dir)
    return build_leave_one_out_baseline(
        records=records,
        target_report_id=target_report_id,
        excluded_report_ids=excluded_report_ids,
    )


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
