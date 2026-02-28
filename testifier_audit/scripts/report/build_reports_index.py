#!/usr/bin/env python3
"""Generate a sortable/filterable reports index page for GitHub Pages."""

from __future__ import annotations

import json
import math
import re
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

PACIFIC_TZ = ZoneInfo("America/Los_Angeles")

# Keep this list aligned with the baseline builder metric set.
_BASELINE_METRIC_SPECS: tuple[tuple[str, str], ...] = (
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
_VALID_SUPPORT_TIERS = {"unavailable", "descriptive_only", "supported"}


@dataclass(frozen=True)
class ReportEntry:
    """Metadata for one rendered report directory."""

    report_id: str
    report_href: str
    report_label: str
    bill_description: str
    meeting_local: str
    meeting_epoch: int | None
    generated_local: str
    generated_epoch: int
    total_testifiers: int | None
    pro_pct: float | None
    con_pct: float | None
    duplicate_name_pct: float | None
    duplicate_name_pct_exact: float | None
    duplicate_name_pct_loose: float | None
    duplicate_rows_exact: int | None
    duplicate_rows_loose: int | None
    duplicate_rows_total: int | None
    voter_match_pct_exact: float | None
    voter_match_pct_loose: float | None
    voter_match_rows_matched_exact: int | None
    voter_match_rows_matched_loose: int | None
    voter_match_rows_total_exact: int | None
    voter_match_rows_total_loose: int | None
    status: str


def project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _load_json_file(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _parse_scalar(value: str) -> Any:
    text = value.strip()
    if text == "":
        return ""
    lowered = text.lower()
    if lowered in {"null", "none", "~"}:
        return None
    if (text.startswith('"') and text.endswith('"')) or (
        text.startswith("'") and text.endswith("'")
    ):
        return text[1:-1]
    try:
        if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
            return int(text)
        return float(text)
    except ValueError:
        return text


def _parse_sidecar_subset(path: Path) -> dict[str, Any]:
    """Parse a constrained subset of the sidecar YAML without third-party deps.

    This parser is intentionally minimal and targets the generated sidecar shape.
    """

    out: dict[str, Any] = {}
    current_section: str | None = None
    current_key: str | None = None

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return out

    for raw_line in lines:
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        stripped = raw_line.strip()

        if indent == 0 and stripped.endswith(":"):
            section_name = stripped[:-1]
            if section_name in {"stats", "source"}:
                current_section = section_name
                out.setdefault(section_name, {})
                current_key = None
            else:
                current_section = None
                current_key = None
            continue

        if indent == 0 and ":" in stripped:
            key, raw_value = stripped.split(":", 1)
            out[key.strip()] = _parse_scalar(raw_value)
            current_section = None
            current_key = None
            continue

        if current_section is None or current_section not in out:
            continue

        section_obj = out.get(current_section)
        if not isinstance(section_obj, dict):
            continue

        if indent >= 2 and ":" in stripped:
            key, raw_value = stripped.split(":", 1)
            parsed_value = _parse_scalar(raw_value)
            key_name = key.strip()
            section_obj[key_name] = parsed_value
            current_key = key_name
            continue

        if indent >= 2 and current_key is not None:
            existing_value = section_obj.get(current_key)
            if isinstance(existing_value, str):
                section_obj[current_key] = f"{existing_value} {stripped}".strip()

    return out


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            return int(value)
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        candidate = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(candidate):
        return None
    return candidate


def _coerce_bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        numeric = _coerce_float(value)
        if numeric is None:
            return default
        return numeric != 0.0
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    return default


def _clean_optional_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=PACIFIC_TZ)
    return parsed


def _format_us_datetime_pacific(value: datetime | None) -> str:
    if value is None:
        return "—"
    pacific = value.astimezone(PACIFIC_TZ)
    month = pacific.strftime("%b")
    day = pacific.day
    hour = pacific.strftime("%I").lstrip("0") or "0"
    minute = pacific.strftime("%M")
    am_pm = pacific.strftime("%p")
    tz_abbr = pacific.strftime("%Z")
    return f"{month} {day}, {pacific.year} {hour}:{minute} {am_pm} {tz_abbr}"


def _format_epoch_pacific(epoch_seconds: int) -> str:
    dt_utc = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
    return _format_us_datetime_pacific(dt_utc)


def _clean_text(value: Any, *, fallback: str = "—") -> str:
    if isinstance(value, str):
        text = value.strip()
        return text if text else fallback
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


def _canonical_match_mode(raw_value: Any) -> str:
    text = _clean_optional_text(raw_value).lower()
    if text in {"strict", "exact"}:
        return "strict"
    if text in {"loose", "fuzzy"}:
        return "loose"
    return ""


def _dedupe_bill_label_from_description(report_label: str, bill_description: str) -> str:
    description = _clean_optional_text(bill_description)
    label = _clean_optional_text(report_label)
    if not description or not label:
        return description or bill_description

    label_tokens = re.findall(r"[A-Za-z0-9]+", label)
    if not label_tokens:
        return description
    prefix_pattern = r"^\s*" + r"[\s:;\-–—]*".join(re.escape(token) for token in label_tokens)
    prefix_pattern += r"[\s:;\-–—]*"
    stripped = re.sub(prefix_pattern, "", description, flags=re.IGNORECASE)
    stripped = stripped.strip()
    return stripped if stripped else description


def _humanize_identifier(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    tokens = [token for token in text.replace("-", "_").split("_") if token]
    return " ".join(tokens)


def _humanize_metric_id(metric_id: str) -> str:
    known = {key: label for key, label in _BASELINE_METRIC_SPECS}
    if metric_id in known:
        return known[metric_id]
    humanized = _humanize_identifier(metric_id)
    return humanized[:1].upper() + humanized[1:] if humanized else metric_id


def _support_tier_from_n_reports(n_reports: int) -> str:
    if int(n_reports) < 10:
        return "unavailable"
    if int(n_reports) < 20:
        return "descriptive_only"
    return "supported"


def _normalize_support_tier(raw_value: Any, n_reports: int) -> str:
    raw_text = _clean_optional_text(raw_value).lower()
    if raw_text in _VALID_SUPPORT_TIERS:
        return raw_text
    return _support_tier_from_n_reports(n_reports)


def _normalize_baseline_comparator_row(
    *,
    report_id: str,
    raw_row: Any,
) -> dict[str, Any] | None:
    if not isinstance(raw_row, dict):
        return None
    metric = _clean_optional_text(raw_row.get("metric"))
    if not metric:
        return None

    observed = _coerce_float(raw_row.get("observed"))
    if observed is None:
        observed = _coerce_float(raw_row.get("value"))

    expected = _coerce_float(raw_row.get("expected"))
    if expected is None:
        expected = _coerce_float(raw_row.get("band_p50"))

    delta = _coerce_float(raw_row.get("delta"))
    if delta is None and observed is not None and expected is not None:
        delta = observed - expected

    percentile = _coerce_float(raw_row.get("percentile"))
    if percentile is not None:
        percentile = max(0.0, min(1.0, percentile))

    n_reports = _coerce_int(raw_row.get("n_reports"))
    if n_reports is None or n_reports < 0:
        n_reports = 0

    support_tier = _normalize_support_tier(raw_row.get("support_tier"), n_reports)

    if "descriptive_only" in raw_row:
        descriptive_only = _coerce_bool(raw_row.get("descriptive_only"), default=False)
    else:
        descriptive_only = support_tier == "descriptive_only"

    if "low_power" in raw_row:
        low_power = _coerce_bool(raw_row.get("low_power"), default=support_tier != "supported")
    else:
        low_power = support_tier != "supported"

    band_p50 = _coerce_float(raw_row.get("band_p50"))

    return {
        "report_id": report_id,
        "metric": metric,
        "label": _clean_optional_text(raw_row.get("label")) or _humanize_metric_id(metric),
        "observed": observed,
        "expected": expected,
        "delta": delta,
        "percentile": percentile,
        "band_p10": _coerce_float(raw_row.get("band_p10")),
        "band_p50": band_p50 if band_p50 is not None else expected,
        "band_p90": _coerce_float(raw_row.get("band_p90")),
        "robust_z": _coerce_float(raw_row.get("robust_z")),
        "empirical_tail_p_two_sided": _coerce_float(raw_row.get("empirical_tail_p_two_sided")),
        "n_reports": int(n_reports),
        "support_tier": support_tier,
        "descriptive_only": bool(descriptive_only),
        "low_power": bool(low_power),
    }


def _normalize_baseline_top_name_row(
    *,
    report_id: str,
    raw_row: Any,
    corpus_report_count: int,
) -> dict[str, Any] | None:
    if not isinstance(raw_row, dict):
        return None

    canonical_name = _clean_optional_text(raw_row.get("canonical_name"))
    if not canonical_name:
        return None

    current_n_records = _coerce_int(raw_row.get("current_n_records"))
    if current_n_records is None:
        current_n_records = _coerce_int(raw_row.get("n_records"))
    if current_n_records is None or current_n_records < 0:
        current_n_records = 0

    max_n_records_across_reports = _coerce_int(raw_row.get("max_n_records_across_reports"))
    if max_n_records_across_reports is None or max_n_records_across_reports < current_n_records:
        max_n_records_across_reports = current_n_records

    report_count = _coerce_int(raw_row.get("report_count"))
    if report_count is None or report_count < 0:
        report_count = 0

    report_share = _coerce_float(raw_row.get("report_share"))
    if report_share is None and corpus_report_count > 0:
        report_share = report_count / float(corpus_report_count)

    max_percentile = _coerce_float(raw_row.get("max_n_records_percentile"))
    if max_percentile is not None:
        max_percentile = max(0.0, min(1.0, max_percentile))

    return {
        "report_id": report_id,
        "canonical_name": canonical_name,
        "display_name": _clean_optional_text(raw_row.get("display_name")) or canonical_name,
        "current_n_records": int(current_n_records),
        "report_count": int(report_count),
        "report_share": report_share,
        "max_n_records_across_reports": int(max_n_records_across_reports),
        "max_n_records_percentile": max_percentile,
    }


def _build_metric_catalog(
    *,
    comparator_rows: list[dict[str, Any]],
    indexed_report_count: int,
) -> list[dict[str, Any]]:
    known_order = [metric for metric, _label in _BASELINE_METRIC_SPECS]
    known_label_map = {metric: label for metric, label in _BASELINE_METRIC_SPECS}

    metric_index: dict[str, dict[str, Any]] = {
        metric: {
            "metric": metric,
            "label": label,
            "report_ids": set(),
        }
        for metric, label in _BASELINE_METRIC_SPECS
    }

    for row in comparator_rows:
        metric = _clean_optional_text(row.get("metric"))
        if not metric:
            continue
        entry = metric_index.setdefault(
            metric,
            {
                "metric": metric,
                "label": _clean_optional_text(row.get("label")) or _humanize_metric_id(metric),
                "report_ids": set(),
            },
        )
        if _clean_optional_text(row.get("label")):
            entry["label"] = _clean_optional_text(row.get("label"))
        report_id = _clean_optional_text(row.get("report_id"))
        if report_id:
            entry["report_ids"].add(report_id)

    indexed = max(int(indexed_report_count), 0)

    ordered_metrics: list[str] = []
    ordered_metrics.extend(known_order)
    extra_metrics = sorted(metric for metric in metric_index.keys() if metric not in known_label_map)
    ordered_metrics.extend(extra_metrics)

    out: list[dict[str, Any]] = []
    for metric in ordered_metrics:
        entry = metric_index.get(metric)
        if not entry:
            continue
        report_count = len(entry.get("report_ids", set()))
        coverage_pct = (report_count / float(indexed)) if indexed > 0 else 0.0
        out.append(
            {
                "metric": metric,
                "label": _clean_optional_text(entry.get("label")) or _humanize_metric_id(metric),
                "report_count": report_count,
                "missing_report_count": max(indexed - report_count, 0),
                "available": report_count > 0,
                "coverage_pct": coverage_pct,
            }
        )
    return out


def _aggregate_top_name_rows(
    *,
    top_name_rows: list[dict[str, Any]],
    indexed_report_count: int,
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in top_name_rows:
        canonical_name = _clean_optional_text(row.get("canonical_name"))
        if not canonical_name:
            continue

        entry = grouped.setdefault(
            canonical_name,
            {
                "canonical_name": canonical_name,
                "display_name": _clean_optional_text(row.get("display_name")) or canonical_name,
                "report_ids": set(),
                "report_count": 0,
                "total_n_records_across_reports": 0,
                "max_n_records_across_reports": 0,
                "max_current_n_records": 0,
                "max_n_records_percentile": None,
                "max_report_share": 0.0,
            },
        )

        report_id = _clean_optional_text(row.get("report_id"))
        if report_id:
            entry["report_ids"].add(report_id)

        report_count = _coerce_int(row.get("report_count"))
        if report_count is not None and report_count > entry["report_count"]:
            entry["report_count"] = report_count

        max_across = _coerce_int(row.get("max_n_records_across_reports"))
        if max_across is not None and max_across > entry["max_n_records_across_reports"]:
            entry["max_n_records_across_reports"] = max_across

        current_n = _coerce_int(row.get("current_n_records"))
        if current_n is not None:
            if current_n > entry["max_current_n_records"]:
                entry["max_current_n_records"] = current_n
            entry["total_n_records_across_reports"] += max(0, current_n)

        percentile = _coerce_float(row.get("max_n_records_percentile"))
        current_percentile = _coerce_float(entry.get("max_n_records_percentile"))
        if percentile is not None and (current_percentile is None or percentile > current_percentile):
            entry["max_n_records_percentile"] = percentile

        report_share = _coerce_float(row.get("report_share"))
        if report_share is not None and report_share > entry["max_report_share"]:
            entry["max_report_share"] = report_share

    indexed = max(int(indexed_report_count), 0)
    out: list[dict[str, Any]] = []
    for entry in grouped.values():
        appearances = len(entry["report_ids"])
        report_count = int(max(entry["report_count"], appearances))
        report_share = entry["max_report_share"]
        if (report_share is None or report_share <= 0.0) and indexed > 0:
            report_share = report_count / float(indexed)

        out.append(
            {
                "canonical_name": entry["canonical_name"],
                "display_name": entry["display_name"],
                "appearance_report_count": appearances,
                "report_count": report_count,
                "report_share": report_share,
                "total_n_records_across_reports": int(entry["total_n_records_across_reports"]),
                "max_n_records_across_reports": int(entry["max_n_records_across_reports"]),
                "max_current_n_records": int(entry["max_current_n_records"]),
                "max_n_records_percentile": _coerce_float(entry.get("max_n_records_percentile")),
            }
        )

    out.sort(
        key=lambda row: (
            -int(row.get("report_count") or 0),
            -int(row.get("total_n_records_across_reports") or 0),
            -int(row.get("max_n_records_across_reports") or 0),
            str(row.get("canonical_name") or ""),
        )
    )
    return out


def _default_baseline_atlas_payload(*, source_path: Path, reason: str) -> dict[str, Any]:
    return {
        "available": False,
        "reason": reason,
        "source_path": str(source_path),
        "schema_version": None,
        "generated_at_utc": "",
        "report_count": 0,
        "reports": [],
        "metric_catalog": [
            {
                "metric": metric,
                "label": label,
                "report_count": 0,
                "missing_report_count": 0,
                "available": False,
                "coverage_pct": 0.0,
            }
            for metric, label in _BASELINE_METRIC_SPECS
        ],
        "comparator_rows": [],
        "top_name_rows": [],
        "top_name_aggregates": [],
        "by_report": {},
        "summary": {
            "indexed_report_count": 0,
            "metrics_available": 0,
            "median_support_n_reports": None,
            "reports_with_top_name_cues": 0,
        },
    }


def build_baseline_atlas_payload(reports_dir: Path) -> dict[str, Any]:
    baseline_path = reports_dir / "global_baselines.json"
    if not baseline_path.exists():
        return _default_baseline_atlas_payload(
            source_path=baseline_path,
            reason="global_baselines_missing",
        )

    payload = _load_json_file(baseline_path)
    if not payload:
        return _default_baseline_atlas_payload(
            source_path=baseline_path,
            reason="global_baselines_invalid",
        )

    by_report_raw = payload.get("by_report")
    if not isinstance(by_report_raw, dict):
        return _default_baseline_atlas_payload(
            source_path=baseline_path,
            reason="global_baselines_invalid",
        )

    report_ids = sorted(
        str(report_id).strip()
        for report_id in by_report_raw.keys()
        if str(report_id).strip()
    )

    indexed_report_count = _coerce_int(payload.get("report_count"))
    if indexed_report_count is None or indexed_report_count <= 0:
        indexed_report_count = len(report_ids)

    comparator_rows: list[dict[str, Any]] = []
    top_name_rows: list[dict[str, Any]] = []
    by_report: dict[str, dict[str, Any]] = {}

    for report_id in report_ids:
        entry = by_report_raw.get(report_id)
        entry_map = entry if isinstance(entry, dict) else {}

        raw_comparators = entry_map.get("metric_comparators")
        raw_top_names = entry_map.get("top_name_cues")

        report_comparators: list[dict[str, Any]] = []
        for row in raw_comparators if isinstance(raw_comparators, list) else []:
            normalized = _normalize_baseline_comparator_row(report_id=report_id, raw_row=row)
            if normalized is None:
                continue
            report_comparators.append(normalized)
            comparator_rows.append(normalized)

        report_top_names: list[dict[str, Any]] = []
        for row in raw_top_names if isinstance(raw_top_names, list) else []:
            normalized = _normalize_baseline_top_name_row(
                report_id=report_id,
                raw_row=row,
                corpus_report_count=indexed_report_count,
            )
            if normalized is None:
                continue
            report_top_names.append(normalized)
            top_name_rows.append(normalized)

        by_report[report_id] = {
            "report_id": report_id,
            "available": bool(report_comparators or report_top_names),
            "metric_comparators": report_comparators,
            "top_name_cues": report_top_names,
        }

    metric_catalog = _build_metric_catalog(
        comparator_rows=comparator_rows,
        indexed_report_count=indexed_report_count,
    )
    top_name_aggregates = _aggregate_top_name_rows(
        top_name_rows=top_name_rows,
        indexed_report_count=indexed_report_count,
    )

    support_values = [
        int(row["n_reports"])
        for row in comparator_rows
        if _coerce_int(row.get("n_reports")) is not None
    ]
    median_support = (
        int(statistics.median(support_values))
        if support_values
        else None
    )

    reports_with_top_name_cues = len(
        {
            _clean_optional_text(row.get("report_id"))
            for row in top_name_rows
            if _clean_optional_text(row.get("report_id"))
        }
    )

    summary = {
        "indexed_report_count": int(indexed_report_count),
        "metrics_available": len([row for row in metric_catalog if bool(row.get("available"))]),
        "median_support_n_reports": median_support,
        "reports_with_top_name_cues": reports_with_top_name_cues,
    }

    return {
        "available": bool(comparator_rows or top_name_rows),
        "reason": "" if (comparator_rows or top_name_rows) else "global_baselines_empty",
        "source_path": str(baseline_path),
        "schema_version": _coerce_int(payload.get("schema_version")),
        "generated_at_utc": _clean_optional_text(payload.get("generated_at_utc")),
        "report_count": int(indexed_report_count),
        "reports": [
            str(report_id).strip()
            for report_id in payload.get("reports", [])
            if str(report_id).strip()
        ]
        if isinstance(payload.get("reports"), list)
        else report_ids,
        "metric_catalog": metric_catalog,
        "comparator_rows": comparator_rows,
        "top_name_rows": top_name_rows,
        "top_name_aggregates": top_name_aggregates,
        "by_report": by_report,
        "summary": summary,
    }


def _load_sidecar_data(
    *,
    report_id: str,
    reports_dir: Path,
    repo_root: Path,
) -> dict[str, Any]:
    default_path = repo_root / "data" / "metadata" / f"{report_id}.hearing.yaml"
    if default_path.exists():
        return _parse_sidecar_subset(default_path)

    report_data_index = reports_dir / report_id / "report_data" / "index.json"
    index_payload = _load_json_file(report_data_index)
    hearing_panel = (
        index_payload.get("interactive_charts", {}) if isinstance(index_payload, dict) else {}
    )
    if not isinstance(hearing_panel, dict):
        return {}
    panel = hearing_panel.get("hearing_context_panel", {})
    if not isinstance(panel, dict):
        return {}
    source_path_raw = panel.get("source_path")
    if not isinstance(source_path_raw, str) or not source_path_raw.strip():
        return {}
    source_path = Path(source_path_raw)
    if not source_path.exists():
        return {}
    return _parse_sidecar_subset(source_path)


def _load_summary_fallback(report_dir: Path) -> dict[str, Any]:
    summary_path = report_dir / "summary" / "investigation_summary.json"
    summary_payload = _load_json_file(summary_path)
    if summary_payload:
        return summary_payload
    feature_path = report_dir / "summary" / "feature_vector.json"
    feature_payload = _load_json_file(feature_path)
    metrics = feature_payload.get("metrics") if isinstance(feature_payload, dict) else {}
    if isinstance(metrics, dict):
        fallback: dict[str, Any] = {
            "total_submissions": metrics.get("total_submissions"),
            "overall_pro_rate": metrics.get("overall_pro_rate"),
            "overall_con_rate": metrics.get("overall_con_rate"),
            "dedup_drop_fraction": metrics.get("dedup_drop_fraction"),
        }
        return fallback
    return {}


def _load_report_data_index(report_dir: Path) -> dict[str, Any]:
    index_path = report_dir / "report_data" / "index.json"
    return _load_json_file(index_path)


def _extract_voter_match_metrics(index_payload: dict[str, Any]) -> dict[str, Any]:
    detector_summaries = (
        index_payload.get("detector_summaries", {}) if isinstance(index_payload, dict) else {}
    )
    table_previews = (
        index_payload.get("table_previews", {}) if isinstance(index_payload, dict) else {}
    )
    if not isinstance(detector_summaries, dict):
        detector_summaries = {}
    if not isinstance(table_previews, dict):
        table_previews = {}

    by_mode: dict[str, dict[str, Any]] = {}

    voter_preview = table_previews.get("voter_registry_match", {})
    if not isinstance(voter_preview, dict):
        voter_preview = {}
    linkage_overview = voter_preview.get("linkage_overview")
    for row in linkage_overview if isinstance(linkage_overview, list) else []:
        if not isinstance(row, dict):
            continue
        mode = _canonical_match_mode(row.get("match_mode") or row.get("primary_match_mode"))
        if not mode:
            continue

        n_rows = _coerce_int(row.get("n_rows"))
        if n_rows is None or n_rows < 0:
            n_rows = None

        matched_unique = _coerce_int(row.get("n_matched_unique_rows"))
        matched_ambiguous = _coerce_int(row.get("n_matched_ambiguous_rows"))
        matched_rows: int | None = None
        if matched_unique is not None or matched_ambiguous is not None:
            matched_rows = max(0, int(matched_unique or 0) + int(matched_ambiguous or 0))

        rate = _coerce_float(row.get("matched_rate_rows"))
        if rate is None and matched_rows is not None and n_rows is not None and n_rows > 0:
            rate = matched_rows / float(n_rows)

        current = by_mode.get(mode)
        current_n = _coerce_int(current.get("n_rows")) if isinstance(current, dict) else None
        if current is None or (n_rows is not None and (current_n is None or n_rows >= current_n)):
            by_mode[mode] = {
                "match_pct": (rate * 100.0) if rate is not None else None,
                "n_rows": n_rows,
                "n_matched_rows": matched_rows,
            }

    voter_summary = detector_summaries.get("voter_registry_match", {})
    if isinstance(voter_summary, dict):
        primary_mode = _canonical_match_mode(
            voter_summary.get("primary_match_mode") or voter_summary.get("match_mode_default")
        )
        if primary_mode and primary_mode not in by_mode:
            n_rows = _coerce_int(voter_summary.get("n_rows"))
            if n_rows is None or n_rows < 0:
                n_rows = None
            matched_unique = _coerce_int(voter_summary.get("n_matched_unique_rows"))
            matched_ambiguous = _coerce_int(voter_summary.get("n_matched_ambiguous_rows"))
            matched_rows: int | None = None
            if matched_unique is not None or matched_ambiguous is not None:
                matched_rows = max(0, int(matched_unique or 0) + int(matched_ambiguous or 0))
            rate = _coerce_float(voter_summary.get("matched_rate_rows"))
            if rate is None and matched_rows is not None and n_rows is not None and n_rows > 0:
                rate = matched_rows / float(n_rows)
            by_mode[primary_mode] = {
                "match_pct": (rate * 100.0) if rate is not None else None,
                "n_rows": n_rows,
                "n_matched_rows": matched_rows,
            }

    strict_entry = by_mode.get("strict", {})
    loose_entry = by_mode.get("loose", {})

    return {
        "voter_match_pct_exact": _coerce_float(strict_entry.get("match_pct")),
        "voter_match_pct_loose": _coerce_float(loose_entry.get("match_pct")),
        "voter_match_rows_matched_exact": _coerce_int(strict_entry.get("n_matched_rows")),
        "voter_match_rows_matched_loose": _coerce_int(loose_entry.get("n_matched_rows")),
        "voter_match_rows_total_exact": _coerce_int(strict_entry.get("n_rows")),
        "voter_match_rows_total_loose": _coerce_int(loose_entry.get("n_rows")),
    }


def _extract_duplicate_name_metrics(index_payload: dict[str, Any]) -> dict[str, Any]:
    detector_summaries = (
        index_payload.get("detector_summaries", {}) if isinstance(index_payload, dict) else {}
    )
    table_previews = (
        index_payload.get("table_previews", {}) if isinstance(index_payload, dict) else {}
    )
    if not isinstance(detector_summaries, dict):
        detector_summaries = {}
    if not isinstance(table_previews, dict):
        table_previews = {}

    duplicates_summary = detector_summaries.get("duplicates_exact", {})
    if not isinstance(duplicates_summary, dict):
        duplicates_summary = {}

    strict_rate = _coerce_float(duplicates_summary.get("duplicate_row_rate"))
    n_records = _coerce_int(duplicates_summary.get("n_records"))
    if n_records is None or n_records < 0:
        n_records = _coerce_int(duplicates_summary.get("n_used"))
    if n_records is None or n_records <= 0:
        n_records = None

    strict_rows = _coerce_int(duplicates_summary.get("duplicate_rows"))
    if strict_rows is None and strict_rate is not None and n_records is not None:
        strict_rows = int(round(strict_rate * float(n_records)))

    duplicates_preview = table_previews.get("duplicates_exact", {})
    if not isinstance(duplicates_preview, dict):
        duplicates_preview = {}
    per_name_by_mode = duplicates_preview.get("per_name_duplicates_by_mode")
    mode_row_totals: dict[str, int] = {"strict": 0, "loose": 0}
    has_per_name_mode_rows = isinstance(per_name_by_mode, list)
    if has_per_name_mode_rows:
        for row in per_name_by_mode:
            if not isinstance(row, dict):
                continue
            mode = _canonical_match_mode(row.get("match_mode"))
            if mode not in mode_row_totals:
                continue
            repeated_rows = _coerce_int(row.get("total_repeated_rows"))
            if repeated_rows is None or repeated_rows < 0:
                continue
            mode_row_totals[mode] += int(repeated_rows)

    if strict_rows is None and has_per_name_mode_rows:
        strict_rows = mode_row_totals["strict"]
    loose_rows = mode_row_totals["loose"] if has_per_name_mode_rows else None

    loose_rate = (
        (float(loose_rows) / float(n_records))
        if loose_rows is not None and n_records is not None and n_records > 0
        else None
    )
    if strict_rate is None and strict_rows is not None and n_records is not None and n_records > 0:
        strict_rate = float(strict_rows) / float(n_records)

    return {
        "duplicate_name_pct_exact": (strict_rate * 100.0) if strict_rate is not None else None,
        "duplicate_name_pct_loose": (loose_rate * 100.0) if loose_rate is not None else None,
        "duplicate_rows_exact": strict_rows,
        "duplicate_rows_loose": loose_rows,
        "duplicate_rows_total": n_records,
    }


def _build_entry(
    *,
    report_dir: Path,
    reports_dir: Path,
    repo_root: Path,
) -> ReportEntry | None:
    report_html = report_dir / "report.html"
    if not report_html.exists():
        return None

    generated_epoch = int(report_html.stat().st_mtime)
    generated_local = _format_epoch_pacific(generated_epoch)

    sidecar = _load_sidecar_data(
        report_id=report_dir.name,
        reports_dir=reports_dir,
        repo_root=repo_root,
    )
    stats = sidecar.get("stats", {})
    source = sidecar.get("source", {})
    if not isinstance(stats, dict):
        stats = {}
    if not isinstance(source, dict):
        source = {}

    report_label = _clean_text(source.get("short_bill_id"), fallback=report_dir.name)
    bill_description_raw = _clean_text(
        source.get("agenda_item_description") or source.get("bill_title"),
        fallback="—",
    )
    bill_description = _dedupe_bill_label_from_description(report_label, bill_description_raw)

    meeting_dt = _parse_iso_datetime(sidecar.get("meeting_start"))
    meeting_local = _format_us_datetime_pacific(meeting_dt)
    meeting_epoch = int(meeting_dt.timestamp()) if meeting_dt is not None else None

    total_testifiers = _coerce_int(stats.get("total_rows"))
    pro_pct = _coerce_float(stats.get("total_pro_pct"))
    con_pct = _coerce_float(stats.get("total_con_pct"))

    if (pro_pct is None or con_pct is None) and total_testifiers is not None and total_testifiers > 0:
        total_pro = _coerce_float(stats.get("total_pro"))
        total_con = _coerce_float(stats.get("total_con"))
        if pro_pct is None and total_pro is not None:
            pro_pct = (total_pro / float(total_testifiers)) * 100.0
        if con_pct is None and total_con is not None:
            con_pct = (total_con / float(total_testifiers)) * 100.0

    summary: dict[str, Any] | None = None

    if total_testifiers is None or pro_pct is None or con_pct is None:
        summary = _load_summary_fallback(report_dir)
        if total_testifiers is None:
            total_testifiers = _coerce_int(summary.get("total_submissions"))
        if pro_pct is None:
            pro_rate = _coerce_float(summary.get("overall_pro_rate"))
            if pro_rate is not None:
                pro_pct = pro_rate * 100.0
        if con_pct is None:
            con_rate = _coerce_float(summary.get("overall_con_rate"))
            if con_rate is not None:
                con_pct = con_rate * 100.0

    report_index_payload = _load_report_data_index(report_dir)
    duplicate_metrics = _extract_duplicate_name_metrics(report_index_payload)
    voter_metrics = _extract_voter_match_metrics(report_index_payload)

    duplicate_name_pct_exact = _coerce_float(duplicate_metrics.get("duplicate_name_pct_exact"))
    duplicate_name_pct_loose = _coerce_float(duplicate_metrics.get("duplicate_name_pct_loose"))
    duplicate_rows_exact = _coerce_int(duplicate_metrics.get("duplicate_rows_exact"))
    duplicate_rows_loose = _coerce_int(duplicate_metrics.get("duplicate_rows_loose"))
    duplicate_rows_total = _coerce_int(duplicate_metrics.get("duplicate_rows_total"))

    duplicate_name_pct = duplicate_name_pct_exact
    if duplicate_name_pct is None:
        if summary is None:
            summary = _load_summary_fallback(report_dir)
        dedup_drop_fraction = _coerce_float(summary.get("dedup_drop_fraction"))
        if dedup_drop_fraction is not None:
            duplicate_name_pct = dedup_drop_fraction * 100.0
    if duplicate_name_pct_exact is None:
        duplicate_name_pct_exact = duplicate_name_pct

    voter_match_pct_exact = _coerce_float(voter_metrics.get("voter_match_pct_exact"))
    voter_match_pct_loose = _coerce_float(voter_metrics.get("voter_match_pct_loose"))
    voter_match_rows_matched_exact = _coerce_int(voter_metrics.get("voter_match_rows_matched_exact"))
    voter_match_rows_matched_loose = _coerce_int(voter_metrics.get("voter_match_rows_matched_loose"))
    voter_match_rows_total_exact = _coerce_int(voter_metrics.get("voter_match_rows_total_exact"))
    voter_match_rows_total_loose = _coerce_int(voter_metrics.get("voter_match_rows_total_loose"))

    status = "open" if (meeting_epoch is not None and generated_epoch < meeting_epoch) else "closed"

    return ReportEntry(
        report_id=report_dir.name,
        report_href=f"./{report_dir.name}/report.html",
        report_label=report_label,
        bill_description=bill_description,
        meeting_local=meeting_local,
        meeting_epoch=meeting_epoch,
        generated_local=generated_local,
        generated_epoch=generated_epoch,
        total_testifiers=total_testifiers,
        pro_pct=pro_pct,
        con_pct=con_pct,
        duplicate_name_pct=duplicate_name_pct,
        duplicate_name_pct_exact=duplicate_name_pct_exact,
        duplicate_name_pct_loose=duplicate_name_pct_loose,
        duplicate_rows_exact=duplicate_rows_exact,
        duplicate_rows_loose=duplicate_rows_loose,
        duplicate_rows_total=duplicate_rows_total,
        voter_match_pct_exact=voter_match_pct_exact,
        voter_match_pct_loose=voter_match_pct_loose,
        voter_match_rows_matched_exact=voter_match_rows_matched_exact,
        voter_match_rows_matched_loose=voter_match_rows_matched_loose,
        voter_match_rows_total_exact=voter_match_rows_total_exact,
        voter_match_rows_total_loose=voter_match_rows_total_loose,
        status=status,
    )


def collect_entries(reports_dir: Path, repo_root: Path) -> list[ReportEntry]:
    entries: list[ReportEntry] = []
    for report_dir in sorted(
        (path for path in reports_dir.iterdir() if path.is_dir()),
        reverse=True,
    ):
        entry = _build_entry(report_dir=report_dir, reports_dir=reports_dir, repo_root=repo_root)
        if entry is not None:
            entries.append(entry)
    return entries


def _json_for_inline_script(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")


def render_index(
    entries: list[ReportEntry],
    generated_at_local: str,
    baseline_atlas: dict[str, Any],
) -> str:
    table_rows = [
        {
            "report_id": entry.report_id,
            "report_href": entry.report_href,
            "report_label": entry.report_label,
            "bill_description": entry.bill_description,
            "meeting_local": entry.meeting_local,
            "meeting_epoch": entry.meeting_epoch,
            "generated_local": entry.generated_local,
            "generated_epoch": entry.generated_epoch,
            "total_testifiers": entry.total_testifiers,
            "pro_pct": entry.pro_pct,
            "con_pct": entry.con_pct,
            "duplicate_name_pct": entry.duplicate_name_pct,
            "duplicate_name_pct_exact": entry.duplicate_name_pct_exact,
            "duplicate_name_pct_loose": entry.duplicate_name_pct_loose,
            "duplicate_rows_exact": entry.duplicate_rows_exact,
            "duplicate_rows_loose": entry.duplicate_rows_loose,
            "duplicate_rows_total": entry.duplicate_rows_total,
            "voter_match_pct_exact": entry.voter_match_pct_exact,
            "voter_match_pct_loose": entry.voter_match_pct_loose,
            "voter_match_rows_matched_exact": entry.voter_match_rows_matched_exact,
            "voter_match_rows_matched_loose": entry.voter_match_rows_matched_loose,
            "voter_match_rows_total_exact": entry.voter_match_rows_total_exact,
            "voter_match_rows_total_loose": entry.voter_match_rows_total_loose,
            "status": entry.status,
        }
        for entry in entries
    ]
    table_data_json = _json_for_inline_script(table_rows)
    baseline_data_json = _json_for_inline_script(baseline_atlas)

    if not entries:
        table_markup = '<p class="empty">No rendered reports found yet.</p>'
    else:
        table_markup = (
            '<section class="controls">'
            '  <label for="report-search">Global filter</label>'
            '  <input id="report-search" class="search-input" type="search" '
            '    placeholder="Filter by bill, description, or date/time">'
            '  <p class="helper">Tip: headers are sortable and each column has its own filter input.</p>'
            '</section>'
            '<section class="table-shell">'
            '  <div id="reports-table"></div>'
            '  <p id="table-stats" class="stats"></p>'
            '</section>'
        )

    baseline_markup = (
        '<section id="baseline-atlas" class="baseline-shell">'
        '  <div class="baseline-header">'
        '    <h2>Baseline Atlas</h2>'
        '    <p class="helper">Cross-hearing baseline context from <code>reports/global_baselines.json</code>.</p>'
        '  </div>'
        '  <p id="baseline-atlas-warning" class="baseline-warning hidden"></p>'
        '  <section id="baseline-summary-cards" class="baseline-summary-cards"></section>'
        '  <section class="baseline-panel baseline-panel-wide">'
        '    <h3>Outlier Leaderboard</h3>'
        '    <p class="helper">Outlier rule: <code>|robust_z| &gt;= 3</code>; otherwise percentile tails only when robust z is unavailable. Tables are grouped by metric.</p>'
        '    <div id="baseline-outlier-sections" class="mini-table-host outlier-sections-grid"></div>'
        '  </section>'
        '  <section class="baseline-panel baseline-panel-wide">'
        '    <h3>Top Repeated Names Across Hearings</h3>'
        '    <div id="baseline-top-names-controls" class="top-names-metric-grid">'
        '      <section class="top-names-metric-panel">'
        '        <h4>By # Reports</h4>'
        '        <div id="baseline-top-names-report-count-chart" class="chart-host chart-host-short" aria-label="Top repeated names by reports chart"></div>'
        '        <div id="baseline-top-names-report-count-table" class="mini-table-host"></div>'
        '        <div id="baseline-top-names-report-count-controls" class="mini-pager"></div>'
        '      </section>'
        '      <section class="top-names-metric-panel">'
        '        <h4>By Total Sign-ins</h4>'
        '        <div id="baseline-top-names-total-signins-chart" class="chart-host chart-host-short" aria-label="Top repeated names by total sign-ins chart"></div>'
        '        <div id="baseline-top-names-total-signins-table" class="mini-table-host"></div>'
        '        <div id="baseline-top-names-total-signins-controls" class="mini-pager"></div>'
        '      </section>'
        '      <section class="top-names-metric-panel">'
        '        <h4>By Max Records</h4>'
        '        <div id="baseline-top-names-max-records-chart" class="chart-host chart-host-short" aria-label="Top repeated names by max records chart"></div>'
        '        <div id="baseline-top-names-max-records-table" class="mini-table-host"></div>'
        '        <div id="baseline-top-names-max-records-controls" class="mini-pager"></div>'
        '      </section>'
        '    </div>'
        '  </section>'
        '  <details class="facet-glossary">'
        '    <summary>Facet Glossary</summary>'
        '    <ul>'
        '      <li><strong>Observed:</strong> Value measured in one report.</li>'
        '      <li><strong>Expected:</strong> Baseline median/expected value from cross-hearing corpus.</li>'
        '      <li><strong>Percentile:</strong> Relative rank in corpus (0 to 1).</li>'
        '      <li><strong>Support tier:</strong> <code>supported</code> (20+), <code>descriptive_only</code> (10–19), <code>unavailable</code> (&lt;10).</li>'
        '      <li><strong>Robust z:</strong> Median/MAD-scaled deviation from corpus center.</li>'
        '      <li><strong>Empirical tail p:</strong> Two-sided rarity score from empirical corpus tails.</li>'
        '    </ul>'
        '  </details>'
        '</section>'
    )

    html_template = """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Legislature Tools Reports</title>
    <link
      rel="stylesheet"
      href="https://unpkg.com/tabulator-tables@6.3.0/dist/css/tabulator.min.css"
    >
    <style>
      :root {
        color-scheme: light;
        --bg: #edf2f7;
        --surface: #ffffff;
        --ink: #1b2a3a;
        --muted: #58697d;
        --border: #d3dce8;
        --accent: #1f4f82;
        --accent-soft: #d9e9ff;
        --ok: #2c7a5d;
        --warn: #a85c00;
        --alert: #9a2530;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        font-family: "Avenir Next", "Segoe UI", sans-serif;
        font-size: 14px;
        line-height: 1.35;
        color: var(--ink);
        background: linear-gradient(180deg, #f8fbff 0%, var(--bg) 100%);
      }
      main {
        max-width: 1500px;
        margin: 0 auto;
        padding: 2rem 1rem 3rem;
      }
      header {
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 1rem 1.25rem;
      }
      h1 {
        margin: 0 0 0.35rem 0;
        font-size: 1.45rem;
      }
      h2 {
        margin: 0;
        font-size: 1.18rem;
      }
      h3 {
        margin: 0 0 0.35rem 0;
        font-size: 1rem;
      }
      h4 {
        margin: 0 0 0.35rem 0;
        font-size: 0.92rem;
        color: var(--muted);
      }
      .subtitle {
        margin: 0;
        color: var(--muted);
        font-size: 0.92rem;
      }
      .helper {
        margin: 0;
        color: var(--muted);
        font-size: 0.84rem;
      }
      code {
        font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
        font-size: 0.82rem;
      }

      .baseline-shell {
        margin-top: 1rem;
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 0.9rem;
        display: grid;
        gap: 0.8rem;
      }
      .baseline-header {
        display: grid;
        gap: 0.3rem;
      }
      .baseline-warning {
        margin: 0;
        border: 1px solid color-mix(in srgb, var(--warn) 35%, white);
        background: color-mix(in srgb, var(--warn) 10%, white);
        color: color-mix(in srgb, var(--warn) 80%, black);
        border-radius: 8px;
        padding: 0.55rem 0.7rem;
        font-size: 0.84rem;
      }
      .hidden {
        display: none !important;
      }
      .baseline-summary-cards {
        display: grid;
        gap: 0.65rem;
        grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      }
      .baseline-card {
        border: 1px solid var(--border);
        border-radius: 10px;
        padding: 0.6rem 0.7rem;
        background: linear-gradient(165deg, #ffffff 0%, #f7fbff 100%);
      }
      .baseline-card-label {
        margin: 0;
        color: var(--muted);
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }
      .baseline-card-value {
        margin: 0.2rem 0 0 0;
        font-size: 1.1rem;
        font-weight: 700;
      }
      .baseline-card-meta {
        margin: 0.15rem 0 0 0;
        color: var(--muted);
        font-size: 0.78rem;
      }
      .baseline-panel {
        border: 1px solid var(--border);
        border-radius: 10px;
        padding: 0.65rem;
        background: #ffffff;
        display: grid;
        gap: 0.5rem;
      }
      .baseline-panel-wide {
        width: 100%;
      }
      .chart-host {
        width: 100%;
        height: 260px;
      }
      .chart-host-short {
        height: 220px;
      }
      .mini-table-host {
        width: 100%;
      }
      .top-names-metric-grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 0.55rem;
      }
      .top-names-metric-panel {
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 0.5rem 0.55rem;
        background: #fff;
        display: grid;
        gap: 0.4rem;
      }
      .top-names-metric-panel h4 {
        margin: 0;
        color: var(--ink);
        font-size: 0.86rem;
      }
      .outlier-sections-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 0.55rem;
      }
      .outlier-metric-section {
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 0.5rem 0.55rem;
        background: #fff;
      }
      .outlier-metric-head {
        margin: 0 0 0.4rem 0;
        display: flex;
        flex-wrap: wrap;
        align-items: baseline;
        gap: 0.35rem;
      }
      .outlier-metric-title {
        font-size: 0.86rem;
        font-weight: 700;
        color: var(--ink);
      }
      .outlier-metric-count {
        font-size: 0.76rem;
        color: var(--muted);
      }
      .mini-empty {
        margin: 0;
        border: 1px dashed var(--border);
        border-radius: 8px;
        padding: 0.5rem 0.6rem;
        color: var(--muted);
        font-size: 0.82rem;
      }
      .mini-table {
        width: 100%;
        border-collapse: collapse;
        table-layout: fixed;
        font-size: 0.82rem;
      }
      .mini-table th,
      .mini-table td {
        border-bottom: 1px solid #edf2f8;
        padding: 0.34rem 0.4rem;
        vertical-align: top;
        word-wrap: break-word;
      }
      .mini-table th {
        text-align: left;
        color: var(--muted);
        font-weight: 600;
        font-size: 0.76rem;
      }
      .mini-table th button {
        border: none;
        background: transparent;
        color: inherit;
        font: inherit;
        font-weight: inherit;
        cursor: pointer;
        padding: 0;
      }
      .mini-table th button:hover {
        color: var(--accent);
      }
      .mini-table .align-right {
        text-align: right;
      }
      .mini-table .align-center {
        text-align: center;
      }
      .mini-pager {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        gap: 0.4rem;
        margin-top: 0.4rem;
        color: var(--muted);
        font-size: 0.8rem;
      }
      .mini-pager button {
        border: 1px solid var(--border);
        border-radius: 6px;
        background: #fff;
        color: var(--ink);
        font-size: 0.78rem;
        padding: 0.2rem 0.45rem;
        cursor: pointer;
      }
      .mini-pager button:disabled {
        opacity: 0.5;
        cursor: not-allowed;
      }
      .support-badge {
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        padding: 0.12rem 0.48rem;
        font-size: 0.72rem;
        font-weight: 600;
        border: 1px solid transparent;
      }
      .support-supported {
        color: var(--ok);
        border-color: color-mix(in srgb, var(--ok) 30%, white);
        background: color-mix(in srgb, var(--ok) 10%, white);
      }
      .support-descriptive_only {
        color: var(--warn);
        border-color: color-mix(in srgb, var(--warn) 30%, white);
        background: color-mix(in srgb, var(--warn) 10%, white);
      }
      .support-unavailable {
        color: var(--alert);
        border-color: color-mix(in srgb, var(--alert) 30%, white);
        background: color-mix(in srgb, var(--alert) 10%, white);
      }
      .status-badge {
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        padding: 0.12rem 0.48rem;
        font-size: 0.72rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        border: 1px solid transparent;
      }
      .status-open {
        color: var(--warn);
        border-color: color-mix(in srgb, var(--warn) 30%, white);
        background: color-mix(in srgb, var(--warn) 12%, white);
      }
      .status-closed {
        color: var(--ok);
        border-color: color-mix(in srgb, var(--ok) 30%, white);
        background: color-mix(in srgb, var(--ok) 12%, white);
      }
      .table-row-baseline-detail {
        margin: 0.25rem 0 0.2rem 0;
        border: 1px solid #e3eaf4;
        border-radius: 8px;
        padding: 0.55rem;
        background: #f8fbff;
      }
      .table-row-baseline-detail .detail-head {
        margin: 0 0 0.35rem 0;
        font-size: 0.82rem;
        color: var(--muted);
      }
      .table-row-baseline-detail .detail-grid {
        display: grid;
        gap: 0.5rem;
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      .table-row-baseline-detail h4 {
        margin: 0 0 0.3rem 0;
        font-size: 0.8rem;
        color: var(--muted);
      }
      .facet-glossary {
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 0.55rem 0.65rem;
        background: #fcfdff;
      }
      .facet-glossary summary {
        cursor: pointer;
        font-weight: 600;
        color: var(--ink);
      }
      .facet-glossary ul {
        margin: 0.45rem 0 0 1rem;
        padding: 0;
        display: grid;
        gap: 0.3rem;
        font-size: 0.84rem;
        color: var(--muted);
      }

      .controls {
        margin-top: 1rem;
        display: grid;
        gap: 0.6rem;
      }
      .controls label {
        color: var(--muted);
        font-size: 0.84rem;
      }
      .search-input {
        width: min(560px, 100%);
        border: 1px solid var(--border);
        border-radius: 8px;
        font-size: 0.9rem;
        padding: 0.62rem 0.72rem;
      }
      .search-input:focus {
        outline: 2px solid color-mix(in srgb, var(--accent) 35%, white);
        outline-offset: 1px;
      }
      .table-shell {
        margin-top: 0.9rem;
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 0.75rem;
      }
      .stats {
        margin-top: 0.6rem;
        color: var(--muted);
        font-size: 0.82rem;
      }
      .tabulator {
        border: none;
        background: transparent;
        font-size: 0.86rem;
      }
      .tabulator .tabulator-header {
        border-bottom: 1px solid var(--border);
      }
      .tabulator .tabulator-header .tabulator-col {
        background: #f8fbff;
        border-right: 1px solid var(--border);
      }
      .tabulator .tabulator-header .tabulator-col .tabulator-col-title {
        font-size: 0.82rem;
      }
      .tabulator .tabulator-row .tabulator-cell {
        border-right: 1px solid #e9eff8;
      }
      .tabulator .tabulator-cell {
        padding: 6px 8px;
      }
      .tabulator .tabulator-footer {
        border-top: 1px solid var(--border);
        font-size: 0.8rem;
      }
      .tabulator .tabulator-responsive-collapse table {
        font-size: 0.8rem;
      }
      .tabulator .tabulator-responsive-collapse table td {
        padding: 4px 6px;
      }
      .report-link {
        color: var(--accent);
        text-decoration: none;
        font-weight: 600;
      }
      .report-link:hover {
        text-decoration: underline;
      }
      .empty {
        margin-top: 1rem;
        background: var(--surface);
        border: 1px dashed var(--border);
        border-radius: 12px;
        padding: 1rem;
      }
      @media (max-width: 1200px) {
        .top-names-metric-grid {
          grid-template-columns: repeat(2, minmax(0, 1fr));
        }
      }
      @media (max-width: 900px) {
        main {
          padding: 1rem 0.6rem 2rem;
        }
        header {
          padding: 0.8rem 0.9rem;
        }
        h1 {
          font-size: 1.26rem;
        }
        .table-shell {
          padding: 0.45rem;
        }
        .tabulator .tabulator-header .tabulator-col {
          min-height: 42px;
        }
        .tabulator .tabulator-header .tabulator-col .tabulator-col-title {
          white-space: normal;
          line-height: 1.12;
        }
        .outlier-sections-grid {
          grid-template-columns: 1fr;
        }
        .top-names-metric-grid {
          grid-template-columns: 1fr;
        }
      }
      @media (max-width: 640px) {
        body {
          font-size: 13px;
        }
        .search-input {
          width: 100%;
          font-size: 0.86rem;
          padding: 0.55rem 0.62rem;
        }
        .tabulator {
          font-size: 0.8rem;
        }
        .tabulator .tabulator-cell {
          padding: 5px 6px;
        }
        .tabulator .tabulator-header .tabulator-col .tabulator-col-title {
          font-size: 0.75rem;
        }
        .chart-host {
          height: 230px;
        }
        .chart-host-short {
          height: 200px;
        }
        .table-row-baseline-detail .detail-grid {
          grid-template-columns: 1fr;
        }
      }
    </style>
  </head>
  <body>
    <main>
      <header>
        <h1>Legislature Tools Reports</h1>
        <p class="subtitle">Generated index: __GENERATED_AT_LOCAL__</p>
      </header>
      __BASELINE_MARKUP__
      __TABLE_MARKUP__
    </main>
    <script src="https://unpkg.com/tabulator-tables@6.3.0/dist/js/tabulator.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.1/dist/echarts.min.js"></script>
    <script>
      const tableRows = __TABLE_DATA_JSON__;
      const baselineAtlas = __BASELINE_DATA_JSON__;

      const tableElement = document.getElementById("reports-table");
      const tableStats = document.getElementById("table-stats");
      const globalSearchInput = document.getElementById("report-search");
      let reportTable = null;

      const TOP_NAME_METRICS = [
        {
          key: "report_count",
          title: "# Reports",
          panelSuffix: "report_count",
          seriesName: "Reports",
          valueLabel: "Reports",
        },
        {
          key: "total_n_records_across_reports",
          title: "Total sign-ins",
          panelSuffix: "total_signins",
          seriesName: "Total sign-ins",
          valueLabel: "Total sign-ins",
        },
        {
          key: "max_n_records_across_reports",
          title: "Max records",
          panelSuffix: "max_records",
          seriesName: "Max records",
          valueLabel: "Max records",
        },
      ];

      const baselineHosts = {
        warning: document.getElementById("baseline-atlas-warning"),
        summaryCards: document.getElementById("baseline-summary-cards"),
        outlierSections: document.getElementById("baseline-outlier-sections"),
        topNamesControls: document.getElementById("baseline-top-names-controls"),
        topNamesByMetric: Object.fromEntries(
          TOP_NAME_METRICS.map((metric) => [
            metric.key,
            {
              chart: document.getElementById(`baseline-top-names-${metric.panelSuffix.replaceAll("_", "-")}-chart`),
              table: document.getElementById(`baseline-top-names-${metric.panelSuffix.replaceAll("_", "-")}-table`),
              controls: document.getElementById(`baseline-top-names-${metric.panelSuffix.replaceAll("_", "-")}-controls`),
            },
          ])
        ),
      };

      let baselineSelectedReportId = "";
      const baselineCharts = {
        topNamesByMetric: {},
      };
      const topNamesState = {
        pageSize: 10,
        pageByMetric: Object.fromEntries(TOP_NAME_METRICS.map((metric) => [metric.key, 1])),
      };
      const outlierState = {
        pageSize: 5,
        pageByMetric: {},
      };
      const reportRowsById = new Map(
        (Array.isArray(tableRows) ? tableRows : [])
          .map((row) => [String(row?.report_id || "").trim(), row])
          .filter((pair) => !!pair[0])
      );

      function toFiniteNumber(value) {
        const numeric = Number(value);
        return Number.isFinite(numeric) ? numeric : null;
      }

      function formatInt(value) {
        const numeric = toFiniteNumber(value);
        if (numeric === null) {
          return "-";
        }
        return new Intl.NumberFormat("en-US").format(Math.trunc(numeric));
      }

      function formatPercentFrom100(value, digits = 1) {
        const numeric = toFiniteNumber(value);
        if (numeric === null) {
          return "-";
        }
        return `${numeric.toFixed(digits)}%`;
      }

      function formatPercentFraction(value, digits = 1) {
        const numeric = toFiniteNumber(value);
        if (numeric === null) {
          return "-";
        }
        return `${(numeric * 100).toFixed(digits)}%`;
      }

      function formatSigned(value, digits = 3) {
        const numeric = toFiniteNumber(value);
        if (numeric === null) {
          return "-";
        }
        const prefix = numeric > 0 ? "+" : "";
        return `${prefix}${numeric.toFixed(digits)}`;
      }

      function formatOutlierObservedExpected(metricId, value) {
        const metric = String(metricId || "").trim();
        const numeric = toFiniteNumber(value);
        if (numeric === null) {
          return "-";
        }
        if (metric === "total_submissions" || metric === "top_name_max_records") {
          return formatInt(numeric);
        }
        if (metric === "off_hours_ratio" || metric === "window_top_dup_fraction") {
          return formatPercentFraction(numeric, 1);
        }
        return numeric.toFixed(3);
      }

      function htmlEscape(value) {
        return String(value === null || value === undefined ? "" : value)
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;")
          .replaceAll("'", "&#39;");
      }

      function supportTierLabel(tier) {
        const value = String(tier || "").trim().toLowerCase();
        if (value === "supported") {
          return "Supported";
        }
        if (value === "descriptive_only") {
          return "Descriptive-only";
        }
        return "Unavailable";
      }

      function supportBadge(tier) {
        const normalized = String(tier || "").trim().toLowerCase();
        const tierClass = normalized || "unavailable";
        return `<span class="support-badge support-${htmlEscape(tierClass)}">${htmlEscape(
          supportTierLabel(normalized)
        )}</span>`;
      }

      function statusBadge(status) {
        const normalized = String(status || "closed").trim().toLowerCase() === "open" ? "open" : "closed";
        return `<span class="status-badge status-${htmlEscape(normalized)}">${htmlEscape(normalized)}</span>`;
      }

      function reportLinkHtml(reportId) {
        const reportKey = String(reportId || "").trim();
        if (!reportKey) {
          return "-";
        }
        const row = reportRowsById.get(reportKey);
        if (!row) {
          return htmlEscape(reportKey);
        }
        const href = String(row.report_href || "");
        const label = String(row.report_label || reportKey);
        if (!href) {
          return htmlEscape(label);
        }
        return `<a class="report-link" href="${htmlEscape(href)}">${htmlEscape(label)}</a>`;
      }

      function quantileSorted(sortedValues, q) {
        if (!Array.isArray(sortedValues) || !sortedValues.length) {
          return null;
        }
        const index = (sortedValues.length - 1) * q;
        const lower = Math.floor(index);
        const upper = Math.ceil(index);
        if (lower === upper) {
          return sortedValues[lower];
        }
        const weight = index - lower;
        return sortedValues[lower] * (1 - weight) + sortedValues[upper] * weight;
      }

      function median(values) {
        if (!Array.isArray(values) || !values.length) {
          return null;
        }
        const sorted = values
          .map((value) => toFiniteNumber(value))
          .filter((value) => value !== null)
          .sort((left, right) => left - right);
        return quantileSorted(sorted, 0.5);
      }

      function simpleTableHtml(columns, rows, emptyText) {
        const sourceRows = Array.isArray(rows) ? rows : [];
        if (!sourceRows.length) {
          return `<p class="mini-empty">${htmlEscape(emptyText || "No rows available.")}</p>`;
        }

        const headers = columns
          .map((column) => {
            const className = column.align === "right" ? "align-right" : column.align === "center" ? "align-center" : "";
            return `<th class="${className}">${htmlEscape(column.label || column.key || "")}</th>`;
          })
          .join("");

        const bodyRows = sourceRows
          .map((row) => {
            const cells = columns
              .map((column) => {
                const className = column.align === "right" ? "align-right" : column.align === "center" ? "align-center" : "";
                let rendered = "";
                if (typeof column.render === "function") {
                  rendered = column.render(row);
                } else {
                  rendered = htmlEscape(row?.[column.key] ?? "");
                }
                return `<td class="${className}">${rendered}</td>`;
              })
              .join("");
            return `<tr>${cells}</tr>`;
          })
          .join("");

        return `<table class="mini-table"><thead><tr>${headers}</tr></thead><tbody>${bodyRows}</tbody></table>`;
      }

      function renderSimpleTable(host, columns, rows, emptyText) {
        if (!host) {
          return;
        }
        host.innerHTML = simpleTableHtml(columns, rows, emptyText);
      }

      function activeTableRows(table) {
        if (!table) {
          return tableRows.slice();
        }
        if (typeof table.getData === "function") {
          try {
            const rows = table.getData("active");
            if (Array.isArray(rows)) {
              return rows;
            }
          } catch (_error) {
            // Fall through to full data.
          }
        }
        return tableRows.slice();
      }

      function activeReportIdSet(table) {
        const rows = activeTableRows(table);
        const ids = new Set();
        rows.forEach((row) => {
          const reportId = String(row?.report_id || "").trim();
          if (reportId) {
            ids.add(reportId);
          }
        });
        return ids;
      }

      function filterRowsByReportIds(rows, activeIds) {
        if (!(activeIds instanceof Set) || !activeIds.size) {
          return Array.isArray(rows) ? rows.slice() : [];
        }
        return (Array.isArray(rows) ? rows : []).filter((row) => activeIds.has(String(row?.report_id || "")));
      }

      function safeFraction(numerator, denominator) {
        const den = toFiniteNumber(denominator);
        if (den === null || den <= 0) {
          return null;
        }
        const num = toFiniteNumber(numerator);
        if (num === null) {
          return null;
        }
        return num / den;
      }

      function computeOutlierRows(comparatorRows) {
        const rows = [];
        (Array.isArray(comparatorRows) ? comparatorRows : []).forEach((row) => {
          const robustZ = toFiniteNumber(row?.robust_z);
          const percentile = toFiniteNumber(row?.percentile);
          let isOutlier = false;
          let method = "";
          let score = 0;

          if (robustZ !== null && Math.abs(robustZ) >= 3) {
            isOutlier = true;
            method = "robust_z";
            score = Math.abs(robustZ);
          } else if (
            robustZ === null
            && percentile !== null
            && (percentile <= 0.05 || percentile >= 0.95)
          ) {
            isOutlier = true;
            method = "percentile_tail";
            score = Math.abs(percentile - 0.5);
          }

          if (!isOutlier) {
            return;
          }

          rows.push({
            report_id: String(row?.report_id || ""),
            metric_id: String(row?.metric || ""),
            metric_label: String(row?.label || row?.metric || ""),
            observed: toFiniteNumber(row?.observed),
            expected: toFiniteNumber(row?.expected),
            delta: toFiniteNumber(row?.delta),
            percentile: percentile,
            empirical_tail_p_two_sided: toFiniteNumber(row?.empirical_tail_p_two_sided),
            support_tier: String(row?.support_tier || "unavailable"),
            outlier_method: method,
            outlier_score: score,
          });
        });

        rows.sort((left, right) => {
          const methodPriorityLeft = left.outlier_method === "robust_z" ? 0 : 1;
          const methodPriorityRight = right.outlier_method === "robust_z" ? 0 : 1;
          if (methodPriorityLeft !== methodPriorityRight) {
            return methodPriorityLeft - methodPriorityRight;
          }
          const scoreDelta = right.outlier_score - left.outlier_score;
          if (scoreDelta !== 0) {
            return scoreDelta;
          }
          return String(left.report_id || "").localeCompare(String(right.report_id || ""));
        });

        return rows;
      }

      function aggregateTopNames(topNameRows, indexedReportCount) {
        const grouped = new Map();
        (Array.isArray(topNameRows) ? topNameRows : []).forEach((row) => {
          const canonicalName = String(row?.canonical_name || "").trim();
          if (!canonicalName) {
            return;
          }
          let entry = grouped.get(canonicalName);
          if (!entry) {
            entry = {
              canonical_name: canonicalName,
              display_name: String(row?.display_name || canonicalName),
              reportIds: new Set(),
              report_count: 0,
              total_n_records_across_reports: 0,
              max_n_records_across_reports: 0,
              max_current_n_records: 0,
              max_n_records_percentile: null,
              report_share: null,
            };
            grouped.set(canonicalName, entry);
          }

          const reportId = String(row?.report_id || "").trim();
          if (reportId) {
            entry.reportIds.add(reportId);
          }

          const reportCount = Math.max(0, Math.round(Number(row?.report_count) || 0));
          if (reportCount > entry.report_count) {
            entry.report_count = reportCount;
          }

          const maxAcross = Math.max(0, Math.round(Number(row?.max_n_records_across_reports) || 0));
          if (maxAcross > entry.max_n_records_across_reports) {
            entry.max_n_records_across_reports = maxAcross;
          }

          const currentN = Math.max(0, Math.round(Number(row?.current_n_records) || 0));
          if (currentN > entry.max_current_n_records) {
            entry.max_current_n_records = currentN;
          }
          entry.total_n_records_across_reports += currentN;

          const percentile = toFiniteNumber(row?.max_n_records_percentile);
          if (percentile !== null) {
            if (entry.max_n_records_percentile === null || percentile > entry.max_n_records_percentile) {
              entry.max_n_records_percentile = percentile;
            }
          }

          const reportShare = toFiniteNumber(row?.report_share);
          if (reportShare !== null) {
            if (entry.report_share === null || reportShare > entry.report_share) {
              entry.report_share = reportShare;
            }
          }
        });

        const indexed = Math.max(0, Number(indexedReportCount) || 0);
        const rows = Array.from(grouped.values()).map((entry) => {
          const appearanceCount = entry.reportIds.size;
          const reportCount = Math.max(entry.report_count, appearanceCount);
          const reportShare = entry.report_share !== null
            ? entry.report_share
            : indexed > 0
              ? reportCount / indexed
              : 0;
          return {
            canonical_name: entry.canonical_name,
            display_name: entry.display_name,
            appearance_report_count: appearanceCount,
            report_count: reportCount,
            report_share: reportShare,
            total_n_records_across_reports: entry.total_n_records_across_reports,
            max_n_records_across_reports: entry.max_n_records_across_reports,
            max_current_n_records: entry.max_current_n_records,
            max_n_records_percentile: entry.max_n_records_percentile,
          };
        });

        rows.sort((left, right) => {
          const reportDelta = right.report_count - left.report_count;
          if (reportDelta !== 0) {
            return reportDelta;
          }
          const totalDelta = right.total_n_records_across_reports - left.total_n_records_across_reports;
          if (totalDelta !== 0) {
            return totalDelta;
          }
          const maxDelta = right.max_n_records_across_reports - left.max_n_records_across_reports;
          if (maxDelta !== 0) {
            return maxDelta;
          }
          return String(left.display_name || "").localeCompare(String(right.display_name || ""));
        });
        return rows;
      }

      function createSummaryCard(label, value, meta) {
        return `
          <article class="baseline-card">
            <p class="baseline-card-label">${htmlEscape(label)}</p>
            <p class="baseline-card-value">${htmlEscape(value)}</p>
            <p class="baseline-card-meta">${htmlEscape(meta || "")}</p>
          </article>
        `;
      }

      function overallRateCard(rows, {
        label,
        numeratorField,
        denominatorField,
        fallbackPctField,
        numeratorLabel,
      }) {
        let numeratorSum = 0;
        let denominatorSum = 0;
        let countWithNumerators = 0;
        const fallbackFractions = [];

        (Array.isArray(rows) ? rows : []).forEach((row) => {
          const numerator = toFiniteNumber(row?.[numeratorField]);
          const denominator = toFiniteNumber(row?.[denominatorField]);
          if (numerator !== null && denominator !== null && denominator > 0) {
            numeratorSum += numerator;
            denominatorSum += denominator;
            countWithNumerators += 1;
            return;
          }
          const fallbackPct = toFiniteNumber(row?.[fallbackPctField]);
          if (fallbackPct !== null) {
            fallbackFractions.push(fallbackPct / 100);
          }
        });

        if (denominatorSum > 0) {
          const fraction = numeratorSum / denominatorSum;
          const reportLabel = countWithNumerators === 1 ? "report" : "reports";
          const meta = `${formatInt(numeratorSum)} ${numeratorLabel} of ${formatInt(denominatorSum)} rows (${formatInt(countWithNumerators)} ${reportLabel})`;
          return createSummaryCard(label, formatPercentFraction(fraction, 1), meta);
        }

        if (fallbackFractions.length) {
          const meanFraction = fallbackFractions.reduce((acc, value) => acc + value, 0) / fallbackFractions.length;
          return createSummaryCard(
            label,
            formatPercentFraction(meanFraction, 1),
            `Mean of ${formatInt(fallbackFractions.length)} report-level value(s)`
          );
        }

        return createSummaryCard(label, "-", "No data in active reports");
      }

      function renderBaselineSummaryCards({
        reportRows,
      }) {
        if (!baselineHosts.summaryCards) {
          return;
        }
        const rows = Array.isArray(reportRows) ? reportRows : [];
        const cards = [
          overallRateCard(rows, {
            label: "Voter Match % (Exact)",
            numeratorField: "voter_match_rows_matched_exact",
            denominatorField: "voter_match_rows_total_exact",
            fallbackPctField: "voter_match_pct_exact",
            numeratorLabel: "matched",
          }),
          overallRateCard(rows, {
            label: "Voter Match % (Loose)",
            numeratorField: "voter_match_rows_matched_loose",
            denominatorField: "voter_match_rows_total_loose",
            fallbackPctField: "voter_match_pct_loose",
            numeratorLabel: "matched",
          }),
          overallRateCard(rows, {
            label: "Duplicate Name % (Exact)",
            numeratorField: "duplicate_rows_exact",
            denominatorField: "duplicate_rows_total",
            fallbackPctField: "duplicate_name_pct_exact",
            numeratorLabel: "repeated",
          }),
          overallRateCard(rows, {
            label: "Duplicate Name % (Loose)",
            numeratorField: "duplicate_rows_loose",
            denominatorField: "duplicate_rows_total",
            fallbackPctField: "duplicate_name_pct_loose",
            numeratorLabel: "repeated",
          }),
        ];

        baselineHosts.summaryCards.innerHTML = cards.join("");
      }

      function ensureChart(instance, host) {
        if (!host || typeof window.echarts === "undefined") {
          return null;
        }
        if (instance) {
          return instance;
        }
        return window.echarts.init(host, null, { renderer: "canvas" });
      }

      function topNamesMetricRows(rows, metricKey) {
        const source = Array.isArray(rows) ? rows.slice() : [];
        source.sort((left, right) => {
          const leftValue = toFiniteNumber(left?.[metricKey]);
          const rightValue = toFiniteNumber(right?.[metricKey]);
          const leftNumeric = leftValue === null ? Number.NEGATIVE_INFINITY : leftValue;
          const rightNumeric = rightValue === null ? Number.NEGATIVE_INFINITY : rightValue;
          if (rightNumeric !== leftNumeric) {
            return rightNumeric - leftNumeric;
          }
          const leftReports = toFiniteNumber(left?.report_count) || 0;
          const rightReports = toFiniteNumber(right?.report_count) || 0;
          if (rightReports !== leftReports) {
            return rightReports - leftReports;
          }
          return String(left?.display_name || left?.canonical_name || "").localeCompare(
            String(right?.display_name || right?.canonical_name || "")
          );
        });
        return source.slice(0, 100);
      }

      function topNamesMetricPage(rows, metricKey) {
        const sorted = topNamesMetricRows(rows, metricKey);
        const total = sorted.length;
        const pageSize = Math.max(1, Math.round(Number(topNamesState.pageSize) || 10));
        const totalPages = Math.max(1, Math.ceil(total / pageSize));
        const currentRaw = Number(topNamesState.pageByMetric?.[metricKey] || 1);
        const current = Math.min(totalPages, Math.max(1, Math.round(currentRaw)));
        topNamesState.pageByMetric[metricKey] = current;
        const start = (current - 1) * pageSize;
        const end = start + pageSize;
        return {
          sortedRows: sorted,
          pageRows: sorted.slice(start, end),
          totalRows: total,
          totalPages: totalPages,
          page: current,
          pageStart: start + 1,
          pageEnd: Math.min(end, total),
        };
      }

      function topNamesMetricHosts(metricKey) {
        const hosts = baselineHosts.topNamesByMetric || {};
        return hosts[metricKey] || {};
      }

      function renderTopNamesMetricChart(metricConfig, pageRows) {
        const metricKey = String(metricConfig?.key || "");
        if (!metricKey) {
          return;
        }
        const hosts = topNamesMetricHosts(metricKey);
        const host = hosts.chart;
        if (!host) {
          return;
        }

        const currentChart = baselineCharts.topNamesByMetric[metricKey] || null;
        baselineCharts.topNamesByMetric[metricKey] = ensureChart(currentChart, host);
        const chart = baselineCharts.topNamesByMetric[metricKey];
        if (!chart) {
          host.innerHTML = '<p class="mini-empty">Chart library unavailable.</p>';
          return;
        }

        const rows = Array.isArray(pageRows) ? pageRows : [];
        if (!rows.length) {
          chart.clear();
          chart.setOption({
            title: {
              text: "No repeated-name cues",
              left: "center",
              top: "middle",
              textStyle: { color: "#58697d", fontSize: 13, fontWeight: 500 },
            },
          });
          return;
        }

        chart.setOption({
          animation: false,
          grid: { left: 50, right: 12, top: 16, bottom: 88 },
          tooltip: {
            trigger: "item",
            formatter: (params) => {
              const row = rows[params.dataIndex] || {};
              return `${htmlEscape(row.display_name || row.canonical_name || "")}<br>`
                + `${htmlEscape(metricConfig.seriesName)}: ${formatInt(row[metricKey])}<br>`
                + `Reports: ${formatInt(row.report_count)}<br>`
                + `Total sign-ins: ${formatInt(row.total_n_records_across_reports)}<br>`
                + `Max records: ${formatInt(row.max_n_records_across_reports)}`;
            },
          },
          xAxis: {
            type: "category",
            data: rows.map((row) => String(row.display_name || row.canonical_name || "").slice(0, 22)),
            axisLabel: { rotate: 32, interval: 0 },
          },
          yAxis: {
            type: "value",
            minInterval: 1,
            name: String(metricConfig.seriesName || "Count"),
          },
          series: [
            {
              name: String(metricConfig.seriesName || "Value"),
              type: "bar",
              data: rows.map((row) => Math.max(0, Math.round(Number(row?.[metricKey]) || 0))),
              barMaxWidth: 26,
            },
          ],
        });
      }

      function renderOutlierTablesByMetric(outlierRows) {
        const host = baselineHosts.outlierSections;
        if (!host) {
          return;
        }
        const rows = Array.isArray(outlierRows) ? outlierRows : [];
        if (!rows.length) {
          host.innerHTML = '<p class="mini-empty">No outlier rows for the active report set.</p>';
          return;
        }

        const grouped = new Map();
        rows.forEach((row) => {
          const metricId = String(row?.metric_id || row?.metric_label || "unknown");
          const metricLabel = String(row?.metric_label || row?.metric_id || "Unknown metric");
          let entry = grouped.get(metricId);
          if (!entry) {
            entry = {
              metric_id: metricId,
              metric_label: metricLabel,
              rows: [],
            };
            grouped.set(metricId, entry);
          }
          entry.rows.push(row);
        });

        const groups = Array.from(grouped.values());
        groups.forEach((group) => {
          group.rows.sort((left, right) => {
            const scoreDelta = (toFiniteNumber(right?.outlier_score) || 0) - (toFiniteNumber(left?.outlier_score) || 0);
            if (scoreDelta !== 0) {
              return scoreDelta;
            }
            return String(left?.report_id || "").localeCompare(String(right?.report_id || ""));
          });
        });
        groups.sort((left, right) => {
          const sizeDelta = right.rows.length - left.rows.length;
          if (sizeDelta !== 0) {
            return sizeDelta;
          }
          return String(left.metric_label || "").localeCompare(String(right.metric_label || ""));
        });

        const columnsForMetric = (metricId) => [
          {
            key: "report_id",
            label: "Report",
            render: (row) => reportLinkHtml(row?.report_id),
          },
          {
            key: "observed",
            label: "Observed",
            align: "right",
            render: (row) => formatOutlierObservedExpected(metricId, row?.observed),
          },
          {
            key: "expected",
            label: "Expected",
            align: "right",
            render: (row) => formatOutlierObservedExpected(metricId, row?.expected),
          },
          {
            key: "percentile",
            label: "Percentile",
            align: "right",
            render: (row) => formatPercentFraction(row?.percentile, 1),
          },
        ];

        host.innerHTML = groups
          .map((group) => {
            const pageSize = Math.max(1, Math.round(Number(outlierState.pageSize) || 5));
            const tableHtml = simpleTableHtml(
              columnsForMetric(group.metric_id),
              group.rows.slice(
                (Math.min(
                  Math.max(1, Math.round(Number(outlierState.pageByMetric?.[group.metric_id] || 1))),
                  Math.max(1, Math.ceil(group.rows.length / pageSize))
                ) - 1) * pageSize,
                (Math.min(
                  Math.max(1, Math.round(Number(outlierState.pageByMetric?.[group.metric_id] || 1))),
                  Math.max(1, Math.ceil(group.rows.length / pageSize))
                ) - 1) * pageSize + pageSize
              ),
              "No rows"
            );
            const count = group.rows.length;
            const totalPages = Math.max(1, Math.ceil(count / pageSize));
            const currentPage = Math.min(
              totalPages,
              Math.max(1, Math.round(Number(outlierState.pageByMetric?.[group.metric_id] || 1)))
            );
            outlierState.pageByMetric[group.metric_id] = currentPage;
            const pageStart = count ? ((currentPage - 1) * pageSize + 1) : 0;
            const pageEnd = count ? Math.min(currentPage * pageSize, count) : 0;
            const countLabel = count === 1 ? "outlier row" : "outlier rows";
            return `
              <section class="outlier-metric-section">
                <p class="outlier-metric-head">
                  <span class="outlier-metric-title">${htmlEscape(group.metric_label)}</span>
                  <span class="outlier-metric-count">${formatInt(count)} ${htmlEscape(countLabel)}</span>
                </p>
                ${tableHtml}
                <div class="mini-pager outlier-pager">
                  <span>Showing ${count ? `${formatInt(pageStart)}-${formatInt(pageEnd)}` : "0"} of ${formatInt(count)}</span>
                  <button
                    type="button"
                    data-outlier-action="prev"
                    data-outlier-metric-id="${htmlEscape(group.metric_id)}"
                    ${currentPage <= 1 ? "disabled" : ""}
                  >Prev</button>
                  <span>Page ${formatInt(currentPage)} / ${formatInt(totalPages)}</span>
                  <button
                    type="button"
                    data-outlier-action="next"
                    data-outlier-metric-id="${htmlEscape(group.metric_id)}"
                    ${currentPage >= totalPages ? "disabled" : ""}
                  >Next</button>
                </div>
              </section>
            `;
          })
          .join("");

        host.querySelectorAll("button[data-outlier-action][data-outlier-metric-id]").forEach((button) => {
          button.addEventListener("click", () => {
            const metricId = String(button.getAttribute("data-outlier-metric-id") || "");
            const action = String(button.getAttribute("data-outlier-action") || "");
            if (!metricId || (action !== "prev" && action !== "next")) {
              return;
            }
            const delta = action === "prev" ? -1 : 1;
            const current = Math.max(1, Math.round(Number(outlierState.pageByMetric?.[metricId] || 1)));
            outlierState.pageByMetric[metricId] = Math.max(1, current + delta);
            renderOutlierTablesByMetric(outlierRows);
          });
        });
      }

      function renderTopNamesMetricPanel(metricConfig, topNameRows) {
        const metricKey = String(metricConfig?.key || "");
        if (!metricKey) {
          return;
        }
        const hosts = topNamesMetricHosts(metricKey);
        if (!hosts.table || !hosts.controls) {
          return;
        }

        const page = topNamesMetricPage(topNameRows, metricKey);
        const rows = page.pageRows;
        const tableHtml = simpleTableHtml(
          [
            {
              key: "display_name",
              label: "Name",
              render: (row) => htmlEscape(row?.display_name || row?.canonical_name || ""),
            },
            {
              key: metricKey,
              label: String(metricConfig.valueLabel || "Value"),
              align: "right",
              render: (row) => formatInt(row?.[metricKey]),
            },
          ],
          rows,
          "No repeated-name rows for the active report set."
        );
        hosts.table.innerHTML = tableHtml;

        hosts.controls.innerHTML = `
          <span>Showing ${page.totalRows ? `${page.pageStart}-${page.pageEnd}` : "0"} of ${formatInt(page.totalRows)} names</span>
          <button type="button" data-top-names-metric-action="prev" data-top-names-metric-key="${htmlEscape(metricKey)}" ${page.page <= 1 ? "disabled" : ""}>Prev</button>
          <span>Page ${formatInt(page.page)} / ${formatInt(page.totalPages)}</span>
          <button type="button" data-top-names-metric-action="next" data-top-names-metric-key="${htmlEscape(metricKey)}" ${page.page >= page.totalPages ? "disabled" : ""}>Next</button>
        `;
        hosts.controls.querySelectorAll("button[data-top-names-metric-action][data-top-names-metric-key]").forEach((button) => {
          button.addEventListener("click", () => {
            const action = String(button.getAttribute("data-top-names-metric-action") || "");
            const actionMetricKey = String(button.getAttribute("data-top-names-metric-key") || "");
            if (!actionMetricKey || (action !== "prev" && action !== "next")) {
              return;
            }
            const delta = action === "prev" ? -1 : 1;
            const current = Math.max(1, Math.round(Number(topNamesState.pageByMetric?.[actionMetricKey] || 1)));
            topNamesState.pageByMetric[actionMetricKey] = Math.max(1, current + delta);
            renderTopNamesSection(topNameRows);
          });
        });

        renderTopNamesMetricChart(metricConfig, rows);
      }

      function renderTopNamesSection(topNameRows) {
        TOP_NAME_METRICS.forEach((metricConfig) => {
          renderTopNamesMetricPanel(metricConfig, topNameRows);
        });
      }

      function metricRowsForReportDetail(detail) {
        return Array.isArray(detail?.metric_comparators)
          ? detail.metric_comparators.slice().sort((left, right) => {
              const leftDelta = Math.abs(toFiniteNumber(left?.delta) || 0);
              const rightDelta = Math.abs(toFiniteNumber(right?.delta) || 0);
              if (rightDelta !== leftDelta) {
                return rightDelta - leftDelta;
              }
              return String(left?.label || left?.metric || "").localeCompare(String(right?.label || right?.metric || ""));
            })
          : [];
      }

      function topNameRowsForReportDetail(detail) {
        return Array.isArray(detail?.top_name_cues)
          ? detail.top_name_cues.slice().sort((left, right) => {
              const leftN = Math.max(0, Math.round(Number(left?.current_n_records) || 0));
              const rightN = Math.max(0, Math.round(Number(right?.current_n_records) || 0));
              if (rightN !== leftN) {
                return rightN - leftN;
              }
              return String(left?.display_name || left?.canonical_name || "").localeCompare(
                String(right?.display_name || right?.canonical_name || "")
              );
            })
          : [];
      }

      function reportDetailMarkup(reportId) {
        const byReport = baselineAtlas && typeof baselineAtlas.by_report === "object" ? baselineAtlas.by_report : {};
        const detail = byReport[String(reportId || "")] || null;
        if (!detail) {
          return '<p class="detail-head">Baseline detail unavailable for this report.</p>';
        }
        const metricRows = metricRowsForReportDetail(detail);
        const topNameRows = topNameRowsForReportDetail(detail);
        const metricsCount = metricRows.length;
        const namesCount = topNameRows.length;

        const metricTable = simpleTableHtml(
          [
            { key: "label", label: "Metric" },
            {
              key: "observed",
              label: "Observed",
              align: "right",
              render: (row) => {
                const value = toFiniteNumber(row?.observed);
                return value === null ? "-" : value.toFixed(3);
              },
            },
            {
              key: "expected",
              label: "Expected",
              align: "right",
              render: (row) => {
                const value = toFiniteNumber(row?.expected);
                return value === null ? "-" : value.toFixed(3);
              },
            },
            {
              key: "delta",
              label: "Delta",
              align: "right",
              render: (row) => formatSigned(row?.delta, 3),
            },
            {
              key: "support_tier",
              label: "Support",
              align: "center",
              render: (row) => supportBadge(row?.support_tier),
            },
          ],
          metricRows,
          "No comparator metrics for this report."
        );

        const topNameTable = simpleTableHtml(
          [
            {
              key: "display_name",
              label: "Name",
              render: (row) => htmlEscape(row?.display_name || row?.canonical_name || ""),
            },
            {
              key: "current_n_records",
              label: "Current n",
              align: "right",
              render: (row) => formatInt(row?.current_n_records),
            },
            {
              key: "report_count",
              label: "Reports",
              align: "right",
              render: (row) => formatInt(row?.report_count),
            },
            {
              key: "max_n_records_across_reports",
              label: "Max n",
              align: "right",
              render: (row) => formatInt(row?.max_n_records_across_reports),
            },
          ],
          topNameRows,
          "No top-name cues for this report."
        );

        return `
          <p class="detail-head">${htmlEscape(String(reportId || ""))}: ${formatInt(metricsCount)} comparator metric(s), ${formatInt(namesCount)} top-name cue(s).</p>
          <div class="detail-grid">
            <div>
              <h4>Metric Comparators</h4>
              ${metricTable}
            </div>
            <div>
              <h4>Top Name Cues</h4>
              ${topNameTable}
            </div>
          </div>
        `;
      }

      function applyInlineBaselineDetail(row) {
        const rowElement = row.getElement();
        if (!rowElement) {
          return;
        }
        rowElement.querySelectorAll(".table-row-baseline-detail").forEach((node) => node.remove());

        const reportId = String(row.getData()?.report_id || "");
        if (!baselineSelectedReportId || reportId !== baselineSelectedReportId) {
          rowElement.classList.remove("row-has-baseline-detail");
          if (typeof row.normalizeHeight === "function") {
            row.normalizeHeight();
          }
          return;
        }

        const detailContainer = document.createElement("div");
        detailContainer.className = "table-row-baseline-detail";
        detailContainer.innerHTML = reportDetailMarkup(reportId);
        rowElement.appendChild(detailContainer);
        rowElement.classList.add("row-has-baseline-detail");
        if (typeof row.normalizeHeight === "function") {
          row.normalizeHeight();
        }
      }

      function renderBaselineAtlas(table) {
        const summary = baselineAtlas && typeof baselineAtlas.summary === "object" ? baselineAtlas.summary : {};
        const totalIndexedCount = Math.max(
          0,
          Math.round(
            Number(summary.indexed_report_count || baselineAtlas.report_count || tableRows.length || 0)
          )
        );

        const activeIds = activeReportIdSet(table);
        const activeReportRows = activeTableRows(table);
        const activeIndexedCount = activeIds.size > 0 ? activeIds.size : totalIndexedCount;

        const comparatorRowsRaw = Array.isArray(baselineAtlas.comparator_rows)
          ? baselineAtlas.comparator_rows
          : [];
        const topNameRowsRaw = Array.isArray(baselineAtlas.top_name_rows)
          ? baselineAtlas.top_name_rows
          : [];

        const filteredComparatorRows = filterRowsByReportIds(comparatorRowsRaw, activeIds);
        const filteredTopNameRows = filterRowsByReportIds(topNameRowsRaw, activeIds);
        if (baselineSelectedReportId && activeIds.size > 0 && !activeIds.has(baselineSelectedReportId)) {
          baselineSelectedReportId = "";
        }
        const outlierRows = computeOutlierRows(filteredComparatorRows);
        const topNameAggregates = aggregateTopNames(filteredTopNameRows, activeIndexedCount).slice(0, 100);

        renderBaselineSummaryCards({
          reportRows: activeReportRows,
        });
        renderOutlierTablesByMetric(outlierRows);
        renderTopNamesSection(topNameAggregates);

        if (baselineHosts.warning) {
          const available = !!baselineAtlas.available;
          if (!available) {
            const reason = String(baselineAtlas.reason || "global_baselines_unavailable");
            baselineHosts.warning.classList.remove("hidden");
            baselineHosts.warning.textContent = `Baseline atlas unavailable: ${reason.replaceAll("_", " ")}. Report table remains fully available.`;
          } else if (!filteredComparatorRows.length && !filteredTopNameRows.length && activeIds.size > 0) {
            baselineHosts.warning.classList.remove("hidden");
            baselineHosts.warning.textContent = "No baseline rows for the current report filter.";
          } else {
            baselineHosts.warning.classList.add("hidden");
            baselineHosts.warning.textContent = "";
          }
        }
      }

      function resizeBaselineCharts() {
        const queue = [baselineCharts];
        while (queue.length) {
          const current = queue.pop();
          if (!current || typeof current !== "object") {
            continue;
          }
          Object.values(current).forEach((value) => {
            if (!value) {
              return;
            }
            if (typeof value.resize === "function") {
              value.resize();
              return;
            }
            if (typeof value === "object") {
              queue.push(value);
            }
          });
        }
      }

      window.addEventListener("resize", () => {
        resizeBaselineCharts();
        if (reportTable && typeof reportTable.redraw === "function") {
          window.requestAnimationFrame(() => {
            reportTable.redraw(true);
          });
        }
      });

      if (tableElement && typeof window.Tabulator !== "undefined") {
        const isMobile = window.matchMedia("(max-width: 700px)").matches;

        const asLink = (href, label) => {
          if (!href) {
            return "<span>-</span>";
          }
          const anchor = document.createElement("a");
          anchor.className = "report-link";
          anchor.href = String(href);
          anchor.textContent = String(label);
          return anchor.outerHTML;
        };

        const sortEpoch = (a, b) => {
          const aRaw = Number(a);
          const bRaw = Number(b);
          const aEpoch = Number.isFinite(aRaw) ? aRaw : Number.NEGATIVE_INFINITY;
          const bEpoch = Number.isFinite(bRaw) ? bRaw : Number.NEGATIVE_INFINITY;
          return aEpoch - bEpoch;
        };

        const matchesDisplayText = (headerValue, rowData, displayField) => {
          const needle = String(headerValue || "").trim().toLowerCase();
          if (!needle) {
            return true;
          }
          return String(rowData?.[displayField] || "").toLowerCase().includes(needle);
        };

        const toPlainText = (value) => {
          if (value === null || value === undefined) {
            return "";
          }
          if (typeof value === "string") {
            const parser = document.createElement("div");
            parser.innerHTML = value;
            return (parser.textContent || "").trim();
          }
          if (value instanceof Node) {
            return (value.textContent || "").trim();
          }
          return String(value).trim();
        };

        const renderResponsiveCollapse = (items) => {
          const table = document.createElement("table");
          const seen = new Set();

          for (const item of items || []) {
            const title = String(item?.title || "").trim();
            const valueText = toPlainText(item?.value);
            if (!title || title === "Details") {
              continue;
            }
            const key = `${title}::${valueText}`;
            if (seen.has(key)) {
              continue;
            }
            seen.add(key);

            const row = document.createElement("tr");
            const titleCell = document.createElement("td");
            const strong = document.createElement("strong");
            strong.textContent = title;
            titleCell.appendChild(strong);

            const valueCell = document.createElement("td");
            valueCell.textContent = valueText;

            row.appendChild(titleCell);
            row.appendChild(valueCell);
            table.appendChild(row);
          }

          return table;
        };

        reportTable = new window.Tabulator(tableElement, {
          data: tableRows,
          layout: "fitDataStretch",
          responsiveLayout: "collapse",
          responsiveLayoutCollapseStartOpen: false,
          responsiveLayoutCollapseUseFormatters: true,
          responsiveLayoutCollapseFormatter: renderResponsiveCollapse,
          rowHeader: {
            formatter: "responsiveCollapse",
            width: 48,
            minWidth: 48,
            hozAlign: "center",
            headerSort: false,
            resizable: false,
            frozen: true,
          },
          pagination: "local",
          paginationSize: isMobile ? 10 : 25,
          paginationSizeSelector: [10, 25, 50, 100],
          initialSort: [{ column: "meeting_epoch", dir: "desc" }],
          rowFormatter: (row) => {
            applyInlineBaselineDetail(row);
          },
          columns: [
            {
              title: "Report",
              field: "report_label",
              width: 132,
              minWidth: 112,
              headerFilter: "input",
              formatter: (cell) => {
                const row = cell.getRow().getData();
                return asLink(row.report_href, row.report_label || row.report_id);
              },
            },
            {
              title: "Bill Description",
              field: "bill_description",
              width: 320,
              minWidth: 220,
              maxWidth: 420,
              headerFilter: "input",
              formatter: (cell) => {
                const value = String(cell.getValue() || "-");
                return `<span title="${htmlEscape(value)}">${htmlEscape(value)}</span>`;
              },
            },
            {
              title: "Total Testifiers",
              field: "total_testifiers",
              hozAlign: "right",
              width: 124,
              minWidth: 102,
              sorter: "number",
              headerFilter: "number",
              formatter: (cell) => formatInt(cell.getValue()),
            },
            {
              title: "Pro %",
              field: "pro_pct",
              hozAlign: "right",
              width: 82,
              minWidth: 72,
              sorter: "number",
              headerFilter: "number",
              formatter: (cell) => formatPercentFrom100(cell.getValue(), 1),
            },
            {
              title: "Con %",
              field: "con_pct",
              hozAlign: "right",
              width: 82,
              minWidth: 72,
              sorter: "number",
              headerFilter: "number",
              formatter: (cell) => formatPercentFrom100(cell.getValue(), 1),
            },
            {
              title: "Duplicate Name %",
              field: "duplicate_name_pct",
              hozAlign: "right",
              width: 122,
              minWidth: 108,
              sorter: "number",
              headerFilter: "number",
              formatter: (cell) => formatPercentFrom100(cell.getValue(), 1),
            },
            {
              title: "Status",
              field: "status",
              hozAlign: "center",
              width: 88,
              minWidth: 78,
              sorter: "string",
              headerFilter: "input",
              formatter: (cell) => statusBadge(cell.getValue()),
            },
            {
              title: "Meeting Datetime (PT)",
              field: "meeting_epoch",
              width: 190,
              minWidth: 160,
              headerFilter: "input",
              headerFilterFunc: (headerValue, _rowValue, rowData) =>
                matchesDisplayText(headerValue, rowData, "meeting_local"),
              sorter: sortEpoch,
              formatter: (cell) => String(cell.getRow().getData().meeting_local || "—"),
            },
            {
              title: "Last Updated (PT)",
              field: "generated_epoch",
              width: 190,
              minWidth: 160,
              headerFilter: "input",
              headerFilterFunc: (headerValue, _rowValue, rowData) =>
                matchesDisplayText(headerValue, rowData, "generated_local"),
              sorter: sortEpoch,
              formatter: (cell) => String(cell.getRow().getData().generated_local || "—"),
            },
          ],
        });
        const table = reportTable;

        const refreshBaseline = () => {
          renderBaselineAtlas(table);
        };

        const updateStats = () => {
          if (!tableStats) {
            return;
          }
          const shown = typeof table.getDataCount === "function"
            ? table.getDataCount("active")
            : table.getRows("active").length;
          const total = tableRows.length;
          tableStats.textContent = `Showing ${shown} of ${total} reports`;
        };

        table.on("tableBuilt", () => {
          updateStats();
          refreshBaseline();
        });
        table.on("dataLoaded", () => {
          updateStats();
          refreshBaseline();
        });
        table.on("dataFiltered", () => {
          updateStats();
          refreshBaseline();
        });
        table.on("pageLoaded", () => {
          updateStats();
          refreshBaseline();
        });
        table.on("renderComplete", () => {
          updateStats();
          refreshBaseline();
        });
        table.on("rowClick", (_event, row) => {
          baselineSelectedReportId = String(row.getData()?.report_id || "");
          table.redraw(true);
          refreshBaseline();
        });

        window.requestAnimationFrame(() => {
          updateStats();
          refreshBaseline();
        });

        if (globalSearchInput) {
          globalSearchInput.addEventListener("input", (event) => {
            const needle = String(event.target.value || "").trim().toLowerCase();
            if (!needle) {
              table.clearFilter();
              updateStats();
              refreshBaseline();
              return;
            }
            table.setFilter((rowData) => {
              return (
                String(rowData.report_label || "").toLowerCase().includes(needle)
                || String(rowData.bill_description || "").toLowerCase().includes(needle)
                || String(rowData.meeting_local || "").toLowerCase().includes(needle)
                || String(rowData.generated_local || "").toLowerCase().includes(needle)
                || String(rowData.status || "").toLowerCase().includes(needle)
              );
            });
            updateStats();
            refreshBaseline();
          });
        }
      } else {
        renderBaselineAtlas(null);
      }
    </script>
  </body>
</html>
"""

    return (
        html_template
        .replace("__GENERATED_AT_LOCAL__", escape(generated_at_local))
        .replace("__BASELINE_MARKUP__", baseline_markup)
        .replace("__TABLE_MARKUP__", table_markup)
        .replace("__TABLE_DATA_JSON__", table_data_json)
        .replace("__BASELINE_DATA_JSON__", baseline_data_json)
    )


def main() -> None:
    repo_root = project_root()
    reports_dir = repo_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    entries = collect_entries(reports_dir, repo_root)
    baseline_atlas = build_baseline_atlas_payload(reports_dir)
    generated_at_local = _format_us_datetime_pacific(datetime.now(tz=timezone.utc))
    output_path = reports_dir / "index.html"
    output_path.write_text(
        render_index(
            entries,
            generated_at_local,
            baseline_atlas,
        ),
        encoding="utf-8",
    )
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
