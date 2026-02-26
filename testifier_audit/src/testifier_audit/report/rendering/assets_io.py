from __future__ import annotations

import hashlib
import json
import logging
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from testifier_audit.report.rendering.constants import (
    REPORT_ASSETS_DIRECTORY,
    REPORT_CSS_ASSET_FILENAME,
    REPORT_DATA_DIRECTORY,
    REPORT_JS_ASSET_FILENAME,
)
from testifier_audit.report.rendering.payload.common import (
    _coerce_bucket_minutes,
    _slugify_path_component,
)
from testifier_audit.report.rendering.serialization import _json_safe

LOGGER = logging.getLogger(__name__)

def _template_env() -> Environment:
    templates_path = Path(__file__).resolve().parents[1] / "templates"
    return Environment(
        loader=FileSystemLoader(str(templates_path)),
        autoescape=select_autoescape(enabled_extensions=("html", "xml")),
        trim_blocks=True,
        lstrip_blocks=True,
    )


def _copy_report_static_assets(out_dir: Path) -> dict[str, str]:
    source_root = Path(__file__).resolve().parents[1] / "static" / "report"
    if not source_root.exists():
        raise FileNotFoundError(f"Report static assets directory not found: {source_root}")

    destination_root = out_dir / REPORT_ASSETS_DIRECTORY
    if destination_root.exists():
        shutil.rmtree(destination_root)
    destination_root.mkdir(parents=True, exist_ok=True)

    copied_files: set[str] = set()
    for source_path in sorted(source_root.iterdir()):
        if not source_path.is_file():
            continue
        shutil.copy2(source_path, destination_root / source_path.name)
        copied_files.add(source_path.name)

    required_files = {"report.css", "main.js"}
    missing = sorted(required_files.difference(copied_files))
    if missing:
        missing_label = ", ".join(missing)
        raise FileNotFoundError(f"Missing required report static asset(s): {missing_label}")

    css_path = destination_root / "report.css"
    js_path = destination_root / "main.js"
    css_version = hashlib.sha256(css_path.read_bytes()).hexdigest()[:12]
    js_version = hashlib.sha256(js_path.read_bytes()).hexdigest()[:12]

    return {
        "css_url": f"{REPORT_CSS_ASSET_FILENAME}?v={css_version}",
        "js_url": f"{REPORT_JS_ASSET_FILENAME}?v={js_version}",
    }


def _write_json_payload(path: Path, payload: Any) -> int:
    encoded = json.dumps(
        _json_safe(payload),
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(encoded, encoding="utf-8")
    return len(encoded.encode("utf-8"))


def _ordered_chart_ids_for_analysis(analysis_entry: Any) -> list[str]:
    if not isinstance(analysis_entry, dict):
        return []
    seen: set[str] = set()
    ordered: list[str] = []
    hero = str(analysis_entry.get("hero_chart_id") or "").strip()
    if hero:
        seen.add(hero)
        ordered.append(hero)
    details = analysis_entry.get("detail_chart_ids")
    if isinstance(details, list):
        for raw in details:
            chart_id = str(raw or "").strip()
            if not chart_id or chart_id in seen:
                continue
            seen.add(chart_id)
            ordered.append(chart_id)
    return ordered


def _build_chart_data_manifest(
    out_dir: Path,
    interactive_charts: dict[str, Any],
) -> tuple[dict[str, Any], int]:
    charts_raw = interactive_charts.get("charts", {})
    charts: dict[str, list[dict[str, Any]]] = {}
    if isinstance(charts_raw, dict):
        for chart_id, rows in charts_raw.items():
            normalized_id = str(chart_id or "").strip()
            if not normalized_id:
                continue
            charts[normalized_id] = rows if isinstance(rows, list) else []

    analysis_catalog_raw = interactive_charts.get("analysis_catalog", [])
    analysis_catalog = (
        analysis_catalog_raw if isinstance(analysis_catalog_raw, list) else []
    )

    chart_to_analysis: dict[str, str] = {}
    analysis_to_chart_ids: dict[str, list[str]] = {}
    analysis_order: list[str] = []
    for entry in analysis_catalog:
        if not isinstance(entry, dict):
            continue
        analysis_id = str(entry.get("id") or "").strip()
        if not analysis_id:
            continue
        analysis_order.append(analysis_id)
        ordered_chart_ids = _ordered_chart_ids_for_analysis(entry)
        analysis_to_chart_ids[analysis_id] = ordered_chart_ids
        for chart_id in ordered_chart_ids:
            chart_to_analysis[chart_id] = analysis_id

    shared_chart_ids = sorted(
        [chart_id for chart_id in charts.keys() if chart_id not in chart_to_analysis]
    )
    if shared_chart_ids:
        shared_analysis_id = "__shared__"
        analysis_order.append(shared_analysis_id)
        analysis_to_chart_ids[shared_analysis_id] = shared_chart_ids
        for chart_id in shared_chart_ids:
            chart_to_analysis[chart_id] = shared_analysis_id

    used_slug_paths: set[str] = set()
    analysis_slug_map: dict[str, str] = {}
    for analysis_id in analysis_order:
        base = _slugify_path_component(analysis_id)
        slug = base
        suffix = 2
        while slug in used_slug_paths:
            slug = f"{base}-{suffix}"
            suffix += 1
        used_slug_paths.add(slug)
        analysis_slug_map[analysis_id] = slug

    analysis_manifest: dict[str, dict[str, Any]] = {}
    all_urls: list[str] = []
    shard_bytes_total = 0
    analyses_root = out_dir / REPORT_DATA_DIRECTORY / "analyses"
    for analysis_id in analysis_order:
        chart_ids = analysis_to_chart_ids.get(analysis_id, [])
        slug = analysis_slug_map[analysis_id]
        base_rows_by_chart: dict[str, list[dict[str, Any]]] = {}
        bucket_rows_by_bucket: dict[int, dict[str, list[dict[str, Any]]]] = {}
        chart_bucket_options: dict[str, list[int]] = {}

        for chart_id in chart_ids:
            rows = charts.get(chart_id, [])
            row_buckets: dict[int, list[dict[str, Any]]] = defaultdict(list)
            row_base: list[dict[str, Any]] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                bucket_minutes = _coerce_bucket_minutes(row.get("bucket_minutes"))
                if bucket_minutes is None:
                    row_base.append(row)
                else:
                    row_buckets[bucket_minutes].append(row)
            if row_base:
                base_rows_by_chart[chart_id] = row_base
            chart_bucket_options[chart_id] = sorted(row_buckets.keys())
            for bucket_minutes, bucket_rows in row_buckets.items():
                chart_rows = bucket_rows_by_bucket.setdefault(bucket_minutes, {})
                chart_rows[chart_id] = bucket_rows

        analysis_dir = analyses_root / slug
        base_file = analysis_dir / "base.json"
        base_url = f"{REPORT_DATA_DIRECTORY}/analyses/{slug}/base.json"
        shard_bytes_total += _write_json_payload(
            base_file,
            {
                "analysis_id": analysis_id,
                "bucket_minutes": None,
                "charts": base_rows_by_chart,
            },
        )
        all_urls.append(base_url)

        bucket_urls: dict[str, str] = {}
        for bucket_minutes in sorted(bucket_rows_by_bucket.keys()):
            bucket_key = str(bucket_minutes)
            bucket_file = analysis_dir / f"bucket-{bucket_key}m.json"
            bucket_url = f"{REPORT_DATA_DIRECTORY}/analyses/{slug}/bucket-{bucket_key}m.json"
            shard_bytes_total += _write_json_payload(
                bucket_file,
                {
                    "analysis_id": analysis_id,
                    "bucket_minutes": bucket_minutes,
                    "charts": bucket_rows_by_bucket.get(bucket_minutes, {}),
                },
            )
            bucket_urls[bucket_key] = bucket_url
            all_urls.append(bucket_url)

        analysis_manifest[analysis_id] = {
            "base_url": base_url,
            "bucket_urls": bucket_urls,
            "chart_ids": chart_ids,
            "chart_bucket_options": chart_bucket_options,
        }

    return (
        {
            "version": 1,
            "analysis": analysis_manifest,
            "chart_to_analysis": chart_to_analysis,
            "all_urls": all_urls,
        },
        shard_bytes_total,
    )


def _build_report_data_payload(
    out_dir: Path,
    *,
    artifact_rows_safe: dict[str, Any],
    detector_summaries_safe: dict[str, Any],
    table_previews_safe: dict[str, Any],
    table_column_docs_safe: dict[str, Any],
    table_help_docs_safe: dict[str, Any],
    interactive_charts_safe: dict[str, Any],
) -> tuple[dict[str, Any], int]:
    chart_data_manifest, shard_bytes_total = _build_chart_data_manifest(
        out_dir=out_dir,
        interactive_charts=interactive_charts_safe,
    )
    interactive_without_rows = dict(interactive_charts_safe)
    interactive_without_rows["charts"] = {}
    interactive_without_rows["chart_data_manifest"] = chart_data_manifest

    return (
        {
            "artifact_rows": artifact_rows_safe,
            "detector_summaries": detector_summaries_safe,
            "table_previews": table_previews_safe,
            "table_column_docs": table_column_docs_safe,
            "table_help_docs": table_help_docs_safe,
            "interactive_charts": interactive_without_rows,
        },
        shard_bytes_total,
    )
