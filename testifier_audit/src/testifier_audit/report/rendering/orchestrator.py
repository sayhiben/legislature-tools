from __future__ import annotations

import json
import logging
import shutil
from datetime import datetime
from pathlib import Path
from time import perf_counter
from zoneinfo import ZoneInfo

import pandas as pd

from testifier_audit.detectors.base import DetectorResult
from testifier_audit.features.dedup import DEFAULT_DEDUP_MODE
from testifier_audit.io.hearing_metadata import HearingMetadata
from testifier_audit.report.rendering.assets_io import (
    _build_report_data_payload,
    _copy_report_static_assets,
    _template_env,
)
from testifier_audit.report.rendering.constants import (
    PACIFIC_TIMEZONE_NAME,
    REPORT_DATA_DIRECTORY,
    REPORT_DATA_FILENAME,
)
from testifier_audit.report.rendering.investigation_artifacts import _write_investigation_artifacts
from testifier_audit.report.rendering.payload.builder import (
    _interactive_chart_payload_from_disk,
    _interactive_chart_payload_from_results,
)
from testifier_audit.report.rendering.serialization import _json_safe
from testifier_audit.report.rendering.table_docs import _build_table_column_docs, _build_table_help_docs
from testifier_audit.report.rendering.table_previews import (
    _artifact_rows_from_disk,
    _load_summaries_from_disk,
    _load_table_previews_from_disk,
    _table_previews_from_results,
)

LOGGER = logging.getLogger(__name__)

def render_report(
    results: dict[str, DetectorResult],
    artifacts: dict[str, pd.DataFrame],
    out_dir: Path,
    *,
    default_dedup_mode: str = DEFAULT_DEDUP_MODE,
    min_cell_n_for_rates: int = 25,
    hearing_metadata: HearingMetadata | None = None,
) -> Path:
    report_started = perf_counter()
    generated_at = datetime.now(ZoneInfo(PACIFIC_TIMEZONE_NAME)).isoformat()
    generated_at_label = PACIFIC_TIMEZONE_NAME
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
    table_column_docs = _build_table_column_docs(
        table_previews=table_previews,
        artifact_rows=artifact_rows,
    )
    table_help_docs = _build_table_help_docs(table_column_docs=table_column_docs)
    interactive_started = perf_counter()
    interactive_charts = (
        _interactive_chart_payload_from_results(
            results=results,
            artifacts=artifacts,
            default_dedup_mode=default_dedup_mode,
            min_cell_n_for_rates=min_cell_n_for_rates,
            hearing_metadata=hearing_metadata,
        )
        if results
        else _interactive_chart_payload_from_disk(
            out_dir=out_dir,
            default_dedup_mode=default_dedup_mode,
            min_cell_n_for_rates=min_cell_n_for_rates,
            hearing_metadata=hearing_metadata,
        )
    )
    interactive_build_ms = round((perf_counter() - interactive_started) * 1000.0, 3)
    if isinstance(interactive_charts.get("controls"), dict):
        runtime_metrics = interactive_charts["controls"].get("runtime", {})
        if not isinstance(runtime_metrics, dict):
            runtime_metrics = {}
        runtime_metrics["interactive_payload_build_ms"] = interactive_build_ms
        interactive_charts["controls"]["runtime"] = runtime_metrics
    _write_investigation_artifacts(
        out_dir=out_dir,
        triage_summary=interactive_charts.get("triage_summary", {}),
        data_quality_panel=interactive_charts.get("data_quality_panel", {}),
    )

    detector_summaries_safe = _json_safe(detector_summaries)
    artifact_rows_safe = _json_safe(artifact_rows)
    table_previews_safe = _json_safe(table_previews)
    table_column_docs_safe = _json_safe(table_column_docs)
    table_help_docs_safe = _json_safe(table_help_docs)
    interactive_charts_safe = _json_safe(interactive_charts)
    report_data_root = out_dir / REPORT_DATA_DIRECTORY
    if report_data_root.exists():
        shutil.rmtree(report_data_root)
    legacy_report_data_path = out_dir / "report_data.json"
    if legacy_report_data_path.exists():
        legacy_report_data_path.unlink()

    report_data_payload, report_data_shards_json_bytes = _build_report_data_payload(
        out_dir=out_dir,
        artifact_rows_safe=artifact_rows_safe,
        detector_summaries_safe=detector_summaries_safe,
        table_previews_safe=table_previews_safe,
        table_column_docs_safe=table_column_docs_safe,
        table_help_docs_safe=table_help_docs_safe,
        interactive_charts_safe=interactive_charts_safe,
    )
    interactive_charts_for_template = report_data_payload.get("interactive_charts", {})
    report_data_path = out_dir / REPORT_DATA_FILENAME
    report_data_path.parent.mkdir(parents=True, exist_ok=True)
    report_data_json = json.dumps(
        report_data_payload,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    report_data_path.write_text(report_data_json, encoding="utf-8")
    report_assets = _copy_report_static_assets(out_dir)

    template_started = perf_counter()
    rendered = template.render(
        generated_at=generated_at,
        generated_at_label=generated_at_label,
        detector_summaries=detector_summaries_safe,
        artifact_rows=artifact_rows_safe,
        table_previews=table_previews_safe,
        table_column_docs=table_column_docs_safe,
        table_help_docs=table_help_docs_safe,
        interactive_charts=interactive_charts_for_template,
        report_data_url=REPORT_DATA_FILENAME,
        report_assets=report_assets,
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
        "report_data_json_bytes": len(report_data_json.encode("utf-8")),
        "report_data_shards_json_bytes": report_data_shards_json_bytes,
    }
    runtime_path = out_dir / "artifacts" / "report_runtime.json"
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_path.write_text(
        json.dumps(_json_safe(runtime_metrics), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return report_path
