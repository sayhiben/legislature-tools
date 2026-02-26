from __future__ import annotations

"""Backward-compatible facade for report rendering internals.

This module preserves the historical import surface while delegating
implementation across focused modules under ``testifier_audit.report.rendering``.
"""

import pandas as pd  # Backward-compatible module attribute for monkeypatch-based tests.

from testifier_audit.report.rendering.assets_io import (
    _build_chart_data_manifest,
    _build_report_data_payload,
    _copy_report_static_assets,
    _ordered_chart_ids_for_analysis,
    _template_env,
    _write_json_payload,
)
from testifier_audit.report.rendering.constants import (
    BASELINE_PROFILE_BUCKET_MINUTES,
    PACIFIC_TIMEZONE_NAME,
    REPORT_ASSETS_DIRECTORY,
    REPORT_CSS_ASSET_FILENAME,
    REPORT_DATA_DIRECTORY,
    REPORT_DATA_FILENAME,
    REPORT_JS_ASSET_FILENAME,
)
from testifier_audit.report.rendering.data_sources import (
    _load_table_map_from_disk,
    _load_table_map_from_results,
)
from testifier_audit.report.rendering.hearing_context import (
    _build_deadline_ramp_metrics,
    _build_hearing_context_panel,
)
from testifier_audit.report.rendering.help_docs import (
    _analysis_help_hints,
    _build_analysis_help_docs,
    _build_chart_help_docs,
    _chart_family,
    _default_chart_legend_docs,
    _detailed_what_to_look_for_by_analysis,
    _fallback_chart_legend_doc,
)
from testifier_audit.report.rendering.investigation_artifacts import (
    _rows_to_frame,
    _write_investigation_artifacts,
)
from testifier_audit.report.rendering.orchestrator import render_report
from testifier_audit.report.rendering.payload.builder import (
    _build_bucketed_baseline_profiles,
    _build_bucketed_day_hour_profiles,
    _build_interactive_chart_payload,
    _build_interactive_chart_payload_v2,
    _interactive_chart_payload_from_disk,
    _interactive_chart_payload_from_results,
)
from testifier_audit.report.rendering.payload.common import (
    _canonical_name_to_display_name,
    _coerce_bucket_minutes,
    _extract_bucket_options,
    _normalize_report_match_mode,
    _records_from_frame,
    _slugify_path_component,
    _table_key,
    _with_expected_columns,
)
from testifier_audit.report.rendering.serialization import (
    _json_safe,
    _serialize_value,
    _to_pacific_timestamp,
)
from testifier_audit.report.rendering.table_docs import (
    _build_table_column_docs,
    _build_table_help_docs,
    _default_column_description,
    _describe_column,
    _humanize_identifier,
    _table_column_docs_from_rows,
)
from testifier_audit.report.rendering.table_previews import (
    _artifact_rows_from_disk,
    _load_frame_from_candidates,
    _load_summaries_from_disk,
    _load_table_previews_from_disk,
    _prepare_table_for_preview,
    _preview_columns_for_detector_table,
    _preview_row_limit_for_detector_table,
    _table_preview,
    _table_previews_from_results,
)

__all__ = [
    "BASELINE_PROFILE_BUCKET_MINUTES",
    "PACIFIC_TIMEZONE_NAME",
    "REPORT_ASSETS_DIRECTORY",
    "REPORT_CSS_ASSET_FILENAME",
    "REPORT_DATA_DIRECTORY",
    "REPORT_DATA_FILENAME",
    "REPORT_JS_ASSET_FILENAME",
    "_analysis_help_hints",
    "_artifact_rows_from_disk",
    "_build_analysis_help_docs",
    "_build_bucketed_baseline_profiles",
    "_build_bucketed_day_hour_profiles",
    "_build_chart_data_manifest",
    "_build_chart_help_docs",
    "_build_deadline_ramp_metrics",
    "_build_hearing_context_panel",
    "_build_interactive_chart_payload",
    "_build_interactive_chart_payload_v2",
    "_build_report_data_payload",
    "_build_table_column_docs",
    "_build_table_help_docs",
    "_canonical_name_to_display_name",
    "_chart_family",
    "_coerce_bucket_minutes",
    "_copy_report_static_assets",
    "_default_chart_legend_docs",
    "_default_column_description",
    "_describe_column",
    "_detailed_what_to_look_for_by_analysis",
    "_extract_bucket_options",
    "_fallback_chart_legend_doc",
    "_humanize_identifier",
    "_interactive_chart_payload_from_disk",
    "_interactive_chart_payload_from_results",
    "_json_safe",
    "_load_frame_from_candidates",
    "_load_summaries_from_disk",
    "_load_table_map_from_disk",
    "_load_table_map_from_results",
    "_load_table_previews_from_disk",
    "_normalize_report_match_mode",
    "_ordered_chart_ids_for_analysis",
    "_prepare_table_for_preview",
    "_preview_columns_for_detector_table",
    "_preview_row_limit_for_detector_table",
    "_records_from_frame",
    "_rows_to_frame",
    "_serialize_value",
    "_slugify_path_component",
    "_table_column_docs_from_rows",
    "_table_key",
    "_table_preview",
    "_table_previews_from_results",
    "_template_env",
    "_to_pacific_timestamp",
    "_with_expected_columns",
    "_write_investigation_artifacts",
    "_write_json_payload",
    "pd",
    "render_report",
]
