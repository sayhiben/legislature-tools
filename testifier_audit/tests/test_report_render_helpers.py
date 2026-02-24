from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from testifier_audit.detectors.base import DetectorResult
from testifier_audit.report import render
from testifier_audit.report.analysis_registry import (
    ANALYSES_TO_PERFORM,
    default_analysis_definitions,
)
from testifier_audit.report.render import (
    REPORT_DATA_FILENAME,
    _artifact_rows_from_disk,
    _build_table_column_docs,
    _json_safe,
    _load_summaries_from_disk,
    _load_table_previews_from_disk,
    _serialize_value,
    _table_previews_from_results,
    render_report,
)


def _configured_focus_analysis_ids() -> list[str]:
    seen: set[str] = set()
    ids: list[str] = []
    for analysis_id in ANALYSES_TO_PERFORM:
        normalized = str(analysis_id or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ids.append(normalized)
    return ids


def _is_off_hours_only_view() -> bool:
    return _configured_focus_analysis_ids() == ["off_hours"]


def _visible_analysis_definitions() -> list[dict[str, object]]:
    definitions = default_analysis_definitions()
    configured_ids = set(_configured_focus_analysis_ids())
    if not configured_ids:
        return definitions
    return [entry for entry in definitions if str(entry.get("id") or "") in configured_ids]


def _load_report_data_payload(out_dir: Path) -> dict[str, object]:
    payload_path = out_dir / REPORT_DATA_FILENAME
    return json.loads(payload_path.read_text(encoding="utf-8"))


def test_serialize_value_handles_timestamp_timedelta_nan_and_item_paths(
    monkeypatch,
) -> None:
    assert _serialize_value(pd.Timestamp("2026-02-01T12:00:00Z")) == "2026-02-01T04:00:00-08:00"
    assert _serialize_value(pd.Timedelta(minutes=5)) == "0 days 00:05:00"
    assert _serialize_value(np.nan) is None

    class HasItem:
        def item(self) -> int:
            return 7

    assert _serialize_value(HasItem()) == 7

    class BrokenItem:
        def item(self) -> int:
            raise RuntimeError("boom")

    broken = BrokenItem()
    monkeypatch.setattr(render.pd, "isna", lambda _value: (_ for _ in ()).throw(TypeError("x")))
    assert _serialize_value(broken) == str(broken)


def test_json_safe_normalizes_non_finite_values() -> None:
    payload = {
        "a": float("nan"),
        "b": [1.0, float("inf"), float("-inf")],
        "c": pd.Timestamp("2026-02-01T12:00:00Z"),
    }
    normalized = _json_safe(payload)
    assert normalized["a"] is None
    assert normalized["b"] == [1.0, None, None]
    assert normalized["c"] == "2026-02-01T04:00:00-08:00"


def test_build_table_column_docs_includes_detector_and_special_preview_tables() -> None:
    table_previews = {
        "bursts": {
            "burst_window_tests": [
                {"window_minutes": 5, "n_windows": 10, "n_significant": 2, "median_rate_ratio": 1.4}
            ]
        }
    }
    docs = _build_table_column_docs(
        table_previews=table_previews,
        artifact_rows={"counts_per_minute": 120},
    )

    assert "bursts.burst_window_tests" in docs
    assert docs["bursts.burst_window_tests"]["window_minutes"]
    assert "artifacts.artifact_rows" in docs
    assert docs["artifacts.artifact_rows"]["artifact"].startswith("Artifact/table")
    assert "composite_score.evidence_bundle_preview" not in docs


def test_disk_summary_artifact_and_table_preview_loaders(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    summary_dir = out_dir / "summary"
    artifacts_dir = out_dir / "artifacts"
    tables_dir = out_dir / "tables"
    summary_dir.mkdir(parents=True)
    artifacts_dir.mkdir(parents=True)
    tables_dir.mkdir(parents=True)

    (summary_dir / "bursts.json").write_text(json.dumps({"n": 1}), encoding="utf-8")
    pd.DataFrame({"value": [1, 2]}).to_csv(artifacts_dir / "counts.csv", index=False)
    pd.DataFrame({"value": [1, 2, 3]}).to_parquet(
        artifacts_dir / "counts_parquet.parquet", index=False
    )

    (tables_dir / "skip_me.csv").write_text("a,b\n1,2\n", encoding="utf-8")  # no "__" in stem
    (tables_dir / "detector__unsupported.txt").write_text("ignore-me", encoding="utf-8")
    (tables_dir / "detector__bad.parquet").write_text("not parquet bytes", encoding="utf-8")
    pd.DataFrame({"x": [1], "y": ["v"]}).to_csv(tables_dir / "detector__table.csv", index=False)
    pd.DataFrame(
        [
            {
                "scope": "matched_only",
                "display_name": f"NAME {index}",
                "observed_count": index + 2,
            }
            for index in range(9)
        ]
    ).to_csv(tables_dir / "duplicates_exact__per_name_display.csv", index=False)
    pd.DataFrame(
        [
            {
                "scope": "matched_only",
                "metric": "repeated_group_rows",
                "observed": index,
            }
            for index in range(9)
        ]
    ).to_csv(tables_dir / "duplicates_exact__collision_overview.csv", index=False)

    assert _load_summaries_from_disk(out_dir)["bursts"]["n"] == 1

    artifact_rows = _artifact_rows_from_disk(out_dir)
    assert artifact_rows["counts"] == 2
    assert artifact_rows["counts_parquet"] == 3

    previews = _load_table_previews_from_disk(out_dir, max_rows=5)
    assert "detector" in previews
    assert "table" in previews["detector"]
    assert previews["detector"]["table"][0]["x"] == 1
    assert len(previews["duplicates_exact"]["per_name_display"]) == 9
    assert len(previews["duplicates_exact"]["collision_overview"]) == 5


def test_table_previews_from_results_keep_full_duplicate_name_tables() -> None:
    duplicate_rows = pd.DataFrame(
        [
            {
                "scope": "matched_only",
                "display_name": f"NAME {index}",
                "observed_count": index + 2,
            }
            for index in range(9)
        ]
    )
    collision_rows = pd.DataFrame(
        [
            {
                "scope": "matched_only",
                "metric": "repeated_group_rows",
                "observed": index,
            }
            for index in range(9)
        ]
    )
    results = {
        "duplicates_exact": DetectorResult(
            detector="duplicates_exact",
            summary={},
            tables={
                "per_name_display": duplicate_rows,
                "collision_overview": collision_rows,
            },
        )
    }

    previews = _table_previews_from_results(results, max_rows=5)
    assert len(previews["duplicates_exact"]["per_name_display"]) == 9
    assert len(previews["duplicates_exact"]["collision_overview"]) == 5


def test_render_report_uses_disk_fallback_when_results_are_empty(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    (out_dir / "summary").mkdir(parents=True)
    (out_dir / "artifacts").mkdir(parents=True)
    (out_dir / "tables").mkdir(parents=True)
    (out_dir / "figures").mkdir(parents=True)

    (out_dir / "summary" / "bursts.json").write_text(
        json.dumps({"n_significant_windows": 2}), encoding="utf-8"
    )
    pd.DataFrame({"value": [1]}).to_csv(
        out_dir / "artifacts" / "counts_per_minute.csv", index=False
    )
    (out_dir / "figures" / "example.png").write_bytes(b"png")

    report_path = render_report(results={}, artifacts={}, out_dir=out_dir)
    rendered = report_path.read_text(encoding="utf-8")
    report_data_payload = _load_report_data_payload(out_dir)

    assert report_path.exists()
    assert (out_dir / REPORT_DATA_FILENAME).exists()
    assert 'id="report-data-source"' in rendered
    assert 'id="report-data"' not in rendered
    assert "interactive_charts" in report_data_payload
    interactive_payload = report_data_payload.get("interactive_charts", {})
    assert isinstance(interactive_payload, dict)
    assert interactive_payload.get("charts", {}) == {}
    manifest = interactive_payload.get("chart_data_manifest", {})
    assert isinstance(manifest, dict)
    assert manifest.get("version") == 1
    shard_urls = manifest.get("all_urls", [])
    assert isinstance(shard_urls, list)
    assert shard_urls
    for shard_url in shard_urls:
        assert (out_dir / str(shard_url)).exists()
    if _is_off_hours_only_view():
        assert 'data-analysis-id="off_hours"' in rendered
    elif _configured_focus_analysis_ids():
        assert "Composite Evidence Score" not in rendered
        assert "Duplicate Names" in rendered
        assert "Registered Voter Match" in rendered
    assert 'data-analysis-id="composite_score"' not in rendered
    assert 'data-analysis-id="rare_names"' not in rendered
    assert 'data-analysis-id="periodicity"' not in rendered
    assert 'data-analysis-id="sortedness"' not in rendered
    assert 'data-analysis-id="changepoints"' not in rendered
    assert 'data-analysis-id="multivariate_anomalies"' not in rendered
    assert "Static Figure Exports" not in rendered


def test_render_report_json_payload_does_not_include_nan_literals(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    (out_dir / "summary").mkdir(parents=True)
    (out_dir / "artifacts").mkdir(parents=True)
    (out_dir / "tables").mkdir(parents=True)
    (out_dir / "figures").mkdir(parents=True)

    results = {
        "multivariate_anomalies": DetectorResult(
            detector="multivariate_anomalies",
            summary={"max_anomaly_score": float("nan")},
            tables={
                "bucket_anomaly_scores": pd.DataFrame(
                    {
                        "bucket_start": [pd.Timestamp("2026-02-01T00:00:00Z")],
                        "anomaly_score": [float("nan")],
                    }
                )
            },
        )
    }
    report_path = render_report(results=results, artifacts={}, out_dir=out_dir)
    rendered = report_path.read_text(encoding="utf-8")
    payload_text = (out_dir / REPORT_DATA_FILENAME).read_text(encoding="utf-8")
    assert "NaN" not in rendered
    assert "NaN" not in payload_text
    for shard_path in (out_dir / "report_data").rglob("*.json"):
        assert "NaN" not in shard_path.read_text(encoding="utf-8")


def test_render_report_does_not_load_cross_hearing_baseline_payload(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    out_dir = reports_dir / "SB9999-20260221-1000"
    out_dir.mkdir(parents=True)
    baseline_path = reports_dir / "global_baselines.json"
    baseline_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_count": 3,
                "by_report": {
                    out_dir.name: {
                        "available": True,
                        "report_id": out_dir.name,
                        "report_count": 3,
                        "metric_comparators": [
                            {
                                "metric": "total_submissions",
                                "label": "Total submissions",
                                "value": 123.0,
                                "percentile": 0.91,
                                "band_p10": 20.0,
                                "band_p50": 80.0,
                                "band_p90": 150.0,
                                "n_reports": 3,
                            }
                        ],
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    report_path = render_report(results={}, artifacts={}, out_dir=out_dir)
    rendered = report_path.read_text(encoding="utf-8")
    report_data_payload = _load_report_data_payload(out_dir)
    interactive_payload = report_data_payload.get("interactive_charts", {})
    assert isinstance(interactive_payload, dict)
    assert "cross_hearing_baseline" not in interactive_payload
    assert "Cross-Hearing Comparator" not in rendered


def test_render_report_includes_external_assets_and_runtime_contracts(
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "out"
    report_path = render_report(results={}, artifacts={}, out_dir=out_dir)
    rendered = report_path.read_text(encoding="utf-8")
    payload_text = (out_dir / REPORT_DATA_FILENAME).read_text(encoding="utf-8")
    css_asset_path = out_dir / "assets" / "report" / "report.css"
    js_asset_path = out_dir / "assets" / "report" / "main.js"
    css_text = css_asset_path.read_text(encoding="utf-8")
    js_text = js_asset_path.read_text(encoding="utf-8")

    assert css_asset_path.exists()
    assert js_asset_path.exists()
    assert 'href="assets/report/report.css"' in rendered
    assert 'type="module" src="assets/report/main.js"' in rendered
    assert "mountAllSections()" in js_text
    assert "Loading report sections..." in js_text
    assert "initLazySectionMounting" not in js_text
    assert "IntersectionObserver" not in js_text
    assert "preload_data" not in js_text
    assert "preload_all_data" not in js_text
    assert "chart_data_manifest" in payload_text
    assert "report_data/analyses/" in payload_text
    assert "rerenderBucketAwareCharts" in js_text
    assert 'mount.chart.on("dataZoom"' in js_text
    assert "fetch(reportDataUrl)" in js_text
    assert "async function ensureHeaderDataLoaded()" in js_text
    assert '"off_hours_summary_compare", "off_hours_control_timeline"' in js_text
    assert "scheduleZoomSync(" in js_text
    assert "parseLinkedZoomFromQueryParams" in js_text
    assert "parseDuplicateOptionFromQueryParams" in js_text
    assert "initializeLinkedZoomOnLoad()" in js_text
    assert "updateSectionViewControlsForHeading" in js_text
    assert "bucketSelectorLabel" in js_text
    assert "applySidebarBillMeta();" in js_text
    assert "setChartLoading(" in js_text
    assert "is-loading" in css_text
    assert "state.zoom" in js_text
    assert 'id="sidebar-global-controls"' in rendered
    assert 'class="global-header-title hidden"' in rendered
    assert 'id="header-bill-title"' in rendered
    assert 'id="header-context-meta"' in rendered
    assert 'id="header-bill-long-title"' in rendered
    assert 'id="global-controls-toggle"' in rendered
    assert 'id="global-controls-grid"' in rendered
    assert 'class="header-view-controls panel hidden"' in rendered
    assert 'id="section-view-controls-panel"' in rendered
    assert 'id="zoom-sync-panel"' in rendered
    assert 'id="zoom-copy-button"' not in rendered
    assert 'id="zoom-reset-button"' in rendered
    assert 'id="zoom-active-banner"' not in rendered
    assert 'id="duplicate-collision-panel"' in rendered
    assert 'id="duplicate-scope-select"' in rendered
    assert 'id="duplicate-metric-select"' in rendered
    assert 'id="sidebar-bill-meta"' not in rendered
    assert 'id="sidebar-report-title"' in rendered
    assert 'id="sidebar-meeting-meta"' in rendered
    assert 'id="report-timezone-summary"' in rendered
    assert "All times in this report are shown in " in js_text
    assert "updateZoomRangeLabel" in js_text
    assert "await ensureHeaderDataLoaded();" in js_text
    assert 'id="report-busy-indicator"' in rendered
    assert "runWithBusyIndicator(" in js_text
    assert "clearAllChartInteractionState();" in js_text
    assert "clearChartInteractionState(mount);" in js_text
    assert "scheduleChartResizeSequence()" in js_text
    assert 'Time (" + reportTimezoneLabel + ")"' in js_text
    assert "force24HourSlots: true" in js_text
    assert js_text.count("inverse: true") >= 3
    assert "ensureReadableAxes(option, mount)" in js_text
    assert 'name: "Date"' in js_text
    assert 'name: "Day of week"' in js_text
    assert 'id="theme-controls"' in rendered
    assert 'id="theme-light-button"' in rendered
    assert 'id="theme-dark-button"' in rendered
    assert 'id="chart-theme-palette"' not in rendered
    assert "testifier_audit_chart_theme" not in js_text
    assert "initSidebarTooltips()" in js_text
    assert "initThemeControl()" in js_text
    assert "initGlobalControlsCollapse()" in js_text
    assert "initChartThemeControl()" not in js_text
    assert "computeLegendDockMode(mount)" in js_text
    assert "scheduleLegendLayoutRerender()" in js_text
    assert "rerenderChartsForLegendLayoutIfNeeded()" in js_text
    assert "reserveXAxisBottomSpace(option)" in js_text
    assert "hasSliderDataZoom(option)" in js_text
    assert "hasVisualMap(option)" in js_text
    assert "attachFunnelCursorHandler(mount)" in js_text
    assert "extractFunnelCursorFromEvent(params)" in js_text
    assert "controls.color_semantics" in js_text
    assert "fallbackColorSemantics" in js_text
    assert "resolveColorSemanticTheme(" in js_text
    assert "semanticTokenCache" in js_text
    assert "renderDuplicateTopNameTiming(" in js_text
    assert "function shouldRetainBucketlessRowsForChart(chartId)" in js_text
    assert "keepBucketlessRows && bucketValue === null" in js_text
    assert "volumeBarOpacity: surfaceTheme === \"dark\" ? 0.42 : 0.4" in js_text
    assert "shadowColor: theme.shadowColor" in js_text
    assert "shadowColor: \"rgba(0,0,0,0.35)\"" not in js_text
    assert "shadowColor: \"rgba(0,0,0,0.3)\"" not in js_text
    assert 'type: "errorBar"' not in js_text
    assert "Wilson low (tested)" in js_text
    assert "Wilson high (tested)" in js_text
    assert "Robust lower-tail alert" in js_text
    assert "Robust upper-tail alert" in js_text
    assert "duplicates_exact_top_name_timing_exact" in rendered
    assert "duplicates_exact_top_name_timing_medium" not in rendered
    assert "duplicates_exact_top_name_timing_loose" not in rendered
    assert "overview_position_volume_by_bucket" in rendered
    assert "renderOverviewPositionVolumeByBucket" in js_text
    assert "SPC-only flag" in js_text
    assert "FDR-only flag" in js_text
    assert "simpleBarCategoricalChartIds" in js_text
    assert "simpleBarRankedChartIds" in js_text
    assert "simpleBarNullDiagnosticChartIds" in js_text
    assert "simpleBarRatioReferenceChartIds" in js_text
    assert "table-cell-semantic-alert" in css_text
    assert "table-cell-semantic-warn" in css_text
    assert "table-cell-semantic-context" in css_text
    assert ".structured-host" in css_text
    assert ".structured-kv-item" in css_text
    assert ".structured-pair-item" in css_text
    assert ".table-host.table-host-compact" in css_text
    assert ".kpi-microchart" in css_text
    assert ".kpi-mini-pie" in css_text
    assert ".kpi-mini-bars" in css_text
    assert "@media print" in css_text
    assert ".toc-sidebar," in css_text
    assert ".page-shell.sidebar-open .report-main," in css_text
    assert "semanticClassForTableCell(tableKey, field, value)" in js_text
    assert "background: var(--table-row-bg);" in css_text
    if _is_off_hours_only_view():
        assert 'id="triage-dedup-mode"' not in rendered
        assert 'id="data-quality-warning-host"' not in rendered
        assert 'id="data-quality-dedup-metrics-host"' not in rendered
        assert 'id="cross-hearing-comparator-host"' not in rendered
        assert 'id="cross-hearing-comparator-summary"' not in rendered
        assert 'id="hearing-context-metadata-host"' not in rendered
        assert 'id="hearing-deadline-ramp-host"' not in rendered
        assert 'id="hearing-stance-by-deadline-host"' not in rendered
        assert 'id="methodology-artifact-rows-host"' not in rendered
        assert 'id="methodology-definitions-host"' not in rendered
        assert 'id="methodology-tests-used-host"' not in rendered
        assert 'id="methodology-guardrails-host"' not in rendered
        assert 'id="methodology-multiple-testing-list"' not in rendered
        assert 'id="off-hours-evidence-tier"' in rendered
        assert 'id="off-hours-inference-banner"' in rendered
        assert 'id="kpi-artifacts-meta"' not in rendered
        assert "sparseWhenLowSupport: true" in js_text
        assert "off_hours_primary_residual_timeline" in js_text
        assert "off_hours_primary_flag_channels" in js_text
        assert "off_hours_model_fit_diagnostics" in js_text
        assert "off_hours_date_hour_primary_residual_heatmap" in js_text
        assert "highlightOffHoursAxis: true" in js_text
        assert "model_fit_diagnostics" in js_text
        assert "flag_channel_summary" in js_text
        assert "flagged_window_diagnostics" in js_text
    else:
        assert 'id="triage-dedup-mode"' not in rendered
        assert 'id="data-quality-warning-host"' in rendered
        assert 'id="data-quality-dedup-metrics-host"' in rendered
        assert 'id="cross-hearing-comparator-host"' not in rendered
        assert 'id="cross-hearing-comparator-summary"' not in rendered
        assert 'id="hearing-context-metadata-host"' in rendered
        assert 'id="hearing-deadline-ramp-host"' in rendered
        assert 'id="hearing-stance-by-deadline-host"' not in rendered
        assert 'id="methodology-artifact-rows-host"' in rendered
        assert 'id="methodology-definitions-host"' in rendered
        assert 'id="methodology-tests-used-host"' in rendered
        assert 'id="methodology-guardrails-host"' in rendered
        assert 'id="methodology-multiple-testing-list"' in rendered
        assert 'id="triage-total-procon-meta"' in rendered
        assert 'id="triage-procon-pie"' in rendered
        assert 'id="triage-date-range-meta"' in rendered
        assert 'id="triage-duplicate-names-total"' in rendered
        assert 'id="triage-duplicate-position-bars"' in rendered
        assert 'id="triage-voter-match-rate"' in rendered
        assert 'id="triage-voter-match-position-bars"' in rendered
        assert 'id="triage-overall-procon"' not in rendered
        assert 'id="triage-top-tier-count"' not in rendered
        assert 'id="forensics-top-names-host" class="table-host table-host-compact"' in rendered
        assert 'id="hearing-context-metadata-host" class="structured-host structured-host-compact"' in rendered
        assert 'id="hearing-deadline-ramp-host" class="structured-host structured-host-compact"' in rendered
        assert 'id="methodology-definitions-host" class="structured-host structured-host-compact"' in rendered
        assert 'id="methodology-guardrails-host" class="structured-host structured-host-compact"' in rendered
    assert "renderMethodologyPanel()" in js_text
    assert "initDedupModeControl()" not in js_text
    assert "renderDataQualityPanel()" in js_text
    assert "renderCrossHearingComparator()" not in js_text
    assert "applyCrossHearingNameCues(" not in js_text
    assert "applyCrossHearingClusterCues(" not in js_text
    assert "getCrossHearingComparator(" not in js_text
    assert "Cross-hearing p10-p90 band" not in js_text
    assert 'comparatorMetric: "overall_pro_rate"' not in js_text
    assert "renderHearingContextPanel()" in js_text
    assert "buildProcessMarkerLines()" in js_text
    assert "voter_registry_match_tiers" in js_text
    voter_rates_block_start = js_text.find("voter_registry_match_rates: {")
    voter_rates_block_end = js_text.find(
        "multivariate_score_timeline:",
        voter_rates_block_start if voter_rates_block_start >= 0 else 0,
    )
    assert voter_rates_block_start >= 0 and voter_rates_block_end > voter_rates_block_start
    voter_rates_block = js_text[voter_rates_block_start:voter_rates_block_end]
    assert "adaptiveLineRange: true" in voter_rates_block
    assert '"unmatched_rate",' not in voter_rates_block
    assert "statistical irregularity requiring review" in payload_text
    assert 'summary.textContent = "artifact_rows"' not in js_text
    assert "mountKeyValueList(metadataHost, metadataRows" in js_text
    assert "mountKeyValueList(rampHost, rampRows" in js_text
    assert "mountTextPairCards(definitionsHost, definitions" in js_text
    assert "mountTextPairCards(guardrailsHost, guardrails" in js_text
    assert "mountTable(metadataHost, metadataRows" not in js_text
    assert "mountTable(guardrailsHost, guardrails" not in js_text
    assert "formatDateRangeHumanized(" in js_text
    assert "renderKpiMiniPie(" in js_text
    assert "renderKpiMiniBars(" in js_text
    assert 'tablePreviewRows("duplicates_exact", "per_name_display")' in js_text
    assert "filterRowsByDuplicateTableBucket(" in js_text
    assert "rerenderBucketAwareTables();" in js_text
    assert "triage-overall-procon" not in js_text
    assert "triage-top-tier-count" not in js_text
    assert '"Total Sign-ins"' in js_text
    assert '"# Pro"' in js_text
    assert '"# Con"' in js_text


def test_render_report_table_semantic_rules_and_cell_background_normalization(
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "out"
    render_report(results={}, artifacts={}, out_dir=out_dir)
    css_text = (out_dir / "assets" / "report" / "report.css").read_text(encoding="utf-8")
    js_text = (out_dir / "assets" / "report" / "main.js").read_text(encoding="utf-8")

    assert "--table-semantic-alert-bg:" in css_text
    assert "--table-semantic-warn-bg:" in css_text
    assert "--table-semantic-context-bg:" in css_text
    assert "off_hours.off_hours_summary" in js_text
    assert "off_hours.model_fit_diagnostics" in js_text
    assert "model_fit_available_fraction" in js_text
    assert "primary_model_fit_converged" in js_text
    assert "semanticClassForTableCell(tableKey, field, value)" in js_text
    assert "delete tableOptions.tableKey;" in js_text
    assert (
        ".tabulator .tabulator-tableholder .tabulator-table .tabulator-row "
        ".tabulator-cell:first-child" in css_text
    )
    assert "--table-row-bg: var(--table-bg-alt);" in css_text
    assert "background: var(--table-row-bg);" in css_text


def test_render_report_template_contract_renders_analysis_hosts_and_placeholders(
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "out"
    report_path = render_report(results={}, artifacts={}, out_dir=out_dir)
    rendered = report_path.read_text(encoding="utf-8")
    report_data_payload = _load_report_data_payload(out_dir)
    interactive_payload = report_data_payload.get("interactive_charts", {})

    for analysis in _visible_analysis_definitions():
        analysis_id = str(analysis["id"])
        hero_chart_id = str(analysis["hero_chart_id"])
        assert f'data-analysis-id="{analysis_id}"' in rendered
        assert f'data-chart-id="{hero_chart_id}"' in rendered
        assert f'data-chart-empty-for="{hero_chart_id}"' in rendered
        for detail_chart_id in analysis.get("detail_chart_ids", []):
            detail_id = str(detail_chart_id)
            assert f'data-chart-id="{detail_id}"' in rendered
            assert f'data-chart-empty-for="{detail_id}"' in rendered

    overview_chart_ids = [
        "overview_position_volume_by_bucket",
        "off_hours_date_hour_pro_heatmap",
        "off_hours_date_hour_primary_residual_heatmap",
        "off_hours_date_hour_volume_heatmap",
    ]
    for chart_id in overview_chart_ids:
        assert f'data-chart-id="{chart_id}"' in rendered
        assert f'data-chart-empty-for="{chart_id}"' in rendered

    assert "PhotoSwipe" not in rendered
    assert "Static Figure Exports" not in rendered
    assert "table_column_docs" in report_data_payload
    assert isinstance(interactive_payload, dict)
    assert "chart_legend_docs" in interactive_payload
    assert 'type="module" src="assets/report/main.js"' in rendered
    assert "<strong>Legend guide:</strong>" in rendered
    assert "Matched-rate axis auto-zooms to observed values" in rendered
    assert "status-ready" not in rendered
