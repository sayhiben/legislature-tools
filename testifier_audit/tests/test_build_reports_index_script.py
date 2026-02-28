from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "report"
    / "build_reports_index.py"
)


def _load_script_module():
    spec = importlib.util.spec_from_file_location("build_reports_index_script", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_support_tier_from_n_reports_boundaries() -> None:
    module = _load_script_module()

    assert module._support_tier_from_n_reports(0) == "unavailable"
    assert module._support_tier_from_n_reports(9) == "unavailable"
    assert module._support_tier_from_n_reports(10) == "descriptive_only"
    assert module._support_tier_from_n_reports(19) == "descriptive_only"
    assert module._support_tier_from_n_reports(20) == "supported"


def test_dedupe_bill_label_from_description() -> None:
    module = _load_script_module()

    assert module._dedupe_bill_label_from_description(
        "ESSB 6346",
        "ESSB 6346 Tax on millionaires",
    ) == "Tax on millionaires"
    assert module._dedupe_bill_label_from_description(
        "SB 1234",
        "SB-1234: Concerning clean water",
    ) == "Concerning clean water"
    assert module._dedupe_bill_label_from_description(
        "HB 9999",
        "Concerning schools",
    ) == "Concerning schools"


def test_normalize_baseline_comparator_row_legacy_shape() -> None:
    module = _load_script_module()

    row = module._normalize_baseline_comparator_row(
        report_id="SB1234-20260201-0800",
        raw_row={
            "metric": "total_submissions",
            "label": "Total submissions",
            "value": 120,
            "band_p50": 80,
            "percentile": 0.9,
            "n_reports": 15,
        },
    )

    assert row is not None
    assert row["observed"] == 120.0
    assert row["expected"] == 80.0
    assert row["delta"] == 40.0
    assert row["support_tier"] == "descriptive_only"
    assert row["descriptive_only"] is True
    assert row["low_power"] is True


def test_normalize_baseline_comparator_row_current_shape() -> None:
    module = _load_script_module()

    row = module._normalize_baseline_comparator_row(
        report_id="SB1234-20260201-0800",
        raw_row={
            "metric": "off_hours_ratio",
            "label": "Off-hours submission ratio",
            "observed": 0.12,
            "expected": 0.04,
            "delta": 0.08,
            "robust_z": 3.7,
            "empirical_tail_p_two_sided": 0.01,
            "n_reports": 33,
            "support_tier": "supported",
            "descriptive_only": False,
            "low_power": False,
        },
    )

    assert row is not None
    assert row["observed"] == 0.12
    assert row["expected"] == 0.04
    assert row["delta"] == 0.08
    assert row["robust_z"] == 3.7
    assert row["empirical_tail_p_two_sided"] == 0.01
    assert row["support_tier"] == "supported"
    assert row["descriptive_only"] is False
    assert row["low_power"] is False


def test_metric_catalog_and_top_name_aggregation_cover_unavailable_metrics() -> None:
    module = _load_script_module()

    comparator_rows = [
        {
            "report_id": "R1",
            "metric": "total_submissions",
            "label": "Total submissions",
        },
        {
            "report_id": "R2",
            "metric": "off_hours_ratio",
            "label": "Off-hours submission ratio",
        },
    ]
    catalog = module._build_metric_catalog(
        comparator_rows=comparator_rows,
        indexed_report_count=3,
    )

    total_submissions = next(row for row in catalog if row["metric"] == "total_submissions")
    window_top_score = next(row for row in catalog if row["metric"] == "window_top_score")

    assert total_submissions["report_count"] == 1
    assert total_submissions["available"] is True
    assert window_top_score["report_count"] == 0
    assert window_top_score["available"] is False

    top_name_rows = [
        {
            "report_id": "R1",
            "canonical_name": "DOE|JANE",
            "display_name": "DOE, JANE",
            "current_n_records": 3,
            "report_count": 2,
            "max_n_records_across_reports": 4,
            "report_share": 0.5,
        },
        {
            "report_id": "R2",
            "canonical_name": "DOE|JANE",
            "display_name": "DOE, JANE",
            "current_n_records": 2,
            "report_count": 3,
            "max_n_records_across_reports": 5,
            "report_share": 0.75,
        },
    ]
    aggregates = module._aggregate_top_name_rows(
        top_name_rows=top_name_rows,
        indexed_report_count=4,
    )
    assert len(aggregates) == 1
    assert aggregates[0]["canonical_name"] == "DOE|JANE"
    assert aggregates[0]["appearance_report_count"] == 2
    assert aggregates[0]["report_count"] == 3
    assert aggregates[0]["total_n_records_across_reports"] == 5
    assert aggregates[0]["max_n_records_across_reports"] == 5


def test_build_baseline_atlas_payload_missing_file_is_unavailable(tmp_path: Path) -> None:
    module = _load_script_module()

    payload = module.build_baseline_atlas_payload(tmp_path / "reports")

    assert payload["available"] is False
    assert payload["reason"] == "global_baselines_missing"
    assert isinstance(payload["metric_catalog"], list)
    assert len(payload["metric_catalog"]) >= 1


def test_build_baseline_atlas_payload_invalid_file_is_unavailable(tmp_path: Path) -> None:
    module = _load_script_module()

    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "global_baselines.json").write_text(
        '{"schema_version": 1, "generated_at_utc": "2026-02-27T00:00:00Z"}',
        encoding="utf-8",
    )

    payload = module.build_baseline_atlas_payload(reports_dir)

    assert payload["available"] is False
    assert payload["reason"] == "global_baselines_invalid"
    assert isinstance(payload["metric_catalog"], list)
    assert len(payload["metric_catalog"]) >= 1


def test_build_entry_includes_duplicate_name_pct_and_status(tmp_path: Path) -> None:
    module = _load_script_module()

    report_id = "SB1234-20990101-0800"
    reports_dir = tmp_path / "reports"
    report_dir = reports_dir / report_id
    report_data_dir = report_dir / "report_data"
    metadata_dir = tmp_path / "data" / "metadata"
    report_data_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    (report_dir / "report.html").write_text("<html></html>", encoding="utf-8")
    (report_data_dir / "index.json").write_text(
        json.dumps(
            {
                "detector_summaries": {
                    "duplicates_exact": {
                        "duplicate_row_rate": 0.25,
                        "duplicate_rows": 2,
                        "n_records": 8,
                    },
                },
                "table_previews": {
                    "duplicates_exact": {
                        "per_name_duplicates_by_mode": [
                            {"match_mode": "strict", "total_repeated_rows": 2},
                            {"match_mode": "loose", "total_repeated_rows": 3},
                        ]
                    },
                    "voter_registry_match": {
                        "linkage_overview": [
                            {
                                "match_mode": "strict",
                                "n_rows": 8,
                                "n_matched_unique_rows": 4,
                                "n_matched_ambiguous_rows": 2,
                            },
                            {
                                "match_mode": "loose",
                                "n_rows": 8,
                                "n_matched_unique_rows": 5,
                                "n_matched_ambiguous_rows": 2,
                            },
                        ]
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    (metadata_dir / f"{report_id}.hearing.yaml").write_text(
        "\n".join(
            [
                "schema_version: 1",
                "meeting_start: '2099-01-01T08:00:00-08:00'",
                "stats:",
                "  total_rows: 10",
                "  total_pro_pct: 60.0",
                "  total_con_pct: 40.0",
                "source:",
                "  short_bill_id: SB 1234",
                "  agenda_item_description: Example bill",
            ]
        ),
        encoding="utf-8",
    )

    entry = module._build_entry(
        report_dir=report_dir,
        reports_dir=reports_dir,
        repo_root=tmp_path,
    )

    assert entry is not None
    assert entry.duplicate_name_pct == 25.0
    assert entry.duplicate_name_pct_exact == 25.0
    assert entry.duplicate_name_pct_loose == 37.5
    assert entry.duplicate_rows_exact == 2
    assert entry.duplicate_rows_loose == 3
    assert entry.duplicate_rows_total == 8
    assert entry.voter_match_pct_exact == 75.0
    assert entry.voter_match_pct_loose == 87.5
    assert entry.voter_match_rows_matched_exact == 6
    assert entry.voter_match_rows_matched_loose == 7
    assert entry.voter_match_rows_total_exact == 8
    assert entry.voter_match_rows_total_loose == 8
    assert entry.status == "open"


def test_render_index_embeds_baseline_hosts_and_payload() -> None:
    module = _load_script_module()

    entry = module.ReportEntry(
        report_id="SB1234-20260201-0800",
        report_href="./SB1234-20260201-0800/report.html",
        report_label="SB 1234",
        bill_description="Example bill",
        meeting_local="Feb 1, 2026 8:00 AM PST",
        meeting_epoch=1769961600,
        generated_local="Feb 27, 2026 6:00 PM PST",
        generated_epoch=1772244000,
        total_testifiers=25,
        pro_pct=60.0,
        con_pct=40.0,
        duplicate_name_pct=12.5,
        duplicate_name_pct_exact=12.5,
        duplicate_name_pct_loose=14.5,
        duplicate_rows_exact=5,
        duplicate_rows_loose=6,
        duplicate_rows_total=40,
        voter_match_pct_exact=85.0,
        voter_match_pct_loose=90.0,
        voter_match_rows_matched_exact=34,
        voter_match_rows_matched_loose=36,
        voter_match_rows_total_exact=40,
        voter_match_rows_total_loose=40,
        status="closed",
    )

    html = module.render_index(
        [entry],
        "Feb 27, 2026 6:00 PM PST",
        {
            "available": True,
            "reason": "",
            "summary": {"indexed_report_count": 1},
            "metric_catalog": [
                {
                    "metric": "total_submissions",
                    "label": "Total submissions",
                    "report_count": 1,
                    "missing_report_count": 0,
                    "available": True,
                    "coverage_pct": 1.0,
                }
            ],
            "comparator_rows": [],
            "top_name_rows": [],
            "by_report": {},
        },
    )

    assert 'id="baseline-atlas"' in html
    assert 'id="baseline-outlier-sections"' in html
    assert 'id="baseline-top-names-controls"' in html
    assert "const baselineAtlas =" in html
    assert '"report_id":"SB1234-20260201-0800"' in html
    assert '"status":"closed"' in html
    assert '"voter_match_pct_exact":85.0' in html
