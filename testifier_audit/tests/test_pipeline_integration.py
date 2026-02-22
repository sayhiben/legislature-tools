from __future__ import annotations

import re
from pathlib import Path

import yaml
from typer.testing import CliRunner

from testifier_audit.cli import app
from testifier_audit.report.analysis_registry import (
    ANALYSES_TO_PERFORM,
    configured_detector_names,
    default_analysis_definitions,
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


def test_run_all_generates_report_and_outputs(tmp_path: Path) -> None:
    workspace = Path(__file__).resolve().parents[1]

    csv_path = tmp_path / "sample.csv"
    csv_path.write_text(
        "\n".join(
            [
                "Count,Name,Organization,Position,Time Signed In",
                '1,"Doe, Jane",,Pro,2/3/2026 5:07 PM',
                '2,"Doe, Jane",,Con,2/3/2026 5:08 PM',
                '3,"Smith, John",Org A,Pro,2/3/2026 5:09 PM',
                '4,"Smyth, Jon",Org A,Pro,2/3/2026 5:10 PM',
                '5,"Brown, Ava",,Con,2/3/2026 5:12 PM',
                '6,"Brown, Ava",,Con,2/3/2026 5:13 PM',
            ]
        ),
        encoding="utf-8",
    )

    first_freq_path = tmp_path / "first_freq.csv"
    first_freq_path.write_text(
        "\n".join(
            [
                "name,count",
                "JANE,1000",
                "JOHN,1200",
                "AVA,500",
                "JON,300",
            ]
        ),
        encoding="utf-8",
    )
    last_freq_path = tmp_path / "last_freq.csv"
    last_freq_path.write_text(
        "\n".join(
            [
                "name,count",
                "DOE,1500",
                "SMITH,2200",
                "SMYTH,250",
                "BROWN,1800",
            ]
        ),
        encoding="utf-8",
    )

    config = {
        "columns": {
            "id": "Count",
            "name": "Name",
            "organization": "Organization",
            "position": "Position",
            "time_signed_in": "Time Signed In",
        },
        "time": {
            "timezone": "America/Los_Angeles",
            "floor": "minute",
            "off_hours_start": 0,
            "off_hours_end": 5,
        },
        "windows": {
            "minute_series_smooth": 15,
            "swing_window_minutes": 10,
            "scan_window_minutes": [5, 10],
        },
        "thresholds": {
            "top_duplicate_names": 20,
            "burst_fdr_alpha": 0.1,
            "procon_swing_fdr_alpha": 0.1,
            "near_dup_max_candidates_per_block": 500,
            "near_dup_similarity_threshold": 90,
            "swing_min_window_total": 4,
        },
        "calibration": {
            "enabled": True,
            "mode": "hour_of_day",
            "significance_policy": "either_fdr",
            "iterations": 5,
            "random_seed": 7,
            "support_alpha": 0.2,
        },
        "changepoints": {
            "enabled": True,
            "min_segment_minutes": 2,
            "penalty_scale": 1.5,
        },
        "names": {
            "strip_punctuation": True,
            "normalize_unicode": True,
            "nickname_map_path": str(workspace / "configs" / "nicknames.csv"),
            "phonetic": "double_metaphone",
        },
        "rarity": {
            "enabled": True,
            "first_name_frequency_path": first_freq_path.name,
            "last_name_frequency_path": last_freq_path.name,
            "epsilon": 1e-9,
        },
        "outputs": {
            "tables_format": "csv",
            "figures_format": "png",
            "interactive_plotly": False,
        },
    }

    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    out_dir = tmp_path / "out"
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run-all",
            "--csv",
            str(csv_path),
            "--out",
            str(out_dir),
            "--config",
            str(config_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert (out_dir / "report.html").exists()
    assert (out_dir / "report_data" / "index.json").exists()
    assert any((out_dir / "report_data" / "analyses").rglob("base.json"))
    assert (out_dir / "summary" / "investigation_summary.json").exists()
    assert (out_dir / "summary" / "feature_vector.json").exists()
    enabled_detector_names = configured_detector_names()
    if _configured_focus_analysis_ids():
        assert enabled_detector_names
        for detector_name in enabled_detector_names:
            assert (out_dir / "summary" / f"{detector_name}.json").exists()
        assert (out_dir / "summary" / "off_hours.json").exists()
        assert any(
            path.name.startswith("off_hours__")
            for path in (out_dir / "tables").glob("*.csv")
        )
        for detector_name in (
            "bursts",
            "procon_swings",
            "changepoints",
            "duplicates_exact",
            "duplicates_near",
            "sortedness",
            "rare_names",
            "org_anomalies",
            "voter_registry_match",
            "periodicity",
            "multivariate_anomalies",
            "composite_score",
        ):
            if detector_name in enabled_detector_names:
                continue
            assert not (out_dir / "summary" / f"{detector_name}.json").exists()
        assert not any((out_dir / "figures").glob("*.png"))
    else:
        assert (out_dir / "summary" / "bursts.json").exists()
        assert (out_dir / "summary" / "procon_swings.json").exists()
        assert (out_dir / "summary" / "changepoints.json").exists()
        assert (out_dir / "summary" / "voter_registry_match.json").exists()
        assert (out_dir / "summary" / "multivariate_anomalies.json").exists()
        assert (out_dir / "tables" / "bursts__burst_window_tests.csv").exists()
        assert (out_dir / "tables" / "procon_swings__swing_window_tests.csv").exists()
        assert (out_dir / "tables" / "duplicates_exact__repeated_same_bucket.csv").exists()
        assert (out_dir / "tables" / "duplicates_exact__repeated_same_bucket_summary.csv").exists()
        assert (out_dir / "tables" / "sortedness__bucket_ordering.csv").exists()
        assert (out_dir / "tables" / "sortedness__bucket_ordering_summary.csv").exists()
        assert (out_dir / "tables" / "procon_swings__time_bucket_profiles.csv").exists()
        assert (out_dir / "tables" / "procon_swings__time_of_day_bucket_profiles.csv").exists()
        assert (out_dir / "tables" / "procon_swings__day_bucket_profiles.csv").exists()
        assert (
            out_dir / "tables" / "org_anomalies__organization_blank_rate_by_bucket.csv"
        ).exists()
        assert (
            out_dir / "tables" / "org_anomalies__organization_blank_rate_by_bucket_position.csv"
        ).exists()
        assert (out_dir / "tables" / "org_anomalies__organization_blank_rate_summary.csv").exists()
        assert (out_dir / "tables" / "voter_registry_match__match_overview.csv").exists()
        assert (out_dir / "tables" / "multivariate_anomalies__bucket_anomaly_scores.csv").exists()
        assert (out_dir / "tables" / "multivariate_anomalies__top_bucket_anomalies.csv").exists()
        assert (out_dir / "tables" / "changepoints__all_changepoints.csv").exists()
        assert (out_dir / "tables" / "rare_names__rarity_by_minute.csv").exists()
        assert (out_dir / "tables" / "rare_names__rarity_top_records.csv").exists()
        assert (out_dir / "tables" / "rare_names__rarity_lookup_coverage.csv").exists()
        assert (out_dir / "tables" / "composite_score__evidence_bundle_windows.csv").exists()
        assert (out_dir / "tables" / "triage__window_evidence_queue.csv").exists()
        assert (out_dir / "tables" / "triage__record_evidence_queue.csv").exists()
        assert (out_dir / "tables" / "triage__cluster_evidence_queue.csv").exists()
        assert (out_dir / "tables" / "data_quality__raw_vs_dedup_metrics.csv").exists()
        assert (out_dir / "figures" / "counts_with_anomalies.png").exists()
        assert (out_dir / "figures" / "pro_rate_with_anomalies.png").exists()
        assert (out_dir / "figures" / "pro_rate_heatmap_day_hour.png").exists()
        assert (out_dir / "figures" / "pro_rate_heatmap_day_hour_1m.png").exists()
        assert (out_dir / "figures" / "pro_rate_heatmap_day_hour_5m.png").exists()
        assert (out_dir / "figures" / "pro_rate_heatmap_day_hour_15m.png").exists()
        assert (out_dir / "figures" / "pro_rate_heatmap_day_hour_30m.png").exists()
        assert (out_dir / "figures" / "pro_rate_heatmap_day_hour_60m.png").exists()
        assert (out_dir / "figures" / "pro_rate_heatmap_day_hour_120m.png").exists()
        assert (out_dir / "figures" / "pro_rate_heatmap_day_hour_240m.png").exists()
        assert (out_dir / "figures" / "pro_rate_shift_heatmap_1m.png").exists()
        assert (out_dir / "figures" / "pro_rate_shift_heatmap_5m.png").exists()
        assert (out_dir / "figures" / "pro_rate_shift_heatmap_15m.png").exists()
        assert (out_dir / "figures" / "pro_rate_shift_heatmap_30m.png").exists()
        assert (out_dir / "figures" / "pro_rate_shift_heatmap_60m.png").exists()
        assert (out_dir / "figures" / "pro_rate_shift_heatmap_120m.png").exists()
        assert (out_dir / "figures" / "pro_rate_shift_heatmap_240m.png").exists()
        assert (out_dir / "figures" / "pro_rate_bucket_trends.png").exists()
        assert (out_dir / "figures" / "pro_rate_bucket_trends_1m.png").exists()
        assert (out_dir / "figures" / "pro_rate_bucket_trends_5m.png").exists()
        assert (out_dir / "figures" / "pro_rate_bucket_trends_15m.png").exists()
        assert (out_dir / "figures" / "pro_rate_bucket_trends_30m.png").exists()
        assert (out_dir / "figures" / "pro_rate_bucket_trends_60m.png").exists()
        assert (out_dir / "figures" / "pro_rate_bucket_trends_120m.png").exists()
        assert (out_dir / "figures" / "pro_rate_bucket_trends_240m.png").exists()
        assert (out_dir / "figures" / "pro_rate_time_of_day_profiles.png").exists()
        assert (out_dir / "figures" / "organization_blank_rates.png").exists()
        assert (out_dir / "figures" / "bursts_null_distribution.png").exists()
        assert (out_dir / "figures" / "swing_null_distribution.png").exists()
        assert (out_dir / "figures" / "periodicity_autocorr.png").exists()
        assert (out_dir / "figures" / "periodicity_spectrum.png").exists()
        assert (out_dir / "figures" / "periodicity_clockface.png").exists()
        assert (out_dir / "figures" / "multivariate_anomaly_scores.png").exists()
    report_text = (out_dir / "report.html").read_text(encoding="utf-8")
    if _is_off_hours_only_view():
        assert 'data-analysis-id="off_hours"' in report_text
        assert 'data-analysis-id="composite_score"' not in report_text
        assert 'data-analysis-id="rare_names"' not in report_text
        assert 'data-analysis-id="periodicity"' not in report_text
    else:
        assert "Composite Evidence Score" in report_text
        assert "Rare / Unique Names" in report_text
        assert "Periodicity" in report_text
    assert "Static Figure Exports" not in report_text
    expected_definitions = default_analysis_definitions()
    configured_ids = set(_configured_focus_analysis_ids())
    if configured_ids:
        expected_definitions = [
            entry for entry in expected_definitions if str(entry.get("id") or "") in configured_ids
        ]
    expected_analyses = {entry["id"] for entry in expected_definitions}
    expected_hero_ids = {entry["hero_chart_id"] for entry in expected_definitions}
    rendered_analysis_ids = set(re.findall(r'data-analysis-id="([^"]+)"', report_text))
    rendered_hero_ids = set(
        re.findall(r'data-chart-id="([^"]+)"\s+data-chart-role="hero"', report_text)
    )
    assert rendered_analysis_ids == expected_analyses
    assert rendered_hero_ids == expected_hero_ids

    report_result = runner.invoke(
        app,
        [
            "report",
            "--out",
            str(out_dir),
            "--config",
            str(config_path),
        ],
    )
    assert report_result.exit_code == 0, report_result.stdout
    reloaded_report_text = (out_dir / "report.html").read_text(encoding="utf-8")
    if _is_off_hours_only_view():
        assert 'data-analysis-id="off_hours"' in reloaded_report_text
        assert 'data-analysis-id="composite_score"' not in reloaded_report_text
        assert 'data-analysis-id="periodicity"' not in reloaded_report_text
    else:
        assert "Composite Evidence Score" in reloaded_report_text
        assert "Periodicity" in reloaded_report_text
    assert "Static Figure Exports" not in reloaded_report_text
    reloaded_analysis_ids = set(re.findall(r'data-analysis-id="([^"]+)"', reloaded_report_text))
    reloaded_hero_ids = set(
        re.findall(r'data-chart-id="([^"]+)"\s+data-chart-role="hero"', reloaded_report_text)
    )
    assert reloaded_analysis_ids == expected_analyses
    assert reloaded_hero_ids == expected_hero_ids
