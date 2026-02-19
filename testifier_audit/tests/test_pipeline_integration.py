from __future__ import annotations

from pathlib import Path

import yaml
from typer.testing import CliRunner

from testifier_audit.cli import app


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
    assert (out_dir / "summary" / "bursts.json").exists()
    assert (out_dir / "summary" / "procon_swings.json").exists()
    assert (out_dir / "summary" / "changepoints.json").exists()
    assert (out_dir / "tables" / "bursts__burst_window_tests.csv").exists()
    assert (out_dir / "tables" / "procon_swings__swing_window_tests.csv").exists()
    assert (out_dir / "tables" / "changepoints__all_changepoints.csv").exists()
    assert (out_dir / "tables" / "rare_names__rarity_by_minute.csv").exists()
    assert (out_dir / "tables" / "rare_names__rarity_top_records.csv").exists()
    assert (out_dir / "tables" / "rare_names__rarity_lookup_coverage.csv").exists()
    assert (out_dir / "tables" / "composite_score__evidence_bundle_windows.csv").exists()
    assert (out_dir / "figures" / "counts_with_anomalies.png").exists()
    assert (out_dir / "figures" / "pro_rate_with_anomalies.png").exists()
    assert (out_dir / "figures" / "bursts_null_distribution.png").exists()
    assert (out_dir / "figures" / "swing_null_distribution.png").exists()
    assert (out_dir / "figures" / "periodicity_autocorr.png").exists()
    assert (out_dir / "figures" / "periodicity_spectrum.png").exists()
    assert (out_dir / "figures" / "periodicity_clockface.png").exists()
    report_text = (out_dir / "report.html").read_text(encoding="utf-8")
    assert "Evidence Bundle Windows" in report_text
    assert "Rarity Coverage" in report_text
    assert "Clock-face Timing" in report_text

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
    assert "Evidence Bundle Windows" in reloaded_report_text
    assert "Clock-face Timing" in reloaded_report_text
