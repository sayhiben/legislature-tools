from __future__ import annotations

from pathlib import Path

import pandas as pd

from testifier_audit.viz.heatmaps import (
    plot_pro_rate_day_hour_heatmap,
    plot_ratio_shift_heatmap_by_bucket,
)


def test_plot_pro_rate_day_hour_heatmap_writes_file(tmp_path: Path) -> None:
    counts = pd.DataFrame(
        {
            "minute_bucket": pd.to_datetime(
                [
                    "2026-02-01 00:00:00",
                    "2026-02-01 00:01:00",
                    "2026-02-01 01:00:00",
                    "2026-02-02 00:00:00",
                ]
            ),
            "n_total": [10, 10, 40, 50],
            "n_pro": [5, 6, 18, 25],
        }
    )
    output_path = tmp_path / "pro_rate_heatmap_day_hour.png"
    result = plot_pro_rate_day_hour_heatmap(counts_per_minute=counts, output_path=output_path)
    assert result == output_path
    assert output_path.exists()


def test_plot_pro_rate_day_hour_heatmap_bucketed_writes_file(tmp_path: Path) -> None:
    counts = pd.DataFrame(
        {
            "minute_bucket": pd.to_datetime(
                [
                    "2026-02-01 00:00:00",
                    "2026-02-01 00:05:00",
                    "2026-02-01 00:10:00",
                    "2026-02-02 00:00:00",
                ]
            ),
            "n_total": [10, 20, 40, 50],
            "n_pro": [5, 6, 18, 25],
        }
    )
    output_path = tmp_path / "pro_rate_heatmap_day_hour_5m.png"
    result = plot_pro_rate_day_hour_heatmap(
        counts_per_minute=counts,
        output_path=output_path,
        bucket_minutes=5,
    )
    assert result == output_path
    assert output_path.exists()


def test_plot_ratio_shift_heatmap_by_bucket_writes_file(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "bucket_minutes": [15, 15, 15, 15],
            "date": ["2026-02-01", "2026-02-01", "2026-02-02", "2026-02-02"],
            "slot_start_minute": [0, 15, 0, 15],
            "delta_from_slot_pro_rate": [0.10, -0.05, 0.0, 0.08],
            "is_low_power": [False, True, False, False],
        }
    )
    output_path = tmp_path / "pro_rate_shift_heatmap_15m.png"
    result = plot_ratio_shift_heatmap_by_bucket(
        day_bucket_profiles=frame,
        bucket_minutes=15,
        output_path=output_path,
    )
    assert result == output_path
    assert output_path.exists()
