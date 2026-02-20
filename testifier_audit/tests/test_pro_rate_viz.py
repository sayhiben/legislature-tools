from __future__ import annotations

from pathlib import Path

import pandas as pd

from testifier_audit.viz.time_series import (
    plot_pro_rate_bucket_trends,
    plot_pro_rate_with_annotations,
    plot_time_of_day_ratio_profiles,
)


def test_plot_pro_rate_with_annotations_writes_file(tmp_path: Path) -> None:
    counts = pd.DataFrame(
        {
            "minute_bucket": pd.date_range("2026-02-01 00:00:00", periods=6, freq="min"),
            "n_total": [8, 40, 42, 6, 44, 45],
            "n_pro": [4, 20, 21, 3, 24, 23],
            "pro_rate": [0.5, 0.5, 0.5, 0.5, 0.545, 0.511],
        }
    )
    swings = pd.DataFrame(
        {
            "start_minute": [pd.Timestamp("2026-02-01 00:01:00")],
            "end_minute": [pd.Timestamp("2026-02-01 00:03:00")],
        }
    )
    changepoints = pd.DataFrame({"change_minute": [pd.Timestamp("2026-02-01 00:04:00")]})
    output_path = tmp_path / "pro_rate_with_anomalies.png"

    result = plot_pro_rate_with_annotations(
        counts_per_minute=counts,
        swing_windows=swings,
        pro_rate_changepoints=changepoints,
        output_path=output_path,
    )

    assert result == output_path
    assert output_path.exists()


def test_plot_pro_rate_bucket_trends_writes_file(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "bucket_minutes": [60, 60, 60, 60],
            "bucket_start": pd.to_datetime(
                [
                    "2026-02-01 00:00:00",
                    "2026-02-01 01:00:00",
                    "2026-02-01 02:00:00",
                    "2026-02-01 03:00:00",
                ]
            ),
            "n_total": [15, 55, 60, 20],
            "n_pro": [7, 28, 40, 8],
            "pro_rate": [0.4667, 0.5090, 0.6667, 0.4],
            "baseline_pro_rate": [0.53, 0.53, 0.53, 0.53],
            "stable_lower": [0.45, 0.45, 0.45, 0.45],
            "stable_upper": [0.60, 0.60, 0.60, 0.60],
            "is_flagged": [False, False, True, False],
            "is_low_power": [True, False, False, True],
            "pro_rate_wilson_low": [0.246, 0.379, 0.540, 0.217],
            "pro_rate_wilson_high": [0.719, 0.633, 0.771, 0.613],
        }
    )
    output_path = tmp_path / "pro_rate_bucket_trends.png"
    result = plot_pro_rate_bucket_trends(time_bucket_profiles=frame, output_path=output_path)
    assert result == output_path
    assert output_path.exists()


def test_plot_pro_rate_bucket_trends_single_bucket_variant_writes_file(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "bucket_minutes": [5, 5, 5, 5, 60, 60],
            "bucket_start": pd.to_datetime(
                [
                    "2026-02-01 00:00:00",
                    "2026-02-01 00:05:00",
                    "2026-02-01 00:10:00",
                    "2026-02-01 00:15:00",
                    "2026-02-01 00:00:00",
                    "2026-02-01 01:00:00",
                ]
            ),
            "n_total": [9, 22, 30, 11, 61, 44],
            "n_pro": [4, 10, 15, 7, 29, 18],
            "pro_rate": [0.4444, 0.4545, 0.5, 0.6364, 0.4754, 0.4091],
            "baseline_pro_rate": [0.48, 0.48, 0.48, 0.48, 0.48, 0.48],
            "stable_lower": [0.38, 0.38, 0.38, 0.38, 0.38, 0.38],
            "stable_upper": [0.58, 0.58, 0.58, 0.58, 0.58, 0.58],
            "is_flagged": [False, False, False, True, False, False],
            "is_low_power": [True, False, False, True, False, False],
            "pro_rate_wilson_low": [0.188, 0.271, 0.332, 0.354, 0.353, 0.276],
            "pro_rate_wilson_high": [0.739, 0.658, 0.668, 0.845, 0.600, 0.557],
        }
    )
    output_path = tmp_path / "pro_rate_bucket_trends_5m.png"
    result = plot_pro_rate_bucket_trends(
        time_bucket_profiles=frame,
        output_path=output_path,
        preferred_buckets=(5,),
    )
    assert result == output_path
    assert output_path.exists()


def test_plot_time_of_day_ratio_profiles_writes_file(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "bucket_minutes": [30, 30, 30, 30],
            "slot_start_minute": [0, 30, 60, 90],
            "n_total": [12, 35, 40, 10],
            "n_pro": [5, 18, 26, 4],
            "pro_rate": [0.4167, 0.5143, 0.65, 0.4],
            "baseline_pro_rate": [0.52, 0.52, 0.52, 0.52],
            "stable_lower": [0.42, 0.42, 0.42, 0.42],
            "stable_upper": [0.62, 0.62, 0.62, 0.62],
            "is_flagged": [False, False, True, False],
            "is_low_power": [True, False, False, True],
            "pro_rate_wilson_low": [0.193, 0.352, 0.495, 0.168],
            "pro_rate_wilson_high": [0.681, 0.675, 0.784, 0.687],
        }
    )
    output_path = tmp_path / "pro_rate_time_of_day_profiles.png"
    result = plot_time_of_day_ratio_profiles(
        time_of_day_bucket_profiles=frame, output_path=output_path
    )
    assert result == output_path
    assert output_path.exists()
