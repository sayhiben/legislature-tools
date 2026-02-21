from __future__ import annotations

import pandas as pd

from testifier_audit.detectors.bursts import BurstsDetector
from testifier_audit.detectors.changepoints import ChangePointsDetector
from testifier_audit.detectors.periodicity import PeriodicityDetector
from testifier_audit.detectors.procon_swings import ProConSwingsDetector


def test_bursts_detector_flags_obvious_burst_window() -> None:
    minute_bucket = pd.date_range("2026-02-01 00:00:00", periods=180, freq="min")
    n_total = [1] * 180
    for idx in range(90, 95):
        n_total[idx] = 25

    counts = pd.DataFrame(
        {
            "minute_bucket": minute_bucket,
            "n_total": n_total,
            "n_unique_names": n_total,
            "n_pro": n_total,
            "n_con": [0] * 180,
            "n_unknown": [0] * 180,
            "dup_name_fraction": [0.0] * 180,
            "pro_rate": [1.0] * 180,
            "con_rate": [0.0] * 180,
            "unique_ratio": [1.0] * 180,
        }
    )

    detector = BurstsDetector(window_minutes=[5], fdr_alpha=0.05)
    result = detector.run(df=pd.DataFrame(), features={"counts_per_minute": counts})

    assert result.summary["n_significant_windows"] > 0
    assert not result.tables["burst_significant_windows"].empty
    assert "pro_rate" in result.tables["burst_window_tests"].columns
    assert "delta_pro_rate" in result.tables["burst_window_tests"].columns
    assert "abs_delta_pro_rate" in result.tables["burst_window_tests"].columns
    assert "pro_rate_wilson_low" in result.tables["burst_window_tests"].columns
    assert "pro_rate_wilson_high" in result.tables["burst_window_tests"].columns
    assert "is_low_power" in result.tables["burst_window_tests"].columns


def test_bursts_detector_captures_composition_shift_metrics() -> None:
    minute_bucket = pd.date_range("2026-02-01 00:00:00", periods=180, freq="min")
    n_total = [4] * 180
    n_pro = [2] * 180
    for idx in range(80, 90):
        n_total[idx] = 30
        n_pro[idx] = 27

    counts = pd.DataFrame(
        {
            "minute_bucket": minute_bucket,
            "n_total": n_total,
            "n_unique_names": n_total,
            "n_pro": n_pro,
            "n_con": [total - pro for total, pro in zip(n_total, n_pro)],
            "n_unknown": [0] * 180,
            "dup_name_fraction": [0.0] * 180,
            "pro_rate": [pro / total for total, pro in zip(n_total, n_pro)],
            "con_rate": [1.0 - (pro / total) for total, pro in zip(n_total, n_pro)],
            "unique_ratio": [1.0] * 180,
        }
    )

    detector = BurstsDetector(window_minutes=[5], fdr_alpha=0.05)
    result = detector.run(df=pd.DataFrame(), features={"counts_per_minute": counts})

    assert result.summary["n_significant_windows"] > 0
    assert result.summary["max_abs_delta_pro_rate"] > 0.0
    assert result.summary["max_significant_abs_delta_pro_rate"] > 0.0
    assert result.summary["n_significant_composition_shifts"] > 0


def test_bursts_detector_calibration_outputs_null_distribution() -> None:
    minute_bucket = pd.date_range("2026-02-01 00:00:00", periods=90, freq="min")
    n_total = [2] * 90
    for idx in range(30, 36):
        n_total[idx] = 15

    counts = pd.DataFrame(
        {
            "minute_bucket": minute_bucket,
            "n_total": n_total,
            "n_unique_names": n_total,
            "n_pro": n_total,
            "n_con": [0] * 90,
            "n_unknown": [0] * 90,
            "dup_name_fraction": [0.0] * 90,
            "pro_rate": [1.0] * 90,
            "con_rate": [0.0] * 90,
            "unique_ratio": [1.0] * 90,
        }
    )

    detector = BurstsDetector(
        window_minutes=[5],
        fdr_alpha=0.1,
        calibration_enabled=True,
        calibration_mode="hour_of_day",
        calibration_iterations=20,
        calibration_seed=7,
        calibration_support_alpha=0.2,
    )
    result = detector.run(df=pd.DataFrame(), features={"counts_per_minute": counts})

    assert result.summary["calibration_enabled"]
    assert not result.tables["burst_null_distribution"].empty
    assert "permutation_p_value" in result.tables["burst_window_tests"].columns
    assert "permutation_q_value" in result.tables["burst_window_tests"].columns
    assert "is_significant_permutation_fdr" in result.tables["burst_window_tests"].columns
    assert "is_calibration_supported" in result.tables["burst_window_tests"].columns


def test_bursts_detector_supports_day_of_week_hour_calibration() -> None:
    minute_bucket = pd.date_range("2026-02-01 00:00:00", periods=24 * 60 * 2, freq="min")
    n_total = [2] * len(minute_bucket)
    for idx in range(420, 480):
        n_total[idx] = 8

    counts = pd.DataFrame(
        {
            "minute_bucket": minute_bucket,
            "n_total": n_total,
            "n_unique_names": n_total,
            "n_pro": n_total,
            "n_con": [0] * len(minute_bucket),
            "n_unknown": [0] * len(minute_bucket),
            "dup_name_fraction": [0.0] * len(minute_bucket),
            "pro_rate": [1.0] * len(minute_bucket),
            "con_rate": [0.0] * len(minute_bucket),
            "unique_ratio": [1.0] * len(minute_bucket),
        }
    )

    detector = BurstsDetector(
        window_minutes=[15],
        fdr_alpha=0.1,
        calibration_enabled=True,
        calibration_mode="day_of_week_hour",
        calibration_iterations=15,
        calibration_seed=11,
        calibration_support_alpha=0.2,
    )
    result = detector.run(df=pd.DataFrame(), features={"counts_per_minute": counts})

    assert result.summary["calibration_enabled"]
    assert result.summary["calibration_mode"] == "day_of_week_hour"
    assert not result.tables["burst_null_distribution"].empty


def test_bursts_detector_permutation_policy_falls_back_without_calibration() -> None:
    minute_bucket = pd.date_range("2026-02-01 00:00:00", periods=180, freq="min")
    n_total = [1] * 180
    for idx in range(90, 95):
        n_total[idx] = 25

    counts = pd.DataFrame(
        {
            "minute_bucket": minute_bucket,
            "n_total": n_total,
            "n_unique_names": n_total,
            "n_pro": n_total,
            "n_con": [0] * 180,
            "n_unknown": [0] * 180,
            "dup_name_fraction": [0.0] * 180,
            "pro_rate": [1.0] * 180,
            "con_rate": [0.0] * 180,
            "unique_ratio": [1.0] * 180,
        }
    )

    detector = BurstsDetector(
        window_minutes=[5],
        fdr_alpha=0.05,
        calibration_enabled=False,
        significance_policy="permutation_fdr",
    )
    result = detector.run(df=pd.DataFrame(), features={"counts_per_minute": counts})

    assert result.summary["significance_policy_requested"] == "permutation_fdr"
    assert result.summary["significance_policy_effective"] == "parametric_fdr"
    assert result.summary["n_significant_windows"] > 0


def test_procon_swings_detector_flags_large_shift() -> None:
    minute_bucket = pd.date_range("2026-02-01 00:00:00", periods=180, freq="min")
    n_total = [20] * 180
    n_pro = [10] * 180
    for idx in range(120, 140):
        n_pro[idx] = 20

    counts = pd.DataFrame(
        {
            "minute_bucket": minute_bucket,
            "n_total": n_total,
            "n_unique_names": n_total,
            "n_pro": n_pro,
            "n_con": [total - pro for total, pro in zip(n_total, n_pro)],
            "n_unknown": [0] * 180,
            "dup_name_fraction": [0.0] * 180,
            "pro_rate": [pro / total for total, pro in zip(n_total, n_pro)],
            "con_rate": [1.0 - (pro / total) for total, pro in zip(n_total, n_pro)],
            "unique_ratio": [1.0] * 180,
        }
    )

    detector = ProConSwingsDetector(window_minutes=[15], fdr_alpha=0.05, min_window_total=200)
    result = detector.run(df=pd.DataFrame(), features={"counts_per_minute": counts})

    assert result.summary["n_significant_windows"] > 0
    assert not result.tables["swing_significant_windows"].empty
    assert not result.tables["time_bucket_profiles"].empty
    assert not result.tables["time_of_day_bucket_profiles"].empty
    assert not result.tables["day_bucket_profiles"].empty
    assert not result.tables["direction_runs"].empty
    assert not result.tables["direction_runs_summary"].empty
    assert "pro_rate_wilson_low" in result.tables["swing_window_tests"].columns
    assert "pro_rate_wilson_high" in result.tables["swing_window_tests"].columns
    assert "is_low_power" in result.tables["swing_window_tests"].columns
    assert "pro_rate_wilson_low" in result.tables["time_bucket_profiles"].columns
    assert "pro_rate_wilson_high" in result.tables["time_bucket_profiles"].columns
    assert "is_low_power" in result.tables["time_bucket_profiles"].columns
    assert "is_low_power" in result.tables["time_of_day_bucket_profiles"].columns
    assert "is_low_power" in result.tables["day_bucket_profiles"].columns
    assert "run_length_buckets" in result.tables["direction_runs"].columns
    assert "is_long_run" in result.tables["direction_runs"].columns
    assert "n_long_runs" in result.tables["direction_runs_summary"].columns
    assert result.summary["n_direction_runs"] > 0
    assert result.summary["max_direction_run_length"] >= 1
    assert set(
        result.tables["time_of_day_bucket_profiles"]["bucket_minutes"].astype(int).unique()
    ) >= {
        15,
        30,
        60,
        120,
        240,
    }


def test_procon_swings_calibration_outputs_empirical_columns() -> None:
    minute_bucket = pd.date_range("2026-02-01 00:00:00", periods=60, freq="min")
    n_total = [10] * 60
    n_pro = [5] * 60
    for idx in range(35, 45):
        n_pro[idx] = 9

    counts = pd.DataFrame(
        {
            "minute_bucket": minute_bucket,
            "n_total": n_total,
            "n_unique_names": n_total,
            "n_pro": n_pro,
            "n_con": [total - pro for total, pro in zip(n_total, n_pro)],
            "n_unknown": [0] * 60,
            "dup_name_fraction": [0.0] * 60,
            "pro_rate": [pro / total for total, pro in zip(n_total, n_pro)],
            "con_rate": [1.0 - (pro / total) for total, pro in zip(n_total, n_pro)],
            "unique_ratio": [1.0] * 60,
        }
    )

    rows: list[dict[str, object]] = []
    for minute, total, pro in zip(minute_bucket, n_total, n_pro):
        rows.extend({"minute_bucket": minute, "position_normalized": "Pro"} for _ in range(pro))
        rows.extend(
            {"minute_bucket": minute, "position_normalized": "Con"} for _ in range(total - pro)
        )
    df = pd.DataFrame(rows)

    detector = ProConSwingsDetector(
        window_minutes=[10],
        fdr_alpha=0.1,
        min_window_total=70,
        calibration_enabled=True,
        calibration_mode="hour_of_day",
        calibration_iterations=20,
        calibration_seed=7,
        calibration_support_alpha=0.2,
    )
    result = detector.run(df=df, features={"counts_per_minute": counts})

    assert result.summary["calibration_enabled"]
    assert not result.tables["swing_null_distribution"].empty
    assert "permutation_p_value" in result.tables["swing_window_tests"].columns
    assert "permutation_q_value" in result.tables["swing_window_tests"].columns
    assert "is_significant_permutation_fdr" in result.tables["swing_window_tests"].columns
    assert "is_calibration_supported" in result.tables["swing_window_tests"].columns
    assert "is_flagged" in result.tables["time_bucket_profiles"].columns
    assert "is_flagged" in result.tables["time_of_day_bucket_profiles"].columns
    assert "is_slot_outlier" in result.tables["day_bucket_profiles"].columns
    assert "run_direction" in result.tables["direction_runs"].columns
    assert "mean_abs_delta_pro_rate" in result.tables["direction_runs"].columns
    assert "pro_rate_wilson_half_width" in result.tables["time_bucket_profiles"].columns
    assert "is_low_power" in result.tables["time_bucket_profiles"].columns


def test_procon_swings_direction_runs_capture_long_streaks() -> None:
    minute_bucket = pd.date_range("2026-02-01 00:00:00", periods=180, freq="min")
    n_total = [20] * 180
    n_pro = [10] * 180
    for idx in range(0, 60):
        n_pro[idx] = 16
    for idx in range(60, 120):
        n_pro[idx] = 4
    for idx in range(120, 180):
        n_pro[idx] = 15

    counts = pd.DataFrame(
        {
            "minute_bucket": minute_bucket,
            "n_total": n_total,
            "n_unique_names": n_total,
            "n_pro": n_pro,
            "n_con": [total - pro for total, pro in zip(n_total, n_pro)],
            "n_unknown": [0] * 180,
            "dup_name_fraction": [0.0] * 180,
            "pro_rate": [pro / total for total, pro in zip(n_total, n_pro)],
            "con_rate": [1.0 - (pro / total) for total, pro in zip(n_total, n_pro)],
            "unique_ratio": [1.0] * 180,
        }
    )

    detector = ProConSwingsDetector(window_minutes=[15], fdr_alpha=0.05, min_window_total=200)
    result = detector.run(df=pd.DataFrame(), features={"counts_per_minute": counts})

    runs = result.tables["direction_runs"]
    assert not runs.empty
    assert set(runs["run_direction"].astype(str).unique()) <= {"pro_heavy", "con_heavy"}
    assert int(runs["run_length_buckets"].max()) >= 3
    assert result.summary["n_long_direction_runs"] >= 1
    assert result.summary["max_direction_run_mean_abs_delta"] > 0.0


def test_procon_swings_supports_day_of_week_hour_calibration() -> None:
    minute_bucket = pd.date_range("2026-02-01 00:00:00", periods=24 * 60 * 2, freq="min")
    n_total = [8] * len(minute_bucket)
    n_pro = [4] * len(minute_bucket)
    for idx in range(1080, 1140):
        n_pro[idx] = 7

    counts = pd.DataFrame(
        {
            "minute_bucket": minute_bucket,
            "n_total": n_total,
            "n_unique_names": n_total,
            "n_pro": n_pro,
            "n_con": [total - pro for total, pro in zip(n_total, n_pro)],
            "n_unknown": [0] * len(minute_bucket),
            "dup_name_fraction": [0.0] * len(minute_bucket),
            "pro_rate": [pro / total for total, pro in zip(n_total, n_pro)],
            "con_rate": [1.0 - (pro / total) for total, pro in zip(n_total, n_pro)],
            "unique_ratio": [1.0] * len(minute_bucket),
        }
    )

    detector = ProConSwingsDetector(
        window_minutes=[30],
        fdr_alpha=0.1,
        min_window_total=150,
        calibration_enabled=True,
        calibration_mode="day_of_week_hour",
        calibration_iterations=15,
        calibration_seed=11,
        calibration_support_alpha=0.2,
    )
    result = detector.run(df=pd.DataFrame(), features={"counts_per_minute": counts})

    assert result.summary["calibration_enabled"]
    assert result.summary["calibration_mode"] == "day_of_week_hour"
    assert not result.tables["swing_null_distribution"].empty


def test_procon_swings_permutation_policy_uses_permutation_column() -> None:
    minute_bucket = pd.date_range("2026-02-01 00:00:00", periods=60, freq="min")
    n_total = [10] * 60
    n_pro = [5] * 60
    for idx in range(35, 45):
        n_pro[idx] = 9

    counts = pd.DataFrame(
        {
            "minute_bucket": minute_bucket,
            "n_total": n_total,
            "n_unique_names": n_total,
            "n_pro": n_pro,
            "n_con": [total - pro for total, pro in zip(n_total, n_pro)],
            "n_unknown": [0] * 60,
            "dup_name_fraction": [0.0] * 60,
            "pro_rate": [pro / total for total, pro in zip(n_total, n_pro)],
            "con_rate": [1.0 - (pro / total) for total, pro in zip(n_total, n_pro)],
            "unique_ratio": [1.0] * 60,
        }
    )

    detector = ProConSwingsDetector(
        window_minutes=[10],
        fdr_alpha=0.1,
        min_window_total=70,
        calibration_enabled=True,
        calibration_mode="hour_of_day",
        significance_policy="permutation_fdr",
        calibration_iterations=20,
        calibration_seed=7,
        calibration_support_alpha=0.2,
    )
    result = detector.run(df=pd.DataFrame(), features={"counts_per_minute": counts})

    tests = result.tables["swing_window_tests"]
    assert result.summary["significance_policy_effective"] == "permutation_fdr"
    assert tests["is_significant"].equals(tests["is_significant_permutation_fdr"])


def test_changepoints_detector_finds_shift() -> None:
    minute_bucket = pd.date_range("2026-02-01 00:00:00", periods=200, freq="min")
    n_total = [5] * 100 + [20] * 100
    n_pro = [2] * 100 + [16] * 100

    counts = pd.DataFrame(
        {
            "minute_bucket": minute_bucket,
            "n_total": n_total,
            "n_unique_names": [5] * 200,
            "n_pro": n_pro,
            "n_con": [total - pro for total, pro in zip(n_total, n_pro)],
            "n_unknown": [0] * 200,
            "dup_name_fraction": [0.0] * 200,
            "pro_rate": [pro / total for total, pro in zip(n_total, n_pro)],
            "con_rate": [1.0 - (pro / total) for total, pro in zip(n_total, n_pro)],
            "unique_ratio": [1.0] * 200,
        }
    )

    detector = ChangePointsDetector(min_segment_minutes=20, penalty_scale=2.0)
    result = detector.run(df=pd.DataFrame(), features={"counts_per_minute": counts})

    assert result.summary["n_volume_changepoints"] >= 1
    assert not result.tables["volume_changepoints"].empty


def test_periodicity_detector_calibration_outputs_significance_tables() -> None:
    minute_bucket = pd.date_range("2026-02-01 00:00:00", periods=300, freq="min")
    n_total = [2 + (8 if (idx % 10 == 0) else 0) for idx in range(300)]

    counts = pd.DataFrame(
        {
            "minute_bucket": minute_bucket,
            "n_total": n_total,
            "n_unique_names": n_total,
            "n_pro": n_total,
            "n_con": [0] * 300,
            "n_unknown": [0] * 300,
            "dup_name_fraction": [0.0] * 300,
            "pro_rate": [1.0] * 300,
            "con_rate": [0.0] * 300,
            "unique_ratio": [1.0] * 300,
        }
    )

    detector = PeriodicityDetector(
        max_lag_minutes=60,
        min_period_minutes=2.0,
        max_period_minutes=120.0,
        top_n_periods=12,
        calibration_iterations=40,
        calibration_seed=11,
        fdr_alpha=0.1,
    )
    result = detector.run(df=pd.DataFrame(), features={"counts_per_minute": counts})

    assert not result.tables["autocorr"].empty
    assert not result.tables["spectrum_top"].empty
    assert not result.tables["periodicity_null_distribution"].empty
    assert not result.tables["clockface_distribution"].empty
    assert not result.tables["clockface_top_minutes"].empty
    assert not result.tables["clockface_null_distribution"].empty
    assert not result.tables["rolling_fano"].empty
    assert not result.tables["rolling_fano_summary"].empty
    assert len(result.tables["clockface_distribution"]) == 60
    assert "p_value" in result.tables["autocorr"].columns
    assert "q_value" in result.tables["autocorr"].columns
    assert "is_significant" in result.tables["spectrum_top"].columns
    assert "fano_factor" in result.tables["rolling_fano"].columns
    assert "is_high_fano" in result.tables["rolling_fano"].columns
    assert "median_fano_factor" in result.tables["rolling_fano_summary"].columns
    assert "max_fano_factor" in result.tables["rolling_fano_summary"].columns
    assert result.summary["strongest_period_minutes"] is not None
    assert result.summary["max_rolling_fano_factor"] >= 0.0
    assert result.summary["median_rolling_fano_factor"] >= 0.0
    assert result.summary["n_high_fano_windows"] >= 0
    assert result.summary["high_fano_threshold"] >= 0.0
    assert result.summary["clockface_chi_square"] >= 0.0
    assert 0.0 <= result.summary["clockface_chi_square_p_value"] <= 1.0
    assert result.summary["clockface_chi_square_empirical_p_value"] is not None
    assert 0.0 <= result.summary["clockface_max_share"] <= 1.0
