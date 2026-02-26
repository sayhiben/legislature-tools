from __future__ import annotations

import pandas as pd

from testifier_audit.detectors.off_hours import OffHoursDetector


def test_off_hours_detector_emits_wilson_and_low_power_columns() -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "position_normalized": ["Pro", "Con", "Pro", "Con", "Pro", "Con"],
            "is_off_hours": [True, True, False, False, False, False],
            "hour": [2, 2, 10, 10, 11, 11],
            "day_of_week": [5, 5, 5, 5, 5, 5],
            "timestamp": pd.to_datetime(
                [
                    "2026-02-05T02:05:00Z",
                    "2026-02-05T02:25:00Z",
                    "2026-02-05T10:10:00Z",
                    "2026-02-05T10:30:00Z",
                    "2026-02-05T11:10:00Z",
                    "2026-02-05T11:25:00Z",
                ]
            ),
            "minute_bucket": pd.to_datetime(
                [
                    "2026-02-05T02:05:00Z",
                    "2026-02-05T02:25:00Z",
                    "2026-02-05T10:10:00Z",
                    "2026-02-05T10:30:00Z",
                    "2026-02-05T11:10:00Z",
                    "2026-02-05T11:25:00Z",
                ]
            ),
        }
    )

    result = OffHoursDetector().run(df=df, features={})

    summary_table = result.tables["off_hours_summary"]
    hourly = result.tables["hourly_distribution"]
    hour_of_week = result.tables["hour_of_week_distribution"]
    date_hour = result.tables["date_hour_distribution"]
    date_hour_primary_residual = result.tables["date_hour_primary_residual_distribution"]
    window_profile = result.tables["window_control_profile"]
    model_fit_diagnostics = result.tables["model_fit_diagnostics"]
    flag_channel_summary = result.tables["flag_channel_summary"]
    flagged_window_diagnostics = result.tables["flagged_window_diagnostics"]

    assert "off_hours_pro_rate_wilson_low" in summary_table.columns
    assert "off_hours_pro_rate_wilson_high" in summary_table.columns
    assert "off_hours_is_low_power" in summary_table.columns
    assert "primary_bucket_minutes" in summary_table.columns
    assert "alert_off_hours_min_fraction" in summary_table.columns
    assert "off_hours_windows_tested" in summary_table.columns
    assert "off_hours_windows_alert_eligible" in summary_table.columns
    assert "off_hours_windows_alert_eligible_low_power" in summary_table.columns
    assert "off_hours_windows_alert_eligible_tested_fraction" in summary_table.columns
    assert "off_hours_windows_alert_eligible_low_power_fraction" in summary_table.columns
    assert "off_hours_windows_primary_spc_998_any" in summary_table.columns
    assert "off_hours_windows_primary_fdr_two_sided" in summary_table.columns
    assert "off_hours_windows_primary_flag_any" in summary_table.columns
    assert "off_hours_windows_primary_flag_both" in summary_table.columns
    assert "off_hours_windows_primary_alert_fraction" in summary_table.columns
    assert "model_hour_harmonics" in summary_table.columns
    assert "primary_model_fit_method" in summary_table.columns
    assert "primary_model_fit_rows" in summary_table.columns
    assert "pro_rate_wilson_low" in hourly.columns
    assert "pro_rate_wilson_high" in hourly.columns
    assert "is_low_power" in hourly.columns
    assert not hour_of_week.empty
    assert "day_of_week" in hour_of_week.columns
    assert "off_hours_fraction" in hour_of_week.columns
    assert "pro_rate_wilson_low" in hour_of_week.columns
    assert "pro_rate_wilson_high" in hour_of_week.columns
    assert not date_hour.empty
    assert "date" in date_hour.columns
    assert "hour" in date_hour.columns
    assert "date" in date_hour_primary_residual.columns
    assert "hour" in date_hour_primary_residual.columns
    assert "z_score_primary" in date_hour_primary_residual.columns
    assert "n_windows_tested" in date_hour_primary_residual.columns
    assert "n_windows_primary_alert" in date_hour_primary_residual.columns
    assert "is_low_power" in date_hour_primary_residual.columns
    assert not window_profile.empty
    assert "bucket_minutes" in window_profile.columns
    assert "expected_pro_rate_day" in window_profile.columns
    assert "expected_pro_rate_primary" in window_profile.columns
    assert "primary_baseline_source" in window_profile.columns
    assert "is_alert_off_hours_window" in window_profile.columns
    assert "p_value_day_lower" in window_profile.columns
    assert "p_value_day_upper" in window_profile.columns
    assert "q_value_primary_two_sided" in window_profile.columns
    assert "is_significant_primary_two_sided" in window_profile.columns
    assert "is_material_primary_lower_shift" in window_profile.columns
    assert "is_primary_alert_window" in window_profile.columns
    assert "is_primary_spc_998_two_sided" in window_profile.columns
    assert "is_primary_fdr_two_sided" in window_profile.columns
    assert "is_primary_any_flag_channel" in window_profile.columns
    assert "is_primary_both_flag_channels" in window_profile.columns
    assert "is_below_primary_control_998" in window_profile.columns
    assert "control_low_95_day" in window_profile.columns
    assert "z_score_day" in window_profile.columns
    assert "model_fit_method" in window_profile.columns
    assert "model_fit_rows" in window_profile.columns
    assert "model_fit_used_harmonics" in window_profile.columns
    assert "bucket_minutes" in model_fit_diagnostics.columns
    assert "model_fit_method" in model_fit_diagnostics.columns
    assert "model_fit_available_fraction" in model_fit_diagnostics.columns
    assert not flag_channel_summary.empty
    assert "channel" in flag_channel_summary.columns
    assert "channel_label" in flag_channel_summary.columns
    assert "count" in flag_channel_summary.columns
    assert "share_of_tested" in flag_channel_summary.columns
    assert "rank" in flag_channel_summary.columns
    assert "bucket_start" in flagged_window_diagnostics.columns
    assert "is_primary_alert_window" in flagged_window_diagnostics.columns
    assert result.summary["off_hours_is_low_power"] is True
    assert result.summary["alert_off_hours_min_fraction"] == 1.0
    assert result.summary["primary_alert_min_abs_delta"] == 0.03
    assert "off_hours_windows_alert_eligible" in result.summary
    assert "off_hours_windows_alert_eligible_low_power" in result.summary
    assert "off_hours_windows_alert_eligible_tested_fraction" in result.summary
    assert "off_hours_windows_alert_eligible_low_power_fraction" in result.summary
    assert "off_hours_windows_primary_spc_998_any" in result.summary
    assert "off_hours_windows_primary_fdr_two_sided" in result.summary
    assert "off_hours_windows_primary_flag_any" in result.summary
    assert "off_hours_windows_primary_flag_both" in result.summary
    assert "off_hours_windows_primary_alert_fraction" in result.summary
    assert "model_hour_harmonics" in result.summary
    assert "primary_model_fit_method" in result.summary
    assert "primary_model_fit_rows" in result.summary
    assert "hour_of_week_cells" in result.summary
    assert "date_hour_primary_residual_cells" in result.summary
    assert result.summary["window_profile_rows"] > 0


def test_off_hours_detector_day_adjusted_control_profile_flags_supported_windows() -> None:
    df = pd.DataFrame(
        {
            "id": list(range(1, 9)),
            "position_normalized": ["Con", "Con", "Con", "Con", "Pro", "Pro", "Con", "Con"],
            "is_off_hours": [True, True, True, True, False, False, False, False],
            "hour": [1, 1, 1, 1, 10, 10, 10, 10],
            "day_of_week": [6, 6, 6, 6, 6, 6, 6, 6],
            "timestamp": pd.to_datetime(
                [
                    "2026-02-01T01:02:00Z",
                    "2026-02-01T01:10:00Z",
                    "2026-02-01T01:26:00Z",
                    "2026-02-01T01:45:00Z",
                    "2026-02-01T10:05:00Z",
                    "2026-02-01T10:14:00Z",
                    "2026-02-01T10:31:00Z",
                    "2026-02-01T10:47:00Z",
                ]
            ),
            "minute_bucket": pd.to_datetime(
                [
                    "2026-02-01T01:02:00Z",
                    "2026-02-01T01:10:00Z",
                    "2026-02-01T01:26:00Z",
                    "2026-02-01T01:45:00Z",
                    "2026-02-01T10:05:00Z",
                    "2026-02-01T10:14:00Z",
                    "2026-02-01T10:31:00Z",
                    "2026-02-01T10:47:00Z",
                ]
            ),
        }
    )

    detector = OffHoursDetector(
        bucket_minutes=(60,),
        min_window_total=1,
        primary_bucket_minutes=60,
        model_min_rows=50,
        primary_alert_min_abs_delta=0.8,
    )
    result = detector.run(df=df, features={})
    profile = result.tables["window_control_profile"]

    off_window = profile.loc[profile["is_off_hours_window"]].iloc[0]
    assert off_window["expected_pro_rate_day"] == 0.5
    assert off_window["expected_pro_rate_primary"] == 0.5
    assert bool(off_window["is_alert_off_hours_window"]) is True
    assert off_window["primary_baseline_source"] in {
        "day_on_hours",
        "global_on_hours",
        "overall_known",
    }
    assert bool(off_window["is_low_power"]) is False
    assert pd.notna(off_window["p_value_day"])
    assert pd.notna(off_window["p_value_day_upper"])
    assert bool(off_window["is_below_day_control_95"]) is True
    assert bool(off_window["is_below_primary_control_95"]) is True
    assert bool(off_window["is_material_primary_lower_shift"]) is False
    assert bool(off_window["is_primary_alert_window"]) is False


def test_off_hours_detector_skips_chi_square_when_contingency_column_is_empty() -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "position_normalized": ["Pro", "Pro", "Pro", "Pro"],
            "is_off_hours": [True, True, False, False],
            "hour": [1, 1, 10, 10],
            "day_of_week": [6, 6, 6, 6],
            "timestamp": pd.to_datetime(
                [
                    "2026-02-01T01:05:00Z",
                    "2026-02-01T01:25:00Z",
                    "2026-02-01T10:10:00Z",
                    "2026-02-01T10:35:00Z",
                ]
            ),
            "minute_bucket": pd.to_datetime(
                [
                    "2026-02-01T01:05:00Z",
                    "2026-02-01T01:25:00Z",
                    "2026-02-01T10:10:00Z",
                    "2026-02-01T10:35:00Z",
                ]
            ),
        }
    )

    result = OffHoursDetector().run(df=df, features={})
    chi_square_p = float(result.summary["chi_square_p_value"])
    assert chi_square_p == 1.0
    assert float(result.tables["off_hours_summary"].iloc[0]["chi_square_p_value"]) == 1.0


def test_off_hours_detector_uses_model_baseline_when_data_supports_fit() -> None:
    records: list[dict[str, object]] = []
    idx = 1
    for date in ("2026-02-01", "2026-02-02"):
        for hour, is_off_hours, positions in (
            (1, True, ("Con", "Con", "Pro", "Con")),
            (3, True, ("Con", "Con", "Con", "Pro")),
            (10, False, ("Pro", "Pro", "Con", "Pro")),
            (14, False, ("Pro", "Con", "Pro", "Pro")),
            (19, False, ("Pro", "Pro", "Con", "Con")),
        ):
            minute = 5
            for position in positions:
                timestamp = f"{date}T{hour:02d}:{minute:02d}:00Z"
                records.append(
                    {
                        "id": idx,
                        "position_normalized": position,
                        "is_off_hours": is_off_hours,
                        "hour": hour,
                        "day_of_week": 6,
                        "timestamp": pd.Timestamp(timestamp),
                        "minute_bucket": pd.Timestamp(timestamp),
                    }
                )
                idx += 1
                minute += 2

    df = pd.DataFrame(records)
    detector = OffHoursDetector(
        bucket_minutes=(60,),
        min_window_total=1,
        primary_bucket_minutes=60,
        model_min_rows=8,
        model_hour_harmonics=4,
    )
    result = detector.run(df=df, features={})
    profile = result.tables["window_control_profile"]
    model_fit_diagnostics = result.tables["model_fit_diagnostics"]
    date_hour_primary_residual = result.tables["date_hour_primary_residual_distribution"]
    off_hours_rows = profile.loc[profile["is_off_hours_window"]].copy()

    assert not off_hours_rows.empty
    assert bool(off_hours_rows["is_model_baseline_available"].any()) is True
    assert bool(off_hours_rows["expected_pro_rate_model"].notna().all()) is True
    assert bool((off_hours_rows["primary_baseline_source"] == "model_day_hour").all()) is True
    assert bool((off_hours_rows["model_fit_used_harmonics"] == 4).all()) is True
    assert bool(off_hours_rows["is_alert_off_hours_window"].all()) is True
    assert not model_fit_diagnostics.empty
    assert not date_hour_primary_residual.empty
    assert (
        bool(
            pd.to_numeric(
                date_hour_primary_residual["z_score_primary"],
                errors="coerce",
            )
            .notna()
            .any()
        )
        is True
    )
    assert bool((model_fit_diagnostics["model_fit_used_harmonics"] == 4).all()) is True
    assert (
        "off_hours_windows_below_primary_control_998" in result.tables["off_hours_summary"].columns
    )
    assert "off_hours_windows_primary_alert" in result.tables["off_hours_summary"].columns
    assert (
        "off_hours_windows_significant_primary_two_sided"
        in result.tables["off_hours_summary"].columns
    )
    assert "off_hours_windows_primary_spc_998_any" in result.tables["off_hours_summary"].columns
    assert "off_hours_windows_primary_fdr_two_sided" in result.tables["off_hours_summary"].columns
    assert "off_hours_windows_primary_flag_any" in result.tables["off_hours_summary"].columns
    assert "off_hours_windows_primary_flag_both" in result.tables["off_hours_summary"].columns
    assert "off_hours_windows_primary_alert_fraction" in result.tables["off_hours_summary"].columns
    assert "primary_model_fit_method" in result.tables["off_hours_summary"].columns
    assert "model_hour_harmonics" in result.tables["off_hours_summary"].columns
    assert result.summary["model_hour_harmonics"] == 4
    assert result.summary["primary_baseline_method"] in {
        "model_day_hour",
        "day_on_hours_fallback",
    }


def test_off_hours_detector_alert_window_threshold_excludes_mixed_windows() -> None:
    df = pd.DataFrame(
        {
            "id": list(range(1, 9)),
            "position_normalized": ["Con", "Con", "Pro", "Pro", "Pro", "Pro", "Con", "Con"],
            "is_off_hours": [True, True, False, False, False, False, False, False],
            "hour": [1, 1, 1, 1, 10, 10, 10, 10],
            "day_of_week": [6, 6, 6, 6, 6, 6, 6, 6],
            "timestamp": pd.to_datetime(
                [
                    "2026-02-01T01:02:00Z",
                    "2026-02-01T01:10:00Z",
                    "2026-02-01T01:20:00Z",
                    "2026-02-01T01:40:00Z",
                    "2026-02-01T10:05:00Z",
                    "2026-02-01T10:14:00Z",
                    "2026-02-01T10:31:00Z",
                    "2026-02-01T10:47:00Z",
                ]
            ),
            "minute_bucket": pd.to_datetime(
                [
                    "2026-02-01T01:02:00Z",
                    "2026-02-01T01:10:00Z",
                    "2026-02-01T01:20:00Z",
                    "2026-02-01T01:40:00Z",
                    "2026-02-01T10:05:00Z",
                    "2026-02-01T10:14:00Z",
                    "2026-02-01T10:31:00Z",
                    "2026-02-01T10:47:00Z",
                ]
            ),
        }
    )

    detector = OffHoursDetector(
        bucket_minutes=(60,),
        min_window_total=1,
        primary_bucket_minutes=60,
        model_min_rows=50,
        alert_off_hours_min_fraction=1.0,
    )
    result = detector.run(df=df, features={})
    profile = result.tables["window_control_profile"]
    mixed_mask = profile["bucket_start"] == pd.Timestamp("2026-02-01T01:00:00Z")
    mixed_window = profile.loc[mixed_mask].iloc[0]

    assert bool(mixed_window["is_off_hours_window"]) is True
    assert bool(mixed_window["is_alert_off_hours_window"]) is False
    assert result.summary["off_hours_windows_alert_eligible"] == 0
    assert result.summary["off_hours_windows_alert_eligible_low_power"] == 0
    assert result.summary["off_hours_windows_tested"] == 0
    assert result.summary["off_hours_windows_primary_spc_998_any"] == 0
    assert result.summary["off_hours_windows_primary_fdr_two_sided"] == 0
    assert result.summary["off_hours_windows_primary_flag_any"] == 0
    assert result.summary["off_hours_windows_primary_flag_both"] == 0
    assert result.summary["off_hours_windows_primary_alert"] == 0


def test_off_hours_primary_residual_distribution_includes_bucket_dimension() -> None:
    records: list[dict[str, object]] = []
    row_id = 1
    for date in ("2026-02-03", "2026-02-04"):
        for hour, is_off_hours, positions in (
            (1, True, ("Con", "Con", "Con", "Pro")),
            (2, True, ("Con", "Con", "Pro", "Pro")),
            (10, False, ("Pro", "Pro", "Con", "Pro")),
            (11, False, ("Pro", "Con", "Pro", "Pro")),
        ):
            minute = 0
            for position in positions:
                timestamp = pd.Timestamp(f"{date}T{hour:02d}:{minute:02d}:00Z")
                records.append(
                    {
                        "id": row_id,
                        "position_normalized": position,
                        "is_off_hours": is_off_hours,
                        "hour": hour,
                        "day_of_week": int(timestamp.dayofweek),
                        "timestamp": timestamp,
                        "minute_bucket": timestamp,
                    }
                )
                row_id += 1
                minute += 10

    detector = OffHoursDetector(
        bucket_minutes=(30, 60),
        min_window_total=1,
        primary_bucket_minutes=30,
        model_min_rows=8,
    )
    result = detector.run(df=pd.DataFrame(records), features={})
    residual = result.tables["date_hour_primary_residual_distribution"]

    assert "bucket_minutes" in residual.columns
    assert set(pd.to_numeric(residual["bucket_minutes"], errors="coerce").dropna().astype(int)) == {
        30,
        60,
    }
    assert result.summary["date_hour_primary_residual_cells"] == int(
        (pd.to_numeric(residual["bucket_minutes"], errors="coerce") == 30).sum()
    )


def test_off_hours_primary_residual_distribution_includes_all_hours_context() -> None:
    records: list[dict[str, object]] = []
    row_id = 1
    for date in ("2026-02-03", "2026-02-04"):
        for hour, is_off_hours, positions in (
            (1, True, ("Con", "Con", "Pro", "Con")),
            (2, True, ("Con", "Pro", "Con", "Con")),
            (10, False, ("Pro", "Pro", "Con", "Pro")),
            (11, False, ("Pro", "Con", "Pro", "Pro")),
        ):
            minute = 0
            for position in positions:
                timestamp = pd.Timestamp(f"{date}T{hour:02d}:{minute:02d}:00Z")
                records.append(
                    {
                        "id": row_id,
                        "position_normalized": position,
                        "is_off_hours": is_off_hours,
                        "hour": hour,
                        "day_of_week": int(timestamp.dayofweek),
                        "timestamp": timestamp,
                        "minute_bucket": timestamp,
                    }
                )
                row_id += 1
                minute += 10

    detector = OffHoursDetector(
        bucket_minutes=(60,),
        min_window_total=1,
        primary_bucket_minutes=60,
        model_min_rows=8,
    )
    result = detector.run(df=pd.DataFrame(records), features={})
    residual = result.tables["date_hour_primary_residual_distribution"]
    residual_primary = residual.loc[
        pd.to_numeric(residual["bucket_minutes"], errors="coerce") == 60
    ].copy()

    hours_present = set(
        pd.to_numeric(residual_primary["hour"], errors="coerce").dropna().astype(int)
    )
    assert {1, 2, 10, 11}.issubset(hours_present)

    on_hours_rows = residual_primary.loc[
        pd.to_numeric(residual_primary["hour"], errors="coerce").isin([10, 11])
    ].copy()
    assert not on_hours_rows.empty
    assert (
        bool(pd.to_numeric(on_hours_rows["z_score_primary"], errors="coerce").notna().any()) is True
    )
    assert (
        bool(
            (
                pd.to_numeric(on_hours_rows["n_windows_alert_eligible"], errors="coerce")
                .fillna(0)
                .astype(int)
                == 0
            ).all()
        )
        is True
    )


def test_off_hours_detector_schema_contract_is_stable() -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "position_normalized": ["Pro", "Con", "Pro", "Con", "Pro", "Con"],
            "is_off_hours": [True, True, False, False, False, False],
            "hour": [2, 2, 10, 10, 11, 11],
            "day_of_week": [5, 5, 5, 5, 5, 5],
            "timestamp": pd.to_datetime(
                [
                    "2026-02-05T02:05:00Z",
                    "2026-02-05T02:25:00Z",
                    "2026-02-05T10:10:00Z",
                    "2026-02-05T10:30:00Z",
                    "2026-02-05T11:10:00Z",
                    "2026-02-05T11:25:00Z",
                ]
            ),
            "minute_bucket": pd.to_datetime(
                [
                    "2026-02-05T02:05:00Z",
                    "2026-02-05T02:25:00Z",
                    "2026-02-05T10:10:00Z",
                    "2026-02-05T10:30:00Z",
                    "2026-02-05T11:10:00Z",
                    "2026-02-05T11:25:00Z",
                ]
            ),
        }
    )

    result = OffHoursDetector().run(df=df, features={})

    expected_table_keys = {
        "date_hour_distribution",
        "date_hour_primary_residual_distribution",
        "flag_channel_summary",
        "flagged_window_diagnostics",
        "hour_of_week_distribution",
        "hourly_distribution",
        "model_fit_diagnostics",
        "off_hours_summary",
        "window_control_profile",
    }
    expected_summary_keys = {
        "alert_off_hours_min_fraction",
        "chi_square_p_value",
        "date_hour_cells",
        "date_hour_primary_residual_cells",
        "hour_of_week_cells",
        "low_power_min_total",
        "max_hour_of_week_pro_rate",
        "model_hour_harmonics",
        "off_hours_is_low_power",
        "off_hours_primary_alert_max_run_minutes",
        "off_hours_primary_alert_max_run_windows",
        "off_hours_primary_alert_run_count",
        "off_hours_ratio",
        "off_hours_windows_alert_eligible",
        "off_hours_windows_alert_eligible_low_power",
        "off_hours_windows_alert_eligible_low_power_fraction",
        "off_hours_windows_alert_eligible_tested_fraction",
        "off_hours_windows_below_day_control_95",
        "off_hours_windows_below_day_control_998",
        "off_hours_windows_below_model_control_95",
        "off_hours_windows_below_model_control_998",
        "off_hours_windows_below_primary_control_95",
        "off_hours_windows_below_primary_control_998",
        "off_hours_windows_model_available",
        "off_hours_windows_primary_alert",
        "off_hours_windows_primary_alert_fraction",
        "off_hours_windows_primary_fdr_two_sided",
        "off_hours_windows_primary_fdr_two_sided_fraction",
        "off_hours_windows_primary_flag_any",
        "off_hours_windows_primary_flag_any_fraction",
        "off_hours_windows_primary_flag_both",
        "off_hours_windows_primary_flag_both_fraction",
        "off_hours_windows_primary_spc_998_any",
        "off_hours_windows_primary_spc_998_any_fraction",
        "off_hours_windows_significant_primary",
        "off_hours_windows_significant_primary_two_sided",
        "off_hours_windows_significant_primary_upper",
        "off_hours_windows_tested",
        "on_hours_is_low_power",
        "primary_alert_min_abs_delta",
        "primary_baseline_method",
        "primary_bucket_minutes",
        "primary_model_fit_aic",
        "primary_model_fit_converged",
        "primary_model_fit_method",
        "primary_model_fit_rows",
        "primary_model_fit_unique_days",
        "primary_model_fit_unique_hours",
        "window_profile_rows",
    }
    expected_table_columns = {
        "off_hours_summary": {
            "alert_off_hours_min_fraction",
            "chi_square_p_value",
            "day_adjusted_fdr_alpha",
            "global_daytime_pro_rate",
            "model_fit_min_rows",
            "model_hour_harmonics",
            "off_hours",
            "off_hours_is_low_power",
            "off_hours_max_abs_day_z",
            "off_hours_max_abs_model_z",
            "off_hours_max_abs_primary_delta",
            "off_hours_max_abs_primary_z",
            "off_hours_min_day_z",
            "off_hours_min_model_z",
            "off_hours_min_primary_delta",
            "off_hours_min_primary_z",
            "off_hours_primary_alert_max_run_minutes",
            "off_hours_primary_alert_max_run_windows",
            "off_hours_primary_alert_run_count",
            "off_hours_pro_rate",
            "off_hours_pro_rate_wilson_half_width",
            "off_hours_pro_rate_wilson_high",
            "off_hours_pro_rate_wilson_low",
            "off_hours_ratio",
            "off_hours_windows_above_primary_control_95",
            "off_hours_windows_above_primary_control_998",
            "off_hours_windows_alert_eligible",
            "off_hours_windows_alert_eligible_low_power",
            "off_hours_windows_alert_eligible_low_power_fraction",
            "off_hours_windows_alert_eligible_tested_fraction",
            "off_hours_windows_below_day_control_95",
            "off_hours_windows_below_day_control_998",
            "off_hours_windows_below_model_control_95",
            "off_hours_windows_below_model_control_998",
            "off_hours_windows_below_primary_control_95",
            "off_hours_windows_below_primary_control_998",
            "off_hours_windows_model_available",
            "off_hours_windows_primary_alert",
            "off_hours_windows_primary_alert_fraction",
            "off_hours_windows_primary_fdr_two_sided",
            "off_hours_windows_primary_fdr_two_sided_fraction",
            "off_hours_windows_primary_flag_any",
            "off_hours_windows_primary_flag_any_fraction",
            "off_hours_windows_primary_flag_both",
            "off_hours_windows_primary_flag_both_fraction",
            "off_hours_windows_primary_spc_998_any",
            "off_hours_windows_primary_spc_998_any_fraction",
            "off_hours_windows_significant_day",
            "off_hours_windows_significant_model",
            "off_hours_windows_significant_primary",
            "off_hours_windows_significant_primary_two_sided",
            "off_hours_windows_significant_primary_upper",
            "off_hours_windows_tested",
            "on_hours",
            "on_hours_is_low_power",
            "on_hours_pro_rate",
            "on_hours_pro_rate_wilson_half_width",
            "on_hours_pro_rate_wilson_high",
            "on_hours_pro_rate_wilson_low",
            "primary_alert_min_abs_delta",
            "primary_baseline_method",
            "primary_bucket_minutes",
            "primary_model_fit_aic",
            "primary_model_fit_converged",
            "primary_model_fit_method",
            "primary_model_fit_rows",
            "primary_model_fit_unique_days",
            "primary_model_fit_unique_hours",
            "total",
        },
        "hourly_distribution": {
            "hour",
            "is_low_power",
            "n_con",
            "n_pro",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_half_width",
            "pro_rate_wilson_high",
            "pro_rate_wilson_low",
        },
        "hour_of_week_distribution": {
            "day_of_week",
            "day_of_week_index",
            "hour",
            "is_low_power",
            "n_con",
            "n_off_hours",
            "n_pro",
            "n_total",
            "off_hours_fraction",
            "pro_rate",
            "pro_rate_wilson_high",
            "pro_rate_wilson_low",
        },
        "date_hour_distribution": {
            "date",
            "day_of_week",
            "hour",
            "is_low_power",
            "n_con",
            "n_known",
            "n_off_hours",
            "n_pro",
            "n_total",
            "n_unknown",
            "off_hours_fraction",
            "pro_rate",
            "pro_rate_wilson_high",
            "pro_rate_wilson_low",
        },
        "date_hour_primary_residual_distribution": {
            "bucket_minutes",
            "date",
            "day_of_week",
            "delta_pro_rate_primary",
            "expected_pro_rate_primary",
            "hour",
            "is_low_power",
            "n_con",
            "n_known",
            "n_known_tested",
            "n_pro",
            "n_total",
            "n_windows",
            "n_windows_alert_eligible",
            "n_windows_low_power",
            "n_windows_primary_alert",
            "n_windows_tested",
            "off_hours_fraction",
            "primary_alert_fraction_tested",
            "pro_rate",
            "z_score_primary",
            "z_score_primary_abs_max",
            "z_score_primary_median",
        },
        "window_control_profile": {
            "baseline_source",
            "bucket_minutes",
            "bucket_start",
            "control_high_95_day",
            "control_high_95_global",
            "control_high_95_model",
            "control_high_95_primary",
            "control_high_998_day",
            "control_high_998_global",
            "control_high_998_model",
            "control_high_998_primary",
            "control_low_95_day",
            "control_low_95_global",
            "control_low_95_model",
            "control_low_95_primary",
            "control_low_998_day",
            "control_low_998_global",
            "control_low_998_model",
            "control_low_998_primary",
            "day_of_week",
            "day_on_hours_known",
            "day_on_hours_pro",
            "day_on_hours_pro_rate",
            "delta_pro_rate_day",
            "delta_pro_rate_model",
            "delta_pro_rate_primary",
            "event_date_key",
            "expected_pro_rate_day",
            "expected_pro_rate_global",
            "expected_pro_rate_model",
            "expected_pro_rate_primary",
            "hour",
            "is_above_day_control_95",
            "is_above_day_control_998",
            "is_above_model_control_95",
            "is_above_model_control_998",
            "is_above_primary_control_95",
            "is_above_primary_control_998",
            "is_alert_off_hours_window",
            "is_below_day_control_95",
            "is_below_day_control_998",
            "is_below_global_control_95",
            "is_below_global_control_998",
            "is_below_model_control_95",
            "is_below_model_control_998",
            "is_below_primary_control_95",
            "is_below_primary_control_998",
            "is_low_power",
            "is_material_primary_lower_shift",
            "is_material_primary_shift",
            "is_material_primary_upper_shift",
            "is_model_baseline_available",
            "is_off_hours_window",
            "is_outside_day_control_95",
            "is_outside_day_control_998",
            "is_outside_model_control_95",
            "is_outside_model_control_998",
            "is_outside_primary_control_95",
            "is_outside_primary_control_998",
            "is_primary_alert_window",
            "is_primary_any_flag_channel",
            "is_primary_both_flag_channels",
            "is_primary_fdr_two_sided",
            "is_primary_spc_998_two_sided",
            "is_pure_off_hours_window",
            "is_significant_day",
            "is_significant_day_lower",
            "is_significant_day_two_sided",
            "is_significant_day_upper",
            "is_significant_model",
            "is_significant_model_lower",
            "is_significant_model_two_sided",
            "is_significant_model_upper",
            "is_significant_primary",
            "is_significant_primary_lower",
            "is_significant_primary_two_sided",
            "is_significant_primary_upper",
            "model_baseline_source",
            "model_fit_aic",
            "model_fit_converged",
            "model_fit_method",
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_used_harmonics",
            "n_con",
            "n_known",
            "n_off_hours",
            "n_pro",
            "n_total",
            "n_unknown",
            "off_hours_fraction",
            "p_value_day",
            "p_value_day_lower",
            "p_value_day_two_sided",
            "p_value_day_upper",
            "p_value_model",
            "p_value_model_lower",
            "p_value_model_two_sided",
            "p_value_model_upper",
            "p_value_primary",
            "p_value_primary_lower",
            "p_value_primary_two_sided",
            "p_value_primary_upper",
            "primary_baseline_source",
            "pro_rate",
            "pro_rate_wilson_half_width",
            "pro_rate_wilson_high",
            "pro_rate_wilson_low",
            "q_value_day",
            "q_value_day_lower",
            "q_value_day_two_sided",
            "q_value_day_upper",
            "q_value_model",
            "q_value_model_lower",
            "q_value_model_two_sided",
            "q_value_model_upper",
            "q_value_primary",
            "q_value_primary_lower",
            "q_value_primary_two_sided",
            "q_value_primary_upper",
            "z_score_day",
            "z_score_model",
            "z_score_primary",
        },
        "model_fit_diagnostics": {
            "bucket_minutes",
            "model_fit_aic",
            "model_fit_available_fraction",
            "model_fit_available_windows",
            "model_fit_converged",
            "model_fit_method",
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_used_harmonics",
            "model_fit_window_count",
        },
        "flag_channel_summary": {
            "channel",
            "channel_label",
            "count",
            "rank",
            "share_of_tested",
        },
        "flagged_window_diagnostics": {
            "bucket_minutes",
            "bucket_start",
            "delta_pro_rate_primary",
            "expected_pro_rate_primary",
            "is_low_power",
            "is_model_baseline_available",
            "is_primary_alert_window",
            "is_primary_any_flag_channel",
            "is_primary_both_flag_channels",
            "is_primary_fdr_two_sided",
            "is_primary_spc_998_two_sided",
            "model_fit_method",
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_used_harmonics",
            "n_con",
            "n_known",
            "n_pro",
            "n_total",
            "p_value_primary_two_sided",
            "primary_baseline_source",
            "pro_rate",
            "q_value_primary_two_sided",
            "z_score_primary",
        },
    }

    assert set(result.tables.keys()) == expected_table_keys
    assert set(result.summary.keys()) == expected_summary_keys
    for table_name, expected_columns in expected_table_columns.items():
        assert set(result.tables[table_name].columns) == expected_columns
