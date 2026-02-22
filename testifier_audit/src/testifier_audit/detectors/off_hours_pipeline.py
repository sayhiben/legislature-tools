from __future__ import annotations

import numpy as np
import pandas as pd

from testifier_audit.proportion_stats import low_power_mask, wilson_half_width, wilson_interval

from .off_hours_statistics import (
    InferenceConfig,
    apply_bh_fdr,
    control_limits,
    exact_binomial_tail_p_values,
    fit_model_expected_pro_rate,
    standardized_residual,
)


def resolve_event_time(df: pd.DataFrame) -> pd.Series:
    if "minute_bucket" in df.columns:
        minute_bucket = pd.to_datetime(df["minute_bucket"], errors="coerce")
        if minute_bucket.notna().any():
            return minute_bucket
    if "timestamp" in df.columns:
        return pd.to_datetime(df["timestamp"], errors="coerce")
    return pd.Series(pd.NaT, index=df.index)


def build_window_control_profile(
    working: pd.DataFrame,
    *,
    bucket_minutes: tuple[int, ...],
    config: InferenceConfig,
) -> pd.DataFrame:
    timed = working.assign(_event_time=resolve_event_time(working))
    timed = timed[timed["_event_time"].notna()].copy()
    if timed.empty:
        return pd.DataFrame()

    timed["event_date_key"] = timed["_event_time"].dt.strftime("%Y-%m-%d")
    known_mask = timed["is_pro"] | timed["is_con"]
    known = timed[known_mask].copy()

    overall_known_total = int(len(known))
    overall_known_pro = int(known["is_pro"].sum())
    overall_known_pro_rate = (
        (overall_known_pro / overall_known_total) if overall_known_total else np.nan
    )

    on_hours_known = known[~known["is_off_hours"]]
    on_hours_known_total = int(len(on_hours_known))
    on_hours_known_pro = int(on_hours_known["is_pro"].sum())
    global_on_hours_pro_rate = (
        (on_hours_known_pro / on_hours_known_total) if on_hours_known_total else np.nan
    )
    if not np.isfinite(global_on_hours_pro_rate):
        global_on_hours_pro_rate = overall_known_pro_rate

    day_baseline = (
        on_hours_known.groupby("event_date_key", dropna=False)
        .agg(
            day_on_hours_known=("is_pro", "size"),
            day_on_hours_pro=("is_pro", "sum"),
        )
        .reset_index()
    )
    day_baseline["day_on_hours_pro_rate"] = (
        day_baseline["day_on_hours_pro"] / day_baseline["day_on_hours_known"]
    ).where(day_baseline["day_on_hours_known"] > 0)

    window_frames: list[pd.DataFrame] = []
    for bucket_value in bucket_minutes:
        bucket_start = timed["_event_time"].dt.floor(f"{bucket_value}min")
        windowed = (
            timed.assign(
                bucket_start=bucket_start,
                event_date_key=bucket_start.dt.strftime("%Y-%m-%d"),
            )
            .groupby(["bucket_start", "event_date_key"], dropna=False)
            .agg(
                n_total=("is_pro", "size"),
                n_pro=("is_pro", "sum"),
                n_con=("is_con", "sum"),
                n_off_hours=("is_off_hours", "sum"),
            )
            .reset_index()
            .sort_values("bucket_start")
        )
        if windowed.empty:
            continue

        windowed["bucket_minutes"] = int(bucket_value)
        windowed["day_of_week"] = windowed["bucket_start"].dt.day_name()
        windowed["hour"] = windowed["bucket_start"].dt.hour
        windowed["n_known"] = windowed["n_pro"] + windowed["n_con"]
        windowed["n_unknown"] = (windowed["n_total"] - windowed["n_known"]).clip(lower=0)
        windowed["off_hours_fraction"] = (
            windowed["n_off_hours"] / windowed["n_total"].replace(0, np.nan)
        ).fillna(0.0)
        windowed["is_off_hours_window"] = windowed["off_hours_fraction"] >= 0.5
        windowed["is_pure_off_hours_window"] = windowed["n_off_hours"] == windowed["n_total"]
        windowed["is_alert_off_hours_window"] = (
            windowed["off_hours_fraction"] >= config.alert_off_hours_min_fraction
        )
        windowed["pro_rate"] = (windowed["n_pro"] / windowed["n_known"]).where(
            windowed["n_known"] > 0
        )

        (
            windowed["pro_rate_wilson_low"],
            windowed["pro_rate_wilson_high"],
        ) = wilson_interval(
            successes=windowed["n_pro"],
            totals=windowed["n_known"],
        )
        windowed["pro_rate_wilson_half_width"] = wilson_half_width(
            successes=windowed["n_pro"],
            totals=windowed["n_known"],
        )
        windowed["is_low_power"] = low_power_mask(
            totals=windowed["n_known"],
            min_total=config.min_window_total,
        )

        windowed = windowed.merge(day_baseline, on="event_date_key", how="left")
        windowed["expected_pro_rate_global"] = (
            float(global_on_hours_pro_rate) if np.isfinite(global_on_hours_pro_rate) else np.nan
        )
        windowed["expected_pro_rate_day"] = windowed["day_on_hours_pro_rate"]
        windowed["baseline_source"] = "day_on_hours"

        invalid_day = (
            pd.to_numeric(windowed["day_on_hours_known"], errors="coerce").fillna(0.0)
            < float(config.min_window_total)
        ) | windowed["expected_pro_rate_day"].isna()
        windowed.loc[invalid_day, "expected_pro_rate_day"] = windowed.loc[
            invalid_day, "expected_pro_rate_global"
        ]
        windowed.loc[invalid_day, "baseline_source"] = "global_on_hours"

        if not np.isfinite(global_on_hours_pro_rate) and np.isfinite(overall_known_pro_rate):
            windowed["expected_pro_rate_global"] = float(overall_known_pro_rate)
            windowed["expected_pro_rate_day"] = float(overall_known_pro_rate)
            windowed["baseline_source"] = "overall_known"
        if not np.isfinite(overall_known_pro_rate):
            windowed["baseline_source"] = "unavailable"

        model_fit = fit_model_expected_pro_rate(
            windowed,
            model_min_rows=config.model_min_rows,
            model_hour_harmonics=config.model_hour_harmonics,
        )
        windowed["expected_pro_rate_model"] = model_fit.expected_pro_rate
        windowed["is_model_baseline_available"] = model_fit.is_model_baseline_available
        windowed["model_baseline_source"] = np.where(
            windowed["is_model_baseline_available"],
            "day_fixed_plus_harmonic_hour",
            "unavailable",
        )
        windowed["model_fit_method"] = str(
            model_fit.diagnostics.get("model_fit_method", "unavailable")
        )
        windowed["model_fit_rows"] = int(model_fit.diagnostics.get("model_fit_rows", 0))
        windowed["model_fit_unique_days"] = int(
            model_fit.diagnostics.get("model_fit_unique_days", 0)
        )
        windowed["model_fit_unique_hours"] = int(
            model_fit.diagnostics.get("model_fit_unique_hours", 0)
        )
        windowed["model_fit_converged"] = pd.to_numeric(
            model_fit.diagnostics.get("model_fit_converged", np.nan),
            errors="coerce",
        )
        windowed["model_fit_aic"] = pd.to_numeric(
            model_fit.diagnostics.get("model_fit_aic", np.nan),
            errors="coerce",
        )
        windowed["model_fit_used_harmonics"] = int(config.model_hour_harmonics)

        windowed["expected_pro_rate_primary"] = windowed["expected_pro_rate_model"]
        windowed["primary_baseline_source"] = "model_day_hour"
        missing_primary = windowed["expected_pro_rate_primary"].isna()
        windowed.loc[missing_primary, "expected_pro_rate_primary"] = windowed.loc[
            missing_primary, "expected_pro_rate_day"
        ]
        windowed.loc[missing_primary, "primary_baseline_source"] = windowed.loc[
            missing_primary, "baseline_source"
        ]

        (
            windowed["control_low_95_day"],
            windowed["control_high_95_day"],
        ) = control_limits(
            windowed["expected_pro_rate_day"],
            windowed["n_known"],
            z=1.96,
        )
        (
            windowed["control_low_998_day"],
            windowed["control_high_998_day"],
        ) = control_limits(
            windowed["expected_pro_rate_day"],
            windowed["n_known"],
            z=3.0,
        )
        (
            windowed["control_low_95_global"],
            windowed["control_high_95_global"],
        ) = control_limits(
            windowed["expected_pro_rate_global"],
            windowed["n_known"],
            z=1.96,
        )
        (
            windowed["control_low_998_global"],
            windowed["control_high_998_global"],
        ) = control_limits(
            windowed["expected_pro_rate_global"],
            windowed["n_known"],
            z=3.0,
        )
        (
            windowed["control_low_95_model"],
            windowed["control_high_95_model"],
        ) = control_limits(
            windowed["expected_pro_rate_model"],
            windowed["n_known"],
            z=1.96,
        )
        (
            windowed["control_low_998_model"],
            windowed["control_high_998_model"],
        ) = control_limits(
            windowed["expected_pro_rate_model"],
            windowed["n_known"],
            z=3.0,
        )
        (
            windowed["control_low_95_primary"],
            windowed["control_high_95_primary"],
        ) = control_limits(
            windowed["expected_pro_rate_primary"],
            windowed["n_known"],
            z=1.96,
        )
        (
            windowed["control_low_998_primary"],
            windowed["control_high_998_primary"],
        ) = control_limits(
            windowed["expected_pro_rate_primary"],
            windowed["n_known"],
            z=3.0,
        )

        tested = (
            (~windowed["is_low_power"]) & windowed["pro_rate"].notna() & (windowed["n_known"] > 0)
        )

        for baseline_name, expected_col in (
            ("day", "expected_pro_rate_day"),
            ("model", "expected_pro_rate_model"),
            ("primary", "expected_pro_rate_primary"),
        ):
            expected_rate = pd.to_numeric(windowed[expected_col], errors="coerce").clip(
                lower=1e-6,
                upper=1.0 - 1e-6,
            )
            z_score = standardized_residual(
                observed_successes=windowed["n_pro"],
                totals=windowed["n_known"],
                expected_rate=expected_rate,
            )
            p_lower, p_upper, p_two_sided, valid_exact = exact_binomial_tail_p_values(
                observed_successes=windowed["n_pro"],
                totals=windowed["n_known"],
                expected_rate=expected_rate,
            )
            usable = valid_exact & tested

            windowed[f"z_score_{baseline_name}"] = z_score
            windowed[f"delta_pro_rate_{baseline_name}"] = (
                windowed["pro_rate"] - expected_rate
            ).where(windowed["pro_rate"].notna())
            windowed[f"p_value_{baseline_name}_lower"] = p_lower.where(usable)
            windowed[f"p_value_{baseline_name}_upper"] = p_upper.where(usable)
            windowed[f"p_value_{baseline_name}_two_sided"] = p_two_sided.where(usable)
            for tail in ("lower", "upper", "two_sided"):
                windowed[f"q_value_{baseline_name}_{tail}"] = np.nan
                windowed[f"is_significant_{baseline_name}_{tail}"] = False

        windowed["is_below_day_control_95"] = tested & (
            windowed["pro_rate"] < windowed["control_low_95_day"]
        )
        windowed["is_below_day_control_998"] = tested & (
            windowed["pro_rate"] < windowed["control_low_998_day"]
        )
        windowed["is_above_day_control_95"] = tested & (
            windowed["pro_rate"] > windowed["control_high_95_day"]
        )
        windowed["is_above_day_control_998"] = tested & (
            windowed["pro_rate"] > windowed["control_high_998_day"]
        )
        windowed["is_below_model_control_95"] = tested & (
            windowed["pro_rate"] < windowed["control_low_95_model"]
        )
        windowed["is_below_model_control_998"] = tested & (
            windowed["pro_rate"] < windowed["control_low_998_model"]
        )
        windowed["is_above_model_control_95"] = tested & (
            windowed["pro_rate"] > windowed["control_high_95_model"]
        )
        windowed["is_above_model_control_998"] = tested & (
            windowed["pro_rate"] > windowed["control_high_998_model"]
        )
        windowed["is_below_primary_control_95"] = tested & (
            windowed["pro_rate"] < windowed["control_low_95_primary"]
        )
        windowed["is_below_primary_control_998"] = tested & (
            windowed["pro_rate"] < windowed["control_low_998_primary"]
        )
        windowed["is_above_primary_control_95"] = tested & (
            windowed["pro_rate"] > windowed["control_high_95_primary"]
        )
        windowed["is_above_primary_control_998"] = tested & (
            windowed["pro_rate"] > windowed["control_high_998_primary"]
        )
        windowed["is_outside_day_control_95"] = tested & (
            (windowed["pro_rate"] < windowed["control_low_95_day"])
            | (windowed["pro_rate"] > windowed["control_high_95_day"])
        )
        windowed["is_outside_day_control_998"] = tested & (
            (windowed["pro_rate"] < windowed["control_low_998_day"])
            | (windowed["pro_rate"] > windowed["control_high_998_day"])
        )
        windowed["is_outside_model_control_95"] = tested & (
            (windowed["pro_rate"] < windowed["control_low_95_model"])
            | (windowed["pro_rate"] > windowed["control_high_95_model"])
        )
        windowed["is_outside_model_control_998"] = tested & (
            (windowed["pro_rate"] < windowed["control_low_998_model"])
            | (windowed["pro_rate"] > windowed["control_high_998_model"])
        )
        windowed["is_outside_primary_control_95"] = tested & (
            (windowed["pro_rate"] < windowed["control_low_95_primary"])
            | (windowed["pro_rate"] > windowed["control_high_95_primary"])
        )
        windowed["is_outside_primary_control_998"] = tested & (
            (windowed["pro_rate"] < windowed["control_low_998_primary"])
            | (windowed["pro_rate"] > windowed["control_high_998_primary"])
        )
        windowed["is_below_global_control_95"] = tested & (
            windowed["pro_rate"] < windowed["control_low_95_global"]
        )
        windowed["is_below_global_control_998"] = tested & (
            windowed["pro_rate"] < windowed["control_low_998_global"]
        )

        windowed["p_value_day"] = windowed["p_value_day_lower"]
        windowed["p_value_model"] = windowed["p_value_model_lower"]
        windowed["p_value_primary"] = windowed["p_value_primary_lower"]

        window_frames.append(windowed)

    if not window_frames:
        return pd.DataFrame()

    profile = pd.concat(window_frames, ignore_index=True)
    bucket_values = sorted(profile["bucket_minutes"].dropna().astype(int).unique())
    for bucket_value in bucket_values:
        bucket_mask = profile["bucket_minutes"] == bucket_value
        tested_off_hours = (
            bucket_mask
            & profile["is_alert_off_hours_window"]
            & (~profile["is_low_power"])
            & (pd.to_numeric(profile["n_known"], errors="coerce").fillna(0.0) > 0.0)
        )
        for baseline_name in ("day", "model", "primary"):
            for tail in ("lower", "upper", "two_sided"):
                p_column = f"p_value_{baseline_name}_{tail}"
                q_column = f"q_value_{baseline_name}_{tail}"
                sig_column = f"is_significant_{baseline_name}_{tail}"

                q_values, is_significant = apply_bh_fdr(
                    profile.loc[tested_off_hours, p_column],
                    alpha=config.fdr_alpha,
                )
                profile.loc[q_values.index, q_column] = q_values
                profile.loc[is_significant.index, sig_column] = is_significant

    profile = profile.copy()
    profile["q_value_day"] = profile["q_value_day_lower"]
    profile["is_significant_day"] = profile["is_significant_day_lower"]
    profile["q_value_model"] = profile["q_value_model_lower"]
    profile["is_significant_model"] = profile["is_significant_model_lower"]
    profile["q_value_primary"] = profile["q_value_primary_lower"]
    profile["is_significant_primary"] = profile["is_significant_primary_lower"]

    tested_profile = (
        (~profile["is_low_power"])
        & profile["pro_rate"].notna()
        & (pd.to_numeric(profile["n_known"], errors="coerce").fillna(0.0) > 0.0)
    )
    primary_delta = pd.to_numeric(profile["delta_pro_rate_primary"], errors="coerce")
    profile["is_material_primary_shift"] = tested_profile & (
        primary_delta.abs() >= config.primary_alert_min_abs_delta
    )
    profile["is_material_primary_lower_shift"] = tested_profile & (
        primary_delta <= -config.primary_alert_min_abs_delta
    )
    profile["is_material_primary_upper_shift"] = tested_profile & (
        primary_delta >= config.primary_alert_min_abs_delta
    )
    profile["is_primary_alert_window"] = (
        tested_profile
        & profile["is_alert_off_hours_window"]
        & profile["is_below_primary_control_998"]
        & profile["is_significant_primary_lower"]
        & profile["is_material_primary_lower_shift"]
    )
    profile["is_primary_spc_998_two_sided"] = (
        tested_profile & profile["is_outside_primary_control_998"]
    )
    profile["is_primary_fdr_two_sided"] = (
        tested_profile & profile["is_significant_primary_two_sided"]
    )
    profile["is_primary_any_flag_channel"] = (
        profile["is_primary_spc_998_two_sided"] | profile["is_primary_fdr_two_sided"]
    )
    profile["is_primary_both_flag_channels"] = (
        profile["is_primary_spc_998_two_sided"] & profile["is_primary_fdr_two_sided"]
    )

    return profile.sort_values(["bucket_minutes", "bucket_start"]).reset_index(drop=True)


def select_primary_bucket(
    profile: pd.DataFrame,
    *,
    primary_bucket_minutes: int,
) -> tuple[int | None, pd.DataFrame]:
    if profile.empty:
        return None, pd.DataFrame()
    available = sorted({int(value) for value in profile["bucket_minutes"].dropna().astype(int)})
    if not available:
        return None, pd.DataFrame()
    if int(primary_bucket_minutes) in available:
        target = int(primary_bucket_minutes)
    elif 30 in available:
        target = 30
    else:
        target = available[0]
    return target, profile[profile["bucket_minutes"] == target].copy()
