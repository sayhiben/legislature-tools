from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency

from testifier_audit.proportion_stats import low_power_mask, wilson_half_width, wilson_interval

from .off_hours_pipeline import resolve_event_time


def _safe_int(value: object) -> int:
    parsed = pd.to_numeric(value, errors="coerce")
    if pd.isna(parsed):
        return 0
    return int(parsed)


def _safe_float(value: object, *, default: float) -> float:
    parsed = pd.to_numeric(value, errors="coerce")
    if pd.isna(parsed):
        return float(default)
    return float(parsed)


def build_date_hour_distribution(
    working: pd.DataFrame,
    *,
    min_window_total: int,
) -> pd.DataFrame:
    timed = working.assign(_event_time=resolve_event_time(working))
    timed = timed[timed["_event_time"].notna()].copy()
    if timed.empty:
        return pd.DataFrame()

    grouped = (
        timed.assign(
            date=timed["_event_time"].dt.strftime("%Y-%m-%d"),
            hour=timed["_event_time"].dt.hour,
        )
        .groupby(["date", "hour"], dropna=False)
        .agg(
            n_total=("is_pro", "size"),
            n_pro=("is_pro", "sum"),
            n_con=("is_con", "sum"),
            n_off_hours=("is_off_hours", "sum"),
        )
        .reset_index()
        .sort_values(["date", "hour"])
    )
    grouped["day_of_week"] = pd.to_datetime(grouped["date"], errors="coerce").dt.day_name()
    grouped["n_known"] = grouped["n_pro"] + grouped["n_con"]
    grouped["n_unknown"] = (grouped["n_total"] - grouped["n_known"]).clip(lower=0)
    grouped["off_hours_fraction"] = (
        grouped["n_off_hours"] / grouped["n_total"].replace(0, np.nan)
    ).fillna(0.0)
    grouped["pro_rate"] = (grouped["n_pro"] / grouped["n_known"]).where(grouped["n_known"] > 0)
    grouped["pro_rate_wilson_low"], grouped["pro_rate_wilson_high"] = wilson_interval(
        successes=grouped["n_pro"],
        totals=grouped["n_known"],
    )
    grouped["is_low_power"] = low_power_mask(
        totals=grouped["n_known"],
        min_total=int(min_window_total),
    )
    return grouped.reset_index(drop=True)


def build_date_hour_primary_residual_distribution(
    window_control_profile: pd.DataFrame,
    *,
    primary_bucket_minutes: int,
) -> pd.DataFrame:
    if window_control_profile.empty:
        return pd.DataFrame()

    frame = window_control_profile.copy()
    frame["bucket_start"] = pd.to_datetime(frame["bucket_start"], errors="coerce")
    if "bucket_minutes" in frame.columns:
        frame["bucket_minutes"] = pd.to_numeric(frame["bucket_minutes"], errors="coerce")
    else:
        frame["bucket_minutes"] = float(primary_bucket_minutes)
    frame = frame[frame["bucket_start"].notna() & frame["bucket_minutes"].notna()].copy()
    if frame.empty:
        return pd.DataFrame()

    frame["is_off_hours_window"] = frame["is_off_hours_window"].fillna(False).astype(bool)

    frame["date"] = frame["bucket_start"].dt.strftime("%Y-%m-%d")
    frame["day_of_week"] = frame["bucket_start"].dt.day_name()
    frame["hour"] = frame["bucket_start"].dt.hour

    for column_name in (
        "n_total",
        "n_known",
        "n_pro",
        "n_con",
        "off_hours_fraction",
        "expected_pro_rate_primary",
        "delta_pro_rate_primary",
        "z_score_primary",
    ):
        frame[column_name] = pd.to_numeric(frame[column_name], errors="coerce")

    frame["is_alert_off_hours_window"] = (
        frame["is_alert_off_hours_window"].fillna(False).astype(bool)
    )
    frame["is_low_power"] = frame["is_low_power"].fillna(False).astype(bool)
    frame["is_primary_alert_window"] = frame["is_primary_alert_window"].fillna(False).astype(bool)
    frame["is_support_window"] = (~frame["is_low_power"]) & (frame["n_known"] > 0)
    frame["is_tested_window"] = frame["is_alert_off_hours_window"] & frame["is_support_window"]

    def _weighted_mean(values: pd.Series, weights: pd.Series) -> float:
        value_arr = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float, copy=False)
        weight_arr = pd.to_numeric(weights, errors="coerce").to_numpy(dtype=float, copy=False)
        valid = np.isfinite(value_arr) & np.isfinite(weight_arr) & (weight_arr > 0.0)
        if not valid.any():
            return float("nan")
        return float(np.average(value_arr[valid], weights=weight_arr[valid]))

    def _summarize_group(group: pd.DataFrame) -> pd.Series:
        support = group[group["is_support_window"]].copy()
        tested = group[group["is_tested_window"]].copy()
        support_z = pd.to_numeric(support["z_score_primary"], errors="coerce")
        support_delta = pd.to_numeric(support["delta_pro_rate_primary"], errors="coerce")
        support_expected = pd.to_numeric(support["expected_pro_rate_primary"], errors="coerce")
        support_n_known = pd.to_numeric(support["n_known"], errors="coerce")
        support_n_pro = pd.to_numeric(support["n_pro"], errors="coerce")

        tested_n_known = pd.to_numeric(tested["n_known"], errors="coerce")
        n_known_support = float(support_n_known.fillna(0.0).sum())
        n_pro_support = float(support_n_pro.fillna(0.0).sum())
        n_known_tested = float(tested_n_known.fillna(0.0).sum())
        n_windows_support = int(len(support))
        n_windows_tested = int(len(tested))
        n_primary_alert = int((tested["is_primary_alert_window"].fillna(False).astype(bool)).sum())
        n_windows = int(len(group))
        n_alert_eligible = int(group["is_alert_off_hours_window"].fillna(False).sum())

        return pd.Series(
            {
                "n_windows": n_windows,
                "n_windows_alert_eligible": n_alert_eligible,
                "n_windows_tested": n_windows_tested,
                "n_windows_low_power": max(0, n_alert_eligible - n_windows_tested),
                "n_windows_primary_alert": n_primary_alert,
                "primary_alert_fraction_tested": (
                    float(n_primary_alert / n_windows_tested)
                    if n_windows_tested > 0
                    else float("nan")
                ),
                "n_total": float(
                    pd.to_numeric(group["n_total"], errors="coerce").fillna(0.0).sum()
                ),
                "n_known": float(
                    pd.to_numeric(group["n_known"], errors="coerce").fillna(0.0).sum()
                ),
                "n_pro": float(pd.to_numeric(group["n_pro"], errors="coerce").fillna(0.0).sum()),
                "n_con": float(pd.to_numeric(group["n_con"], errors="coerce").fillna(0.0).sum()),
                "n_known_tested": n_known_tested,
                "off_hours_fraction": float(
                    pd.to_numeric(group["off_hours_fraction"], errors="coerce").dropna().mean()
                )
                if pd.to_numeric(group["off_hours_fraction"], errors="coerce").notna().any()
                else float("nan"),
                "pro_rate": float(n_pro_support / n_known_support)
                if n_known_support > 0.0
                else float("nan"),
                "expected_pro_rate_primary": _weighted_mean(support_expected, support_n_known),
                "delta_pro_rate_primary": _weighted_mean(support_delta, support_n_known),
                "z_score_primary": float(support_z.mean())
                if support_z.notna().any()
                else float("nan"),
                "z_score_primary_median": float(support_z.median())
                if support_z.notna().any()
                else float("nan"),
                "z_score_primary_abs_max": float(support_z.abs().max())
                if support_z.notna().any()
                else float("nan"),
                "is_low_power": bool(n_windows_support <= 0),
            }
        )

    grouped_rows: list[dict[str, object]] = []
    for (bucket_minutes_value, date, day_of_week, hour), group in frame.groupby(
        ["bucket_minutes", "date", "day_of_week", "hour"],
        dropna=False,
        sort=True,
    ):
        row = _summarize_group(group).to_dict()
        row["bucket_minutes"] = int(bucket_minutes_value)
        row["date"] = date
        row["day_of_week"] = day_of_week
        row["hour"] = hour
        grouped_rows.append(row)

    if not grouped_rows:
        return pd.DataFrame()

    grouped = (
        pd.DataFrame(grouped_rows)
        .sort_values(["bucket_minutes", "date", "hour"])
        .reset_index(drop=True)
    )
    for int_column in (
        "bucket_minutes",
        "hour",
        "n_windows",
        "n_windows_alert_eligible",
        "n_windows_tested",
        "n_windows_low_power",
        "n_windows_primary_alert",
        "n_total",
        "n_known",
        "n_pro",
        "n_con",
        "n_known_tested",
    ):
        grouped[int_column] = (
            pd.to_numeric(grouped[int_column], errors="coerce").fillna(0).astype(int)
        )
    return grouped


def build_off_hours_tables_and_summary(
    working: pd.DataFrame,
    *,
    window_control_profile: pd.DataFrame,
    primary_bucket_minutes: int | None,
    primary_bucket_profile: pd.DataFrame,
    min_window_total: int,
    fdr_alpha: float,
    model_min_rows: int,
    model_hour_harmonics: int,
    alert_off_hours_min_fraction: float,
    primary_alert_min_abs_delta: float,
) -> tuple[dict[str, object], dict[str, pd.DataFrame]]:
    off_hours = working[working["is_off_hours"]]
    on_hours = working[~working["is_off_hours"]]

    total = int(len(working))
    off_count = int(len(off_hours))
    on_count = int(len(on_hours))

    off_pro = int(off_hours["is_pro"].sum())
    off_con = int(off_hours["is_con"].sum())
    on_pro = int(on_hours["is_pro"].sum())
    on_con = int(on_hours["is_con"].sum())
    off_known_total = int(off_pro + off_con)
    on_known_total = int(on_pro + on_con)
    low_power_min_total = int(min_window_total)

    p_value = 1.0
    contingency = np.array([[off_pro, off_con], [on_pro, on_con]], dtype=float)
    if (
        contingency.sum() > 0
        and contingency.shape == (2, 2)
        and (contingency.sum(axis=1) > 0).all()
        and (contingency.sum(axis=0) > 0).all()
    ):
        _chi2, p_value, _dof, _expected = chi2_contingency(contingency, correction=False)

    summary_table = pd.DataFrame(
        [
            {
                "total": total,
                "off_hours": off_count,
                "on_hours": on_count,
                "off_hours_ratio": (off_count / total) if total else 0.0,
                "off_hours_pro_rate": (off_pro / off_known_total) if off_known_total else np.nan,
                "on_hours_pro_rate": (on_pro / on_known_total) if on_known_total else np.nan,
                "chi_square_p_value": p_value,
            }
        ]
    )
    off_low, off_high = wilson_interval(
        successes=pd.Series([off_pro]),
        totals=pd.Series([off_known_total]),
    )
    on_low, on_high = wilson_interval(
        successes=pd.Series([on_pro]),
        totals=pd.Series([on_known_total]),
    )
    summary_table["off_hours_pro_rate_wilson_low"] = float(off_low[0])
    summary_table["off_hours_pro_rate_wilson_high"] = float(off_high[0])
    summary_table["off_hours_pro_rate_wilson_half_width"] = float(
        wilson_half_width(
            successes=pd.Series([off_pro]),
            totals=pd.Series([off_known_total]),
        )[0]
    )
    summary_table["on_hours_pro_rate_wilson_low"] = float(on_low[0])
    summary_table["on_hours_pro_rate_wilson_high"] = float(on_high[0])
    summary_table["on_hours_pro_rate_wilson_half_width"] = float(
        wilson_half_width(
            successes=pd.Series([on_pro]),
            totals=pd.Series([on_known_total]),
        )[0]
    )
    summary_table["off_hours_is_low_power"] = bool(
        low_power_mask(
            totals=pd.Series([off_known_total]),
            min_total=low_power_min_total,
        )[0]
    )
    summary_table["on_hours_is_low_power"] = bool(
        low_power_mask(
            totals=pd.Series([on_known_total]),
            min_total=low_power_min_total,
        )[0]
    )

    if primary_bucket_minutes is None:
        summary_table["primary_bucket_minutes"] = np.nan
        summary_table["primary_baseline_method"] = "unavailable"
        summary_table["primary_model_fit_method"] = "unavailable"
        summary_table["primary_model_fit_rows"] = 0
        summary_table["primary_model_fit_unique_days"] = 0
        summary_table["primary_model_fit_unique_hours"] = 0
        summary_table["primary_model_fit_converged"] = np.nan
        summary_table["primary_model_fit_aic"] = np.nan
        summary_table["alert_off_hours_min_fraction"] = float(alert_off_hours_min_fraction)
        summary_table["primary_alert_min_abs_delta"] = float(primary_alert_min_abs_delta)
        summary_table["off_hours_windows_alert_eligible"] = 0
        summary_table["off_hours_windows_alert_eligible_low_power"] = 0
        summary_table["off_hours_windows_alert_eligible_tested_fraction"] = np.nan
        summary_table["off_hours_windows_alert_eligible_low_power_fraction"] = np.nan
        summary_table["off_hours_windows_tested"] = 0
        summary_table["off_hours_windows_below_day_control_95"] = 0
        summary_table["off_hours_windows_below_day_control_998"] = 0
        summary_table["off_hours_windows_below_model_control_95"] = 0
        summary_table["off_hours_windows_below_model_control_998"] = 0
        summary_table["off_hours_windows_below_primary_control_95"] = 0
        summary_table["off_hours_windows_below_primary_control_998"] = 0
        summary_table["off_hours_windows_above_primary_control_95"] = 0
        summary_table["off_hours_windows_above_primary_control_998"] = 0
        summary_table["off_hours_windows_significant_day"] = 0
        summary_table["off_hours_windows_significant_model"] = 0
        summary_table["off_hours_windows_significant_primary"] = 0
        summary_table["off_hours_windows_significant_primary_upper"] = 0
        summary_table["off_hours_windows_significant_primary_two_sided"] = 0
        summary_table["off_hours_windows_primary_spc_998_any"] = 0
        summary_table["off_hours_windows_primary_fdr_two_sided"] = 0
        summary_table["off_hours_windows_primary_flag_any"] = 0
        summary_table["off_hours_windows_primary_flag_both"] = 0
        summary_table["off_hours_windows_primary_spc_998_any_fraction"] = np.nan
        summary_table["off_hours_windows_primary_fdr_two_sided_fraction"] = np.nan
        summary_table["off_hours_windows_primary_flag_any_fraction"] = np.nan
        summary_table["off_hours_windows_primary_flag_both_fraction"] = np.nan
        summary_table["off_hours_windows_primary_alert"] = 0
        summary_table["off_hours_windows_primary_alert_fraction"] = np.nan
        summary_table["off_hours_primary_alert_run_count"] = 0
        summary_table["off_hours_primary_alert_max_run_windows"] = 0
        summary_table["off_hours_primary_alert_max_run_minutes"] = 0
        summary_table["off_hours_min_day_z"] = np.nan
        summary_table["off_hours_max_abs_day_z"] = np.nan
        summary_table["off_hours_min_model_z"] = np.nan
        summary_table["off_hours_max_abs_model_z"] = np.nan
        summary_table["off_hours_min_primary_z"] = np.nan
        summary_table["off_hours_max_abs_primary_z"] = np.nan
        summary_table["off_hours_min_primary_delta"] = np.nan
        summary_table["off_hours_max_abs_primary_delta"] = np.nan
        summary_table["off_hours_windows_model_available"] = 0
        summary_table["global_daytime_pro_rate"] = np.nan
    else:
        alert_eligible_off_hours = primary_bucket_profile[
            primary_bucket_profile["is_alert_off_hours_window"]
            & primary_bucket_profile["pro_rate"].notna()
        ].copy()
        tested_off_hours = alert_eligible_off_hours[
            ~alert_eligible_off_hours["is_low_power"]
        ].copy()
        alert_eligible_count = int(len(alert_eligible_off_hours))
        alert_eligible_low_power_count = int(alert_eligible_off_hours["is_low_power"].sum())
        model_available = tested_off_hours.get(
            "is_model_baseline_available",
            pd.Series(dtype=bool),
        )
        summary_table["primary_bucket_minutes"] = int(primary_bucket_minutes)
        summary_table["alert_off_hours_min_fraction"] = float(alert_off_hours_min_fraction)
        summary_table["primary_alert_min_abs_delta"] = float(primary_alert_min_abs_delta)
        summary_table["primary_baseline_method"] = (
            "model_day_hour" if bool(model_available.any()) else "day_on_hours_fallback"
        )
        if "model_fit_method" in primary_bucket_profile.columns:
            primary_fit_method_series = primary_bucket_profile["model_fit_method"].astype("string")
            primary_fit_method = (
                str(primary_fit_method_series.dropna().iloc[0]).strip()
                if primary_fit_method_series.notna().any()
                else "unavailable"
            )
        else:
            primary_fit_method = "unavailable"
        summary_table["primary_model_fit_method"] = primary_fit_method or "unavailable"
        summary_table["primary_model_fit_rows"] = int(
            pd.to_numeric(
                primary_bucket_profile.get("model_fit_rows", pd.Series(dtype=float)),
                errors="coerce",
            )
            .fillna(0)
            .max()
        )
        summary_table["primary_model_fit_unique_days"] = int(
            pd.to_numeric(
                primary_bucket_profile.get("model_fit_unique_days", pd.Series(dtype=float)),
                errors="coerce",
            )
            .fillna(0)
            .max()
        )
        summary_table["primary_model_fit_unique_hours"] = int(
            pd.to_numeric(
                primary_bucket_profile.get("model_fit_unique_hours", pd.Series(dtype=float)),
                errors="coerce",
            )
            .fillna(0)
            .max()
        )
        summary_table["primary_model_fit_converged"] = (
            float(
                pd.to_numeric(
                    primary_bucket_profile.get("model_fit_converged", pd.Series(dtype=float)),
                    errors="coerce",
                )
                .dropna()
                .iloc[0]
            )
            if pd.to_numeric(
                primary_bucket_profile.get("model_fit_converged", pd.Series(dtype=float)),
                errors="coerce",
            )
            .notna()
            .any()
            else np.nan
        )
        summary_table["primary_model_fit_aic"] = (
            float(
                pd.to_numeric(
                    primary_bucket_profile.get("model_fit_aic", pd.Series(dtype=float)),
                    errors="coerce",
                )
                .dropna()
                .iloc[0]
            )
            if pd.to_numeric(
                primary_bucket_profile.get("model_fit_aic", pd.Series(dtype=float)),
                errors="coerce",
            )
            .notna()
            .any()
            else np.nan
        )
        summary_table["off_hours_windows_alert_eligible"] = alert_eligible_count
        summary_table["off_hours_windows_alert_eligible_low_power"] = alert_eligible_low_power_count
        summary_table["off_hours_windows_alert_eligible_tested_fraction"] = (
            (len(tested_off_hours) / alert_eligible_count) if alert_eligible_count > 0 else np.nan
        )
        summary_table["off_hours_windows_alert_eligible_low_power_fraction"] = (
            (alert_eligible_low_power_count / alert_eligible_count)
            if alert_eligible_count > 0
            else np.nan
        )
        summary_table["off_hours_windows_tested"] = int(len(tested_off_hours))
        summary_table["off_hours_windows_below_day_control_95"] = int(
            tested_off_hours["is_below_day_control_95"].sum()
        )
        summary_table["off_hours_windows_below_day_control_998"] = int(
            tested_off_hours["is_below_day_control_998"].sum()
        )
        summary_table["off_hours_windows_below_model_control_95"] = int(
            tested_off_hours["is_below_model_control_95"].sum()
        )
        summary_table["off_hours_windows_below_model_control_998"] = int(
            tested_off_hours["is_below_model_control_998"].sum()
        )
        summary_table["off_hours_windows_below_primary_control_95"] = int(
            tested_off_hours["is_below_primary_control_95"].sum()
        )
        summary_table["off_hours_windows_below_primary_control_998"] = int(
            tested_off_hours["is_below_primary_control_998"].sum()
        )
        summary_table["off_hours_windows_above_primary_control_95"] = int(
            tested_off_hours["is_above_primary_control_95"].sum()
        )
        summary_table["off_hours_windows_above_primary_control_998"] = int(
            tested_off_hours["is_above_primary_control_998"].sum()
        )
        summary_table["off_hours_windows_significant_day"] = int(
            tested_off_hours["is_significant_day"].sum()
        )
        summary_table["off_hours_windows_significant_model"] = int(
            tested_off_hours["is_significant_model"].sum()
        )
        summary_table["off_hours_windows_significant_primary"] = int(
            tested_off_hours["is_significant_primary"].sum()
        )
        summary_table["off_hours_windows_significant_primary_upper"] = int(
            tested_off_hours["is_significant_primary_upper"].sum()
        )
        summary_table["off_hours_windows_significant_primary_two_sided"] = int(
            tested_off_hours["is_significant_primary_two_sided"].sum()
        )
        primary_spc_998_any = int(tested_off_hours["is_primary_spc_998_two_sided"].sum())
        primary_fdr_two_sided = int(tested_off_hours["is_primary_fdr_two_sided"].sum())
        primary_any_channel = int(tested_off_hours["is_primary_any_flag_channel"].sum())
        primary_both_channels = int(tested_off_hours["is_primary_both_flag_channels"].sum())
        tested_window_count = int(len(tested_off_hours))
        summary_table["off_hours_windows_primary_spc_998_any"] = primary_spc_998_any
        summary_table["off_hours_windows_primary_fdr_two_sided"] = primary_fdr_two_sided
        summary_table["off_hours_windows_primary_flag_any"] = primary_any_channel
        summary_table["off_hours_windows_primary_flag_both"] = primary_both_channels
        summary_table["off_hours_windows_primary_spc_998_any_fraction"] = (
            (primary_spc_998_any / tested_window_count) if tested_window_count > 0 else np.nan
        )
        summary_table["off_hours_windows_primary_fdr_two_sided_fraction"] = (
            (primary_fdr_two_sided / tested_window_count) if tested_window_count > 0 else np.nan
        )
        summary_table["off_hours_windows_primary_flag_any_fraction"] = (
            (primary_any_channel / tested_window_count) if tested_window_count > 0 else np.nan
        )
        summary_table["off_hours_windows_primary_flag_both_fraction"] = (
            (primary_both_channels / tested_window_count) if tested_window_count > 0 else np.nan
        )
        primary_alert_windows = int(tested_off_hours["is_primary_alert_window"].sum())
        summary_table["off_hours_windows_primary_alert"] = primary_alert_windows
        summary_table["off_hours_windows_primary_alert_fraction"] = (
            (primary_alert_windows / tested_window_count) if tested_window_count > 0 else np.nan
        )
        ordered_primary = primary_bucket_profile.sort_values("bucket_start").copy()
        alert_flags = ordered_primary["is_primary_alert_window"].fillna(False).astype(bool)
        bucket_start_values = pd.to_datetime(
            ordered_primary["bucket_start"],
            errors="coerce",
        )
        gap_break = pd.Series(False, index=ordered_primary.index, dtype=bool)
        if bucket_start_values.notna().any():
            expected_gap = pd.Timedelta(minutes=max(1, int(primary_bucket_minutes)) * 2)
            gap_break = (bucket_start_values.diff() > expected_gap).fillna(True)
        run_starts = alert_flags & (~alert_flags.shift(fill_value=False) | gap_break)
        run_ids = run_starts.cumsum()
        run_lengths = alert_flags.groupby(run_ids).sum()
        run_lengths = run_lengths[run_lengths > 0]
        run_count = int(run_lengths.shape[0]) if not run_lengths.empty else 0
        max_run_windows = int(run_lengths.max()) if not run_lengths.empty else 0
        summary_table["off_hours_primary_alert_run_count"] = run_count
        summary_table["off_hours_primary_alert_max_run_windows"] = max_run_windows
        summary_table["off_hours_primary_alert_max_run_minutes"] = int(
            max_run_windows * int(primary_bucket_minutes)
        )
        z_scores = pd.to_numeric(tested_off_hours["z_score_day"], errors="coerce")
        summary_table["off_hours_min_day_z"] = (
            float(z_scores.min()) if z_scores.notna().any() else np.nan
        )
        summary_table["off_hours_max_abs_day_z"] = (
            float(z_scores.abs().max()) if z_scores.notna().any() else np.nan
        )
        z_model_scores = pd.to_numeric(tested_off_hours["z_score_model"], errors="coerce")
        summary_table["off_hours_min_model_z"] = (
            float(z_model_scores.min()) if z_model_scores.notna().any() else np.nan
        )
        summary_table["off_hours_max_abs_model_z"] = (
            float(z_model_scores.abs().max()) if z_model_scores.notna().any() else np.nan
        )
        z_primary_scores = pd.to_numeric(tested_off_hours["z_score_primary"], errors="coerce")
        summary_table["off_hours_min_primary_z"] = (
            float(z_primary_scores.min()) if z_primary_scores.notna().any() else np.nan
        )
        summary_table["off_hours_max_abs_primary_z"] = (
            float(z_primary_scores.abs().max()) if z_primary_scores.notna().any() else np.nan
        )
        delta_primary = pd.to_numeric(
            tested_off_hours["delta_pro_rate_primary"],
            errors="coerce",
        )
        summary_table["off_hours_min_primary_delta"] = (
            float(delta_primary.min()) if delta_primary.notna().any() else np.nan
        )
        summary_table["off_hours_max_abs_primary_delta"] = (
            float(delta_primary.abs().max()) if delta_primary.notna().any() else np.nan
        )
        summary_table["off_hours_windows_model_available"] = int(
            tested_off_hours["is_model_baseline_available"].sum()
        )
        global_rate = pd.to_numeric(
            primary_bucket_profile["expected_pro_rate_global"],
            errors="coerce",
        )
        summary_table["global_daytime_pro_rate"] = (
            float(global_rate.dropna().iloc[0]) if global_rate.notna().any() else np.nan
        )
    summary_table["day_adjusted_fdr_alpha"] = float(fdr_alpha)
    summary_table["model_fit_min_rows"] = int(model_min_rows)
    summary_table["model_hour_harmonics"] = int(model_hour_harmonics)

    hourly_distribution = (
        working.groupby("hour", dropna=True)
        .agg(
            n_total=("is_pro", "size"),
            n_pro=("is_pro", "sum"),
            n_con=("is_con", "sum"),
        )
        .reset_index()
        .sort_values("hour")
    )
    hourly_distribution["pro_rate"] = (
        hourly_distribution["n_pro"] / (hourly_distribution["n_pro"] + hourly_distribution["n_con"])
    ).where((hourly_distribution["n_pro"] + hourly_distribution["n_con"]) > 0)
    hourly_distribution["pro_rate_wilson_low"], hourly_distribution["pro_rate_wilson_high"] = (
        wilson_interval(
            successes=hourly_distribution["n_pro"],
            totals=hourly_distribution["n_pro"] + hourly_distribution["n_con"],
        )
    )
    hourly_distribution["pro_rate_wilson_half_width"] = wilson_half_width(
        successes=hourly_distribution["n_pro"],
        totals=hourly_distribution["n_pro"] + hourly_distribution["n_con"],
    )
    hourly_distribution["is_low_power"] = low_power_mask(
        totals=hourly_distribution["n_pro"] + hourly_distribution["n_con"],
        min_total=low_power_min_total,
    )

    day_name_lookup = {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday",
    }
    day_index_lookup = {value: key for key, value in day_name_lookup.items()}

    day_labels = pd.Series(["Unknown"] * len(working), index=working.index, dtype="string")
    day_indices = pd.Series([-1] * len(working), index=working.index, dtype="int64")
    timestamps = (
        pd.to_datetime(working["timestamp"], errors="coerce")
        if "timestamp" in working.columns
        else pd.Series(pd.NaT, index=working.index)
    )
    has_valid_timestamps = bool(timestamps.notna().any())
    if has_valid_timestamps:
        day_labels = timestamps.dt.day_name().fillna("Unknown")
        day_indices = timestamps.dt.dayofweek.fillna(-1).astype(int)
    elif "day_of_week" in working.columns:
        day_values = working["day_of_week"]
        numeric_days = pd.to_numeric(day_values, errors="coerce")
        numeric_mask = numeric_days.notna()
        if numeric_mask.any():
            day_labels = numeric_days.astype("Int64").map(day_name_lookup).fillna("Unknown")
            day_indices = numeric_days.fillna(-1).astype(int)
        else:
            text_days = day_values.astype("string").fillna("Unknown")
            normalized_days = text_days.str.strip().str.title()
            day_labels = normalized_days.where(normalized_days != "", "Unknown")
            day_indices = day_labels.map(day_index_lookup).fillna(-1).astype(int)

    hour_of_week_distribution = (
        working.assign(
            day_of_week=day_labels,
            day_of_week_index=day_indices,
        )
        .groupby(["day_of_week", "day_of_week_index", "hour"], dropna=False)
        .agg(
            n_total=("is_pro", "size"),
            n_pro=("is_pro", "sum"),
            n_con=("is_con", "sum"),
            n_off_hours=("is_off_hours", "sum"),
        )
        .reset_index()
        .sort_values(["day_of_week_index", "hour", "day_of_week"])
    )
    hour_of_week_distribution["off_hours_fraction"] = (
        hour_of_week_distribution["n_off_hours"]
        / hour_of_week_distribution["n_total"].replace(0, np.nan)
    ).fillna(0.0)
    hour_of_week_distribution["pro_rate"] = (
        hour_of_week_distribution["n_pro"]
        / (hour_of_week_distribution["n_pro"] + hour_of_week_distribution["n_con"])
    ).where((hour_of_week_distribution["n_pro"] + hour_of_week_distribution["n_con"]) > 0)
    (
        hour_of_week_distribution["pro_rate_wilson_low"],
        hour_of_week_distribution["pro_rate_wilson_high"],
    ) = wilson_interval(
        successes=hour_of_week_distribution["n_pro"],
        totals=hour_of_week_distribution["n_pro"] + hour_of_week_distribution["n_con"],
    )
    hour_of_week_distribution["is_low_power"] = low_power_mask(
        totals=hour_of_week_distribution["n_pro"] + hour_of_week_distribution["n_con"],
        min_total=low_power_min_total,
    )
    hour_of_week_distribution = hour_of_week_distribution[
        hour_of_week_distribution["day_of_week"] != "Unknown"
    ].copy()

    date_hour_distribution = build_date_hour_distribution(
        working,
        min_window_total=low_power_min_total,
    )
    date_hour_primary_residual_distribution = build_date_hour_primary_residual_distribution(
        window_control_profile,
        primary_bucket_minutes=primary_bucket_minutes or 30,
    )

    model_fit_diagnostics_columns = [
        "bucket_minutes",
        "model_fit_method",
        "model_fit_rows",
        "model_fit_unique_days",
        "model_fit_unique_hours",
        "model_fit_converged",
        "model_fit_aic",
        "model_fit_used_harmonics",
        "model_fit_window_count",
        "model_fit_available_windows",
        "model_fit_available_fraction",
    ]
    model_fit_diagnostics = pd.DataFrame(columns=model_fit_diagnostics_columns)
    if not window_control_profile.empty:
        model_fit_diagnostics = (
            window_control_profile.sort_values(["bucket_minutes", "bucket_start"])
            .groupby("bucket_minutes", as_index=False)
            .agg(
                model_fit_method=("model_fit_method", "first"),
                model_fit_rows=("model_fit_rows", "max"),
                model_fit_unique_days=("model_fit_unique_days", "max"),
                model_fit_unique_hours=("model_fit_unique_hours", "max"),
                model_fit_converged=("model_fit_converged", "first"),
                model_fit_aic=("model_fit_aic", "first"),
                model_fit_used_harmonics=("model_fit_used_harmonics", "max"),
                model_fit_window_count=("bucket_start", "count"),
                model_fit_available_windows=("is_model_baseline_available", "sum"),
            )
            .sort_values("bucket_minutes")
            .reset_index(drop=True)
        )
        model_fit_diagnostics["model_fit_available_fraction"] = pd.to_numeric(
            model_fit_diagnostics["model_fit_available_windows"],
            errors="coerce",
        ) / pd.to_numeric(model_fit_diagnostics["model_fit_window_count"], errors="coerce")
        for column_name in (
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_used_harmonics",
            "model_fit_window_count",
            "model_fit_available_windows",
        ):
            model_fit_diagnostics[column_name] = (
                pd.to_numeric(
                    model_fit_diagnostics[column_name],
                    errors="coerce",
                )
                .fillna(0)
                .astype(int)
            )

    tested_windows = _safe_int(summary_table.loc[0, "off_hours_windows_tested"])

    def _share_of_tested(count: int) -> float:
        if tested_windows <= 0:
            return float("nan")
        return float(count / tested_windows)

    channel_rows = [
        {
            "rank": 1,
            "channel": "tested_off_hours_windows",
            "channel_label": "Tested off-hours windows",
            "count": tested_windows,
            "share_of_tested": 1.0 if tested_windows > 0 else np.nan,
        },
        {
            "rank": 2,
            "channel": "primary_spc_998_two_sided",
            "channel_label": "Primary 99.8% breach (two-sided)",
            "count": _safe_int(summary_table.loc[0, "off_hours_windows_primary_spc_998_any"]),
            "share_of_tested": _share_of_tested(
                _safe_int(summary_table.loc[0, "off_hours_windows_primary_spc_998_any"])
            ),
        },
        {
            "rank": 3,
            "channel": "primary_fdr_two_sided",
            "channel_label": "Primary two-sided FDR-significant",
            "count": _safe_int(summary_table.loc[0, "off_hours_windows_primary_fdr_two_sided"]),
            "share_of_tested": _share_of_tested(
                _safe_int(summary_table.loc[0, "off_hours_windows_primary_fdr_two_sided"])
            ),
        },
        {
            "rank": 4,
            "channel": "primary_any_flag_channel",
            "channel_label": "Any primary flag channel",
            "count": _safe_int(summary_table.loc[0, "off_hours_windows_primary_flag_any"]),
            "share_of_tested": _share_of_tested(
                _safe_int(summary_table.loc[0, "off_hours_windows_primary_flag_any"])
            ),
        },
        {
            "rank": 5,
            "channel": "primary_both_flag_channels",
            "channel_label": "Both primary flag channels",
            "count": _safe_int(summary_table.loc[0, "off_hours_windows_primary_flag_both"]),
            "share_of_tested": _share_of_tested(
                _safe_int(summary_table.loc[0, "off_hours_windows_primary_flag_both"])
            ),
        },
        {
            "rank": 6,
            "channel": "robust_primary_alert",
            "channel_label": "Robust primary alerts",
            "count": _safe_int(summary_table.loc[0, "off_hours_windows_primary_alert"]),
            "share_of_tested": _share_of_tested(
                _safe_int(summary_table.loc[0, "off_hours_windows_primary_alert"])
            ),
        },
    ]
    flag_channel_summary = pd.DataFrame(channel_rows)

    flagged_window_columns = [
        "bucket_start",
        "bucket_minutes",
        "n_total",
        "n_known",
        "n_pro",
        "n_con",
        "pro_rate",
        "expected_pro_rate_primary",
        "delta_pro_rate_primary",
        "z_score_primary",
        "p_value_primary_two_sided",
        "q_value_primary_two_sided",
        "is_primary_spc_998_two_sided",
        "is_primary_fdr_two_sided",
        "is_primary_any_flag_channel",
        "is_primary_both_flag_channels",
        "is_primary_alert_window",
        "is_model_baseline_available",
        "model_fit_method",
        "model_fit_rows",
        "model_fit_unique_days",
        "model_fit_unique_hours",
        "model_fit_used_harmonics",
        "primary_baseline_source",
        "is_low_power",
    ]
    flagged_window_diagnostics = pd.DataFrame(columns=flagged_window_columns)
    if primary_bucket_minutes is not None and not primary_bucket_profile.empty:
        inferential_primary = primary_bucket_profile[
            primary_bucket_profile["is_alert_off_hours_window"]
            & (~primary_bucket_profile["is_low_power"])
            & primary_bucket_profile["pro_rate"].notna()
        ].copy()
        if not inferential_primary.empty:
            flagged_primary = inferential_primary[
                inferential_primary["is_primary_any_flag_channel"]
                | inferential_primary["is_primary_alert_window"]
            ].copy()
            if not flagged_primary.empty:
                flagged_primary["abs_z_score_primary"] = pd.to_numeric(
                    flagged_primary["z_score_primary"],
                    errors="coerce",
                ).abs()
                flagged_primary = flagged_primary.sort_values(
                    [
                        "is_primary_alert_window",
                        "is_primary_both_flag_channels",
                        "is_primary_any_flag_channel",
                        "abs_z_score_primary",
                        "bucket_start",
                    ],
                    ascending=[False, False, False, False, True],
                )
                flagged_window_diagnostics = flagged_primary[flagged_window_columns].reset_index(
                    drop=True
                )

    date_hour_primary_residual_cells = int(len(date_hour_primary_residual_distribution))
    if (
        primary_bucket_minutes is not None
        and not date_hour_primary_residual_distribution.empty
        and "bucket_minutes" in date_hour_primary_residual_distribution.columns
    ):
        primary_bucket_mask = pd.to_numeric(
            date_hour_primary_residual_distribution["bucket_minutes"],
            errors="coerce",
        ) == float(primary_bucket_minutes)
        date_hour_primary_residual_cells = int(primary_bucket_mask.sum())

    summary = {
        "off_hours_ratio": float(summary_table.loc[0, "off_hours_ratio"]),
        "chi_square_p_value": float(p_value),
        "low_power_min_total": int(low_power_min_total),
        "off_hours_is_low_power": bool(summary_table.loc[0, "off_hours_is_low_power"]),
        "on_hours_is_low_power": bool(summary_table.loc[0, "on_hours_is_low_power"]),
        "hour_of_week_cells": int(len(hour_of_week_distribution)),
        "date_hour_cells": int(len(date_hour_distribution)),
        "date_hour_primary_residual_cells": date_hour_primary_residual_cells,
        "window_profile_rows": int(len(window_control_profile)),
        "primary_bucket_minutes": int(primary_bucket_minutes)
        if primary_bucket_minutes is not None
        else None,
        "primary_baseline_method": str(summary_table.loc[0, "primary_baseline_method"]).strip(),
        "alert_off_hours_min_fraction": _safe_float(
            summary_table.loc[0, "alert_off_hours_min_fraction"],
            default=alert_off_hours_min_fraction,
        ),
        "primary_alert_min_abs_delta": _safe_float(
            summary_table.loc[0, "primary_alert_min_abs_delta"],
            default=primary_alert_min_abs_delta,
        ),
        "model_hour_harmonics": _safe_int(summary_table.loc[0, "model_hour_harmonics"]),
        "primary_model_fit_method": str(summary_table.loc[0, "primary_model_fit_method"]).strip(),
        "primary_model_fit_rows": _safe_int(summary_table.loc[0, "primary_model_fit_rows"]),
        "primary_model_fit_unique_days": _safe_int(
            summary_table.loc[0, "primary_model_fit_unique_days"]
        ),
        "primary_model_fit_unique_hours": _safe_int(
            summary_table.loc[0, "primary_model_fit_unique_hours"]
        ),
        "primary_model_fit_converged": _safe_float(
            summary_table.loc[0, "primary_model_fit_converged"],
            default=np.nan,
        ),
        "primary_model_fit_aic": _safe_float(
            summary_table.loc[0, "primary_model_fit_aic"],
            default=np.nan,
        ),
        "off_hours_windows_alert_eligible": _safe_int(
            summary_table.loc[0, "off_hours_windows_alert_eligible"]
        ),
        "off_hours_windows_alert_eligible_low_power": _safe_int(
            summary_table.loc[0, "off_hours_windows_alert_eligible_low_power"]
        ),
        "off_hours_windows_alert_eligible_tested_fraction": _safe_float(
            summary_table.loc[0, "off_hours_windows_alert_eligible_tested_fraction"],
            default=np.nan,
        ),
        "off_hours_windows_alert_eligible_low_power_fraction": _safe_float(
            summary_table.loc[0, "off_hours_windows_alert_eligible_low_power_fraction"],
            default=np.nan,
        ),
        "off_hours_windows_tested": _safe_int(summary_table.loc[0, "off_hours_windows_tested"]),
        "off_hours_windows_below_day_control_95": _safe_int(
            summary_table.loc[0, "off_hours_windows_below_day_control_95"]
        ),
        "off_hours_windows_below_day_control_998": _safe_int(
            summary_table.loc[0, "off_hours_windows_below_day_control_998"]
        ),
        "off_hours_windows_below_model_control_95": _safe_int(
            summary_table.loc[0, "off_hours_windows_below_model_control_95"]
        ),
        "off_hours_windows_below_model_control_998": _safe_int(
            summary_table.loc[0, "off_hours_windows_below_model_control_998"]
        ),
        "off_hours_windows_below_primary_control_95": _safe_int(
            summary_table.loc[0, "off_hours_windows_below_primary_control_95"]
        ),
        "off_hours_windows_below_primary_control_998": _safe_int(
            summary_table.loc[0, "off_hours_windows_below_primary_control_998"]
        ),
        "off_hours_windows_significant_primary": _safe_int(
            summary_table.loc[0, "off_hours_windows_significant_primary"]
        ),
        "off_hours_windows_significant_primary_upper": _safe_int(
            summary_table.loc[0, "off_hours_windows_significant_primary_upper"]
        ),
        "off_hours_windows_significant_primary_two_sided": _safe_int(
            summary_table.loc[0, "off_hours_windows_significant_primary_two_sided"]
        ),
        "off_hours_windows_primary_spc_998_any": _safe_int(
            summary_table.loc[0, "off_hours_windows_primary_spc_998_any"]
        ),
        "off_hours_windows_primary_fdr_two_sided": _safe_int(
            summary_table.loc[0, "off_hours_windows_primary_fdr_two_sided"]
        ),
        "off_hours_windows_primary_flag_any": _safe_int(
            summary_table.loc[0, "off_hours_windows_primary_flag_any"]
        ),
        "off_hours_windows_primary_flag_both": _safe_int(
            summary_table.loc[0, "off_hours_windows_primary_flag_both"]
        ),
        "off_hours_windows_primary_spc_998_any_fraction": _safe_float(
            summary_table.loc[0, "off_hours_windows_primary_spc_998_any_fraction"],
            default=np.nan,
        ),
        "off_hours_windows_primary_fdr_two_sided_fraction": _safe_float(
            summary_table.loc[0, "off_hours_windows_primary_fdr_two_sided_fraction"],
            default=np.nan,
        ),
        "off_hours_windows_primary_flag_any_fraction": _safe_float(
            summary_table.loc[0, "off_hours_windows_primary_flag_any_fraction"],
            default=np.nan,
        ),
        "off_hours_windows_primary_flag_both_fraction": _safe_float(
            summary_table.loc[0, "off_hours_windows_primary_flag_both_fraction"],
            default=np.nan,
        ),
        "off_hours_windows_primary_alert": _safe_int(
            summary_table.loc[0, "off_hours_windows_primary_alert"]
        ),
        "off_hours_windows_primary_alert_fraction": _safe_float(
            summary_table.loc[0, "off_hours_windows_primary_alert_fraction"],
            default=np.nan,
        ),
        "off_hours_primary_alert_run_count": _safe_int(
            summary_table.loc[0, "off_hours_primary_alert_run_count"]
        ),
        "off_hours_primary_alert_max_run_windows": _safe_int(
            summary_table.loc[0, "off_hours_primary_alert_max_run_windows"]
        ),
        "off_hours_primary_alert_max_run_minutes": _safe_int(
            summary_table.loc[0, "off_hours_primary_alert_max_run_minutes"]
        ),
        "off_hours_windows_model_available": _safe_int(
            summary_table.loc[0, "off_hours_windows_model_available"]
        ),
        "max_hour_of_week_pro_rate": float(
            pd.to_numeric(hour_of_week_distribution["pro_rate"], errors="coerce").fillna(0.0).max()
        )
        if not hour_of_week_distribution.empty
        else 0.0,
    }

    tables = {
        "off_hours_summary": summary_table,
        "hourly_distribution": hourly_distribution,
        "hour_of_week_distribution": hour_of_week_distribution,
        "date_hour_distribution": date_hour_distribution,
        "date_hour_primary_residual_distribution": date_hour_primary_residual_distribution,
        "window_control_profile": window_control_profile,
        "model_fit_diagnostics": model_fit_diagnostics,
        "flag_channel_summary": flag_channel_summary,
        "flagged_window_diagnostics": flagged_window_diagnostics,
    }
    return summary, tables
