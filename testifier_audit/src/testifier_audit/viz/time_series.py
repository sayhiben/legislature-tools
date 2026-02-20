from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from testifier_audit.proportion_stats import (
    DEFAULT_LOW_POWER_MIN_TOTAL,
    low_power_mask,
    wilson_interval,
)
from testifier_audit.viz.common import save_figure


def plot_counts_per_minute(counts_per_minute: pd.DataFrame, output_path: Path) -> Path:
    plt.figure(figsize=(12, 4))
    plt.plot(counts_per_minute["minute_bucket"], counts_per_minute["n_total"], linewidth=1.5)
    plt.title("Submissions per minute")
    plt.xlabel("Minute")
    plt.ylabel("Count")
    return save_figure(output_path)


def _window_subset(df: pd.DataFrame, limit: int = 20) -> pd.DataFrame:
    if df.empty:
        return df
    if "q_value" in df.columns:
        return df.sort_values(["q_value", "p_value"], ascending=[True, True]).head(limit)
    return df.head(limit)


def _resolve_wilson_bounds(
    subset: pd.DataFrame,
    *,
    low_col: str,
    high_col: str,
    successes_col: str | None = None,
    totals_col: str | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    if low_col in subset.columns and high_col in subset.columns:
        return (
            pd.to_numeric(subset[low_col], errors="coerce").to_numpy(dtype=float),
            pd.to_numeric(subset[high_col], errors="coerce").to_numpy(dtype=float),
        )
    if successes_col and totals_col and {successes_col, totals_col}.issubset(set(subset.columns)):
        lower, upper = wilson_interval(
            successes=pd.to_numeric(subset[successes_col], errors="coerce").fillna(0.0),
            totals=pd.to_numeric(subset[totals_col], errors="coerce").fillna(0.0),
        )
        return lower, upper
    empty = np.full(len(subset), np.nan, dtype=float)
    return empty, empty


def _resolve_low_power_flags(
    subset: pd.DataFrame,
    *,
    low_power_col: str | None = None,
    totals_col: str | None = None,
) -> np.ndarray:
    if low_power_col and low_power_col in subset.columns:
        return subset[low_power_col].fillna(False).astype(bool).to_numpy()
    if totals_col and totals_col in subset.columns:
        return low_power_mask(
            totals=pd.to_numeric(subset[totals_col], errors="coerce").fillna(0.0),
            min_total=DEFAULT_LOW_POWER_MIN_TOTAL,
        )
    return np.zeros(len(subset), dtype=bool)


def plot_counts_with_annotations(
    counts_per_minute: pd.DataFrame,
    burst_windows: pd.DataFrame,
    volume_changepoints: pd.DataFrame,
    output_path: Path,
) -> Path:
    fig, ax = plt.subplots(figsize=(14, 4))
    ax.plot(
        counts_per_minute["minute_bucket"],
        counts_per_minute["n_total"],
        linewidth=1.2,
        color="#0f172a",
    )

    for row in _window_subset(burst_windows, limit=25).itertuples(index=False):
        ax.axvspan(row.start_minute, row.end_minute, color="#f97316", alpha=0.10)

    if not volume_changepoints.empty and "change_minute" in volume_changepoints.columns:
        for point in pd.to_datetime(volume_changepoints["change_minute"], errors="coerce").dropna():
            ax.axvline(point, color="#dc2626", linewidth=1.0, alpha=0.7)

    ax.set_title("Submissions per minute with burst windows and changepoints")
    ax.set_xlabel("Minute")
    ax.set_ylabel("Count")
    return save_figure(output_path)


def plot_pro_rate_with_annotations(
    counts_per_minute: pd.DataFrame,
    swing_windows: pd.DataFrame,
    pro_rate_changepoints: pd.DataFrame,
    output_path: Path,
) -> Path:
    fig, ax = plt.subplots(figsize=(14, 4))
    working = counts_per_minute.copy()
    pro_rate = pd.to_numeric(working["pro_rate"], errors="coerce").ffill().bfill()
    ax.plot(working["minute_bucket"], pro_rate, linewidth=1.2, color="#0369a1", label="Pro rate")

    if {"n_pro", "n_total"}.issubset(set(working.columns)):
        lower, upper = _resolve_wilson_bounds(
            working,
            low_col="pro_rate_wilson_low",
            high_col="pro_rate_wilson_high",
            successes_col="n_pro",
            totals_col="n_total",
        )
        ax.fill_between(
            working["minute_bucket"],
            lower,
            upper,
            color="#0369a1",
            alpha=0.14,
            label="95% Wilson interval",
        )
        is_low_power = _resolve_low_power_flags(
            working,
            low_power_col="is_low_power",
            totals_col="n_total",
        )
        if np.any(is_low_power):
            ax.scatter(
                working.loc[is_low_power, "minute_bucket"],
                pro_rate.loc[is_low_power],
                color="#f59e0b",
                s=10,
                marker="x",
                alpha=0.9,
                label="Low-power bucket",
            )

    for row in _window_subset(swing_windows, limit=25).itertuples(index=False):
        ax.axvspan(row.start_minute, row.end_minute, color="#2563eb", alpha=0.10)

    if not pro_rate_changepoints.empty and "change_minute" in pro_rate_changepoints.columns:
        for point in pd.to_datetime(
            pro_rate_changepoints["change_minute"], errors="coerce"
        ).dropna():
            ax.axvline(point, color="#7c3aed", linewidth=1.0, alpha=0.7)

    ax.set_ylim(0.0, 1.0)
    ax.set_title("Pro rate over time with swing windows and changepoints")
    ax.set_xlabel("Minute")
    ax.set_ylabel("Pro rate")
    ax.legend(loc="upper right", fontsize=8)
    return save_figure(output_path)


def plot_pro_rate_bucket_trends(
    time_bucket_profiles: pd.DataFrame,
    output_path: Path,
    preferred_buckets: tuple[int, ...] = (60, 240),
) -> Path | None:
    required = {"bucket_minutes", "bucket_start", "pro_rate", "baseline_pro_rate"}
    if time_bucket_profiles.empty or not required.issubset(set(time_bucket_profiles.columns)):
        return None

    available = sorted(
        time_bucket_profiles["bucket_minutes"].dropna().astype(int).unique().tolist()
    )
    selected = [bucket for bucket in preferred_buckets if bucket in available]
    if not selected:
        selected = available[: min(2, len(available))]
    if not selected:
        return None

    fig, axes = plt.subplots(len(selected), 1, figsize=(14, 4.0 * len(selected)), sharex=False)
    if len(selected) == 1:
        axes = [axes]

    for axis, bucket_minutes in zip(axes, selected):
        subset = (
            time_bucket_profiles[time_bucket_profiles["bucket_minutes"] == int(bucket_minutes)]
            .copy()
            .sort_values("bucket_start")
        )
        subset["bucket_start"] = pd.to_datetime(subset["bucket_start"], errors="coerce")
        subset = subset.dropna(subset=["bucket_start"])
        if subset.empty:
            continue

        axis.plot(
            subset["bucket_start"],
            subset["pro_rate"],
            color="#0369a1",
            linewidth=1.4,
            label=f"{int(bucket_minutes)}m Pro rate",
        )
        wilson_low, wilson_high = _resolve_wilson_bounds(
            subset,
            low_col="pro_rate_wilson_low",
            high_col="pro_rate_wilson_high",
            successes_col="n_pro",
            totals_col="n_total",
        )
        if np.isfinite(wilson_low).any() and np.isfinite(wilson_high).any():
            axis.fill_between(
                subset["bucket_start"],
                wilson_low,
                wilson_high,
                color="#0369a1",
                alpha=0.12,
                label="95% Wilson interval",
            )
        baseline = float(subset["baseline_pro_rate"].iloc[0])
        axis.axhline(
            baseline,
            color="#334155",
            linewidth=1.0,
            linestyle="--",
            label="Global baseline",
        )

        if {"stable_lower", "stable_upper"}.issubset(set(subset.columns)):
            lower = subset["stable_lower"].astype(float).to_numpy()
            upper = subset["stable_upper"].astype(float).to_numpy()
            axis.fill_between(
                subset["bucket_start"],
                lower,
                upper,
                color="#94a3b8",
                alpha=0.18,
                label="Stable band",
            )

        if "is_flagged" in subset.columns:
            flagged = subset[subset["is_flagged"]]
            if not flagged.empty:
                axis.scatter(
                    flagged["bucket_start"],
                    flagged["pro_rate"],
                    color="#dc2626",
                    s=20,
                    zorder=3,
                    label="Flagged",
                )

        is_low_power = _resolve_low_power_flags(
            subset,
            low_power_col="is_low_power",
            totals_col="n_total",
        )
        if np.any(is_low_power):
            axis.scatter(
                subset.loc[is_low_power, "bucket_start"],
                subset.loc[is_low_power, "pro_rate"],
                color="#f59e0b",
                marker="x",
                s=28,
                zorder=4,
                label="Low-power",
            )

        axis.set_ylim(0.0, 1.0)
        axis.set_ylabel("Pro rate")
        axis.set_title(f"Broad Pro/Con ratio trend ({int(bucket_minutes)}-minute buckets)")
        axis.legend(loc="upper right")

    axes[-1].set_xlabel("Time")
    return save_figure(output_path)


def plot_time_of_day_ratio_profiles(
    time_of_day_bucket_profiles: pd.DataFrame,
    output_path: Path,
) -> Path | None:
    required = {"bucket_minutes", "slot_start_minute", "pro_rate", "baseline_pro_rate"}
    if time_of_day_bucket_profiles.empty or not required.issubset(
        set(time_of_day_bucket_profiles.columns)
    ):
        return None

    bucket_sizes = sorted(
        time_of_day_bucket_profiles["bucket_minutes"].dropna().astype(int).unique().tolist()
    )
    if not bucket_sizes:
        return None

    fig, axes = plt.subplots(
        len(bucket_sizes), 1, figsize=(14, 3.2 * len(bucket_sizes)), sharex=True
    )
    if len(bucket_sizes) == 1:
        axes = [axes]

    for axis, bucket_minutes in zip(axes, bucket_sizes):
        subset = (
            time_of_day_bucket_profiles[
                time_of_day_bucket_profiles["bucket_minutes"] == int(bucket_minutes)
            ]
            .copy()
            .sort_values("slot_start_minute")
        )
        if subset.empty:
            continue

        x_hours = subset["slot_start_minute"].astype(float) / 60.0
        axis.plot(
            x_hours,
            subset["pro_rate"],
            color="#0f766e",
            linewidth=1.25,
            marker="o",
            markersize=2.5,
            label="Pro rate",
        )
        wilson_low, wilson_high = _resolve_wilson_bounds(
            subset,
            low_col="pro_rate_wilson_low",
            high_col="pro_rate_wilson_high",
            successes_col="n_pro",
            totals_col="n_total",
        )
        if np.isfinite(wilson_low).any() and np.isfinite(wilson_high).any():
            axis.fill_between(
                x_hours,
                wilson_low,
                wilson_high,
                color="#0f766e",
                alpha=0.12,
                label="95% Wilson interval",
            )
        baseline = float(subset["baseline_pro_rate"].iloc[0])
        axis.axhline(
            baseline, color="#334155", linewidth=1.0, linestyle="--", label="Global baseline"
        )

        if {"stable_lower", "stable_upper"}.issubset(set(subset.columns)):
            axis.fill_between(
                x_hours,
                subset["stable_lower"].astype(float).to_numpy(),
                subset["stable_upper"].astype(float).to_numpy(),
                color="#94a3b8",
                alpha=0.20,
            )

        if "is_flagged" in subset.columns:
            flagged = subset[subset["is_flagged"]]
            if not flagged.empty:
                axis.scatter(
                    flagged["slot_start_minute"].astype(float) / 60.0,
                    flagged["pro_rate"],
                    color="#dc2626",
                    s=18,
                    zorder=3,
                    label="Flagged",
                )

        is_low_power = _resolve_low_power_flags(
            subset,
            low_power_col="is_low_power",
            totals_col="n_total",
        )
        if np.any(is_low_power):
            axis.scatter(
                x_hours[is_low_power],
                subset.loc[is_low_power, "pro_rate"],
                color="#f59e0b",
                marker="x",
                s=28,
                zorder=4,
                label="Low-power",
            )

        axis.set_ylim(0.0, 1.0)
        axis.set_ylabel("Pro rate")
        axis.set_title(f"Time-of-day Pro/Con profile ({int(bucket_minutes)}-minute slots)")
        axis.legend(loc="upper right", fontsize=8)

    xticks = np.arange(0, 25, 2, dtype=int)
    axes[-1].set_xticks(xticks)
    axes[-1].set_xlim(0.0, 24.0)
    axes[-1].set_xlabel("Hour of day")
    return save_figure(output_path)


def plot_voter_registry_match_rates(
    match_by_bucket: pd.DataFrame,
    output_path: Path,
) -> Path | None:
    required = {"bucket_start", "match_rate", "n_total", "n_matches"}
    if match_by_bucket.empty or not required.issubset(set(match_by_bucket.columns)):
        return None

    subset = match_by_bucket.copy()
    subset["bucket_start"] = pd.to_datetime(subset["bucket_start"], errors="coerce")
    if "bucket_minutes" in subset.columns:
        available = sorted(
            pd.to_numeric(subset["bucket_minutes"], errors="coerce")
            .dropna()
            .astype(int)
            .unique()
            .tolist()
        )
        if available:
            selected_bucket = 30 if 30 in available else available[0]
            subset = subset[
                pd.to_numeric(subset["bucket_minutes"], errors="coerce") == float(selected_bucket)
            ]
    subset = subset.dropna(subset=["bucket_start"]).sort_values("bucket_start")
    if subset.empty:
        return None
    for column in ("n_total", "n_matches"):
        subset[column] = pd.to_numeric(subset[column], errors="coerce").fillna(0).astype(float)

    bucket_minutes = (
        int(subset["bucket_minutes"].dropna().iloc[0])
        if "bucket_minutes" in subset.columns
        else None
    )

    fig, (ax_rates, ax_volume) = plt.subplots(
        2,
        1,
        figsize=(14, 7),
        sharex=True,
        gridspec_kw={"height_ratios": [3, 1]},
    )
    rates = subset["match_rate"].astype(float)
    ax_rates.plot(
        subset["bucket_start"],
        rates,
        color="#0f766e",
        linewidth=1.2,
        label="Overall match rate (raw)",
    )
    overall_lower, overall_upper = _resolve_wilson_bounds(
        subset,
        low_col="match_rate_wilson_low",
        high_col="match_rate_wilson_high",
        successes_col="n_matches",
        totals_col="n_total",
    )
    ax_rates.fill_between(
        subset["bucket_start"],
        overall_lower,
        overall_upper,
        color="#0f766e",
        alpha=0.15,
        label="Overall 95% Wilson interval",
    )
    overall_low_power = _resolve_low_power_flags(
        subset,
        low_power_col="is_low_power",
        totals_col="n_total",
    )
    if np.any(overall_low_power):
        ax_rates.scatter(
            subset.loc[overall_low_power, "bucket_start"],
            rates.loc[overall_low_power],
            color="#f59e0b",
            marker="x",
            s=24,
            zorder=5,
            label="Overall low-power",
        )
    overall_weighted = (
        subset["n_matches"].rolling(5, min_periods=1).sum()
        / subset["n_total"].rolling(5, min_periods=1).sum()
    )
    ax_rates.plot(
        subset["bucket_start"],
        overall_weighted,
        color="#0f766e",
        linewidth=2.4,
        alpha=0.95,
        label="Overall match rate (volume-weighted rolling)",
    )

    if "pro_match_rate" in subset.columns:
        subset["pro_match_rate"] = pd.to_numeric(subset["pro_match_rate"], errors="coerce")
        ax_rates.plot(
            subset["bucket_start"],
            subset["pro_match_rate"],
            color="#1d4ed8",
            linewidth=1.1,
            label="Pro match rate (raw)",
        )
        if {"n_pro", "n_matches_pro"}.issubset(set(subset.columns)):
            subset["n_pro"] = (
                pd.to_numeric(subset["n_pro"], errors="coerce").fillna(0).astype(float)
            )
            subset["n_matches_pro"] = (
                pd.to_numeric(subset["n_matches_pro"], errors="coerce").fillna(0).astype(float)
            )
            pro_lower, pro_upper = _resolve_wilson_bounds(
                subset,
                low_col="pro_match_rate_wilson_low",
                high_col="pro_match_rate_wilson_high",
                successes_col="n_matches_pro",
                totals_col="n_pro",
            )
            ax_rates.fill_between(
                subset["bucket_start"],
                pro_lower,
                pro_upper,
                color="#1d4ed8",
                alpha=0.08,
            )
            pro_low_power = _resolve_low_power_flags(
                subset,
                low_power_col="pro_is_low_power",
                totals_col="n_pro",
            )
            if np.any(pro_low_power):
                ax_rates.scatter(
                    subset.loc[pro_low_power, "bucket_start"],
                    subset.loc[pro_low_power, "pro_match_rate"],
                    color="#93c5fd",
                    marker="x",
                    s=18,
                    zorder=4,
                    label="Pro low-power",
                )
            pro_weighted = subset["n_matches_pro"].rolling(5, min_periods=1).sum() / subset[
                "n_pro"
            ].rolling(5, min_periods=1).sum().where(
                subset["n_pro"].rolling(5, min_periods=1).sum() > 0
            )
            ax_rates.plot(
                subset["bucket_start"],
                pro_weighted,
                color="#1d4ed8",
                linewidth=2.0,
                alpha=0.9,
                label="Pro match rate (volume-weighted rolling)",
            )
    if "con_match_rate" in subset.columns:
        subset["con_match_rate"] = pd.to_numeric(subset["con_match_rate"], errors="coerce")
        ax_rates.plot(
            subset["bucket_start"],
            subset["con_match_rate"],
            color="#b91c1c",
            linewidth=1.1,
            label="Con match rate (raw)",
        )
        if {"n_con", "n_matches_con"}.issubset(set(subset.columns)):
            subset["n_con"] = (
                pd.to_numeric(subset["n_con"], errors="coerce").fillna(0).astype(float)
            )
            subset["n_matches_con"] = (
                pd.to_numeric(subset["n_matches_con"], errors="coerce").fillna(0).astype(float)
            )
            con_lower, con_upper = _resolve_wilson_bounds(
                subset,
                low_col="con_match_rate_wilson_low",
                high_col="con_match_rate_wilson_high",
                successes_col="n_matches_con",
                totals_col="n_con",
            )
            ax_rates.fill_between(
                subset["bucket_start"],
                con_lower,
                con_upper,
                color="#b91c1c",
                alpha=0.08,
            )
            con_low_power = _resolve_low_power_flags(
                subset,
                low_power_col="con_is_low_power",
                totals_col="n_con",
            )
            if np.any(con_low_power):
                ax_rates.scatter(
                    subset.loc[con_low_power, "bucket_start"],
                    subset.loc[con_low_power, "con_match_rate"],
                    color="#fca5a5",
                    marker="x",
                    s=18,
                    zorder=4,
                    label="Con low-power",
                )
            con_weighted = subset["n_matches_con"].rolling(5, min_periods=1).sum() / subset[
                "n_con"
            ].rolling(5, min_periods=1).sum().where(
                subset["n_con"].rolling(5, min_periods=1).sum() > 0
            )
            ax_rates.plot(
                subset["bucket_start"],
                con_weighted,
                color="#b91c1c",
                linewidth=2.0,
                alpha=0.9,
                label="Con match rate (volume-weighted rolling)",
            )

    ax_rates.set_ylim(0.0, 1.0)
    ax_rates.set_ylabel("Match rate")
    ax_rates.set_title("Voter-registry match rates over time (volume-weighted)")
    ax_rates.grid(axis="y", alpha=0.25)
    ax_rates.legend(loc="upper right", fontsize=8)

    if {"n_pro", "n_con"}.issubset(set(subset.columns)):
        n_pro = pd.to_numeric(subset["n_pro"], errors="coerce").fillna(0).astype(float)
        n_con = pd.to_numeric(subset["n_con"], errors="coerce").fillna(0).astype(float)
        n_unknown = np.clip(subset["n_total"] - n_pro - n_con, a_min=0.0, a_max=None)
        bar_width_days = (
            (float(bucket_minutes) * 0.85) / (24.0 * 60.0)
            if bucket_minutes is not None
            else 1 / (24.0 * 60.0)
        )
        ax_volume.bar(
            subset["bucket_start"],
            n_pro,
            width=bar_width_days,
            color="#1d4ed8",
            alpha=0.40,
            label="Pro volume",
        )
        ax_volume.bar(
            subset["bucket_start"],
            n_con,
            width=bar_width_days,
            bottom=n_pro,
            color="#b91c1c",
            alpha=0.40,
            label="Con volume",
        )
        if np.any(n_unknown > 0):
            ax_volume.bar(
                subset["bucket_start"],
                n_unknown,
                width=bar_width_days,
                bottom=n_pro + n_con,
                color="#64748b",
                alpha=0.35,
                label="Unknown volume",
            )

    ax_volume.plot(
        subset["bucket_start"],
        subset["n_total"],
        color="#111827",
        linewidth=1.3,
        label="Total volume",
    )
    ax_volume.set_ylabel("n submissions")
    if bucket_minutes is not None:
        ax_volume.set_xlabel(f"Time ({bucket_minutes}-minute buckets)")
    else:
        ax_volume.set_xlabel("Time")
    ax_volume.grid(axis="y", alpha=0.2)
    ax_volume.legend(loc="upper right", fontsize=8)
    return save_figure(output_path)


def plot_organization_blank_rates(
    blank_rate_by_bucket: pd.DataFrame,
    output_path: Path,
    preferred_buckets: tuple[int, ...] = (1, 5, 15, 30, 60, 120),
) -> Path | None:
    required = {"bucket_start", "bucket_minutes", "blank_org_rate"}
    if blank_rate_by_bucket.empty or not required.issubset(set(blank_rate_by_bucket.columns)):
        return None

    available = sorted(
        blank_rate_by_bucket["bucket_minutes"].dropna().astype(int).unique().tolist()
    )
    selected = [bucket for bucket in preferred_buckets if bucket in available]
    if not selected:
        selected = available
    if not selected:
        return None

    n_panels = len(selected)
    n_cols = 2 if n_panels > 1 else 1
    n_rows = int(np.ceil(n_panels / n_cols))
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(16, 3.6 * n_rows), sharey=True)
    axes_array = np.atleast_1d(axes).ravel()

    for axis, bucket in zip(axes_array, selected):
        subset = (
            blank_rate_by_bucket[blank_rate_by_bucket["bucket_minutes"] == int(bucket)]
            .copy()
            .sort_values("bucket_start")
        )
        subset["bucket_start"] = pd.to_datetime(subset["bucket_start"], errors="coerce")
        subset = subset.dropna(subset=["bucket_start"])
        if subset.empty:
            continue

        axis.plot(
            subset["bucket_start"],
            subset["blank_org_rate"].astype(float),
            color="#0f172a",
            linewidth=1.3,
            label="Total blank-org rate",
        )
        total_low, total_high = _resolve_wilson_bounds(
            subset,
            low_col="blank_org_rate_wilson_low",
            high_col="blank_org_rate_wilson_high",
            successes_col="n_blank_org",
            totals_col="n_total",
        )
        if np.isfinite(total_low).any() and np.isfinite(total_high).any():
            axis.fill_between(
                subset["bucket_start"],
                total_low,
                total_high,
                color="#0f172a",
                alpha=0.12,
                label="Total 95% Wilson interval",
            )
        total_low_power = _resolve_low_power_flags(
            subset,
            low_power_col="is_low_power",
            totals_col="n_total",
        )
        if np.any(total_low_power):
            axis.scatter(
                subset.loc[total_low_power, "bucket_start"],
                subset.loc[total_low_power, "blank_org_rate"],
                color="#f59e0b",
                marker="x",
                s=22,
                zorder=4,
                label="Total low-power",
            )
        if "pro_blank_org_rate" in subset.columns:
            axis.plot(
                subset["bucket_start"],
                subset["pro_blank_org_rate"].astype(float),
                color="#1d4ed8",
                linewidth=1.1,
                label="Pro blank-org rate",
            )
            pro_low, pro_high = _resolve_wilson_bounds(
                subset,
                low_col="pro_blank_org_rate_wilson_low",
                high_col="pro_blank_org_rate_wilson_high",
                successes_col="n_blank_org_pro",
                totals_col="n_pro",
            )
            if np.isfinite(pro_low).any() and np.isfinite(pro_high).any():
                axis.fill_between(
                    subset["bucket_start"],
                    pro_low,
                    pro_high,
                    color="#1d4ed8",
                    alpha=0.08,
                )
        if "con_blank_org_rate" in subset.columns:
            axis.plot(
                subset["bucket_start"],
                subset["con_blank_org_rate"].astype(float),
                color="#b91c1c",
                linewidth=1.1,
                label="Con blank-org rate",
            )
            con_low, con_high = _resolve_wilson_bounds(
                subset,
                low_col="con_blank_org_rate_wilson_low",
                high_col="con_blank_org_rate_wilson_high",
                successes_col="n_blank_org_con",
                totals_col="n_con",
            )
            if np.isfinite(con_low).any() and np.isfinite(con_high).any():
                axis.fill_between(
                    subset["bucket_start"],
                    con_low,
                    con_high,
                    color="#b91c1c",
                    alpha=0.08,
                )
        axis.set_ylim(0.0, 1.0)
        axis.set_title(f"Organization blank-rate trend ({int(bucket)}m buckets)")
        axis.set_ylabel("Rate")
        axis.grid(axis="y", alpha=0.25)

    for axis in axes_array[n_panels:]:
        axis.remove()
    axes_array[0].legend(loc="upper right")
    axes_array[max(0, n_panels - 1)].set_xlabel("Time")
    fig.suptitle("Organization blank/null percentage over time", y=1.01)
    fig.tight_layout()
    return save_figure(output_path)


def plot_multivariate_anomaly_scores(
    bucket_anomaly_scores: pd.DataFrame,
    output_path: Path,
) -> Path | None:
    required = {"bucket_start", "n_total", "anomaly_score"}
    if bucket_anomaly_scores.empty or not required.issubset(set(bucket_anomaly_scores.columns)):
        return None

    subset = bucket_anomaly_scores.copy()
    subset["bucket_start"] = pd.to_datetime(subset["bucket_start"], errors="coerce")
    if "bucket_minutes" in subset.columns:
        available = sorted(
            pd.to_numeric(subset["bucket_minutes"], errors="coerce")
            .dropna()
            .astype(int)
            .unique()
            .tolist()
        )
        if available:
            selected_bucket = 30 if 30 in available else available[0]
            subset = subset[
                pd.to_numeric(subset["bucket_minutes"], errors="coerce") == float(selected_bucket)
            ]
    subset["n_total"] = pd.to_numeric(subset["n_total"], errors="coerce")
    subset["anomaly_score"] = pd.to_numeric(subset["anomaly_score"], errors="coerce")
    subset = subset.dropna(subset=["bucket_start", "n_total"]).sort_values("bucket_start")
    if subset.empty:
        return None
    if not subset["anomaly_score"].notna().any():
        subset["anomaly_score"] = 0.0

    bucket_minutes = (
        int(subset["bucket_minutes"].dropna().iloc[0])
        if "bucket_minutes" in subset.columns
        else None
    )
    anomaly_scores = subset["anomaly_score"].astype(float)
    threshold = float(anomaly_scores.quantile(0.95))

    fig, (ax_score, ax_volume) = plt.subplots(
        2,
        1,
        figsize=(14, 7),
        sharex=True,
        gridspec_kw={"height_ratios": [3, 1]},
    )

    ax_score.plot(
        subset["bucket_start"],
        anomaly_scores,
        color="#5b21b6",
        linewidth=1.4,
        label="IsolationForest anomaly score",
    )
    ax_score.axhline(
        threshold,
        color="#7c3aed",
        linewidth=1.0,
        linestyle="--",
        alpha=0.9,
        label="95th-percentile score",
    )

    if "is_anomaly" in subset.columns:
        anomaly_mask = subset["is_anomaly"].fillna(False).astype(bool)
        if np.any(anomaly_mask):
            ax_score.scatter(
                subset.loc[anomaly_mask, "bucket_start"],
                anomaly_scores.loc[anomaly_mask],
                color="#dc2626",
                s=22,
                zorder=4,
                label="Model anomaly",
            )

    low_power_mask = _resolve_low_power_flags(
        subset,
        low_power_col="is_low_power",
        totals_col="n_total",
    )
    if np.any(low_power_mask):
        ax_score.scatter(
            subset.loc[low_power_mask, "bucket_start"],
            anomaly_scores.loc[low_power_mask],
            color="#f59e0b",
            marker="x",
            s=30,
            zorder=5,
            label="Low-power",
        )

    ax_score.set_ylabel("Anomaly score")
    ax_score.set_title("Multivariate bucket anomaly scores (IsolationForest)")
    ax_score.grid(axis="y", alpha=0.25)
    ax_score.legend(loc="upper right", fontsize=8)

    bar_width_days = (
        (float(bucket_minutes) * 0.85) / (24.0 * 60.0)
        if bucket_minutes is not None
        else 1 / (24.0 * 60.0)
    )
    ax_volume.bar(
        subset["bucket_start"],
        subset["n_total"].astype(float),
        width=bar_width_days,
        color="#475569",
        alpha=0.45,
        label="Bucket volume",
    )
    ax_volume.plot(
        subset["bucket_start"],
        subset["n_total"].astype(float),
        color="#0f172a",
        linewidth=1.1,
    )
    ax_volume.set_ylabel("n submissions")
    if bucket_minutes is not None:
        ax_volume.set_xlabel(f"Time ({bucket_minutes}-minute buckets)")
    else:
        ax_volume.set_xlabel("Time")
    ax_volume.grid(axis="y", alpha=0.2)
    ax_volume.legend(loc="upper right", fontsize=8)
    return save_figure(output_path)
