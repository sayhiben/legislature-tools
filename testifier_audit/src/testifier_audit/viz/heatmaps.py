from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from testifier_audit.proportion_stats import DEFAULT_LOW_POWER_MIN_TOTAL, low_power_mask
from testifier_audit.viz.common import save_figure


def plot_day_hour_heatmap(counts_per_hour: pd.DataFrame, output_path: Path) -> Path:
    pivot = counts_per_hour.pivot(index="day_of_week", columns="hour", values="n_total").fillna(0)
    plt.figure(figsize=(12, 4))
    plt.imshow(pivot.values, aspect="auto")
    plt.title("Submissions by day/hour")
    plt.xlabel("Hour")
    plt.ylabel("Day of week")
    return save_figure(output_path)


def plot_pro_rate_day_hour_heatmap(
    counts_per_minute: pd.DataFrame,
    output_path: Path,
    bucket_minutes: int = 60,
) -> Path | None:
    required = {"minute_bucket", "n_total", "n_pro"}
    if counts_per_minute.empty or not required.issubset(set(counts_per_minute.columns)):
        return None
    bucket_minutes = max(1, int(bucket_minutes))

    working = counts_per_minute.copy()
    working["minute_bucket"] = pd.to_datetime(working["minute_bucket"], errors="coerce")
    working = working.dropna(subset=["minute_bucket"])
    if working.empty:
        return None

    bucket_rule = f"{bucket_minutes}min"
    working["time_bucket"] = working["minute_bucket"].dt.floor(bucket_rule)
    working["date"] = working["minute_bucket"].dt.date.astype(str)
    working["slot_start_minute"] = (working["time_bucket"].dt.hour.astype(int) * 60) + working[
        "time_bucket"
    ].dt.minute.astype(int)
    grouped = (
        working.groupby(["date", "slot_start_minute"], dropna=True)
        .agg(n_total=("n_total", "sum"), n_pro=("n_pro", "sum"))
        .reset_index()
    )
    grouped["pro_rate"] = (grouped["n_pro"] / grouped["n_total"]).where(grouped["n_total"] > 0)
    grouped["is_low_power"] = low_power_mask(
        totals=grouped["n_total"],
        min_total=DEFAULT_LOW_POWER_MIN_TOTAL,
    )

    pivot = (
        grouped.pivot(index="date", columns="slot_start_minute", values="pro_rate")
        .sort_index()
        .reindex(columns=range(0, 24 * 60, bucket_minutes))
    )
    low_power_pivot = (
        grouped.pivot(index="date", columns="slot_start_minute", values="is_low_power")
        .sort_index()
        .reindex(columns=range(0, 24 * 60, bucket_minutes))
    )
    if pivot.empty:
        return None

    matrix_values = pivot.to_numpy(dtype=float)
    low_power_values = low_power_pivot.astype("boolean").fillna(False).to_numpy(dtype=bool)
    matrix_values[low_power_values] = np.nan
    matrix = np.ma.masked_invalid(matrix_values)
    cmap = plt.get_cmap("coolwarm").copy()
    cmap.set_bad("#e2e8f0")

    fig_height = max(4.0, min(14.0, 0.22 * len(pivot.index)))
    plt.figure(figsize=(13, fig_height))
    image = plt.imshow(matrix, aspect="auto", cmap=cmap, vmin=0.0, vmax=1.0)
    plt.colorbar(image, label="Pro rate")
    plt.title(
        "Pro/Con ratio heatmap by date and time of day "
        f"({bucket_minutes}-minute bins, low-power cells masked)"
    )
    plt.xlabel(f"Time of day ({bucket_minutes}-minute bins)")
    plt.ylabel("Date")

    slot_minutes = pivot.columns.to_numpy(dtype=int)
    slot_ticks = np.linspace(0, len(slot_minutes) - 1, num=min(12, len(slot_minutes)), dtype=int)
    slot_labels = [
        f"{int(slot_minutes[idx] // 60):02d}:{int(slot_minutes[idx] % 60):02d}"
        for idx in slot_ticks
    ]
    plt.xticks(slot_ticks, slot_labels, rotation=45, ha="right")

    date_ticks = np.linspace(0, len(pivot.index) - 1, num=min(10, len(pivot.index)), dtype=int)
    date_labels = [pivot.index[idx] for idx in date_ticks]
    plt.yticks(date_ticks, date_labels)
    return save_figure(output_path)


def plot_ratio_shift_heatmap_by_bucket(
    day_bucket_profiles: pd.DataFrame,
    bucket_minutes: int,
    output_path: Path,
) -> Path | None:
    required = {"bucket_minutes", "date", "slot_start_minute", "delta_from_slot_pro_rate"}
    if day_bucket_profiles.empty or not required.issubset(set(day_bucket_profiles.columns)):
        return None

    subset = day_bucket_profiles[
        day_bucket_profiles["bucket_minutes"] == int(bucket_minutes)
    ].copy()
    if subset.empty:
        return None
    if "is_low_power" not in subset.columns and "n_total" in subset.columns:
        subset["is_low_power"] = low_power_mask(
            totals=subset["n_total"],
            min_total=DEFAULT_LOW_POWER_MIN_TOTAL,
        )

    pivot = (
        subset.pivot(index="date", columns="slot_start_minute", values="delta_from_slot_pro_rate")
        .sort_index()
        .sort_index(axis=1)
    )
    low_power_pivot = (
        subset.pivot(index="date", columns="slot_start_minute", values="is_low_power")
        .sort_index()
        .sort_index(axis=1)
        if "is_low_power" in subset.columns
        else pd.DataFrame(index=pivot.index, columns=pivot.columns)
    )
    if pivot.empty:
        return None

    matrix_values = pivot.to_numpy(dtype=float)
    magnitude = np.nanmax(np.abs(matrix_values))
    if not np.isfinite(magnitude) or magnitude <= 0.0:
        magnitude = 0.05

    low_power_values = low_power_pivot.astype("boolean").fillna(False).to_numpy(dtype=bool)
    matrix_values[low_power_values] = np.nan
    matrix = np.ma.masked_invalid(matrix_values)
    cmap = plt.get_cmap("RdBu_r").copy()
    cmap.set_bad("#e2e8f0")

    fig_height = max(4.0, min(14.0, 0.22 * len(pivot.index)))
    plt.figure(figsize=(13, fig_height))
    image = plt.imshow(matrix, aspect="auto", cmap=cmap, vmin=-magnitude, vmax=magnitude)
    plt.colorbar(image, label="Delta from slot baseline Pro rate")
    plt.title(f"Pro/Con ratio shift heatmap ({int(bucket_minutes)}-minute slots, low-power masked)")
    plt.xlabel(f"Slot start time ({int(bucket_minutes)}-minute bins)")
    plt.ylabel("Date")

    slot_minutes = pivot.columns.to_numpy(dtype=int)
    slot_ticks = np.linspace(0, len(slot_minutes) - 1, num=min(10, len(slot_minutes)), dtype=int)
    slot_labels = [
        f"{int(slot_minutes[idx] // 60):02d}:{int(slot_minutes[idx] % 60):02d}"
        for idx in slot_ticks
    ]
    plt.xticks(slot_ticks, slot_labels, rotation=45, ha="right")

    date_ticks = np.linspace(0, len(pivot.index) - 1, num=min(10, len(pivot.index)), dtype=int)
    date_labels = [pivot.index[idx] for idx in date_ticks]
    plt.yticks(date_ticks, date_labels)
    return save_figure(output_path)
