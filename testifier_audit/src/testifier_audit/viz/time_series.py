from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

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


def plot_counts_with_annotations(
    counts_per_minute: pd.DataFrame,
    burst_windows: pd.DataFrame,
    volume_changepoints: pd.DataFrame,
    output_path: Path,
) -> Path:
    fig, ax = plt.subplots(figsize=(14, 4))
    ax.plot(counts_per_minute["minute_bucket"], counts_per_minute["n_total"], linewidth=1.2, color="#0f172a")

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
    pro_rate = counts_per_minute["pro_rate"].ffill().bfill()
    ax.plot(counts_per_minute["minute_bucket"], pro_rate, linewidth=1.2, color="#0369a1")

    for row in _window_subset(swing_windows, limit=25).itertuples(index=False):
        ax.axvspan(row.start_minute, row.end_minute, color="#2563eb", alpha=0.10)

    if not pro_rate_changepoints.empty and "change_minute" in pro_rate_changepoints.columns:
        for point in pd.to_datetime(pro_rate_changepoints["change_minute"], errors="coerce").dropna():
            ax.axvline(point, color="#7c3aed", linewidth=1.0, alpha=0.7)

    ax.set_ylim(0.0, 1.0)
    ax.set_title("Pro rate over time with swing windows and changepoints")
    ax.set_xlabel("Minute")
    ax.set_ylabel("Pro rate")
    return save_figure(output_path)
