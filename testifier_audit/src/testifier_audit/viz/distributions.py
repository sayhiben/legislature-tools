from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from testifier_audit.viz.common import save_figure


def plot_name_length_distribution(df: pd.DataFrame, output_path: Path) -> Path:
    plt.figure(figsize=(10, 4))
    plt.hist(df["name_length"], bins=40)
    plt.title("Name length distribution")
    plt.xlabel("Length")
    plt.ylabel("Frequency")
    return save_figure(output_path)


def _plot_windowed_null_histograms(
    null_distribution: pd.DataFrame,
    observed_tests: pd.DataFrame,
    value_column: str,
    observed_column: str,
    title_prefix: str,
    x_label: str,
    output_path: Path,
) -> Path | None:
    if null_distribution.empty or value_column not in null_distribution.columns:
        return None
    if "window_minutes" not in null_distribution.columns:
        return None

    window_values = sorted(
        null_distribution["window_minutes"].dropna().astype(int).unique().tolist()
    )
    if not window_values:
        return None

    selected_windows = window_values[:4]
    fig, axes = plt.subplots(len(selected_windows), 1, figsize=(10, 3.3 * len(selected_windows)))
    if len(selected_windows) == 1:
        axes = [axes]

    observed_max: dict[int, float] = {}
    if (
        not observed_tests.empty
        and "window_minutes" in observed_tests.columns
        and observed_column in observed_tests.columns
    ):
        grouped = (
            observed_tests.groupby("window_minutes", dropna=False)[observed_column]
            .max()
            .dropna()
            .astype(float)
        )
        observed_max = {int(index): float(value) for index, value in grouped.items()}

    for axis, window in zip(axes, selected_windows):
        window_samples = null_distribution[null_distribution["window_minutes"] == window]
        values = window_samples[value_column].dropna().astype(float)
        axis.hist(values, bins=min(40, max(len(values) // 5, 10)), color="#3b82f6", alpha=0.75)
        expected = observed_max.get(int(window))
        if expected is not None:
            axis.axvline(
                expected, color="#dc2626", linewidth=1.5, linestyle="--", label="Observed max"
            )
            axis.legend(loc="upper right")
        axis.set_title(f"{title_prefix} ({window} minute window)")
        axis.set_xlabel(x_label)
        axis.set_ylabel("Simulations")

    fig.tight_layout()
    return save_figure(output_path)


def plot_burst_null_distribution(
    null_distribution: pd.DataFrame,
    burst_tests: pd.DataFrame,
    output_path: Path,
) -> Path | None:
    return _plot_windowed_null_histograms(
        null_distribution=null_distribution,
        observed_tests=burst_tests,
        value_column="max_window_count",
        observed_column="observed_count",
        title_prefix="Burst null maxima",
        x_label="Simulated max rolling count",
        output_path=output_path,
    )


def plot_swing_null_distribution(
    null_distribution: pd.DataFrame,
    swing_tests: pd.DataFrame,
    output_path: Path,
) -> Path | None:
    return _plot_windowed_null_histograms(
        null_distribution=null_distribution,
        observed_tests=swing_tests,
        value_column="max_abs_delta_pro_rate",
        observed_column="abs_delta_pro_rate",
        title_prefix="Pro/Con swing null maxima",
        x_label="Simulated max absolute Pro-rate delta",
        output_path=output_path,
    )


def plot_periodicity_autocorrelation(autocorr: pd.DataFrame, output_path: Path) -> Path | None:
    if (
        autocorr.empty
        or "lag_minutes" not in autocorr.columns
        or "autocorr" not in autocorr.columns
    ):
        return None
    plt.figure(figsize=(10, 4))
    plt.plot(autocorr["lag_minutes"], autocorr["autocorr"], linewidth=1.2, color="#0f766e")
    plt.axhline(0.0, color="#334155", linewidth=0.8, linestyle="--")
    plt.title("Autocorrelation of submissions per minute")
    plt.xlabel("Lag (minutes)")
    plt.ylabel("Autocorrelation")
    return save_figure(output_path)


def plot_periodicity_spectrum(spectrum_top: pd.DataFrame, output_path: Path) -> Path | None:
    if (
        spectrum_top.empty
        or "period_minutes" not in spectrum_top.columns
        or "power" not in spectrum_top.columns
    ):
        return None
    ranked = spectrum_top.sort_values("power", ascending=False).head(20).copy()
    labels = ranked["period_minutes"].round(1).astype(str).tolist()
    x = np.arange(len(ranked), dtype=float)
    plt.figure(figsize=(10, 4))
    plt.bar(x, ranked["power"], color="#7c3aed", alpha=0.85)
    plt.title("Top periodicity power peaks")
    plt.xlabel("Period (minutes)")
    plt.ylabel("Power")
    plt.xticks(x, labels, rotation=45, ha="right")
    plt.tight_layout()
    return save_figure(output_path)


def plot_periodicity_clockface(
    clockface_distribution: pd.DataFrame, output_path: Path
) -> Path | None:
    required = {"minute_of_hour", "share"}
    if clockface_distribution.empty or not required.issubset(set(clockface_distribution.columns)):
        return None

    ordered = clockface_distribution.sort_values("minute_of_hour").copy()
    x = ordered["minute_of_hour"].astype(int).to_numpy()
    y = ordered["share"].astype(float).to_numpy()
    expected_share = float(1.0 / 60.0)

    plt.figure(figsize=(11, 4.2))
    plt.bar(x, y, color="#0284c7", alpha=0.85, width=0.85)
    plt.axhline(
        expected_share, color="#dc2626", linewidth=1.1, linestyle="--", label="Uniform baseline"
    )
    plt.title("Clock-face minute-of-hour concentration")
    plt.xlabel("Minute of hour")
    plt.ylabel("Share of submissions")
    plt.xlim(-1, 60)
    plt.legend(loc="upper right")
    plt.tight_layout()
    return save_figure(output_path)
