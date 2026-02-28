from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
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
