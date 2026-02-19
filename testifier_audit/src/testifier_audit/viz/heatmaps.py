from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from testifier_audit.viz.common import save_figure


def plot_day_hour_heatmap(counts_per_hour: pd.DataFrame, output_path: Path) -> Path:
    pivot = counts_per_hour.pivot(index="day_of_week", columns="hour", values="n_total").fillna(0)
    plt.figure(figsize=(12, 4))
    plt.imshow(pivot.values, aspect="auto")
    plt.title("Submissions by day/hour")
    plt.xlabel("Hour")
    plt.ylabel("Day of week")
    return save_figure(output_path)
