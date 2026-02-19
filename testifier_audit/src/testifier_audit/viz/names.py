from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from testifier_audit.viz.common import save_figure


def plot_top_names(name_frequency: pd.DataFrame, output_path: Path, top_n: int = 20) -> Path:
    top = name_frequency.head(top_n)
    plt.figure(figsize=(10, 5))
    plt.barh(top["canonical_name"], top["n"])
    plt.gca().invert_yaxis()
    plt.title("Top repeated canonical names")
    plt.xlabel("Count")
    return save_figure(output_path)
