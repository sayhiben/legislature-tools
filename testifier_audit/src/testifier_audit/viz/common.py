from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt


def save_figure(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    return path
