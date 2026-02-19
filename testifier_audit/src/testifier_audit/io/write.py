from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def write_table(df: pd.DataFrame, path: Path, fmt: str = "parquet") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "parquet":
        df.to_parquet(path, index=False)
        return path
    if fmt == "csv":
        df.to_csv(path, index=False)
        return path
    raise ValueError(f"Unsupported table format: {fmt}")


def write_summary(data: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    return path
