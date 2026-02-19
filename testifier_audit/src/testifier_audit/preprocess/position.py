from __future__ import annotations

import pandas as pd


POSITION_MAP = {
    "PRO": "Pro",
    "CON": "Con",
}


def normalize_position(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    position = working["position"].fillna("").astype(str).str.strip().str.upper()
    working["position_normalized"] = position.map(POSITION_MAP).fillna("Unknown")
    return working
