from __future__ import annotations

import pandas as pd


def build_blocking_key(df: pd.DataFrame) -> pd.Series:
    return (
        df["last"].fillna("").str[:4].str.upper()
        + "|"
        + df["first_canonical"].fillna(df["first"]).astype(str).str[:1].str.upper()
    )
