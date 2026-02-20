from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd

NAME_COLUMN_CANDIDATES = (
    "name",
    "first",
    "first_name",
    "given_name",
    "last",
    "last_name",
    "surname",
)
PROB_COLUMN_CANDIDATES = (
    "probability",
    "prob",
    "p",
    "share",
    "fraction",
    "freq",
    "frequency",
    "pct",
    "percent",
)
COUNT_COLUMN_CANDIDATES = (
    "count",
    "n",
    "occurrences",
    "total",
)
TOKEN_RE = re.compile(r"[^A-Z'\- ]")


def normalize_name_token(value: str) -> str:
    token = str(value).strip().upper()
    token = TOKEN_RE.sub("", token)
    token = re.sub(r"\s+", " ", token).strip()
    return token


def _read_lookup(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _resolve_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    column_index = {column.lower(): column for column in columns}
    for candidate in candidates:
        match = column_index.get(candidate.lower())
        if match:
            return match
    return None


def load_name_frequency_lookup(path: str | None) -> dict[str, float]:
    """Load name-frequency data and return normalized probabilities by token."""
    if not path:
        return {}

    file_path = Path(path)
    if not file_path.exists():
        return {}

    table = _read_lookup(file_path)
    if table.empty:
        return {}

    name_column = _resolve_column(list(table.columns), NAME_COLUMN_CANDIDATES) or table.columns[0]
    prob_column = _resolve_column(list(table.columns), PROB_COLUMN_CANDIDATES)
    count_column = _resolve_column(list(table.columns), COUNT_COLUMN_CANDIDATES)

    normalized_names = table[name_column].fillna("").astype(str).map(normalize_name_token)
    valid_names = normalized_names != ""
    if not valid_names.any():
        return {}

    weights: pd.Series
    if prob_column is not None:
        weights = pd.to_numeric(table[prob_column], errors="coerce").fillna(0.0)
    elif count_column is not None:
        weights = pd.to_numeric(table[count_column], errors="coerce").fillna(0.0)
    else:
        numeric_columns = table.select_dtypes(include=["number"]).columns.tolist()
        if numeric_columns:
            weights = pd.to_numeric(table[numeric_columns[0]], errors="coerce").fillna(0.0)
        else:
            weights = pd.Series(1.0, index=table.index, dtype=float)

    weights = weights.where(weights > 0, 0.0)
    subset = pd.DataFrame({"token": normalized_names[valid_names], "weight": weights[valid_names]})
    grouped = subset.groupby("token", dropna=True)["weight"].sum()
    total = float(grouped.sum())
    if total <= 0:
        return {}

    probabilities = grouped / total
    return probabilities.to_dict()


def score_name_rarity(
    first_tokens: pd.Series,
    last_tokens: pd.Series,
    first_lookup: dict[str, float],
    last_lookup: dict[str, float],
    epsilon: float,
    first_missing_probability: float | None = None,
    last_missing_probability: float | None = None,
) -> pd.Series:
    """Return additive rarity score -log(P(first) * P(last))."""
    first_default = (
        epsilon if first_missing_probability is None else float(first_missing_probability)
    )
    last_default = epsilon if last_missing_probability is None else float(last_missing_probability)
    first_prob = first_tokens.map(first_lookup).fillna(first_default).astype(float)
    last_prob = last_tokens.map(last_lookup).fillna(last_default).astype(float)
    combined = np.maximum(first_prob * last_prob, epsilon)
    return pd.Series(-np.log(combined), index=first_tokens.index, dtype=float)
