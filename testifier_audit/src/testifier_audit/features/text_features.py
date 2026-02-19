from __future__ import annotations

import math

import pandas as pd


ALPHABET = set("abcdefghijklmnopqrstuvwxyz")


def _entropy(value: str) -> float:
    if not value:
        return 0.0
    counts: dict[str, int] = {}
    for char in value:
        counts[char] = counts.get(char, 0) + 1
    total = len(value)
    return -sum((count / total) * math.log2(count / total) for count in counts.values())


def build_name_text_features(df: pd.DataFrame) -> pd.DataFrame:
    names = df["name_normalized"].fillna("").astype(str)
    features = pd.DataFrame(
        {
            "canonical_name": df["canonical_name"],
            "name_normalized": names,
            "name_length": names.str.len(),
            "non_alpha_fraction": names.map(
                lambda s: 0.0
                if not s
                else sum(1 for ch in s.lower() if ch not in ALPHABET and ch != " ") / len(s)
            ),
            "name_entropy": names.map(_entropy),
            "is_initial_only": df["first"].fillna("").str.fullmatch(r"[A-Z]\.?", na=False),
        }
    )
    features["weirdness_score"] = (
        (features["name_length"] / (features["name_length"].median() + 1.0))
        + (features["non_alpha_fraction"] * 2.0)
        + (features["name_entropy"] / 8.0)
        + features["is_initial_only"].astype(float)
    )
    return features
