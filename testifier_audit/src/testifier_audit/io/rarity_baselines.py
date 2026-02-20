from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import pandas as pd

from testifier_audit.features.rarity import (
    COUNT_COLUMN_CANDIDATES,
    NAME_COLUMN_CANDIDATES,
    PROB_COLUMN_CANDIDATES,
    normalize_name_token,
)


@dataclass(frozen=True)
class BaselineBuildResult:
    output_path: Path
    rows_input: int
    rows_output: int
    total_weight: float
    name_column_used: str
    value_column_used: str


class BaselineProfileName(str, Enum):
    generic = "generic"
    ssa_first = "ssa_first"
    census_last = "census_last"


@dataclass(frozen=True)
class BaselineProfile:
    name_candidates: tuple[str, ...]
    value_candidates: tuple[str, ...]
    default_min_weight: float


PROFILE_MAP: dict[BaselineProfileName, BaselineProfile] = {
    BaselineProfileName.generic: BaselineProfile(
        name_candidates=NAME_COLUMN_CANDIDATES,
        value_candidates=PROB_COLUMN_CANDIDATES + COUNT_COLUMN_CANDIDATES,
        default_min_weight=1.0,
    ),
    BaselineProfileName.ssa_first: BaselineProfile(
        name_candidates=("name", "first_name", "first", "forename", "given_name"),
        value_candidates=("count", "births", "n_births", "frequency"),
        default_min_weight=5.0,
    ),
    BaselineProfileName.census_last: BaselineProfile(
        name_candidates=("name", "surname", "last_name", "last"),
        value_candidates=("count", "frequency", "pct", "percent", "probability"),
        default_min_weight=1.0,
    ),
}


def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _resolve_column(
    columns: list[str],
    explicit: str | None,
    candidates: tuple[str, ...],
    fallback: str | None = None,
) -> str:
    if explicit:
        if explicit not in columns:
            raise ValueError(
                f"Column '{explicit}' was not found. Available columns: {', '.join(columns)}"
            )
        return explicit

    lowered = {column.lower(): column for column in columns}
    for candidate in candidates:
        match = lowered.get(candidate.lower())
        if match:
            return match

    if fallback is not None:
        return fallback
    raise ValueError(f"Could not infer column from: {', '.join(columns)}")


def _resolve_value_column(
    table: pd.DataFrame,
    explicit: str | None,
    candidates: tuple[str, ...],
) -> str:
    columns = list(table.columns)
    if explicit:
        if explicit not in columns:
            raise ValueError(
                f"Column '{explicit}' was not found. Available columns: {', '.join(columns)}"
            )
        return explicit

    lowered = {column.lower(): column for column in columns}
    for candidate in candidates:
        match = lowered.get(candidate.lower())
        if match:
            return match

    numeric_columns = table.select_dtypes(include=["number"]).columns.tolist()
    if numeric_columns:
        return numeric_columns[0]
    raise ValueError("Could not infer a numeric value column for baseline normalization")


def resolve_profile(profile_name: BaselineProfileName | str | None) -> BaselineProfile:
    if profile_name is None:
        return PROFILE_MAP[BaselineProfileName.generic]
    name = BaselineProfileName(profile_name)
    if name not in PROFILE_MAP:
        raise ValueError(f"Unsupported baseline profile: {profile_name}")
    return PROFILE_MAP[name]


def normalize_frequency_baseline(
    table: pd.DataFrame,
    name_column: str | None = None,
    value_column: str | None = None,
    min_weight: float = 1.0,
    name_candidates: tuple[str, ...] | None = None,
    value_candidates: tuple[str, ...] | None = None,
) -> tuple[pd.DataFrame, str, str]:
    if table.empty:
        return pd.DataFrame(columns=["name", "count", "probability"]), "", ""

    name_col = _resolve_column(
        list(table.columns),
        name_column,
        name_candidates or NAME_COLUMN_CANDIDATES,
        fallback=table.columns[0],
    )
    value_col = _resolve_value_column(
        table,
        explicit=value_column,
        candidates=value_candidates or (PROB_COLUMN_CANDIDATES + COUNT_COLUMN_CANDIDATES),
    )

    names = table[name_col].fillna("").astype(str).map(normalize_name_token)
    weights = pd.to_numeric(table[value_col], errors="coerce").fillna(0.0)
    valid = (names != "") & (weights > 0.0)
    if min_weight > 0:
        valid = valid & (weights >= float(min_weight))

    if not valid.any():
        return pd.DataFrame(columns=["name", "count", "probability"]), name_col, value_col

    normalized = (
        pd.DataFrame({"name": names[valid], "count": weights[valid].astype(float)})
        .groupby("name", dropna=True, as_index=False)["count"]
        .sum()
        .sort_values(["count", "name"], ascending=[False, True])
        .reset_index(drop=True)
    )
    total = float(normalized["count"].sum())
    if total > 0.0:
        normalized["probability"] = normalized["count"] / total
    else:
        normalized["probability"] = 0.0
    return normalized, name_col, value_col


def build_frequency_baseline_file(
    raw_path: Path,
    output_path: Path,
    name_column: str | None = None,
    value_column: str | None = None,
    min_weight: float = 1.0,
    profile_name: BaselineProfileName | str | None = None,
) -> BaselineBuildResult:
    profile = resolve_profile(profile_name)
    min_weight_effective = float(max(min_weight, profile.default_min_weight))
    raw_table = _read_table(raw_path)
    normalized, name_col, value_col = normalize_frequency_baseline(
        table=raw_table,
        name_column=name_column,
        value_column=value_column,
        min_weight=min_weight_effective,
        name_candidates=profile.name_candidates,
        value_candidates=profile.value_candidates,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    normalized.to_csv(output_path, index=False)

    return BaselineBuildResult(
        output_path=output_path,
        rows_input=int(len(raw_table)),
        rows_output=int(len(normalized)),
        total_weight=float(normalized["count"].sum()) if not normalized.empty else 0.0,
        name_column_used=name_col,
        value_column_used=value_col,
    )
