from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd


def _normalize_dataframe(
    frame: pd.DataFrame,
    *,
    columns: Sequence[str],
    sort_by: Sequence[str],
    datetime_columns: Sequence[str],
) -> pd.DataFrame:
    normalized = frame.copy()
    for column in columns:
        if column not in normalized.columns:
            normalized[column] = pd.NA
    normalized = normalized.loc[:, list(columns)]

    for column in datetime_columns:
        if column not in normalized.columns:
            continue
        normalized[column] = pd.to_datetime(normalized[column], errors="coerce", utc=True)
        normalized[column] = normalized[column].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    if sort_by:
        missing = [column for column in sort_by if column not in normalized.columns]
        if not missing:
            normalized = normalized.sort_values(list(sort_by), kind="mergesort")

    return normalized.reset_index(drop=True)


def assert_mapping_subset(
    *,
    actual: Mapping[str, Any],
    expected_subset: Mapping[str, Any],
    float_tolerance: float = 1e-12,
) -> None:
    for key, expected_value in expected_subset.items():
        assert key in actual, f"Missing key: {key}"
        actual_value = actual[key]
        if isinstance(expected_value, float):
            assert np.isclose(
                float(actual_value),
                expected_value,
                rtol=0.0,
                atol=float_tolerance,
                equal_nan=True,
            ), f"Mismatch for {key}: actual={actual_value} expected={expected_value}"
        else:
            assert actual_value == expected_value, (
                f"Mismatch for {key}: actual={actual_value} expected={expected_value}"
            )


def assert_frame_matches_records(
    *,
    actual: pd.DataFrame,
    expected_records: Sequence[Mapping[str, Any]],
    columns: Sequence[str],
    sort_by: Sequence[str] = (),
    datetime_columns: Sequence[str] = (),
    float_tolerance: float = 1e-12,
) -> None:
    expected = pd.DataFrame(list(expected_records))
    actual_norm = _normalize_dataframe(
        actual,
        columns=columns,
        sort_by=sort_by,
        datetime_columns=datetime_columns,
    )
    expected_norm = _normalize_dataframe(
        expected,
        columns=columns,
        sort_by=sort_by,
        datetime_columns=datetime_columns,
    )

    assert len(actual_norm) == len(expected_norm), (
        f"Row mismatch: actual={len(actual_norm)} expected={len(expected_norm)}"
    )

    for column in columns:
        actual_col = actual_norm[column]
        expected_col = expected_norm[column]
        if pd.api.types.is_numeric_dtype(actual_col) or pd.api.types.is_numeric_dtype(expected_col):
            np.testing.assert_allclose(
                pd.to_numeric(actual_col, errors="coerce").to_numpy(dtype=float),
                pd.to_numeric(expected_col, errors="coerce").to_numpy(dtype=float),
                rtol=0.0,
                atol=float_tolerance,
                equal_nan=True,
                err_msg=f"Numeric mismatch for column {column}",
            )
            continue

        actual_values = actual_col.where(actual_col.notna(), None).tolist()
        expected_values = expected_col.where(expected_col.notna(), None).tolist()
        assert actual_values == expected_values, (
            f"Mismatch for column {column}: actual={actual_values} expected={expected_values}"
        )
