from __future__ import annotations

import math

import numpy as np
import pandas as pd

from testifier_audit.io.vrdb_collision_null import compute_vrdb_collision_null_for_slices


def _probability_rows(
    *,
    counts_by_name: dict[str, int],
    baseline_variant: str = "all_registrants",
    geo_level: str = "state",
    geo_value: str = "WA",
    name_key_type: str = "full_name_key",
) -> pd.DataFrame:
    denominator = int(sum(int(value) for value in counts_by_name.values()))
    rows: list[dict[str, object]] = []
    for name_key, count in counts_by_name.items():
        rows.append(
            {
                "name_key": name_key,
                "name_key_type": name_key_type,
                "count": int(count),
                "probability": float(count) / float(max(denominator, 1)),
                "denominator": denominator,
                "geo_level": geo_level,
                "geo_value": geo_value,
                "baseline_variant": baseline_variant,
                "vrdb_version": "vrdb_extract_v4",
                "normalization_version": "shared_name_normalization_v1",
            }
        )
    return pd.DataFrame(rows)


def _slice_rows(
    *,
    slice_id: str,
    names: list[str],
    slice_type: str = "bucket",
    baseline_variant: str = "all_registrants",
    name_key_type: str = "full_name_key",
    requested_geo_level: str = "state",
    requested_geo_value: str = "WA",
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "slice_id": [slice_id] * len(names),
            "slice_type": [slice_type] * len(names),
            "name_key": names,
            "baseline_variant": [baseline_variant] * len(names),
            "name_key_type": [name_key_type] * len(names),
            "requested_geo_level": [requested_geo_level] * len(names),
            "requested_geo_value": [requested_geo_value] * len(names),
        }
    )


def test_vrdb_collision_null_closed_form_expected_pairs_toy_case() -> None:
    probability_rows = _probability_rows(counts_by_name={"A": 1, "B": 1})
    slice_rows = _slice_rows(slice_id="toy", names=["A", "A", "B", "B"])

    metrics_rows, expected_name_rows = compute_vrdb_collision_null_for_slices(
        slice_rows=slice_rows,
        probability_rows=probability_rows,
        monte_carlo_draws=500,
        random_seed=123,
    )

    assert len(metrics_rows) == 1
    assert not expected_name_rows.empty
    row = metrics_rows.iloc[0]
    expected_pairs = math.comb(4, 2) * ((0.5**2) + (0.5**2))
    assert row["evidence_family"] == "vrdb_collision_null"
    assert abs(float(row["expected_pairs_closed_form"]) - expected_pairs) < 1e-12
    assert abs(float(row["expected_pairs_analytic"]) - expected_pairs) < 1e-12
    assert abs(float(row["observed_pairs"]) - 2.0) < 1e-12


def test_vrdb_collision_null_simulation_matches_multinomial_expectation() -> None:
    probability_rows = _probability_rows(counts_by_name={"A": 50, "B": 30, "C": 20})
    names = ["A"] * 14 + ["B"] * 9 + ["C"] * 7
    slice_rows = _slice_rows(slice_id="sim", names=names)

    metrics_rows, _expected_name_rows = compute_vrdb_collision_null_for_slices(
        slice_rows=slice_rows,
        probability_rows=probability_rows,
        monte_carlo_draws=1200,
        random_seed=2026,
    )

    row = metrics_rows.iloc[0]
    expected_pairs = math.comb(len(names), 2) * (0.5**2 + 0.3**2 + 0.2**2)
    simulated_mean = float(row["expected_pairs_mean"])
    relative_error = abs(simulated_mean - expected_pairs) / expected_pairs
    assert relative_error < 0.20
    assert 0.0 <= float(row["tail_prob_pairs"]) <= 1.0
    assert int(row["monte_carlo_draws_effective"]) == 1200


def test_vrdb_collision_null_bucket_expectation_is_not_linear_rescale() -> None:
    probability_rows = _probability_rows(counts_by_name={"A": 1, "B": 1})
    full_slice = _slice_rows(
        slice_id="full",
        names=["A", "A", "A", "A", "A", "B", "B", "B", "B", "B"],
    )
    bucket_slice = _slice_rows(slice_id="bucket_1m", names=["A", "B"])
    slice_rows = pd.concat([full_slice, bucket_slice], ignore_index=True)

    metrics_rows, _expected_name_rows = compute_vrdb_collision_null_for_slices(
        slice_rows=slice_rows,
        probability_rows=probability_rows,
        monte_carlo_draws=300,
        random_seed=9,
    )

    by_id = metrics_rows.set_index("slice_id")
    full_expected = float(by_id.loc["full", "expected_pairs_analytic"])
    bucket_expected = float(by_id.loc["bucket_1m", "expected_pairs_analytic"])
    linear_rescaled = full_expected * (2.0 / 10.0)

    assert abs(full_expected - (math.comb(10, 2) * 0.5)) < 1e-12
    assert abs(bucket_expected - (math.comb(2, 2) * 0.5)) < 1e-12
    assert not np.isclose(bucket_expected, linear_rescaled, rtol=1e-6, atol=1e-6)
    assert bucket_expected < linear_rescaled


def test_vrdb_collision_null_uses_effective_geography_backoff() -> None:
    probability_rows = _probability_rows(
        counts_by_name={"A": 60, "B": 40},
        geo_level="state",
        geo_value="WA",
    )
    backoff_rows = pd.DataFrame(
        [
            {
                "baseline_variant": "all_registrants",
                "requested_geo_level": "county",
                "requested_geo_value": "AD",
                "effective_geo_level": "state",
                "effective_geo_value": "WA",
                "fallback_steps": 1,
                "backoff_reason": "county_denominator_below_threshold",
                "effective_denominator": 100,
            }
        ]
    )
    slice_rows = _slice_rows(
        slice_id="county_slice",
        names=["A", "A", "A", "B", "B"],
        requested_geo_level="county",
        requested_geo_value="AD",
    )

    metrics_rows, _expected_name_rows = compute_vrdb_collision_null_for_slices(
        slice_rows=slice_rows,
        probability_rows=probability_rows,
        backoff_rows=backoff_rows,
        monte_carlo_draws=300,
        random_seed=1234,
    )

    row = metrics_rows.iloc[0]
    assert row["requested_geo_level"] == "county"
    assert row["requested_geo_value"] == "AD"
    assert row["effective_geo_level"] == "state"
    assert row["effective_geo_value"] == "WA"
    assert int(row["fallback_steps"]) == 1
    assert row["backoff_reason"] == "county_denominator_below_threshold"
    expected_pairs = math.comb(5, 2) * ((0.6**2) + (0.4**2))
    assert abs(float(row["expected_pairs_analytic"]) - expected_pairs) < 1e-12
