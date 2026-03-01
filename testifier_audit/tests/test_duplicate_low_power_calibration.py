from __future__ import annotations

from dataclasses import asdict
import math

import pandas as pd

from testifier_audit.backtests.duplicate_low_power_calibration import (
    BUCKET_FAMILY,
    POSITION_FAMILY,
    SCOPE_FAMILY,
    build_calibration_report_markdown,
    default_scenarios,
    default_targets,
    run_duplicate_low_power_calibration,
)


def _smoke_artifacts():
    return run_duplicate_low_power_calibration(
        scenarios=default_scenarios(),
        scenario_replicates=4,
        seed=6346,
        bucket_minutes=30,
        scope_draws=64,
        bucket_draws=48,
        position_permutations=80,
    )


def _normalize_nans(value):
    if isinstance(value, dict):
        return {k: _normalize_nans(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_nans(v) for v in value]
    if isinstance(value, float) and math.isnan(value):
        return "__nan__"
    return value


def test_duplicate_low_power_calibration_is_deterministic_for_seed() -> None:
    first = _smoke_artifacts()
    second = _smoke_artifacts()

    pd.testing.assert_frame_equal(first.case_summary, second.case_summary)
    pd.testing.assert_frame_equal(first.bucket_details, second.bucket_details)
    pd.testing.assert_frame_equal(first.threshold_grid, second.threshold_grid)
    assert asdict(first.recommendations) == asdict(second.recommendations)
    assert _normalize_nans(first.benchmark_summary) == _normalize_nans(
        second.benchmark_summary
    )


def test_duplicate_low_power_threshold_grid_contains_expected_families() -> None:
    artifacts = _smoke_artifacts()
    families = set(artifacts.threshold_grid["family"].astype(str))
    assert families == {SCOPE_FAMILY, BUCKET_FAMILY, POSITION_FAMILY}

    for family in sorted(families):
        subset = artifacts.threshold_grid[artifacts.threshold_grid["family"] == family]
        assert not subset.empty
        assert subset["min_unique_names"].nunique() >= 3
        assert subset["min_expected_duplicates"].nunique() >= 3

    recommendations = asdict(artifacts.recommendations)
    assert recommendations["low_power_min_unique_names"] >= 1
    assert recommendations["low_power_min_expected_duplicates"] >= 0.0
    assert recommendations["low_power_min_unique_names_bucket"] >= 1
    assert recommendations["low_power_min_expected_duplicates_bucket"] >= 0.0
    assert recommendations["low_power_min_unique_names_position"] >= 1
    assert recommendations["low_power_min_expected_duplicates_position"] >= 0.0


def test_duplicate_low_power_report_mentions_targets_and_recommendations() -> None:
    artifacts = _smoke_artifacts()
    report = build_calibration_report_markdown(
        artifacts=artifacts,
        scenarios=default_scenarios(),
        targets=default_targets(),
    )

    assert "Operating Targets" in report
    assert "Recommended Thresholds" in report
    assert "low_power_min_unique_names" in report
    assert "low_power_min_expected_duplicates" in report
    assert "scope" in report.lower()
    assert "bucket" in report.lower()
    assert "position" in report.lower()
