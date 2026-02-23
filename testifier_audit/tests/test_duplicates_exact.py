from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from testifier_audit.detectors import duplicates_exact as duplicates_exact_module
from testifier_audit.detectors.duplicates_exact import DuplicatesExactDetector


def _build_submission_frame(name_counts: dict[str, int]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    base = pd.Timestamp("2026-02-01 00:00:00")
    row_id = 1
    offset = 0
    for canonical_name, count in name_counts.items():
        for index in range(int(count)):
            timestamp = base + pd.Timedelta(minutes=offset + index)
            rows.append(
                {
                    "id": row_id,
                    "canonical_name": canonical_name,
                    "name_display": canonical_name.replace("|", ", "),
                    "position_normalized": "Pro" if index % 2 == 0 else "Con",
                    "timestamp": timestamp,
                    "minute_bucket": timestamp.floor("min"),
                }
            )
            row_id += 1
        offset += int(count) + 1
    return pd.DataFrame(rows)


def test_collision_baseline_failure_policy_fail_and_degrade() -> None:
    frame = _build_submission_frame({"DOE|JANE": 4, "SMITH|JOHN": 3, "BROWN|AVA": 2})

    degraded = DuplicatesExactDetector(
        top_n=25,
        bucket_minutes=[1, 5],
        collision_baseline_source="vrdb_full_histogram",
        collision_baseline_failure_policy="degrade",
        collision_uncertainty_mode="analytic_only",
        voter_db_url=None,
    )
    degraded_result = degraded.run(df=frame, features={})
    methods = degraded_result.tables["collision_methods"]
    matched_only = methods[methods["scope"] == "matched_only"].reset_index(drop=True)
    assert not matched_only.empty
    assert bool(matched_only.loc[0, "baseline_degraded"]) is True
    assert str(matched_only.loc[0, "baseline_source"]) == "hearing_empirical"
    assert str(matched_only.loc[0, "fallback_policy"]) == "degrade"
    assert bool(degraded_result.summary["baseline_degraded"]) is True
    assert str(degraded_result.summary["baseline_source"]) == "hearing_empirical"

    fail_fast = DuplicatesExactDetector(
        top_n=25,
        bucket_minutes=[1, 5],
        collision_baseline_source="vrdb_full_histogram",
        collision_baseline_failure_policy="fail",
        collision_uncertainty_mode="analytic_only",
        voter_db_url=None,
    )
    with pytest.raises(RuntimeError, match="collision baseline requires voter_registry.db_url"):
        fail_fast.run(df=frame, features={})


def test_per_name_display_limit_does_not_censor_tested_totals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = _build_submission_frame({f"NAME{i:03d}|JANE": 2 for i in range(30)})
    display_limit = 10

    monkeypatch.setattr(
        duplicates_exact_module,
        "binomial_tail_p_value",
        lambda observed_successes, total_trials, success_probability: 0.0,
    )
    monkeypatch.setattr(
        duplicates_exact_module,
        "benjamini_hochberg",
        lambda p_values: pd.Series(
            np.zeros(len(p_values), dtype=float),
            index=p_values.index if isinstance(p_values, pd.Series) else None,
        ),
    )

    detector = DuplicatesExactDetector(
        top_n=100,
        bucket_minutes=[30],
        collision_uncertainty_mode="analytic_only",
        per_name_display_limit=display_limit,
    )
    result = detector.run(df=frame, features={})
    per_name_tests = result.tables["per_name_tests"]
    per_name_display = result.tables["per_name_display"]

    primary_tests = per_name_tests[per_name_tests["scope"] == "matched_only"].copy()
    primary_display = per_name_display[per_name_display["scope"] == "matched_only"].copy()

    assert len(primary_tests) == 30
    assert len(primary_display) == display_limit
    assert primary_display["display_truncated"].astype(bool).all()
    assert int(result.summary["n_significant_per_name"]) == 30
    assert int(result.summary["n_significant_per_name"]) > display_limit


def test_bucket_level_expectations_obey_n_equals_one_invariants() -> None:
    frame = _build_submission_frame(
        {
            "ALPHA|ONE": 1,
            "BRAVO|TWO": 1,
            "CHARLIE|THREE": 1,
            "DELTA|FOUR": 1,
        }
    )
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[1],
        collision_uncertainty_mode="analytic_only",
    )
    result = detector.run(df=frame, features={})
    bucket = result.tables["collision_by_bucket"]
    primary_bucket = bucket[bucket["scope"] == "matched_only"].copy()
    assert not primary_bucket.empty
    assert (primary_bucket["n_bucket"] == 1).all()
    assert (primary_bucket["expected"] == 0.0).all()
    assert (primary_bucket["observed"] == 0.0).all()


def test_stratification_request_degrades_when_registry_baseline_unavailable() -> None:
    frame = _build_submission_frame({"DOE|JANE": 3, "SMITH|JOHN": 2})
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_baseline_source="hearing_empirical",
        collision_stratification="birth_decade",
        collision_baseline_failure_policy="degrade",
        collision_uncertainty_mode="analytic_only",
    )
    result = detector.run(df=frame, features={})
    methods = result.tables["collision_methods"]
    matched = methods[methods["scope"] == "matched_only"].reset_index(drop=True)
    assert not matched.empty
    assert str(matched.loc[0, "stratification"]) == "none"
    assert bool(matched.loc[0, "baseline_degraded"]) is True
    sensitivity = result.tables["collision_stratification_sensitivity"]
    matched_sensitivity = sensitivity[sensitivity["scope"] == "matched_only"].copy()
    assert not matched_sensitivity.empty
    assert set(matched_sensitivity["stratification_requested"]) == {"birth_decade"}
    assert set(matched_sensitivity["stratification_effective"]) == {"none"}


def test_birth_decade_stratification_updates_expectations_with_registry_mix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = _build_submission_frame({"DOE|JANE": 4, "SMITH|JOHN": 1})
    monkeypatch.setattr(
        duplicates_exact_module,
        "fetch_voter_name_key_count_histogram",
        lambda **kwargs: pd.DataFrame(
            {
                "name_count": [1000],
                "n_keys": [2],
                "N": [2000],
            }
        ),
    )
    monkeypatch.setattr(
        duplicates_exact_module,
        "fetch_voter_name_key_stratum_frequencies",
        lambda **kwargs: pd.DataFrame(
            [
                {"name_key": "DOE|JANE", "stratum": "1980s", "n_registry_rows": 900},
                {"name_key": "DOE|JANE", "stratum": "1990s", "n_registry_rows": 100},
                {"name_key": "SMITH|JOHN", "stratum": "1980s", "n_registry_rows": 100},
                {"name_key": "SMITH|JOHN", "stratum": "1990s", "n_registry_rows": 900},
            ]
        ),
    )
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_baseline_source="vrdb_full_histogram",
        collision_stratification="birth_decade",
        collision_baseline_failure_policy="fail",
        collision_uncertainty_mode="analytic_only",
        voter_db_url="postgresql://example",
    )
    result = detector.run(df=frame, features={})
    methods = result.tables["collision_methods"]
    matched = methods[methods["scope"] == "matched_only"].reset_index(drop=True)
    assert not matched.empty
    assert str(matched.loc[0, "stratification"]) == "birth_decade"
    assert bool(matched.loc[0, "baseline_degraded"]) is False

    sensitivity = result.tables["collision_stratification_sensitivity"]
    matched_sensitivity = sensitivity[sensitivity["scope"] == "matched_only"].copy()
    assert not matched_sensitivity.empty
    assert set(matched_sensitivity["stratification_effective"]) == {"birth_decade"}
    assert (matched_sensitivity["expected_effective"] != matched_sensitivity["expected_unstratified"]).any()


def test_birth_decade_stratification_monte_carlo_uses_stratified_sampler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = _build_submission_frame({"DOE|JANE": 4, "SMITH|JOHN": 1})
    monkeypatch.setattr(
        duplicates_exact_module,
        "fetch_voter_name_key_count_histogram",
        lambda **kwargs: pd.DataFrame(
            {
                "name_count": [1000],
                "n_keys": [2],
                "N": [2000],
            }
        ),
    )
    monkeypatch.setattr(
        duplicates_exact_module,
        "fetch_voter_name_key_stratum_frequencies",
        lambda **kwargs: pd.DataFrame(
            [
                {"name_key": "DOE|JANE", "stratum": "1980s", "n_registry_rows": 900},
                {"name_key": "DOE|JANE", "stratum": "1990s", "n_registry_rows": 100},
                {"name_key": "SMITH|JOHN", "stratum": "1980s", "n_registry_rows": 100},
                {"name_key": "SMITH|JOHN", "stratum": "1990s", "n_registry_rows": 900},
            ]
        ),
    )
    monkeypatch.setattr(
        duplicates_exact_module,
        "simulate_collision_null_from_histogram",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("histogram null simulator should not run for stratified multinomial path")
        ),
    )

    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_baseline_source="vrdb_full_histogram",
        collision_stratification="birth_decade",
        collision_baseline_failure_policy="fail",
        collision_uncertainty_mode="monte_carlo",
        voter_db_url="postgresql://example",
        monte_carlo_draws=300,
    )
    result = detector.run(df=frame, features={})
    overview = result.tables["collision_overview"]
    matched = overview[overview["scope"] == "matched_only"].copy()
    assert not matched.empty
