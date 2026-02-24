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


def _build_top_name_timing_frame() -> pd.DataFrame:
    base = pd.Timestamp("2026-02-01 00:00:00")
    rows = [
        {
            "id": 1,
            "canonical_name": "DOE|ROBERT",
            "name_display": "DOE, ROBERT",
            "collision_key_medium": "DOE|ROBERT",
            "canonical_key_nickname": "DOE|ROBERT",
            "collision_key_loose": "DOE|B",
            "position_normalized": "Pro",
            "timestamp": base + pd.Timedelta(minutes=0),
        },
        {
            "id": 2,
            "canonical_name": "DOE|ROBERT",
            "name_display": "DOE, ROBERT",
            "collision_key_medium": "DOE|ROBERT",
            "canonical_key_nickname": "DOE|ROBERT",
            "collision_key_loose": "DOE|B",
            "position_normalized": "Con",
            "timestamp": base + pd.Timedelta(minutes=1),
        },
        {
            "id": 3,
            "canonical_name": "DOE|BOB",
            "name_display": "DOE, BOB",
            "collision_key_medium": "DOE|BOB",
            "canonical_key_nickname": "DOE|ROBERT",
            "collision_key_loose": "DOE|B",
            "position_normalized": "Pro",
            "timestamp": base + pd.Timedelta(minutes=2),
        },
        {
            "id": 4,
            "canonical_name": "DOE|BOB",
            "name_display": "DOE, BOB",
            "collision_key_medium": "DOE|BOB",
            "canonical_key_nickname": "DOE|ROBERT",
            "collision_key_loose": "DOE|B",
            "position_normalized": "Con",
            "timestamp": base + pd.Timedelta(minutes=3),
        },
        {
            "id": 5,
            "canonical_name": "DOE|BEN",
            "name_display": "DOE, BEN",
            "collision_key_medium": "DOE|BEN",
            "canonical_key_nickname": "DOE|BEN",
            "collision_key_loose": "DOE|B",
            "position_normalized": "Pro",
            "timestamp": base + pd.Timedelta(minutes=4),
        },
        {
            "id": 6,
            "canonical_name": "DOE|BEN",
            "name_display": "DOE, BEN",
            "collision_key_medium": "DOE|BEN",
            "canonical_key_nickname": "DOE|BEN",
            "collision_key_loose": "DOE|B",
            "position_normalized": "Con",
            "timestamp": base + pd.Timedelta(minutes=4, seconds=30),
        },
        {
            "id": 7,
            "canonical_name": "LEE|ALICE",
            "name_display": "LEE, ALICE",
            "collision_key_medium": "LEE|ALICE",
            "canonical_key_nickname": "LEE|ALICE",
            "collision_key_loose": "LEE|A",
            "position_normalized": "Pro",
            "timestamp": base + pd.Timedelta(minutes=10),
        },
        {
            "id": 8,
            "canonical_name": "LEE|ALICE",
            "name_display": "LEE, ALICE",
            "collision_key_medium": "LEE|ALICE",
            "canonical_key_nickname": "LEE|ALICE",
            "collision_key_loose": "LEE|A",
            "position_normalized": "Con",
            "timestamp": base + pd.Timedelta(minutes=11),
        },
    ]
    frame = pd.DataFrame(rows)
    frame["minute_bucket"] = pd.to_datetime(frame["timestamp"], errors="coerce").dt.floor("min")
    return frame


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
    primary_scope = degraded.collision_scope_primary
    primary_methods = methods[methods["scope"] == primary_scope].reset_index(drop=True)
    assert not primary_methods.empty
    assert bool(primary_methods.loc[0, "baseline_degraded"]) is True
    assert str(primary_methods.loc[0, "baseline_source"]) == "hearing_empirical"
    assert str(primary_methods.loc[0, "fallback_policy"]) == "degrade"
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


def test_default_scope_analyzes_full_hearing_even_when_voter_matches_exist() -> None:
    frame = _build_submission_frame({"DOE|JANE": 3, "SMITH|JOHN": 2, "BROWN|AVA": 1})
    assignments = pd.DataFrame(
        [
            {"canonical_name": "DOE|JANE", "primary_outcome": "matched_unique"},
            {"canonical_name": "SMITH|JOHN", "primary_outcome": "unmatched"},
            {"canonical_name": "BROWN|AVA", "primary_outcome": "unmatched"},
        ]
    )
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_uncertainty_mode="analytic_only",
    )
    result = detector.run(
        df=frame,
        features={"voter_registry_match.match_assignments": assignments},
    )

    assert detector.collision_scope_primary == "full_hearing"
    assert str(result.summary["collision_scope_primary"]) == "full_hearing"
    assert int(result.summary["n_records"]) == len(frame)
    assert set(result.tables["collision_methods"]["scope"].astype(str)) == {"full_hearing"}
    assert set(result.tables["per_name_tests"]["scope"].astype(str)) == {"full_hearing"}


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

    primary_scope = detector.collision_scope_primary
    primary_tests = per_name_tests[per_name_tests["scope"] == primary_scope].copy()
    primary_display = per_name_display[per_name_display["scope"] == primary_scope].copy()

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
    primary_scope = detector.collision_scope_primary
    primary_bucket = bucket[bucket["scope"] == primary_scope].copy()
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
    primary_scope = detector.collision_scope_primary
    primary = methods[methods["scope"] == primary_scope].reset_index(drop=True)
    assert not primary.empty
    assert str(primary.loc[0, "stratification"]) == "none"
    assert bool(primary.loc[0, "baseline_degraded"]) is True
    sensitivity = result.tables["collision_stratification_sensitivity"]
    primary_sensitivity = sensitivity[sensitivity["scope"] == primary_scope].copy()
    assert not primary_sensitivity.empty
    assert set(primary_sensitivity["stratification_requested"]) == {"birth_decade"}
    assert set(primary_sensitivity["stratification_effective"]) == {"none"}


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
    primary_scope = detector.collision_scope_primary
    primary = methods[methods["scope"] == primary_scope].reset_index(drop=True)
    assert not primary.empty
    assert str(primary.loc[0, "stratification"]) == "birth_decade"
    assert bool(primary.loc[0, "baseline_degraded"]) is False

    sensitivity = result.tables["collision_stratification_sensitivity"]
    primary_sensitivity = sensitivity[sensitivity["scope"] == primary_scope].copy()
    assert not primary_sensitivity.empty
    assert set(primary_sensitivity["stratification_effective"]) == {"birth_decade"}
    assert (primary_sensitivity["expected_effective"] != primary_sensitivity["expected_unstratified"]).any()


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
    primary_scope = detector.collision_scope_primary
    primary = overview[overview["scope"] == primary_scope].copy()
    assert not primary.empty


def test_collision_monte_carlo_draw_budget_scales_with_bucket_size() -> None:
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        monte_carlo_draws=20_000,
    )
    assert detector._collision_monte_carlo_draw_budget(n_rows=0, hard_cap=250) == 0
    assert detector._collision_monte_carlo_draw_budget(n_rows=1, hard_cap=250) == 0
    assert detector._collision_monte_carlo_draw_budget(n_rows=2, hard_cap=250) == 64
    assert detector._collision_monte_carlo_draw_budget(n_rows=4, hard_cap=250) == 50
    assert detector._collision_monte_carlo_draw_budget(n_rows=100, hard_cap=250) == 125
    assert detector._collision_monte_carlo_draw_budget(n_rows=400, hard_cap=250) == 250

    budgets = [
        detector._collision_monte_carlo_draw_budget(n_rows=n_rows, hard_cap=250)
        for n_rows in (2, 5, 10, 20, 40, 80, 100, 200)
    ]
    assert budgets[1:] == sorted(budgets[1:])
    assert all(48 <= value <= 250 for value in budgets[1:])


def test_bucket_monte_carlo_draw_budget_skips_guaranteed_low_power() -> None:
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        monte_carlo_draws=20_000,
        low_power_min_unique_names=25,
        low_power_min_expected_duplicates=5.0,
    )
    assert (
        detector._bucket_monte_carlo_draw_budget(
            n_rows=10,
            expected_primary_metric=10.0,
            hard_cap=250,
        )
        == 0
    )
    assert (
        detector._bucket_monte_carlo_draw_budget(
            n_rows=40,
            expected_primary_metric=2.0,
            hard_cap=250,
        )
        == 0
    )
    expected = detector._collision_monte_carlo_draw_budget(n_rows=40, hard_cap=250)
    assert (
        detector._bucket_monte_carlo_draw_budget(
            n_rows=40,
            expected_primary_metric=8.0,
            hard_cap=250,
        )
        == expected
    )


def test_low_power_bucket_skips_bucket_level_null_simulation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = _build_submission_frame(
        {
            "DOE|JANE": 4,
            "SMITH|JOHN": 4,
            "LEE|ALICE": 4,
            "BROWN|KAI": 4,
        }
    )
    simulated_n_rows: list[int] = []

    def _fake_simulate_collision_null(**kwargs) -> pd.DataFrame:
        simulated_n_rows.append(int(kwargs.get("n_rows", 0)))
        return pd.DataFrame(
            {
                "pairs": [0.0],
                "excess_rows": [0.0],
                "repeated_group_rows": [0.0],
            }
        )

    monkeypatch.setattr(
        duplicates_exact_module,
        "simulate_collision_null_from_histogram",
        _fake_simulate_collision_null,
    )

    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[1],
        collision_uncertainty_mode="monte_carlo",
        monte_carlo_draws=300,
        low_power_min_unique_names=25,
    )
    detector.run(df=frame, features={})

    assert simulated_n_rows
    assert len(simulated_n_rows) == 1
    assert set(simulated_n_rows) == {len(frame)}


def test_top_name_timing_by_mode_emits_ranked_rows_with_expected_mode_collapsing() -> None:
    frame = _build_top_name_timing_frame()
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[1, 5],
        collision_uncertainty_mode="analytic_only",
    )
    result = detector.run(df=frame, features={})
    timing = result.tables["top_name_timing_by_mode"]

    required = {
        "scope",
        "match_mode",
        "match_label",
        "match_definition",
        "rank",
        "name_key",
        "display_name",
        "total_repeated_rows",
        "bucket_start",
        "bucket_minutes",
        "duplicate_rows",
        "n_pro",
        "n_con",
        "n_other",
        "first_seen",
        "last_seen",
    }
    assert required.issubset(timing.columns)
    assert not timing.empty
    assert set(timing["match_mode"]) == {"strict", "loose"}
    assert (timing["duplicate_rows"] >= 1).all()
    assert (timing["duplicate_rows"] == 1).any()
    assert (timing["n_other"] >= 0).all()
    assert (
        timing["n_pro"].astype(int)
        + timing["n_con"].astype(int)
        + timing["n_other"].astype(int)
        == timing["duplicate_rows"].astype(int)
    ).all()

    strict_names = set(timing[timing["match_mode"] == "strict"]["name_key"])
    assert strict_names == {"DOE|ROBERT", "DOE|BOB", "DOE|BEN", "LEE|ALICE"}
    loose_names = set(timing[timing["match_mode"] == "loose"]["name_key"])
    assert loose_names == {"DOE|ROBERT", "DOE|BEN", "LEE|ALICE"}
    mode_ranked = (
        timing[timing["match_mode"] == "strict"][["name_key", "rank", "total_repeated_rows"]]
        .drop_duplicates()
        .sort_values(["rank", "name_key"])
    )
    assert len(mode_ranked) <= 100
    expected_ranks = list(range(1, len(mode_ranked) + 1))
    assert mode_ranked["rank"].astype(int).tolist() == expected_ranks
    totals = mode_ranked["total_repeated_rows"].astype(int).tolist()
    assert totals == sorted(totals, reverse=True)

    per_name_by_mode = result.tables["per_name_duplicates_by_mode"]
    assert not per_name_by_mode.empty
    assert set(per_name_by_mode["match_mode"]) == {"strict", "loose"}
    assert (per_name_by_mode["observed_count"].astype(int) >= 2).all()

    full_timing = result.tables["per_name_submission_timing_by_mode"]
    required_timing_columns = {
        "scope",
        "match_mode",
        "match_label",
        "match_definition",
        "canonical_name",
        "name_key",
        "display_name",
        "bucket_start",
        "position_normalized",
    }
    assert required_timing_columns.issubset(full_timing.columns)
    assert not full_timing.empty
    assert set(full_timing["match_mode"]) == {"strict", "loose"}
