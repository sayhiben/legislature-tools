from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from testifier_audit.detectors import duplicates_exact as duplicates_exact_module
from testifier_audit.detectors.duplicates_exact import DuplicatesExactDetector
from testifier_audit.profiling import RuntimeProfiler, activate_runtime_profiler


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
            "collision_key_strict": "DOE|ROBERT",
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
            "collision_key_strict": "DOE|ROBERT",
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
            "collision_key_strict": "DOE|BOB",
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
            "collision_key_strict": "DOE|BOB",
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
            "collision_key_strict": "DOE|BEN",
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
            "collision_key_strict": "DOE|BEN",
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
            "collision_key_strict": "LEE|ALICE",
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
            "collision_key_strict": "LEE|ALICE",
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
    assert str(primary_methods.loc[0, "inferential_status"]) == "descriptive_only"
    assert (
        str(primary_methods.loc[0, "inferential_reason"])
        == degraded.INFERENTIAL_REASON_DEGRADED_TO_SELF_REFERENTIAL
    )
    assert bool(degraded_result.summary["baseline_degraded"]) is True
    assert str(degraded_result.summary["baseline_source"]) == "hearing_empirical"
    assert str(degraded_result.summary["inferential_status"]) == "descriptive_only"
    assert (
        str(degraded_result.summary["inferential_reason"])
        == degraded.INFERENTIAL_REASON_DEGRADED_TO_SELF_REFERENTIAL
    )

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


def test_collision_key_mode_guard_rejects_non_strict_inferential_mode() -> None:
    with pytest.raises(ValueError, match="collision_key_mode must be 'strict'"):
        DuplicatesExactDetector(
            top_n=10,
            bucket_minutes=[5],
            collision_key_mode="medium",
        )


def test_duplicate_statistical_contract_is_emitted_in_summary_and_methods() -> None:
    frame = _build_submission_frame({"DOE|JANE": 4, "SMITH|JOHN": 3, "BROWN|AVA": 2})
    detector = DuplicatesExactDetector(
        top_n=25,
        bucket_minutes=[5],
        collision_uncertainty_mode="analytic_only",
    )
    result = detector.run(df=frame, features={})

    summary = result.summary
    assert summary["claim_class"] == detector.COLLISION_CLAIM_CLASS
    assert summary["estimand_primary"] == detector.STATISTICAL_CONTRACT_ESTIMAND_PRIMARY
    assert summary["non_goals"] == detector.STATISTICAL_CONTRACT_NON_GOALS
    assert summary["baseline_semantics"] == detector.STATISTICAL_CONTRACT_BASELINE_SEMANTICS
    assert isinstance(summary.get("statistical_contract"), dict)
    assert summary["statistical_contract"]["estimand_primary"] == detector.STATISTICAL_CONTRACT_ESTIMAND_PRIMARY
    assert summary["statistical_contract"]["non_goals"] == detector.STATISTICAL_CONTRACT_NON_GOALS
    assert summary["statistical_contract"]["baseline_semantics"] == detector.STATISTICAL_CONTRACT_BASELINE_SEMANTICS
    assert summary["statistical_contract"]["inferential_status"] == summary["inferential_status"]
    assert summary["statistical_contract"]["inferential_reason"] == summary["inferential_reason"]

    methods = result.tables["collision_methods"]
    required_columns = {
        "baseline_label",
        "scope_status",
        "scope_reason",
        "claim_class",
        "inferential_status",
        "inferential_reason",
        "estimand_primary",
        "non_goals",
        "baseline_semantics",
    }
    assert required_columns.issubset(set(methods.columns))
    assert set(methods["claim_class"].astype(str)) == {detector.COLLISION_CLAIM_CLASS}
    assert set(methods["estimand_primary"].astype(str)) == {
        detector.STATISTICAL_CONTRACT_ESTIMAND_PRIMARY
    }
    assert set(methods["non_goals"].astype(str)) == {detector.STATISTICAL_CONTRACT_NON_GOALS}
    assert set(methods["baseline_semantics"].astype(str)) == {
        detector.STATISTICAL_CONTRACT_BASELINE_SEMANTICS
    }
    assert methods["baseline_label"].astype(str).str.len().gt(0).all()
    assert methods["inferential_status"].astype(str).isin(
        {"descriptive_only", "reference_model_inference"}
    ).all()


def test_matched_scope_is_unavailable_when_match_assignments_missing() -> None:
    frame = _build_submission_frame({"DOE|JANE": 3, "SMITH|JOHN": 2, "BROWN|AVA": 1})
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_scope_primary="matched_only",
        collision_scope_overlays=["full_hearing"],
        collision_uncertainty_mode="analytic_only",
    )
    result = detector.run(df=frame, features={})

    methods = result.tables["collision_methods"]
    matched = methods[methods["scope"] == "matched_only"].reset_index(drop=True)
    full_hearing = methods[methods["scope"] == "full_hearing"].reset_index(drop=True)
    assert not matched.empty
    assert not full_hearing.empty
    assert str(matched.loc[0, "scope_status"]) == detector.SCOPE_STATUS_UNAVAILABLE
    assert (
        str(matched.loc[0, "scope_reason"])
        == detector.SCOPE_REASON_UNAVAILABLE_MISSING_MATCH_ASSIGNMENTS
    )
    assert int(matched.loc[0, "n_used"]) == 0
    assert str(full_hearing.loc[0, "scope_status"]) == detector.SCOPE_STATUS_AVAILABLE
    assert int(full_hearing.loc[0, "n_used"]) == len(frame)
    assert str(result.summary["scope_status"]) == detector.SCOPE_STATUS_UNAVAILABLE
    assert (
        str(result.summary["scope_reason"])
        == detector.SCOPE_REASON_UNAVAILABLE_MISSING_MATCH_ASSIGNMENTS
    )
    assert int(result.summary["n_records"]) == 0


def test_malformed_match_assignments_mark_requested_scopes_unavailable() -> None:
    frame = _build_submission_frame({"DOE|JANE": 3, "SMITH|JOHN": 2, "BROWN|AVA": 1})
    malformed_assignments = pd.DataFrame(
        [
            {"canonical_name": "DOE|JANE"},
            {"canonical_name": "SMITH|JOHN"},
        ]
    )
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_scope_primary="matched_only",
        collision_scope_overlays=["full_hearing", "unmatched_only"],
        collision_uncertainty_mode="analytic_only",
    )
    result = detector.run(
        df=frame,
        features={"voter_registry_match.match_assignments": malformed_assignments},
    )
    methods = result.tables["collision_methods"]
    for scope in ("matched_only", "unmatched_only"):
        scope_row = methods[methods["scope"] == scope].reset_index(drop=True)
        assert not scope_row.empty
        assert str(scope_row.loc[0, "scope_status"]) == detector.SCOPE_STATUS_UNAVAILABLE
        assert (
            str(scope_row.loc[0, "scope_reason"])
            == detector.SCOPE_REASON_UNAVAILABLE_MISSING_MATCH_ASSIGNMENTS
        )
        assert int(scope_row.loc[0, "n_used"]) == 0


def test_no_person_filtering_marks_scope_unavailable_without_fallback() -> None:
    frame = _build_submission_frame({"DOE|JANE": 3, "SMITH|JOHN": 2})
    frame["is_person_name"] = False
    assignments = pd.DataFrame(
        [
            {"canonical_name": "DOE|JANE", "primary_outcome": "matched_unique"},
            {"canonical_name": "SMITH|JOHN", "primary_outcome": "unmatched"},
        ]
    )
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_scope_primary="full_hearing",
        collision_scope_overlays=["matched_only"],
        collision_uncertainty_mode="analytic_only",
        exclude_non_person_from_inference=True,
    )
    result = detector.run(
        df=frame,
        features={"voter_registry_match.match_assignments": assignments},
    )
    methods = result.tables["collision_methods"]
    for scope in ("full_hearing", "matched_only"):
        scope_row = methods[methods["scope"] == scope].reset_index(drop=True)
        assert not scope_row.empty
        assert str(scope_row.loc[0, "scope_status"]) == detector.SCOPE_STATUS_UNAVAILABLE
        assert (
            str(scope_row.loc[0, "scope_reason"])
            == detector.SCOPE_REASON_UNAVAILABLE_NO_PERSON_ROWS
        )
        assert int(scope_row.loc[0, "n_used"]) == 0
    assert str(result.summary["scope_status"]) == detector.SCOPE_STATUS_UNAVAILABLE
    assert (
        str(result.summary["scope_reason"])
        == detector.SCOPE_REASON_UNAVAILABLE_NO_PERSON_ROWS
    )
    assert int(result.summary["n_records"]) == 0


def test_requested_scope_with_no_rows_after_filtering_is_unavailable() -> None:
    frame = _build_submission_frame({"DOE|JANE": 3, "SMITH|JOHN": 2})
    assignments = pd.DataFrame(
        [
            {"canonical_name": "DOE|JANE", "primary_outcome": "unmatched"},
            {"canonical_name": "SMITH|JOHN", "primary_outcome": "unmatched"},
        ]
    )
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_scope_primary="matched_only",
        collision_scope_overlays=["full_hearing"],
        collision_uncertainty_mode="analytic_only",
    )
    result = detector.run(
        df=frame,
        features={"voter_registry_match.match_assignments": assignments},
    )
    methods = result.tables["collision_methods"]
    matched = methods[methods["scope"] == "matched_only"].reset_index(drop=True)
    assert not matched.empty
    assert str(matched.loc[0, "scope_status"]) == detector.SCOPE_STATUS_UNAVAILABLE
    assert (
        str(matched.loc[0, "scope_reason"])
        == detector.SCOPE_REASON_UNAVAILABLE_NO_ROWS_AFTER_FILTERING
    )
    assert int(matched.loc[0, "n_used"]) == 0


def test_self_referential_baseline_suppresses_inferential_fields() -> None:
    frame = _build_submission_frame({"DOE|JANE": 4, "SMITH|JOHN": 3, "BROWN|AVA": 2})
    detector = DuplicatesExactDetector(
        top_n=25,
        bucket_minutes=[5],
        collision_baseline_source="hearing_empirical",
        collision_uncertainty_mode="monte_carlo",
        monte_carlo_draws=400,
        low_power_min_unique_names=1,
        low_power_min_expected_duplicates=0.0,
    )
    result = detector.run(df=frame, features={})
    primary_scope = detector.collision_scope_primary

    methods = result.tables["collision_methods"]
    primary_methods = methods[methods["scope"] == primary_scope].reset_index(drop=True)
    assert not primary_methods.empty
    assert str(primary_methods.loc[0, "inferential_status"]) == "descriptive_only"
    assert (
        str(primary_methods.loc[0, "inferential_reason"])
        == detector.INFERENTIAL_REASON_SELF_REFERENTIAL_BASELINE
    )

    overview = result.tables["collision_overview"]
    primary_overview = overview[overview["scope"] == primary_scope].copy()
    assert not primary_overview.empty
    assert primary_overview["p_value"].isna().all()
    assert primary_overview["z_score"].isna().all()

    per_name = result.tables["per_name_tests"]
    primary_per_name = per_name[per_name["scope"] == primary_scope].copy()
    assert not primary_per_name.empty
    assert primary_per_name["p_value"].isna().all()
    assert primary_per_name["q_value"].isna().all()
    assert primary_per_name["is_significant"].isna().all()
    assert not primary_per_name["tested"].astype(bool).any()

    buckets = result.tables["collision_by_bucket"]
    primary_buckets = buckets[buckets["scope"] == primary_scope].copy()
    assert not primary_buckets.empty
    assert set(primary_buckets["inference_status"].astype(str)) == {"descriptive_only"}
    assert primary_buckets["p_value"].isna().all()
    assert primary_buckets["z_score"].isna().all()


def test_analytic_only_null_path_reports_unavailable_and_suppresses_inference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = _build_submission_frame({"DOE|JANE": 4, "SMITH|JOHN": 3, "BROWN|AVA": 2})
    monkeypatch.setattr(
        duplicates_exact_module,
        "fetch_voter_name_key_count_histogram",
        lambda **kwargs: pd.DataFrame(
            {
                "name_count": [1],
                "n_keys": [50_000],
                "N": [50_000],
            }
        ),
    )
    detector = DuplicatesExactDetector(
        top_n=25,
        bucket_minutes=[5],
        collision_baseline_source="vrdb_full_histogram",
        collision_uncertainty_mode="analytic_only",
        voter_db_url="postgresql://example",
        low_power_min_unique_names=1,
        low_power_min_expected_duplicates=0.0,
    )
    result = detector.run(df=frame, features={})
    primary_scope = detector.collision_scope_primary

    methods = result.tables["collision_methods"]
    primary_methods = methods[methods["scope"] == primary_scope].reset_index(drop=True)
    assert not primary_methods.empty
    assert str(primary_methods.loc[0, "inferential_status"]) == "unavailable"
    assert (
        str(primary_methods.loc[0, "inferential_reason"])
        == detector.INFERENTIAL_REASON_NO_NULL_SAMPLES
    )

    overview = result.tables["collision_overview"]
    primary_overview = overview[overview["scope"] == primary_scope].copy()
    assert not primary_overview.empty
    assert primary_overview["p_value"].isna().all()
    assert primary_overview["z_score"].isna().all()

    per_name = result.tables["per_name_tests"]
    primary_per_name = per_name[per_name["scope"] == primary_scope].copy()
    assert not primary_per_name.empty
    assert primary_per_name["p_value"].isna().all()
    assert primary_per_name["q_value"].isna().all()
    assert primary_per_name["is_significant"].isna().all()
    assert not primary_per_name["tested"].astype(bool).any()

    buckets = result.tables["collision_by_bucket"]
    primary_buckets = buckets[buckets["scope"] == primary_scope].copy()
    assert not primary_buckets.empty
    assert set(primary_buckets["inference_status"].astype(str)) == {"unavailable"}
    assert primary_buckets["p_value"].isna().all()
    assert primary_buckets["z_score"].isna().all()


def test_stratified_hypergeometric_rounding_path_is_explicitly_non_inferential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = _build_submission_frame({"DOE|JANE": 4, "SMITH|JOHN": 3, "BROWN|AVA": 2})
    monkeypatch.setattr(
        duplicates_exact_module,
        "fetch_voter_name_key_count_histogram",
        lambda **kwargs: pd.DataFrame(
            {
                "name_count": [1, 2, 3],
                "n_keys": [120, 30, 10],
                "N": [210, 210, 210],
            }
        ),
    )
    monkeypatch.setattr(
        duplicates_exact_module,
        "fetch_voter_name_key_stratum_frequencies",
        lambda **kwargs: pd.DataFrame(
            {
                "name_key": [
                    "DOE|JANE",
                    "SMITH|JOHN",
                    "BROWN|AVA",
                    "LEE|ALICE",
                ],
                "stratum": ["1980s", "1980s", "1990s", "1990s"],
                "n_registry_rows": [40, 50, 30, 20],
            }
        ),
    )

    detector = DuplicatesExactDetector(
        top_n=25,
        bucket_minutes=[5],
        collision_baseline_source="vrdb_full_histogram",
        collision_baseline_model="hypergeometric",
        collision_uncertainty_mode="monte_carlo",
        collision_stratification="birth_decade",
        voter_db_url="postgresql://example",
        monte_carlo_draws=400,
        low_power_min_unique_names=1,
        low_power_min_expected_duplicates=0.0,
    )
    result = detector.run(df=frame, features={})
    primary_scope = detector.collision_scope_primary

    methods = result.tables["collision_methods"]
    primary_methods = methods[methods["scope"] == primary_scope].reset_index(drop=True)
    assert not primary_methods.empty
    assert str(primary_methods.loc[0, "inferential_status"]) == "unavailable"
    assert (
        str(primary_methods.loc[0, "inferential_reason"])
        == detector.INFERENTIAL_REASON_HYPERGEOMETRIC_STRATIFIED_ROUNDING_DISABLED
    )

    overview = result.tables["collision_overview"]
    primary_overview = overview[overview["scope"] == primary_scope].copy()
    assert not primary_overview.empty
    assert primary_overview["p_value"].isna().all()
    assert primary_overview["z_score"].isna().all()


def test_stratified_hypergeometric_rounding_guard_blocks_inference_even_if_null_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = _build_submission_frame({"DOE|JANE": 4, "SMITH|JOHN": 3, "BROWN|AVA": 2})
    monkeypatch.setattr(
        duplicates_exact_module,
        "fetch_voter_name_key_count_histogram",
        lambda **kwargs: pd.DataFrame(
            {
                "name_count": [1, 2, 3],
                "n_keys": [120, 30, 10],
                "N": [210, 210, 210],
            }
        ),
    )
    monkeypatch.setattr(
        duplicates_exact_module,
        "fetch_voter_name_key_stratum_frequencies",
        lambda **kwargs: pd.DataFrame(
            {
                "name_key": [
                    "DOE|JANE",
                    "SMITH|JOHN",
                    "BROWN|AVA",
                    "LEE|ALICE",
                ],
                "stratum": ["1980s", "1980s", "1990s", "1990s"],
                "n_registry_rows": [40, 50, 30, 20],
            }
        ),
    )
    monkeypatch.setattr(
        duplicates_exact_module,
        "simulate_collision_null_from_histogram",
        lambda **kwargs: pd.DataFrame(
            {
                "pairs": [1.0, 2.0, 3.0],
                "excess_rows": [0.5, 1.0, 1.5],
                "repeated_group_rows": [2.0, 3.0, 4.0],
            }
        ),
    )

    detector = DuplicatesExactDetector(
        top_n=25,
        bucket_minutes=[5],
        collision_baseline_source="vrdb_full_histogram",
        collision_baseline_model="hypergeometric",
        collision_uncertainty_mode="monte_carlo",
        collision_stratification="birth_decade",
        voter_db_url="postgresql://example",
        monte_carlo_draws=400,
        low_power_min_unique_names=1,
        low_power_min_expected_duplicates=0.0,
    )
    result = detector.run(df=frame, features={})
    primary_scope = detector.collision_scope_primary

    methods = result.tables["collision_methods"]
    primary_methods = methods[methods["scope"] == primary_scope].reset_index(drop=True)
    assert not primary_methods.empty
    assert str(primary_methods.loc[0, "inferential_status"]) == "unavailable"
    assert (
        str(primary_methods.loc[0, "inferential_reason"])
        == detector.INFERENTIAL_REASON_HYPERGEOMETRIC_STRATIFIED_ROUNDING_DISABLED
    )

    overview = result.tables["collision_overview"]
    primary_overview = overview[overview["scope"] == primary_scope].copy()
    assert not primary_overview.empty
    assert primary_overview["p_value"].isna().all()
    assert primary_overview["z_score"].isna().all()


def test_duplicates_exact_emits_scope_phase_runtime_profile_keys() -> None:
    frame = _build_submission_frame({"DOE|JANE": 3, "SMITH|JOHN": 2, "BROWN|AVA": 1})
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_uncertainty_mode="analytic_only",
    )
    profiler = RuntimeProfiler()
    with activate_runtime_profiler(profiler):
        detector.run(df=frame, features={})

    timings = profiler.to_dict()["timings"]
    expected_keys = {
        "detector.duplicates_exact.prepare_working",
        "detector.duplicates_exact.resolve_scope_frames",
        "detector.duplicates_exact.load_contextual_baseline",
        "detector.duplicates_exact.prepare_stratification",
        "detector.duplicates_exact.scope.prepare_frame",
        "detector.duplicates_exact.scope.top_name_timing",
        "detector.duplicates_exact.scope.expected_metrics_and_null",
        "detector.duplicates_exact.scope.per_name_tests",
        "detector.duplicates_exact.scope.temporal_metrics",
        "detector.duplicates_exact.scope.bucket_scan",
        "detector.duplicates_exact.scope.primary_legacy_outputs",
        "detector.duplicates_exact.assemble_outputs",
    }
    assert expected_keys.issubset(set(timings))
    for key in expected_keys:
        assert timings[key]["calls"] >= 1


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
    monkeypatch.setattr(
        duplicates_exact_module,
        "fetch_voter_name_key_count_histogram",
        lambda **kwargs: pd.DataFrame(
            {
                "name_count": [1],
                "n_keys": [10_000],
                "N": [10_000],
            }
        ),
    )

    detector = DuplicatesExactDetector(
        top_n=100,
        bucket_minutes=[30],
        collision_uncertainty_mode="monte_carlo",
        collision_baseline_source="vrdb_full_histogram",
        voter_db_url="postgresql://example",
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
    simulated_n_rows: list[int] = []
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
        lambda **kwargs: (
            simulated_n_rows.append(int(kwargs.get("n_rows", 0)))
            or pd.DataFrame(
                {
                    "pairs": [0.0],
                    "excess_rows": [0.0],
                    "repeated_group_rows": [0.0],
                }
            )
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
    assert simulated_n_rows
    assert len(frame) not in set(simulated_n_rows)


def test_stratified_same_hearing_weights_are_descriptive_only_with_provenance(
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
        collision_baseline_model="multinomial",
        collision_stratification="birth_decade",
        collision_baseline_failure_policy="fail",
        collision_uncertainty_mode="monte_carlo",
        voter_db_url="postgresql://example",
        monte_carlo_draws=300,
        low_power_min_unique_names=1,
        low_power_min_expected_duplicates=0.0,
    )
    result = detector.run(df=frame, features={})
    primary_scope = detector.collision_scope_primary

    methods = result.tables["collision_methods"]
    primary_methods = methods[methods["scope"] == primary_scope].reset_index(drop=True)
    assert not primary_methods.empty
    assert str(primary_methods.loc[0, "stratification"]) == "birth_decade"
    assert (
        str(primary_methods.loc[0, "inferential_status"])
        == "descriptive_only"
    )
    assert (
        str(primary_methods.loc[0, "inferential_reason"])
        == detector.INFERENTIAL_REASON_STRATIFICATION_ENDOGENEITY_UNCONTROLLED
    )
    assert (
        str(primary_methods.loc[0, "stratification_weight_source"])
        == detector.STRATIFICATION_WEIGHT_SOURCE_SAME_HEARING
    )
    assert (
        str(primary_methods.loc[0, "stratification_leakage_control"])
        == detector.STRATIFICATION_LEAKAGE_CONTROL_NONE
    )
    assert (
        str(primary_methods.loc[0, "stratification_weight_uncertainty"])
        == detector.STRATIFICATION_WEIGHT_UNCERTAINTY_NOT_PROPAGATED
    )
    assert bool(primary_methods.loc[0, "stratification_endogeneity_uncontrolled"]) is True

    summary = result.summary
    assert (
        str(summary.get("stratification_weight_source", ""))
        == detector.STRATIFICATION_WEIGHT_SOURCE_SAME_HEARING
    )
    assert (
        str(summary.get("stratification_leakage_control", ""))
        == detector.STRATIFICATION_LEAKAGE_CONTROL_NONE
    )
    assert (
        str(summary.get("stratification_weight_uncertainty", ""))
        == detector.STRATIFICATION_WEIGHT_UNCERTAINTY_NOT_PROPAGATED
    )
    assert bool(summary.get("stratification_endogeneity_uncontrolled")) is True
    assert (
        str(summary.get("inferential_reason", ""))
        == detector.INFERENTIAL_REASON_STRATIFICATION_ENDOGENEITY_UNCONTROLLED
    )

    overview = result.tables["collision_overview"]
    primary_overview = overview[overview["scope"] == primary_scope].copy()
    assert not primary_overview.empty
    assert primary_overview["p_value"].isna().all()
    assert primary_overview["z_score"].isna().all()

    sensitivity = result.tables["collision_stratification_sensitivity"]
    primary_sensitivity = sensitivity[sensitivity["scope"] == primary_scope].copy()
    assert not primary_sensitivity.empty
    assert set(primary_sensitivity["stratification_weight_source"].astype(str)) == {
        detector.STRATIFICATION_WEIGHT_SOURCE_SAME_HEARING
    }
    assert set(primary_sensitivity["stratification_leakage_control"].astype(str)) == {
        detector.STRATIFICATION_LEAKAGE_CONTROL_NONE
    }
    assert set(primary_sensitivity["stratification_weight_uncertainty"].astype(str)) == {
        detector.STRATIFICATION_WEIGHT_UNCERTAINTY_NOT_PROPAGATED
    }
    assert primary_sensitivity["stratification_endogeneity_uncontrolled"].astype(bool).all()


def test_collision_monte_carlo_draw_budget_is_not_row_scaled() -> None:
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        monte_carlo_draws=20_000,
    )
    assert detector._collision_monte_carlo_draw_budget(n_rows=0, hard_cap=250) == 0
    assert detector._collision_monte_carlo_draw_budget(n_rows=1, hard_cap=250) == 0
    assert detector._collision_monte_carlo_draw_budget(n_rows=2, hard_cap=250) == 250
    assert detector._collision_monte_carlo_draw_budget(n_rows=400, hard_cap=250) == 250


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
    expected = 250
    assert (
        detector._bucket_monte_carlo_draw_budget(
            n_rows=40,
            expected_primary_metric=8.0,
            hard_cap=250,
        )
        == expected
    )


def test_collision_outputs_include_monte_carlo_precision_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = _build_submission_frame({"DOE|JANE": 5, "SMITH|JOHN": 5, "BROWN|AVA": 4})
    monkeypatch.setattr(
        duplicates_exact_module,
        "fetch_voter_name_key_count_histogram",
        lambda **kwargs: pd.DataFrame(
            {
                "name_count": [1, 2, 3],
                "n_keys": [1200, 300, 100],
                "N": [2100, 2100, 2100],
            }
        ),
    )
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_baseline_source="vrdb_full_histogram",
        collision_uncertainty_mode="monte_carlo",
        voter_db_url="postgresql://example",
        monte_carlo_draws=800,
        monte_carlo_min_draws=64,
        low_power_min_unique_names=1,
        low_power_min_expected_duplicates=0.0,
        random_seed=11,
    )
    result = detector.run(df=frame, features={})

    overview = result.tables["collision_overview"]
    primary_scope = detector.collision_scope_primary
    primary_row = overview[
        (overview["scope"].astype(str) == primary_scope)
        & (overview["metric"].astype(str) == detector.PRIMARY_SCOPE_ENDPOINT_METRIC)
    ].reset_index(drop=True)
    assert not primary_row.empty
    assert int(primary_row.loc[0, "monte_carlo_draws_effective"]) > 0
    assert np.isfinite(float(primary_row.loc[0, "monte_carlo_p_value_mcse"]))
    ci_low = float(primary_row.loc[0, "monte_carlo_p_value_ci_low"])
    ci_high = float(primary_row.loc[0, "monte_carlo_p_value_ci_high"])
    assert 0.0 <= ci_low <= ci_high <= 1.0

    by_bucket = result.tables["collision_by_bucket"]
    bucket_primary = by_bucket[
        (by_bucket["scope"].astype(str) == primary_scope)
        & (by_bucket["metric"].astype(str) == detector.PRIMARY_SCOPE_ENDPOINT_METRIC)
    ].reset_index(drop=True)
    assert not bucket_primary.empty
    assert int(bucket_primary.loc[0, "monte_carlo_draws_effective"]) > 0


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
    assert simulated_n_rows.count(len(frame)) == 1
    position_counts = set(frame["position_normalized"].value_counts().astype(int).tolist())
    allowed_n_rows = {len(frame), *position_counts}
    assert set(simulated_n_rows).issubset(allowed_n_rows)


def test_top_name_timing_by_mode_emits_ranked_rows_with_expected_mode_collapsing() -> None:
    frame = _build_top_name_timing_frame()
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[1, 5],
        collision_uncertainty_mode="analytic_only",
    )
    result = detector.run(df=frame, features={})
    assert result.summary["inferential_key_mode"] == "strict"
    timing = result.tables["top_name_timing_by_mode"]

    required = {
        "scope",
        "match_mode",
        "match_label",
        "match_definition",
        "match_mode_role",
        "inferential_key_mode",
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
    assert set(timing["inferential_key_mode"].astype(str)) == {"strict"}
    assert set(timing["match_mode_role"].astype(str)) == {
        detector.MATCH_MODE_ROLE_PRIMARY_INFERENTIAL,
        detector.MATCH_MODE_ROLE_SENSITIVITY,
    }
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
    assert len(mode_ranked) <= 200
    expected_ranks = list(range(1, len(mode_ranked) + 1))
    assert mode_ranked["rank"].astype(int).tolist() == expected_ranks
    totals = mode_ranked["total_repeated_rows"].astype(int).tolist()
    assert totals == sorted(totals, reverse=True)

    per_name_by_mode = result.tables["per_name_duplicates_by_mode"]
    assert not per_name_by_mode.empty
    assert set(per_name_by_mode["match_mode"]) == {"strict", "loose"}
    assert set(per_name_by_mode["inferential_key_mode"].astype(str)) == {"strict"}
    assert set(per_name_by_mode["match_mode_role"].astype(str)) == {
        detector.MATCH_MODE_ROLE_PRIMARY_INFERENTIAL,
        detector.MATCH_MODE_ROLE_SENSITIVITY,
    }
    assert (per_name_by_mode["observed_count"].astype(int) >= 2).all()

    full_timing = result.tables["per_name_submission_timing_by_mode"]
    required_timing_columns = {
        "scope",
        "match_mode",
        "match_label",
        "match_definition",
        "match_mode_role",
        "inferential_key_mode",
        "canonical_name",
        "name_key",
        "display_name",
        "bucket_start",
        "position_normalized",
    }
    assert required_timing_columns.issubset(full_timing.columns)
    assert not full_timing.empty
    assert set(full_timing["match_mode"]) == {"strict", "loose"}


def test_collision_by_bucket_position_emits_expected_hearing_position_baseline() -> None:
    frame = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "canonical_name": ["DOE|JANE", "DOE|JANE", "SMITH|JOHN"],
            "position_normalized": ["Pro", "Pro", "Con"],
            "timestamp": pd.to_datetime(
                ["2026-02-01 00:00:00", "2026-02-01 00:01:00", "2026-02-01 00:02:00"]
            ),
            "minute_bucket": pd.to_datetime(
                ["2026-02-01 00:00:00", "2026-02-01 00:01:00", "2026-02-01 00:02:00"]
            ),
            "name_display": ["DOE, JANE", "DOE, JANE", "SMITH, JOHN"],
        }
    )
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_uncertainty_mode="analytic_only",
        position_hearing_baseline_enabled=True,
        position_baseline_shrink_k=0.0,
    )
    result = detector.run(df=frame, features={})

    by_bucket_position = result.tables["collision_by_bucket_position"]
    assert not by_bucket_position.empty
    required = {
        "scope",
        "metric",
        "bucket_start",
        "bucket_minutes",
        "position_normalized",
        "n_bucket_position",
        "observed",
        "expected",
        "excess",
        "deviance",
        "deviance_ratio",
        "lambda_side",
        "shrink_k",
        "prior_level",
        "is_low_power",
        "inference_status",
    }
    assert required.issubset(by_bucket_position.columns)
    pro_row = by_bucket_position[by_bucket_position["position_normalized"] == "Pro"].iloc[0]
    con_row = by_bucket_position[by_bucket_position["position_normalized"] == "Con"].iloc[0]
    assert pro_row["n_bucket_position"] == 2
    assert pro_row["observed"] == pytest.approx(2.0)
    assert pro_row["expected"] == pytest.approx(2.0)
    assert pro_row["deviance"] == pytest.approx(0.0)
    assert pro_row["lambda_side"] == pytest.approx(1.0)
    assert con_row["n_bucket_position"] == 1
    assert con_row["observed"] == pytest.approx(0.0)
    assert con_row["expected"] == pytest.approx(0.0)


def test_collision_by_bucket_position_uses_contextual_shrink_k(tmp_path: Path) -> None:
    contextual_path = tmp_path / "contextual_baseline.csv"
    contextual_path.write_text(
        "level,committee,chamber,hour_bin,weekday_bin,bucket_minutes,n_windows,n_rows_total,duplicate_row_rate_mean,duplicate_row_rate_median,median_n_rows,shrink_k\n"
        "bucket,,,-1,-1,5,10,1000,0.10,0.10,50,12\n",
        encoding="utf-8",
    )
    frame = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "canonical_name": ["DOE|JANE", "DOE|JANE", "SMITH|JOHN"],
            "position_normalized": ["Pro", "Pro", "Con"],
            "timestamp": pd.to_datetime(
                ["2026-02-01 00:00:00", "2026-02-01 00:01:00", "2026-02-01 00:02:00"]
            ),
            "minute_bucket": pd.to_datetime(
                ["2026-02-01 00:00:00", "2026-02-01 00:01:00", "2026-02-01 00:02:00"]
            ),
            "name_display": ["DOE, JANE", "DOE, JANE", "SMITH, JOHN"],
        }
    )
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_uncertainty_mode="analytic_only",
        position_hearing_baseline_enabled=True,
        position_baseline_shrink_k=1.0,
        contextual_baseline_path=str(contextual_path),
    )
    result = detector.run(df=frame, features={})
    by_bucket_position = result.tables["collision_by_bucket_position"]
    pro_row = by_bucket_position[by_bucket_position["position_normalized"] == "Pro"].iloc[0]
    assert pro_row["shrink_k"] == pytest.approx(12.0)
    assert pro_row["prior_level"] == "bucket"


def test_collision_by_bucket_position_uses_scope_level_position_priors() -> None:
    rows: list[dict[str, object]] = []
    row_id = 1
    first_bucket = pd.Timestamp("2026-02-01 00:00:00")

    for index in range(40):
        timestamp = first_bucket + pd.Timedelta(minutes=index)
        rows.append(
            {
                "id": row_id,
                "canonical_name": f"TARGET_CON_{index:03d}|NAME",
                "position_normalized": "Con",
                "timestamp": timestamp,
                "minute_bucket": timestamp.floor("min"),
                "name_display": f"TARGET CON {index:03d}, NAME",
            }
        )
        row_id += 1

    for index in range(40):
        timestamp = first_bucket + pd.Timedelta(minutes=index)
        rows.append(
            {
                "id": row_id,
                "canonical_name": f"TARGET_PRO_{index:03d}|NAME",
                "position_normalized": "Pro",
                "timestamp": timestamp,
                "minute_bucket": timestamp.floor("min"),
                "name_display": f"TARGET PRO {index:03d}, NAME",
            }
        )
        row_id += 1

    background_start = pd.Timestamp("2026-02-01 01:00:00")
    for index in range(400):
        timestamp = background_start + pd.Timedelta(minutes=index)
        rows.append(
            {
                "id": row_id,
                "canonical_name": f"BACKGROUND_CON_{index:03d}|NAME",
                "position_normalized": "Con",
                "timestamp": timestamp,
                "minute_bucket": timestamp.floor("min"),
                "name_display": f"BACKGROUND CON {index:03d}, NAME",
            }
        )
        row_id += 1

    frame = pd.DataFrame(rows)
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[60],
        collision_uncertainty_mode="analytic_only",
        position_hearing_baseline_enabled=True,
        position_baseline_shrink_k=30.0,
    )
    result = detector.run(df=frame, features={})
    by_bucket_position = result.tables["collision_by_bucket_position"]
    focus_row = by_bucket_position[
        (pd.to_datetime(by_bucket_position["bucket_start"], errors="coerce") == first_bucket)
        & (by_bucket_position["position_normalized"].astype(str) == "Con")
    ].iloc[0]

    assert int(focus_row["n_bucket_position"]) == 40
    assert int(focus_row["n_unique_names"]) == 40
    assert float(focus_row["observed"]) == pytest.approx(0.0)
    assert float(focus_row["expected"]) < 8.0


def _build_position_interval_frame(
    *,
    n_rows_per_position: int,
    include_unknown: bool = False,
) -> pd.DataFrame:
    positions = ["Pro", "Con"] + (["Unknown"] if include_unknown else [])
    rows: list[dict[str, object]] = []
    base = pd.Timestamp("2026-02-01 00:00:00")
    row_id = 1
    minute_offset = 0
    for position in positions:
        for index in range(int(n_rows_per_position)):
            name_id = int(index % 8)
            canonical_name = f"NAME{name_id:02d}|TEST"
            timestamp = base + pd.Timedelta(minutes=minute_offset)
            rows.append(
                {
                    "id": row_id,
                    "canonical_name": canonical_name,
                    "name_display": canonical_name.replace("|", ", "),
                    "position_normalized": position,
                    "timestamp": timestamp,
                    "minute_bucket": timestamp.floor("min"),
                }
            )
            row_id += 1
            minute_offset += 1
    return pd.DataFrame(rows)


def test_position_duplicate_metrics_emit_interval_contract_and_are_order_stable() -> None:
    frame = _build_position_interval_frame(n_rows_per_position=40)
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_uncertainty_mode="analytic_only",
        collision_baseline_model="multinomial",
        position_interval_draws=500,
        position_claim_min_rows_per_position=20,
        low_power_min_unique_names=1,
        low_power_min_expected_duplicates=0.0,
        random_seed=17,
    )

    result = detector.run(df=frame, features={})
    shuffled = frame.sample(frac=1.0, random_state=9).reset_index(drop=True)
    shuffled_result = detector.run(df=shuffled, features={})

    metrics = result.tables["position_duplicate_metrics"].sort_values("position_normalized").reset_index(drop=True)
    shuffled_metrics = (
        shuffled_result.tables["position_duplicate_metrics"]
        .sort_values("position_normalized")
        .reset_index(drop=True)
    )

    required = {
        "expected_duplicate_rows_p05",
        "expected_duplicate_rows_p50",
        "expected_duplicate_rows_p95",
        "expected_duplicate_row_rate_p05",
        "expected_duplicate_row_rate_p50",
        "expected_duplicate_row_rate_p95",
        "interval_method_id",
        "interval_draws_effective",
    }
    assert required.issubset(metrics.columns)
    assert (metrics["interval_method_id"] == detector.POSITION_INTERVAL_METHOD_ID).all()
    assert (metrics["interval_draws_effective"].astype(int) > 0).all()
    assert np.isfinite(metrics["expected_duplicate_rows_p05"]).all()
    assert np.isfinite(metrics["expected_duplicate_rows_p50"]).all()
    assert np.isfinite(metrics["expected_duplicate_rows_p95"]).all()
    assert np.isfinite(metrics["expected_duplicate_row_rate_p05"]).all()
    assert np.isfinite(metrics["expected_duplicate_row_rate_p50"]).all()
    assert np.isfinite(metrics["expected_duplicate_row_rate_p95"]).all()

    assert (metrics["expected_duplicate_rows_p05"] <= metrics["expected_duplicate_rows_p50"]).all()
    assert (metrics["expected_duplicate_rows_p50"] <= metrics["expected_duplicate_rows_p95"]).all()
    assert (metrics["expected_duplicate_row_rate_p05"] <= metrics["expected_duplicate_row_rate_p50"]).all()
    assert (metrics["expected_duplicate_row_rate_p50"] <= metrics["expected_duplicate_row_rate_p95"]).all()

    pd.testing.assert_frame_equal(
        metrics[
            [
                "position_normalized",
                "expected_duplicate_rows_p05",
                "expected_duplicate_rows_p50",
                "expected_duplicate_rows_p95",
                "expected_duplicate_row_rate_p05",
                "expected_duplicate_row_rate_p50",
                "expected_duplicate_row_rate_p95",
                "interval_draws_effective",
            ]
        ],
        shuffled_metrics[
            [
                "position_normalized",
                "expected_duplicate_rows_p05",
                "expected_duplicate_rows_p50",
                "expected_duplicate_rows_p95",
                "expected_duplicate_row_rate_p05",
                "expected_duplicate_row_rate_p50",
                "expected_duplicate_row_rate_p95",
                "interval_draws_effective",
            ]
        ],
        check_exact=True,
    )

    assert bool(result.summary["position_claim_eligible"]) is True
    assert str(result.summary["position_claim_reason"]) == detector.POSITION_CLAIM_REASON_ELIGIBLE
    assert float(result.summary["position_interval_nominal"]) == pytest.approx(
        detector.position_interval_nominal
    )
    assert (
        str(result.summary["position_interval_method_id"]) == detector.POSITION_INTERVAL_METHOD_ID
    )


def test_position_permutation_test_uses_two_sided_absolute_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    detector = DuplicatesExactDetector(
        top_n=10,
        bucket_minutes=[5],
        position_permutation_draws=3,
        position_cluster_bootstrap_draws=200,
    )
    working = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "canonical_name": ["A|X", "A|X", "C|X", "D|X"],
            "position_normalized": ["Pro", "Pro", "Con", "Con"],
        }
    )

    class _FakePermutationRng:
        def __init__(self, permutations: list[list[int]]) -> None:
            self._permutations = [np.asarray(values, dtype=np.int64) for values in permutations]
            self._cursor = 0

        def permutation(self, _n: int) -> np.ndarray:
            values = self._permutations[self._cursor]
            self._cursor += 1
            return values

    monkeypatch.setattr(
        detector,
        "_cluster_bootstrap_rate_difference",
        lambda **_kwargs: (1.0, 0.7, 1.0, 123),
    )
    rng = _FakePermutationRng(
        permutations=[
            [0, 1, 2, 3],  # +1.0
            [0, 2, 1, 3],  # 0.0
            [2, 3, 0, 1],  # -1.0
        ]
    )

    position_tests = detector._position_permutation_test(
        working,
        "canonical_name",
        rng=rng,  # type: ignore[arg-type]
        n_permutations=3,
    )
    assert not position_tests.empty
    row = position_tests.iloc[0]

    assert "permutation_p_value_one_sided" not in position_tests.columns
    assert float(row["permutation_p_value_two_sided"]) == pytest.approx(0.75)
    assert str(row["permutation_test_sidedness"]) == "two_sided_abs_effect"
    assert str(row["permutation_test_id"]) == detector.POSITION_PERMUTATION_TEST_ID
    assert str(row["rate_difference_interval_method"]) == detector.POSITION_RATE_DIFF_INTERVAL_METHOD_ID
    assert int(row["rate_difference_interval_draws"]) == 123


def test_cluster_bootstrap_rate_difference_is_finite_and_respects_draw_limit() -> None:
    detector = DuplicatesExactDetector(
        top_n=10,
        bucket_minutes=[5],
        position_cluster_bootstrap_draws=250,
        random_seed=31,
    )
    observed, ci_low, ci_high, draws_effective = detector._cluster_bootstrap_rate_difference(
        pro_counts_observed=np.asarray([3, 1, 0, 0], dtype=np.int64),
        con_counts_observed=np.asarray([0, 0, 3, 1], dtype=np.int64),
        rng=np.random.default_rng(31),
        n_bootstrap_draws=120,
    )
    assert np.isfinite(observed)
    assert np.isfinite(ci_low)
    assert np.isfinite(ci_high)
    assert ci_low <= ci_high
    assert 0 < draws_effective <= 120


def test_position_claim_is_gated_when_position_support_is_insufficient() -> None:
    frame = _build_position_interval_frame(n_rows_per_position=12, include_unknown=True)
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_baseline_model="multinomial",
        collision_uncertainty_mode="analytic_only",
        position_interval_draws=400,
        position_claim_min_rows_per_position=25,
        low_power_min_unique_names=1,
        low_power_min_expected_duplicates=0.0,
        random_seed=22,
    )
    result = detector.run(df=frame, features={})
    metrics = result.tables["position_duplicate_metrics"]

    assert bool(result.summary["position_claim_eligible"]) is False
    assert (
        str(result.summary["position_claim_reason"])
        == detector.POSITION_CLAIM_REASON_INSUFFICIENT_SUPPORT
    )
    assert (metrics["is_low_power"].astype(bool)).all()
    assert set(metrics["inference_status"].astype(str)) == {"descriptive_only"}


def test_position_claim_is_gated_for_unsupported_baseline_model() -> None:
    frame = _build_position_interval_frame(n_rows_per_position=40)
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_baseline_model="hypergeometric",
        collision_uncertainty_mode="analytic_only",
        position_interval_draws=400,
        position_claim_min_rows_per_position=10,
        low_power_min_unique_names=1,
        low_power_min_expected_duplicates=0.0,
        random_seed=23,
    )
    result = detector.run(df=frame, features={})
    metrics = result.tables["position_duplicate_metrics"]

    assert bool(result.summary["position_claim_eligible"]) is False
    assert (
        str(result.summary["position_claim_reason"])
        == detector.POSITION_CLAIM_REASON_UNSUPPORTED_MODEL
    )
    assert (metrics["interval_draws_effective"].astype(int) == 0).all()
    assert set(metrics["inference_status"].astype(str)) == {"descriptive_only"}


def test_collision_null_simulation_cache_reuses_histogram_draws(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_uncertainty_mode="monte_carlo",
        monte_carlo_draws=500,
    )
    calls = {"count": 0}

    def _fake_simulation(**_kwargs) -> pd.DataFrame:
        calls["count"] += 1
        return pd.DataFrame(
            {
                "pairs": [1.0, 2.0],
                "excess_rows": [0.0, 1.0],
                "repeated_group_rows": [2.0, 3.0],
            }
        )

    monkeypatch.setattr(
        duplicates_exact_module,
        "simulate_collision_null_from_histogram",
        _fake_simulation,
    )

    histogram = pd.DataFrame(
        {
            "name_count": [1, 2],
            "n_keys": [8, 3],
            "N": [14, 14],
        }
    )
    rng = np.random.default_rng(9)
    cache: dict[tuple[int, str, int, int, str, str], pd.DataFrame] = {}
    digest_cache: dict[int, str] = {}

    first = detector._simulate_collision_null_cached(
        n_rows=120,
        histogram=histogram,
        draws=500,
        rng=rng,
        baseline_model="multinomial",
        n_population=14,
        max_draws=250,
        tail_observed=None,
        cache=cache,
        histogram_digest_cache=digest_cache,
    )
    second = detector._simulate_collision_null_cached(
        n_rows=120,
        histogram=histogram,
        draws=500,
        rng=rng,
        baseline_model="multinomial",
        n_population=14,
        max_draws=250,
        tail_observed=None,
        cache=cache,
        histogram_digest_cache=digest_cache,
    )

    assert calls["count"] == 1
    pd.testing.assert_frame_equal(first, second, check_exact=True)


def test_contextual_shrink_lookup_is_memoized_by_bucket_hour_weekday(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "canonical_name": ["DOE|JANE", "DOE|JANE", "SMITH|JOHN"],
            "position_normalized": ["Pro", "Con", "Pro"],
            "timestamp": pd.to_datetime(
                ["2026-02-01 00:00:00", "2026-02-01 00:01:00", "2026-02-01 00:02:00"]
            ),
            "minute_bucket": pd.to_datetime(
                ["2026-02-01 00:00:00", "2026-02-01 00:01:00", "2026-02-01 00:02:00"]
            ),
            "name_display": ["DOE, JANE", "DOE, JANE", "SMITH, JOHN"],
        }
    )
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_uncertainty_mode="analytic_only",
        position_hearing_baseline_enabled=True,
        position_baseline_shrink_k=10.0,
    )

    calls = {"count": 0}
    original = detector._resolve_contextual_shrink_k

    def _wrapped_resolve(*args, **kwargs):
        calls["count"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(detector, "_resolve_contextual_shrink_k", _wrapped_resolve)
    detector.run(df=frame, features={})

    # Same (bucket_minutes, hour_bin, weekday_bin) should resolve once then hit cache.
    assert calls["count"] == 1


def test_hypothesis_family_metadata_gates_downstream_when_family_a_not_significant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = _build_submission_frame({"DOE|JANE": 4, "SMITH|JOHN": 4, "BROWN|AVA": 4})

    monkeypatch.setattr(
        duplicates_exact_module,
        "fetch_voter_name_key_count_histogram",
        lambda **kwargs: pd.DataFrame(
            {
                "name_count": [1],
                "n_keys": [50_000],
                "N": [50_000],
            }
        ),
    )

    def _high_null(**kwargs) -> pd.DataFrame:
        draws = max(10, int(kwargs.get("max_draws", kwargs.get("draws", 50))))
        return pd.DataFrame(
            {
                "pairs": np.full(draws, 10_000.0),
                "excess_rows": np.full(draws, 10_000.0),
                "repeated_group_rows": np.full(draws, 10_000.0),
            }
        )

    monkeypatch.setattr(
        duplicates_exact_module,
        "simulate_collision_null_from_histogram",
        _high_null,
    )

    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_baseline_source="vrdb_full_histogram",
        collision_uncertainty_mode="monte_carlo",
        voter_db_url="postgresql://example",
        low_power_min_unique_names=1,
        low_power_min_expected_duplicates=0.0,
        monte_carlo_draws=400,
    )
    result = detector.run(df=frame, features={})
    primary_scope = detector.collision_scope_primary

    methods = result.tables["collision_methods"]
    primary_method = methods[methods["scope"] == primary_scope].reset_index(drop=True)
    assert not primary_method.empty
    assert str(primary_method.loc[0, "family_id"]) == detector.FAMILY_ID_SCOPE
    assert str(primary_method.loc[0, "adjustment_method"]) == detector.ADJUSTMENT_METHOD_HOLM
    assert int(primary_method.loc[0, "n_tests"]) >= 1
    assert bool(primary_method.loc[0, "is_significant"]) is False

    per_name = result.tables["per_name_tests"]
    primary_per_name = per_name[per_name["scope"] == primary_scope].copy()
    assert not primary_per_name.empty
    assert set(primary_per_name["family_id"].astype(str)) == {detector.FAMILY_ID_PER_NAME}
    assert not primary_per_name["eligible_by_gate"].astype(bool).any()
    assert set(primary_per_name["gate_reason"].astype(str)) == {
        detector.GATE_REASON_FAMILY_A_NOT_SIGNIFICANT
    }
    assert primary_per_name["p_value"].isna().all()
    assert primary_per_name["q_value"].isna().all()

    buckets = result.tables["collision_by_bucket"]
    primary_bucket = buckets[
        (buckets["scope"] == primary_scope)
        & (buckets["metric"].astype(str) == detector.PRIMARY_SCOPE_ENDPOINT_METRIC)
    ].copy()
    assert not primary_bucket.empty
    assert set(primary_bucket["family_id"].astype(str)) == {detector.FAMILY_ID_BUCKET}
    assert not primary_bucket["eligible_by_gate"].astype(bool).any()
    assert primary_bucket["p_value"].isna().all()

    temporal = result.tables["temporal_burst_signals"]
    assert not temporal.empty
    assert set(temporal["family_id"].astype(str)) == {detector.FAMILY_ID_TEMPORAL}
    assert not temporal["eligible_by_gate"].astype(bool).any()
    assert temporal["temporal_p_value_min_gap"].isna().all()
    assert temporal["temporal_q_value_min_gap"].isna().all()

    family_table = result.tables["hypothesis_families"]
    assert not family_table.empty
    assert {
        detector.FAMILY_ID_SCOPE,
        detector.FAMILY_ID_BUCKET,
        detector.FAMILY_ID_PER_NAME,
        detector.FAMILY_ID_TEMPORAL,
    }.issubset(set(family_table["family_id"].astype(str)))


def test_hypothesis_family_metadata_opens_downstream_when_family_a_significant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = _build_submission_frame({"DOE|JANE": 4, "SMITH|JOHN": 4, "BROWN|AVA": 4})

    monkeypatch.setattr(
        duplicates_exact_module,
        "fetch_voter_name_key_count_histogram",
        lambda **kwargs: pd.DataFrame(
            {
                "name_count": [1],
                "n_keys": [50_000],
                "N": [50_000],
            }
        ),
    )

    def _low_null(**kwargs) -> pd.DataFrame:
        draws = max(10, int(kwargs.get("max_draws", kwargs.get("draws", 50))))
        return pd.DataFrame(
            {
                "pairs": np.zeros(draws, dtype=float),
                "excess_rows": np.zeros(draws, dtype=float),
                "repeated_group_rows": np.zeros(draws, dtype=float),
            }
        )

    monkeypatch.setattr(
        duplicates_exact_module,
        "simulate_collision_null_from_histogram",
        _low_null,
    )
    monkeypatch.setattr(
        duplicates_exact_module,
        "binomial_tail_p_value",
        lambda **kwargs: 0.0,
    )

    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_baseline_source="vrdb_full_histogram",
        collision_uncertainty_mode="monte_carlo",
        voter_db_url="postgresql://example",
        low_power_min_unique_names=1,
        low_power_min_expected_duplicates=0.0,
        monte_carlo_draws=400,
    )
    result = detector.run(df=frame, features={})
    primary_scope = detector.collision_scope_primary

    methods = result.tables["collision_methods"]
    primary_method = methods[methods["scope"] == primary_scope].reset_index(drop=True)
    assert not primary_method.empty
    assert bool(primary_method.loc[0, "is_significant"]) is True

    per_name = result.tables["per_name_tests"]
    primary_per_name = per_name[per_name["scope"] == primary_scope].copy()
    assert not primary_per_name.empty
    assert primary_per_name["eligible_by_gate"].astype(bool).all()
    assert primary_per_name["tested"].astype(bool).all()
    assert primary_per_name["q_value"].notna().all()

    primary_bucket = result.tables["collision_by_bucket"]
    primary_bucket = primary_bucket[
        (primary_bucket["scope"] == primary_scope)
        & (primary_bucket["metric"].astype(str) == detector.PRIMARY_SCOPE_ENDPOINT_METRIC)
    ].copy()
    assert not primary_bucket.empty
    assert primary_bucket["eligible_by_gate"].astype(bool).all()
    assert primary_bucket["adjusted_p_value"].notna().all()

    temporal = result.tables["temporal_burst_signals"]
    assert not temporal.empty
    assert temporal["eligible_by_gate"].astype(bool).all()
    assert temporal["temporal_q_value_min_gap"].notna().all()

    family_table = result.tables["hypothesis_families"]
    scoped_scope = family_table[
        (family_table["scope"].astype(str) == primary_scope)
        & (family_table["family_id"].astype(str) == detector.FAMILY_ID_SCOPE)
    ].reset_index(drop=True)
    assert not scoped_scope.empty
    assert int(scoped_scope.loc[0, "n_significant"]) == 1


def test_temporal_metrics_only_compute_inferential_p_values_for_name_gate() -> None:
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        temporal_permutation_draws=250,
        temporal_null_mode="hearing_intensity",
        random_seed=5,
    )
    frame = _build_submission_frame({"ALPHA|A": 3, "BETA|B": 3})
    temporal = detector._temporal_metrics_by_name(
        frame,
        "canonical_name",
        rng=np.random.default_rng(5),
        inferential_name_gate={"ALPHA|A"},
    ).sort_values("canonical_name").reset_index(drop=True)

    assert not temporal.empty
    alpha_row = temporal[temporal["canonical_name"].astype(str) == "ALPHA|A"].iloc[0]
    beta_row = temporal[temporal["canonical_name"].astype(str) == "BETA|B"].iloc[0]

    assert bool(alpha_row["temporal_inferential_name_gate_passed"]) is True
    assert bool(alpha_row["temporal_null_supported"]) is True
    assert np.isfinite(float(alpha_row["temporal_p_value_min_gap"]))
    assert int(alpha_row["temporal_permutation_draws"]) > 0

    assert bool(beta_row["temporal_inferential_name_gate_passed"]) is False
    assert bool(beta_row["temporal_null_supported"]) is False
    assert str(beta_row["temporal_null_support_reason"]) == (
        detector.TEMPORAL_NULL_SUPPORT_REASON_NAME_NOT_GATED
    )
    assert pd.isna(beta_row["temporal_p_value_min_gap"])
    assert int(beta_row["temporal_permutation_draws"]) == 0


def test_temporal_conditioned_null_downgrades_when_position_pool_is_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pd.DataFrame(
        [
            {
                "id": 1,
                "canonical_name": "ALPHA|A",
                "name_display": "ALPHA, A",
                "position_normalized": "Pro",
                "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
                "minute_bucket": pd.Timestamp("2026-02-01 10:00:00"),
            },
            {
                "id": 2,
                "canonical_name": "ALPHA|A",
                "name_display": "ALPHA, A",
                "position_normalized": "Pro",
                "timestamp": pd.Timestamp("2026-02-01 10:02:00"),
                "minute_bucket": pd.Timestamp("2026-02-01 10:02:00"),
            },
            {
                "id": 3,
                "canonical_name": "BETA|B",
                "name_display": "BETA, B",
                "position_normalized": "Con",
                "timestamp": pd.Timestamp("2026-02-01 10:04:00"),
                "minute_bucket": pd.Timestamp("2026-02-01 10:04:00"),
            },
            {
                "id": 4,
                "canonical_name": "BETA|B",
                "name_display": "BETA, B",
                "position_normalized": "Con",
                "timestamp": pd.Timestamp("2026-02-01 10:06:00"),
                "minute_bucket": pd.Timestamp("2026-02-01 10:06:00"),
            },
        ]
    )

    monkeypatch.setattr(
        duplicates_exact_module,
        "fetch_voter_name_key_count_histogram",
        lambda **kwargs: pd.DataFrame(
            {
                "name_count": [1],
                "n_keys": [50_000],
                "N": [50_000],
            }
        ),
    )

    def _low_null(**kwargs) -> pd.DataFrame:
        draws = max(10, int(kwargs.get("max_draws", kwargs.get("draws", 50))))
        return pd.DataFrame(
            {
                "pairs": np.zeros(draws, dtype=float),
                "excess_rows": np.zeros(draws, dtype=float),
                "repeated_group_rows": np.zeros(draws, dtype=float),
            }
        )

    monkeypatch.setattr(
        duplicates_exact_module,
        "simulate_collision_null_from_histogram",
        _low_null,
    )
    monkeypatch.setattr(
        duplicates_exact_module,
        "binomial_tail_p_value",
        lambda **kwargs: 0.0,
    )

    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_baseline_source="vrdb_full_histogram",
        collision_uncertainty_mode="monte_carlo",
        voter_db_url="postgresql://example",
        low_power_min_unique_names=1,
        low_power_min_expected_duplicates=0.0,
        monte_carlo_draws=400,
        temporal_null_mode="hearing_intensity_by_position",
        temporal_permutation_draws=250,
    )
    result = detector.run(df=frame, features={})

    temporal = result.tables["temporal_burst_signals"]
    assert not temporal.empty
    assert set(temporal["temporal_null_model"].astype(str)) == {
        detector.TEMPORAL_NULL_MODE_HEARING_INTENSITY_BY_POSITION
    }

    unsupported = temporal[
        temporal["gate_reason"].astype(str) == detector.GATE_REASON_TEMPORAL_NULL_UNSUPPORTED
    ].copy()
    assert not unsupported.empty
    assert unsupported["temporal_null_supported"].astype(bool).eq(False).all()
    assert unsupported["eligible_by_gate"].astype(bool).eq(False).all()
    assert unsupported["inferential_status"].astype(str).eq("descriptive_only").all()
    assert unsupported["inferential_reason"].astype(str).eq(
        detector.INFERENTIAL_REASON_TEMPORAL_NULL_UNSUPPORTED
    ).all()
    assert unsupported["temporal_p_value_min_gap"].isna().all()
    assert unsupported["temporal_q_value_min_gap"].isna().all()


def test_rng_seed_lineage_is_exposed_in_summary_and_collision_methods() -> None:
    frame = _build_submission_frame({"DOE|JANE": 3, "SMITH|JOHN": 2, "BROWN|AVA": 1})
    detector = DuplicatesExactDetector(
        top_n=20,
        bucket_minutes=[5],
        collision_uncertainty_mode="analytic_only",
        random_seed=73,
    )
    result = detector.run(df=frame, features={})

    summary = result.summary
    assert int(summary["rng_root_seed"]) == 73
    seed_lineage = summary.get("rng_seed_lineage")
    assert isinstance(seed_lineage, dict)
    assert set(seed_lineage.get("streams", {}).keys()) == set(detector.RNG_STREAM_NAMES)

    methods = result.tables["collision_methods"]
    assert not methods.empty
    assert int(methods["rng_root_seed"].iloc[0]) == 73
    assert str(methods["rng_root_stream_id"].iloc[0]).strip() != ""
    for stream_name in detector.RNG_STREAM_NAMES:
        column = f"rng_stream_{stream_name}"
        assert column in methods.columns
        assert methods[column].astype(str).str.strip().str.len().gt(0).all()


def test_temporal_rng_consumption_does_not_perturb_bucket_collision_outputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = _build_submission_frame(
        {
            "ALPHA|A": 5,
            "BETA|B": 4,
            "GAMMA|C": 3,
            "DELTA|D": 2,
            "EPSILON|E": 2,
        }
    )
    monkeypatch.setattr(
        duplicates_exact_module,
        "fetch_voter_name_key_count_histogram",
        lambda **kwargs: pd.DataFrame(
            {
                "name_count": [1],
                "n_keys": [50_000],
                "N": [50_000],
            }
        ),
    )

    detector_kwargs = {
        "top_n": 20,
        "bucket_minutes": [5, 15],
        "collision_baseline_source": "vrdb_full_histogram",
        "collision_uncertainty_mode": "monte_carlo",
        "voter_db_url": "postgresql://example",
        "monte_carlo_draws": 300,
        "temporal_permutation_draws": 250,
        "low_power_min_unique_names": 1,
        "low_power_min_expected_duplicates": 0.0,
        "random_seed": 41,
    }

    baseline_detector = DuplicatesExactDetector(**detector_kwargs)
    baseline_result = baseline_detector.run(df=frame, features={})
    baseline_bucket = (
        baseline_result.tables["collision_by_bucket"][
            [
                "scope",
                "bucket_start",
                "bucket_minutes",
                "metric",
                "observed",
                "expected",
                "expected_p05",
                "expected_p95",
                "p_value",
                "z_score",
            ]
        ]
        .sort_values(["scope", "bucket_minutes", "bucket_start", "metric"])
        .reset_index(drop=True)
    )

    original_temporal_metrics = DuplicatesExactDetector._temporal_metrics_by_name

    def _temporal_with_extra_rng_draws(self, *args, **kwargs):
        rng = kwargs.get("rng")
        if rng is not None:
            rng.random(10_000)
        return original_temporal_metrics(self, *args, **kwargs)

    monkeypatch.setattr(
        DuplicatesExactDetector,
        "_temporal_metrics_by_name",
        _temporal_with_extra_rng_draws,
    )

    perturbed_detector = DuplicatesExactDetector(**detector_kwargs)
    perturbed_result = perturbed_detector.run(df=frame, features={})
    perturbed_bucket = (
        perturbed_result.tables["collision_by_bucket"][
            [
                "scope",
                "bucket_start",
                "bucket_minutes",
                "metric",
                "observed",
                "expected",
                "expected_p05",
                "expected_p95",
                "p_value",
                "z_score",
            ]
        ]
        .sort_values(["scope", "bucket_minutes", "bucket_start", "metric"])
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(baseline_bucket, perturbed_bucket, check_exact=True)
