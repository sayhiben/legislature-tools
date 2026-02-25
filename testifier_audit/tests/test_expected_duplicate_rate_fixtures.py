from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from rapidfuzz import fuzz

from testifier_audit.detectors.duplicates_exact import DuplicatesExactDetector
from testifier_audit.names.stat_tests import (
    benjamini_hochberg,
    binomial_tail_p_value,
    fisher_pairwise_rate_test,
    hypergeometric_tail_p_value,
)
from tests._methodology_assertions import (
    assert_frame_matches_records,
    assert_mapping_subset,
)
from tests._methodology_fixture_loader import load_fixture_json


def test_primitive_methodology_references_match_external_known_good_examples() -> None:
    fixture = load_fixture_json("primitive", "numerical_references.json")

    for case in fixture["rapidfuzz_ratio_cases"]:
        score = fuzz.ratio(case["left"], case["right"])
        assert score == pytest.approx(float(case["expected"]), abs=1e-12)

    bh_case = fixture["bh_case"]
    q_values = benjamini_hochberg(pd.Series(bh_case["p_values"], dtype=float))
    np.testing.assert_allclose(
        q_values.to_numpy(dtype=float),
        np.asarray(bh_case["expected"], dtype=float),
        atol=1e-8,
        rtol=0.0,
        equal_nan=True,
    )

    binomial_case = fixture["binomial_tail_case"]
    p_value = binomial_tail_p_value(
        observed_successes=int(binomial_case["observed_successes"]),
        total_trials=int(binomial_case["total_trials"]),
        success_probability=float(binomial_case["success_probability"]),
    )
    assert p_value == pytest.approx(float(binomial_case["expected"]), abs=1e-12)

    fisher_case = fixture["fisher_exact_case"]
    fisher_result = fisher_pairwise_rate_test(
        successes_left=int(fisher_case["successes_left"]),
        total_left=int(fisher_case["total_left"]),
        successes_right=int(fisher_case["successes_right"]),
        total_right=int(fisher_case["total_right"]),
    )
    assert fisher_result["p_value"] == pytest.approx(float(fisher_case["expected_p_value"]), abs=1e-12)

    hyper_case = fixture["hypergeometric_tail_case"]
    hyper_tail = hypergeometric_tail_p_value(
        observed_successes=int(hyper_case["observed_successes"]),
        population_size=int(hyper_case["population_size"]),
        population_successes=int(hyper_case["population_successes"]),
        sample_size=int(hyper_case["sample_size"]),
    )
    assert hyper_tail == pytest.approx(float(hyper_case["expected"]), abs=1e-12)


def test_duplicates_detector_fixture_asserts_expected_duplicate_rate_methodology() -> None:
    fixture = load_fixture_json("primitive", "duplicates_detector_case.json")

    frame = pd.DataFrame(fixture["records"])
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce", utc=True)
    frame["minute_bucket"] = pd.to_datetime(frame["minute_bucket"], errors="coerce", utc=True)

    assignments = pd.DataFrame(fixture["match_assignments"])

    cfg = fixture["detector_config"]
    detector = DuplicatesExactDetector(
        top_n=int(cfg["top_n"]),
        bucket_minutes=[int(value) for value in cfg["bucket_minutes"]],
        collision_uncertainty_mode=str(cfg["collision_uncertainty_mode"]),
        collision_scope_primary=str(cfg["collision_scope_primary"]),
        collision_scope_overlays=[str(value) for value in cfg["collision_scope_overlays"]],
        collision_baseline_source=str(cfg["collision_baseline_source"]),
        collision_baseline_model=str(cfg["collision_baseline_model"]),
        random_seed=int(cfg["random_seed"]),
    )

    result = detector.run(
        df=frame,
        features={"voter_registry_match.match_assignments": assignments},
    )

    assert_mapping_subset(
        actual=result.summary,
        expected_subset=fixture["expected_summary"],
        float_tolerance=1e-9,
    )

    assert_frame_matches_records(
        actual=result.tables["collision_overview"],
        expected_records=fixture["expected_collision_overview"],
        columns=("scope", "metric", "observed", "expected"),
        sort_by=("scope", "metric"),
        float_tolerance=1e-9,
    )

    timing = result.tables["top_name_timing_by_mode"]
    strict_names = sorted(
        set(timing[timing["match_mode"] == "strict"]["name_key"].astype(str).tolist())
    )
    loose_names = sorted(
        set(timing[timing["match_mode"] == "loose"]["name_key"].astype(str).tolist())
    )

    assert strict_names == sorted(fixture["expected_top_name_timing_modes"]["strict"])
    assert loose_names == sorted(fixture["expected_top_name_timing_modes"]["loose"])
