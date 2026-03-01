from __future__ import annotations

import pandas as pd

from testifier_audit.backtests.vrdb_collision_backtest import (
    BaselineScenario,
    attach_bucket_fields,
    filter_probability_rows,
    required_geo_targets,
    required_geo_target_keys,
    select_historical_case_families,
    slice_rows_for_case,
    split_case_ids,
    summarize_case_metrics,
    synthetic_case_frame,
)


def test_required_geo_targets_for_city_include_city_county_state() -> None:
    targets = required_geo_targets(requested_geo_level="city", requested_geo_value="KI|SEATTLE")
    assert ("city", "KI|SEATTLE") in targets
    assert ("county", "KI") in targets
    assert ("state", "WA") in targets


def test_required_geo_target_keys_union_for_scenarios() -> None:
    scenarios = (
        BaselineScenario(
            scenario_id="state",
            baseline_variant="all_registrants",
            requested_geo_level="state",
            requested_geo_value="WA",
        ),
        BaselineScenario(
            scenario_id="county",
            baseline_variant="all_registrants",
            requested_geo_level="county",
            requested_geo_value="KI",
        ),
    )
    keys = required_geo_target_keys(scenarios=scenarios)
    assert "state|WA" in keys
    assert "county|KI" in keys


def test_filter_probability_rows_filters_variant_key_type_and_geo() -> None:
    frame = pd.DataFrame(
        {
            "name_key": ["alpha", "beta", "gamma", "delta"],
            "name_key_type": ["full_name_key", "full_name_key", "full_name_key", "full_name_key"],
            "count": [100, 80, 70, 50],
            "probability": [0.4, 0.3, 0.2, 0.1],
            "denominator": [1000, 1000, 1000, 1000],
            "geo_level": ["state", "county", "city", "state"],
            "geo_value": ["WA", "KI", "KI|SEATTLE", "OR"],
            "baseline_variant": ["all_registrants", "all_registrants", "all_registrants", "active_only"],
            "vrdb_version": ["v1", "v1", "v1", "v1"],
            "normalization_version": ["norm_v1", "norm_v1", "norm_v1", "norm_v1"],
        }
    )

    filtered = filter_probability_rows(
        probability_rows=frame,
        baseline_variants={"all_registrants"},
        name_key_type="full_name_key",
        geo_target_keys={"state|WA", "county|KI", "city|KI|SEATTLE"},
    )

    assert set(filtered["name_key"].tolist()) == {"alpha", "beta", "gamma"}


def test_slice_rows_for_case_supports_proxy_name_column() -> None:
    frame = pd.DataFrame(
        {
            "full_name_key": ["a|b", "c|d"],
            "canonical_key_medium": ["x|y", "z|w"],
            "timestamp": pd.to_datetime(["2026-02-01T00:00:00Z", "2026-02-01T00:01:00Z"]),
        }
    )
    rows = slice_rows_for_case(
        case_id="case-1",
        frame=frame,
        bucket_minutes=[5],
        baseline_variant="all_registrants",
        requested_geo_level="state",
        requested_geo_value="WA",
        name_column="canonical_key_medium",
    )

    assert not rows.empty
    assert (rows["name_key"].isin({"x|y", "z|w"})).all()
    assert rows["slice_id"].str.startswith("case-1::").all()


def test_attach_bucket_fields_extracts_bucket_minutes() -> None:
    frame = pd.DataFrame(
        {
            "slice_id": ["case::full_hearing", "case::bucket_15m:2026-02-01T00:00:00+0000"],
        }
    )
    enriched = attach_bucket_fields(frame)
    assert enriched.loc[0, "bucket_minutes"] == 0
    assert enriched.loc[1, "bucket_minutes"] == 15
    assert pd.notna(enriched.loc[1, "bucket_start"])


def test_summarize_case_metrics_returns_expected_fields() -> None:
    metrics = pd.DataFrame(
        {
            "slice_id": [
                "case-1::full_hearing",
                "case-1::bucket_5m:2026-02-01T00:00:00+0000",
                "case-1::bucket_5m:2026-02-01T00:05:00+0000",
            ],
            "tail_prob_pairs": [0.001, 0.002, 0.200],
            "tail_prob_max_name": [0.003, 0.004, 0.500],
            "observed_pairs": [10.0, 5.0, 1.0],
            "expected_pairs_mean": [2.0, 2.5, 2.0],
            "observed_max_name_count": [4.0, 3.0, 2.0],
            "expected_max_name_count_mean": [2.0, 2.2, 1.8],
            "fallback_steps": [0, 1, 1],
            "inferential_status": ["inferential", "inferential", "descriptive_only"],
            "inferential_reason": ["reference_model_inference_available", "", "low_power_support"],
            "effective_geo_level": ["state", "state", "state"],
            "effective_geo_value": ["WA", "WA", "WA"],
            "vrdb_version": ["vrdb_v1", "vrdb_v1", "vrdb_v1"],
            "normalization_version": ["norm_v1", "norm_v1", "norm_v1"],
        }
    )

    summary = summarize_case_metrics(
        metrics_rows=metrics,
        case_id="case-1",
        family="historical_normal",
        scenario_id="state_wa",
        baseline_variant="all_registrants",
        requested_geo_level="state",
        requested_geo_value="WA",
        normalization_mode="default",
        tail_alpha=0.01,
        small_bucket_minutes=5,
    )

    assert summary["has_metrics"] is True
    assert summary["n_slices_total"] == 3
    assert summary["fallback_steps_max"] == 1
    assert summary["full_pairs_ratio"] == 5.0
    assert summary["small_bucket_alert_share"] == 0.5


def test_select_historical_case_families_respects_forced_suspect() -> None:
    case_stats = pd.DataFrame(
        {
            "case_id": [f"case-{idx}" for idx in range(8)],
            "n_rows": [500, 520, 510, 530, 540, 550, 560, 570],
            "duplicate_pairs_ratio": [0.001, 0.0015, 0.002, 0.0025, 0.003, 0.004, 0.005, 0.006],
        }
    )

    families = select_historical_case_families(
        case_stats=case_stats,
        normal_count=3,
        suspect_count=2,
        min_rows=250,
        force_suspect_case_ids=["case-1"],
    )

    suspect_ids = set(families[families["family"] == "historical_suspect"]["case_id"].tolist())
    normal_ids = set(families[families["family"] == "historical_normal"]["case_id"].tolist())
    assert "case-1" in suspect_ids
    assert len(suspect_ids) == 2
    assert len(normal_ids) == 3
    assert suspect_ids.isdisjoint(normal_ids)


def test_split_case_ids_deterministic() -> None:
    case_ids = [f"case-{idx}" for idx in range(6)]
    calibration_a, holdout_a = split_case_ids(case_ids=case_ids, seed=42, holdout_fraction=0.4)
    calibration_b, holdout_b = split_case_ids(case_ids=case_ids, seed=42, holdout_fraction=0.4)

    assert calibration_a == calibration_b
    assert holdout_a == holdout_b
    assert set(calibration_a).isdisjoint(set(holdout_a))
    assert set(calibration_a) | set(holdout_a) == set(case_ids)


def test_synthetic_case_frame_injection_increases_injected_name_count() -> None:
    probability_rows = pd.DataFrame(
        {
            "name_key": ["alpha", "beta", "gamma"],
            "probability": [0.40, 0.35, 0.25],
        }
    )

    no_injection = synthetic_case_frame(
        probability_rows=probability_rows,
        n_rows=200,
        case_seed=123,
        start_timestamp=pd.Timestamp("2026-02-01T00:00:00Z"),
        span_minutes=120,
        injection_name_key="gamma",
        injection_fraction=0.0,
    )
    injected = synthetic_case_frame(
        probability_rows=probability_rows,
        n_rows=200,
        case_seed=123,
        start_timestamp=pd.Timestamp("2026-02-01T00:00:00Z"),
        span_minutes=120,
        injection_name_key="gamma",
        injection_fraction=0.20,
        injection_burst_minutes=5,
    )

    no_injection_count = int((no_injection["full_name_key"] == "gamma").sum())
    injected_count = int((injected["full_name_key"] == "gamma").sum())

    assert injected_count > no_injection_count
    assert len(injected) == 200
    assert injected["timestamp"].notna().all()
