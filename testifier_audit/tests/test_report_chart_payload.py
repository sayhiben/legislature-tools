from __future__ import annotations

from typing import Any

import pandas as pd

from testifier_audit.io.hearing_metadata import parse_hearing_metadata
from testifier_audit.report.analysis_registry import (
    ANALYSES_TO_PERFORM,
    default_analysis_definitions,
)
from testifier_audit.report.render import _build_interactive_chart_payload_v2

EXPECTED_ANALYSES = {
    str(entry.get("id") or "")
    for entry in default_analysis_definitions()
    if str(entry.get("id") or "")
}
EXPECTED_BASELINE_BUCKETS = [1, 5, 15, 30, 60, 120, 240]
EXPECTED_FOCUS_ANALYSIS_IDS = [
    analysis_id
    for analysis_id in ANALYSES_TO_PERFORM
    if str(analysis_id or "").strip() in EXPECTED_ANALYSES
]
EXPECTED_VISIBLE_ANALYSES = (
    set(EXPECTED_FOCUS_ANALYSIS_IDS) if EXPECTED_FOCUS_ANALYSIS_IDS else EXPECTED_ANALYSES
)
IS_OFF_HOURS_ONLY_VIEW = EXPECTED_FOCUS_ANALYSIS_IDS == ["off_hours"]


def _walk_scalars(value: Any) -> list[Any]:
    if isinstance(value, dict):
        items: list[Any] = []
        for nested in value.values():
            items.extend(_walk_scalars(nested))
        return items
    if isinstance(value, list):
        items: list[Any] = []
        for nested in value:
            items.extend(_walk_scalars(nested))
        return items
    return [value]


def test_payload_contract_exposes_catalog_controls_and_chart_ids() -> None:
    table_map = {
        "artifacts.counts_per_minute": pd.DataFrame(
            {
                "minute_bucket": pd.to_datetime(
                    [
                        "2026-02-01T00:00:00Z",
                        "2026-02-01T00:01:00Z",
                        "2026-02-01T00:02:00Z",
                    ]
                ),
                "n_total": [10, 12, 9],
                "n_pro": [4, 6, 3],
                "n_con": [6, 6, 6],
                "pro_rate": [0.4, 0.5, 0.3333333333],
                "pro_rate_wilson_low": [0.2, 0.3, 0.15],
                "pro_rate_wilson_high": [0.6, 0.7, 0.55],
                "is_low_power": [False, False, True],
                "n_unique_names": [10, 11, 8],
                "unique_ratio": [1.0, 0.92, 0.88],
            }
        ),
        "procon_swings.time_bucket_profiles": pd.DataFrame(
            {
                "bucket_start": pd.to_datetime(
                    [
                        "2026-02-01T00:00:00Z",
                        "2026-02-01T00:30:00Z",
                    ]
                ),
                "bucket_minutes": [30, 30],
                "n_total": [22, 19],
                "pro_rate": [0.45, 0.37],
                "pro_rate_wilson_low": [0.25, 0.2],
                "pro_rate_wilson_high": [0.65, 0.55],
                "baseline_pro_rate": [0.4, 0.4],
                "stable_lower": [0.3, 0.3],
                "stable_upper": [0.5, 0.5],
                "is_flagged": [False, True],
                "is_low_power": [False, False],
            }
        ),
        "voter_registry_match.match_by_bucket": pd.DataFrame(
            {
                "bucket_start": pd.to_datetime(
                    [
                        "2026-02-01T00:00:00Z",
                        "2026-02-01T00:30:00Z",
                    ]
                ),
                "bucket_minutes": [30, 30],
                "n_total": [22, 19],
                "n_matched_unique": [17, 13],
                "n_matched_ambiguous": [2, 2],
                "n_unmatched": [3, 4],
                "matched_rate": [0.86, 0.79],
                "unmatched_rate": [0.14, 0.21],
                "matched_rate_wilson_low": [0.67, 0.59],
                "matched_rate_wilson_high": [0.95, 0.90],
                "unmatched_rate_wilson_low": [0.05, 0.10],
                "unmatched_rate_wilson_high": [0.33, 0.41],
                "is_low_power": [False, False],
                "n_pro": [10, 8],
                "n_con": [12, 11],
            }
        ),
        "voter_registry_match.linkage_by_position_rows": pd.DataFrame(
            {
                "position_normalized": ["Con", "Pro"],
                "n_total": [21, 20],
                "n_matched_unique": [15, 15],
                "n_matched_ambiguous": [2, 2],
                "n_unmatched": [4, 3],
                "matched_rate": [0.81, 0.85],
                "unmatched_rate": [0.19, 0.15],
                "matched_rate_wilson_low": [0.62, 0.65],
                "matched_rate_wilson_high": [0.92, 0.95],
                "unmatched_rate_wilson_low": [0.08, 0.05],
                "unmatched_rate_wilson_high": [0.38, 0.35],
                "is_low_power": [False, False],
            }
        ),
        "voter_registry_match.linkage_by_position_unique": pd.DataFrame(
            {
                "position_normalized": ["Con", "Pro"],
                "n_total": [18, 16],
                "n_matched_unique": [12, 12],
                "n_matched_ambiguous": [1, 1],
                "n_unmatched": [5, 3],
                "matched_rate": [0.72, 0.81],
                "unmatched_rate": [0.28, 0.19],
                "matched_rate_wilson_low": [0.49, 0.57],
                "matched_rate_wilson_high": [0.88, 0.93],
                "unmatched_rate_wilson_low": [0.12, 0.07],
                "unmatched_rate_wilson_high": [0.51, 0.43],
                "is_low_power": [False, False],
            }
        ),
        "voter_registry_match.position_pairwise_tests": pd.DataFrame(
            {
                "unit": ["rows", "unique_names"],
                "position_left": ["Pro", "Pro"],
                "position_right": ["Con", "Con"],
                "left_n_total": [20, 16],
                "left_n_unmatched": [3, 3],
                "left_unmatched_rate": [0.15, 0.19],
                "right_n_total": [21, 18],
                "right_n_unmatched": [4, 5],
                "right_unmatched_rate": [0.19, 0.28],
                "rate_difference": [-0.04, -0.09],
                "odds_ratio": [0.75, 0.62],
                "p_value": [0.62, 0.41],
                "alpha": [0.05, 0.05],
                "is_significant": [False, False],
                "inference_status": ["tested", "tested"],
            }
        ),
        "voter_registry_match.sensitivity_modes": pd.DataFrame(
            {
                "mode": ["balanced", "broad", "conservative"],
                "n_rows": [41, 41, 41],
                "n_unmatched_rows": [6, 4, 7],
                "unmatched_rate_rows": [0.15, 0.10, 0.17],
                "n_unique_names": [34, 34, 34],
                "n_unmatched_unique": [6, 4, 7],
                "unmatched_rate_unique": [0.18, 0.12, 0.21],
            }
        ),
        "voter_registry_match.unmatched_names": pd.DataFrame(
            {
                "canonical_name": ["SMITH|JOHN", "BROWN|AVA"],
                "n_rows": [4, 3],
                "n_pro": [2, 1],
                "n_con": [2, 2],
                "top_caveat": ["below_similarity_threshold", "no_last_name_candidates"],
            }
        ),
    }
    summaries = {
        "voter_registry_match": {
            "enabled": True,
            "active": True,
        }
    }

    payload = _build_interactive_chart_payload_v2(table_map=table_map, detector_summaries=summaries)

    assert payload["version"] == 4
    assert isinstance(payload["analysis_catalog"], list)
    assert isinstance(payload["charts"], dict)
    assert isinstance(payload["controls"], dict)
    assert isinstance(payload["chart_legend_docs"], dict)
    assert isinstance(payload["triage_views"], dict)
    assert isinstance(payload["triage_summary"], dict)
    assert isinstance(payload["data_quality_panel"], dict)
    assert isinstance(payload["hearing_context_panel"], dict)

    ids = {entry["id"] for entry in payload["analysis_catalog"]}
    assert ids == EXPECTED_VISIBLE_ANALYSES

    for entry in payload["analysis_catalog"]:
        hero_chart_id = entry["hero_chart_id"]
        assert hero_chart_id in payload["charts"]
        assert isinstance(hero_chart_id, str) and hero_chart_id
        assert entry["bucket_options"] == EXPECTED_BASELINE_BUCKETS
        assert isinstance(entry["group"], str) and entry["group"]
        assert isinstance(entry["priority"], int)
        assert isinstance(entry["expected_metric_keys"], list)
        assert isinstance(entry["what_to_look_for_details"], list)
        assert entry["what_to_look_for_details"]
        assert hero_chart_id in payload["chart_legend_docs"]
        for detail_chart_id in entry["detail_chart_ids"]:
            assert detail_chart_id in payload["charts"]
            assert detail_chart_id in payload["chart_legend_docs"]

    voter_rates = payload["charts"]["voter_registry_match_rates"]
    assert voter_rates
    assert "matched_rate" in voter_rates[0]
    assert "unmatched_rate" in voter_rates[0]
    assert "n_matched_unique" in voter_rates[0]
    assert "matched_rate_pro" in voter_rates[0]
    assert "matched_rate_con" in voter_rates[0]
    assert "is_match_rate_alert_any" in voter_rates[0]
    voter_legend = payload["chart_legend_docs"]["voter_registry_match_rates"]
    voter_legend_labels = {
        str(item.get("label", ""))
        for item in voter_legend.get("items", [])
        if isinstance(item, dict)
    }
    assert "Unmatched rate" not in voter_legend_labels
    assert "Pro match rate" in voter_legend_labels
    assert "Con match rate" in voter_legend_labels
    assert "voter_registry_sensitivity_modes" not in payload["charts"]
    linkage_rows_chart = payload["charts"]["voter_registry_linkage_by_position_rows"]
    linkage_unique_chart = payload["charts"]["voter_registry_linkage_by_position_unique"]
    assert linkage_rows_chart
    assert linkage_unique_chart
    assert list(linkage_rows_chart[0].keys()) == list(linkage_unique_chart[0].keys())
    assert "expected_match_rate_global" in linkage_rows_chart[0]
    assert "expected_unmatched_rate_global" in linkage_rows_chart[0]
    expected_match_rate = linkage_rows_chart[0]["expected_match_rate_global"]
    expected_unmatched_rate = linkage_rows_chart[0]["expected_unmatched_rate_global"]
    assert isinstance(expected_match_rate, (int, float))
    assert isinstance(expected_unmatched_rate, (int, float))
    assert 0.0 <= float(expected_match_rate) <= 1.0
    assert 0.0 <= float(expected_unmatched_rate) <= 1.0

    controls = payload["controls"]
    assert "global_bucket_options" in controls
    assert "zoom_sync_groups" in controls
    assert controls["timezone"] == "America/Los_Angeles"
    assert controls["timezone_label"] == "America/Los_Angeles"
    assert controls["process_markers"] == []
    assert isinstance(controls["evidence_taxonomy"], list)
    assert isinstance(controls["methodology"], dict)
    assert isinstance(controls["methodology"]["definitions"], list)
    assert isinstance(controls["methodology"]["tests_used"], list)
    guardrails = controls["methodology"]["ethical_guardrails"]
    assert isinstance(guardrails, list) and guardrails
    assert any(
        "statistical irregularity" in str(row.get("requirement", "")).lower()
        for row in guardrails
    )
    assert any(
        "standalone attribution" in str(row.get("requirement", "")).lower()
        for row in guardrails
    )
    assert controls["theme_options"] == [
        {"id": "light", "label": "Light"},
        {"id": "dark", "label": "Dark"},
    ]
    assert controls["default_theme"] == "light"
    assert "chart_theme_options" not in controls
    assert "default_chart_theme" not in controls
    assert isinstance(controls["color_semantics"], dict)
    assert controls["color_semantics"]["light"]["series"]["primary"] == "#0072B2"
    assert controls["color_semantics"]["dark"]["series"]["primary"] == "#5AB0FF"
    assert controls["color_semantics"]["light"]["alert"]["lower"] == "#D55E00"
    assert controls["color_semantics"]["dark"]["alert"]["upper"] == "#F2A7D4"
    assert controls["color_semantics"]["light"]["heatmap"]["residual_diverging"][0] == "#B13A00"
    assert controls["color_semantics"]["dark"]["heatmap"]["volume_seq"][-1] == "#94A3B8"
    assert controls["dedup_modes"] == ["raw", "exact_row_dedup", "side_by_side"]
    assert controls["default_dedup_mode"] in controls["dedup_modes"]
    assert isinstance(controls["duplicate_collision_scope_default"], str)
    assert isinstance(controls["duplicate_collision_metric_default"], str)
    assert isinstance(controls["duplicate_collision_scope_options"], list)
    assert isinstance(controls["duplicate_collision_metric_options"], list)
    assert isinstance(controls["duplicate_match_mode_default"], str)
    assert isinstance(controls["duplicate_match_mode_options"], list)
    assert isinstance(controls["voter_match_mode_default"], str)
    assert isinstance(controls["voter_match_mode_options"], list)
    assert "absolute_time" in controls["zoom_sync_groups"]
    assert isinstance(controls["zoom_sync_groups"]["absolute_time"], list)
    assert 30 in controls["global_bucket_options"]
    assert 240 in controls["global_bucket_options"]
    if IS_OFF_HOURS_ONLY_VIEW:
        assert controls.get("focus_mode") == "off_hours_only"
        assert controls.get("focus_analysis_ids") == ["off_hours"]
    elif EXPECTED_FOCUS_ANALYSIS_IDS:
        assert controls.get("focus_mode") == "analysis_subset"
        assert controls.get("focus_analysis_ids") == EXPECTED_FOCUS_ANALYSIS_IDS
    else:
        assert controls.get("focus_mode") in {None, "full_report"}
        assert controls.get("focus_analysis_ids") == []

    catalog_by_id = {entry["id"]: entry for entry in payload["analysis_catalog"]}
    target_analysis = (
        "baseline_profile"
        if "baseline_profile" in catalog_by_id
        else next(iter(catalog_by_id.keys()))
    )
    assert catalog_by_id[target_analysis]["bucket_options"] == EXPECTED_BASELINE_BUCKETS

    triage_views = payload["triage_views"]
    assert {"raw", "exact_row_dedup", "side_by_side"}.issubset(set(triage_views.keys()))
    assert payload["data_quality_panel"]["status"] in {"ok", "warning"}
    assert isinstance(payload["data_quality_panel"]["triage_raw_vs_dedup_metrics"], list)
    assert payload["hearing_context_panel"]["available"] is False
    cross_hearing = payload["cross_hearing_baseline"]
    assert isinstance(cross_hearing, dict)
    assert int(cross_hearing["schema_version"]) >= 2
    assert isinstance(cross_hearing["channels"], dict)
    assert {"cohort_loo", "global_loo"}.issubset(set(cross_hearing["channels"].keys()))
    assert isinstance(cross_hearing["analysis_metric_map"], dict)


def test_payload_color_semantics_cover_key_chart_families() -> None:
    payload = _build_interactive_chart_payload_v2(
        table_map={
            "off_hours.window_control_profile": pd.DataFrame(
                {
                    "bucket_start": [pd.Timestamp("2026-02-06T20:00:00Z")],
                    "bucket_minutes": [30],
                    "n_total": [120],
                    "n_known": [100],
                    "n_pro": [35],
                    "n_con": [65],
                    "pro_rate": [0.35],
                    "pro_rate_wilson_low": [0.28],
                    "pro_rate_wilson_high": [0.43],
                    "expected_pro_rate_global": [0.5],
                    "expected_pro_rate_day": [0.48],
                    "expected_pro_rate_primary": [0.49],
                    "control_low_95_primary": [0.39],
                    "control_high_95_primary": [0.59],
                    "control_low_998_primary": [0.34],
                    "control_high_998_primary": [0.64],
                    "control_low_95_global": [0.4],
                    "control_high_95_global": [0.6],
                    "control_low_998_global": [0.35],
                    "control_high_998_global": [0.65],
                    "z_score_day": [-2.2],
                    "z_score_primary": [-2.6],
                    "delta_pro_rate_primary": [-0.14],
                    "q_value_primary_lower": [0.003],
                    "q_value_primary_two_sided": [0.011],
                    "is_significant_primary_lower": [True],
                    "is_significant_primary_upper": [False],
                    "is_significant_primary_two_sided": [True],
                    "is_material_primary_lower_shift": [True],
                    "is_material_primary_upper_shift": [False],
                    "is_below_primary_control_998": [True],
                    "is_above_primary_control_998": [False],
                    "is_alert_off_hours_window": [True],
                    "is_primary_alert_window": [True],
                    "is_model_baseline_available": [True],
                    "primary_baseline_source": ["model_day_hour"],
                    "is_low_power": [False],
                    "is_off_hours_window": [True],
                    "is_pure_off_hours_window": [True],
                    "is_primary_spc_998_two_sided": [True],
                    "is_primary_fdr_two_sided": [True],
                }
            ),
            "off_hours.date_hour_primary_residual_distribution": pd.DataFrame(
                {
                    "bucket_minutes": [30],
                    "date": ["2026-02-06"],
                    "day_of_week": ["Friday"],
                    "hour": [12],
                    "z_score_primary": [-2.6],
                    "is_low_power": [False],
                    "is_primary_alert_window": [True],
                    "n_known": [100],
                    "n_total": [120],
                    "pro_rate": [0.35],
                    "expected_pro_rate_primary": [0.49],
                    "delta_pro_rate_primary": [-0.14],
                    "n_windows_alert_eligible": [1],
                    "n_windows_tested": [1],
                    "n_windows_low_power": [0],
                    "n_windows_primary_alert": [1],
                    "primary_alert_fraction_tested": [1.0],
                    "z_score_primary_median": [-2.6],
                    "z_score_primary_abs_max": [2.6],
                }
            ),
            "off_hours.model_fit_diagnostics": pd.DataFrame(
                {
                    "bucket_minutes": [30],
                    "model_fit_method": ["glm"],
                    "model_fit_rows": [48],
                    "model_fit_unique_days": [4],
                    "model_fit_unique_hours": [20],
                    "model_fit_converged": [1.0],
                    "model_fit_aic": [101.2],
                    "model_fit_used_harmonics": [3],
                    "model_fit_window_count": [10],
                    "model_fit_available_windows": [9],
                    "model_fit_available_fraction": [0.9],
                }
            ),
            "off_hours.off_hours_summary": pd.DataFrame(
                {
                    "off_hours": [120],
                    "on_hours": [740],
                    "off_hours_ratio": [0.1395],
                    "off_hours_pro_rate": [0.35],
                    "on_hours_pro_rate": [0.52],
                    "primary_bucket_minutes": [30],
                    "primary_baseline_method": ["model_day_hour"],
                    "off_hours_windows_alert_eligible": [1],
                    "off_hours_windows_alert_eligible_low_power": [0],
                    "off_hours_windows_primary_alert": [1],
                }
            ),
        },
        detector_summaries={},
    )

    semantics = payload["controls"]["color_semantics"]
    assert semantics["light"]["series"]["interval"] == "#8B99A8"
    assert semantics["dark"]["series"]["reference"] == "#94A3B8"
    assert semantics["light"]["state"]["low_power"] == "#E69F00"
    assert semantics["dark"]["state"]["outlier"] == "#7CC7FF"
    assert semantics["light"]["heatmap"]["rate_diverging"] == [
        "#2C7FB8",
        "#9ECAE1",
        "#F7F7F7",
        "#FDD49E",
        "#D95F0E",
    ]

    charts = payload["charts"]
    assert charts["off_hours_control_timeline"]
    assert charts["off_hours_funnel_plot"]
    assert charts["off_hours_date_hour_primary_residual_heatmap"]
    assert "off_hours_model_fit_diagnostics" not in charts
    assert charts["overview_position_volume_by_bucket"]

    timeline_row = charts["off_hours_control_timeline"][0]
    assert timeline_row["is_primary_alert_window"] is True
    assert timeline_row["is_primary_lower_alert_window"] is True
    assert "is_primary_upper_alert_window" in timeline_row
    assert "is_primary_two_sided_alert_window" in timeline_row
    assert timeline_row["is_material_primary_lower_shift"] is True
    assert "is_material_primary_upper_shift" in timeline_row
    assert "is_primary_spc_998_two_sided" in timeline_row
    assert "is_primary_fdr_two_sided" in timeline_row

    funnel_row = charts["off_hours_funnel_plot"][0]
    assert funnel_row["is_significant_primary_two_sided"] is True
    assert "is_significant_primary" in funnel_row
    assert "is_above_primary_control_998" in funnel_row

    heatmap_row = charts["off_hours_date_hour_primary_residual_heatmap"][0]
    assert heatmap_row["z_score_primary"] == -2.6
    assert "n_windows_primary_alert" in heatmap_row

    overview_volume_row = charts["overview_position_volume_by_bucket"][0]
    assert overview_volume_row["n_other_position"] == 20.0


def test_empty_and_disabled_analyses_are_still_in_catalog() -> None:
    payload = _build_interactive_chart_payload_v2(
        table_map={},
        detector_summaries={
            "voter_registry_match": {
                "enabled": False,
                "active": False,
                "reason": "disabled_in_config",
            }
        },
    )

    catalog = {entry["id"]: entry for entry in payload["analysis_catalog"]}
    assert set(catalog.keys()) == EXPECTED_VISIBLE_ANALYSES

    if IS_OFF_HOURS_ONLY_VIEW:
        assert payload["controls"].get("focus_mode") == "off_hours_only"
    else:
        voter_entry = catalog.get("voter_registry_match")
        if voter_entry is not None:
            assert voter_entry["status"] == "disabled"
            assert voter_entry["reason"]

        non_voter_empty = [
            entry for entry in payload["analysis_catalog"] if entry["id"] != "voter_registry_match"
        ]
        assert all(entry["status"] in {"empty", "ready"} for entry in non_voter_empty)


def test_payload_uses_collision_metric_tables_and_provenance_fields() -> None:
    payload = _build_interactive_chart_payload_v2(
        table_map={
            "artifacts.counts_per_minute": pd.DataFrame(
                {
                    "minute_bucket": pd.to_datetime(["2026-02-01T00:00:00Z"]),
                    "n_total": [5],
                    "n_pro": [2],
                    "n_con": [3],
                    "pro_rate": [0.4],
                    "pro_rate_wilson_low": [0.1],
                    "pro_rate_wilson_high": [0.8],
                    "is_low_power": [False],
                    "n_unique_names": [4],
                    "unique_ratio": [0.8],
                }
            ),
            "duplicates_exact.collision_methods": pd.DataFrame(
                [
                    {
                        "scope": "matched_only",
                        "baseline_source": "hearing_empirical",
                        "baseline_model": "multinomial",
                        "uncertainty_model": "monte_carlo",
                        "n_used": 100,
                        "N_used": 1000,
                        "metric_primary": "repeated_group_rows",
                        "metrics_reported": "repeated_group_rows,excess_rows,pairs",
                        "baseline_degraded": True,
                        "fallback_policy": "degrade",
                        "collision_key_mode": "strict",
                        "normalization_version_hash": "abc123",
                        "stratification": "none",
                        "censored": False,
                    }
                ]
            ),
            "duplicates_exact.collision_overview": pd.DataFrame(
                [
                    {
                        "scope": "matched_only",
                        "metric": "repeated_group_rows",
                        "observed": 15.0,
                        "expected": 10.0,
                        "expected_p05": 8.0,
                        "expected_p50": 10.0,
                        "expected_p95": 12.0,
                        "z_score": 2.0,
                        "p_value": 0.01,
                        "n_used": 100,
                        "N_used": 1000,
                    },
                    {
                        "scope": "matched_only",
                        "metric": "excess_rows",
                        "observed": 11.0,
                        "expected": 8.0,
                        "expected_p05": 6.0,
                        "expected_p50": 8.0,
                        "expected_p95": 10.0,
                        "z_score": 1.5,
                        "p_value": 0.04,
                        "n_used": 100,
                        "N_used": 1000,
                    },
                    {
                        "scope": "matched_only",
                        "metric": "pairs",
                        "observed": 30.0,
                        "expected": 22.0,
                        "expected_p05": 18.0,
                        "expected_p50": 22.0,
                        "expected_p95": 26.0,
                        "z_score": 2.1,
                        "p_value": 0.02,
                        "n_used": 100,
                        "N_used": 1000,
                    },
                ]
            ),
            "duplicates_exact.collision_by_bucket": pd.DataFrame(
                [
                    {
                        "scope": "matched_only",
                        "metric": "repeated_group_rows",
                        "bucket_start": pd.Timestamp("2026-02-01T00:00:00Z"),
                        "bucket_minutes": 1,
                        "n_bucket": 5,
                        "n_used": 100,
                        "N_used": 1000,
                        "n_unique_names": 4,
                        "n_pro": 2,
                        "n_con": 3,
                        "observed": 2.0,
                        "expected": 1.0,
                        "expected_p05": 0.0,
                        "expected_p95": 2.0,
                        "z_score": 1.0,
                        "p_value": 0.12,
                        "excess": 1.0,
                        "baseline_model": "multinomial",
                        "baseline_source": "hearing_empirical",
                        "baseline_degraded": True,
                        "is_low_power": False,
                        "inference_status": "tested",
                    }
                ]
            ),
            "duplicates_exact.per_name_display": pd.DataFrame(
                [
                    {
                        "scope": "matched_only",
                        "display_name": "DOE, JANE",
                        "canonical_name": "DOE|JANE",
                        "observed_count": 3,
                        "n_pro": 1,
                        "n_con": 2,
                        "time_span_minutes": 10.0,
                        "expected_count": 1.2,
                        "p_value": 0.01,
                        "q_value": 0.02,
                        "is_significant": True,
                        "display_truncated": False,
                    }
                ]
            ),
            "duplicates_exact.per_name_duplicates_by_mode": pd.DataFrame(
                [
                    {
                        "scope": "matched_only",
                        "match_mode": "strict",
                        "match_label": "Exact (last + first)",
                        "match_definition": "Exact match on last-name and first-name tokens.",
                        "canonical_name": "DOE|JANE",
                        "name_key": "DOE|JANE",
                        "display_name": "DOE, JANE",
                        "observed_count": 3,
                        "total_repeated_rows": 3,
                    },
                    {
                        "scope": "matched_only",
                        "match_mode": "strict",
                        "match_label": "Exact (last + first)",
                        "match_definition": "Exact match on last-name and first-name tokens.",
                        "canonical_name": "SMITH|JOHN",
                        "name_key": "SMITH|JOHN",
                        "display_name": "SMITH, JOHN",
                        "observed_count": 2,
                        "total_repeated_rows": 2,
                    },
                ]
            ),
            "duplicates_exact.per_name_submission_timing_by_mode": pd.DataFrame(
                [
                    {
                        "scope": "matched_only",
                        "match_mode": "strict",
                        "match_label": "Exact (last + first)",
                        "match_definition": "Exact match on last-name and first-name tokens.",
                        "canonical_name": "DOE|JANE",
                        "name_key": "DOE|JANE",
                        "display_name": "DOE, JANE",
                        "bucket_start": pd.Timestamp("2026-02-01T00:00:00Z"),
                        "position_normalized": "Pro",
                    },
                    {
                        "scope": "matched_only",
                        "match_mode": "strict",
                        "match_label": "Exact (last + first)",
                        "match_definition": "Exact match on last-name and first-name tokens.",
                        "canonical_name": "DOE|JANE",
                        "name_key": "DOE|JANE",
                        "display_name": "DOE, JANE",
                        "bucket_start": pd.Timestamp("2026-02-01T00:00:10Z"),
                        "position_normalized": "Con",
                    },
                    {
                        "scope": "matched_only",
                        "match_mode": "strict",
                        "match_label": "Exact (last + first)",
                        "match_definition": "Exact match on last-name and first-name tokens.",
                        "canonical_name": "SMITH|JOHN",
                        "name_key": "SMITH|JOHN",
                        "display_name": "SMITH, JOHN",
                        "bucket_start": pd.Timestamp("2026-02-01T00:00:20Z"),
                        "position_normalized": "Con",
                    },
                ]
            ),
            "duplicates_exact.top_name_timing_by_mode": pd.DataFrame(
                [
                    {
                        "scope": "matched_only",
                        "match_mode": "exact",
                        "match_label": "Exact (last + first)",
                        "match_definition": "Exact match on last-name and first-name tokens.",
                        "rank": 1,
                        "name_key": "DOE|JANE",
                        "display_name": "DOE, JANE",
                        "total_repeated_rows": 3,
                        "bucket_start": pd.Timestamp("2026-02-01T00:00:00Z"),
                        "bucket_minutes": 5,
                        "duplicate_rows": 3,
                        "n_pro": 1,
                        "n_con": 2,
                        "n_other": 0,
                        "first_seen": pd.Timestamp("2026-02-01T00:00:00Z"),
                        "last_seen": pd.Timestamp("2026-02-01T00:04:00Z"),
                    },
                    {
                        "scope": "matched_only",
                        "match_mode": "exact",
                        "match_label": "Exact (last + first)",
                        "match_definition": "Exact match on last-name and first-name tokens.",
                        "rank": 2,
                        "name_key": "SMITH|JOHN",
                        "display_name": "SMITH, JOHN",
                        "total_repeated_rows": 2,
                        "bucket_start": pd.Timestamp("2026-02-01T00:05:00Z"),
                        "bucket_minutes": 5,
                        "duplicate_rows": 2,
                        "n_pro": 1,
                        "n_con": 1,
                        "n_other": 0,
                        "first_seen": pd.Timestamp("2026-02-01T00:05:00Z"),
                        "last_seen": pd.Timestamp("2026-02-01T00:09:00Z"),
                    },
                ]
            ),
        },
        detector_summaries={},
    )

    diagnostics = payload["charts"]["duplicates_exact_metric_diagnostics"]
    assert {row["metric"] for row in diagnostics} == {"repeated_group_rows", "excess_rows", "pairs"}

    bucket_rows = payload["charts"]["duplicates_exact_bucket_concentration"]
    assert bucket_rows
    rows_by_metric = {str(row["metric"]): row for row in bucket_rows}
    assert set(rows_by_metric) == {"rows_anywhere", "names_anywhere"}
    rows_row = rows_by_metric["rows_anywhere"]
    names_row = rows_by_metric["names_anywhere"]
    assert rows_row["scope"] == "matched_only"
    assert rows_row["match_mode"] == "strict"
    assert rows_row["n_used"] == 100
    assert rows_row["N_used"] == 1000
    assert rows_row["baseline_model"] == "multinomial"
    assert rows_row["baseline_source"] == "hearing_empirical"
    assert rows_row["baseline_degraded"] is True
    assert abs(float(rows_row["duplicate_rows"]) - 3.0) < 1e-9
    assert abs(float(rows_row["expected_duplicate_rows"]) - 0.25) < 1e-9
    assert abs(float(rows_row["excess_duplicate_rows"]) - 2.75) < 1e-9
    assert abs(float(names_row["duplicate_rows"]) - 2.0) < 1e-9
    assert abs(float(names_row["expected_duplicate_rows"]) - 0.1) < 1e-9
    assert abs(float(names_row["excess_duplicate_rows"]) - 1.9) < 1e-9
    assert "unit_observed_rows" in rows_row
    assert "unit_expected_rows" in rows_row
    assert "unit_deviation_rows" in rows_row
    assert "unit_observed_names" in rows_row
    assert "unit_expected_names" in rows_row
    assert "unit_deviation_names" in rows_row

    timing_exact_rows = payload["charts"]["duplicates_exact_top_name_timing_exact"]
    assert timing_exact_rows
    assert {entry["match_mode"] for entry in timing_exact_rows} == {"strict"}
    timing_required = {
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
    assert timing_required.issubset(set(timing_exact_rows[0].keys()))

    controls = payload["controls"]
    assert controls["duplicate_collision_scope_default"] == "matched_only"
    assert controls["duplicate_collision_metric_default"] == "rows_anywhere"
    assert "matched_only" in controls["duplicate_collision_scope_options"]
    assert controls["duplicate_collision_metric_options"] == ["rows_anywhere", "names_anywhere"]
    assert controls["duplicate_match_mode_default"] in {"strict", "loose"}
    assert set(controls["duplicate_match_mode_options"]).issubset({"strict", "loose"})

    methodology = payload["controls"]["methodology"]
    assert methodology["duplicate_runtime"]
    assert any("degraded" in str(item).lower() for item in methodology["caveats"])


def test_duplicates_exact_top_name_timing_rows_include_rank_metadata_rows() -> None:
    payload = _build_interactive_chart_payload_v2(
        table_map={
            "duplicates_exact.top_name_timing_by_mode": pd.DataFrame(
                [
                    {
                        "scope": "matched_only",
                        "match_mode": "exact",
                        "match_label": "Exact (last + first)",
                        "match_definition": "Exact match on last-name and first-name tokens.",
                        "rank": 1,
                        "name_key": "DOE|JANE",
                        "display_name": "DOE, JANE",
                        "total_repeated_rows": 6,
                        "bucket_start": pd.Timestamp("2026-02-01T00:00:00Z"),
                        "bucket_minutes": 5,
                        "duplicate_rows": 4,
                        "n_pro": 2,
                        "n_con": 2,
                        "n_other": 0,
                        "first_seen": pd.Timestamp("2026-02-01T00:00:00Z"),
                        "last_seen": pd.Timestamp("2026-02-01T00:04:00Z"),
                    },
                    {
                        "scope": "matched_only",
                        "match_mode": "exact",
                        "match_label": "Exact (last + first)",
                        "match_definition": "Exact match on last-name and first-name tokens.",
                        "rank": 2,
                        "name_key": "SMITH|JOHN",
                        "display_name": "SMITH, JOHN",
                        "total_repeated_rows": 5,
                        "bucket_start": pd.Timestamp("2026-02-01T01:00:00Z"),
                        "bucket_minutes": 15,
                        "duplicate_rows": 3,
                        "n_pro": 1,
                        "n_con": 2,
                        "n_other": 0,
                        "first_seen": pd.Timestamp("2026-02-01T01:00:00Z"),
                        "last_seen": pd.Timestamp("2026-02-01T01:14:00Z"),
                    },
                ]
            ),
        },
        detector_summaries={},
    )

    exact_rows = payload["charts"]["duplicates_exact_top_name_timing_exact"]
    assert exact_rows

    bucket_rows = [row for row in exact_rows if row.get("row_kind") != "name_rank"]
    metadata_rows = [row for row in exact_rows if row.get("row_kind") == "name_rank"]

    assert len(bucket_rows) == 2
    assert len(metadata_rows) == 2
    assert sorted(int(row["rank"]) for row in metadata_rows) == [1, 2]
    assert {str(row["name_key"]) for row in metadata_rows} == {"DOE|JANE", "SMITH|JOHN"}
    assert all(row.get("bucket_minutes") is None for row in metadata_rows)


def test_duplicates_exact_bucket_concentration_keeps_signed_deviation() -> None:
    payload = _build_interactive_chart_payload_v2(
        table_map={
            "duplicates_exact.collision_by_bucket": pd.DataFrame(
                [
                    {
                        "scope": "full_hearing",
                        "metric": "repeated_group_rows",
                        "bucket_start": pd.Timestamp("2026-02-01T00:00:00Z"),
                        "bucket_minutes": 30,
                        "n_bucket": 20,
                        "n_used": 1000,
                        "N_used": 10000,
                        "n_unique_names": 18,
                        "n_pro": 10,
                        "n_con": 10,
                        "observed": 0.0,
                        "expected": 0.25,
                        "expected_p05": 0.0,
                        "expected_p95": 1.0,
                        "z_score": -0.5,
                        "p_value": 1.0,
                        "excess": -0.25,
                        "baseline_model": "multinomial",
                        "baseline_source": "vrdb_full_histogram",
                        "baseline_degraded": False,
                        "is_low_power": False,
                        "inference_status": "tested",
                    }
                ]
            ),
        },
        detector_summaries={},
    )

    bucket_rows = payload["charts"]["duplicates_exact_bucket_concentration"]
    assert bucket_rows
    rows_row = next(row for row in bucket_rows if row["metric"] == "rows_anywhere")
    assert rows_row["duplicate_rows"] == 0.0
    assert rows_row["expected_duplicate_rows"] == 0.25
    assert rows_row["excess_duplicate_rows"] == -0.25


def test_payload_preserves_duplicate_bucket_options_when_one_minute_rows_dominate() -> None:
    n_one_minute = 25_010
    n_thirty_minute = 15
    one_minute = pd.DataFrame(
        {
            "scope": "matched_only",
            "metric": "repeated_group_rows",
            "bucket_start": pd.date_range(
                "2026-02-01T00:00:00Z", periods=n_one_minute, freq="min", tz="UTC"
            ),
            "bucket_minutes": 1,
            "n_bucket": 2,
            "n_used": 1000,
            "N_used": 10000,
            "n_unique_names": 2,
            "n_pro": 1,
            "n_con": 1,
            "observed": 1.0,
            "expected": 0.2,
            "expected_p05": 0.0,
            "expected_p95": 1.0,
            "z_score": 0.0,
            "p_value": 1.0,
            "excess": 0.8,
            "baseline_model": "multinomial",
            "baseline_source": "vrdb_full_histogram",
            "baseline_degraded": False,
            "is_low_power": False,
            "inference_status": "tested",
        }
    )
    thirty_minute = pd.DataFrame(
        {
            "scope": "matched_only",
            "metric": "repeated_group_rows",
            "bucket_start": pd.date_range(
                "2026-03-01T00:00:00Z", periods=n_thirty_minute, freq="30min", tz="UTC"
            ),
            "bucket_minutes": 30,
            "n_bucket": 40,
            "n_used": 1000,
            "N_used": 10000,
            "n_unique_names": 35,
            "n_pro": 20,
            "n_con": 20,
            "observed": 8.0,
            "expected": 2.0,
            "expected_p05": 1.0,
            "expected_p95": 4.0,
            "z_score": 2.0,
            "p_value": 0.01,
            "excess": 6.0,
            "baseline_model": "multinomial",
            "baseline_source": "vrdb_full_histogram",
            "baseline_degraded": False,
            "is_low_power": False,
            "inference_status": "tested",
        }
    )
    collision_by_bucket = pd.concat([one_minute, thirty_minute], ignore_index=True)

    payload = _build_interactive_chart_payload_v2(
        table_map={
            "artifacts.counts_per_minute": pd.DataFrame(
                {
                    "minute_bucket": pd.to_datetime(["2026-02-01T00:00:00Z"]),
                    "n_total": [2],
                    "n_pro": [1],
                    "n_con": [1],
                    "pro_rate": [0.5],
                    "pro_rate_wilson_low": [0.1],
                    "pro_rate_wilson_high": [0.9],
                    "is_low_power": [False],
                    "n_unique_names": [2],
                    "unique_ratio": [1.0],
                }
            ),
            "duplicates_exact.collision_methods": pd.DataFrame(
                [
                    {
                        "scope": "matched_only",
                        "baseline_source": "vrdb_full_histogram",
                        "baseline_model": "multinomial",
                        "uncertainty_model": "monte_carlo",
                        "n_used": 1000,
                        "N_used": 10000,
                        "metric_primary": "repeated_group_rows",
                        "metrics_reported": "repeated_group_rows,excess_rows,pairs",
                        "baseline_degraded": False,
                        "fallback_policy": "fail",
                        "collision_key_mode": "strict",
                        "normalization_version_hash": "abc123",
                        "stratification": "none",
                        "censored": False,
                    }
                ]
            ),
            "duplicates_exact.collision_overview": pd.DataFrame(
                [
                    {
                        "scope": "matched_only",
                        "metric": "repeated_group_rows",
                        "observed": 200.0,
                        "expected": 50.0,
                        "expected_p05": 40.0,
                        "expected_p50": 50.0,
                        "expected_p95": 60.0,
                        "z_score": 3.0,
                        "p_value": 0.001,
                        "n_used": 1000,
                        "N_used": 10000,
                    }
                ]
            ),
            "duplicates_exact.collision_by_bucket": collision_by_bucket,
        },
        detector_summaries={},
    )

    bucket_rows = payload["charts"]["duplicates_exact_bucket_concentration"]
    bucket_minutes = {int(row["bucket_minutes"]) for row in bucket_rows}
    assert 1 in bucket_minutes
    assert 30 in bucket_minutes


def test_duplicates_exact_bucket_concentration_emits_scope_mode_unit_rows() -> None:
    payload = _build_interactive_chart_payload_v2(
        table_map={
            "duplicates_exact.collision_methods": pd.DataFrame(
                [
                    {
                        "scope": "matched_only",
                        "collision_key_mode": "strict",
                        "metric_primary": "repeated_group_rows",
                        "n_used": 10,
                        "N_used": 1000,
                    },
                    {
                        "scope": "full_hearing",
                        "collision_key_mode": "strict",
                        "metric_primary": "repeated_group_rows",
                        "n_used": 20,
                        "N_used": 1000,
                    },
                ]
            ),
            "duplicates_exact.collision_by_bucket": pd.DataFrame(
                [
                    {
                        "scope": "matched_only",
                        "metric": "repeated_group_rows",
                        "bucket_start": pd.Timestamp("2026-02-01T00:00:00Z"),
                        "bucket_minutes": 30,
                        "n_bucket": 4,
                        "n_used": 10,
                        "N_used": 1000,
                        "n_unique_names": 3,
                        "n_pro": 2,
                        "n_con": 2,
                        "observed": 2.0,
                        "expected": 1.0,
                        "excess": 1.0,
                    },
                    {
                        "scope": "full_hearing",
                        "metric": "repeated_group_rows",
                        "bucket_start": pd.Timestamp("2026-02-01T00:00:00Z"),
                        "bucket_minutes": 30,
                        "n_bucket": 6,
                        "n_used": 20,
                        "N_used": 1000,
                        "n_unique_names": 4,
                        "n_pro": 3,
                        "n_con": 3,
                        "observed": 3.0,
                        "expected": 2.0,
                        "excess": 1.0,
                    },
                ]
            ),
            "duplicates_exact.per_name_duplicates_by_mode": pd.DataFrame(
                [
                    {
                        "scope": "matched_only",
                        "match_mode": "strict",
                        "name_key": "A",
                        "canonical_name": "A",
                        "total_repeated_rows": 2,
                    },
                    {
                        "scope": "matched_only",
                        "match_mode": "strict",
                        "name_key": "B",
                        "canonical_name": "B",
                        "total_repeated_rows": 2,
                    },
                    {
                        "scope": "matched_only",
                        "match_mode": "loose",
                        "name_key": "A",
                        "canonical_name": "A",
                        "total_repeated_rows": 3,
                    },
                    {
                        "scope": "matched_only",
                        "match_mode": "loose",
                        "name_key": "B",
                        "canonical_name": "B",
                        "total_repeated_rows": 2,
                    },
                    {
                        "scope": "matched_only",
                        "match_mode": "loose",
                        "name_key": "C",
                        "canonical_name": "C",
                        "total_repeated_rows": 1,
                    },
                    {
                        "scope": "full_hearing",
                        "match_mode": "strict",
                        "name_key": "A",
                        "canonical_name": "A",
                        "total_repeated_rows": 5,
                    },
                    {
                        "scope": "full_hearing",
                        "match_mode": "strict",
                        "name_key": "D",
                        "canonical_name": "D",
                        "total_repeated_rows": 3,
                    },
                ]
            ),
            "duplicates_exact.per_name_submission_timing_by_mode": pd.DataFrame(
                [
                    {
                        "scope": "matched_only",
                        "match_mode": "strict",
                        "name_key": "A",
                        "canonical_name": "A",
                        "bucket_start": pd.Timestamp("2026-02-01T00:00:00Z"),
                    },
                    {
                        "scope": "matched_only",
                        "match_mode": "strict",
                        "name_key": "B",
                        "canonical_name": "B",
                        "bucket_start": pd.Timestamp("2026-02-01T00:01:00Z"),
                    },
                    {
                        "scope": "matched_only",
                        "match_mode": "loose",
                        "name_key": "A",
                        "canonical_name": "A",
                        "bucket_start": pd.Timestamp("2026-02-01T00:00:00Z"),
                    },
                    {
                        "scope": "matched_only",
                        "match_mode": "loose",
                        "name_key": "C",
                        "canonical_name": "C",
                        "bucket_start": pd.Timestamp("2026-02-01T00:01:00Z"),
                    },
                    {
                        "scope": "matched_only",
                        "match_mode": "loose",
                        "name_key": "C",
                        "canonical_name": "C",
                        "bucket_start": pd.Timestamp("2026-02-01T00:02:00Z"),
                    },
                    {
                        "scope": "full_hearing",
                        "match_mode": "strict",
                        "name_key": "A",
                        "canonical_name": "A",
                        "bucket_start": pd.Timestamp("2026-02-01T00:00:00Z"),
                    },
                    {
                        "scope": "full_hearing",
                        "match_mode": "strict",
                        "name_key": "A",
                        "canonical_name": "A",
                        "bucket_start": pd.Timestamp("2026-02-01T00:01:00Z"),
                    },
                    {
                        "scope": "full_hearing",
                        "match_mode": "strict",
                        "name_key": "A",
                        "canonical_name": "A",
                        "bucket_start": pd.Timestamp("2026-02-01T00:02:00Z"),
                    },
                    {
                        "scope": "full_hearing",
                        "match_mode": "strict",
                        "name_key": "D",
                        "canonical_name": "D",
                        "bucket_start": pd.Timestamp("2026-02-01T00:03:00Z"),
                    },
                ]
            ),
        },
        detector_summaries={},
    )

    rows = payload["charts"]["duplicates_exact_bucket_concentration"]
    assert rows
    combos = {(row["scope"], row["match_mode"], row["metric"]) for row in rows}
    assert ("matched_only", "strict", "rows_anywhere") in combos
    assert ("matched_only", "strict", "names_anywhere") in combos
    assert ("matched_only", "loose", "rows_anywhere") in combos
    assert ("matched_only", "loose", "names_anywhere") in combos
    assert ("full_hearing", "strict", "rows_anywhere") in combos
    assert ("full_hearing", "strict", "names_anywhere") in combos

    lookup = {(row["scope"], row["match_mode"], row["metric"]): row for row in rows}
    strict_rows = lookup[("matched_only", "strict", "rows_anywhere")]
    loose_rows = lookup[("matched_only", "loose", "rows_anywhere")]
    strict_names = lookup[("matched_only", "strict", "names_anywhere")]

    assert strict_rows["duplicate_rows"] == 2.0
    assert abs(float(strict_rows["expected_duplicate_rows"]) - 1.6) < 1e-9
    assert abs(float(strict_rows["excess_duplicate_rows"]) - 0.4) < 1e-9
    assert loose_rows["duplicate_rows"] == 3.0
    assert abs(float(loose_rows["expected_duplicate_rows"]) - 2.4) < 1e-9
    assert abs(float(loose_rows["excess_duplicate_rows"]) - 0.6) < 1e-9
    assert strict_names["duplicate_rows"] == 2.0
    assert abs(float(strict_names["expected_duplicate_rows"]) - 0.8) < 1e-9
    assert abs(float(strict_names["excess_duplicate_rows"]) - 1.2) < 1e-9


def test_duplicates_exact_chart_limits_and_null_distribution_visibility_contract() -> None:
    per_name_rows = [
        {
            "scope": "matched_only",
            "display_name": f"NAME {index:02d}",
            "canonical_name": f"NAME|{index:02d}",
            "observed_count": 40 - index,
            "n_pro": (20 - min(index, 19)) if index % 2 == 0 and index % 6 != 0 else 0,
            "n_con": 20 if index % 2 == 1 and index % 6 != 0 else 0,
            "first_seen": pd.Timestamp("2026-02-01T00:00:00Z") + pd.Timedelta(minutes=index),
            "last_seen": pd.Timestamp("2026-02-01T00:09:00Z") + pd.Timedelta(minutes=index),
            "time_span_minutes": 60.0 + index,
            "expected_count": 1.0 + index * 0.1,
            "p_value": 0.001 + index * 0.001,
            "q_value": 0.002 + index * 0.002,
            "is_significant": True,
        }
        for index in range(18)
    ]
    for mixed_index in (0, 6, 12):
        per_name_rows[mixed_index]["n_pro"] = 10
        per_name_rows[mixed_index]["n_con"] = 10
    payload = _build_interactive_chart_payload_v2(
        table_map={
            "artifacts.counts_per_minute": pd.DataFrame(
                {
                    "minute_bucket": pd.to_datetime(["2026-02-01T00:00:00Z"]),
                    "n_total": [10],
                    "n_pro": [4],
                    "n_con": [6],
                    "pro_rate": [0.4],
                    "pro_rate_wilson_low": [0.2],
                    "pro_rate_wilson_high": [0.6],
                    "is_low_power": [False],
                    "n_unique_names": [8],
                    "unique_ratio": [0.8],
                }
            ),
            "duplicates_exact.collision_methods": pd.DataFrame(
                [
                    {
                        "scope": "matched_only",
                        "baseline_source": "hearing_empirical",
                        "baseline_model": "multinomial",
                        "metric_primary": "repeated_group_rows",
                    }
                ]
            ),
            "duplicates_exact.collision_overview": pd.DataFrame(
                [
                    {
                        "scope": "matched_only",
                        "metric": "repeated_group_rows",
                        "observed": 50.0,
                        "expected": 20.0,
                        "expected_p05": 15.0,
                        "expected_p50": 20.0,
                        "expected_p95": 30.0,
                        "z_score": 3.0,
                        "p_value": 0.001,
                        "n_used": 1000,
                        "N_used": 10000,
                    }
                ]
            ),
            "duplicates_exact.collision_by_bucket": pd.DataFrame(
                [
                    {
                        "scope": "matched_only",
                        "metric": "repeated_group_rows",
                        "bucket_start": pd.Timestamp("2026-02-01T00:00:00Z"),
                        "bucket_minutes": 30,
                        "n_bucket": 10,
                        "n_used": 1000,
                        "N_used": 10000,
                        "n_unique_names": 8,
                        "n_pro": 4,
                        "n_con": 6,
                        "observed": 5.0,
                        "expected": 2.0,
                        "expected_p05": 1.0,
                        "expected_p95": 3.0,
                        "z_score": 2.0,
                        "p_value": 0.01,
                        "excess": 3.0,
                        "baseline_model": "multinomial",
                        "baseline_source": "hearing_empirical",
                        "baseline_degraded": False,
                        "is_low_power": False,
                    }
                ]
            ),
            "duplicates_exact.per_name_display": pd.DataFrame(per_name_rows),
            "duplicates_exact.null_distribution": pd.DataFrame(
                {
                    "iteration": [1, 2, 3],
                    "duplicate_rows": [10, 11, 12],
                    "duplicate_row_rate": [0.1, 0.11, 0.12],
                    "duplicate_pairs": [5, 6, 7],
                    "n_names_ge2": [3, 3, 4],
                    "n_names_ge3": [1, 1, 2],
                    "n_names_ge5": [0, 0, 1],
                    "n_names_ge10": [0, 0, 0],
                    "max_count": [4, 4, 5],
                }
            ),
        },
        detector_summaries={},
    )

    per_name_chart = payload["charts"]["duplicates_exact_per_name_anomalies"]
    assert len(per_name_chart) == 18
    assert any((row["n_pro"] > 0) and (row["n_con"] > 0) for row in per_name_chart)
    assert all(str(row.get("first_seen", "")).strip() for row in per_name_chart)
    assert all(str(row.get("last_seen", "")).strip() for row in per_name_chart)

    by_id = {entry["id"]: entry for entry in payload["analysis_catalog"]}
    duplicates_exact_entry = by_id.get("duplicates_exact")
    if duplicates_exact_entry is not None:
        assert "duplicates_exact_null_distribution" not in duplicates_exact_entry["detail_chart_ids"]
        assert "duplicates_exact_top_name_timing_exact" in duplicates_exact_entry["detail_chart_ids"]
        assert "duplicates_exact_top_name_timing_medium" not in duplicates_exact_entry["detail_chart_ids"]
        assert "duplicates_exact_top_name_timing_loose" not in duplicates_exact_entry["detail_chart_ids"]
    assert isinstance(payload["charts"]["duplicates_exact_top_name_timing_exact"], list)
    assert "duplicates_exact_temporal_burst" not in payload["charts"]
    assert "duplicates_exact_top_name_timing_medium" not in payload["charts"]
    assert "duplicates_exact_top_name_timing_loose" not in payload["charts"]
    assert payload["charts"]["duplicates_exact_null_distribution"]


def test_voter_registry_unmatched_names_chart_is_capped_to_top_100() -> None:
    unmatched_rows = pd.DataFrame(
        [
            {
                "canonical_name": f"NAME|{index:02d}",
                "match_mode": "loose",
                "n_rows": 200 - index,
                "n_pro": 10,
                "n_con": 20,
                "first_seen": pd.Timestamp("2026-02-01T00:00:00Z") + pd.Timedelta(minutes=index),
                "last_seen": pd.Timestamp("2026-02-01T00:05:00Z") + pd.Timedelta(minutes=index),
                "top_caveat": "no_match",
                "best_similarity_score": 0.5,
                "candidate_pool_size": 3,
            }
            for index in range(175)
        ]
    )
    payload = _build_interactive_chart_payload_v2(
        table_map={
            "artifacts.counts_per_minute": pd.DataFrame(
                {
                    "minute_bucket": pd.to_datetime(["2026-02-01T00:00:00Z"]),
                    "n_total": [10],
                    "n_pro": [4],
                    "n_con": [6],
                    "pro_rate": [0.4],
                    "pro_rate_wilson_low": [0.2],
                    "pro_rate_wilson_high": [0.6],
                    "is_low_power": [False],
                    "n_unique_names": [8],
                    "unique_ratio": [0.8],
                }
            ),
            "voter_registry_match.match_by_bucket": pd.DataFrame(
                {
                    "bucket_start": pd.to_datetime(["2026-02-01T00:00:00Z"]),
                    "bucket_minutes": [30],
                    "n_total": [10],
                    "n_matched_unique": [6],
                    "n_matched_ambiguous": [1],
                    "n_unmatched": [3],
                    "matched_rate": [0.7],
                    "unmatched_rate": [0.3],
                    "matched_rate_wilson_low": [0.4],
                    "matched_rate_wilson_high": [0.9],
                    "unmatched_rate_wilson_low": [0.1],
                    "unmatched_rate_wilson_high": [0.6],
                    "is_low_power": [False],
                    "n_pro": [4],
                    "n_con": [6],
                }
            ),
            "voter_registry_match.unmatched_names": unmatched_rows,
        },
        detector_summaries={},
    )

    chart_rows = payload["charts"]["voter_registry_unmatched_names"]
    assert len(chart_rows) == 100
    assert [row["n_records"] for row in chart_rows] == sorted(
        [row["n_records"] for row in chart_rows],
        reverse=True,
    )
    assert all(str(row.get("display_name", "")).strip() for row in chart_rows)
    assert all(str(row.get("first_seen", "")).strip() for row in chart_rows)
    assert all(str(row.get("last_seen", "")).strip() for row in chart_rows)
    assert chart_rows[0]["display_name"] == "NAME, 00"


def test_voter_registry_match_rates_preserve_mode_bucket_rows_without_cross_join() -> None:
    match_by_bucket = pd.DataFrame(
        [
            {
                "match_mode": "strict",
                "bucket_start": pd.Timestamp("2026-02-01T00:00:00Z"),
                "bucket_minutes": 1,
                "n_total": 10,
                "n_matched_unique": 3,
                "n_matched_ambiguous": 1,
                "n_unmatched": 6,
                "matched_rate": 0.4,
                "unmatched_rate": 0.6,
                "matched_rate_wilson_low": 0.2,
                "matched_rate_wilson_high": 0.6,
                "unmatched_rate_wilson_low": 0.4,
                "unmatched_rate_wilson_high": 0.8,
                "is_low_power": False,
                "n_pro": 4,
                "n_con": 6,
            },
            {
                "match_mode": "strict",
                "bucket_start": pd.Timestamp("2026-02-01T00:05:00Z"),
                "bucket_minutes": 5,
                "n_total": 20,
                "n_matched_unique": 9,
                "n_matched_ambiguous": 1,
                "n_unmatched": 10,
                "matched_rate": 0.5,
                "unmatched_rate": 0.5,
                "matched_rate_wilson_low": 0.3,
                "matched_rate_wilson_high": 0.7,
                "unmatched_rate_wilson_low": 0.3,
                "unmatched_rate_wilson_high": 0.7,
                "is_low_power": False,
                "n_pro": 10,
                "n_con": 10,
            },
            {
                "match_mode": "loose",
                "bucket_start": pd.Timestamp("2026-02-01T00:00:00Z"),
                "bucket_minutes": 1,
                "n_total": 10,
                "n_matched_unique": 7,
                "n_matched_ambiguous": 1,
                "n_unmatched": 2,
                "matched_rate": 0.8,
                "unmatched_rate": 0.2,
                "matched_rate_wilson_low": 0.6,
                "matched_rate_wilson_high": 0.9,
                "unmatched_rate_wilson_low": 0.1,
                "unmatched_rate_wilson_high": 0.4,
                "is_low_power": False,
                "n_pro": 4,
                "n_con": 6,
            },
            {
                "match_mode": "loose",
                "bucket_start": pd.Timestamp("2026-02-01T00:05:00Z"),
                "bucket_minutes": 5,
                "n_total": 20,
                "n_matched_unique": 15,
                "n_matched_ambiguous": 1,
                "n_unmatched": 4,
                "matched_rate": 0.8,
                "unmatched_rate": 0.2,
                "matched_rate_wilson_low": 0.6,
                "matched_rate_wilson_high": 0.9,
                "unmatched_rate_wilson_low": 0.1,
                "unmatched_rate_wilson_high": 0.4,
                "is_low_power": False,
                "n_pro": 10,
                "n_con": 10,
            },
        ]
    )
    match_by_bucket_position = pd.DataFrame(
        [
            {
                "match_mode": "strict",
                "bucket_start": pd.Timestamp("2026-02-01T00:00:00Z"),
                "bucket_minutes": 1,
                "position_normalized": "Pro",
                "n_total": 4,
                "n_matched_unique": 1,
                "n_matched_ambiguous": 0,
                "n_unmatched": 3,
                "matched_rate": 0.10,
                "unmatched_rate": 0.90,
                "matched_rate_wilson_low": 0.01,
                "matched_rate_wilson_high": 0.30,
                "unmatched_rate_wilson_low": 0.70,
                "unmatched_rate_wilson_high": 0.99,
                "is_low_power": False,
            },
            {
                "match_mode": "strict",
                "bucket_start": pd.Timestamp("2026-02-01T00:00:00Z"),
                "bucket_minutes": 1,
                "position_normalized": "Con",
                "n_total": 6,
                "n_matched_unique": 2,
                "n_matched_ambiguous": 1,
                "n_unmatched": 3,
                "matched_rate": 0.20,
                "unmatched_rate": 0.80,
                "matched_rate_wilson_low": 0.05,
                "matched_rate_wilson_high": 0.45,
                "unmatched_rate_wilson_low": 0.55,
                "unmatched_rate_wilson_high": 0.95,
                "is_low_power": False,
            },
            {
                "match_mode": "strict",
                "bucket_start": pd.Timestamp("2026-02-01T00:05:00Z"),
                "bucket_minutes": 5,
                "position_normalized": "Pro",
                "n_total": 10,
                "n_matched_unique": 4,
                "n_matched_ambiguous": 1,
                "n_unmatched": 5,
                "matched_rate": 0.25,
                "unmatched_rate": 0.75,
                "matched_rate_wilson_low": 0.10,
                "matched_rate_wilson_high": 0.50,
                "unmatched_rate_wilson_low": 0.50,
                "unmatched_rate_wilson_high": 0.90,
                "is_low_power": False,
            },
            {
                "match_mode": "strict",
                "bucket_start": pd.Timestamp("2026-02-01T00:05:00Z"),
                "bucket_minutes": 5,
                "position_normalized": "Con",
                "n_total": 10,
                "n_matched_unique": 5,
                "n_matched_ambiguous": 0,
                "n_unmatched": 5,
                "matched_rate": 0.35,
                "unmatched_rate": 0.65,
                "matched_rate_wilson_low": 0.15,
                "matched_rate_wilson_high": 0.55,
                "unmatched_rate_wilson_low": 0.45,
                "unmatched_rate_wilson_high": 0.85,
                "is_low_power": False,
            },
            {
                "match_mode": "loose",
                "bucket_start": pd.Timestamp("2026-02-01T00:00:00Z"),
                "bucket_minutes": 1,
                "position_normalized": "Pro",
                "n_total": 4,
                "n_matched_unique": 3,
                "n_matched_ambiguous": 0,
                "n_unmatched": 1,
                "matched_rate": 0.80,
                "unmatched_rate": 0.20,
                "matched_rate_wilson_low": 0.45,
                "matched_rate_wilson_high": 0.97,
                "unmatched_rate_wilson_low": 0.03,
                "unmatched_rate_wilson_high": 0.55,
                "is_low_power": False,
            },
            {
                "match_mode": "loose",
                "bucket_start": pd.Timestamp("2026-02-01T00:00:00Z"),
                "bucket_minutes": 1,
                "position_normalized": "Con",
                "n_total": 6,
                "n_matched_unique": 4,
                "n_matched_ambiguous": 1,
                "n_unmatched": 1,
                "matched_rate": 0.90,
                "unmatched_rate": 0.10,
                "matched_rate_wilson_low": 0.60,
                "matched_rate_wilson_high": 0.99,
                "unmatched_rate_wilson_low": 0.01,
                "unmatched_rate_wilson_high": 0.40,
                "is_low_power": False,
            },
            {
                "match_mode": "loose",
                "bucket_start": pd.Timestamp("2026-02-01T00:05:00Z"),
                "bucket_minutes": 5,
                "position_normalized": "Pro",
                "n_total": 10,
                "n_matched_unique": 8,
                "n_matched_ambiguous": 0,
                "n_unmatched": 2,
                "matched_rate": 0.85,
                "unmatched_rate": 0.15,
                "matched_rate_wilson_low": 0.55,
                "matched_rate_wilson_high": 0.97,
                "unmatched_rate_wilson_low": 0.03,
                "unmatched_rate_wilson_high": 0.45,
                "is_low_power": False,
            },
            {
                "match_mode": "loose",
                "bucket_start": pd.Timestamp("2026-02-01T00:05:00Z"),
                "bucket_minutes": 5,
                "position_normalized": "Con",
                "n_total": 10,
                "n_matched_unique": 7,
                "n_matched_ambiguous": 1,
                "n_unmatched": 2,
                "matched_rate": 0.95,
                "unmatched_rate": 0.05,
                "matched_rate_wilson_low": 0.70,
                "matched_rate_wilson_high": 0.99,
                "unmatched_rate_wilson_low": 0.01,
                "unmatched_rate_wilson_high": 0.30,
                "is_low_power": False,
            },
        ]
    )

    payload = _build_interactive_chart_payload_v2(
        table_map={
            "artifacts.counts_per_minute": pd.DataFrame(
                {
                    "minute_bucket": pd.to_datetime(["2026-02-01T00:00:00Z"]),
                    "n_total": [10],
                    "n_pro": [4],
                    "n_con": [6],
                    "pro_rate": [0.4],
                    "pro_rate_wilson_low": [0.2],
                    "pro_rate_wilson_high": [0.6],
                    "is_low_power": [False],
                    "n_unique_names": [8],
                    "unique_ratio": [0.8],
                }
            ),
            "voter_registry_match.match_by_bucket": match_by_bucket,
            "voter_registry_match.match_by_bucket_position": match_by_bucket_position,
        },
        detector_summaries={},
    )

    rate_rows = payload["charts"]["voter_registry_match_rates"]
    assert len(rate_rows) == 4
    by_mode_bucket = {(row["match_mode"], row["bucket_minutes"]): row for row in rate_rows}
    assert set(by_mode_bucket) == {("strict", 1), ("strict", 5), ("loose", 1), ("loose", 5)}

    strict_1 = by_mode_bucket[("strict", 1)]
    assert strict_1["matched_rate_pro"] == 0.10
    assert strict_1["matched_rate_con"] == 0.20
    loose_1 = by_mode_bucket[("loose", 1)]
    assert loose_1["matched_rate_pro"] == 0.80
    assert loose_1["matched_rate_con"] == 0.90
    strict_5 = by_mode_bucket[("strict", 5)]
    assert strict_5["matched_rate_pro"] == 0.25
    assert strict_5["matched_rate_con"] == 0.35
    loose_5 = by_mode_bucket[("loose", 5)]
    assert loose_5["matched_rate_pro"] == 0.85
    assert loose_5["matched_rate_con"] == 0.95


def test_duplicates_per_name_chart_prefers_mode_aware_rows_when_available() -> None:
    payload = _build_interactive_chart_payload_v2(
        table_map={
            "artifacts.counts_per_minute": pd.DataFrame(
                {
                    "minute_bucket": pd.to_datetime(["2026-02-01T00:00:00Z"]),
                    "n_total": [10],
                    "n_pro": [4],
                    "n_con": [6],
                    "pro_rate": [0.4],
                    "pro_rate_wilson_low": [0.2],
                    "pro_rate_wilson_high": [0.6],
                    "is_low_power": [False],
                    "n_unique_names": [8],
                    "unique_ratio": [0.8],
                }
            ),
            "duplicates_exact.per_name_duplicates_by_mode": pd.DataFrame(
                [
                    {
                        "scope": "full_hearing",
                        "match_mode": "strict",
                        "display_name": "HARSHAW, NORMAN",
                        "canonical_name": "HARSHAW|NORMAN",
                        "name_key": "HARSHAW|NORMAN",
                        "observed_count": 12,
                        "n_pro": 12,
                        "n_con": 0,
                        "first_seen": pd.Timestamp("2026-02-01T00:00:00Z"),
                        "last_seen": pd.Timestamp("2026-02-01T00:30:00Z"),
                        "time_span_minutes": 30.0,
                    },
                    {
                        "scope": "full_hearing",
                        "match_mode": "loose",
                        "display_name": "HARSHAW, NORM",
                        "canonical_name": "HARSHAW|NORM",
                        "name_key": "HARSHAW|NORM",
                        "observed_count": 18,
                        "n_pro": 0,
                        "n_con": 18,
                        "first_seen": pd.Timestamp("2026-02-01T00:40:00Z"),
                        "last_seen": pd.Timestamp("2026-02-01T01:20:00Z"),
                        "time_span_minutes": 40.0,
                    },
                ]
            ),
            "duplicates_exact.per_name_anomalies": pd.DataFrame(
                [
                    {
                        "scope": "full_hearing",
                        "match_mode": "strict",
                        "display_name": "HARSHAW, NORMAN",
                        "canonical_name": "HARSHAW|NORMAN",
                        "n": 12,
                        "n_pro": 12,
                        "n_con": 0,
                        "first_seen": pd.Timestamp("2026-02-01T00:00:00Z"),
                        "last_seen": pd.Timestamp("2026-02-01T00:30:00Z"),
                        "time_span_minutes": 30.0,
                        "expected_count": 2.0,
                        "p_value": 0.001,
                        "q_value": 0.002,
                        "is_significant": True,
                    }
                ]
            ),
        },
        detector_summaries={},
    )

    chart_rows = payload["charts"]["duplicates_exact_per_name_anomalies"]
    assert {row["match_mode"] for row in chart_rows} == {"strict", "loose"}
    by_mode = {row["match_mode"]: row for row in chart_rows}
    assert by_mode["strict"]["n"] == 12
    assert by_mode["loose"]["n"] == 18
    assert by_mode["strict"]["position_series"] == "Pro"
    assert by_mode["loose"]["position_series"] == "Con"
    assert str(by_mode["strict"]["first_seen"]).strip()
    assert str(by_mode["strict"]["last_seen"]).strip()
    assert str(by_mode["loose"]["first_seen"]).strip()
    assert str(by_mode["loose"]["last_seen"]).strip()


def test_payload_values_are_json_safe_scalars() -> None:
    payload = _build_interactive_chart_payload_v2(
        table_map={
            "multivariate_anomalies.bucket_anomaly_scores": pd.DataFrame(
                {
                    "bucket_start": [pd.Timestamp("2026-02-01T00:00:00Z")],
                    "bucket_minutes": [15],
                    "n_total": [55],
                    "anomaly_score": [float("nan")],
                    "anomaly_score_percentile": [float("inf")],
                    "pro_rate": [0.45],
                }
            )
        },
        detector_summaries={},
    )

    for scalar in _walk_scalars(payload):
        if isinstance(scalar, float):
            assert scalar == scalar  # not NaN
            assert scalar not in {float("inf"), float("-inf")}


def test_payload_includes_hearing_context_and_process_markers_when_metadata_present() -> None:
    metadata = parse_hearing_metadata(
        {
            "schema_version": 1,
            "hearing_id": "SB6346",
            "timezone": "America/Los_Angeles",
            "meeting_start": "2026-02-06T13:30:00-08:00",
            "sign_in_open": "2026-02-03T09:00:00-08:00",
            "sign_in_cutoff": "2026-02-06T12:30:00-08:00",
        }
    )

    payload = _build_interactive_chart_payload_v2(
        table_map={
            "artifacts.counts_per_minute": pd.DataFrame(
                {
                    "minute_bucket": pd.to_datetime(
                        [
                            "2026-02-06T20:00:00Z",
                            "2026-02-06T20:45:00Z",
                            "2026-02-06T21:10:00Z",
                        ]
                    ),
                    "n_total": [4, 9, 3],
                    "n_pro": [2, 7, 1],
                    "n_con": [2, 2, 2],
                }
            )
        },
        detector_summaries={},
        hearing_metadata=metadata,
    )

    panel = payload["hearing_context_panel"]
    assert panel["available"] is True
    assert panel["hearing_id"] == "SB6346"
    assert panel["timezone"] == "America/Los_Angeles"
    assert len(panel["process_markers"]) >= 3
    assert isinstance(panel["deadline_ramp_metrics"], dict)

    controls = payload["controls"]
    assert controls["timezone"] == "America/Los_Angeles"
    assert len(controls["process_markers"]) >= 3


def test_payload_emits_position_baseline_and_dual_bounds_charts() -> None:
    table_map = {
        "artifacts.counts_per_minute": pd.DataFrame(
            {
                "minute_bucket": pd.to_datetime(["2026-02-01T00:00:00Z"]),
                "n_total": [3],
                "n_pro": [2],
                "n_con": [1],
                "pro_rate": [2 / 3],
                "pro_rate_wilson_low": [0.2],
                "pro_rate_wilson_high": [0.95],
                "is_low_power": [False],
                "n_unique_names": [2],
                "unique_ratio": [2 / 3],
            }
        ),
        "duplicates_exact.collision_by_bucket_position": pd.DataFrame(
            {
                "scope": ["full_hearing", "full_hearing"],
                "metric": ["repeated_group_rows", "repeated_group_rows"],
                "bucket_start": pd.to_datetime(
                    ["2026-02-01T00:00:00Z", "2026-02-01T00:00:00Z"]
                ),
                "bucket_minutes": [5, 5],
                "position_normalized": ["Pro", "Con"],
                "n_bucket_position": [2, 1],
                "n_unique_names": [1, 1],
                "observed": [2.0, 0.0],
                "expected": [2.0, 0.0],
                "excess": [0.0, 0.0],
                "deviance": [0.0, 0.0],
                "deviance_ratio": [0.0, 0.0],
                "lambda_side": [1.0, 1.0],
                "shrink_k": [0.0, 0.0],
                "prior_level": ["default", "default"],
                "is_low_power": [True, True],
                "inference_status": ["descriptive_only", "descriptive_only"],
            }
        ),
        "voter_registry_match.position_bounds": pd.DataFrame(
            {
                "match_mode": ["strict", "strict"],
                "unit": ["rows", "rows"],
                "position_normalized": ["Pro", "Con"],
                "n_total_lower": [10, 10],
                "n_total_upper": [10, 10],
                "matched_rate_lower": [0.6, 0.4],
                "matched_rate_upper": [0.8, 0.7],
                "matched_rate_span": [0.2, 0.3],
                "unmatched_rate_lower": [0.4, 0.6],
                "unmatched_rate_upper": [0.2, 0.3],
                "unmatched_rate_span": [0.2, 0.3],
                "inference_status": ["tested", "tested"],
            }
        ),
    }
    payload = _build_interactive_chart_payload_v2(
        table_map=table_map,
        detector_summaries={},
        hearing_metadata=None,
    )

    assert payload["charts"]["duplicates_exact_position_bucket_deviance"]
    assert payload["charts"]["voter_registry_position_bounds"]


def test_voter_position_bounds_chart_falls_back_to_rows_unique_span_when_bounds_missing() -> None:
    table_map = {
        "artifacts.counts_per_minute": pd.DataFrame(
            {
                "minute_bucket": pd.to_datetime(["2026-02-01T00:00:00Z"]),
                "n_total": [20],
                "n_pro": [12],
                "n_con": [8],
                "pro_rate": [0.6],
                "pro_rate_wilson_low": [0.4],
                "pro_rate_wilson_high": [0.8],
                "is_low_power": [False],
                "n_unique_names": [15],
                "unique_ratio": [0.75],
            }
        ),
        "voter_registry_match.match_by_bucket": pd.DataFrame(
            {
                "match_mode": ["loose"],
                "bucket_start": pd.to_datetime(["2026-02-01T00:00:00Z"]),
                "bucket_minutes": [30],
                "n_total": [20],
                "n_matched_unique": [11],
                "n_matched_ambiguous": [3],
                "n_unmatched": [6],
                "matched_rate": [0.7],
                "unmatched_rate": [0.3],
                "matched_rate_wilson_low": [0.5],
                "matched_rate_wilson_high": [0.85],
                "unmatched_rate_wilson_low": [0.15],
                "unmatched_rate_wilson_high": [0.5],
                "is_low_power": [False],
                "n_pro": [12],
                "n_con": [8],
            }
        ),
        "voter_registry_match.linkage_by_position_rows": pd.DataFrame(
            {
                "match_mode": ["loose", "loose"],
                "position_normalized": ["Pro", "Unknown"],
                "n_total": [10, 5],
                "n_matched_unique": [8, 2],
                "n_matched_ambiguous": [1, 1],
                "n_unmatched": [1, 2],
                "matched_rate": [0.9, 0.6],
                "unmatched_rate": [0.1, 0.4],
                "matched_rate_wilson_low": [0.7, 0.2],
                "matched_rate_wilson_high": [1.0, 0.9],
                "unmatched_rate_wilson_low": [0.0, 0.1],
                "unmatched_rate_wilson_high": [0.3, 0.8],
                "is_low_power": [False, False],
            }
        ),
        "voter_registry_match.linkage_by_position_unique": pd.DataFrame(
            {
                "match_mode": ["loose", "loose"],
                "position_normalized": ["Pro", "Other"],
                "n_total": [9, 4],
                "n_matched_unique": [7, 1],
                "n_matched_ambiguous": [1, 1],
                "n_unmatched": [1, 2],
                "matched_rate": [8 / 9, 0.5],
                "unmatched_rate": [1 / 9, 0.5],
                "matched_rate_wilson_low": [0.6, 0.1],
                "matched_rate_wilson_high": [1.0, 0.9],
                "unmatched_rate_wilson_low": [0.0, 0.1],
                "unmatched_rate_wilson_high": [0.4, 0.9],
                "is_low_power": [False, False],
            }
        ),
    }
    payload = _build_interactive_chart_payload_v2(
        table_map=table_map,
        detector_summaries={},
        hearing_metadata=None,
    )

    bounds_rows = payload["charts"]["voter_registry_position_bounds"]
    assert bounds_rows
    assert {row["position_normalized"] for row in bounds_rows} == {"Pro", "Other"}
    assert all(row["unit"] == "rows_vs_unique" for row in bounds_rows)
    assert all(row["inference_status"] == "derived_from_rows_and_unique" for row in bounds_rows)
