from __future__ import annotations

from typing import Any

import pandas as pd

from testifier_audit.io.hearing_metadata import parse_hearing_metadata
from testifier_audit.report.analysis_registry import ANALYSES_TO_PERFORM
from testifier_audit.report.render import _build_interactive_chart_payload_v2

EXPECTED_ANALYSES = {
    "baseline_profile",
    "bursts",
    "procon_swings",
    "changepoints",
    "off_hours",
    "duplicates_exact",
    "duplicates_near",
    "sortedness",
    "rare_names",
    "org_anomalies",
    "voter_registry_match",
    "periodicity",
    "multivariate_anomalies",
    "composite_score",
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
                "match_rate": [0.9, 0.84],
                "exact_match_rate": [0.72, 0.66],
                "strong_fuzzy_match_rate": [0.12, 0.11],
                "weak_fuzzy_match_rate": [0.06, 0.07],
                "expected_match_rate": [0.84, 0.79],
                "mean_match_confidence": [0.84, 0.79],
                "match_rate_wilson_low": [0.75, 0.64],
                "match_rate_wilson_high": [0.98, 0.94],
                "pro_match_rate": [0.86, 0.8],
                "con_match_rate": [0.94, 0.87],
                "is_low_power": [False, False],
            }
        ),
        "voter_registry_match.match_tier_summary": pd.DataFrame(
            {
                "match_tier": ["exact", "strong_fuzzy", "weak_fuzzy", "unmatched"],
                "n_records": [28, 4, 2, 5],
                "record_rate": [0.72, 0.1, 0.05, 0.13],
                "mean_match_confidence": [1.0, 0.88, 0.66, 0.0],
                "min_match_confidence": [1.0, 0.82, 0.57, 0.0],
                "max_match_confidence": [1.0, 0.95, 0.74, 0.0],
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

    assert payload["version"] == 3
    assert isinstance(payload["analysis_catalog"], list)
    assert isinstance(payload["charts"], dict)
    assert isinstance(payload["controls"], dict)
    assert isinstance(payload["chart_legend_docs"], dict)
    assert isinstance(payload["triage_views"], dict)
    assert isinstance(payload["triage_summary"], dict)
    assert isinstance(payload["window_evidence_queue"], list)
    assert isinstance(payload["record_evidence_queue"], list)
    assert isinstance(payload["cluster_evidence_queue"], list)
    assert isinstance(payload["data_quality_panel"], dict)
    assert isinstance(payload["hearing_context_panel"], dict)
    assert isinstance(payload["cross_hearing_baseline"], dict)

    ids = {entry["id"] for entry in payload["analysis_catalog"]}
    assert ids == EXPECTED_VISIBLE_ANALYSES

    for entry in payload["analysis_catalog"]:
        hero_chart_id = entry["hero_chart_id"]
        assert hero_chart_id in payload["charts"]
        assert isinstance(hero_chart_id, str) and hero_chart_id
        assert entry["bucket_options"] == EXPECTED_BASELINE_BUCKETS
        assert isinstance(entry["group"], str) and entry["group"]
        assert isinstance(entry["priority"], int)
        assert isinstance(entry["what_to_look_for_details"], list)
        assert entry["what_to_look_for_details"]
        assert hero_chart_id in payload["chart_legend_docs"]
        for detail_chart_id in entry["detail_chart_ids"]:
            assert detail_chart_id in payload["charts"]
            assert detail_chart_id in payload["chart_legend_docs"]

    voter_rates = payload["charts"]["voter_registry_match_rates"]
    assert voter_rates
    assert "exact_match_rate" in voter_rates[0]
    assert "strong_fuzzy_match_rate" in voter_rates[0]
    assert "weak_fuzzy_match_rate" in voter_rates[0]
    assert "expected_match_rate" in voter_rates[0]
    assert "mean_match_confidence" in voter_rates[0]
    assert payload["charts"]["voter_registry_match_tiers"]

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
    assert payload["cross_hearing_baseline"]["available"] is False


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
    assert charts["off_hours_model_fit_diagnostics"]

    timeline_row = charts["off_hours_control_timeline"][0]
    assert timeline_row["is_primary_alert_window"] is True
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

    model_fit_row = charts["off_hours_model_fit_diagnostics"][0]
    assert model_fit_row["model_fit_available_fraction"] == 0.9
    assert model_fit_row["model_fit_converged"] == 1.0


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
    assert isinstance(panel["stance_by_deadline"], list)

    controls = payload["controls"]
    assert controls["timezone"] == "America/Los_Angeles"
    assert len(controls["process_markers"]) >= 3
