from __future__ import annotations

from typing import Any

import pandas as pd

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
                "match_rate_wilson_low": [0.75, 0.64],
                "match_rate_wilson_high": [0.98, 0.94],
                "pro_match_rate": [0.86, 0.8],
                "con_match_rate": [0.94, 0.87],
                "is_low_power": [False, False],
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

    assert payload["version"] == 2
    assert isinstance(payload["analysis_catalog"], list)
    assert isinstance(payload["charts"], dict)
    assert isinstance(payload["controls"], dict)
    assert isinstance(payload["chart_legend_docs"], dict)
    assert isinstance(payload["triage_summary"], dict)
    assert isinstance(payload["window_evidence_queue"], list)
    assert isinstance(payload["record_evidence_queue"], list)
    assert isinstance(payload["cluster_evidence_queue"], list)

    ids = {entry["id"] for entry in payload["analysis_catalog"]}
    assert EXPECTED_ANALYSES.issubset(ids)

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

    controls = payload["controls"]
    assert "global_bucket_options" in controls
    assert "zoom_sync_groups" in controls
    assert controls["timezone"] == "UTC"
    assert controls["timezone_label"] == "UTC"
    assert isinstance(controls["evidence_taxonomy"], list)
    assert controls["dedup_modes"] == ["raw", "exact_row_dedup", "side_by_side"]
    assert "absolute_time" in controls["zoom_sync_groups"]
    assert isinstance(controls["zoom_sync_groups"]["absolute_time"], list)
    assert 30 in controls["global_bucket_options"]
    assert 240 in controls["global_bucket_options"]

    catalog_by_id = {entry["id"]: entry for entry in payload["analysis_catalog"]}
    assert catalog_by_id["baseline_profile"]["bucket_options"] == EXPECTED_BASELINE_BUCKETS


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
    assert set(catalog.keys()) == EXPECTED_ANALYSES

    voter_entry = catalog["voter_registry_match"]
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
