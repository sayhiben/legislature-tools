from __future__ import annotations

import pandas as pd

from testifier_audit.report.render import _build_interactive_chart_payload_v2

REQUIRED_WINDOW_FIELDS = {
    "window_id",
    "start_time",
    "end_time",
    "count",
    "expected",
    "z",
    "q_value",
    "pro_rate",
    "delta_pro_rate",
    "dup_fraction",
    "near_dup_fraction",
    "name_weirdness_mean",
    "support_n",
    "evidence_tier",
    "primary_explanation",
}


def test_window_queue_schema_contains_required_fields_and_valid_tiers() -> None:
    table_map = {
        "artifacts.counts_per_minute": pd.DataFrame(
            {
                "minute_bucket": pd.to_datetime(
                    [
                        "2026-02-01T00:00:00Z",
                        "2026-02-01T00:01:00Z",
                        "2026-02-01T00:02:00Z",
                        "2026-02-01T00:03:00Z",
                    ]
                ),
                "n_total": [8, 10, 11, 9],
                "n_pro": [3, 4, 7, 4],
                "n_con": [5, 6, 4, 5],
                "dup_name_fraction": [0.0, 0.2, 0.25, 0.1],
            }
        ),
        "bursts.burst_significant_windows": pd.DataFrame(
            {
                "window_minutes": [2],
                "start_minute": pd.to_datetime(["2026-02-01T00:01:00Z"]),
                "end_minute": pd.to_datetime(["2026-02-01T00:02:00Z"]),
                "observed_count": [21],
                "expected_count": [9.5],
                "rate_ratio": [2.2],
                "p_value": [0.002],
                "q_value": [0.01],
            }
        ),
        "procon_swings.swing_significant_windows": pd.DataFrame(
            {
                "window_minutes": [2],
                "start_minute": pd.to_datetime(["2026-02-01T00:01:00Z"]),
                "end_minute": pd.to_datetime(["2026-02-01T00:02:00Z"]),
                "n_total": [21],
                "pro_rate": [0.58],
                "delta_pro_rate": [0.2],
                "abs_delta_pro_rate": [0.2],
                "z_score": [2.3],
                "p_value": [0.01],
                "q_value": [0.03],
                "is_low_power": [False],
            }
        ),
        "duplicates_near.cluster_summary": pd.DataFrame(
            {
                "cluster_id": ["cluster_0001"],
                "cluster_size": [4],
                "n_records": [8],
                "n_pro": [4],
                "n_con": [4],
                "first_seen": pd.to_datetime(["2026-02-01T00:00:00Z"]),
                "last_seen": pd.to_datetime(["2026-02-01T00:03:00Z"]),
                "time_span_minutes": [3.0],
            }
        ),
        "rare_names.rarity_by_minute": pd.DataFrame(
            {
                "minute_bucket": pd.to_datetime(
                    ["2026-02-01T00:01:00Z", "2026-02-01T00:02:00Z"]
                ),
                "bucket_minutes": [1, 1],
                "n_total": [10, 11],
                "rarity_mean": [2.5, 2.1],
            }
        ),
    }

    payload = _build_interactive_chart_payload_v2(table_map=table_map, detector_summaries={})
    queue = payload["window_evidence_queue"]

    assert queue
    for row in queue:
        assert REQUIRED_WINDOW_FIELDS.issubset(set(row.keys()))
        assert row["evidence_tier"] in {"high", "medium", "watch"}
        assert row["primary_explanation"] in {
            "data_quality_artifact",
            "legitimate_mobilization",
            "potential_manipulation",
            "mixed",
            "insufficient_evidence",
            "none",
        }
        assert row["window_id"]
        assert row["start_time"]
        assert row["end_time"]
        assert "score_primary_driver" in row
        assert "score_detector_breakdown" in row
        assert "score_signal_breakdown" in row
