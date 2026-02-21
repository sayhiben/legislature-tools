from __future__ import annotations

import pandas as pd

from testifier_audit.report.render import _build_interactive_chart_payload_v2


def test_triage_summary_contract_contains_required_phase2_fields() -> None:
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
                "n_total": [10, 8, 12],
                "n_pro": [4, 3, 8],
                "n_con": [6, 5, 4],
                "dup_name_fraction": [0.1, 0.0, 0.25],
                "pro_rate": [0.4, 0.375, 0.6666666667],
            }
        ),
        "bursts.burst_significant_windows": pd.DataFrame(
            {
                "window_minutes": [2],
                "start_minute": pd.to_datetime(["2026-02-01T00:01:00Z"]),
                "end_minute": pd.to_datetime(["2026-02-01T00:02:00Z"]),
                "observed_count": [20],
                "expected_count": [10],
                "rate_ratio": [2.0],
                "p_value": [0.005],
                "q_value": [0.01],
            }
        ),
        "procon_swings.swing_significant_windows": pd.DataFrame(
            {
                "window_minutes": [2],
                "start_minute": pd.to_datetime(["2026-02-01T00:01:00Z"]),
                "end_minute": pd.to_datetime(["2026-02-01T00:02:00Z"]),
                "n_total": [20],
                "pro_rate": [0.55],
                "delta_pro_rate": [0.18],
                "abs_delta_pro_rate": [0.18],
                "z_score": [2.1],
                "p_value": [0.03],
                "q_value": [0.04],
                "is_low_power": [False],
            }
        ),
        "duplicates_exact.top_repeated_names": pd.DataFrame(
            {
                "display_name": ["Doe, Jane"],
                "canonical_name": ["JANE DOE"],
                "n": [4],
                "n_pro": [2],
                "n_con": [2],
                "time_span_minutes": [15.0],
            }
        ),
        "duplicates_near.cluster_summary": pd.DataFrame(
            {
                "cluster_id": ["cluster_0001"],
                "cluster_size": [3],
                "n_records": [6],
                "n_pro": [3],
                "n_con": [3],
                "first_seen": pd.to_datetime(["2026-02-01T00:00:00Z"]),
                "last_seen": pd.to_datetime(["2026-02-01T00:03:00Z"]),
                "time_span_minutes": [3.0],
            }
        ),
        "off_hours.off_hours_summary": pd.DataFrame(
            {
                "off_hours": [12],
                "on_hours": [18],
                "off_hours_ratio": [0.4],
                "off_hours_pro_rate": [0.45],
                "on_hours_pro_rate": [0.55],
                "chi_square_p_value": [0.2],
            }
        ),
    }

    payload = _build_interactive_chart_payload_v2(table_map=table_map, detector_summaries={})

    summary = payload["triage_summary"]
    assert summary["total_submissions"] == 30
    assert summary["date_range_start"]
    assert summary["date_range_end"]
    assert summary["overall_pro_rate"] is not None
    assert summary["overall_con_rate"] is not None
    assert isinstance(summary["top_burst_windows"], list)
    assert isinstance(summary["top_swing_windows"], list)
    assert isinstance(summary["top_repeated_names"], list)
    assert isinstance(summary["top_near_dup_clusters"], list)
    assert isinstance(summary["off_hours_summary"], dict)
    assert summary["queue_counts"]["window"] >= 1
