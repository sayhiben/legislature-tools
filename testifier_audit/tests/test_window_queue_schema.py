from __future__ import annotations

import pandas as pd

from testifier_audit.report.render import _build_interactive_chart_payload_v2

REQUIRED_TRIAGE_SUMMARY_FIELDS = {
    "total_submissions",
    "date_range_start",
    "date_range_end",
    "overall_pro_rate",
    "overall_con_rate",
    "top_burst_windows",
    "top_swing_windows",
    "top_repeated_names",
    "off_hours_summary",
    "queue_counts",
    "window_tier_counts",
    "lens",
}


def test_triage_summary_schema_contains_required_fields() -> None:
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
        "duplicates_exact.top_repeated_names": pd.DataFrame(
            {
                "display_name": ["Doe, Jane"],
                "canonical_name": ["DOE|JANE"],
                "n": [6],
                "n_pro": [4],
                "n_con": [2],
                "time_span_minutes": [15],
            }
        ),
    }

    payload = _build_interactive_chart_payload_v2(table_map=table_map, detector_summaries={})
    summary = payload["triage_summary"]

    assert REQUIRED_TRIAGE_SUMMARY_FIELDS.issubset(set(summary.keys()))
    assert summary["queue_counts"] == {"window": 0, "record": 0, "cluster": 0}
    assert summary["window_tier_counts"] == {"high": 0, "medium": 0, "watch": 0}
