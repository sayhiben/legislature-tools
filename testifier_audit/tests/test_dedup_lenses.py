from __future__ import annotations

import pandas as pd

from testifier_audit.features.dedup import (
    DEFAULT_DEDUP_MODE,
    counts_columns_for_mode,
    ensure_dedup_count_columns,
    normalize_dedup_mode,
)
from testifier_audit.report.triage_builder import build_investigation_views


def test_dedup_mode_helpers_normalize_and_map_columns() -> None:
    assert normalize_dedup_mode("RAW", default=DEFAULT_DEDUP_MODE) == "raw"
    assert normalize_dedup_mode("bad-value", default=DEFAULT_DEDUP_MODE) == DEFAULT_DEDUP_MODE

    raw_columns = counts_columns_for_mode("raw")
    dedup_columns = counts_columns_for_mode("exact_row_dedup")
    assert raw_columns["n_total"] == "n_total"
    assert dedup_columns["n_total"] == "n_total_dedup"


def test_ensure_dedup_count_columns_populates_missing_fields() -> None:
    counts = pd.DataFrame(
        {
            "minute_bucket": pd.to_datetime(
                ["2026-02-01T00:00:00Z", "2026-02-01T00:01:00Z"],
            ),
            "n_total": [10, 8],
            "n_pro": [6, 3],
            "n_con": [4, 5],
            "n_unique_names": [8, 7],
            "dup_name_fraction": [0.2, 0.125],
        }
    )
    enriched = ensure_dedup_count_columns(counts)

    assert "n_total_dedup" in enriched.columns
    assert "pro_rate_dedup" in enriched.columns
    assert "dup_name_fraction_dedup" in enriched.columns
    assert float(enriched["n_total_dedup"].sum()) == 15.0


def test_investigation_views_expose_raw_dedup_and_side_by_side_lenses() -> None:
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
                "n_total": [10, 12, 8],
                "n_pro": [6, 8, 2],
                "n_con": [4, 4, 6],
                "dup_name_fraction": [0.2, 0.25, 0.125],
                "n_total_dedup": [8, 9, 7],
                "n_pro_dedup": [5, 6, 2],
                "n_con_dedup": [3, 3, 5],
                "dup_name_fraction_dedup": [0.0, 0.0, 0.0],
            }
        ),
        "bursts.burst_significant_windows": pd.DataFrame(
            {
                "start_minute": pd.to_datetime(["2026-02-01T00:01:00Z"]),
                "end_minute": pd.to_datetime(["2026-02-01T00:02:00Z"]),
                "observed_count": [20],
                "expected_count": [10],
                "rate_ratio": [2.0],
                "p_value": [0.01],
                "q_value": [0.02],
                "window_minutes": [2],
            }
        ),
    }

    views = build_investigation_views(table_map=table_map)

    assert set(views.keys()) == {"raw", "exact_row_dedup", "side_by_side"}
    assert views["raw"]["triage_summary"]["total_submissions"] == 30
    assert views["exact_row_dedup"]["triage_summary"]["total_submissions"] == 24

    side = views["side_by_side"]
    summary = side["triage_summary"]
    assert summary["lens"] == "side_by_side"
    assert summary["total_submissions_raw"] == 30
    assert summary["total_submissions_exact_row_dedup"] == 24
    assert summary["total_submissions_delta"] == -6.0

    window_rows = side["window_evidence_queue"]
    assert window_rows
    row = window_rows[0]
    assert "count_raw" in row
    assert "count_exact_row_dedup" in row
    assert "count_delta" in row
