from __future__ import annotations

import pandas as pd

from testifier_audit.report.quality_builder import build_data_quality_panel


def test_data_quality_panel_emits_high_value_warnings_and_dedup_metrics() -> None:
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
                "n_total": [40, 42, 38],
                "n_pro": [20, 24, 10],
                "n_con": [20, 18, 28],
                "n_unknown": [0, 6, 0],
                "dup_name_fraction": [0.2, 0.3, 0.15],
                "n_total_dedup": [30, 32, 34],
                "n_pro_dedup": [15, 18, 9],
                "n_con_dedup": [15, 14, 25],
                "dup_name_fraction_dedup": [0.0, 0.0, 0.0],
            }
        ),
        "artifacts.basic_quality": pd.DataFrame(
            {
                "metric": [
                    "rows_total",
                    "missing_name",
                    "unknown_position",
                    "duplicate_ids",
                    "non_monotonic_timestamp_vs_id",
                ],
                "value": [120, 4, 6, 8, 3],
            }
        ),
        "org_anomalies.organization_blank_rate_by_bucket": pd.DataFrame(
            {
                "bucket_start": pd.to_datetime(
                    [
                        "2026-02-01T00:00:00Z",
                        "2026-02-01T00:30:00Z",
                        "2026-02-01T01:00:00Z",
                    ]
                ),
                "bucket_minutes": [30, 30, 30],
                "n_total": [35, 37, 33],
                "blank_org_rate": [0.10, 0.58, 0.11],
            }
        ),
    }
    triage_views = {
        "raw": {"window_evidence_queue": [{"evidence_tier": "high"}]},
        "exact_row_dedup": {"window_evidence_queue": []},
    }

    panel = build_data_quality_panel(table_map=table_map, triage_views=triage_views)

    assert panel["status"] == "warning"
    assert panel["warning_count"] >= 4
    warning_codes = {warning["code"] for warning in panel["warnings"]}
    assert "invalid_or_missing_positions" in warning_codes
    assert "unparsable_or_missing_names" in warning_codes
    assert "duplicate_ids" in warning_codes
    assert "non_monotonic_timestamps_vs_id" in warning_codes
    assert "time_varying_missingness_spikes" in warning_codes

    metrics = panel["raw_vs_dedup_metrics"]
    assert metrics
    metric_names = {row["metric"] for row in metrics}
    assert "total_submissions" in metric_names
    assert "overall_pro_rate" in metric_names
    assert "high_tier_windows" in metric_names
    triage_metrics = panel["triage_raw_vs_dedup_metrics"]
    assert triage_metrics
    assert all(bool(row.get("material_change")) for row in triage_metrics)


def test_data_quality_panel_is_ok_when_no_warnings() -> None:
    table_map = {
        "artifacts.counts_per_minute": pd.DataFrame(
            {
                "minute_bucket": pd.to_datetime(["2026-02-01T00:00:00Z"]),
                "n_total": [30],
                "n_pro": [15],
                "n_con": [15],
                "dup_name_fraction": [0.0],
                "n_total_dedup": [30],
                "n_pro_dedup": [15],
                "n_con_dedup": [15],
                "dup_name_fraction_dedup": [0.0],
            }
        ),
        "artifacts.basic_quality": pd.DataFrame(
            {
                "metric": [
                    "rows_total",
                    "missing_name",
                    "unknown_position",
                    "duplicate_ids",
                    "non_monotonic_timestamp_vs_id",
                ],
                "value": [30, 0, 0, 0, 0],
            }
        ),
    }

    panel = build_data_quality_panel(table_map=table_map)

    assert panel["status"] == "ok"
    assert panel["warning_count"] == 0
    assert isinstance(panel["raw_vs_dedup_metrics"], list)
    assert isinstance(panel["triage_raw_vs_dedup_metrics"], list)


def test_data_quality_panel_org_missingness_spikes_respect_min_cell_threshold() -> None:
    table_map = {
        "artifacts.counts_per_minute": pd.DataFrame(
            {
                "minute_bucket": pd.to_datetime(
                    [
                        "2026-02-01T00:00:00Z",
                        "2026-02-01T00:01:00Z",
                    ]
                ),
                "n_total": [8, 9],
                "n_pro": [4, 4],
                "n_con": [4, 5],
                "dup_name_fraction": [0.0, 0.0],
                "n_total_dedup": [8, 9],
                "n_pro_dedup": [4, 4],
                "n_con_dedup": [4, 5],
                "dup_name_fraction_dedup": [0.0, 0.0],
            }
        ),
        "artifacts.basic_quality": pd.DataFrame(
            {
                "metric": [
                    "rows_total",
                    "missing_name",
                    "unknown_position",
                    "duplicate_ids",
                    "non_monotonic_timestamp_vs_id",
                ],
                "value": [17, 0, 0, 0, 0],
            }
        ),
        "org_anomalies.organization_blank_rate_by_bucket": pd.DataFrame(
            {
                "bucket_start": pd.to_datetime(
                    [
                        "2026-02-01T00:00:00Z",
                        "2026-02-01T00:30:00Z",
                        "2026-02-01T01:00:00Z",
                    ]
                ),
                "bucket_minutes": [30, 30, 30],
                "n_total": [8, 9, 8],
                "blank_org_rate": [0.10, 0.58, 0.11],
            }
        ),
    }

    default_panel = build_data_quality_panel(table_map=table_map)
    assert all(
        warning["code"] != "time_varying_missingness_spikes"
        for warning in default_panel["warnings"]
    )

    lowered_threshold_panel = build_data_quality_panel(
        table_map=table_map,
        min_cell_n_for_rates=5,
    )
    assert any(
        warning["code"] == "time_varying_missingness_spikes"
        for warning in lowered_threshold_panel["warnings"]
    )
