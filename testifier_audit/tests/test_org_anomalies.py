from __future__ import annotations

import pandas as pd
import pytest

from testifier_audit.detectors.org_anomalies import OrganizationAnomaliesDetector


def test_org_anomalies_emits_blank_org_rate_tables_for_standard_buckets() -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "organization": ["", "Org A", "", "Org B", "", ""],
            "position_normalized": ["Pro", "Pro", "Con", "Con", "Pro", "Con"],
            "timestamp": pd.to_datetime(
                [
                    "2026-02-03 10:00:00",
                    "2026-02-03 10:01:00",
                    "2026-02-03 10:02:00",
                    "2026-02-03 10:03:00",
                    "2026-02-03 10:04:00",
                    "2026-02-03 10:05:00",
                ]
            ),
            "minute_bucket": pd.to_datetime(
                [
                    "2026-02-03 10:00:00",
                    "2026-02-03 10:01:00",
                    "2026-02-03 10:02:00",
                    "2026-02-03 10:03:00",
                    "2026-02-03 10:04:00",
                    "2026-02-03 10:05:00",
                ]
            ),
        }
    )

    detector = OrganizationAnomaliesDetector()
    result = detector.run(df=df, features={})

    by_bucket = result.tables["organization_blank_rate_by_bucket"]
    by_bucket_position = result.tables["organization_blank_rate_by_bucket_position"]
    by_bucket_summary = result.tables["organization_blank_rate_summary"]

    assert not by_bucket.empty
    assert not by_bucket_position.empty
    assert not by_bucket_summary.empty
    assert set(by_bucket["bucket_minutes"].astype(int).unique()) == {1, 5, 15, 30, 60, 120, 240}
    assert set(by_bucket_summary["bucket_minutes"].astype(int).unique()) == {
        1,
        5,
        15,
        30,
        60,
        120,
        240,
    }
    assert "blank_org_rate_wilson_low" in by_bucket.columns
    assert "blank_org_rate_wilson_high" in by_bucket.columns
    assert "is_low_power" in by_bucket.columns
    assert "blank_org_rate_wilson_half_width" in by_bucket_position.columns
    assert "is_low_power" in by_bucket_position.columns
    assert "n_low_power_buckets" in by_bucket_summary.columns

    one_minute = (
        by_bucket[by_bucket["bucket_minutes"] == 1]
        .sort_values("bucket_start")
        .reset_index(drop=True)
    )
    assert one_minute.loc[0, "blank_org_rate"] == pytest.approx(1.0)
    assert one_minute.loc[1, "blank_org_rate"] == pytest.approx(0.0)
    assert one_minute.loc[0, "pro_blank_org_rate"] == pytest.approx(1.0)
    assert bool(one_minute["is_low_power"].all())

    assert result.summary["blank_org_ratio"] == pytest.approx(4 / 6)
    assert result.summary["n_low_power_blank_org_buckets"] > 0
