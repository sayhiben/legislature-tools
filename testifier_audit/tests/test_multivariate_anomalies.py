from __future__ import annotations

import pandas as pd

from testifier_audit.detectors.multivariate_anomalies import MultivariateAnomaliesDetector


def test_multivariate_anomalies_detector_scores_buckets_and_flags_outlier() -> None:
    minute_bucket = pd.date_range("2026-02-01 00:00:00", periods=240, freq="min")
    n_total = [25] * 240
    n_pro = [12] * 240
    dup_name_fraction = [0.03] * 240

    for idx in range(150, 165):
        n_total[idx] = 160
        n_pro[idx] = 148
        dup_name_fraction[idx] = 0.45

    counts = pd.DataFrame(
        {
            "minute_bucket": minute_bucket,
            "n_total": n_total,
            "n_pro": n_pro,
            "n_con": [total - pro for total, pro in zip(n_total, n_pro)],
            "dup_name_fraction": dup_name_fraction,
        }
    )
    df = pd.DataFrame(
        {
            "id": list(range(len(minute_bucket))),
            "minute_bucket": minute_bucket,
            "organization": [
                "" if 150 <= idx < 165 else "Org A" for idx in range(len(minute_bucket))
            ],
        }
    )

    detector = MultivariateAnomaliesDetector(
        enabled=True,
        bucket_minutes=15,
        contamination=0.10,
        min_bucket_total=120,
        top_n=10,
        random_seed=7,
    )
    result = detector.run(df=df, features={"counts_per_minute": counts})

    assert result.detector == "multivariate_anomalies"
    assert result.summary["active"] is True
    assert result.summary["n_buckets_model_eligible"] >= 8
    assert result.summary["n_anomaly_buckets"] >= 1

    bucket_scores = result.tables["bucket_anomaly_scores"]
    assert not bucket_scores.empty
    assert "anomaly_score" in bucket_scores.columns
    assert "is_model_eligible" in bucket_scores.columns
    assert bucket_scores["anomaly_score"].notna().any()

    top = result.tables["top_bucket_anomalies"]
    assert not top.empty
    assert "anomaly_score_percentile" in top.columns


def test_multivariate_anomalies_detector_disabled_returns_inactive_summary() -> None:
    detector = MultivariateAnomaliesDetector(enabled=False)
    result = detector.run(df=pd.DataFrame(), features={"counts_per_minute": pd.DataFrame()})

    assert result.summary["enabled"] is False
    assert result.summary["active"] is False
    assert result.summary["reason"] == "multivariate_anomalies_disabled"
    assert result.tables["bucket_anomaly_scores"].empty


def test_multivariate_anomalies_detector_supports_multiple_bucket_windows() -> None:
    minute_bucket = pd.date_range("2026-02-01 00:00:00", periods=360, freq="min")
    counts = pd.DataFrame(
        {
            "minute_bucket": minute_bucket,
            "n_total": [40 + (12 if idx % 37 == 0 else 0) for idx in range(len(minute_bucket))],
            "n_pro": [20 + (8 if idx % 53 == 0 else 0) for idx in range(len(minute_bucket))],
            "n_con": [20 for _ in range(len(minute_bucket))],
            "dup_name_fraction": [
                0.03 + (0.08 if idx % 47 == 0 else 0.0) for idx in range(len(minute_bucket))
            ],
        }
    )
    counts["n_con"] = counts["n_total"] - counts["n_pro"]

    df = pd.DataFrame(
        {
            "id": list(range(len(minute_bucket))),
            "minute_bucket": minute_bucket,
            "organization": ["" if idx % 41 == 0 else "Org A" for idx in range(len(minute_bucket))],
        }
    )

    detector = MultivariateAnomaliesDetector(
        enabled=True,
        bucket_minutes=[15, 30],
        contamination=0.10,
        min_bucket_total=60,
        top_n=10,
        random_seed=7,
    )
    result = detector.run(df=df, features={"counts_per_minute": counts})

    bucket_scores = result.tables["bucket_anomaly_scores"]
    assert not bucket_scores.empty
    assert set(bucket_scores["bucket_minutes"].astype(int).unique()) == {15, 30}
    assert result.summary["bucket_minutes"] == [15, 30]
