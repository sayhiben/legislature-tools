from __future__ import annotations

import pandas as pd

from testifier_audit.detectors.vrdb_collision_evidence import VrdbCollisionEvidenceDetector


def test_vrdb_collision_evidence_disables_when_artifacts_are_missing() -> None:
    detector = VrdbCollisionEvidenceDetector(bucket_minutes=[60])
    detector._default_probability_candidates = []
    detector._default_backoff_candidates = []

    df = pd.DataFrame(
        {
            "full_name_key": ["alpha"],
            "timestamp": [pd.Timestamp("2026-02-01T00:00:00Z")],
        }
    )
    result = detector.run(df=df, features={})

    assert result.summary["enabled"] is False
    assert result.summary["active"] is False
    assert result.summary["reason"] == "missing_probability_artifact"
    assert result.tables["slice_metrics"].empty
    assert result.tables["top_overrun_names"].empty


def test_vrdb_collision_evidence_emits_bucket_metrics_and_overrun_rows(tmp_path) -> None:
    probability_rows = pd.DataFrame(
        {
            "name_key": ["alpha", "beta", "gamma"],
            "name_key_type": ["full_name_key", "full_name_key", "full_name_key"],
            "count": [500, 490, 10],
            "probability": [0.50, 0.49, 0.01],
            "denominator": [1000, 1000, 1000],
            "geo_level": ["state", "state", "state"],
            "geo_value": ["WA", "WA", "WA"],
            "baseline_variant": ["all_registrants", "all_registrants", "all_registrants"],
            "vrdb_version": ["vrdb_extract_v4", "vrdb_extract_v4", "vrdb_extract_v4"],
            "normalization_version": ["name_norm_v1", "name_norm_v1", "name_norm_v1"],
        }
    )
    backoff_rows = pd.DataFrame(
        {
            "baseline_variant": ["all_registrants"],
            "requested_geo_level": ["state"],
            "requested_geo_value": ["WA"],
            "effective_geo_level": ["state"],
            "effective_geo_value": ["WA"],
            "fallback_steps": [0],
            "backoff_reason": [""],
            "effective_denominator": [1000],
        }
    )
    probability_path = tmp_path / "probability.csv"
    backoff_path = tmp_path / "backoff.csv"
    probability_rows.to_csv(probability_path, index=False)
    backoff_rows.to_csv(backoff_path, index=False)

    detector = VrdbCollisionEvidenceDetector(
        bucket_minutes=[60],
        monte_carlo_draws=64,
        top_name_limit=3,
        probability_rows_path=str(probability_path),
        backoff_rows_path=str(backoff_path),
    )
    df = pd.DataFrame(
        {
            "full_name_key": ["gamma", "gamma", "gamma", "gamma", "gamma", "beta", "alpha"],
            "timestamp": pd.to_datetime(
                [
                    "2026-02-01T00:02:00Z",
                    "2026-02-01T00:10:00Z",
                    "2026-02-01T00:20:00Z",
                    "2026-02-01T00:31:00Z",
                    "2026-02-01T01:05:00Z",
                    "2026-02-01T01:11:00Z",
                    "2026-02-01T01:24:00Z",
                ]
            ),
        }
    )

    result = detector.run(df=df, features={})
    metrics = result.tables["slice_metrics"]
    overrun = result.tables["top_overrun_names"]

    assert result.summary["enabled"] is True
    assert result.summary["active"] is True
    assert result.summary["reason"] == ""
    assert result.summary["slice_metrics_rows"] == len(metrics)
    assert result.summary["top_overrun_rows"] == len(overrun)
    assert not metrics.empty
    assert "full_hearing" in set(metrics["slice_id"].astype(str))
    assert (metrics["evidence_family"] == "vrdb_collision_null").all()
    assert set(metrics["bucket_minutes"].astype(int)).issuperset({0, 60})
    assert not overrun.empty
    assert (overrun["rank"].astype(int) <= 3).all()
    assert (overrun["overrun_count"].astype(float) > 0).any()
    assert overrun["bucket_start"].notna().any()
