from __future__ import annotations

import pandas as pd

from testifier_audit.detectors.composite_score import CompositeScoreDetector


def test_composite_score_emits_evidence_bundle_table() -> None:
    minute_bucket = pd.date_range("2026-02-01 12:00:00", periods=6, freq="min")
    counts = pd.DataFrame(
        {
            "minute_bucket": minute_bucket,
            "n_total": [1, 2, 10, 9, 2, 1],
            "dup_name_fraction": [0.0, 0.0, 0.5, 0.4, 0.0, 0.0],
            "pro_rate": [0.5, 0.5, 0.9, 0.85, 0.4, 0.5],
        }
    )
    bursts = pd.DataFrame(
        {
            "start_minute": [minute_bucket[2]],
            "end_minute": [minute_bucket[3]],
            "q_value": [0.001],
        }
    )
    swings = pd.DataFrame(
        {
            "start_minute": [minute_bucket[2]],
            "end_minute": [minute_bucket[4]],
            "q_value": [0.005],
        }
    )
    rarity_windows = pd.DataFrame({"minute_bucket": [minute_bucket[3]]})
    changepoints = pd.DataFrame({"change_minute": [minute_bucket[2]]})

    detector = CompositeScoreDetector()
    result = detector.run(
        df=pd.DataFrame(),
        features={
            "counts_per_minute": counts,
            "bursts.burst_significant_windows": bursts,
            "procon_swings.swing_significant_windows": swings,
            "rare_names.unique_ratio_windows": pd.DataFrame(),
            "rare_names.rarity_high_windows": rarity_windows,
            "changepoints.volume_changepoints": changepoints,
            "changepoints.pro_rate_changepoints": pd.DataFrame(),
        },
    )

    evidence = result.tables["evidence_bundle_windows"]
    assert not evidence.empty
    assert "evidence_count" in evidence.columns
    assert "evidence_flags" in evidence.columns
