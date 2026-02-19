from __future__ import annotations

import pandas as pd
import pytest

from testifier_audit.detectors.duplicates_near import DuplicatesNearDetector


def _base_near_duplicate_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "canonical_name": [
                "DOE|JANE",
                "DOE|JANEE",
                "DOE|JANET",
                "DOE|JANE",
                "DOE|JANEE",
                "SMITH|ALAN",
            ],
            "name_display": [
                "DOE, JANE",
                "DOE, JANEE",
                "DOE, JANET",
                "DOE, JANE",
                "DOE, JANEE",
                "SMITH, ALAN",
            ],
            "last": ["DOE", "DOE", "DOE", "DOE", "DOE", "SMITH"],
            "first": ["JANE", "JANEE", "JANET", "JANE", "JANEE", "ALAN"],
            "first_canonical": ["JANE", "JANEE", "JANET", "JANE", "JANEE", "ALAN"],
            "position_normalized": ["Pro", "Con", "Pro", "Pro", "Con", "Pro"],
            "timestamp": pd.to_datetime(
                [
                    "2026-02-01 12:00:00",
                    "2026-02-01 12:01:00",
                    "2026-02-01 12:02:00",
                    "2026-02-01 12:03:00",
                    "2026-02-01 12:04:00",
                    "2026-02-01 12:05:00",
                ]
            ),
        }
    )


def test_duplicates_near_raises_when_required_columns_missing() -> None:
    detector = DuplicatesNearDetector(similarity_threshold=90, max_candidates_per_block=100)
    with pytest.raises(ValueError, match="Missing required columns"):
        detector.run(df=pd.DataFrame({"canonical_name": ["DOE|JANE"]}), features={})


def test_duplicates_near_skips_oversized_blocks() -> None:
    df = _base_near_duplicate_df()
    detector = DuplicatesNearDetector(similarity_threshold=99, max_candidates_per_block=2)
    result = detector.run(df=df, features={})

    assert result.summary["n_similarity_edges"] == 0
    assert result.summary["n_clusters"] == 0
    assert result.summary["n_skipped_blocks"] >= 1
    assert result.tables["skipped_blocks"].iloc[0]["reason"] == "exceeds_max_candidates_per_block"
    assert result.tables["cluster_summary"].empty
    assert result.tables["cluster_members"].empty


def test_duplicates_near_builds_clusters_and_membership() -> None:
    df = _base_near_duplicate_df()
    detector = DuplicatesNearDetector(similarity_threshold=90, max_candidates_per_block=50)
    result = detector.run(df=df, features={})

    assert result.summary["n_similarity_edges"] > 0
    assert result.summary["n_clusters"] >= 1
    assert result.summary["max_cluster_size"] >= 2
    assert not result.tables["similarity_edges"].empty
    assert not result.tables["cluster_summary"].empty
    assert not result.tables["cluster_members"].empty
    assert "cluster_id" in result.tables["cluster_members"].columns
    assert "time_span_minutes" in result.tables["cluster_summary"].columns
