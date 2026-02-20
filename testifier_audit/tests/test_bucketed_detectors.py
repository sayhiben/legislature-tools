from __future__ import annotations

import pandas as pd

from testifier_audit.detectors.duplicates_exact import DuplicatesExactDetector
from testifier_audit.detectors.sortedness import SortednessDetector


def test_duplicates_exact_emits_multi_bucket_duplicate_tables() -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6, 7],
            "canonical_name": [
                "DOE|JANE",
                "DOE|JANE",
                "DOE|JANE",
                "DOE|JANE",
                "SMITH|JOHN",
                "SMITH|JOHN",
                "BROWN|AVA",
            ],
            "name_display": [
                "DOE, JANE",
                "DOE, JANE",
                "DOE, JANE",
                "DOE, JANE",
                "SMITH, JOHN",
                "SMITH, JOHN",
                "BROWN, AVA",
            ],
            "position_normalized": ["Pro", "Con", "Pro", "Con", "Pro", "Pro", "Con"],
            "timestamp": pd.to_datetime(
                [
                    "2026-02-01 00:01:00",
                    "2026-02-01 00:01:00",
                    "2026-02-01 00:03:00",
                    "2026-02-01 00:16:00",
                    "2026-02-01 00:05:00",
                    "2026-02-01 00:06:00",
                    "2026-02-01 01:00:00",
                ]
            ),
            "minute_bucket": pd.to_datetime(
                [
                    "2026-02-01 00:01:00",
                    "2026-02-01 00:01:00",
                    "2026-02-01 00:03:00",
                    "2026-02-01 00:16:00",
                    "2026-02-01 00:05:00",
                    "2026-02-01 00:06:00",
                    "2026-02-01 01:00:00",
                ]
            ),
        }
    )

    detector = DuplicatesExactDetector(top_n=20, bucket_minutes=[1, 5, 15])
    result = detector.run(df=df, features={})

    repeated_same_bucket = result.tables["repeated_same_bucket"]
    assert not repeated_same_bucket.empty
    assert {1, 5, 15}.issubset(set(repeated_same_bucket["bucket_minutes"].astype(int).unique()))
    assert "bucket_end" in repeated_same_bucket.columns

    repeated_same_minute = result.tables["repeated_same_minute"]
    assert not repeated_same_minute.empty
    assert "minute_bucket" in repeated_same_minute.columns

    bucket_summary = result.tables["repeated_same_bucket_summary"]
    assert not bucket_summary.empty
    assert {1, 5, 15}.issubset(set(bucket_summary["bucket_minutes"].astype(int).unique()))


def test_sortedness_emits_bucket_ordering_across_multiple_window_sizes() -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "name_display": [
                "ALPHA, ANN",
                "BETA, BOB",
                "GAMMA, GIA",
                "ZETA, ZED",
                "ETA, EON",
                "THETA, TOM",
            ],
            "last": ["ALPHA", "BETA", "GAMMA", "ZETA", "ETA", "THETA"],
            "first": ["ANN", "BOB", "GIA", "ZED", "EON", "TOM"],
            "timestamp": pd.to_datetime(
                [
                    "2026-02-01 00:01:00",
                    "2026-02-01 00:01:00",
                    "2026-02-01 00:01:00",
                    "2026-02-01 00:02:00",
                    "2026-02-01 00:02:00",
                    "2026-02-01 00:02:00",
                ]
            ),
            "minute_bucket": pd.to_datetime(
                [
                    "2026-02-01 00:01:00",
                    "2026-02-01 00:01:00",
                    "2026-02-01 00:01:00",
                    "2026-02-01 00:02:00",
                    "2026-02-01 00:02:00",
                    "2026-02-01 00:02:00",
                ]
            ),
        }
    )

    detector = SortednessDetector(bucket_minutes=[1, 5])
    result = detector.run(df=df, features={})

    bucket_ordering = result.tables["bucket_ordering"]
    assert not bucket_ordering.empty
    assert {1, 5}.issubset(set(bucket_ordering["bucket_minutes"].astype(int).unique()))

    minute_ordering = result.tables["minute_ordering"]
    assert not minute_ordering.empty
    assert "minute_bucket" in minute_ordering.columns

    bucket_summary = result.tables["bucket_ordering_summary"]
    assert not bucket_summary.empty
    assert {1, 5}.issubset(set(bucket_summary["bucket_minutes"].astype(int).unique()))
