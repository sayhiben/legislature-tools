from __future__ import annotations

import pandas as pd
import pytest

from testifier_audit.detectors.voter_registry_match import VoterRegistryMatchDetector


def test_voter_registry_match_detector_disabled_returns_inactive_summary() -> None:
    detector = VoterRegistryMatchDetector(enabled=False, db_url="postgresql://unused")
    result = detector.run(df=pd.DataFrame(), features={})

    assert result.detector == "voter_registry_match"
    assert result.summary["active"] is False
    assert result.summary["reason"] == "voter_registry_match_disabled"
    assert set(result.tables) == {
        "match_overview",
        "match_by_position",
        "match_by_bucket",
        "match_by_bucket_position",
        "matched_names",
        "unmatched_names",
    }


def test_voter_registry_match_detector_emits_expected_rates(monkeypatch) -> None:
    df = pd.DataFrame(
        {
            "canonical_name": [
                "DOE|JANE",
                "DOE|JANE",
                "SMITH|JOHN",
                "BROWN|AVA",
                "CHANG|MEI",
                "CHANG|MEI",
            ],
            "position_normalized": ["Pro", "Con", "Pro", "Con", "Pro", "Con"],
            "minute_bucket": pd.to_datetime(
                [
                    "2026-02-01 00:05:00",
                    "2026-02-01 00:06:00",
                    "2026-02-01 00:10:00",
                    "2026-02-01 00:35:00",
                    "2026-02-01 00:40:00",
                    "2026-02-01 00:42:00",
                ]
            ),
        }
    )

    monkeypatch.setattr(
        "testifier_audit.detectors.voter_registry_match.fetch_matching_voter_names",
        lambda **_kwargs: pd.DataFrame(
            {
                "canonical_name": ["DOE|JANE", "CHANG|MEI"],
                "n_registry_rows": [1, 1],
            }
        ),
    )
    monkeypatch.setattr(
        "testifier_audit.detectors.voter_registry_match.count_registry_rows",
        lambda **_kwargs: 2,
    )

    detector = VoterRegistryMatchDetector(
        enabled=True,
        db_url="postgresql://user:pass@localhost:5432/legislature",
        table_name="voter_registry",
        bucket_minutes=30,
        active_only=True,
    )
    result = detector.run(df=df, features={})

    assert result.summary["active"] is True
    assert result.summary["n_records"] == 6
    assert result.summary["n_matches"] == 4
    assert result.summary["n_unmatched"] == 2
    assert result.summary["match_rate"] == pytest.approx(4 / 6)

    by_position = result.tables["match_by_position"].set_index("position_normalized")
    assert by_position.loc["Pro", "n_total"] == 3
    assert by_position.loc["Con", "n_total"] == 3
    assert by_position.loc["Pro", "match_rate"] == pytest.approx(2 / 3)
    assert by_position.loc["Con", "match_rate"] == pytest.approx(2 / 3)
    assert "match_rate_wilson_low" in by_position.columns
    assert "match_rate_wilson_high" in by_position.columns
    assert "is_low_power" in by_position.columns

    by_bucket = result.tables["match_by_bucket"].sort_values("bucket_start").reset_index(drop=True)
    assert len(by_bucket) == 2
    assert set(by_bucket["bucket_minutes"].astype(int).tolist()) == {30}
    assert by_bucket.loc[0, "n_total"] == 3
    assert by_bucket.loc[0, "n_matches"] == 2
    assert by_bucket.loc[0, "match_rate"] == pytest.approx(2 / 3)
    assert by_bucket.loc[0, "pro_match_rate"] == pytest.approx(1 / 2)
    assert by_bucket.loc[0, "con_match_rate"] == pytest.approx(1.0)

    assert by_bucket.loc[1, "n_total"] == 3
    assert by_bucket.loc[1, "n_matches"] == 2
    assert by_bucket.loc[1, "match_rate"] == pytest.approx(2 / 3)
    assert by_bucket.loc[1, "pro_match_rate"] == pytest.approx(1.0)
    assert by_bucket.loc[1, "con_match_rate"] == pytest.approx(1 / 2)
    assert "match_rate_wilson_low" in by_bucket.columns
    assert "match_rate_wilson_high" in by_bucket.columns
    assert "pro_match_rate_wilson_low" in by_bucket.columns
    assert "con_match_rate_wilson_low" in by_bucket.columns
    assert bool(by_bucket["is_low_power"].all())
    assert bool(by_bucket["pro_is_low_power"].all())
    assert bool(by_bucket["con_is_low_power"].all())

    matched_names = result.tables["matched_names"]
    assert set(matched_names["canonical_name"]) == {"DOE|JANE", "CHANG|MEI"}
    assert "n_registry_rows" in matched_names.columns
    assert result.summary["n_low_power_match_buckets"] == 2


def test_voter_registry_match_detector_supports_multiple_bucket_windows(monkeypatch) -> None:
    df = pd.DataFrame(
        {
            "canonical_name": ["DOE|JANE"] * 6 + ["SMITH|JOHN"] * 6,
            "position_normalized": ["Pro", "Con"] * 6,
            "minute_bucket": pd.to_datetime(
                [
                    "2026-02-01 00:00:00",
                    "2026-02-01 00:01:00",
                    "2026-02-01 00:02:00",
                    "2026-02-01 00:03:00",
                    "2026-02-01 00:04:00",
                    "2026-02-01 00:05:00",
                    "2026-02-01 00:10:00",
                    "2026-02-01 00:11:00",
                    "2026-02-01 00:12:00",
                    "2026-02-01 00:13:00",
                    "2026-02-01 00:14:00",
                    "2026-02-01 00:15:00",
                ]
            ),
        }
    )

    monkeypatch.setattr(
        "testifier_audit.detectors.voter_registry_match.fetch_matching_voter_names",
        lambda **_kwargs: pd.DataFrame(
            {
                "canonical_name": ["DOE|JANE", "SMITH|JOHN"],
                "n_registry_rows": [1, 1],
            }
        ),
    )
    monkeypatch.setattr(
        "testifier_audit.detectors.voter_registry_match.count_registry_rows",
        lambda **_kwargs: 2,
    )

    detector = VoterRegistryMatchDetector(
        enabled=True,
        db_url="postgresql://user:pass@localhost:5432/legislature",
        table_name="voter_registry",
        bucket_minutes=[1, 5, 15],
        active_only=True,
    )
    result = detector.run(df=df, features={})

    by_bucket = result.tables["match_by_bucket"]
    assert not by_bucket.empty
    assert set(by_bucket["bucket_minutes"].astype(int).unique()) == {1, 5, 15}
    assert result.summary["bucket_minutes"] == [1, 5, 15]
