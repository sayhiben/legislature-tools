from __future__ import annotations

import pandas as pd
import pytest

from testifier_audit.detectors.voter_registry_match import VoterRegistryMatchDetector


_EXPECTED_TABLES = {
    "linkage_overview",
    "linkage_by_position_rows",
    "linkage_by_position_unique",
    "position_pairwise_tests",
    "sensitivity_modes",
    "match_assignments",
    "match_by_bucket",
    "match_by_bucket_position",
    "unmatched_names",
}


def test_voter_registry_match_detector_disabled_returns_inactive_summary() -> None:
    detector = VoterRegistryMatchDetector(enabled=False, db_url="postgresql://unused")
    result = detector.run(df=pd.DataFrame(), features={})

    assert result.detector == "voter_registry_match"
    assert result.summary["active"] is False
    assert result.summary["reason"] == "voter_registry_match_disabled"
    assert set(result.tables) == _EXPECTED_TABLES


def test_voter_registry_match_detector_emits_conservative_outputs(monkeypatch) -> None:
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
            "name_display": [
                "DOE, JANE",
                "DOE, JANE",
                "SMITH, JOHN",
                "BROWN, AVA",
                "CHANG, MEI",
                "CHANG, MEI",
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
    monkeypatch.setattr(
        "testifier_audit.detectors.voter_registry_match.fetch_voter_candidates_by_last_name",
        lambda **_kwargs: pd.DataFrame(
            {
                "canonical_last": ["DOE", "CHANG"],
                "canonical_first": ["JANE", "MEI"],
                "canonical_name": ["DOE|JANE", "CHANG|MEI"],
                "n_registry_rows": [1, 1],
            }
        ),
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
    assert result.summary["primary_match_mode"] == "loose"
    assert result.summary["n_rows"] == 6
    assert result.summary["n_matched_unique_rows"] == 4
    assert result.summary["n_matched_ambiguous_rows"] == 0
    assert result.summary["n_unmatched_rows"] == 2
    assert result.summary["matched_rate_rows"] == pytest.approx(4 / 6)
    assert result.summary["unmatched_rate_rows"] == pytest.approx(2 / 6)
    assert result.summary["voter_signal_role"] == "supporting_evidence_only"

    overview = result.tables["linkage_overview"]
    overview = overview[overview["match_mode"] == "loose"].iloc[0]
    assert overview["n_rows"] == 6
    assert overview["n_unmatched_rows"] == 2
    assert overview["matched_rate_rows"] == pytest.approx(4 / 6)
    assert overview["unmatched_rate_rows"] == pytest.approx(2 / 6)

    by_position = result.tables["linkage_by_position_rows"]
    by_position = by_position[by_position["match_mode"] == "loose"].set_index("position_normalized")
    assert by_position.loc["Pro", "n_total"] == 3
    assert by_position.loc["Con", "n_total"] == 3
    assert by_position.loc["Pro", "unmatched_rate"] == pytest.approx(1 / 3)
    assert by_position.loc["Con", "unmatched_rate"] == pytest.approx(1 / 3)

    by_bucket = result.tables["match_by_bucket"]
    by_bucket = by_bucket[by_bucket["match_mode"] == "loose"].sort_values("bucket_start").reset_index(drop=True)
    assert len(by_bucket) == 2
    assert set(by_bucket["bucket_minutes"].astype(int).tolist()) == {30}
    assert by_bucket.loc[0, "n_total"] == 3
    assert by_bucket.loc[0, "matched_rate"] == pytest.approx(2 / 3)
    assert by_bucket.loc[0, "unmatched_rate"] == pytest.approx(1 / 3)
    assert by_bucket.loc[1, "n_total"] == 3
    assert by_bucket.loc[1, "matched_rate"] == pytest.approx(2 / 3)
    assert by_bucket.loc[1, "unmatched_rate"] == pytest.approx(1 / 3)

    pairwise = result.tables["position_pairwise_tests"]
    assert not pairwise.empty
    assert set(pairwise["unit"]) == {"rows", "unique_names"}
    assert set(pairwise["match_mode"]) == {"strict", "loose"}

    unmatched = result.tables["unmatched_names"]
    unmatched = unmatched[unmatched["match_mode"] == "loose"].set_index("canonical_name")
    assert set(unmatched.index) == {"SMITH|JOHN", "BROWN|AVA"}
    assert unmatched.loc["SMITH|JOHN", "n_rows"] == 1
    assert unmatched.loc["BROWN|AVA", "n_rows"] == 1
    assert unmatched.loc["SMITH|JOHN", "display_name"] == "SMITH, JOHN"
    assert unmatched.loc["BROWN|AVA", "display_name"] == "BROWN, AVA"


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
    monkeypatch.setattr(
        "testifier_audit.detectors.voter_registry_match.fetch_voter_candidates_by_last_name",
        lambda **_kwargs: pd.DataFrame(
            {
                "canonical_last": ["DOE", "SMITH"],
                "canonical_first": ["JANE", "JOHN"],
                "canonical_name": ["DOE|JANE", "SMITH|JOHN"],
                "n_registry_rows": [1, 1],
            }
        ),
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

    by_bucket_position = result.tables["match_by_bucket_position"]
    assert not by_bucket_position.empty
    assert set(by_bucket_position["bucket_minutes"].astype(int).unique()) == {1, 5, 15}


def test_voter_registry_match_detector_reports_sensitivity_modes(monkeypatch) -> None:
    df = pd.DataFrame(
        {
            "canonical_name": ["DOE|JANE", "SMITH|JON", "LEE|ALEXA", "BROWN|AVA"],
            "position_normalized": ["Pro", "Pro", "Con", "Con"],
            "minute_bucket": pd.to_datetime(
                [
                    "2026-02-01 00:05:00",
                    "2026-02-01 00:06:00",
                    "2026-02-01 00:10:00",
                    "2026-02-01 00:35:00",
                ]
            ),
        }
    )

    monkeypatch.setattr(
        "testifier_audit.detectors.voter_registry_match.fetch_matching_voter_names",
        lambda **_kwargs: pd.DataFrame(
            {
                "canonical_name": ["DOE|JANE"],
                "n_registry_rows": [1],
            }
        ),
    )
    monkeypatch.setattr(
        "testifier_audit.detectors.voter_registry_match.fetch_voter_candidates_by_last_name",
        lambda **_kwargs: pd.DataFrame(
            {
                "canonical_last": ["DOE", "SMITH", "LEE"],
                "canonical_first": ["JANE", "JON", "ALEX"],
                "canonical_name": ["DOE|JANE", "SMITH|JON", "LEE|ALEX"],
                "n_registry_rows": [1, 1, 1],
            }
        ),
    )
    monkeypatch.setattr(
        "testifier_audit.detectors.voter_registry_match.count_registry_rows",
        lambda **_kwargs: 3,
    )

    detector = VoterRegistryMatchDetector(
        enabled=True,
        db_url="postgresql://user:pass@localhost:5432/legislature",
        table_name="voter_registry",
        bucket_minutes=30,
        active_only=True,
        strong_fuzzy_min_score=95.0,
        weak_fuzzy_min_score=80.0,
    )
    result = detector.run(df=df, features={})

    # Loose mode keeps deterministic/nickname equivalents.
    assert result.summary["n_matched_unique_rows"] == 2
    assert result.summary["n_unmatched_rows"] == 2

    sensitivity = result.tables["sensitivity_modes"].set_index("mode")
    assert set(sensitivity.index) == {"strict", "loose"}
    assert sensitivity.loc["strict", "n_unmatched_rows"] == 3
    assert sensitivity.loc["loose", "n_unmatched_rows"] == 2

    assignments = result.tables["match_assignments"].set_index("canonical_name")
    assert assignments.loc["DOE|JANE", "primary_outcome"] == "matched_unique"
    assert assignments.loc["SMITH|JON", "primary_outcome"] == "matched_unique"
    assert assignments.loc["SMITH|JON", "balanced_outcome"] == "matched_unique"
    assert assignments.loc["LEE|ALEXA", "broad_outcome"] == "matched_unique"
    assert assignments.loc["DOE|JANE", "strict_outcome_selected"] == "matched_unique"
    assert assignments.loc["SMITH|JON", "loose_outcome_selected"] == "matched_unique"
    assert assignments.loc["BROWN|AVA", "primary_outcome"] == "unmatched"
