from __future__ import annotations

from pathlib import Path

import pandas as pd

from testifier_audit.detectors.voter_registry_match import VoterRegistryMatchDetector
from testifier_audit.names.linkage import LinkageThresholds, classify_name_linkage
from tests._methodology_assertions import assert_frame_matches_records, assert_mapping_subset
from tests._methodology_fixture_loader import load_fixture_json


def test_classify_name_linkage_fixture_covers_exact_nickname_fuzzy_and_ambiguous_paths() -> None:
    fixture = load_fixture_json("primitive", "linkage_cases.json")
    thresholds = fixture["thresholds"]

    output = classify_name_linkage(
        submission_names=[str(value) for value in fixture["submission_names"]],
        exact_lookup={str(k): int(v) for k, v in fixture["exact_lookup"].items()},
        candidate_lookup_by_last=fixture["candidate_lookup_by_last"],
        nickname_map={str(k): str(v) for k, v in fixture["nickname_map"].items()},
        thresholds=LinkageThresholds(
            strong_fuzzy_min_score=float(thresholds["strong_fuzzy_min_score"]),
            weak_fuzzy_min_score=float(thresholds["weak_fuzzy_min_score"]),
            ambiguous_score_gap=float(thresholds["ambiguous_score_gap"]),
        ),
    )

    assert_frame_matches_records(
        actual=output,
        expected_records=fixture["expected_rows"],
        columns=(
            "canonical_name",
            "match_tier",
            "primary_outcome",
            "balanced_outcome",
            "broad_outcome",
            "is_ambiguous",
            "match_caveat",
        ),
        sort_by=("canonical_name",),
    )


def test_voter_registry_match_detector_fixture_asserts_strict_vs_loose_methodology(
    monkeypatch,
    tmp_path: Path,
) -> None:
    fixture = load_fixture_json("primitive", "voter_detector_case.json")

    submissions = pd.DataFrame(fixture["submissions"])
    submissions["minute_bucket"] = pd.to_datetime(submissions["minute_bucket"], errors="coerce", utc=True)

    registry_rows = pd.DataFrame(fixture["registry_rows"])

    nickname_map = fixture["nickname_map"]
    nickname_path = tmp_path / "nicknames.csv"
    nickname_lines = ["alias,canonical"] + [f"{alias},{canonical}" for alias, canonical in nickname_map.items()]
    nickname_path.write_text("\n".join(nickname_lines) + "\n", encoding="utf-8")

    exact_lookup = (
        registry_rows.groupby("canonical_name", dropna=False)["n_registry_rows"]
        .max()
        .reset_index()
        .sort_values("canonical_name")
    )

    def _mock_fetch_matching_voter_names(**kwargs) -> pd.DataFrame:  # noqa: ANN003
        canonical_names = set(kwargs.get("canonical_names", []))
        return exact_lookup[exact_lookup["canonical_name"].isin(canonical_names)].reset_index(drop=True)

    def _mock_fetch_voter_candidates_by_last_name(**kwargs) -> pd.DataFrame:  # noqa: ANN003
        canonical_lasts = set(kwargs.get("canonical_lasts", []))
        return registry_rows[registry_rows["canonical_last"].isin(canonical_lasts)].reset_index(drop=True)

    monkeypatch.setattr(
        "testifier_audit.detectors.voter_registry_match.fetch_matching_voter_names",
        _mock_fetch_matching_voter_names,
    )
    monkeypatch.setattr(
        "testifier_audit.detectors.voter_registry_match.fetch_voter_candidates_by_last_name",
        _mock_fetch_voter_candidates_by_last_name,
    )
    monkeypatch.setattr(
        "testifier_audit.detectors.voter_registry_match.count_registry_rows",
        lambda **_kwargs: int(registry_rows["n_registry_rows"].sum()),
    )

    detector = VoterRegistryMatchDetector(
        enabled=True,
        db_url="postgresql://example",
        table_name="voter_registry",
        bucket_minutes=[int(value) for value in fixture["bucket_minutes"]],
        active_only=True,
        nickname_map_path=str(nickname_path),
    )
    result = detector.run(df=submissions, features={})

    assert_mapping_subset(
        actual=result.summary,
        expected_subset=fixture["expected_summary"],
        float_tolerance=1e-9,
    )

    assert_frame_matches_records(
        actual=result.tables["sensitivity_modes"],
        expected_records=fixture["expected_sensitivity_modes"],
        columns=(
            "mode",
            "match_mode",
            "n_rows",
            "n_matched_unique_rows",
            "n_matched_ambiguous_rows",
            "n_unmatched_rows",
        ),
        sort_by=("mode",),
    )

    assert_frame_matches_records(
        actual=result.tables["match_by_bucket"],
        expected_records=fixture["expected_match_by_bucket"],
        columns=(
            "match_mode",
            "bucket_minutes",
            "bucket_start",
            "n_total",
            "n_matched_unique",
            "n_matched_ambiguous",
            "n_unmatched",
        ),
        sort_by=("match_mode", "bucket_minutes", "bucket_start"),
        datetime_columns=("bucket_start",),
    )
