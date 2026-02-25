from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from testifier_audit.cli import app
from tests._external_test_utils import registry_lookup_tables, write_run_all_config
from tests._methodology_assertions import assert_frame_matches_records, assert_mapping_subset
from tests._methodology_fixture_loader import fixture_path, load_fixture_json


def _load_expected_payload(relative_path: str) -> dict[str, object]:
    parts = [part for part in relative_path.split("/") if part]
    return load_fixture_json("external", *parts)


def test_external_voter_matching_pipeline_matches_frozen_reference_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    manifest = load_fixture_json("external", "expected", "voter_manifest.json")
    case = manifest["cases"][0]

    submissions_csv = fixture_path("external", *str(case["submissions_csv"]).split("/"))
    registry_csv = fixture_path("external", *str(case["registry_csv"]).split("/"))
    expected = _load_expected_payload(str(case["expected_json"]))

    exact_lookup, candidates, registry_count = registry_lookup_tables(registry_csv)

    def _mock_fetch_matching_voter_names(**kwargs) -> pd.DataFrame:  # noqa: ANN003
        canonical_names = set(kwargs.get("canonical_names", []))
        return exact_lookup[exact_lookup["canonical_name"].isin(canonical_names)].reset_index(drop=True)

    def _mock_fetch_voter_candidates_by_last_name(**kwargs) -> pd.DataFrame:  # noqa: ANN003
        canonical_lasts = set(kwargs.get("canonical_lasts", []))
        return candidates[candidates["canonical_last"].isin(canonical_lasts)].reset_index(drop=True)

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
        lambda **_kwargs: int(registry_count),
    )

    config_path = write_run_all_config(
        out_path=tmp_path / "voter_e2e.yaml",
        voter_enabled=True,
        voter_db_url="postgresql://example",
        analysis_bucket_minutes=[30],
        collision_uncertainty_mode="analytic_only",
        collision_scope_primary="full_hearing",
        collision_scope_overlays=[],
    )
    out_dir = tmp_path / "voter_e2e"

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run-all",
            "--csv",
            str(submissions_csv),
            "--out",
            str(out_dir),
            "--config",
            str(config_path),
        ],
    )
    assert result.exit_code == 0, result.stdout

    summary = json.loads((out_dir / "summary" / "voter_registry_match.json").read_text(encoding="utf-8"))
    expected_summary = expected["summary_by_mode"]["loose"]

    assert_mapping_subset(
        actual=summary,
        expected_subset={
            "primary_match_mode": "loose",
            "n_rows": int(expected_summary["n_rows"]),
            "n_unique_names": int(expected_summary["n_unique_names"]),
            "n_matched_unique_rows": int(expected_summary["n_matched_unique_rows"]),
            "n_matched_ambiguous_rows": int(expected_summary["n_matched_ambiguous_rows"]),
            "n_unmatched_rows": int(expected_summary["n_unmatched_rows"]),
            "matched_rate_rows": float(expected_summary["matched_rate_rows"]),
            "unmatched_rate_rows": float(expected_summary["unmatched_rate_rows"]),
        },
        float_tolerance=float(expected["tolerances"]["float_atol"]),
    )

    sensitivity = pd.read_csv(out_dir / "tables" / "voter_registry_match__sensitivity_modes.csv")
    assert_frame_matches_records(
        actual=sensitivity,
        expected_records=expected["sensitivity_modes"],
        columns=(
            "mode",
            "match_mode",
            "n_rows",
            "n_matched_unique_rows",
            "n_matched_ambiguous_rows",
            "n_unmatched_rows",
            "matched_rate_rows",
            "unmatched_rate_rows",
        ),
        sort_by=("mode",),
        float_tolerance=float(expected["tolerances"]["float_atol"]),
    )

    by_bucket = pd.read_csv(out_dir / "tables" / "voter_registry_match__match_by_bucket.csv")
    assert_frame_matches_records(
        actual=by_bucket,
        expected_records=expected["match_by_bucket"],
        columns=(
            "match_mode",
            "bucket_minutes",
            "bucket_start",
            "n_total",
            "n_matched_unique",
            "n_matched_ambiguous",
            "n_unmatched",
            "matched_rate",
            "unmatched_rate",
        ),
        sort_by=("match_mode", "bucket_minutes", "bucket_start"),
        datetime_columns=("bucket_start",),
        float_tolerance=float(expected["tolerances"]["float_atol"]),
    )
