from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from testifier_audit.cli import app
from tests._external_test_utils import (
    canonical_names_from_submission_csv,
    registry_lookup_tables,
    write_run_all_config,
)
from tests._methodology_fixture_loader import fixture_path, load_fixture_json


_MATCHED_OUTCOMES = {"matched_unique", "matched_ambiguous"}


def test_external_handoff_voter_assignments_drive_duplicate_scope_partitions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    manifest = load_fixture_json("external", "expected", "voter_manifest.json")
    case = manifest["cases"][0]

    submissions_csv = fixture_path("external", *str(case["submissions_csv"]).split("/"))
    registry_csv = fixture_path("external", *str(case["registry_csv"]).split("/"))

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
        out_path=tmp_path / "handoff_e2e.yaml",
        voter_enabled=True,
        voter_db_url="postgresql://example",
        analysis_bucket_minutes=[30],
        collision_uncertainty_mode="analytic_only",
        collision_scope_primary="matched_only",
        collision_scope_overlays=["full_hearing", "unmatched_only"],
    )
    out_dir = tmp_path / "handoff_e2e"

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

    assignments = pd.read_csv(out_dir / "tables" / "voter_registry_match__match_assignments.csv")
    methods = pd.read_csv(out_dir / "tables" / "duplicates_exact__collision_methods.csv")

    submissions_canonical = canonical_names_from_submission_csv(submissions_csv)
    full_rows = int(len(submissions_canonical))

    matched_names = set(
        assignments[assignments["primary_outcome_selected"].isin(_MATCHED_OUTCOMES)][
            "canonical_name"
        ].astype(str)
    )
    unmatched_names = set(
        assignments[~assignments["primary_outcome_selected"].isin(_MATCHED_OUTCOMES)][
            "canonical_name"
        ].astype(str)
    )

    matched_rows = int(submissions_canonical.isin(matched_names).sum())
    unmatched_rows = int(submissions_canonical.isin(unmatched_names).sum())

    n_used_by_scope = methods.set_index("scope")["n_used"].astype(int).to_dict()

    assert n_used_by_scope["full_hearing"] == full_rows
    assert n_used_by_scope["matched_only"] == matched_rows
    assert n_used_by_scope["unmatched_only"] == unmatched_rows
    assert matched_rows + unmatched_rows == full_rows

    duplicates_summary = json.loads(
        (out_dir / "summary" / "duplicates_exact.json").read_text(encoding="utf-8")
    )
    assert duplicates_summary["collision_scope_primary"] == "matched_only"
    assert int(duplicates_summary["n_records"]) == matched_rows
