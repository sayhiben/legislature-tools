from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from typer.testing import CliRunner

from testifier_audit.cli import app
from tests._external_test_utils import (
    canonicalize_name_frame,
    registry_lookup_tables,
    write_run_all_config,
)
from tests._methodology_assertions import assert_mapping_subset
from tests._methodology_fixture_loader import fixture_path, load_fixture_json

_MATCHED_OUTCOMES = {"matched_unique", "matched_ambiguous"}
_REC_ID_PATTERN = re.compile(r"rec-(\d+)-")


def _load_expected_payload(relative_path: str) -> dict[str, Any]:
    parts = [part for part in relative_path.split("/") if part]
    return load_fixture_json("external", *parts)


def _extract_record_key(value: object) -> int | None:
    match = _REC_ID_PATTERN.search(str(value or ""))
    if not match:
        return None
    return int(match.group(1))


def _build_truth_rows(*, submissions_csv: Path, registry_csv: Path) -> pd.DataFrame:
    submissions_raw = pd.read_csv(submissions_csv)
    name_features = canonicalize_name_frame(frame=submissions_raw, name_column="Name")

    rows = submissions_raw.copy()
    rows["canonical_name"] = name_features["canonical_name"].fillna("").astype(str)
    rows = rows[(rows["canonical_name"] != "") & (rows["canonical_name"] != "|")].copy()
    rows["record_key"] = rows["rec_id"].map(_extract_record_key)
    rows = rows[rows["record_key"].notna()].copy()
    rows["record_key"] = rows["record_key"].astype(int)

    registry = pd.read_csv(registry_csv)
    registry_keys = {
        key
        for key in (_extract_record_key(value) for value in registry.get("rec_id", []))
        if key is not None
    }
    rows["truth_match"] = rows["record_key"].isin(registry_keys)
    return rows[["canonical_name", "record_key", "truth_match"]].reset_index(drop=True)


def _evaluate_mode(
    *,
    truth_rows: pd.DataFrame,
    assignments: pd.DataFrame,
    mode: str,
) -> dict[str, float | int]:
    outcome_column = f"{mode}_outcome_selected"
    assignment_columns = ["canonical_name", outcome_column]
    by_name = assignments[assignment_columns].drop_duplicates(subset=["canonical_name"]).copy()

    merged = truth_rows.merge(by_name, on="canonical_name", how="left")
    merged[outcome_column] = merged[outcome_column].fillna("unmatched").astype(str)

    truth = merged["truth_match"].astype(bool)
    predicted_match = merged[outcome_column].isin(_MATCHED_OUTCOMES)

    tp = int((predicted_match & truth).sum())
    fp = int((predicted_match & (~truth)).sum())
    fn = int(((~predicted_match) & truth).sum())
    tn = int(((~predicted_match) & (~truth)).sum())

    n_rows = int(len(merged))
    precision = float(tp / (tp + fp)) if (tp + fp) else 0.0
    recall = float(tp / (tp + fn)) if (tp + fn) else 0.0
    f1 = float((2.0 * precision * recall) / (precision + recall)) if (precision + recall) else 0.0
    false_positive_rate = float(fp / (fp + tn)) if (fp + tn) else 0.0
    predicted_match_rate_rows = float((tp + fp) / n_rows) if n_rows else 0.0

    return {
        "n_rows": n_rows,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "false_positive_rate": false_positive_rate,
        "predicted_match_rate_rows": predicted_match_rate_rows,
    }


def test_external_voter_matching_meets_febrl_ground_truth_quality_thresholds(
    tmp_path: Path,
    monkeypatch,
) -> None:
    manifest = load_fixture_json("external", "expected", "voter_manifest.json")
    case = manifest["cases"][0]

    submissions_csv = fixture_path("external", *str(case["submissions_csv"]).split("/"))
    registry_csv = fixture_path("external", *str(case["registry_csv"]).split("/"))
    expected = _load_expected_payload(str(case["ground_truth_json"]))

    exact_lookup, candidates, registry_count = registry_lookup_tables(registry_csv)

    def _mock_fetch_matching_voter_names(**kwargs) -> pd.DataFrame:  # noqa: ANN003
        canonical_names = set(kwargs.get("canonical_names", []))
        return (
            exact_lookup[exact_lookup["canonical_name"].isin(canonical_names)]
            .reset_index(drop=True)
        )

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
        out_path=tmp_path / "voter_ground_truth.yaml",
        voter_enabled=True,
        voter_db_url="postgresql://example",
        analysis_bucket_minutes=[30],
        collision_uncertainty_mode="analytic_only",
        collision_scope_primary="full_hearing",
        collision_scope_overlays=[],
    )
    out_dir = tmp_path / "voter_ground_truth"

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

    truth_rows = _build_truth_rows(submissions_csv=submissions_csv, registry_csv=registry_csv)
    actual_truth_summary = {
        "n_rows_evaluable": int(len(truth_rows)),
        "n_truth_matched_rows": int(truth_rows["truth_match"].sum()),
        "n_truth_unmatched_rows": int((~truth_rows["truth_match"]).sum()),
        "truth_match_rate_rows": (
            float(truth_rows["truth_match"].mean()) if len(truth_rows) else 0.0
        ),
        "truth_unmatched_rate_rows": (
            float((~truth_rows["truth_match"]).mean()) if len(truth_rows) else 0.0
        ),
    }
    assert_mapping_subset(
        actual=actual_truth_summary,
        expected_subset=expected["truth_summary"],
        float_tolerance=float(expected["tolerances"]["float_atol"]),
    )

    assignments = pd.read_csv(out_dir / "tables" / "voter_registry_match__match_assignments.csv")
    sensitivity = pd.read_csv(out_dir / "tables" / "voter_registry_match__sensitivity_modes.csv")
    sensitivity_by_mode = sensitivity.set_index("mode")

    for mode in expected["evaluation_contract"]["modes"]:
        metrics = _evaluate_mode(
            truth_rows=truth_rows,
            assignments=assignments,
            mode=str(mode),
        )
        thresholds = expected["quality_thresholds_by_mode"][str(mode)]
        atol = float(expected["tolerances"]["float_atol"])

        assert metrics["precision"] + atol >= float(thresholds["min_precision"])
        assert metrics["recall"] + atol >= float(thresholds["min_recall"])
        assert metrics["f1"] + atol >= float(thresholds["min_f1"])
        assert metrics["false_positive_rate"] <= float(thresholds["max_false_positive_rate"]) + atol

        assert str(mode) in sensitivity_by_mode.index
        matched_rate_rows = float(sensitivity_by_mode.at[str(mode), "matched_rate_rows"])
        assert np.isclose(
            matched_rate_rows,
            float(metrics["predicted_match_rate_rows"]),
            rtol=0.0,
            atol=atol,
        )
