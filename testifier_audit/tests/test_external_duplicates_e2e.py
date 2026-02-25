from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from testifier_audit.cli import app
from tests._external_test_utils import write_run_all_config
from tests._methodology_assertions import assert_frame_matches_records
from tests._methodology_fixture_loader import fixture_path, list_cases, load_fixture_json


def _load_expected_payload(relative_path: str) -> dict[str, object]:
    parts = [part for part in relative_path.split("/") if part]
    return load_fixture_json("external", *parts)


def test_external_duplicates_pipeline_matches_frozen_reference_outputs(tmp_path: Path) -> None:
    manifest = load_fixture_json("external", "expected", "duplicates_manifest.json")
    include_extended = os.getenv("TESTIFIER_AUDIT_EXTERNAL_BENCHMARK_FULL", "0") == "1"
    cases = list_cases(manifest, include_extended=include_extended)

    runner = CliRunner()

    for case in cases:
        case_id = str(case["case_id"])
        csv_path = fixture_path("external", *str(case["input_csv"]).split("/"))
        expected = _load_expected_payload(str(case["expected_json"]))

        config_path = write_run_all_config(
            out_path=tmp_path / f"{case_id}.yaml",
            voter_enabled=False,
            voter_db_url=None,
            analysis_bucket_minutes=[30],
            collision_uncertainty_mode="analytic_only",
            collision_scope_primary="full_hearing",
            collision_scope_overlays=[],
        )
        out_dir = tmp_path / case_id

        result = runner.invoke(
            app,
            [
                "run-all",
                "--csv",
                str(csv_path),
                "--out",
                str(out_dir),
                "--config",
                str(config_path),
            ],
        )
        assert result.exit_code == 0, f"{case_id}: {result.stdout}"

        summary_path = out_dir / "summary" / "duplicates_exact.json"
        summary = json.loads(summary_path.read_text(encoding="utf-8"))

        expected_summary = expected["summary"]
        expected_observed = expected_summary["observed"]
        assert int(summary["n_records"]) == int(expected_summary["n_rows"]), case_id
        assert int(summary["n_unique_names"]) == int(expected_summary["n_unique_names"]), case_id
        assert float(summary["duplicate_rows"]) == float(expected_observed["repeated_group_rows"]), case_id
        assert float(summary["duplicate_pairs"]) == float(expected_observed["pairs"]), case_id
        expected_position_summary = expected["position_interval_summary"]
        assert float(summary["position_interval_nominal"]) == float(
            expected_position_summary["position_interval_nominal"]
        ), case_id
        assert str(summary["position_interval_method_id"]) == str(
            expected_position_summary["position_interval_method_id"]
        ), case_id
        assert bool(summary["position_claim_eligible"]) is bool(
            expected_position_summary["position_claim_eligible"]
        ), case_id
        assert str(summary["position_claim_reason"]) == str(
            expected_position_summary["position_claim_reason"]
        ), case_id

        overview = pd.read_csv(out_dir / "tables" / "duplicates_exact__collision_overview.csv")
        expected_rows = expected["collision_overview_rows"]

        assert_frame_matches_records(
            actual=overview,
            expected_records=expected_rows,
            columns=("scope", "metric", "observed", "expected"),
            sort_by=("scope", "metric"),
            float_tolerance=float(expected["tolerances"]["float_atol"]),
        )

        position_metrics = pd.read_csv(
            out_dir / "tables" / "duplicates_exact__position_duplicate_metrics.csv"
        )
        expected_position_rows = expected["position_duplicate_metrics_rows"]
        assert_frame_matches_records(
            actual=position_metrics,
            expected_records=expected_position_rows,
            columns=(
                "position_normalized",
                "n_rows",
                "duplicate_rows",
                "duplicate_row_rate",
                "expected_duplicate_rows",
                "expected_duplicate_rows_p05",
                "expected_duplicate_rows_p50",
                "expected_duplicate_rows_p95",
                "expected_duplicate_row_rate",
                "expected_duplicate_row_rate_p05",
                "expected_duplicate_row_rate_p50",
                "expected_duplicate_row_rate_p95",
                "interval_method_id",
                "interval_draws_effective",
                "is_low_power",
                "inference_status",
            ),
            sort_by=("position_normalized",),
            float_tolerance=float(expected["tolerances"]["float_atol"]),
        )
