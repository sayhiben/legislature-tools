from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from typer.testing import CliRunner

from testifier_audit.cli import app
from testifier_audit.io.csi_testifiers import CSIDownloadResult
from testifier_audit.io.submissions_postgres import SubmissionImportResult
from testifier_audit.io.vrdb_postgres import VRDBImportResult


def test_cli_help() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "download-csi-testifiers" in result.stdout
    assert "sample-baseline-corpus" in result.stdout
    assert "import-submissions" in result.stdout
    assert "import-vrdb" in result.stdout
    assert "run-all" in result.stdout


def test_download_csi_testifiers_command_runs(monkeypatch, tmp_path: Path) -> None:
    csv_out_dir = tmp_path / "raw"
    metadata_out_dir = tmp_path / "hearing_metadata"
    captured: dict[str, object] = {}

    def _fake_download(**kwargs: object) -> CSIDownloadResult:
        captured.update(kwargs)
        return CSIDownloadResult(
            search_query="sb 6005",
            csv_path=csv_out_dir / "SB6005-20260224-1600.csv",
            metadata_path=metadata_out_dir / "SB6005-20260224-1600.hearing.yaml",
            short_bill_id="SB 6005",
            bill_title="Transportation funding and appropriations",
            meeting_family_id="34001",
            agenda_item_family_id="170646",
            agenda_item_id="28434",
            meeting_start=datetime(2026, 2, 24, 16, 0, tzinfo=ZoneInfo("America/Los_Angeles")),
            testifying_rows=7,
            not_testifying_rows=204,
        )

    monkeypatch.setattr("testifier_audit.cli.download_csi_testifier_csv", _fake_download)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "download-csi-testifiers",
            "sb 6005",
            "--csv-out-dir",
            str(csv_out_dir),
            "--metadata-out-dir",
            str(metadata_out_dir),
            "--meeting-index",
            "1",
            "--agenda-index",
            "0",
            "--top",
            "50",
            "--timeout-seconds",
            "15",
            "--max-retries",
            "2",
            "--retry-backoff-seconds",
            "0.5",
            "--no-overwrite",
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert captured["bill_query"] == "sb 6005"
    assert captured["meeting_index"] == 1
    assert captured["agenda_index"] == 0
    assert captured["top"] == 50
    assert captured["timeout_seconds"] == 15.0
    assert captured["max_retries"] == 2
    assert captured["retry_backoff_seconds"] == 0.5
    assert captured["overwrite"] is False
    assert "CSI testifier download complete" in result.stdout
    assert "total_rows: 211" in result.stdout


def test_sample_baseline_corpus_command_runs(monkeypatch, tmp_path: Path) -> None:
    index_json = tmp_path / "index.json"
    index_csv = tmp_path / "index.csv"
    csv_out_dir = tmp_path / "raw"
    metadata_out_dir = tmp_path / "metadata"
    manifest_out = tmp_path / "manifest.json"
    sampled_a = tmp_path / "sampled-a"
    sampled_b = tmp_path / "sampled-b"
    captured: dict[str, object] = {}

    def _fake_sample(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "session_years": [2026, 2025, 2024],
            "sample_size_requested": 2,
            "sample_size_selected": 2,
            "sample_size_downloaded": 2,
            "sample_size_failed": 0,
            "index_refreshed": True,
        }

    def _fake_write(path: Path, manifest: dict[str, object]) -> None:
        captured["manifest_path"] = path
        captured["manifest_payload"] = manifest

    monkeypatch.setattr("testifier_audit.cli.sample_unsampled_baseline_corpus", _fake_sample)
    monkeypatch.setattr("testifier_audit.cli.write_baseline_sample_manifest", _fake_write)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "sample-baseline-corpus",
            "--sample-size",
            "2",
            "--session-count",
            "3",
            "--index-json",
            str(index_json),
            "--index-csv",
            str(index_csv),
            "--csv-out-dir",
            str(csv_out_dir),
            "--metadata-out-dir",
            str(metadata_out_dir),
            "--manifest-out",
            str(manifest_out),
            "--sampled-metadata-dir",
            str(sampled_a),
            "--sampled-metadata-dir",
            str(sampled_b),
            "--refresh-index",
            "--seed",
            "17",
            "--rate-limit-seconds",
            "0.2",
            "--timeout-seconds",
            "15",
            "--max-retries",
            "2",
            "--retry-backoff-seconds",
            "0.4",
            "--overwrite",
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert captured["sample_size"] == 2
    assert captured["session_count"] == 3
    assert captured["index_json_path"] == index_json
    assert captured["index_csv_path"] == index_csv
    assert captured["csv_out_dir"] == csv_out_dir
    assert captured["metadata_out_dir"] == metadata_out_dir
    assert captured["manifest_path"] == manifest_out
    assert captured["sampled_metadata_dirs"] == [sampled_a, sampled_b]
    assert captured["refresh_index"] is True
    assert captured["seed"] == 17
    assert captured["rate_limit_seconds"] == 0.2
    assert captured["timeout_seconds"] == 15.0
    assert captured["max_retries"] == 2
    assert captured["retry_backoff_seconds"] == 0.4
    assert captured["overwrite"] is True
    assert "Baseline corpus sampling complete" in result.stdout


def test_import_vrdb_command_runs_with_config_defaults(monkeypatch, tmp_path: Path) -> None:
    extract_path = tmp_path / "extract.txt"
    extract_path.write_text("StateVoterID|FName|LName\n1|JANE|DOE\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "testifier_audit.cli._load_app_config",
        lambda _path: SimpleNamespace(
            voter_registry=SimpleNamespace(
                db_url="postgresql://user:pass@localhost:5432/legislature",
                table_name="voter_registry",
            )
        ),
    )

    captured: dict[str, object] = {}

    def _fake_import(
        extract_path: Path,
        db_url: str,
        table_name: str,
        chunk_size: int,
        force: bool,
    ) -> VRDBImportResult:
        captured["extract_path"] = extract_path
        captured["db_url"] = db_url
        captured["table_name"] = table_name
        captured["chunk_size"] = chunk_size
        captured["force"] = force
        return VRDBImportResult(
            source_file=extract_path.name,
            table_name=table_name,
            rows_processed=1,
            rows_upserted=1,
            rows_with_state_voter_id=1,
            rows_with_canonical_name=1,
            chunk_size=chunk_size,
            file_hash="abc123",
        )

    monkeypatch.setattr("testifier_audit.cli.import_vrdb_extract_to_postgres", _fake_import)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "import-vrdb",
            "--extract",
            str(extract_path),
            "--config",
            str(config_path),
            "--chunk-size",
            "2000",
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert captured["extract_path"] == extract_path
    assert captured["table_name"] == "voter_registry"
    assert captured["chunk_size"] == 2000
    assert captured["force"] is False
    assert "rows_upserted: 1" in result.stdout


def test_import_vrdb_command_requires_db_url(monkeypatch, tmp_path: Path) -> None:
    extract_path = tmp_path / "extract.txt"
    extract_path.write_text("StateVoterID|FName|LName\n1|JANE|DOE\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "testifier_audit.cli._load_app_config",
        lambda _path: SimpleNamespace(
            voter_registry=SimpleNamespace(
                db_url=None,
                table_name="voter_registry",
            )
        ),
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "import-vrdb",
            "--extract",
            str(extract_path),
            "--config",
            str(config_path),
        ],
    )

    assert result.exit_code != 0
    combined_output = result.stdout
    if hasattr(result, "stderr") and result.stderr:
        combined_output += result.stderr
    assert "Missing database URL" in combined_output or "Missing database URL" in str(
        result.exception
    )


def test_import_submissions_command_runs_with_config_defaults(
    monkeypatch,
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "submissions.csv"
    csv_path.write_text(
        "\n".join(
            [
                "Group,Name,Organization,Position,Time Signed In",
                'Testifying,"Doe, Jane",,Pro,2/3/2026 5:07 PM',
            ]
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "testifier_audit.cli._load_app_config",
        lambda _path: SimpleNamespace(
            columns=SimpleNamespace(
                id="Group",
                name="Name",
                organization="Organization",
                position="Position",
                time_signed_in="Time Signed In",
            ),
            time=SimpleNamespace(timezone="America/Los_Angeles"),
            input=SimpleNamespace(
                db_url="postgresql://user:pass@localhost:5432/legislature",
                submissions_table="public_submissions",
            ),
        ),
    )

    captured: dict[str, object] = {}

    def _fake_import(**kwargs: object) -> SubmissionImportResult:
        captured.update(kwargs)
        return SubmissionImportResult(
            source_file="submissions.csv",
            table_name=str(kwargs["table_name"]),
            rows_processed=1,
            rows_upserted=1,
            rows_blank_organization=1,
            rows_invalid_timestamp=0,
            chunk_size=int(kwargs["chunk_size"]),
            file_hash="def456",
        )

    monkeypatch.setattr("testifier_audit.cli.import_submission_csv_to_postgres", _fake_import)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "import-submissions",
            "--csv",
            str(csv_path),
            "--config",
            str(config_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert captured["csv_path"] == csv_path
    assert captured["table_name"] == "public_submissions"
    assert captured["force"] is False
    assert "rows_upserted: 1" in result.stdout


def test_run_all_requires_source_file_in_postgres_mode(monkeypatch, tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "testifier_audit.cli._load_app_config",
        lambda _path: SimpleNamespace(
            input=SimpleNamespace(mode="postgres", source_file=None),
        ),
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run-all",
            "--out",
            str(out_dir),
            "--config",
            str(config_path),
        ],
    )

    assert result.exit_code != 0
    combined_output = result.stdout
    if hasattr(result, "stderr") and result.stderr:
        combined_output += result.stderr
    message = "requires a single source_file"
    assert message in combined_output or message in str(result.exception)


def test_run_all_allows_postgres_mode_without_source_file_when_comparative(
    monkeypatch,
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "testifier_audit.cli._load_app_config",
        lambda _path: SimpleNamespace(
            input=SimpleNamespace(mode="postgres", source_file=None),
        ),
    )

    captured: dict[str, object] = {}

    def _fake_run_all(csv_path: Path | None, out_dir: Path, config: object) -> Path:
        captured["csv_path"] = csv_path
        captured["out_dir"] = out_dir
        captured["config"] = config
        out_dir.mkdir(parents=True, exist_ok=True)
        report_path = out_dir / "report.html"
        report_path.write_text("<html></html>", encoding="utf-8")
        return report_path

    monkeypatch.setattr("testifier_audit.cli.run_all", _fake_run_all)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run-all",
            "--out",
            str(out_dir),
            "--config",
            str(config_path),
            "--comparative",
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert captured["csv_path"] is None
    assert "Run complete. Report:" in result.stdout


def test_run_all_forwards_dedup_mode_override(monkeypatch, tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "testifier_audit.cli._load_app_config",
        lambda _path: SimpleNamespace(
            input=SimpleNamespace(mode="postgres", source_file="seed.csv"),
            report=SimpleNamespace(default_dedup_mode="side_by_side", min_cell_n_for_rates=25),
        ),
    )

    captured: dict[str, object] = {}

    def _fake_run_all(
        csv_path: Path | None,
        out_dir: Path,
        config: object,
        *,
        dedup_mode: str | None = None,
    ) -> Path:
        captured["csv_path"] = csv_path
        captured["out_dir"] = out_dir
        captured["config"] = config
        captured["dedup_mode"] = dedup_mode
        out_dir.mkdir(parents=True, exist_ok=True)
        report_path = out_dir / "report.html"
        report_path.write_text("<html></html>", encoding="utf-8")
        return report_path

    monkeypatch.setattr("testifier_audit.cli.run_all", _fake_run_all)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run-all",
            "--out",
            str(out_dir),
            "--config",
            str(config_path),
            "--dedup-mode",
            "raw",
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert captured["csv_path"] is None
    assert captured["dedup_mode"] == "raw"


def test_run_all_forwards_source_file_override(monkeypatch, tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "testifier_audit.cli._load_app_config",
        lambda _path: SimpleNamespace(
            input=SimpleNamespace(mode="postgres", source_file=None),
            report=SimpleNamespace(default_dedup_mode="side_by_side", min_cell_n_for_rates=25),
        ),
    )

    captured: dict[str, object] = {}

    def _fake_run_all(
        csv_path: Path | None,
        out_dir: Path,
        config: object,
        *,
        dedup_mode: str | None = None,
    ) -> Path:
        captured["csv_path"] = csv_path
        captured["config"] = config
        captured["dedup_mode"] = dedup_mode
        report_path = out_dir / "report.html"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("<html></html>", encoding="utf-8")
        return report_path

    monkeypatch.setattr("testifier_audit.cli.run_all", _fake_run_all)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run-all",
            "--out",
            str(out_dir),
            "--config",
            str(config_path),
            "--source-file",
            "SB6346-20260206-1330.csv",
        ],
    )

    assert result.exit_code == 0, result.stdout
    cfg = captured["config"]
    assert getattr(cfg.input, "source_file") == "SB6346-20260206-1330.csv"


def test_report_uses_configured_report_settings(monkeypatch, tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "testifier_audit.cli._load_app_config",
        lambda _path: SimpleNamespace(
            report=SimpleNamespace(default_dedup_mode="exact_row_dedup", min_cell_n_for_rates=33)
        ),
    )

    captured: dict[str, object] = {}

    def _fake_render_report(**kwargs: object) -> Path:
        captured.update(kwargs)
        path = Path(str(kwargs["out_dir"])) / "report.html"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("<html></html>", encoding="utf-8")
        return path

    monkeypatch.setattr("testifier_audit.cli.render_report", _fake_render_report)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "report",
            "--out",
            str(out_dir),
            "--config",
            str(config_path),
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert captured["default_dedup_mode"] == "exact_row_dedup"
    assert captured["min_cell_n_for_rates"] == 33

    result_override = runner.invoke(
        app,
        [
            "report",
            "--out",
            str(out_dir),
            "--config",
            str(config_path),
            "--dedup-mode",
            "side_by_side",
        ],
    )
    assert result_override.exit_code == 0, result_override.stdout
    assert captured["default_dedup_mode"] == "side_by_side"


def test_run_all_applies_hearing_metadata_override(monkeypatch, tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")
    hearing_path = tmp_path / "hearing.yaml"
    hearing_path.write_text(
        "schema_version: 1\nhearing_id: TEST\ntimezone: UTC\nmeeting_start: 2026-02-06T13:30:00Z\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "testifier_audit.cli._load_app_config",
        lambda _path: SimpleNamespace(
            input=SimpleNamespace(
                mode="postgres",
                source_file="sample.csv",
                hearing_metadata_path=None,
            ),
            report=SimpleNamespace(default_dedup_mode="side_by_side", min_cell_n_for_rates=25),
        ),
    )

    captured: dict[str, object] = {}

    def _fake_run_all(
        csv_path: Path | None,
        out_dir: Path,
        config: object,
        *,
        dedup_mode: str | None = None,
    ) -> Path:
        captured["csv_path"] = csv_path
        captured["config"] = config
        captured["dedup_mode"] = dedup_mode
        report_path = out_dir / "report.html"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("<html></html>", encoding="utf-8")
        return report_path

    monkeypatch.setattr("testifier_audit.cli.run_all", _fake_run_all)
    monkeypatch.setattr(
        "testifier_audit.cli.load_hearing_metadata",
        lambda path: SimpleNamespace(path=path),
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run-all",
            "--out",
            str(out_dir),
            "--config",
            str(config_path),
            "--hearing-metadata",
            str(hearing_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    cfg = captured["config"]
    assert getattr(cfg.input, "hearing_metadata_path") == str(hearing_path)


def test_report_forwards_hearing_metadata_to_render(monkeypatch, tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")
    hearing_path = tmp_path / "hearing.yaml"
    hearing_path.write_text(
        "schema_version: 1\nhearing_id: TEST\ntimezone: UTC\nmeeting_start: 2026-02-06T13:30:00Z\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "testifier_audit.cli._load_app_config",
        lambda _path: SimpleNamespace(
            report=SimpleNamespace(default_dedup_mode="raw", min_cell_n_for_rates=25),
            input=SimpleNamespace(hearing_metadata_path=None),
        ),
    )

    sentinel_metadata = object()
    captured: dict[str, object] = {}

    def _fake_load(path: str | None) -> object | None:
        captured["metadata_path"] = path
        if path:
            return sentinel_metadata
        return None

    def _fake_render_report(**kwargs: object) -> Path:
        captured.update(kwargs)
        path = Path(str(kwargs["out_dir"])) / "report.html"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("<html></html>", encoding="utf-8")
        return path

    monkeypatch.setattr("testifier_audit.cli.load_hearing_metadata", _fake_load)
    monkeypatch.setattr("testifier_audit.cli.render_report", _fake_render_report)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "report",
            "--out",
            str(out_dir),
            "--config",
            str(config_path),
            "--hearing-metadata",
            str(hearing_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert captured["metadata_path"] == str(hearing_path)
    assert captured["hearing_metadata"] is sentinel_metadata
