from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from testifier_audit.cli import app
from testifier_audit.io.submissions_postgres import SubmissionImportResult
from testifier_audit.io.vrdb_postgres import VRDBImportResult


def test_cli_help() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "import-submissions" in result.stdout
    assert "import-vrdb" in result.stdout
    assert "run-all" in result.stdout
    assert "prepare-rarity-baselines" in result.stdout


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
                "Count,Name,Organization,Position,Time Signed In",
                '1,"Doe, Jane",,Pro,2/3/2026 5:07 PM',
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
                id="Count",
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


def test_run_all_allows_postgres_mode_without_csv(monkeypatch, tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "testifier_audit.cli._load_app_config",
        lambda _path: SimpleNamespace(
            input=SimpleNamespace(mode="postgres"),
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
            input=SimpleNamespace(mode="postgres"),
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
