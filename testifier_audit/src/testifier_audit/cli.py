from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import typer
import yaml

from testifier_audit.config import DEFAULT_CONFIG_PATH, AppConfig, load_config
from testifier_audit.io.hearing_metadata import load_hearing_metadata
from testifier_audit.io.rarity_baselines import BaselineProfileName, build_frequency_baseline_file
from testifier_audit.io.submissions_postgres import import_submission_csv_to_postgres
from testifier_audit.io.vrdb_postgres import import_vrdb_extract_to_postgres
from testifier_audit.logging import configure_logging
from testifier_audit.paths import build_output_paths
from testifier_audit.pipeline.pass1_profile import build_profile_artifacts, load_profile_artifacts
from testifier_audit.pipeline.pass2_deep_dive import run_detectors
from testifier_audit.pipeline.run_all import run_all
from testifier_audit.report.render import render_report

app = typer.Typer(no_args_is_help=True, add_completion=False)


def _load_app_config(config_path: Path) -> AppConfig:
    return load_config(config_path)


def _require_csv_for_csv_mode(csv: Path | None, cfg: AppConfig) -> Path | None:
    if cfg.input.mode == "csv" and csv is None:
        raise typer.BadParameter(
            "Missing --csv. Required when input.mode='csv'. "
            "Set input.mode='postgres' and configure "
            "input.db_url/input.submissions_table to hydrate from Postgres."
        )
    return csv


def _apply_hearing_metadata_override(
    cfg: AppConfig,
    hearing_metadata: Path | None,
) -> None:
    if hearing_metadata is None:
        return
    cfg.input.hearing_metadata_path = str(hearing_metadata)
    # Fail fast with a clear CLI error if sidecar contents are invalid.
    load_hearing_metadata(cfg.input.hearing_metadata_path)


def _config_hearing_metadata_path(cfg: object) -> str | None:
    input_cfg = getattr(cfg, "input", None)
    return getattr(input_cfg, "hearing_metadata_path", None)


@app.command()
def profile(
    csv: Path | None = typer.Option(None, exists=True, readable=True, resolve_path=True),
    out: Path = typer.Option(Path("out"), resolve_path=True),
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, exists=True, readable=True, resolve_path=True),
    hearing_metadata: Path | None = typer.Option(
        None,
        exists=True,
        readable=True,
        resolve_path=True,
        help="Optional hearing metadata sidecar for hearing-relative timing features.",
    ),
) -> None:
    """Build pass-1 profile artifacts from CSV or PostgreSQL input."""
    configure_logging()
    cfg = _load_app_config(config)
    _apply_hearing_metadata_override(cfg, hearing_metadata)
    csv = _require_csv_for_csv_mode(csv=csv, cfg=cfg)
    paths = build_output_paths(out)
    artifacts = build_profile_artifacts(csv_path=csv, out_dir=paths.root, config=cfg)
    typer.echo(f"Profile complete. Artifacts: {', '.join(sorted(artifacts.keys()))}")


@app.command()
def detect(
    csv: Path | None = typer.Option(None, exists=True, readable=True, resolve_path=True),
    out: Path = typer.Option(Path("out"), resolve_path=True),
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, exists=True, readable=True, resolve_path=True),
    hearing_metadata: Path | None = typer.Option(
        None,
        exists=True,
        readable=True,
        resolve_path=True,
        help="Optional hearing metadata sidecar for hearing-relative timing features.",
    ),
    rebuild_profile: bool = typer.Option(
        False, help="Recompute profile artifacts before detection."
    ),
) -> None:
    """Run detector pass using configured input source and profile artifacts."""
    configure_logging()
    cfg = _load_app_config(config)
    _apply_hearing_metadata_override(cfg, hearing_metadata)
    csv = _require_csv_for_csv_mode(csv=csv, cfg=cfg)
    paths = build_output_paths(out)

    artifacts = load_profile_artifacts(out_dir=paths.root, config=cfg)
    if rebuild_profile or not artifacts:
        artifacts = build_profile_artifacts(csv_path=csv, out_dir=paths.root, config=cfg)

    results = run_detectors(csv_path=csv, artifacts=artifacts, out_dir=paths.root, config=cfg)
    typer.echo(f"Detection complete. Detectors: {len(results)}")


@app.command()
def report(
    out: Path = typer.Option(Path("out"), resolve_path=True),
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, exists=True, readable=True, resolve_path=True),
    hearing_metadata: Path | None = typer.Option(
        None,
        exists=True,
        readable=True,
        resolve_path=True,
        help="Optional hearing metadata sidecar for hearing-relative context overlays.",
    ),
    dedup_mode: Literal["raw", "exact_row_dedup", "side_by_side"] | None = typer.Option(
        None,
        help="Override report dedup lens mode for triage views.",
    ),
) -> None:
    """Render HTML report from existing outputs in out/."""
    configure_logging()
    cfg = _load_app_config(config)
    _apply_hearing_metadata_override(cfg, hearing_metadata)
    resolved_hearing_metadata = load_hearing_metadata(_config_hearing_metadata_path(cfg))
    build_output_paths(out)
    report_path = render_report(
        results={},
        artifacts={},
        out_dir=out,
        default_dedup_mode=dedup_mode or cfg.report.default_dedup_mode,
        min_cell_n_for_rates=int(cfg.report.min_cell_n_for_rates),
        hearing_metadata=resolved_hearing_metadata,
    )
    typer.echo(f"Report written to: {report_path}")


@app.command("run-all")
def run_all_command(
    csv: Path | None = typer.Option(None, exists=True, readable=True, resolve_path=True),
    out: Path = typer.Option(Path("out"), resolve_path=True),
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, exists=True, readable=True, resolve_path=True),
    hearing_metadata: Path | None = typer.Option(
        None,
        exists=True,
        readable=True,
        resolve_path=True,
        help="Optional hearing metadata sidecar for hearing-relative timing/context.",
    ),
    dedup_mode: Literal["raw", "exact_row_dedup", "side_by_side"] | None = typer.Option(
        None,
        help="Override report dedup lens mode for triage views.",
    ),
) -> None:
    """Execute profile, detect, and report in one command."""
    configure_logging()
    cfg = _load_app_config(config)
    _apply_hearing_metadata_override(cfg, hearing_metadata)
    csv = _require_csv_for_csv_mode(csv=csv, cfg=cfg)
    if dedup_mode is None:
        report_path = run_all(csv_path=csv, out_dir=out, config=cfg)
    else:
        report_path = run_all(csv_path=csv, out_dir=out, config=cfg, dedup_mode=dedup_mode)
    typer.echo(f"Run complete. Report: {report_path}")


@app.command("prepare-rarity-baselines")
def prepare_rarity_baselines(
    out_dir: Path = typer.Option(Path("configs"), resolve_path=True),
    first_raw: Path | None = typer.Option(None, exists=True, readable=True, resolve_path=True),
    last_raw: Path | None = typer.Option(None, exists=True, readable=True, resolve_path=True),
    first_profile: BaselineProfileName = typer.Option(BaselineProfileName.ssa_first),
    last_profile: BaselineProfileName = typer.Option(BaselineProfileName.census_last),
    first_name_col: str | None = typer.Option(None),
    first_value_col: str | None = typer.Option(None),
    last_name_col: str | None = typer.Option(None),
    last_value_col: str | None = typer.Option(None),
    min_weight: float = typer.Option(1.0, min=0.0),
    write_config: Path | None = typer.Option(None, resolve_path=True),
    paths_relative_to_config: bool = typer.Option(True),
) -> None:
    """Normalize raw first/last-name frequency files into canonical lookup tables."""
    configure_logging()
    if first_raw is None and last_raw is None:
        raise typer.BadParameter("Provide at least one of --first-raw or --last-raw")

    output_messages: list[str] = []
    first_output_path: Path | None = None
    last_output_path: Path | None = None
    if first_raw is not None:
        first_output = out_dir / "first_name_frequency.csv"
        first_result = build_frequency_baseline_file(
            raw_path=first_raw,
            output_path=first_output,
            name_column=first_name_col,
            value_column=first_value_col,
            min_weight=min_weight,
            profile_name=first_profile,
        )
        first_output_path = first_result.output_path
        output_messages.append(
            "first:"
            f" {first_result.output_path}"
            f" rows={first_result.rows_output}"
            f" source_cols=({first_result.name_column_used}, {first_result.value_column_used})"
        )

    if last_raw is not None:
        last_output = out_dir / "last_name_frequency.csv"
        last_result = build_frequency_baseline_file(
            raw_path=last_raw,
            output_path=last_output,
            name_column=last_name_col,
            value_column=last_value_col,
            min_weight=min_weight,
            profile_name=last_profile,
        )
        last_output_path = last_result.output_path
        output_messages.append(
            "last:"
            f" {last_result.output_path}"
            f" rows={last_result.rows_output}"
            f" source_cols=({last_result.name_column_used}, {last_result.value_column_used})"
        )

    if write_config is not None:
        config_path = write_config
        if config_path.exists():
            with config_path.open("r", encoding="utf-8") as handle:
                config_data = yaml.safe_load(handle) or {}
        else:
            config_data = {}

        rarity_config = dict(config_data.get("rarity") or {})
        rarity_config["enabled"] = True
        if first_output_path is not None:
            rarity_config["first_name_frequency_path"] = (
                os.path.relpath(first_output_path, start=config_path.parent)
                if paths_relative_to_config
                else str(first_output_path)
            )
        if last_output_path is not None:
            rarity_config["last_name_frequency_path"] = (
                os.path.relpath(last_output_path, start=config_path.parent)
                if paths_relative_to_config
                else str(last_output_path)
            )
        config_data["rarity"] = rarity_config
        config_path.parent.mkdir(parents=True, exist_ok=True)
        with config_path.open("w", encoding="utf-8") as handle:
            yaml.safe_dump(config_data, handle, sort_keys=False)
        output_messages.append(f"config updated: {config_path}")

    typer.echo("Prepared rarity baselines")
    for line in output_messages:
        typer.echo(f"- {line}")


@app.command("import-submissions")
def import_submissions(
    csv: Path = typer.Option(..., exists=True, readable=True, resolve_path=True),
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, exists=True, readable=True, resolve_path=True),
    db_url: str | None = typer.Option(
        None,
        envvar=["TESTIFIER_AUDIT_DB_URL", "DATABASE_URL"],
        help="PostgreSQL connection string. Falls back to config.input.db_url.",
    ),
    table_name: str | None = typer.Option(
        None,
        help="Destination table name. Falls back to config.input.submissions_table.",
    ),
    source_file: str | None = typer.Option(
        None,
        help="Logical source file label stored in Postgres. Defaults to CSV file name.",
    ),
    chunk_size: int = typer.Option(50_000, min=1000),
    force: bool = typer.Option(
        False,
        help="Re-import even when an identical file checksum was already imported.",
    ),
) -> None:
    """Import legislature submissions CSV into PostgreSQL with normalized columns."""
    configure_logging()
    cfg = _load_app_config(config)

    effective_db_url = db_url or cfg.input.db_url
    if not effective_db_url:
        raise typer.BadParameter(
            "Missing database URL. "
            "Set --db-url or TESTIFIER_AUDIT_DB_URL or input.db_url in config."
        )

    effective_table_name = table_name or cfg.input.submissions_table
    result = import_submission_csv_to_postgres(
        csv_path=csv,
        db_url=effective_db_url,
        columns=cfg.columns,
        timezone=cfg.time.timezone,
        table_name=effective_table_name,
        chunk_size=int(chunk_size),
        source_file=source_file,
        force=force,
    )
    typer.echo("Submission import complete")
    typer.echo(f"- source_file: {result.source_file}")
    if result.file_hash:
        typer.echo(f"- file_hash: {result.file_hash}")
    typer.echo(f"- table_name: {result.table_name}")
    typer.echo(f"- rows_processed: {result.rows_processed}")
    typer.echo(f"- rows_upserted: {result.rows_upserted}")
    typer.echo(f"- rows_blank_organization: {result.rows_blank_organization}")
    typer.echo(f"- rows_invalid_timestamp: {result.rows_invalid_timestamp}")
    typer.echo(f"- chunk_size: {result.chunk_size}")
    typer.echo(f"- import_skipped: {str(result.import_skipped).lower()}")
    if result.skip_reason:
        typer.echo(f"- skip_reason: {result.skip_reason}")


@app.command("import-vrdb")
def import_vrdb(
    extract: Path = typer.Option(..., exists=True, readable=True, resolve_path=True),
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, exists=True, readable=True, resolve_path=True),
    db_url: str | None = typer.Option(
        None,
        envvar=["TESTIFIER_AUDIT_DB_URL", "DATABASE_URL"],
        help="PostgreSQL connection string. Falls back to config.voter_registry.db_url.",
    ),
    table_name: str | None = typer.Option(
        None,
        help="Destination table name. Falls back to config.voter_registry.table_name.",
    ),
    chunk_size: int = typer.Option(50_000, min=1000),
    force: bool = typer.Option(
        False,
        help="Re-import even when an identical file checksum was already imported.",
    ),
) -> None:
    """Import a VRDB extract into PostgreSQL with upsert semantics."""
    configure_logging()
    cfg = _load_app_config(config)

    effective_db_url = db_url or cfg.voter_registry.db_url
    if not effective_db_url:
        raise typer.BadParameter(
            "Missing database URL. Set --db-url or TESTIFIER_AUDIT_DB_URL "
            "or voter_registry.db_url in config."
        )

    effective_table_name = table_name or cfg.voter_registry.table_name
    result = import_vrdb_extract_to_postgres(
        extract_path=extract,
        db_url=effective_db_url,
        table_name=effective_table_name,
        chunk_size=int(chunk_size),
        force=force,
    )

    typer.echo("VRDB import complete")
    typer.echo(f"- source_file: {result.source_file}")
    if result.file_hash:
        typer.echo(f"- file_hash: {result.file_hash}")
    typer.echo(f"- table_name: {result.table_name}")
    typer.echo(f"- rows_processed: {result.rows_processed}")
    typer.echo(f"- rows_upserted: {result.rows_upserted}")
    typer.echo(f"- rows_with_state_voter_id: {result.rows_with_state_voter_id}")
    typer.echo(f"- rows_with_canonical_name: {result.rows_with_canonical_name}")
    typer.echo(f"- chunk_size: {result.chunk_size}")
    typer.echo(f"- import_skipped: {str(result.import_skipped).lower()}")
    if result.skip_reason:
        typer.echo(f"- skip_reason: {result.skip_reason}")


if __name__ == "__main__":
    app()
