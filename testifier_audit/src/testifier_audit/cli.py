from __future__ import annotations

from pathlib import Path
from typing import Literal

import typer

from testifier_audit.config import DEFAULT_CONFIG_PATH, AppConfig, load_config
from testifier_audit.io.baseline_corpus_sampler import (
    DEFAULT_CSV_OUT_DIR as DEFAULT_BASELINE_SAMPLE_CSV_OUT_DIR,
)
from testifier_audit.io.baseline_corpus_sampler import (
    DEFAULT_INDEX_CSV as DEFAULT_BASELINE_SAMPLE_INDEX_CSV,
)
from testifier_audit.io.baseline_corpus_sampler import (
    DEFAULT_INDEX_JSON as DEFAULT_BASELINE_SAMPLE_INDEX_JSON,
)
from testifier_audit.io.baseline_corpus_sampler import (
    DEFAULT_MANIFEST_OUT as DEFAULT_BASELINE_SAMPLE_MANIFEST_OUT,
)
from testifier_audit.io.baseline_corpus_sampler import (
    DEFAULT_MEETING_ITEMS_CACHE_DIR as DEFAULT_BASELINE_SAMPLE_MEETING_ITEMS_CACHE_DIR,
)
from testifier_audit.io.baseline_corpus_sampler import (
    DEFAULT_MEETINGS_CACHE_MAX_AGE_HOURS as DEFAULT_BASELINE_SAMPLE_MEETINGS_CACHE_MAX_AGE_HOURS,
)
from testifier_audit.io.baseline_corpus_sampler import (
    DEFAULT_METADATA_OUT_DIR as DEFAULT_BASELINE_SAMPLE_METADATA_OUT_DIR,
)
from testifier_audit.io.baseline_corpus_sampler import (
    sample_unsampled_baseline_corpus,
)
from testifier_audit.io.baseline_corpus_sampler import (
    write_manifest as write_baseline_sample_manifest,
)
from testifier_audit.io.csi_testifiers import download_csi_testifier_csv
from testifier_audit.io.hearing_metadata import load_hearing_metadata
from testifier_audit.io.submissions_postgres import import_submission_csv_to_postgres
from testifier_audit.io.vrdb_probability_artifacts import (
    build_and_write_vrdb_probability_artifacts_from_postgres,
)
from testifier_audit.io.vrdb_postgres import import_vrdb_extract_to_postgres
from testifier_audit.logging import configure_logging
from testifier_audit.paths import build_output_paths
from testifier_audit.pipeline.pass1_profile import (
    build_profile_artifacts,
    load_profile_artifacts,
    prepare_base_dataframe,
)
from testifier_audit.pipeline.pass2_deep_dive import run_detectors
from testifier_audit.pipeline.run_all import run_all
from testifier_audit.report.render import render_report

app = typer.Typer(no_args_is_help=True, add_completion=False)
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CSI_CSV_OUT_DIR = REPO_ROOT / "data" / "raw"
DEFAULT_CSI_METADATA_OUT_DIR = REPO_ROOT / "data" / "metadata"


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
    base_df = None
    if rebuild_profile or not artifacts:
        base_df = prepare_base_dataframe(csv_path=csv, config=cfg)
        artifacts = build_profile_artifacts(
            csv_path=csv,
            out_dir=paths.root,
            config=cfg,
            base_df=base_df,
        )

    results = run_detectors(
        csv_path=csv,
        artifacts=artifacts,
        out_dir=paths.root,
        config=cfg,
        base_df=base_df,
    )
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
    source_file: str | None = typer.Option(
        None,
        help=(
            "Filter postgres-mode input to a specific source_file label. "
            "Ignored in csv mode."
        ),
    ),
    comparative: bool = typer.Option(
        False,
        help=(
            "Allow postgres-mode analysis over all source_file values. "
            "Use only for explicitly comparative multi-source reports."
        ),
    ),
) -> None:
    """Execute profile, detect, and report in one command."""
    configure_logging()
    cfg = _load_app_config(config)
    _apply_hearing_metadata_override(cfg, hearing_metadata)
    if source_file is not None and getattr(cfg, "input", None) is not None:
        cfg.input.source_file = source_file
    if (
        getattr(cfg, "input", None) is not None
        and cfg.input.mode == "postgres"
        and not getattr(cfg.input, "source_file", None)
        and csv is not None
    ):
        # When a CSV path is provided in postgres mode, default to its basename
        # as the source-file filter to prevent accidental multi-source conflation.
        cfg.input.source_file = csv.name
    if (
        getattr(cfg, "input", None) is not None
        and cfg.input.mode == "postgres"
        and not str(getattr(cfg.input, "source_file", "") or "").strip()
        and not comparative
    ):
        raise typer.BadParameter(
            "Postgres mode requires a single source_file to avoid cross-dataset conflation. "
            "Set --source-file (or input.source_file in config). "
            "Use --comparative only for explicit multi-source comparative reports."
        )
    csv = _require_csv_for_csv_mode(csv=csv, cfg=cfg)
    if dedup_mode is None:
        report_path = run_all(csv_path=csv, out_dir=out, config=cfg)
    else:
        report_path = run_all(csv_path=csv, out_dir=out, config=cfg, dedup_mode=dedup_mode)
    typer.echo(f"Run complete. Report: {report_path}")


@app.command("download-csi-testifiers")
def download_csi_testifiers_command(
    bill_query: str = typer.Argument(
        ...,
        help="Partial or full bill identifier/title used in CSI search.",
    ),
    csv_out_dir: Path = typer.Option(
        DEFAULT_CSI_CSV_OUT_DIR,
        resolve_path=True,
        help="Directory where combined CSV output will be written.",
    ),
    metadata_out_dir: Path = typer.Option(
        DEFAULT_CSI_METADATA_OUT_DIR,
        resolve_path=True,
        help="Directory where hearing metadata sidecar YAML will be written.",
    ),
    meeting_index: int = typer.Option(
        0,
        min=0,
        help="Index within matching meeting candidates (0 = most recent).",
    ),
    agenda_index: int = typer.Option(
        0,
        min=0,
        help="Index within matching agenda-item candidates (0 = first best match).",
    ),
    meeting_family_id: str | None = typer.Option(
        None,
        help="Optional explicit meetingFamilyId to force selection.",
    ),
    agenda_item_id: str | None = typer.Option(
        None,
        help="Optional explicit agenda item id to force selection.",
    ),
    top: int = typer.Option(
        100,
        min=1,
        max=500,
        help="Max number of meetings to request from SearchMeetings.",
    ),
    timeout_seconds: float = typer.Option(
        30.0,
        min=1.0,
        help="HTTP timeout in seconds for each request.",
    ),
    max_retries: int = typer.Option(
        3,
        min=0,
        max=10,
        help="Number of retries for transient HTTP/network failures.",
    ),
    retry_backoff_seconds: float = typer.Option(
        1.5,
        min=0.1,
        help="Base backoff seconds for retry delays (exponential).",
    ),
    overwrite: bool = typer.Option(
        True,
        "--overwrite/--no-overwrite",
        help="Replace existing CSV/sidecar files when output paths already exist.",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable debug logging for request/selection details.",
    ),
) -> None:
    """Download WA CSI testifier data, write combined CSV, and write hearing metadata sidecar."""
    configure_logging(level="DEBUG" if verbose else "INFO")
    result = download_csi_testifier_csv(
        bill_query=bill_query,
        csv_out_dir=csv_out_dir,
        metadata_out_dir=metadata_out_dir,
        meeting_index=meeting_index,
        agenda_index=agenda_index,
        meeting_family_id=meeting_family_id,
        agenda_item_id=agenda_item_id,
        top=top,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        overwrite=overwrite,
    )

    typer.echo("CSI testifier download complete")
    typer.echo(f"- search_query: {result.search_query}")
    typer.echo(f"- short_bill_id: {result.short_bill_id}")
    typer.echo(f"- bill_title: {result.bill_title}")
    typer.echo(f"- meeting_family_id: {result.meeting_family_id}")
    typer.echo(f"- agenda_item_family_id: {result.agenda_item_family_id}")
    typer.echo(f"- agenda_item_id: {result.agenda_item_id}")
    typer.echo(f"- meeting_start_pacific: {result.meeting_start.isoformat()}")
    typer.echo(f"- testifying_rows: {result.testifying_rows}")
    typer.echo(f"- not_testifying_rows: {result.not_testifying_rows}")
    typer.echo(f"- total_rows: {result.total_rows}")
    typer.echo(f"- csv_path: {result.csv_path}")
    typer.echo(f"- hearing_metadata_path: {result.metadata_path}")


@app.command("sample-baseline-corpus")
def sample_baseline_corpus_command(
    sample_size: int = typer.Option(
        ...,
        min=1,
        help="Number of unsampled hearings to sample and download.",
    ),
    session_count: int = typer.Option(
        3,
        min=1,
        help="How many most-recent session years to include.",
    ),
    index_json: Path = typer.Option(
        DEFAULT_BASELINE_SAMPLE_INDEX_JSON,
        resolve_path=True,
        help="Cached GetCommitteeMeetings JSON path.",
    ),
    index_csv: Path = typer.Option(
        DEFAULT_BASELINE_SAMPLE_INDEX_CSV,
        resolve_path=True,
        help="Legacy option retained for compatibility (unused).",
    ),
    meeting_items_cache_dir: Path = typer.Option(
        DEFAULT_BASELINE_SAMPLE_MEETING_ITEMS_CACHE_DIR,
        resolve_path=True,
        help=(
            "Directory containing per-meeting GetCommitteeMeetingItems cache files. "
            "Missing meeting files are fetched on demand."
        ),
    ),
    meetings_cache_max_age_hours: float = typer.Option(
        float(DEFAULT_BASELINE_SAMPLE_MEETINGS_CACHE_MAX_AGE_HOURS),
        min=0.0,
        help="Max cache age before GetCommitteeMeetings is refreshed.",
    ),
    max_meeting_items_fetches: int | None = typer.Option(
        None,
        min=0,
        help=(
            "Maximum uncached GetCommitteeMeetingItems requests per run. "
            "Defaults to sample-size."
        ),
    ),
    csv_out_dir: Path = typer.Option(
        DEFAULT_BASELINE_SAMPLE_CSV_OUT_DIR,
        resolve_path=True,
        help="Directory where sampled CSV files will be written.",
    ),
    metadata_out_dir: Path = typer.Option(
        DEFAULT_BASELINE_SAMPLE_METADATA_OUT_DIR,
        resolve_path=True,
        help="Directory where sampled hearing metadata sidecars will be written.",
    ),
    manifest_out: Path = typer.Option(
        DEFAULT_BASELINE_SAMPLE_MANIFEST_OUT,
        resolve_path=True,
        help="Output manifest JSON path.",
    ),
    sampled_metadata_dir: list[Path] | None = typer.Option(
        None,
        resolve_path=True,
        help=(
            "Additional metadata directory with existing sidecars to treat as already sampled. "
            "Repeat the option for multiple directories."
        ),
    ),
    refresh_index: bool = typer.Option(
        False,
        "--refresh-index/--no-refresh-index",
        help="Force refreshing cached GetCommitteeMeetings data.",
    ),
    seed: int | None = typer.Option(
        None,
        help="Optional random seed for reproducible sampling.",
    ),
    rate_limit_seconds: float = typer.Option(
        1.0,
        min=0.0,
        help="Sleep duration between sampled download requests.",
    ),
    timeout_seconds: float = typer.Option(
        30.0,
        min=1.0,
        help="HTTP timeout for index and CSI requests.",
    ),
    max_retries: int = typer.Option(
        3,
        min=0,
        max=10,
        help="Retry count for transient CSI errors.",
    ),
    retry_backoff_seconds: float = typer.Option(
        1.5,
        min=0.1,
        help="Base retry backoff seconds for CSI requests.",
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite/--no-overwrite",
        help="Replace existing sampled CSV/sidecar files if output paths already exist.",
    ),
) -> None:
    """Sample N unsampled hearings and download CSV + sidecars with request-aware rate limiting."""
    configure_logging()
    extra_metadata_dirs = sampled_metadata_dir or []
    manifest = sample_unsampled_baseline_corpus(
        sample_size=sample_size,
        session_count=session_count,
        index_json_path=index_json,
        index_csv_path=index_csv,
        meeting_items_cache_dir=meeting_items_cache_dir,
        csv_out_dir=csv_out_dir,
        metadata_out_dir=metadata_out_dir,
        manifest_path=manifest_out,
        sampled_metadata_dirs=extra_metadata_dirs,
        refresh_index=refresh_index,
        seed=seed,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        rate_limit_seconds=rate_limit_seconds,
        meetings_cache_max_age_hours=meetings_cache_max_age_hours,
        max_meeting_items_fetches=max_meeting_items_fetches,
        overwrite=overwrite,
    )
    write_baseline_sample_manifest(manifest_out, manifest)
    typer.echo("Baseline corpus sampling complete")
    typer.echo(f"- session_years: {', '.join(str(year) for year in manifest['session_years'])}")
    typer.echo(f"- sample_size_requested: {manifest['sample_size_requested']}")
    typer.echo(f"- sample_size_selected: {manifest['sample_size_selected']}")
    typer.echo(f"- sample_size_downloaded: {manifest['sample_size_downloaded']}")
    typer.echo(f"- sample_size_failed: {manifest['sample_size_failed']}")
    typer.echo(f"- index_refreshed: {str(manifest['index_refreshed']).lower()}")
    typer.echo(f"- manifest_path: {manifest_out}")


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
        nickname_map_path=cfg.names.nickname_map_path,
        normalize_unicode=cfg.names.normalize_unicode,
        strip_punctuation=cfg.names.strip_punctuation,
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
    typer.echo(f"- normalization_version: {result.normalization_version}")
    typer.echo(f"- normalization_version_hash: {result.normalization_version_hash}")
    typer.echo(f"- chunk_size: {result.chunk_size}")
    typer.echo(f"- import_skipped: {str(result.import_skipped).lower()}")
    if result.skip_reason:
        typer.echo(f"- skip_reason: {result.skip_reason}")


@app.command("build-vrdb-probability-artifacts")
def build_vrdb_probability_artifacts(
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, exists=True, readable=True, resolve_path=True),
    db_url: str | None = typer.Option(
        None,
        envvar=["TESTIFIER_AUDIT_DB_URL", "DATABASE_URL"],
        help="PostgreSQL connection string. Falls back to config.voter_registry.db_url.",
    ),
    table_name: str | None = typer.Option(
        None,
        help="Source VRDB table name. Falls back to config.voter_registry.table_name.",
    ),
    probability_csv: Path = typer.Option(
        Path("data/metadata/vrdb_name_probabilities.csv"),
        resolve_path=True,
        help="Output CSV path for name probability rows.",
    ),
    backoff_csv: Path = typer.Option(
        Path("data/metadata/vrdb_geo_backoff.csv"),
        resolve_path=True,
        help="Output CSV path for geography backoff rows.",
    ),
    metadata_json: Path = typer.Option(
        Path("data/metadata/vrdb_probability_artifacts.json"),
        resolve_path=True,
        help="Output JSON metadata path with checksums and provenance.",
    ),
    chunk_size: int = typer.Option(250_000, min=1000, help="Streaming fetch chunk size."),
    min_county_denominator: int = typer.Option(
        1_000,
        min=1,
        help="Minimum county denominator required before county-conditioned rows are published.",
    ),
    min_city_denominator: int = typer.Option(
        250,
        min=1,
        help="Minimum city denominator required before city-conditioned rows are published.",
    ),
    min_city_coverage: float = typer.Option(
        0.75,
        min=0.0,
        max=1.0,
        help="Minimum county-level city coverage ratio required before city-conditioned rows are published.",
    ),
    include_marginals: bool = typer.Option(
        False,
        help="Also emit first-name and last-name marginals alongside full-name rows.",
    ),
) -> None:
    """Build versioned VRDB probability + geography-backoff artifacts from PostgreSQL."""
    configure_logging()
    cfg = _load_app_config(config)

    effective_db_url = db_url or cfg.voter_registry.db_url
    if not effective_db_url:
        raise typer.BadParameter(
            "Missing database URL. Set --db-url or TESTIFIER_AUDIT_DB_URL "
            "or voter_registry.db_url in config."
        )

    effective_table_name = table_name or cfg.voter_registry.table_name
    result = build_and_write_vrdb_probability_artifacts_from_postgres(
        db_url=effective_db_url,
        table_name=effective_table_name,
        probability_rows_path=probability_csv,
        backoff_rows_path=backoff_csv,
        metadata_path=metadata_json,
        chunk_size=int(chunk_size),
        include_marginals=bool(include_marginals),
        min_county_denominator=int(min_county_denominator),
        min_city_denominator=int(min_city_denominator),
        min_city_coverage=float(min_city_coverage),
    )

    typer.echo("VRDB probability artifact build complete")
    typer.echo(f"- table_name: {effective_table_name}")
    typer.echo(f"- probability_rows_path: {result.probability_rows_path}")
    typer.echo(f"- backoff_rows_path: {result.backoff_rows_path}")
    typer.echo(f"- metadata_path: {result.metadata_path}")
    typer.echo(f"- probability_row_count: {result.probability_row_count}")
    typer.echo(f"- backoff_row_count: {result.backoff_row_count}")
    typer.echo(f"- probability_rows_sha256: {result.probability_rows_sha256}")
    typer.echo(f"- backoff_rows_sha256: {result.backoff_rows_sha256}")
    typer.echo(f"- vrdb_version: {result.vrdb_version}")
    typer.echo(f"- normalization_version: {result.normalization_version}")


if __name__ == "__main__":
    app()
