from __future__ import annotations

import os
from pathlib import Path

import typer
import yaml

from testifier_audit.config import DEFAULT_CONFIG_PATH, AppConfig, load_config
from testifier_audit.io.rarity_baselines import BaselineProfileName, build_frequency_baseline_file
from testifier_audit.logging import configure_logging
from testifier_audit.paths import build_output_paths
from testifier_audit.pipeline.pass1_profile import build_profile_artifacts, load_profile_artifacts
from testifier_audit.pipeline.pass2_deep_dive import run_detectors
from testifier_audit.pipeline.run_all import run_all
from testifier_audit.report.render import render_report

app = typer.Typer(no_args_is_help=True, add_completion=False)


def _load_app_config(config_path: Path) -> AppConfig:
    return load_config(config_path)


@app.command()
def profile(
    csv: Path = typer.Option(..., exists=True, readable=True, resolve_path=True),
    out: Path = typer.Option(Path("out"), resolve_path=True),
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, exists=True, readable=True, resolve_path=True),
) -> None:
    """Build pass-1 profile artifacts from input CSV."""
    configure_logging()
    cfg = _load_app_config(config)
    paths = build_output_paths(out)
    artifacts = build_profile_artifacts(csv_path=csv, out_dir=paths.root, config=cfg)
    typer.echo(f"Profile complete. Artifacts: {', '.join(sorted(artifacts.keys()))}")


@app.command()
def detect(
    csv: Path = typer.Option(..., exists=True, readable=True, resolve_path=True),
    out: Path = typer.Option(Path("out"), resolve_path=True),
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, exists=True, readable=True, resolve_path=True),
    rebuild_profile: bool = typer.Option(False, help="Recompute profile artifacts before detection."),
) -> None:
    """Run detector pass using CSV and profile artifacts."""
    configure_logging()
    cfg = _load_app_config(config)
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
) -> None:
    """Render HTML report from existing outputs in out/."""
    configure_logging()
    _ = _load_app_config(config)
    build_output_paths(out)
    report_path = render_report(results={}, artifacts={}, out_dir=out)
    typer.echo(f"Report written to: {report_path}")


@app.command("run-all")
def run_all_command(
    csv: Path = typer.Option(..., exists=True, readable=True, resolve_path=True),
    out: Path = typer.Option(Path("out"), resolve_path=True),
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, exists=True, readable=True, resolve_path=True),
) -> None:
    """Execute profile, detect, and report in one command."""
    configure_logging()
    cfg = _load_app_config(config)
    report_path = run_all(csv_path=csv, out_dir=out, config=cfg)
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


if __name__ == "__main__":
    app()
