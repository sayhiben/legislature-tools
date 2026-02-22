from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from testifier_audit.config import AppConfig
from testifier_audit.features.aggregates import (
    build_basic_quality,
    build_counts_per_hour,
    build_counts_per_minute,
    build_name_frequency,
)
from testifier_audit.features.text_features import build_name_text_features
from testifier_audit.io.hearing_metadata import load_hearing_metadata
from testifier_audit.io.read import load_records, load_table
from testifier_audit.io.write import write_table
from testifier_audit.paths import build_output_paths
from testifier_audit.preprocess.names import add_name_features
from testifier_audit.preprocess.position import normalize_position
from testifier_audit.preprocess.time import add_time_features
from testifier_audit.report.analysis_registry import (
    configured_analysis_ids as registry_configured_analysis_ids,
)
from testifier_audit.viz.distributions import plot_name_length_distribution
from testifier_audit.viz.heatmaps import plot_day_hour_heatmap
from testifier_audit.viz.names import plot_top_names
from testifier_audit.viz.time_series import plot_counts_per_minute

LOGGER = logging.getLogger(__name__)


def _remove_stale_profile_figures(out_dir: Path, figure_suffix: str) -> None:
    paths = build_output_paths(out_dir)
    suffix = str(figure_suffix or "").strip().lstrip(".") or "png"
    for figure_name in (
        "counts_per_minute",
        "counts_heatmap_day_hour",
        "top_duplicate_names",
        "name_length_distribution",
    ):
        (paths.figures / f"{figure_name}.{suffix}").unlink(missing_ok=True)


def prepare_base_dataframe(csv_path: Path | None, config: AppConfig) -> pd.DataFrame:
    df = load_records(csv_path=csv_path, config=config)
    df = add_name_features(df=df, config=config.names)
    df = normalize_position(df=df)
    hearing_metadata = load_hearing_metadata(config.input.hearing_metadata_path)
    df = add_time_features(df=df, config=config.time, hearing_metadata=hearing_metadata)
    return df


def _render_profile_figures(
    artifacts: dict[str, pd.DataFrame],
    out_dir: Path,
    config: AppConfig,
) -> None:
    paths = build_output_paths(out_dir)
    figure_suffix = config.outputs.figures_format

    try:
        counts_per_minute = artifacts.get("counts_per_minute", pd.DataFrame())
        if not counts_per_minute.empty:
            plot_counts_per_minute(
                counts_per_minute,
                paths.figures / f"counts_per_minute.{figure_suffix}",
            )

        counts_per_hour = artifacts.get("counts_per_hour", pd.DataFrame())
        if not counts_per_hour.empty:
            plot_day_hour_heatmap(
                counts_per_hour,
                paths.figures / f"counts_heatmap_day_hour.{figure_suffix}",
            )

        name_frequency = artifacts.get("name_frequency", pd.DataFrame())
        if not name_frequency.empty:
            plot_top_names(
                name_frequency,
                paths.figures / f"top_duplicate_names.{figure_suffix}",
            )

        name_text_features = artifacts.get("name_text_features", pd.DataFrame())
        if not name_text_features.empty:
            plot_name_length_distribution(
                name_text_features,
                paths.figures / f"name_length_distribution.{figure_suffix}",
            )
    except Exception:  # pragma: no cover
        LOGGER.exception("Failed rendering one or more profile figures")


def build_profile_artifacts(
    csv_path: Path | None, out_dir: Path, config: AppConfig
) -> dict[str, pd.DataFrame]:
    paths = build_output_paths(out_dir)
    df = prepare_base_dataframe(csv_path=csv_path, config=config)

    artifacts: dict[str, pd.DataFrame] = {
        "counts_per_minute": build_counts_per_minute(df),
        "counts_per_hour": build_counts_per_hour(df),
        "name_frequency": build_name_frequency(df),
        "name_text_features": build_name_text_features(df),
        "basic_quality": build_basic_quality(df),
    }

    extension = "parquet" if config.outputs.tables_format == "parquet" else "csv"
    for name, table in artifacts.items():
        output_path = paths.artifacts / f"{name}.{extension}"
        write_table(table, output_path, fmt=config.outputs.tables_format)

    analysis_scope_ids = registry_configured_analysis_ids()
    if analysis_scope_ids and "baseline_profile" not in set(analysis_scope_ids):
        _remove_stale_profile_figures(out_dir=out_dir, figure_suffix=config.outputs.figures_format)
        return artifacts

    _render_profile_figures(artifacts=artifacts, out_dir=out_dir, config=config)
    return artifacts


def _coerce_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    datetime_candidates = {
        "minute_bucket",
        "start_minute",
        "end_minute",
        "first_seen",
        "last_seen",
        "timestamp",
    }
    for column in working.columns:
        if column in datetime_candidates:
            working[column] = pd.to_datetime(working[column], errors="ignore")
    return working


def load_profile_artifacts(out_dir: Path, config: AppConfig) -> dict[str, pd.DataFrame]:
    paths = build_output_paths(out_dir)
    extension = ".parquet" if config.outputs.tables_format == "parquet" else ".csv"

    artifacts: dict[str, pd.DataFrame] = {}
    for path in sorted(paths.artifacts.glob(f"*{extension}")):
        artifacts[path.stem] = _coerce_datetime_columns(load_table(path))
    return artifacts
