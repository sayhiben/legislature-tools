from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from testifier_audit.config import AppConfig
from testifier_audit.detectors.base import DetectorResult
from testifier_audit.detectors.registry import default_detectors
from testifier_audit.io.write import write_summary, write_table
from testifier_audit.paths import OutputPaths, build_output_paths
from testifier_audit.pipeline.pass1_profile import prepare_base_dataframe
from testifier_audit.report.analysis_registry import (
    configured_analysis_ids as registry_configured_analysis_ids,
)
from testifier_audit.report.analysis_registry import (
    configured_detector_names as registry_configured_detector_names,
)
from testifier_audit.viz.distributions import (
    plot_burst_null_distribution,
)
from testifier_audit.viz.heatmaps import (
    plot_pro_rate_day_hour_heatmap,
)
from testifier_audit.viz.time_series import (
    plot_counts_with_annotations,
    plot_organization_blank_rates,
    plot_pro_rate_with_annotations,
    plot_voter_registry_match_rates,
)

LOGGER = logging.getLogger(__name__)


def _series_to_table(series: pd.Series, value_column: str) -> pd.DataFrame:
    return (
        series.rename(value_column).to_frame().reset_index().rename(columns={"index": "row_index"})
    )


def _remove_stale_detector_outputs(paths: OutputPaths, detector_names: set[str]) -> None:
    for detector_name in sorted(detector_names):
        normalized = str(detector_name or "").strip()
        if not normalized:
            continue
        (paths.summary / f"{normalized}.json").unlink(missing_ok=True)
        for directory in (paths.tables, paths.flags):
            for artifact_path in directory.glob(f"{normalized}__*.*"):
                artifact_path.unlink(missing_ok=True)


def _resolved_heatmap_bucket_minutes(config: AppConfig) -> list[int]:
    resolved = sorted({int(value) for value in config.windows.analysis_bucket_minutes if int(value) > 0})
    return resolved or [1]


def _remove_stale_overlay_figures(
    paths: OutputPaths,
    figure_suffix: str,
    bucket_minutes: list[int],
) -> None:
    suffix = str(figure_suffix or "").strip().lstrip(".") or "png"
    figure_names = {
        "counts_with_anomalies",
        "pro_rate_with_anomalies",
        "bursts_null_distribution",
        "pro_rate_heatmap_day_hour",
        "organization_blank_rates",
        "voter_registry_match_rates",
    }
    for bucket in bucket_minutes:
        figure_names.add(f"pro_rate_heatmap_day_hour_{int(bucket)}m")
    for figure_name in sorted(figure_names):
        (paths.figures / f"{figure_name}.{suffix}").unlink(missing_ok=True)


def _render_detector_figures(
    feature_context: dict[str, pd.DataFrame],
    out_dir: Path,
    config: AppConfig,
) -> None:
    paths = build_output_paths(out_dir)
    figure_suffix = config.outputs.figures_format

    counts = feature_context.get("counts_per_minute", pd.DataFrame())
    if counts.empty:
        return

    bursts = feature_context.get("bursts.burst_significant_windows", pd.DataFrame())
    burst_tests = feature_context.get("bursts.burst_window_tests", pd.DataFrame())
    burst_null_distribution = feature_context.get("bursts.burst_null_distribution", pd.DataFrame())
    organization_blank_rates = feature_context.get(
        "org_anomalies.organization_blank_rate_by_bucket",
        pd.DataFrame(),
    )
    voter_match_by_bucket = feature_context.get(
        "voter_registry_match.match_by_bucket", pd.DataFrame()
    )
    heatmap_bucket_minutes = _resolved_heatmap_bucket_minutes(config)

    try:
        plot_counts_with_annotations(
            counts_per_minute=counts,
            burst_windows=bursts,
            output_path=paths.figures / f"counts_with_anomalies.{figure_suffix}",
        )
        plot_pro_rate_with_annotations(
            counts_per_minute=counts,
            swing_windows=pd.DataFrame(),
            output_path=paths.figures / f"pro_rate_with_anomalies.{figure_suffix}",
        )
        plot_burst_null_distribution(
            null_distribution=burst_null_distribution,
            burst_tests=burst_tests,
            output_path=paths.figures / f"bursts_null_distribution.{figure_suffix}",
        )
        plot_pro_rate_day_hour_heatmap(
            counts_per_minute=counts,
            output_path=paths.figures / f"pro_rate_heatmap_day_hour.{figure_suffix}",
        )
        for bucket_minutes in heatmap_bucket_minutes:
            plot_pro_rate_day_hour_heatmap(
                counts_per_minute=counts,
                output_path=paths.figures
                / f"pro_rate_heatmap_day_hour_{int(bucket_minutes)}m.{figure_suffix}",
                bucket_minutes=bucket_minutes,
            )
        plot_organization_blank_rates(
            blank_rate_by_bucket=organization_blank_rates,
            output_path=paths.figures / f"organization_blank_rates.{figure_suffix}",
        )
        plot_voter_registry_match_rates(
            match_by_bucket=voter_match_by_bucket,
            output_path=paths.figures / f"voter_registry_match_rates.{figure_suffix}",
        )
    except Exception:  # pragma: no cover
        LOGGER.exception("Failed rendering detector overlay figures")


def run_detectors(
    csv_path: Path | None,
    artifacts: dict[str, pd.DataFrame],
    out_dir: Path,
    config: AppConfig,
    *,
    base_df: pd.DataFrame | None = None,
) -> dict[str, DetectorResult]:
    paths = build_output_paths(out_dir)
    df = base_df.copy() if base_df is not None else prepare_base_dataframe(csv_path=csv_path, config=config)

    extension = "parquet" if config.outputs.tables_format == "parquet" else "csv"
    feature_context: dict[str, pd.DataFrame] = dict(artifacts)
    analysis_scope_ids = registry_configured_analysis_ids()
    scoped_detector_names = registry_configured_detector_names()
    all_detector_instances = default_detectors(config)
    detector_instances = list(all_detector_instances)
    if analysis_scope_ids:
        detector_instances = [
            detector for detector in detector_instances if detector.name in scoped_detector_names
        ]
        skipped_detector_names = {detector.name for detector in all_detector_instances} - {
            detector.name for detector in detector_instances
        }
        _remove_stale_detector_outputs(paths, skipped_detector_names)
        LOGGER.info(
            "Scoped detector execution to analyses: %s (detectors: %s)",
            ", ".join(analysis_scope_ids),
            ", ".join(sorted({detector.name for detector in detector_instances})),
        )
        _remove_stale_overlay_figures(
            paths=paths,
            figure_suffix=config.outputs.figures_format,
            bucket_minutes=_resolved_heatmap_bucket_minutes(config),
        )

    results: dict[str, DetectorResult] = {}
    for detector in detector_instances:
        result = detector.run(df=df, features=feature_context)
        results[result.detector] = result

        write_summary(result.summary, paths.summary / f"{result.detector}.json")
        for table_name, table in result.tables.items():
            write_table(
                table,
                paths.tables / f"{result.detector}__{table_name}.{extension}",
                fmt=config.outputs.tables_format,
            )
            feature_context[f"{result.detector}.{table_name}"] = table

        if result.record_scores is not None:
            write_table(
                _series_to_table(result.record_scores, "score"),
                paths.flags / f"{result.detector}__record_scores.{extension}",
                fmt=config.outputs.tables_format,
            )
        if result.record_flags is not None:
            write_table(
                _series_to_table(result.record_flags, "flag"),
                paths.flags / f"{result.detector}__record_flags.{extension}",
                fmt=config.outputs.tables_format,
            )

    if not analysis_scope_ids:
        _render_detector_figures(feature_context=feature_context, out_dir=out_dir, config=config)
    return results
