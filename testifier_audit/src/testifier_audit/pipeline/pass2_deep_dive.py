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
    plot_periodicity_autocorrelation,
    plot_periodicity_clockface,
    plot_periodicity_spectrum,
    plot_swing_null_distribution,
)
from testifier_audit.viz.heatmaps import (
    plot_pro_rate_day_hour_heatmap,
    plot_ratio_shift_heatmap_by_bucket,
)
from testifier_audit.viz.time_series import (
    plot_counts_with_annotations,
    plot_multivariate_anomaly_scores,
    plot_organization_blank_rates,
    plot_pro_rate_bucket_trends,
    plot_pro_rate_with_annotations,
    plot_time_of_day_ratio_profiles,
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


def _remove_stale_overlay_figures(paths: OutputPaths, figure_suffix: str) -> None:
    suffix = str(figure_suffix or "").strip().lstrip(".") or "png"
    figure_names = {
        "counts_with_anomalies",
        "pro_rate_with_anomalies",
        "bursts_null_distribution",
        "swing_null_distribution",
        "periodicity_autocorr",
        "periodicity_spectrum",
        "periodicity_clockface",
        "pro_rate_heatmap_day_hour",
        "pro_rate_bucket_trends",
        "pro_rate_time_of_day_profiles",
        "organization_blank_rates",
        "voter_registry_match_rates",
        "multivariate_anomaly_scores",
    }
    for bucket_minutes in (1, 5, 15, 30, 60, 120, 240):
        figure_names.add(f"pro_rate_heatmap_day_hour_{int(bucket_minutes)}m")
        figure_names.add(f"pro_rate_shift_heatmap_{int(bucket_minutes)}m")
        figure_names.add(f"pro_rate_bucket_trends_{int(bucket_minutes)}m")
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
    swings = feature_context.get("procon_swings.swing_significant_windows", pd.DataFrame())
    swing_tests = feature_context.get("procon_swings.swing_window_tests", pd.DataFrame())
    swing_null_distribution = feature_context.get(
        "procon_swings.swing_null_distribution", pd.DataFrame()
    )
    time_bucket_profiles = feature_context.get("procon_swings.time_bucket_profiles", pd.DataFrame())
    time_of_day_bucket_profiles = feature_context.get(
        "procon_swings.time_of_day_bucket_profiles", pd.DataFrame()
    )
    day_bucket_profiles = feature_context.get("procon_swings.day_bucket_profiles", pd.DataFrame())
    organization_blank_rates = feature_context.get(
        "org_anomalies.organization_blank_rate_by_bucket",
        pd.DataFrame(),
    )
    voter_match_by_bucket = feature_context.get(
        "voter_registry_match.match_by_bucket", pd.DataFrame()
    )
    multivariate_scores = feature_context.get(
        "multivariate_anomalies.bucket_anomaly_scores", pd.DataFrame()
    )
    periodicity_autocorr = feature_context.get("periodicity.autocorr", pd.DataFrame())
    periodicity_spectrum = feature_context.get("periodicity.spectrum_top", pd.DataFrame())
    periodicity_clockface = feature_context.get(
        "periodicity.clockface_distribution", pd.DataFrame()
    )
    volume_changes = feature_context.get("changepoints.volume_changepoints", pd.DataFrame())
    pro_rate_changes = feature_context.get("changepoints.pro_rate_changepoints", pd.DataFrame())

    try:
        plot_counts_with_annotations(
            counts_per_minute=counts,
            burst_windows=bursts,
            volume_changepoints=volume_changes,
            output_path=paths.figures / f"counts_with_anomalies.{figure_suffix}",
        )
        plot_pro_rate_with_annotations(
            counts_per_minute=counts,
            swing_windows=swings,
            pro_rate_changepoints=pro_rate_changes,
            output_path=paths.figures / f"pro_rate_with_anomalies.{figure_suffix}",
        )
        plot_burst_null_distribution(
            null_distribution=burst_null_distribution,
            burst_tests=burst_tests,
            output_path=paths.figures / f"bursts_null_distribution.{figure_suffix}",
        )
        plot_swing_null_distribution(
            null_distribution=swing_null_distribution,
            swing_tests=swing_tests,
            output_path=paths.figures / f"swing_null_distribution.{figure_suffix}",
        )
        plot_periodicity_autocorrelation(
            autocorr=periodicity_autocorr,
            output_path=paths.figures / f"periodicity_autocorr.{figure_suffix}",
        )
        plot_periodicity_spectrum(
            spectrum_top=periodicity_spectrum,
            output_path=paths.figures / f"periodicity_spectrum.{figure_suffix}",
        )
        plot_periodicity_clockface(
            clockface_distribution=periodicity_clockface,
            output_path=paths.figures / f"periodicity_clockface.{figure_suffix}",
        )
        plot_pro_rate_day_hour_heatmap(
            counts_per_minute=counts,
            output_path=paths.figures / f"pro_rate_heatmap_day_hour.{figure_suffix}",
        )
        for bucket_minutes in (1, 5, 15, 30, 60, 120, 240):
            plot_pro_rate_day_hour_heatmap(
                counts_per_minute=counts,
                output_path=paths.figures
                / f"pro_rate_heatmap_day_hour_{int(bucket_minutes)}m.{figure_suffix}",
                bucket_minutes=bucket_minutes,
            )
        for bucket_minutes in (1, 5, 15, 30, 60, 120, 240):
            plot_ratio_shift_heatmap_by_bucket(
                day_bucket_profiles=day_bucket_profiles,
                bucket_minutes=bucket_minutes,
                output_path=paths.figures
                / f"pro_rate_shift_heatmap_{int(bucket_minutes)}m.{figure_suffix}",
            )
        plot_pro_rate_bucket_trends(
            time_bucket_profiles=time_bucket_profiles,
            output_path=paths.figures / f"pro_rate_bucket_trends.{figure_suffix}",
        )
        requested_bucket_variants = (1, 5, 15, 30, 60, 120, 240)
        available_bucket_variants = set(
            time_bucket_profiles.get("bucket_minutes", pd.Series(dtype=float))
            .dropna()
            .astype(int)
            .tolist()
        )
        for bucket_minutes in requested_bucket_variants:
            if bucket_minutes not in available_bucket_variants:
                continue
            plot_pro_rate_bucket_trends(
                time_bucket_profiles=time_bucket_profiles,
                output_path=paths.figures
                / f"pro_rate_bucket_trends_{int(bucket_minutes)}m.{figure_suffix}",
                preferred_buckets=(int(bucket_minutes),),
            )
        plot_time_of_day_ratio_profiles(
            time_of_day_bucket_profiles=time_of_day_bucket_profiles,
            output_path=paths.figures / f"pro_rate_time_of_day_profiles.{figure_suffix}",
        )
        plot_organization_blank_rates(
            blank_rate_by_bucket=organization_blank_rates,
            output_path=paths.figures / f"organization_blank_rates.{figure_suffix}",
        )
        plot_voter_registry_match_rates(
            match_by_bucket=voter_match_by_bucket,
            output_path=paths.figures / f"voter_registry_match_rates.{figure_suffix}",
        )
        plot_multivariate_anomaly_scores(
            bucket_anomaly_scores=multivariate_scores,
            output_path=paths.figures / f"multivariate_anomaly_scores.{figure_suffix}",
        )
    except Exception:  # pragma: no cover
        LOGGER.exception("Failed rendering detector overlay figures")


def run_detectors(
    csv_path: Path | None,
    artifacts: dict[str, pd.DataFrame],
    out_dir: Path,
    config: AppConfig,
) -> dict[str, DetectorResult]:
    paths = build_output_paths(out_dir)
    df = prepare_base_dataframe(csv_path=csv_path, config=config)

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
        _remove_stale_overlay_figures(paths=paths, figure_suffix=config.outputs.figures_format)

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
