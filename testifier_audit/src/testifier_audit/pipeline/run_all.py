from __future__ import annotations

from pathlib import Path
from time import perf_counter
from typing import Any

from testifier_audit.config import AppConfig
from testifier_audit.io.hearing_metadata import load_hearing_metadata
from testifier_audit.pipeline.pass1_profile import build_profile_artifacts, prepare_base_dataframe
from testifier_audit.pipeline.pass2_deep_dive import run_detectors
from testifier_audit.report.render import render_report


def run_all(
    csv_path: Path | None,
    out_dir: Path,
    config: AppConfig,
    *,
    dedup_mode: str | None = None,
) -> Path:
    hearing_metadata = load_hearing_metadata(config.input.hearing_metadata_path)
    prepare_started = perf_counter()
    base_df = prepare_base_dataframe(csv_path=csv_path, config=config)
    prepare_ms = round((perf_counter() - prepare_started) * 1000.0, 3)
    profile_started = perf_counter()
    artifacts = build_profile_artifacts(
        csv_path=csv_path,
        out_dir=out_dir,
        config=config,
        base_df=base_df,
    )
    profile_ms = round((perf_counter() - profile_started) * 1000.0, 3)
    detector_runtime: dict[str, Any] = {}
    detect_started = perf_counter()
    results = run_detectors(
        csv_path=csv_path,
        artifacts=artifacts,
        out_dir=out_dir,
        config=config,
        base_df=base_df,
        runtime_profile_out=detector_runtime,
    )
    detect_ms = round((perf_counter() - detect_started) * 1000.0, 3)
    return render_report(
        results=results,
        artifacts=artifacts,
        out_dir=out_dir,
        default_dedup_mode=dedup_mode or config.report.default_dedup_mode,
        min_cell_n_for_rates=int(config.report.min_cell_n_for_rates),
        hearing_metadata=hearing_metadata,
        additional_runtime_metrics={
            "pipeline": {
                "prepare_base_dataframe_ms": prepare_ms,
                "profile_artifacts_ms": profile_ms,
                "detector_pass_ms": detect_ms,
            },
            "detectors": detector_runtime,
        },
    )
