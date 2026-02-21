from __future__ import annotations

from pathlib import Path

from testifier_audit.config import AppConfig
from testifier_audit.io.hearing_metadata import load_hearing_metadata
from testifier_audit.pipeline.pass1_profile import build_profile_artifacts
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
    artifacts = build_profile_artifacts(csv_path=csv_path, out_dir=out_dir, config=config)
    results = run_detectors(csv_path=csv_path, artifacts=artifacts, out_dir=out_dir, config=config)
    return render_report(
        results=results,
        artifacts=artifacts,
        out_dir=out_dir,
        default_dedup_mode=dedup_mode or config.report.default_dedup_mode,
        min_cell_n_for_rates=int(config.report.min_cell_n_for_rates),
        hearing_metadata=hearing_metadata,
    )
