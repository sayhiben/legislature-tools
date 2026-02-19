from __future__ import annotations

from pathlib import Path

from testifier_audit.config import AppConfig
from testifier_audit.pipeline.pass1_profile import build_profile_artifacts
from testifier_audit.pipeline.pass2_deep_dive import run_detectors
from testifier_audit.report.render import render_report


def run_all(csv_path: Path, out_dir: Path, config: AppConfig) -> Path:
    artifacts = build_profile_artifacts(csv_path=csv_path, out_dir=out_dir, config=config)
    results = run_detectors(csv_path=csv_path, artifacts=artifacts, out_dir=out_dir, config=config)
    return render_report(results=results, artifacts=artifacts, out_dir=out_dir)
