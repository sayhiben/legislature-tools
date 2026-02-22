from __future__ import annotations

from pathlib import Path

import pandas as pd

from testifier_audit.config import load_config
from testifier_audit.pipeline.pass1_profile import build_profile_artifacts


def test_build_profile_artifacts_skips_profile_figures_when_baseline_analysis_not_enabled(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cfg_path = Path(__file__).resolve().parents[1] / "configs/default.yaml"
    cfg = load_config(cfg_path)
    cfg.outputs.tables_format = "csv"
    cfg.outputs.figures_format = "png"

    out_dir = tmp_path / "out"
    (out_dir / "figures").mkdir(parents=True)
    stale_figure = out_dir / "figures" / "counts_per_minute.png"
    stale_figure.write_bytes(b"png")

    base_df = pd.DataFrame({"id": [1], "minute_bucket": pd.to_datetime(["2026-02-01T00:00:00Z"])})
    monkeypatch.setattr(
        "testifier_audit.pipeline.pass1_profile.registry_configured_analysis_ids",
        lambda: ["off_hours"],
    )
    monkeypatch.setattr(
        "testifier_audit.pipeline.pass1_profile.prepare_base_dataframe",
        lambda csv_path, config: base_df.copy(),
    )
    monkeypatch.setattr(
        "testifier_audit.pipeline.pass1_profile.build_counts_per_minute",
        lambda df: pd.DataFrame({"minute_bucket": pd.to_datetime(["2026-02-01T00:00:00Z"]), "n_total": [1]}),
    )
    monkeypatch.setattr(
        "testifier_audit.pipeline.pass1_profile.build_counts_per_hour",
        lambda df: pd.DataFrame({"hour": [0], "n_total": [1]}),
    )
    monkeypatch.setattr(
        "testifier_audit.pipeline.pass1_profile.build_name_frequency",
        lambda df: pd.DataFrame({"canonical_name": ["DOE|JANE"], "n": [1]}),
    )
    monkeypatch.setattr(
        "testifier_audit.pipeline.pass1_profile.build_name_text_features",
        lambda df: pd.DataFrame({"canonical_name": ["DOE|JANE"], "name_length": [8]}),
    )
    monkeypatch.setattr(
        "testifier_audit.pipeline.pass1_profile.build_basic_quality",
        lambda df: pd.DataFrame({"metric": ["rows"], "value": [1]}),
    )

    render_calls: list[bool] = []
    monkeypatch.setattr(
        "testifier_audit.pipeline.pass1_profile._render_profile_figures",
        lambda artifacts, out_dir, config: render_calls.append(True),
    )

    artifacts = build_profile_artifacts(csv_path=None, out_dir=out_dir, config=cfg)

    assert artifacts
    assert not render_calls
    assert not stale_figure.exists()
