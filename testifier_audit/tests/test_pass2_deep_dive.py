from __future__ import annotations

from pathlib import Path

import pandas as pd

from testifier_audit.config import load_config
from testifier_audit.detectors.base import DetectorResult
from testifier_audit.pipeline.pass2_deep_dive import run_detectors


class _FakeDetector:
    def __init__(self, name: str, run_log: list[str]) -> None:
        self.name = name
        self._run_log = run_log

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        self._run_log.append(self.name)
        _ = (df, features)
        return DetectorResult(
            detector=self.name,
            summary={"active": True, "enabled": True},
            tables={"example": pd.DataFrame({"value": [1]})},
        )


def test_run_detectors_scopes_execution_and_prunes_stale_outputs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cfg_path = Path(__file__).resolve().parents[1] / "configs/default.yaml"
    cfg = load_config(cfg_path)
    cfg.outputs.tables_format = "csv"

    out_dir = tmp_path / "out"
    (out_dir / "summary").mkdir(parents=True)
    (out_dir / "tables").mkdir(parents=True)
    (out_dir / "flags").mkdir(parents=True)
    (out_dir / "figures").mkdir(parents=True)

    stale_summary = out_dir / "summary" / "bursts.json"
    stale_table = out_dir / "tables" / "bursts__example.csv"
    stale_flag = out_dir / "flags" / "bursts__record_flags.csv"
    stale_figure = out_dir / "figures" / "counts_with_anomalies.png"
    stale_summary.write_text('{"old":true}', encoding="utf-8")
    stale_table.write_text("value\n9\n", encoding="utf-8")
    stale_flag.write_text("flag\n1\n", encoding="utf-8")
    stale_figure.write_bytes(b"png")

    run_log: list[str] = []
    fake_detectors = [_FakeDetector("off_hours", run_log), _FakeDetector("bursts", run_log)]

    monkeypatch.setattr(
        "testifier_audit.pipeline.pass2_deep_dive.registry_configured_analysis_ids",
        lambda: ["off_hours"],
    )
    monkeypatch.setattr(
        "testifier_audit.pipeline.pass2_deep_dive.registry_configured_detector_names",
        lambda: {"off_hours"},
    )
    monkeypatch.setattr(
        "testifier_audit.pipeline.pass2_deep_dive.default_detectors",
        lambda _cfg: list(fake_detectors),
    )
    monkeypatch.setattr(
        "testifier_audit.pipeline.pass2_deep_dive.prepare_base_dataframe",
        lambda csv_path, config: pd.DataFrame({"minute_bucket": pd.to_datetime([])}),
    )
    render_called: list[bool] = []
    monkeypatch.setattr(
        "testifier_audit.pipeline.pass2_deep_dive._render_detector_figures",
        lambda feature_context, out_dir, config: render_called.append(True),
    )

    results = run_detectors(csv_path=None, artifacts={}, out_dir=out_dir, config=cfg)

    assert set(results.keys()) == {"off_hours"}
    assert run_log == ["off_hours"]
    assert not render_called

    assert (out_dir / "summary" / "off_hours.json").exists()
    assert (out_dir / "tables" / "off_hours__example.csv").exists()

    assert not stale_summary.exists()
    assert not stale_table.exists()
    assert not stale_flag.exists()
    assert not stale_figure.exists()
