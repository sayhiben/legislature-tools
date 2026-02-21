from __future__ import annotations

import json
from pathlib import Path

from testifier_audit.report.render import _build_interactive_chart_payload_v2, render_report


def test_payload_runtime_metrics_include_size_and_build_time() -> None:
    payload = _build_interactive_chart_payload_v2(table_map={}, detector_summaries={})
    runtime = payload["controls"]["runtime"]

    assert "payload_build_ms" in runtime
    assert "payload_json_bytes" in runtime
    assert runtime["payload_build_ms"] >= 0.0
    assert runtime["payload_json_bytes"] > 0


def test_render_report_writes_runtime_artifact(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    report_path = render_report(results={}, artifacts={}, out_dir=out_dir)

    assert report_path.exists()
    runtime_path = out_dir / "artifacts" / "report_runtime.json"
    assert runtime_path.exists()

    runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
    assert runtime["report_html_bytes"] > 0
    assert runtime["template_render_ms"] >= 0.0
    assert runtime["report_write_ms"] >= 0.0
    assert runtime["report_total_ms"] >= 0.0
    assert runtime["interactive_payload_build_ms"] >= 0.0
