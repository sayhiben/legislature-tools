from __future__ import annotations

from pathlib import Path

from testifier_audit.report.render import render_report


def test_report_template_includes_drilldown_and_export_runtime_hooks(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    report_path = render_report(results={}, artifacts={}, out_dir=out_dir)
    rendered = report_path.read_text(encoding="utf-8")

    assert 'id="download-selected-window-rows"' in rendered
    assert 'id="download-top-evidence-windows"' in rendered
    assert 'id="download-top-evidence-records"' in rendered
    assert 'id="drilldown-causative-rows-host"' in rendered
    assert 'id="drilldown-dup-names-host"' in rendered
    assert 'id="drilldown-clusters-host"' in rendered
    assert 'id="drilldown-runs-weirdness-host"' in rendered

    assert "function renderWindowDrilldown(" in rendered
    assert "function getWindowSpanRows(" in rendered
    assert "function downloadCsv(" in rendered
