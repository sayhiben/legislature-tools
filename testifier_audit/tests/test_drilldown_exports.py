from __future__ import annotations

from pathlib import Path

from testifier_audit.report.render import render_report

from ._report_js_assets import load_report_js_corpus


def test_report_template_includes_drilldown_and_export_runtime_hooks(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    report_path = render_report(results={}, artifacts={}, out_dir=out_dir)
    rendered = report_path.read_text(encoding="utf-8")
    js_text = load_report_js_corpus(out_dir)

    assert 'id="download-selected-window-rows"' not in rendered
    assert 'id="download-top-evidence-windows"' not in rendered
    assert 'id="download-top-evidence-records"' not in rendered
    assert 'id="drilldown-causative-rows-host"' not in rendered
    assert 'id="drilldown-dup-names-host"' not in rendered
    assert 'id="drilldown-clusters-host"' not in rendered
    assert 'id="drilldown-runs-weirdness-host"' not in rendered

    assert "function renderWindowDrilldown(" not in js_text
    assert "function getWindowSpanRows(" not in js_text
    assert "function downloadCsv(" not in js_text
