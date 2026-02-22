from __future__ import annotations

from pathlib import Path

from testifier_audit.report.analysis_registry import ANALYSES_TO_PERFORM
from testifier_audit.report.render import render_report


def _is_off_hours_only_view() -> bool:
    seen: set[str] = set()
    analysis_ids: list[str] = []
    for analysis_id in ANALYSES_TO_PERFORM:
        normalized = str(analysis_id or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        analysis_ids.append(normalized)
    return analysis_ids == ["off_hours"]


def test_report_template_includes_drilldown_and_export_runtime_hooks(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    report_path = render_report(results={}, artifacts={}, out_dir=out_dir)
    rendered = report_path.read_text(encoding="utf-8")
    js_text = (out_dir / "assets" / "report" / "main.js").read_text(encoding="utf-8")

    if _is_off_hours_only_view():
        assert 'id="download-selected-window-rows"' not in rendered
        assert 'id="download-top-evidence-windows"' not in rendered
        assert 'id="download-top-evidence-records"' not in rendered
        assert 'id="drilldown-causative-rows-host"' not in rendered
        assert 'id="drilldown-dup-names-host"' not in rendered
        assert 'id="drilldown-clusters-host"' not in rendered
        assert 'id="drilldown-runs-weirdness-host"' not in rendered
    else:
        assert 'id="download-selected-window-rows"' in rendered
        assert 'id="download-top-evidence-windows"' in rendered
        assert 'id="download-top-evidence-records"' in rendered
        assert 'id="drilldown-causative-rows-host"' in rendered
        assert 'id="drilldown-dup-names-host"' in rendered
        assert 'id="drilldown-clusters-host"' in rendered
        assert 'id="drilldown-runs-weirdness-host"' in rendered

    assert "function renderWindowDrilldown(" in js_text
    assert "function getWindowSpanRows(" in js_text
    assert "function downloadCsv(" in js_text
