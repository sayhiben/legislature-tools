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


def test_report_layout_contains_phase2_investigation_sections(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    report_path = render_report(results={}, artifacts={}, out_dir=out_dir)
    rendered = report_path.read_text(encoding="utf-8")

    if _is_off_hours_only_view():
        assert "Triage" not in rendered
        assert "Window Drilldown" not in rendered
        assert "Methodology" not in rendered
        assert 'href="#triage"' not in rendered
        assert 'href="#window-drilldown"' not in rendered
        assert 'href="#name-cluster-forensics"' not in rendered
        assert 'href="#methodology"' not in rendered
        assert 'id="section-triage"' not in rendered
        assert 'id="section-window-drilldown"' not in rendered
        assert 'id="section-name-cluster-forensics"' not in rendered
        assert 'id="section-methodology"' not in rendered
    else:
        assert "Triage" in rendered
        assert "Window Drilldown" not in rendered
        assert "Methodology" in rendered
        assert 'href="#triage"' not in rendered
        assert 'href="#window-drilldown"' not in rendered
        assert 'href="#name-cluster-forensics"' not in rendered
        assert 'href="#methodology"' not in rendered
        assert 'id="section-triage"' not in rendered
        assert "<h3>Triage</h3>" in rendered
        assert 'id="section-window-drilldown"' not in rendered
        assert 'id="section-name-cluster-forensics"' not in rendered
        assert 'id="section-methodology"' in rendered
