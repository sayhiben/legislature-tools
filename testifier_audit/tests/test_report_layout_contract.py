from __future__ import annotations

from pathlib import Path

from testifier_audit.report.render import render_report


def test_report_layout_contains_phase2_investigation_sections(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    report_path = render_report(results={}, artifacts={}, out_dir=out_dir)
    rendered = report_path.read_text(encoding="utf-8")

    assert "Triage" in rendered
    assert "Window Drilldown" in rendered
    assert "Name/Cluster Forensics" in rendered
    assert "Methodology" in rendered

    assert 'href="#triage"' in rendered
    assert 'href="#window-drilldown"' in rendered
    assert 'href="#name-cluster-forensics"' in rendered
    assert 'href="#methodology"' in rendered

    assert 'id="section-triage"' in rendered
    assert 'id="section-window-drilldown"' in rendered
    assert 'id="section-name-cluster-forensics"' in rendered
    assert 'id="section-methodology"' in rendered
