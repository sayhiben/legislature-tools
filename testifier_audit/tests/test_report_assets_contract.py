from __future__ import annotations

from pathlib import Path

from testifier_audit.report.render import render_report


def test_render_report_copies_report_static_assets(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    report_path = render_report(results={}, artifacts={}, out_dir=out_dir)
    rendered = report_path.read_text(encoding="utf-8")

    css_asset = out_dir / "assets" / "report" / "report.css"
    js_asset = out_dir / "assets" / "report" / "main.js"

    assert css_asset.exists()
    assert js_asset.exists()
    assert css_asset.stat().st_size > 0
    assert js_asset.stat().st_size > 0
    assert 'href="assets/report/report.css?v=' in rendered
    assert 'type="module" src="assets/report/main.js?v=' in rendered


def test_report_js_uses_single_mount_flow(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    render_report(results={}, artifacts={}, out_dir=out_dir)

    js_text = (out_dir / "assets" / "report" / "main.js").read_text(encoding="utf-8")

    assert "mountAllSections()" in js_text
    assert "Loading report sections..." in js_text
    assert "initLazySectionMounting" not in js_text
    assert "preloadAllChartShardFiles" not in js_text
    assert "IntersectionObserver" not in js_text
