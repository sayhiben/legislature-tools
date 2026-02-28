from __future__ import annotations

import re
from pathlib import Path

from testifier_audit.report.render import render_report

from ._report_js_assets import list_report_js_assets, load_report_js_corpus


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

    js_text = load_report_js_corpus(out_dir)

    assert "mountAllSections()" in js_text
    assert "Loading report sections..." in js_text
    assert "initLazySectionMounting" not in js_text
    assert "preloadAllChartShardFiles" not in js_text
    assert "IntersectionObserver" not in js_text


def test_report_js_modules_are_copied_and_imports_resolve(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    render_report(results={}, artifacts={}, out_dir=out_dir)

    js_assets = list_report_js_assets(out_dir)
    asset_paths = {path.relative_to(out_dir / "assets" / "report").as_posix() for path in js_assets}
    main_js = out_dir / "assets" / "report" / "main.js"
    main_text = main_js.read_text(encoding="utf-8")
    imports = re.findall(r"""import\s+(?:[\s\S]*?\s+from\s+)?["']([^"']+)["']""", main_text)

    assert "modules/app.js" in asset_paths
    for import_path in imports:
        if not import_path.startswith("./"):
            continue
        normalized = import_path[2:]
        assert normalized in asset_paths


def test_report_js_boot_sequence_is_ordered(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    render_report(results={}, artifacts={}, out_dir=out_dir)

    app_js = out_dir / "assets" / "report" / "modules" / "app.js"
    js_text = app_js.read_text(encoding="utf-8")
    ordered_markers = [
        "applySidebarBillMeta();",
        "updateReportTimezoneSummary();",
        "await ensureHeaderDataLoaded();",
        "buildKpis();",
        "initThemeControl();",
        "initSidebarTooltips();",
        "await runWithBusyIndicator(\"Loading report sections...\", async () => {",
        "await mountAllSections();",
        "initializeLinkedZoomOnLoad();",
        "syncControlOverridesToUrl();",
        "updateCursorAcrossTimeCharts();",
    ]
    positions = [js_text.rfind(marker) for marker in ordered_markers]
    assert all(position >= 0 for position in positions)
    assert positions == sorted(positions)
