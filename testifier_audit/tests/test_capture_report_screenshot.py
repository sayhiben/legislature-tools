from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "report"
    / "capture_report_screenshot.py"
)


def _load_script_module():
    spec = importlib.util.spec_from_file_location("capture_report_screenshot", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_defaults_hide_fixed_chrome() -> None:
    module = _load_script_module()
    args = module.parse_args(["https://example.com/report.html", "/tmp/report.png"])

    assert args.target == "https://example.com/report.html"
    assert args.output == "/tmp/report.png"
    assert args.keep_fixed_chrome is False
    assert args.no_open_sidebar is False
    assert args.no_expand_details is False
    assert args.command_timeout_sec == 90.0
    assert args.max_tiles == 0


def test_parse_args_accepts_capture_behavior_flags() -> None:
    module = _load_script_module()
    args = module.parse_args(
        [
            "https://example.com/report.html",
            "/tmp/report.png",
            "--keep-fixed-chrome",
            "--no-open-sidebar",
            "--no-expand-details",
            "--keep-tiles",
            "--width",
            "1440",
            "--height",
            "900",
            "--settle-ms",
            "600",
            "--command-timeout-sec",
            "45",
            "--max-tiles",
            "20",
        ]
    )

    assert args.keep_fixed_chrome is True
    assert args.no_open_sidebar is True
    assert args.no_expand_details is True
    assert args.keep_tiles is True
    assert args.width == 1440
    assert args.height == 900
    assert args.settle_ms == 600
    assert args.command_timeout_sec == 45
    assert args.max_tiles == 20


def test_planned_scroll_positions_include_trailing_max_offset() -> None:
    module = _load_script_module()
    positions = module._planned_scroll_positions(scroll_height=3000, viewport_height=1400)

    assert positions == [0, 1400, 1600]


def test_planned_scroll_positions_rejects_non_positive_viewport_height() -> None:
    module = _load_script_module()
    with pytest.raises(ValueError):
        module._planned_scroll_positions(scroll_height=3000, viewport_height=0)
