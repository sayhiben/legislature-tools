#!/usr/bin/env python3
"""Capture very tall report pages without full-page screenshot tiling artifacts.

This script drives `playwright-cli` in a named session, scroll-captures viewport tiles,
and stitches them into a single image. It avoids Chromium full-page capture limits
that can repeat content every ~16,384px on very tall pages.
"""

from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from PIL import Image

Image.MAX_IMAGE_PIXELS = None


@dataclass
class PageMetrics:
    scroll_height: int
    viewport_height: int
    viewport_width: int


@dataclass
class StitchMetrics:
    stitched_height: int
    tile_height: int
    max_covered_y: int


def _run(
    args: list[str],
    *,
    cwd: Path,
    check: bool = True,
    timeout_seconds: float | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        proc = subprocess.run(
            args,
            cwd=cwd,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        command = " ".join(args)
        timeout_text = (
            f"{timeout_seconds:.1f}s" if isinstance(timeout_seconds, (float, int)) else "timeout"
        )
        raise RuntimeError(
            f"Command timed out after {timeout_text}: {command}\n"
            f"stdout:\n{exc.stdout or ''}\n"
            f"stderr:\n{exc.stderr or ''}"
        ) from exc
    if check and proc.returncode != 0:
        command = " ".join(args)
        raise RuntimeError(
            f"Command failed ({proc.returncode}): {command}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    return proc


def _run_session(
    session: str,
    command: list[str],
    *,
    cwd: Path,
    timeout_seconds: float | None = None,
) -> str:
    proc = _run(
        ["playwright-cli", "--session", session, *command],
        cwd=cwd,
        timeout_seconds=timeout_seconds,
    )
    return proc.stdout


def _extract_json_result(output: str) -> dict[str, object]:
    lines = output.splitlines()
    for index, line in enumerate(lines):
        if line.strip() == "### Result":
            for candidate in lines[index + 1 :]:
                text = candidate.strip()
                if not text:
                    continue
                if text.startswith("### "):
                    break
                return json.loads(text)
    raise RuntimeError(f"Could not parse JSON result from output:\n{output}")


def _ensure_browser_installed(cwd: Path, *, timeout_seconds: float) -> None:
    _run(
        ["playwright-cli", "install-browser"],
        cwd=cwd,
        timeout_seconds=timeout_seconds,
    )


def _open_with_retry(session: str, url: str, *, cwd: Path, timeout_seconds: float) -> None:
    for attempt in (1, 2):
        proc = _run(
            ["playwright-cli", "--session", session, "open", url],
            cwd=cwd,
            check=False,
            timeout_seconds=timeout_seconds,
        )
        if proc.returncode == 0:
            return
        output = f"{proc.stdout}\n{proc.stderr}"
        needs_install = (
            "install-browser" in output
            or "Executable doesn't exist" in output
            or "Failed to launch" in output
            or "browserType.launch" in output
        )
        if attempt == 1 and needs_install:
            _ensure_browser_installed(cwd, timeout_seconds=timeout_seconds)
            continue
        raise RuntimeError(
            f"Failed to open browser session.\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )


def _prepare_page(
    session: str,
    *,
    cwd: Path,
    wait_ms: int,
    settle_ms: int,
    open_sidebar: bool,
    expand_details: bool,
    hide_fixed_chrome: bool,
    command_timeout_sec: float,
) -> PageMetrics:
    js = (
        "async (page) => {"
        f"const waitMs = {wait_ms};"
        f"const settleMs = {settle_ms};"
        f"const openSidebar = {str(open_sidebar).lower()};"
        f"const expandDetails = {str(expand_details).lower()};"
        f"const hideFixedChrome = {str(hide_fixed_chrome).lower()};"
        "await page.waitForLoadState('domcontentloaded');"
        "try { await page.waitForLoadState('networkidle', { timeout: 30000 }); } catch (_) {}"
        "await page.waitForTimeout(waitMs);"
        "await page.evaluate(() => {"
        "  document.documentElement.style.scrollBehavior = 'auto';"
        "  document.body.style.scrollBehavior = 'auto';"
        "});"
        "if (openSidebar) {"
        "  await page.evaluate(() => {"
        "    const shell = document.querySelector('.page-shell');"
        "    shell?.classList.add('sidebar-open');"
        "    const toggle = document.getElementById('sidebar-toggle');"
        "    if (toggle) {"
        "      toggle.setAttribute('aria-expanded', 'true');"
        "      toggle.textContent = 'Hide Menu';"
        "    }"
        "  });"
        "}"
        "if (expandDetails) {"
        "  await page.evaluate(() => {"
        "    document.querySelectorAll('details').forEach((node) => { node.open = true; });"
        "  });"
        "}"
        "if (hideFixedChrome) {"
        "  await page.evaluate(() => {"
        "    const styleId = 'capture-hide-fixed-chrome';"
        "    let styleTag = document.getElementById(styleId);"
        "    if (!styleTag) {"
        "      styleTag = document.createElement('style');"
        "      styleTag.id = styleId;"
        "      styleTag.textContent = ["
        "        '.sidebar-toggle, .toc-sidebar, .sidebar-backdrop, #report-busy-indicator {'"
        "        + ' visibility: hidden !important; }',"
        "        '.page-shell.sidebar-open .report-main { margin-left: 0 !important; }'"
        "      ].join(' ');"
        "      document.head.appendChild(styleTag);"
        "    }"
        "  });"
        "}"
        "await page.waitForTimeout(settleMs);"
        "await page.evaluate(() => window.scrollTo(0, 0));"
        "const dims = await page.evaluate(() => ({"
        "  scrollHeight: Math.max("
        "document.body.scrollHeight, document.documentElement.scrollHeight"
        "),"
        "  viewportHeight: window.innerHeight,"
        "  viewportWidth: window.innerWidth"
        "}));"
        "return dims;"
        "}"
    )
    output = _run_session(
        session,
        ["run-code", js],
        cwd=cwd,
        timeout_seconds=command_timeout_sec,
    )
    data = _extract_json_result(output)
    return PageMetrics(
        scroll_height=int(data["scrollHeight"]),
        viewport_height=int(data["viewportHeight"]),
        viewport_width=int(data["viewportWidth"]),
    )


def _planned_scroll_positions(*, scroll_height: int, viewport_height: int) -> list[int]:
    if viewport_height <= 0:
        raise ValueError("viewport_height must be positive")
    max_y = max(0, scroll_height - viewport_height)
    positions = list(range(0, max_y + 1, viewport_height))
    if positions[-1] != max_y:
        positions.append(max_y)
    return positions


def _capture_tiles(
    session: str,
    *,
    cwd: Path,
    tiles_dir: Path,
    metrics: PageMetrics,
    settle_ms: int,
    keep_fixed_chrome: bool,
    command_timeout_sec: float,
    max_tiles: int | None,
) -> tuple[list[tuple[int, Path]], dict[str, Any]]:
    tiles_dir.mkdir(parents=True, exist_ok=True)
    positions = _planned_scroll_positions(
        scroll_height=metrics.scroll_height,
        viewport_height=metrics.viewport_height,
    )

    files: list[tuple[int, Path]] = []
    seen_actual_positions: set[int] = set()
    actual_positions: list[int] = []
    duplicate_scroll_positions_skipped = 0
    warnings: list[str] = []

    for index, y in enumerate(positions):
        if max_tiles is not None and max_tiles > 0 and len(files) >= max_tiles:
            warnings.append(
                f"Capture truncated after {max_tiles} tiles by --max-tiles limit."
            )
            break

        print(
            f"[capture] tile {index + 1}/{len(positions)} requested_y={y}",
            flush=True,
        )
        scroll_js = (
            "async (page) => {"
            f"const y = {y};"
            f"const settleMs = {settle_ms};"
            "await page.evaluate((yy) => window.scrollTo(0, yy), y);"
            "await page.waitForTimeout(settleMs);"
            "const scrollY = await page.evaluate(() => window.scrollY);"
            "return { scrollY };"
            "}"
        )
        scroll_output = _run_session(
            session,
            ["run-code", scroll_js],
            cwd=cwd,
            timeout_seconds=command_timeout_sec,
        )
        scroll_result = _extract_json_result(scroll_output)
        try:
            actual_y = int(float(scroll_result.get("scrollY", y)))
        except (TypeError, ValueError):
            actual_y = y
        actual_positions.append(actual_y)

        if actual_y in seen_actual_positions:
            duplicate_scroll_positions_skipped += 1
            continue
        seen_actual_positions.add(actual_y)

        tile_path = tiles_dir / f"tile-{index:04d}-y{actual_y}.png"
        _run_session(
            session,
            ["screenshot", "--filename", str(tile_path)],
            cwd=cwd,
            timeout_seconds=command_timeout_sec,
        )
        files.append((actual_y, tile_path))

    files.sort(key=lambda item: item[0])

    if duplicate_scroll_positions_skipped:
        warnings.append(
            "Skipped duplicate tile positions caused by repeated scroll offsets; this avoids "
            "false repeated-content interpretation in stitched output."
        )
    if keep_fixed_chrome and len(files) > 1:
        warnings.append(
            "Fixed UI chrome was kept. Repeated sidebar/menu visuals can be stitching artifacts "
            "and do not imply duplicated report content."
        )

    diagnostics = {
        "requested_positions": positions,
        "actual_positions": sorted(seen_actual_positions),
        "duplicate_scroll_positions_skipped": duplicate_scroll_positions_skipped,
        "warnings": warnings,
    }
    return files, diagnostics


def _stitch_tiles(
    tile_data: list[tuple[int, Path]],
    output_path: Path,
    scroll_height: int,
) -> StitchMetrics:
    if not tile_data:
        raise RuntimeError("No tiles captured.")

    with Image.open(tile_data[0][1]) as first_tile:
        tile_width, tile_height = first_tile.size

    max_covered_y = min(
        scroll_height,
        max((y_offset + tile_height) for y_offset, _ in tile_data),
    )
    stitched_height = max(1, max_covered_y)
    stitched = Image.new("RGB", (tile_width, stitched_height), color=(255, 255, 255))

    for y_offset, tile_path in tile_data:
        with Image.open(tile_path) as tile:
            crop_height = min(tile_height, stitched_height - y_offset)
            if crop_height <= 0:
                break
            tile_crop = tile.crop((0, 0, tile_width, crop_height))
            stitched.paste(tile_crop, (0, y_offset))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    stitched.save(output_path)
    return StitchMetrics(
        stitched_height=stitched_height,
        tile_height=tile_height,
        max_covered_y=max_covered_y,
    )


def _cleanup_session(session: str, *, cwd: Path) -> None:
    _run(["playwright-cli", "--session", session, "close"], cwd=cwd, check=False)


def _wait_for_port(host: str, port: int, timeout_seconds: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.settimeout(0.2)
            if client.connect_ex((host, port)) == 0:
                return
        time.sleep(0.1)
    raise RuntimeError(f"Timed out waiting for {host}:{port} to start listening.")


def _pick_available_port(host: str = "127.0.0.1") -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((host, 0))
        server.listen(1)
        return int(server.getsockname()[1])


def _resolve_target(target: str) -> tuple[str, subprocess.Popen[str] | None]:
    parsed = urlparse(target)
    if parsed.scheme in {"http", "https"}:
        return target, None

    path = Path(target).expanduser().resolve()
    if path.is_dir():
        report_dir = path
        report_name = "report.html"
    else:
        report_dir = path.parent
        report_name = path.name

    if not (report_dir / report_name).exists():
        raise FileNotFoundError(f"Could not find report file: {report_dir / report_name}")

    port = _pick_available_port()
    server_proc = subprocess.Popen(
        [sys.executable, "-m", "http.server", str(port), "--bind", "127.0.0.1"],
        cwd=report_dir,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    _wait_for_port("127.0.0.1", port)
    return f"http://127.0.0.1:{port}/{report_name}", server_proc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Capture a very tall report page via viewport tiling and stitch into one PNG.")
    )
    parser.add_argument(
        "target",
        help=(
            "Report URL or local report path. "
            "For local paths, a temporary localhost server is started automatically."
        ),
    )
    parser.add_argument("output", help="Path to final stitched PNG")
    parser.add_argument(
        "--session",
        default=f"rc{int(time.time()) % 100000}",
        help="Playwright CLI session name",
    )
    parser.add_argument("--width", type=int, default=1920, help="Viewport width")
    parser.add_argument("--height", type=int, default=1400, help="Viewport height")
    parser.add_argument(
        "--wait-ms",
        type=int,
        default=5000,
        help="Initial wait after load before measurements",
    )
    parser.add_argument(
        "--settle-ms",
        type=int,
        default=300,
        help="Wait after each scroll before capture",
    )
    parser.add_argument(
        "--command-timeout-sec",
        type=float,
        default=90.0,
        help="Timeout in seconds for each playwright-cli command call",
    )
    parser.add_argument(
        "--max-tiles",
        type=int,
        default=0,
        help="Optional hard cap on captured tiles (0 means no cap)",
    )
    parser.add_argument(
        "--tiles-dir",
        default="/Users/sayhiben/dev/legislature-tools/output/playwright/tiles",
        help="Directory for temporary tile images",
    )
    parser.add_argument(
        "--no-open-sidebar",
        action="store_true",
        help="Do not force sidebar open before capture",
    )
    parser.add_argument(
        "--keep-fixed-chrome",
        action="store_true",
        help=(
            "Keep fixed page chrome (for example sidebar/menu buttons). "
            "By default fixed chrome is hidden to reduce stitched-image artifact confusion."
        ),
    )
    parser.add_argument(
        "--no-expand-details",
        action="store_true",
        help="Do not auto-expand all <details> blocks",
    )
    parser.add_argument(
        "--keep-tiles",
        action="store_true",
        help="Keep temporary tile images instead of deleting them",
    )
    return parser.parse_args(argv)


def main() -> int:
    args = parse_args()

    cwd = Path("/Users/sayhiben/dev/legislature-tools")
    output_path = Path(args.output).expanduser().resolve()
    tiles_root = Path(args.tiles_dir).expanduser().resolve() / output_path.stem
    command_timeout_sec = max(1.0, float(args.command_timeout_sec))
    max_tiles = int(args.max_tiles) if int(args.max_tiles) > 0 else None

    target_url, server_proc = _resolve_target(args.target)
    _cleanup_session(args.session, cwd=cwd)

    try:
        _open_with_retry(
            args.session,
            target_url,
            cwd=cwd,
            timeout_seconds=command_timeout_sec,
        )
        _run_session(
            args.session,
            ["resize", str(args.width), str(args.height)],
            cwd=cwd,
            timeout_seconds=command_timeout_sec,
        )
        metrics = _prepare_page(
            args.session,
            cwd=cwd,
            wait_ms=args.wait_ms,
            settle_ms=args.settle_ms,
            open_sidebar=not args.no_open_sidebar,
            expand_details=not args.no_expand_details,
            hide_fixed_chrome=not args.keep_fixed_chrome,
            command_timeout_sec=command_timeout_sec,
        )
        tile_data, capture_diagnostics = _capture_tiles(
            args.session,
            cwd=cwd,
            tiles_dir=tiles_root,
            metrics=metrics,
            settle_ms=args.settle_ms,
            keep_fixed_chrome=args.keep_fixed_chrome,
            command_timeout_sec=command_timeout_sec,
            max_tiles=max_tiles,
        )
    finally:
        _cleanup_session(args.session, cwd=cwd)
        if server_proc is not None:
            server_proc.terminate()
            try:
                server_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server_proc.kill()

    stitch_metrics = _stitch_tiles(tile_data, output_path, metrics.scroll_height)

    warnings = list(capture_diagnostics.get("warnings", []))
    if stitch_metrics.stitched_height < metrics.scroll_height:
        warnings.append(
            "Captured height is shorter than page scroll height; the page likely changed size or "
            "hit a scroll clamp while tiling."
        )

    metadata = {
        "target": args.target,
        "url": target_url,
        "output": str(output_path),
        "command_timeout_sec": command_timeout_sec,
        "max_tiles": max_tiles,
        "truncated_by_max_tiles": (
            max_tiles is not None
            and len(tile_data) >= max_tiles
            and len(capture_diagnostics.get("requested_positions", [])) > len(tile_data)
        ),
        "scroll_height": metrics.scroll_height,
        "stitched_height": stitch_metrics.stitched_height,
        "viewport_width": metrics.viewport_width,
        "viewport_height": metrics.viewport_height,
        "tiles": len(tile_data),
        "requested_tiles": len(capture_diagnostics.get("requested_positions", [])),
        "actual_scroll_positions": capture_diagnostics.get("actual_positions", []),
        "duplicate_scroll_positions_skipped": capture_diagnostics.get(
            "duplicate_scroll_positions_skipped",
            0,
        ),
        "fixed_chrome_hidden": not args.keep_fixed_chrome,
        "warnings": warnings,
        "tiles_dir": str(tiles_root),
    }

    metadata_path = output_path.with_suffix(".json")
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    if not args.keep_tiles:
        for _, tile_path in tile_data:
            tile_path.unlink(missing_ok=True)
        try:
            tiles_root.rmdir()
        except OSError:
            pass

    print(json.dumps(metadata, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
