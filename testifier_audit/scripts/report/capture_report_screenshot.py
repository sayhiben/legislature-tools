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
from urllib.parse import urlparse

from PIL import Image

Image.MAX_IMAGE_PIXELS = None


@dataclass
class PageMetrics:
    scroll_height: int
    viewport_height: int
    viewport_width: int


def _run(
    args: list[str],
    *,
    cwd: Path,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(args, cwd=cwd, text=True, capture_output=True)
    if check and proc.returncode != 0:
        command = " ".join(args)
        raise RuntimeError(
            f"Command failed ({proc.returncode}): {command}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    return proc


def _run_session(session: str, command: list[str], *, cwd: Path) -> str:
    proc = _run(["playwright-cli", "--session", session, *command], cwd=cwd)
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


def _ensure_browser_installed(cwd: Path) -> None:
    _run(["playwright-cli", "install-browser"], cwd=cwd)


def _open_with_retry(session: str, url: str, *, cwd: Path) -> None:
    for attempt in (1, 2):
        proc = _run(
            ["playwright-cli", "--session", session, "open", url],
            cwd=cwd,
            check=False,
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
            _ensure_browser_installed(cwd)
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
) -> PageMetrics:
    js = (
        "async (page) => {"
        f"const waitMs = {wait_ms};"
        f"const settleMs = {settle_ms};"
        f"const openSidebar = {str(open_sidebar).lower()};"
        f"const expandDetails = {str(expand_details).lower()};"
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
    output = _run_session(session, ["run-code", js], cwd=cwd)
    data = _extract_json_result(output)
    return PageMetrics(
        scroll_height=int(data["scrollHeight"]),
        viewport_height=int(data["viewportHeight"]),
        viewport_width=int(data["viewportWidth"]),
    )


def _capture_tiles(
    session: str,
    *,
    cwd: Path,
    tiles_dir: Path,
    metrics: PageMetrics,
    settle_ms: int,
) -> list[tuple[int, Path]]:
    tiles_dir.mkdir(parents=True, exist_ok=True)
    max_y = max(0, metrics.scroll_height - metrics.viewport_height)
    positions = list(range(0, max_y + 1, metrics.viewport_height))
    if positions[-1] != max_y:
        positions.append(max_y)

    files: list[tuple[int, Path]] = []
    for index, y in enumerate(positions):
        scroll_js = (
            "async (page) => {"
            f"const y = {y};"
            f"const settleMs = {settle_ms};"
            "await page.evaluate((yy) => window.scrollTo(0, yy), y);"
            "await page.waitForTimeout(settleMs);"
            "return await page.evaluate(() => window.scrollY);"
            "}"
        )
        _run_session(session, ["run-code", scroll_js], cwd=cwd)

        tile_path = tiles_dir / f"tile-{index:04d}-y{y}.png"
        _run_session(
            session,
            ["screenshot", "--filename", str(tile_path)],
            cwd=cwd,
        )
        files.append((y, tile_path))

    return files


def _stitch_tiles(tile_data: list[tuple[int, Path]], output_path: Path, scroll_height: int) -> None:
    if not tile_data:
        raise RuntimeError("No tiles captured.")

    with Image.open(tile_data[0][1]) as first_tile:
        tile_width, tile_height = first_tile.size

    stitched = Image.new("RGB", (tile_width, scroll_height), color=(255, 255, 255))

    for y_offset, tile_path in tile_data:
        with Image.open(tile_path) as tile:
            crop_height = min(tile_height, scroll_height - y_offset)
            if crop_height <= 0:
                break
            tile_crop = tile.crop((0, 0, tile_width, crop_height))
            stitched.paste(tile_crop, (0, y_offset))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    stitched.save(output_path)


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


def parse_args() -> argparse.Namespace:
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
        "--no-expand-details",
        action="store_true",
        help="Do not auto-expand all <details> blocks",
    )
    parser.add_argument(
        "--keep-tiles",
        action="store_true",
        help="Keep temporary tile images instead of deleting them",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    cwd = Path("/Users/sayhiben/dev/legislature-tools")
    output_path = Path(args.output).expanduser().resolve()
    tiles_root = Path(args.tiles_dir).expanduser().resolve() / output_path.stem

    target_url, server_proc = _resolve_target(args.target)
    _cleanup_session(args.session, cwd=cwd)

    try:
        _open_with_retry(args.session, target_url, cwd=cwd)
        _run_session(
            args.session,
            ["resize", str(args.width), str(args.height)],
            cwd=cwd,
        )
        metrics = _prepare_page(
            args.session,
            cwd=cwd,
            wait_ms=args.wait_ms,
            settle_ms=args.settle_ms,
            open_sidebar=not args.no_open_sidebar,
            expand_details=not args.no_expand_details,
        )
        tile_data = _capture_tiles(
            args.session,
            cwd=cwd,
            tiles_dir=tiles_root,
            metrics=metrics,
            settle_ms=args.settle_ms,
        )
    finally:
        _cleanup_session(args.session, cwd=cwd)
        if server_proc is not None:
            server_proc.terminate()
            try:
                server_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server_proc.kill()

    _stitch_tiles(tile_data, output_path, metrics.scroll_height)

    metadata = {
        "target": args.target,
        "url": target_url,
        "output": str(output_path),
        "scroll_height": metrics.scroll_height,
        "viewport_width": metrics.viewport_width,
        "viewport_height": metrics.viewport_height,
        "tiles": len(tile_data),
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
