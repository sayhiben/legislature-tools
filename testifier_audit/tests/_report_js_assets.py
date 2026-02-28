from __future__ import annotations

from pathlib import Path


def list_report_js_assets(out_dir: Path) -> list[Path]:
    assets_root = out_dir / "assets" / "report"
    if not assets_root.exists():
        return []
    return sorted(path for path in assets_root.rglob("*.js") if path.is_file())


def load_report_js_corpus(out_dir: Path) -> str:
    parts: list[str] = []
    for path in list_report_js_assets(out_dir):
        parts.append(path.read_text(encoding="utf-8"))
    return "\n\n".join(parts)
