from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "methodology"


def fixture_path(*parts: str) -> Path:
    if not parts:
        return FIXTURE_ROOT
    return FIXTURE_ROOT.joinpath(*parts)


def load_fixture_json(*parts: str) -> dict[str, Any]:
    path = fixture_path(*parts)
    return json.loads(path.read_text(encoding="utf-8"))


def load_fixture_csv(*parts: str, parse_dates: list[str] | None = None) -> pd.DataFrame:
    path = fixture_path(*parts)
    parse_dates = parse_dates or None
    return pd.read_csv(path, parse_dates=parse_dates)


def list_cases(manifest: dict[str, Any], *, include_extended: bool) -> list[dict[str, Any]]:
    cases = manifest.get("cases", [])
    out: list[dict[str, Any]] = []
    for raw in cases:
        if not isinstance(raw, dict):
            continue
        tier = str(raw.get("tier", "default") or "default").strip().lower()
        if tier != "default" and not include_extended:
            continue
        out.append(raw)
    return out
