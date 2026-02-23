from __future__ import annotations

import csv
from functools import lru_cache
from pathlib import Path

from testifier_audit.features.rarity import normalize_name_token


@lru_cache(maxsize=8)
def load_nickname_map(path: str) -> dict[str, str]:
    file_path = Path(path)
    if not file_path.exists():
        return {}

    mapping: dict[str, str] = {}
    with file_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            alias = normalize_name_token(row.get("alias", ""))
            canonical = normalize_name_token(row.get("canonical", ""))
            if not alias or not canonical:
                continue
            mapping[alias] = canonical
    return mapping


def nickname_root(first_name_token: str, mapping: dict[str, str]) -> str:
    normalized = normalize_name_token(first_name_token)
    if not normalized:
        return ""
    return mapping.get(normalized, normalized)
