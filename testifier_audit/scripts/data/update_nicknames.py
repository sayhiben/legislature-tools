#!/usr/bin/env python3
from __future__ import annotations

import csv
import io
import re
import unicodedata
import urllib.request
from collections import defaultdict
from pathlib import Path


SOURCE_COMMIT = "e13a5c051689bebe5178c0b2d4730cb46a3cb698"
SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    f"carltonnorthern/nicknames/{SOURCE_COMMIT}/names.csv"
)
OUTPUT_PATH = Path(__file__).resolve().parents[2] / "configs" / "nicknames.csv"

# Preserve the project-specific nicknames that were previously curated.
MANUAL_OVERRIDES: dict[str, str] = {
    "BOB": "ROBERT",
    "BILL": "WILLIAM",
    "JIM": "JAMES",
}

TOKEN_RE = re.compile(r"[^A-Z'\- ]")
WHITESPACE_RE = re.compile(r"\s+")


def normalize_token(value: str) -> str:
    text = unicodedata.normalize("NFKD", value)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.strip().upper()
    text = TOKEN_RE.sub("", text)
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text


def load_source_rows() -> list[dict[str, str]]:
    with urllib.request.urlopen(SOURCE_URL, timeout=30) as response:
        payload = response.read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(payload))
    return [dict(row) for row in reader]


def build_mapping(rows: list[dict[str, str]]) -> dict[str, str]:
    by_alias: dict[str, set[str]] = defaultdict(set)

    for row in rows:
        if (row.get("relationship") or "").strip() != "has_nickname":
            continue
        canonical = normalize_token(row.get("name1") or "")
        alias = normalize_token(row.get("name2") or "")
        if not canonical or not alias:
            continue
        if alias == canonical:
            continue
        if " " in alias or " " in canonical:
            # The pipeline canonicalizes only the first token.
            continue
        by_alias[alias].add(canonical)

    unambiguous = {
        alias: next(iter(canonicals))
        for alias, canonicals in by_alias.items()
        if len(canonicals) == 1
    }
    unambiguous.update(MANUAL_OVERRIDES)
    return unambiguous


def write_mapping(mapping: dict[str, str]) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["canonical", "alias"])
        writer.writeheader()
        for alias in sorted(mapping):
            writer.writerow({"canonical": mapping[alias], "alias": alias})


def main() -> None:
    rows = load_source_rows()
    mapping = build_mapping(rows)
    write_mapping(mapping)
    print(f"source_url={SOURCE_URL}")
    print(f"output_path={OUTPUT_PATH}")
    print(f"rows_written={len(mapping)}")


if __name__ == "__main__":
    main()
