from __future__ import annotations

import csv
import re
import unicodedata
from functools import lru_cache
from pathlib import Path

import pandas as pd

from testifier_audit.config import NamesConfig

WHITESPACE_RE = re.compile(r"\s+")
PUNCT_RE = re.compile(r"[^\w\s\-']")
PUNCT_KEEP_COMMA_RE = re.compile(r"[^\w\s,\-']")


@lru_cache(maxsize=8)
def _load_nickname_map(path: str) -> dict[str, str]:
    file_path = Path(path)
    if not file_path.exists():
        return {}

    mapping: dict[str, str] = {}
    with file_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            alias = (row.get("alias") or "").strip().upper()
            canonical = (row.get("canonical") or "").strip().upper()
            if alias and canonical:
                mapping[alias] = canonical
    return mapping


def _normalize_name(value: str, config: NamesConfig, preserve_commas: bool = False) -> str:
    text = value.strip()
    if config.normalize_unicode:
        text = unicodedata.normalize("NFKC", text)
    if config.strip_punctuation:
        regex = PUNCT_KEEP_COMMA_RE if preserve_commas else PUNCT_RE
        text = regex.sub("", text)
    text = WHITESPACE_RE.sub(" ", text)
    return text


def _split_name(value: str) -> tuple[str, str]:
    if "," in value:
        last, first = value.split(",", 1)
        return last.strip(), first.strip()

    parts = [part for part in value.split(" ") if part]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[-1], " ".join(parts[:-1])


def _first_token(value: str) -> str:
    return value.split(" ", 1)[0].strip() if value else ""


def add_name_features(df: pd.DataFrame, config: NamesConfig) -> pd.DataFrame:
    working = df.copy()
    raw_name = working["name"].fillna("").astype(str)
    normalized = raw_name.map(lambda item: _normalize_name(item, config))
    normalized_for_split = raw_name.map(
        lambda item: _normalize_name(item, config, preserve_commas=True)
    )
    nickname_map = _load_nickname_map(config.nickname_map_path)

    split_values = normalized_for_split.map(_split_name)
    working["name_normalized"] = normalized
    working["last"] = split_values.str[0].fillna("").str.strip().str.upper()
    working["first"] = split_values.str[1].fillna("").str.strip().str.upper()

    first_primary = working["first"].map(_first_token)
    first_canonical = first_primary.map(lambda token: nickname_map.get(token, token))

    working["first_canonical"] = first_canonical
    working["first_initial"] = first_canonical.str[:1]
    working["canonical_name"] = working["last"].fillna("") + "|" + first_canonical.fillna("")
    working["name_display"] = working["last"].fillna("") + ", " + working["first"].fillna("")
    return working
