from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from typing import Mapping

from testifier_audit.names.canonicalize import CanonicalizedName, canonicalize_name

NORMALIZATION_POLICY_ID = "shared_name_normalization_v1"
FULL_NAME_KEY_KIND = "canonical_key_strict"


@dataclass(frozen=True, slots=True)
class NormalizedNameRecord:
    canonicalized: CanonicalizedName
    full_name_key: str
    first_name_key: str
    last_name_key: str
    normalization_version: str
    normalization_version_hash: str


def compose_person_name(
    *,
    first_name: str,
    last_name: str,
    middle_name: str = "",
    suffix: str = "",
) -> str:
    first_tokens = [str(first_name or "").strip(), str(middle_name or "").strip(), str(suffix or "").strip()]
    first_clause = " ".join(token for token in first_tokens if token)
    last_clause = str(last_name or "").strip()
    if last_clause and first_clause:
        return f"{last_clause}, {first_clause}"
    if last_clause:
        return last_clause
    return first_clause


def normalization_version_hash(
    *,
    normalize_unicode: bool = True,
    strip_punctuation: bool = True,
    nickname_map: Mapping[str, str] | None = None,
    nickname_map_path: str | Path | None = None,
) -> str:
    hasher = sha1()
    hasher.update(NORMALIZATION_POLICY_ID.encode("utf-8"))
    hasher.update(f"|normalize_unicode={int(bool(normalize_unicode))}".encode("utf-8"))
    hasher.update(f"|strip_punctuation={int(bool(strip_punctuation))}".encode("utf-8"))

    if nickname_map:
        normalized_items = sorted(
            (str(alias or "").strip().upper(), str(root or "").strip().upper())
            for alias, root in nickname_map.items()
            if str(alias or "").strip() and str(root or "").strip()
        )
        for alias, root in normalized_items:
            hasher.update(alias.encode("utf-8"))
            hasher.update(b"->")
            hasher.update(root.encode("utf-8"))
            hasher.update(b"\n")
    elif nickname_map_path:
        path = Path(nickname_map_path)
        if path.exists():
            hasher.update(path.read_bytes())

    return hasher.hexdigest()


def normalization_version(
    *,
    normalize_unicode: bool = True,
    strip_punctuation: bool = True,
    nickname_map: Mapping[str, str] | None = None,
    nickname_map_path: str | Path | None = None,
) -> str:
    version_hash = normalization_version_hash(
        normalize_unicode=normalize_unicode,
        strip_punctuation=strip_punctuation,
        nickname_map=nickname_map,
        nickname_map_path=nickname_map_path,
    )
    return f"{NORMALIZATION_POLICY_ID}:{version_hash[:12]}"


def normalize_name_record(
    raw_name: str,
    *,
    nickname_map: Mapping[str, str],
    normalize_unicode: bool = True,
    strip_punctuation: bool = True,
    normalization_version_value: str | None = None,
    normalization_version_hash_value: str | None = None,
) -> NormalizedNameRecord:
    canonicalized = canonicalize_name(
        raw_name,
        nickname_map=nickname_map,
        normalize_unicode=normalize_unicode,
        strip_punctuation=strip_punctuation,
    )
    version_hash = str(normalization_version_hash_value or "").strip() or normalization_version_hash(
        normalize_unicode=normalize_unicode,
        strip_punctuation=strip_punctuation,
        nickname_map=nickname_map,
    )
    version = str(normalization_version_value or "").strip() or f"{NORMALIZATION_POLICY_ID}:{version_hash[:12]}"
    return NormalizedNameRecord(
        canonicalized=canonicalized,
        full_name_key=canonicalized.canonical_key_strict,
        first_name_key=canonicalized.first_primary,
        last_name_key=canonicalized.last,
        normalization_version=version,
        normalization_version_hash=version_hash,
    )
