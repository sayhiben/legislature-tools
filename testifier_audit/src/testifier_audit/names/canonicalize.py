from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from testifier_audit.features.rarity import normalize_name_token
from testifier_audit.names.nickname_map import nickname_root

WHITESPACE_RE = re.compile(r"\s+")
PUNCT_RE = re.compile(r"[^\w\s\-']")
PUNCT_KEEP_COMMA_RE = re.compile(r"[^\w\s,\-']")

PERSON_SUFFIXES = frozenset(
    {
        "JR",
        "SR",
        "II",
        "III",
        "IV",
        "V",
    }
)

ORG_LIKE_KEYWORDS = (
    "LLC",
    "INC",
    "COALITION",
    "COMMITTEE",
    "ASSOCIATION",
    "FOUNDATION",
    "UNION",
    "COMPANY",
    "CORP",
    "ORGANIZATION",
    "PARTNERSHIP",
    "PAC",
)


@dataclass(frozen=True, slots=True)
class CanonicalizedName:
    name_normalized: str
    last: str
    first: str
    first_primary: str
    first_canonical: str
    first_nickname_root: str
    first_initial: str
    middle_initial: str
    suffix_normalized: str
    canonical_key_strict: str
    canonical_key_medium: str
    canonical_key_loose: str
    canonical_key_nickname: str
    name_display: str
    name_parse_quality: str
    name_parse_flags: str
    is_person_name: bool


def _normalize_text(value: str, *, normalize_unicode: bool, strip_punctuation: bool) -> str:
    text = str(value or "").strip()
    if normalize_unicode:
        text = unicodedata.normalize("NFKC", text)
    if strip_punctuation:
        text = PUNCT_RE.sub("", text)
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text


def _normalize_for_split(value: str, *, normalize_unicode: bool, strip_punctuation: bool) -> str:
    text = str(value or "").strip()
    if normalize_unicode:
        text = unicodedata.normalize("NFKC", text)
    if strip_punctuation:
        text = PUNCT_KEEP_COMMA_RE.sub("", text)
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text


def _split_name(value: str) -> tuple[str, str]:
    if "," in value:
        last, first = value.split(",", 1)
        return last.strip(), first.strip()
    parts = [item.strip() for item in value.split(" ") if item.strip()]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[-1], " ".join(parts[:-1])


def _org_like(raw_upper: str, *, token_count: int) -> bool:
    if not raw_upper:
        return False
    if any(keyword in raw_upper for keyword in ORG_LIKE_KEYWORDS):
        return True
    if re.search(r"\d", raw_upper):
        return True
    # Very long names are often organization-style strings in this dataset.
    if token_count >= 5:
        return True
    return False


def canonicalize_name(
    raw_name: str,
    *,
    nickname_map: dict[str, str],
    normalize_unicode: bool = True,
    strip_punctuation: bool = True,
) -> CanonicalizedName:
    normalized = _normalize_text(
        raw_name,
        normalize_unicode=normalize_unicode,
        strip_punctuation=strip_punctuation,
    )
    normalized_for_split = _normalize_for_split(
        raw_name,
        normalize_unicode=normalize_unicode,
        strip_punctuation=strip_punctuation,
    )
    last_raw, first_raw = _split_name(normalized_for_split)

    last = normalize_name_token(last_raw)
    first = normalize_name_token(first_raw)

    first_tokens = [normalize_name_token(token) for token in first_raw.split(" ") if token.strip()]
    first_tokens = [token for token in first_tokens if token]

    suffix_normalized = ""
    if first_tokens and first_tokens[-1] in PERSON_SUFFIXES:
        suffix_normalized = first_tokens.pop()

    first_primary = first_tokens[0] if first_tokens else ""
    middle_initial = first_tokens[1][:1] if len(first_tokens) > 1 else ""
    first_nickname_root = nickname_root(first_primary, nickname_map)
    first_canonical = first_nickname_root
    first_initial = first_canonical[:1]

    canonical_key_strict = (
        f"{last}|{first_canonical}|{middle_initial}|{suffix_normalized}"
        if last or first_canonical or middle_initial or suffix_normalized
        else "|||"
    )
    canonical_key_medium = f"{last}|{first_canonical}" if last or first_canonical else "|"
    canonical_key_loose = f"{last}|{first_initial}" if last or first_initial else "|"
    canonical_key_nickname = (
        f"{last}|{first_nickname_root}" if last or first_nickname_root else "|"
    )
    name_display = f"{last}, {first}".strip(", ").strip()

    parse_flags: list[str] = []
    if not normalized:
        parse_flags.append("missing_name")
    if not last:
        parse_flags.append("missing_last")
    if not first_primary:
        parse_flags.append("missing_first")
    if "," not in normalized_for_split and normalized_for_split:
        parse_flags.append("implied_last_parse")
    org_like = _org_like(normalized.upper(), token_count=len(normalized.split(" ")))
    if org_like:
        parse_flags.append("org_like")

    is_person_name = bool(last and first_primary and not org_like)
    if not normalized:
        name_parse_quality = "invalid"
    elif is_person_name:
        name_parse_quality = "high"
    elif last or first_primary:
        name_parse_quality = "low"
    else:
        name_parse_quality = "invalid"

    return CanonicalizedName(
        name_normalized=normalized,
        last=last,
        first=first,
        first_primary=first_primary,
        first_canonical=first_canonical,
        first_nickname_root=first_nickname_root,
        first_initial=first_initial,
        middle_initial=middle_initial,
        suffix_normalized=suffix_normalized,
        canonical_key_strict=canonical_key_strict,
        canonical_key_medium=canonical_key_medium,
        canonical_key_loose=canonical_key_loose,
        canonical_key_nickname=canonical_key_nickname,
        name_display=name_display,
        name_parse_quality=name_parse_quality,
        name_parse_flags=",".join(sorted(set(parse_flags))),
        is_person_name=is_person_name,
    )
