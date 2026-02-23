from __future__ import annotations

import pandas as pd

from testifier_audit.config import NamesConfig
from testifier_audit.names.canonicalize import canonicalize_name
from testifier_audit.names.nickname_map import load_nickname_map


def add_name_features(df: pd.DataFrame, config: NamesConfig) -> pd.DataFrame:
    working = df.copy()
    raw_name = working["name"].fillna("").astype(str)
    nickname_map = load_nickname_map(config.nickname_map_path)

    canonicalized = raw_name.map(
        lambda value: canonicalize_name(
            value,
            nickname_map=nickname_map,
            normalize_unicode=config.normalize_unicode,
            strip_punctuation=config.strip_punctuation,
        )
    )

    working["name_normalized"] = canonicalized.map(lambda item: item.name_normalized)
    working["last"] = canonicalized.map(lambda item: item.last)
    working["first"] = canonicalized.map(lambda item: item.first)
    working["first_primary"] = canonicalized.map(lambda item: item.first_primary)
    working["first_canonical"] = canonicalized.map(lambda item: item.first_canonical)
    working["first_nickname_root"] = canonicalized.map(lambda item: item.first_nickname_root)
    working["first_initial"] = canonicalized.map(lambda item: item.first_initial)
    working["middle_initial"] = canonicalized.map(lambda item: item.middle_initial)
    working["suffix_normalized"] = canonicalized.map(lambda item: item.suffix_normalized)
    working["canonical_key_strict"] = canonicalized.map(lambda item: item.canonical_key_strict)
    working["canonical_key_medium"] = canonicalized.map(lambda item: item.canonical_key_medium)
    working["canonical_key_loose"] = canonicalized.map(lambda item: item.canonical_key_loose)
    working["canonical_key_nickname"] = canonicalized.map(lambda item: item.canonical_key_nickname)
    working["name_parse_quality"] = canonicalized.map(lambda item: item.name_parse_quality)
    working["name_parse_flags"] = canonicalized.map(lambda item: item.name_parse_flags)
    working["is_person_name"] = canonicalized.map(lambda item: bool(item.is_person_name))
    working["name_display"] = canonicalized.map(lambda item: item.name_display)

    # Legacy compatibility alias retained while downstream modules migrate.
    working["canonical_name"] = working["canonical_key_medium"].fillna("|").astype(str)
    return working
