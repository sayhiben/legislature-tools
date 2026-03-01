from __future__ import annotations

import pandas as pd

from testifier_audit.config import NamesConfig
from testifier_audit.names.nickname_map import load_nickname_map
from testifier_audit.names.normalization import (
    normalization_version,
    normalization_version_hash,
    normalize_name_record,
)


def add_name_features(df: pd.DataFrame, config: NamesConfig) -> pd.DataFrame:
    working = df.copy()
    raw_name = working["name"].fillna("").astype(str)
    nickname_map = load_nickname_map(config.nickname_map_path)
    version_hash = normalization_version_hash(
        normalize_unicode=config.normalize_unicode,
        strip_punctuation=config.strip_punctuation,
        nickname_map=nickname_map,
    )
    version_value = normalization_version(
        normalize_unicode=config.normalize_unicode,
        strip_punctuation=config.strip_punctuation,
        nickname_map=nickname_map,
    )

    normalized = raw_name.map(
        lambda value: normalize_name_record(
            value,
            nickname_map=nickname_map,
            normalize_unicode=config.normalize_unicode,
            strip_punctuation=config.strip_punctuation,
            normalization_version_value=version_value,
            normalization_version_hash_value=version_hash,
        )
    )
    canonicalized = normalized.map(lambda item: item.canonicalized)

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
    working["collision_key_strict"] = canonicalized.map(lambda item: item.collision_key_strict)
    working["collision_key_medium"] = canonicalized.map(lambda item: item.collision_key_medium)
    working["collision_key_loose"] = canonicalized.map(lambda item: item.collision_key_loose)
    working["name_parse_quality"] = canonicalized.map(lambda item: item.name_parse_quality)
    working["name_parse_flags"] = canonicalized.map(lambda item: item.name_parse_flags)
    working["is_person_name"] = canonicalized.map(lambda item: bool(item.is_person_name))
    working["name_display"] = canonicalized.map(lambda item: item.name_display)
    working["full_name_key"] = normalized.map(lambda item: item.full_name_key)
    working["first_name_key"] = normalized.map(lambda item: item.first_name_key)
    working["last_name_key"] = normalized.map(lambda item: item.last_name_key)
    working["normalization_version"] = normalized.map(lambda item: item.normalization_version)
    working["normalization_version_hash"] = normalized.map(
        lambda item: item.normalization_version_hash
    )

    # Legacy compatibility alias retained while downstream modules migrate.
    working["canonical_name"] = working["canonical_key_medium"].fillna("|").astype(str)
    return working
