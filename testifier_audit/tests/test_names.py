from __future__ import annotations

from pathlib import Path

import pandas as pd

from testifier_audit.config import NamesConfig
from testifier_audit.preprocess.names import add_name_features


def test_add_name_features_parses_last_first() -> None:
    df = pd.DataFrame({"name": ["Doe, Jane"]})
    cfg = NamesConfig(nickname_map_path="/tmp/does-not-exist.csv")
    out = add_name_features(df=df, config=cfg)

    assert out.loc[0, "last"] == "DOE"
    assert out.loc[0, "first"] == "JANE"
    assert out.loc[0, "first_initial"] == "J"
    assert out.loc[0, "canonical_name"] == "DOE|JANE"


def test_add_name_features_preserves_strict_keys_and_emits_nickname_key(tmp_path: Path) -> None:
    nickname_path = tmp_path / "nicknames.csv"
    nickname_path.write_text("alias,canonical\nNORM,NORMAN\n", encoding="utf-8")

    df = pd.DataFrame({"name": ["Hershaw, Norm", "Hershaw, Norman"]})
    cfg = NamesConfig(nickname_map_path=str(nickname_path))
    out = add_name_features(df=df, config=cfg)

    assert out.loc[0, "canonical_key_medium"] == "HERSHAW|NORM"
    assert out.loc[1, "canonical_key_medium"] == "HERSHAW|NORMAN"
    assert out.loc[0, "canonical_key_nickname"] == "HERSHAW|NORMAN"
    assert out.loc[1, "canonical_key_nickname"] == "HERSHAW|NORMAN"
    assert out.loc[0, "canonical_name"] == "HERSHAW|NORM"
