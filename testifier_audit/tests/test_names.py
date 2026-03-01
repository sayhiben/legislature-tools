from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from testifier_audit.config import NamesConfig, load_config
from testifier_audit.names.nickname_map import load_nickname_map, nickname_root
from testifier_audit.preprocess.names import add_name_features


def test_add_name_features_parses_last_first() -> None:
    df = pd.DataFrame({"name": ["Doe, Jane"]})
    cfg = NamesConfig(nickname_map_path="/tmp/does-not-exist.csv")
    out = add_name_features(df=df, config=cfg)

    assert out.loc[0, "last"] == "DOE"
    assert out.loc[0, "first"] == "JANE"
    assert out.loc[0, "first_initial"] == "J"
    assert out.loc[0, "canonical_name"] == "DOE|JANE"
    assert out.loc[0, "full_name_key"] == "DOE|JANE||"
    assert out.loc[0, "first_name_key"] == "JANE"
    assert out.loc[0, "last_name_key"] == "DOE"
    assert out.loc[0, "normalization_version"] != ""
    assert out.loc[0, "normalization_version_hash"] != ""


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


def test_add_name_features_uses_configs_prefixed_nickname_map_from_loaded_config(
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "nicknames.csv").write_text("alias,canonical\nNORM,NORMAN\n", encoding="utf-8")

    config_path = config_dir / "voter_registry_enabled.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "columns": {
                    "id": "id",
                    "name": "name",
                    "organization": "organization",
                    "position": "position",
                    "time_signed_in": "time_signed_in",
                },
                "names": {
                    "nickname_map_path": "configs/nicknames.csv",
                },
            }
        ),
        encoding="utf-8",
    )

    cfg = load_config(config_path)
    df = pd.DataFrame({"name": ["Harshaw, Norm", "Harshaw, Norman"]})
    out = add_name_features(df=df, config=cfg.names)

    assert out.loc[0, "canonical_key_nickname"] == "HARSHAW|NORMAN"
    assert out.loc[1, "canonical_key_nickname"] == "HARSHAW|NORMAN"


def test_project_nickname_map_includes_becky_to_rebecca_override() -> None:
    nickname_path = Path(__file__).resolve().parents[1] / "configs" / "nicknames.csv"
    mapping = load_nickname_map(str(nickname_path))

    assert mapping.get("BECKY") == "REBECCA"
    assert nickname_root("BECKY", mapping) == "REBECCA"
