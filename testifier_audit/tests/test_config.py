from __future__ import annotations

from pathlib import Path

import yaml

from testifier_audit.config import load_config


def test_load_config_resolves_relative_paths(tmp_path: Path) -> None:
    (tmp_path / "nicknames.csv").write_text("alias,canonical\nBOB,ROBERT\n", encoding="utf-8")
    (tmp_path / "first.csv").write_text("name,count\nJANE,10\n", encoding="utf-8")
    (tmp_path / "last.csv").write_text("name,count\nDOE,10\n", encoding="utf-8")

    config_data = {
        "columns": {
            "id": "id",
            "name": "name",
            "organization": "organization",
            "position": "position",
            "time_signed_in": "time_signed_in",
        },
        "names": {
            "nickname_map_path": "nicknames.csv",
        },
        "rarity": {
            "enabled": True,
            "first_name_frequency_path": "first.csv",
            "last_name_frequency_path": "last.csv",
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config_data), encoding="utf-8")

    cfg = load_config(config_path)

    assert Path(cfg.names.nickname_map_path).is_absolute()
    assert Path(cfg.rarity.first_name_frequency_path or "").is_absolute()
    assert Path(cfg.rarity.last_name_frequency_path or "").is_absolute()
