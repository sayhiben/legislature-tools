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


def test_load_config_uses_env_db_url_for_input(monkeypatch, tmp_path: Path) -> None:
    config_data = {
        "columns": {
            "id": "id",
            "name": "name",
            "organization": "organization",
            "position": "position",
            "time_signed_in": "time_signed_in",
        },
        "input": {
            "mode": "postgres",
            "db_url": None,
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config_data), encoding="utf-8")

    monkeypatch.setenv(
        "TESTIFIER_AUDIT_DB_URL", "postgresql://env-user:env-pass@localhost:55432/legislature"
    )
    cfg = load_config(config_path)
    assert cfg.input.db_url == "postgresql://env-user:env-pass@localhost:55432/legislature"


def test_load_config_report_defaults_and_overrides(tmp_path: Path) -> None:
    base = {
        "columns": {
            "id": "id",
            "name": "name",
            "organization": "organization",
            "position": "position",
            "time_signed_in": "time_signed_in",
        }
    }
    base_path = tmp_path / "base.yaml"
    base_path.write_text(yaml.safe_dump(base), encoding="utf-8")
    base_cfg = load_config(base_path)
    assert base_cfg.report.default_dedup_mode == "side_by_side"
    assert base_cfg.report.min_cell_n_for_rates == 25

    override = {
        **base,
        "report": {
            "default_dedup_mode": "raw",
            "min_cell_n_for_rates": 40,
        },
    }
    override_path = tmp_path / "override.yaml"
    override_path.write_text(yaml.safe_dump(override), encoding="utf-8")
    override_cfg = load_config(override_path)
    assert override_cfg.report.default_dedup_mode == "raw"
    assert override_cfg.report.min_cell_n_for_rates == 40
