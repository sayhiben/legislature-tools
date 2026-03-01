from __future__ import annotations

from pathlib import Path

import yaml

from testifier_audit.config import load_config


def test_load_config_resolves_relative_paths(tmp_path: Path) -> None:
    (tmp_path / "nicknames.csv").write_text("alias,canonical\nBOB,ROBERT\n", encoding="utf-8")
    (tmp_path / "hearing.yaml").write_text(
        "schema_version: 1\nhearing_id: TEST\ntimezone: UTC\nmeeting_start: 2026-02-06T13:30:00Z\n",
        encoding="utf-8",
    )

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
        "name_analysis": {
            "contextual_baseline_path": "contextual_baseline.csv",
            "historical_reference_reports_dir": "reports",
            "historical_reference_loo_path": "cross_hearing_baseline_loo.json",
        },
        "input": {
            "hearing_metadata_path": "hearing.yaml",
        },
    }
    config_path = tmp_path / "config.yaml"
    (tmp_path / "contextual_baseline.csv").write_text(
        "bucket_minutes,shrink_k\n30,30.0\n",
        encoding="utf-8",
    )
    (tmp_path / "reports").mkdir(parents=True, exist_ok=True)
    (tmp_path / "cross_hearing_baseline_loo.json").write_text("{}", encoding="utf-8")
    config_path.write_text(yaml.safe_dump(config_data), encoding="utf-8")

    cfg = load_config(config_path)

    assert Path(cfg.names.nickname_map_path).is_absolute()
    assert Path(cfg.input.hearing_metadata_path or "").is_absolute()
    assert Path(cfg.name_analysis.contextual_baseline_path or "").is_absolute()
    assert Path(cfg.name_analysis.historical_reference_reports_dir or "").is_absolute()
    assert Path(cfg.name_analysis.historical_reference_loo_path or "").is_absolute()


def test_load_config_resolves_configs_prefixed_paths_from_configs_dir(tmp_path: Path) -> None:
    config_dir = tmp_path / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    nickname_path = config_dir / "nicknames.csv"
    nickname_path.write_text("alias,canonical\nNORM,NORMAN\n", encoding="utf-8")

    config_data = {
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
    config_path = config_dir / "voter_registry_enabled.yaml"
    config_path.write_text(yaml.safe_dump(config_data), encoding="utf-8")

    cfg = load_config(config_path)
    assert Path(cfg.names.nickname_map_path) == nickname_path.resolve()


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
    assert base_cfg.name_analysis.collision_scope_primary == "full_hearing"
    assert base_cfg.name_analysis.collision_scope_overlays == []
    assert base_cfg.name_analysis.position_hearing_baseline_enabled is True
    assert base_cfg.name_analysis.position_baseline_shrink_k == 30.0
    assert base_cfg.name_analysis.position_interval_nominal == 0.95
    assert base_cfg.name_analysis.position_interval_draws == 5000
    assert base_cfg.name_analysis.position_claim_min_rows_per_position == 25
    assert base_cfg.name_analysis.low_power_min_unique_names_scope is None
    assert base_cfg.name_analysis.low_power_min_expected_duplicates_scope is None
    assert base_cfg.name_analysis.low_power_min_unique_names_bucket is None
    assert base_cfg.name_analysis.low_power_min_expected_duplicates_bucket is None
    assert base_cfg.name_analysis.low_power_min_unique_names_position is None
    assert base_cfg.name_analysis.low_power_min_expected_duplicates_position is None
    assert base_cfg.voter_registry.status_mode == "single"

    override = {
        **base,
        "report": {
            "default_dedup_mode": "raw",
            "min_cell_n_for_rates": 40,
        },
        "name_analysis": {
            "position_interval_nominal": 0.9,
            "position_interval_draws": 1234,
            "position_claim_min_rows_per_position": 40,
            "low_power_min_unique_names_scope": 22,
            "low_power_min_expected_duplicates_scope": 4.0,
            "low_power_min_unique_names_bucket": 30,
            "low_power_min_expected_duplicates_bucket": 6.0,
            "low_power_min_unique_names_position": 20,
            "low_power_min_expected_duplicates_position": 3.0,
        },
    }
    override_path = tmp_path / "override.yaml"
    override_path.write_text(yaml.safe_dump(override), encoding="utf-8")
    override_cfg = load_config(override_path)
    assert override_cfg.report.default_dedup_mode == "raw"
    assert override_cfg.report.min_cell_n_for_rates == 40
    assert override_cfg.name_analysis.position_interval_nominal == 0.9
    assert override_cfg.name_analysis.position_interval_draws == 1234
    assert override_cfg.name_analysis.position_claim_min_rows_per_position == 40
    assert override_cfg.name_analysis.low_power_min_unique_names_scope == 22
    assert override_cfg.name_analysis.low_power_min_expected_duplicates_scope == 4.0
    assert override_cfg.name_analysis.low_power_min_unique_names_bucket == 30
    assert override_cfg.name_analysis.low_power_min_expected_duplicates_bucket == 6.0
    assert override_cfg.name_analysis.low_power_min_unique_names_position == 20
    assert override_cfg.name_analysis.low_power_min_expected_duplicates_position == 3.0


def test_load_config_off_hours_defaults_and_overrides(tmp_path: Path) -> None:
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
    assert base_cfg.off_hours.min_window_total == 25
    assert base_cfg.off_hours.fdr_alpha == 0.05
    assert base_cfg.off_hours.primary_bucket_minutes == 30
    assert base_cfg.off_hours.model_hour_harmonics == 3
    assert base_cfg.off_hours.alert_off_hours_min_fraction == 1.0
    assert base_cfg.off_hours.primary_alert_min_abs_delta == 0.03

    override = {
        **base,
        "off_hours": {
            "bucket_minutes": [15, 30, 60],
            "min_window_total": 40,
            "fdr_alpha": 0.02,
            "primary_bucket_minutes": 60,
            "model_min_rows": 30,
            "model_hour_harmonics": 4,
            "alert_off_hours_min_fraction": 0.9,
            "primary_alert_min_abs_delta": 0.06,
        },
    }
    override_path = tmp_path / "override.yaml"
    override_path.write_text(yaml.safe_dump(override), encoding="utf-8")
    override_cfg = load_config(override_path)
    assert override_cfg.off_hours.bucket_minutes == [15, 30, 60]
    assert override_cfg.off_hours.min_window_total == 40
    assert override_cfg.off_hours.fdr_alpha == 0.02
    assert override_cfg.off_hours.primary_bucket_minutes == 60
    assert override_cfg.off_hours.model_min_rows == 30
    assert override_cfg.off_hours.model_hour_harmonics == 4
    assert override_cfg.off_hours.alert_off_hours_min_fraction == 0.9
    assert override_cfg.off_hours.primary_alert_min_abs_delta == 0.06
