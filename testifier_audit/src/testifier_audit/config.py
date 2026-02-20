from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

STANDARD_ANALYSIS_BUCKET_MINUTES = [1, 5, 15, 30, 60, 120, 240]


class ColumnsConfig(BaseModel):
    id: str
    name: str
    organization: str
    position: str
    time_signed_in: str


class TimeConfig(BaseModel):
    timezone: str = "America/Los_Angeles"
    floor: str = "minute"
    off_hours_start: int = Field(default=0, ge=0, le=23)
    off_hours_end: int = Field(default=5, ge=0, le=23)


class WindowsConfig(BaseModel):
    minute_series_smooth: int = Field(default=15, ge=1)
    swing_window_minutes: int = Field(default=60, ge=1)
    scan_window_minutes: list[int] = Field(
        default_factory=lambda: list(STANDARD_ANALYSIS_BUCKET_MINUTES)
    )
    analysis_bucket_minutes: list[int] = Field(
        default_factory=lambda: list(STANDARD_ANALYSIS_BUCKET_MINUTES)
    )


class ThresholdsConfig(BaseModel):
    top_duplicate_names: int = Field(default=200, ge=1)
    burst_fdr_alpha: float = Field(default=0.01, gt=0, lt=1)
    procon_swing_fdr_alpha: float = Field(default=0.01, gt=0, lt=1)
    near_dup_max_candidates_per_block: int = Field(default=5000, ge=1)
    near_dup_similarity_threshold: int = Field(default=92, ge=0, le=100)
    swing_min_window_total: int = Field(default=25, ge=1)


class CalibrationConfig(BaseModel):
    enabled: bool = True
    mode: Literal["global", "hour_of_day", "day_of_week_hour"] = "hour_of_day"
    significance_policy: Literal["parametric_fdr", "permutation_fdr", "either_fdr"] = (
        "parametric_fdr"
    )
    iterations: int = Field(default=50, ge=0)
    random_seed: int = Field(default=42, ge=0)
    support_alpha: float = Field(default=0.1, gt=0, lt=1)


class ChangePointConfig(BaseModel):
    enabled: bool = True
    min_segment_minutes: int = Field(default=30, ge=2)
    penalty_scale: float = Field(default=3.0, gt=0.0)


class PeriodicityConfig(BaseModel):
    enabled: bool = True
    max_lag_minutes: int = Field(default=180, ge=2)
    min_period_minutes: float = Field(default=5.0, gt=0.0)
    max_period_minutes: float = Field(default=720.0, gt=0.0)
    top_n_periods: int = Field(default=20, ge=1)
    calibration_iterations: int = Field(default=100, ge=0)
    random_seed: int = Field(default=42, ge=0)
    fdr_alpha: float = Field(default=0.05, gt=0.0, lt=1.0)


class NamesConfig(BaseModel):
    strip_punctuation: bool = True
    normalize_unicode: bool = True
    nickname_map_path: str = "configs/nicknames.csv"
    phonetic: str = "double_metaphone"


class RarityConfig(BaseModel):
    enabled: bool = False
    first_name_frequency_path: str | None = None
    last_name_frequency_path: str | None = None
    epsilon: float = Field(default=1e-9, gt=0.0, lt=1.0)


class InputConfig(BaseModel):
    mode: Literal["csv", "postgres"] = "csv"
    db_url: str | None = None
    submissions_table: str = "public_submissions"
    source_file: str | None = None


class VoterRegistryConfig(BaseModel):
    enabled: bool = False
    db_url: str | None = None
    table_name: str = "voter_registry"
    active_only: bool = True
    match_bucket_minutes: int = Field(default=30, ge=1)


class MultivariateAnomalyConfig(BaseModel):
    enabled: bool = True
    bucket_minutes: int = Field(default=15, ge=1)
    contamination: float = Field(default=0.03, gt=0.0, le=0.5)
    min_bucket_total: int = Field(default=25, ge=1)
    top_n: int = Field(default=50, ge=1)
    random_seed: int = Field(default=42, ge=0)


class OutputsConfig(BaseModel):
    tables_format: str = "parquet"
    figures_format: str = "png"
    interactive_plotly: bool = False


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    columns: ColumnsConfig
    time: TimeConfig = Field(default_factory=TimeConfig)
    windows: WindowsConfig = Field(default_factory=WindowsConfig)
    thresholds: ThresholdsConfig = Field(default_factory=ThresholdsConfig)
    calibration: CalibrationConfig = Field(default_factory=CalibrationConfig)
    changepoints: ChangePointConfig = Field(default_factory=ChangePointConfig)
    periodicity: PeriodicityConfig = Field(default_factory=PeriodicityConfig)
    names: NamesConfig = Field(default_factory=NamesConfig)
    rarity: RarityConfig = Field(default_factory=RarityConfig)
    input: InputConfig = Field(default_factory=InputConfig)
    voter_registry: VoterRegistryConfig = Field(default_factory=VoterRegistryConfig)
    multivariate_anomaly: MultivariateAnomalyConfig = Field(
        default_factory=MultivariateAnomalyConfig
    )
    outputs: OutputsConfig = Field(default_factory=OutputsConfig)


DEFAULT_CONFIG_PATH = Path("configs/default.yaml")


def _resolve_optional_path(path_value: str | None, base_dir: Path) -> str | None:
    if not path_value:
        return None
    candidate = Path(path_value)
    if candidate.is_absolute():
        return str(candidate)
    return str((base_dir / candidate).resolve())


def load_config(path: Path) -> AppConfig:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    config = AppConfig.model_validate(data)
    base_dir = path.resolve().parent

    config.names.nickname_map_path = (
        _resolve_optional_path(config.names.nickname_map_path, base_dir) or ""
    )
    config.rarity.first_name_frequency_path = _resolve_optional_path(
        config.rarity.first_name_frequency_path,
        base_dir,
    )
    config.rarity.last_name_frequency_path = _resolve_optional_path(
        config.rarity.last_name_frequency_path,
        base_dir,
    )
    config.input.db_url = (
        config.input.db_url or os.getenv("TESTIFIER_AUDIT_DB_URL") or os.getenv("DATABASE_URL")
    )
    config.voter_registry.db_url = (
        config.voter_registry.db_url
        or os.getenv("TESTIFIER_AUDIT_DB_URL")
        or os.getenv("DATABASE_URL")
    )
    return config
