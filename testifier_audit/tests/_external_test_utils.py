from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from testifier_audit.config import NamesConfig
from testifier_audit.preprocess.names import add_name_features


def workspace_root() -> Path:
    return Path(__file__).resolve().parents[1]


def nickname_map_path() -> str:
    return str((workspace_root() / "configs" / "nicknames.csv").resolve())


def write_run_all_config(
    *,
    out_path: Path,
    voter_enabled: bool,
    voter_db_url: str | None,
    analysis_bucket_minutes: list[int],
    collision_uncertainty_mode: str,
    collision_scope_primary: str,
    collision_scope_overlays: list[str],
) -> Path:
    default_config_path = workspace_root() / "configs" / "default.yaml"
    config = yaml.safe_load(default_config_path.read_text(encoding="utf-8"))

    config["names"]["nickname_map_path"] = nickname_map_path()
    config["outputs"]["tables_format"] = "csv"
    config["outputs"]["interactive_plotly"] = False
    config["input"]["mode"] = "csv"

    config["windows"]["analysis_bucket_minutes"] = list(analysis_bucket_minutes)
    config["windows"]["scan_window_minutes"] = list(analysis_bucket_minutes)
    config["windows"]["swing_window_minutes"] = max(analysis_bucket_minutes)

    config["name_analysis"]["collision_uncertainty_mode"] = collision_uncertainty_mode
    config["name_analysis"]["collision_scope_primary"] = collision_scope_primary
    config["name_analysis"]["collision_scope_overlays"] = list(collision_scope_overlays)

    config["voter_registry"]["enabled"] = bool(voter_enabled)
    config["voter_registry"]["db_url"] = voter_db_url
    config["voter_registry"]["match_bucket_minutes"] = int(analysis_bucket_minutes[0])

    config["off_hours"]["bucket_minutes"] = list(analysis_bucket_minutes)
    config["off_hours"]["primary_bucket_minutes"] = int(analysis_bucket_minutes[0])

    out_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return out_path


def canonicalize_name_frame(
    *,
    frame: pd.DataFrame,
    name_column: str,
) -> pd.DataFrame:
    return add_name_features(
        df=frame.rename(columns={name_column: "name"}),
        config=NamesConfig(nickname_map_path=nickname_map_path()),
    )


def canonical_names_from_submission_csv(csv_path: Path) -> pd.Series:
    frame = pd.read_csv(csv_path)
    names = canonicalize_name_frame(frame=frame, name_column="Name")
    person_mask = names["is_person_name"].astype(bool)
    canonical = names["canonical_name"].fillna("").astype(str)
    canonical = canonical[person_mask]
    canonical = canonical[(canonical != "") & (canonical != "|")]
    return canonical


def registry_lookup_tables(registry_csv: Path) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    frame = pd.read_csv(registry_csv)
    names = canonicalize_name_frame(frame=frame, name_column="Name")
    names["canonical_name"] = names["canonical_name"].fillna("").astype(str)
    names = names[(names["canonical_name"] != "") & (names["canonical_name"] != "|")].copy()

    exact_lookup = (
        names.groupby("canonical_name", dropna=False)
        .size()
        .rename("n_registry_rows")
        .reset_index()
        .sort_values("canonical_name")
        .reset_index(drop=True)
    )
    candidates = (
        names.groupby(["last", "first_primary", "canonical_name"], dropna=False)
        .size()
        .rename("n_registry_rows")
        .reset_index()
        .rename(columns={"last": "canonical_last", "first_primary": "canonical_first"})
        .sort_values(["canonical_last", "canonical_first", "canonical_name"])
        .reset_index(drop=True)
    )
    return exact_lookup, candidates, int(len(names))


def load_expected_json(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))
