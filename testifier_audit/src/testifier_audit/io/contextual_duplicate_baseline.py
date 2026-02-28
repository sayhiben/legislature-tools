from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
import yaml

from testifier_audit.names.canonicalize import canonicalize_name
from testifier_audit.names.collision_baseline import collision_metrics_from_counts
from testifier_audit.names.nickname_map import load_nickname_map

DEFAULT_BUCKET_MINUTES = (1, 5, 15, 30, 60, 120, 240, 480, 720, 1440)


def _log_info(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} [contextual-baseline][info] {message}")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build position-free contextual duplicate baselines from local corpus CSV files."
    )
    parser.add_argument(
        "--csv-dir",
        default="data/raw/baseline_corpus",
        help="Directory containing baseline corpus CSV files.",
    )
    parser.add_argument(
        "--metadata-dir",
        default="data/metadata/baseline_corpus",
        help="Directory containing hearing sidecars (*.hearing.yaml).",
    )
    parser.add_argument(
        "--nickname-map-path",
        default="testifier_audit/configs/nicknames.csv",
        help="Nickname map CSV path used for canonicalization.",
    )
    parser.add_argument(
        "--output-json",
        default="data/metadata/contextual_duplicate_baseline.json",
        help="Output JSON path.",
    )
    parser.add_argument(
        "--output-csv",
        default="data/metadata/contextual_duplicate_baseline.csv",
        help="Output CSV path.",
    )
    parser.add_argument(
        "--bucket-minutes",
        nargs="*",
        type=int,
        default=list(DEFAULT_BUCKET_MINUTES),
        help="Bucket widths to aggregate.",
    )
    return parser.parse_args(argv)


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_level_text(value: Any) -> str:
    return _safe_text(value).lower()


def _parse_time_signed_in(values: pd.Series) -> pd.Series:
    # Fast-path the known WA exports timestamp format; fall back for edge-case rows.
    parsed = pd.to_datetime(
        values,
        format="%m/%d/%Y %I:%M %p",
        errors="coerce",
    )
    missing_mask = parsed.isna()
    if missing_mask.any():
        parsed.loc[missing_mask] = pd.to_datetime(values.loc[missing_mask], errors="coerce")
    return parsed


def load_sidecar_context(metadata_path: Path) -> tuple[str, str]:
    if not metadata_path.exists():
        return "", ""
    payload = yaml.safe_load(metadata_path.read_text(encoding="utf-8")) or {}
    source = payload.get("source", {}) if isinstance(payload, dict) else {}
    if not isinstance(source, dict):
        source = {}
    committee = _safe_text(source.get("committee_name"))
    chamber = _safe_text(source.get("chamber") or source.get("agency"))
    return committee, chamber


def build_window_rows(
    *,
    csv_path: Path,
    metadata_dir: Path,
    nickname_map: dict[str, str],
    bucket_minutes: list[int],
) -> list[dict[str, Any]]:
    frame = pd.read_csv(csv_path)
    if frame.empty:
        return []
    required = {"Name", "Time Signed In"}
    if not required.issubset(set(frame.columns)):
        return []
    frame = frame.copy()
    frame["timestamp"] = _parse_time_signed_in(frame["Time Signed In"])
    frame = frame.dropna(subset=["timestamp"])
    if frame.empty:
        return []

    canonical_keys: list[str] = []
    for raw_name in frame["Name"].fillna("").astype(str).tolist():
        canonical = canonicalize_name(raw_name, nickname_map=nickname_map)
        canonical_keys.append(canonical.collision_key_strict if canonical.is_person_name else "")
    frame["collision_key_strict"] = pd.Series(canonical_keys, index=frame.index)
    frame = frame[frame["collision_key_strict"].astype(str).str.strip() != ""].copy()
    if frame.empty:
        return []

    metadata_path = metadata_dir / f"{csv_path.stem}.hearing.yaml"
    committee, chamber = load_sidecar_context(metadata_path)

    rows: list[dict[str, Any]] = []
    for minutes in bucket_minutes:
        bucket_start = frame["timestamp"].dt.floor(f"{int(minutes)}min")
        bucketed = frame.assign(bucket_start=bucket_start).dropna(subset=["bucket_start"])
        if bucketed.empty:
            continue
        for bucket_value, group in bucketed.groupby("bucket_start", dropna=False):
            counts = group.groupby("collision_key_strict", dropna=False).size().to_numpy(dtype=float)
            metrics = collision_metrics_from_counts(counts)
            n_rows = int(metrics.get("n_rows", 0))
            if n_rows <= 0:
                continue
            duplicate_row_rate = float(metrics.get("repeated_group_rows", 0.0) / float(n_rows))
            bucket_ts = pd.Timestamp(bucket_value)
            rows.append(
                {
                    "csv_stem": csv_path.stem,
                    "committee": _safe_level_text(committee),
                    "chamber": _safe_level_text(chamber),
                    "hour_bin": int(bucket_ts.hour),
                    "weekday_bin": int(bucket_ts.weekday()),
                    "bucket_minutes": int(minutes),
                    "n_rows": int(n_rows),
                    "n_unique_names": int(metrics.get("n_unique_names", 0)),
                    "duplicate_row_rate": duplicate_row_rate,
                }
            )
    return rows


def aggregate_baseline_level(
    *,
    windows: pd.DataFrame,
    level: str,
    keys: list[str],
) -> pd.DataFrame:
    required = [*keys, "bucket_minutes"]
    grouped = (
        windows.groupby(required, dropna=False)
        .agg(
            n_windows=("duplicate_row_rate", "count"),
            n_rows_total=("n_rows", "sum"),
            duplicate_row_rate_mean=("duplicate_row_rate", "mean"),
            duplicate_row_rate_median=("duplicate_row_rate", "median"),
            median_n_rows=("n_rows", "median"),
        )
        .reset_index()
    )
    grouped["level"] = level
    grouped["shrink_k"] = grouped["median_n_rows"].astype(float).clip(lower=10.0)
    grouped.loc[grouped["n_windows"] < 5, "shrink_k"] = grouped.loc[
        grouped["n_windows"] < 5, "shrink_k"
    ].clip(lower=30.0)
    for column, default in (
        ("committee", ""),
        ("chamber", ""),
        ("hour_bin", -1),
        ("weekday_bin", -1),
    ):
        if column not in grouped.columns:
            grouped[column] = default
    return grouped[
        [
            "level",
            "committee",
            "chamber",
            "hour_bin",
            "weekday_bin",
            "bucket_minutes",
            "n_windows",
            "n_rows_total",
            "duplicate_row_rate_mean",
            "duplicate_row_rate_median",
            "median_n_rows",
            "shrink_k",
        ]
    ]


def build_contextual_duplicate_baseline_payload(
    *,
    csv_dir: Path,
    metadata_dir: Path,
    nickname_map_path: Path,
    bucket_minutes: list[int],
) -> dict[str, Any]:
    nickname_map = load_nickname_map(str(nickname_map_path))
    buckets = sorted({int(value) for value in bucket_minutes if int(value) > 0})
    window_rows: list[dict[str, Any]] = []
    csv_paths = sorted(csv_dir.glob("*.csv"))
    for csv_path in csv_paths:
        window_rows.extend(
            build_window_rows(
                csv_path=csv_path,
                metadata_dir=metadata_dir,
                nickname_map=nickname_map,
                bucket_minutes=buckets,
            )
        )
    windows = pd.DataFrame(window_rows)
    if windows.empty:
        aggregated = pd.DataFrame(
            columns=[
                "level",
                "committee",
                "chamber",
                "hour_bin",
                "weekday_bin",
                "bucket_minutes",
                "n_windows",
                "n_rows_total",
                "duplicate_row_rate_mean",
                "duplicate_row_rate_median",
                "median_n_rows",
                "shrink_k",
            ]
        )
    else:
        levels: list[pd.DataFrame] = []
        levels.append(
            aggregate_baseline_level(
                windows=windows,
                level="committee_chamber_hour_weekday_bucket",
                keys=["committee", "chamber", "hour_bin", "weekday_bin"],
            )
        )
        levels.append(
            aggregate_baseline_level(
                windows=windows,
                level="committee_hour_weekday_bucket",
                keys=["committee", "hour_bin", "weekday_bin"],
            )
        )
        levels.append(
            aggregate_baseline_level(
                windows=windows,
                level="chamber_hour_weekday_bucket",
                keys=["chamber", "hour_bin", "weekday_bin"],
            )
        )
        levels.append(
            aggregate_baseline_level(
                windows=windows,
                level="hour_weekday_bucket",
                keys=["hour_bin", "weekday_bin"],
            )
        )
        levels.append(
            aggregate_baseline_level(
                windows=windows,
                level="bucket",
                keys=[],
            )
        )
        aggregated = pd.concat(levels, ignore_index=True)
        aggregated = aggregated.sort_values(
            ["level", "bucket_minutes", "committee", "chamber", "hour_bin", "weekday_bin"]
        ).reset_index(drop=True)
    rows = aggregated.to_dict(orient="records")
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "source_csv_dir": str(csv_dir),
        "source_metadata_dir": str(metadata_dir),
        "csv_file_count": int(len(csv_paths)),
        "window_row_count": int(len(window_rows)),
        "rows": rows,
    }


def write_contextual_baseline_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "level",
        "committee",
        "chamber",
        "hour_bin",
        "weekday_bin",
        "bucket_minutes",
        "n_windows",
        "n_rows_total",
        "duplicate_row_rate_mean",
        "duplicate_row_rate_median",
        "median_n_rows",
        "shrink_k",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def write_contextual_baseline_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    csv_dir = Path(args.csv_dir).resolve()
    metadata_dir = Path(args.metadata_dir).resolve()
    nickname_map_path = Path(args.nickname_map_path).resolve()
    buckets = [int(value) for value in args.bucket_minutes if int(value) > 0]
    payload = build_contextual_duplicate_baseline_payload(
        csv_dir=csv_dir,
        metadata_dir=metadata_dir,
        nickname_map_path=nickname_map_path,
        bucket_minutes=buckets,
    )
    output_json = Path(args.output_json).resolve()
    output_csv = Path(args.output_csv).resolve()
    write_contextual_baseline_json(output_json, payload)
    rows = payload.get("rows", [])
    write_contextual_baseline_csv(
        output_csv,
        [row for row in rows if isinstance(row, dict)],
    )
    _log_info(f"Wrote contextual baseline JSON: {output_json} ({len(rows)} rows)")
    _log_info(f"Wrote contextual baseline CSV: {output_csv}")


if __name__ == "__main__":
    main()
