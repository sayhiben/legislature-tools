from __future__ import annotations

from pathlib import Path

import pandas as pd

from testifier_audit.detectors.base import DetectorResult
from testifier_audit.report.rendering.payload.common import _table_key
from testifier_audit.report.rendering.table_previews import _load_frame_from_candidates

def _load_table_map_from_results(
    results: dict[str, DetectorResult],
    artifacts: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    table_map: dict[str, pd.DataFrame] = {}
    for name, frame in artifacts.items():
        table_map[f"artifacts.{name}"] = frame.copy()
    for detector_name, result in results.items():
        for table_name, frame in result.tables.items():
            table_map[_table_key(detector_name, table_name)] = frame.copy()
    return table_map


def _load_table_map_from_disk(out_dir: Path) -> dict[str, pd.DataFrame]:
    table_map: dict[str, pd.DataFrame] = {}

    artifacts_dir = out_dir / "artifacts"
    if artifacts_dir.exists():
        artifact_candidates: dict[str, Path] = {}
        for path in artifacts_dir.iterdir():
            if path.suffix not in {".parquet", ".csv"}:
                continue
            existing = artifact_candidates.get(path.stem)
            if existing is None or (existing.suffix == ".csv" and path.suffix == ".parquet"):
                artifact_candidates[path.stem] = path
        for stem, path in sorted(artifact_candidates.items()):
            frame = _load_frame_from_candidates([path])
            table_map[f"artifacts.{stem}"] = frame

    tables_dir = out_dir / "tables"
    if tables_dir.exists():
        table_candidates: dict[str, Path] = {}
        for path in tables_dir.iterdir():
            if path.suffix not in {".parquet", ".csv"}:
                continue
            if "__" not in path.stem:
                continue
            detector_name, table_name = path.stem.split("__", 1)
            key = _table_key(detector_name, table_name)
            existing = table_candidates.get(key)
            if existing is None or (existing.suffix == ".csv" and path.suffix == ".parquet"):
                table_candidates[key] = path
        for key, path in sorted(table_candidates.items()):
            frame = _load_frame_from_candidates([path])
            table_map[key] = frame

    return table_map
