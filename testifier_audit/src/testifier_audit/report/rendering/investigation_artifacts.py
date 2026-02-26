from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from testifier_audit.report.rendering.serialization import _json_safe

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pq = None

LOGGER = logging.getLogger(__name__)

def _rows_to_frame(rows: Any) -> pd.DataFrame:
    if not isinstance(rows, list) or not rows:
        return pd.DataFrame()
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            normalized_rows.append(_json_safe(row))
    if not normalized_rows:
        return pd.DataFrame()
    frame = pd.DataFrame(normalized_rows)
    for column in frame.columns:
        if frame[column].map(lambda value: isinstance(value, (list, dict))).any():
            frame[column] = frame[column].map(
                lambda value: (
                    json.dumps(_json_safe(value), ensure_ascii=False)
                    if isinstance(value, (list, dict))
                    else value
                )
            )
    return frame


def _write_investigation_artifacts(
    out_dir: Path,
    triage_summary: dict[str, Any],
    data_quality_panel: Any,
) -> None:
    summary_dir = out_dir / "summary"
    tables_dir = out_dir / "tables"
    summary_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)

    summary_payload = _json_safe(triage_summary if isinstance(triage_summary, dict) else {})
    (summary_dir / "investigation_summary.json").write_text(
        json.dumps(summary_payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    raw_vs_dedup_rows = []
    if isinstance(data_quality_panel, dict):
        candidate_rows = data_quality_panel.get("raw_vs_dedup_metrics", [])
        if isinstance(candidate_rows, list):
            raw_vs_dedup_rows = candidate_rows

    queue_table = _rows_to_frame(raw_vs_dedup_rows)
    for table_name, frame in {"data_quality__raw_vs_dedup_metrics": queue_table}.items():
        csv_path = tables_dir / f"{table_name}.csv"
        frame.to_csv(csv_path, index=False)
        if pq is not None:
            parquet_path = tables_dir / f"{table_name}.parquet"
            frame.to_parquet(parquet_path, index=False)
