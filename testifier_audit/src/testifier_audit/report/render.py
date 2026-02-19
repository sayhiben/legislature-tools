from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape

from testifier_audit.detectors.base import DetectorResult

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pq = None


def _template_env() -> Environment:
    templates_path = Path(__file__).resolve().parent / "templates"
    return Environment(
        loader=FileSystemLoader(str(templates_path)),
        autoescape=select_autoescape(enabled_extensions=("html", "xml")),
        trim_blocks=True,
        lstrip_blocks=True,
    )


def _serialize_value(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, pd.Timedelta):
        return str(value)
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value


def _table_preview(df: pd.DataFrame, max_rows: int = 12) -> list[dict[str, Any]]:
    limited = df.head(max_rows).copy()
    for column in limited.columns:
        limited[column] = limited[column].map(_serialize_value)
    return limited.to_dict(orient="records")


def _load_summaries_from_disk(out_dir: Path) -> dict[str, dict[str, Any]]:
    summary_dir = out_dir / "summary"
    if not summary_dir.exists():
        return {}

    summaries: dict[str, dict[str, Any]] = {}
    for path in sorted(summary_dir.glob("*.json")):
        with path.open("r", encoding="utf-8") as handle:
            summaries[path.stem] = json.load(handle)
    return summaries


def _artifact_rows_from_disk(out_dir: Path) -> dict[str, int]:
    artifacts_dir = out_dir / "artifacts"
    if not artifacts_dir.exists():
        return {}

    rows: dict[str, int] = {}
    for path in sorted(artifacts_dir.iterdir()):
        if path.suffix == ".parquet":
            if pq is not None:
                rows[path.stem] = int(pq.ParquetFile(path).metadata.num_rows)
        elif path.suffix == ".csv":
            with path.open("r", encoding="utf-8") as handle:
                line_count = sum(1 for _ in handle)
            rows[path.stem] = max(line_count - 1, 0)
    return rows


def _table_previews_from_results(
    results: dict[str, DetectorResult],
    max_rows: int = 12,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    previews: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for detector_name, result in sorted(results.items()):
        detector_tables: dict[str, list[dict[str, Any]]] = {}
        for table_name, table in sorted(result.tables.items()):
            if table.empty:
                continue
            detector_tables[table_name] = _table_preview(table, max_rows=max_rows)
        if detector_tables:
            previews[detector_name] = detector_tables
    return previews


def _load_table_previews_from_disk(
    out_dir: Path,
    max_rows: int = 12,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    tables_dir = out_dir / "tables"
    if not tables_dir.exists():
        return {}

    previews: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(dict)
    for path in sorted(tables_dir.iterdir()):
        if "__" not in path.stem:
            continue
        detector_name, table_name = path.stem.split("__", 1)

        table: pd.DataFrame
        try:
            if path.suffix == ".csv":
                table = pd.read_csv(path, nrows=max_rows)
            elif path.suffix == ".parquet":
                table = pd.read_parquet(path).head(max_rows)
            else:
                continue
        except Exception:
            continue

        if table.empty:
            continue
        previews[detector_name][table_name] = _table_preview(table, max_rows=max_rows)

    return dict(previews)


def _evidence_bundle_preview_from_results(
    results: dict[str, DetectorResult],
    max_rows: int = 25,
) -> list[dict[str, Any]]:
    composite = results.get("composite_score")
    if composite is None:
        return []
    table = composite.tables.get("evidence_bundle_windows")
    if table is None or table.empty:
        return []
    return _table_preview(table, max_rows=max_rows)


def _evidence_bundle_preview_from_disk(
    out_dir: Path,
    max_rows: int = 25,
) -> list[dict[str, Any]]:
    tables_dir = out_dir / "tables"
    if not tables_dir.exists():
        return []

    candidates = [
        tables_dir / "composite_score__evidence_bundle_windows.parquet",
        tables_dir / "composite_score__evidence_bundle_windows.csv",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            if path.suffix == ".parquet":
                table = pd.read_parquet(path).head(max_rows)
            else:
                table = pd.read_csv(path, nrows=max_rows)
        except Exception:
            continue
        if table.empty:
            return []
        return _table_preview(table, max_rows=max_rows)
    return []


def _rare_names_table_preview_from_results(
    results: dict[str, DetectorResult],
    table_name: str,
    max_rows: int = 25,
) -> list[dict[str, Any]]:
    rare_names = results.get("rare_names")
    if rare_names is None:
        return []
    table = rare_names.tables.get(table_name)
    if table is None or table.empty:
        return []
    return _table_preview(table, max_rows=max_rows)


def _rare_names_table_preview_from_disk(
    out_dir: Path,
    table_name: str,
    max_rows: int = 25,
) -> list[dict[str, Any]]:
    tables_dir = out_dir / "tables"
    if not tables_dir.exists():
        return []

    candidates = [
        tables_dir / f"rare_names__{table_name}.parquet",
        tables_dir / f"rare_names__{table_name}.csv",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            if path.suffix == ".parquet":
                table = pd.read_parquet(path).head(max_rows)
            else:
                table = pd.read_csv(path, nrows=max_rows)
        except Exception:
            continue
        if table.empty:
            return []
        return _table_preview(table, max_rows=max_rows)
    return []


def _periodicity_table_preview_from_results(
    results: dict[str, DetectorResult],
    table_name: str,
    max_rows: int = 25,
) -> list[dict[str, Any]]:
    periodicity = results.get("periodicity")
    if periodicity is None:
        return []
    table = periodicity.tables.get(table_name)
    if table is None or table.empty:
        return []
    return _table_preview(table, max_rows=max_rows)


def _periodicity_table_preview_from_disk(
    out_dir: Path,
    table_name: str,
    max_rows: int = 25,
) -> list[dict[str, Any]]:
    tables_dir = out_dir / "tables"
    if not tables_dir.exists():
        return []

    candidates = [
        tables_dir / f"periodicity__{table_name}.parquet",
        tables_dir / f"periodicity__{table_name}.csv",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            if path.suffix == ".parquet":
                table = pd.read_parquet(path).head(max_rows)
            else:
                table = pd.read_csv(path, nrows=max_rows)
        except Exception:
            continue
        if table.empty:
            return []
        return _table_preview(table, max_rows=max_rows)
    return []


def render_report(
    results: dict[str, DetectorResult],
    artifacts: dict[str, pd.DataFrame],
    out_dir: Path,
) -> Path:
    env = _template_env()
    template = env.get_template("report.html.j2")

    detector_summaries = (
        {name: result.summary for name, result in sorted(results.items())}
        if results
        else _load_summaries_from_disk(out_dir)
    )
    artifact_rows = (
        {name: len(table) for name, table in sorted(artifacts.items())}
        if artifacts
        else _artifact_rows_from_disk(out_dir)
    )
    table_previews = (
        _table_previews_from_results(results)
        if results
        else _load_table_previews_from_disk(out_dir)
    )
    evidence_bundle_preview = (
        _evidence_bundle_preview_from_results(results)
        if results
        else _evidence_bundle_preview_from_disk(out_dir)
    )
    rarity_coverage_preview = (
        _rare_names_table_preview_from_results(results, table_name="rarity_lookup_coverage", max_rows=5)
        if results
        else _rare_names_table_preview_from_disk(out_dir, table_name="rarity_lookup_coverage", max_rows=5)
    )
    rarity_unmatched_first_preview = (
        _rare_names_table_preview_from_results(results, table_name="rarity_unmatched_first_tokens", max_rows=12)
        if results
        else _rare_names_table_preview_from_disk(out_dir, table_name="rarity_unmatched_first_tokens", max_rows=12)
    )
    rarity_unmatched_last_preview = (
        _rare_names_table_preview_from_results(results, table_name="rarity_unmatched_last_tokens", max_rows=12)
        if results
        else _rare_names_table_preview_from_disk(out_dir, table_name="rarity_unmatched_last_tokens", max_rows=12)
    )
    clockface_top_preview = (
        _periodicity_table_preview_from_results(results, table_name="clockface_top_minutes", max_rows=12)
        if results
        else _periodicity_table_preview_from_disk(out_dir, table_name="clockface_top_minutes", max_rows=12)
    )

    rendered = template.render(
        generated_at=datetime.now(timezone.utc).isoformat(),
        detector_summaries=detector_summaries,
        artifact_rows=artifact_rows,
        table_previews=table_previews,
        evidence_bundle_preview=evidence_bundle_preview,
        rarity_coverage_preview=rarity_coverage_preview,
        rarity_unmatched_first_preview=rarity_unmatched_first_preview,
        rarity_unmatched_last_preview=rarity_unmatched_last_preview,
        clockface_top_preview=clockface_top_preview,
        figure_files=sorted(path.name for path in (out_dir / "figures").glob("*")),
    )

    report_path = out_dir / "report.html"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(rendered, encoding="utf-8")
    return report_path
