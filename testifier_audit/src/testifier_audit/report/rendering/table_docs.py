from __future__ import annotations

from typing import Any

from testifier_audit.report.rendering.constants import _COLUMN_DESCRIPTION_OVERRIDES
from testifier_audit.report.rendering.payload.common import _table_key

def _humanize_identifier(value: str) -> str:
    return " ".join(token for token in str(value).strip().replace("-", "_").split("_") if token)


def _default_column_description(column: str) -> str:
    label = _humanize_identifier(column)
    if not label:
        return "Column value from detector output."
    lower = str(column).lower()
    if lower.startswith("n_"):
        return f"Count of {_humanize_identifier(lower[2:])} in this row grouping."
    if lower.endswith("_rate"):
        return f"Proportion metric for {label}, on a 0 to 1 scale."
    if lower.endswith("_ratio"):
        return f"Ratio metric for {label}; compare against section baseline/threshold context."
    if lower.endswith("_wilson_low"):
        base = _humanize_identifier(lower.removesuffix("_wilson_low"))
        return f"Lower Wilson confidence bound for {base}."
    if lower.endswith("_wilson_high"):
        base = _humanize_identifier(lower.removesuffix("_wilson_high"))
        return f"Upper Wilson confidence bound for {base}."
    if lower.startswith("is_"):
        return f"Boolean indicator for {label}."
    if "minute" in lower or "hour" in lower or lower.endswith("_time") or lower.endswith("_date"):
        return f"Time coordinate for {label}."
    return f"Detector output field for {label}."


def _describe_column(column: str) -> str:
    normalized = str(column or "").strip()
    if not normalized:
        return "Column value from detector output."
    return _COLUMN_DESCRIPTION_OVERRIDES.get(normalized, _default_column_description(normalized))


def _table_column_docs_from_rows(rows: list[dict[str, Any]]) -> dict[str, str]:
    if not rows:
        return {}
    ordered_columns: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for column in row.keys():
            key = str(column)
            if key in seen:
                continue
            seen.add(key)
            ordered_columns.append(key)
    return {column: _describe_column(column) for column in ordered_columns}


def _build_table_column_docs(
    table_previews: dict[str, dict[str, list[dict[str, Any]]]],
    artifact_rows: dict[str, int],
) -> dict[str, dict[str, str]]:
    docs: dict[str, dict[str, str]] = {}
    for detector_name, detector_tables in sorted(table_previews.items()):
        for table_name, rows in sorted(detector_tables.items()):
            key = _table_key(detector_name, table_name)
            docs[key] = _table_column_docs_from_rows(rows)

    docs["artifacts.artifact_rows"] = _table_column_docs_from_rows(
        [
            {"artifact": artifact_name, "rows": row_count}
            for artifact_name, row_count in sorted(artifact_rows.items())
        ]
    )

    return docs


def _build_table_help_docs(
    table_column_docs: dict[str, dict[str, str]],
) -> dict[str, dict[str, str]]:
    docs: dict[str, dict[str, str]] = {}
    for table_key, column_docs in sorted(table_column_docs.items()):
        column_names = list(column_docs.keys())
        has_rate = any(
            name.endswith("_rate") or "ratio" in name or "percentile" in name
            for name in column_names
        )
        has_counts = any(
            name.startswith("n_") or name in {"count", "rows", "n"}
            for name in column_names
        )
        has_time = any(
            token in name
            for name in column_names
            for token in ("minute", "hour", "bucket", "date", "time")
        )
        detector_label = _humanize_identifier(table_key.replace(".", " "))
        first_columns = ", ".join(column_names[:6]) if column_names else "no preview columns"

        value_context = []
        if has_rate:
            value_context.append("rate/proportion columns")
        if has_counts:
            value_context.append("volume/count columns")
        if has_time:
            value_context.append("time keys")
        context_text = ", ".join(value_context) if value_context else "detector-specific fields"

        docs[table_key] = {
            "what_is_this": (
                f"This table is a preview of {detector_label}. "
                "It exposes row-level values behind chart aggregates so you can inspect "
                "the exact buckets, categories, and flags that produced a visual signal. "
                "Use it when you need to answer which concrete records created a peak, "
                "dip, or anomaly marker."
            ),
            "why_it_matters": (
                "Tables are essential for auditability: they let you sort, filter, "
                "and verify whether visual anomalies are supported by real volume, "
                "consistent metadata, and non-sparse support. "
                "They also reveal false positives where a chart looks dramatic but "
                "underlying rows are low-power or internally contradictory."
            ),
            "how_to_interpret": (
                "Start with key columns and filter around flagged times/categories. "
                f"This table includes {context_text}. "
                "Read left-to-right from identifiers to volume/rate fields to flags, "
                "and compare adjacent rows to separate isolated outliers from "
                "persistent structure. Use the column glossary to avoid over-"
                "interpreting similarly named fields with different semantics."
            ),
            "what_to_look_for": (
                "Look for rows where multiple indicators move together (for example, "
                "high counts plus directional rates plus flags), and check whether "
                "those rows cluster in adjacent windows. "
                "Strong evidence usually appears as recurring patterns across nearby "
                "rows, not single extreme entries."
            ),
            "momentary_high_low": (
                "A single extreme row can be a genuine event or a sparse-data outlier. "
                "Momentary lows can be normal lulls; validate by checking neighboring "
                "rows and low-power indicators. "
                "Short-lived highs often map to reminders, queue releases, or reporting "
                "timing; short-lived lows often map to expected inactivity or ingest lag."
            ),
            "extended_high_low": (
                "Extended runs of high/low values across many rows are stronger signs "
                "of regime-level behavior. Persistent shifts that also align with "
                "chart-level signals are higher-confidence anomalies. "
                "Extended highs may indicate sustained mobilization or process skew; "
                "extended lows may indicate suppressed activity, missing data segments, "
                "or a stable low-intensity baseline."
            ),
            "column_highlight": (
                f"Primary columns in this preview: {first_columns}."
            ),
        }

    return docs
