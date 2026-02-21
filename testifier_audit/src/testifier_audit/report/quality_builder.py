from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

from testifier_audit.features.dedup import ensure_dedup_count_columns


def _table(table_map: Mapping[str, pd.DataFrame], key: str) -> pd.DataFrame:
    frame = table_map.get(key)
    if isinstance(frame, pd.DataFrame):
        return frame.copy()
    return pd.DataFrame()


def _to_float(value: Any) -> float | None:
    if value is None or value is pd.NA:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not pd.notna(parsed):
        return None
    if parsed in {float("inf"), float("-inf")}:
        return None
    return parsed


def _metric_value(basic_quality: pd.DataFrame, metric: str) -> float:
    if basic_quality.empty or "metric" not in basic_quality.columns:
        return 0.0
    mask = basic_quality["metric"].astype(str) == metric
    if not mask.any():
        return 0.0
    value = _to_float(basic_quality.loc[mask, "value"].iloc[0])
    return float(value or 0.0)


def _weighted_mean(values: pd.Series, weights: pd.Series) -> float | None:
    numeric_values = pd.to_numeric(values, errors="coerce")
    numeric_weights = pd.to_numeric(weights, errors="coerce").fillna(0.0)
    valid = numeric_values.notna() & (numeric_weights > 0)
    if not valid.any():
        return None
    denominator = float(numeric_weights[valid].sum())
    if denominator <= 0:
        return None
    return float((numeric_values[valid] * numeric_weights[valid]).sum() / denominator)


def _build_delta_metric_row(
    metric: str,
    label: str,
    raw_value: float | None,
    dedup_value: float | None,
    *,
    kind: str,
) -> dict[str, Any]:
    absolute_delta = (
        (dedup_value - raw_value)
        if raw_value is not None and dedup_value is not None
        else None
    )
    relative_delta = (
        (absolute_delta / raw_value)
        if absolute_delta is not None and raw_value not in {None, 0.0}
        else None
    )

    material_change = False
    if absolute_delta is not None:
        if kind == "count":
            material_change = abs(absolute_delta) >= 5 and abs(relative_delta or 0.0) >= 0.10
        elif kind == "rate":
            material_change = abs(absolute_delta) >= 0.03
        else:
            material_change = abs(absolute_delta) >= 0.05

    return {
        "metric": metric,
        "label": label,
        "raw_value": raw_value,
        "exact_row_dedup_value": dedup_value,
        "absolute_delta": absolute_delta,
        "relative_delta": relative_delta,
        "material_change": bool(material_change),
    }


def build_raw_vs_dedup_metrics(
    table_map: Mapping[str, pd.DataFrame],
    *,
    triage_views: Mapping[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    counts = ensure_dedup_count_columns(_table(table_map, "artifacts.counts_per_minute"))
    if counts.empty:
        return []

    def numeric_series(column: str, default: float = 0.0) -> pd.Series:
        if column in counts.columns:
            raw = counts[column]
        else:
            raw = pd.Series(default, index=counts.index, dtype=float)
        parsed = pd.to_numeric(raw, errors="coerce")
        if isinstance(parsed, pd.Series):
            return parsed.fillna(default)
        return pd.Series(parsed, index=counts.index, dtype=float).fillna(default)

    raw_total_series = numeric_series("n_total")
    dedup_total_series = numeric_series("n_total_dedup")
    raw_pro_series = numeric_series("n_pro")
    raw_con_series = numeric_series("n_con")
    dedup_pro_series = numeric_series("n_pro_dedup")
    dedup_con_series = numeric_series("n_con_dedup")
    raw_dup_fraction_series = numeric_series("dup_name_fraction")
    dedup_dup_fraction_series = numeric_series("dup_name_fraction_dedup")

    raw_total = _to_float(raw_total_series.sum())
    dedup_total = _to_float(dedup_total_series.sum())
    raw_pro = _to_float(raw_pro_series.sum())
    raw_con = _to_float(raw_con_series.sum())
    dedup_pro = _to_float(dedup_pro_series.sum())
    dedup_con = _to_float(dedup_con_series.sum())

    raw_pro_rate = (raw_pro / raw_total) if raw_total and raw_total > 0 else None
    raw_con_rate = (raw_con / raw_total) if raw_total and raw_total > 0 else None
    dedup_pro_rate = (dedup_pro / dedup_total) if dedup_total and dedup_total > 0 else None
    dedup_con_rate = (dedup_con / dedup_total) if dedup_total and dedup_total > 0 else None

    raw_dup_fraction = _weighted_mean(raw_dup_fraction_series, raw_total_series)
    dedup_dup_fraction = _weighted_mean(
        dedup_dup_fraction_series,
        dedup_total_series,
    )

    rows = [
        _build_delta_metric_row(
            "total_submissions",
            "Total submissions",
            raw_total,
            dedup_total,
            kind="count",
        ),
        _build_delta_metric_row(
            "overall_pro_rate",
            "Overall pro rate",
            raw_pro_rate,
            dedup_pro_rate,
            kind="rate",
        ),
        _build_delta_metric_row(
            "overall_con_rate",
            "Overall con rate",
            raw_con_rate,
            dedup_con_rate,
            kind="rate",
        ),
        _build_delta_metric_row(
            "mean_duplicate_fraction",
            "Mean duplicate fraction",
            raw_dup_fraction,
            dedup_dup_fraction,
            kind="ratio",
        ),
    ]

    if triage_views:
        raw_view = triage_views.get("raw", {})
        dedup_view = triage_views.get("exact_row_dedup", {})
        raw_queue = raw_view.get("window_evidence_queue", [])
        dedup_queue = dedup_view.get("window_evidence_queue", [])
        raw_high = float(
            sum(1 for row in raw_queue if str((row or {}).get("evidence_tier")) == "high")
        )
        dedup_high = float(
            sum(1 for row in dedup_queue if str((row or {}).get("evidence_tier")) == "high")
        )
        rows.append(
            _build_delta_metric_row(
                "high_tier_windows",
                "High-tier windows",
                raw_high,
                dedup_high,
                kind="count",
            )
        )

    return rows


def build_data_quality_panel(
    table_map: Mapping[str, pd.DataFrame],
    *,
    triage_views: Mapping[str, dict[str, Any]] | None = None,
    min_cell_n_for_rates: int = 25,
) -> dict[str, Any]:
    counts = ensure_dedup_count_columns(_table(table_map, "artifacts.counts_per_minute"))
    basic_quality = _table(table_map, "artifacts.basic_quality")
    org_blank = _table(table_map, "org_anomalies.organization_blank_rate_by_bucket")

    total_rows = _to_float(
        pd.to_numeric(
            counts.get("n_total", pd.Series(dtype=float)),
            errors="coerce",
        ).sum()
    )
    if total_rows is None or total_rows <= 0:
        total_rows = _metric_value(basic_quality, "rows_total")

    warnings: list[dict[str, Any]] = []

    unknown_position = _metric_value(basic_quality, "unknown_position")
    if unknown_position <= 0 and not counts.empty and "n_unknown" in counts.columns:
        unknown_position = float(
            pd.to_numeric(counts["n_unknown"], errors="coerce").fillna(0.0).sum()
        )
    if unknown_position > 0:
        unknown_fraction = (unknown_position / total_rows) if total_rows > 0 else None
        warnings.append(
            {
                "code": "invalid_or_missing_positions",
                "severity": "high" if (unknown_fraction or 0.0) >= 0.10 else "medium",
                "affected_rows": int(unknown_position),
                "affected_fraction": unknown_fraction,
                "summary": (
                    "Rows with missing/unknown testimony position can skew "
                    "stance-rate interpretation."
                ),
            }
        )

    missing_name = _metric_value(basic_quality, "missing_name")
    if missing_name > 0:
        missing_fraction = (missing_name / total_rows) if total_rows > 0 else None
        warnings.append(
            {
                "code": "unparsable_or_missing_names",
                "severity": "high" if (missing_fraction or 0.0) >= 0.05 else "medium",
                "affected_rows": int(missing_name),
                "affected_fraction": missing_fraction,
                "summary": (
                    "Rows with missing/unparsable names reduce confidence in duplicate "
                    "and rarity analyses."
                ),
            }
        )

    duplicate_ids = _metric_value(basic_quality, "duplicate_ids")
    if duplicate_ids > 0:
        duplicate_fraction = (duplicate_ids / total_rows) if total_rows > 0 else None
        warnings.append(
            {
                "code": "duplicate_ids",
                "severity": "high" if (duplicate_fraction or 0.0) >= 0.01 else "medium",
                "affected_rows": int(duplicate_ids),
                "affected_fraction": duplicate_fraction,
                "summary": (
                    "Duplicate submission IDs detected; row identity may include import "
                    "or export artifacts."
                ),
            }
        )

    non_monotonic = _metric_value(basic_quality, "non_monotonic_timestamp_vs_id")
    if non_monotonic > 0:
        warnings.append(
            {
                "code": "non_monotonic_timestamps_vs_id",
                "severity": "medium",
                "affected_rows": int(non_monotonic),
                "affected_fraction": None,
                "summary": (
                    "ID order and timestamp order diverge; ingest/export ordering "
                    "assumptions may be unsafe."
                ),
            }
        )

    if (
        not org_blank.empty
        and "blank_org_rate" in org_blank.columns
        and "n_total" in org_blank.columns
    ):
        rate = pd.to_numeric(org_blank["blank_org_rate"], errors="coerce")
        support = pd.to_numeric(org_blank["n_total"], errors="coerce").fillna(0.0)
        valid = rate.notna() & (support >= max(1, int(min_cell_n_for_rates)))
        if valid.any():
            median_rate = float(rate[valid].median())
            threshold = max(0.35, median_rate + 0.15)
            spikes = valid & (rate >= threshold)
            if spikes.any():
                warnings.append(
                    {
                        "code": "time_varying_missingness_spikes",
                        "severity": "medium",
                        "affected_rows": int(spikes.sum()),
                        "affected_fraction": float(spikes.sum()) / float(valid.sum()),
                        "summary": (
                            "Organization-missingness spikes over time (rate >= "
                            f"{threshold:.2f}) can indicate selective data quality drift."
                        ),
                    }
                )

    raw_vs_dedup_metrics = build_raw_vs_dedup_metrics(table_map, triage_views=triage_views)
    material_metric_count = int(
        sum(1 for row in raw_vs_dedup_metrics if row.get("material_change"))
    )
    triage_metrics = [row for row in raw_vs_dedup_metrics if row.get("material_change")]
    if not triage_metrics:
        triage_metrics = raw_vs_dedup_metrics[:1]

    if warnings:
        status = "warning"
        summary = (
            f"{len(warnings)} high-value data-quality warning(s) detected; "
            "review before strong attribution."
        )
    else:
        status = "ok"
        summary = "No high-value data-quality warnings were detected in this run."

    return {
        "status": status,
        "summary": summary,
        "warnings": warnings,
        "warning_count": int(len(warnings)),
        "raw_vs_dedup_metrics": raw_vs_dedup_metrics,
        "triage_raw_vs_dedup_metrics": triage_metrics,
        "material_metric_count": material_metric_count,
        "lens_note": (
            "Exact-row dedup lens collapses repeated canonical-name submissions within "
            "each minute bucket."
        ),
    }
