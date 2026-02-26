from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from testifier_audit.io.hearing_metadata import HearingMetadata
from testifier_audit.report.rendering.constants import PACIFIC_TIMEZONE_NAME
from testifier_audit.report.rendering.payload.common import _with_expected_columns
from testifier_audit.report.rendering.serialization import _to_pacific_timestamp

def _build_deadline_ramp_metrics(
    counts_per_minute: pd.DataFrame,
    *,
    cutoff_time: datetime,
    min_cell_n_for_rates: int,
) -> dict[str, Any]:
    if counts_per_minute.empty:
        return {
            "status": "unavailable",
            "reason": "Counts-per-minute artifact is empty.",
        }

    working = _with_expected_columns(
        counts_per_minute,
        ["minute_bucket", "n_total", "n_pro", "n_con"],
    ).copy()
    working["minute_bucket"] = pd.to_datetime(working["minute_bucket"], errors="coerce")
    working = working.dropna(subset=["minute_bucket"])
    if working.empty:
        return {
            "status": "unavailable",
            "reason": "No valid minute-bucket timestamps available for deadline ramp metrics.",
        }
    for column in ["n_total", "n_pro", "n_con"]:
        working[column] = pd.to_numeric(working[column], errors="coerce").fillna(0.0)

    cutoff = pd.Timestamp(cutoff_time)
    recent_mask = (working["minute_bucket"] > (cutoff - pd.Timedelta(minutes=60))) & (
        working["minute_bucket"] <= cutoff
    )
    prior_mask = (
        working["minute_bucket"] > (cutoff - pd.Timedelta(minutes=120))
    ) & (working["minute_bucket"] <= (cutoff - pd.Timedelta(minutes=60)))

    recent = working.loc[recent_mask]
    prior = working.loc[prior_mask]
    recent_total = float(recent["n_total"].sum())
    prior_total = float(prior["n_total"].sum())
    recent_pro = float(recent["n_pro"].sum())
    prior_pro = float(prior["n_pro"].sum())
    recent_con = float(recent["n_con"].sum())
    prior_con = float(prior["n_con"].sum())

    recent_pro_rate = recent_pro / recent_total if recent_total > 0 else None
    prior_pro_rate = prior_pro / prior_total if prior_total > 0 else None
    recent_con_rate = recent_con / recent_total if recent_total > 0 else None
    prior_con_rate = prior_con / prior_total if prior_total > 0 else None

    return {
        "status": "ok",
        "window_minutes": 60,
        "recent_window_start": _to_pacific_timestamp(
            cutoff - pd.Timedelta(minutes=60)
        ).isoformat(),
        "recent_window_end": _to_pacific_timestamp(cutoff).isoformat(),
        "prior_window_start": _to_pacific_timestamp(
            cutoff - pd.Timedelta(minutes=120)
        ).isoformat(),
        "prior_window_end": _to_pacific_timestamp(
            cutoff - pd.Timedelta(minutes=60)
        ).isoformat(),
        "recent_n_total": int(round(recent_total)),
        "prior_n_total": int(round(prior_total)),
        "recent_pro_rate": float(recent_pro_rate) if recent_pro_rate is not None else None,
        "prior_pro_rate": float(prior_pro_rate) if prior_pro_rate is not None else None,
        "recent_con_rate": float(recent_con_rate) if recent_con_rate is not None else None,
        "prior_con_rate": float(prior_con_rate) if prior_con_rate is not None else None,
        "recent_is_low_power": bool(recent_total < float(max(1, min_cell_n_for_rates))),
        "prior_is_low_power": bool(prior_total < float(max(1, min_cell_n_for_rates))),
        "ramp_ratio_recent_vs_prior": (
            float(recent_total / prior_total) if prior_total > 0 else None
        ),
        "pro_rate_delta_recent_minus_prior": (
            float(recent_pro_rate - prior_pro_rate)
            if recent_pro_rate is not None and prior_pro_rate is not None
            else None
        ),
    }


def _build_hearing_context_panel(
    counts_per_minute: pd.DataFrame,
    *,
    hearing_metadata: HearingMetadata | None,
    min_cell_n_for_rates: int,
) -> dict[str, Any]:
    if hearing_metadata is None:
        return {
            "status": "unavailable",
            "available": False,
            "reason": "No hearing metadata sidecar provided.",
            "process_markers": [],
            "deadline_ramp_metrics": {
                "status": "unavailable",
                "reason": "No sign_in_cutoff provided in hearing metadata.",
            },
        }

    process_markers = []
    for key, value in hearing_metadata.marker_times().items():
        marker_time = _to_pacific_timestamp(pd.Timestamp(value))
        if pd.isna(marker_time):
            continue
        process_markers.append(
            {
                "key": key,
                "label": key.replace("_", " "),
                "time_iso": marker_time.isoformat(),
            }
        )
    process_markers = sorted(process_markers, key=lambda item: item["time_iso"])
    sidecar_source = dict(hearing_metadata.source or {})
    sidecar_stats = dict(hearing_metadata.stats or {})
    meeting_start_iso = (
        _to_pacific_timestamp(pd.Timestamp(hearing_metadata.meeting_start)).isoformat()
        if hearing_metadata.meeting_start is not None
        else None
    )

    metadata_rows = [
        {
            "field": "hearing_id",
            "value": hearing_metadata.hearing_id,
        },
        {
            "field": "timezone",
            "value": PACIFIC_TIMEZONE_NAME,
        },
        {
            "field": "meeting_start",
            "value": meeting_start_iso,
        },
        {
            "field": "sign_in_open",
            "value": (
                _to_pacific_timestamp(pd.Timestamp(hearing_metadata.sign_in_open)).isoformat()
                if hearing_metadata.sign_in_open is not None
                else None
            ),
        },
        {
            "field": "sign_in_cutoff",
            "value": (
                _to_pacific_timestamp(pd.Timestamp(hearing_metadata.sign_in_cutoff)).isoformat()
                if hearing_metadata.sign_in_cutoff is not None
                else None
            ),
        },
        {
            "field": "written_testimony_deadline",
            "value": (
                _to_pacific_timestamp(
                    pd.Timestamp(hearing_metadata.written_testimony_deadline)
                ).isoformat()
                if hearing_metadata.written_testimony_deadline is not None
                else None
            ),
        },
    ]
    if sidecar_source:
        metadata_rows.extend(
            [
                {
                    "field": "short_bill_id",
                    "value": sidecar_source.get("short_bill_id"),
                },
                {
                    "field": "agenda_item_description",
                    "value": sidecar_source.get("agenda_item_description"),
                },
            ]
        )
    if sidecar_stats:
        metadata_rows.extend(
            [
                {
                    "field": "total_rows",
                    "value": sidecar_stats.get("total_rows"),
                },
                {
                    "field": "total_pro_pct",
                    "value": sidecar_stats.get("total_pro_pct"),
                },
                {
                    "field": "total_con_pct",
                    "value": sidecar_stats.get("total_con_pct"),
                },
            ]
        )

    if hearing_metadata.sign_in_cutoff is None:
        deadline_ramp_metrics = {
            "status": "unavailable",
            "reason": "No sign_in_cutoff provided in hearing metadata.",
        }
    else:
        deadline_ramp_metrics = _build_deadline_ramp_metrics(
            counts_per_minute,
            cutoff_time=hearing_metadata.sign_in_cutoff,
            min_cell_n_for_rates=min_cell_n_for_rates,
        )

    return {
        "status": "ok",
        "available": True,
        "hearing_id": hearing_metadata.hearing_id,
        "timezone": PACIFIC_TIMEZONE_NAME,
        "meeting_start": meeting_start_iso,
        "source_path": hearing_metadata.source_path,
        "source": sidecar_source,
        "stats": sidecar_stats,
        "process_markers": process_markers,
        "metadata_rows": metadata_rows,
        "deadline_ramp_metrics": deadline_ramp_metrics,
    }
