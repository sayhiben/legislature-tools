from __future__ import annotations

from collections import Counter
from typing import Any, Mapping, Sequence

import pandas as pd

from testifier_audit.features.dedup import (
    DEDUP_MODES,
    counts_columns_for_mode,
    ensure_dedup_count_columns,
    normalize_dedup_mode,
)
from testifier_audit.report.contracts import (
    EvidenceKind,
    EvidenceSignal,
    EvidenceTier,
    ExplanationLabel,
    QueueKind,
    TriageEvidenceItem,
    TriageTierThresholds,
)

EVIDENCE_KIND_WEIGHTS: Mapping[EvidenceKind, float] = {
    "stat_fdr": 1.00,
    "calibrated_empirical": 0.90,
    "heuristic": 0.60,
}


def _clamp_unit_interval(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def aggregate_signal_score(
    signals: Sequence[EvidenceSignal],
    kind_weights: Mapping[EvidenceKind, float] | None = None,
) -> float:
    if not signals:
        return 0.0

    weights = kind_weights or EVIDENCE_KIND_WEIGHTS
    weighted_sum = 0.0
    total_weight = 0.0

    for signal in signals:
        weight = max(0.0, float(weights.get(signal.evidence_kind, 0.0)))
        if weight <= 0.0:
            continue
        weighted_sum += signal.signal_score * weight
        total_weight += weight

    if total_weight <= 0.0:
        return _clamp_unit_interval(
            sum(float(signal.signal_score) for signal in signals) / float(len(signals))
        )
    return _clamp_unit_interval(weighted_sum / total_weight)


def merge_caveat_flags(
    signals: Sequence[EvidenceSignal],
    min_support_n: int,
) -> tuple[str, ...]:
    flags: set[str] = set()
    if not signals:
        return ()

    flags.update(
        token
        for signal in signals
        for token in signal.caveat_flags
        if isinstance(token, str) and token.strip()
    )
    if any(signal.support_n < min_support_n for signal in signals):
        flags.add("low_support_n")
    if any(signal.is_low_power for signal in signals):
        flags.add("low_power")
    if all(signal.evidence_kind == "heuristic" for signal in signals):
        flags.add("heuristic_only")
    if any(
        signal.evidence_kind in {"stat_fdr", "calibrated_empirical"} and signal.effect_size is None
        for signal in signals
    ):
        flags.add("missing_effect_size")
    if any(
        signal.evidence_kind == "stat_fdr" and signal.p_value is None and signal.q_value is None
        for signal in signals
    ):
        flags.add("missing_significance")
    return tuple(sorted(flags))


def choose_explanations(
    signals: Sequence[EvidenceSignal],
) -> tuple[ExplanationLabel, ExplanationLabel]:
    hints = [
        signal.explanation_hint
        for signal in signals
        if signal.explanation_hint and signal.explanation_hint != "none"
    ]
    if not hints:
        return "insufficient_evidence", "none"

    counts = Counter(hints)
    ranked = counts.most_common()
    primary, primary_count = ranked[0]

    if len(ranked) == 1:
        return primary, "none"

    secondary = ranked[1][0]
    if primary_count == ranked[1][1] or (primary_count / float(len(hints))) < 0.60:
        return "mixed", secondary
    return primary, secondary


def compute_evidence_tier(
    score: float,
    support_n: int,
    evidence_kinds: Sequence[EvidenceKind],
    is_low_power: bool,
    thresholds: TriageTierThresholds,
) -> EvidenceTier:
    has_calibrated = any(kind in {"stat_fdr", "calibrated_empirical"} for kind in evidence_kinds)

    tier: EvidenceTier = "watch"
    if score >= thresholds.high and support_n >= thresholds.min_support_n and has_calibrated:
        tier = "high"
    elif score >= thresholds.medium and support_n >= thresholds.min_support_n:
        tier = "medium"

    if is_low_power and tier == "high":
        return "medium"
    return tier


def build_evidence_item(
    queue_id: str,
    queue_kind: QueueKind,
    contributors: Sequence[EvidenceSignal],
    *,
    score: float | None = None,
    support_n: int | None = None,
    thresholds: TriageTierThresholds | None = None,
) -> TriageEvidenceItem:
    if not contributors:
        raise ValueError("contributors must be non-empty.")

    resolved_thresholds = thresholds or TriageTierThresholds()
    resolved_score = _clamp_unit_interval(
        score if score is not None else aggregate_signal_score(contributors)
    )
    resolved_support_n = int(
        support_n
        if support_n is not None
        else max(int(signal.support_n) for signal in contributors)
    )
    is_low_power = any(bool(signal.is_low_power) for signal in contributors)
    caveat_flags = merge_caveat_flags(contributors, min_support_n=resolved_thresholds.min_support_n)
    primary_explanation, secondary_explanation = choose_explanations(contributors)
    evidence_tier = compute_evidence_tier(
        score=resolved_score,
        support_n=resolved_support_n,
        evidence_kinds=[signal.evidence_kind for signal in contributors],
        is_low_power=is_low_power,
        thresholds=resolved_thresholds,
    )

    return TriageEvidenceItem(
        queue_id=queue_id,
        queue_kind=queue_kind,
        score=resolved_score,
        support_n=resolved_support_n,
        contributors=tuple(contributors),
        evidence_tier=evidence_tier,
        primary_explanation=primary_explanation,
        secondary_explanation=secondary_explanation,
        caveat_flags=caveat_flags,
        is_low_power=is_low_power,
    )


def _table(table_map: Mapping[str, pd.DataFrame], key: str) -> pd.DataFrame:
    frame = table_map.get(key)
    if isinstance(frame, pd.DataFrame):
        return frame.copy()
    return pd.DataFrame()


def _with_columns(frame: pd.DataFrame, expected: Sequence[str]) -> pd.DataFrame:
    working = frame.copy()
    for column in expected:
        if column not in working.columns:
            working[column] = pd.NA
    return working


def _to_timestamp(value: Any) -> pd.Timestamp | None:
    if value is None or value is pd.NA:
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return pd.Timestamp(ts)


def _to_float(value: Any) -> float | None:
    if value is None or value is pd.NA:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not pd.notna(parsed):  # handles NaN/inf/-inf
        return None
    if parsed in {float("inf"), float("-inf")}:
        return None
    return parsed


def _iso_or_none(value: pd.Timestamp | None) -> str | None:
    if value is None:
        return None
    return pd.Timestamp(value).isoformat()


def _score_breakdown_strings(
    contributors: Sequence[EvidenceSignal],
    *,
    top_n_detectors: int = 5,
    top_n_signals: int = 5,
) -> tuple[str, str, str]:
    if not contributors:
        return ("none", "", "")

    weighted_signals: list[tuple[EvidenceSignal, float]] = []
    for signal in contributors:
        kind_weight = max(0.0, float(EVIDENCE_KIND_WEIGHTS.get(signal.evidence_kind, 0.0)))
        effective_weight = kind_weight if kind_weight > 0.0 else 1.0
        weighted_signals.append((signal, float(signal.signal_score) * effective_weight))

    total = float(sum(value for _signal, value in weighted_signals))
    if total <= 0.0:
        weighted_signals = [(signal, 1.0) for signal, _value in weighted_signals]
        total = float(len(weighted_signals))

    by_detector: dict[str, float] = {}
    for signal, contribution in weighted_signals:
        detector = str(signal.detector or "unknown")
        by_detector[detector] = by_detector.get(detector, 0.0) + float(contribution)

    ranked_detectors = sorted(by_detector.items(), key=lambda item: (-item[1], item[0]))
    detector_breakdown = "; ".join(
        f"{detector} ({(value / total) * 100.0:.1f}%)"
        for detector, value in ranked_detectors[:top_n_detectors]
    )

    ranked_signals = sorted(
        weighted_signals,
        key=lambda item: (-item[1], str(item[0].signal_id)),
    )
    signal_breakdown = "; ".join(
        (
            f"{signal.signal_id}:{signal.evidence_kind}"
            f" ({(contribution / total) * 100.0:.1f}%)"
        )
        for signal, contribution in ranked_signals[:top_n_signals]
    )

    primary_driver = ranked_detectors[0][0] if ranked_detectors else "unknown"
    return primary_driver, detector_breakdown, signal_breakdown


def _empty_summary() -> dict[str, Any]:
    return {
        "total_submissions": 0,
        "date_range_start": None,
        "date_range_end": None,
        "overall_pro_rate": None,
        "overall_con_rate": None,
        "top_burst_windows": [],
        "top_swing_windows": [],
        "top_repeated_names": [],
        "off_hours_summary": {},
        "queue_counts": {"window": 0, "record": 0, "cluster": 0},
        "window_tier_counts": {"high": 0, "medium": 0, "watch": 0},
    }


def _delta(value: Any, baseline: Any) -> float | None:
    left = _to_float(value)
    right = _to_float(baseline)
    if left is None or right is None:
        return None
    return left - right


def _relative_delta(value: Any, baseline: Any) -> float | None:
    left = _to_float(value)
    right = _to_float(baseline)
    if left is None or right is None or right == 0:
        return None
    return (left - right) / right


def _merge_side_by_side_views(
    raw_view: Mapping[str, Any],
    dedup_view: Mapping[str, Any],
) -> dict[str, Any]:
    raw_summary = dict(raw_view.get("triage_summary", {}))
    dedup_summary = dict(dedup_view.get("triage_summary", {}))

    side_summary = dict(raw_summary)
    side_summary["lens"] = "side_by_side"
    side_summary["total_submissions_raw"] = raw_summary.get("total_submissions")
    side_summary["total_submissions_exact_row_dedup"] = dedup_summary.get("total_submissions")
    side_summary["total_submissions_delta"] = _delta(
        dedup_summary.get("total_submissions"),
        raw_summary.get("total_submissions"),
    )
    side_summary["total_submissions_relative_delta"] = _relative_delta(
        dedup_summary.get("total_submissions"),
        raw_summary.get("total_submissions"),
    )
    side_summary["overall_pro_rate_raw"] = raw_summary.get("overall_pro_rate")
    side_summary["overall_pro_rate_exact_row_dedup"] = dedup_summary.get("overall_pro_rate")
    side_summary["overall_pro_rate_delta"] = _delta(
        dedup_summary.get("overall_pro_rate"),
        raw_summary.get("overall_pro_rate"),
    )
    side_summary["overall_con_rate_raw"] = raw_summary.get("overall_con_rate")
    side_summary["overall_con_rate_exact_row_dedup"] = dedup_summary.get("overall_con_rate")
    side_summary["overall_con_rate_delta"] = _delta(
        dedup_summary.get("overall_con_rate"),
        raw_summary.get("overall_con_rate"),
    )

    return {"triage_summary": side_summary}


def build_investigation_view(
    table_map: Mapping[str, pd.DataFrame],
    *,
    thresholds: TriageTierThresholds | None = None,
    top_n_windows: int = 250,
    top_n_records: int = 250,
    top_n_clusters: int = 250,
    dedup_mode: str = "raw",
) -> dict[str, Any]:
    """Build Phase 2 investigation-first triage contracts from detector tables."""
    resolved_mode = normalize_dedup_mode(dedup_mode, default="raw")
    if resolved_mode == "side_by_side":
        raw_view = build_investigation_view(
            table_map=table_map,
            thresholds=thresholds,
            top_n_windows=top_n_windows,
            top_n_records=top_n_records,
            top_n_clusters=top_n_clusters,
            dedup_mode="raw",
        )
        dedup_view = build_investigation_view(
            table_map=table_map,
            thresholds=thresholds,
            top_n_windows=top_n_windows,
            top_n_records=top_n_records,
            top_n_clusters=top_n_clusters,
            dedup_mode="exact_row_dedup",
        )
        return _merge_side_by_side_views(raw_view=raw_view, dedup_view=dedup_view)

    counts = _with_columns(
        _table(table_map, "artifacts.counts_per_minute"),
        [
            "minute_bucket",
            "n_total",
            "n_pro",
            "n_con",
            "dup_name_fraction",
            "n_total_dedup",
            "n_pro_dedup",
            "n_con_dedup",
            "dup_name_fraction_dedup",
            "pro_rate",
            "pro_rate_dedup",
        ],
    )
    counts = ensure_dedup_count_columns(counts)
    count_columns = counts_columns_for_mode(resolved_mode)
    bursts = _with_columns(
        _table(table_map, "bursts.burst_significant_windows"),
        [
            "start_minute",
            "end_minute",
            "observed_count",
            "expected_count",
            "rate_ratio",
            "p_value",
            "q_value",
            "window_minutes",
        ],
    )
    dup_exact_top = _with_columns(
        _table(table_map, "duplicates_exact.top_repeated_names"),
        ["display_name", "canonical_name", "n", "n_pro", "n_con", "time_span_minutes"],
    )
    dup_exact_anomalies = _with_columns(
        _table(table_map, "duplicates_exact.per_name_anomalies"),
        [
            "display_name",
            "canonical_name",
            "n",
            "n_pro",
            "n_con",
            "time_span_minutes",
            "p_value",
            "q_value",
            "is_significant",
            "inference_status",
        ],
    )
    off_hours_summary = _with_columns(
        _table(table_map, "off_hours.off_hours_summary"),
        [
            "off_hours",
            "on_hours",
            "off_hours_ratio",
            "off_hours_pro_rate",
            "on_hours_pro_rate",
            "chi_square_p_value",
        ],
    )

    counts["minute_bucket"] = pd.to_datetime(counts["minute_bucket"], errors="coerce")
    counts = (
        counts.dropna(subset=["minute_bucket"]).sort_values("minute_bucket").reset_index(drop=True)
    )

    bursts["start_minute"] = pd.to_datetime(bursts["start_minute"], errors="coerce")
    bursts["end_minute"] = pd.to_datetime(bursts["end_minute"], errors="coerce")
    bursts = bursts.dropna(subset=["start_minute", "end_minute"]).reset_index(drop=True)

    summary = _empty_summary()
    if not counts.empty:
        n_total = float(
            pd.to_numeric(counts[count_columns["n_total"]], errors="coerce").fillna(0.0).sum()
        )
        n_pro = float(
            pd.to_numeric(counts[count_columns["n_pro"]], errors="coerce").fillna(0.0).sum()
        )
        n_con = float(
            pd.to_numeric(counts[count_columns["n_con"]], errors="coerce").fillna(0.0).sum()
        )
        summary["total_submissions"] = int(n_total)
        summary["date_range_start"] = _iso_or_none(_to_timestamp(counts["minute_bucket"].min()))
        summary["date_range_end"] = _iso_or_none(_to_timestamp(counts["minute_bucket"].max()))
        summary["overall_pro_rate"] = (n_pro / n_total) if n_total > 0 else None
        summary["overall_con_rate"] = (n_con / n_total) if n_total > 0 else None

    if not bursts.empty:
        burst_preview = bursts.copy()
        for column in ("q_value", "rate_ratio", "observed_count"):
            burst_preview[column] = pd.to_numeric(burst_preview[column], errors="coerce")
        burst_preview = burst_preview.sort_values(
            by=["q_value", "rate_ratio", "observed_count"],
            ascending=[True, False, False],
            na_position="last",
        ).head(5)
        summary["top_burst_windows"] = [
            {
                "start_time": _iso_or_none(_to_timestamp(row.start_minute)),
                "end_time": _iso_or_none(_to_timestamp(row.end_minute)),
                "observed_count": _to_float(row.observed_count),
                "expected_count": _to_float(row.expected_count),
                "rate_ratio": _to_float(row.rate_ratio),
                "q_value": _to_float(row.q_value),
            }
            for row in burst_preview.itertuples(index=False)
        ]

    top_repeated_source = dup_exact_top if not dup_exact_top.empty else dup_exact_anomalies
    summary["top_repeated_names"] = [
        {
            "display_name": str(row.get("display_name") or ""),
            "canonical_name": str(row.get("canonical_name") or ""),
            "n_records": int(_to_float(row.get("n")) or 0),
            "n_pro": int(_to_float(row.get("n_pro")) or 0),
            "n_con": int(_to_float(row.get("n_con")) or 0),
        }
        for row in top_repeated_source.head(5).to_dict(orient="records")
    ]
    if not off_hours_summary.empty:
        summary["off_hours_summary"] = {
            key: _to_float(value) if not isinstance(value, bool) else bool(value)
            for key, value in off_hours_summary.iloc[0].to_dict().items()
        }

    summary["queue_counts"] = {"window": 0, "record": 0, "cluster": 0}
    summary["window_tier_counts"] = {"high": 0, "medium": 0, "watch": 0}
    summary["lens"] = resolved_mode

    return {"triage_summary": summary}


def build_investigation_views(
    table_map: Mapping[str, pd.DataFrame],
    *,
    thresholds: TriageTierThresholds | None = None,
    top_n_windows: int = 250,
    top_n_records: int = 250,
    top_n_clusters: int = 250,
) -> dict[str, dict[str, Any]]:
    views: dict[str, dict[str, Any]] = {}
    for mode in DEDUP_MODES:
        views[mode] = build_investigation_view(
            table_map=table_map,
            thresholds=thresholds,
            top_n_windows=top_n_windows,
            top_n_records=top_n_records,
            top_n_clusters=top_n_clusters,
            dedup_mode=mode,
        )
    return views
