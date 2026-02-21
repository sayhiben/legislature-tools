from __future__ import annotations

from collections import Counter
from typing import Any, Mapping, Sequence

import pandas as pd

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


WINDOW_QUEUE_REQUIRED_COLUMNS: tuple[str, ...] = (
    "window_id",
    "start_time",
    "end_time",
    "count",
    "expected",
    "z",
    "q_value",
    "pro_rate",
    "delta_pro_rate",
    "dup_fraction",
    "near_dup_fraction",
    "name_weirdness_mean",
    "support_n",
    "evidence_tier",
    "primary_explanation",
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


def _safe_window_id(start_time: pd.Timestamp, end_time: pd.Timestamp) -> str:
    start_label = start_time.strftime("%Y%m%dT%H%M%S")
    end_label = end_time.strftime("%Y%m%dT%H%M%S")
    return f"w_{start_label}_{end_label}"


def _tier_rank(tier: str) -> int:
    if tier == "high":
        return 0
    if tier == "medium":
        return 1
    return 2


def _weighted_mean(
    values: pd.Series,
    weights: pd.Series | None = None,
) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce")
    if numeric.dropna().empty:
        return None
    if weights is None:
        return float(numeric.mean())

    weight_values = pd.to_numeric(weights, errors="coerce").fillna(0.0)
    valid = numeric.notna() & (weight_values > 0)
    if not valid.any():
        return float(numeric.mean())

    numerator = float((numeric[valid] * weight_values[valid]).sum())
    denominator = float(weight_values[valid].sum())
    if denominator <= 0.0:
        return float(numeric[valid].mean())
    return numerator / denominator


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


def _window_overlap_mask(
    frame: pd.DataFrame,
    *,
    start_column: str,
    end_column: str,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
) -> pd.Series:
    starts = pd.to_datetime(frame[start_column], errors="coerce")
    ends = pd.to_datetime(frame[end_column], errors="coerce")
    return starts.le(end_time) & ends.ge(start_time)


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
        "top_near_dup_clusters": [],
        "off_hours_summary": {},
        "queue_counts": {"window": 0, "record": 0, "cluster": 0},
        "window_tier_counts": {"high": 0, "medium": 0, "watch": 0},
    }


def build_investigation_view(
    table_map: Mapping[str, pd.DataFrame],
    *,
    thresholds: TriageTierThresholds | None = None,
    top_n_windows: int = 250,
    top_n_records: int = 250,
    top_n_clusters: int = 250,
) -> dict[str, Any]:
    """Build Phase 2 investigation-first triage contracts from detector tables."""
    resolved_thresholds = thresholds or TriageTierThresholds()

    counts = _with_columns(
        _table(table_map, "artifacts.counts_per_minute"),
        ["minute_bucket", "n_total", "n_pro", "n_con", "dup_name_fraction", "pro_rate"],
    )
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
    swings = _with_columns(
        _table(table_map, "procon_swings.swing_significant_windows"),
        [
            "start_minute",
            "end_minute",
            "n_total",
            "pro_rate",
            "delta_pro_rate",
            "abs_delta_pro_rate",
            "z_score",
            "p_value",
            "q_value",
            "window_minutes",
            "is_low_power",
        ],
    )
    composite_high = _with_columns(
        _table(table_map, "composite_score.high_priority_windows"),
        ["minute_bucket", "n_total", "composite_score", "pro_rate"],
    )
    dup_exact_top = _with_columns(
        _table(table_map, "duplicates_exact.top_repeated_names"),
        ["display_name", "canonical_name", "n", "n_pro", "n_con", "time_span_minutes"],
    )
    dup_near_clusters = _with_columns(
        _table(table_map, "duplicates_near.cluster_summary"),
        [
            "cluster_id",
            "cluster_size",
            "n_records",
            "n_pro",
            "n_con",
            "first_seen",
            "last_seen",
            "time_span_minutes",
        ],
    )
    rarity_by_minute = _with_columns(
        _table(table_map, "rare_names.rarity_by_minute"),
        ["minute_bucket", "rarity_mean", "n_total", "n_records"],
    )
    weird_names = _with_columns(
        _table(table_map, "rare_names.weird_names"),
        ["canonical_name", "sample_name", "weirdness_score"],
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

    swings["start_minute"] = pd.to_datetime(swings["start_minute"], errors="coerce")
    swings["end_minute"] = pd.to_datetime(swings["end_minute"], errors="coerce")
    swings = swings.dropna(subset=["start_minute", "end_minute"]).reset_index(drop=True)

    composite_high["minute_bucket"] = pd.to_datetime(
        composite_high["minute_bucket"], errors="coerce"
    )
    composite_high = composite_high.dropna(subset=["minute_bucket"]).reset_index(drop=True)

    rarity_by_minute["minute_bucket"] = pd.to_datetime(
        rarity_by_minute["minute_bucket"], errors="coerce"
    )
    rarity_by_minute = rarity_by_minute.dropna(subset=["minute_bucket"]).reset_index(drop=True)

    dup_near_clusters["first_seen"] = pd.to_datetime(
        dup_near_clusters["first_seen"], errors="coerce"
    )
    dup_near_clusters["last_seen"] = pd.to_datetime(dup_near_clusters["last_seen"], errors="coerce")
    dup_near_clusters = dup_near_clusters.dropna(subset=["first_seen", "last_seen"]).reset_index(
        drop=True
    )

    rarity_scale = _to_float(
        pd.to_numeric(
            rarity_by_minute.get("rarity_mean", pd.Series(dtype=float)), errors="coerce"
        ).quantile(0.95)
    )
    if rarity_scale is None or rarity_scale <= 0:
        rarity_scale = 1.0

    window_candidates: dict[str, dict[str, Any]] = {}

    def ensure_window(start_time: pd.Timestamp, end_time: pd.Timestamp) -> dict[str, Any]:
        key = f"{start_time.isoformat()}__{end_time.isoformat()}"
        candidate = window_candidates.get(key)
        if candidate is None:
            candidate = {
                "window_id": _safe_window_id(start_time=start_time, end_time=end_time),
                "start_time": start_time,
                "end_time": end_time,
                "count": None,
                "expected": None,
                "z": None,
                "q_value": None,
                "pro_rate": None,
                "delta_pro_rate": None,
                "dup_fraction": None,
                "near_dup_fraction": None,
                "name_weirdness_mean": None,
                "support_n": 0,
                "contributors": [],
            }
            window_candidates[key] = candidate
        return candidate

    for row in bursts.itertuples(index=False):
        start_time = _to_timestamp(getattr(row, "start_minute", None))
        end_time = _to_timestamp(getattr(row, "end_minute", None))
        if start_time is None or end_time is None:
            continue
        candidate = ensure_window(start_time, end_time)

        observed = _to_float(getattr(row, "observed_count", None))
        expected = _to_float(getattr(row, "expected_count", None))
        q_value = _to_float(getattr(row, "q_value", None))
        p_value = _to_float(getattr(row, "p_value", None))
        rate_ratio = _to_float(getattr(row, "rate_ratio", None))
        support_n = int(observed) if observed is not None else 0

        if expected is not None and (
            candidate["expected"] is None or expected > float(candidate["expected"])
        ):
            candidate["expected"] = expected
        if q_value is not None:
            if candidate["q_value"] is None:
                candidate["q_value"] = q_value
            else:
                candidate["q_value"] = min(float(candidate["q_value"]), q_value)

        burst_components = []
        if q_value is not None:
            burst_components.append(1.0 - max(0.0, min(1.0, q_value)))
        if rate_ratio is not None:
            burst_components.append(_clamp_unit_interval((rate_ratio - 1.0) / 3.0))
        burst_score = _clamp_unit_interval(
            sum(burst_components) / float(len(burst_components)) if burst_components else 0.0
        )

        candidate["contributors"].append(
            EvidenceSignal(
                signal_id=f"burst:{candidate['window_id']}",
                detector="bursts",
                evidence_kind="stat_fdr",
                signal_score=burst_score,
                support_n=max(0, support_n),
                effect_size=(rate_ratio - 1.0) if rate_ratio is not None else None,
                p_value=p_value,
                q_value=q_value,
                is_low_power=support_n < resolved_thresholds.min_support_n,
                explanation_hint="potential_manipulation",
            )
        )

    for row in swings.itertuples(index=False):
        start_time = _to_timestamp(getattr(row, "start_minute", None))
        end_time = _to_timestamp(getattr(row, "end_minute", None))
        if start_time is None or end_time is None:
            continue
        candidate = ensure_window(start_time, end_time)

        q_value = _to_float(getattr(row, "q_value", None))
        p_value = _to_float(getattr(row, "p_value", None))
        delta_pro = _to_float(getattr(row, "delta_pro_rate", None))
        abs_delta = _to_float(getattr(row, "abs_delta_pro_rate", None))
        z_score = _to_float(getattr(row, "z_score", None))
        pro_rate = _to_float(getattr(row, "pro_rate", None))
        support_n = int(_to_float(getattr(row, "n_total", None)) or 0)
        is_low_power = bool(getattr(row, "is_low_power", False))

        if candidate["z"] is None and z_score is not None:
            candidate["z"] = z_score
        if candidate["delta_pro_rate"] is None and delta_pro is not None:
            candidate["delta_pro_rate"] = delta_pro
        if candidate["pro_rate"] is None and pro_rate is not None:
            candidate["pro_rate"] = pro_rate
        if q_value is not None:
            if candidate["q_value"] is None:
                candidate["q_value"] = q_value
            else:
                candidate["q_value"] = min(float(candidate["q_value"]), q_value)

        swing_components = []
        if q_value is not None:
            swing_components.append(1.0 - max(0.0, min(1.0, q_value)))
        if abs_delta is not None:
            swing_components.append(_clamp_unit_interval(abs_delta / 0.35))
        swing_score = _clamp_unit_interval(
            sum(swing_components) / float(len(swing_components)) if swing_components else 0.0
        )

        candidate["contributors"].append(
            EvidenceSignal(
                signal_id=f"swing:{candidate['window_id']}",
                detector="procon_swings",
                evidence_kind="stat_fdr",
                signal_score=swing_score,
                support_n=max(0, support_n),
                effect_size=delta_pro,
                p_value=p_value,
                q_value=q_value,
                is_low_power=is_low_power,
                explanation_hint="potential_manipulation",
            )
        )

    for row in composite_high.itertuples(index=False):
        minute = _to_timestamp(getattr(row, "minute_bucket", None))
        if minute is None:
            continue
        candidate = ensure_window(minute, minute)
        composite_score = _to_float(getattr(row, "composite_score", None)) or 0.0
        support_n = int(_to_float(getattr(row, "n_total", None)) or 0)
        candidate["contributors"].append(
            EvidenceSignal(
                signal_id=f"composite:{candidate['window_id']}",
                detector="composite_score",
                evidence_kind="heuristic",
                signal_score=_clamp_unit_interval(composite_score),
                support_n=max(0, support_n),
                is_low_power=support_n < resolved_thresholds.min_support_n,
                explanation_hint="mixed",
            )
        )

    window_rows: list[dict[str, Any]] = []
    for candidate in window_candidates.values():
        start_time = candidate["start_time"]
        end_time = candidate["end_time"]
        if not isinstance(start_time, pd.Timestamp) or not isinstance(end_time, pd.Timestamp):
            continue

        if not counts.empty:
            in_window = counts[
                counts["minute_bucket"].between(start_time, end_time, inclusive="both")
            ].copy()
        else:
            in_window = pd.DataFrame()

        if not in_window.empty:
            n_total = float(pd.to_numeric(in_window["n_total"], errors="coerce").fillna(0.0).sum())
            n_pro = float(pd.to_numeric(in_window["n_pro"], errors="coerce").fillna(0.0).sum())
            candidate["count"] = n_total
            candidate["support_n"] = int(n_total)
            candidate["pro_rate"] = n_pro / n_total if n_total > 0 else None
            candidate["dup_fraction"] = _weighted_mean(
                in_window["dup_name_fraction"],
                in_window["n_total"],
            )

        if not dup_near_clusters.empty and candidate["count"]:
            overlap_mask = _window_overlap_mask(
                dup_near_clusters,
                start_column="first_seen",
                end_column="last_seen",
                start_time=start_time,
                end_time=end_time,
            )
            near_records = float(
                pd.to_numeric(
                    dup_near_clusters.loc[overlap_mask, "n_records"],
                    errors="coerce",
                )
                .fillna(0.0)
                .sum()
            )
            if float(candidate["count"]) > 0:
                candidate["near_dup_fraction"] = _clamp_unit_interval(
                    near_records / float(candidate["count"])
                )

        if not rarity_by_minute.empty:
            rarity_window = rarity_by_minute[
                rarity_by_minute["minute_bucket"].between(start_time, end_time, inclusive="both")
            ].copy()
            if not rarity_window.empty:
                weight_column = (
                    "n_total"
                    if "n_total" in rarity_window.columns
                    else ("n_records" if "n_records" in rarity_window.columns else None)
                )
                candidate["name_weirdness_mean"] = _weighted_mean(
                    rarity_window["rarity_mean"],
                    rarity_window[weight_column] if weight_column else None,
                )

        contributor_ids = {
            str(signal.signal_id)
            for signal in candidate["contributors"]
            if isinstance(signal, EvidenceSignal)
        }
        support_n = int(candidate["support_n"]) if candidate["support_n"] else 0

        dup_fraction = _to_float(candidate["dup_fraction"])
        if (
            dup_fraction is not None
            and dup_fraction > 0.0
            and "dup_fraction" not in contributor_ids
        ):
            candidate["contributors"].append(
                EvidenceSignal(
                    signal_id="dup_fraction",
                    detector="duplicates_exact",
                    evidence_kind="heuristic",
                    signal_score=_clamp_unit_interval(dup_fraction / 0.40),
                    support_n=max(0, support_n),
                    effect_size=dup_fraction,
                    is_low_power=support_n < resolved_thresholds.min_support_n,
                    explanation_hint="data_quality_artifact",
                )
            )

        near_dup_fraction = _to_float(candidate["near_dup_fraction"])
        if (
            near_dup_fraction is not None
            and near_dup_fraction > 0.0
            and "near_dup_fraction" not in contributor_ids
        ):
            candidate["contributors"].append(
                EvidenceSignal(
                    signal_id="near_dup_fraction",
                    detector="duplicates_near",
                    evidence_kind="heuristic",
                    signal_score=_clamp_unit_interval(near_dup_fraction / 0.30),
                    support_n=max(0, support_n),
                    effect_size=near_dup_fraction,
                    is_low_power=support_n < resolved_thresholds.min_support_n,
                    explanation_hint="potential_manipulation",
                )
            )

        name_weirdness_mean = _to_float(candidate["name_weirdness_mean"])
        if (
            name_weirdness_mean is not None
            and name_weirdness_mean > 0.0
            and "name_weirdness_mean" not in contributor_ids
        ):
            candidate["contributors"].append(
                EvidenceSignal(
                    signal_id="name_weirdness_mean",
                    detector="rare_names",
                    evidence_kind="heuristic",
                    signal_score=_clamp_unit_interval(name_weirdness_mean / rarity_scale),
                    support_n=max(0, support_n),
                    effect_size=name_weirdness_mean,
                    is_low_power=support_n < resolved_thresholds.min_support_n,
                    explanation_hint="potential_manipulation",
                )
            )

        if not candidate["contributors"]:
            candidate["contributors"].append(
                EvidenceSignal(
                    signal_id="support_only",
                    detector="baseline_profile",
                    evidence_kind="heuristic",
                    signal_score=0.0,
                    support_n=max(0, support_n),
                    is_low_power=support_n < resolved_thresholds.min_support_n,
                    explanation_hint="insufficient_evidence",
                )
            )

        evidence_item = build_evidence_item(
            queue_id=str(candidate["window_id"]),
            queue_kind="window",
            contributors=tuple(candidate["contributors"]),
            support_n=max(
                int(candidate["support_n"] or 0),
                max(int(signal.support_n) for signal in candidate["contributors"]),
            ),
            thresholds=resolved_thresholds,
        )
        (
            score_primary_driver,
            score_detector_breakdown,
            score_signal_breakdown,
        ) = _score_breakdown_strings(evidence_item.contributors)

        window_row = {
            "window_id": evidence_item.queue_id,
            "start_time": _iso_or_none(start_time),
            "end_time": _iso_or_none(end_time),
            "count": _to_float(candidate["count"]),
            "expected": _to_float(candidate["expected"]),
            "z": _to_float(candidate["z"]),
            "q_value": _to_float(candidate["q_value"]),
            "pro_rate": _to_float(candidate["pro_rate"]),
            "delta_pro_rate": _to_float(candidate["delta_pro_rate"]),
            "dup_fraction": _to_float(candidate["dup_fraction"]),
            "near_dup_fraction": _to_float(candidate["near_dup_fraction"]),
            "name_weirdness_mean": _to_float(candidate["name_weirdness_mean"]),
            "support_n": int(evidence_item.support_n),
            "evidence_tier": evidence_item.evidence_tier,
            "primary_explanation": evidence_item.primary_explanation,
            "secondary_explanation": evidence_item.secondary_explanation,
            "score": float(evidence_item.score),
            "is_low_power": bool(evidence_item.is_low_power),
            "caveat_flags": list(evidence_item.caveat_flags),
            "evidence_kinds": list(evidence_item.evidence_kinds),
            "source_detectors": sorted(
                {
                    str(signal.detector)
                    for signal in evidence_item.contributors
                    if signal.detector is not None
                }
            ),
            "score_primary_driver": score_primary_driver,
            "score_detector_breakdown": score_detector_breakdown,
            "score_signal_breakdown": score_signal_breakdown,
        }
        for field in WINDOW_QUEUE_REQUIRED_COLUMNS:
            window_row.setdefault(field, None)
        window_rows.append(window_row)

    window_rows.sort(
        key=lambda row: (
            _tier_rank(str(row.get("evidence_tier", "watch"))),
            -float(row.get("score") or 0.0),
            -int(row.get("support_n") or 0),
            str(row.get("start_time") or ""),
        )
    )
    window_rows = window_rows[: max(1, int(top_n_windows))]

    # Record evidence queue (name-level)
    max_repeat = _to_float(pd.to_numeric(dup_exact_top["n"], errors="coerce").max()) or 1.0
    weirdness_scale = _to_float(
        pd.to_numeric(weird_names["weirdness_score"], errors="coerce").quantile(0.95)
    )
    if weirdness_scale is None or weirdness_scale <= 0:
        weirdness_scale = 1.0

    record_entries: dict[str, dict[str, Any]] = {}

    for row in dup_exact_top.itertuples(index=False):
        canonical_name = str(getattr(row, "canonical_name", "") or "").strip()
        if not canonical_name:
            continue
        entry = record_entries.setdefault(
            canonical_name,
            {
                "record_id": canonical_name,
                "canonical_name": canonical_name,
                "display_name": str(getattr(row, "display_name", "") or "").strip()
                or canonical_name,
                "n_records": int(_to_float(getattr(row, "n", None)) or 0),
                "n_pro": int(_to_float(getattr(row, "n_pro", None)) or 0),
                "n_con": int(_to_float(getattr(row, "n_con", None)) or 0),
                "time_span_minutes": _to_float(getattr(row, "time_span_minutes", None)),
                "weirdness_score": None,
                "contributors": [],
            },
        )
        repeat_count = int(_to_float(getattr(row, "n", None)) or 0)
        entry["n_records"] = max(int(entry["n_records"]), repeat_count)
        repeat_score = _clamp_unit_interval(float(repeat_count) / max_repeat)
        entry["contributors"].append(
            EvidenceSignal(
                signal_id=f"repeat:{canonical_name}",
                detector="duplicates_exact",
                evidence_kind="heuristic",
                signal_score=repeat_score,
                support_n=max(0, repeat_count),
                effect_size=float(repeat_count),
                is_low_power=repeat_count < resolved_thresholds.min_support_n,
                explanation_hint="data_quality_artifact",
            )
        )

    for row in weird_names.itertuples(index=False):
        canonical_name = str(getattr(row, "canonical_name", "") or "").strip()
        if not canonical_name:
            continue
        weirdness_score = _to_float(getattr(row, "weirdness_score", None))
        if weirdness_score is None:
            continue

        entry = record_entries.setdefault(
            canonical_name,
            {
                "record_id": canonical_name,
                "canonical_name": canonical_name,
                "display_name": str(getattr(row, "sample_name", "") or "").strip()
                or canonical_name,
                "n_records": 1,
                "n_pro": 0,
                "n_con": 0,
                "time_span_minutes": None,
                "weirdness_score": weirdness_score,
                "contributors": [],
            },
        )
        entry["weirdness_score"] = weirdness_score
        entry["contributors"].append(
            EvidenceSignal(
                signal_id=f"weirdness:{canonical_name}",
                detector="rare_names",
                evidence_kind="heuristic",
                signal_score=_clamp_unit_interval(weirdness_score / weirdness_scale),
                support_n=max(1, int(entry["n_records"])),
                effect_size=weirdness_score,
                is_low_power=int(entry["n_records"]) < resolved_thresholds.min_support_n,
                explanation_hint="potential_manipulation",
            )
        )

    record_rows: list[dict[str, Any]] = []
    for entry in record_entries.values():
        contributors = tuple(entry["contributors"])
        if not contributors:
            continue
        evidence_item = build_evidence_item(
            queue_id=str(entry["record_id"]),
            queue_kind="record",
            contributors=contributors,
            support_n=max(
                int(entry["n_records"]), max(signal.support_n for signal in contributors)
            ),
            thresholds=resolved_thresholds,
        )
        (
            score_primary_driver,
            score_detector_breakdown,
            score_signal_breakdown,
        ) = _score_breakdown_strings(evidence_item.contributors)
        record_rows.append(
            {
                "record_id": evidence_item.queue_id,
                "canonical_name": entry["canonical_name"],
                "display_name": entry["display_name"],
                "n_records": int(entry["n_records"]),
                "n_pro": int(entry["n_pro"]),
                "n_con": int(entry["n_con"]),
                "time_span_minutes": _to_float(entry["time_span_minutes"]),
                "weirdness_score": _to_float(entry["weirdness_score"]),
                "support_n": int(evidence_item.support_n),
                "evidence_tier": evidence_item.evidence_tier,
                "primary_explanation": evidence_item.primary_explanation,
                "secondary_explanation": evidence_item.secondary_explanation,
                "score": float(evidence_item.score),
                "is_low_power": bool(evidence_item.is_low_power),
                "caveat_flags": list(evidence_item.caveat_flags),
                "evidence_kinds": list(evidence_item.evidence_kinds),
                "score_primary_driver": score_primary_driver,
                "score_detector_breakdown": score_detector_breakdown,
                "score_signal_breakdown": score_signal_breakdown,
            }
        )

    record_rows.sort(
        key=lambda row: (
            _tier_rank(str(row.get("evidence_tier", "watch"))),
            -float(row.get("score") or 0.0),
            -int(row.get("n_records") or 0),
            str(row.get("canonical_name") or ""),
        )
    )
    record_rows = record_rows[: max(1, int(top_n_records))]

    # Cluster evidence queue
    max_cluster_size = (
        _to_float(pd.to_numeric(dup_near_clusters["cluster_size"], errors="coerce").max()) or 1.0
    )
    max_cluster_records = (
        _to_float(pd.to_numeric(dup_near_clusters["n_records"], errors="coerce").max()) or 1.0
    )
    cluster_rows: list[dict[str, Any]] = []
    for row in dup_near_clusters.itertuples(index=False):
        cluster_id = str(getattr(row, "cluster_id", "") or "").strip()
        if not cluster_id:
            continue
        cluster_size = int(_to_float(getattr(row, "cluster_size", None)) or 0)
        n_records = int(_to_float(getattr(row, "n_records", None)) or 0)
        time_span_minutes = _to_float(getattr(row, "time_span_minutes", None)) or 0.0
        first_seen = _to_timestamp(getattr(row, "first_seen", None))
        last_seen = _to_timestamp(getattr(row, "last_seen", None))
        compactness = _clamp_unit_interval(1.0 - min(time_span_minutes / 1440.0, 1.0))
        cluster_score = _clamp_unit_interval(
            (0.45 * (cluster_size / max_cluster_size))
            + (0.35 * (n_records / max_cluster_records))
            + (0.20 * compactness)
        )
        evidence_item = build_evidence_item(
            queue_id=cluster_id,
            queue_kind="cluster",
            contributors=(
                EvidenceSignal(
                    signal_id=f"cluster:{cluster_id}",
                    detector="duplicates_near",
                    evidence_kind="heuristic",
                    signal_score=cluster_score,
                    support_n=max(0, n_records),
                    effect_size=float(cluster_size),
                    is_low_power=n_records < resolved_thresholds.min_support_n,
                    explanation_hint="potential_manipulation",
                ),
            ),
            support_n=n_records,
            thresholds=resolved_thresholds,
        )
        (
            score_primary_driver,
            score_detector_breakdown,
            score_signal_breakdown,
        ) = _score_breakdown_strings(evidence_item.contributors)
        cluster_rows.append(
            {
                "cluster_id": cluster_id,
                "cluster_size": cluster_size,
                "n_records": n_records,
                "n_pro": int(_to_float(getattr(row, "n_pro", None)) or 0),
                "n_con": int(_to_float(getattr(row, "n_con", None)) or 0),
                "first_seen": _iso_or_none(first_seen),
                "last_seen": _iso_or_none(last_seen),
                "time_span_minutes": time_span_minutes,
                "support_n": int(evidence_item.support_n),
                "evidence_tier": evidence_item.evidence_tier,
                "primary_explanation": evidence_item.primary_explanation,
                "secondary_explanation": evidence_item.secondary_explanation,
                "score": float(evidence_item.score),
                "is_low_power": bool(evidence_item.is_low_power),
                "caveat_flags": list(evidence_item.caveat_flags),
                "evidence_kinds": list(evidence_item.evidence_kinds),
                "score_primary_driver": score_primary_driver,
                "score_detector_breakdown": score_detector_breakdown,
                "score_signal_breakdown": score_signal_breakdown,
            }
        )

    cluster_rows.sort(
        key=lambda row: (
            _tier_rank(str(row.get("evidence_tier", "watch"))),
            -float(row.get("score") or 0.0),
            -int(row.get("n_records") or 0),
            str(row.get("cluster_id") or ""),
        )
    )
    cluster_rows = cluster_rows[: max(1, int(top_n_clusters))]

    summary = _empty_summary()
    if not counts.empty:
        n_total = float(pd.to_numeric(counts["n_total"], errors="coerce").fillna(0.0).sum())
        n_pro = float(pd.to_numeric(counts["n_pro"], errors="coerce").fillna(0.0).sum())
        n_con = float(pd.to_numeric(counts["n_con"], errors="coerce").fillna(0.0).sum())
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

    if not swings.empty:
        swing_preview = swings.copy()
        for column in ("q_value", "abs_delta_pro_rate", "n_total"):
            swing_preview[column] = pd.to_numeric(swing_preview[column], errors="coerce")
        swing_preview = swing_preview.sort_values(
            by=["q_value", "abs_delta_pro_rate", "n_total"],
            ascending=[True, False, False],
            na_position="last",
        ).head(5)
        summary["top_swing_windows"] = [
            {
                "start_time": _iso_or_none(_to_timestamp(row.start_minute)),
                "end_time": _iso_or_none(_to_timestamp(row.end_minute)),
                "n_total": _to_float(row.n_total),
                "pro_rate": _to_float(row.pro_rate),
                "delta_pro_rate": _to_float(row.delta_pro_rate),
                "z_score": _to_float(row.z_score),
                "q_value": _to_float(row.q_value),
            }
            for row in swing_preview.itertuples(index=False)
        ]

    summary["top_repeated_names"] = [
        {
            "display_name": str(row.get("display_name") or ""),
            "canonical_name": str(row.get("canonical_name") or ""),
            "n_records": int(_to_float(row.get("n")) or 0),
            "n_pro": int(_to_float(row.get("n_pro")) or 0),
            "n_con": int(_to_float(row.get("n_con")) or 0),
        }
        for row in dup_exact_top.head(5).to_dict(orient="records")
    ]
    summary["top_near_dup_clusters"] = [
        {
            "cluster_id": str(row.get("cluster_id") or ""),
            "cluster_size": int(_to_float(row.get("cluster_size")) or 0),
            "n_records": int(_to_float(row.get("n_records")) or 0),
            "first_seen": _iso_or_none(_to_timestamp(row.get("first_seen"))),
            "last_seen": _iso_or_none(_to_timestamp(row.get("last_seen"))),
        }
        for row in dup_near_clusters.head(5).to_dict(orient="records")
    ]
    if not off_hours_summary.empty:
        summary["off_hours_summary"] = {
            key: _to_float(value) if not isinstance(value, bool) else bool(value)
            for key, value in off_hours_summary.iloc[0].to_dict().items()
        }

    summary["queue_counts"] = {
        "window": int(len(window_rows)),
        "record": int(len(record_rows)),
        "cluster": int(len(cluster_rows)),
    }
    summary["window_tier_counts"] = dict(
        Counter(str(row.get("evidence_tier") or "watch") for row in window_rows)
    )
    for tier in ("high", "medium", "watch"):
        summary["window_tier_counts"].setdefault(tier, 0)

    return {
        "triage_summary": summary,
        "window_evidence_queue": window_rows,
        "record_evidence_queue": record_rows,
        "cluster_evidence_queue": cluster_rows,
    }
