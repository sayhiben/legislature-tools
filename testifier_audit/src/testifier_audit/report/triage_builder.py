from __future__ import annotations

from collections import Counter
from typing import Mapping, Sequence

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
    has_calibrated = any(
        kind in {"stat_fdr", "calibrated_empirical"} for kind in evidence_kinds
    )

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
