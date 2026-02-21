from __future__ import annotations

from testifier_audit.report.contracts import EvidenceSignal, TriageTierThresholds
from testifier_audit.report.triage_builder import (
    _score_breakdown_strings,
    aggregate_signal_score,
    build_evidence_item,
    choose_explanations,
    compute_evidence_tier,
    merge_caveat_flags,
)


def test_compute_evidence_tier_requires_calibrated_support_for_high() -> None:
    thresholds = TriageTierThresholds(high=0.80, medium=0.60, min_support_n=25)

    heuristic_only = compute_evidence_tier(
        score=0.92,
        support_n=80,
        evidence_kinds=["heuristic"],
        is_low_power=False,
        thresholds=thresholds,
    )
    calibrated = compute_evidence_tier(
        score=0.92,
        support_n=80,
        evidence_kinds=["heuristic", "stat_fdr"],
        is_low_power=False,
        thresholds=thresholds,
    )

    assert heuristic_only == "medium"
    assert calibrated == "high"


def test_compute_evidence_tier_caps_high_when_low_power() -> None:
    tier = compute_evidence_tier(
        score=0.95,
        support_n=100,
        evidence_kinds=["stat_fdr"],
        is_low_power=True,
        thresholds=TriageTierThresholds(),
    )
    assert tier == "medium"


def test_build_evidence_item_applies_scoring_explanations_and_caveats() -> None:
    signals = (
        EvidenceSignal(
            signal_id="bursts.window_15m",
            detector="bursts",
            evidence_kind="stat_fdr",
            signal_score=0.90,
            support_n=60,
            q_value=0.01,
            explanation_hint="potential_manipulation",
        ),
        EvidenceSignal(
            signal_id="duplicates.cluster_near",
            detector="duplicates_near",
            evidence_kind="heuristic",
            signal_score=0.75,
            support_n=18,
            is_low_power=True,
            explanation_hint="data_quality_artifact",
        ),
    )

    score = aggregate_signal_score(signals)
    caveats = merge_caveat_flags(signals, min_support_n=25)
    primary, secondary = choose_explanations(signals)
    item = build_evidence_item(
        queue_id="window-2026-02-01T00:00:00Z",
        queue_kind="window",
        contributors=signals,
    )

    assert 0.0 <= score <= 1.0
    assert "low_power" in caveats
    assert "low_support_n" in caveats
    assert primary == "mixed"
    assert secondary in {"data_quality_artifact", "potential_manipulation"}
    assert item.is_low_power is True
    assert item.evidence_tier == "medium"


def test_score_breakdown_strings_ranks_primary_driver() -> None:
    signals = (
        EvidenceSignal(
            signal_id="bursts.window_15m",
            detector="bursts",
            evidence_kind="stat_fdr",
            signal_score=0.80,
            support_n=40,
        ),
        EvidenceSignal(
            signal_id="dup.cluster_001",
            detector="duplicates_near",
            evidence_kind="heuristic",
            signal_score=0.60,
            support_n=22,
        ),
    )

    primary_driver, detector_breakdown, signal_breakdown = _score_breakdown_strings(signals)

    assert primary_driver == "bursts"
    assert "bursts" in detector_breakdown
    assert "duplicates_near" in detector_breakdown
    assert "bursts.window_15m" in signal_breakdown
