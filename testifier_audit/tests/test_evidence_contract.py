from __future__ import annotations

import pytest

from testifier_audit.report.contracts import EvidenceSignal, TriageEvidenceItem


def test_evidence_signal_normalizes_caveats_and_serializes() -> None:
    signal = EvidenceSignal(
        signal_id="bursts.window_5m",
        detector="bursts",
        evidence_kind="stat_fdr",
        signal_score=0.87,
        support_n=42,
        q_value=0.01,
        caveat_flags=(" low_power ", "low_power", "low_support_n"),
    )

    assert signal.caveat_flags == ("low_power", "low_support_n")
    payload = signal.to_dict()
    assert payload["evidence_kind"] == "stat_fdr"
    assert payload["caveat_flags"] == ["low_power", "low_support_n"]


def test_evidence_signal_rejects_invalid_probability_values() -> None:
    with pytest.raises(ValueError):
        EvidenceSignal(
            signal_id="bursts.window_5m",
            detector="bursts",
            evidence_kind="stat_fdr",
            signal_score=0.50,
            support_n=20,
            q_value=1.25,
        )


def test_triage_evidence_item_exposes_evidence_kinds() -> None:
    item = TriageEvidenceItem(
        queue_id="window-1",
        queue_kind="window",
        score=0.76,
        support_n=55,
        contributors=(
            EvidenceSignal(
                signal_id="bursts.window_5m",
                detector="bursts",
                evidence_kind="stat_fdr",
                signal_score=0.85,
                support_n=55,
                q_value=0.01,
            ),
            EvidenceSignal(
                signal_id="dup.cluster_count",
                detector="duplicates_near",
                evidence_kind="heuristic",
                signal_score=0.60,
                support_n=30,
            ),
        ),
        evidence_tier="medium",
        primary_explanation="insufficient_evidence",
    )

    assert item.evidence_kinds == ("heuristic", "stat_fdr")
    item_dict = item.to_dict()
    assert item_dict["evidence_tier"] == "medium"
    assert item_dict["evidence_kinds"] == ["heuristic", "stat_fdr"]
