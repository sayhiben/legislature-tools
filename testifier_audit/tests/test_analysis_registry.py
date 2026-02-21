from __future__ import annotations

from testifier_audit.report.analysis_registry import analysis_status, default_analysis_definitions


def test_default_analysis_definitions_have_unique_ids_and_hero_chart_ids() -> None:
    definitions = default_analysis_definitions()
    ids = [entry["id"] for entry in definitions]
    hero_ids = [entry["hero_chart_id"] for entry in definitions]

    assert len(ids) == len(set(ids))
    assert len(hero_ids) == len(set(hero_ids))
    assert "baseline_profile" in ids
    assert "composite_score" in ids


def test_analysis_status_reports_ready_when_any_chart_has_rows() -> None:
    status, reason = analysis_status(
        detector="bursts",
        charts={"bursts_hero_timeline": [{"minute_bucket": "2026-02-01T00:00:00Z"}]},
        hero_chart_id="bursts_hero_timeline",
        detail_chart_ids=["bursts_significance_by_window"],
        detector_summaries={},
    )
    assert status == "ready"
    assert reason == ""


def test_analysis_status_reports_disabled_with_detector_reason() -> None:
    status, reason = analysis_status(
        detector="voter_registry_match",
        charts={},
        hero_chart_id="voter_registry_match_rates",
        detail_chart_ids=["voter_registry_match_by_position"],
        detector_summaries={
            "voter_registry_match": {"enabled": False, "reason": "disabled_in_config"}
        },
    )
    assert status == "disabled"
    assert reason == "disabled_in_config"
