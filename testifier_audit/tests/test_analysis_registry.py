from __future__ import annotations

from testifier_audit.report.analysis_registry import (
    analysis_status,
    configured_analysis_ids,
    configured_detector_names,
    default_analysis_definitions,
    focus_mode_for_analysis_ids,
)


def test_default_analysis_definitions_have_unique_ids_and_hero_chart_ids() -> None:
    definitions = default_analysis_definitions()
    ids = [entry["id"] for entry in definitions]
    hero_ids = [entry["hero_chart_id"] for entry in definitions]

    assert len(ids) == len(set(ids))
    assert len(hero_ids) == len(set(hero_ids))
    assert "baseline_profile" in ids
    assert "composite_score" in ids
    assert all("group" in entry for entry in definitions)
    assert all("priority" in entry for entry in definitions)


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


def test_configured_analysis_scope_maps_to_known_analyses_and_detectors() -> None:
    definitions = default_analysis_definitions()
    known_analysis_ids = {entry["id"] for entry in definitions}
    configured_ids = configured_analysis_ids()

    assert len(configured_ids) == len(set(configured_ids))
    assert all(analysis_id in known_analysis_ids for analysis_id in configured_ids)

    detectors = configured_detector_names()
    detector_lookup = {
        str(entry.get("id") or ""): str(entry.get("detector") or "")
        for entry in definitions
        if entry.get("detector")
    }
    for analysis_id in configured_ids:
        detector_name = detector_lookup.get(analysis_id, "")
        if detector_name:
            assert detector_name in detectors


def test_focus_mode_for_analysis_ids() -> None:
    assert focus_mode_for_analysis_ids([]) == "full_report"
    assert focus_mode_for_analysis_ids(["off_hours"]) == "off_hours_only"
    assert focus_mode_for_analysis_ids(["off_hours", "bursts"]) == "analysis_subset"
