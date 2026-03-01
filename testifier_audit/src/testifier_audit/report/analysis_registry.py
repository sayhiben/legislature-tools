from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class AnalysisDefinition:
    id: str
    title: str
    detector: str | None
    hero_chart_id: str
    detail_chart_ids: tuple[str, ...]
    how_to_read: str
    what_to_look_for: str
    common_benign_causes: str
    expected_metric_keys: tuple[str, ...] = ()
    group: str = "detector_analysis"
    priority: int = 50

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "detector": self.detector,
            "hero_chart_id": self.hero_chart_id,
            "detail_chart_ids": list(self.detail_chart_ids),
            "how_to_read": self.how_to_read,
            "what_to_look_for": self.what_to_look_for,
            "common_benign_causes": self.common_benign_causes,
            "expected_metric_keys": list(self.expected_metric_keys),
            "group": self.group,
            "priority": self.priority,
        }


# Temporary analysis scope for active development.
# Uncomment analyses to run/render; leave empty to run/render the full pack.
ANALYSES_TO_PERFORM: tuple[str, ...] = (
    "bursts",
    "duplicates_exact",
    "org_anomalies",
    "voter_registry_match",
    "off_hours",
)


_ANALYSIS_DEFINITIONS: tuple[AnalysisDefinition, ...] = (
    AnalysisDefinition(
        id="bursts",
        title="Burst Windows",
        detector="bursts",
        hero_chart_id="bursts_hero_timeline",
        detail_chart_ids=(
            "bursts_significance_by_window",
            "bursts_composition_shift",
        ),
        how_to_read=(
            "Start with burst windows over time, then confirm each burst's duration and "
            "position-specific impact magnitude."
        ),
        what_to_look_for=(
            "Sustained windows with clear excess volume where pro/con impact counts are "
            "materially imbalanced, and where merged burst durations extend beyond a "
            "single isolated minute."
        ),
        common_benign_causes=(
            "Agenda release timing and outbound campaign alerts can generate short-lived "
            "legitimate bursts."
        ),
        expected_metric_keys=("window_high_share", "window_top_score"),
    ),
    AnalysisDefinition(
        id="off_hours",
        title="Off-Hours Profile",
        detector="off_hours",
        hero_chart_id="off_hours_control_timeline",
        detail_chart_ids=(
            "off_hours_funnel_plot",
            "off_hours_primary_residual_timeline",
        ),
        how_to_read=(
            "Use the off-hours control timeline to compare observed pro share with "
            "Wilson uncertainty and primary expected/control bands "
            "(model-based when available, day-adjusted fallback otherwise) at each bucket size."
        ),
        what_to_look_for=(
            "Sustained robust primary alerts (alert-eligible, beyond primary 99.8% "
            "control bands with tail-consistent FDR support and material effect size) "
            "at adequate support, then verify whether lower- or upper-tail patterns "
            "repeat or cluster in specific dates/hours."
        ),
        common_benign_causes=(
            "Time-zone spillover, campaign scheduling, and hearing-deadline pushes can "
            "produce legitimate overnight composition shifts."
        ),
        expected_metric_keys=("off_hours_ratio",),
    ),
    AnalysisDefinition(
        id="duplicates_exact",
        title="Duplicate-Name Collisions",
        detector="duplicates_exact",
        hero_chart_id="duplicates_exact_bucket_concentration",
        detail_chart_ids=(
            "duplicates_exact_metric_diagnostics",
            "duplicates_exact_swing_impact",
            "duplicates_exact_per_name_anomalies",
            "duplicates_exact_top_name_timing_exact",
            "duplicates_exact_position_bucket_deviance",
        ),
        how_to_read=(
            "Interpret bucket values as duplicate-name collision burden: rows (or distinct names) "
            "in the bucket whose normalized name keys repeat anywhere in the hearing timeline. "
            "Reference-baseline expectation uses volume-share scaling from hearing-level collision "
            "totals, and deviation is signed (observed minus expected)."
        ),
        what_to_look_for=(
            "Buckets where duplicate-name collision burden is persistently higher than expected "
            "under the selected baseline across neighboring windows, then verify whether those "
            "intervals also show position-specific deviance or concentrated repeated names."
        ),
        common_benign_causes=(
            "Common names and legitimate coordinated outreach can elevate collision burden."
        ),
        expected_metric_keys=("top_name_max_records", "dedup_drop_fraction", "window_top_dup_fraction"),
    ),
    AnalysisDefinition(
        id="org_anomalies",
        title="Organization Field Anomalies",
        detector="org_anomalies",
        hero_chart_id="org_anomalies_blank_rate",
        detail_chart_ids=(
            "org_anomalies_position_rates",
        ),
        how_to_read=(
            "Track overall blank-organization rate over time first using stacked "
            "Pro/Con volume context bars, then compare position-specific blank-rate "
            "differences in the same windows."
        ),
        what_to_look_for=(
            "Sustained blank-rate elevation in bucketed timelines, and consistent Pro/Con "
            "separation in position-specific blank rates across adjacent windows."
        ),
        common_benign_causes=(
            "Form UX and campaign guidance often increase legitimate blank organization "
            "submissions."
        ),
        expected_metric_keys=("blank_org_ratio", "blank_org_gap_pro_minus_con"),
    ),
    AnalysisDefinition(
        id="voter_registry_match",
        title="Registered Voter Match",
        detector="voter_registry_match",
        hero_chart_id="voter_registry_match_rates",
        detail_chart_ids=(
            "voter_registry_linkage_by_position_rows",
            "voter_registry_linkage_by_position_unique",
            "voter_registry_position_bounds",
            "voter_registry_unmatched_names",
        ),
        how_to_read=(
            "Hero chart emphasizes conservative matched-rate trajectory with uncertainty; "
            "detail charts retain unmatched-rate diagnostics across units and linkage modes, "
            "including historical-status lower/upper bounds when enabled."
        ),
        what_to_look_for=(
            "Sustained unmatched-rate differences across positions at both row and unique-name "
            "units, then verify whether those differences remain stable in the rows-vs-unique "
            "position-bounds span panel."
        ),
        common_benign_causes=(
            "Name normalization gaps, registration recency, and non-registered participants can "
            "raise unmatched rates."
        ),
        expected_metric_keys=("overall_pro_rate",),
    ),
)

_ANALYSIS_DETECTOR_DEPENDENCIES: dict[str, tuple[str, ...]] = {}


_ANALYSIS_GROUP_PRIORITY: dict[str, tuple[str, int]] = {
    "bursts": ("window_signals", 95),
    "off_hours": ("window_signals", 86),
    "duplicates_exact": ("identity_forensics", 88),
    "org_anomalies": ("field_quality", 78),
    "voter_registry_match": ("external_enrichment", 65),
}


def configured_analysis_ids() -> list[str]:
    known_analysis_ids = {definition.id for definition in _ANALYSIS_DEFINITIONS}
    seen: set[str] = set()
    selected: list[str] = []
    for analysis_id in ANALYSES_TO_PERFORM:
        normalized = str(analysis_id or "").strip()
        if not normalized or normalized in seen or normalized not in known_analysis_ids:
            continue
        seen.add(normalized)
        selected.append(normalized)
    return selected


def configured_detector_names() -> set[str]:
    selected_analysis_ids = configured_analysis_ids()
    if not selected_analysis_ids:
        return {
            definition.detector
            for definition in _ANALYSIS_DEFINITIONS
            if isinstance(definition.detector, str) and definition.detector
        }

    definitions_by_id = {definition.id: definition for definition in _ANALYSIS_DEFINITIONS}
    selected_detectors: set[str] = set()
    for analysis_id in selected_analysis_ids:
        definition = definitions_by_id.get(analysis_id)
        detector_name = definition.detector if definition else None
        if isinstance(detector_name, str) and detector_name:
            selected_detectors.add(detector_name)
        for dependency in _ANALYSIS_DETECTOR_DEPENDENCIES.get(analysis_id, ()):
            dependency_name = str(dependency or "").strip()
            if dependency_name:
                selected_detectors.add(dependency_name)
    return selected_detectors


def focus_mode_for_analysis_ids(analysis_ids: list[str]) -> str:
    if not analysis_ids:
        return "full_report"
    if len(analysis_ids) == 1 and analysis_ids[0] == "off_hours":
        return "off_hours_only"
    return "analysis_subset"


def default_analysis_definitions() -> list[dict[str, Any]]:
    definitions: list[dict[str, Any]] = []
    for definition in _ANALYSIS_DEFINITIONS:
        payload = definition.to_dict()
        group_priority = _ANALYSIS_GROUP_PRIORITY.get(definition.id)
        if group_priority:
            payload["group"] = group_priority[0]
            payload["priority"] = int(group_priority[1])
        definitions.append(payload)
    return definitions


def analysis_status(
    detector: str | None,
    charts: dict[str, list[dict[str, Any]]],
    hero_chart_id: str,
    detail_chart_ids: list[str],
    detector_summaries: dict[str, dict[str, Any]],
) -> tuple[str, str]:
    total_rows = len(charts.get(hero_chart_id, []))
    total_rows += sum(len(charts.get(chart_id, [])) for chart_id in detail_chart_ids)
    if total_rows > 0:
        return "ready", ""

    if detector:
        summary = detector_summaries.get(detector, {})
        if summary:
            enabled = summary.get("enabled")
            active = summary.get("active")
            if enabled is False or active is False:
                reason = str(summary.get("reason") or "disabled")
                return "disabled", reason

    return "empty", "No chartable records were produced for this analysis in this run."
