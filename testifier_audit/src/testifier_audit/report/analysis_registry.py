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
            "group": self.group,
            "priority": self.priority,
        }


# Temporary analysis scope for active development.
# Uncomment analyses to run/render; leave empty to run/render the full pack.
ANALYSES_TO_PERFORM: tuple[str, ...] = (
    # "baseline_profile",
    # "bursts",
    # "procon_swings",
    # "changepoints",
    "off_hours",
    # "duplicates_exact",
    # "duplicates_near",
    # "sortedness",
    # "rare_names",
    # "org_anomalies",
    # "voter_registry_match",
    # "periodicity",
    # "multivariate_anomalies",
    # "composite_score",
)


_ANALYSIS_DEFINITIONS: tuple[AnalysisDefinition, ...] = (
    AnalysisDefinition(
        id="baseline_profile",
        title="Baseline Profile",
        detector=None,
        hero_chart_id="baseline_volume_pro_rate",
        detail_chart_ids=(
            "baseline_day_hour_volume",
            "baseline_top_names",
            "baseline_name_length_distribution",
        ),
        how_to_read=(
            "Start with baseline volume and pro-rate movement to establish normal tempo "
            "before detector-specific interpretation."
        ),
        what_to_look_for=(
            "Baseline breaks in volume or composition that align with detector flags "
            "and repeat across adjacent windows."
        ),
        common_benign_causes=(
            "Hearing schedule transitions and reminder cascades can create expected "
            "baseline shifts."
        ),
    ),
    AnalysisDefinition(
        id="bursts",
        title="Burst Windows",
        detector="bursts",
        hero_chart_id="bursts_hero_timeline",
        detail_chart_ids=(
            "bursts_significance_by_window",
            "bursts_composition_shift",
            "bursts_null_distribution",
        ),
        how_to_read="Burst windows compare observed local volume to expected background volume.",
        what_to_look_for=(
            "Repeated high-rate-ratio windows with composition shifts at multiple "
            "window sizes rather than isolated spikes."
        ),
        common_benign_causes=(
            "Agenda release timing and outbound campaign alerts can generate short-lived "
            "legitimate bursts."
        ),
    ),
    AnalysisDefinition(
        id="procon_swings",
        title="Pro/Con Swings",
        detector="procon_swings",
        hero_chart_id="procon_swings_hero_bucket_trend",
        detail_chart_ids=(
            "procon_swings_shift_heatmap",
            "procon_swings_day_hour_heatmap",
            "procon_swings_time_of_day_profile",
            "procon_swings_direction_runs",
            "procon_swings_null_distribution",
        ),
        how_to_read=(
            "Track pro-rate relative to stable bands and baseline while preserving "
            "per-bucket uncertainty context."
        ),
        what_to_look_for=(
            "Sustained directional ratio changes across neighboring buckets and repeated "
            "dayparts, especially when contiguous same-direction runs lengthen."
        ),
        common_benign_causes=(
            "Daypart participation mix and event-response waves can move ratios without "
            "manipulation."
        ),
    ),
    AnalysisDefinition(
        id="changepoints",
        title="Structural Changepoints",
        detector="changepoints",
        hero_chart_id="changepoints_hero_timeline",
        detail_chart_ids=("changepoints_magnitude", "changepoints_hour_hist"),
        how_to_read=(
            "Changepoints mark regime boundaries where level means differ before and "
            "after a boundary."
        ),
        what_to_look_for=(
            "Clusters of large-magnitude changes that align with other detector evidence "
            "windows."
        ),
        common_benign_causes=(
            "Hearing open/close windows and coverage surges naturally create structural "
            "breaks."
        ),
    ),
    AnalysisDefinition(
        id="off_hours",
        title="Off-Hours Profile",
        detector="off_hours",
        hero_chart_id="off_hours_control_timeline",
        detail_chart_ids=(
            "off_hours_funnel_plot",
            "off_hours_primary_residual_timeline",
            "off_hours_primary_flag_channels",
            "off_hours_model_fit_diagnostics",
            "off_hours_date_hour_pro_heatmap",
            "off_hours_date_hour_primary_residual_heatmap",
            "off_hours_date_hour_volume_heatmap",
        ),
        how_to_read=(
            "Use the off-hours control timeline to compare observed pro share with "
            "Wilson uncertainty and primary expected/control bands "
            "(model-based when available, day-adjusted fallback otherwise) at each bucket size."
        ),
        what_to_look_for=(
            "Sustained robust primary alerts (alert-eligible, below primary 99.8% "
            "lower control band, lower-tail FDR-supported, and materially negative) "
            "at adequate support, then verify whether the pattern repeats or clusters "
            "in specific dates/hours."
        ),
        common_benign_causes=(
            "Time-zone spillover, campaign scheduling, and hearing-deadline pushes can "
            "produce legitimate overnight composition shifts."
        ),
    ),
    AnalysisDefinition(
        id="duplicates_exact",
        title="Exact Duplicate Names",
        detector="duplicates_exact",
        hero_chart_id="duplicates_exact_bucket_concentration",
        detail_chart_ids=("duplicates_exact_top_names", "duplicates_exact_position_switch"),
        how_to_read=(
            "Bucket-level duplicate concentration highlights repeated identical names "
            "within narrow windows."
        ),
        what_to_look_for=(
            "High duplicate concentration with frequent position switching for the same "
            "canonical name."
        ),
        common_benign_causes=(
            "Common household names and family submissions may elevate duplicate counts."
        ),
    ),
    AnalysisDefinition(
        id="duplicates_near",
        title="Near-Duplicate Clusters",
        detector="duplicates_near",
        hero_chart_id="duplicates_near_cluster_timeline",
        detail_chart_ids=(
            "duplicates_near_cluster_size",
            "duplicates_near_time_concentration",
            "duplicates_near_similarity",
        ),
        how_to_read=(
            "Near-duplicate clusters group highly similar names that appear in related "
            "windows."
        ),
        what_to_look_for="Large or fast-forming clusters with high edge similarity scores.",
        common_benign_causes=(
            "Typos, OCR noise, and multilingual transliteration can inflate "
            "near-duplicate clusters."
        ),
    ),
    AnalysisDefinition(
        id="sortedness",
        title="Ordering / Sortedness",
        detector="sortedness",
        hero_chart_id="sortedness_bucket_ratio",
        detail_chart_ids=(
            "sortedness_bucket_summary",
            "sortedness_kendall_tau_summary",
            "sortedness_minute_spikes",
        ),
        how_to_read=(
            "Ordering metrics test whether names arrive in unusually sorted or monotonic "
            "patterns."
        ),
        what_to_look_for=(
            "Bucket ranges with elevated alphabetical ratios and repeated minute-level "
            "ordering spikes."
        ),
        common_benign_causes=(
            "Batch exports or admin processing can produce temporary ordering artifacts."
        ),
    ),
    AnalysisDefinition(
        id="rare_names",
        title="Rare / Unique Names",
        detector="rare_names",
        hero_chart_id="rare_names_unique_ratio",
        detail_chart_ids=(
            "rare_names_weird_scores",
            "rare_names_singletons",
            "rare_names_rarity_timeline",
        ),
        how_to_read=(
            "Unique-ratio and rarity indicators help identify sudden novelty surges in "
            "the name stream."
        ),
        what_to_look_for=(
            "Sustained unique-ratio lifts with concurrent weirdness-score concentration."
        ),
        common_benign_causes=(
            "Reference lookup gaps and nickname coverage gaps can overstate rarity."
        ),
    ),
    AnalysisDefinition(
        id="org_anomalies",
        title="Organization Field Anomalies",
        detector="org_anomalies",
        hero_chart_id="org_anomalies_blank_rate",
        detail_chart_ids=(
            "org_anomalies_position_rates",
            "org_anomalies_bursts",
            "org_anomalies_top_orgs",
        ),
        how_to_read="Track blank-organization share with Wilson intervals and per-position splits.",
        what_to_look_for=(
            "Blank-rate surges that persist across higher-volume windows and one position "
            "side."
        ),
        common_benign_causes=(
            "Form UX and campaign guidance often increase legitimate blank organization "
            "submissions."
        ),
    ),
    AnalysisDefinition(
        id="voter_registry_match",
        title="Registered Voter Match",
        detector="voter_registry_match",
        hero_chart_id="voter_registry_match_rates",
        detail_chart_ids=(
            "voter_registry_match_by_position",
            "voter_registry_match_tiers",
            "voter_registry_unmatched_names",
            "voter_registry_position_buckets",
        ),
        how_to_read=(
            "Voter linkage uses probabilistic tiers (exact/strong fuzzy/weak fuzzy/unmatched); "
            "interpret tier shifts with confidence and support context."
        ),
        what_to_look_for=(
            "Material and sustained tier-composition shifts with adequate support, especially when "
            "exact+strong tiers decline while weak/unmatched tiers increase."
        ),
        common_benign_causes=(
            "Name normalization variance and registration recency can reduce observed "
            "match rates."
        ),
    ),
    AnalysisDefinition(
        id="periodicity",
        title="Periodicity",
        detector="periodicity",
        hero_chart_id="periodicity_clockface",
        detail_chart_ids=(
            "periodicity_autocorr",
            "periodicity_spectrum",
            "periodicity_rolling_fano",
        ),
        how_to_read=(
            "Clock-face, autocorrelation, and spectrum views test for recurring timing "
            "patterns."
        ),
        what_to_look_for=(
            "Narrow periodic peaks and elevated rolling overdispersion that recur over "
            "long spans and align across methods."
        ),
        common_benign_causes=(
            "Calendar reminders and regular campaign sends can produce expected periodic "
            "structure."
        ),
    ),
    AnalysisDefinition(
        id="multivariate_anomalies",
        title="Multivariate Anomalies",
        detector="multivariate_anomalies",
        hero_chart_id="multivariate_score_timeline",
        detail_chart_ids=("multivariate_top_buckets", "multivariate_feature_projection"),
        how_to_read=(
            "Composite feature-space anomaly scoring identifies unusual bucket "
            "combinations."
        ),
        what_to_look_for=(
            "Consecutive high anomaly-score buckets supported by other detector flags."
        ),
        common_benign_causes=(
            "Correlated event shocks can move multiple features together without abuse."
        ),
    ),
    AnalysisDefinition(
        id="composite_score",
        title="Composite Evidence Score",
        detector="composite_score",
        hero_chart_id="composite_score_timeline",
        detail_chart_ids=("composite_evidence_flags", "composite_high_priority"),
        how_to_read=(
            "Composite score ranks windows by multi-detector agreement and evidence density."
        ),
        what_to_look_for=(
            "High-score windows with overlapping detector flags and strong local support."
        ),
        common_benign_causes=(
            "Major events can legitimately raise multiple detectors in the same period."
        ),
    ),
)

_ANALYSIS_DETECTOR_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "composite_score": (
        "bursts",
        "procon_swings",
        "changepoints",
        "rare_names",
        "multivariate_anomalies",
    ),
}


_ANALYSIS_GROUP_PRIORITY: dict[str, tuple[str, int]] = {
    "baseline_profile": ("baseline", 100),
    "bursts": ("window_signals", 95),
    "procon_swings": ("window_signals", 94),
    "changepoints": ("window_signals", 90),
    "off_hours": ("window_signals", 86),
    "duplicates_exact": ("identity_forensics", 88),
    "duplicates_near": ("identity_forensics", 87),
    "sortedness": ("process_signals", 80),
    "rare_names": ("identity_forensics", 82),
    "org_anomalies": ("field_quality", 78),
    "voter_registry_match": ("external_enrichment", 65),
    "periodicity": ("temporal_structure", 70),
    "multivariate_anomalies": ("multisignal", 92),
    "composite_score": ("triage", 99),
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
