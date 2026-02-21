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
        }


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
        detail_chart_ids=("bursts_significance_by_window", "bursts_null_distribution"),
        how_to_read="Burst windows compare observed local volume to expected background volume.",
        what_to_look_for=(
            "Repeated high-rate-ratio windows at multiple window sizes rather than "
            "isolated spikes."
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
            "procon_swings_null_distribution",
        ),
        how_to_read=(
            "Track pro-rate relative to stable bands and baseline while preserving "
            "per-bucket uncertainty context."
        ),
        what_to_look_for=(
            "Sustained directional ratio changes across neighboring buckets and repeated "
            "dayparts."
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
        hero_chart_id="off_hours_hourly_profile",
        detail_chart_ids=("off_hours_summary_compare",),
        how_to_read=(
            "Compare hourly volume and pro-rate with Wilson uncertainty and low-power "
            "flags."
        ),
        what_to_look_for=(
            "Consistent off-hours elevation in volume or composition beyond daytime "
            "baselines."
        ),
        common_benign_causes=(
            "Statewide campaigns spanning time zones can shift participation into "
            "late-hour windows."
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
        detail_chart_ids=("duplicates_near_cluster_size", "duplicates_near_similarity"),
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
        detail_chart_ids=("sortedness_bucket_summary", "sortedness_minute_spikes"),
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
            "voter_registry_unmatched_names",
            "voter_registry_position_buckets",
        ),
        how_to_read=(
            "Match-rate trends are volume-weighted and should be interpreted with Wilson "
            "confidence width."
        ),
        what_to_look_for=(
            "Material and sustained match-rate departures with adequate per-bucket support."
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
        detail_chart_ids=("periodicity_autocorr", "periodicity_spectrum"),
        how_to_read=(
            "Clock-face, autocorrelation, and spectrum views test for recurring timing "
            "patterns."
        ),
        what_to_look_for=(
            "Narrow periodic peaks that recur over long spans and align across methods."
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


def default_analysis_definitions() -> list[dict[str, Any]]:
    return [definition.to_dict() for definition in _ANALYSIS_DEFINITIONS]


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
