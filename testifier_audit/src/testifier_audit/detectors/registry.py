from __future__ import annotations

from testifier_audit.config import AppConfig
from testifier_audit.detectors.base import Detector
from testifier_audit.detectors.bursts import BurstsDetector
from testifier_audit.detectors.duplicates_exact import DuplicatesExactDetector
from testifier_audit.detectors.off_hours import OffHoursDetector
from testifier_audit.detectors.org_anomalies import OrganizationAnomaliesDetector
from testifier_audit.detectors.voter_registry_match import VoterRegistryMatchDetector
from testifier_audit.io.hearing_metadata import load_hearing_metadata


def default_detectors(config: AppConfig) -> list[Detector]:
    bucket_minutes = sorted(
        {int(value) for value in config.windows.analysis_bucket_minutes if int(value) > 0}
    )
    configured_off_hours_buckets = config.off_hours.bucket_minutes or bucket_minutes
    off_hours_bucket_minutes = sorted(
        {int(value) for value in configured_off_hours_buckets if int(value) > 0}
    )
    if not off_hours_bucket_minutes:
        off_hours_bucket_minutes = list(bucket_minutes or [30])
    requested_primary_bucket = int(config.off_hours.primary_bucket_minutes)
    off_hours_primary_bucket = (
        requested_primary_bucket
        if requested_primary_bucket in off_hours_bucket_minutes
        else (30 if 30 in off_hours_bucket_minutes else off_hours_bucket_minutes[0])
    )
    hearing_committee = ""
    hearing_chamber = ""
    if config.input.hearing_metadata_path:
        try:
            hearing_metadata = load_hearing_metadata(config.input.hearing_metadata_path)
        except Exception:
            hearing_metadata = None
        if hearing_metadata is not None and isinstance(hearing_metadata.source, dict):
            hearing_committee = str(hearing_metadata.source.get("committee_name") or "").strip()
            hearing_chamber = str(hearing_metadata.source.get("chamber") or "").strip()
    detectors: list[Detector] = [
        VoterRegistryMatchDetector(
            enabled=config.voter_registry.enabled,
            db_url=config.voter_registry.db_url,
            table_name=config.voter_registry.table_name,
            bucket_minutes=sorted(
                set(bucket_minutes + [config.voter_registry.match_bucket_minutes])
            ),
            active_only=config.voter_registry.active_only,
            primary_match_mode=config.voter_registry.primary_match_mode,
            strong_fuzzy_min_score=config.voter_registry.strong_fuzzy_min_score,
            weak_fuzzy_min_score=config.voter_registry.weak_fuzzy_min_score,
            ambiguous_score_gap=config.voter_registry.ambiguous_score_gap,
            pairwise_alpha=config.voter_registry.pairwise_alpha,
            nickname_map_path=config.names.nickname_map_path,
            status_mode=config.voter_registry.status_mode,
            registry_snapshot_date=config.voter_registry.registry_snapshot_date,
        ),
        DuplicatesExactDetector(
            top_n=config.thresholds.top_duplicate_names,
            bucket_minutes=bucket_minutes,
            primary_name_key=config.name_analysis.primary_name_key,
            sensitivity_name_keys=list(config.name_analysis.sensitivity_name_keys),
            collision_metrics=list(config.name_analysis.collision_metrics),
            collision_primary_metric=config.name_analysis.collision_primary_metric,
            collision_key_mode=config.name_analysis.collision_key_mode,
            collision_baseline_source=config.name_analysis.collision_baseline_source,
            collision_baseline_model=config.name_analysis.collision_baseline_model,
            collision_uncertainty_mode=config.name_analysis.collision_uncertainty_mode,
            collision_scope_primary=config.name_analysis.collision_scope_primary,
            collision_scope_overlays=list(config.name_analysis.collision_scope_overlays),
            collision_baseline_failure_policy=config.name_analysis.collision_baseline_failure_policy,
            collision_stratification=config.name_analysis.collision_stratification,
            per_name_significance_model=config.name_analysis.per_name_significance_model,
            per_name_display_limit=config.name_analysis.per_name_display_limit,
            exclude_non_person_from_inference=config.name_analysis.exclude_non_person_from_inference,
            monte_carlo_draws=config.name_analysis.monte_carlo_draws,
            position_permutation_draws=config.name_analysis.position_permutation_draws,
            temporal_permutation_draws=config.name_analysis.temporal_permutation_draws,
            bh_fdr_q=config.name_analysis.bh_fdr_q,
            low_power_min_unique_names=config.name_analysis.low_power_min_unique_names,
            low_power_min_expected_duplicates=config.name_analysis.low_power_min_expected_duplicates,
            max_per_name_rows=config.name_analysis.max_per_name_rows,
            position_hearing_baseline_enabled=config.name_analysis.position_hearing_baseline_enabled,
            position_baseline_shrink_k=config.name_analysis.position_baseline_shrink_k,
            position_interval_nominal=config.name_analysis.position_interval_nominal,
            position_interval_draws=config.name_analysis.position_interval_draws,
            position_claim_min_rows_per_position=(
                config.name_analysis.position_claim_min_rows_per_position
            ),
            contextual_baseline_path=config.name_analysis.contextual_baseline_path,
            contextual_committee=hearing_committee,
            contextual_chamber=hearing_chamber,
            voter_db_url=config.voter_registry.db_url,
            voter_table_name=config.voter_registry.table_name,
            voter_active_only=config.voter_registry.active_only,
        ),
        BurstsDetector(
            window_minutes=sorted(set(config.windows.scan_window_minutes + bucket_minutes)),
            fdr_alpha=config.thresholds.burst_fdr_alpha,
            calibration_enabled=config.calibration.enabled,
            calibration_mode=config.calibration.mode,
            significance_policy=config.calibration.significance_policy,
            calibration_iterations=config.calibration.iterations,
            calibration_seed=config.calibration.random_seed,
            calibration_support_alpha=config.calibration.support_alpha,
        ),
        OffHoursDetector(
            bucket_minutes=off_hours_bucket_minutes,
            min_window_total=int(config.off_hours.min_window_total),
            fdr_alpha=float(config.off_hours.fdr_alpha),
            primary_bucket_minutes=int(off_hours_primary_bucket),
            model_min_rows=int(config.off_hours.model_min_rows),
            model_hour_harmonics=int(config.off_hours.model_hour_harmonics),
            alert_off_hours_min_fraction=float(config.off_hours.alert_off_hours_min_fraction),
            primary_alert_min_abs_delta=float(config.off_hours.primary_alert_min_abs_delta),
        ),
        OrganizationAnomaliesDetector(bucket_minutes=bucket_minutes),
    ]
    return detectors
