from __future__ import annotations

from testifier_audit.config import AppConfig
from testifier_audit.detectors.base import Detector
from testifier_audit.detectors.bursts import BurstsDetector
from testifier_audit.detectors.changepoints import ChangePointsDetector
from testifier_audit.detectors.composite_score import CompositeScoreDetector
from testifier_audit.detectors.duplicates_exact import DuplicatesExactDetector
from testifier_audit.detectors.duplicates_near import DuplicatesNearDetector
from testifier_audit.detectors.multivariate_anomalies import MultivariateAnomaliesDetector
from testifier_audit.detectors.off_hours import OffHoursDetector
from testifier_audit.detectors.org_anomalies import OrganizationAnomaliesDetector
from testifier_audit.detectors.periodicity import PeriodicityDetector
from testifier_audit.detectors.procon_swings import ProConSwingsDetector
from testifier_audit.detectors.rare_names import RareNamesDetector
from testifier_audit.detectors.sortedness import SortednessDetector
from testifier_audit.detectors.voter_registry_match import VoterRegistryMatchDetector


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
            voter_db_url=config.voter_registry.db_url,
            voter_table_name=config.voter_registry.table_name,
            voter_active_only=config.voter_registry.active_only,
        ),
        DuplicatesNearDetector(
            similarity_threshold=config.thresholds.near_dup_similarity_threshold,
            max_candidates_per_block=config.thresholds.near_dup_max_candidates_per_block,
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
        ProConSwingsDetector(
            window_minutes=sorted(
                set(config.windows.scan_window_minutes + [config.windows.swing_window_minutes])
            ),
            fdr_alpha=config.thresholds.procon_swing_fdr_alpha,
            min_window_total=config.thresholds.swing_min_window_total,
            calibration_enabled=config.calibration.enabled,
            calibration_mode=config.calibration.mode,
            significance_policy=config.calibration.significance_policy,
            calibration_iterations=config.calibration.iterations,
            calibration_seed=config.calibration.random_seed,
            calibration_support_alpha=config.calibration.support_alpha,
            profile_bucket_minutes=sorted(
                set(bucket_minutes + ProConSwingsDetector.DEFAULT_PROFILE_BUCKET_MINUTES)
            ),
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
        SortednessDetector(bucket_minutes=bucket_minutes),
        RareNamesDetector(
            min_window_total=config.thresholds.swing_min_window_total,
            rarity_enabled=config.rarity.enabled,
            first_name_frequency_path=config.rarity.first_name_frequency_path,
            last_name_frequency_path=config.rarity.last_name_frequency_path,
            rarity_epsilon=config.rarity.epsilon,
            bucket_minutes=bucket_minutes,
        ),
        OrganizationAnomaliesDetector(bucket_minutes=bucket_minutes),
        MultivariateAnomaliesDetector(
            enabled=config.multivariate_anomaly.enabled,
            bucket_minutes=sorted(
                set(bucket_minutes + [config.multivariate_anomaly.bucket_minutes])
            ),
            contamination=config.multivariate_anomaly.contamination,
            min_bucket_total=config.multivariate_anomaly.min_bucket_total,
            top_n=config.multivariate_anomaly.top_n,
            random_seed=config.multivariate_anomaly.random_seed,
        ),
        CompositeScoreDetector(),
    ]
    if config.periodicity.enabled:
        detectors.insert(
            5 if config.changepoints.enabled else 4,
            PeriodicityDetector(
                max_lag_minutes=config.periodicity.max_lag_minutes,
                min_period_minutes=config.periodicity.min_period_minutes,
                max_period_minutes=config.periodicity.max_period_minutes,
                top_n_periods=config.periodicity.top_n_periods,
                calibration_iterations=config.periodicity.calibration_iterations,
                calibration_seed=config.periodicity.random_seed,
                fdr_alpha=config.periodicity.fdr_alpha,
                rolling_fano_windows=sorted(
                    set(bucket_minutes + list(PeriodicityDetector.DEFAULT_ROLLING_FANO_WINDOWS))
                ),
            ),
        )
    if config.changepoints.enabled:
        detectors.insert(
            4,
            ChangePointsDetector(
                min_segment_minutes=config.changepoints.min_segment_minutes,
                penalty_scale=config.changepoints.penalty_scale,
            ),
        )
    return detectors
