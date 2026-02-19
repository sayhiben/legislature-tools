from __future__ import annotations

from testifier_audit.config import AppConfig
from testifier_audit.detectors.base import Detector
from testifier_audit.detectors.bursts import BurstsDetector
from testifier_audit.detectors.changepoints import ChangePointsDetector
from testifier_audit.detectors.composite_score import CompositeScoreDetector
from testifier_audit.detectors.duplicates_exact import DuplicatesExactDetector
from testifier_audit.detectors.duplicates_near import DuplicatesNearDetector
from testifier_audit.detectors.off_hours import OffHoursDetector
from testifier_audit.detectors.org_anomalies import OrganizationAnomaliesDetector
from testifier_audit.detectors.periodicity import PeriodicityDetector
from testifier_audit.detectors.procon_swings import ProConSwingsDetector
from testifier_audit.detectors.rare_names import RareNamesDetector
from testifier_audit.detectors.sortedness import SortednessDetector


def default_detectors(config: AppConfig) -> list[Detector]:
    detectors: list[Detector] = [
        DuplicatesExactDetector(top_n=config.thresholds.top_duplicate_names),
        DuplicatesNearDetector(
            similarity_threshold=config.thresholds.near_dup_similarity_threshold,
            max_candidates_per_block=config.thresholds.near_dup_max_candidates_per_block,
        ),
        BurstsDetector(
            window_minutes=config.windows.scan_window_minutes,
            fdr_alpha=config.thresholds.burst_fdr_alpha,
            calibration_enabled=config.calibration.enabled,
            calibration_mode=config.calibration.mode,
            significance_policy=config.calibration.significance_policy,
            calibration_iterations=config.calibration.iterations,
            calibration_seed=config.calibration.random_seed,
            calibration_support_alpha=config.calibration.support_alpha,
        ),
        ProConSwingsDetector(
            window_minutes=sorted(set(config.windows.scan_window_minutes + [config.windows.swing_window_minutes])),
            fdr_alpha=config.thresholds.procon_swing_fdr_alpha,
            min_window_total=config.thresholds.swing_min_window_total,
            calibration_enabled=config.calibration.enabled,
            calibration_mode=config.calibration.mode,
            significance_policy=config.calibration.significance_policy,
            calibration_iterations=config.calibration.iterations,
            calibration_seed=config.calibration.random_seed,
            calibration_support_alpha=config.calibration.support_alpha,
        ),
        OffHoursDetector(),
        SortednessDetector(),
        RareNamesDetector(
            min_window_total=config.thresholds.swing_min_window_total,
            rarity_enabled=config.rarity.enabled,
            first_name_frequency_path=config.rarity.first_name_frequency_path,
            last_name_frequency_path=config.rarity.last_name_frequency_path,
            rarity_epsilon=config.rarity.epsilon,
        ),
        OrganizationAnomaliesDetector(),
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
