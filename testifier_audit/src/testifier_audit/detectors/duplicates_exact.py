from __future__ import annotations

import json
import logging
import math
from hashlib import sha1
from pathlib import Path
from time import perf_counter
from typing import Literal

import numpy as np
import pandas as pd
from scipy.stats import binom, hypergeom

from testifier_audit.detectors.base import Detector, DetectorResult
from testifier_audit.io.vrdb_postgres import (
    fetch_matching_voter_keys,
    fetch_voter_name_key_count_histogram,
    fetch_voter_name_key_frequencies,
    fetch_voter_name_key_stratum_frequencies,
)
from testifier_audit.names.collision_baseline import (
    COLLISION_METRICS,
    collision_metrics_from_counts,
    expected_collision_metrics,
    expected_collision_metrics_from_probabilities,
    histogram_from_name_counts,
    histogram_from_probabilities,
    simulate_collision_null_from_histogram,
    summarize_collision_observed_vs_null,
)
from testifier_audit.names.normalization import normalization_version, normalization_version_hash
from testifier_audit.names.stat_tests import (
    benjamini_hochberg,
    binomial_tail_p_value,
    hypergeometric_tail_p_value,
)
from testifier_audit.profiling import (
    profile_runtime_block,
    record_runtime_counter,
    record_runtime_timing,
)

CollisionBaselineModel = Literal["multinomial", "hypergeometric"]
CollisionBaselineSource = Literal["vrdb_full_histogram", "vrdb_full_keys", "hearing_empirical"]
CollisionScope = Literal["matched_only", "full_hearing", "unmatched_only"]
TemporalNullMode = Literal["hearing_intensity", "hearing_intensity_by_position"]

_KEY_TO_COLUMN = {
    "strict": "collision_key_strict",
    "medium": "collision_key_medium",
    "loose": "collision_key_loose",
    "nickname": "canonical_key_nickname",
}
_MATCHED_OUTCOMES = {"matched_unique", "matched_ambiguous"}
_ALLOWED_STRATIFICATIONS = {"none", "birth_decade"}
_TOP_NAME_TIMING_MATCH_MODES: tuple[dict[str, str], ...] = (
    {
        "match_mode": "strict",
        "key_column": "collision_key_medium",
        "match_label": "Strict (last + first)",
        "match_definition": "Exact match on last-name and first-name tokens.",
    },
    {
        "match_mode": "loose",
        "key_column": "canonical_key_nickname",
        "match_label": "Loose (last + nickname-root first)",
        "match_definition": (
            "Matches last-name exactly and applies nickname equivalence to first-name tokens only."
        ),
    },
)
_TOP_NAME_TIMING_TOP_N = 200
LOGGER = logging.getLogger(__name__)


def _safe_str_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str)


def _uses_default_binomial_tail() -> bool:
    return (
        getattr(binomial_tail_p_value, "__module__", "") == "testifier_audit.names.stat_tests"
        and getattr(binomial_tail_p_value, "__name__", "") == "binomial_tail_p_value"
    )


def _uses_default_hypergeometric_tail() -> bool:
    return (
        getattr(hypergeometric_tail_p_value, "__module__", "") == "testifier_audit.names.stat_tests"
        and getattr(hypergeometric_tail_p_value, "__name__", "") == "hypergeometric_tail_p_value"
    )


class DuplicatesExactDetector(Detector):
    name = "duplicates_exact"
    DEFAULT_BUCKET_MINUTES = [1, 5, 15, 30, 60, 120, 240, 480, 720, 1440]
    RNG_STREAM_SCOPE_COLLISION = "scope_collision"
    RNG_STREAM_SCOPE_STRATIFIED_COLLISION = "scope_stratified_collision"
    RNG_STREAM_BUCKET_COLLISION = "bucket_collision"
    RNG_STREAM_BUCKET_STRATIFIED_COLLISION = "bucket_stratified_collision"
    RNG_STREAM_POSITION_INTERVAL = "position_interval"
    RNG_STREAM_POSITION_PERMUTATION = "position_permutation"
    RNG_STREAM_POSITION_CLUSTER_BOOTSTRAP = "position_cluster_bootstrap"
    RNG_STREAM_TEMPORAL_PERMUTATION = "temporal_permutation"
    RNG_STREAM_NAMES = (
        RNG_STREAM_SCOPE_COLLISION,
        RNG_STREAM_SCOPE_STRATIFIED_COLLISION,
        RNG_STREAM_BUCKET_COLLISION,
        RNG_STREAM_BUCKET_STRATIFIED_COLLISION,
        RNG_STREAM_POSITION_INTERVAL,
        RNG_STREAM_POSITION_PERMUTATION,
        RNG_STREAM_POSITION_CLUSTER_BOOTSTRAP,
        RNG_STREAM_TEMPORAL_PERMUTATION,
    )
    STATISTICAL_CONTRACT_ESTIMAND_PRIMARY = (
        "name-key collision burden relative to reference baseline"
    )
    STATISTICAL_CONTRACT_NON_GOALS = (
        "cannot infer identity, intent, IP-based behavior, or per-person duplication from "
        "the public dataset"
    )
    STATISTICAL_CONTRACT_BASELINE_SEMANTICS = (
        "reference model; not the data-generating process"
    )
    COLLISION_CLAIM_CLASS = "collision_signal"
    SCOPE_STATUS_AVAILABLE = "available"
    SCOPE_STATUS_UNAVAILABLE = "unavailable"
    SCOPE_REASON_AVAILABLE = "available"
    SCOPE_REASON_UNAVAILABLE_MISSING_MATCH_ASSIGNMENTS = "unavailable_missing_match_assignments"
    SCOPE_REASON_UNAVAILABLE_NO_PERSON_ROWS = "unavailable_no_person_rows"
    SCOPE_REASON_UNAVAILABLE_NO_ROWS_AFTER_FILTERING = "unavailable_no_rows_after_filtering"
    INFERENTIAL_REASON_REFERENCE_MODEL_INFERENCE = "reference_model_inference_available"
    INFERENTIAL_REASON_SELF_REFERENTIAL_BASELINE = "self_referential_baseline"
    INFERENTIAL_REASON_DEGRADED_TO_SELF_REFERENTIAL = "degraded_to_self_referential_baseline"
    INFERENTIAL_REASON_NO_NULL_SAMPLES = "analytic_only_no_null_samples"
    INFERENTIAL_REASON_LOW_POWER = "low_power_support"
    INFERENTIAL_REASON_SCOPE_UNAVAILABLE = "scope_unavailable"
    POSITION_INTERVAL_METHOD_ID = "position_duplicate_interval_multinomial_mc_v1"
    POSITION_RATE_DIFF_INTERVAL_METHOD_ID = "position_rate_difference_cluster_bootstrap_v1"
    POSITION_PERMUTATION_TEST_ID = "position_rate_difference_permutation_abs_two_sided_v1"
    TEMPORAL_NULL_MODE_HEARING_INTENSITY = "hearing_intensity"
    TEMPORAL_NULL_MODE_HEARING_INTENSITY_BY_POSITION = "hearing_intensity_by_position"
    TEMPORAL_NULL_SUPPORT_REASON_SUPPORTED = "supported"
    TEMPORAL_NULL_SUPPORT_REASON_NAME_NOT_GATED = "name_not_family_c_discovery"
    TEMPORAL_NULL_SUPPORT_REASON_POSITION_POOL_SPARSE = "position_pool_sparse"
    POSITION_CLAIM_REASON_ELIGIBLE = "eligible"
    POSITION_CLAIM_REASON_UNSUPPORTED_MODEL = "unsupported_collision_baseline_model"
    POSITION_CLAIM_REASON_NO_POSITION_ROWS = "no_position_rows"
    POSITION_CLAIM_REASON_INSUFFICIENT_SUPPORT = "insufficient_position_support"
    POSITION_CLAIM_REASON_INTERVAL_UNAVAILABLE = "position_interval_unavailable"
    PRIMARY_SCOPE_ENDPOINT_METRIC = "excess_rows"
    FAMILY_ID_SCOPE = "A_scope_excess_rows"
    FAMILY_ID_BUCKET = "B_bucket_follow_up"
    FAMILY_ID_PER_NAME = "C_per_name_upper_tail"
    FAMILY_ID_TEMPORAL = "D_temporal_follow_up"
    FAMILY_ID_POSITION = "E_position_follow_up"
    ADJUSTMENT_METHOD_HOLM = "holm"
    ADJUSTMENT_METHOD_BH = "benjamini_hochberg"
    ADJUSTMENT_METHOD_BY = "benjamini_yekutieli"
    ADJUSTMENT_METHOD_NONE = "none"
    GATE_REASON_ELIGIBLE = "eligible"
    GATE_REASON_SCOPE_UNAVAILABLE = "scope_unavailable"
    GATE_REASON_SCOPE_NOT_INFERENTIAL = "scope_not_inferential"
    GATE_REASON_FAMILY_A_NOT_SIGNIFICANT = "family_a_not_significant"
    GATE_REASON_FAMILY_A_NOT_TESTED = "family_a_not_tested"
    GATE_REASON_SECONDARY_SCOPE_METRIC = "secondary_scope_metric"
    GATE_REASON_SECONDARY_BUCKET_METRIC = "secondary_bucket_metric"
    GATE_REASON_NO_FAMILY_C_DISCOVERIES = "no_family_c_discoveries"
    GATE_REASON_NAME_NOT_FAMILY_C_DISCOVERY = "name_not_family_c_discovery"
    GATE_REASON_TEMPORAL_NULL_UNSUPPORTED = "temporal_null_not_supportable"
    INFERENTIAL_REASON_SECONDARY_SCOPE_METRIC = "secondary_scope_metric_descriptive"
    INFERENTIAL_REASON_SECONDARY_BUCKET_METRIC = "secondary_bucket_metric_descriptive"
    INFERENTIAL_REASON_FAMILY_A_GATE_NOT_PASSED = "family_a_gate_not_passed"
    INFERENTIAL_REASON_FAMILY_C_GATE_NOT_PASSED = "family_c_gate_not_passed"
    INFERENTIAL_REASON_TEMPORAL_NULL_UNSUPPORTED = "temporal_null_not_supportable"

    def __init__(
        self,
        top_n: int,
        bucket_minutes: list[int] | None = None,
        *,
        primary_name_key: str = "medium",
        sensitivity_name_keys: list[str] | None = None,
        collision_metrics: list[str] | None = None,
        collision_primary_metric: str = "repeated_group_rows",
        collision_key_mode: str = "strict",
        collision_baseline_source: str = "hearing_empirical",
        collision_baseline_model: str = "multinomial",
        collision_uncertainty_mode: str = "monte_carlo",
        collision_scope_primary: str = "full_hearing",
        collision_scope_overlays: list[str] | None = None,
        collision_baseline_failure_policy: str = "degrade",
        collision_stratification: str = "none",
        per_name_significance_model: str = "binomial_tail",
        per_name_display_limit: int = 1000,
        exclude_non_person_from_inference: bool = True,
        monte_carlo_draws: int = 20_000,
        monte_carlo_min_draws: int = 128,
        monte_carlo_target_p_mcse: float = 0.01,
        monte_carlo_decision_p_threshold: float = 0.05,
        monte_carlo_decision_confidence_level: float = 0.95,
        position_permutation_draws: int = 10_000,
        temporal_permutation_draws: int = 5_000,
        temporal_null_mode: str = "hearing_intensity",
        bh_fdr_q: float = 0.10,
        low_power_min_unique_names: int = 25,
        low_power_min_expected_duplicates: float = 5.0,
        max_per_name_rows: int = 1000,
        position_hearing_baseline_enabled: bool = True,
        position_baseline_shrink_k: float = 30.0,
        position_interval_nominal: float = 0.95,
        position_interval_draws: int = 5000,
        position_cluster_bootstrap_draws: int = 1000,
        position_claim_min_rows_per_position: int = 25,
        contextual_baseline_path: str | None = None,
        contextual_committee: str = "",
        contextual_chamber: str = "",
        nickname_map_path: str | None = None,
        normalize_unicode: bool = True,
        strip_punctuation: bool = True,
        voter_db_url: str | None = None,
        voter_table_name: str = "voter_registry",
        voter_active_only: bool = True,
        random_seed: int = 42,
    ) -> None:
        self.top_n = int(top_n)
        buckets = bucket_minutes or self.DEFAULT_BUCKET_MINUTES
        self.bucket_minutes = sorted({int(value) for value in buckets if int(value) > 0})
        self.primary_name_key = str(primary_name_key or "medium").strip().lower()
        self.sensitivity_name_keys = [
            str(value).strip().lower() for value in (sensitivity_name_keys or ["strict", "nickname"])
        ]
        metrics = [str(value or "").strip().lower() for value in (collision_metrics or COLLISION_METRICS)]
        self.collision_metrics = [metric for metric in metrics if metric in COLLISION_METRICS]
        if not self.collision_metrics:
            self.collision_metrics = list(COLLISION_METRICS)
        primary_metric = str(collision_primary_metric or "repeated_group_rows").strip().lower()
        self.collision_primary_metric = (
            primary_metric if primary_metric in self.collision_metrics else self.collision_metrics[0]
        )
        self.collision_key_mode = str(collision_key_mode or "strict").strip().lower()
        source = str(collision_baseline_source or "hearing_empirical").strip().lower()
        self.collision_baseline_source: CollisionBaselineSource = (
            source if source in {"vrdb_full_histogram", "vrdb_full_keys", "hearing_empirical"} else "hearing_empirical"
        )
        model = str(collision_baseline_model or "multinomial").strip().lower()
        self.collision_baseline_model: CollisionBaselineModel = (
            model if model in {"multinomial", "hypergeometric"} else "multinomial"
        )
        uncertainty = str(collision_uncertainty_mode or "monte_carlo").strip().lower()
        self.collision_uncertainty_mode = uncertainty if uncertainty in {"monte_carlo", "analytic_only"} else "monte_carlo"
        primary_scope = str(collision_scope_primary or "full_hearing").strip().lower()
        self.collision_scope_primary: CollisionScope = (
            primary_scope if primary_scope in {"matched_only", "full_hearing", "unmatched_only"} else "full_hearing"
        )
        overlays = [str(value or "").strip().lower() for value in (collision_scope_overlays or [])]
        self.collision_scope_overlays = [
            value
            for value in overlays
            if value in {"matched_only", "full_hearing", "unmatched_only"} and value != self.collision_scope_primary
        ]
        self.collision_baseline_failure_policy = (
            "fail"
            if str(collision_baseline_failure_policy or "degrade").strip().lower() == "fail"
            else "degrade"
        )
        self.collision_stratification = str(collision_stratification or "none").strip().lower() or "none"
        if self.collision_stratification not in _ALLOWED_STRATIFICATIONS:
            allowed = ", ".join(sorted(_ALLOWED_STRATIFICATIONS))
            raise ValueError(
                f"Unsupported collision_stratification={self.collision_stratification!r}; "
                f"expected one of: {allowed}."
            )
        sig_model = str(per_name_significance_model or "binomial_tail").strip().lower()
        self.per_name_significance_model = (
            sig_model if sig_model in {"binomial_tail", "hypergeometric_tail"} else "binomial_tail"
        )
        self.per_name_display_limit = max(10, int(per_name_display_limit))
        self.exclude_non_person_from_inference = bool(exclude_non_person_from_inference)
        self.monte_carlo_draws = max(100, int(monte_carlo_draws))
        self.monte_carlo_min_draws = max(1, int(monte_carlo_min_draws))
        target_mcse = float(monte_carlo_target_p_mcse)
        if not math.isfinite(target_mcse) or target_mcse <= 0.0:
            target_mcse = 0.01
        self.monte_carlo_target_p_mcse = target_mcse
        threshold = float(monte_carlo_decision_p_threshold)
        if not math.isfinite(threshold):
            threshold = 0.05
        self.monte_carlo_decision_p_threshold = float(min(max(threshold, 0.0), 1.0))
        confidence = float(monte_carlo_decision_confidence_level)
        if not math.isfinite(confidence):
            confidence = 0.95
        self.monte_carlo_decision_confidence_level = float(min(max(confidence, 0.0), 1.0))
        self.position_permutation_draws = max(100, int(position_permutation_draws))
        self.temporal_permutation_draws = max(100, int(temporal_permutation_draws))
        temporal_null_mode_normalized = str(temporal_null_mode or "").strip().lower()
        if temporal_null_mode_normalized not in {
            self.TEMPORAL_NULL_MODE_HEARING_INTENSITY,
            self.TEMPORAL_NULL_MODE_HEARING_INTENSITY_BY_POSITION,
        }:
            temporal_null_mode_normalized = self.TEMPORAL_NULL_MODE_HEARING_INTENSITY
        self.temporal_null_mode: TemporalNullMode = temporal_null_mode_normalized
        self.bh_fdr_q = float(min(max(bh_fdr_q, 0.0), 1.0))
        self.low_power_min_unique_names = max(1, int(low_power_min_unique_names))
        self.low_power_min_expected_duplicates = float(max(low_power_min_expected_duplicates, 0.0))
        self.max_per_name_rows = max(10, int(max_per_name_rows))
        self.position_hearing_baseline_enabled = bool(position_hearing_baseline_enabled)
        self.position_baseline_shrink_k = float(max(float(position_baseline_shrink_k), 0.0))
        nominal = float(position_interval_nominal)
        if not math.isfinite(nominal):
            nominal = 0.95
        self.position_interval_nominal = float(min(max(nominal, 1e-6), 1.0 - 1e-6))
        self.position_interval_draws = max(100, int(position_interval_draws))
        self.position_cluster_bootstrap_draws = max(100, int(position_cluster_bootstrap_draws))
        self.position_claim_min_rows_per_position = max(1, int(position_claim_min_rows_per_position))
        self.contextual_baseline_path = str(contextual_baseline_path or "").strip()
        self.contextual_committee = str(contextual_committee or "").strip()
        self.contextual_chamber = str(contextual_chamber or "").strip()
        self.nickname_map_path = str(nickname_map_path or "").strip()
        self.normalize_unicode = bool(normalize_unicode)
        self.strip_punctuation = bool(strip_punctuation)
        self.voter_db_url = voter_db_url
        self.voter_table_name = voter_table_name
        self.voter_active_only = bool(voter_active_only)
        self.random_seed = int(random_seed)

    @classmethod
    def _baseline_label_for_source(cls, source: str) -> str:
        source_norm = str(source or "").strip().lower()
        if source_norm in {"vrdb_full_histogram", "vrdb_full_keys"}:
            return "Statewide registry reference baseline"
        if source_norm == "hearing_empirical":
            return "Same-hearing empirical baseline"
        return "Reference baseline"

    @classmethod
    def _scope_inferential_status(cls, baseline_source: str) -> str:
        status, _reason = cls._scope_inferential_metadata(
            baseline_source=baseline_source,
            baseline_degraded=False,
            null_samples=pd.DataFrame({"pairs": [0.0]}),
        )
        return status

    @classmethod
    def _scope_inferential_metadata(
        cls,
        *,
        baseline_source: str,
        baseline_degraded: bool,
        null_samples: pd.DataFrame,
    ) -> tuple[str, str]:
        source_norm = str(baseline_source or "").strip().lower()
        if source_norm == "hearing_empirical":
            if baseline_degraded:
                return (
                    "descriptive_only",
                    cls.INFERENTIAL_REASON_DEGRADED_TO_SELF_REFERENTIAL,
                )
            return (
                "descriptive_only",
                cls.INFERENTIAL_REASON_SELF_REFERENTIAL_BASELINE,
            )
        if null_samples.empty:
            return ("unavailable", cls.INFERENTIAL_REASON_NO_NULL_SAMPLES)
        return ("reference_model_inference", cls.INFERENTIAL_REASON_REFERENCE_MODEL_INFERENCE)

    @staticmethod
    def _duplicate_rows_for_subset(working: pd.DataFrame, key_column: str) -> tuple[int, int]:
        if working.empty:
            return (0, 0)
        counts = working.groupby(key_column, dropna=False).size().to_numpy(dtype=float)
        duplicate_rows = int(counts[counts >= 2.0].sum())
        return duplicate_rows, int(len(working))

    @staticmethod
    def _duplicate_rows_for_factorized_subset(
        *,
        key_ids: np.ndarray,
        subset_mask: np.ndarray,
        n_keys: int,
    ) -> tuple[int, int]:
        if key_ids.size == 0 or subset_mask.size == 0 or n_keys <= 0:
            return (0, 0)
        subset_ids = key_ids[subset_mask]
        if subset_ids.size == 0:
            return (0, 0)
        valid_subset_ids = subset_ids[subset_ids >= 0]
        if valid_subset_ids.size == 0:
            return (0, 0)
        counts = np.bincount(valid_subset_ids, minlength=n_keys)
        duplicate_rows = int(counts[counts >= 2].sum())
        return duplicate_rows, int(valid_subset_ids.size)

    def _collision_monte_carlo_draw_budget(self, *, n_rows: int, hard_cap: int) -> int:
        if int(max(int(n_rows), 0)) <= 1:
            return 0
        requested = int(min(max(int(self.monte_carlo_draws), 0), max(int(hard_cap), 0)))
        if requested <= 0:
            return 0
        return int(requested)

    def _bucket_monte_carlo_draw_budget(
        self,
        *,
        n_rows: int,
        expected_primary_metric: float,
        hard_cap: int,
    ) -> int:
        n = int(max(int(n_rows), 0))
        if n <= 1:
            return 0
        # Buckets that are guaranteed low-power should avoid expensive null simulation.
        if n < self.low_power_min_unique_names:
            return 0
        if float(expected_primary_metric) < self.low_power_min_expected_duplicates:
            return 0
        return int(min(max(int(self.monte_carlo_draws), 0), max(int(hard_cap), 0)))

    def _position_interval_bounds(self) -> tuple[float, float]:
        alpha = 1.0 - float(self.position_interval_nominal)
        lower = max(0.0, min(0.5, alpha / 2.0))
        upper = min(1.0, max(0.5, 1.0 - lower))
        return float(lower), float(upper)

    def _position_interval_from_histogram(
        self,
        *,
        n_rows: int,
        histogram: pd.DataFrame,
        n_population: int | None,
        rng: np.random.Generator,
    ) -> dict[str, float | int]:
        n = int(max(int(n_rows), 0))
        expected_rows = float(
            expected_collision_metrics(
                n_rows=n,
                histogram=histogram,
                baseline_model=self.collision_baseline_model,
                n_population=n_population if (n_population or 0) > 0 else None,
            ).get("repeated_group_rows", 0.0)
        )
        if n <= 0:
            return {
                "expected_duplicate_rows": 0.0,
                "expected_duplicate_rows_p05": 0.0,
                "expected_duplicate_rows_p50": 0.0,
                "expected_duplicate_rows_p95": 0.0,
                "expected_duplicate_row_rate": 0.0,
                "expected_duplicate_row_rate_p05": 0.0,
                "expected_duplicate_row_rate_p50": 0.0,
                "expected_duplicate_row_rate_p95": 0.0,
                "interval_draws_effective": 0,
            }

        p05_rows = expected_rows
        p50_rows = expected_rows
        p95_rows = expected_rows
        draws_effective = 0
        if self.collision_baseline_model == "multinomial":
            null_samples = simulate_collision_null_from_histogram(
                n_rows=n,
                histogram=histogram,
                draws=self.position_interval_draws,
                rng=rng,
                baseline_model="multinomial",
                n_population=n_population if (n_population or 0) > 0 else None,
                max_draws=self.position_interval_draws,
                min_draws=self.position_interval_draws,
                target_p_mcse=float("nan"),
            )
            repeated_rows = (
                pd.to_numeric(
                    null_samples.get("repeated_group_rows", pd.Series(dtype=float)),
                    errors="coerce",
                )
                .dropna()
                .to_numpy(dtype=float)
            )
            draws_effective = int(repeated_rows.size)
            if repeated_rows.size > 0:
                quantile_low, quantile_high = self._position_interval_bounds()
                p05_rows = float(np.quantile(repeated_rows, quantile_low))
                p50_rows = float(np.quantile(repeated_rows, 0.5))
                p95_rows = float(np.quantile(repeated_rows, quantile_high))

        p05_rows = float(max(0.0, min(p05_rows, float(n))))
        p50_rows = float(max(0.0, min(p50_rows, float(n))))
        p95_rows = float(max(0.0, min(p95_rows, float(n))))
        ordered_rows = sorted([p05_rows, p50_rows, p95_rows])
        p05_rows = float(ordered_rows[0])
        p50_rows = float(ordered_rows[1])
        p95_rows = float(ordered_rows[2])
        row_rate_scale = float(n)

        return {
            "expected_duplicate_rows": float(expected_rows),
            "expected_duplicate_rows_p05": p05_rows,
            "expected_duplicate_rows_p50": p50_rows,
            "expected_duplicate_rows_p95": p95_rows,
            "expected_duplicate_row_rate": float(expected_rows / row_rate_scale),
            "expected_duplicate_row_rate_p05": float(p05_rows / row_rate_scale),
            "expected_duplicate_row_rate_p50": float(p50_rows / row_rate_scale),
            "expected_duplicate_row_rate_p95": float(p95_rows / row_rate_scale),
            "interval_draws_effective": int(draws_effective),
        }

    def _position_claim_status(
        self,
        *,
        position_metrics: pd.DataFrame,
    ) -> tuple[bool, str]:
        if self.collision_baseline_model != "multinomial":
            return False, self.POSITION_CLAIM_REASON_UNSUPPORTED_MODEL
        if position_metrics.empty:
            return False, self.POSITION_CLAIM_REASON_NO_POSITION_ROWS
        if bool(position_metrics["is_low_power"].astype(bool).any()):
            return False, self.POSITION_CLAIM_REASON_INSUFFICIENT_SUPPORT
        if int(position_metrics["interval_draws_effective"].fillna(0).max()) <= 0:
            return False, self.POSITION_CLAIM_REASON_INTERVAL_UNAVAILABLE
        return True, self.POSITION_CLAIM_REASON_ELIGIBLE

    def _cluster_bootstrap_rate_difference(
        self,
        *,
        pro_counts_observed: np.ndarray,
        con_counts_observed: np.ndarray,
        rng: np.random.Generator,
        n_bootstrap_draws: int | None = None,
    ) -> tuple[float, float, float, int]:
        n_keys = int(len(pro_counts_observed))
        if n_keys <= 0:
            return 0.0, 0.0, 0.0, 0

        pro_counts = np.asarray(pro_counts_observed, dtype=np.int64)
        con_counts = np.asarray(con_counts_observed, dtype=np.int64)
        pro_total_observed = int(pro_counts.sum())
        con_total_observed = int(con_counts.sum())
        if pro_total_observed <= 0 or con_total_observed <= 0:
            return 0.0, 0.0, 0.0, 0

        observed_pro_dup_rows = int(pro_counts[pro_counts >= 2].sum())
        observed_con_dup_rows = int(con_counts[con_counts >= 2].sum())
        observed = float(observed_pro_dup_rows / pro_total_observed) - float(
            observed_con_dup_rows / con_total_observed
        )

        draws = (
            self.position_cluster_bootstrap_draws
            if n_bootstrap_draws is None
            else min(int(n_bootstrap_draws), int(self.position_cluster_bootstrap_draws))
        )
        draws = max(0, int(draws))
        if draws <= 0:
            return observed, observed, observed, 0

        deltas = np.empty(draws, dtype=float)
        draws_effective = 0
        for draw_idx in range(draws):
            # Cluster bootstrap by name key: sample name-key clusters with replacement,
            # then recompute position duplicate-row rates from the reweighted clusters.
            sampled_indices = rng.integers(0, n_keys, size=n_keys, endpoint=False)
            weights = np.bincount(sampled_indices, minlength=n_keys).astype(np.int64, copy=False)
            boot_pro_counts = pro_counts * weights
            boot_con_counts = con_counts * weights
            boot_pro_total = int(boot_pro_counts.sum())
            boot_con_total = int(boot_con_counts.sum())
            if boot_pro_total <= 0 or boot_con_total <= 0:
                deltas[draw_idx] = np.nan
                continue
            boot_pro_dup_rows = int(boot_pro_counts[boot_pro_counts >= 2].sum())
            boot_con_dup_rows = int(boot_con_counts[boot_con_counts >= 2].sum())
            deltas[draw_idx] = float(boot_pro_dup_rows / boot_pro_total) - float(
                boot_con_dup_rows / boot_con_total
            )
            draws_effective += 1

        valid = deltas[np.isfinite(deltas)]
        if valid.size <= 0:
            return observed, observed, observed, 0
        quantile_low, quantile_high = self._position_interval_bounds()
        ci_low = float(np.quantile(valid, quantile_low))
        ci_high = float(np.quantile(valid, quantile_high))
        return observed, ci_low, ci_high, int(draws_effective)

    @staticmethod
    def _vectorized_binomial_tail_p_values(
        *,
        observed_successes: pd.Series,
        total_trials: int,
        success_probabilities: pd.Series,
    ) -> pd.Series:
        index = observed_successes.index
        n_trials = int(max(int(total_trials), 0))
        if n_trials <= 0:
            return pd.Series(1.0, index=index, dtype=float)

        observed = (
            pd.to_numeric(observed_successes, errors="coerce")
            .fillna(0.0)
            .round()
            .clip(lower=0.0)
            .to_numpy(dtype=np.int64)
        )
        probabilities = (
            pd.to_numeric(success_probabilities, errors="coerce")
            .fillna(0.0)
            .clip(lower=0.0, upper=1.0)
            .to_numpy(dtype=float)
        )

        p_values = np.ones(observed.size, dtype=float)
        over_trials = observed > n_trials
        if np.any(over_trials):
            p_values[over_trials] = 0.0
        valid = (~over_trials) & (observed > 0)
        if np.any(valid):
            values = binom.sf(observed[valid] - 1, n_trials, probabilities[valid])
            values = np.where(np.isfinite(values), values, 1.0)
            p_values[valid] = np.clip(values, 0.0, 1.0)
        return pd.Series(p_values, index=index, dtype=float)

    @staticmethod
    def _vectorized_hypergeometric_tail_p_values(
        *,
        observed_successes: pd.Series,
        population_size: int,
        population_successes: pd.Series,
        sample_size: int,
    ) -> pd.Series:
        index = observed_successes.index
        n_population = int(max(int(population_size), 0))
        n_sample = int(max(int(sample_size), 0))
        if n_population <= 0 or n_sample <= 0:
            return pd.Series(1.0, index=index, dtype=float)
        if n_sample > n_population:
            return pd.Series(0.0, index=index, dtype=float)

        observed = (
            pd.to_numeric(observed_successes, errors="coerce")
            .fillna(0.0)
            .round()
            .clip(lower=0.0)
            .to_numpy(dtype=np.int64)
        )
        successes = (
            pd.to_numeric(population_successes, errors="coerce")
            .fillna(0.0)
            .round()
            .clip(lower=0.0, upper=float(n_population))
            .to_numpy(dtype=np.int64)
        )

        p_values = np.ones(observed.size, dtype=float)
        max_observable = np.minimum(n_sample, successes)
        over_max = observed > max_observable
        if np.any(over_max):
            p_values[over_max] = 0.0
        valid = (~over_max) & (observed > 0)
        if np.any(valid):
            values = hypergeom.sf(observed[valid] - 1, n_population, successes[valid], n_sample)
            values = np.where(np.isfinite(values), values, 1.0)
            p_values[valid] = np.clip(values, 0.0, 1.0)
        return pd.Series(p_values, index=index, dtype=float)

    @staticmethod
    def _harmonic_number(n: int) -> float:
        if n <= 0:
            return 0.0
        return float(sum(1.0 / float(index) for index in range(1, int(n) + 1)))

    @classmethod
    def _holm_adjust(cls, p_values: pd.Series | list[float]) -> pd.Series:
        if isinstance(p_values, list):
            p_series = pd.Series(p_values, dtype=float)
        else:
            p_series = pd.to_numeric(p_values, errors="coerce").astype(float)
        if p_series.empty:
            return pd.Series(dtype=float)
        order = p_series.sort_values(kind="mergesort").index
        ordered = p_series.loc[order].to_numpy(dtype=float)
        adjusted = np.full(len(ordered), np.nan, dtype=float)
        running = 0.0
        n_total = float(len(ordered))
        for i, p_value in enumerate(ordered):
            if not np.isfinite(p_value):
                continue
            multiplier = n_total - float(i)
            candidate = min(1.0, float(p_value) * multiplier)
            running = max(running, candidate)
            adjusted[i] = min(1.0, running)
        return pd.Series(index=order, data=adjusted, dtype=float).reindex(p_series.index)

    @classmethod
    def _benjamini_yekutieli(cls, p_values: pd.Series | list[float]) -> pd.Series:
        bh = benjamini_hochberg(p_values)
        if bh.empty:
            return pd.Series(dtype=float)
        n_tests = int(pd.to_numeric(bh, errors="coerce").dropna().size)
        if n_tests <= 0:
            return pd.Series(np.nan, index=bh.index, dtype=float)
        correction = cls._harmonic_number(n_tests)
        if correction <= 0.0:
            correction = 1.0
        return (bh * float(correction)).clip(lower=0.0, upper=1.0)

    @classmethod
    def _adjust_p_values(
        cls,
        p_values: pd.Series | list[float],
        *,
        method: str,
    ) -> pd.Series:
        if isinstance(p_values, list):
            p_series = pd.Series(p_values, dtype=float)
        else:
            p_series = pd.to_numeric(p_values, errors="coerce").astype(float)
        if p_series.empty:
            return pd.Series(dtype=float)
        adjusted = pd.Series(np.nan, index=p_series.index, dtype=float)
        valid = p_series[p_series.notna() & np.isfinite(p_series)]
        if valid.empty:
            return adjusted
        method_norm = str(method or "").strip().lower()
        if method_norm == cls.ADJUSTMENT_METHOD_HOLM:
            valid_adjusted = cls._holm_adjust(valid)
        elif method_norm == cls.ADJUSTMENT_METHOD_BY:
            valid_adjusted = cls._benjamini_yekutieli(valid)
        elif method_norm == cls.ADJUSTMENT_METHOD_BH:
            valid_adjusted = benjamini_hochberg(valid)
        else:
            valid_adjusted = valid.copy()
        adjusted.loc[valid.index] = (
            pd.to_numeric(valid_adjusted, errors="coerce")
            .astype(float)
            .clip(lower=0.0, upper=1.0)
        )
        return adjusted

    def _resolved_collision_key_column(self, frame: pd.DataFrame) -> str:
        configured = _KEY_TO_COLUMN.get(self.collision_key_mode, "canonical_key_strict")
        if configured in frame.columns:
            return configured
        fallback = _KEY_TO_COLUMN.get(self.primary_name_key, "canonical_key_medium")
        if fallback in frame.columns:
            return fallback
        if "canonical_name" in frame.columns:
            return "canonical_name"
        if "canonical_key_medium" in frame.columns:
            return "canonical_key_medium"
        raise ValueError(
            "Missing canonical name key columns. Expected one of canonical_name/canonical_key_medium."
        )

    def _scope_list(self) -> list[str]:
        return [self.collision_scope_primary] + [scope for scope in self.collision_scope_overlays if scope != self.collision_scope_primary]

    def _normalization_version_metadata(self) -> tuple[str, str]:
        version_hash = normalization_version_hash(
            normalize_unicode=self.normalize_unicode,
            strip_punctuation=self.strip_punctuation,
            nickname_map_path=self.nickname_map_path,
        )
        version_value = normalization_version(
            normalize_unicode=self.normalize_unicode,
            strip_punctuation=self.strip_punctuation,
            nickname_map_path=self.nickname_map_path,
        )
        return version_value, version_hash

    @staticmethod
    def _seed_sequence_stream_id(sequence: np.random.SeedSequence) -> str:
        spawn_key = ".".join(str(value) for value in sequence.spawn_key) or "root"
        state_words = sequence.generate_state(4, dtype=np.uint32)
        state_hex = "".join(f"{int(value):08x}" for value in state_words.tolist())
        return f"{spawn_key}:{state_hex}"

    @classmethod
    def _spawn_rng_streams(
        cls,
        *,
        root_seed: int,
    ) -> tuple[dict[str, np.random.Generator], dict[str, object]]:
        root_sequence = np.random.SeedSequence(int(root_seed))
        child_sequences = root_sequence.spawn(len(cls.RNG_STREAM_NAMES))
        rng_streams: dict[str, np.random.Generator] = {}
        stream_lineage: dict[str, dict[str, str]] = {}
        for stream_name, child_sequence in zip(cls.RNG_STREAM_NAMES, child_sequences):
            rng_streams[stream_name] = np.random.default_rng(child_sequence)
            stream_lineage[stream_name] = {
                "stream_id": cls._seed_sequence_stream_id(child_sequence),
            }
        lineage = {
            "root_seed": int(root_seed),
            "root_stream_id": cls._seed_sequence_stream_id(root_sequence),
            "streams": stream_lineage,
        }
        return rng_streams, lineage

    @classmethod
    def _flatten_rng_lineage(
        cls,
        *,
        lineage: dict[str, object],
    ) -> dict[str, object]:
        streams = lineage.get("streams", {})
        if not isinstance(streams, dict):
            streams = {}
        flattened: dict[str, object] = {
            "rng_root_seed": int(lineage.get("root_seed", 0) or 0),
            "rng_root_stream_id": str(lineage.get("root_stream_id", "") or ""),
        }
        for stream_name in cls.RNG_STREAM_NAMES:
            stream_value = streams.get(stream_name, {})
            stream_id = ""
            if isinstance(stream_value, dict):
                stream_id = str(stream_value.get("stream_id", "") or "")
            flattened[f"rng_stream_{stream_name}"] = stream_id
        return flattened

    def _scope_frames(
        self,
        infer: pd.DataFrame,
        features: dict[str, pd.DataFrame],
        *,
        no_person_rows_after_filter: bool,
    ) -> tuple[dict[str, pd.DataFrame], dict[str, dict[str, str]]]:
        scopes: dict[str, pd.DataFrame] = {}
        scope_availability: dict[str, dict[str, str]] = {}

        def _record_scope(
            scope_name: str,
            frame: pd.DataFrame,
            *,
            scope_status: str,
            scope_reason: str,
        ) -> None:
            scopes[scope_name] = frame
            scope_availability[scope_name] = {
                "scope_status": scope_status,
                "scope_reason": scope_reason,
            }

        full_scope_reason = self.SCOPE_REASON_AVAILABLE
        full_scope_status = self.SCOPE_STATUS_AVAILABLE
        if infer.empty:
            full_scope_status = self.SCOPE_STATUS_UNAVAILABLE
            full_scope_reason = (
                self.SCOPE_REASON_UNAVAILABLE_NO_PERSON_ROWS
                if no_person_rows_after_filter
                else self.SCOPE_REASON_UNAVAILABLE_NO_ROWS_AFTER_FILTERING
            )
        _record_scope(
            "full_hearing",
            infer.copy(),
            scope_status=full_scope_status,
            scope_reason=full_scope_reason,
        )

        requested = set(self._scope_list())
        if "matched_only" not in requested and "unmatched_only" not in requested:
            return scopes, scope_availability

        if infer.empty:
            unavailable_reason = (
                self.SCOPE_REASON_UNAVAILABLE_NO_PERSON_ROWS
                if no_person_rows_after_filter
                else self.SCOPE_REASON_UNAVAILABLE_NO_ROWS_AFTER_FILTERING
            )
            if "matched_only" in requested:
                _record_scope(
                    "matched_only",
                    pd.DataFrame(columns=infer.columns),
                    scope_status=self.SCOPE_STATUS_UNAVAILABLE,
                    scope_reason=unavailable_reason,
                )
            if "unmatched_only" in requested:
                _record_scope(
                    "unmatched_only",
                    pd.DataFrame(columns=infer.columns),
                    scope_status=self.SCOPE_STATUS_UNAVAILABLE,
                    scope_reason=unavailable_reason,
                )
            return scopes, scope_availability

        assignments = features.get("voter_registry_match.match_assignments", pd.DataFrame())
        if assignments is None or not isinstance(assignments, pd.DataFrame) or assignments.empty:
            if "matched_only" in requested:
                _record_scope(
                    "matched_only",
                    pd.DataFrame(columns=infer.columns),
                    scope_status=self.SCOPE_STATUS_UNAVAILABLE,
                    scope_reason=self.SCOPE_REASON_UNAVAILABLE_MISSING_MATCH_ASSIGNMENTS,
                )
            if "unmatched_only" in requested:
                _record_scope(
                    "unmatched_only",
                    pd.DataFrame(columns=infer.columns),
                    scope_status=self.SCOPE_STATUS_UNAVAILABLE,
                    scope_reason=self.SCOPE_REASON_UNAVAILABLE_MISSING_MATCH_ASSIGNMENTS,
                )
            return scopes, scope_availability

        assignments = assignments.copy()
        if "canonical_name" not in assignments.columns:
            if "matched_only" in requested:
                _record_scope(
                    "matched_only",
                    pd.DataFrame(columns=infer.columns),
                    scope_status=self.SCOPE_STATUS_UNAVAILABLE,
                    scope_reason=self.SCOPE_REASON_UNAVAILABLE_MISSING_MATCH_ASSIGNMENTS,
                )
            if "unmatched_only" in requested:
                _record_scope(
                    "unmatched_only",
                    pd.DataFrame(columns=infer.columns),
                    scope_status=self.SCOPE_STATUS_UNAVAILABLE,
                    scope_reason=self.SCOPE_REASON_UNAVAILABLE_MISSING_MATCH_ASSIGNMENTS,
                )
            return scopes, scope_availability
        assignments["canonical_name"] = _safe_str_series(
            assignments.get("canonical_name", pd.Series(dtype=str))
        )
        outcome_column = "primary_outcome_selected"
        if outcome_column not in assignments.columns:
            outcome_column = "primary_outcome" if "primary_outcome" in assignments.columns else ""
        if not outcome_column:
            if "matched_only" in requested:
                _record_scope(
                    "matched_only",
                    pd.DataFrame(columns=infer.columns),
                    scope_status=self.SCOPE_STATUS_UNAVAILABLE,
                    scope_reason=self.SCOPE_REASON_UNAVAILABLE_MISSING_MATCH_ASSIGNMENTS,
                )
            if "unmatched_only" in requested:
                _record_scope(
                    "unmatched_only",
                    pd.DataFrame(columns=infer.columns),
                    scope_status=self.SCOPE_STATUS_UNAVAILABLE,
                    scope_reason=self.SCOPE_REASON_UNAVAILABLE_MISSING_MATCH_ASSIGNMENTS,
                )
            return scopes, scope_availability
        assignments[outcome_column] = _safe_str_series(assignments[outcome_column]).replace("", "unmatched")
        matched_names = set(
            assignments.loc[assignments[outcome_column].isin(_MATCHED_OUTCOMES), "canonical_name"].tolist()
        )
        unmatched_names = set(
            assignments.loc[assignments[outcome_column] == "unmatched", "canonical_name"].tolist()
        )
        infer_names = _safe_str_series(infer.get("canonical_name", pd.Series(dtype=str)))
        if "matched_only" in requested:
            matched_scope = infer.loc[infer_names.isin(matched_names)].copy()
            matched_scope_available = not matched_scope.empty
            _record_scope(
                "matched_only",
                matched_scope,
                scope_status=(
                    self.SCOPE_STATUS_AVAILABLE
                    if matched_scope_available
                    else self.SCOPE_STATUS_UNAVAILABLE
                ),
                scope_reason=(
                    self.SCOPE_REASON_AVAILABLE
                    if matched_scope_available
                    else self.SCOPE_REASON_UNAVAILABLE_NO_ROWS_AFTER_FILTERING
                ),
            )
        if "unmatched_only" in requested:
            unmatched_scope = infer.loc[infer_names.isin(unmatched_names)].copy()
            unmatched_scope_available = not unmatched_scope.empty
            _record_scope(
                "unmatched_only",
                unmatched_scope,
                scope_status=(
                    self.SCOPE_STATUS_AVAILABLE
                    if unmatched_scope_available
                    else self.SCOPE_STATUS_UNAVAILABLE
                ),
                scope_reason=(
                    self.SCOPE_REASON_AVAILABLE
                    if unmatched_scope_available
                    else self.SCOPE_REASON_UNAVAILABLE_NO_ROWS_AFTER_FILTERING
                ),
            )
        return scopes, scope_availability

    def _resolve_histogram(
        self,
        *,
        observed_counts: pd.Series,
        key_column: str,
    ) -> tuple[pd.DataFrame, str, bool]:
        source = self.collision_baseline_source
        if source == "hearing_empirical":
            return histogram_from_name_counts(observed_counts), "hearing_empirical", False

        if not self.voter_db_url:
            if self.collision_baseline_failure_policy == "fail":
                raise RuntimeError("collision baseline requires voter_registry.db_url but none was configured.")
            return histogram_from_name_counts(observed_counts), "hearing_empirical", True

        try:
            if source == "vrdb_full_histogram":
                hist = fetch_voter_name_key_count_histogram(
                    db_url=self.voter_db_url,
                    table_name=self.voter_table_name,
                    key_column=key_column,
                    active_only=self.voter_active_only,
                )
            elif source == "vrdb_full_keys":
                frequencies = fetch_voter_name_key_frequencies(
                    db_url=self.voter_db_url,
                    table_name=self.voter_table_name,
                    key_column=key_column,
                    active_only=self.voter_active_only,
                )
                hist = histogram_from_name_counts(frequencies.get("n_registry_rows", pd.Series(dtype=float)))
            else:
                hist = histogram_from_name_counts(observed_counts)
            if hist.empty:
                raise RuntimeError(f"No rows available for baseline source={source}.")
            return hist, source, False
        except Exception as exc:
            if self.collision_baseline_failure_policy == "fail":
                raise RuntimeError(
                    f"Failed computing collision baseline source={source}: {exc}"
                ) from exc
            return histogram_from_name_counts(observed_counts), "hearing_empirical", True

    @staticmethod
    def _global_stratum_weights(stratum_frequencies: pd.DataFrame) -> pd.Series:
        if stratum_frequencies.empty:
            return pd.Series(dtype=float)
        weights = (
            stratum_frequencies.groupby("stratum", dropna=False)["n_registry_rows"]
            .sum()
            .astype(float)
        )
        total = float(weights.sum())
        if total <= 0.0:
            return pd.Series(dtype=float)
        return (weights / total).sort_index()

    def _scope_stratum_weights(
        self,
        *,
        observed_counts: pd.Series,
        stratum_frequencies: pd.DataFrame,
    ) -> pd.Series:
        global_weights = self._global_stratum_weights(stratum_frequencies)
        if observed_counts.empty or stratum_frequencies.empty:
            return global_weights
        observed = (
            observed_counts.rename_axis("name_key")
            .reset_index(name="observed_count")
            .assign(name_key=lambda frame: frame["name_key"].fillna("").astype(str).str.strip())
        )
        observed = observed[observed["name_key"] != ""].copy()
        if observed.empty:
            return global_weights
        joined = stratum_frequencies.merge(observed, on="name_key", how="inner")
        if joined.empty:
            return global_weights
        joined["name_total"] = (
            joined.groupby("name_key", dropna=False)["n_registry_rows"].transform("sum").astype(float)
        )
        joined = joined[joined["name_total"] > 0.0].copy()
        if joined.empty:
            return global_weights
        joined["assigned_rows"] = (
            joined["observed_count"].astype(float)
            * joined["n_registry_rows"].astype(float)
            / joined["name_total"]
        )
        by_stratum = joined.groupby("stratum", dropna=False)["assigned_rows"].sum().astype(float)
        total = float(by_stratum.sum())
        if total <= 0.0:
            return global_weights
        return (by_stratum / total).sort_index()

    def _mixture_probabilities_from_strata(
        self,
        *,
        observed_counts: pd.Series,
        stratum_frequencies: pd.DataFrame,
    ) -> tuple[pd.Series, pd.Series, int]:
        if stratum_frequencies.empty:
            return pd.Series(dtype=float), pd.Series(dtype=float), 0
        weights = self._scope_stratum_weights(
            observed_counts=observed_counts,
            stratum_frequencies=stratum_frequencies,
        )
        if weights.empty:
            return pd.Series(dtype=float), weights, 0
        working = stratum_frequencies.copy()
        stratum_totals = (
            working.groupby("stratum", dropna=False)["n_registry_rows"].sum().astype(float)
        )
        working["stratum_total"] = working["stratum"].map(stratum_totals).astype(float)
        working["weight"] = working["stratum"].map(weights).fillna(0.0).astype(float)
        working = working[(working["stratum_total"] > 0.0) & (working["weight"] > 0.0)].copy()
        if working.empty:
            return pd.Series(dtype=float), weights, int(max(float(stratum_totals.sum()), 0.0))
        working["prob_component"] = (
            working["weight"] * working["n_registry_rows"].astype(float) / working["stratum_total"]
        )
        probabilities = (
            working.groupby("name_key", dropna=False)["prob_component"].sum().astype(float)
        )
        probabilities = probabilities[np.isfinite(probabilities) & (probabilities > 0.0)]
        if probabilities.empty:
            return pd.Series(dtype=float), weights, int(max(float(stratum_totals.sum()), 0.0))
        total_probability = float(probabilities.sum())
        if total_probability <= 0.0:
            return pd.Series(dtype=float), weights, int(max(float(stratum_totals.sum()), 0.0))
        probabilities = probabilities / total_probability
        n_population = int(max(float(stratum_totals.sum()), 0.0))
        return probabilities.sort_index(), weights, n_population

    @staticmethod
    def _build_stratified_sampling_inputs(
        *,
        stratum_frequencies: pd.DataFrame,
        stratum_weights: pd.Series,
    ) -> tuple[np.ndarray, list[np.ndarray], list[np.ndarray]]:
        if stratum_frequencies.empty or stratum_weights.empty:
            return np.asarray([], dtype=float), [], []

        grouped = {
            str(stratum): frame.copy()
            for stratum, frame in stratum_frequencies.groupby("stratum", dropna=False)
        }
        weight_values: list[float] = []
        raw_key_arrays: list[np.ndarray] = []
        prob_arrays: list[np.ndarray] = []
        for stratum, weight in stratum_weights.items():
            stratum_name = str(stratum)
            group = grouped.get(stratum_name)
            if group is None or group.empty:
                continue
            key_values = _safe_str_series(group["name_key"]).to_numpy(dtype=object)
            count_values = (
                pd.to_numeric(group["n_registry_rows"], errors="coerce")
                .fillna(0.0)
                .to_numpy(dtype=float)
            )
            valid = (key_values != "") & np.isfinite(count_values) & (count_values > 0.0)
            if not np.any(valid):
                continue
            key_values = key_values[valid]
            count_values = count_values[valid]
            total = float(count_values.sum())
            if total <= 0.0 or not np.isfinite(total):
                continue
            probabilities = count_values / total
            if probabilities.size == 0:
                continue
            weight_values.append(float(max(float(weight), 0.0)))
            raw_key_arrays.append(key_values.astype(str, copy=False))
            prob_arrays.append(probabilities)

        if not weight_values or not raw_key_arrays:
            return np.asarray([], dtype=float), [], []
        all_keys = np.concatenate(raw_key_arrays)
        if all_keys.size == 0:
            return np.asarray([], dtype=float), [], []
        global_key_index = pd.Index(np.unique(all_keys))
        key_arrays = [
            global_key_index.get_indexer(key_values).astype(np.int64, copy=False)
            for key_values in raw_key_arrays
        ]
        if any(np.any(key_values < 0) for key_values in key_arrays):
            return np.asarray([], dtype=float), [], []
        weight_array = np.asarray(weight_values, dtype=float)
        weight_total = float(weight_array.sum())
        if weight_total <= 0.0 or not np.isfinite(weight_total):
            return np.asarray([], dtype=float), [], []
        weight_array = weight_array / weight_total
        return weight_array, key_arrays, prob_arrays

    def _simulate_stratified_collision_null(
        self,
        *,
        n_rows: int,
        draws: int,
        rng: np.random.Generator,
        stratum_weights: np.ndarray,
        stratum_keys: list[np.ndarray],
        stratum_probabilities: list[np.ndarray],
        max_draws: int,
        tail_observed: dict[str, float] | None = None,
    ) -> pd.DataFrame:
        started = perf_counter()
        output = pd.DataFrame(columns=list(COLLISION_METRICS))
        target_draws = 0
        draws_effective = 0
        stop_reason = "not_started"
        try:
            n = int(max(int(n_rows), 0))
            n_draws = int(max(int(draws), 0))
            if (
                n <= 0
                or n_draws <= 0
                or stratum_weights.size == 0
                or not stratum_keys
                or not stratum_probabilities
            ):
                return pd.DataFrame(columns=list(COLLISION_METRICS))
            draw_cap = int(min(max(int(max_draws), 0), max(int(n_draws), 0)))
            if draw_cap <= 0:
                return pd.DataFrame(columns=list(COLLISION_METRICS))
            target_draws = int(draw_cap)
            minimum_draws = int(min(draw_cap, max(int(self.monte_carlo_min_draws), 1)))
            if math.isfinite(self.monte_carlo_target_p_mcse) and self.monte_carlo_target_p_mcse > 0.0:
                threshold = float(
                    min(max(float(self.monte_carlo_decision_p_threshold), 0.0), 1.0)
                )
                variance = threshold * (1.0 - threshold)
                required = (
                    int(math.ceil(variance / (self.monte_carlo_target_p_mcse**2)))
                    if variance > 0.0
                    else minimum_draws
                )
                target_draws = int(min(draw_cap, max(required, minimum_draws)))
            if target_draws <= 0:
                return pd.DataFrame(columns=list(COLLISION_METRICS))

            pairs = np.zeros(target_draws, dtype=float)
            excess_rows = np.zeros(target_draws, dtype=float)
            repeated_rows = np.zeros(target_draws, dtype=float)
            observed_by_metric: dict[str, float] = {}
            for metric, value in (tail_observed or {}).items():
                metric_name = str(metric).strip().lower()
                if metric_name not in COLLISION_METRICS:
                    continue
                try:
                    numeric_value = float(value)
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(numeric_value):
                    continue
                observed_by_metric[metric_name] = numeric_value
            exceedances = {metric: 0 for metric in observed_by_metric}
            stop_reason = "target_draws_reached"

            for draw_idx in range(target_draws):
                draws_effective = int(draw_idx + 1)
                sampled_by_stratum = rng.multinomial(n, stratum_weights)
                sampled_key_chunks: list[np.ndarray] = []
                for idx, draw_count in enumerate(sampled_by_stratum):
                    draw_n = int(draw_count)
                    if draw_n <= 0:
                        continue
                    keys = stratum_keys[idx]
                    probs = stratum_probabilities[idx]
                    if keys.size == 0 or probs.size == 0:
                        continue
                    sampled_idx = rng.choice(keys.size, size=draw_n, replace=True, p=probs)
                    sampled_key_chunks.append(keys[sampled_idx])

                if not sampled_key_chunks:
                    continue
                if len(sampled_key_chunks) == 1:
                    _, occupancy = np.unique(sampled_key_chunks[0], return_counts=True)
                else:
                    _, occupancy = np.unique(np.concatenate(sampled_key_chunks), return_counts=True)
                occupancy = occupancy.astype(float)
                over_one = np.maximum(occupancy - 1.0, 0.0)
                if occupancy.size == 0:
                    continue
                pairs[draw_idx] = float((occupancy * over_one / 2.0).sum())
                excess_rows[draw_idx] = float(over_one.sum())
                repeated_rows[draw_idx] = float(occupancy[occupancy >= 2.0].sum())

                if observed_by_metric:
                    if "pairs" in exceedances:
                        exceedances["pairs"] += int(pairs[draw_idx] >= observed_by_metric["pairs"])
                    if "excess_rows" in exceedances:
                        exceedances["excess_rows"] += int(
                            excess_rows[draw_idx] >= observed_by_metric["excess_rows"]
                        )
                    if "repeated_group_rows" in exceedances:
                        exceedances["repeated_group_rows"] += int(
                            repeated_rows[draw_idx] >= observed_by_metric["repeated_group_rows"]
                        )
                    if draws_effective < minimum_draws:
                        continue
                    resolved = True
                    for metric_name in observed_by_metric:
                        k = int(exceedances.get(metric_name, 0))
                        p_estimate = float((k + 1) / (draws_effective + 1))
                        mcse = float(
                            math.sqrt(
                                max(p_estimate * (1.0 - p_estimate), 0.0)
                                / float(max(draws_effective, 1))
                            )
                        )
                        confidence = float(
                            min(max(float(self.monte_carlo_decision_confidence_level), 0.0), 1.0)
                        )
                        if confidence == 0.90:
                            z = 1.6448536269514722
                        elif confidence == 0.99:
                            z = 2.5758293035489004
                        else:
                            z = 1.959963984540054
                        p_raw = float(k) / float(max(draws_effective, 1))
                        z2_over_n = (z * z) / float(max(draws_effective, 1))
                        denominator = 1.0 + z2_over_n
                        center = (p_raw + (z2_over_n / 2.0)) / denominator
                        half = (
                            z
                            * math.sqrt(
                                (
                                    p_raw * (1.0 - p_raw) / float(max(draws_effective, 1))
                                )
                                + ((z * z) / (4.0 * float(max(draws_effective, 1)) ** 2))
                            )
                            / denominator
                        )
                        ci_low = max(0.0, center - half)
                        ci_high = min(1.0, center + half)
                        threshold = float(
                            min(max(float(self.monte_carlo_decision_p_threshold), 0.0), 1.0)
                        )
                        ci_separated = bool(ci_high < threshold or ci_low > threshold)
                        mcse_resolved = bool(
                            math.isfinite(self.monte_carlo_target_p_mcse)
                            and self.monte_carlo_target_p_mcse > 0.0
                            and mcse <= self.monte_carlo_target_p_mcse
                        )
                        if not (ci_separated or mcse_resolved):
                            resolved = False
                            break
                    if resolved:
                        stop_reason = "precision_resolved"
                        pairs = pairs[:draws_effective]
                        excess_rows = excess_rows[:draws_effective]
                        repeated_rows = repeated_rows[:draws_effective]
                        break

            if draws_effective <= 0:
                draws_effective = int(target_draws)
            output = pd.DataFrame(
                {
                    "pairs": pairs,
                    "excess_rows": excess_rows,
                    "repeated_group_rows": repeated_rows,
                },
                columns=list(COLLISION_METRICS),
            )
            output.attrs["monte_carlo_precision"] = {
                "draws_requested": int(n_draws),
                "draws_target": int(target_draws),
                "draws_effective": int(draws_effective),
                "min_draws": int(minimum_draws),
                "target_p_mcse": float(self.monte_carlo_target_p_mcse),
                "decision_p_threshold": float(self.monte_carlo_decision_p_threshold),
                "decision_confidence_level": float(
                    self.monte_carlo_decision_confidence_level
                ),
                "stopped_early": bool(draws_effective < target_draws),
                "stop_reason": str(stop_reason),
            }
            return output
        finally:
            record_runtime_timing(
                "simulation.duplicates_exact_stratified_collision_null",
                (perf_counter() - started) * 1000.0,
            )
            record_runtime_counter("simulation.duplicates_exact_stratified_collision_null.calls", 1)
            record_runtime_counter(
                "simulation.duplicates_exact_stratified_collision_null.n_rows",
                max(int(n_rows), 0),
            )
            record_runtime_counter(
                "simulation.duplicates_exact_stratified_collision_null.draws_requested",
                max(int(draws), 0),
            )
            record_runtime_counter(
                "simulation.duplicates_exact_stratified_collision_null.draws_effective",
                int(max(draws_effective, 0)),
            )
            record_runtime_counter(
                "simulation.duplicates_exact_stratified_collision_null.draws_target",
                int(max(target_draws, 0)),
            )
            record_runtime_counter(
                "simulation.duplicates_exact_stratified_collision_null.output_samples",
                int(len(output)),
            )

    def _population_counts_for_observed_names(
        self,
        *,
        key_column: str,
        key_values: list[str],
        observed_counts: pd.Series,
        effective_baseline_source: str,
    ) -> pd.Series:
        if effective_baseline_source == "hearing_empirical":
            return observed_counts.astype(float)
        if not self.voter_db_url:
            if self.collision_baseline_failure_policy == "fail":
                raise RuntimeError("voter_db_url is required for registry-derived per-name counts.")
            return pd.Series(index=key_values, data=np.zeros(len(key_values), dtype=float))
        try:
            lookup = fetch_matching_voter_keys(
                db_url=self.voter_db_url,
                table_name=self.voter_table_name,
                key_values=key_values,
                key_column=key_column,
                active_only=self.voter_active_only,
            )
            if lookup.empty:
                return pd.Series(index=key_values, data=np.zeros(len(key_values), dtype=float))
            key_col = key_column if key_column in lookup.columns else "canonical_name"
            out = (
                lookup[[key_col, "n_registry_rows"]]
                .rename(columns={key_col: "name_key"})
                .set_index("name_key")["n_registry_rows"]
                .astype(float)
            )
            return out
        except Exception as exc:
            if self.collision_baseline_failure_policy == "fail":
                raise RuntimeError(f"Failed fetching per-name registry counts: {exc}") from exc
            return pd.Series(index=key_values, data=np.zeros(len(key_values), dtype=float))

    def _position_permutation_test(
        self,
        working: pd.DataFrame,
        key_column: str,
        *,
        rng: np.random.Generator,
        bootstrap_rng: np.random.Generator | None = None,
        n_permutations: int | None = None,
    ) -> pd.DataFrame:
        started = perf_counter()
        has_result = False
        permutations_effective = 0
        try:
            if working.empty:
                return pd.DataFrame()
            positions = _safe_str_series(working["position_normalized"]).to_numpy(dtype=object)
            if not {"Pro", "Con"}.issubset(set(np.unique(positions))):
                return pd.DataFrame()

            key_ids, _ = pd.factorize(_safe_str_series(working[key_column]), sort=False)
            key_ids = key_ids.astype(np.int64, copy=False)
            valid_rows = key_ids >= 0
            if not bool(np.any(valid_rows)):
                return pd.DataFrame()
            if not bool(np.all(valid_rows)):
                key_ids = key_ids[valid_rows]
                positions = positions[valid_rows]

            if key_ids.size == 0:
                return pd.DataFrame()
            n_keys = int(max(int(key_ids.max()), -1) + 1)
            if n_keys <= 0:
                return pd.DataFrame()

            pro_mask_observed = positions == "Pro"
            con_mask_observed = positions == "Con"
            pro_total = int(np.count_nonzero(pro_mask_observed))
            con_total = int(np.count_nonzero(con_mask_observed))
            if pro_total <= 0 or con_total <= 0:
                return pd.DataFrame()
            pro_counts_observed = np.bincount(key_ids[pro_mask_observed], minlength=n_keys)
            con_counts_observed = np.bincount(key_ids[con_mask_observed], minlength=n_keys)
            pro_dup_rows = int(pro_counts_observed[pro_counts_observed >= 2].sum())
            con_dup_rows = int(con_counts_observed[con_counts_observed >= 2].sum())
            pro_rate = (pro_dup_rows / pro_total) if pro_total else 0.0
            con_rate = (con_dup_rows / con_total) if con_total else 0.0
            observed_diff = pro_rate - con_rate
            observed_rr = (pro_rate / con_rate) if con_rate > 0 else np.inf

            n_rows = int(key_ids.size)
            pro_n = int(pro_total)
            con_n = int(con_total)
            permutations_effective = max(
                0,
                int(
                    self.position_permutation_draws
                    if n_permutations is None
                    else min(int(n_permutations), int(self.position_permutation_draws))
                ),
            )
            if permutations_effective <= 0:
                return pd.DataFrame()
            perm_values = np.empty(permutations_effective, dtype=float)
            for draw_idx in range(permutations_effective):
                # Equivalent to permuting categorical labels while preserving label totals.
                # Assign first pro_n indices to Pro, next con_n to Con, remainder to non-Pro/Con.
                permuted_indices = rng.permutation(n_rows)
                pro_indices = permuted_indices[:pro_n]
                con_indices = permuted_indices[pro_n : pro_n + con_n]
                pro_counts = np.bincount(key_ids[pro_indices], minlength=n_keys)
                con_counts = np.bincount(key_ids[con_indices], minlength=n_keys)
                pro_perm_dup_rows = int(pro_counts[pro_counts >= 2].sum())
                con_perm_dup_rows = int(con_counts[con_counts >= 2].sum())
                pro_perm_rate = pro_perm_dup_rows / pro_total
                con_perm_rate = con_perm_dup_rows / con_total
                perm_values[draw_idx] = pro_perm_rate - con_perm_rate

            perm_series = np.asarray(perm_values, dtype=float)
            observed_abs_effect = float(abs(observed_diff))
            p_value_two_sided = float(
                (np.sum(np.abs(perm_series) >= observed_abs_effect) + 1) / (perm_series.size + 1)
            )
            bootstrap_rng_effective = rng if bootstrap_rng is None else bootstrap_rng
            effect, ci_low, ci_high, interval_draws_effective = self._cluster_bootstrap_rate_difference(
                pro_counts_observed=pro_counts_observed,
                con_counts_observed=con_counts_observed,
                rng=bootstrap_rng_effective,
                n_bootstrap_draws=self.position_cluster_bootstrap_draws,
            )
            has_result = True
            return pd.DataFrame(
                [
                    {
                        "position_left": "Pro",
                        "position_right": "Con",
                        "left_duplicate_rows": int(pro_dup_rows),
                        "left_total_rows": int(pro_total),
                        "left_duplicate_row_rate": float(pro_rate),
                        "right_duplicate_rows": int(con_dup_rows),
                        "right_total_rows": int(con_total),
                        "right_duplicate_row_rate": float(con_rate),
                        "rate_difference": float(effect),
                        "rate_difference_ci_low": float(ci_low),
                        "rate_difference_ci_high": float(ci_high),
                        "rate_difference_interval_method": self.POSITION_RATE_DIFF_INTERVAL_METHOD_ID,
                        "rate_difference_interval_draws": int(interval_draws_effective),
                        "rate_ratio": float(observed_rr) if np.isfinite(observed_rr) else 0.0,
                        "permutation_test_id": self.POSITION_PERMUTATION_TEST_ID,
                        "permutation_test_sidedness": "two_sided_abs_effect",
                        "permutation_statistic_abs_rate_difference": observed_abs_effect,
                        "permutation_p_value_two_sided": float(p_value_two_sided),
                        "n_permutations": int(permutations_effective),
                    }
                ]
            )
        finally:
            record_runtime_timing(
                "simulation.duplicates_exact_position_permutation",
                (perf_counter() - started) * 1000.0,
            )
            record_runtime_counter("simulation.duplicates_exact_position_permutation.calls", 1)
            record_runtime_counter(
                "simulation.duplicates_exact_position_permutation.permutations",
                int(permutations_effective),
            )
            record_runtime_counter(
                "simulation.duplicates_exact_position_permutation.successful_results",
                1 if has_result else 0,
            )

    def _temporal_metrics_by_name(
        self,
        working: pd.DataFrame,
        key_column: str,
        *,
        rng: np.random.Generator,
        inferential_name_gate: set[str] | None = None,
    ) -> pd.DataFrame:
        started = perf_counter()
        rows: list[dict[str, object]] = []
        draws = 0
        cached_sample_sizes = 0
        cached_conditioned_signatures = 0
        conditioned_downgraded_rows = 0
        try:
            if working.empty:
                return pd.DataFrame()
            gate_names: set[str] | None = None
            if inferential_name_gate is not None:
                gate_names = {
                    str(value).strip()
                    for value in inferential_name_gate
                    if str(value).strip()
                }
            all_times = pd.to_datetime(working["timestamp"], errors="coerce").dropna().to_numpy(
                dtype="datetime64[m]"
            )
            if all_times.size == 0:
                return pd.DataFrame()
            all_minutes = all_times.astype("datetime64[m]").astype(np.int64)
            position_series = _safe_str_series(
                working.get("position_normalized", pd.Series(dtype=str))
            ).replace("", "Unknown")
            working = working.assign(_position_key=position_series)
            draws = min(self.temporal_permutation_draws, 1000)
            if draws <= 0:
                return pd.DataFrame()

            duplicate_keys = (
                working.groupby(key_column, dropna=False)
                .size()
                .rename("observed_count")
                .reset_index()
            )
            duplicate_keys = duplicate_keys[duplicate_keys["observed_count"] >= 2]
            if duplicate_keys.empty:
                return pd.DataFrame()
            working = working[
                working[key_column].isin(duplicate_keys[key_column].astype(str).tolist())
            ].copy()
            if working.empty:
                return pd.DataFrame()

            temporal_null_cache: dict[int, tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
            conditioned_temporal_null_cache: dict[
                tuple[tuple[str, int], ...],
                tuple[np.ndarray, np.ndarray, np.ndarray],
            ] = {}
            all_minutes_by_position: dict[str, np.ndarray] = {}
            for position_value, position_frame in working.groupby("_position_key", dropna=False):
                normalized_position = str(position_value).strip() or "Unknown"
                position_minutes = pd.to_datetime(
                    position_frame["timestamp"], errors="coerce"
                ).dropna()
                if position_minutes.empty:
                    all_minutes_by_position[normalized_position] = np.empty(0, dtype=np.int64)
                    continue
                all_minutes_by_position[normalized_position] = position_minutes.to_numpy(
                    dtype="datetime64[m]"
                ).astype(np.int64)

            def _cached_temporal_null(sample_size: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
                cached = temporal_null_cache.get(sample_size)
                if cached is not None:
                    return cached
                min_gap_null = np.empty(draws, dtype=np.int64)
                within_5_null = np.empty(draws, dtype=np.int64)
                within_15_null = np.empty(draws, dtype=np.int64)
                for draw_idx in range(draws):
                    sampled = np.sort(rng.choice(all_minutes, size=sample_size, replace=False))
                    sampled_gaps = np.diff(sampled)
                    if sampled_gaps.size:
                        min_gap_null[draw_idx] = int(sampled_gaps.min())
                        within_5_null[draw_idx] = int(np.count_nonzero(sampled_gaps <= 5))
                        within_15_null[draw_idx] = int(np.count_nonzero(sampled_gaps <= 15))
                    else:
                        min_gap_null[draw_idx] = 0
                        within_5_null[draw_idx] = 0
                        within_15_null[draw_idx] = 0
                out = (min_gap_null, within_5_null, within_15_null)
                temporal_null_cache[sample_size] = out
                return out

            def _signature_key(position_counts: dict[str, int]) -> tuple[tuple[str, int], ...]:
                return tuple(
                    sorted(
                        (
                            str(position).strip() or "Unknown",
                            int(count),
                        )
                        for position, count in position_counts.items()
                        if int(count) > 0
                    )
                )

            def _cached_conditioned_temporal_null(
                position_counts: dict[str, int],
            ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
                signature = _signature_key(position_counts)
                cached = conditioned_temporal_null_cache.get(signature)
                if cached is not None:
                    return cached
                min_gap_null = np.empty(draws, dtype=np.int64)
                within_5_null = np.empty(draws, dtype=np.int64)
                within_15_null = np.empty(draws, dtype=np.int64)
                for draw_idx in range(draws):
                    sampled_chunks: list[np.ndarray] = []
                    for position_value, requested_count in signature:
                        pool = all_minutes_by_position.get(position_value, np.empty(0, dtype=np.int64))
                        sampled_chunks.append(
                            np.asarray(
                                rng.choice(pool, size=int(requested_count), replace=False),
                                dtype=np.int64,
                            )
                        )
                    sampled = (
                        np.sort(np.concatenate(sampled_chunks))
                        if sampled_chunks
                        else np.empty(0, dtype=np.int64)
                    )
                    sampled_gaps = np.diff(sampled)
                    if sampled_gaps.size:
                        min_gap_null[draw_idx] = int(sampled_gaps.min())
                        within_5_null[draw_idx] = int(np.count_nonzero(sampled_gaps <= 5))
                        within_15_null[draw_idx] = int(np.count_nonzero(sampled_gaps <= 15))
                    else:
                        min_gap_null[draw_idx] = 0
                        within_5_null[draw_idx] = 0
                        within_15_null[draw_idx] = 0
                out = (min_gap_null, within_5_null, within_15_null)
                conditioned_temporal_null_cache[signature] = out
                return out

            for key, group in working.groupby(key_column, dropna=False):
                times = pd.to_datetime(group["timestamp"], errors="coerce").dropna().to_numpy(
                    dtype="datetime64[m]"
                )
                if times.size < 2:
                    continue
                minutes = np.sort(times.astype("datetime64[m]").astype(np.int64))
                gaps = np.diff(minutes)
                min_gap = int(gaps.min()) if gaps.size else 0
                within_5 = int(np.sum(gaps <= 5))
                within_15 = int(np.sum(gaps <= 15))
                span_minutes = int(minutes.max() - minutes.min()) if minutes.size else 0

                canonical_name = str(key).strip()
                sample_size = int(len(minutes))
                inferential_gate_passed = (
                    True if gate_names is None else canonical_name in gate_names
                )
                temporal_null_conditioning_supported = True
                temporal_null_support_reason = self.TEMPORAL_NULL_SUPPORT_REASON_SUPPORTED
                min_gap_null = np.empty(0, dtype=np.int64)
                within_5_null = np.empty(0, dtype=np.int64)
                within_15_null = np.empty(0, dtype=np.int64)
                if self.temporal_null_mode == self.TEMPORAL_NULL_MODE_HEARING_INTENSITY_BY_POSITION:
                    position_counts = (
                        _safe_str_series(group.get("_position_key", pd.Series(dtype=str)))
                        .replace("", "Unknown")
                        .value_counts(dropna=False)
                        .astype(int)
                        .to_dict()
                    )
                    for position_value, requested_count in position_counts.items():
                        pool_size = int(
                            len(
                                all_minutes_by_position.get(
                                    str(position_value).strip() or "Unknown",
                                    np.empty(0, dtype=np.int64),
                                )
                            )
                        )
                        # Require at least one non-self row in each conditioning stratum;
                        # otherwise the conditioned null has no alternative support.
                        if pool_size <= int(requested_count):
                            temporal_null_conditioning_supported = False
                            temporal_null_support_reason = (
                                self.TEMPORAL_NULL_SUPPORT_REASON_POSITION_POOL_SPARSE
                            )
                            conditioned_downgraded_rows += 1
                            break
                    if temporal_null_conditioning_supported:
                        min_gap_null, within_5_null, within_15_null = (
                            _cached_conditioned_temporal_null(position_counts)
                        )
                else:
                    min_gap_null, within_5_null, within_15_null = _cached_temporal_null(
                        sample_size
                    )

                temporal_null_supported = (
                    inferential_gate_passed and temporal_null_conditioning_supported
                )
                if not inferential_gate_passed:
                    temporal_null_support_reason = self.TEMPORAL_NULL_SUPPORT_REASON_NAME_NOT_GATED

                p_value_min_gap = np.nan
                p_value_within_5 = np.nan
                p_value_within_15 = np.nan
                draws_effective = 0
                if temporal_null_supported and draws:
                    p_value_min_gap = float((np.sum(min_gap_null <= min_gap) + 1) / (draws + 1))
                    p_value_within_5 = float(
                        (np.sum(within_5_null >= within_5) + 1) / (draws + 1)
                    )
                    p_value_within_15 = float(
                        (np.sum(within_15_null >= within_15) + 1) / (draws + 1)
                    )
                    draws_effective = draws

                rows.append(
                    {
                        "canonical_name": canonical_name,
                        "min_gap_minutes": min_gap,
                        "within_5m_pairs": within_5,
                        "within_15m_pairs": within_15,
                        "time_span_minutes": span_minutes,
                        "temporal_p_value_min_gap": p_value_min_gap,
                        "temporal_p_value_within_5m": p_value_within_5,
                        "temporal_p_value_within_15m": p_value_within_15,
                        "temporal_permutation_draws": int(draws_effective),
                        "temporal_null_model": self.temporal_null_mode,
                        "temporal_null_supported": bool(temporal_null_supported),
                        "temporal_null_support_reason": temporal_null_support_reason,
                        "temporal_inferential_name_gate_passed": bool(inferential_gate_passed),
                    }
                )
            cached_sample_sizes = int(len(temporal_null_cache))
            cached_conditioned_signatures = int(len(conditioned_temporal_null_cache))
            if conditioned_downgraded_rows > 0:
                LOGGER.info(
                    "duplicates_exact temporal conditioned-null downgrade rows=%s mode=%s",
                    int(conditioned_downgraded_rows),
                    self.temporal_null_mode,
                )
            return pd.DataFrame(rows)
        finally:
            record_runtime_timing(
                "simulation.duplicates_exact_temporal_null",
                (perf_counter() - started) * 1000.0,
            )
            record_runtime_counter("simulation.duplicates_exact_temporal_null.calls", 1)
            record_runtime_counter(
                "simulation.duplicates_exact_temporal_null.draws_effective",
                max(int(draws), 0),
            )
            record_runtime_counter(
                "simulation.duplicates_exact_temporal_null.cached_sample_sizes",
                max(int(cached_sample_sizes), 0),
            )
            record_runtime_counter(
                "simulation.duplicates_exact_temporal_null.cached_conditioned_signatures",
                max(int(cached_conditioned_signatures), 0),
            )
            record_runtime_counter(
                "simulation.duplicates_exact_temporal_null.conditioned_downgraded_rows",
                max(int(conditioned_downgraded_rows), 0),
            )
            record_runtime_counter(
                "simulation.duplicates_exact_temporal_null.output_rows",
                int(len(rows)),
            )

    def _legacy_duplicate_metrics_overview(
        self,
        *,
        scope_overview: pd.DataFrame,
        n_rows: int,
    ) -> pd.DataFrame:
        if scope_overview.empty:
            return pd.DataFrame(
                columns=[
                    "metric",
                    "observed_value",
                    "expected_mean",
                    "expected_p05",
                    "expected_p50",
                    "expected_p95",
                    "excess_over_expected",
                    "p_value_one_sided",
                ]
            )
        by_metric = scope_overview.set_index("metric")
        repeated = by_metric.loc["repeated_group_rows"] if "repeated_group_rows" in by_metric.index else None
        pairs = by_metric.loc["pairs"] if "pairs" in by_metric.index else None
        rows: list[dict[str, float | str]] = []
        if repeated is not None:
            observed = float(repeated["observed"])
            expected = float(repeated["expected"])
            rows.append(
                {
                    "metric": "duplicate_rows",
                    "observed_value": observed,
                    "expected_mean": expected,
                    "expected_p05": float(repeated["expected_p05"]),
                    "expected_p50": float(repeated["expected_p50"]),
                    "expected_p95": float(repeated["expected_p95"]),
                    "excess_over_expected": observed - expected,
                    "p_value_one_sided": float(repeated["p_value"]),
                }
            )
            rows.append(
                {
                    "metric": "duplicate_row_rate",
                    "observed_value": observed / float(max(n_rows, 1)),
                    "expected_mean": expected / float(max(n_rows, 1)),
                    "expected_p05": float(repeated["expected_p05"]) / float(max(n_rows, 1)),
                    "expected_p50": float(repeated["expected_p50"]) / float(max(n_rows, 1)),
                    "expected_p95": float(repeated["expected_p95"]) / float(max(n_rows, 1)),
                    "excess_over_expected": (observed - expected) / float(max(n_rows, 1)),
                    "p_value_one_sided": float(repeated["p_value"]),
                }
            )
        if pairs is not None:
            observed_pairs = float(pairs["observed"])
            expected_pairs = float(pairs["expected"])
            rows.append(
                {
                    "metric": "duplicate_pairs",
                    "observed_value": observed_pairs,
                    "expected_mean": expected_pairs,
                    "expected_p05": float(pairs["expected_p05"]),
                    "expected_p50": float(pairs["expected_p50"]),
                    "expected_p95": float(pairs["expected_p95"]),
                    "excess_over_expected": observed_pairs - expected_pairs,
                    "p_value_one_sided": float(pairs["p_value"]),
                }
            )
        return pd.DataFrame(rows)

    @staticmethod
    def _normalize_context_token(value: object) -> str:
        return str(value or "").strip().lower()

    def _load_contextual_baseline(self) -> pd.DataFrame:
        path_value = self.contextual_baseline_path
        if not path_value:
            return pd.DataFrame()
        path = Path(path_value)
        if not path.exists():
            return pd.DataFrame()
        try:
            if path.suffix.lower() == ".json":
                payload = json.loads(path.read_text(encoding="utf-8"))
                rows_raw = payload.get("rows", []) if isinstance(payload, dict) else payload
                contextual = pd.DataFrame(rows_raw if isinstance(rows_raw, list) else [])
            elif path.suffix.lower() == ".parquet":
                contextual = pd.read_parquet(path)
            else:
                contextual = pd.read_csv(path)
        except Exception:
            return pd.DataFrame()
        if contextual.empty:
            return pd.DataFrame()
        normalized = contextual.copy()
        normalized["bucket_minutes"] = (
            pd.to_numeric(normalized.get("bucket_minutes"), errors="coerce").fillna(-1).astype(int)
        )
        normalized["hour_bin"] = (
            pd.to_numeric(normalized.get("hour_bin"), errors="coerce").fillna(-1).astype(int)
        )
        normalized["weekday_bin"] = (
            pd.to_numeric(normalized.get("weekday_bin"), errors="coerce").fillna(-1).astype(int)
        )
        normalized["committee"] = normalized.get("committee", "").map(self._normalize_context_token)
        normalized["chamber"] = normalized.get("chamber", "").map(self._normalize_context_token)
        normalized["shrink_k"] = pd.to_numeric(
            normalized.get("shrink_k", self.position_baseline_shrink_k), errors="coerce"
        ).fillna(self.position_baseline_shrink_k)
        normalized["n_rows_total"] = pd.to_numeric(
            normalized.get("n_rows_total", 0), errors="coerce"
        ).fillna(0.0)
        return normalized

    def _resolve_contextual_shrink_k(
        self,
        *,
        contextual_baseline: pd.DataFrame,
        bucket_start: pd.Timestamp,
        bucket_minutes: int,
    ) -> tuple[float, str]:
        default_k = float(self.position_baseline_shrink_k)
        if contextual_baseline.empty:
            return default_k, "default"
        if pd.isna(bucket_start):
            return default_k, "default"
        hour_bin = int(pd.Timestamp(bucket_start).hour)
        weekday_bin = int(pd.Timestamp(bucket_start).weekday())
        committee = self._normalize_context_token(self.contextual_committee)
        chamber = self._normalize_context_token(self.contextual_chamber)

        by_bucket = contextual_baseline[
            contextual_baseline["bucket_minutes"].astype(int) == int(bucket_minutes)
        ].copy()
        if by_bucket.empty:
            return default_k, "default"

        fallbacks: list[tuple[str, dict[str, object]]] = []
        if committee and chamber:
            fallbacks.append(
                (
                    "committee_chamber_hour_weekday_bucket",
                    {
                        "committee": committee,
                        "chamber": chamber,
                        "hour_bin": hour_bin,
                        "weekday_bin": weekday_bin,
                    },
                )
            )
        if committee:
            fallbacks.append(
                (
                    "committee_hour_weekday_bucket",
                    {"committee": committee, "hour_bin": hour_bin, "weekday_bin": weekday_bin},
                )
            )
        if chamber:
            fallbacks.append(
                (
                    "chamber_hour_weekday_bucket",
                    {"chamber": chamber, "hour_bin": hour_bin, "weekday_bin": weekday_bin},
                )
            )
        fallbacks.append(
            (
                "hour_weekday_bucket",
                {"hour_bin": hour_bin, "weekday_bin": weekday_bin},
            )
        )
        fallbacks.append(("bucket", {}))

        for level, required in fallbacks:
            candidates = by_bucket.copy()
            for column, value in required.items():
                if column not in candidates.columns:
                    candidates = pd.DataFrame()
                    break
                candidates = candidates[candidates[column] == value]
            if candidates.empty:
                continue
            ordered = candidates.sort_values(["n_rows_total", "shrink_k"], ascending=[False, True])
            candidate_k = float(ordered["shrink_k"].iloc[0])
            if not np.isfinite(candidate_k) or candidate_k < 0:
                candidate_k = default_k
            return candidate_k, level
        return default_k, "default"

    @staticmethod
    def _histogram_digest(histogram: pd.DataFrame) -> str:
        if not isinstance(histogram, pd.DataFrame) or histogram.empty:
            return "empty"
        digest_frame = pd.DataFrame(
            {
                "name_count": pd.to_numeric(histogram.get("name_count", 0), errors="coerce")
                .fillna(0)
                .astype(int),
                "n_keys": pd.to_numeric(histogram.get("n_keys", 0), errors="coerce")
                .fillna(0)
                .astype(int),
                "N": pd.to_numeric(histogram.get("N", 0), errors="coerce").fillna(0).astype(int),
            }
        )
        if digest_frame.empty:
            return "empty"
        digest_frame = digest_frame.sort_values(
            ["name_count", "n_keys", "N"],
            ascending=[True, True, True],
        ).reset_index(drop=True)
        return sha1(digest_frame.to_csv(index=False).encode("utf-8")).hexdigest()

    def _simulate_collision_null_cached(
        self,
        *,
        n_rows: int,
        histogram: pd.DataFrame,
        draws: int,
        rng: np.random.Generator,
        baseline_model: CollisionBaselineModel,
        n_population: int | None,
        max_draws: int,
        tail_observed: dict[str, float] | None,
        cache: dict[tuple[int, str, int, int, str, str], pd.DataFrame],
        histogram_digest_cache: dict[int, str],
    ) -> pd.DataFrame:
        normalized_rows = int(max(int(n_rows), 0))
        normalized_draws = int(max(int(draws), 0))
        normalized_max_draws = int(max(int(max_draws), 0))
        if normalized_rows <= 0 or normalized_draws <= 0 or normalized_max_draws <= 0:
            return pd.DataFrame(columns=["pairs", "excess_rows", "repeated_group_rows"])
        normalized_population = int(max(int(n_population or 0), 0))
        histogram_id = id(histogram)
        histogram_digest = histogram_digest_cache.get(histogram_id)
        if histogram_digest is None:
            histogram_digest = self._histogram_digest(histogram)
            histogram_digest_cache[histogram_id] = histogram_digest
        normalized_tail_observed: dict[str, float] = {}
        for metric, value in (tail_observed or {}).items():
            metric_name = str(metric).strip().lower()
            if metric_name not in COLLISION_METRICS:
                continue
            try:
                numeric_value = float(value)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(numeric_value):
                continue
            normalized_tail_observed[metric_name] = numeric_value
        if normalized_tail_observed:
            tail_signature = "|".join(
                f"{metric}:{normalized_tail_observed[metric]:.8f}"
                for metric in sorted(normalized_tail_observed)
            )
        else:
            tail_signature = "__none__"
        cache_key = (
            normalized_rows,
            str(baseline_model),
            normalized_population,
            normalized_max_draws,
            histogram_digest,
            tail_signature,
        )
        cached = cache.get(cache_key)
        if cached is not None:
            record_runtime_counter(
                "detector.duplicates_exact.simulation.collision_null_from_histogram.cache_hit",
                1,
            )
            return cached.copy()
        record_runtime_counter(
            "detector.duplicates_exact.simulation.collision_null_from_histogram.cache_miss",
            1,
        )
        null_samples = simulate_collision_null_from_histogram(
            n_rows=normalized_rows,
            histogram=histogram,
            draws=normalized_draws,
            rng=rng,
            baseline_model=baseline_model,
            n_population=normalized_population if normalized_population > 0 else None,
            max_draws=normalized_max_draws,
            min_draws=self.monte_carlo_min_draws,
            target_p_mcse=self.monte_carlo_target_p_mcse,
            decision_p_threshold=self.monte_carlo_decision_p_threshold,
            decision_confidence_level=self.monte_carlo_decision_confidence_level,
            tail_observed=normalized_tail_observed or None,
        )
        cache[cache_key] = null_samples.copy()
        return null_samples

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        if df.empty:
            return DetectorResult(detector=self.name, summary={"n_records": 0}, tables={})

        with profile_runtime_block("detector.duplicates_exact.prepare_working"):
            working = df.copy()
            key_column = self._resolved_collision_key_column(working)
            working[key_column] = _safe_str_series(working[key_column])
            working = working[working[key_column] != ""].copy()
            if working.empty:
                return DetectorResult(detector=self.name, summary={"n_records": 0}, tables={})
            working["canonical_name"] = _safe_str_series(working.get("canonical_name", working[key_column]))
            working["position_normalized"] = _safe_str_series(
                working.get("position_normalized", "Unknown")
            ).replace("", "Unknown")
            working["timestamp"] = pd.to_datetime(working.get("timestamp"), errors="coerce")
            working["minute_bucket"] = pd.to_datetime(working.get("minute_bucket"), errors="coerce")
            working["name_display"] = _safe_str_series(
                working.get("name_display", working[key_column].map(lambda value: str(value)))
            )
            infer = working.copy()
            infer_no_person_rows = False
            if self.exclude_non_person_from_inference and "is_person_name" in infer.columns:
                infer = infer[infer["is_person_name"].astype(bool)].copy()
                infer_no_person_rows = bool(infer.empty and not working.empty)
        record_runtime_counter("detector.duplicates_exact.rows.working", int(len(working)))
        record_runtime_counter("detector.duplicates_exact.rows.inference", int(len(infer)))

        # Use per-submethod RNG streams so unrelated stochastic paths do not perturb each other.
        rng_streams, rng_seed_lineage = self._spawn_rng_streams(root_seed=self.random_seed)
        rng_lineage_columns = self._flatten_rng_lineage(lineage=rng_seed_lineage)
        rng_scope_collision = rng_streams[self.RNG_STREAM_SCOPE_COLLISION]
        rng_scope_stratified_collision = rng_streams[self.RNG_STREAM_SCOPE_STRATIFIED_COLLISION]
        rng_bucket_collision = rng_streams[self.RNG_STREAM_BUCKET_COLLISION]
        rng_bucket_stratified_collision = rng_streams[self.RNG_STREAM_BUCKET_STRATIFIED_COLLISION]
        rng_position_interval = rng_streams[self.RNG_STREAM_POSITION_INTERVAL]
        rng_position_permutation = rng_streams[self.RNG_STREAM_POSITION_PERMUTATION]
        rng_position_cluster_bootstrap = rng_streams[self.RNG_STREAM_POSITION_CLUSTER_BOOTSTRAP]
        rng_temporal_permutation = rng_streams[self.RNG_STREAM_TEMPORAL_PERMUTATION]
        LOGGER.info(
            "duplicates_exact rng lineage root_seed=%s root_stream=%s",
            rng_lineage_columns.get("rng_root_seed"),
            rng_lineage_columns.get("rng_root_stream_id"),
        )
        with profile_runtime_block("detector.duplicates_exact.resolve_scope_frames"):
            scope_frames, scope_availability = self._scope_frames(
                infer=infer,
                features=features,
                no_person_rows_after_filter=infer_no_person_rows,
            )
            scope_names = self._scope_list()
            for required_scope in scope_names:
                if required_scope not in scope_frames:
                    scope_frames[required_scope] = pd.DataFrame(columns=infer.columns)
                if required_scope not in scope_availability:
                    scope_availability[required_scope] = {
                        "scope_status": self.SCOPE_STATUS_UNAVAILABLE,
                        "scope_reason": self.SCOPE_REASON_UNAVAILABLE_NO_ROWS_AFTER_FILTERING,
                    }
        record_runtime_counter("detector.duplicates_exact.scope.count", int(len(scope_names)))
        record_runtime_counter(
            "detector.duplicates_exact.scope.unavailable_count",
            int(
                sum(
                    1
                    for scope in scope_names
                    if scope_availability.get(scope, {}).get("scope_status")
                    != self.SCOPE_STATUS_AVAILABLE
                )
            ),
        )

        normalization_version_value, normalization_hash = self._normalization_version_metadata()
        methods_rows: list[dict[str, object]] = []
        overview_frames: list[pd.DataFrame] = []
        bucket_frames: list[pd.DataFrame] = []
        position_bucket_frames: list[dict[str, object]] = []
        per_name_tests_frames: list[pd.DataFrame] = []
        per_name_display_frames: list[pd.DataFrame] = []
        per_name_duplicates_by_mode_frames: list[pd.DataFrame] = []
        per_name_submission_timing_by_mode_frames: list[pd.DataFrame] = []
        temporal_frames: list[pd.DataFrame] = []
        top_name_timing_frames: list[pd.DataFrame] = []
        stratified_sensitivity_frames: list[pd.DataFrame] = []
        legacy_null_distribution = pd.DataFrame()
        legacy_duplicate_by_bucket = pd.DataFrame()
        legacy_per_name_anomalies = pd.DataFrame()
        legacy_top_repeated = pd.DataFrame()
        legacy_position_metrics = pd.DataFrame()
        legacy_position_tests = pd.DataFrame()
        legacy_repeated_same_bucket = pd.DataFrame()
        legacy_repeated_same_bucket_summary = pd.DataFrame()
        legacy_repeated_same_minute = pd.DataFrame()
        legacy_switch_names = pd.DataFrame()
        legacy_swing_impact = pd.DataFrame()
        primary_scope_row_count = 0
        primary_scope_unique_count = 0
        primary_scope_repeated = 0.0
        primary_scope_pairs = 0.0
        primary_scope_significant = 0
        primary_scope_baseline_source = "hearing_empirical"
        primary_scope_degraded = False
        primary_scope_n_used = 0
        primary_scope_n_population = 0
        primary_scope_low_power = True
        primary_scope_stratification = "none"
        primary_scope_inferential_status = "descriptive_only"
        primary_scope_inferential_reason = self.INFERENTIAL_REASON_SELF_REFERENTIAL_BASELINE
        primary_scope_status = self.SCOPE_STATUS_UNAVAILABLE
        primary_scope_reason = self.SCOPE_REASON_UNAVAILABLE_NO_ROWS_AFTER_FILTERING
        position_claim_eligible = False
        position_claim_reason = self.POSITION_CLAIM_REASON_NO_POSITION_ROWS
        requested_stratification = self.collision_stratification
        effective_stratification = requested_stratification
        stratification_degraded = False
        stratum_frequencies = pd.DataFrame(columns=["name_key", "stratum", "n_registry_rows"])
        with profile_runtime_block("detector.duplicates_exact.load_contextual_baseline"):
            contextual_baseline = self._load_contextual_baseline()
        contextual_shrink_cache: dict[tuple[int, int, int], tuple[float, str]] = {}
        null_simulation_cache: dict[tuple[int, str, int, int, str, str], pd.DataFrame] = {}
        histogram_digest_cache: dict[int, str] = {}

        with profile_runtime_block("detector.duplicates_exact.prepare_stratification"):
            if requested_stratification != "none":
                if self.collision_baseline_source == "hearing_empirical":
                    if self.collision_baseline_failure_policy == "fail":
                        raise RuntimeError(
                            "collision_stratification requires a registry-derived baseline_source."
                        )
                    effective_stratification = "none"
                    stratification_degraded = True
                elif not self.voter_db_url:
                    if self.collision_baseline_failure_policy == "fail":
                        raise RuntimeError(
                            "collision_stratification requires voter_registry.db_url but none was configured."
                        )
                    effective_stratification = "none"
                    stratification_degraded = True
                else:
                    try:
                        stratum_frequencies = fetch_voter_name_key_stratum_frequencies(
                            db_url=self.voter_db_url,
                            table_name=self.voter_table_name,
                            key_column=key_column,
                            stratification=requested_stratification,
                            active_only=self.voter_active_only,
                        )
                        if stratum_frequencies.empty:
                            raise RuntimeError("No registry rows available for requested stratification.")
                    except Exception as exc:
                        if self.collision_baseline_failure_policy == "fail":
                            raise RuntimeError(
                                f"Failed loading stratified collision baseline inputs: {exc}"
                            ) from exc
                        effective_stratification = "none"
                        stratification_degraded = True

        for scope in scope_names:
            record_runtime_counter("detector.duplicates_exact.scope.iterations", 1)
            scope_prepare_started = perf_counter()
            scope_frame = scope_frames.get(scope, pd.DataFrame(columns=infer.columns)).copy()
            scope_frame[key_column] = _safe_str_series(scope_frame.get(key_column, pd.Series(dtype=str)))
            scope_frame = scope_frame[scope_frame[key_column] != ""].copy()
            scope_meta = scope_availability.get(
                scope,
                {
                    "scope_status": (
                        self.SCOPE_STATUS_AVAILABLE
                        if not scope_frame.empty
                        else self.SCOPE_STATUS_UNAVAILABLE
                    ),
                    "scope_reason": (
                        self.SCOPE_REASON_AVAILABLE
                        if not scope_frame.empty
                        else self.SCOPE_REASON_UNAVAILABLE_NO_ROWS_AFTER_FILTERING
                    ),
                },
            )
            scope_status = str(scope_meta.get("scope_status", "")).strip() or (
                self.SCOPE_STATUS_AVAILABLE
                if not scope_frame.empty
                else self.SCOPE_STATUS_UNAVAILABLE
            )
            scope_reason = str(scope_meta.get("scope_reason", "")).strip() or (
                self.SCOPE_REASON_AVAILABLE
                if scope_status == self.SCOPE_STATUS_AVAILABLE
                else self.SCOPE_REASON_UNAVAILABLE_NO_ROWS_AFTER_FILTERING
            )
            if not scope_frame.empty:
                position_series = _safe_str_series(
                    scope_frame.get(
                        "position_normalized",
                        pd.Series("Unknown", index=scope_frame.index, dtype=str),
                    )
                ).replace("", "Unknown")
                scope_frame["position_normalized"] = position_series
                scope_frame["_n_pro"] = (position_series == "Pro").astype(np.int64)
                scope_frame["_n_con"] = (position_series == "Con").astype(np.int64)
                scope_frame["_n_unknown"] = (position_series == "Unknown").astype(np.int64)
                scope_frame["_n_other_position"] = (~position_series.isin({"Pro", "Con"})).astype(
                    np.int64
                )
            else:
                scope_frame["_n_pro"] = pd.Series(dtype=np.int64)
                scope_frame["_n_con"] = pd.Series(dtype=np.int64)
                scope_frame["_n_unknown"] = pd.Series(dtype=np.int64)
                scope_frame["_n_other_position"] = pd.Series(dtype=np.int64)
            record_runtime_timing(
                "detector.duplicates_exact.scope.prepare_frame",
                (perf_counter() - scope_prepare_started) * 1000.0,
            )
            record_runtime_counter("detector.duplicates_exact.scope.rows_total", int(len(scope_frame)))
            record_runtime_counter(
                "detector.duplicates_exact.scope.unique_names_total",
                int(scope_frame[key_column].nunique()) if not scope_frame.empty else 0,
            )

            scope_top_name_started = perf_counter()
            scope_top_name_timing: list[pd.DataFrame] = []
            for mode_spec in _TOP_NAME_TIMING_MATCH_MODES:
                mode_key_column = str(mode_spec.get("key_column", "")).strip()
                if not mode_key_column or mode_key_column not in scope_frame.columns:
                    continue
                mode_frame = scope_frame.copy()
                mode_frame["name_key"] = _safe_str_series(mode_frame[mode_key_column])
                mode_frame = mode_frame[mode_frame["name_key"] != ""].copy()
                if mode_frame.empty:
                    continue
                mode_frame["display_name_mode"] = _safe_str_series(
                    mode_frame.get("name_display", mode_frame["name_key"])
                )

                mode_totals_all = (
                    mode_frame.groupby("name_key", dropna=False)
                    .agg(
                        total_repeated_rows=("id", "count"),
                        observed_count=("id", "count"),
                        display_name=("display_name_mode", "first"),
                        n_pro=("_n_pro", "sum"),
                        n_con=("_n_con", "sum"),
                        first_seen=("timestamp", "min"),
                        last_seen=("timestamp", "max"),
                    )
                    .reset_index()
                )
                mode_totals_all = mode_totals_all[mode_totals_all["total_repeated_rows"] >= 2].copy()
                if mode_totals_all.empty:
                    continue
                mode_totals_all["time_span_minutes"] = (
                    (
                        pd.to_datetime(mode_totals_all["last_seen"], errors="coerce")
                        - pd.to_datetime(mode_totals_all["first_seen"], errors="coerce")
                    ).dt.total_seconds()
                    / 60.0
                ).fillna(0.0)
                mode_totals_all["scope"] = scope
                mode_totals_all["match_mode"] = str(mode_spec.get("match_mode", ""))
                mode_totals_all["match_label"] = str(mode_spec.get("match_label", ""))
                mode_totals_all["match_definition"] = str(mode_spec.get("match_definition", ""))
                mode_totals_all["canonical_name"] = mode_totals_all["name_key"].astype(str)
                per_name_duplicates_by_mode_frames.append(
                    mode_totals_all[
                        [
                            "scope",
                            "match_mode",
                            "match_label",
                            "match_definition",
                            "canonical_name",
                            "name_key",
                            "display_name",
                            "observed_count",
                            "total_repeated_rows",
                            "n_pro",
                            "n_con",
                            "first_seen",
                            "last_seen",
                            "time_span_minutes",
                        ]
                    ].copy()
                )
                duplicate_name_keys = set(mode_totals_all["name_key"].astype(str).tolist())
                if duplicate_name_keys:
                    mode_duplicate_rows = mode_frame[mode_frame["name_key"].isin(duplicate_name_keys)].copy()
                    if not mode_duplicate_rows.empty:
                        mode_duplicate_rows["scope"] = scope
                        mode_duplicate_rows["match_mode"] = str(mode_spec.get("match_mode", ""))
                        mode_duplicate_rows["match_label"] = str(mode_spec.get("match_label", ""))
                        mode_duplicate_rows["match_definition"] = str(mode_spec.get("match_definition", ""))
                        mode_duplicate_rows["canonical_name"] = mode_duplicate_rows["name_key"].astype(str)
                        mode_duplicate_rows = mode_duplicate_rows.rename(
                            columns={
                                "display_name_mode": "display_name",
                                "minute_bucket": "bucket_start",
                            }
                        )
                        mode_duplicate_rows["bucket_start"] = pd.to_datetime(
                            mode_duplicate_rows["bucket_start"], errors="coerce"
                        )
                        mode_duplicate_rows = mode_duplicate_rows.dropna(subset=["bucket_start"])
                        if not mode_duplicate_rows.empty:
                            per_name_submission_timing_by_mode_frames.append(
                                mode_duplicate_rows[
                                    [
                                        "scope",
                                        "match_mode",
                                        "match_label",
                                        "match_definition",
                                        "canonical_name",
                                        "name_key",
                                        "display_name",
                                        "bucket_start",
                                        "position_normalized",
                                    ]
                                ].copy()
                            )

                mode_totals = mode_totals_all.sort_values(
                    ["total_repeated_rows", "display_name", "name_key"],
                    ascending=[False, True, True],
                ).head(_TOP_NAME_TIMING_TOP_N)
                mode_totals = mode_totals.sort_values(
                    ["total_repeated_rows", "display_name", "name_key"],
                    ascending=[False, True, True],
                ).reset_index(drop=True)
                mode_totals["rank"] = mode_totals.index.astype(int) + 1

                top_keys = set(mode_totals["name_key"].astype(str).tolist())
                if not top_keys:
                    continue
                mode_top = mode_frame[mode_frame["name_key"].isin(top_keys)].copy()
                for bucket_minutes in self.bucket_minutes:
                    bucket_start = pd.to_datetime(mode_top["minute_bucket"], errors="coerce").dt.floor(
                        f"{int(bucket_minutes)}min"
                    )
                    mode_bucketed = mode_top.assign(bucket_start=bucket_start).dropna(
                        subset=["bucket_start"]
                    )
                    if mode_bucketed.empty:
                        continue

                    bucket_counts = (
                        mode_bucketed.groupby(["name_key", "bucket_start"], dropna=False)
                        .agg(
                            duplicate_rows=("id", "count"),
                            n_pro=("_n_pro", "sum"),
                            n_con=("_n_con", "sum"),
                            n_other=("_n_other_position", "sum"),
                            first_seen=("timestamp", "min"),
                            last_seen=("timestamp", "max"),
                        )
                        .reset_index()
                    )
                    if bucket_counts.empty:
                        continue

                    merged = bucket_counts.merge(
                        mode_totals[["name_key", "display_name", "total_repeated_rows", "rank"]],
                        on="name_key",
                        how="inner",
                    )
                    if merged.empty:
                        continue
                    merged["scope"] = scope
                    merged["match_mode"] = str(mode_spec.get("match_mode", ""))
                    merged["match_label"] = str(mode_spec.get("match_label", ""))
                    merged["match_definition"] = str(mode_spec.get("match_definition", ""))
                    merged["bucket_minutes"] = int(bucket_minutes)
                    scope_top_name_timing.append(
                        merged[
                            [
                                "scope",
                                "match_mode",
                                "match_label",
                                "match_definition",
                                "rank",
                                "name_key",
                                "display_name",
                                "total_repeated_rows",
                                "bucket_start",
                                "bucket_minutes",
                                "duplicate_rows",
                                "n_pro",
                                "n_con",
                                "n_other",
                                "first_seen",
                                "last_seen",
                            ]
                        ].copy()
                    )
            record_runtime_timing(
                "detector.duplicates_exact.scope.top_name_timing",
                (perf_counter() - scope_top_name_started) * 1000.0,
            )
            if scope_top_name_timing:
                top_name_timing_frames.append(pd.concat(scope_top_name_timing, ignore_index=True))

            scope_inference_started = perf_counter()
            grouped = (
                scope_frame.groupby(key_column, dropna=False)
                .agg(
                    observed_count=("id", "count"),
                    n_pro=("_n_pro", "sum"),
                    n_con=("_n_con", "sum"),
                    first_seen=("timestamp", "min"),
                    last_seen=("timestamp", "max"),
                    display_name=("name_display", "first"),
                )
                .reset_index()
                .rename(columns={key_column: "canonical_name"})
            )
            if grouped.empty:
                grouped["time_span_minutes"] = pd.Series(dtype=float)
            else:
                first_seen = pd.to_datetime(grouped["first_seen"], errors="coerce")
                last_seen = pd.to_datetime(grouped["last_seen"], errors="coerce")
                grouped["time_span_minutes"] = (
                    (last_seen - first_seen).dt.total_seconds() / 60.0
                ).fillna(0.0)
            observed_counts = grouped.set_index("canonical_name")["observed_count"] if not grouped.empty else pd.Series(dtype=float)
            histogram, effective_baseline_source, baseline_degraded = self._resolve_histogram(
                observed_counts=observed_counts,
                key_column=key_column,
            )
            n_population = int(
                pd.to_numeric(histogram.get("N", pd.Series(dtype=float)), errors="coerce")
                .dropna()
                .max()
                if not histogram.empty and "N" in histogram.columns
                else 0
            )
            n_scope = int(len(scope_frame))
            observed_metrics = collision_metrics_from_counts(grouped["observed_count"].to_numpy(dtype=float))
            expected_metrics_unstratified = expected_collision_metrics(
                n_rows=n_scope,
                histogram=histogram,
                baseline_model=self.collision_baseline_model,
                n_population=n_population if n_population > 0 else None,
            )
            stratified_probabilities = pd.Series(dtype=float)
            scope_stratum_weights = pd.Series(dtype=float)
            stratified_sampling_weights = np.asarray([], dtype=float)
            stratified_sampling_keys: list[np.ndarray] = []
            stratified_sampling_probabilities: list[np.ndarray] = []
            effective_scope_stratification = effective_stratification
            if (
                effective_scope_stratification != "none"
                and effective_baseline_source != "hearing_empirical"
                and not stratum_frequencies.empty
            ):
                stratified_probabilities, scope_stratum_weights, stratified_n_population = (
                    self._mixture_probabilities_from_strata(
                        observed_counts=observed_counts.astype(float),
                        stratum_frequencies=stratum_frequencies,
                    )
                )
                if not stratified_probabilities.empty and stratified_n_population > 0:
                    n_population = int(stratified_n_population)
                    (
                        stratified_sampling_weights,
                        stratified_sampling_keys,
                        stratified_sampling_probabilities,
                    ) = self._build_stratified_sampling_inputs(
                        stratum_frequencies=stratum_frequencies,
                        stratum_weights=scope_stratum_weights,
                    )
                else:
                    effective_scope_stratification = "none"
                    stratification_degraded = True

            stratified_null_histogram = pd.DataFrame()
            if effective_scope_stratification != "none" and not stratified_probabilities.empty:
                if self.collision_baseline_model == "multinomial":
                    expected_metrics = expected_collision_metrics_from_probabilities(
                        n_rows=n_scope,
                        probabilities=stratified_probabilities.to_numpy(dtype=float),
                    )
                else:
                    stratified_null_histogram = histogram_from_probabilities(
                        probabilities=stratified_probabilities.to_numpy(dtype=float),
                        n_population=max(int(n_population), 1),
                    )
                    expected_metrics = expected_collision_metrics(
                        n_rows=n_scope,
                        histogram=stratified_null_histogram,
                        baseline_model=self.collision_baseline_model,
                        n_population=n_population if n_population > 0 else None,
                    )
            else:
                expected_metrics = expected_metrics_unstratified

            if self.collision_uncertainty_mode == "monte_carlo":
                scope_max_draws = self._collision_monte_carlo_draw_budget(
                    n_rows=n_scope,
                    hard_cap=1000,
                )
                if (
                    effective_scope_stratification != "none"
                    and not stratified_probabilities.empty
                    and self.collision_baseline_model == "multinomial"
                ):
                    if (
                        stratified_sampling_weights.size > 0
                        and stratified_sampling_keys
                        and stratified_sampling_probabilities
                    ):
                        null_samples = self._simulate_stratified_collision_null(
                            n_rows=n_scope,
                            draws=self.monte_carlo_draws,
                            rng=rng_scope_stratified_collision,
                            stratum_weights=stratified_sampling_weights,
                            stratum_keys=stratified_sampling_keys,
                            stratum_probabilities=stratified_sampling_probabilities,
                            max_draws=scope_max_draws,
                            tail_observed=observed_metrics,
                        )
                    else:
                        if stratified_null_histogram.empty:
                            stratified_null_histogram = histogram_from_probabilities(
                                probabilities=stratified_probabilities.to_numpy(dtype=float),
                                n_population=max(int(n_population), 1),
                            )
                        null_samples = self._simulate_collision_null_cached(
                            n_rows=n_scope,
                            histogram=stratified_null_histogram,
                            draws=self.monte_carlo_draws,
                            rng=rng_scope_collision,
                            baseline_model="multinomial",
                            n_population=n_population if n_population > 0 else None,
                            max_draws=scope_max_draws,
                            tail_observed=observed_metrics,
                            cache=null_simulation_cache,
                            histogram_digest_cache=histogram_digest_cache,
                        )
                else:
                    null_samples = self._simulate_collision_null_cached(
                        n_rows=n_scope,
                        histogram=histogram,
                        draws=self.monte_carlo_draws,
                        rng=rng_scope_collision,
                        baseline_model=self.collision_baseline_model,
                        n_population=n_population if n_population > 0 else None,
                        max_draws=scope_max_draws,
                        tail_observed=observed_metrics,
                        cache=null_simulation_cache,
                        histogram_digest_cache=histogram_digest_cache,
                    )
            else:
                null_samples = pd.DataFrame()
            scope_degraded = bool(
                baseline_degraded
                or stratification_degraded
                or (
                    requested_stratification != "none"
                    and effective_scope_stratification != requested_stratification
                )
            )
            scope_inferential_status, scope_inferential_reason = self._scope_inferential_metadata(
                baseline_source=effective_baseline_source,
                baseline_degraded=scope_degraded,
                null_samples=null_samples,
            )
            if scope_status != self.SCOPE_STATUS_AVAILABLE:
                scope_inferential_status = "unavailable"
                scope_inferential_reason = self.INFERENTIAL_REASON_SCOPE_UNAVAILABLE
            if scope_inferential_status != "reference_model_inference":
                LOGGER.info(
                    "duplicates_exact scope=%s status=%s reason=%s scope_status=%s scope_reason=%s baseline_source=%s baseline_degraded=%s",
                    scope,
                    scope_inferential_status,
                    scope_inferential_reason,
                    scope_status,
                    scope_reason,
                    effective_baseline_source,
                    scope_degraded,
                )
            overview = summarize_collision_observed_vs_null(
                observed=observed_metrics,
                expected=expected_metrics,
                null_samples=null_samples,
                metrics=self.collision_metrics,
            )
            if scope_inferential_status != "reference_model_inference":
                # Descriptive-only/unavailable scopes intentionally suppress inferential fields.
                for inferential_column in (
                    "expected_p05",
                    "expected_p50",
                    "expected_p95",
                    "z_score",
                    "p_value",
                    "monte_carlo_quantile_resolution",
                    "monte_carlo_p_value_mcse",
                    "monte_carlo_p_value_ci_low",
                    "monte_carlo_p_value_ci_high",
                ):
                    if inferential_column in overview.columns:
                        overview[inferential_column] = np.nan
            overview["scope"] = scope
            overview["n_used"] = int(n_scope)
            overview["N_used"] = int(n_population)
            overview["scope_status"] = scope_status
            overview["scope_reason"] = scope_reason
            overview["inferential_status"] = scope_inferential_status
            overview["inferential_reason"] = scope_inferential_reason
            overview_frames.append(overview)

            stratified_sensitivity_frames.append(
                pd.DataFrame(
                    {
                        "scope": scope,
                        "metric": self.collision_metrics,
                        "observed": [float(observed_metrics.get(metric, 0.0)) for metric in self.collision_metrics],
                        "expected_unstratified": [
                            float(expected_metrics_unstratified.get(metric, 0.0))
                            for metric in self.collision_metrics
                        ],
                        "expected_effective": [
                            float(expected_metrics.get(metric, 0.0)) for metric in self.collision_metrics
                        ],
                        "delta_expected": [
                            float(expected_metrics.get(metric, 0.0) - expected_metrics_unstratified.get(metric, 0.0))
                            for metric in self.collision_metrics
                        ],
                        "n_used": int(n_scope),
                        "N_used": int(n_population),
                        "stratification_requested": requested_stratification,
                        "stratification_effective": effective_scope_stratification,
                    }
                )
            )
            record_runtime_timing(
                "detector.duplicates_exact.scope.expected_metrics_and_null",
                (perf_counter() - scope_inference_started) * 1000.0,
            )

            per_name_started = perf_counter()
            key_values = grouped["canonical_name"].astype(str).tolist()
            if effective_scope_stratification != "none" and not stratified_probabilities.empty:
                grouped["population_probability"] = (
                    grouped["canonical_name"].map(stratified_probabilities).fillna(0.0).astype(float)
                )
                grouped["population_count"] = (
                    grouped["population_probability"] * float(max(n_population, 1))
                )
            else:
                population_counts = self._population_counts_for_observed_names(
                    key_column=key_column,
                    key_values=key_values,
                    observed_counts=observed_counts.astype(float),
                    effective_baseline_source=effective_baseline_source,
                )
                grouped["population_count"] = (
                    grouped["canonical_name"].map(population_counts).fillna(0.0).astype(float)
                )
                denom = float(max(n_population, 1))
                grouped["population_probability"] = grouped["population_count"] / denom
            grouped["expected_count"] = grouped["population_probability"] * float(n_scope)
            grouped["scope"] = scope
            grouped["scope_status"] = scope_status
            grouped["scope_reason"] = scope_reason
            grouped["inferential_status"] = scope_inferential_status
            grouped["inferential_reason"] = scope_inferential_reason
            if scope_inferential_status == "reference_model_inference":
                if grouped.empty:
                    grouped["p_value"] = pd.Series(dtype=float)
                elif self.per_name_significance_model == "hypergeometric_tail":
                    if _uses_default_hypergeometric_tail():
                        grouped["p_value"] = self._vectorized_hypergeometric_tail_p_values(
                            observed_successes=grouped["observed_count"],
                            population_size=int(max(n_population, 0)),
                            population_successes=grouped["population_count"],
                            sample_size=int(n_scope),
                        )
                    else:
                        grouped["p_value"] = grouped.apply(
                            lambda row: hypergeometric_tail_p_value(
                                observed_successes=int(row["observed_count"]),
                                population_size=int(max(n_population, 0)),
                                population_successes=int(max(row["population_count"], 0)),
                                sample_size=int(n_scope),
                            ),
                            axis=1,
                        )
                else:
                    if _uses_default_binomial_tail():
                        grouped["p_value"] = self._vectorized_binomial_tail_p_values(
                            observed_successes=grouped["observed_count"],
                            total_trials=int(n_scope),
                            success_probabilities=grouped["population_probability"],
                        )
                    else:
                        grouped["p_value"] = grouped.apply(
                            lambda row: binomial_tail_p_value(
                                observed_successes=int(row["observed_count"]),
                                total_trials=int(n_scope),
                                success_probability=float(
                                    max(min(row["population_probability"], 1.0), 0.0)
                                ),
                            ),
                            axis=1,
                        )
                grouped["q_value"] = benjamini_hochberg(grouped["p_value"]).fillna(1.0)
                grouped["is_significant"] = grouped["q_value"] <= self.bh_fdr_q
                grouped["tested"] = True
            else:
                grouped["p_value"] = np.nan
                grouped["q_value"] = np.nan
                grouped["is_significant"] = pd.Series(pd.NA, index=grouped.index, dtype="object")
                grouped["tested"] = False
            grouped = grouped.sort_values(["q_value", "p_value", "observed_count"], ascending=[True, True, False])
            per_name_tests_frames.append(
                grouped[
                    [
                        "scope",
                        "scope_status",
                        "scope_reason",
                        "inferential_status",
                        "inferential_reason",
                        "canonical_name",
                        "display_name",
                        "observed_count",
                        "expected_count",
                        "population_count",
                        "population_probability",
                        "p_value",
                        "q_value",
                        "is_significant",
                        "tested",
                        "n_pro",
                        "n_con",
                        "time_span_minutes",
                    ]
                ].copy()
            )
            display_limit = self.per_name_display_limit
            per_name_display = grouped.head(display_limit).copy()
            per_name_display["display_truncated"] = bool(len(grouped) > display_limit)
            per_name_display_frames.append(
                per_name_display[
                    [
                        "scope",
                        "scope_status",
                        "scope_reason",
                        "inferential_status",
                        "inferential_reason",
                        "canonical_name",
                        "display_name",
                        "observed_count",
                        "expected_count",
                        "population_count",
                        "population_probability",
                        "p_value",
                        "q_value",
                        "is_significant",
                        "tested",
                        "n_pro",
                        "n_con",
                        "time_span_minutes",
                        "display_truncated",
                    ]
                ].copy()
            )
            record_runtime_timing(
                "detector.duplicates_exact.scope.per_name_tests",
                (perf_counter() - per_name_started) * 1000.0,
            )

            temporal_inferential_gate: set[str] = set()
            if scope_inferential_status == "reference_model_inference" and not grouped.empty:
                temporal_significant = grouped[
                    grouped.get("is_significant", pd.Series(dtype=bool))
                    .fillna(False)
                    .astype(bool)
                ]
                temporal_inferential_gate = {
                    str(value).strip()
                    for value in temporal_significant.get(
                        "canonical_name", pd.Series(dtype=str)
                    ).tolist()
                    if str(value).strip()
                }
            temporal_started = perf_counter()
            temporal = self._temporal_metrics_by_name(
                scope_frame,
                key_column,
                rng=rng_temporal_permutation,
                inferential_name_gate=temporal_inferential_gate,
            )
            if not temporal.empty:
                temporal["scope"] = scope
                temporal["scope_status"] = scope_status
                temporal["scope_reason"] = scope_reason
                temporal["inferential_status"] = scope_inferential_status
                temporal["inferential_reason"] = scope_inferential_reason
                if scope_inferential_status != "reference_model_inference":
                    for temporal_column in (
                        "temporal_p_value_min_gap",
                        "temporal_p_value_within_5m",
                        "temporal_p_value_within_15m",
                    ):
                        if temporal_column in temporal.columns:
                            temporal[temporal_column] = np.nan
                temporal_frames.append(temporal)
            record_runtime_timing(
                "detector.duplicates_exact.scope.temporal_metrics",
                (perf_counter() - temporal_started) * 1000.0,
            )
            methods_rows.append(
                {
                    "scope": scope,
                    "baseline_source": effective_baseline_source,
                    "baseline_label": self._baseline_label_for_source(effective_baseline_source),
                    "baseline_model": self.collision_baseline_model,
                    "uncertainty_model": self.collision_uncertainty_mode,
                    "n_used": int(n_scope),
                    "N_used": int(n_population),
                    "scope_status": scope_status,
                    "scope_reason": scope_reason,
                    "metric_primary": self.collision_primary_metric,
                    "metrics_reported": ",".join(self.collision_metrics),
                    "baseline_degraded": scope_degraded,
                    "fallback_policy": self.collision_baseline_failure_policy,
                    "collision_key_mode": self.collision_key_mode,
                    "normalization_version": normalization_version_value,
                    "normalization_version_hash": normalization_hash,
                    "stratification": effective_scope_stratification,
                    "censored": bool(len(grouped) > self.per_name_display_limit),
                    "claim_class": self.COLLISION_CLAIM_CLASS,
                    "inferential_status": scope_inferential_status,
                    "inferential_reason": scope_inferential_reason,
                    "estimand_primary": self.STATISTICAL_CONTRACT_ESTIMAND_PRIMARY,
                    "non_goals": self.STATISTICAL_CONTRACT_NON_GOALS,
                    "baseline_semantics": self.STATISTICAL_CONTRACT_BASELINE_SEMANTICS,
                    **rng_lineage_columns,
                }
            )

            scope_bucket_scan_started = perf_counter()
            metric_summary_by_n: dict[int, dict[str, object]] = {}
            scope_all_name_counts = scope_frame.groupby(key_column, dropna=False).size().astype(float)
            scope_all_histogram = histogram_from_name_counts(scope_all_name_counts)
            scope_position_histograms: dict[str, pd.DataFrame] = {}
            if not scope_frame.empty:
                scope_positions = _safe_str_series(scope_frame["position_normalized"]).replace("", "Unknown")
                scope_for_position = scope_frame.assign(_position_key=scope_positions)
                for position_label, position_frame in scope_for_position.groupby(
                    "_position_key",
                    dropna=False,
                ):
                    n_position_rows = int(len(position_frame))
                    if n_position_rows <= 0:
                        continue
                    position_counts = position_frame.groupby(key_column, dropna=False).size().astype(float)
                    if position_counts.empty:
                        continue
                    normalized_position = str(position_label).strip() or "Unknown"
                    scope_position_histograms[normalized_position] = histogram_from_name_counts(position_counts)
            expected_all_primary_by_n: dict[int, float] = {}
            expected_position_primary_by_n: dict[str, dict[int, float]] = {}
            minute_series = pd.to_datetime(scope_frame.get("minute_bucket"), errors="coerce")
            scope_bucketed_by_minutes: dict[int, pd.DataFrame] = {}
            for bucket_minutes in self.bucket_minutes:
                bucket_start = minute_series.dt.floor(f"{int(bucket_minutes)}min")
                bucketed = scope_frame.assign(bucket_start=bucket_start).dropna(subset=["bucket_start"])
                if bucketed.empty:
                    continue
                scope_bucketed_by_minutes[int(bucket_minutes)] = bucketed
            record_runtime_counter(
                "detector.duplicates_exact.scope.bucketed_frame_cache.entries",
                int(len(scope_bucketed_by_minutes)),
            )
            for bucket_minutes in self.bucket_minutes:
                bucketed = scope_bucketed_by_minutes.get(int(bucket_minutes), pd.DataFrame())
                if bucketed.empty:
                    continue
                for start, group_frame in bucketed.groupby("bucket_start", dropna=False):
                    group_counts = group_frame.groupby(key_column, dropna=False).size().to_numpy(dtype=float)
                    metric_values = collision_metrics_from_counts(group_counts)
                    n_bucket = int(len(group_frame))
                    if n_bucket not in metric_summary_by_n:
                        if effective_scope_stratification != "none" and not stratified_probabilities.empty:
                            if self.collision_baseline_model == "multinomial":
                                expected_for_n = expected_collision_metrics_from_probabilities(
                                    n_rows=n_bucket,
                                    probabilities=stratified_probabilities.to_numpy(dtype=float),
                                )
                            else:
                                if stratified_null_histogram.empty:
                                    stratified_null_histogram = histogram_from_probabilities(
                                        probabilities=stratified_probabilities.to_numpy(dtype=float),
                                        n_population=max(int(n_population), 1),
                                    )
                                expected_for_n = expected_collision_metrics(
                                    n_rows=n_bucket,
                                    histogram=stratified_null_histogram,
                                        baseline_model=self.collision_baseline_model,
                                        n_population=n_population if n_population > 0 else None,
                                    )
                            expected_primary_for_n = float(
                                expected_for_n.get(self.collision_primary_metric, 0.0)
                            )
                            bucket_max_draws = self._bucket_monte_carlo_draw_budget(
                                n_rows=n_bucket,
                                expected_primary_metric=expected_primary_for_n,
                                hard_cap=250,
                            )
                            if (
                                self.collision_uncertainty_mode == "monte_carlo"
                                and self.collision_baseline_model == "multinomial"
                                and bucket_max_draws > 0
                            ):
                                if (
                                    stratified_sampling_weights.size > 0
                                    and stratified_sampling_keys
                                    and stratified_sampling_probabilities
                                ):
                                    null_for_n = self._simulate_stratified_collision_null(
                                        n_rows=n_bucket,
                                        draws=self.monte_carlo_draws,
                                        rng=rng_bucket_stratified_collision,
                                        stratum_weights=stratified_sampling_weights,
                                        stratum_keys=stratified_sampling_keys,
                                        stratum_probabilities=stratified_sampling_probabilities,
                                        max_draws=bucket_max_draws,
                                        tail_observed=None,
                                    )
                                else:
                                    null_for_n = self._simulate_collision_null_cached(
                                        n_rows=n_bucket,
                                        histogram=(
                                            stratified_null_histogram
                                            if not stratified_null_histogram.empty
                                            else histogram_from_probabilities(
                                                probabilities=stratified_probabilities.to_numpy(dtype=float),
                                                n_population=max(int(n_population), 1),
                                            )
                                        ),
                                        draws=self.monte_carlo_draws,
                                        rng=rng_bucket_collision,
                                        baseline_model="multinomial",
                                        n_population=n_population if n_population > 0 else None,
                                        max_draws=bucket_max_draws,
                                        tail_observed=None,
                                        cache=null_simulation_cache,
                                        histogram_digest_cache=histogram_digest_cache,
                                    )
                            else:
                                null_for_n = pd.DataFrame()
                        else:
                            expected_for_n = expected_collision_metrics(
                                n_rows=n_bucket,
                                histogram=histogram,
                                baseline_model=self.collision_baseline_model,
                                n_population=n_population if n_population > 0 else None,
                            )
                            expected_primary_for_n = float(
                                expected_for_n.get(self.collision_primary_metric, 0.0)
                            )
                            bucket_max_draws = self._bucket_monte_carlo_draw_budget(
                                n_rows=n_bucket,
                                expected_primary_metric=expected_primary_for_n,
                                hard_cap=250,
                            )
                            null_for_n = (
                                self._simulate_collision_null_cached(
                                    n_rows=n_bucket,
                                    histogram=histogram,
                                    draws=self.monte_carlo_draws,
                                    rng=rng_bucket_collision,
                                    baseline_model=self.collision_baseline_model,
                                    n_population=n_population if n_population > 0 else None,
                                    max_draws=bucket_max_draws,
                                    tail_observed=None,
                                    cache=null_simulation_cache,
                                    histogram_digest_cache=histogram_digest_cache,
                                )
                                if self.collision_uncertainty_mode == "monte_carlo"
                                and bucket_max_draws > 0
                                else pd.DataFrame()
                            )
                        summary_for_n = summarize_collision_observed_vs_null(
                            observed=metric_values,
                            expected=expected_for_n,
                            null_samples=null_for_n,
                            metrics=self.collision_metrics,
                        )
                        bucket_null_available = bool(not null_for_n.empty)
                        metric_summary_by_n[n_bucket] = {
                            "__null_samples_available": bucket_null_available,
                            **{
                                row.metric: {
                                    "expected": float(row.expected),
                                    "expected_p05": float(row.expected_p05),
                                    "expected_p95": float(row.expected_p95),
                                    "p_value": float(row.p_value),
                                    "z_score": float(row.z_score),
                                    "monte_carlo_draws_effective": int(
                                        getattr(row, "monte_carlo_draws_effective", 0)
                                    ),
                                    "monte_carlo_quantile_resolution": float(
                                        getattr(row, "monte_carlo_quantile_resolution", np.nan)
                                    ),
                                    "monte_carlo_p_value_mcse": float(
                                        getattr(row, "monte_carlo_p_value_mcse", np.nan)
                                    ),
                                    "monte_carlo_p_value_ci_low": float(
                                        getattr(row, "monte_carlo_p_value_ci_low", np.nan)
                                    ),
                                    "monte_carlo_p_value_ci_high": float(
                                        getattr(row, "monte_carlo_p_value_ci_high", np.nan)
                                    ),
                                }
                                for row in summary_for_n.itertuples(index=False)
                            },
                        }
                    n_unique = int(group_frame[key_column].nunique())
                    n_pro = int((group_frame["position_normalized"] == "Pro").sum())
                    n_con = int((group_frame["position_normalized"] == "Con").sum())
                    summary_for_bucket = metric_summary_by_n[n_bucket]
                    expected_primary_entry = summary_for_bucket.get(self.collision_primary_metric, {})
                    expected_primary_bucket = (
                        float(expected_primary_entry.get("expected", 0.0))
                        if isinstance(expected_primary_entry, dict)
                        else 0.0
                    )
                    low_power = bool(
                        n_unique < self.low_power_min_unique_names
                        or expected_primary_bucket < self.low_power_min_expected_duplicates
                    )
                    for metric in self.collision_metrics:
                        metric_obs = float(metric_values.get(metric, 0.0))
                        summary_entry_raw = summary_for_bucket.get(metric, {})
                        summary_entry = (
                            summary_entry_raw if isinstance(summary_entry_raw, dict) else {}
                        )
                        metric_exp = float(summary_entry.get("expected", 0.0))
                        metric_inferential_status = scope_inferential_status
                        metric_inferential_reason = scope_inferential_reason
                        if metric_inferential_status == "reference_model_inference":
                            if low_power:
                                metric_inferential_status = "descriptive_only"
                                metric_inferential_reason = self.INFERENTIAL_REASON_LOW_POWER
                            elif not bool(summary_for_bucket.get("__null_samples_available", False)):
                                metric_inferential_status = "unavailable"
                                metric_inferential_reason = self.INFERENTIAL_REASON_NO_NULL_SAMPLES
                        metric_p05 = float(summary_entry.get("expected_p05", metric_exp))
                        metric_p95 = float(summary_entry.get("expected_p95", metric_exp))
                        metric_z = float(summary_entry.get("z_score", 0.0))
                        metric_p = float(summary_entry.get("p_value", 1.0))
                        metric_mc_draws = int(summary_entry.get("monte_carlo_draws_effective", 0))
                        metric_quantile_resolution = float(
                            summary_entry.get("monte_carlo_quantile_resolution", np.nan)
                        )
                        metric_p_mcse = float(summary_entry.get("monte_carlo_p_value_mcse", np.nan))
                        metric_p_ci_low = float(summary_entry.get("monte_carlo_p_value_ci_low", np.nan))
                        metric_p_ci_high = float(summary_entry.get("monte_carlo_p_value_ci_high", np.nan))
                        if metric_inferential_status != "reference_model_inference":
                            metric_p05 = np.nan
                            metric_p95 = np.nan
                            metric_z = np.nan
                            metric_p = np.nan
                            metric_quantile_resolution = np.nan
                            metric_p_mcse = np.nan
                            metric_p_ci_low = np.nan
                            metric_p_ci_high = np.nan
                        bucket_frames.append(
                            {
                                "scope": scope,
                                "metric": metric,
                                "bucket_start": pd.to_datetime(start),
                                "bucket_minutes": int(bucket_minutes),
                                "n_bucket": int(n_bucket),
                                "n_used": int(n_scope),
                                "N_used": int(n_population),
                                "n_unique_names": int(n_unique),
                                "n_pro": int(n_pro),
                                "n_con": int(n_con),
                                "observed": metric_obs,
                                "expected": metric_exp,
                                "expected_p05": metric_p05,
                                "expected_p95": metric_p95,
                                "z_score": metric_z,
                                "p_value": metric_p,
                                "monte_carlo_draws_effective": int(metric_mc_draws),
                                "monte_carlo_quantile_resolution": metric_quantile_resolution,
                                "monte_carlo_p_value_mcse": metric_p_mcse,
                                "monte_carlo_p_value_ci_low": metric_p_ci_low,
                                "monte_carlo_p_value_ci_high": metric_p_ci_high,
                                "excess": float(metric_obs - metric_exp),
                                "baseline_model": self.collision_baseline_model,
                                "baseline_source": effective_baseline_source,
                                "baseline_degraded": bool(scope_degraded),
                                "scope_status": scope_status,
                                "scope_reason": scope_reason,
                                "is_low_power": low_power,
                                "inference_status": (
                                    "tested"
                                    if metric_inferential_status == "reference_model_inference"
                                    else metric_inferential_status
                                ),
                                "inferential_status": metric_inferential_status,
                                "inferential_reason": metric_inferential_reason,
                            }
                        )
                    if self.position_hearing_baseline_enabled and not scope_all_histogram.empty:
                        if n_bucket > 0:
                            for position_label in sorted(
                                {
                                    str(value).strip() or "Unknown"
                                    for value in group_frame["position_normalized"].tolist()
                                }
                            ):
                                subset = group_frame[
                                    _safe_str_series(group_frame["position_normalized"]).replace("", "Unknown")
                                    == position_label
                                ].copy()
                                n_side = int(len(subset))
                                if n_side <= 0:
                                    continue
                                side_counts = subset.groupby(key_column, dropna=False).size().astype(float)
                                if side_counts.empty:
                                    continue
                                position_histogram = scope_position_histograms.get(position_label)
                                if position_histogram is None or position_histogram.empty:
                                    position_histogram = scope_all_histogram
                                if position_histogram.empty:
                                    continue
                                bucket_start_ts = pd.to_datetime(start)
                                cache_key = (
                                    int(bucket_minutes),
                                    int(bucket_start_ts.hour),
                                    int(bucket_start_ts.weekday()),
                                )
                                cached_contextual = contextual_shrink_cache.get(cache_key)
                                if cached_contextual is not None:
                                    record_runtime_counter(
                                        "detector.duplicates_exact.contextual_shrink.cache_hit",
                                        1,
                                    )
                                    shrink_k, prior_level = cached_contextual
                                else:
                                    record_runtime_counter(
                                        "detector.duplicates_exact.contextual_shrink.cache_miss",
                                        1,
                                    )
                                    shrink_k, prior_level = self._resolve_contextual_shrink_k(
                                        contextual_baseline=contextual_baseline,
                                        bucket_start=bucket_start_ts,
                                        bucket_minutes=int(bucket_minutes),
                                    )
                                    contextual_shrink_cache[cache_key] = (
                                        float(shrink_k),
                                        str(prior_level),
                                    )
                                lambda_side = (
                                    float(n_side) / float(n_side + shrink_k)
                                    if (float(n_side + shrink_k) > 0.0)
                                    else 1.0
                                )
                                if n_side not in expected_all_primary_by_n:
                                    expected_all_metrics = expected_collision_metrics(
                                        n_rows=n_side,
                                        histogram=scope_all_histogram,
                                        baseline_model=self.collision_baseline_model,
                                        n_population=None,
                                    )
                                    expected_all_primary_by_n[n_side] = float(
                                        expected_all_metrics.get(self.collision_primary_metric, 0.0)
                                    )
                                position_cache = expected_position_primary_by_n.setdefault(position_label, {})
                                if n_side not in position_cache:
                                    expected_position_metrics = expected_collision_metrics(
                                        n_rows=n_side,
                                        histogram=position_histogram,
                                        baseline_model=self.collision_baseline_model,
                                        n_population=None,
                                    )
                                    position_cache[n_side] = float(
                                        expected_position_metrics.get(self.collision_primary_metric, 0.0)
                                    )
                                observed_position_metrics = collision_metrics_from_counts(
                                    side_counts.to_numpy(dtype=float)
                                )
                                observed_primary = float(
                                    observed_position_metrics.get(self.collision_primary_metric, 0.0)
                                )
                                expected_primary = float(
                                    lambda_side * position_cache[n_side]
                                    + (1.0 - lambda_side) * expected_all_primary_by_n[n_side]
                                )
                                n_side_unique = int(side_counts.size)
                                low_power_position = bool(
                                    n_side_unique < self.low_power_min_unique_names
                                    or expected_primary < self.low_power_min_expected_duplicates
                                )
                                position_inference_status = (
                                    "descriptive_only" if low_power_position else "tested"
                                )
                                position_inferential_reason = (
                                    self.INFERENTIAL_REASON_LOW_POWER
                                    if low_power_position
                                    else self.INFERENTIAL_REASON_REFERENCE_MODEL_INFERENCE
                                )
                                if scope_inferential_status != "reference_model_inference":
                                    position_inference_status = scope_inferential_status
                                    position_inferential_reason = scope_inferential_reason
                                deviance = float(observed_primary - expected_primary)
                                position_bucket_frames.append(
                                    {
                                        "scope": scope,
                                        "metric": self.collision_primary_metric,
                                        "bucket_start": pd.to_datetime(start),
                                        "bucket_minutes": int(bucket_minutes),
                                        "position_normalized": position_label,
                                        "n_bucket_position": int(n_side),
                                        "n_unique_names": int(n_side_unique),
                                        "observed": observed_primary,
                                        "expected": expected_primary,
                                        "excess": deviance,
                                        "deviance": deviance,
                                        "deviance_ratio": (
                                            float(deviance / expected_primary)
                                            if expected_primary > 0.0
                                            else 0.0
                                        ),
                                        "lambda_side": float(lambda_side),
                                        "shrink_k": float(shrink_k),
                                        "prior_level": str(prior_level),
                                        "is_low_power": bool(low_power_position),
                                        "scope_status": scope_status,
                                        "scope_reason": scope_reason,
                                        "inference_status": position_inference_status,
                                        "inferential_status": position_inference_status,
                                        "inferential_reason": position_inferential_reason,
                                    }
                                )
            record_runtime_timing(
                "detector.duplicates_exact.scope.bucket_scan",
                (perf_counter() - scope_bucket_scan_started) * 1000.0,
            )

            if scope == self.collision_scope_primary:
                primary_scope_started = perf_counter()
                primary_scope_row_count = int(n_scope)
                primary_scope_unique_count = int(observed_metrics["n_unique_names"])
                primary_scope_repeated = float(observed_metrics["repeated_group_rows"])
                primary_scope_pairs = float(observed_metrics["pairs"])
                primary_scope_significant = int(
                    pd.to_numeric(grouped.get("is_significant", pd.Series(dtype=float)), errors="coerce")
                    .fillna(0.0)
                    .astype(float)
                    .sum()
                )
                primary_scope_baseline_source = effective_baseline_source
                primary_scope_degraded = bool(scope_degraded)
                primary_scope_n_used = int(n_scope)
                primary_scope_n_population = int(n_population)
                primary_scope_stratification = effective_scope_stratification
                primary_scope_inferential_status = scope_inferential_status
                primary_scope_inferential_reason = scope_inferential_reason
                primary_scope_status = scope_status
                primary_scope_reason = scope_reason
                primary_scope_low_power = bool(
                    primary_scope_unique_count < self.low_power_min_unique_names
                    or float(expected_metrics.get(self.collision_primary_metric, 0.0))
                    < self.low_power_min_expected_duplicates
                )

                if not null_samples.empty:
                    legacy_null_distribution = null_samples.copy().reset_index(drop=True)
                    legacy_null_distribution["iteration"] = legacy_null_distribution.index.astype(int)
                    legacy_null_distribution["duplicate_rows"] = legacy_null_distribution["repeated_group_rows"]
                    if n_scope > 0:
                        legacy_null_distribution["duplicate_row_rate"] = (
                            legacy_null_distribution["duplicate_rows"] / float(n_scope)
                        )
                    else:
                        legacy_null_distribution["duplicate_row_rate"] = 0.0
                    legacy_null_distribution["duplicate_pairs"] = legacy_null_distribution["pairs"]
                    legacy_null_distribution["n_names_ge2"] = np.nan
                    legacy_null_distribution["n_names_ge3"] = np.nan
                    legacy_null_distribution["n_names_ge5"] = np.nan
                    legacy_null_distribution["n_names_ge10"] = np.nan
                    legacy_null_distribution["max_count"] = np.nan

                if bucket_frames:
                    primary_bucket = pd.DataFrame(bucket_frames)
                    primary_bucket = primary_bucket[
                        (primary_bucket["scope"] == scope)
                        & (primary_bucket["metric"] == self.collision_primary_metric)
                    ].copy()
                    if not primary_bucket.empty:
                        legacy_duplicate_by_bucket = primary_bucket.rename(
                            columns={
                                "n_bucket": "n_rows",
                                "observed": "duplicate_rows",
                                "expected": "expected_duplicate_rows",
                                "excess": "excess_duplicate_rows",
                            }
                        )[
                            [
                                "bucket_start",
                                "bucket_minutes",
                                "n_rows",
                                "n_unique_names",
                                "n_pro",
                                "n_con",
                                "duplicate_rows",
                                "expected_duplicate_rows",
                                "excess_duplicate_rows",
                            ]
                        ].copy()
                        legacy_duplicate_by_bucket["duplicate_row_rate"] = (
                            legacy_duplicate_by_bucket["duplicate_rows"]
                            / legacy_duplicate_by_bucket["n_rows"]
                        ).where(legacy_duplicate_by_bucket["n_rows"] > 0, 0.0)

                legacy_per_name_anomalies = per_name_display_frames[-1].rename(
                    columns={
                        "observed_count": "n",
                    }
                ).copy()
                if "inferential_status" in legacy_per_name_anomalies.columns:
                    legacy_per_name_anomalies["inference_status"] = legacy_per_name_anomalies[
                        "inferential_status"
                    ]
                else:
                    legacy_per_name_anomalies["inference_status"] = scope_inferential_status
                if "inferential_reason" in legacy_per_name_anomalies.columns:
                    legacy_per_name_anomalies["inference_reason"] = legacy_per_name_anomalies[
                        "inferential_reason"
                    ]
                else:
                    legacy_per_name_anomalies["inference_reason"] = scope_inferential_reason
                legacy_top_repeated = legacy_per_name_anomalies[
                    legacy_per_name_anomalies["n"] >= 2
                ][["display_name", "canonical_name", "n", "n_pro", "n_con", "time_span_minutes"]].copy()
                legacy_top_repeated = legacy_top_repeated.sort_values("n", ascending=False).head(self.top_n)

                position_rows: list[dict[str, object]] = []
                position_interval_by_n: dict[int, dict[str, float | int]] = {}
                position_model_supported = self.collision_baseline_model == "multinomial"
                position_metric_columns = [
                    "position_normalized",
                    "n_rows",
                    "n_unique_names",
                    "duplicate_rows",
                    "duplicate_row_rate",
                    "duplicate_pairs",
                    "expected_duplicate_rows",
                    "expected_duplicate_rows_p05",
                    "expected_duplicate_rows_p50",
                    "expected_duplicate_rows_p95",
                    "expected_duplicate_row_rate",
                    "expected_duplicate_row_rate_p05",
                    "expected_duplicate_row_rate_p50",
                    "expected_duplicate_row_rate_p95",
                    "interval_method_id",
                    "interval_draws_effective",
                    "excess_duplicate_rows",
                    "is_low_power",
                    "inference_status",
                ]
                for position in sorted(set(scope_frame["position_normalized"].unique())):
                    subset = scope_frame[scope_frame["position_normalized"] == position]
                    subset_counts = subset.groupby(key_column, dropna=False).size().to_numpy(dtype=float)
                    subset_metrics = collision_metrics_from_counts(subset_counts)
                    n_subset_rows = int(subset_metrics["n_rows"])
                    if n_subset_rows not in position_interval_by_n:
                        position_interval_by_n[n_subset_rows] = self._position_interval_from_histogram(
                            n_rows=n_subset_rows,
                            histogram=histogram,
                            n_population=n_population if n_population > 0 else None,
                            rng=rng_position_interval,
                        )
                    interval_stats = position_interval_by_n[n_subset_rows]
                    expected_rows = float(interval_stats.get("expected_duplicate_rows", 0.0))
                    low_power = bool(
                        n_subset_rows < self.position_claim_min_rows_per_position
                        or int(subset_metrics["n_unique_names"]) < self.low_power_min_unique_names
                        or expected_rows < self.low_power_min_expected_duplicates
                    )
                    has_interval_draws = int(interval_stats.get("interval_draws_effective", 0)) > 0
                    inference_status = (
                        "tested"
                        if position_model_supported and not low_power and has_interval_draws
                        else "descriptive_only"
                    )
                    position_rows.append(
                        {
                            "position_normalized": position,
                            "n_rows": n_subset_rows,
                            "n_unique_names": int(subset_metrics["n_unique_names"]),
                            "duplicate_rows": int(subset_metrics["repeated_group_rows"]),
                            "duplicate_row_rate": (
                                float(subset_metrics["repeated_group_rows"] / subset_metrics["n_rows"])
                                if subset_metrics["n_rows"] > 0
                                else 0.0
                            ),
                            "duplicate_pairs": float(subset_metrics["pairs"]),
                            "expected_duplicate_rows": float(expected_rows),
                            "expected_duplicate_rows_p05": float(
                                interval_stats.get("expected_duplicate_rows_p05", expected_rows)
                            ),
                            "expected_duplicate_rows_p50": float(
                                interval_stats.get("expected_duplicate_rows_p50", expected_rows)
                            ),
                            "expected_duplicate_rows_p95": float(
                                interval_stats.get("expected_duplicate_rows_p95", expected_rows)
                            ),
                            "expected_duplicate_row_rate": float(
                                interval_stats.get("expected_duplicate_row_rate", 0.0)
                            ),
                            "expected_duplicate_row_rate_p05": float(
                                interval_stats.get("expected_duplicate_row_rate_p05", 0.0)
                            ),
                            "expected_duplicate_row_rate_p50": float(
                                interval_stats.get("expected_duplicate_row_rate_p50", 0.0)
                            ),
                            "expected_duplicate_row_rate_p95": float(
                                interval_stats.get("expected_duplicate_row_rate_p95", 0.0)
                            ),
                            "interval_method_id": self.POSITION_INTERVAL_METHOD_ID,
                            "interval_draws_effective": int(
                                interval_stats.get("interval_draws_effective", 0)
                            ),
                            "excess_duplicate_rows": float(
                                max(subset_metrics["repeated_group_rows"] - expected_rows, 0.0)
                            ),
                            "is_low_power": low_power,
                            "inference_status": inference_status,
                        }
                    )
                legacy_position_metrics = pd.DataFrame(position_rows, columns=position_metric_columns)
                position_claim_eligible, position_claim_reason = self._position_claim_status(
                    position_metrics=legacy_position_metrics
                )
                if not position_claim_eligible and not legacy_position_metrics.empty:
                    legacy_position_metrics["inference_status"] = "descriptive_only"
                if position_model_supported:
                    permutation_draws = (
                        int(self.position_permutation_draws)
                        if position_claim_eligible
                        else int(min(256, int(self.position_permutation_draws)))
                    )
                    legacy_position_tests = self._position_permutation_test(
                        scope_frame,
                        key_column,
                        rng=rng_position_permutation,
                        bootstrap_rng=rng_position_cluster_bootstrap,
                        n_permutations=permutation_draws,
                    )
                else:
                    legacy_position_tests = pd.DataFrame()
                if not legacy_position_tests.empty and not legacy_position_metrics.empty:
                    legacy_position_tests["left_is_low_power"] = bool(
                        legacy_position_metrics.set_index("position_normalized")
                        .reindex(["Pro"])["is_low_power"]
                        .fillna(True)
                        .iloc[0]
                    )
                    legacy_position_tests["right_is_low_power"] = bool(
                        legacy_position_metrics.set_index("position_normalized")
                        .reindex(["Con"])["is_low_power"]
                        .fillna(True)
                        .iloc[0]
                    )

                duplicate_name_keys = set(
                    grouped[grouped["observed_count"] >= 2]["canonical_name"].astype(str).tolist()
                )
                repeated_same_bucket_frames: list[pd.DataFrame] = []
                for bucket_minutes in self.bucket_minutes:
                    bucketed_scope = scope_bucketed_by_minutes.get(int(bucket_minutes), pd.DataFrame())
                    if bucketed_scope.empty or not duplicate_name_keys:
                        continue
                    bucketed = bucketed_scope[
                        bucketed_scope[key_column].isin(duplicate_name_keys)
                    ].copy()
                    if bucketed.empty:
                        continue
                    repeated_same_bucket = (
                        bucketed.groupby([key_column, "bucket_start"], dropna=False)
                        .agg(
                            n=("id", "count"),
                            n_pro=("_n_pro", "sum"),
                            n_con=("_n_con", "sum"),
                            n_unknown=("_n_unknown", "sum"),
                        )
                        .reset_index()
                    )
                    repeated_same_bucket = repeated_same_bucket[repeated_same_bucket["n"] > 1]
                    if repeated_same_bucket.empty:
                        continue
                    repeated_same_bucket["bucket_minutes"] = int(bucket_minutes)
                    repeated_same_bucket["bucket_end"] = repeated_same_bucket["bucket_start"] + pd.Timedelta(
                        minutes=int(bucket_minutes) - 1
                    )
                    repeated_same_bucket = repeated_same_bucket.rename(columns={key_column: "canonical_name"})
                    repeated_same_bucket_frames.append(repeated_same_bucket)

                if repeated_same_bucket_frames:
                    legacy_repeated_same_bucket = (
                        pd.concat(repeated_same_bucket_frames, ignore_index=True)
                        .sort_values(
                            ["bucket_minutes", "n", "canonical_name", "bucket_start"],
                            ascending=[True, False, True, True],
                        )
                    )
                    legacy_repeated_same_bucket_summary = (
                        legacy_repeated_same_bucket.groupby("bucket_minutes", dropna=False)
                        .agg(
                            n_repeated_rows=("n", "sum"),
                            n_repeated_name_windows=("canonical_name", "count"),
                            n_unique_names=("canonical_name", "nunique"),
                            max_repeats_in_window=("n", "max"),
                        )
                        .reset_index()
                        .sort_values("bucket_minutes")
                    )
                    legacy_repeated_same_minute = legacy_repeated_same_bucket[
                        legacy_repeated_same_bucket["bucket_minutes"] == 1
                    ].copy()
                    if not legacy_repeated_same_minute.empty:
                        legacy_repeated_same_minute["minute_bucket"] = legacy_repeated_same_minute["bucket_start"]

                switch_names = grouped[(grouped["n_pro"] > 0) & (grouped["n_con"] > 0)].copy()
                legacy_switch_names = switch_names.rename(columns={"observed_count": "n"}).sort_values(
                    "n", ascending=False
                )

                swing_rows = []
                raw_pro = int((scope_frame["position_normalized"] == "Pro").sum())
                raw_con = int((scope_frame["position_normalized"] == "Con").sum())
                raw_total = raw_pro + raw_con
                if raw_total > 0:
                    swing_rows.append(
                        {
                            "scenario": "raw_rows",
                            "n_pro_effective": raw_pro,
                            "n_con_effective": raw_con,
                            "pro_share": raw_pro / raw_total,
                        }
                    )
                strict_col = "canonical_key_strict" if "canonical_key_strict" in scope_frame.columns else key_column
                strict_names = scope_frame[[strict_col, "position_normalized"]].copy()
                strict_names = strict_names[strict_names["position_normalized"].isin({"Pro", "Con"})]
                strict_dedup = (
                    strict_names.groupby(["position_normalized", strict_col], dropna=False)
                    .size()
                    .reset_index(name="n")
                )
                strict_pro = int((strict_dedup["position_normalized"] == "Pro").sum())
                strict_con = int((strict_dedup["position_normalized"] == "Con").sum())
                strict_total = strict_pro + strict_con
                if strict_total > 0:
                    swing_rows.append(
                        {
                            "scenario": "strict_unique_name_dedupe",
                            "n_pro_effective": strict_pro,
                            "n_con_effective": strict_con,
                            "pro_share": strict_pro / strict_total,
                        }
                    )
                pos_index = (
                    legacy_position_metrics.set_index("position_normalized")
                    if not legacy_position_metrics.empty
                    else pd.DataFrame()
                )
                pro_excess = float(pos_index.at["Pro", "excess_duplicate_rows"]) if "Pro" in pos_index.index else 0.0
                con_excess = float(pos_index.at["Con", "excess_duplicate_rows"]) if "Con" in pos_index.index else 0.0
                excess_pro = max(raw_pro - pro_excess, 0.0)
                excess_con = max(raw_con - con_excess, 0.0)
                excess_total = excess_pro + excess_con
                if excess_total > 0:
                    swing_rows.append(
                        {
                            "scenario": "excess_only_collision_adjustment",
                            "n_pro_effective": float(excess_pro),
                            "n_con_effective": float(excess_con),
                            "pro_share": float(excess_pro / excess_total),
                        }
                    )
                legacy_swing_impact = pd.DataFrame(swing_rows)
                record_runtime_timing(
                    "detector.duplicates_exact.scope.primary_legacy_outputs",
                    (perf_counter() - primary_scope_started) * 1000.0,
                )

        assemble_started = perf_counter()
        record_runtime_counter(
            "detector.duplicates_exact.simulation.collision_null_from_histogram.cache_size",
            int(len(null_simulation_cache)),
        )
        record_runtime_counter(
            "detector.duplicates_exact.contextual_shrink.cache_size",
            int(len(contextual_shrink_cache)),
        )
        collision_methods = pd.DataFrame(methods_rows)
        collision_overview = pd.concat(overview_frames, ignore_index=True) if overview_frames else pd.DataFrame()
        collision_by_bucket = (
            pd.DataFrame(bucket_frames).sort_values(["scope", "bucket_minutes", "bucket_start", "metric"])
            if bucket_frames
            else pd.DataFrame()
        )
        collision_by_bucket_position = (
            pd.DataFrame(position_bucket_frames).sort_values(
                ["scope", "bucket_minutes", "bucket_start", "position_normalized"]
            )
            if position_bucket_frames
            else pd.DataFrame()
        )
        per_name_tests = pd.concat(per_name_tests_frames, ignore_index=True) if per_name_tests_frames else pd.DataFrame()
        per_name_display = (
            pd.concat(per_name_display_frames, ignore_index=True) if per_name_display_frames else pd.DataFrame()
        )
        per_name_duplicates_by_mode = (
            pd.concat(per_name_duplicates_by_mode_frames, ignore_index=True).sort_values(
                ["scope", "match_mode", "observed_count", "display_name", "canonical_name"],
                ascending=[True, True, False, True, True],
            )
            if per_name_duplicates_by_mode_frames
            else pd.DataFrame()
        )
        per_name_submission_timing_by_mode = (
            pd.concat(per_name_submission_timing_by_mode_frames, ignore_index=True).sort_values(
                ["scope", "match_mode", "name_key", "bucket_start"]
            )
            if per_name_submission_timing_by_mode_frames
            else pd.DataFrame()
        )
        collision_stratification_sensitivity = (
            pd.concat(stratified_sensitivity_frames, ignore_index=True)
            if stratified_sensitivity_frames
            else pd.DataFrame()
        )
        temporal_burst = (
            pd.concat(temporal_frames, ignore_index=True).sort_values(
                ["scope", "temporal_p_value_within_5m", "temporal_p_value_min_gap", "within_5m_pairs"],
                ascending=[True, True, True, False],
            )
            if temporal_frames
            else pd.DataFrame()
        )
        top_name_timing_by_mode = (
            pd.concat(top_name_timing_frames, ignore_index=True).sort_values(
                ["scope", "match_mode", "rank", "bucket_minutes", "bucket_start", "name_key"]
            )
            if top_name_timing_frames
            else pd.DataFrame()
        )
        hypothesis_family_rows: list[dict[str, object]] = []
        family_a_test_scopes: set[str] = set()
        family_a_significant_scopes: set[str] = set()
        family_a_adjusted_by_scope: dict[str, float] = {}
        family_a_gate_reason_by_scope: dict[str, str] = {}
        n_family_a_tests = 0

        if not collision_overview.empty:
            collision_overview["scope"] = (
                collision_overview.get("scope", pd.Series(dtype=str)).fillna("").astype(str)
            )
            collision_overview["metric"] = (
                collision_overview.get("metric", pd.Series(dtype=str)).fillna("").astype(str)
            )
            collision_overview["scope_status"] = (
                collision_overview.get("scope_status", pd.Series(dtype=str)).fillna("").astype(str)
            )
            collision_overview["inferential_status"] = (
                collision_overview.get("inferential_status", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
            )
            collision_overview["inferential_reason"] = (
                collision_overview.get("inferential_reason", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
            )
            collision_overview["p_value"] = pd.to_numeric(
                collision_overview.get("p_value", pd.Series(dtype=float)),
                errors="coerce",
            )
            for metadata_column, default_value in (
                ("family_id", ""),
                ("adjustment_method", self.ADJUSTMENT_METHOD_NONE),
                ("n_tests", 0),
                ("n_tests_in_family", 0),
                ("eligible_by_gate", False),
                ("gate_reason", ""),
                ("adjusted_p_value", np.nan),
                ("is_significant", pd.NA),
            ):
                if metadata_column not in collision_overview.columns:
                    collision_overview[metadata_column] = default_value

            overview_scope_available = (
                collision_overview["scope_status"].str.strip().str.lower()
                == self.SCOPE_STATUS_AVAILABLE
            )
            overview_inferential_supported = (
                collision_overview["inferential_status"].str.strip().str.lower()
                == "reference_model_inference"
            )
            overview_primary_metric = (
                collision_overview["metric"].str.strip().str.lower()
                == self.PRIMARY_SCOPE_ENDPOINT_METRIC
            )
            overview_primary_rows = overview_primary_metric
            overview_valid_p = collision_overview["p_value"].notna() & np.isfinite(
                collision_overview["p_value"]
            )
            family_a_eligible = (
                overview_primary_metric
                & overview_scope_available
                & overview_inferential_supported
                & overview_valid_p
            )
            n_family_a_tests = int(family_a_eligible.sum())
            family_a_adjusted = self._adjust_p_values(
                collision_overview.loc[family_a_eligible, "p_value"],
                method=self.ADJUSTMENT_METHOD_HOLM,
            )
            collision_overview.loc[overview_primary_rows, "family_id"] = self.FAMILY_ID_SCOPE
            collision_overview.loc[
                overview_primary_rows, "adjustment_method"
            ] = self.ADJUSTMENT_METHOD_HOLM
            collision_overview.loc[overview_primary_rows, "n_tests"] = int(n_family_a_tests)
            collision_overview.loc[overview_primary_rows, "n_tests_in_family"] = int(
                n_family_a_tests
            )
            collision_overview.loc[overview_primary_rows, "eligible_by_gate"] = (
                family_a_eligible.loc[overview_primary_rows].astype(bool)
            )
            collision_overview.loc[family_a_eligible, "adjusted_p_value"] = family_a_adjusted
            collision_overview.loc[family_a_eligible, "is_significant"] = (
                pd.to_numeric(family_a_adjusted, errors="coerce")
                .astype(float)
                .le(float(self.bh_fdr_q))
                .astype("object")
            )

            family_a_gate_reason = pd.Series(
                self.GATE_REASON_FAMILY_A_NOT_TESTED,
                index=collision_overview.index,
                dtype="object",
            )
            family_a_gate_reason.loc[family_a_eligible] = self.GATE_REASON_ELIGIBLE
            family_a_gate_reason.loc[
                overview_primary_metric & (~overview_scope_available)
            ] = self.GATE_REASON_SCOPE_UNAVAILABLE
            family_a_gate_reason.loc[
                overview_primary_metric
                & overview_scope_available
                & (~overview_inferential_supported)
            ] = self.GATE_REASON_SCOPE_NOT_INFERENTIAL
            collision_overview.loc[overview_primary_rows, "gate_reason"] = family_a_gate_reason.loc[
                overview_primary_rows
            ]

            secondary_scope_rows = ~overview_primary_metric
            collision_overview.loc[secondary_scope_rows, "family_id"] = (
                f"{self.FAMILY_ID_SCOPE}_secondary"
            )
            collision_overview.loc[
                secondary_scope_rows, "adjustment_method"
            ] = self.ADJUSTMENT_METHOD_NONE
            collision_overview.loc[secondary_scope_rows, "n_tests"] = 0
            collision_overview.loc[secondary_scope_rows, "n_tests_in_family"] = 0
            collision_overview.loc[secondary_scope_rows, "eligible_by_gate"] = False
            collision_overview.loc[
                secondary_scope_rows, "gate_reason"
            ] = self.GATE_REASON_SECONDARY_SCOPE_METRIC
            collision_overview.loc[secondary_scope_rows, "adjusted_p_value"] = np.nan
            collision_overview.loc[secondary_scope_rows, "is_significant"] = pd.NA
            secondary_with_inference = (
                secondary_scope_rows
                & (
                    collision_overview["inferential_status"].str.strip().str.lower()
                    == "reference_model_inference"
                )
            )
            collision_overview.loc[
                secondary_with_inference, "inferential_status"
            ] = "descriptive_only"
            collision_overview.loc[
                secondary_with_inference, "inferential_reason"
            ] = self.INFERENTIAL_REASON_SECONDARY_SCOPE_METRIC
            for inferential_column in (
                "expected_p05",
                "expected_p50",
                "expected_p95",
                "z_score",
                "p_value",
                "monte_carlo_quantile_resolution",
                "monte_carlo_p_value_mcse",
                "monte_carlo_p_value_ci_low",
                "monte_carlo_p_value_ci_high",
            ):
                if inferential_column in collision_overview.columns:
                    collision_overview.loc[secondary_scope_rows, inferential_column] = np.nan

            if bool(family_a_eligible.any()):
                scope_level = (
                    collision_overview.loc[family_a_eligible, ["scope", "adjusted_p_value"]]
                    .dropna(subset=["scope"])
                    .drop_duplicates(subset=["scope"], keep="first")
                )
                family_a_test_scopes = {
                    str(value).strip()
                    for value in scope_level["scope"].tolist()
                    if str(value).strip()
                }
                family_a_adjusted_by_scope = {
                    str(row.scope): float(row.adjusted_p_value)
                    for row in scope_level.itertuples(index=False)
                    if pd.notna(row.adjusted_p_value)
                }
                family_a_significant_scopes = {
                    scope
                    for scope, adjusted in family_a_adjusted_by_scope.items()
                    if math.isfinite(float(adjusted)) and float(adjusted) <= float(self.bh_fdr_q)
                }

        if not collision_methods.empty:
            collision_methods["scope"] = (
                collision_methods.get("scope", pd.Series(dtype=str)).fillna("").astype(str)
            )
            collision_methods["scope_status"] = (
                collision_methods.get("scope_status", pd.Series(dtype=str)).fillna("").astype(str)
            )
            collision_methods["inferential_status"] = (
                collision_methods.get("inferential_status", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
            )
            for metadata_column, default_value in (
                ("family_id", self.FAMILY_ID_SCOPE),
                ("adjustment_method", self.ADJUSTMENT_METHOD_HOLM),
                ("n_tests", int(n_family_a_tests)),
                ("n_tests_in_family", int(n_family_a_tests)),
                ("eligible_by_gate", False),
                ("gate_reason", self.GATE_REASON_FAMILY_A_NOT_TESTED),
                ("adjusted_p_value", np.nan),
                ("is_significant", pd.NA),
            ):
                if metadata_column not in collision_methods.columns:
                    collision_methods[metadata_column] = default_value
            collision_methods["family_id"] = self.FAMILY_ID_SCOPE
            collision_methods["adjustment_method"] = self.ADJUSTMENT_METHOD_HOLM
            collision_methods["n_tests"] = int(n_family_a_tests)
            collision_methods["n_tests_in_family"] = int(n_family_a_tests)
            collision_methods["eligible_by_gate"] = collision_methods["scope"].isin(
                family_a_test_scopes
            )
            collision_methods["adjusted_p_value"] = (
                collision_methods["scope"].map(family_a_adjusted_by_scope).astype(float)
            )
            collision_methods["is_significant"] = (
                collision_methods["scope"].isin(family_a_significant_scopes).astype("object")
            )

        scope_methods_by_scope: dict[str, dict[str, object]] = {}
        if not collision_methods.empty:
            scope_methods_by_scope = {
                str(row.scope): {
                    "scope_status": str(getattr(row, "scope_status", "")).strip(),
                    "inferential_status": str(getattr(row, "inferential_status", "")).strip(),
                }
                for row in collision_methods.drop_duplicates(subset=["scope"], keep="first").itertuples(
                    index=False
                )
            }
        for scope_name in scope_names:
            normalized_scope = str(scope_name).strip()
            if not normalized_scope:
                continue
            if normalized_scope in family_a_significant_scopes:
                gate_reason_value = self.GATE_REASON_ELIGIBLE
            elif normalized_scope in family_a_test_scopes:
                gate_reason_value = self.GATE_REASON_FAMILY_A_NOT_SIGNIFICANT
            else:
                scope_meta = scope_methods_by_scope.get(normalized_scope, {})
                scope_status = str(scope_meta.get("scope_status", "")).strip().lower()
                inferential_status = str(scope_meta.get("inferential_status", "")).strip().lower()
                if scope_status and scope_status != self.SCOPE_STATUS_AVAILABLE:
                    gate_reason_value = self.GATE_REASON_SCOPE_UNAVAILABLE
                elif inferential_status and inferential_status != "reference_model_inference":
                    gate_reason_value = self.GATE_REASON_SCOPE_NOT_INFERENTIAL
                else:
                    gate_reason_value = self.GATE_REASON_FAMILY_A_NOT_TESTED
            family_a_gate_reason_by_scope[normalized_scope] = gate_reason_value

        if not collision_methods.empty:
            collision_methods["gate_reason"] = (
                collision_methods["scope"].map(family_a_gate_reason_by_scope).fillna(
                    self.GATE_REASON_FAMILY_A_NOT_TESTED
                )
            )

        if not per_name_tests.empty:
            per_name_tests["scope"] = (
                per_name_tests.get("scope", pd.Series(dtype=str)).fillna("").astype(str)
            )
            per_name_tests["canonical_name"] = (
                per_name_tests.get("canonical_name", pd.Series(dtype=str)).fillna("").astype(str)
            )
            per_name_tests["p_value"] = pd.to_numeric(
                per_name_tests.get("p_value", pd.Series(dtype=float)),
                errors="coerce",
            )
            per_name_tests["q_value"] = pd.to_numeric(
                per_name_tests.get("q_value", pd.Series(dtype=float)),
                errors="coerce",
            )
            if "tested" not in per_name_tests.columns:
                per_name_tests["tested"] = False
            per_name_tests["inferential_status"] = (
                per_name_tests.get("inferential_status", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
            )
            per_name_tests["inferential_reason"] = (
                per_name_tests.get("inferential_reason", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
            )
            for metadata_column, default_value in (
                ("family_id", self.FAMILY_ID_PER_NAME),
                ("adjustment_method", self.ADJUSTMENT_METHOD_BH),
                ("n_tests", 0),
                ("n_tests_in_family", 0),
                ("eligible_by_gate", False),
                ("gate_reason", self.GATE_REASON_FAMILY_A_NOT_TESTED),
                ("adjusted_p_value", np.nan),
            ):
                if metadata_column not in per_name_tests.columns:
                    per_name_tests[metadata_column] = default_value
            per_name_tests["family_id"] = self.FAMILY_ID_PER_NAME
            per_name_tests["adjustment_method"] = self.ADJUSTMENT_METHOD_BH
            per_name_tests["is_significant"] = pd.to_numeric(
                per_name_tests.get(
                    "is_significant",
                    pd.Series(pd.NA, index=per_name_tests.index, dtype="object"),
                ),
                errors="coerce",
            ).astype("object")
            for scope_value, scope_frame in per_name_tests.groupby("scope", dropna=False):
                scope_name = str(scope_value).strip()
                scope_index = scope_frame.index
                scope_passes_family_a = scope_name in family_a_significant_scopes
                scope_gate_reason = family_a_gate_reason_by_scope.get(
                    scope_name, self.GATE_REASON_FAMILY_A_NOT_TESTED
                )
                scope_inferential_supported = (
                    per_name_tests.loc[scope_index, "inferential_status"]
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    == "reference_model_inference"
                )
                scope_valid_p = per_name_tests.loc[scope_index, "p_value"].notna() & np.isfinite(
                    per_name_tests.loc[scope_index, "p_value"]
                )
                scope_eligible = (
                    pd.Series(scope_passes_family_a, index=scope_index, dtype=bool)
                    & scope_inferential_supported
                    & scope_valid_p
                )
                n_scope_tests = int(scope_eligible.sum())
                per_name_tests.loc[scope_index, "n_tests"] = n_scope_tests
                per_name_tests.loc[scope_index, "n_tests_in_family"] = n_scope_tests
                per_name_tests.loc[scope_index, "eligible_by_gate"] = scope_eligible.astype(bool)

                scope_gate_reason_series = pd.Series(
                    scope_gate_reason,
                    index=scope_index,
                    dtype="object",
                )
                scope_gate_reason_series.loc[scope_eligible] = self.GATE_REASON_ELIGIBLE
                scope_gate_reason_series.loc[
                    (~scope_eligible) & scope_passes_family_a & (~scope_inferential_supported)
                ] = self.GATE_REASON_SCOPE_NOT_INFERENTIAL
                per_name_tests.loc[scope_index, "gate_reason"] = scope_gate_reason_series

                if n_scope_tests > 0:
                    scope_adjusted = self._adjust_p_values(
                        per_name_tests.loc[scope_eligible, "p_value"],
                        method=self.ADJUSTMENT_METHOD_BH,
                    )
                    per_name_tests.loc[scope_eligible, "q_value"] = scope_adjusted
                    per_name_tests.loc[scope_eligible, "adjusted_p_value"] = scope_adjusted
                    per_name_tests.loc[scope_eligible, "is_significant"] = (
                        pd.to_numeric(scope_adjusted, errors="coerce")
                        .astype(float)
                        .le(float(self.bh_fdr_q))
                        .astype("object")
                    )
                    per_name_tests.loc[scope_eligible, "tested"] = True

                non_eligible = ~scope_eligible
                if bool(non_eligible.any()):
                    for inferential_column in ("p_value", "q_value", "adjusted_p_value"):
                        per_name_tests.loc[scope_index[non_eligible], inferential_column] = np.nan
                    per_name_tests.loc[scope_index[non_eligible], "is_significant"] = pd.NA
                    per_name_tests.loc[scope_index[non_eligible], "tested"] = False
                downgraded = non_eligible & (
                    per_name_tests.loc[scope_index, "inferential_status"]
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    == "reference_model_inference"
                )
                if bool(downgraded.any()):
                    per_name_tests.loc[scope_index[downgraded], "inferential_status"] = (
                        "descriptive_only"
                    )
                    if scope_passes_family_a:
                        per_name_tests.loc[scope_index[downgraded], "inferential_reason"] = (
                            self.INFERENTIAL_REASON_FAMILY_C_GATE_NOT_PASSED
                        )
                    else:
                        per_name_tests.loc[scope_index[downgraded], "inferential_reason"] = (
                            self.INFERENTIAL_REASON_FAMILY_A_GATE_NOT_PASSED
                        )

            per_name_tests = per_name_tests.sort_values(
                ["scope", "q_value", "p_value", "observed_count"],
                ascending=[True, True, True, False],
            )

        if not per_name_display.empty and not per_name_tests.empty:
            per_name_display["scope"] = (
                per_name_display.get("scope", pd.Series(dtype=str)).fillna("").astype(str)
            )
            per_name_display["canonical_name"] = (
                per_name_display.get("canonical_name", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
            )
            merge_columns = [
                "scope",
                "canonical_name",
                "p_value",
                "q_value",
                "is_significant",
                "tested",
                "inferential_status",
                "inferential_reason",
                "family_id",
                "adjustment_method",
                "n_tests",
                "n_tests_in_family",
                "eligible_by_gate",
                "gate_reason",
                "adjusted_p_value",
            ]
            display_merge = per_name_tests[merge_columns].drop_duplicates(
                subset=["scope", "canonical_name"], keep="first"
            )
            per_name_display = per_name_display.drop(
                columns=[column for column in merge_columns if column in per_name_display.columns and column not in {"scope", "canonical_name"}],
                errors="ignore",
            ).merge(
                display_merge,
                on=["scope", "canonical_name"],
                how="left",
            )

        if not per_name_display.empty:
            legacy_per_name_anomalies = per_name_display.rename(
                columns={"observed_count": "n"}
            ).copy()
            if "inferential_status" in legacy_per_name_anomalies.columns:
                legacy_per_name_anomalies["inference_status"] = legacy_per_name_anomalies[
                    "inferential_status"
                ]
            if "inferential_reason" in legacy_per_name_anomalies.columns:
                legacy_per_name_anomalies["inference_reason"] = legacy_per_name_anomalies[
                    "inferential_reason"
                ]
            legacy_top_repeated = legacy_per_name_anomalies[
                legacy_per_name_anomalies.get("n", pd.Series(dtype=float)).astype(float) >= 2
            ][["display_name", "canonical_name", "n", "n_pro", "n_con", "time_span_minutes"]].copy()
            legacy_top_repeated = legacy_top_repeated.sort_values(
                "n", ascending=False
            ).head(self.top_n)

        if not per_name_tests.empty:
            primary_scope_significant = int(
                pd.to_numeric(
                    per_name_tests.loc[
                        per_name_tests["scope"].astype(str) == self.collision_scope_primary,
                        "is_significant",
                    ],
                    errors="coerce",
                )
                .fillna(0.0)
                .astype(float)
                .sum()
            )

        if not collision_by_bucket.empty:
            collision_by_bucket["scope"] = (
                collision_by_bucket.get("scope", pd.Series(dtype=str)).fillna("").astype(str)
            )
            collision_by_bucket["metric"] = (
                collision_by_bucket.get("metric", pd.Series(dtype=str)).fillna("").astype(str)
            )
            collision_by_bucket["inferential_status"] = (
                collision_by_bucket.get("inferential_status", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
            )
            collision_by_bucket["inferential_reason"] = (
                collision_by_bucket.get("inferential_reason", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
            )
            collision_by_bucket["p_value"] = pd.to_numeric(
                collision_by_bucket.get("p_value", pd.Series(dtype=float)),
                errors="coerce",
            )
            for metadata_column, default_value in (
                ("family_id", self.FAMILY_ID_BUCKET),
                ("adjustment_method", self.ADJUSTMENT_METHOD_BH),
                ("n_tests", 0),
                ("n_tests_in_family", 0),
                ("eligible_by_gate", False),
                ("gate_reason", self.GATE_REASON_FAMILY_A_NOT_TESTED),
                ("adjusted_p_value", np.nan),
                ("is_significant", pd.NA),
            ):
                if metadata_column not in collision_by_bucket.columns:
                    collision_by_bucket[metadata_column] = default_value

            primary_bucket_metric = (
                collision_by_bucket["metric"].str.strip().str.lower()
                == self.PRIMARY_SCOPE_ENDPOINT_METRIC
            )
            secondary_bucket_metric = ~primary_bucket_metric
            collision_by_bucket.loc[primary_bucket_metric, "family_id"] = self.FAMILY_ID_BUCKET
            collision_by_bucket.loc[
                primary_bucket_metric, "adjustment_method"
            ] = self.ADJUSTMENT_METHOD_BH
            collision_by_bucket.loc[secondary_bucket_metric, "family_id"] = (
                f"{self.FAMILY_ID_BUCKET}_secondary"
            )
            collision_by_bucket.loc[
                secondary_bucket_metric, "adjustment_method"
            ] = self.ADJUSTMENT_METHOD_NONE
            collision_by_bucket.loc[secondary_bucket_metric, "n_tests"] = 0
            collision_by_bucket.loc[secondary_bucket_metric, "n_tests_in_family"] = 0
            collision_by_bucket.loc[secondary_bucket_metric, "eligible_by_gate"] = False
            collision_by_bucket.loc[
                secondary_bucket_metric, "gate_reason"
            ] = self.GATE_REASON_SECONDARY_BUCKET_METRIC
            collision_by_bucket.loc[secondary_bucket_metric, "adjusted_p_value"] = np.nan
            collision_by_bucket.loc[secondary_bucket_metric, "is_significant"] = pd.NA
            secondary_bucket_with_inference = (
                secondary_bucket_metric
                & (
                    collision_by_bucket["inferential_status"].str.strip().str.lower()
                    == "reference_model_inference"
                )
            )
            collision_by_bucket.loc[
                secondary_bucket_with_inference, "inferential_status"
            ] = "descriptive_only"
            collision_by_bucket.loc[
                secondary_bucket_with_inference, "inferential_reason"
            ] = self.INFERENTIAL_REASON_SECONDARY_BUCKET_METRIC
            for inferential_column in (
                "expected_p05",
                "expected_p95",
                "z_score",
                "p_value",
                "monte_carlo_quantile_resolution",
                "monte_carlo_p_value_mcse",
                "monte_carlo_p_value_ci_low",
                "monte_carlo_p_value_ci_high",
            ):
                if inferential_column in collision_by_bucket.columns:
                    collision_by_bucket.loc[secondary_bucket_metric, inferential_column] = np.nan

            for scope_value, scope_frame in collision_by_bucket.groupby("scope", dropna=False):
                scope_name = str(scope_value).strip()
                scope_index = scope_frame.index
                scope_gate_passes = scope_name in family_a_significant_scopes
                scope_gate_reason = family_a_gate_reason_by_scope.get(
                    scope_name, self.GATE_REASON_FAMILY_A_NOT_TESTED
                )
                scope_primary_rows = (
                    pd.Series(False, index=scope_index)
                    | primary_bucket_metric.loc[scope_index]
                )
                scope_inferential_supported = (
                    collision_by_bucket.loc[scope_index, "inferential_status"]
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    == "reference_model_inference"
                )
                scope_valid_p = collision_by_bucket.loc[scope_index, "p_value"].notna() & np.isfinite(
                    collision_by_bucket.loc[scope_index, "p_value"]
                )
                scope_eligible = (
                    scope_primary_rows
                    & pd.Series(scope_gate_passes, index=scope_index, dtype=bool)
                    & scope_inferential_supported
                    & scope_valid_p
                )
                n_scope_tests = int(scope_eligible.sum())
                collision_by_bucket.loc[scope_index[scope_primary_rows], "n_tests"] = n_scope_tests
                collision_by_bucket.loc[
                    scope_index[scope_primary_rows], "n_tests_in_family"
                ] = n_scope_tests
                collision_by_bucket.loc[
                    scope_index[scope_primary_rows], "eligible_by_gate"
                ] = scope_eligible.loc[scope_primary_rows].astype(bool)

                scope_gate_reason_series = pd.Series(
                    scope_gate_reason,
                    index=scope_index,
                    dtype="object",
                )
                scope_gate_reason_series.loc[scope_eligible] = self.GATE_REASON_ELIGIBLE
                scope_gate_reason_series.loc[
                    (~scope_eligible)
                    & scope_primary_rows
                    & scope_gate_passes
                    & (~scope_inferential_supported)
                ] = self.GATE_REASON_SCOPE_NOT_INFERENTIAL
                collision_by_bucket.loc[
                    scope_index[scope_primary_rows], "gate_reason"
                ] = scope_gate_reason_series.loc[scope_primary_rows]

                if n_scope_tests > 0:
                    scope_adjusted = self._adjust_p_values(
                        collision_by_bucket.loc[scope_eligible, "p_value"],
                        method=self.ADJUSTMENT_METHOD_BH,
                    )
                    collision_by_bucket.loc[scope_eligible, "adjusted_p_value"] = scope_adjusted
                    collision_by_bucket.loc[scope_eligible, "is_significant"] = (
                        pd.to_numeric(scope_adjusted, errors="coerce")
                        .astype(float)
                        .le(float(self.bh_fdr_q))
                        .astype("object")
                    )

                non_eligible_primary = scope_primary_rows & (~scope_eligible)
                if bool(non_eligible_primary.any()):
                    for inferential_column in (
                        "expected_p05",
                        "expected_p95",
                        "z_score",
                        "p_value",
                        "monte_carlo_quantile_resolution",
                        "monte_carlo_p_value_mcse",
                        "monte_carlo_p_value_ci_low",
                        "monte_carlo_p_value_ci_high",
                        "adjusted_p_value",
                    ):
                        if inferential_column in collision_by_bucket.columns:
                            collision_by_bucket.loc[
                                scope_index[non_eligible_primary], inferential_column
                            ] = np.nan
                    collision_by_bucket.loc[
                        scope_index[non_eligible_primary], "is_significant"
                    ] = pd.NA
                downgraded = non_eligible_primary & (
                    collision_by_bucket.loc[scope_index, "inferential_status"]
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    == "reference_model_inference"
                )
                if bool(downgraded.any()):
                    collision_by_bucket.loc[
                        scope_index[downgraded], "inferential_status"
                    ] = "descriptive_only"
                    if scope_gate_passes:
                        collision_by_bucket.loc[
                            scope_index[downgraded], "inferential_reason"
                        ] = self.INFERENTIAL_REASON_FAMILY_C_GATE_NOT_PASSED
                    else:
                        collision_by_bucket.loc[
                            scope_index[downgraded], "inferential_reason"
                        ] = self.INFERENTIAL_REASON_FAMILY_A_GATE_NOT_PASSED

            if "inference_status" in collision_by_bucket.columns:
                collision_by_bucket["inference_status"] = np.where(
                    collision_by_bucket["inferential_status"].astype(str)
                    .str.strip()
                    .str.lower()
                    == "reference_model_inference",
                    "tested",
                    collision_by_bucket["inferential_status"],
                )

        if not temporal_burst.empty:
            temporal_burst["scope"] = (
                temporal_burst.get("scope", pd.Series(dtype=str)).fillna("").astype(str)
            )
            temporal_burst["canonical_name"] = (
                temporal_burst.get("canonical_name", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
            )
            temporal_burst["inferential_status"] = (
                temporal_burst.get("inferential_status", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
            )
            temporal_burst["inferential_reason"] = (
                temporal_burst.get("inferential_reason", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
            )
            for temporal_p_column in (
                "temporal_p_value_min_gap",
                "temporal_p_value_within_5m",
                "temporal_p_value_within_15m",
            ):
                temporal_burst[temporal_p_column] = pd.to_numeric(
                    temporal_burst.get(temporal_p_column, pd.Series(dtype=float)),
                    errors="coerce",
                )
            for metadata_column, default_value in (
                ("family_id", self.FAMILY_ID_TEMPORAL),
                ("adjustment_method", self.ADJUSTMENT_METHOD_BH),
                ("n_tests", 0),
                ("n_tests_in_family", 0),
                ("eligible_by_gate", False),
                ("gate_reason", self.GATE_REASON_FAMILY_A_NOT_TESTED),
                ("temporal_null_model", self.temporal_null_mode),
                ("temporal_null_supported", False),
                (
                    "temporal_null_support_reason",
                    self.TEMPORAL_NULL_SUPPORT_REASON_NAME_NOT_GATED,
                ),
                ("temporal_inferential_name_gate_passed", False),
                ("temporal_q_value_min_gap", np.nan),
                ("temporal_q_value_within_5m", np.nan),
                ("temporal_q_value_within_15m", np.nan),
                ("temporal_is_significant_min_gap", pd.NA),
                ("temporal_is_significant_within_5m", pd.NA),
                ("temporal_is_significant_within_15m", pd.NA),
            ):
                if metadata_column not in temporal_burst.columns:
                    temporal_burst[metadata_column] = default_value
            temporal_burst["family_id"] = self.FAMILY_ID_TEMPORAL
            temporal_burst["adjustment_method"] = self.ADJUSTMENT_METHOD_BH
            temporal_burst["temporal_null_model"] = (
                temporal_burst.get("temporal_null_model", pd.Series(dtype=str))
                .fillna(self.temporal_null_mode)
                .astype(str)
            )
            temporal_burst["temporal_null_supported"] = (
                temporal_burst.get("temporal_null_supported", pd.Series(dtype=bool))
                .fillna(False)
                .astype(bool)
            )
            temporal_burst["temporal_null_support_reason"] = (
                temporal_burst.get("temporal_null_support_reason", pd.Series(dtype=str))
                .fillna(self.TEMPORAL_NULL_SUPPORT_REASON_NAME_NOT_GATED)
                .astype(str)
            )
            temporal_burst["temporal_inferential_name_gate_passed"] = (
                temporal_burst.get(
                    "temporal_inferential_name_gate_passed", pd.Series(dtype=bool)
                )
                .fillna(False)
                .astype(bool)
            )

            significant_name_lookup: dict[str, set[str]] = {}
            if not per_name_tests.empty:
                significant_rows = per_name_tests[
                    pd.to_numeric(
                        per_name_tests.get("is_significant", pd.Series(dtype=float)),
                        errors="coerce",
                    )
                    .fillna(0.0)
                    .astype(float)
                    .gt(0.0)
                ].copy()
                if not significant_rows.empty:
                    significant_rows["scope"] = (
                        significant_rows.get("scope", pd.Series(dtype=str))
                        .fillna("")
                        .astype(str)
                    )
                    significant_rows["canonical_name"] = (
                        significant_rows.get("canonical_name", pd.Series(dtype=str))
                        .fillna("")
                        .astype(str)
                    )
                    for scope_value, scope_frame in significant_rows.groupby("scope", dropna=False):
                        scope_name = str(scope_value).strip()
                        significant_name_lookup[scope_name] = {
                            str(value).strip()
                            for value in scope_frame["canonical_name"].tolist()
                            if str(value).strip()
                        }

            for scope_value, scope_frame in temporal_burst.groupby("scope", dropna=False):
                scope_name = str(scope_value).strip()
                scope_index = scope_frame.index
                scope_gate_passes = scope_name in family_a_significant_scopes
                scope_gate_reason = family_a_gate_reason_by_scope.get(
                    scope_name, self.GATE_REASON_FAMILY_A_NOT_TESTED
                )
                eligible_names = significant_name_lookup.get(scope_name, set())
                scope_name_eligible = temporal_burst.loc[scope_index, "canonical_name"].map(
                    lambda value: str(value).strip() in eligible_names
                )
                scope_inferential_supported = (
                    temporal_burst.loc[scope_index, "inferential_status"]
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    == "reference_model_inference"
                )
                scope_null_supported = temporal_burst.loc[
                    scope_index, "temporal_null_supported"
                ].fillna(False).astype(bool)
                scope_eligible = (
                    pd.Series(scope_gate_passes, index=scope_index, dtype=bool)
                    & scope_name_eligible
                    & scope_inferential_supported
                    & scope_null_supported
                )
                n_scope_tests = int(scope_eligible.sum())
                temporal_burst.loc[scope_index, "n_tests"] = n_scope_tests
                temporal_burst.loc[scope_index, "n_tests_in_family"] = n_scope_tests
                temporal_burst.loc[scope_index, "eligible_by_gate"] = scope_eligible.astype(bool)

                scope_gate_reason_series = pd.Series(
                    scope_gate_reason,
                    index=scope_index,
                    dtype="object",
                )
                scope_gate_reason_series.loc[scope_eligible] = self.GATE_REASON_ELIGIBLE
                scope_gate_reason_series.loc[
                    (~scope_eligible) & scope_gate_passes & (~scope_name_eligible)
                ] = self.GATE_REASON_NAME_NOT_FAMILY_C_DISCOVERY
                scope_gate_reason_series.loc[
                    (~scope_eligible) & scope_gate_passes & (~scope_inferential_supported)
                ] = self.GATE_REASON_SCOPE_NOT_INFERENTIAL
                scope_gate_reason_series.loc[
                    (~scope_eligible)
                    & scope_gate_passes
                    & scope_name_eligible
                    & scope_inferential_supported
                    & (~scope_null_supported)
                ] = self.GATE_REASON_TEMPORAL_NULL_UNSUPPORTED
                temporal_burst.loc[scope_index, "gate_reason"] = scope_gate_reason_series

                for p_column, q_column, significant_column in (
                    (
                        "temporal_p_value_min_gap",
                        "temporal_q_value_min_gap",
                        "temporal_is_significant_min_gap",
                    ),
                    (
                        "temporal_p_value_within_5m",
                        "temporal_q_value_within_5m",
                        "temporal_is_significant_within_5m",
                    ),
                    (
                        "temporal_p_value_within_15m",
                        "temporal_q_value_within_15m",
                        "temporal_is_significant_within_15m",
                    ),
                ):
                    p_values = pd.to_numeric(
                        temporal_burst.loc[scope_index, p_column],
                        errors="coerce",
                    )
                    valid_mask = scope_eligible & p_values.notna() & np.isfinite(p_values)
                    if bool(valid_mask.any()):
                        adjusted = self._adjust_p_values(
                            temporal_burst.loc[scope_index[valid_mask], p_column],
                            method=self.ADJUSTMENT_METHOD_BH,
                        )
                        temporal_burst.loc[scope_index[valid_mask], q_column] = adjusted
                        temporal_burst.loc[scope_index[valid_mask], significant_column] = (
                            pd.to_numeric(adjusted, errors="coerce")
                            .astype(float)
                            .le(float(self.bh_fdr_q))
                            .astype("object")
                        )
                    temporal_burst.loc[scope_index[~scope_eligible], p_column] = np.nan
                    temporal_burst.loc[scope_index[~scope_eligible], q_column] = np.nan
                    temporal_burst.loc[scope_index[~scope_eligible], significant_column] = pd.NA

                downgraded = (~scope_eligible) & (
                    temporal_burst.loc[scope_index, "inferential_status"]
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    == "reference_model_inference"
                )
                if bool(downgraded.any()):
                    temporal_burst.loc[scope_index[downgraded], "inferential_status"] = (
                        "descriptive_only"
                    )
                    downgraded_due_null = downgraded & scope_name_eligible & scope_inferential_supported & (
                        ~scope_null_supported
                    )
                    if bool(downgraded_due_null.any()):
                        temporal_burst.loc[
                            scope_index[downgraded_due_null], "inferential_reason"
                        ] = self.INFERENTIAL_REASON_TEMPORAL_NULL_UNSUPPORTED
                    remaining_downgraded = downgraded & (~downgraded_due_null)
                    if bool(remaining_downgraded.any()) and scope_gate_passes:
                        temporal_burst.loc[
                            scope_index[remaining_downgraded], "inferential_reason"
                        ] = self.INFERENTIAL_REASON_FAMILY_C_GATE_NOT_PASSED
                    elif bool(remaining_downgraded.any()):
                        temporal_burst.loc[
                            scope_index[remaining_downgraded], "inferential_reason"
                        ] = (
                            self.INFERENTIAL_REASON_FAMILY_A_GATE_NOT_PASSED
                        )

        if not legacy_position_tests.empty:
            legacy_position_tests["family_id"] = self.FAMILY_ID_POSITION
            legacy_position_tests["adjustment_method"] = self.ADJUSTMENT_METHOD_HOLM
            position_gate_passes = self.collision_scope_primary in family_a_significant_scopes
            position_gate_reason = (
                self.GATE_REASON_ELIGIBLE
                if position_gate_passes
                else family_a_gate_reason_by_scope.get(
                    self.collision_scope_primary, self.GATE_REASON_FAMILY_A_NOT_TESTED
                )
            )
            legacy_position_tests["n_tests"] = int(
                len(legacy_position_tests) if position_gate_passes else 0
            )
            legacy_position_tests["n_tests_in_family"] = int(
                len(legacy_position_tests) if position_gate_passes else 0
            )
            legacy_position_tests["eligible_by_gate"] = bool(position_gate_passes)
            legacy_position_tests["gate_reason"] = position_gate_reason
            legacy_position_tests["adjusted_p_value"] = np.nan
            legacy_position_tests["is_significant"] = False
            if "permutation_p_value_two_sided" in legacy_position_tests.columns:
                if position_gate_passes:
                    position_adjusted = self._adjust_p_values(
                        legacy_position_tests["permutation_p_value_two_sided"],
                        method=self.ADJUSTMENT_METHOD_HOLM,
                    )
                    legacy_position_tests["adjusted_p_value"] = pd.to_numeric(
                        position_adjusted, errors="coerce"
                    )
                    legacy_position_tests["is_significant"] = (
                        pd.to_numeric(
                            legacy_position_tests["adjusted_p_value"], errors="coerce"
                        )
                        .le(float(self.bh_fdr_q))
                        .fillna(False)
                        .astype(bool)
                    )
                else:
                    legacy_position_tests["permutation_p_value_two_sided"] = np.nan
                    legacy_position_tests["adjusted_p_value"] = np.nan
                    legacy_position_tests["is_significant"] = False

        for scope_name in scope_names:
            scope_key = str(scope_name).strip()
            if not scope_key:
                continue
            family_a_significant = scope_key in family_a_significant_scopes
            family_a_gate = family_a_gate_reason_by_scope.get(
                scope_key, self.GATE_REASON_FAMILY_A_NOT_TESTED
            )
            family_a_tests = 1 if scope_key in family_a_test_scopes else 0
            family_a_adjusted = family_a_adjusted_by_scope.get(scope_key, np.nan)
            hypothesis_family_rows.append(
                {
                    "scope": scope_key,
                    "family_id": self.FAMILY_ID_SCOPE,
                    "family_label": "Scope-level excess_rows",
                    "family_order": 1,
                    "adjustment_method": self.ADJUSTMENT_METHOD_HOLM,
                    "n_tests": int(family_a_tests),
                    "n_significant": int(1 if family_a_significant else 0),
                    "eligible_by_gate": bool(scope_key in family_a_test_scopes),
                    "gate_reason": family_a_gate,
                    "adjusted_p_value": float(family_a_adjusted)
                    if pd.notna(family_a_adjusted)
                    else np.nan,
                }
            )
            if not collision_by_bucket.empty:
                bucket_scope = collision_by_bucket[
                    (collision_by_bucket["scope"].astype(str) == scope_key)
                    & (collision_by_bucket["family_id"].astype(str) == self.FAMILY_ID_BUCKET)
                ].copy()
                hypothesis_family_rows.append(
                    {
                        "scope": scope_key,
                        "family_id": self.FAMILY_ID_BUCKET,
                        "family_label": "Bucket follow-up",
                        "family_order": 2,
                        "adjustment_method": self.ADJUSTMENT_METHOD_BH,
                        "n_tests": int(bucket_scope["eligible_by_gate"].fillna(False).astype(bool).sum()),
                        "n_significant": int(
                            pd.to_numeric(bucket_scope.get("is_significant", pd.Series(dtype=float)), errors="coerce")
                            .fillna(0.0)
                            .astype(float)
                            .sum()
                        ),
                        "eligible_by_gate": bool(scope_key in family_a_significant_scopes),
                        "gate_reason": family_a_gate
                        if scope_key not in family_a_significant_scopes
                        else self.GATE_REASON_ELIGIBLE,
                    }
                )
            if not per_name_tests.empty:
                per_name_scope = per_name_tests[
                    (per_name_tests["scope"].astype(str) == scope_key)
                    & (per_name_tests["family_id"].astype(str) == self.FAMILY_ID_PER_NAME)
                ].copy()
                hypothesis_family_rows.append(
                    {
                        "scope": scope_key,
                        "family_id": self.FAMILY_ID_PER_NAME,
                        "family_label": "Per-name follow-up",
                        "family_order": 3,
                        "adjustment_method": self.ADJUSTMENT_METHOD_BH,
                        "n_tests": int(per_name_scope["eligible_by_gate"].fillna(False).astype(bool).sum()),
                        "n_significant": int(
                            pd.to_numeric(per_name_scope.get("is_significant", pd.Series(dtype=float)), errors="coerce")
                            .fillna(0.0)
                            .astype(float)
                            .sum()
                        ),
                        "eligible_by_gate": bool(scope_key in family_a_significant_scopes),
                        "gate_reason": family_a_gate
                        if scope_key not in family_a_significant_scopes
                        else self.GATE_REASON_ELIGIBLE,
                    }
                )
            if not temporal_burst.empty:
                temporal_scope = temporal_burst[
                    (temporal_burst["scope"].astype(str) == scope_key)
                    & (temporal_burst["family_id"].astype(str) == self.FAMILY_ID_TEMPORAL)
                ].copy()
                temporal_tests = int(
                    temporal_scope["eligible_by_gate"].fillna(False).astype(bool).sum()
                )
                temporal_significant = int(
                    pd.to_numeric(
                        temporal_scope.get(
                            "temporal_is_significant_within_5m",
                            pd.Series(dtype=float),
                        ),
                        errors="coerce",
                    )
                    .fillna(0.0)
                    .astype(float)
                    .sum()
                )
                hypothesis_family_rows.append(
                    {
                        "scope": scope_key,
                        "family_id": self.FAMILY_ID_TEMPORAL,
                        "family_label": "Within-name temporal follow-up",
                        "family_order": 4,
                        "adjustment_method": self.ADJUSTMENT_METHOD_BH,
                        "n_tests": temporal_tests,
                        "n_significant": temporal_significant,
                        "eligible_by_gate": bool(scope_key in family_a_significant_scopes),
                        "gate_reason": family_a_gate
                        if scope_key not in family_a_significant_scopes
                        else self.GATE_REASON_ELIGIBLE,
                    }
                )

        if not legacy_position_tests.empty:
            hypothesis_family_rows.append(
                {
                    "scope": self.collision_scope_primary,
                    "family_id": self.FAMILY_ID_POSITION,
                    "family_label": "Position follow-up",
                    "family_order": 5,
                    "adjustment_method": self.ADJUSTMENT_METHOD_HOLM,
                    "n_tests": int(
                        pd.to_numeric(
                            legacy_position_tests.get("n_tests", pd.Series(dtype=float)),
                            errors="coerce",
                        )
                        .fillna(0.0)
                        .astype(float)
                        .sum()
                    ),
                    "n_significant": int(
                        legacy_position_tests.get("is_significant", pd.Series(dtype=bool))
                        .fillna(False)
                        .astype(bool)
                        .astype(int)
                        .sum()
                    ),
                    "eligible_by_gate": bool(
                        self.collision_scope_primary in family_a_significant_scopes
                    ),
                    "gate_reason": (
                        self.GATE_REASON_ELIGIBLE
                        if self.collision_scope_primary in family_a_significant_scopes
                        else family_a_gate_reason_by_scope.get(
                            self.collision_scope_primary,
                            self.GATE_REASON_FAMILY_A_NOT_TESTED,
                        )
                    ),
                }
            )

        hypothesis_families = (
            pd.DataFrame(hypothesis_family_rows).sort_values(
                ["scope", "family_order", "family_id"],
                ascending=[True, True, True],
            )
            if hypothesis_family_rows
            else pd.DataFrame()
        )

        primary_scope_overview = collision_overview[
            collision_overview["scope"] == self.collision_scope_primary
        ].copy()
        legacy_overview = self._legacy_duplicate_metrics_overview(
            scope_overview=primary_scope_overview,
            n_rows=primary_scope_n_used,
        )
        hypothesis_family_records: list[dict[str, object]] = []
        hypothesis_family_totals: list[dict[str, object]] = []
        if not hypothesis_families.empty:
            hypothesis_family_records = [
                {
                    "scope": str(getattr(row, "scope", "") or ""),
                    "family_id": str(getattr(row, "family_id", "") or ""),
                    "family_label": str(getattr(row, "family_label", "") or ""),
                    "family_order": int(getattr(row, "family_order", 0) or 0),
                    "adjustment_method": str(getattr(row, "adjustment_method", "") or ""),
                    "n_tests": int(getattr(row, "n_tests", 0) or 0),
                    "n_significant": int(getattr(row, "n_significant", 0) or 0),
                    "eligible_by_gate": bool(getattr(row, "eligible_by_gate", False)),
                    "gate_reason": str(getattr(row, "gate_reason", "") or ""),
                    "adjusted_p_value": (
                        float(getattr(row, "adjusted_p_value"))
                        if pd.notna(getattr(row, "adjusted_p_value", np.nan))
                        else None
                    ),
                }
                for row in hypothesis_families.itertuples(index=False)
            ]
            totals_frame = (
                hypothesis_families.groupby(
                    ["family_id", "family_label", "family_order", "adjustment_method"],
                    dropna=False,
                )
                .agg(
                    n_tests=("n_tests", "sum"),
                    n_significant=("n_significant", "sum"),
                    n_scopes=("scope", "nunique"),
                )
                .reset_index()
                .sort_values(["family_order", "family_id"])
            )
            hypothesis_family_totals = [
                {
                    "family_id": str(getattr(row, "family_id", "") or ""),
                    "family_label": str(getattr(row, "family_label", "") or ""),
                    "family_order": int(getattr(row, "family_order", 0) or 0),
                    "adjustment_method": str(getattr(row, "adjustment_method", "") or ""),
                    "n_tests": int(getattr(row, "n_tests", 0) or 0),
                    "n_significant": int(getattr(row, "n_significant", 0) or 0),
                    "n_scopes": int(getattr(row, "n_scopes", 0) or 0),
                }
                for row in totals_frame.itertuples(index=False)
            ]

        summary = {
            "name_key": self.collision_key_mode,
            "baseline_source": primary_scope_baseline_source,
            "baseline_label": self._baseline_label_for_source(primary_scope_baseline_source),
            "baseline_model": self.collision_baseline_model,
            "n_records": int(primary_scope_row_count),
            "n_unique_names": int(primary_scope_unique_count),
            "duplicate_rows": int(primary_scope_repeated),
            "duplicate_row_rate": (
                float(primary_scope_repeated / primary_scope_row_count) if primary_scope_row_count > 0 else 0.0
            ),
            "duplicate_pairs": float(primary_scope_pairs),
            "n_significant_per_name": int(primary_scope_significant),
            "bh_fdr_q": float(self.bh_fdr_q),
            "primary_low_power": bool(primary_scope_low_power),
            "collision_scope_primary": self.collision_scope_primary,
            "scope_status": primary_scope_status,
            "scope_reason": primary_scope_reason,
            "n_used": int(primary_scope_n_used),
            "N_used": int(primary_scope_n_population),
            "baseline_degraded": bool(primary_scope_degraded),
            "normalization_version": normalization_version_value,
            "normalization_version_hash": normalization_hash,
            "stratification": primary_scope_stratification,
            "position_hearing_baseline_enabled": bool(self.position_hearing_baseline_enabled),
            "position_baseline_shrink_k": float(self.position_baseline_shrink_k),
            "position_interval_nominal": float(self.position_interval_nominal),
            "position_interval_method_id": self.POSITION_INTERVAL_METHOD_ID,
            "position_claim_eligible": bool(position_claim_eligible),
            "position_claim_reason": str(position_claim_reason),
            "claim_class": self.COLLISION_CLAIM_CLASS,
            "inferential_status": primary_scope_inferential_status,
            "inferential_reason": primary_scope_inferential_reason,
            "estimand_primary": self.STATISTICAL_CONTRACT_ESTIMAND_PRIMARY,
            "non_goals": self.STATISTICAL_CONTRACT_NON_GOALS,
            "baseline_semantics": self.STATISTICAL_CONTRACT_BASELINE_SEMANTICS,
            "hypothesis_families": hypothesis_family_records,
            "hypothesis_family_totals": hypothesis_family_totals,
            "n_hypothesis_tests_total": int(
                sum(int(row.get("n_tests", 0) or 0) for row in hypothesis_family_totals)
            ),
            "statistical_contract": {
                "estimand_primary": self.STATISTICAL_CONTRACT_ESTIMAND_PRIMARY,
                "non_goals": self.STATISTICAL_CONTRACT_NON_GOALS,
                "baseline_semantics": self.STATISTICAL_CONTRACT_BASELINE_SEMANTICS,
                "inferential_status": primary_scope_inferential_status,
                "inferential_reason": primary_scope_inferential_reason,
                "hypothesis_families": hypothesis_family_records,
                "hypothesis_family_totals": hypothesis_family_totals,
            },
            "scope_availability": [
                {
                    "scope": scope_name,
                    "scope_status": str(
                        scope_availability.get(scope_name, {}).get(
                            "scope_status", self.SCOPE_STATUS_UNAVAILABLE
                        )
                    ),
                    "scope_reason": str(
                        scope_availability.get(scope_name, {}).get(
                            "scope_reason",
                            self.SCOPE_REASON_UNAVAILABLE_NO_ROWS_AFTER_FILTERING,
                        )
                    ),
                }
                for scope_name in scope_names
            ],
            "rng_root_seed": int(rng_lineage_columns.get("rng_root_seed", 0) or 0),
            "rng_root_stream_id": str(rng_lineage_columns.get("rng_root_stream_id", "") or ""),
            "rng_seed_lineage": rng_seed_lineage,
        }
        record_runtime_timing(
            "detector.duplicates_exact.assemble_outputs",
            (perf_counter() - assemble_started) * 1000.0,
        )

        return DetectorResult(
            detector=self.name,
            summary=summary,
            tables={
                "collision_methods": collision_methods,
                "collision_overview": collision_overview,
                "collision_by_bucket": collision_by_bucket,
                "collision_by_bucket_position": collision_by_bucket_position,
                "collision_stratification_sensitivity": collision_stratification_sensitivity,
                "per_name_tests": per_name_tests,
                "per_name_display": per_name_display,
                "per_name_duplicates_by_mode": per_name_duplicates_by_mode,
                "per_name_submission_timing_by_mode": per_name_submission_timing_by_mode,
                "temporal_burst_signals": temporal_burst,
                "top_name_timing_by_mode": top_name_timing_by_mode,
                "hypothesis_families": hypothesis_families,
                # Legacy compatibility tables retained while render contracts migrate.
                "duplicate_metrics_overview": legacy_overview,
                "duplicate_by_bucket": legacy_duplicate_by_bucket,
                "position_duplicate_metrics": legacy_position_metrics,
                "position_concentration_tests": legacy_position_tests,
                "per_name_anomalies": legacy_per_name_anomalies,
                "null_distribution": legacy_null_distribution,
                "swing_impact_scenarios": legacy_swing_impact,
                "top_repeated_names": legacy_top_repeated,
                "repeated_same_bucket": legacy_repeated_same_bucket,
                "repeated_same_bucket_summary": legacy_repeated_same_bucket_summary,
                "repeated_same_minute": legacy_repeated_same_minute,
                "position_switching_names": legacy_switch_names,
            },
        )
