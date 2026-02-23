from __future__ import annotations

import math
from hashlib import sha1
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

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
from testifier_audit.names.stat_tests import (
    benjamini_hochberg,
    binomial_tail_p_value,
    bootstrap_rate_difference,
    hypergeometric_tail_p_value,
)

CollisionBaselineModel = Literal["multinomial", "hypergeometric"]
CollisionBaselineSource = Literal["vrdb_full_histogram", "vrdb_full_keys", "hearing_empirical"]
CollisionScope = Literal["matched_only", "full_hearing", "unmatched_only"]

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
        "match_mode": "exact",
        "key_column": "collision_key_medium",
        "match_label": "Exact (last + first)",
        "match_definition": "Exact match on last-name and first-name tokens.",
    },
    {
        "match_mode": "medium",
        "key_column": "canonical_key_nickname",
        "match_label": "Medium (last + nickname root)",
        "match_definition": "Matches last-name with nickname-root first-name normalization.",
    },
    {
        "match_mode": "loose",
        "key_column": "collision_key_loose",
        "match_label": "Loose (last + first initial)",
        "match_definition": "Matches last-name with first-name initial only.",
    },
)


def _safe_str_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str)


class DuplicatesExactDetector(Detector):
    name = "duplicates_exact"
    DEFAULT_BUCKET_MINUTES = [1, 5, 15, 30, 60, 120, 240]

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
        collision_scope_primary: str = "matched_only",
        collision_scope_overlays: list[str] | None = None,
        collision_baseline_failure_policy: str = "degrade",
        collision_stratification: str = "none",
        per_name_significance_model: str = "binomial_tail",
        per_name_display_limit: int = 1000,
        exclude_non_person_from_inference: bool = True,
        monte_carlo_draws: int = 20_000,
        position_permutation_draws: int = 10_000,
        temporal_permutation_draws: int = 5_000,
        bh_fdr_q: float = 0.10,
        low_power_min_unique_names: int = 25,
        low_power_min_expected_duplicates: float = 5.0,
        max_per_name_rows: int = 1000,
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
        primary_scope = str(collision_scope_primary or "matched_only").strip().lower()
        self.collision_scope_primary: CollisionScope = (
            primary_scope if primary_scope in {"matched_only", "full_hearing", "unmatched_only"} else "matched_only"
        )
        overlays = [str(value or "").strip().lower() for value in (collision_scope_overlays or ["full_hearing"])]
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
        self.position_permutation_draws = max(100, int(position_permutation_draws))
        self.temporal_permutation_draws = max(100, int(temporal_permutation_draws))
        self.bh_fdr_q = float(min(max(bh_fdr_q, 0.0), 1.0))
        self.low_power_min_unique_names = max(1, int(low_power_min_unique_names))
        self.low_power_min_expected_duplicates = float(max(low_power_min_expected_duplicates, 0.0))
        self.max_per_name_rows = max(10, int(max_per_name_rows))
        self.voter_db_url = voter_db_url
        self.voter_table_name = voter_table_name
        self.voter_active_only = bool(voter_active_only)
        self.random_seed = int(random_seed)

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
        requested = int(min(max(int(self.monte_carlo_draws), 0), max(int(hard_cap), 0)))
        n = int(max(int(n_rows), 0))
        if requested <= 0 or n <= 1:
            return 0
        if n <= 3:
            return int(min(requested, 64))
        scale = min(1.0, max(0.20, math.sqrt(float(n) / 400.0)))
        budget = int(round(float(requested) * scale))
        return int(min(requested, max(48, budget)))

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

    @staticmethod
    def _normalization_version_hash(nickname_map_path: str | None = None) -> str:
        hasher = sha1()
        canonicalize_path = Path(__file__).resolve().parents[1] / "names" / "canonicalize.py"
        if canonicalize_path.exists():
            hasher.update(canonicalize_path.read_bytes())
        if nickname_map_path:
            nickname_path = Path(str(nickname_map_path))
            if nickname_path.exists():
                hasher.update(nickname_path.read_bytes())
        return hasher.hexdigest()

    def _scope_frames(self, infer: pd.DataFrame, features: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        scopes: dict[str, pd.DataFrame] = {"full_hearing": infer.copy()}
        requested = set(self._scope_list())
        if "matched_only" not in requested and "unmatched_only" not in requested:
            return scopes

        assignments = features.get("voter_registry_match.match_assignments", pd.DataFrame())
        if assignments is None or not isinstance(assignments, pd.DataFrame) or assignments.empty:
            scopes["matched_only"] = infer.copy()
            scopes["unmatched_only"] = pd.DataFrame(columns=infer.columns)
            return scopes

        assignments = assignments.copy()
        assignments["canonical_name"] = _safe_str_series(
            assignments.get("canonical_name", pd.Series(dtype=str))
        )
        outcome_column = "primary_outcome_selected"
        if outcome_column not in assignments.columns:
            outcome_column = "primary_outcome" if "primary_outcome" in assignments.columns else ""
        if not outcome_column:
            scopes["matched_only"] = infer.copy()
            scopes["unmatched_only"] = pd.DataFrame(columns=infer.columns)
            return scopes
        assignments[outcome_column] = _safe_str_series(assignments[outcome_column]).replace("", "unmatched")
        matched_names = set(
            assignments.loc[assignments[outcome_column].isin(_MATCHED_OUTCOMES), "canonical_name"].tolist()
        )
        unmatched_names = set(
            assignments.loc[assignments[outcome_column] == "unmatched", "canonical_name"].tolist()
        )
        infer_names = _safe_str_series(infer.get("canonical_name", pd.Series(dtype=str)))
        scopes["matched_only"] = infer.loc[infer_names.isin(matched_names)].copy()
        scopes["unmatched_only"] = infer.loc[infer_names.isin(unmatched_names)].copy()
        return scopes

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
    ) -> pd.DataFrame:
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
        limited_draws = int(min(max_draws, n_draws))
        if limited_draws <= 0:
            return pd.DataFrame(columns=list(COLLISION_METRICS))

        pairs = np.zeros(limited_draws, dtype=float)
        excess_rows = np.zeros(limited_draws, dtype=float)
        repeated_rows = np.zeros(limited_draws, dtype=float)

        for draw_idx in range(limited_draws):
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

        return pd.DataFrame(
            {
                "pairs": pairs,
                "excess_rows": excess_rows,
                "repeated_group_rows": repeated_rows,
            },
            columns=list(COLLISION_METRICS),
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
    ) -> pd.DataFrame:
        if working.empty:
            return pd.DataFrame()
        positions = _safe_str_series(working["position_normalized"]).to_numpy(dtype=object)
        if not {"Pro", "Con"}.issubset(set(np.unique(positions))):
            return pd.DataFrame()

        key_ids, _ = pd.factorize(_safe_str_series(working[key_column]), sort=False)
        key_ids = key_ids.astype(np.int64, copy=False)
        if key_ids.size == 0:
            return pd.DataFrame()
        n_keys = int(max(int(key_ids.max()), -1) + 1)
        if n_keys <= 0:
            return pd.DataFrame()

        pro_mask_observed = positions == "Pro"
        con_mask_observed = positions == "Con"
        pro_dup_rows, pro_total = self._duplicate_rows_for_factorized_subset(
            key_ids=key_ids,
            subset_mask=pro_mask_observed,
            n_keys=n_keys,
        )
        con_dup_rows, con_total = self._duplicate_rows_for_factorized_subset(
            key_ids=key_ids,
            subset_mask=con_mask_observed,
            n_keys=n_keys,
        )
        pro_rate = (pro_dup_rows / pro_total) if pro_total else 0.0
        con_rate = (con_dup_rows / con_total) if con_total else 0.0
        observed_diff = pro_rate - con_rate
        observed_rr = (pro_rate / con_rate) if con_rate > 0 else np.inf

        perm_values = np.empty(self.position_permutation_draws, dtype=float)
        for draw_idx in range(self.position_permutation_draws):
            permuted_positions = rng.permutation(positions)
            pro_perm_dup_rows, pro_perm_total = self._duplicate_rows_for_factorized_subset(
                key_ids=key_ids,
                subset_mask=permuted_positions == "Pro",
                n_keys=n_keys,
            )
            con_perm_dup_rows, con_perm_total = self._duplicate_rows_for_factorized_subset(
                key_ids=key_ids,
                subset_mask=permuted_positions == "Con",
                n_keys=n_keys,
            )
            pro_perm_rate = (pro_perm_dup_rows / pro_perm_total) if pro_perm_total else 0.0
            con_perm_rate = (con_perm_dup_rows / con_perm_total) if con_perm_total else 0.0
            perm_values[draw_idx] = pro_perm_rate - con_perm_rate

        perm_series = perm_values
        if observed_diff >= 0:
            p_value = float((np.sum(perm_series >= observed_diff) + 1) / (perm_series.size + 1))
        else:
            p_value = float((np.sum(perm_series <= observed_diff) + 1) / (perm_series.size + 1))
        effect, ci_low, ci_high = bootstrap_rate_difference(
            successes_a=pro_dup_rows,
            total_a=pro_total,
            successes_b=con_dup_rows,
            total_b=con_total,
            n_boot=4000,
            rng=rng,
        )
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
                    "rate_ratio": float(observed_rr) if np.isfinite(observed_rr) else 0.0,
                    "permutation_p_value_one_sided": float(p_value),
                    "n_permutations": int(self.position_permutation_draws),
                }
            ]
        )

    def _temporal_metrics_by_name(
        self,
        working: pd.DataFrame,
        key_column: str,
        *,
        rng: np.random.Generator,
    ) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        if working.empty:
            return pd.DataFrame()
        all_times = pd.to_datetime(working["timestamp"], errors="coerce").dropna().to_numpy(dtype="datetime64[m]")
        if all_times.size == 0:
            return pd.DataFrame()
        all_minutes = all_times.astype("datetime64[m]").astype(np.int64)
        for key, group in working.groupby(key_column, dropna=False):
            times = pd.to_datetime(group["timestamp"], errors="coerce").dropna().to_numpy(dtype="datetime64[m]")
            if times.size < 2:
                continue
            minutes = np.sort(times.astype("datetime64[m]").astype(np.int64))
            gaps = np.diff(minutes)
            min_gap = int(gaps.min()) if gaps.size else 0
            within_5 = int(np.sum(gaps <= 5))
            within_15 = int(np.sum(gaps <= 15))
            span_minutes = int(minutes.max() - minutes.min()) if minutes.size else 0

            min_gap_null: list[int] = []
            within_5_null: list[int] = []
            within_15_null: list[int] = []
            draws = min(self.temporal_permutation_draws, 1000)
            sample_size = int(len(minutes))
            for _ in range(draws):
                sampled = np.sort(rng.choice(all_minutes, size=sample_size, replace=False))
                sampled_gaps = np.diff(sampled)
                min_gap_null.append(int(sampled_gaps.min()) if sampled_gaps.size else 0)
                within_5_null.append(int(np.sum(sampled_gaps <= 5)))
                within_15_null.append(int(np.sum(sampled_gaps <= 15)))
            p_value_min_gap = (
                float((np.sum(np.asarray(min_gap_null) <= min_gap) + 1) / (draws + 1)) if draws else 1.0
            )
            p_value_within_5 = (
                float((np.sum(np.asarray(within_5_null) >= within_5) + 1) / (draws + 1))
                if draws
                else 1.0
            )
            p_value_within_15 = (
                float((np.sum(np.asarray(within_15_null) >= within_15) + 1) / (draws + 1))
                if draws
                else 1.0
            )
            rows.append(
                {
                    "canonical_name": str(key),
                    "min_gap_minutes": min_gap,
                    "within_5m_pairs": within_5,
                    "within_15m_pairs": within_15,
                    "time_span_minutes": span_minutes,
                    "temporal_p_value_min_gap": p_value_min_gap,
                    "temporal_p_value_within_5m": p_value_within_5,
                    "temporal_p_value_within_15m": p_value_within_15,
                    "temporal_permutation_draws": draws,
                }
            )
        return pd.DataFrame(rows)

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

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        if df.empty:
            return DetectorResult(detector=self.name, summary={"n_records": 0}, tables={})

        working = df.copy()
        key_column = self._resolved_collision_key_column(working)
        working[key_column] = _safe_str_series(working[key_column])
        working = working[working[key_column] != ""].copy()
        if working.empty:
            return DetectorResult(detector=self.name, summary={"n_records": 0}, tables={})
        working["canonical_name"] = _safe_str_series(working.get("canonical_name", working[key_column]))
        working["position_normalized"] = _safe_str_series(working.get("position_normalized", "Unknown")).replace(
            "", "Unknown"
        )
        working["timestamp"] = pd.to_datetime(working.get("timestamp"), errors="coerce")
        working["minute_bucket"] = pd.to_datetime(working.get("minute_bucket"), errors="coerce")
        working["name_display"] = _safe_str_series(
            working.get("name_display", working[key_column].map(lambda value: str(value)))
        )
        infer = working.copy()
        if self.exclude_non_person_from_inference and "is_person_name" in infer.columns:
            infer = infer[infer["is_person_name"].astype(bool)].copy()
        if infer.empty:
            infer = working.copy()

        rng = np.random.default_rng(self.random_seed)
        scope_frames = self._scope_frames(infer=infer, features=features)
        scope_names = self._scope_list()
        for required_scope in scope_names:
            if required_scope not in scope_frames:
                scope_frames[required_scope] = pd.DataFrame(columns=infer.columns)

        normalization_hash = self._normalization_version_hash()
        methods_rows: list[dict[str, object]] = []
        overview_frames: list[pd.DataFrame] = []
        bucket_frames: list[pd.DataFrame] = []
        per_name_tests_frames: list[pd.DataFrame] = []
        per_name_display_frames: list[pd.DataFrame] = []
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
        requested_stratification = self.collision_stratification
        effective_stratification = requested_stratification
        stratification_degraded = False
        stratum_frequencies = pd.DataFrame(columns=["name_key", "stratum", "n_registry_rows"])

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
            scope_frame = scope_frames.get(scope, pd.DataFrame(columns=infer.columns)).copy()
            scope_frame[key_column] = _safe_str_series(scope_frame.get(key_column, pd.Series(dtype=str)))
            scope_frame = scope_frame[scope_frame[key_column] != ""].copy()

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

                mode_totals = (
                    mode_frame.groupby("name_key", dropna=False)
                    .agg(
                        total_repeated_rows=("id", "count"),
                        display_name=("display_name_mode", "first"),
                    )
                    .reset_index()
                )
                mode_totals = mode_totals[mode_totals["total_repeated_rows"] >= 2].copy()
                if mode_totals.empty:
                    continue
                mode_totals = mode_totals.sort_values(
                    ["total_repeated_rows", "display_name", "name_key"],
                    ascending=[False, True, True],
                ).head(10)
                mode_totals = mode_totals.reset_index(drop=True)
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
                            n_pro=("position_normalized", lambda series: int((series == "Pro").sum())),
                            n_con=("position_normalized", lambda series: int((series == "Con").sum())),
                            first_seen=("timestamp", "min"),
                            last_seen=("timestamp", "max"),
                        )
                        .reset_index()
                    )
                    bucket_counts = bucket_counts[bucket_counts["duplicate_rows"] >= 2].copy()
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
                                "first_seen",
                                "last_seen",
                            ]
                        ].copy()
                    )
            if scope_top_name_timing:
                top_name_timing_frames.append(pd.concat(scope_top_name_timing, ignore_index=True))

            grouped = (
                scope_frame.groupby(key_column, dropna=False)
                .agg(
                    observed_count=("id", "count"),
                    n_pro=("position_normalized", lambda series: int((series == "Pro").sum())),
                    n_con=("position_normalized", lambda series: int((series == "Con").sum())),
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
                            rng=rng,
                            stratum_weights=stratified_sampling_weights,
                            stratum_keys=stratified_sampling_keys,
                            stratum_probabilities=stratified_sampling_probabilities,
                            max_draws=scope_max_draws,
                        )
                    else:
                        if stratified_null_histogram.empty:
                            stratified_null_histogram = histogram_from_probabilities(
                                probabilities=stratified_probabilities.to_numpy(dtype=float),
                                n_population=max(int(n_population), 1),
                            )
                        null_samples = simulate_collision_null_from_histogram(
                            n_rows=n_scope,
                            histogram=stratified_null_histogram,
                            draws=self.monte_carlo_draws,
                            rng=rng,
                            baseline_model="multinomial",
                            n_population=n_population if n_population > 0 else None,
                            max_draws=scope_max_draws,
                        )
                else:
                    null_samples = simulate_collision_null_from_histogram(
                        n_rows=n_scope,
                        histogram=histogram,
                        draws=self.monte_carlo_draws,
                        rng=rng,
                        baseline_model=self.collision_baseline_model,
                        n_population=n_population if n_population > 0 else None,
                        max_draws=scope_max_draws,
                    )
            else:
                null_samples = pd.DataFrame()
            overview = summarize_collision_observed_vs_null(
                observed=observed_metrics,
                expected=expected_metrics,
                null_samples=null_samples,
                metrics=self.collision_metrics,
            )
            overview["scope"] = scope
            overview["n_used"] = int(n_scope)
            overview["N_used"] = int(n_population)
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
            if grouped.empty:
                grouped["p_value"] = pd.Series(dtype=float)
            elif self.per_name_significance_model == "hypergeometric_tail":
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
                grouped["p_value"] = grouped.apply(
                    lambda row: binomial_tail_p_value(
                        observed_successes=int(row["observed_count"]),
                        total_trials=int(n_scope),
                        success_probability=float(max(min(row["population_probability"], 1.0), 0.0)),
                    ),
                    axis=1,
                )
            grouped["q_value"] = benjamini_hochberg(grouped["p_value"]).fillna(1.0)
            grouped["is_significant"] = grouped["q_value"] <= self.bh_fdr_q
            grouped["tested"] = True
            grouped["scope"] = scope
            grouped = grouped.sort_values(["q_value", "p_value", "observed_count"], ascending=[True, True, False])
            per_name_tests_frames.append(
                grouped[
                    [
                        "scope",
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

            temporal = self._temporal_metrics_by_name(scope_frame, key_column, rng=rng)
            if not temporal.empty:
                temporal["scope"] = scope
                temporal_frames.append(temporal)

            scope_degraded = bool(
                baseline_degraded
                or stratification_degraded
                or (
                    requested_stratification != "none"
                    and effective_scope_stratification != requested_stratification
                )
            )
            methods_rows.append(
                {
                    "scope": scope,
                    "baseline_source": effective_baseline_source,
                    "baseline_model": self.collision_baseline_model,
                    "uncertainty_model": self.collision_uncertainty_mode,
                    "n_used": int(n_scope),
                    "N_used": int(n_population),
                    "metric_primary": self.collision_primary_metric,
                    "metrics_reported": ",".join(self.collision_metrics),
                    "baseline_degraded": scope_degraded,
                    "fallback_policy": self.collision_baseline_failure_policy,
                    "collision_key_mode": self.collision_key_mode,
                    "normalization_version_hash": normalization_hash,
                    "stratification": effective_scope_stratification,
                    "censored": bool(len(grouped) > self.per_name_display_limit),
                }
            )

            metric_summary_by_n: dict[int, dict[str, dict[str, float]]] = {}
            minute_series = pd.to_datetime(scope_frame.get("minute_bucket"), errors="coerce")
            for bucket_minutes in self.bucket_minutes:
                bucket_start = minute_series.dt.floor(f"{int(bucket_minutes)}min")
                bucketed = scope_frame.assign(bucket_start=bucket_start).dropna(subset=["bucket_start"])
                if bucketed.empty:
                    continue
                for start, group_frame in bucketed.groupby("bucket_start", dropna=False):
                    group_counts = group_frame.groupby(key_column, dropna=False).size().to_numpy(dtype=float)
                    metric_values = collision_metrics_from_counts(group_counts)
                    n_bucket = int(len(group_frame))
                    if n_bucket not in metric_summary_by_n:
                        bucket_max_draws = self._collision_monte_carlo_draw_budget(
                            n_rows=n_bucket,
                            hard_cap=250,
                        )
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
                            if (
                                self.collision_uncertainty_mode == "monte_carlo"
                                and self.collision_baseline_model == "multinomial"
                            ):
                                if (
                                    stratified_sampling_weights.size > 0
                                    and stratified_sampling_keys
                                    and stratified_sampling_probabilities
                                ):
                                    null_for_n = self._simulate_stratified_collision_null(
                                        n_rows=n_bucket,
                                        draws=self.monte_carlo_draws,
                                        rng=rng,
                                        stratum_weights=stratified_sampling_weights,
                                        stratum_keys=stratified_sampling_keys,
                                        stratum_probabilities=stratified_sampling_probabilities,
                                        max_draws=bucket_max_draws,
                                    )
                                else:
                                    null_for_n = simulate_collision_null_from_histogram(
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
                                        rng=rng,
                                        baseline_model="multinomial",
                                        n_population=n_population if n_population > 0 else None,
                                        max_draws=bucket_max_draws,
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
                            null_for_n = (
                                simulate_collision_null_from_histogram(
                                    n_rows=n_bucket,
                                    histogram=histogram,
                                    draws=self.monte_carlo_draws,
                                    rng=rng,
                                    baseline_model=self.collision_baseline_model,
                                    n_population=n_population if n_population > 0 else None,
                                    max_draws=bucket_max_draws,
                                )
                                if self.collision_uncertainty_mode == "monte_carlo"
                                else pd.DataFrame()
                            )
                        summary_for_n = summarize_collision_observed_vs_null(
                            observed=metric_values,
                            expected=expected_for_n,
                            null_samples=null_for_n,
                            metrics=self.collision_metrics,
                        )
                        metric_summary_by_n[n_bucket] = {
                            row.metric: {
                                "expected": float(row.expected),
                                "expected_p05": float(row.expected_p05),
                                "expected_p95": float(row.expected_p95),
                                "p_value": float(row.p_value),
                                "z_score": float(row.z_score),
                            }
                            for row in summary_for_n.itertuples(index=False)
                        }
                    n_unique = int(group_frame[key_column].nunique())
                    n_pro = int((group_frame["position_normalized"] == "Pro").sum())
                    n_con = int((group_frame["position_normalized"] == "Con").sum())
                    expected_primary_bucket = metric_summary_by_n[n_bucket].get(self.collision_primary_metric, {}).get("expected", 0.0)
                    low_power = bool(
                        n_unique < self.low_power_min_unique_names
                        or expected_primary_bucket < self.low_power_min_expected_duplicates
                    )
                    for metric in self.collision_metrics:
                        metric_obs = float(metric_values.get(metric, 0.0))
                        summary_entry = metric_summary_by_n[n_bucket].get(metric, {})
                        metric_exp = float(summary_entry.get("expected", 0.0))
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
                                "expected_p05": float(summary_entry.get("expected_p05", metric_exp)),
                                "expected_p95": float(summary_entry.get("expected_p95", metric_exp)),
                                "z_score": float(summary_entry.get("z_score", 0.0)),
                                "p_value": float(summary_entry.get("p_value", 1.0)),
                                "excess": float(metric_obs - metric_exp),
                                "baseline_model": self.collision_baseline_model,
                                "baseline_source": effective_baseline_source,
                                "baseline_degraded": bool(scope_degraded),
                                "is_low_power": low_power,
                                "inference_status": "descriptive_only" if low_power else "tested",
                            }
                        )

            if scope == self.collision_scope_primary:
                primary_scope_row_count = int(n_scope)
                primary_scope_unique_count = int(observed_metrics["n_unique_names"])
                primary_scope_repeated = float(observed_metrics["repeated_group_rows"])
                primary_scope_pairs = float(observed_metrics["pairs"])
                primary_scope_significant = int(grouped["is_significant"].sum())
                primary_scope_baseline_source = effective_baseline_source
                primary_scope_degraded = bool(scope_degraded)
                primary_scope_n_used = int(n_scope)
                primary_scope_n_population = int(n_population)
                primary_scope_stratification = effective_scope_stratification
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
                legacy_per_name_anomalies["inference_status"] = "tested"
                legacy_top_repeated = legacy_per_name_anomalies[
                    legacy_per_name_anomalies["n"] >= 2
                ][["display_name", "canonical_name", "n", "n_pro", "n_con", "time_span_minutes"]].copy()
                legacy_top_repeated = legacy_top_repeated.sort_values("n", ascending=False).head(self.top_n)

                position_rows: list[dict[str, object]] = []
                for position in sorted(set(scope_frame["position_normalized"].unique())):
                    subset = scope_frame[scope_frame["position_normalized"] == position]
                    subset_counts = subset.groupby(key_column, dropna=False).size().to_numpy(dtype=float)
                    subset_metrics = collision_metrics_from_counts(subset_counts)
                    expected_rows = float(
                        expected_collision_metrics(
                            n_rows=int(len(subset)),
                            histogram=histogram,
                            baseline_model=self.collision_baseline_model,
                            n_population=n_population if n_population > 0 else None,
                        ).get("repeated_group_rows", 0.0)
                    )
                    low_power = bool(
                        int(subset_metrics["n_unique_names"]) < self.low_power_min_unique_names
                        or expected_rows < self.low_power_min_expected_duplicates
                    )
                    position_rows.append(
                        {
                            "position_normalized": position,
                            "n_rows": int(subset_metrics["n_rows"]),
                            "n_unique_names": int(subset_metrics["n_unique_names"]),
                            "duplicate_rows": int(subset_metrics["repeated_group_rows"]),
                            "duplicate_row_rate": (
                                float(subset_metrics["repeated_group_rows"] / subset_metrics["n_rows"])
                                if subset_metrics["n_rows"] > 0
                                else 0.0
                            ),
                            "duplicate_pairs": float(subset_metrics["pairs"]),
                            "expected_duplicate_rows": float(expected_rows),
                            "expected_duplicate_row_rate": (
                                float(expected_rows / subset_metrics["n_rows"])
                                if subset_metrics["n_rows"] > 0
                                else 0.0
                            ),
                            "excess_duplicate_rows": float(
                                max(subset_metrics["repeated_group_rows"] - expected_rows, 0.0)
                            ),
                            "is_low_power": low_power,
                            "inference_status": "descriptive_only" if low_power else "tested",
                        }
                    )
                legacy_position_metrics = pd.DataFrame(position_rows)
                legacy_position_tests = self._position_permutation_test(scope_frame, key_column, rng=rng)
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

                repeated_same_bucket_frames: list[pd.DataFrame] = []
                for bucket_minutes in self.bucket_minutes:
                    bucket_start = pd.to_datetime(scope_frame["minute_bucket"], errors="coerce").dt.floor(
                        f"{int(bucket_minutes)}min"
                    )
                    bucketed = scope_frame.assign(bucket_start=bucket_start).dropna(subset=["bucket_start"])
                    if bucketed.empty:
                        continue
                    repeated_same_bucket = (
                        bucketed.groupby([key_column, "bucket_start"], dropna=False)
                        .agg(
                            n=("id", "count"),
                            n_pro=("position_normalized", lambda series: int((series == "Pro").sum())),
                            n_con=("position_normalized", lambda series: int((series == "Con").sum())),
                            n_unknown=("position_normalized", lambda series: int((series == "Unknown").sum())),
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

        collision_methods = pd.DataFrame(methods_rows)
        collision_overview = pd.concat(overview_frames, ignore_index=True) if overview_frames else pd.DataFrame()
        collision_by_bucket = (
            pd.DataFrame(bucket_frames).sort_values(["scope", "bucket_minutes", "bucket_start", "metric"])
            if bucket_frames
            else pd.DataFrame()
        )
        per_name_tests = pd.concat(per_name_tests_frames, ignore_index=True) if per_name_tests_frames else pd.DataFrame()
        per_name_display = (
            pd.concat(per_name_display_frames, ignore_index=True) if per_name_display_frames else pd.DataFrame()
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

        primary_scope_overview = collision_overview[
            collision_overview["scope"] == self.collision_scope_primary
        ].copy()
        legacy_overview = self._legacy_duplicate_metrics_overview(
            scope_overview=primary_scope_overview,
            n_rows=primary_scope_n_used,
        )

        summary = {
            "name_key": self.collision_key_mode,
            "baseline_source": primary_scope_baseline_source,
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
            "n_used": int(primary_scope_n_used),
            "N_used": int(primary_scope_n_population),
            "baseline_degraded": bool(primary_scope_degraded),
            "stratification": primary_scope_stratification,
        }

        return DetectorResult(
            detector=self.name,
            summary=summary,
            tables={
                "collision_methods": collision_methods,
                "collision_overview": collision_overview,
                "collision_by_bucket": collision_by_bucket,
                "collision_stratification_sensitivity": collision_stratification_sensitivity,
                "per_name_tests": per_name_tests,
                "per_name_display": per_name_display,
                "temporal_burst_signals": temporal_burst,
                "top_name_timing_by_mode": top_name_timing_by_mode,
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
