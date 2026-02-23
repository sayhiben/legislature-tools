from __future__ import annotations

from collections import defaultdict

import numpy as np
import pandas as pd
from scipy.stats import poisson

from testifier_audit.detectors.base import Detector, DetectorResult
from testifier_audit.io.vrdb_postgres import (
    fetch_matching_voter_keys,
)
from testifier_audit.names.collision_baseline import (
    duplicate_metrics_from_counts,
    simulate_duplicate_null,
    summarize_observed_vs_null,
)
from testifier_audit.names.stat_tests import (
    benjamini_hochberg,
    bootstrap_rate_difference,
)

_KEY_TO_COLUMN = {
    "strict": "canonical_key_strict",
    "medium": "canonical_key_medium",
    "loose": "canonical_key_loose",
    "nickname": "canonical_key_nickname",
}


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

    def _resolved_primary_key_column(self, frame: pd.DataFrame) -> str:
        configured = _KEY_TO_COLUMN.get(self.primary_name_key, "canonical_key_medium")
        if configured in frame.columns:
            return configured
        if "canonical_name" in frame.columns:
            return "canonical_name"
        if "canonical_key_medium" in frame.columns:
            return "canonical_key_medium"
        raise ValueError(
            "Missing canonical name key columns. Expected one of canonical_name/canonical_key_medium."
        )

    @staticmethod
    def _position_counts(working: pd.DataFrame, key_column: str, position: str) -> np.ndarray:
        subset = working[working["position_normalized"] == position]
        if subset.empty:
            return np.asarray([], dtype=float)
        counts = (
            subset.groupby(key_column, dropna=False)
            .size()
            .rename("n")
            .reset_index()["n"]
            .to_numpy(dtype=float)
        )
        return counts

    @staticmethod
    def _duplicate_rows_for_subset(working: pd.DataFrame, key_column: str) -> tuple[int, int]:
        if working.empty:
            return (0, 0)
        counts = working.groupby(key_column, dropna=False).size().to_numpy(dtype=float)
        duplicate_rows = int(counts[counts >= 2.0].sum())
        return duplicate_rows, int(len(working))

    def _population_name_counts(self, working: pd.DataFrame, key_column: str) -> tuple[pd.Series, str]:
        observed_counts = (
            working.groupby(key_column, dropna=False)
            .size()
            .rename("n_registry_rows")
            .sort_values(ascending=False)
        )
        if not self.voter_db_url:
            return observed_counts, "hearing_empirical_fallback"
        try:
            key_values = sorted({str(value or "").strip() for value in working[key_column].tolist() if value})
            lookup = fetch_matching_voter_keys(
                db_url=self.voter_db_url,
                table_name=self.voter_table_name,
                key_values=key_values,
                key_column=key_column if key_column != "canonical_name" else "canonical_name",
                active_only=self.voter_active_only,
            )
            if lookup.empty:
                return observed_counts, "hearing_empirical_fallback"
            counts = (
                lookup[[key_column if key_column in lookup.columns else "canonical_name", "n_registry_rows"]]
                .rename(
                    columns={
                        key_column if key_column in lookup.columns else "canonical_name": key_column
                    }
                )
                .set_index(key_column)["n_registry_rows"]
                .astype(float)
            )
            # Include unmatched hearing keys with pseudocount so tail probabilities remain finite.
            missing = sorted(set(observed_counts.index) - set(counts.index))
            if missing:
                pseudo = pd.Series(index=missing, data=np.full(len(missing), 1.0), dtype=float)
                counts = pd.concat([counts, pseudo])
            return counts.sort_values(ascending=False), "wa_active_observed_key_slice"
        except Exception:
            return observed_counts, "hearing_empirical_fallback"

    def _position_permutation_test(
        self,
        working: pd.DataFrame,
        key_column: str,
        *,
        rng: np.random.Generator,
    ) -> pd.DataFrame:
        if working.empty:
            return pd.DataFrame()
        positions = _safe_str_series(working["position_normalized"])
        if not {"Pro", "Con"}.issubset(set(positions.unique())):
            return pd.DataFrame()

        pro_subset = working[positions == "Pro"]
        con_subset = working[positions == "Con"]
        pro_dup_rows, pro_total = self._duplicate_rows_for_subset(pro_subset, key_column)
        con_dup_rows, con_total = self._duplicate_rows_for_subset(con_subset, key_column)
        pro_rate = (pro_dup_rows / pro_total) if pro_total else 0.0
        con_rate = (con_dup_rows / con_total) if con_total else 0.0
        observed_diff = pro_rate - con_rate
        observed_rr = (pro_rate / con_rate) if con_rate > 0 else np.inf

        keys = _safe_str_series(working[key_column]).to_numpy()
        original_positions = positions.to_numpy()
        perm_values: list[float] = []
        for _ in range(self.position_permutation_draws):
            permuted_positions = rng.permutation(original_positions)
            permuted = pd.DataFrame({"key": keys, "position": permuted_positions})
            pro_perm = permuted[permuted["position"] == "Pro"]
            con_perm = permuted[permuted["position"] == "Con"]
            pro_perm_dup_rows, pro_perm_total = self._duplicate_rows_for_subset(pro_perm, "key")
            con_perm_dup_rows, con_perm_total = self._duplicate_rows_for_subset(con_perm, "key")
            pro_perm_rate = (pro_perm_dup_rows / pro_perm_total) if pro_perm_total else 0.0
            con_perm_rate = (con_perm_dup_rows / con_perm_total) if con_perm_total else 0.0
            perm_values.append(pro_perm_rate - con_perm_rate)

        perm_series = np.asarray(perm_values, dtype=float)
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
        self, working: pd.DataFrame, key_column: str, *, rng: np.random.Generator
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

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        if df.empty:
            return DetectorResult(detector=self.name, summary={"n_records": 0}, tables={})

        working = df.copy()
        key_column = self._resolved_primary_key_column(working)
        working[key_column] = _safe_str_series(working[key_column])
        working = working[working[key_column] != ""].copy()
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
        population_counts, baseline_source = self._population_name_counts(infer, key_column)
        grouped = (
            infer.groupby(key_column, dropna=False)
            .agg(
                n=("id", "count"),
                n_pro=("position_normalized", lambda series: int((series == "Pro").sum())),
                n_con=("position_normalized", lambda series: int((series == "Con").sum())),
                n_other=(
                    "position_normalized",
                    lambda series: int(~series.isin({"Pro", "Con"}).sum()),
                ),
                first_seen=("timestamp", "min"),
                last_seen=("timestamp", "max"),
                display_name=("name_display", "first"),
            )
            .reset_index()
            .rename(columns={key_column: "canonical_name"})
        )
        grouped["time_span_minutes"] = (
            (grouped["last_seen"] - grouped["first_seen"]).dt.total_seconds() / 60.0
        ).fillna(0.0)

        counts = grouped["n"].to_numpy(dtype=float)
        observed_metrics = duplicate_metrics_from_counts(counts)
        null_samples = simulate_duplicate_null(
            n_rows=int(observed_metrics["n_rows"]),
            population_name_counts=population_counts,
            draws=self.monte_carlo_draws,
            rng=rng,
        )
        overview = summarize_observed_vs_null(
            observed=observed_metrics,
            null_samples=null_samples,
            metric_fields=("duplicate_rows", "duplicate_row_rate", "duplicate_pairs"),
        )
        overview["n_rows"] = int(observed_metrics["n_rows"])
        overview["n_unique_names"] = int(observed_metrics["n_unique_names"])
        overview["name_key"] = self.primary_name_key
        overview["baseline_source"] = baseline_source

        expected_dup_rows = float(
            overview.loc[overview["metric"] == "duplicate_rows", "expected_mean"].iloc[0]
        ) if not overview.empty and "duplicate_rows" in set(overview["metric"]) else 0.0

        position_rows: list[dict[str, object]] = []
        for position in sorted(set(infer["position_normalized"].unique())):
            subset_counts = self._position_counts(infer, key_column, position)
            metrics = duplicate_metrics_from_counts(subset_counts)
            expected_rows = metrics["n_rows"] * (
                expected_dup_rows / float(observed_metrics["n_rows"] or 1.0)
            )
            low_power = bool(
                metrics["n_unique_names"] < self.low_power_min_unique_names
                or expected_rows < self.low_power_min_expected_duplicates
            )
            position_rows.append(
                {
                    "position_normalized": position,
                    "n_rows": int(metrics["n_rows"]),
                    "n_unique_names": int(metrics["n_unique_names"]),
                    "duplicate_rows": int(metrics["duplicate_rows"]),
                    "duplicate_row_rate": float(metrics["duplicate_row_rate"]),
                    "duplicate_pairs": float(metrics["duplicate_pairs"]),
                    "expected_duplicate_rows": float(expected_rows),
                    "expected_duplicate_row_rate": (
                        float(expected_rows / metrics["n_rows"]) if metrics["n_rows"] else 0.0
                    ),
                    "excess_duplicate_rows": float(max(metrics["duplicate_rows"] - expected_rows, 0.0)),
                    "is_low_power": low_power,
                    "inference_status": "descriptive_only" if low_power else "tested",
                }
            )
        position_metrics = pd.DataFrame(position_rows)
        position_tests = self._position_permutation_test(infer, key_column, rng=rng)
        if not position_tests.empty and not position_metrics.empty:
            position_tests["left_is_low_power"] = bool(
                position_metrics.set_index("position_normalized")
                .reindex(["Pro"])["is_low_power"]
                .fillna(True)
                .iloc[0]
            )
            position_tests["right_is_low_power"] = bool(
                position_metrics.set_index("position_normalized")
                .reindex(["Con"])["is_low_power"]
                .fillna(True)
                .iloc[0]
            )

        temporal_by_name = self._temporal_metrics_by_name(infer, key_column, rng=rng)
        if not temporal_by_name.empty:
            temporal_by_name = temporal_by_name.sort_values(
                ["temporal_p_value_within_5m", "temporal_p_value_min_gap", "within_5m_pairs"],
                ascending=[True, True, False],
            )

        population_lookup = defaultdict(float)
        for idx, value in population_counts.items():
            population_lookup[str(idx)] = float(value)

        per_name = grouped[grouped["n"] >= 2].copy()
        n_total = float(len(infer))
        if n_total <= 0:
            n_total = 1.0
        per_name["population_count"] = per_name["canonical_name"].map(population_lookup).fillna(1.0)
        total_population = float(sum(population_lookup.values()) or 1.0)
        per_name["population_probability"] = per_name["population_count"] / total_population
        per_name["expected_count"] = per_name["population_probability"] * n_total
        per_name["p_value"] = per_name.apply(
            lambda row: float(
                poisson.sf(
                    int(max(row["n"], 1) - 1),
                    max(float(row["expected_count"]), 1e-12),
                )
            ),
            axis=1,
        )
        per_name["q_value"] = benjamini_hochberg(per_name["p_value"]).fillna(1.0)
        if not temporal_by_name.empty:
            per_name = per_name.merge(temporal_by_name, on="canonical_name", how="left")
        else:
            per_name["min_gap_minutes"] = np.nan
            per_name["within_5m_pairs"] = 0
            per_name["within_15m_pairs"] = 0
            per_name["temporal_p_value_min_gap"] = np.nan
            per_name["temporal_p_value_within_5m"] = np.nan
            per_name["temporal_p_value_within_15m"] = np.nan
        per_name["is_significant"] = per_name["q_value"] <= self.bh_fdr_q
        per_name["name_key"] = self.primary_name_key
        per_name["rarity_tier"] = pd.qcut(
            per_name["population_probability"].rank(method="average"),
            q=min(4, max(1, len(per_name))),
            labels=False,
            duplicates="drop",
        ).astype(float)
        per_name = per_name.sort_values(["q_value", "p_value", "n"], ascending=[True, True, False])
        per_name = per_name.head(self.max_per_name_rows)

        bucket_frames: list[pd.DataFrame] = []
        repeated_same_bucket_frames: list[pd.DataFrame] = []
        minute_series = pd.to_datetime(infer["minute_bucket"], errors="coerce")
        expected_dup_rate = expected_dup_rows / float(observed_metrics["n_rows"] or 1.0)
        for bucket_minutes in self.bucket_minutes:
            floor_rule = f"{int(bucket_minutes)}min"
            bucket_start = minute_series.dt.floor(floor_rule)
            bucketed = infer.assign(bucket_start=bucket_start).dropna(subset=["bucket_start"])
            if bucketed.empty:
                continue

            by_bucket = (
                bucketed.groupby("bucket_start", dropna=False)
                .agg(
                    n_rows=("id", "count"),
                    n_unique_names=(key_column, "nunique"),
                    n_pro=("position_normalized", lambda series: int((series == "Pro").sum())),
                    n_con=("position_normalized", lambda series: int((series == "Con").sum())),
                )
                .reset_index()
            )
            by_bucket["bucket_minutes"] = int(bucket_minutes)
            by_bucket["expected_duplicate_rows"] = by_bucket["n_rows"] * float(expected_dup_rate)
            by_bucket["duplicate_rows"] = (
                bucketed.groupby("bucket_start")[key_column]
                .apply(lambda series: int(series.value_counts()[lambda s: s >= 2].sum()))
                .reset_index(drop=True)
            )
            by_bucket["duplicate_row_rate"] = (
                by_bucket["duplicate_rows"] / by_bucket["n_rows"]
            ).where(by_bucket["n_rows"] > 0, 0.0)
            by_bucket["excess_duplicate_rows"] = (
                by_bucket["duplicate_rows"] - by_bucket["expected_duplicate_rows"]
            ).clip(lower=0.0)
            bucket_frames.append(by_bucket)

            repeated_same_bucket = (
                bucketed.groupby([key_column, "bucket_start"], dropna=False)
                .agg(
                    n=("id", "count"),
                    n_pro=("position_normalized", lambda series: int((series == "Pro").sum())),
                    n_con=("position_normalized", lambda series: int((series == "Con").sum())),
                    n_unknown=(
                        "position_normalized",
                        lambda series: int((series == "Unknown").sum()),
                    ),
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

        duplicate_by_bucket = (
            pd.concat(bucket_frames, ignore_index=True).sort_values(["bucket_minutes", "bucket_start"])
            if bucket_frames
            else pd.DataFrame()
        )
        repeated_same_bucket = (
            pd.concat(repeated_same_bucket_frames, ignore_index=True)
            .sort_values(["bucket_minutes", "n", "canonical_name", "bucket_start"], ascending=[True, False, True, True])
            if repeated_same_bucket_frames
            else pd.DataFrame(
                columns=[
                    "canonical_name",
                    "bucket_start",
                    "n",
                    "n_pro",
                    "n_con",
                    "n_unknown",
                    "bucket_minutes",
                    "bucket_end",
                ]
            )
        )

        switch_names = grouped[(grouped["n_pro"] > 0) & (grouped["n_con"] > 0)].copy()
        switch_names = switch_names.rename(columns={"canonical_name": "canonical_name"}).sort_values(
            "n", ascending=False
        )

        swing_rows = []
        raw_pro = int((infer["position_normalized"] == "Pro").sum())
        raw_con = int((infer["position_normalized"] == "Con").sum())
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
        strict_col = "canonical_key_strict" if "canonical_key_strict" in infer.columns else key_column
        strict_names = infer[[strict_col, "position_normalized"]].copy()
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
        pos_index = position_metrics.set_index("position_normalized") if not position_metrics.empty else pd.DataFrame()
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
        swing_impact = pd.DataFrame(swing_rows)

        null_distribution = pd.DataFrame()
        if not null_samples.empty:
            null_distribution = null_samples.reset_index(drop=True).copy()
            null_distribution["iteration"] = null_distribution.index.astype(int)

        temporal_burst = temporal_by_name.copy()
        if not temporal_burst.empty:
            temporal_burst = temporal_burst.sort_values(
                ["temporal_p_value_within_5m", "temporal_p_value_min_gap", "within_5m_pairs"],
                ascending=[True, True, False],
            )

        top_repeated_names = grouped.sort_values("n", ascending=False).head(self.top_n)

        repeated_same_minute = (
            repeated_same_bucket[repeated_same_bucket["bucket_minutes"] == 1].copy()
            if not repeated_same_bucket.empty and "bucket_minutes" in repeated_same_bucket.columns
            else pd.DataFrame(
                columns=[
                    "minute_bucket",
                    "canonical_name",
                    "bucket_start",
                    "n",
                    "n_pro",
                    "n_con",
                    "n_unknown",
                    "bucket_minutes",
                    "bucket_end",
                ]
            )
        )
        if not repeated_same_minute.empty:
            repeated_same_minute["minute_bucket"] = repeated_same_minute["bucket_start"]
        if not repeated_same_bucket.empty:
            repeated_same_bucket_summary = (
                repeated_same_bucket.groupby("bucket_minutes", dropna=False)
                .agg(
                    n_repeated_rows=("n", "sum"),
                    n_repeated_name_windows=("canonical_name", "count"),
                    n_unique_names=("canonical_name", "nunique"),
                    max_repeats_in_window=("n", "max"),
                )
                .reset_index()
                .sort_values("bucket_minutes")
            )
        else:
            repeated_same_bucket_summary = pd.DataFrame(
                columns=[
                    "bucket_minutes",
                    "n_repeated_rows",
                    "n_repeated_name_windows",
                    "n_unique_names",
                    "max_repeats_in_window",
                ]
            )

        summary = {
            "name_key": self.primary_name_key,
            "baseline_source": baseline_source,
            "n_records": int(len(infer)),
            "n_unique_names": int(observed_metrics["n_unique_names"]),
            "duplicate_rows": int(observed_metrics["duplicate_rows"]),
            "duplicate_row_rate": float(observed_metrics["duplicate_row_rate"]),
            "duplicate_pairs": float(observed_metrics["duplicate_pairs"]),
            "n_repeated_names": int((grouped["n"] >= 2).sum()),
            "max_repeat_count": int(grouped["n"].max()) if not grouped.empty else 0,
            "n_significant_per_name": int(per_name["is_significant"].sum()) if not per_name.empty else 0,
            "bh_fdr_q": float(self.bh_fdr_q),
            "n_low_power_positions": int(position_metrics["is_low_power"].sum())
            if not position_metrics.empty
            else 0,
            "primary_low_power": bool(
                int(observed_metrics["n_unique_names"]) < self.low_power_min_unique_names
                or expected_dup_rows < self.low_power_min_expected_duplicates
            ),
        }

        return DetectorResult(
            detector=self.name,
            summary=summary,
            tables={
                "duplicate_metrics_overview": overview,
                "duplicate_by_bucket": duplicate_by_bucket,
                "position_duplicate_metrics": position_metrics,
                "position_concentration_tests": position_tests,
                "per_name_anomalies": per_name,
                "null_distribution": null_distribution,
                "temporal_burst_signals": temporal_burst,
                "swing_impact_scenarios": swing_impact,
                # Compatibility tables retained for one migration cycle.
                "top_repeated_names": top_repeated_names,
                "repeated_same_bucket": repeated_same_bucket,
                "repeated_same_bucket_summary": repeated_same_bucket_summary,
                "repeated_same_minute": repeated_same_minute,
                "position_switching_names": switch_names,
            },
        )
