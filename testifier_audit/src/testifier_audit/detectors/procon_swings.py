from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import norm

from testifier_audit.detectors.base import Detector, DetectorResult
from testifier_audit.detectors.stats import (
    benjamini_hochberg,
    empirical_tail_p_values,
    rolling_sum,
    simulate_binomial_max_abs_delta,
    simulate_binomial_max_abs_delta_hourly,
    simulate_binomial_max_abs_delta_probability_series,
)
from testifier_audit.proportion_stats import (
    DEFAULT_LOW_POWER_MIN_TOTAL,
    low_power_mask,
    wilson_half_width,
    wilson_interval,
)


class ProConSwingsDetector(Detector):
    name = "procon_swings"
    DEFAULT_PROFILE_BUCKET_MINUTES = [15, 30, 60, 120, 240]
    LONG_DIRECTION_RUN_MIN_BUCKETS = 3

    def __init__(
        self,
        window_minutes: list[int],
        fdr_alpha: float,
        min_window_total: int,
        calibration_enabled: bool = False,
        calibration_mode: str = "global",
        significance_policy: str = "parametric_fdr",
        calibration_iterations: int = 0,
        calibration_seed: int = 42,
        calibration_support_alpha: float = 0.1,
        profile_bucket_minutes: list[int] | None = None,
        low_power_min_total: int = DEFAULT_LOW_POWER_MIN_TOTAL,
    ) -> None:
        self.window_minutes = sorted(set(window_minutes))
        self.fdr_alpha = fdr_alpha
        self.min_window_total = min_window_total
        self.calibration_enabled = calibration_enabled
        self.calibration_mode = calibration_mode
        self.significance_policy = significance_policy
        self.calibration_iterations = calibration_iterations
        self.calibration_seed = calibration_seed
        self.calibration_support_alpha = calibration_support_alpha
        self.low_power_min_total = max(1, int(low_power_min_total))
        buckets = profile_bucket_minutes or self.DEFAULT_PROFILE_BUCKET_MINUTES
        self.profile_bucket_minutes = sorted({int(value) for value in buckets if int(value) > 0})

    @staticmethod
    def _empty_windows() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "window_minutes",
                "start_minute",
                "end_minute",
                "n_total",
                "n_pro",
                "pro_rate",
                "pro_rate_wilson_low",
                "pro_rate_wilson_high",
                "pro_rate_wilson_half_width",
                "baseline_pro_rate",
                "delta_pro_rate",
                "abs_delta_pro_rate",
                "z_score",
                "p_value",
                "q_value",
                "is_low_power",
                "direction",
                "is_significant_parametric_fdr",
                "permutation_p_value",
                "permutation_q_value",
                "is_significant_permutation_raw",
                "is_significant_permutation_fdr",
                "is_significant_permutation",
                "is_calibration_supported",
                "is_significant",
            ]
        )

    @staticmethod
    def _empty_bucket_profiles() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "bucket_minutes",
                "bucket_start",
                "n_total",
                "n_pro",
                "n_con",
                "pro_rate",
                "pro_rate_wilson_low",
                "pro_rate_wilson_high",
                "pro_rate_wilson_half_width",
                "baseline_pro_rate",
                "delta_pro_rate",
                "abs_delta_pro_rate",
                "z_score",
                "p_value",
                "q_value",
                "stable_center",
                "stable_half_width",
                "stable_lower",
                "stable_upper",
                "outside_stable_range",
                "is_significant",
                "is_flagged",
                "is_low_power",
                "direction",
            ]
        )

    @staticmethod
    def _empty_time_of_day_profiles() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "bucket_minutes",
                "slot_index",
                "slot_start_minute",
                "slot_start_label",
                "n_total",
                "n_pro",
                "n_con",
                "pro_rate",
                "pro_rate_wilson_low",
                "pro_rate_wilson_high",
                "pro_rate_wilson_half_width",
                "baseline_pro_rate",
                "delta_pro_rate",
                "abs_delta_pro_rate",
                "z_score",
                "p_value",
                "q_value",
                "stable_center",
                "stable_half_width",
                "stable_lower",
                "stable_upper",
                "outside_stable_range",
                "is_significant",
                "is_flagged",
                "is_low_power",
                "direction",
            ]
        )

    @staticmethod
    def _empty_day_bucket_profiles() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "bucket_minutes",
                "date",
                "slot_index",
                "slot_start_minute",
                "slot_start_label",
                "n_total",
                "n_pro",
                "n_con",
                "pro_rate",
                "pro_rate_wilson_low",
                "pro_rate_wilson_high",
                "pro_rate_wilson_half_width",
                "baseline_pro_rate",
                "delta_pro_rate",
                "abs_delta_pro_rate",
                "expected_slot_pro_rate",
                "delta_from_slot_pro_rate",
                "abs_delta_from_slot_pro_rate",
                "slot_z_score",
                "slot_p_value",
                "slot_q_value",
                "is_slot_significant",
                "is_slot_outlier",
                "is_low_power",
            ]
        )

    @staticmethod
    def _empty_direction_runs() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "bucket_minutes",
                "run_id",
                "run_direction",
                "start_bucket",
                "end_bucket",
                "run_length_buckets",
                "total_n",
                "support_n",
                "mean_abs_delta_pro_rate",
                "max_abs_delta_pro_rate",
                "n_flagged_buckets",
                "n_low_power_buckets",
                "flagged_ratio",
                "low_power_ratio",
                "is_long_run",
            ]
        )

    @staticmethod
    def _empty_direction_runs_summary() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "bucket_minutes",
                "n_runs",
                "n_long_runs",
                "max_run_length_buckets",
                "max_run_mean_abs_delta",
                "max_run_total_n",
                "pro_heavy_run_ratio",
                "flagged_run_ratio",
            ]
        )

    @staticmethod
    def _stable_band(
        pro_rates: pd.Series, min_half_width: float = 0.05
    ) -> tuple[float, float, float, float]:
        valid = pd.to_numeric(pro_rates, errors="coerce").dropna().astype(float)
        if valid.empty:
            return 0.0, 1.0, 0.5, 0.5

        center = float(valid.median())
        median_abs_deviation = float(np.median(np.abs(valid.to_numpy(dtype=float) - center)))
        robust_sigma = 1.4826 * median_abs_deviation
        half_width = float(max(min_half_width, 2.5 * robust_sigma))
        lower = float(max(0.0, center - half_width))
        upper = float(min(1.0, center + half_width))
        return lower, upper, center, half_width

    def _enrich_rate_table(
        self,
        table: pd.DataFrame,
        baseline_pro_rate: float,
        *,
        min_half_width: float = 0.05,
    ) -> pd.DataFrame:
        working = table.copy()
        totals = working["n_total"].astype(float).to_numpy(dtype=float)
        pros = working["n_pro"].astype(float).to_numpy(dtype=float)

        pro_rate = np.divide(
            pros,
            totals,
            out=np.full_like(pros, np.nan, dtype=float),
            where=totals > 0,
        )
        pro_rate_wilson_low, pro_rate_wilson_high = wilson_interval(
            successes=pros,
            totals=totals,
        )
        pro_rate_wilson_half_width = wilson_half_width(
            successes=pros,
            totals=totals,
        )
        is_low_power = low_power_mask(totals=totals, min_total=self.low_power_min_total)
        delta = pro_rate - baseline_pro_rate
        abs_delta = np.abs(delta)

        se = np.full_like(totals, np.nan, dtype=float)
        positive = totals > 0
        se[positive] = np.sqrt((baseline_pro_rate * (1.0 - baseline_pro_rate)) / totals[positive])

        z_score = np.zeros_like(totals, dtype=float)
        valid = positive & (~np.isnan(delta)) & (se > 0)
        z_score[valid] = delta[valid] / se[valid]

        p_values = np.ones_like(totals, dtype=float)
        p_values[valid] = 2.0 * norm.sf(np.abs(z_score[valid]))
        rejected, q_values = benjamini_hochberg(p_values.astype(float), alpha=self.fdr_alpha)

        stable_lower, stable_upper, stable_center, stable_half_width = self._stable_band(
            pd.Series(pro_rate),
            min_half_width=min_half_width,
        )
        outside_stable_range = (~np.isnan(pro_rate)) & (
            (pro_rate < stable_lower) | (pro_rate > stable_upper)
        )
        is_significant = rejected.astype(bool)
        flag_threshold = max(0.10, stable_half_width)
        is_flagged = outside_stable_range & (is_significant | (abs_delta >= flag_threshold))

        working["n_con"] = totals - pros
        working["pro_rate"] = pro_rate
        working["pro_rate_wilson_low"] = pro_rate_wilson_low
        working["pro_rate_wilson_high"] = pro_rate_wilson_high
        working["pro_rate_wilson_half_width"] = pro_rate_wilson_half_width
        working["baseline_pro_rate"] = baseline_pro_rate
        working["delta_pro_rate"] = delta
        working["abs_delta_pro_rate"] = abs_delta
        working["z_score"] = z_score
        working["p_value"] = p_values
        working["q_value"] = q_values
        working["stable_center"] = stable_center
        working["stable_half_width"] = stable_half_width
        working["stable_lower"] = stable_lower
        working["stable_upper"] = stable_upper
        working["outside_stable_range"] = outside_stable_range
        working["is_significant"] = is_significant
        working["is_flagged"] = is_flagged
        working["is_low_power"] = is_low_power
        working["direction"] = np.where(working["delta_pro_rate"] >= 0.0, "pro_heavy", "con_heavy")
        return working

    @staticmethod
    def _slot_label(slot_start_minute: int) -> str:
        hour = int(slot_start_minute // 60)
        minute = int(slot_start_minute % 60)
        return f"{hour:02d}:{minute:02d}"

    def _build_direction_runs(
        self, time_bucket_profiles: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        required = {"bucket_minutes", "bucket_start", "delta_pro_rate", "n_total"}
        if time_bucket_profiles.empty or not required.issubset(set(time_bucket_profiles.columns)):
            return self._empty_direction_runs(), self._empty_direction_runs_summary()

        working = time_bucket_profiles.copy()
        working["bucket_start"] = pd.to_datetime(working["bucket_start"], errors="coerce")
        working["bucket_minutes"] = pd.to_numeric(working["bucket_minutes"], errors="coerce")
        working["delta_pro_rate"] = pd.to_numeric(
            working["delta_pro_rate"], errors="coerce"
        ).fillna(0.0)
        working["abs_delta_pro_rate"] = pd.to_numeric(
            working.get("abs_delta_pro_rate", working["delta_pro_rate"].abs()),
            errors="coerce",
        ).fillna(0.0)
        working["n_total"] = pd.to_numeric(working["n_total"], errors="coerce").fillna(0.0)
        if "is_low_power" in working.columns:
            working["is_low_power"] = working["is_low_power"].astype(bool)
        else:
            working["is_low_power"] = False
        if "is_flagged" in working.columns:
            working["is_flagged"] = working["is_flagged"].astype(bool)
        else:
            working["is_flagged"] = False
        working = working.dropna(subset=["bucket_start", "bucket_minutes"]).copy()
        if working.empty:
            return self._empty_direction_runs(), self._empty_direction_runs_summary()

        working["run_direction"] = np.where(
            working["abs_delta_pro_rate"] <= 1e-9,
            "neutral",
            np.where(working["delta_pro_rate"] >= 0.0, "pro_heavy", "con_heavy"),
        )

        run_frames: list[pd.DataFrame] = []
        summary_rows: list[dict[str, float]] = []
        for bucket_minutes in sorted(
            working["bucket_minutes"].dropna().astype(int).unique().tolist()
        ):
            bucket_frame = (
                working[working["bucket_minutes"] == int(bucket_minutes)]
                .sort_values("bucket_start")
                .reset_index(drop=True)
            )
            if bucket_frame.empty:
                continue

            bucket_frame["run_group"] = (
                bucket_frame["run_direction"] != bucket_frame["run_direction"].shift(1)
            ).cumsum()
            runs = (
                bucket_frame.groupby("run_group", dropna=False)
                .agg(
                    run_direction=("run_direction", "first"),
                    start_bucket=("bucket_start", "min"),
                    end_bucket=("bucket_start", "max"),
                    run_length_buckets=("bucket_start", "size"),
                    total_n=("n_total", "sum"),
                    mean_abs_delta_pro_rate=("abs_delta_pro_rate", "mean"),
                    max_abs_delta_pro_rate=("abs_delta_pro_rate", "max"),
                    n_flagged_buckets=("is_flagged", "sum"),
                    n_low_power_buckets=("is_low_power", "sum"),
                )
                .reset_index(drop=True)
            )
            runs = runs[runs["run_direction"] != "neutral"].copy()
            if runs.empty:
                continue

            runs["bucket_minutes"] = int(bucket_minutes)
            runs["run_id"] = [
                f"{int(bucket_minutes)}m-r{index + 1}" for index in range(len(runs))
            ]
            runs["support_n"] = runs["total_n"]
            runs["flagged_ratio"] = (
                pd.to_numeric(runs["n_flagged_buckets"], errors="coerce").fillna(0.0)
                / pd.to_numeric(runs["run_length_buckets"], errors="coerce").replace(0, np.nan)
            ).fillna(0.0)
            runs["low_power_ratio"] = (
                pd.to_numeric(runs["n_low_power_buckets"], errors="coerce").fillna(0.0)
                / pd.to_numeric(runs["run_length_buckets"], errors="coerce").replace(0, np.nan)
            ).fillna(0.0)
            runs["is_long_run"] = (
                pd.to_numeric(runs["run_length_buckets"], errors="coerce").fillna(0).astype(int)
                >= int(self.LONG_DIRECTION_RUN_MIN_BUCKETS)
            )
            run_frames.append(runs)

            summary_rows.append(
                {
                    "bucket_minutes": int(bucket_minutes),
                    "n_runs": int(len(runs)),
                    "n_long_runs": int(runs["is_long_run"].sum()),
                    "max_run_length_buckets": int(runs["run_length_buckets"].max()),
                    "max_run_mean_abs_delta": float(runs["mean_abs_delta_pro_rate"].max()),
                    "max_run_total_n": int(runs["total_n"].max()),
                    "pro_heavy_run_ratio": float((runs["run_direction"] == "pro_heavy").mean()),
                    "flagged_run_ratio": float((runs["n_flagged_buckets"] > 0).mean()),
                }
            )

        if not run_frames:
            return self._empty_direction_runs(), self._empty_direction_runs_summary()

        direction_runs = (
            pd.concat(run_frames, ignore_index=True)
            .sort_values(["bucket_minutes", "start_bucket"])
            .reset_index(drop=True)
        )
        direction_runs_summary = (
            pd.DataFrame(summary_rows).sort_values("bucket_minutes").reset_index(drop=True)
            if summary_rows
            else self._empty_direction_runs_summary()
        )
        return direction_runs, direction_runs_summary

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        counts = features.get("counts_per_minute", pd.DataFrame())
        if counts.empty:
            empty = self._empty_windows()
            empty_bucket_profiles = self._empty_bucket_profiles()
            empty_time_of_day_profiles = self._empty_time_of_day_profiles()
            empty_day_bucket_profiles = self._empty_day_bucket_profiles()
            empty_direction_runs = self._empty_direction_runs()
            empty_direction_runs_summary = self._empty_direction_runs_summary()
            return DetectorResult(
                detector=self.name,
                summary={
                    "n_tests": 0,
                    "n_significant_windows": 0,
                    "baseline_pro_rate": 0.0,
                    "calibration_enabled": False,
                    "n_time_bucket_flags": 0,
                    "n_time_of_day_flags": 0,
                    "n_day_slot_outliers": 0,
                    "n_direction_runs": 0,
                    "n_long_direction_runs": 0,
                    "max_direction_run_length": 0,
                    "max_direction_run_mean_abs_delta": 0.0,
                },
                tables={
                    "swing_window_tests": empty,
                    "swing_significant_windows": empty,
                    "pro_rate_by_hour": pd.DataFrame(),
                    "swing_null_distribution": pd.DataFrame(),
                    "time_bucket_profiles": empty_bucket_profiles,
                    "time_of_day_bucket_profiles": empty_time_of_day_profiles,
                    "day_bucket_profiles": empty_day_bucket_profiles,
                    "direction_runs": empty_direction_runs,
                    "direction_runs_summary": empty_direction_runs_summary,
                },
            )

        total_submissions = float(counts["n_total"].sum())
        if total_submissions <= 0:
            empty = self._empty_windows()
            empty_bucket_profiles = self._empty_bucket_profiles()
            empty_time_of_day_profiles = self._empty_time_of_day_profiles()
            empty_day_bucket_profiles = self._empty_day_bucket_profiles()
            empty_direction_runs = self._empty_direction_runs()
            empty_direction_runs_summary = self._empty_direction_runs_summary()
            return DetectorResult(
                detector=self.name,
                summary={
                    "n_tests": 0,
                    "n_significant_windows": 0,
                    "baseline_pro_rate": 0.0,
                    "calibration_enabled": False,
                    "n_time_bucket_flags": 0,
                    "n_time_of_day_flags": 0,
                    "n_day_slot_outliers": 0,
                    "n_direction_runs": 0,
                    "n_long_direction_runs": 0,
                    "max_direction_run_length": 0,
                    "max_direction_run_mean_abs_delta": 0.0,
                },
                tables={
                    "swing_window_tests": empty,
                    "swing_significant_windows": empty,
                    "pro_rate_by_hour": pd.DataFrame(),
                    "swing_null_distribution": pd.DataFrame(),
                    "time_bucket_profiles": empty_bucket_profiles,
                    "time_of_day_bucket_profiles": empty_time_of_day_profiles,
                    "day_bucket_profiles": empty_day_bucket_profiles,
                    "direction_runs": empty_direction_runs,
                    "direction_runs_summary": empty_direction_runs_summary,
                },
            )

        baseline_pro_rate = float(counts["n_pro"].sum() / total_submissions)
        baseline_pro_rate = float(np.clip(baseline_pro_rate, 1e-6, 1.0 - 1e-6))

        minute_bucket_series = pd.to_datetime(counts["minute_bucket"], errors="coerce")
        minute_bucket = minute_bucket_series.to_numpy()
        minute_hours = minute_bucket_series.dt.hour.fillna(0).astype(int).to_numpy(dtype=int)
        minute_day_of_week = (
            minute_bucket_series.dt.dayofweek.fillna(0).astype(int).to_numpy(dtype=int)
        )

        totals = counts["n_total"].astype(float).to_numpy()
        pros = counts["n_pro"].astype(float).to_numpy()

        test_frames: list[pd.DataFrame] = []
        for window in self.window_minutes:
            if window < 1 or window > len(counts):
                continue

            total_roll = rolling_sum(totals, window)
            pro_roll = rolling_sum(pros, window)

            valid = total_roll >= float(self.min_window_total)
            if not valid.any():
                continue

            sample_total = total_roll[valid]
            sample_pro = pro_roll[valid]
            sample_rate = sample_pro / sample_total
            delta = sample_rate - baseline_pro_rate

            se = np.sqrt((baseline_pro_rate * (1.0 - baseline_pro_rate)) / sample_total)
            z_score = np.where(se > 0, delta / se, 0.0)
            p_values = 2.0 * norm.sf(np.abs(z_score))

            all_indices = np.arange(len(total_roll), dtype=int)
            start_idx = all_indices[valid]
            end_idx = start_idx + window - 1

            test_frames.append(
                pd.DataFrame(
                    {
                        "window_minutes": window,
                        "start_minute": minute_bucket[start_idx],
                        "end_minute": minute_bucket[end_idx],
                        "n_total": sample_total.astype(float),
                        "n_pro": sample_pro.astype(float),
                        "pro_rate": sample_rate.astype(float),
                        "baseline_pro_rate": baseline_pro_rate,
                        "delta_pro_rate": delta.astype(float),
                        "abs_delta_pro_rate": np.abs(delta).astype(float),
                        "z_score": z_score.astype(float),
                        "p_value": p_values.astype(float),
                    }
                )
            )

        if not test_frames:
            empty = self._empty_windows()
            empty_bucket_profiles = self._empty_bucket_profiles()
            empty_time_of_day_profiles = self._empty_time_of_day_profiles()
            empty_day_bucket_profiles = self._empty_day_bucket_profiles()
            empty_direction_runs = self._empty_direction_runs()
            empty_direction_runs_summary = self._empty_direction_runs_summary()
            return DetectorResult(
                detector=self.name,
                summary={
                    "n_tests": 0,
                    "n_significant_windows": 0,
                    "baseline_pro_rate": baseline_pro_rate,
                    "calibration_enabled": False,
                    "n_time_bucket_flags": 0,
                    "n_time_of_day_flags": 0,
                    "n_day_slot_outliers": 0,
                    "n_direction_runs": 0,
                    "n_long_direction_runs": 0,
                    "max_direction_run_length": 0,
                    "max_direction_run_mean_abs_delta": 0.0,
                },
                tables={
                    "swing_window_tests": empty,
                    "swing_significant_windows": empty,
                    "pro_rate_by_hour": pd.DataFrame(),
                    "swing_null_distribution": pd.DataFrame(),
                    "time_bucket_profiles": empty_bucket_profiles,
                    "time_of_day_bucket_profiles": empty_time_of_day_profiles,
                    "day_bucket_profiles": empty_day_bucket_profiles,
                    "direction_runs": empty_direction_runs,
                    "direction_runs_summary": empty_direction_runs_summary,
                },
            )

        tests = pd.concat(test_frames, ignore_index=True)
        tests["pro_rate_wilson_low"], tests["pro_rate_wilson_high"] = wilson_interval(
            successes=tests["n_pro"],
            totals=tests["n_total"],
        )
        tests["pro_rate_wilson_half_width"] = wilson_half_width(
            successes=tests["n_pro"],
            totals=tests["n_total"],
        )
        tests["is_low_power"] = low_power_mask(
            totals=tests["n_total"],
            min_total=self.low_power_min_total,
        )
        rejected, q_values = benjamini_hochberg(tests["p_value"].to_numpy(), alpha=self.fdr_alpha)
        tests["q_value"] = q_values.astype(float)
        tests["is_significant_parametric_fdr"] = rejected.astype(bool)
        tests["direction"] = np.where(tests["delta_pro_rate"] >= 0.0, "pro_heavy", "con_heavy")

        calibration_active = self.calibration_enabled and self.calibration_iterations > 0
        tests["permutation_p_value"] = np.nan
        tests["permutation_q_value"] = np.nan
        tests["is_significant_permutation_raw"] = True
        tests["is_significant_permutation_fdr"] = True
        tests["is_significant_permutation"] = True
        tests["is_calibration_supported"] = True
        null_distribution_frames: list[pd.DataFrame] = []

        if calibration_active:
            rng = np.random.default_rng(self.calibration_seed)
            tests["is_significant_permutation_raw"] = False
            tests["is_significant_permutation_fdr"] = False
            tests["is_significant_permutation"] = False
            tests["is_calibration_supported"] = False
            calibration_alpha = max(
                self.fdr_alpha,
                self.calibration_support_alpha,
                1.0 / (self.calibration_iterations + 1.0),
            )

            hourly_probabilities = (
                counts.assign(hour=minute_hours)
                .groupby("hour", dropna=False)
                .agg(n_pro=("n_pro", "sum"), n_total=("n_total", "sum"))
                .assign(
                    p=lambda frame: np.where(
                        frame["n_total"] > 0, frame["n_pro"] / frame["n_total"], baseline_pro_rate
                    )
                )
                .reindex(range(24), fill_value=baseline_pro_rate)["p"]
                .to_numpy(dtype=float)
            )
            minute_probabilities = hourly_probabilities[np.clip(minute_hours, 0, 23)]
            day_hour_probabilities = (
                counts.assign(day_of_week=minute_day_of_week, hour=minute_hours)
                .groupby(["day_of_week", "hour"], dropna=False)
                .agg(n_pro=("n_pro", "sum"), n_total=("n_total", "sum"))
                .assign(
                    p=lambda frame: np.where(
                        frame["n_total"] > 0,
                        frame["n_pro"] / frame["n_total"],
                        baseline_pro_rate,
                    )
                )
                .reindex(
                    pd.MultiIndex.from_product(
                        [range(7), range(24)], names=["day_of_week", "hour"]
                    ),
                    fill_value=baseline_pro_rate,
                )["p"]
                .to_numpy(dtype=float)
            )
            day_hour_index = (np.clip(minute_day_of_week, 0, 6) * 24) + np.clip(minute_hours, 0, 23)
            minute_probabilities_day_hour = day_hour_probabilities[
                np.clip(day_hour_index, 0, (7 * 24) - 1)
            ]

            for window in sorted(tests["window_minutes"].unique()):
                window_int = int(window)
                window_mask = tests["window_minutes"] == window
                observed_abs_delta = tests.loc[window_mask, "abs_delta_pro_rate"].to_numpy(
                    dtype=float
                )

                if self.calibration_mode == "hour_of_day":
                    null_max, expected_rate_series = simulate_binomial_max_abs_delta_hourly(
                        totals_per_minute=totals,
                        hourly_probabilities=minute_probabilities,
                        window=window_int,
                        min_window_total=self.min_window_total,
                        iterations=self.calibration_iterations,
                        rng=rng,
                    )
                    total_roll = rolling_sum(totals, window_int)
                    valid = total_roll >= float(self.min_window_total)
                    observed_pro_rate = tests.loc[window_mask, "pro_rate"].to_numpy(dtype=float)
                    observed_abs_delta = np.abs(observed_pro_rate - expected_rate_series[valid])
                elif self.calibration_mode == "day_of_week_hour":
                    null_max, expected_rate_series = (
                        simulate_binomial_max_abs_delta_probability_series(
                            totals_per_minute=totals,
                            probabilities_per_minute=minute_probabilities_day_hour,
                            window=window_int,
                            min_window_total=self.min_window_total,
                            iterations=self.calibration_iterations,
                            rng=rng,
                        )
                    )
                    total_roll = rolling_sum(totals, window_int)
                    valid = total_roll >= float(self.min_window_total)
                    observed_pro_rate = tests.loc[window_mask, "pro_rate"].to_numpy(dtype=float)
                    observed_abs_delta = np.abs(observed_pro_rate - expected_rate_series[valid])
                else:
                    null_max = simulate_binomial_max_abs_delta(
                        totals_per_minute=totals,
                        baseline_probability=baseline_pro_rate,
                        window=window_int,
                        min_window_total=self.min_window_total,
                        iterations=self.calibration_iterations,
                        rng=rng,
                    )

                empirical_p = empirical_tail_p_values(observed_abs_delta, null_max)
                tests.loc[window_mask, "permutation_p_value"] = empirical_p
                tests.loc[window_mask, "is_significant_permutation_raw"] = (
                    empirical_p <= self.fdr_alpha
                )
                tests.loc[window_mask, "is_calibration_supported"] = (
                    empirical_p <= calibration_alpha
                )

                null_distribution_frames.append(
                    pd.DataFrame(
                        {
                            "window_minutes": window_int,
                            "iteration": np.arange(1, len(null_max) + 1, dtype=int),
                            "max_abs_delta_pro_rate": null_max.astype(float),
                        }
                    )
                )

            permutation_mask = tests["permutation_p_value"].notna()
            if permutation_mask.any():
                perm_reject, perm_q = benjamini_hochberg(
                    tests.loc[permutation_mask, "permutation_p_value"].to_numpy(dtype=float),
                    alpha=self.fdr_alpha,
                )
                tests.loc[permutation_mask, "permutation_q_value"] = perm_q
                tests.loc[permutation_mask, "is_significant_permutation_fdr"] = perm_reject
                tests["is_significant_permutation"] = tests["is_significant_permutation_fdr"]

        significance_policy_effective = (
            self.significance_policy if calibration_active else "parametric_fdr"
        )
        if significance_policy_effective == "permutation_fdr":
            tests["is_significant"] = tests["is_significant_permutation_fdr"]
        elif significance_policy_effective == "either_fdr":
            tests["is_significant"] = (
                tests["is_significant_parametric_fdr"] | tests["is_significant_permutation_fdr"]
            )
        else:
            tests["is_significant"] = tests["is_significant_parametric_fdr"]

        tests = tests.sort_values(
            ["is_significant", "q_value", "abs_delta_pro_rate"],
            ascending=[False, True, False],
        )
        significant = tests[tests["is_significant"]].copy()

        counts_per_hour = features.get("counts_per_hour", pd.DataFrame()).copy()
        if not counts_per_hour.empty:
            counts_per_hour["pro_rate"] = (
                counts_per_hour["n_pro"] / counts_per_hour["n_total"]
            ).where(counts_per_hour["n_total"] > 0)

        counts_for_profiles = counts.copy()
        counts_for_profiles["minute_bucket"] = pd.to_datetime(
            counts_for_profiles["minute_bucket"],
            errors="coerce",
        )
        counts_for_profiles = counts_for_profiles.dropna(subset=["minute_bucket"]).copy()
        counts_for_profiles["date"] = counts_for_profiles["minute_bucket"].dt.date.astype(str)
        counts_for_profiles["minute_of_day"] = (
            counts_for_profiles["minute_bucket"].dt.hour * 60
            + counts_for_profiles["minute_bucket"].dt.minute
        ).astype(int)

        time_bucket_frames: list[pd.DataFrame] = []
        time_of_day_frames: list[pd.DataFrame] = []
        day_bucket_frames: list[pd.DataFrame] = []

        for bucket_minutes in self.profile_bucket_minutes:
            bucket_label = f"{bucket_minutes}min"
            profile_working = counts_for_profiles.copy()
            profile_working["bucket_start"] = profile_working["minute_bucket"].dt.floor(
                bucket_label
            )

            bucket_timeline = (
                profile_working.groupby("bucket_start", dropna=True)
                .agg(n_total=("n_total", "sum"), n_pro=("n_pro", "sum"))
                .reset_index()
                .sort_values("bucket_start")
            )
            if not bucket_timeline.empty:
                bucket_timeline["bucket_minutes"] = int(bucket_minutes)
                bucket_timeline = self._enrich_rate_table(
                    bucket_timeline, baseline_pro_rate=baseline_pro_rate
                )
                time_bucket_frames.append(bucket_timeline)

            profile_working["slot_index"] = (
                profile_working["minute_of_day"] // int(bucket_minutes)
            ).astype(int)
            profile_working["slot_start_minute"] = profile_working["slot_index"] * int(
                bucket_minutes
            )

            slot_profile = (
                profile_working.groupby(["slot_index", "slot_start_minute"], dropna=True)
                .agg(n_total=("n_total", "sum"), n_pro=("n_pro", "sum"))
                .reset_index()
                .sort_values("slot_start_minute")
            )
            if slot_profile.empty:
                continue

            slot_profile["bucket_minutes"] = int(bucket_minutes)
            slot_profile["slot_start_label"] = slot_profile["slot_start_minute"].map(
                self._slot_label
            )
            slot_profile = self._enrich_rate_table(
                slot_profile, baseline_pro_rate=baseline_pro_rate
            )
            time_of_day_frames.append(slot_profile)

            expected_slot_rate_map = dict(
                zip(
                    slot_profile["slot_index"].astype(int).tolist(),
                    slot_profile["pro_rate"].astype(float).tolist(),
                )
            )
            day_slot_profile = (
                profile_working.groupby(["date", "slot_index", "slot_start_minute"], dropna=True)
                .agg(n_total=("n_total", "sum"), n_pro=("n_pro", "sum"))
                .reset_index()
                .sort_values(["date", "slot_start_minute"])
            )
            day_slot_profile["bucket_minutes"] = int(bucket_minutes)
            day_slot_profile["slot_start_label"] = day_slot_profile["slot_start_minute"].map(
                self._slot_label
            )
            day_slot_profile = self._enrich_rate_table(
                day_slot_profile,
                baseline_pro_rate=baseline_pro_rate,
                min_half_width=0.07,
            )
            day_slot_profile["expected_slot_pro_rate"] = day_slot_profile["slot_index"].map(
                expected_slot_rate_map
            )
            slot_delta = day_slot_profile["pro_rate"] - day_slot_profile["expected_slot_pro_rate"]
            day_slot_profile["delta_from_slot_pro_rate"] = slot_delta
            day_slot_profile["abs_delta_from_slot_pro_rate"] = slot_delta.abs()

            slot_expected = (
                day_slot_profile["expected_slot_pro_rate"]
                .astype(float)
                .clip(lower=1e-6, upper=1.0 - 1e-6)
            )
            slot_totals = day_slot_profile["n_total"].astype(float)
            slot_se = np.sqrt(
                (slot_expected * (1.0 - slot_expected)) / slot_totals.where(slot_totals > 0)
            )
            slot_z = (slot_delta / slot_se).replace([np.inf, -np.inf], np.nan).fillna(0.0)
            slot_p = (2.0 * norm.sf(np.abs(slot_z))).astype(float)
            slot_reject, slot_q = benjamini_hochberg(slot_p.astype(float), alpha=self.fdr_alpha)
            slot_band_half = float(max(0.10, slot_profile["stable_half_width"].iloc[0]))

            day_slot_profile["slot_z_score"] = slot_z.astype(float)
            day_slot_profile["slot_p_value"] = slot_p
            day_slot_profile["slot_q_value"] = slot_q
            day_slot_profile["is_slot_significant"] = slot_reject.astype(bool)
            day_slot_profile["is_slot_outlier"] = (
                day_slot_profile["is_slot_significant"]
                & (day_slot_profile["abs_delta_from_slot_pro_rate"] >= slot_band_half)
                & (~day_slot_profile["is_low_power"])
            )
            day_bucket_frames.append(day_slot_profile)

        null_distribution = (
            pd.concat(null_distribution_frames, ignore_index=True)
            if null_distribution_frames
            else pd.DataFrame(columns=["window_minutes", "iteration", "max_abs_delta_pro_rate"])
        )

        time_bucket_profiles = (
            pd.concat(time_bucket_frames, ignore_index=True)
            if time_bucket_frames
            else self._empty_bucket_profiles()
        )
        time_of_day_bucket_profiles = (
            pd.concat(time_of_day_frames, ignore_index=True)
            if time_of_day_frames
            else self._empty_time_of_day_profiles()
        )
        day_bucket_profiles = (
            pd.concat(day_bucket_frames, ignore_index=True)
            if day_bucket_frames
            else self._empty_day_bucket_profiles()
        )
        direction_runs, direction_runs_summary = self._build_direction_runs(time_bucket_profiles)

        summary = {
            "n_tests": int(len(tests)),
            "n_significant_windows": int(len(significant)),
            "n_parametric_fdr_significant_windows": int(
                tests["is_significant_parametric_fdr"].sum()
            ),
            "n_permutation_raw_significant_windows": int(
                tests["is_significant_permutation_raw"].sum()
            )
            if calibration_active
            else int(len(tests)),
            "n_permutation_fdr_significant_windows": int(
                tests["is_significant_permutation_fdr"].sum()
            )
            if calibration_active
            else int(len(tests)),
            "n_permutation_significant_windows": int(tests["is_significant_permutation"].sum())
            if calibration_active
            else int(len(tests)),
            "n_calibration_supported_windows": int(tests["is_calibration_supported"].sum())
            if calibration_active
            else int(len(tests)),
            "baseline_pro_rate": baseline_pro_rate,
            "min_q_value": float(tests["q_value"].min()) if not tests.empty else 1.0,
            "max_abs_delta_pro_rate": float(tests["abs_delta_pro_rate"].max())
            if not tests.empty
            else 0.0,
            "calibration_enabled": calibration_active,
            "calibration_mode": self.calibration_mode if calibration_active else "disabled",
            "calibration_iterations": int(self.calibration_iterations if calibration_active else 0),
            "low_power_min_total": int(self.low_power_min_total),
            "significance_policy_requested": self.significance_policy,
            "significance_policy_effective": significance_policy_effective,
            "n_time_bucket_flags": int(time_bucket_profiles["is_flagged"].sum())
            if not time_bucket_profiles.empty
            else 0,
            "n_time_of_day_flags": (
                int(time_of_day_bucket_profiles["is_flagged"].sum())
                if not time_of_day_bucket_profiles.empty
                else 0
            ),
            "n_day_slot_outliers": (
                int(day_bucket_profiles["is_slot_outlier"].sum())
                if not day_bucket_profiles.empty
                else 0
            ),
            "n_low_power_windows": int(tests["is_low_power"].sum()) if not tests.empty else 0,
            "n_low_power_time_buckets": (
                int(time_bucket_profiles["is_low_power"].sum())
                if not time_bucket_profiles.empty
                else 0
            ),
            "n_low_power_time_of_day_slots": (
                int(time_of_day_bucket_profiles["is_low_power"].sum())
                if not time_of_day_bucket_profiles.empty
                else 0
            ),
            "n_low_power_day_slots": (
                int(day_bucket_profiles["is_low_power"].sum())
                if not day_bucket_profiles.empty
                else 0
            ),
            "max_abs_time_of_day_delta": (
                float(time_of_day_bucket_profiles["abs_delta_pro_rate"].max())
                if not time_of_day_bucket_profiles.empty
                else 0.0
            ),
            "max_abs_day_slot_delta_from_slot": (
                float(day_bucket_profiles["abs_delta_from_slot_pro_rate"].max())
                if not day_bucket_profiles.empty
                else 0.0
            ),
            "n_direction_runs": int(len(direction_runs)),
            "n_long_direction_runs": int(direction_runs["is_long_run"].sum())
            if not direction_runs.empty
            else 0,
            "max_direction_run_length": int(direction_runs["run_length_buckets"].max())
            if not direction_runs.empty
            else 0,
            "max_direction_run_mean_abs_delta": (
                float(direction_runs["mean_abs_delta_pro_rate"].max())
                if not direction_runs.empty
                else 0.0
            ),
        }

        return DetectorResult(
            detector=self.name,
            summary=summary,
            tables={
                "swing_window_tests": tests,
                "swing_significant_windows": significant,
                "pro_rate_by_hour": counts_per_hour,
                "swing_null_distribution": null_distribution,
                "time_bucket_profiles": time_bucket_profiles,
                "time_of_day_bucket_profiles": time_of_day_bucket_profiles,
                "day_bucket_profiles": day_bucket_profiles,
                "direction_runs": direction_runs,
                "direction_runs_summary": direction_runs_summary,
            },
        )
