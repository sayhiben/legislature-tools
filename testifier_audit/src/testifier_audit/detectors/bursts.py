from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import poisson

from testifier_audit.detectors.base import Detector, DetectorResult
from testifier_audit.detectors.stats import (
    benjamini_hochberg,
    empirical_tail_p_values,
    rolling_sum,
    simulate_poisson_max_rolling_sums,
    simulate_poisson_max_rolling_sums_hourly,
    simulate_poisson_max_rolling_sums_stratified,
)
from testifier_audit.proportion_stats import (
    DEFAULT_LOW_POWER_MIN_TOTAL,
    low_power_mask,
    wilson_interval,
)


class BurstsDetector(Detector):
    name = "bursts"

    def __init__(
        self,
        window_minutes: list[int],
        fdr_alpha: float,
        calibration_enabled: bool = False,
        calibration_mode: str = "global",
        significance_policy: str = "parametric_fdr",
        calibration_iterations: int = 0,
        calibration_seed: int = 42,
        calibration_support_alpha: float = 0.1,
        low_power_min_total: int = DEFAULT_LOW_POWER_MIN_TOTAL,
    ) -> None:
        self.window_minutes = sorted(set(window_minutes))
        self.fdr_alpha = fdr_alpha
        self.calibration_enabled = calibration_enabled
        self.calibration_mode = calibration_mode
        self.significance_policy = significance_policy
        self.calibration_iterations = calibration_iterations
        self.calibration_seed = calibration_seed
        self.calibration_support_alpha = calibration_support_alpha
        self.low_power_min_total = max(1, int(low_power_min_total))

    @staticmethod
    def _empty_windows() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "window_minutes",
                "start_minute",
                "end_minute",
                "observed_count",
                "expected_count",
                "rate_ratio",
                "n_pro",
                "n_con",
                "pro_rate",
                "baseline_pro_rate",
                "delta_pro_rate",
                "abs_delta_pro_rate",
                "pro_rate_wilson_low",
                "pro_rate_wilson_high",
                "is_low_power",
                "p_value",
                "q_value",
                "is_significant_poisson_fdr",
                "permutation_p_value",
                "permutation_q_value",
                "is_significant_permutation_raw",
                "is_significant_permutation_fdr",
                "is_significant_permutation",
                "is_calibration_supported",
                "is_significant",
            ]
        )

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        counts = features.get("counts_per_minute", pd.DataFrame())
        if counts.empty:
            empty = self._empty_windows()
            return DetectorResult(
                detector=self.name,
                summary={
                    "n_tests": 0,
                    "n_significant_windows": 0,
                    "baseline_rate_per_minute": 0.0,
                    "calibration_enabled": False,
                },
                tables={
                    "burst_window_tests": empty,
                    "burst_significant_windows": empty,
                    "burst_null_distribution": pd.DataFrame(),
                },
            )

        series = counts["n_total"].astype(float).to_numpy()
        minute_bucket_series = pd.to_datetime(counts["minute_bucket"], errors="coerce")
        minute_bucket = minute_bucket_series.to_numpy()
        baseline_rate = float(np.mean(series)) if len(series) else 0.0
        n_pro_series = (
            pd.to_numeric(counts.get("n_pro", 0.0), errors="coerce")
            .fillna(0.0)
            .to_numpy(dtype=float)
        )
        if "n_con" in counts.columns:
            n_con_series = pd.to_numeric(counts["n_con"], errors="coerce").fillna(0.0).to_numpy(
                dtype=float
            )
        else:
            n_con_series = np.maximum(series - n_pro_series, 0.0)
        total_pro = float(np.sum(n_pro_series))
        total_records = float(np.sum(series))
        baseline_pro_rate = (
            float(total_pro / total_records) if total_records > 0.0 else 0.0
        )

        minute_hours = minute_bucket_series.dt.hour.fillna(0).astype(int)
        minute_day_of_week = minute_bucket_series.dt.dayofweek.fillna(0).astype(int)
        hourly_rates = (
            counts.assign(hour=minute_hours)
            .groupby("hour", dropna=False)["n_total"]
            .mean()
            .reindex(range(24), fill_value=baseline_rate)
            .to_numpy(dtype=float)
        )
        day_hour_rates = (
            counts.assign(day_of_week=minute_day_of_week, hour=minute_hours)
            .groupby(["day_of_week", "hour"], dropna=False)["n_total"]
            .mean()
            .reindex(
                pd.MultiIndex.from_product([range(7), range(24)], names=["day_of_week", "hour"]),
                fill_value=baseline_rate,
            )
            .to_numpy(dtype=float)
        )

        test_frames: list[pd.DataFrame] = []
        for window in self.window_minutes:
            if window < 1 or window > len(series):
                continue

            rolling_counts = rolling_sum(series, window)
            rolling_pro = rolling_sum(n_pro_series, window)
            rolling_con = rolling_sum(n_con_series, window)
            expected_count = baseline_rate * window
            if expected_count > 0:
                p_values = poisson.sf(rolling_counts - 1, expected_count)
                rate_ratio = rolling_counts / expected_count
            else:
                p_values = np.where(rolling_counts > 0, 0.0, 1.0)
                rate_ratio = np.where(rolling_counts > 0, np.inf, 0.0)
            pro_rate = np.divide(
                rolling_pro,
                rolling_counts,
                out=np.full_like(rolling_pro, np.nan, dtype=float),
                where=rolling_counts > 0.0,
            )
            delta_pro_rate = pro_rate - baseline_pro_rate
            abs_delta_pro_rate = np.abs(delta_pro_rate)
            pro_rate_wilson_low, pro_rate_wilson_high = wilson_interval(
                successes=rolling_pro,
                totals=rolling_counts,
            )
            is_low_power = low_power_mask(
                totals=rolling_counts,
                min_total=self.low_power_min_total,
            )

            start_idx = np.arange(len(rolling_counts), dtype=int)
            end_idx = start_idx + window - 1

            test_frames.append(
                pd.DataFrame(
                    {
                        "window_minutes": window,
                        "start_minute": minute_bucket[start_idx],
                        "end_minute": minute_bucket[end_idx],
                        "observed_count": rolling_counts.astype(float),
                        "expected_count": float(expected_count),
                        "rate_ratio": rate_ratio.astype(float),
                        "n_pro": rolling_pro.astype(float),
                        "n_con": rolling_con.astype(float),
                        "pro_rate": pro_rate.astype(float),
                        "baseline_pro_rate": float(baseline_pro_rate),
                        "delta_pro_rate": delta_pro_rate.astype(float),
                        "abs_delta_pro_rate": abs_delta_pro_rate.astype(float),
                        "pro_rate_wilson_low": pro_rate_wilson_low,
                        "pro_rate_wilson_high": pro_rate_wilson_high,
                        "is_low_power": is_low_power.astype(bool),
                        "p_value": p_values.astype(float),
                    }
                )
            )

        if not test_frames:
            empty = self._empty_windows()
            return DetectorResult(
                detector=self.name,
                summary={
                    "n_tests": 0,
                    "n_significant_windows": 0,
                    "baseline_rate_per_minute": baseline_rate,
                    "calibration_enabled": False,
                },
                tables={
                    "burst_window_tests": empty,
                    "burst_significant_windows": empty,
                    "burst_null_distribution": pd.DataFrame(),
                },
            )

        tests = pd.concat(test_frames, ignore_index=True)
        rejected, q_values = benjamini_hochberg(tests["p_value"].to_numpy(), alpha=self.fdr_alpha)
        tests["q_value"] = q_values.astype(float)
        tests["is_significant_poisson_fdr"] = rejected.astype(bool)

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

            hour_index = minute_hours.to_numpy(dtype=int)
            day_index = minute_day_of_week.to_numpy(dtype=int)
            for window in sorted(tests["window_minutes"].unique()):
                window_int = int(window)
                window_mask = tests["window_minutes"] == window
                observed = tests.loc[window_mask, "observed_count"].to_numpy(dtype=float)

                if self.calibration_mode == "hour_of_day":
                    null_maxima = simulate_poisson_max_rolling_sums_hourly(
                        hour_index=hour_index,
                        hourly_rates=hourly_rates,
                        window=window_int,
                        iterations=self.calibration_iterations,
                        rng=rng,
                    )
                elif self.calibration_mode == "day_of_week_hour":
                    day_hour_index = (np.clip(day_index, 0, 6) * 24) + np.clip(hour_index, 0, 23)
                    lambdas = day_hour_rates[np.clip(day_hour_index, 0, (7 * 24) - 1)]
                    null_maxima = simulate_poisson_max_rolling_sums_stratified(
                        lambdas_per_minute=lambdas,
                        window=window_int,
                        iterations=self.calibration_iterations,
                        rng=rng,
                    )
                else:
                    null_maxima = simulate_poisson_max_rolling_sums(
                        n_minutes=len(series),
                        rate_per_minute=baseline_rate,
                        window=window_int,
                        iterations=self.calibration_iterations,
                        rng=rng,
                    )

                empirical_p = empirical_tail_p_values(observed, null_maxima)
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
                            "iteration": np.arange(1, len(null_maxima) + 1, dtype=int),
                            "max_window_count": null_maxima.astype(float),
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
                tests["is_significant_poisson_fdr"] | tests["is_significant_permutation_fdr"]
            )
        else:
            tests["is_significant"] = tests["is_significant_poisson_fdr"]

        tests = tests.sort_values(
            ["is_significant", "q_value", "rate_ratio"], ascending=[False, True, False]
        )
        significant = tests[tests["is_significant"]].copy()
        null_distribution = (
            pd.concat(null_distribution_frames, ignore_index=True)
            if null_distribution_frames
            else pd.DataFrame(columns=["window_minutes", "iteration", "max_window_count"])
        )

        summary = {
            "n_tests": int(len(tests)),
            "n_significant_windows": int(len(significant)),
            "n_poisson_fdr_significant_windows": int(tests["is_significant_poisson_fdr"].sum()),
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
            "baseline_rate_per_minute": baseline_rate,
            "max_observed_window_count": float(tests["observed_count"].max())
            if not tests.empty
            else 0.0,
            "max_abs_delta_pro_rate": float(tests["abs_delta_pro_rate"].max())
            if not tests.empty
            else 0.0,
            "max_significant_abs_delta_pro_rate": float(significant["abs_delta_pro_rate"].max())
            if not significant.empty
            else 0.0,
            "n_significant_composition_shifts": int(
                (
                    significant["abs_delta_pro_rate"]
                    >= max(0.10, float(significant["abs_delta_pro_rate"].median()))
                ).sum()
            )
            if not significant.empty
            else 0,
            "min_q_value": float(tests["q_value"].min()) if not tests.empty else 1.0,
            "calibration_enabled": calibration_active,
            "calibration_mode": self.calibration_mode if calibration_active else "disabled",
            "calibration_iterations": int(self.calibration_iterations if calibration_active else 0),
            "significance_policy_requested": self.significance_policy,
            "significance_policy_effective": significance_policy_effective,
        }

        return DetectorResult(
            detector=self.name,
            summary=summary,
            tables={
                "burst_window_tests": tests,
                "burst_significant_windows": significant,
                "burst_null_distribution": null_distribution,
            },
        )
