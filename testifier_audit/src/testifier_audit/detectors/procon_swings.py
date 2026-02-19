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


class ProConSwingsDetector(Detector):
    name = "procon_swings"

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
                "baseline_pro_rate",
                "delta_pro_rate",
                "abs_delta_pro_rate",
                "z_score",
                "p_value",
                "q_value",
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

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        counts = features.get("counts_per_minute", pd.DataFrame())
        if counts.empty:
            empty = self._empty_windows()
            return DetectorResult(
                detector=self.name,
                summary={
                    "n_tests": 0,
                    "n_significant_windows": 0,
                    "baseline_pro_rate": 0.0,
                    "calibration_enabled": False,
                },
                tables={
                    "swing_window_tests": empty,
                    "swing_significant_windows": empty,
                    "pro_rate_by_hour": pd.DataFrame(),
                    "swing_null_distribution": pd.DataFrame(),
                },
            )

        total_submissions = float(counts["n_total"].sum())
        if total_submissions <= 0:
            empty = self._empty_windows()
            return DetectorResult(
                detector=self.name,
                summary={
                    "n_tests": 0,
                    "n_significant_windows": 0,
                    "baseline_pro_rate": 0.0,
                    "calibration_enabled": False,
                },
                tables={
                    "swing_window_tests": empty,
                    "swing_significant_windows": empty,
                    "pro_rate_by_hour": pd.DataFrame(),
                    "swing_null_distribution": pd.DataFrame(),
                },
            )

        baseline_pro_rate = float(counts["n_pro"].sum() / total_submissions)
        baseline_pro_rate = float(np.clip(baseline_pro_rate, 1e-6, 1.0 - 1e-6))

        minute_bucket_series = pd.to_datetime(counts["minute_bucket"], errors="coerce")
        minute_bucket = minute_bucket_series.to_numpy()
        minute_hours = minute_bucket_series.dt.hour.fillna(0).astype(int).to_numpy(dtype=int)
        minute_day_of_week = minute_bucket_series.dt.dayofweek.fillna(0).astype(int).to_numpy(dtype=int)

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
            return DetectorResult(
                detector=self.name,
                summary={
                    "n_tests": 0,
                    "n_significant_windows": 0,
                    "baseline_pro_rate": baseline_pro_rate,
                    "calibration_enabled": False,
                },
                tables={
                    "swing_window_tests": empty,
                    "swing_significant_windows": empty,
                    "pro_rate_by_hour": pd.DataFrame(),
                    "swing_null_distribution": pd.DataFrame(),
                },
            )

        tests = pd.concat(test_frames, ignore_index=True)
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
                .assign(p=lambda frame: np.where(frame["n_total"] > 0, frame["n_pro"] / frame["n_total"], baseline_pro_rate))
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
                    pd.MultiIndex.from_product([range(7), range(24)], names=["day_of_week", "hour"]),
                    fill_value=baseline_pro_rate,
                )["p"]
                .to_numpy(dtype=float)
            )
            day_hour_index = (np.clip(minute_day_of_week, 0, 6) * 24) + np.clip(minute_hours, 0, 23)
            minute_probabilities_day_hour = day_hour_probabilities[np.clip(day_hour_index, 0, (7 * 24) - 1)]

            for window in sorted(tests["window_minutes"].unique()):
                window_int = int(window)
                window_mask = tests["window_minutes"] == window
                observed_abs_delta = tests.loc[window_mask, "abs_delta_pro_rate"].to_numpy(dtype=float)

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
                    null_max, expected_rate_series = simulate_binomial_max_abs_delta_probability_series(
                        totals_per_minute=totals,
                        probabilities_per_minute=minute_probabilities_day_hour,
                        window=window_int,
                        min_window_total=self.min_window_total,
                        iterations=self.calibration_iterations,
                        rng=rng,
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
                tests.loc[window_mask, "is_significant_permutation_raw"] = empirical_p <= self.fdr_alpha
                tests.loc[window_mask, "is_calibration_supported"] = empirical_p <= calibration_alpha

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

        null_distribution = (
            pd.concat(null_distribution_frames, ignore_index=True)
            if null_distribution_frames
            else pd.DataFrame(columns=["window_minutes", "iteration", "max_abs_delta_pro_rate"])
        )

        summary = {
            "n_tests": int(len(tests)),
            "n_significant_windows": int(len(significant)),
            "n_parametric_fdr_significant_windows": int(tests["is_significant_parametric_fdr"].sum()),
            "n_permutation_raw_significant_windows": int(tests["is_significant_permutation_raw"].sum())
            if calibration_active
            else int(len(tests)),
            "n_permutation_fdr_significant_windows": int(tests["is_significant_permutation_fdr"].sum())
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
            "max_abs_delta_pro_rate": float(tests["abs_delta_pro_rate"].max()) if not tests.empty else 0.0,
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
                "swing_window_tests": tests,
                "swing_significant_windows": significant,
                "pro_rate_by_hour": counts_per_hour,
                "swing_null_distribution": null_distribution,
            },
        )
