from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import chi2

from testifier_audit.detectors.base import Detector, DetectorResult
from testifier_audit.detectors.stats import benjamini_hochberg, empirical_tail_p_values


class PeriodicityDetector(Detector):
    name = "periodicity"

    def __init__(
        self,
        max_lag_minutes: int = 180,
        min_period_minutes: float = 5.0,
        max_period_minutes: float = 720.0,
        top_n_periods: int = 20,
        calibration_iterations: int = 100,
        calibration_seed: int = 42,
        fdr_alpha: float = 0.05,
    ) -> None:
        self.max_lag_minutes = max_lag_minutes
        self.min_period_minutes = min_period_minutes
        self.max_period_minutes = max_period_minutes
        self.top_n_periods = top_n_periods
        self.calibration_iterations = calibration_iterations
        self.calibration_seed = calibration_seed
        self.fdr_alpha = fdr_alpha

    @staticmethod
    def _lag_autocorr(values: np.ndarray, lag: int) -> float:
        left = values[:-lag]
        right = values[lag:]
        if len(left) < 2 or np.std(left) == 0.0 or np.std(right) == 0.0:
            return 0.0
        corr = float(np.corrcoef(left, right)[0, 1])
        if np.isnan(corr):
            return 0.0
        return corr

    def _build_autocorr(self, values: np.ndarray) -> pd.DataFrame:
        max_lag = min(self.max_lag_minutes, values.size - 1)
        rows: list[dict[str, float]] = []
        for lag in range(1, max_lag + 1):
            corr = self._lag_autocorr(values, lag)
            rows.append(
                {
                    "lag_minutes": float(lag),
                    "autocorr": corr,
                    "abs_autocorr": abs(corr),
                }
            )
        return pd.DataFrame(rows).sort_values("lag_minutes")

    def _build_spectrum(self, values: np.ndarray) -> pd.DataFrame:
        demeaned = values - values.mean()
        fft = np.fft.rfft(demeaned)
        power = np.abs(fft) ** 2
        frequency = np.fft.rfftfreq(n=len(demeaned), d=1.0)
        frequency_index = np.arange(frequency.size, dtype=int)

        mask = frequency > 0
        period_minutes = np.divide(
            1.0,
            frequency,
            out=np.full_like(frequency, np.inf, dtype=float),
            where=frequency > 0,
        )
        mask = mask & (period_minutes >= float(self.min_period_minutes))
        mask = mask & (period_minutes <= float(self.max_period_minutes))

        return pd.DataFrame(
            {
                "frequency_index": frequency_index[mask],
                "frequency_per_minute": frequency[mask],
                "period_minutes": period_minutes[mask],
                "power": power[mask],
            }
        ).sort_values("power", ascending=False)

    def _build_clockface_distribution(
        self, counts: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame, float, float]:
        minute_bucket = pd.to_datetime(counts["minute_bucket"])
        n_total = counts["n_total"].astype(float).fillna(0.0).to_numpy()
        minute_of_hour = minute_bucket.dt.minute.to_numpy(dtype=int)

        observed = (
            pd.DataFrame(
                {
                    "minute_of_hour": minute_of_hour,
                    "n_events": n_total,
                }
            )
            .groupby("minute_of_hour", as_index=False)["n_events"]
            .sum()
            .set_index("minute_of_hour")
            .reindex(range(60), fill_value=0.0)
            .rename_axis("minute_of_hour")
            .reset_index()
        )

        total_events = float(observed["n_events"].sum())
        expected = total_events / 60.0 if total_events > 0.0 else 0.0
        observed["expected_n_events_uniform"] = expected
        observed["deviation_from_uniform"] = observed["n_events"] - expected
        observed["share"] = observed["n_events"] / total_events if total_events > 0.0 else 0.0
        if expected > 0.0:
            observed["z_score_uniform"] = observed["deviation_from_uniform"] / np.sqrt(expected)
            chi_square_stat = float(((observed["deviation_from_uniform"] ** 2) / expected).sum())
            chi_square_p_value = float(chi2.sf(chi_square_stat, df=59))
        else:
            observed["z_score_uniform"] = 0.0
            chi_square_stat = 0.0
            chi_square_p_value = 1.0

        top_minutes = observed.sort_values(
            ["n_events", "minute_of_hour"],
            ascending=[False, True],
        ).head(min(self.top_n_periods, 60))

        return observed, top_minutes, chi_square_stat, chi_square_p_value

    def _build_clockface_null_distribution(
        self,
        counts: pd.DataFrame,
        rng: np.random.Generator,
    ) -> pd.DataFrame:
        if self.calibration_iterations <= 0:
            return pd.DataFrame(
                columns=["iteration", "clockface_chi_square", "clockface_max_share"]
            )

        minute_bucket = pd.to_datetime(counts["minute_bucket"])
        hour_totals = (
            pd.DataFrame(
                {
                    "hour_bucket": minute_bucket.dt.floor("h"),
                    "n_total": counts["n_total"].astype(float).fillna(0.0),
                }
            )
            .groupby("hour_bucket", as_index=False)["n_total"]
            .sum()
        )
        hour_counts = hour_totals["n_total"].round().astype(int).to_numpy()
        hour_counts = hour_counts[hour_counts > 0]
        if hour_counts.size == 0:
            return pd.DataFrame(
                columns=["iteration", "clockface_chi_square", "clockface_max_share"]
            )

        probs = np.full(60, 1.0 / 60.0, dtype=float)
        total_events = float(hour_counts.sum())
        expected = total_events / 60.0
        rows: list[dict[str, float]] = []
        for iteration in range(1, self.calibration_iterations + 1):
            simulated = np.zeros(60, dtype=float)
            for hour_count in hour_counts:
                simulated += rng.multinomial(int(hour_count), probs).astype(float)

            deviation = simulated - expected
            chi_square_stat = float(((deviation**2) / expected).sum()) if expected > 0.0 else 0.0
            max_share = float(simulated.max() / total_events) if total_events > 0.0 else 0.0
            rows.append(
                {
                    "iteration": float(iteration),
                    "clockface_chi_square": chi_square_stat,
                    "clockface_max_share": max_share,
                }
            )
        return pd.DataFrame(rows)

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        counts = features.get("counts_per_minute", pd.DataFrame())
        if counts.empty or len(counts) < 3:
            empty = pd.DataFrame()
            return DetectorResult(
                detector=self.name,
                summary={
                    "max_abs_autocorr": 0.0,
                    "strongest_period_minutes": None,
                    "n_significant_autocorr_lags": 0,
                    "n_significant_periods": 0,
                    "clockface_chi_square": 0.0,
                    "clockface_chi_square_p_value": 1.0,
                    "clockface_chi_square_empirical_p_value": 1.0,
                    "clockface_max_share": 0.0,
                    "clockface_max_share_empirical_p_value": 1.0,
                },
                tables={
                    "autocorr": empty,
                    "autocorr_significant": empty,
                    "spectrum_top": empty,
                    "spectrum_significant": empty,
                    "periodicity_null_distribution": empty,
                    "clockface_distribution": empty,
                    "clockface_top_minutes": empty,
                    "clockface_null_distribution": empty,
                },
            )

        values = counts["n_total"].astype(float).to_numpy()
        autocorr = self._build_autocorr(values)
        spectrum = self._build_spectrum(values)
        spectrum_top = spectrum.head(self.top_n_periods).copy()
        (
            clockface_distribution,
            clockface_top_minutes,
            clockface_chi_square,
            clockface_chi_square_p_value,
        ) = self._build_clockface_distribution(counts)
        clockface_max_share = (
            float(clockface_distribution["share"].max())
            if not clockface_distribution.empty
            else 0.0
        )

        autocorr["p_value"] = np.nan
        autocorr["q_value"] = np.nan
        autocorr["is_significant"] = False

        spectrum_top["p_value"] = np.nan
        spectrum_top["q_value"] = np.nan
        spectrum_top["is_significant"] = False
        clockface_chi_square_empirical_p_value = np.nan
        clockface_max_share_empirical_p_value = np.nan

        null_distribution = pd.DataFrame(
            columns=["iteration", "max_abs_autocorr", "max_spectrum_power"]
        )
        clockface_null_distribution = pd.DataFrame(
            columns=["iteration", "clockface_chi_square", "clockface_max_share"]
        )
        if self.calibration_iterations > 0 and (not autocorr.empty) and (not spectrum_top.empty):
            rng = np.random.default_rng(self.calibration_seed)

            n_lags = len(autocorr)
            n_periods = len(spectrum_top)
            null_abs_autocorr = np.zeros((self.calibration_iterations, n_lags), dtype=float)
            null_period_power = np.zeros((self.calibration_iterations, n_periods), dtype=float)
            null_max_abs_autocorr = np.zeros(self.calibration_iterations, dtype=float)
            null_max_spectrum_power = np.zeros(self.calibration_iterations, dtype=float)

            top_frequency_indices = spectrum_top["frequency_index"].astype(int).to_numpy()
            all_frequency_indices = spectrum["frequency_index"].astype(int).to_numpy()
            max_lag = min(self.max_lag_minutes, values.size - 1)

            for idx in range(self.calibration_iterations):
                permuted = rng.permutation(values)

                lag_values: list[float] = []
                for lag in range(1, max_lag + 1):
                    lag_values.append(abs(self._lag_autocorr(permuted, lag)))
                lag_array = np.array(lag_values, dtype=float)
                null_abs_autocorr[idx, :] = lag_array
                null_max_abs_autocorr[idx] = float(lag_array.max()) if lag_array.size else 0.0

                perm_demeaned = permuted - permuted.mean()
                perm_power = np.abs(np.fft.rfft(perm_demeaned)) ** 2
                null_period_power[idx, :] = perm_power[top_frequency_indices]
                null_max_spectrum_power[idx] = (
                    float(np.max(perm_power[all_frequency_indices]))
                    if all_frequency_indices.size
                    else 0.0
                )

            autocorr_p_values = np.array(
                [
                    empirical_tail_p_values(
                        np.array([observed], dtype=float),
                        null_abs_autocorr[:, lag_idx],
                    )[0]
                    for lag_idx, observed in enumerate(
                        autocorr["abs_autocorr"].to_numpy(dtype=float)
                    )
                ],
                dtype=float,
            )
            autocorr_reject, autocorr_q_values = benjamini_hochberg(
                autocorr_p_values,
                alpha=self.fdr_alpha,
            )
            autocorr["p_value"] = autocorr_p_values
            autocorr["q_value"] = autocorr_q_values
            autocorr["is_significant"] = autocorr_reject

            spectrum_p_values = np.array(
                [
                    empirical_tail_p_values(
                        np.array([observed], dtype=float),
                        null_period_power[:, period_idx],
                    )[0]
                    for period_idx, observed in enumerate(
                        spectrum_top["power"].to_numpy(dtype=float)
                    )
                ],
                dtype=float,
            )
            spectrum_reject, spectrum_q_values = benjamini_hochberg(
                spectrum_p_values,
                alpha=self.fdr_alpha,
            )
            spectrum_top["p_value"] = spectrum_p_values
            spectrum_top["q_value"] = spectrum_q_values
            spectrum_top["is_significant"] = spectrum_reject

            null_distribution = pd.DataFrame(
                {
                    "iteration": np.arange(1, self.calibration_iterations + 1, dtype=int),
                    "max_abs_autocorr": null_max_abs_autocorr,
                    "max_spectrum_power": null_max_spectrum_power,
                }
            )

            clockface_null_distribution = self._build_clockface_null_distribution(
                counts=counts, rng=rng
            )
            if not clockface_null_distribution.empty:
                clockface_chi_square_empirical_p_value = float(
                    empirical_tail_p_values(
                        np.array([clockface_chi_square], dtype=float),
                        clockface_null_distribution["clockface_chi_square"].to_numpy(dtype=float),
                    )[0]
                )
                clockface_max_share_empirical_p_value = float(
                    empirical_tail_p_values(
                        np.array([clockface_max_share], dtype=float),
                        clockface_null_distribution["clockface_max_share"].to_numpy(dtype=float),
                    )[0]
                )

        autocorr_significant = autocorr[autocorr["is_significant"]].copy()
        spectrum_significant = spectrum_top[spectrum_top["is_significant"]].copy()
        strongest_period = (
            float(spectrum_top.iloc[0]["period_minutes"]) if not spectrum_top.empty else None
        )
        max_abs_autocorr = float(autocorr["abs_autocorr"].max()) if not autocorr.empty else 0.0

        return DetectorResult(
            detector=self.name,
            summary={
                "max_abs_autocorr": max_abs_autocorr,
                "strongest_period_minutes": strongest_period,
                "n_significant_autocorr_lags": int(len(autocorr_significant)),
                "n_significant_periods": int(len(spectrum_significant)),
                "calibration_iterations": int(self.calibration_iterations),
                "fdr_alpha": float(self.fdr_alpha),
                "clockface_chi_square": float(clockface_chi_square),
                "clockface_chi_square_p_value": float(clockface_chi_square_p_value),
                "clockface_chi_square_empirical_p_value": (
                    float(clockface_chi_square_empirical_p_value)
                    if not np.isnan(clockface_chi_square_empirical_p_value)
                    else None
                ),
                "clockface_max_share": float(clockface_max_share),
                "clockface_max_share_empirical_p_value": (
                    float(clockface_max_share_empirical_p_value)
                    if not np.isnan(clockface_max_share_empirical_p_value)
                    else None
                ),
            },
            tables={
                "autocorr": autocorr,
                "autocorr_significant": autocorr_significant,
                "spectrum_top": spectrum_top,
                "spectrum_significant": spectrum_significant,
                "periodicity_null_distribution": null_distribution,
                "clockface_distribution": clockface_distribution,
                "clockface_top_minutes": clockface_top_minutes,
                "clockface_null_distribution": clockface_null_distribution,
            },
        )
