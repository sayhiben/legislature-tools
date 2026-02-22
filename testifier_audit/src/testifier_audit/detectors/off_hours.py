from __future__ import annotations

import warnings
from collections.abc import Iterable

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy.stats import binom, binomtest, chi2_contingency
from statsmodels.stats.multitest import multipletests
from statsmodels.tools.sm_exceptions import PerfectSeparationWarning

from testifier_audit.detectors.base import Detector, DetectorResult
from testifier_audit.proportion_stats import (
    DEFAULT_LOW_POWER_MIN_TOTAL,
    low_power_mask,
    wilson_half_width,
    wilson_interval,
)


def _resolve_event_time(df: pd.DataFrame) -> pd.Series:
    if "minute_bucket" in df.columns:
        minute_bucket = pd.to_datetime(df["minute_bucket"], errors="coerce")
        if minute_bucket.notna().any():
            return minute_bucket
    if "timestamp" in df.columns:
        return pd.to_datetime(df["timestamp"], errors="coerce")
    return pd.Series(pd.NaT, index=df.index)


def _control_limits(
    expected_pro_rate: pd.Series,
    totals: pd.Series,
    *,
    z: float,
) -> tuple[pd.Series, pd.Series]:
    expected = pd.to_numeric(expected_pro_rate, errors="coerce")
    n_total = pd.to_numeric(totals, errors="coerce")

    se = np.sqrt((expected * (1.0 - expected)) / n_total)
    lower = (expected - (z * se)).clip(lower=0.0, upper=1.0)
    upper = (expected + (z * se)).clip(lower=0.0, upper=1.0)

    invalid = (~np.isfinite(expected)) | (~np.isfinite(n_total)) | (n_total <= 0.0)
    lower = lower.where(~invalid)
    upper = upper.where(~invalid)
    return lower, upper


def _safe_int(value: object) -> int:
    parsed = pd.to_numeric(value, errors="coerce")
    if pd.isna(parsed):
        return 0
    return int(parsed)


def _safe_float(value: object, *, default: float) -> float:
    parsed = pd.to_numeric(value, errors="coerce")
    if pd.isna(parsed):
        return float(default)
    return float(parsed)


class OffHoursDetector(Detector):
    name = "off_hours"
    DEFAULT_BUCKET_MINUTES = (1, 5, 15, 30, 60, 120, 240)

    def __init__(
        self,
        *,
        bucket_minutes: Iterable[int] | None = None,
        min_window_total: int = DEFAULT_LOW_POWER_MIN_TOTAL,
        fdr_alpha: float = 0.05,
        primary_bucket_minutes: int = 30,
        model_min_rows: int = 24,
        model_hour_harmonics: int = 3,
        alert_off_hours_min_fraction: float = 1.0,
        primary_alert_min_abs_delta: float = 0.03,
    ) -> None:
        if bucket_minutes is None:
            resolved_buckets = list(self.DEFAULT_BUCKET_MINUTES)
        else:
            resolved_buckets = sorted({int(value) for value in bucket_minutes if int(value) > 0})
        self.bucket_minutes = tuple(int(value) for value in resolved_buckets)
        self.min_window_total = max(1, int(min_window_total))
        self.fdr_alpha = float(min(0.5, max(1e-6, fdr_alpha)))
        self.primary_bucket_minutes = int(primary_bucket_minutes)
        self.model_min_rows = max(8, int(model_min_rows))
        self.model_hour_harmonics = max(1, min(6, int(model_hour_harmonics)))
        self.alert_off_hours_min_fraction = float(
            min(1.0, max(0.5, alert_off_hours_min_fraction))
        )
        self.primary_alert_min_abs_delta = float(min(1.0, max(0.0, primary_alert_min_abs_delta)))

    def _design_matrix(
        self,
        frame: pd.DataFrame,
        *,
        day_levels: list[str],
    ) -> pd.DataFrame:
        hour_numeric = pd.to_numeric(frame["hour"], errors="coerce")
        radians = (2.0 * np.pi * hour_numeric) / 24.0
        design = pd.DataFrame({"intercept": 1.0}, index=frame.index)
        for harmonic in range(1, self.model_hour_harmonics + 1):
            design[f"hour_sin_{harmonic}"] = np.sin(float(harmonic) * radians)
            design[f"hour_cos_{harmonic}"] = np.cos(float(harmonic) * radians)
        for level in day_levels[1:]:
            column = f"day__{level}"
            design[column] = (frame["event_date_key"] == level).astype(float)
        return design

    def _fit_model_expected_pro_rate(
        self,
        windowed: pd.DataFrame,
    ) -> tuple[pd.Series, pd.Series, dict[str, object]]:
        diagnostics: dict[str, object] = {
            "model_fit_method": "unavailable",
            "model_fit_rows": 0,
            "model_fit_unique_days": 0,
            "model_fit_unique_hours": 0,
            "model_fit_converged": np.nan,
            "model_fit_aic": np.nan,
            "model_fit_used_harmonics": int(self.model_hour_harmonics),
        }
        if windowed.empty:
            empty = pd.Series(dtype=float)
            return empty, pd.Series(dtype=bool), diagnostics

        expected = pd.Series(np.nan, index=windowed.index, dtype=float)
        model_available = pd.Series(False, index=windowed.index, dtype=bool)

        known_mask = pd.to_numeric(windowed["n_known"], errors="coerce").fillna(0.0) > 0.0
        fit_frame = windowed[known_mask].copy()
        if fit_frame.empty:
            diagnostics["model_fit_method"] = "unavailable_no_known_rows"
            return expected, model_available, diagnostics

        fit_frame["hour"] = pd.to_numeric(fit_frame["hour"], errors="coerce")
        fit_frame = fit_frame[fit_frame["hour"].notna()].copy()
        if fit_frame.empty:
            diagnostics["model_fit_method"] = "unavailable_no_valid_hours"
            return expected, model_available, diagnostics

        day_levels = sorted(
            {
                str(value)
                for value in fit_frame["event_date_key"].astype("string").dropna().tolist()
                if str(value).strip()
            }
        )
        if not day_levels:
            diagnostics["model_fit_method"] = "unavailable_no_day_levels"
            return expected, model_available, diagnostics
        diagnostics["model_fit_rows"] = int(len(fit_frame))
        diagnostics["model_fit_unique_days"] = int(len(day_levels))
        diagnostics["model_fit_unique_hours"] = int(fit_frame["hour"].nunique(dropna=True))
        if len(fit_frame) < self.model_min_rows:
            diagnostics["model_fit_method"] = "unavailable_insufficient_rows"
            return expected, model_available, diagnostics
        if fit_frame["hour"].nunique(dropna=True) < 3:
            diagnostics["model_fit_method"] = "unavailable_insufficient_hour_coverage"
            return expected, model_available, diagnostics

        y = (
            pd.to_numeric(fit_frame["n_pro"], errors="coerce")
            / pd.to_numeric(fit_frame["n_known"], errors="coerce")
        ).clip(lower=1e-6, upper=1.0 - 1e-6)
        weights = pd.to_numeric(fit_frame["n_known"], errors="coerce").fillna(0.0).clip(lower=1.0)
        x_fit = self._design_matrix(fit_frame, day_levels=day_levels)

        fit_result = None
        fit_method = "unavailable"
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("error", category=PerfectSeparationWarning)
                fit_result = sm.GLM(
                    y,
                    x_fit,
                    family=sm.families.Binomial(),
                    freq_weights=weights,
                ).fit(maxiter=250, disp=0)
                fit_method = "glm"
        except Exception:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=PerfectSeparationWarning)
                    fit_result = sm.GLM(
                        y,
                        x_fit,
                        family=sm.families.Binomial(),
                        freq_weights=weights,
                    ).fit_regularized(
                        alpha=1e-4,
                        L1_wt=0.0,
                        maxiter=500,
                    )
                    fit_method = "glm_regularized"
            except Exception:
                diagnostics["model_fit_method"] = "unavailable_fit_failure"
                return expected, model_available, diagnostics

        full_frame = windowed.copy()
        full_frame["hour"] = pd.to_numeric(full_frame["hour"], errors="coerce")
        full_frame = full_frame[full_frame["hour"].notna()].copy()
        if full_frame.empty:
            diagnostics["model_fit_method"] = "unavailable_prediction_frame_empty"
            return expected, model_available, diagnostics
        x_full = self._design_matrix(full_frame, day_levels=day_levels)

        predictions = fit_result.predict(x_full)
        predictions = pd.to_numeric(predictions, errors="coerce").clip(lower=1e-6, upper=1.0 - 1e-6)
        expected.loc[x_full.index] = predictions
        model_available.loc[x_full.index] = predictions.notna()
        diagnostics["model_fit_method"] = fit_method
        diagnostics["model_fit_converged"] = (
            float(bool(getattr(fit_result, "converged", False)))
            if hasattr(fit_result, "converged")
            else np.nan
        )
        diagnostics["model_fit_aic"] = float(getattr(fit_result, "aic", np.nan))
        return expected, model_available, diagnostics

    def _build_window_control_profile(self, working: pd.DataFrame) -> pd.DataFrame:
        timed = working.assign(_event_time=_resolve_event_time(working))
        timed = timed[timed["_event_time"].notna()].copy()
        if timed.empty:
            return pd.DataFrame()

        timed["event_date_key"] = timed["_event_time"].dt.strftime("%Y-%m-%d")
        known_mask = timed["is_pro"] | timed["is_con"]
        known = timed[known_mask].copy()

        overall_known_total = int(len(known))
        overall_known_pro = int(known["is_pro"].sum())
        overall_known_pro_rate = (
            (overall_known_pro / overall_known_total) if overall_known_total else np.nan
        )

        on_hours_known = known[~known["is_off_hours"]]
        on_hours_known_total = int(len(on_hours_known))
        on_hours_known_pro = int(on_hours_known["is_pro"].sum())
        global_on_hours_pro_rate = (
            (on_hours_known_pro / on_hours_known_total) if on_hours_known_total else np.nan
        )
        if not np.isfinite(global_on_hours_pro_rate):
            global_on_hours_pro_rate = overall_known_pro_rate

        day_baseline = (
            on_hours_known.groupby("event_date_key", dropna=False)
            .agg(
                day_on_hours_known=("is_pro", "size"),
                day_on_hours_pro=("is_pro", "sum"),
            )
            .reset_index()
        )
        day_baseline["day_on_hours_pro_rate"] = (
            day_baseline["day_on_hours_pro"] / day_baseline["day_on_hours_known"]
        ).where(day_baseline["day_on_hours_known"] > 0)

        window_frames: list[pd.DataFrame] = []
        for bucket_minutes in self.bucket_minutes:
            bucket_start = timed["_event_time"].dt.floor(f"{bucket_minutes}min")
            windowed = (
                timed.assign(
                    bucket_start=bucket_start,
                    event_date_key=bucket_start.dt.strftime("%Y-%m-%d"),
                )
                .groupby(["bucket_start", "event_date_key"], dropna=False)
                .agg(
                    n_total=("is_pro", "size"),
                    n_pro=("is_pro", "sum"),
                    n_con=("is_con", "sum"),
                    n_off_hours=("is_off_hours", "sum"),
                )
                .reset_index()
                .sort_values("bucket_start")
            )
            if windowed.empty:
                continue

            windowed["bucket_minutes"] = int(bucket_minutes)
            windowed["day_of_week"] = windowed["bucket_start"].dt.day_name()
            windowed["hour"] = windowed["bucket_start"].dt.hour
            windowed["n_known"] = windowed["n_pro"] + windowed["n_con"]
            windowed["n_unknown"] = (windowed["n_total"] - windowed["n_known"]).clip(lower=0)
            windowed["off_hours_fraction"] = (
                windowed["n_off_hours"] / windowed["n_total"].replace(0, np.nan)
            ).fillna(0.0)
            windowed["is_off_hours_window"] = windowed["off_hours_fraction"] >= 0.5
            windowed["is_pure_off_hours_window"] = windowed["n_off_hours"] == windowed["n_total"]
            windowed["is_alert_off_hours_window"] = (
                windowed["off_hours_fraction"] >= self.alert_off_hours_min_fraction
            )
            windowed["pro_rate"] = (windowed["n_pro"] / windowed["n_known"]).where(
                windowed["n_known"] > 0
            )

            (
                windowed["pro_rate_wilson_low"],
                windowed["pro_rate_wilson_high"],
            ) = wilson_interval(
                successes=windowed["n_pro"],
                totals=windowed["n_known"],
            )
            windowed["pro_rate_wilson_half_width"] = wilson_half_width(
                successes=windowed["n_pro"],
                totals=windowed["n_known"],
            )
            windowed["is_low_power"] = low_power_mask(
                totals=windowed["n_known"],
                min_total=self.min_window_total,
            )

            windowed = windowed.merge(day_baseline, on="event_date_key", how="left")
            windowed["expected_pro_rate_global"] = (
                float(global_on_hours_pro_rate) if np.isfinite(global_on_hours_pro_rate) else np.nan
            )
            windowed["expected_pro_rate_day"] = windowed["day_on_hours_pro_rate"]
            windowed["baseline_source"] = "day_on_hours"

            invalid_day = (
                pd.to_numeric(windowed["day_on_hours_known"], errors="coerce").fillna(0.0)
                < float(self.min_window_total)
            ) | windowed["expected_pro_rate_day"].isna()
            windowed.loc[invalid_day, "expected_pro_rate_day"] = windowed.loc[
                invalid_day, "expected_pro_rate_global"
            ]
            windowed.loc[invalid_day, "baseline_source"] = "global_on_hours"

            if not np.isfinite(global_on_hours_pro_rate) and np.isfinite(overall_known_pro_rate):
                windowed["expected_pro_rate_global"] = float(overall_known_pro_rate)
                windowed["expected_pro_rate_day"] = float(overall_known_pro_rate)
                windowed["baseline_source"] = "overall_known"
            if not np.isfinite(overall_known_pro_rate):
                windowed["baseline_source"] = "unavailable"

            expected_model, model_available, model_diagnostics = self._fit_model_expected_pro_rate(
                windowed
            )
            windowed["expected_pro_rate_model"] = expected_model
            windowed["is_model_baseline_available"] = model_available
            windowed["model_baseline_source"] = np.where(
                windowed["is_model_baseline_available"],
                "day_fixed_plus_harmonic_hour",
                "unavailable",
            )
            windowed["model_fit_method"] = str(
                model_diagnostics.get("model_fit_method", "unavailable")
            )
            windowed["model_fit_rows"] = int(model_diagnostics.get("model_fit_rows", 0))
            windowed["model_fit_unique_days"] = int(
                model_diagnostics.get("model_fit_unique_days", 0)
            )
            windowed["model_fit_unique_hours"] = int(
                model_diagnostics.get("model_fit_unique_hours", 0)
            )
            windowed["model_fit_converged"] = pd.to_numeric(
                model_diagnostics.get("model_fit_converged", np.nan),
                errors="coerce",
            )
            windowed["model_fit_aic"] = pd.to_numeric(
                model_diagnostics.get("model_fit_aic", np.nan),
                errors="coerce",
            )
            windowed["model_fit_used_harmonics"] = int(self.model_hour_harmonics)

            windowed["expected_pro_rate_primary"] = windowed["expected_pro_rate_model"]
            windowed["primary_baseline_source"] = "model_day_hour"
            missing_primary = windowed["expected_pro_rate_primary"].isna()
            windowed.loc[missing_primary, "expected_pro_rate_primary"] = windowed.loc[
                missing_primary, "expected_pro_rate_day"
            ]
            windowed.loc[missing_primary, "primary_baseline_source"] = windowed.loc[
                missing_primary, "baseline_source"
            ]

            (
                windowed["control_low_95_day"],
                windowed["control_high_95_day"],
            ) = _control_limits(
                windowed["expected_pro_rate_day"],
                windowed["n_known"],
                z=1.96,
            )
            (
                windowed["control_low_998_day"],
                windowed["control_high_998_day"],
            ) = _control_limits(
                windowed["expected_pro_rate_day"],
                windowed["n_known"],
                z=3.0,
            )
            (
                windowed["control_low_95_global"],
                windowed["control_high_95_global"],
            ) = _control_limits(
                windowed["expected_pro_rate_global"],
                windowed["n_known"],
                z=1.96,
            )
            (
                windowed["control_low_998_global"],
                windowed["control_high_998_global"],
            ) = _control_limits(
                windowed["expected_pro_rate_global"],
                windowed["n_known"],
                z=3.0,
            )
            (
                windowed["control_low_95_model"],
                windowed["control_high_95_model"],
            ) = _control_limits(
                windowed["expected_pro_rate_model"],
                windowed["n_known"],
                z=1.96,
            )
            (
                windowed["control_low_998_model"],
                windowed["control_high_998_model"],
            ) = _control_limits(
                windowed["expected_pro_rate_model"],
                windowed["n_known"],
                z=3.0,
            )
            (
                windowed["control_low_95_primary"],
                windowed["control_high_95_primary"],
            ) = _control_limits(
                windowed["expected_pro_rate_primary"],
                windowed["n_known"],
                z=1.96,
            )
            (
                windowed["control_low_998_primary"],
                windowed["control_high_998_primary"],
            ) = _control_limits(
                windowed["expected_pro_rate_primary"],
                windowed["n_known"],
                z=3.0,
            )

            tested = (~windowed["is_low_power"]) & windowed["pro_rate"].notna() & (
                windowed["n_known"] > 0
            )
            n_known_numeric = pd.to_numeric(windowed["n_known"], errors="coerce").fillna(0.0)
            n_known_int = n_known_numeric.round().astype(int)
            n_pro_int = (
                pd.to_numeric(windowed["n_pro"], errors="coerce").fillna(0.0).round().astype(int)
            )

            for baseline_name, expected_col in (
                ("day", "expected_pro_rate_day"),
                ("model", "expected_pro_rate_model"),
                ("primary", "expected_pro_rate_primary"),
            ):
                expected_rate = pd.to_numeric(windowed[expected_col], errors="coerce").clip(
                    lower=1e-6,
                    upper=1.0 - 1e-6,
                )
                expected_count = n_known_numeric * expected_rate
                variance = n_known_numeric * expected_rate * (1.0 - expected_rate)
                valid_z = (n_known_numeric > 0.0) & (variance > 0.0) & expected_rate.notna()
                z_score = ((windowed["n_pro"] - expected_count) / np.sqrt(variance)).where(valid_z)
                valid_exact = valid_z & (n_known_int > 0)
                p_lower = pd.Series(np.nan, index=windowed.index, dtype=float)
                p_upper = pd.Series(np.nan, index=windowed.index, dtype=float)
                p_two_sided = pd.Series(np.nan, index=windowed.index, dtype=float)
                if valid_exact.any():
                    valid_idx = valid_exact[valid_exact].index
                    k_values = n_pro_int.loc[valid_idx].to_numpy(dtype=int)
                    n_values = n_known_int.loc[valid_idx].to_numpy(dtype=int)
                    p_values = expected_rate.loc[valid_idx].to_numpy(dtype=float)
                    p_lower.loc[valid_idx] = binom.cdf(
                        k_values,
                        n_values,
                        p_values,
                    )
                    p_upper.loc[valid_idx] = binom.sf(
                        k_values - 1,
                        n_values,
                        p_values,
                    )
                    p_two_sided.loc[valid_idx] = np.fromiter(
                        (
                            float(
                                binomtest(int(k), int(n), p=float(p), alternative="two-sided").pvalue
                            )
                            for k, n, p in zip(k_values, n_values, p_values, strict=False)
                        ),
                        dtype=float,
                        count=len(valid_idx),
                    )
                usable = valid_exact & tested

                windowed[f"z_score_{baseline_name}"] = z_score
                windowed[f"delta_pro_rate_{baseline_name}"] = (
                    windowed["pro_rate"] - expected_rate
                ).where(windowed["pro_rate"].notna())
                windowed[f"p_value_{baseline_name}_lower"] = p_lower.where(usable)
                windowed[f"p_value_{baseline_name}_upper"] = p_upper.where(usable)
                windowed[f"p_value_{baseline_name}_two_sided"] = p_two_sided.where(usable)
                for tail in ("lower", "upper", "two_sided"):
                    windowed[f"q_value_{baseline_name}_{tail}"] = np.nan
                    windowed[f"is_significant_{baseline_name}_{tail}"] = False

            windowed["is_below_day_control_95"] = tested & (
                windowed["pro_rate"] < windowed["control_low_95_day"]
            )
            windowed["is_below_day_control_998"] = tested & (
                windowed["pro_rate"] < windowed["control_low_998_day"]
            )
            windowed["is_above_day_control_95"] = tested & (
                windowed["pro_rate"] > windowed["control_high_95_day"]
            )
            windowed["is_above_day_control_998"] = tested & (
                windowed["pro_rate"] > windowed["control_high_998_day"]
            )
            windowed["is_below_model_control_95"] = tested & (
                windowed["pro_rate"] < windowed["control_low_95_model"]
            )
            windowed["is_below_model_control_998"] = tested & (
                windowed["pro_rate"] < windowed["control_low_998_model"]
            )
            windowed["is_above_model_control_95"] = tested & (
                windowed["pro_rate"] > windowed["control_high_95_model"]
            )
            windowed["is_above_model_control_998"] = tested & (
                windowed["pro_rate"] > windowed["control_high_998_model"]
            )
            windowed["is_below_primary_control_95"] = tested & (
                windowed["pro_rate"] < windowed["control_low_95_primary"]
            )
            windowed["is_below_primary_control_998"] = tested & (
                windowed["pro_rate"] < windowed["control_low_998_primary"]
            )
            windowed["is_above_primary_control_95"] = tested & (
                windowed["pro_rate"] > windowed["control_high_95_primary"]
            )
            windowed["is_above_primary_control_998"] = tested & (
                windowed["pro_rate"] > windowed["control_high_998_primary"]
            )
            windowed["is_outside_day_control_95"] = tested & (
                (windowed["pro_rate"] < windowed["control_low_95_day"])
                | (windowed["pro_rate"] > windowed["control_high_95_day"])
            )
            windowed["is_outside_day_control_998"] = tested & (
                (windowed["pro_rate"] < windowed["control_low_998_day"])
                | (windowed["pro_rate"] > windowed["control_high_998_day"])
            )
            windowed["is_outside_model_control_95"] = tested & (
                (windowed["pro_rate"] < windowed["control_low_95_model"])
                | (windowed["pro_rate"] > windowed["control_high_95_model"])
            )
            windowed["is_outside_model_control_998"] = tested & (
                (windowed["pro_rate"] < windowed["control_low_998_model"])
                | (windowed["pro_rate"] > windowed["control_high_998_model"])
            )
            windowed["is_outside_primary_control_95"] = tested & (
                (windowed["pro_rate"] < windowed["control_low_95_primary"])
                | (windowed["pro_rate"] > windowed["control_high_95_primary"])
            )
            windowed["is_outside_primary_control_998"] = tested & (
                (windowed["pro_rate"] < windowed["control_low_998_primary"])
                | (windowed["pro_rate"] > windowed["control_high_998_primary"])
            )
            windowed["is_below_global_control_95"] = tested & (
                windowed["pro_rate"] < windowed["control_low_95_global"]
            )
            windowed["is_below_global_control_998"] = tested & (
                windowed["pro_rate"] < windowed["control_low_998_global"]
            )

            windowed["p_value_day"] = windowed["p_value_day_lower"]
            windowed["p_value_model"] = windowed["p_value_model_lower"]
            windowed["p_value_primary"] = windowed["p_value_primary_lower"]

            window_frames.append(windowed)

        if not window_frames:
            return pd.DataFrame()

        profile = pd.concat(window_frames, ignore_index=True)
        bucket_values = sorted(profile["bucket_minutes"].dropna().astype(int).unique())
        for bucket_minutes in bucket_values:
            bucket_mask = profile["bucket_minutes"] == bucket_minutes
            tested_off_hours = (
                bucket_mask
                & profile["is_alert_off_hours_window"]
                & (~profile["is_low_power"])
                & (pd.to_numeric(profile["n_known"], errors="coerce").fillna(0.0) > 0.0)
            )
            for baseline_name in ("day", "model", "primary"):
                for tail in ("lower", "upper", "two_sided"):
                    p_column = f"p_value_{baseline_name}_{tail}"
                    q_column = f"q_value_{baseline_name}_{tail}"
                    sig_column = f"is_significant_{baseline_name}_{tail}"

                    p_values = pd.to_numeric(
                        profile.loc[tested_off_hours, p_column],
                        errors="coerce",
                    )
                    valid = p_values.notna()
                    if not valid.any():
                        continue
                    _reject, q_values, _alphac_sidak, _alphac_bonf = multipletests(
                        p_values[valid].to_numpy(dtype=float),
                        alpha=self.fdr_alpha,
                        method="fdr_bh",
                    )
                    valid_index = p_values[valid].index
                    profile.loc[valid_index, q_column] = q_values
                    profile.loc[valid_index, sig_column] = q_values <= self.fdr_alpha

        profile["q_value_day"] = profile["q_value_day_lower"]
        profile["is_significant_day"] = profile["is_significant_day_lower"]
        profile["q_value_model"] = profile["q_value_model_lower"]
        profile["is_significant_model"] = profile["is_significant_model_lower"]
        profile["q_value_primary"] = profile["q_value_primary_lower"]
        profile["is_significant_primary"] = profile["is_significant_primary_lower"]
        profile = profile.copy()
        tested_profile = (~profile["is_low_power"]) & profile["pro_rate"].notna() & (
            pd.to_numeric(profile["n_known"], errors="coerce").fillna(0.0) > 0.0
        )
        primary_delta = pd.to_numeric(profile["delta_pro_rate_primary"], errors="coerce")
        profile["is_material_primary_shift"] = tested_profile & (
            primary_delta.abs() >= self.primary_alert_min_abs_delta
        )
        profile["is_material_primary_lower_shift"] = tested_profile & (
            primary_delta <= -self.primary_alert_min_abs_delta
        )
        profile["is_material_primary_upper_shift"] = tested_profile & (
            primary_delta >= self.primary_alert_min_abs_delta
        )
        profile["is_primary_alert_window"] = (
            tested_profile
            & profile["is_alert_off_hours_window"]
            & profile["is_below_primary_control_998"]
            & profile["is_significant_primary_lower"]
            & profile["is_material_primary_lower_shift"]
        )
        profile["is_primary_spc_998_two_sided"] = (
            tested_profile & profile["is_outside_primary_control_998"]
        )
        profile["is_primary_fdr_two_sided"] = (
            tested_profile & profile["is_significant_primary_two_sided"]
        )
        profile["is_primary_any_flag_channel"] = (
            profile["is_primary_spc_998_two_sided"] | profile["is_primary_fdr_two_sided"]
        )
        profile["is_primary_both_flag_channels"] = (
            profile["is_primary_spc_998_two_sided"] & profile["is_primary_fdr_two_sided"]
        )

        return profile.sort_values(["bucket_minutes", "bucket_start"]).reset_index(drop=True)

    def _build_date_hour_distribution(self, working: pd.DataFrame) -> pd.DataFrame:
        timed = working.assign(_event_time=_resolve_event_time(working))
        timed = timed[timed["_event_time"].notna()].copy()
        if timed.empty:
            return pd.DataFrame()

        grouped = (
            timed.assign(
                date=timed["_event_time"].dt.strftime("%Y-%m-%d"),
                hour=timed["_event_time"].dt.hour,
            )
            .groupby(["date", "hour"], dropna=False)
            .agg(
                n_total=("is_pro", "size"),
                n_pro=("is_pro", "sum"),
                n_con=("is_con", "sum"),
                n_off_hours=("is_off_hours", "sum"),
            )
            .reset_index()
            .sort_values(["date", "hour"])
        )
        grouped["day_of_week"] = pd.to_datetime(grouped["date"], errors="coerce").dt.day_name()
        grouped["n_known"] = grouped["n_pro"] + grouped["n_con"]
        grouped["n_unknown"] = (grouped["n_total"] - grouped["n_known"]).clip(lower=0)
        grouped["off_hours_fraction"] = (
            grouped["n_off_hours"] / grouped["n_total"].replace(0, np.nan)
        ).fillna(0.0)
        grouped["pro_rate"] = (grouped["n_pro"] / grouped["n_known"]).where(grouped["n_known"] > 0)
        grouped["pro_rate_wilson_low"], grouped["pro_rate_wilson_high"] = wilson_interval(
            successes=grouped["n_pro"],
            totals=grouped["n_known"],
        )
        grouped["is_low_power"] = low_power_mask(
            totals=grouped["n_known"],
            min_total=self.min_window_total,
        )
        return grouped.reset_index(drop=True)

    def _build_date_hour_primary_residual_distribution(
        self,
        window_control_profile: pd.DataFrame,
    ) -> pd.DataFrame:
        if window_control_profile.empty:
            return pd.DataFrame()

        frame = window_control_profile.copy()
        frame["bucket_start"] = pd.to_datetime(frame["bucket_start"], errors="coerce")
        if "bucket_minutes" in frame.columns:
            frame["bucket_minutes"] = pd.to_numeric(frame["bucket_minutes"], errors="coerce")
        else:
            frame["bucket_minutes"] = float(self.primary_bucket_minutes)
        frame = frame[frame["bucket_start"].notna() & frame["bucket_minutes"].notna()].copy()
        if frame.empty:
            return pd.DataFrame()

        frame["is_off_hours_window"] = frame["is_off_hours_window"].fillna(False).astype(bool)

        frame["date"] = frame["bucket_start"].dt.strftime("%Y-%m-%d")
        frame["day_of_week"] = frame["bucket_start"].dt.day_name()
        frame["hour"] = frame["bucket_start"].dt.hour

        for column_name in (
            "n_total",
            "n_known",
            "n_pro",
            "n_con",
            "off_hours_fraction",
            "expected_pro_rate_primary",
            "delta_pro_rate_primary",
            "z_score_primary",
        ):
            frame[column_name] = pd.to_numeric(frame[column_name], errors="coerce")

        frame["is_alert_off_hours_window"] = (
            frame["is_alert_off_hours_window"].fillna(False).astype(bool)
        )
        frame["is_low_power"] = frame["is_low_power"].fillna(False).astype(bool)
        frame["is_primary_alert_window"] = (
            frame["is_primary_alert_window"].fillna(False).astype(bool)
        )
        frame["is_support_window"] = (~frame["is_low_power"]) & (frame["n_known"] > 0)
        frame["is_tested_window"] = frame["is_alert_off_hours_window"] & frame["is_support_window"]

        def _weighted_mean(values: pd.Series, weights: pd.Series) -> float:
            value_arr = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float, copy=False)
            weight_arr = pd.to_numeric(weights, errors="coerce").to_numpy(dtype=float, copy=False)
            valid = np.isfinite(value_arr) & np.isfinite(weight_arr) & (weight_arr > 0.0)
            if not valid.any():
                return float("nan")
            return float(np.average(value_arr[valid], weights=weight_arr[valid]))

        def _summarize_group(group: pd.DataFrame) -> pd.Series:
            support = group[group["is_support_window"]].copy()
            tested = group[group["is_tested_window"]].copy()
            support_z = pd.to_numeric(support["z_score_primary"], errors="coerce")
            support_delta = pd.to_numeric(support["delta_pro_rate_primary"], errors="coerce")
            support_expected = pd.to_numeric(support["expected_pro_rate_primary"], errors="coerce")
            support_n_known = pd.to_numeric(support["n_known"], errors="coerce")
            support_n_pro = pd.to_numeric(support["n_pro"], errors="coerce")

            tested_n_known = pd.to_numeric(tested["n_known"], errors="coerce")
            n_known_support = float(support_n_known.fillna(0.0).sum())
            n_pro_support = float(support_n_pro.fillna(0.0).sum())
            n_known_tested = float(tested_n_known.fillna(0.0).sum())
            n_windows_support = int(len(support))
            n_windows_tested = int(len(tested))
            n_primary_alert = int(
                (tested["is_primary_alert_window"].fillna(False).astype(bool)).sum()
            )
            n_windows = int(len(group))
            n_alert_eligible = int(group["is_alert_off_hours_window"].fillna(False).sum())

            return pd.Series(
                {
                    "n_windows": n_windows,
                    "n_windows_alert_eligible": n_alert_eligible,
                    "n_windows_tested": n_windows_tested,
                    "n_windows_low_power": max(0, n_alert_eligible - n_windows_tested),
                    "n_windows_primary_alert": n_primary_alert,
                    "primary_alert_fraction_tested": (
                        float(n_primary_alert / n_windows_tested)
                        if n_windows_tested > 0
                        else float("nan")
                    ),
                    "n_total": float(
                        pd.to_numeric(group["n_total"], errors="coerce").fillna(0.0).sum()
                    ),
                    "n_known": float(
                        pd.to_numeric(group["n_known"], errors="coerce").fillna(0.0).sum()
                    ),
                    "n_pro": float(
                        pd.to_numeric(group["n_pro"], errors="coerce").fillna(0.0).sum()
                    ),
                    "n_con": float(
                        pd.to_numeric(group["n_con"], errors="coerce").fillna(0.0).sum()
                    ),
                    "n_known_tested": n_known_tested,
                    "off_hours_fraction": float(
                        pd.to_numeric(group["off_hours_fraction"], errors="coerce")
                        .dropna()
                        .mean()
                    )
                    if pd.to_numeric(group["off_hours_fraction"], errors="coerce").notna().any()
                    else float("nan"),
                    "pro_rate": (
                        float(n_pro_support / n_known_support)
                        if n_known_support > 0.0
                        else float("nan")
                    ),
                    "expected_pro_rate_primary": _weighted_mean(
                        support_expected, support_n_known
                    ),
                    "delta_pro_rate_primary": _weighted_mean(support_delta, support_n_known),
                    "z_score_primary": (
                        float(support_z.mean()) if support_z.notna().any() else float("nan")
                    ),
                    "z_score_primary_median": (
                        float(support_z.median()) if support_z.notna().any() else float("nan")
                    ),
                    "z_score_primary_abs_max": (
                        float(support_z.abs().max()) if support_z.notna().any() else float("nan")
                    ),
                    "is_low_power": bool(n_windows_support <= 0),
                }
            )

        grouped_rows: list[dict[str, object]] = []
        for (bucket_minutes, date, day_of_week, hour), group in frame.groupby(
            ["bucket_minutes", "date", "day_of_week", "hour"],
            dropna=False,
            sort=True,
        ):
            row = _summarize_group(group).to_dict()
            row["bucket_minutes"] = int(bucket_minutes)
            row["date"] = date
            row["day_of_week"] = day_of_week
            row["hour"] = hour
            grouped_rows.append(row)

        if not grouped_rows:
            return pd.DataFrame()

        grouped = (
            pd.DataFrame(grouped_rows)
            .sort_values(["bucket_minutes", "date", "hour"])
            .reset_index(drop=True)
        )
        for int_column in (
            "bucket_minutes",
            "hour",
            "n_windows",
            "n_windows_alert_eligible",
            "n_windows_tested",
            "n_windows_low_power",
            "n_windows_primary_alert",
            "n_total",
            "n_known",
            "n_pro",
            "n_con",
            "n_known_tested",
        ):
            grouped[int_column] = (
                pd.to_numeric(grouped[int_column], errors="coerce").fillna(0).astype(int)
            )
        return grouped

    def _select_primary_bucket(self, profile: pd.DataFrame) -> tuple[int | None, pd.DataFrame]:
        if profile.empty:
            return None, pd.DataFrame()
        available = sorted({int(value) for value in profile["bucket_minutes"].dropna().astype(int)})
        if not available:
            return None, pd.DataFrame()
        if self.primary_bucket_minutes in available:
            target = self.primary_bucket_minutes
        elif 30 in available:
            target = 30
        else:
            target = available[0]
        return target, profile[profile["bucket_minutes"] == target].copy()

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        _ = features
        if df.empty:
            empty = pd.DataFrame()
            return DetectorResult(
                detector=self.name,
                summary={"off_hours_ratio": 0.0, "chi_square_p_value": 1.0},
                tables={
                    "off_hours_summary": empty,
                    "hourly_distribution": empty,
                    "hour_of_week_distribution": empty,
                    "date_hour_distribution": empty,
                    "date_hour_primary_residual_distribution": empty,
                    "window_control_profile": empty,
                    "model_fit_diagnostics": empty,
                    "flag_channel_summary": empty,
                    "flagged_window_diagnostics": empty,
                },
            )

        working = df.copy()
        if "is_off_hours" not in working.columns:
            working["is_off_hours"] = False
        working["is_pro"] = working["position_normalized"] == "Pro"
        working["is_con"] = working["position_normalized"] == "Con"

        off_hours = working[working["is_off_hours"]]
        on_hours = working[~working["is_off_hours"]]

        total = int(len(working))
        off_count = int(len(off_hours))
        on_count = int(len(on_hours))

        off_pro = int(off_hours["is_pro"].sum())
        off_con = int(off_hours["is_con"].sum())
        on_pro = int(on_hours["is_pro"].sum())
        on_con = int(on_hours["is_con"].sum())
        off_known_total = int(off_pro + off_con)
        on_known_total = int(on_pro + on_con)
        low_power_min_total = int(self.min_window_total)

        p_value = 1.0
        contingency = np.array([[off_pro, off_con], [on_pro, on_con]], dtype=float)
        if (
            contingency.sum() > 0
            and contingency.shape == (2, 2)
            and (contingency.sum(axis=1) > 0).all()
        ):
            _chi2, p_value, _dof, _expected = chi2_contingency(contingency, correction=False)

        summary_table = pd.DataFrame(
            [
                {
                    "total": total,
                    "off_hours": off_count,
                    "on_hours": on_count,
                    "off_hours_ratio": (off_count / total) if total else 0.0,
                    "off_hours_pro_rate": (off_pro / off_known_total)
                    if off_known_total
                    else np.nan,
                    "on_hours_pro_rate": (on_pro / on_known_total) if on_known_total else np.nan,
                    "chi_square_p_value": p_value,
                }
            ]
        )
        off_low, off_high = wilson_interval(
            successes=pd.Series([off_pro]),
            totals=pd.Series([off_known_total]),
        )
        on_low, on_high = wilson_interval(
            successes=pd.Series([on_pro]),
            totals=pd.Series([on_known_total]),
        )
        summary_table["off_hours_pro_rate_wilson_low"] = float(off_low[0])
        summary_table["off_hours_pro_rate_wilson_high"] = float(off_high[0])
        summary_table["off_hours_pro_rate_wilson_half_width"] = float(
            wilson_half_width(
                successes=pd.Series([off_pro]),
                totals=pd.Series([off_known_total]),
            )[0]
        )
        summary_table["on_hours_pro_rate_wilson_low"] = float(on_low[0])
        summary_table["on_hours_pro_rate_wilson_high"] = float(on_high[0])
        summary_table["on_hours_pro_rate_wilson_half_width"] = float(
            wilson_half_width(
                successes=pd.Series([on_pro]),
                totals=pd.Series([on_known_total]),
            )[0]
        )
        summary_table["off_hours_is_low_power"] = bool(
            low_power_mask(
                totals=pd.Series([off_known_total]),
                min_total=low_power_min_total,
            )[0]
        )
        summary_table["on_hours_is_low_power"] = bool(
            low_power_mask(
                totals=pd.Series([on_known_total]),
                min_total=low_power_min_total,
            )[0]
        )

        window_control_profile = self._build_window_control_profile(working)
        primary_bucket_minutes, primary_bucket_profile = self._select_primary_bucket(
            window_control_profile
        )
        if primary_bucket_minutes is None:
            summary_table["primary_bucket_minutes"] = np.nan
            summary_table["primary_baseline_method"] = "unavailable"
            summary_table["primary_model_fit_method"] = "unavailable"
            summary_table["primary_model_fit_rows"] = 0
            summary_table["primary_model_fit_unique_days"] = 0
            summary_table["primary_model_fit_unique_hours"] = 0
            summary_table["primary_model_fit_converged"] = np.nan
            summary_table["primary_model_fit_aic"] = np.nan
            summary_table["alert_off_hours_min_fraction"] = float(
                self.alert_off_hours_min_fraction
            )
            summary_table["primary_alert_min_abs_delta"] = float(self.primary_alert_min_abs_delta)
            summary_table["off_hours_windows_alert_eligible"] = 0
            summary_table["off_hours_windows_alert_eligible_low_power"] = 0
            summary_table["off_hours_windows_alert_eligible_tested_fraction"] = np.nan
            summary_table["off_hours_windows_alert_eligible_low_power_fraction"] = np.nan
            summary_table["off_hours_windows_tested"] = 0
            summary_table["off_hours_windows_below_day_control_95"] = 0
            summary_table["off_hours_windows_below_day_control_998"] = 0
            summary_table["off_hours_windows_below_model_control_95"] = 0
            summary_table["off_hours_windows_below_model_control_998"] = 0
            summary_table["off_hours_windows_below_primary_control_95"] = 0
            summary_table["off_hours_windows_below_primary_control_998"] = 0
            summary_table["off_hours_windows_above_primary_control_95"] = 0
            summary_table["off_hours_windows_above_primary_control_998"] = 0
            summary_table["off_hours_windows_significant_day"] = 0
            summary_table["off_hours_windows_significant_model"] = 0
            summary_table["off_hours_windows_significant_primary"] = 0
            summary_table["off_hours_windows_significant_primary_upper"] = 0
            summary_table["off_hours_windows_significant_primary_two_sided"] = 0
            summary_table["off_hours_windows_primary_spc_998_any"] = 0
            summary_table["off_hours_windows_primary_fdr_two_sided"] = 0
            summary_table["off_hours_windows_primary_flag_any"] = 0
            summary_table["off_hours_windows_primary_flag_both"] = 0
            summary_table["off_hours_windows_primary_spc_998_any_fraction"] = np.nan
            summary_table["off_hours_windows_primary_fdr_two_sided_fraction"] = np.nan
            summary_table["off_hours_windows_primary_flag_any_fraction"] = np.nan
            summary_table["off_hours_windows_primary_flag_both_fraction"] = np.nan
            summary_table["off_hours_windows_primary_alert"] = 0
            summary_table["off_hours_windows_primary_alert_fraction"] = np.nan
            summary_table["off_hours_primary_alert_run_count"] = 0
            summary_table["off_hours_primary_alert_max_run_windows"] = 0
            summary_table["off_hours_primary_alert_max_run_minutes"] = 0
            summary_table["off_hours_min_day_z"] = np.nan
            summary_table["off_hours_max_abs_day_z"] = np.nan
            summary_table["off_hours_min_model_z"] = np.nan
            summary_table["off_hours_max_abs_model_z"] = np.nan
            summary_table["off_hours_min_primary_z"] = np.nan
            summary_table["off_hours_max_abs_primary_z"] = np.nan
            summary_table["off_hours_min_primary_delta"] = np.nan
            summary_table["off_hours_max_abs_primary_delta"] = np.nan
            summary_table["off_hours_windows_model_available"] = 0
            summary_table["global_daytime_pro_rate"] = np.nan
        else:
            alert_eligible_off_hours = primary_bucket_profile[
                primary_bucket_profile["is_alert_off_hours_window"]
                & primary_bucket_profile["pro_rate"].notna()
            ].copy()
            tested_off_hours = alert_eligible_off_hours[
                ~alert_eligible_off_hours["is_low_power"]
            ].copy()
            alert_eligible_count = int(len(alert_eligible_off_hours))
            alert_eligible_low_power_count = int(alert_eligible_off_hours["is_low_power"].sum())
            model_available = tested_off_hours.get(
                "is_model_baseline_available",
                pd.Series(dtype=bool),
            )
            summary_table["primary_bucket_minutes"] = int(primary_bucket_minutes)
            summary_table["alert_off_hours_min_fraction"] = float(
                self.alert_off_hours_min_fraction
            )
            summary_table["primary_alert_min_abs_delta"] = float(self.primary_alert_min_abs_delta)
            summary_table["primary_baseline_method"] = (
                "model_day_hour"
                if bool(model_available.any())
                else "day_on_hours_fallback"
            )
            if "model_fit_method" in primary_bucket_profile.columns:
                primary_fit_method_series = primary_bucket_profile["model_fit_method"].astype("string")
                primary_fit_method = (
                    str(primary_fit_method_series.dropna().iloc[0]).strip()
                    if primary_fit_method_series.notna().any()
                    else "unavailable"
                )
            else:
                primary_fit_method = "unavailable"
            summary_table["primary_model_fit_method"] = primary_fit_method or "unavailable"
            summary_table["primary_model_fit_rows"] = int(
                pd.to_numeric(
                    primary_bucket_profile.get("model_fit_rows", pd.Series(dtype=float)),
                    errors="coerce",
                )
                .fillna(0)
                .max()
            )
            summary_table["primary_model_fit_unique_days"] = int(
                pd.to_numeric(
                    primary_bucket_profile.get("model_fit_unique_days", pd.Series(dtype=float)),
                    errors="coerce",
                )
                .fillna(0)
                .max()
            )
            summary_table["primary_model_fit_unique_hours"] = int(
                pd.to_numeric(
                    primary_bucket_profile.get("model_fit_unique_hours", pd.Series(dtype=float)),
                    errors="coerce",
                )
                .fillna(0)
                .max()
            )
            summary_table["primary_model_fit_converged"] = float(
                pd.to_numeric(
                    primary_bucket_profile.get("model_fit_converged", pd.Series(dtype=float)),
                    errors="coerce",
                )
                .dropna()
                .iloc[0]
            ) if pd.to_numeric(
                primary_bucket_profile.get("model_fit_converged", pd.Series(dtype=float)),
                errors="coerce",
            ).notna().any() else np.nan
            summary_table["primary_model_fit_aic"] = float(
                pd.to_numeric(
                    primary_bucket_profile.get("model_fit_aic", pd.Series(dtype=float)),
                    errors="coerce",
                )
                .dropna()
                .iloc[0]
            ) if pd.to_numeric(
                primary_bucket_profile.get("model_fit_aic", pd.Series(dtype=float)),
                errors="coerce",
            ).notna().any() else np.nan
            summary_table["off_hours_windows_alert_eligible"] = alert_eligible_count
            summary_table["off_hours_windows_alert_eligible_low_power"] = (
                alert_eligible_low_power_count
            )
            summary_table["off_hours_windows_alert_eligible_tested_fraction"] = (
                (len(tested_off_hours) / alert_eligible_count)
                if alert_eligible_count > 0
                else np.nan
            )
            summary_table["off_hours_windows_alert_eligible_low_power_fraction"] = (
                (alert_eligible_low_power_count / alert_eligible_count)
                if alert_eligible_count > 0
                else np.nan
            )
            summary_table["off_hours_windows_tested"] = int(len(tested_off_hours))
            summary_table["off_hours_windows_below_day_control_95"] = int(
                tested_off_hours["is_below_day_control_95"].sum()
            )
            summary_table["off_hours_windows_below_day_control_998"] = int(
                tested_off_hours["is_below_day_control_998"].sum()
            )
            summary_table["off_hours_windows_below_model_control_95"] = int(
                tested_off_hours["is_below_model_control_95"].sum()
            )
            summary_table["off_hours_windows_below_model_control_998"] = int(
                tested_off_hours["is_below_model_control_998"].sum()
            )
            summary_table["off_hours_windows_below_primary_control_95"] = int(
                tested_off_hours["is_below_primary_control_95"].sum()
            )
            summary_table["off_hours_windows_below_primary_control_998"] = int(
                tested_off_hours["is_below_primary_control_998"].sum()
            )
            summary_table["off_hours_windows_above_primary_control_95"] = int(
                tested_off_hours["is_above_primary_control_95"].sum()
            )
            summary_table["off_hours_windows_above_primary_control_998"] = int(
                tested_off_hours["is_above_primary_control_998"].sum()
            )
            summary_table["off_hours_windows_significant_day"] = int(
                tested_off_hours["is_significant_day"].sum()
            )
            summary_table["off_hours_windows_significant_model"] = int(
                tested_off_hours["is_significant_model"].sum()
            )
            summary_table["off_hours_windows_significant_primary"] = int(
                tested_off_hours["is_significant_primary"].sum()
            )
            summary_table["off_hours_windows_significant_primary_upper"] = int(
                tested_off_hours["is_significant_primary_upper"].sum()
            )
            summary_table["off_hours_windows_significant_primary_two_sided"] = int(
                tested_off_hours["is_significant_primary_two_sided"].sum()
            )
            primary_spc_998_any = int(tested_off_hours["is_primary_spc_998_two_sided"].sum())
            primary_fdr_two_sided = int(tested_off_hours["is_primary_fdr_two_sided"].sum())
            primary_any_channel = int(tested_off_hours["is_primary_any_flag_channel"].sum())
            primary_both_channels = int(tested_off_hours["is_primary_both_flag_channels"].sum())
            tested_window_count = int(len(tested_off_hours))
            summary_table["off_hours_windows_primary_spc_998_any"] = primary_spc_998_any
            summary_table["off_hours_windows_primary_fdr_two_sided"] = primary_fdr_two_sided
            summary_table["off_hours_windows_primary_flag_any"] = primary_any_channel
            summary_table["off_hours_windows_primary_flag_both"] = primary_both_channels
            summary_table["off_hours_windows_primary_spc_998_any_fraction"] = (
                (primary_spc_998_any / tested_window_count) if tested_window_count > 0 else np.nan
            )
            summary_table["off_hours_windows_primary_fdr_two_sided_fraction"] = (
                (primary_fdr_two_sided / tested_window_count) if tested_window_count > 0 else np.nan
            )
            summary_table["off_hours_windows_primary_flag_any_fraction"] = (
                (primary_any_channel / tested_window_count) if tested_window_count > 0 else np.nan
            )
            summary_table["off_hours_windows_primary_flag_both_fraction"] = (
                (primary_both_channels / tested_window_count)
                if tested_window_count > 0
                else np.nan
            )
            primary_alert_windows = int(tested_off_hours["is_primary_alert_window"].sum())
            summary_table["off_hours_windows_primary_alert"] = primary_alert_windows
            summary_table["off_hours_windows_primary_alert_fraction"] = (
                (primary_alert_windows / tested_window_count)
                if tested_window_count > 0
                else np.nan
            )
            ordered_primary = primary_bucket_profile.sort_values("bucket_start").copy()
            alert_flags = ordered_primary["is_primary_alert_window"].fillna(False).astype(bool)
            bucket_start_values = pd.to_datetime(
                ordered_primary["bucket_start"],
                errors="coerce",
            )
            gap_break = pd.Series(False, index=ordered_primary.index, dtype=bool)
            if bucket_start_values.notna().any():
                expected_gap = pd.Timedelta(minutes=max(1, int(primary_bucket_minutes)) * 2)
                gap_break = (bucket_start_values.diff() > expected_gap).fillna(True)
            run_starts = alert_flags & (~alert_flags.shift(fill_value=False) | gap_break)
            run_ids = run_starts.cumsum()
            run_lengths = alert_flags.groupby(run_ids).sum()
            run_lengths = run_lengths[run_lengths > 0]
            run_count = int(run_lengths.shape[0]) if not run_lengths.empty else 0
            max_run_windows = int(run_lengths.max()) if not run_lengths.empty else 0
            summary_table["off_hours_primary_alert_run_count"] = run_count
            summary_table["off_hours_primary_alert_max_run_windows"] = max_run_windows
            summary_table["off_hours_primary_alert_max_run_minutes"] = int(
                max_run_windows * int(primary_bucket_minutes)
            )
            z_scores = pd.to_numeric(tested_off_hours["z_score_day"], errors="coerce")
            summary_table["off_hours_min_day_z"] = (
                float(z_scores.min()) if z_scores.notna().any() else np.nan
            )
            summary_table["off_hours_max_abs_day_z"] = (
                float(z_scores.abs().max()) if z_scores.notna().any() else np.nan
            )
            z_model_scores = pd.to_numeric(tested_off_hours["z_score_model"], errors="coerce")
            summary_table["off_hours_min_model_z"] = (
                float(z_model_scores.min()) if z_model_scores.notna().any() else np.nan
            )
            summary_table["off_hours_max_abs_model_z"] = (
                float(z_model_scores.abs().max()) if z_model_scores.notna().any() else np.nan
            )
            z_primary_scores = pd.to_numeric(tested_off_hours["z_score_primary"], errors="coerce")
            summary_table["off_hours_min_primary_z"] = (
                float(z_primary_scores.min()) if z_primary_scores.notna().any() else np.nan
            )
            summary_table["off_hours_max_abs_primary_z"] = (
                float(z_primary_scores.abs().max()) if z_primary_scores.notna().any() else np.nan
            )
            delta_primary = pd.to_numeric(
                tested_off_hours["delta_pro_rate_primary"],
                errors="coerce",
            )
            summary_table["off_hours_min_primary_delta"] = (
                float(delta_primary.min()) if delta_primary.notna().any() else np.nan
            )
            summary_table["off_hours_max_abs_primary_delta"] = (
                float(delta_primary.abs().max()) if delta_primary.notna().any() else np.nan
            )
            summary_table["off_hours_windows_model_available"] = int(
                tested_off_hours["is_model_baseline_available"].sum()
            )
            global_rate = pd.to_numeric(
                primary_bucket_profile["expected_pro_rate_global"],
                errors="coerce",
            )
            summary_table["global_daytime_pro_rate"] = (
                float(global_rate.dropna().iloc[0]) if global_rate.notna().any() else np.nan
            )
        summary_table["day_adjusted_fdr_alpha"] = float(self.fdr_alpha)
        summary_table["model_fit_min_rows"] = int(self.model_min_rows)
        summary_table["model_hour_harmonics"] = int(self.model_hour_harmonics)

        hourly_distribution = (
            working.groupby("hour", dropna=True)
            .agg(
                n_total=("is_pro", "size"),
                n_pro=("is_pro", "sum"),
                n_con=("is_con", "sum"),
            )
            .reset_index()
            .sort_values("hour")
        )
        hourly_distribution["pro_rate"] = (
            hourly_distribution["n_pro"]
            / (hourly_distribution["n_pro"] + hourly_distribution["n_con"])
        ).where((hourly_distribution["n_pro"] + hourly_distribution["n_con"]) > 0)
        hourly_distribution["pro_rate_wilson_low"], hourly_distribution["pro_rate_wilson_high"] = (
            wilson_interval(
                successes=hourly_distribution["n_pro"],
                totals=hourly_distribution["n_pro"] + hourly_distribution["n_con"],
            )
        )
        hourly_distribution["pro_rate_wilson_half_width"] = wilson_half_width(
            successes=hourly_distribution["n_pro"],
            totals=hourly_distribution["n_pro"] + hourly_distribution["n_con"],
        )
        hourly_distribution["is_low_power"] = low_power_mask(
            totals=hourly_distribution["n_pro"] + hourly_distribution["n_con"],
            min_total=low_power_min_total,
        )

        day_name_lookup = {
            0: "Monday",
            1: "Tuesday",
            2: "Wednesday",
            3: "Thursday",
            4: "Friday",
            5: "Saturday",
            6: "Sunday",
        }
        day_index_lookup = {value: key for key, value in day_name_lookup.items()}

        day_labels = pd.Series(["Unknown"] * len(working), index=working.index, dtype="string")
        day_indices = pd.Series([-1] * len(working), index=working.index, dtype="int64")
        timestamps = (
            pd.to_datetime(working["timestamp"], errors="coerce")
            if "timestamp" in working.columns
            else pd.Series(pd.NaT, index=working.index)
        )
        has_valid_timestamps = bool(timestamps.notna().any())
        if has_valid_timestamps:
            day_labels = timestamps.dt.day_name().fillna("Unknown")
            day_indices = timestamps.dt.dayofweek.fillna(-1).astype(int)
        elif "day_of_week" in working.columns:
            day_values = working["day_of_week"]
            numeric_days = pd.to_numeric(day_values, errors="coerce")
            numeric_mask = numeric_days.notna()
            if numeric_mask.any():
                day_labels = numeric_days.astype("Int64").map(day_name_lookup).fillna("Unknown")
                day_indices = numeric_days.fillna(-1).astype(int)
            else:
                text_days = day_values.astype("string").fillna("Unknown")
                normalized_days = text_days.str.strip().str.title()
                day_labels = normalized_days.where(normalized_days != "", "Unknown")
                day_indices = day_labels.map(day_index_lookup).fillna(-1).astype(int)

        hour_of_week_distribution = (
            working.assign(
                day_of_week=day_labels,
                day_of_week_index=day_indices,
            )
            .groupby(["day_of_week", "day_of_week_index", "hour"], dropna=False)
            .agg(
                n_total=("is_pro", "size"),
                n_pro=("is_pro", "sum"),
                n_con=("is_con", "sum"),
                n_off_hours=("is_off_hours", "sum"),
            )
            .reset_index()
            .sort_values(["day_of_week_index", "hour", "day_of_week"])
        )
        hour_of_week_distribution["off_hours_fraction"] = (
            hour_of_week_distribution["n_off_hours"]
            / hour_of_week_distribution["n_total"].replace(0, np.nan)
        ).fillna(0.0)
        hour_of_week_distribution["pro_rate"] = (
            hour_of_week_distribution["n_pro"]
            / (hour_of_week_distribution["n_pro"] + hour_of_week_distribution["n_con"])
        ).where((hour_of_week_distribution["n_pro"] + hour_of_week_distribution["n_con"]) > 0)
        (
            hour_of_week_distribution["pro_rate_wilson_low"],
            hour_of_week_distribution["pro_rate_wilson_high"],
        ) = wilson_interval(
            successes=hour_of_week_distribution["n_pro"],
            totals=hour_of_week_distribution["n_pro"] + hour_of_week_distribution["n_con"],
        )
        hour_of_week_distribution["is_low_power"] = low_power_mask(
            totals=hour_of_week_distribution["n_pro"] + hour_of_week_distribution["n_con"],
            min_total=low_power_min_total,
        )
        hour_of_week_distribution = hour_of_week_distribution[
            hour_of_week_distribution["day_of_week"] != "Unknown"
        ].copy()
        date_hour_distribution = self._build_date_hour_distribution(working)
        date_hour_primary_residual_distribution = (
            self._build_date_hour_primary_residual_distribution(window_control_profile)
        )
        model_fit_diagnostics_columns = [
            "bucket_minutes",
            "model_fit_method",
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_converged",
            "model_fit_aic",
            "model_fit_used_harmonics",
            "model_fit_window_count",
            "model_fit_available_windows",
            "model_fit_available_fraction",
        ]
        model_fit_diagnostics = pd.DataFrame(columns=model_fit_diagnostics_columns)
        if not window_control_profile.empty:
            model_fit_diagnostics = (
                window_control_profile.sort_values(["bucket_minutes", "bucket_start"])
                .groupby("bucket_minutes", as_index=False)
                .agg(
                    model_fit_method=("model_fit_method", "first"),
                    model_fit_rows=("model_fit_rows", "max"),
                    model_fit_unique_days=("model_fit_unique_days", "max"),
                    model_fit_unique_hours=("model_fit_unique_hours", "max"),
                    model_fit_converged=("model_fit_converged", "first"),
                    model_fit_aic=("model_fit_aic", "first"),
                    model_fit_used_harmonics=("model_fit_used_harmonics", "max"),
                    model_fit_window_count=("bucket_start", "count"),
                    model_fit_available_windows=("is_model_baseline_available", "sum"),
                )
                .sort_values("bucket_minutes")
                .reset_index(drop=True)
            )
            model_fit_diagnostics["model_fit_available_fraction"] = (
                pd.to_numeric(
                    model_fit_diagnostics["model_fit_available_windows"],
                    errors="coerce",
                )
                / pd.to_numeric(model_fit_diagnostics["model_fit_window_count"], errors="coerce")
            )
            for column_name in (
                "model_fit_rows",
                "model_fit_unique_days",
                "model_fit_unique_hours",
                "model_fit_used_harmonics",
                "model_fit_window_count",
                "model_fit_available_windows",
            ):
                model_fit_diagnostics[column_name] = pd.to_numeric(
                    model_fit_diagnostics[column_name],
                    errors="coerce",
                ).fillna(0).astype(int)
        tested_windows = _safe_int(summary_table.loc[0, "off_hours_windows_tested"])

        def _share_of_tested(count: int) -> float:
            if tested_windows <= 0:
                return float("nan")
            return float(count / tested_windows)

        channel_rows = [
            {
                "rank": 1,
                "channel": "tested_off_hours_windows",
                "channel_label": "Tested off-hours windows",
                "count": tested_windows,
                "share_of_tested": 1.0 if tested_windows > 0 else np.nan,
            },
            {
                "rank": 2,
                "channel": "primary_spc_998_two_sided",
                "channel_label": "Primary 99.8% breach (two-sided)",
                "count": _safe_int(summary_table.loc[0, "off_hours_windows_primary_spc_998_any"]),
                "share_of_tested": _share_of_tested(
                    _safe_int(summary_table.loc[0, "off_hours_windows_primary_spc_998_any"])
                ),
            },
            {
                "rank": 3,
                "channel": "primary_fdr_two_sided",
                "channel_label": "Primary two-sided FDR-significant",
                "count": _safe_int(summary_table.loc[0, "off_hours_windows_primary_fdr_two_sided"]),
                "share_of_tested": _share_of_tested(
                    _safe_int(summary_table.loc[0, "off_hours_windows_primary_fdr_two_sided"])
                ),
            },
            {
                "rank": 4,
                "channel": "primary_any_flag_channel",
                "channel_label": "Any primary flag channel",
                "count": _safe_int(summary_table.loc[0, "off_hours_windows_primary_flag_any"]),
                "share_of_tested": _share_of_tested(
                    _safe_int(summary_table.loc[0, "off_hours_windows_primary_flag_any"])
                ),
            },
            {
                "rank": 5,
                "channel": "primary_both_flag_channels",
                "channel_label": "Both primary flag channels",
                "count": _safe_int(summary_table.loc[0, "off_hours_windows_primary_flag_both"]),
                "share_of_tested": _share_of_tested(
                    _safe_int(summary_table.loc[0, "off_hours_windows_primary_flag_both"])
                ),
            },
            {
                "rank": 6,
                "channel": "robust_primary_alert",
                "channel_label": "Robust primary alerts",
                "count": _safe_int(summary_table.loc[0, "off_hours_windows_primary_alert"]),
                "share_of_tested": _share_of_tested(
                    _safe_int(summary_table.loc[0, "off_hours_windows_primary_alert"])
                ),
            },
        ]
        flag_channel_summary = pd.DataFrame(channel_rows)
        flagged_window_columns = [
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "n_known",
            "n_pro",
            "n_con",
            "pro_rate",
            "expected_pro_rate_primary",
            "delta_pro_rate_primary",
            "z_score_primary",
            "p_value_primary_two_sided",
            "q_value_primary_two_sided",
            "is_primary_spc_998_two_sided",
            "is_primary_fdr_two_sided",
            "is_primary_any_flag_channel",
            "is_primary_both_flag_channels",
            "is_primary_alert_window",
            "is_model_baseline_available",
            "model_fit_method",
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_used_harmonics",
            "primary_baseline_source",
            "is_low_power",
        ]
        flagged_window_diagnostics = pd.DataFrame(columns=flagged_window_columns)
        if primary_bucket_minutes is not None and not primary_bucket_profile.empty:
            inferential_primary = primary_bucket_profile[
                primary_bucket_profile["is_alert_off_hours_window"]
                & (~primary_bucket_profile["is_low_power"])
                & primary_bucket_profile["pro_rate"].notna()
            ].copy()
            if not inferential_primary.empty:
                flagged_primary = inferential_primary[
                    inferential_primary["is_primary_any_flag_channel"]
                    | inferential_primary["is_primary_alert_window"]
                ].copy()
                if not flagged_primary.empty:
                    flagged_primary["abs_z_score_primary"] = pd.to_numeric(
                        flagged_primary["z_score_primary"],
                        errors="coerce",
                    ).abs()
                    flagged_primary = flagged_primary.sort_values(
                        [
                            "is_primary_alert_window",
                            "is_primary_both_flag_channels",
                            "is_primary_any_flag_channel",
                            "abs_z_score_primary",
                            "bucket_start",
                        ],
                        ascending=[False, False, False, False, True],
                    )
                    flagged_window_diagnostics = flagged_primary[flagged_window_columns].reset_index(
                        drop=True
                    )

        date_hour_primary_residual_cells = int(len(date_hour_primary_residual_distribution))
        if (
            primary_bucket_minutes is not None
            and not date_hour_primary_residual_distribution.empty
            and "bucket_minutes" in date_hour_primary_residual_distribution.columns
        ):
            primary_bucket_mask = (
                pd.to_numeric(
                    date_hour_primary_residual_distribution["bucket_minutes"],
                    errors="coerce",
                )
                == float(primary_bucket_minutes)
            )
            date_hour_primary_residual_cells = int(primary_bucket_mask.sum())

        return DetectorResult(
            detector=self.name,
            summary={
                "off_hours_ratio": float(summary_table.loc[0, "off_hours_ratio"]),
                "chi_square_p_value": float(p_value),
                "low_power_min_total": int(low_power_min_total),
                "off_hours_is_low_power": bool(summary_table.loc[0, "off_hours_is_low_power"]),
                "on_hours_is_low_power": bool(summary_table.loc[0, "on_hours_is_low_power"]),
                "hour_of_week_cells": int(len(hour_of_week_distribution)),
                "date_hour_cells": int(len(date_hour_distribution)),
                "date_hour_primary_residual_cells": date_hour_primary_residual_cells,
                "window_profile_rows": int(len(window_control_profile)),
                "primary_bucket_minutes": (
                    int(primary_bucket_minutes) if primary_bucket_minutes is not None else None
                ),
                "primary_baseline_method": str(
                    summary_table.loc[0, "primary_baseline_method"]
                ).strip(),
                "alert_off_hours_min_fraction": _safe_float(
                    summary_table.loc[0, "alert_off_hours_min_fraction"],
                    default=self.alert_off_hours_min_fraction,
                ),
                "primary_alert_min_abs_delta": _safe_float(
                    summary_table.loc[0, "primary_alert_min_abs_delta"],
                    default=self.primary_alert_min_abs_delta,
                ),
                "model_hour_harmonics": _safe_int(
                    summary_table.loc[0, "model_hour_harmonics"]
                ),
                "primary_model_fit_method": str(
                    summary_table.loc[0, "primary_model_fit_method"]
                ).strip(),
                "primary_model_fit_rows": _safe_int(
                    summary_table.loc[0, "primary_model_fit_rows"]
                ),
                "primary_model_fit_unique_days": _safe_int(
                    summary_table.loc[0, "primary_model_fit_unique_days"]
                ),
                "primary_model_fit_unique_hours": _safe_int(
                    summary_table.loc[0, "primary_model_fit_unique_hours"]
                ),
                "primary_model_fit_converged": _safe_float(
                    summary_table.loc[0, "primary_model_fit_converged"],
                    default=np.nan,
                ),
                "primary_model_fit_aic": _safe_float(
                    summary_table.loc[0, "primary_model_fit_aic"],
                    default=np.nan,
                ),
                "off_hours_windows_alert_eligible": _safe_int(
                    summary_table.loc[0, "off_hours_windows_alert_eligible"]
                ),
                "off_hours_windows_alert_eligible_low_power": _safe_int(
                    summary_table.loc[0, "off_hours_windows_alert_eligible_low_power"]
                ),
                "off_hours_windows_alert_eligible_tested_fraction": _safe_float(
                    summary_table.loc[0, "off_hours_windows_alert_eligible_tested_fraction"],
                    default=np.nan,
                ),
                "off_hours_windows_alert_eligible_low_power_fraction": _safe_float(
                    summary_table.loc[0, "off_hours_windows_alert_eligible_low_power_fraction"],
                    default=np.nan,
                ),
                "off_hours_windows_tested": _safe_int(
                    summary_table.loc[0, "off_hours_windows_tested"]
                ),
                "off_hours_windows_below_day_control_95": _safe_int(
                    summary_table.loc[0, "off_hours_windows_below_day_control_95"]
                ),
                "off_hours_windows_below_day_control_998": _safe_int(
                    summary_table.loc[0, "off_hours_windows_below_day_control_998"]
                ),
                "off_hours_windows_below_model_control_95": _safe_int(
                    summary_table.loc[0, "off_hours_windows_below_model_control_95"]
                ),
                "off_hours_windows_below_model_control_998": _safe_int(
                    summary_table.loc[0, "off_hours_windows_below_model_control_998"]
                ),
                "off_hours_windows_below_primary_control_95": _safe_int(
                    summary_table.loc[0, "off_hours_windows_below_primary_control_95"]
                ),
                "off_hours_windows_below_primary_control_998": _safe_int(
                    summary_table.loc[0, "off_hours_windows_below_primary_control_998"]
                ),
                "off_hours_windows_significant_primary": _safe_int(
                    summary_table.loc[0, "off_hours_windows_significant_primary"]
                ),
                "off_hours_windows_significant_primary_upper": _safe_int(
                    summary_table.loc[0, "off_hours_windows_significant_primary_upper"]
                ),
                "off_hours_windows_significant_primary_two_sided": _safe_int(
                    summary_table.loc[0, "off_hours_windows_significant_primary_two_sided"]
                ),
                "off_hours_windows_primary_spc_998_any": _safe_int(
                    summary_table.loc[0, "off_hours_windows_primary_spc_998_any"]
                ),
                "off_hours_windows_primary_fdr_two_sided": _safe_int(
                    summary_table.loc[0, "off_hours_windows_primary_fdr_two_sided"]
                ),
                "off_hours_windows_primary_flag_any": _safe_int(
                    summary_table.loc[0, "off_hours_windows_primary_flag_any"]
                ),
                "off_hours_windows_primary_flag_both": _safe_int(
                    summary_table.loc[0, "off_hours_windows_primary_flag_both"]
                ),
                "off_hours_windows_primary_spc_998_any_fraction": _safe_float(
                    summary_table.loc[0, "off_hours_windows_primary_spc_998_any_fraction"],
                    default=np.nan,
                ),
                "off_hours_windows_primary_fdr_two_sided_fraction": _safe_float(
                    summary_table.loc[0, "off_hours_windows_primary_fdr_two_sided_fraction"],
                    default=np.nan,
                ),
                "off_hours_windows_primary_flag_any_fraction": _safe_float(
                    summary_table.loc[0, "off_hours_windows_primary_flag_any_fraction"],
                    default=np.nan,
                ),
                "off_hours_windows_primary_flag_both_fraction": _safe_float(
                    summary_table.loc[0, "off_hours_windows_primary_flag_both_fraction"],
                    default=np.nan,
                ),
                "off_hours_windows_primary_alert": _safe_int(
                    summary_table.loc[0, "off_hours_windows_primary_alert"]
                ),
                "off_hours_windows_primary_alert_fraction": _safe_float(
                    summary_table.loc[0, "off_hours_windows_primary_alert_fraction"],
                    default=np.nan,
                ),
                "off_hours_primary_alert_run_count": _safe_int(
                    summary_table.loc[0, "off_hours_primary_alert_run_count"]
                ),
                "off_hours_primary_alert_max_run_windows": _safe_int(
                    summary_table.loc[0, "off_hours_primary_alert_max_run_windows"]
                ),
                "off_hours_primary_alert_max_run_minutes": _safe_int(
                    summary_table.loc[0, "off_hours_primary_alert_max_run_minutes"]
                ),
                "off_hours_windows_model_available": _safe_int(
                    summary_table.loc[0, "off_hours_windows_model_available"]
                ),
                "max_hour_of_week_pro_rate": float(
                    pd.to_numeric(hour_of_week_distribution["pro_rate"], errors="coerce")
                    .fillna(0.0)
                    .max()
                )
                if not hour_of_week_distribution.empty
                else 0.0,
            },
            tables={
                "off_hours_summary": summary_table,
                "hourly_distribution": hourly_distribution,
                "hour_of_week_distribution": hour_of_week_distribution,
                "date_hour_distribution": date_hour_distribution,
                "date_hour_primary_residual_distribution": (
                    date_hour_primary_residual_distribution
                ),
                "window_control_profile": window_control_profile,
                "model_fit_diagnostics": model_fit_diagnostics,
                "flag_channel_summary": flag_channel_summary,
                "flagged_window_diagnostics": flagged_window_diagnostics,
            },
        )
