from __future__ import annotations

import warnings
from dataclasses import dataclass

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy.stats import binom, binomtest
from statsmodels.stats.multitest import multipletests
from statsmodels.tools.sm_exceptions import PerfectSeparationWarning


@dataclass(frozen=True)
class InferenceConfig:
    min_window_total: int
    fdr_alpha: float
    model_min_rows: int
    model_hour_harmonics: int
    alert_off_hours_min_fraction: float
    primary_alert_min_abs_delta: float


@dataclass(frozen=True)
class ModelFitResult:
    expected_pro_rate: pd.Series
    is_model_baseline_available: pd.Series
    diagnostics: dict[str, object]


def control_limits(
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


def build_design_matrix(
    frame: pd.DataFrame,
    *,
    day_levels: list[str],
    hour_harmonics: int,
) -> pd.DataFrame:
    hour_numeric = pd.to_numeric(frame["hour"], errors="coerce")
    radians = (2.0 * np.pi * hour_numeric) / 24.0
    design = pd.DataFrame({"intercept": 1.0}, index=frame.index)
    for harmonic in range(1, int(hour_harmonics) + 1):
        design[f"hour_sin_{harmonic}"] = np.sin(float(harmonic) * radians)
        design[f"hour_cos_{harmonic}"] = np.cos(float(harmonic) * radians)
    for level in day_levels[1:]:
        column = f"day__{level}"
        design[column] = (frame["event_date_key"] == level).astype(float)
    return design


def fit_model_expected_pro_rate(
    windowed: pd.DataFrame,
    *,
    model_min_rows: int,
    model_hour_harmonics: int,
) -> ModelFitResult:
    diagnostics: dict[str, object] = {
        "model_fit_method": "unavailable",
        "model_fit_rows": 0,
        "model_fit_unique_days": 0,
        "model_fit_unique_hours": 0,
        "model_fit_converged": np.nan,
        "model_fit_aic": np.nan,
        "model_fit_used_harmonics": int(model_hour_harmonics),
    }
    if windowed.empty:
        empty = pd.Series(dtype=float)
        return ModelFitResult(
            expected_pro_rate=empty,
            is_model_baseline_available=pd.Series(dtype=bool),
            diagnostics=diagnostics,
        )

    expected = pd.Series(np.nan, index=windowed.index, dtype=float)
    model_available = pd.Series(False, index=windowed.index, dtype=bool)

    known_mask = pd.to_numeric(windowed["n_known"], errors="coerce").fillna(0.0) > 0.0
    fit_frame = windowed[known_mask].copy()
    if fit_frame.empty:
        diagnostics["model_fit_method"] = "unavailable_no_known_rows"
        return ModelFitResult(expected, model_available, diagnostics)

    fit_frame["hour"] = pd.to_numeric(fit_frame["hour"], errors="coerce")
    fit_frame = fit_frame[fit_frame["hour"].notna()].copy()
    if fit_frame.empty:
        diagnostics["model_fit_method"] = "unavailable_no_valid_hours"
        return ModelFitResult(expected, model_available, diagnostics)

    day_levels = sorted(
        {
            str(value)
            for value in fit_frame["event_date_key"].astype("string").dropna().tolist()
            if str(value).strip()
        }
    )
    if not day_levels:
        diagnostics["model_fit_method"] = "unavailable_no_day_levels"
        return ModelFitResult(expected, model_available, diagnostics)

    diagnostics["model_fit_rows"] = int(len(fit_frame))
    diagnostics["model_fit_unique_days"] = int(len(day_levels))
    diagnostics["model_fit_unique_hours"] = int(fit_frame["hour"].nunique(dropna=True))
    if len(fit_frame) < int(model_min_rows):
        diagnostics["model_fit_method"] = "unavailable_insufficient_rows"
        return ModelFitResult(expected, model_available, diagnostics)
    if fit_frame["hour"].nunique(dropna=True) < 3:
        diagnostics["model_fit_method"] = "unavailable_insufficient_hour_coverage"
        return ModelFitResult(expected, model_available, diagnostics)

    y = (
        pd.to_numeric(fit_frame["n_pro"], errors="coerce")
        / pd.to_numeric(fit_frame["n_known"], errors="coerce")
    ).clip(lower=1e-6, upper=1.0 - 1e-6)
    weights = pd.to_numeric(fit_frame["n_known"], errors="coerce").fillna(0.0).clip(lower=1.0)
    x_fit = build_design_matrix(
        fit_frame,
        day_levels=day_levels,
        hour_harmonics=int(model_hour_harmonics),
    )

    fit_result = None
    fit_method = "unavailable"
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("error", category=PerfectSeparationWarning)
            warnings.filterwarnings(
                "error",
                category=RuntimeWarning,
                module=r"statsmodels\.genmod\.families\..*",
            )
            with np.errstate(over="raise", divide="raise", invalid="raise"):
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
                warnings.filterwarnings(
                    "ignore",
                    category=RuntimeWarning,
                    module=r"statsmodels\.genmod\.families\..*",
                )
                with np.errstate(over="ignore", divide="ignore", invalid="ignore"):
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
            return ModelFitResult(expected, model_available, diagnostics)

    full_frame = windowed.copy()
    full_frame["hour"] = pd.to_numeric(full_frame["hour"], errors="coerce")
    full_frame = full_frame[full_frame["hour"].notna()].copy()
    if full_frame.empty:
        diagnostics["model_fit_method"] = "unavailable_prediction_frame_empty"
        return ModelFitResult(expected, model_available, diagnostics)

    x_full = build_design_matrix(
        full_frame,
        day_levels=day_levels,
        hour_harmonics=int(model_hour_harmonics),
    )

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=RuntimeWarning,
            module=r"statsmodels\.genmod\.families\..*",
        )
        with np.errstate(over="ignore", divide="ignore", invalid="ignore"):
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
    return ModelFitResult(expected, model_available, diagnostics)


def standardized_residual(
    *,
    observed_successes: pd.Series,
    totals: pd.Series,
    expected_rate: pd.Series,
) -> pd.Series:
    n_total = pd.to_numeric(totals, errors="coerce")
    observed = pd.to_numeric(observed_successes, errors="coerce")
    expected = pd.to_numeric(expected_rate, errors="coerce").clip(lower=1e-6, upper=1.0 - 1e-6)

    expected_count = n_total * expected
    variance = n_total * expected * (1.0 - expected)
    valid = (n_total > 0.0) & (variance > 0.0) & expected.notna() & observed.notna()
    return ((observed - expected_count) / np.sqrt(variance)).where(valid)


def exact_binomial_tail_p_values(
    *,
    observed_successes: pd.Series,
    totals: pd.Series,
    expected_rate: pd.Series,
) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    n_total = pd.to_numeric(totals, errors="coerce").fillna(0.0)
    observed = pd.to_numeric(observed_successes, errors="coerce").fillna(0.0)
    expected = pd.to_numeric(expected_rate, errors="coerce").clip(lower=1e-6, upper=1.0 - 1e-6)

    n_total_int = n_total.round().astype(int)
    observed_int = observed.round().astype(int)
    variance = n_total * expected * (1.0 - expected)
    valid = (n_total > 0.0) & (variance > 0.0) & expected.notna() & (n_total_int > 0)

    p_lower = pd.Series(np.nan, index=totals.index, dtype=float)
    p_upper = pd.Series(np.nan, index=totals.index, dtype=float)
    p_two_sided = pd.Series(np.nan, index=totals.index, dtype=float)

    if valid.any():
        valid_idx = valid[valid].index
        k_values = observed_int.loc[valid_idx].to_numpy(dtype=int)
        n_values = n_total_int.loc[valid_idx].to_numpy(dtype=int)
        p_values = expected.loc[valid_idx].to_numpy(dtype=float)
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
                float(binomtest(int(k), int(n), p=float(p), alternative="two-sided").pvalue)
                for k, n, p in zip(k_values, n_values, p_values, strict=False)
            ),
            dtype=float,
            count=len(valid_idx),
        )

    return p_lower, p_upper, p_two_sided, valid


def apply_bh_fdr(
    p_values: pd.Series,
    *,
    alpha: float,
) -> tuple[pd.Series, pd.Series]:
    numeric = pd.to_numeric(p_values, errors="coerce")
    q_values = pd.Series(np.nan, index=p_values.index, dtype=float)
    is_significant = pd.Series(False, index=p_values.index, dtype=bool)

    valid = numeric.notna()
    if not valid.any():
        return q_values, is_significant

    _reject, adjusted, _alphac_sidak, _alphac_bonf = multipletests(
        numeric[valid].to_numpy(dtype=float),
        alpha=float(alpha),
        method="fdr_bh",
    )
    valid_index = numeric[valid].index
    q_values.loc[valid_index] = adjusted
    is_significant.loc[valid_index] = adjusted <= float(alpha)
    return q_values, is_significant
