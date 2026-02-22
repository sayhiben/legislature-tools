from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StatMethodSpec:
    method_id: str
    label: str
    purpose: str
    formula: str
    assumptions: tuple[str, ...]
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    caveats: tuple[str, ...]
    source_columns: tuple[str, ...]


_OFF_HOURS_METHOD_SPECS: tuple[StatMethodSpec, ...] = (
    StatMethodSpec(
        method_id="overall_off_vs_on_chi_square",
        label="Overall Off-vs-On Chi-Square",
        purpose=("Test whether off-hours and on-hours Pro/Con composition differs in aggregate."),
        formula=(
            "Pearson chi-square on 2x2 table [[off_pro, off_con], [on_pro, on_con]] without"
            " continuity correction."
        ),
        assumptions=(
            "Rows are treated as independent records for the contingency test.",
            "Both off-hours and on-hours groups must contain known-position rows.",
        ),
        inputs=(
            "off_pro",
            "off_con",
            "on_pro",
            "on_con",
        ),
        outputs=("chi_square_p_value",),
        caveats=(
            "Large n can produce very small p-values for modest effect sizes.",
            "This is a global aggregate test and does not localize time windows.",
        ),
        source_columns=(
            "position_normalized",
            "is_off_hours",
        ),
    ),
    StatMethodSpec(
        method_id="wilson_interval_low_power",
        label="Wilson Interval and Low-Power Gating",
        purpose=(
            "Quantify binomial uncertainty for Pro-rate estimates and identify sparse-support"
            " windows/cells."
        ),
        formula=(
            "Wilson score interval for binomial proportion p_hat=k/n and low-power mask"
            " n < min_window_total."
        ),
        assumptions=(
            "Known-position records are modeled as binomial trials.",
            "Low-power threshold is a policy gate, not a probabilistic confidence level.",
        ),
        inputs=(
            "n_pro",
            "n_con",
            "min_window_total",
        ),
        outputs=(
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "pro_rate_wilson_half_width",
            "is_low_power",
            "off_hours_pro_rate_wilson_low",
            "off_hours_pro_rate_wilson_high",
            "on_hours_pro_rate_wilson_low",
            "on_hours_pro_rate_wilson_high",
            "off_hours_is_low_power",
            "on_hours_is_low_power",
        ),
        caveats=(
            "Wilson intervals describe uncertainty, not anomaly probability.",
            "Low-power gating can suppress inference in sparse windows.",
        ),
        source_columns=(
            "n_pro",
            "n_con",
        ),
    ),
    StatMethodSpec(
        method_id="day_global_baseline_fallback",
        label="Day/Global Baseline Fallback",
        purpose=(
            "Construct expected Pro-rate baselines using on-hours day-specific rates with"
            " global fallback when support is insufficient."
        ),
        formula=(
            "expected_pro_rate_day = day_on_hours_pro/day_on_hours_known when supported;"
            " otherwise expected_pro_rate_global from all on-hours rows;"
            " fallback to overall known rate when on-hours baseline unavailable."
        ),
        assumptions=(
            "On-hours rows are used as baseline reference for off-hours composition.",
            (
                "Day-level baseline requires adequate support "
                "(day_on_hours_known >= min_window_total)."
            ),
        ),
        inputs=(
            "day_on_hours_pro",
            "day_on_hours_known",
            "global_on_hours_pro_rate",
            "overall_known_pro_rate",
        ),
        outputs=(
            "expected_pro_rate_global",
            "expected_pro_rate_day",
            "baseline_source",
            "day_on_hours_pro_rate",
        ),
        caveats=(
            "Baseline quality depends on representativeness of on-hours composition.",
            "Fallback tiers can mix temporal regimes if day support is sparse.",
        ),
        source_columns=(
            "is_off_hours",
            "position_normalized",
            "bucket_start",
        ),
    ),
    StatMethodSpec(
        method_id="day_fixed_plus_harmonic_hour_glm",
        label="GLM Day Fixed + Harmonic Hour Baseline",
        purpose=(
            "Estimate model-based expected Pro-rate using day fixed effects and harmonic"
            " hour terms with regularized fallback on fit failure."
        ),
        formula=(
            "Binomial GLM with logit link on y=n_pro/n_known, freq_weights=n_known,"
            " predictors: intercept + sin/cos harmonics(hour) + day fixed effects;"
            " fallback to ridge-like fit_regularized(alpha=1e-4, L1_wt=0)."
        ),
        assumptions=(
            "Windowed known counts support weighted binomial regression.",
            "Hour smoothness is approximated via Fourier harmonics.",
            "At least model_min_rows rows and >=3 distinct hours are required for fitting.",
        ),
        inputs=(
            "n_pro",
            "n_known",
            "hour",
            "event_date_key",
            "model_hour_harmonics",
            "model_min_rows",
        ),
        outputs=(
            "expected_pro_rate_model",
            "is_model_baseline_available",
            "model_baseline_source",
            "model_fit_method",
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_converged",
            "model_fit_aic",
            "model_fit_used_harmonics",
        ),
        caveats=(
            "Model availability depends on data support and fit stability.",
            "Fixed effects and harmonics do not capture all hearing-process covariates.",
        ),
        source_columns=(
            "n_pro",
            "n_known",
            "hour",
            "event_date_key",
        ),
    ),
    StatMethodSpec(
        method_id="normal_approx_control_limits",
        label="Normal-Approximation Control Limits",
        purpose=(
            "Build control limits and standardized residual diagnostics around expected"
            " Pro-rates for multiple baseline variants."
        ),
        formula=(
            "Control limits: p_exp ± z * sqrt(p_exp*(1-p_exp)/n) clipped to [0,1],"
            " with z in {1.96, 3.0}; residual z_score=(k-n*p_exp)/sqrt(n*p_exp*(1-p_exp))."
        ),
        assumptions=(
            "Normal approximation is used for control-band diagnostics.",
            "Valid only when n>0 and binomial variance n*p*(1-p)>0.",
        ),
        inputs=(
            "n_pro",
            "n_known",
            "expected_pro_rate_day",
            "expected_pro_rate_model",
            "expected_pro_rate_primary",
            "expected_pro_rate_global",
        ),
        outputs=(
            "control_low_95_day",
            "control_high_95_day",
            "control_low_998_day",
            "control_high_998_day",
            "control_low_95_model",
            "control_high_95_model",
            "control_low_998_model",
            "control_high_998_model",
            "control_low_95_primary",
            "control_high_95_primary",
            "control_low_998_primary",
            "control_high_998_primary",
            "z_score_day",
            "z_score_model",
            "z_score_primary",
            "is_below_day_control_95",
            "is_below_day_control_998",
            "is_above_day_control_95",
            "is_above_day_control_998",
            "is_outside_day_control_95",
            "is_outside_day_control_998",
            "is_below_model_control_95",
            "is_below_model_control_998",
            "is_above_model_control_95",
            "is_above_model_control_998",
            "is_outside_model_control_95",
            "is_outside_model_control_998",
            "is_below_primary_control_95",
            "is_below_primary_control_998",
            "is_above_primary_control_95",
            "is_above_primary_control_998",
            "is_outside_primary_control_95",
            "is_outside_primary_control_998",
            "is_below_global_control_95",
            "is_below_global_control_998",
        ),
        caveats=(
            "Control limits are diagnostic thresholds, not direct posterior probabilities.",
            "Approximation degrades when p is near 0/1 and support is low.",
        ),
        source_columns=(
            "n_pro",
            "n_known",
            "expected_pro_rate_day",
            "expected_pro_rate_model",
            "expected_pro_rate_primary",
            "expected_pro_rate_global",
        ),
    ),
    StatMethodSpec(
        method_id="binomial_exact_tail_tests",
        label="Exact Binomial Tail Tests",
        purpose=(
            "Compute exact lower, upper, and two-sided p-values for observed Pro counts"
            " against expected binomial rate."
        ),
        formula=(
            "Lower tail: BinomCDF(k; n, p_exp), upper tail: BinomSF(k-1; n, p_exp),"
            " two-sided from exact binomtest."
        ),
        assumptions=(
            "Conditioned on expected rate, observed known counts are modeled as binomial.",
            (
                "Inferential p-values are computed only for tested windows "
                "(non-low-power, eligible support)."
            ),
        ),
        inputs=(
            "n_pro",
            "n_known",
            "expected_pro_rate_day",
            "expected_pro_rate_model",
            "expected_pro_rate_primary",
            "is_low_power",
            "is_alert_off_hours_window",
        ),
        outputs=(
            "delta_pro_rate_day",
            "delta_pro_rate_model",
            "delta_pro_rate_primary",
            "p_value_day_lower",
            "p_value_day_upper",
            "p_value_day_two_sided",
            "p_value_model_lower",
            "p_value_model_upper",
            "p_value_model_two_sided",
            "p_value_primary_lower",
            "p_value_primary_upper",
            "p_value_primary_two_sided",
            "p_value_day",
            "p_value_model",
            "p_value_primary",
        ),
        caveats=(
            "Exact p-values still depend on baseline model validity.",
            "Two-sided exact tests can be conservative with discrete counts.",
        ),
        source_columns=(
            "n_pro",
            "n_known",
            "expected_pro_rate_day",
            "expected_pro_rate_model",
            "expected_pro_rate_primary",
        ),
    ),
    StatMethodSpec(
        method_id="bh_fdr_by_bucket_and_tail",
        label="Benjamini-Hochberg FDR by Bucket/Tail",
        purpose=(
            "Control false discovery rate over tested off-hours windows separately by"
            " bucket size, baseline, and tail family."
        ),
        formula=(
            "Apply Benjamini-Hochberg step-up adjustment to p-values within each"
            " (bucket_minutes, baseline, tail) family at alpha=fdr_alpha."
        ),
        assumptions=(
            "Families are defined by bucket and hypothesis tail channel.",
            "Only tested off-hours windows are included in each BH adjustment set.",
        ),
        inputs=(
            "p_value_day_lower",
            "p_value_day_upper",
            "p_value_day_two_sided",
            "p_value_model_lower",
            "p_value_model_upper",
            "p_value_model_two_sided",
            "p_value_primary_lower",
            "p_value_primary_upper",
            "p_value_primary_two_sided",
            "fdr_alpha",
        ),
        outputs=(
            "q_value_day_lower",
            "q_value_day_upper",
            "q_value_day_two_sided",
            "q_value_model_lower",
            "q_value_model_upper",
            "q_value_model_two_sided",
            "q_value_primary_lower",
            "q_value_primary_upper",
            "q_value_primary_two_sided",
            "is_significant_day_lower",
            "is_significant_day_upper",
            "is_significant_day_two_sided",
            "is_significant_model_lower",
            "is_significant_model_upper",
            "is_significant_model_two_sided",
            "is_significant_primary_lower",
            "is_significant_primary_upper",
            "is_significant_primary_two_sided",
            "q_value_day",
            "q_value_model",
            "q_value_primary",
            "is_significant_day",
            "is_significant_model",
            "is_significant_primary",
        ),
        caveats=(
            "FDR control is family-specific and depends on chosen grouping policy.",
            "NaN p-values are excluded from adjustment and remain non-significant.",
        ),
        source_columns=(
            "p_value_day_lower",
            "p_value_day_upper",
            "p_value_day_two_sided",
            "p_value_model_lower",
            "p_value_model_upper",
            "p_value_model_two_sided",
            "p_value_primary_lower",
            "p_value_primary_upper",
            "p_value_primary_two_sided",
        ),
    ),
    StatMethodSpec(
        method_id="primary_alert_decision_rule",
        label="Primary Alert Decision Rule",
        purpose=(
            "Define alert-eligible windows and robust primary alerts by combining support"
            " gating, control-band breach, FDR support, and minimum effect size."
        ),
        formula=(
            "is_primary_alert_window = tested & is_alert_off_hours_window"
            " & is_below_primary_control_998 & is_significant_primary_lower"
            " & (delta_pro_rate_primary <= -primary_alert_min_abs_delta)."
        ),
        assumptions=(
            "Alert logic is conjunctive and intentionally conservative.",
            "Primary baseline uses model when available, day/global fallback otherwise.",
        ),
        inputs=(
            "off_hours_fraction",
            "alert_off_hours_min_fraction",
            "is_low_power",
            "delta_pro_rate_primary",
            "primary_alert_min_abs_delta",
            "is_below_primary_control_998",
            "is_significant_primary_lower",
            "expected_pro_rate_primary",
        ),
        outputs=(
            "off_hours_fraction",
            "is_off_hours_window",
            "is_pure_off_hours_window",
            "is_alert_off_hours_window",
            "expected_pro_rate_primary",
            "primary_baseline_source",
            "is_material_primary_shift",
            "is_material_primary_lower_shift",
            "is_material_primary_upper_shift",
            "is_primary_alert_window",
            "is_primary_spc_998_two_sided",
            "is_primary_fdr_two_sided",
            "is_primary_any_flag_channel",
            "is_primary_both_flag_channels",
        ),
        caveats=(
            "Alert eligibility threshold depends on configured off-hours window fraction.",
            (
                "Primary alerts are support-gated and can be zero when all eligible windows "
                "are low-power."
            ),
        ),
        source_columns=(
            "n_off_hours",
            "n_total",
            "n_known",
            "delta_pro_rate_primary",
            "is_low_power",
            "is_below_primary_control_998",
            "is_significant_primary_lower",
            "expected_pro_rate_primary",
        ),
    ),
)


_OFF_HOURS_COLUMN_METHOD_MAP: dict[str, str] = {
    "chi_square_p_value": "overall_off_vs_on_chi_square",
    "pro_rate_wilson_low": "wilson_interval_low_power",
    "pro_rate_wilson_high": "wilson_interval_low_power",
    "pro_rate_wilson_half_width": "wilson_interval_low_power",
    "is_low_power": "wilson_interval_low_power",
    "off_hours_pro_rate_wilson_low": "wilson_interval_low_power",
    "off_hours_pro_rate_wilson_high": "wilson_interval_low_power",
    "off_hours_pro_rate_wilson_half_width": "wilson_interval_low_power",
    "on_hours_pro_rate_wilson_low": "wilson_interval_low_power",
    "on_hours_pro_rate_wilson_high": "wilson_interval_low_power",
    "on_hours_pro_rate_wilson_half_width": "wilson_interval_low_power",
    "off_hours_is_low_power": "wilson_interval_low_power",
    "on_hours_is_low_power": "wilson_interval_low_power",
    "day_on_hours_pro_rate": "day_global_baseline_fallback",
    "expected_pro_rate_global": "day_global_baseline_fallback",
    "expected_pro_rate_day": "day_global_baseline_fallback",
    "baseline_source": "day_global_baseline_fallback",
    "expected_pro_rate_model": "day_fixed_plus_harmonic_hour_glm",
    "is_model_baseline_available": "day_fixed_plus_harmonic_hour_glm",
    "model_baseline_source": "day_fixed_plus_harmonic_hour_glm",
    "model_fit_method": "day_fixed_plus_harmonic_hour_glm",
    "model_fit_rows": "day_fixed_plus_harmonic_hour_glm",
    "model_fit_unique_days": "day_fixed_plus_harmonic_hour_glm",
    "model_fit_unique_hours": "day_fixed_plus_harmonic_hour_glm",
    "model_fit_converged": "day_fixed_plus_harmonic_hour_glm",
    "model_fit_aic": "day_fixed_plus_harmonic_hour_glm",
    "model_fit_used_harmonics": "day_fixed_plus_harmonic_hour_glm",
    "control_low_95_day": "normal_approx_control_limits",
    "control_high_95_day": "normal_approx_control_limits",
    "control_low_998_day": "normal_approx_control_limits",
    "control_high_998_day": "normal_approx_control_limits",
    "control_low_95_global": "normal_approx_control_limits",
    "control_high_95_global": "normal_approx_control_limits",
    "control_low_998_global": "normal_approx_control_limits",
    "control_high_998_global": "normal_approx_control_limits",
    "control_low_95_model": "normal_approx_control_limits",
    "control_high_95_model": "normal_approx_control_limits",
    "control_low_998_model": "normal_approx_control_limits",
    "control_high_998_model": "normal_approx_control_limits",
    "control_low_95_primary": "normal_approx_control_limits",
    "control_high_95_primary": "normal_approx_control_limits",
    "control_low_998_primary": "normal_approx_control_limits",
    "control_high_998_primary": "normal_approx_control_limits",
    "z_score_day": "normal_approx_control_limits",
    "z_score_model": "normal_approx_control_limits",
    "z_score_primary": "normal_approx_control_limits",
    "is_below_day_control_95": "normal_approx_control_limits",
    "is_below_day_control_998": "normal_approx_control_limits",
    "is_above_day_control_95": "normal_approx_control_limits",
    "is_above_day_control_998": "normal_approx_control_limits",
    "is_outside_day_control_95": "normal_approx_control_limits",
    "is_outside_day_control_998": "normal_approx_control_limits",
    "is_below_model_control_95": "normal_approx_control_limits",
    "is_below_model_control_998": "normal_approx_control_limits",
    "is_above_model_control_95": "normal_approx_control_limits",
    "is_above_model_control_998": "normal_approx_control_limits",
    "is_outside_model_control_95": "normal_approx_control_limits",
    "is_outside_model_control_998": "normal_approx_control_limits",
    "is_below_primary_control_95": "normal_approx_control_limits",
    "is_below_primary_control_998": "normal_approx_control_limits",
    "is_above_primary_control_95": "normal_approx_control_limits",
    "is_above_primary_control_998": "normal_approx_control_limits",
    "is_outside_primary_control_95": "normal_approx_control_limits",
    "is_outside_primary_control_998": "normal_approx_control_limits",
    "is_below_global_control_95": "normal_approx_control_limits",
    "is_below_global_control_998": "normal_approx_control_limits",
    "delta_pro_rate_day": "binomial_exact_tail_tests",
    "delta_pro_rate_model": "binomial_exact_tail_tests",
    "delta_pro_rate_primary": "binomial_exact_tail_tests",
    "p_value_day_lower": "binomial_exact_tail_tests",
    "p_value_day_upper": "binomial_exact_tail_tests",
    "p_value_day_two_sided": "binomial_exact_tail_tests",
    "p_value_model_lower": "binomial_exact_tail_tests",
    "p_value_model_upper": "binomial_exact_tail_tests",
    "p_value_model_two_sided": "binomial_exact_tail_tests",
    "p_value_primary_lower": "binomial_exact_tail_tests",
    "p_value_primary_upper": "binomial_exact_tail_tests",
    "p_value_primary_two_sided": "binomial_exact_tail_tests",
    "p_value_day": "binomial_exact_tail_tests",
    "p_value_model": "binomial_exact_tail_tests",
    "p_value_primary": "binomial_exact_tail_tests",
    "q_value_day_lower": "bh_fdr_by_bucket_and_tail",
    "q_value_day_upper": "bh_fdr_by_bucket_and_tail",
    "q_value_day_two_sided": "bh_fdr_by_bucket_and_tail",
    "q_value_model_lower": "bh_fdr_by_bucket_and_tail",
    "q_value_model_upper": "bh_fdr_by_bucket_and_tail",
    "q_value_model_two_sided": "bh_fdr_by_bucket_and_tail",
    "q_value_primary_lower": "bh_fdr_by_bucket_and_tail",
    "q_value_primary_upper": "bh_fdr_by_bucket_and_tail",
    "q_value_primary_two_sided": "bh_fdr_by_bucket_and_tail",
    "is_significant_day_lower": "bh_fdr_by_bucket_and_tail",
    "is_significant_day_upper": "bh_fdr_by_bucket_and_tail",
    "is_significant_day_two_sided": "bh_fdr_by_bucket_and_tail",
    "is_significant_model_lower": "bh_fdr_by_bucket_and_tail",
    "is_significant_model_upper": "bh_fdr_by_bucket_and_tail",
    "is_significant_model_two_sided": "bh_fdr_by_bucket_and_tail",
    "is_significant_primary_lower": "bh_fdr_by_bucket_and_tail",
    "is_significant_primary_upper": "bh_fdr_by_bucket_and_tail",
    "is_significant_primary_two_sided": "bh_fdr_by_bucket_and_tail",
    "q_value_day": "bh_fdr_by_bucket_and_tail",
    "q_value_model": "bh_fdr_by_bucket_and_tail",
    "q_value_primary": "bh_fdr_by_bucket_and_tail",
    "is_significant_day": "bh_fdr_by_bucket_and_tail",
    "is_significant_model": "bh_fdr_by_bucket_and_tail",
    "is_significant_primary": "bh_fdr_by_bucket_and_tail",
    "off_hours_fraction": "primary_alert_decision_rule",
    "is_off_hours_window": "primary_alert_decision_rule",
    "is_pure_off_hours_window": "primary_alert_decision_rule",
    "is_alert_off_hours_window": "primary_alert_decision_rule",
    "expected_pro_rate_primary": "primary_alert_decision_rule",
    "primary_baseline_source": "primary_alert_decision_rule",
    "is_material_primary_shift": "primary_alert_decision_rule",
    "is_material_primary_lower_shift": "primary_alert_decision_rule",
    "is_material_primary_upper_shift": "primary_alert_decision_rule",
    "is_primary_alert_window": "primary_alert_decision_rule",
    "is_primary_spc_998_two_sided": "primary_alert_decision_rule",
    "is_primary_fdr_two_sided": "primary_alert_decision_rule",
    "is_primary_any_flag_channel": "primary_alert_decision_rule",
    "is_primary_both_flag_channels": "primary_alert_decision_rule",
}


def off_hours_method_specs() -> tuple[StatMethodSpec, ...]:
    return _OFF_HOURS_METHOD_SPECS


def off_hours_column_method_map() -> dict[str, str]:
    return dict(_OFF_HOURS_COLUMN_METHOD_MAP)
