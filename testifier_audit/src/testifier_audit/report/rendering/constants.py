from __future__ import annotations

BASELINE_PROFILE_BUCKET_MINUTES = [1, 5, 15, 30, 60, 120, 240, 480, 720, 1440]

PACIFIC_TIMEZONE_NAME = "America/Los_Angeles"

REPORT_DATA_DIRECTORY = "report_data"

REPORT_DATA_FILENAME = f"{REPORT_DATA_DIRECTORY}/index.json"

REPORT_ASSETS_DIRECTORY = "assets/report"

REPORT_CSS_ASSET_FILENAME = f"{REPORT_ASSETS_DIRECTORY}/report.css"

REPORT_JS_ASSET_FILENAME = f"{REPORT_ASSETS_DIRECTORY}/main.js"

_VOTER_LINKAGE_POSITION_PREVIEW_COLUMNS = [
    "match_mode",
    "unit",
    "position_normalized",
    "n_total",
    "n_matched_unique",
    "n_matched_ambiguous",
    "n_unmatched",
    "matched_rate",
    "unmatched_rate",
    "matched_rate_wilson_low",
    "matched_rate_wilson_high",
    "unmatched_rate_wilson_low",
    "unmatched_rate_wilson_high",
    "is_low_power",
]

_VOTER_LINKAGE_POSITION_CHART_COLUMNS = [
    "match_mode",
    "position_normalized",
    "n_total",
    "n_matched_unique",
    "n_matched_ambiguous",
    "n_unmatched",
    "matched_rate",
    "unmatched_rate",
    "match_rate_wilson_low",
    "match_rate_wilson_high",
    "unmatched_rate_wilson_low",
    "unmatched_rate_wilson_high",
    "is_low_power",
]

_COLUMN_DESCRIPTION_OVERRIDES: dict[str, str] = {
    "artifact": "Artifact/table identifier written by the pipeline.",
    "rows": "Number of rows available for that artifact/table in this run.",
    "metric": "Detector metric or score family represented by the row.",
    "window": "Window label or index used by that detector output.",
    "window_minutes": "Window size in minutes used to aggregate records.",
    "bucket_minutes": "Bucket size in minutes for this time-series point.",
    "bucket_start": "Pacific timestamp marking the start of the aggregation bucket.",
    "minute_bucket": "Pacific timestamp rounded to the bucket minute boundary.",
    "start_minute": "Pacific minute where the evaluated window starts.",
    "end_minute": "Pacific minute where the evaluated window ends.",
    "change_minute": "Pacific minute where a structural changepoint was detected.",
    "change_hour": "Hour of day (0-23) for changepoint timing summaries.",
    "change_index": "Sequential changepoint identifier in detector output order.",
    "day_of_week": "Weekday label derived from the event timestamp.",
    "day_of_week_index": "Numeric weekday index (Monday=0 .. Sunday=6).",
    "date": "Calendar date (Pacific Time) associated with the aggregated slot.",
    "hour": "Hour of day (0-23) used for hour-level aggregation.",
    "slot_start_minute": "Minute offset from midnight for the day/time slot.",
    "minute_of_hour": "Minute within the hour (0-59) used in clock-face tests.",
    "run_id": "Stable identifier for a contiguous directional run segment.",
    "run_direction": "Dominant direction of the run (`pro_heavy` or `con_heavy`).",
    "start_bucket": "Pacific timestamp where the directional run starts.",
    "end_bucket": "Pacific timestamp where the directional run ends.",
    "run_length_buckets": "Number of contiguous buckets in the directional run.",
    "position_normalized": "Normalized testimony position label (for example pro/con/other).",
    "display_name": "Raw name string as it appeared in submitted data.",
    "canonical_name": "Normalized name used for duplicate and match analysis.",
    "sample_name": "Representative example name for the cluster/score row.",
    "organization_clean": "Normalized organization text after cleanup rules.",
    "left_display_name": "Left display name in a pairwise similarity row.",
    "right_display_name": "Right display name in a pairwise similarity row.",
    "block_key": "Blocking key used to limit candidate-pair generation.",
    "cluster_id": "Identifier for a clustered grouping.",
    "cluster_size": "Number of unique names contained in the cluster.",
    "time_span_minutes": "Minutes between first and last observed record in this grouping.",
    "first_seen": "First observed Pacific timestamp for this entity/group.",
    "last_seen": "Last observed Pacific timestamp for this entity/group.",
    "n": "Count of records in the row's grouping.",
    "n_total": "Total records in the bucket/group (all positions combined).",
    "n_pro": "Count of records labeled pro within the bucket/group.",
    "n_con": "Count of records labeled con within the bucket/group.",
    "n_other_position": (
        "Count of records in the bucket not labeled pro/con (derived as unknown or residual)."
    ),
    "n_records": "Total raw records represented by this row.",
    "n_unique_names": "Count of distinct canonical names in the bucket/group.",
    "match_mode": (
        "Name-match mode used for duplicate grouping (`strict` exact-style keys, "
        "`loose` nickname/variant-aware keys)."
    ),
    "n_matches": "Count of records whose name matched the voter registry reference.",
    "n_unmatched": "Count of records with no voter-registry name match.",
    "n_exact_matches": "Count of exact canonical-name matches to voter registry names.",
    "n_strong_fuzzy_matches": "Count of strong fuzzy voter-linkage matches.",
    "n_weak_fuzzy_matches": "Count of weak fuzzy voter-linkage matches.",
    "expected_matches": (
        "Confidence-weighted expected match count from probabilistic linkage tiers."
    ),
    "expected_match_count": (
        "Confidence-weighted expected matched records from probabilistic linkage tiers."
    ),
    "support_n": "Total contributing records supporting the run or signal.",
    "n_windows": "Number of windows evaluated for that parameter setting.",
    "n_windows_alert_eligible": (
        "Number of off-hours windows meeting the configured alert-eligibility threshold."
    ),
    "n_windows_tested": (
        "Number of alert-eligible windows that remained inferentially tested "
        "after low-power filtering."
    ),
    "n_windows_low_power": (
        "Alert-eligible windows filtered out as low-power for inferential scoring."
    ),
    "n_windows_primary_alert": "Number of robust primary-alert windows in this grouped cell.",
    "primary_alert_fraction_tested": (
        "Share of inferentially tested windows in this group that met robust primary-alert "
        "criteria."
    ),
    "n_known_tested": (
        "Known pro/con records contributing to inferentially tested windows in this grouped cell."
    ),
    "z_score_primary_median": (
        "Median primary-baseline standardized residual across supportable windows in this group."
    ),
    "z_score_primary_abs_max": (
        "Maximum absolute primary-baseline standardized residual across supportable windows."
    ),
    "n_significant": "Number of windows passing the detector's significance threshold.",
    "n_runs": "Number of directional runs identified for the grouping.",
    "n_long_runs": "Number of directional runs meeting the long-run threshold.",
    "n_clusters": "Number of clusters with that size.",
    "observed_count": "Observed submission count in the tested burst window.",
    "expected_count": "Expected submission count from the fitted baseline/null model.",
    "duplicate_rows": (
        "Observed duplicated-anywhere count for the selected unit in this bucket "
        "(rows when unit is rows_anywhere, distinct names when unit is names_anywhere)."
    ),
    "duplicate_row_rate": (
        "Observed duplicated-anywhere count divided by total rows in this bucket "
        "for the selected unit."
    ),
    "expected_duplicate_rows": (
        "Expected duplicated-anywhere count for the selected unit in this bucket "
        "(rows use report proportional-share baseline; names use report occupancy baseline "
        "when multiplicity profiles are available)."
    ),
    "excess_duplicate_rows": (
        "Signed deviation from expected for the selected unit "
        "(observed - expected; negative means below expectation)."
    ),
    "unit_observed_rows": (
        "Observed row count in this bucket where the name is duplicated anywhere in the timeline."
    ),
    "unit_expected_rows": (
        "Expected duplicated-anywhere row count from report proportional-share baseline "
        "(bucket rows * global duplicated-row share)."
    ),
    "unit_deviation_rows": "Signed row deviation from expected (unit_observed_rows - unit_expected_rows).",
    "unit_observed_names": (
        "Observed distinct-name count in this bucket where each name is duplicated "
        "anywhere in the timeline."
    ),
    "unit_expected_names": (
        "Expected duplicated-anywhere distinct-name count from report occupancy baseline "
        "(sum over names of 1 - C(N-c, n)/C(N, n)); falls back to row-volume share when "
        "multiplicity profiles are unavailable."
    ),
    "unit_expected_names_method": (
        "Method used for unit_expected_names (occupancy_without_replacement or "
        "row_share_fallback_missing_multiplicity)."
    ),
    "unit_deviation_names": (
        "Signed distinct-name deviation from expected "
        "(unit_observed_names - unit_expected_names)."
    ),
    "report_baseline_family": (
        "Report-layer baseline family used for the current row/metric."
    ),
    "report_baseline_label": "Human-readable label for the report-layer baseline family.",
    "report_baseline_method": "Report-layer baseline method identifier for this row.",
    "report_baseline_method_label": "Human-readable explanation of the report-layer baseline method.",
    "detector_baseline_family": "Detector baseline family (for example VRDB collision-null).",
    "detector_baseline_family_label": "Human-readable detector baseline family label.",
    "detector_baseline_label": "Detector baseline label propagated from duplicate runtime metadata.",
    "off_hours": "Count of records in configured off-hours period.",
    "on_hours": "Count of records in configured on-hours period.",
    "off_hours_ratio": "Fraction of all records submitted during off-hours.",
    "off_hours_pro_rate": "Pro share during off-hours windows.",
    "on_hours_pro_rate": "Pro share during on-hours windows.",
    "off_hours_pro_rate_wilson_low": "Lower Wilson bound for off-hours pro share.",
    "off_hours_pro_rate_wilson_high": "Upper Wilson bound for off-hours pro share.",
    "on_hours_pro_rate_wilson_low": "Lower Wilson bound for on-hours pro share.",
    "on_hours_pro_rate_wilson_high": "Upper Wilson bound for on-hours pro share.",
    "off_hours_is_low_power": "True when off-hours sample size is too small for stable inference.",
    "on_hours_is_low_power": "True when on-hours sample size is too small for stable inference.",
    "primary_bucket_minutes": "Primary bucket size used for off-hours control summaries.",
    "primary_baseline_method": (
        "Primary baseline used in summary scoring (model_day_hour or day_on_hours_fallback)."
    ),
    "alert_off_hours_min_fraction": (
        "Minimum off-hours share required for an alert-eligible window."
    ),
    "primary_alert_min_abs_delta": (
        "Minimum absolute primary-baseline pro-rate delta required before alert escalation."
    ),
    "off_hours_windows_alert_eligible": (
        "Count of alert-eligible off-hours windows before low-power filtering."
    ),
    "off_hours_windows_alert_eligible_low_power": (
        "Number of alert-eligible off-hours windows flagged as low-power."
    ),
    "off_hours_windows_alert_eligible_tested_fraction": (
        "Fraction of alert-eligible off-hours windows that remained inferentially tested "
        "after low-power filtering."
    ),
    "off_hours_windows_alert_eligible_low_power_fraction": (
        "Fraction of alert-eligible off-hours windows filtered as low-power."
    ),
    "off_hours_windows_tested": (
        "Count of alert-eligible off-hours windows tested after low-power filtering."
    ),
    "off_hours_windows_below_day_control_95": (
        "Number of off-hours windows below the day-adjusted lower 95% control limit."
    ),
    "off_hours_windows_below_day_control_998": (
        "Number of off-hours windows below the day-adjusted lower 99.8% control limit."
    ),
    "off_hours_windows_below_model_control_95": (
        "Number of off-hours windows below the model-based lower 95% control limit."
    ),
    "off_hours_windows_below_model_control_998": (
        "Number of off-hours windows below the model-based lower 99.8% control limit."
    ),
    "off_hours_windows_below_primary_control_95": (
        "Number of off-hours windows below the active primary-baseline 95% lower control limit."
    ),
    "off_hours_windows_below_primary_control_998": (
        "Number of off-hours windows below the active primary-baseline 99.8% lower control limit."
    ),
    "off_hours_windows_above_primary_control_95": (
        "Number of off-hours windows above the active primary-baseline 95% upper control limit."
    ),
    "off_hours_windows_above_primary_control_998": (
        "Number of off-hours windows above the active primary-baseline 99.8% upper control limit."
    ),
    "off_hours_windows_significant_day": (
        "Number of off-hours windows passing day-adjusted FDR control in the primary bucket."
    ),
    "off_hours_windows_significant_model": (
        "Number of off-hours windows passing model-based lower-tail FDR control."
    ),
    "off_hours_windows_significant_primary": (
        "Number of off-hours windows passing primary-baseline lower-tail FDR control."
    ),
    "off_hours_windows_significant_primary_upper": (
        "Number of off-hours windows passing primary-baseline upper-tail FDR control."
    ),
    "off_hours_windows_significant_primary_two_sided": (
        "Number of off-hours windows passing primary-baseline two-sided FDR control."
    ),
    "off_hours_windows_primary_spc_998_any": (
        "Number of tested off-hours windows outside primary-baseline 99.8% control limits "
        "(two-sided SPC-style channel)."
    ),
    "off_hours_windows_primary_fdr_two_sided": (
        "Number of tested off-hours windows passing primary-baseline two-sided FDR control."
    ),
    "off_hours_windows_primary_flag_any": (
        "Number of tested off-hours windows flagged by either primary 99.8% control breach "
        "or two-sided primary FDR channel."
    ),
    "off_hours_windows_primary_flag_both": (
        "Number of tested off-hours windows flagged by both primary 99.8% control breach "
        "and two-sided primary FDR channels."
    ),
    "off_hours_windows_primary_spc_998_any_fraction": (
        "Share of tested off-hours windows flagged by the primary 99.8% control-breach "
        "channel."
    ),
    "off_hours_windows_primary_fdr_two_sided_fraction": (
        "Share of tested off-hours windows flagged by the primary two-sided FDR channel."
    ),
    "off_hours_windows_primary_flag_any_fraction": (
        "Share of tested off-hours windows flagged by either primary SPC or primary FDR "
        "two-sided channels."
    ),
    "off_hours_windows_primary_flag_both_fraction": (
        "Share of tested off-hours windows flagged by both primary SPC and primary FDR "
        "two-sided channels."
    ),
    "off_hours_windows_primary_alert": (
        "Count of robust primary alerts (alert-eligible, low-power filtered, below 99.8% "
        "primary lower limit, lower-tail FDR-significant, and material effect size)."
    ),
    "off_hours_windows_primary_alert_fraction": (
        "Share of tested off-hours windows that meet robust primary-alert criteria."
    ),
    "off_hours_primary_alert_run_count": (
        "Count of contiguous robust primary-alert runs in the primary bucket."
    ),
    "off_hours_primary_alert_max_run_windows": (
        "Length (in windows) of the longest contiguous robust primary-alert run."
    ),
    "off_hours_primary_alert_max_run_minutes": (
        "Duration (minutes) of the longest contiguous robust primary-alert run."
    ),
    "off_hours_min_day_z": "Most negative day-adjusted standardized residual among tested windows.",
    "off_hours_max_abs_day_z": (
        "Largest absolute day-adjusted standardized residual among tested windows."
    ),
    "off_hours_min_model_z": (
        "Most negative model-based standardized residual among tested windows."
    ),
    "off_hours_max_abs_model_z": (
        "Largest absolute model-based standardized residual among tested windows."
    ),
    "off_hours_min_primary_z": (
        "Most negative primary-baseline standardized residual among tested windows."
    ),
    "off_hours_max_abs_primary_z": (
        "Largest absolute primary-baseline standardized residual among tested windows."
    ),
    "off_hours_min_primary_delta": (
        "Most negative observed-minus-expected pro-rate delta under the primary baseline."
    ),
    "off_hours_max_abs_primary_delta": (
        "Largest absolute observed-minus-expected pro-rate delta under the primary baseline."
    ),
    "off_hours_windows_model_available": (
        "Count of tested off-hours windows where the model baseline was available."
    ),
    "global_daytime_pro_rate": "Global on-hours pro-rate baseline used for funnel/control context.",
    "day_adjusted_fdr_alpha": "FDR alpha used for day-adjusted window scanning.",
    "model_fit_min_rows": (
        "Minimum number of non-empty windows required before fitting model baseline."
    ),
    "model_hour_harmonics": (
        "Number of cyclic hour-of-day harmonic pairs (sin/cos) used in the model baseline."
    ),
    "primary_model_fit_method": "Model fit method used for the primary bucket baseline.",
    "primary_model_fit_rows": "Number of windows used to fit the primary-bucket model baseline.",
    "primary_model_fit_unique_days": "Unique day count used in the primary-bucket model fit.",
    "primary_model_fit_unique_hours": (
        "Unique hour-of-day count used in the primary-bucket model fit."
    ),
    "primary_model_fit_converged": (
        "Model convergence indicator for the primary bucket fit (1 converged, 0 not converged)."
    ),
    "primary_model_fit_aic": "AIC for the primary-bucket fitted model when available.",
    "n_off_hours": "Count of records in the day/hour cell that fall in off-hours.",
    "off_hours_fraction": "Share of day/hour cell records that fall in off-hours.",
    "n_known": "Count of records with known pro/con position labels.",
    "n_unknown": "Count of records without a known pro/con position label.",
    "is_off_hours_window": "True when at least half of records in a bucket fall in off-hours.",
    "is_pure_off_hours_window": "True when all records in a bucket fall in off-hours.",
    "is_alert_off_hours_window": (
        "True when a bucket meets the configured off-hours-share threshold for alert scanning."
    ),
    "expected_pro_rate_day": "Day-adjusted expected pro rate used for residual/control scoring.",
    "expected_pro_rate_model": (
        "Model-based expected pro rate from binomial logit with day effects and harmonic "
        "hour terms."
    ),
    "expected_pro_rate_primary": (
        "Primary expected pro rate used for alerting (model-based where available, else "
        "day-adjusted)."
    ),
    "expected_pro_rate_global": "Global on-hours expected pro rate baseline.",
    "baseline_source": (
        "Baseline source used for this row (day_on_hours, global_on_hours, or fallback)."
    ),
    "baseline_label": "Human-readable label for the selected baseline source in this row.",
    "inferential_status": (
        "Inference availability status for this row (`reference_model_inference`, "
        "`descriptive_only`, or `unavailable`)."
    ),
    "claim_class": "Public-facing claim class for the row-level signal family.",
    "model_baseline_source": "Model baseline fit source for this row.",
    "primary_baseline_source": "Baseline source for primary scoring columns in this row.",
    "is_model_baseline_available": "True when model expected pro rate is available for this row.",
    "model_fit_method": "Per-bucket model fit method backing this row.",
    "model_fit_rows": "Per-bucket number of windows used for model fitting.",
    "model_fit_unique_days": "Per-bucket unique day count in model fit data.",
    "model_fit_unique_hours": "Per-bucket unique hour-of-day count in model fit data.",
    "model_fit_converged": "Per-bucket model convergence indicator (1 converged, 0 not converged).",
    "model_fit_aic": "Per-bucket model AIC when available.",
    "model_fit_used_harmonics": (
        "Per-bucket number of harmonic pairs used in the model design matrix."
    ),
    "model_fit_window_count": "Total windows available in this bucket-size profile.",
    "model_fit_available_windows": (
        "Windows in this bucket size where model expected rates were available."
    ),
    "model_fit_available_fraction": (
        "Share of windows in this bucket size where model expected rates were available."
    ),
    "control_low_95_day": "Lower 95% day-adjusted p-chart control limit for pro rate.",
    "control_high_95_day": "Upper 95% day-adjusted p-chart control limit for pro rate.",
    "control_low_998_day": "Lower 99.8% day-adjusted p-chart control limit for pro rate.",
    "control_high_998_day": "Upper 99.8% day-adjusted p-chart control limit for pro rate.",
    "control_low_95_model": "Lower 95% model-based p-chart control limit for pro rate.",
    "control_high_95_model": "Upper 95% model-based p-chart control limit for pro rate.",
    "control_low_998_model": "Lower 99.8% model-based p-chart control limit for pro rate.",
    "control_high_998_model": "Upper 99.8% model-based p-chart control limit for pro rate.",
    "control_low_95_primary": "Lower 95% primary-baseline p-chart control limit for pro rate.",
    "control_high_95_primary": "Upper 95% primary-baseline p-chart control limit for pro rate.",
    "control_low_998_primary": "Lower 99.8% primary-baseline p-chart control limit for pro rate.",
    "control_high_998_primary": "Upper 99.8% primary-baseline p-chart control limit for pro rate.",
    "control_low_95_global": "Lower 95% global-baseline control limit for pro rate.",
    "control_high_95_global": "Upper 95% global-baseline control limit for pro rate.",
    "control_low_998_global": "Lower 99.8% global-baseline control limit for pro rate.",
    "control_high_998_global": "Upper 99.8% global-baseline control limit for pro rate.",
    "z_score_day": "Day-adjusted standardized residual for observed pro count.",
    "z_score_model": "Model-based standardized residual for observed pro count.",
    "z_score_primary": "Primary-baseline standardized residual for observed pro count.",
    "delta_pro_rate_day": "Observed minus expected pro rate under day-adjusted baseline.",
    "delta_pro_rate_model": "Observed minus expected pro rate under model baseline.",
    "delta_pro_rate_primary": "Observed minus expected pro rate under primary baseline.",
    "p_value_day": (
        "Exact binomial lower-tail p-value from day-adjusted expected rate "
        "(tests for unusually low pro share)."
    ),
    "p_value_day_two_sided": (
        "Two-sided p-value (equal-tail) from day-adjusted exact binomial test."
    ),
    "p_value_day_lower": "Exact binomial lower-tail p-value from day-adjusted expected rate.",
    "p_value_day_upper": "Exact binomial upper-tail p-value from day-adjusted expected rate.",
    "q_value_day": "FDR-adjusted lower-tail q-value for day-adjusted off-hours scan.",
    "q_value_day_lower": "FDR-adjusted lower-tail q-value for day-adjusted off-hours scan.",
    "q_value_day_upper": "FDR-adjusted upper-tail q-value for day-adjusted off-hours scan.",
    "q_value_day_two_sided": "FDR-adjusted two-sided q-value for day-adjusted off-hours scan.",
    "is_significant_day": "True when day-adjusted lower-tail q-value <= configured FDR alpha.",
    "is_significant_day_lower": (
        "True when day-adjusted lower-tail q-value <= configured FDR alpha."
    ),
    "is_significant_day_upper": (
        "True when day-adjusted upper-tail q-value <= configured FDR alpha."
    ),
    "is_significant_day_two_sided": (
        "True when day-adjusted two-sided q-value <= configured FDR alpha."
    ),
    "p_value_model": "Exact binomial lower-tail p-value from model-based expected rate.",
    "p_value_model_two_sided": (
        "Two-sided p-value (equal-tail) from model-based exact binomial test."
    ),
    "p_value_model_lower": "Exact binomial lower-tail p-value from model-based expected rate.",
    "p_value_model_upper": "Exact binomial upper-tail p-value from model-based expected rate.",
    "q_value_model": "FDR-adjusted lower-tail q-value for model-based off-hours scan.",
    "q_value_model_lower": "FDR-adjusted lower-tail q-value for model-based off-hours scan.",
    "q_value_model_upper": "FDR-adjusted upper-tail q-value for model-based off-hours scan.",
    "q_value_model_two_sided": "FDR-adjusted two-sided q-value for model-based off-hours scan.",
    "is_significant_model": "True when model-based lower-tail q-value <= configured FDR alpha.",
    "is_significant_model_lower": (
        "True when model-based lower-tail q-value <= configured FDR alpha."
    ),
    "is_significant_model_upper": (
        "True when model-based upper-tail q-value <= configured FDR alpha."
    ),
    "is_significant_model_two_sided": (
        "True when model-based two-sided q-value <= configured FDR alpha."
    ),
    "p_value_primary": "Exact binomial lower-tail p-value from primary-baseline expected rate.",
    "p_value_primary_two_sided": (
        "Two-sided p-value (equal-tail) from primary-baseline exact binomial test."
    ),
    "p_value_primary_lower": (
        "Exact binomial lower-tail p-value from primary-baseline expected rate."
    ),
    "p_value_primary_upper": (
        "Exact binomial upper-tail p-value from primary-baseline expected rate."
    ),
    "q_value_primary": "FDR-adjusted lower-tail q-value for primary-baseline off-hours scan.",
    "q_value_primary_lower": "FDR-adjusted lower-tail q-value for primary-baseline off-hours scan.",
    "q_value_primary_upper": "FDR-adjusted upper-tail q-value for primary-baseline off-hours scan.",
    "q_value_primary_two_sided": (
        "FDR-adjusted two-sided q-value for primary-baseline off-hours scan."
    ),
    "is_significant_primary": (
        "True when primary-baseline lower-tail q-value <= configured FDR alpha."
    ),
    "is_significant_primary_lower": (
        "True when primary-baseline lower-tail q-value <= configured FDR alpha."
    ),
    "is_significant_primary_upper": (
        "True when primary-baseline upper-tail q-value <= configured FDR alpha."
    ),
    "is_significant_primary_two_sided": (
        "True when primary-baseline two-sided q-value <= configured FDR alpha."
    ),
    "is_material_primary_shift": (
        "True when absolute primary observed-minus-expected pro-rate delta meets configured floor."
    ),
    "is_material_primary_lower_shift": (
        "True when negative primary pro-rate delta meets configured absolute floor."
    ),
    "is_material_primary_upper_shift": (
        "True when positive primary pro-rate delta meets configured absolute floor."
    ),
    "is_primary_alert_window": (
        "True for robust primary alerts: alert-eligible off-hours window with adequate support, "
        "below primary 99.8% lower limit, lower-tail FDR-significant, and material effect size."
    ),
    "is_primary_lower_alert_window": (
        "True for robust lower-tail primary alerts (alert-eligible, support-qualified, below "
        "99.8% primary lower limit, lower-tail FDR-supported, and materially negative)."
    ),
    "is_primary_upper_alert_window": (
        "True for robust upper-tail primary alerts (alert-eligible, support-qualified, above "
        "99.8% primary upper limit, upper-tail FDR-supported, and materially positive)."
    ),
    "is_primary_two_sided_alert_window": (
        "True when either robust lower-tail or robust upper-tail primary alert criteria are met."
    ),
    "is_primary_spc_998_two_sided": (
        "True when an inferentially tested off-hours window is outside primary-baseline "
        "99.8% control limits (either direction)."
    ),
    "is_primary_fdr_two_sided": (
        "True when an inferentially tested off-hours window passes primary-baseline two-sided "
        "FDR control."
    ),
    "is_primary_any_flag_channel": (
        "True when either primary SPC 99.8% breach or primary two-sided FDR channel flags "
        "the inferentially tested off-hours window."
    ),
    "is_primary_both_flag_channels": (
        "True when both primary SPC 99.8% breach and primary two-sided FDR channels flag "
        "the inferentially tested off-hours window."
    ),
    "channel": "Flag channel identifier used in off-hours channel-breakdown summaries.",
    "channel_label": "Human-readable label for the off-hours flag channel.",
    "share_of_tested": (
        "Fraction of inferentially tested off-hours windows represented by this channel."
    ),
    "is_below_day_control_95": "True when observed pro rate is below day-adjusted 95% lower limit.",
    "is_below_day_control_998": (
        "True when observed pro rate is below day-adjusted 99.8% lower limit."
    ),
    "is_below_model_control_95": (
        "True when observed pro rate is below model-based 95% lower limit."
    ),
    "is_below_model_control_998": (
        "True when observed pro rate is below model-based 99.8% lower limit."
    ),
    "is_below_primary_control_95": (
        "True when observed pro rate is below primary-baseline 95% lower limit."
    ),
    "is_below_primary_control_998": (
        "True when observed pro rate is below primary-baseline 99.8% lower limit."
    ),
    "is_above_day_control_95": "True when observed pro rate is above day-adjusted 95% upper limit.",
    "is_above_day_control_998": (
        "True when observed pro rate is above day-adjusted 99.8% upper limit."
    ),
    "is_above_model_control_95": (
        "True when observed pro rate is above model-based 95% upper limit."
    ),
    "is_above_model_control_998": (
        "True when observed pro rate is above model-based 99.8% upper limit."
    ),
    "is_above_primary_control_95": (
        "True when observed pro rate is above primary-baseline 95% upper limit."
    ),
    "is_above_primary_control_998": (
        "True when observed pro rate is above primary-baseline 99.8% upper limit."
    ),
    "is_outside_day_control_95": (
        "True when observed pro rate lies outside day-adjusted 95% control limits."
    ),
    "is_outside_day_control_998": (
        "True when observed pro rate lies outside day-adjusted 99.8% control limits."
    ),
    "is_outside_model_control_95": (
        "True when observed pro rate lies outside model-based 95% control limits."
    ),
    "is_outside_model_control_998": (
        "True when observed pro rate lies outside model-based 99.8% control limits."
    ),
    "is_outside_primary_control_95": (
        "True when observed pro rate lies outside primary-baseline 95% control limits."
    ),
    "is_outside_primary_control_998": (
        "True when observed pro rate lies outside primary-baseline 99.8% control limits."
    ),
    "is_below_global_control_95": "True when observed pro rate is below global 95% lower limit.",
    "is_below_global_control_998": (
        "True when observed pro rate is below global 99.8% lower limit."
    ),
    "pro_rate": "Share of records that are pro in this row (0 to 1).",
    "baseline_pro_rate": "Reference pro share expected for this day/time context.",
    "stable_lower": "Lower bound of the expected stable pro-share band.",
    "stable_upper": "Upper bound of the expected stable pro-share band.",
    "delta_from_slot_pro_rate": "Difference between observed pro share and slot baseline.",
    "deviation_from_uniform": (
        "Difference between observed and uniform-expected periodic count/share."
    ),
    "rate_ratio": "Observed/expected rate ratio for burst testing.",
    "match_rate": "Share of records matched to voter registry (0 to 1).",
    "matched_rate_pro": "Matched-rate share for Pro rows in the bucket.",
    "matched_rate_con": "Matched-rate share for Con rows in the bucket.",
    "matched_rate_pro_wilson_low": "Lower Wilson bound for Pro-only matched-rate share.",
    "matched_rate_pro_wilson_high": "Upper Wilson bound for Pro-only matched-rate share.",
    "matched_rate_con_wilson_low": "Lower Wilson bound for Con-only matched-rate share.",
    "matched_rate_con_wilson_high": "Upper Wilson bound for Con-only matched-rate share.",
    "expected_match_rate_global": "Global matched-rate baseline used for bucket anomaly checks.",
    "control_low_95_match_global": (
        "Lower 95% control limit for matched rate under the global matched-rate baseline."
    ),
    "control_high_95_match_global": (
        "Upper 95% control limit for matched rate under the global matched-rate baseline."
    ),
    "control_low_998_match_global": (
        "Lower 99.8% control limit for matched rate under the global matched-rate baseline."
    ),
    "control_high_998_match_global": (
        "Upper 99.8% control limit for matched rate under the global matched-rate baseline."
    ),
    "match_rate_delta_global": (
        "Observed matched rate minus global matched-rate baseline for the bucket."
    ),
    "is_match_rate_alert_lower": (
        "True when matched rate is below the global 99.8% lower control limit and support is "
        "not low-power."
    ),
    "is_match_rate_alert_upper": (
        "True when matched rate is above the global 99.8% upper control limit and support is "
        "not low-power."
    ),
    "is_match_rate_alert_any": (
        "True when either lower- or upper-tail global 99.8% matched-rate alert criteria are met."
    ),
    "exact_match_rate": "Share of records in the exact voter-linkage tier.",
    "strong_fuzzy_match_rate": "Share of records in the strong fuzzy linkage tier.",
    "weak_fuzzy_match_rate": "Share of records in the weak fuzzy linkage tier.",
    "expected_match_rate": "Confidence-weighted expected match share from probabilistic tiers.",
    "mean_match_confidence": "Average linkage confidence (0 to 1) across rows in this grouping.",
    "matched_confidence_mean": "Average linkage confidence among matched-tier rows only.",
    "pro_match_rate": "Match rate for pro-position records only.",
    "con_match_rate": "Match rate for con-position records only.",
    "blank_org_rate": "Share of records with blank/null organization values.",
    "pro_blank_org_rate": "Blank organization share among pro records.",
    "con_blank_org_rate": "Blank organization share among con records.",
    "unique_ratio": "Distinct-name ratio: unique names divided by total records.",
    "threshold_unique_ratio": "Configured or modeled threshold used to flag unusual uniqueness.",
    "alphabetical_ratio": "Share of windows flagged as alphabetically ordered.",
    "kendall_tau": "Kendall tau rank correlation between arrival order and alphabetical order.",
    "kendall_p_value": "P-value for Kendall tau rank-correlation test.",
    "abs_kendall_tau": "Absolute Kendall tau magnitude (ordering strength).",
    "mean_kendall_tau": "Average Kendall tau value for buckets at this granularity.",
    "mean_abs_kendall_tau": "Average absolute Kendall tau for buckets at this granularity.",
    "max_abs_kendall_tau": "Maximum absolute Kendall tau observed at this granularity.",
    "strong_ordering_ratio": "Share of buckets with strong ordering (|Kendall tau| >= 0.8).",
    "avg_records_per_bucket": "Average number of records per evaluated bucket.",
    "is_alphabetical": "1/true when local ordering is alphabetical under detector rules.",
    "is_significant": "True when the hypothesis test passes configured significance thresholds.",
    "is_flagged": "Detector-level flag for windows considered anomalous/elevated.",
    "is_long_run": "True when a run spans at least the configured long-run bucket threshold.",
    "is_slot_outlier": "True when day/slot delta is an outlier versus peer slots.",
    "is_anomaly": "True when multivariate model scores this bucket as anomalous.",
    "is_model_eligible": "True when bucket has enough support/features for model scoring.",
    "is_changepoint": "True when timestamp coincides with a detected structural break.",
    "is_low_power": "True when sample size is too small for stable proportion inference.",
    "pro_is_low_power": "True when pro-side subgroup support is low.",
    "con_is_low_power": "True when con-side subgroup support is low.",
    "pro_rate_wilson_low": "Lower Wilson confidence bound for pro rate.",
    "pro_rate_wilson_high": "Upper Wilson confidence bound for pro rate.",
    "blank_org_rate_wilson_low": "Lower Wilson confidence bound for blank-org rate.",
    "blank_org_rate_wilson_high": "Upper Wilson confidence bound for blank-org rate.",
    "match_rate_wilson_low": "Lower Wilson confidence bound for registry match rate.",
    "match_rate_wilson_high": "Upper Wilson confidence bound for registry match rate.",
    "pro_match_rate_wilson_low": "Lower Wilson bound for pro-only registry match rate.",
    "pro_match_rate_wilson_high": "Upper Wilson bound for pro-only registry match rate.",
    "con_match_rate_wilson_low": "Lower Wilson bound for con-only registry match rate.",
    "con_match_rate_wilson_high": "Upper Wilson bound for con-only registry match rate.",
    "q_value": "Multiple-testing-adjusted p-value controlling false discovery rate.",
    "chi_square_p_value": "P-value from chi-square comparison between grouped distributions.",
    "autocorr": "Autocorrelation value at the specified lag.",
    "abs_autocorr": "Absolute autocorrelation magnitude (strength regardless of sign).",
    "lag_minutes": "Lag distance in minutes for autocorrelation analysis.",
    "period_minutes": "Cycle length in minutes derived from spectral analysis.",
    "frequency_per_minute": "Equivalent cycle frequency in events per minute.",
    "power": "Spectral power (relative strength) at that detected frequency.",
    "n_points": "Number of minute-level observations included in the rolling statistic.",
    "mean_count": "Rolling-window mean submissions per minute.",
    "variance_count": "Rolling-window variance of submissions per minute.",
    "fano_factor": "Variance divided by mean for rolling window counts (Poisson baseline ~=1).",
    "is_high_fano": (
        "True when rolling Fano factor exceeds configured high-overdispersion threshold."
    ),
    "median_fano_factor": "Median rolling Fano factor for the evaluated window size.",
    "p95_fano_factor": "95th percentile rolling Fano factor for the evaluated window size.",
    "max_fano_factor": "Maximum rolling Fano factor observed for the evaluated window size.",
    "high_fano_ratio": "Share of rolling windows above the high-Fano threshold.",
    "share": "Fraction of all events in the specified minute-of-hour bin.",
    "z_score_uniform": "Standardized deviation from uniform expectation.",
    "anomaly_score": "Model anomaly score; higher values indicate rarer feature combinations.",
    "anomaly_score_percentile": "Percentile rank of anomaly score within the run.",
    "evidence_count": "Number of detector signals contributing to this score.",
    "evidence_flags": "Comma-separated detector signal tags active in this bucket/window.",
    "flag": "Detector flag name counted in evidence composition.",
    "count": "Count of rows/windows/flags for the grouped label.",
    "burst_signal": "Binary indicator that burst detector contributed evidence.",
    "swing_signal": "Binary indicator that pro/con swing detector contributed evidence.",
    "weirdness_score": "Name-string irregularity score; higher implies less typical structure.",
    "name_length": "Character length of the normalized name token/string.",
    "n_names": "Count of names in the associated histogram bin.",
    "non_alpha_fraction": "Fraction of characters that are non alphabetic.",
    "name_entropy": "Character-level entropy; higher values suggest more randomness.",
    "rarity_median": "Median rarity score among names in the bucket.",
    "rarity_p95": "95th percentile rarity score in the bucket.",
    "threshold": "Detector threshold used to flag bursts/excess concentration.",
    "iteration": "Null-simulation iteration index.",
    "max_window_count": "Maximum simulated count observed in that iteration/window setup.",
    "max_abs_delta_pro_rate": "Maximum absolute pro-rate delta observed in null simulation.",
    "max_run_length_buckets": "Maximum run length (in buckets) observed for the grouping.",
    "max_run_mean_abs_delta": "Largest run-level mean absolute pro-rate delta in the grouping.",
    "max_run_total_n": "Largest support volume observed among runs in the grouping.",
    "similarity": "String similarity score for a candidate pair.",
    "n_active_buckets": "Number of time buckets in which a cluster appears.",
    "peak_bucket_start": "Timestamp of the highest-density bucket for a cluster.",
    "peak_bucket_records": "Record count in the highest-density bucket for a cluster.",
    "peak_bucket_fraction": "Share of cluster records contained in the highest-density bucket.",
    "concentration_hhi": "Herfindahl index of cluster bucket concentration (higher = tighter).",
    "records_per_cluster": "Average records per active cluster in the bucket.",
    "match_tier": (
        "Probabilistic voter-linkage tier (`exact`, `strong_fuzzy`, `weak_fuzzy`, "
        "`unmatched`)."
    ),
    "matched_registry_name": "Best matched registry canonical name for linkage diagnostics.",
    "matched_registry_rows": "Count of registry rows supporting the matched registry name.",
    "best_similarity_score": (
        "Highest fuzzy-name similarity score for the candidate match (0 to 1)."
    ),
    "second_best_similarity_score": "Second-highest fuzzy-name similarity score (0 to 1).",
    "candidate_pool_size": "Number of registry-name candidates evaluated for this last name.",
    "is_ambiguous": "True when multiple top fuzzy candidates had near-tied similarity scores.",
    "match_caveat": "Comma-separated uncertainty caveat flags emitted during linkage.",
    "caveat_flag": "Uncertainty caveat identifier for probabilistic voter linkage.",
    "strong_fuzzy_min_score": "Configured similarity threshold for strong fuzzy linkage.",
    "weak_fuzzy_min_score": "Configured similarity threshold for weak fuzzy linkage.",
    "uncertainty_caveat": "Summary string of uncertainty caveat counts for probabilistic linkage.",
    "attribution_caveat": (
        "Reminder that voter linkage is supporting context, not standalone attribution."
    ),
    "score_primary_driver": "Detector family contributing the largest share of suspicion score.",
    "score_detector_breakdown": "Detector-level contribution shares for suspicion score.",
    "score_signal_breakdown": "Top signal-level contribution shares for suspicion score.",
    "n_flagged_buckets": "Count of run buckets flagged by swing thresholds.",
    "n_low_power_buckets": "Count of run buckets marked low-power.",
    "flagged_ratio": "Share of run buckets flagged by swing thresholds.",
    "low_power_ratio": "Share of run buckets marked low-power.",
    "pro_heavy_run_ratio": "Share of directional runs that are pro-heavy.",
    "flagged_run_ratio": "Share of directional runs with at least one flagged bucket.",
    "token": "Name token extracted during rarity/coverage diagnostics.",
    "value": "Detector-specific numeric value for the metric column.",
    "score": "Detector/model score for the row.",
}

_DUPLICATES_EXACT_FULL_PREVIEW_TABLES = frozenset(
    {
        "per_name_tests",
        "per_name_display",
        "per_name_anomalies",
        "per_name_duplicates_by_mode",
        "per_name_submission_timing_by_mode",
        "repeated_same_bucket",
        "position_switching_names",
        "top_repeated_names",
        "top_name_timing_by_mode",
    }
)

_REPORT_MATCH_MODE_ALIASES: dict[str, str] = {
    "strict": "strict",
    "exact": "strict",
    "medium": "strict",
    "loose": "loose",
    "nickname": "loose",
}

_SCATTER_CHART_IDS = {
    "multivariate_feature_projection",
    "multivariate_top_buckets",
    "off_hours_funnel_plot",
    "duplicates_exact_top_name_timing_exact",
}
