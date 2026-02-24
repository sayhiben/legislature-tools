from __future__ import annotations

import json
import math
import re
import shutil
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape

from testifier_audit.detectors.base import DetectorResult
from testifier_audit.features.dedup import DEDUP_MODES, DEFAULT_DEDUP_MODE, normalize_dedup_mode
from testifier_audit.io.hearing_metadata import HearingMetadata
from testifier_audit.proportion_stats import (
    DEFAULT_LOW_POWER_MIN_TOTAL,
    low_power_mask,
    wilson_interval,
)
from testifier_audit.report.analysis_registry import (
    analysis_status as analysis_registry_status,
)
from testifier_audit.report.analysis_registry import (
    configured_analysis_ids as registry_configured_analysis_ids,
)
from testifier_audit.report.analysis_registry import (
    default_analysis_definitions as registry_analysis_definitions,
)
from testifier_audit.report.analysis_registry import (
    focus_mode_for_analysis_ids as registry_focus_mode_for_analysis_ids,
)
from testifier_audit.report.contracts import default_color_semantics
from testifier_audit.report.help_registry import (
    build_methodology_content,
    default_evidence_taxonomy,
    default_theme_options,
)
from testifier_audit.report.quality_builder import build_data_quality_panel
from testifier_audit.report.triage_builder import build_investigation_views

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pq = None


BASELINE_PROFILE_BUCKET_MINUTES = [1, 5, 15, 30, 60, 120, 240]
PACIFIC_TIMEZONE_NAME = "America/Los_Angeles"
REPORT_DATA_DIRECTORY = "report_data"
REPORT_DATA_FILENAME = f"{REPORT_DATA_DIRECTORY}/index.json"
REPORT_ASSETS_DIRECTORY = "assets/report"
REPORT_CSS_ASSET_FILENAME = f"{REPORT_ASSETS_DIRECTORY}/report.css"
REPORT_JS_ASSET_FILENAME = f"{REPORT_ASSETS_DIRECTORY}/main.js"

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
    "n_changes": "Number of changepoints detected in that summarized bucket.",
    "n_clusters": "Number of clusters with that size.",
    "n_events": "Observed event count for the periodicity slot.",
    "expected_n_events_uniform": "Expected event count under a uniform minute-of-hour baseline.",
    "observed_count": "Observed submission count in the tested burst window.",
    "expected_count": "Expected submission count from the fitted baseline/null model.",
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
    "composite_score": "Combined detector evidence score used for prioritization.",
    "evidence_count": "Number of detector signals contributing to this score.",
    "evidence_flags": "Comma-separated detector signal tags active in this bucket/window.",
    "flag": "Detector flag name counted in evidence composition.",
    "count": "Count of rows/windows/flags for the grouped label.",
    "burst_signal": "Binary indicator that burst detector contributed evidence.",
    "swing_signal": "Binary indicator that pro/con swing detector contributed evidence.",
    "changepoint_signal": "Binary indicator that changepoint detector contributed evidence.",
    "ml_anomaly_signal": (
        "Binary indicator that multivariate anomaly detector contributed evidence."
    ),
    "rarity_signal": "Binary indicator from rarity-focused detector components.",
    "unique_signal": "Binary indicator from unique-name ratio detector components.",
    "mean_before": "Mean value in the segment before the changepoint.",
    "mean_after": "Mean value in the segment after the changepoint.",
    "delta": "Signed difference (after - before) at the changepoint.",
    "abs_delta": "Absolute change magnitude at the changepoint.",
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


def _template_env() -> Environment:
    templates_path = Path(__file__).resolve().parent / "templates"
    return Environment(
        loader=FileSystemLoader(str(templates_path)),
        autoescape=select_autoescape(enabled_extensions=("html", "xml")),
        trim_blocks=True,
        lstrip_blocks=True,
    )


def _copy_report_static_assets(out_dir: Path) -> dict[str, str]:
    source_root = Path(__file__).resolve().parent / "static" / "report"
    if not source_root.exists():
        raise FileNotFoundError(f"Report static assets directory not found: {source_root}")

    destination_root = out_dir / REPORT_ASSETS_DIRECTORY
    if destination_root.exists():
        shutil.rmtree(destination_root)
    destination_root.mkdir(parents=True, exist_ok=True)

    copied_files: set[str] = set()
    for source_path in sorted(source_root.iterdir()):
        if not source_path.is_file():
            continue
        shutil.copy2(source_path, destination_root / source_path.name)
        copied_files.add(source_path.name)

    required_files = {"report.css", "main.js"}
    missing = sorted(required_files.difference(copied_files))
    if missing:
        missing_label = ", ".join(missing)
        raise FileNotFoundError(f"Missing required report static asset(s): {missing_label}")

    return {
        "css_url": REPORT_CSS_ASSET_FILENAME,
        "js_url": REPORT_JS_ASSET_FILENAME,
    }


def _to_pacific_timestamp(value: pd.Timestamp) -> pd.Timestamp:
    if value.tzinfo is None:
        localized = value.tz_localize(
            PACIFIC_TIMEZONE_NAME,
            nonexistent="shift_forward",
            ambiguous="NaT",
        )
    else:
        localized = value.tz_convert(PACIFIC_TIMEZONE_NAME)
    return localized


def _serialize_value(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        converted = _to_pacific_timestamp(value)
        if pd.isna(converted):
            return None
        return converted.isoformat()
    if isinstance(value, pd.Timedelta):
        return str(value)
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, pd.Timestamp):
        converted = _to_pacific_timestamp(value)
        if pd.isna(converted):
            return None
        return converted.isoformat()
    if isinstance(value, pd.Timedelta):
        return str(value)
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (int, str, bool)) or value is None:
        return value
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _slugify_path_component(value: Any) -> str:
    normalized = re.sub(r"[^a-z0-9_-]+", "-", str(value or "").strip().lower()).strip("-")
    return normalized or "analysis"


def _coerce_bucket_minutes(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return int(value) if value > 0 else None
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        rounded = int(round(value))
        return rounded if rounded > 0 else None
    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            return None
        try:
            parsed = float(trimmed)
        except ValueError:
            return None
        if not math.isfinite(parsed):
            return None
        rounded = int(round(parsed))
        return rounded if rounded > 0 else None
    return None


def _canonical_name_to_display_name(value: Any) -> str:
    canonical_name = str(value or "").strip()
    if not canonical_name:
        return ""
    if "|" not in canonical_name:
        return canonical_name
    last_name, first_name = canonical_name.split("|", 1)
    display_name = f"{last_name.strip()}, {first_name.strip()}".strip(", ").strip()
    return display_name if display_name else canonical_name


def _write_json_payload(path: Path, payload: Any) -> int:
    encoded = json.dumps(
        _json_safe(payload),
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(encoded, encoding="utf-8")
    return len(encoded.encode("utf-8"))


def _ordered_chart_ids_for_analysis(analysis_entry: Any) -> list[str]:
    if not isinstance(analysis_entry, dict):
        return []
    seen: set[str] = set()
    ordered: list[str] = []
    hero = str(analysis_entry.get("hero_chart_id") or "").strip()
    if hero:
        seen.add(hero)
        ordered.append(hero)
    details = analysis_entry.get("detail_chart_ids")
    if isinstance(details, list):
        for raw in details:
            chart_id = str(raw or "").strip()
            if not chart_id or chart_id in seen:
                continue
            seen.add(chart_id)
            ordered.append(chart_id)
    return ordered


def _build_chart_data_manifest(
    out_dir: Path,
    interactive_charts: dict[str, Any],
) -> tuple[dict[str, Any], int]:
    charts_raw = interactive_charts.get("charts", {})
    charts: dict[str, list[dict[str, Any]]] = {}
    if isinstance(charts_raw, dict):
        for chart_id, rows in charts_raw.items():
            normalized_id = str(chart_id or "").strip()
            if not normalized_id:
                continue
            charts[normalized_id] = rows if isinstance(rows, list) else []

    analysis_catalog_raw = interactive_charts.get("analysis_catalog", [])
    analysis_catalog = (
        analysis_catalog_raw if isinstance(analysis_catalog_raw, list) else []
    )

    chart_to_analysis: dict[str, str] = {}
    analysis_to_chart_ids: dict[str, list[str]] = {}
    analysis_order: list[str] = []
    for entry in analysis_catalog:
        if not isinstance(entry, dict):
            continue
        analysis_id = str(entry.get("id") or "").strip()
        if not analysis_id:
            continue
        analysis_order.append(analysis_id)
        ordered_chart_ids = _ordered_chart_ids_for_analysis(entry)
        analysis_to_chart_ids[analysis_id] = ordered_chart_ids
        for chart_id in ordered_chart_ids:
            chart_to_analysis[chart_id] = analysis_id

    shared_chart_ids = sorted(
        [chart_id for chart_id in charts.keys() if chart_id not in chart_to_analysis]
    )
    if shared_chart_ids:
        shared_analysis_id = "__shared__"
        analysis_order.append(shared_analysis_id)
        analysis_to_chart_ids[shared_analysis_id] = shared_chart_ids
        for chart_id in shared_chart_ids:
            chart_to_analysis[chart_id] = shared_analysis_id

    used_slug_paths: set[str] = set()
    analysis_slug_map: dict[str, str] = {}
    for analysis_id in analysis_order:
        base = _slugify_path_component(analysis_id)
        slug = base
        suffix = 2
        while slug in used_slug_paths:
            slug = f"{base}-{suffix}"
            suffix += 1
        used_slug_paths.add(slug)
        analysis_slug_map[analysis_id] = slug

    analysis_manifest: dict[str, dict[str, Any]] = {}
    all_urls: list[str] = []
    shard_bytes_total = 0
    analyses_root = out_dir / REPORT_DATA_DIRECTORY / "analyses"
    for analysis_id in analysis_order:
        chart_ids = analysis_to_chart_ids.get(analysis_id, [])
        slug = analysis_slug_map[analysis_id]
        base_rows_by_chart: dict[str, list[dict[str, Any]]] = {}
        bucket_rows_by_bucket: dict[int, dict[str, list[dict[str, Any]]]] = {}
        chart_bucket_options: dict[str, list[int]] = {}

        for chart_id in chart_ids:
            rows = charts.get(chart_id, [])
            row_buckets: dict[int, list[dict[str, Any]]] = defaultdict(list)
            row_base: list[dict[str, Any]] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                bucket_minutes = _coerce_bucket_minutes(row.get("bucket_minutes"))
                if bucket_minutes is None:
                    row_base.append(row)
                else:
                    row_buckets[bucket_minutes].append(row)
            if row_base:
                base_rows_by_chart[chart_id] = row_base
            chart_bucket_options[chart_id] = sorted(row_buckets.keys())
            for bucket_minutes, bucket_rows in row_buckets.items():
                chart_rows = bucket_rows_by_bucket.setdefault(bucket_minutes, {})
                chart_rows[chart_id] = bucket_rows

        analysis_dir = analyses_root / slug
        base_file = analysis_dir / "base.json"
        base_url = f"{REPORT_DATA_DIRECTORY}/analyses/{slug}/base.json"
        shard_bytes_total += _write_json_payload(
            base_file,
            {
                "analysis_id": analysis_id,
                "bucket_minutes": None,
                "charts": base_rows_by_chart,
            },
        )
        all_urls.append(base_url)

        bucket_urls: dict[str, str] = {}
        for bucket_minutes in sorted(bucket_rows_by_bucket.keys()):
            bucket_key = str(bucket_minutes)
            bucket_file = analysis_dir / f"bucket-{bucket_key}m.json"
            bucket_url = f"{REPORT_DATA_DIRECTORY}/analyses/{slug}/bucket-{bucket_key}m.json"
            shard_bytes_total += _write_json_payload(
                bucket_file,
                {
                    "analysis_id": analysis_id,
                    "bucket_minutes": bucket_minutes,
                    "charts": bucket_rows_by_bucket.get(bucket_minutes, {}),
                },
            )
            bucket_urls[bucket_key] = bucket_url
            all_urls.append(bucket_url)

        analysis_manifest[analysis_id] = {
            "base_url": base_url,
            "bucket_urls": bucket_urls,
            "chart_ids": chart_ids,
            "chart_bucket_options": chart_bucket_options,
        }

    return (
        {
            "version": 1,
            "analysis": analysis_manifest,
            "chart_to_analysis": chart_to_analysis,
            "all_urls": all_urls,
        },
        shard_bytes_total,
    )


def _build_report_data_payload(
    out_dir: Path,
    *,
    artifact_rows_safe: dict[str, Any],
    detector_summaries_safe: dict[str, Any],
    table_previews_safe: dict[str, Any],
    table_column_docs_safe: dict[str, Any],
    table_help_docs_safe: dict[str, Any],
    interactive_charts_safe: dict[str, Any],
) -> tuple[dict[str, Any], int]:
    chart_data_manifest, shard_bytes_total = _build_chart_data_manifest(
        out_dir=out_dir,
        interactive_charts=interactive_charts_safe,
    )
    interactive_without_rows = dict(interactive_charts_safe)
    interactive_without_rows["charts"] = {}
    interactive_without_rows["chart_data_manifest"] = chart_data_manifest

    return (
        {
            "artifact_rows": artifact_rows_safe,
            "detector_summaries": detector_summaries_safe,
            "table_previews": table_previews_safe,
            "table_column_docs": table_column_docs_safe,
            "table_help_docs": table_help_docs_safe,
            "interactive_charts": interactive_without_rows,
        },
        shard_bytes_total,
    )


def _preview_columns_for_detector_table(
    detector_name: str,
    table_name: str,
) -> list[str] | None:
    if detector_name == "voter_registry_match" and table_name == "unmatched_names":
        return [
            "display_name",
            "n_rows",
            "n_pro",
            "n_con",
            "top_caveat",
            "best_similarity_score",
            "candidate_pool_size",
        ]
    if detector_name == "duplicates_exact":
        preview_columns: dict[str, list[str]] = {
            "per_name_tests": [
                "scope",
                "canonical_name",
                "display_name",
                "observed_count",
                "n_pro",
                "n_con",
                "time_span_minutes",
            ],
            "per_name_display": [
                "scope",
                "canonical_name",
                "display_name",
                "observed_count",
                "n_pro",
                "n_con",
                "time_span_minutes",
            ],
            "per_name_anomalies": [
                "scope",
                "canonical_name",
                "display_name",
                "n",
                "n_pro",
                "n_con",
                "time_span_minutes",
            ],
            "repeated_same_bucket": [
                "canonical_name",
                "bucket_start",
                "bucket_minutes",
                "n",
                "n_pro",
                "n_con",
                "n_unknown",
                "bucket_end",
            ],
        }
        return preview_columns.get(table_name)
    if detector_name != "off_hours":
        return None
    preview_columns: dict[str, list[str]] = {
        "off_hours_summary": [
            "off_hours",
            "on_hours",
            "off_hours_ratio",
            "off_hours_pro_rate",
            "on_hours_pro_rate",
            "primary_bucket_minutes",
            "primary_baseline_method",
            "alert_off_hours_min_fraction",
            "primary_alert_min_abs_delta",
            "off_hours_windows_alert_eligible",
            "off_hours_windows_alert_eligible_low_power",
            "off_hours_windows_alert_eligible_tested_fraction",
            "off_hours_windows_alert_eligible_low_power_fraction",
            "off_hours_windows_tested",
            "off_hours_windows_below_primary_control_998",
            "off_hours_windows_above_primary_control_998",
            "off_hours_windows_significant_primary",
            "off_hours_windows_significant_primary_two_sided",
            "off_hours_windows_primary_spc_998_any",
            "off_hours_windows_primary_fdr_two_sided",
            "off_hours_windows_primary_flag_any",
            "off_hours_windows_primary_flag_both",
            "off_hours_windows_primary_spc_998_any_fraction",
            "off_hours_windows_primary_fdr_two_sided_fraction",
            "off_hours_windows_primary_flag_any_fraction",
            "off_hours_windows_primary_flag_both_fraction",
            "off_hours_windows_primary_alert",
            "off_hours_windows_primary_alert_fraction",
            "off_hours_primary_alert_run_count",
            "off_hours_primary_alert_max_run_minutes",
            "off_hours_windows_model_available",
            "off_hours_min_primary_delta",
            "off_hours_min_primary_z",
            "day_adjusted_fdr_alpha",
            "model_fit_min_rows",
            "model_hour_harmonics",
            "primary_model_fit_method",
            "primary_model_fit_rows",
            "primary_model_fit_unique_days",
            "primary_model_fit_unique_hours",
            "primary_model_fit_converged",
            "primary_model_fit_aic",
        ],
        "window_control_profile": [
            "bucket_start",
            "bucket_minutes",
            "is_alert_off_hours_window",
            "is_off_hours_window",
            "is_pure_off_hours_window",
            "n_total",
            "n_known",
            "n_pro",
            "n_con",
            "pro_rate",
            "is_low_power",
            "expected_pro_rate_primary",
            "delta_pro_rate_primary",
            "control_low_95_primary",
            "control_low_998_primary",
            "control_high_95_primary",
            "control_high_998_primary",
            "z_score_primary",
            "q_value_primary",
            "is_significant_primary",
            "is_below_primary_control_998",
            "is_above_primary_control_998",
            "is_material_primary_lower_shift",
            "is_primary_alert_window",
            "is_primary_spc_998_two_sided",
            "is_primary_fdr_two_sided",
            "is_primary_any_flag_channel",
            "is_primary_both_flag_channels",
        ],
        "model_fit_diagnostics": [
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
        ],
        "flag_channel_summary": [
            "rank",
            "channel",
            "channel_label",
            "count",
            "share_of_tested",
        ],
        "flagged_window_diagnostics": [
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
            "model_fit_method",
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_used_harmonics",
        ],
        "date_hour_distribution": [
            "date",
            "day_of_week",
            "hour",
            "n_total",
            "n_known",
            "n_pro",
            "n_con",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "n_off_hours",
            "off_hours_fraction",
        ],
        "date_hour_primary_residual_distribution": [
            "bucket_minutes",
            "date",
            "day_of_week",
            "hour",
            "n_windows",
            "n_windows_alert_eligible",
            "n_windows_tested",
            "n_windows_low_power",
            "n_windows_primary_alert",
            "primary_alert_fraction_tested",
            "n_total",
            "n_known",
            "n_known_tested",
            "n_pro",
            "n_con",
            "off_hours_fraction",
            "pro_rate",
            "expected_pro_rate_primary",
            "delta_pro_rate_primary",
            "z_score_primary",
            "z_score_primary_median",
            "z_score_primary_abs_max",
            "is_low_power",
        ],
        "hour_of_week_distribution": [
            "day_of_week",
            "day_of_week_index",
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "n_off_hours",
            "off_hours_fraction",
        ],
        "hourly_distribution": [
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
    }
    return preview_columns.get(table_name)


_DUPLICATES_EXACT_FULL_PREVIEW_TABLES = frozenset(
    {
        "per_name_tests",
        "per_name_display",
        "per_name_anomalies",
        "repeated_same_bucket",
        "position_switching_names",
        "top_repeated_names",
        "top_name_timing_by_mode",
    }
)


def _preview_row_limit_for_detector_table(
    detector_name: str,
    table_name: str,
    *,
    default_max_rows: int,
) -> int | None:
    if (
        detector_name == "duplicates_exact"
        and table_name in _DUPLICATES_EXACT_FULL_PREVIEW_TABLES
    ):
        return None
    return default_max_rows


def _prepare_table_for_preview(
    detector_name: str,
    table_name: str,
    table: pd.DataFrame,
) -> pd.DataFrame:
    if table.empty:
        return table
    if detector_name == "duplicates_exact":
        prepared = table.copy()
        if table_name in {"per_name_tests", "per_name_display", "per_name_anomalies"}:
            count_column = (
                "observed_count"
                if "observed_count" in prepared.columns
                else "n"
                if "n" in prepared.columns
                else None
            )
            if count_column is not None:
                counts = pd.to_numeric(prepared[count_column], errors="coerce")
                prepared = prepared[counts >= 2].copy()
            sort_columns: list[str] = []
            ascending: list[bool] = []
            if count_column is not None and count_column in prepared.columns:
                sort_columns.append(count_column)
                ascending.append(False)
            if "display_name" in prepared.columns:
                sort_columns.append("display_name")
                ascending.append(True)
            if "canonical_name" in prepared.columns:
                sort_columns.append("canonical_name")
                ascending.append(True)
            if sort_columns:
                prepared = prepared.sort_values(sort_columns, ascending=ascending)
            return prepared
        if table_name == "repeated_same_bucket":
            sort_columns = [
                column
                for column in ("bucket_minutes", "bucket_start", "canonical_name")
                if column in prepared.columns
            ]
            if sort_columns:
                prepared = prepared.sort_values(sort_columns)
            return prepared
        return prepared

    if detector_name != "voter_registry_match" or table_name != "unmatched_names":
        return table

    prepared = table.copy()
    if "canonical_name" in prepared.columns:
        canonical_display_names = (
            prepared["canonical_name"].fillna("").astype(str).map(_canonical_name_to_display_name)
        )
    else:
        canonical_display_names = pd.Series("", index=prepared.index, dtype=str)

    if "display_name" not in prepared.columns:
        prepared["display_name"] = canonical_display_names
    else:
        prepared["display_name"] = prepared["display_name"].fillna("").astype(str)
        prepared["display_name"] = prepared["display_name"].where(
            prepared["display_name"].str.strip() != "",
            canonical_display_names,
        )
    return prepared


def _table_preview(
    df: pd.DataFrame,
    max_rows: int | None = 12,
    *,
    columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    limited = df.copy() if max_rows is None else df.head(max_rows).copy()
    if columns:
        selected_columns = [column for column in columns if column in limited.columns]
        if selected_columns:
            limited = limited[selected_columns]
    for column in limited.columns:
        limited[column] = limited[column].map(_serialize_value)
    return _json_safe(limited.to_dict(orient="records"))


def _load_summaries_from_disk(out_dir: Path) -> dict[str, dict[str, Any]]:
    summary_dir = out_dir / "summary"
    if not summary_dir.exists():
        return {}

    summaries: dict[str, dict[str, Any]] = {}
    for path in sorted(summary_dir.glob("*.json")):
        with path.open("r", encoding="utf-8") as handle:
            summaries[path.stem] = json.load(handle)
    return summaries


def _artifact_rows_from_disk(out_dir: Path) -> dict[str, int]:
    artifacts_dir = out_dir / "artifacts"
    if not artifacts_dir.exists():
        return {}

    rows: dict[str, int] = {}
    for path in sorted(artifacts_dir.iterdir()):
        if path.suffix == ".parquet":
            if pq is not None:
                rows[path.stem] = int(pq.ParquetFile(path).metadata.num_rows)
        elif path.suffix == ".csv":
            with path.open("r", encoding="utf-8") as handle:
                line_count = sum(1 for _ in handle)
            rows[path.stem] = max(line_count - 1, 0)
    return rows


def _table_previews_from_results(
    results: dict[str, DetectorResult],
    max_rows: int = 12,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    previews: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for detector_name, result in sorted(results.items()):
        detector_tables: dict[str, list[dict[str, Any]]] = {}
        for table_name, table in sorted(result.tables.items()):
            if table.empty:
                continue
            table_max_rows = _preview_row_limit_for_detector_table(
                detector_name,
                table_name,
                default_max_rows=max_rows,
            )
            table = _prepare_table_for_preview(detector_name, table_name, table)
            detector_tables[table_name] = _table_preview(
                table,
                max_rows=table_max_rows,
                columns=_preview_columns_for_detector_table(detector_name, table_name),
            )
        if detector_tables:
            previews[detector_name] = detector_tables
    return previews


def _load_table_previews_from_disk(
    out_dir: Path,
    max_rows: int = 12,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    tables_dir = out_dir / "tables"
    if not tables_dir.exists():
        return {}

    previews: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(dict)
    for path in sorted(tables_dir.iterdir()):
        if "__" not in path.stem:
            continue
        detector_name, table_name = path.stem.split("__", 1)
        table_max_rows = _preview_row_limit_for_detector_table(
            detector_name,
            table_name,
            default_max_rows=max_rows,
        )

        table: pd.DataFrame
        try:
            if path.suffix == ".csv":
                table = (
                    pd.read_csv(path)
                    if table_max_rows is None
                    else pd.read_csv(path, nrows=table_max_rows)
                )
            elif path.suffix == ".parquet":
                table = (
                    pd.read_parquet(path)
                    if table_max_rows is None
                    else pd.read_parquet(path).head(table_max_rows)
                )
            else:
                continue
        except Exception:
            continue

        if table.empty:
            continue
        table = _prepare_table_for_preview(detector_name, table_name, table)
        previews[detector_name][table_name] = _table_preview(
            table,
            max_rows=table_max_rows,
            columns=_preview_columns_for_detector_table(detector_name, table_name),
        )

    return dict(previews)


def _load_frame_from_candidates(candidates: list[Path]) -> pd.DataFrame:
    for path in candidates:
        if not path.exists():
            continue
        try:
            if path.suffix == ".parquet":
                return pd.read_parquet(path)
            if path.suffix == ".csv":
                return pd.read_csv(path)
        except Exception:
            continue
    return pd.DataFrame()


def _records_from_frame(
    frame: pd.DataFrame,
    columns: list[str],
    max_rows: int | None = None,
) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    selected = [column for column in columns if column in frame.columns]
    if not selected:
        return []
    working = frame[selected].copy()
    if max_rows is not None:
        working = working.head(max_rows)
    for column in working.columns:
        working[column] = working[column].map(_serialize_value)
    return _json_safe(working.to_dict(orient="records"))


def _table_key(detector: str, table_name: str) -> str:
    return f"{detector}.{table_name}"


def _humanize_identifier(value: str) -> str:
    return " ".join(token for token in str(value).strip().replace("-", "_").split("_") if token)


def _default_column_description(column: str) -> str:
    label = _humanize_identifier(column)
    if not label:
        return "Column value from detector output."
    lower = str(column).lower()
    if lower.startswith("n_"):
        return f"Count of {_humanize_identifier(lower[2:])} in this row grouping."
    if lower.endswith("_rate"):
        return f"Proportion metric for {label}, on a 0 to 1 scale."
    if lower.endswith("_ratio"):
        return f"Ratio metric for {label}; compare against section baseline/threshold context."
    if lower.endswith("_wilson_low"):
        base = _humanize_identifier(lower.removesuffix("_wilson_low"))
        return f"Lower Wilson confidence bound for {base}."
    if lower.endswith("_wilson_high"):
        base = _humanize_identifier(lower.removesuffix("_wilson_high"))
        return f"Upper Wilson confidence bound for {base}."
    if lower.startswith("is_"):
        return f"Boolean indicator for {label}."
    if "minute" in lower or "hour" in lower or lower.endswith("_time") or lower.endswith("_date"):
        return f"Time coordinate for {label}."
    return f"Detector output field for {label}."


def _describe_column(column: str) -> str:
    normalized = str(column or "").strip()
    if not normalized:
        return "Column value from detector output."
    return _COLUMN_DESCRIPTION_OVERRIDES.get(normalized, _default_column_description(normalized))


def _table_column_docs_from_rows(rows: list[dict[str, Any]]) -> dict[str, str]:
    if not rows:
        return {}
    ordered_columns: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for column in row.keys():
            key = str(column)
            if key in seen:
                continue
            seen.add(key)
            ordered_columns.append(key)
    return {column: _describe_column(column) for column in ordered_columns}


def _build_table_column_docs(
    table_previews: dict[str, dict[str, list[dict[str, Any]]]],
    artifact_rows: dict[str, int],
) -> dict[str, dict[str, str]]:
    docs: dict[str, dict[str, str]] = {}
    for detector_name, detector_tables in sorted(table_previews.items()):
        for table_name, rows in sorted(detector_tables.items()):
            key = _table_key(detector_name, table_name)
            docs[key] = _table_column_docs_from_rows(rows)

    docs["artifacts.artifact_rows"] = _table_column_docs_from_rows(
        [
            {"artifact": artifact_name, "rows": row_count}
            for artifact_name, row_count in sorted(artifact_rows.items())
        ]
    )

    return docs


def _build_table_help_docs(
    table_column_docs: dict[str, dict[str, str]],
) -> dict[str, dict[str, str]]:
    docs: dict[str, dict[str, str]] = {}
    for table_key, column_docs in sorted(table_column_docs.items()):
        column_names = list(column_docs.keys())
        has_rate = any(
            name.endswith("_rate") or "ratio" in name or "percentile" in name
            for name in column_names
        )
        has_counts = any(
            name.startswith("n_") or name in {"count", "rows", "n"}
            for name in column_names
        )
        has_time = any(
            token in name
            for name in column_names
            for token in ("minute", "hour", "bucket", "date", "time")
        )
        detector_label = _humanize_identifier(table_key.replace(".", " "))
        first_columns = ", ".join(column_names[:6]) if column_names else "no preview columns"

        value_context = []
        if has_rate:
            value_context.append("rate/proportion columns")
        if has_counts:
            value_context.append("volume/count columns")
        if has_time:
            value_context.append("time keys")
        context_text = ", ".join(value_context) if value_context else "detector-specific fields"

        docs[table_key] = {
            "what_is_this": (
                f"This table is a preview of {detector_label}. "
                "It exposes row-level values behind chart aggregates so you can inspect "
                "the exact buckets, categories, and flags that produced a visual signal. "
                "Use it when you need to answer which concrete records created a peak, "
                "dip, or anomaly marker."
            ),
            "why_it_matters": (
                "Tables are essential for auditability: they let you sort, filter, "
                "and verify whether visual anomalies are supported by real volume, "
                "consistent metadata, and non-sparse support. "
                "They also reveal false positives where a chart looks dramatic but "
                "underlying rows are low-power or internally contradictory."
            ),
            "how_to_interpret": (
                "Start with key columns and filter around flagged times/categories. "
                f"This table includes {context_text}. "
                "Read left-to-right from identifiers to volume/rate fields to flags, "
                "and compare adjacent rows to separate isolated outliers from "
                "persistent structure. Use the column glossary to avoid over-"
                "interpreting similarly named fields with different semantics."
            ),
            "what_to_look_for": (
                "Look for rows where multiple indicators move together (for example, "
                "high counts plus directional rates plus flags), and check whether "
                "those rows cluster in adjacent windows. "
                "Strong evidence usually appears as recurring patterns across nearby "
                "rows, not single extreme entries."
            ),
            "momentary_high_low": (
                "A single extreme row can be a genuine event or a sparse-data outlier. "
                "Momentary lows can be normal lulls; validate by checking neighboring "
                "rows and low-power indicators. "
                "Short-lived highs often map to reminders, queue releases, or reporting "
                "timing; short-lived lows often map to expected inactivity or ingest lag."
            ),
            "extended_high_low": (
                "Extended runs of high/low values across many rows are stronger signs "
                "of regime-level behavior. Persistent shifts that also align with "
                "chart-level signals are higher-confidence anomalies. "
                "Extended highs may indicate sustained mobilization or process skew; "
                "extended lows may indicate suppressed activity, missing data segments, "
                "or a stable low-intensity baseline."
            ),
            "column_highlight": (
                f"Primary columns in this preview: {first_columns}."
            ),
        }

    return docs


def _detailed_what_to_look_for_by_analysis() -> dict[str, list[str]]:
    return {
        "baseline_profile": [
            (
                "Short, isolated spikes in volume with no matching shift in pro rate "
                "or corroborating detector flags are often random campaign pulses "
                "rather than systemic manipulation."
            ),
            (
                "Extended level shifts (for example, 60-240 minutes) in both volume "
                "and composition, especially when Wilson bands tighten, suggest a "
                "meaningful regime change worth cross-checking against changepoints "
                "and composite evidence."
            ),
            (
                "Very low overnight volume can create dramatic percentage swings; "
                "prioritize windows where elevated rates persist after local volume "
                "recovers into daytime traffic."
            ),
        ],
        "bursts": [
            (
                "Single-window rate-ratio peaks can be benign; stronger signals are "
                "contiguous runs of elevated rate ratios that recur at multiple window "
                "sizes (for example 5m and 30m both elevated)."
            ),
            (
                "High observed counts with low q-values in sustained windows imply "
                "concentration beyond baseline expectation, especially when these "
                "bursts overlap with duplicate-name or swing anomalies."
            ),
            (
                "Suppressed or unusually flat burst activity can also be informative "
                "if baseline volume is high; a lack of natural variability may "
                "indicate synchronized intake behavior or batching."
            ),
        ],
        "procon_swings": [
            (
                "Brief pro-rate jumps with wide Wilson intervals typically indicate "
                "low-support noise; treat them as weak unless adjacent buckets move "
                "in the same direction with tighter intervals."
            ),
            (
                "Extended daytime streaks of positive or negative shifts (multiple "
                "contiguous buckets) can indicate directional mobilization, queueing "
                "effects, or operational gating; confirm with day/hour and "
                "time-of-day panels."
            ),
            (
                "Large off-hours directional blocks that reverse at wake-hour "
                "transitions may indicate temporally segmented participation behavior, "
                "including potential strategic timing by one side."
            ),
        ],
        "changepoints": [
            (
                "Look for clustered breakpoints across both volume and pro rate; "
                "multi-metric co-occurrence is usually more meaningful than a "
                "solitary break in one metric."
            ),
            (
                "Large absolute deltas with sustained post-break behavior "
                "(not immediate reversion) indicate structural transitions rather "
                "than transient spikes."
            ),
            (
                "Repeated changes at similar hours across days can reflect "
                "operational schedules; treat as lower risk unless change magnitudes "
                "are extreme and detector corroboration is strong."
            ),
        ],
        "off_hours": [
            (
                "Prioritize robust primary alerts (below primary 99.8% lower limit, "
                "lower-tail FDR-significant, and materially negative delta at "
                "adequate support); avoid interpreting low-n windows even if raw "
                "rates look extreme."
            ),
            (
                "If tested off-hours windows are zero after low-power filtering, treat "
                "the section as descriptive-only and avoid inferential conclusions."
            ),
            (
                "Use the funnel view to compare primary and global expected bands; "
                "treat primary-baseline breaches as the decision metric and global "
                "bands as context."
            ),
            (
                "In date-hour heatmaps, repeated overnight blocks across multiple "
                "dates are stronger than a single-night dip; corroborate with burst, "
                "periodicity, and duplicate detectors before escalation."
            ),
        ],
        "duplicates_exact": [
            (
                "Short bursts of repeated names in tiny windows may occur during "
                "legitimate group actions; concern rises when concentration repeats "
                "across multiple larger buckets."
            ),
            (
                "Names that appear repeatedly while switching pro/con positions are "
                "higher-priority review targets because they indicate inconsistent "
                "stance representation under one canonical identity."
            ),
            (
                "Persistent duplicate concentration during otherwise stable baseline "
                "periods can imply scripted submissions or queue replay effects "
                "rather than organic participation."
            ),
        ],
        "sortedness": [
            (
                "Single alphabetical spikes in small buckets can be accidental; "
                "repeated elevated alphabetical ratios across 15m-120m buckets "
                "suggest process-level ordering behavior."
            ),
            (
                "Sustained ordered streaks during high-volume windows are unusual "
                "for organic arrivals and may imply batch uploads, sorted lists, or "
                "deterministic queue processing."
            ),
            (
                "Low sortedness is expected for organic traffic, so abrupt "
                "transitions from unsorted to highly sorted and back are more "
                "informative than consistently modest ratios."
            ),
        ],
        "rare_names": [
            (
                "Short-lived unique-ratio increases during low volume can be "
                "misleading; investigate when unique-ratio elevation persists into "
                "higher-support windows."
            ),
            (
                "Concurrent rises in weirdness scores, singleton concentration, and "
                "rarity quantiles indicate novelty concentration beyond normal "
                "lexical drift."
            ),
            (
                "Extended rarity suppression (unusually low novelty) can also be "
                "noteworthy in broad public hearings and may suggest repeated "
                "template populations."
            ),
        ],
        "org_anomalies": [
            (
                "Blank-organization spikes in low-support windows are weak evidence; "
                "prioritize wide windows where blank rate rises and Wilson bands "
                "remain narrow."
            ),
            (
                "Divergence between pro and con blank-org rates over sustained "
                "periods can indicate side-specific form behavior, campaign guidance, "
                "or data-entry heterogeneity."
            ),
            (
                "Sharp blank-rate reversals around specific times may indicate UX "
                "changes, batch imports, or conditional form paths and should be "
                "checked against operational logs."
            ),
        ],
        "voter_registry_match": [
            (
                "Interpret primary linkage through conservative outcomes "
                "(matched unique, matched ambiguous, unmatched) and keep unmatched "
                "language scoped to the WA active voter file."
            ),
            (
                "Compare unmatched-rate differences at both row and unique-name units; "
                "pairwise tests are strongest when support is adequate and adjacent "
                "windows corroborate the pattern."
            ),
            (
                "Use balanced and broad sensitivity panels to assess how strong/weak "
                "fuzzy assumptions move outcomes before interpreting directional claims."
            ),
        ],
        "periodicity": [
            (
                "Minor periodic peaks are normal in outreach-driven datasets; "
                "stronger signals appear when clock-face concentration, "
                "autocorrelation peaks, and spectrum peaks align."
            ),
            (
                "Narrow high-power peaks at specific periods (for example near exact "
                "campaign cadence intervals) can indicate automation or tightly "
                "scheduled reminders."
            ),
            (
                "Extended suppression of expected periodic structure in otherwise "
                "campaign-heavy contexts may imply missing intervals or "
                "preprocessing artifacts."
            ),
        ],
        "multivariate_anomalies": [
            (
                "Single high anomaly buckets with low support can be model-noise; "
                "prioritize consecutive high-score windows with model eligibility and "
                "corroborating detector evidence."
            ),
            (
                "Joint excursions in volume, duplicate fraction, blank-org rate, and "
                "pro-rate shape are stronger than any one feature spike in isolation."
            ),
            (
                "Extended high-percentile stretches can indicate sustained "
                "behavioral mode changes; inspect top buckets and feature projection "
                "for which dimensions drive score elevation."
            ),
        ],
        "composite_score": [
            (
                "High composite windows are most useful when evidence-count is high "
                "and signals come from independent detectors rather than one "
                "detector repeated across scales."
            ),
            (
                "Short isolated composite spikes can still be benign; extended "
                "elevated runs with overlapping burst/swing/changepoint/ML evidence "
                "are higher-priority review candidates."
            ),
            (
                "Very low composite scores during known high-activity periods can "
                "reveal under-sensitive detector settings or data-quality gaps and "
                "should trigger configuration review."
            ),
        ],
    }


def _analysis_help_hints() -> dict[str, dict[str, str]]:
    return {
        "baseline_profile": {
            "primary_metric": "baseline volume and composition drift",
            "momentary_high": (
                "a short notice event, reminder blast, or temporary queue release"
            ),
            "momentary_low": (
                "normal minute-level quiet periods or ingest timing jitter"
            ),
            "extended_high": (
                "a sustained participation regime shift that can affect all downstream "
                "detectors"
            ),
            "extended_low": (
                "potential ingestion gaps, hearing lulls, or sustained reduced campaign "
                "activity"
            ),
        },
        "bursts": {
            "primary_metric": "observed-vs-expected burst intensity",
            "momentary_high": (
                "legitimate synchronized outreach or one-off reminder cascades"
            ),
            "momentary_low": (
                "normal random fluctuation when expected baseline is already elevated"
            ),
            "extended_high": (
                "repeated concentration windows that deserve correlation with duplicate "
                "and swing signals"
            ),
            "extended_low": (
                "suppressed variance that can indicate workflow smoothing or batching"
            ),
        },
        "procon_swings": {
            "primary_metric": (
                "directional pro/con ratio movement relative to expected bands"
            ),
            "momentary_high": "small-sample randomness, especially in low-power buckets",
            "momentary_low": (
                "brief balancing waves where opposite-side submissions cluster together"
            ),
            "extended_high": (
                "persistent directional mobilization or process-side skew in intake "
                "timing"
            ),
            "extended_low": (
                "prolonged suppression of one side that may indicate queueing or "
                "campaign fatigue"
            ),
        },
        "changepoints": {
            "primary_metric": "structural breaks in level or composition",
            "momentary_high": (
                "single regime boundaries caused by predictable hearing state "
                "transitions"
            ),
            "momentary_low": (
                "noisy micro-fluctuations that do not persist across adjacent windows"
            ),
            "extended_high": (
                "multi-break episodes indicating stable before/after behavioral regimes"
            ),
            "extended_low": "a relatively stationary process with fewer systemic shifts",
        },
        "off_hours": {
            "primary_metric": "model-aware off-hours composition shift with volume context",
            "momentary_high": (
                "short overnight swings that stay within primary control limits after "
                "day/hour adjustment"
            ),
            "momentary_low": (
                "small support windows where apparent extremes are likely sampling noise"
            ),
            "extended_high": (
                "repeating robust primary alerts across adjacent windows or nights at "
                "moderate/high support"
            ),
            "extended_low": (
                "stable overnight behavior that remains inside expected primary "
                "control bands"
            ),
        },
        "duplicates_exact": {
            "primary_metric": "exact repeated-name concentration",
            "momentary_high": (
                "household/shared-name collisions or small coordinated batches"
            ),
            "momentary_low": "normal diversity of distinct names in organic intake",
            "extended_high": (
                "repeat-name patterns likely to influence authenticity and weighting "
                "assumptions"
            ),
            "extended_low": "healthy name diversity with limited exact repetition pressure",
        },
        "sortedness": {
            "primary_metric": "alphabetical/ordered submission behavior",
            "momentary_high": (
                "small sorted snippets caused by chance or local administrative handling"
            ),
            "momentary_low": "expected unsorted arrivals from organic user behavior",
            "extended_high": "batch-oriented or deterministic ordering processes across windows",
            "extended_low": (
                "persistent organic ordering noise without process-level sorting artifacts"
            ),
        },
        "rare_names": {
            "primary_metric": "novelty, uniqueness, and rarity concentration",
            "momentary_high": (
                "brief novelty spikes from campaign expansion to new participants"
            ),
            "momentary_low": (
                "common-name clustering or temporary shrinkage in participant diversity"
            ),
            "extended_high": (
                "sustained lexical novelty requiring cross-check against lookup coverage"
            ),
            "extended_low": "repeated-name dominance or limited participant turnover",
        },
        "org_anomalies": {
            "primary_metric": "blank/null organization usage and split behavior",
            "momentary_high": "form UX friction or temporary omission guidance in outreach",
            "momentary_low": "short windows where organization prompts were more salient",
            "extended_high": (
                "systemic metadata sparsity that can bias affiliation interpretation"
            ),
            "extended_low": "more complete organization capture across participation streams",
        },
        "voter_registry_match": {
            "primary_metric": "conservative matched/unmatched composition with uncertainty accounting",
            "momentary_high": "brief matched concentration that may reflect clean registry overlap",
            "momentary_low": "short-lived unmatched growth in sparse buckets",
            "extended_high": "stable conservative matched coverage across windows",
            "extended_low": (
                "persistent unmatched dominance requiring normalization and source review"
            ),
        },
        "periodicity": {
            "primary_metric": "recurring timing structure across minute and lag spaces",
            "momentary_high": "single reminder cycles or one-time timed campaign sends",
            "momentary_low": "flat/noisy slots where periodic patterns are not dominant",
            "extended_high": (
                "repeated cadence signatures that may indicate automation or strict "
                "scheduling"
            ),
            "extended_low": "weak periodic structure consistent with more organic arrival timing",
        },
        "multivariate_anomalies": {
            "primary_metric": "joint anomaly score across multiple behavioral features",
            "momentary_high": (
                "single-bucket feature coincidence without sustained corroboration"
            ),
            "momentary_low": "brief reversion to feature-space baseline",
            "extended_high": (
                "multi-feature regime changes needing manual validation and context "
                "checks"
            ),
            "extended_low": "feature combinations staying near historically typical mixtures",
        },
        "composite_score": {
            "primary_metric": "cross-detector evidence overlap and prioritization",
            "momentary_high": "short-lived detector agreement around a local event",
            "momentary_low": "isolated detector activity without consensus evidence",
            "extended_high": (
                "durable multi-detector agreement that should drive investigation "
                "priority"
            ),
            "extended_low": "broad detector disagreement suggesting mostly baseline behavior",
        },
    }


def _build_analysis_help_docs(
    analysis_definitions: list[dict[str, Any]],
    detailed_look_for: dict[str, list[str]],
) -> dict[str, dict[str, str]]:
    hints = _analysis_help_hints()
    docs: dict[str, dict[str, str]] = {}

    for definition in analysis_definitions:
        analysis_id = str(definition["id"])
        title = str(definition["title"])
        hint = hints.get(analysis_id, {})
        detail_points = detailed_look_for.get(analysis_id, [])
        detail_excerpt = " ".join(detail_points[:3]).strip()
        detail_suffix = (
            detail_excerpt
            if detail_excerpt
            else "Prioritize patterns that persist across adjacent windows and align "
            "with at least one independent detector signal."
        )

        primary_metric = hint.get("primary_metric", "this detector's primary signal")
        momentary_high = hint.get("momentary_high", "a local transient event")
        momentary_low = hint.get("momentary_low", "short-term random variation")
        extended_high = hint.get("extended_high", "a sustained process-level shift")
        extended_low = hint.get("extended_low", "a stable low-intensity regime")

        docs[analysis_id] = {
            "what_is_this": (
                f"{title} focuses on {primary_metric}. "
                "This section combines a hero chart, supporting charts, and tables to "
                "separate one-off noise from meaningful sustained behavior. "
                "Treat it as a detector notebook: start broad, then drill into "
                "specific windows with evidence context."
            ),
            "why_it_matters": (
                "This data matters because it changes how confident you should be in "
                "an anomaly narrative. Strong claims should come from persistent, "
                "well-supported patterns rather than isolated spikes. "
                "It also prevents both over-calling benign fluctuations and missing "
                "slow-burn anomalies that only emerge over longer runs."
            ),
            "how_to_interpret": (
                "Read the hero chart first for the dominant temporal structure, then "
                "use detail charts to test whether the signal repeats across scales, "
                "dayparts, or subgroup splits. Use tables to verify exact values and "
                "support counts behind flagged windows. "
                "When uncertainty bands or low-power markers are present, discount "
                "single-window jumps unless they recur with stronger support."
            ),
            "what_to_look_for": (
                f"{definition['what_to_look_for']} "
                f"{detail_suffix} "
                "Investigation priority should increase when multiple independent views "
                "tell the same story at the same time."
            ),
            "momentary_high_low": (
                "Momentary highs can indicate "
                f"{momentary_high}. Momentary lows can indicate {momentary_low}. "
                "Treat both cautiously when low-power flags are present. "
                "A practical rule: do not escalate on a single bucket unless a nearby "
                "table row and at least one companion chart support the same direction."
            ),
            "extended_high_low": (
                f"Extended highs can indicate {extended_high}. "
                f"Extended lows can indicate {extended_low}. "
                "Persistence across adjacent windows and corroborating detectors raises "
                "confidence that the shift is meaningful. "
                "Extended runs deserve timeline annotation and root-cause notes so later "
                "reviewers can separate operational context from suspicious behavior."
            ),
        }

    return docs


_SCATTER_CHART_IDS = {
    "multivariate_feature_projection",
    "multivariate_top_buckets",
    "off_hours_funnel_plot",
    "duplicates_exact_top_name_timing_exact",
}


def _chart_family(chart_id: str) -> str:
    chart_id_norm = str(chart_id or "")
    if chart_id_norm in _SCATTER_CHART_IDS or "funnel" in chart_id_norm:
        return "scatter"
    if "heatmap" in chart_id_norm:
        return "heatmap"
    if any(
        token in chart_id_norm
        for token in ("timeline", "rates", "ratio", "trend", "bucket", "profile")
    ):
        return "timeseries"
    return "categorical"


def _build_chart_help_docs(
    chart_legend_docs: dict[str, dict[str, Any]],
) -> dict[str, dict[str, str]]:
    docs: dict[str, dict[str, str]] = {}
    for chart_id, legend_doc in sorted(chart_legend_docs.items()):
        summary = str(legend_doc.get("summary") or "").strip()
        legend_items = legend_doc.get("items", [])
        labels = ", ".join(
            str(item.get("label", "")).strip() for item in legend_items if item
        )
        family = _chart_family(chart_id)

        if family == "heatmap":
            docs[chart_id] = {
                "what_is_this": (
                    f"{summary} This is a matrix view where color encodes magnitude "
                    "across paired axes such as date/hour or slot/day. "
                    "Each cell is a compact summary of one intersection, so the chart "
                    "is optimized for pattern shape over exact per-cell precision."
                ),
                "why_it_matters": (
                    "Heatmaps reveal spatially contiguous patterns that line charts can "
                    "hide, especially repeated daypart behavior and slot-level drift. "
                    "They are especially useful for finding regime-like blocks that "
                    "persist across many adjacent cells."
                ),
                "how_to_interpret": (
                    "Scan for contiguous blocks before focusing on single cells. "
                    "Compare high-intensity and low-intensity regions with bucket "
                    "support and related detector outputs. "
                    "Then check whether color transitions occur at meaningful "
                    "boundaries such as day changes, hearing windows, or slot shifts."
                ),
                "what_to_look_for": (
                    "Look for coherent blocks, repeated stripes, or abrupt regime "
                    "boundaries that persist across adjacent rows/columns. "
                    "Short isolated hot/cold cells are weaker evidence; long bands or "
                    "rectangles are stronger. "
                    f"Legend components: {labels}."
                ),
                "momentary_high_low": (
                    "A single hot/cold cell can reflect transient activity or low "
                    "support. Interpret isolated cells cautiously, especially if they "
                    "do not repeat in neighboring slots. "
                    "Momentary highs can map to one reminder wave; momentary lows can "
                    "map to ordinary quiet periods."
                ),
                "extended_high_low": (
                    "Extended hot/cold regions typically indicate sustained behavioral "
                    "mode shifts. Persistence across multiple dates/slots is stronger "
                    "evidence than one transition point. "
                    "Extended hot regions may indicate durable mobilization or process "
                    "bias; extended cold regions may indicate suppression or inactivity."
                ),
            }
            continue

        if family == "scatter":
            docs[chart_id] = {
                "what_is_this": (
                    f"{summary} This scatter plot maps each bucket as a point in "
                    "feature space, often with color and size as additional signals. "
                    "It is a relationship view, showing joint behavior rather than a "
                    "single metric over time."
                ),
                "why_it_matters": (
                    "Scatter views expose joint-feature structure, clusters, and "
                    "outliers that are not visible in one-dimensional summaries. "
                    "They help determine whether anomalies are isolated outliers or "
                    "part of a broader feature-space regime."
                ),
                "how_to_interpret": (
                    "Read axis meaning first, then evaluate whether outliers are "
                    "isolated or part of a cluster. Use color/size encodings to "
                    "understand confidence and support. "
                    "Cross-reference extreme points with time-based charts to determine "
                    "whether they are single events or repeated states."
                ),
                "what_to_look_for": (
                    "Look for detached point clouds, extreme tails, and dense anomaly "
                    "clusters that align with flagged windows. "
                    "A compact cluster far from baseline often carries more weight than "
                    "one far-away point with low support. "
                    f"Legend components: {labels}."
                ),
                "momentary_high_low": (
                    "A single extreme point may be a one-off event or model artifact. "
                    "Validate with timeline charts and table support counts. "
                    "Momentary lows are usually returns toward baseline and are often "
                    "benign unless paired with abrupt nearby outliers."
                ),
                "extended_high_low": (
                    "Large persistent outlier clusters imply broad feature-space drift. "
                    "Extended low-intensity clustering implies stable baseline behavior. "
                    "Sustained dual-cluster structure can indicate mixed populations or "
                    "alternating operational modes."
                ),
            }
            continue

        if family == "timeseries":
            docs[chart_id] = {
                "what_is_this": (
                    f"{summary} This time-aligned view shows how the measured signal "
                    "changes across chronological buckets. "
                    "It is the primary lens for identifying sequence, duration, and "
                    "coincidence with external events."
                ),
                "why_it_matters": (
                    "Time-series structure distinguishes transient spikes from sustained "
                    "regime changes and helps align detector evidence by timestamp. "
                    "Without duration context, it is easy to overreact to one-bucket "
                    "noise and miss broad shifts."
                ),
                "how_to_interpret": (
                    "Read left to right, compare volume with rate/score overlays, and "
                    "pay attention to uncertainty bounds and low-power markers where "
                    "available. "
                    "When zoomed in, verify whether local extremes persist across "
                    "neighboring buckets and remain visible at wider scales."
                ),
                "what_to_look_for": (
                    "Look for repeated peaks, troughs, trend breaks, and persistent "
                    "drifts across adjacent windows. "
                    "Patterns that recur at the same daypart across dates are usually "
                    "stronger than one isolated wave. "
                    f"Legend components: {labels}."
                ),
                "momentary_high_low": (
                    "Short highs/lows can reflect event timing, random variance, or "
                    "small-sample effects. Confirm with neighboring buckets before "
                    "treating them as material anomalies. "
                    "A momentary high near a known outreach time can be benign; a "
                    "momentary low during expected peak periods may indicate data lag."
                ),
                "extended_high_low": (
                    "Extended highs/lows are stronger indicators of behavioral shifts, "
                    "especially when they persist across multiple bucket sizes and "
                    "coincide with corroborating detector outputs. "
                    "Extended highs may indicate sustained mobilization or systematic "
                    "bias; extended lows may indicate prolonged inactivity or missing "
                    "segments."
                ),
            }
            continue

        docs[chart_id] = {
            "what_is_this": (
                f"{summary} This categorical/ranked chart compares values across "
                "labels, groups, or parameter settings. "
                "It emphasizes composition and concentration instead of chronology."
            ),
            "why_it_matters": (
                "Category comparisons show concentration, imbalance, and dominance "
                "patterns that can explain why timeline signals moved. "
                "They are often the fastest way to identify which subgroup is driving "
                "a detector outcome."
            ),
            "how_to_interpret": (
                "Sort by magnitude, compare head vs tail behavior, and relate category "
                "concentration to corresponding detector windows. "
                "Check both absolute values and relative spacing so you can distinguish "
                "true concentration from a uniformly low baseline."
            ),
            "what_to_look_for": (
                "Look for heavy concentration in a few categories, abrupt drop-offs, "
                "or rare categories with disproportionately high values. "
                "A long flat tail with one or two dominant bars often indicates a "
                "targeted driver worth validating in tables. "
                f"Legend components: {labels}."
            ),
            "momentary_high_low": (
                "A single dominant category may come from one campaign event or local "
                "data artifact. Check whether the dominance repeats over time. "
                "Momentary category suppression can also happen when total volume is "
                "temporarily low."
            ),
            "extended_high_low": (
                "Persistent dominance/absence across many categories can indicate "
                "structural participation effects rather than random variation. "
                "Extended concentration deserves follow-up to determine whether it is "
                "policy-driven outreach, operational process, or suspicious patterning."
            ),
        }

    return docs


def _fallback_chart_legend_doc(chart_id: str) -> dict[str, Any]:
    return {
        "summary": "Legend semantics for this chart.",
        "items": [
            {
                "label": "Primary series",
                "description": f"Main plotted signal for {chart_id.replace('_', ' ')}.",
            },
            {
                "label": "Axes",
                "description": (
                    "X-axis encodes time/category context; "
                    "Y-axis encodes magnitude or rate."
                ),
            },
        ],
    }


def _default_chart_legend_docs() -> dict[str, dict[str, Any]]:
    def timebar(
        *,
        summary: str,
        primary_label: str,
        primary_desc: str,
        include_wilson: bool = False,
        include_low_power: bool = True,
        flagged_label: str | None = None,
        flagged_desc: str | None = None,
        extra: list[dict[str, str]] | None = None,
        volume_label: str = "Volume",
        volume_desc: str = "Bars show record volume in each time bucket.",
    ) -> dict[str, Any]:
        items: list[dict[str, str]] = [
            {"label": volume_label, "description": volume_desc},
            {"label": primary_label, "description": primary_desc},
        ]
        if include_wilson:
            items.extend(
                [
                    {
                        "label": "Wilson low / Wilson high",
                        "description": (
                            "Confidence band for the proportion metric; "
                            "wider bands indicate higher uncertainty."
                        ),
                    }
                ]
            )
        if extra:
            items.extend(extra)
        if flagged_label and flagged_desc:
            items.append({"label": flagged_label, "description": flagged_desc})
        if include_low_power:
            items.append(
                {
                    "label": "Low-power",
                    "description": (
                        "Markers for buckets with insufficient support where rates "
                        "can swing from noise."
                    ),
                }
            )
        return {"summary": summary, "items": items}

    docs: dict[str, dict[str, Any]] = {
        "baseline_volume_pro_rate": timebar(
            summary="Baseline trend of submissions and pro share.",
            primary_label="Pro rate",
            primary_desc="Line shows pro-position share per bucket.",
            include_wilson=True,
        ),
        "baseline_day_hour_volume": {
            "summary": "Day/hour baseline heatmap.",
            "items": [
                {
                    "label": "Cell color",
                    "description": (
                        "Darker cells indicate higher submission volume for that "
                        "weekday/hour."
                    ),
                },
                {"label": "X/Y axes", "description": "X-axis is hour of day; Y-axis is weekday."},
            ],
        },
        "baseline_top_names": {
            "summary": "Top-frequency names.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Total submissions associated with each displayed name.",
                },
                {
                    "label": "X-axis names",
                    "description": "Most frequent names (trimmed to top slice for readability).",
                },
            ],
        },
        "baseline_name_length_distribution": {
            "summary": "Name-length histogram view.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Count of names with the corresponding character length.",
                },
                {"label": "X-axis", "description": "Normalized name length in characters."},
            ],
        },
        "bursts_hero_timeline": timebar(
            summary="Observed burst counts with burst intensity overlay.",
            primary_label="Rate ratio",
            primary_desc="Observed-to-expected count ratio per tested window.",
            include_wilson=False,
            include_low_power=False,
            volume_label="Observed count",
            volume_desc="Bars show observed submissions in each burst window.",
        ),
        "bursts_significance_by_window": {
            "summary": "Burst significance by window size.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Number of significant windows for each tested window size.",
                },
                {"label": "X-axis", "description": "Window size in minutes."},
            ],
        },
        "bursts_composition_shift": timebar(
            summary="Burst composition shift over time.",
            primary_label="Absolute pro-rate shift",
            primary_desc="Absolute deviation of burst-window pro rate from run baseline.",
            include_wilson=False,
            include_low_power=True,
            volume_label="Observed count",
            volume_desc="Observed submissions in each burst window.",
            extra=[
                {
                    "label": "Baseline pro rate",
                    "description": "Run-level baseline pro share for composition comparison.",
                },
                {
                    "label": "Delta pro rate",
                    "description": "Signed burst-window pro-rate shift from baseline.",
                },
            ],
        ),
        "bursts_null_distribution": {
            "summary": "Burst null simulation output.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Maximum simulated count observed in each null iteration.",
                },
                {"label": "X-axis", "description": "Simulation iteration index."},
            ],
        },
        "procon_swings_hero_bucket_trend": timebar(
            summary="Pro-rate trend against baseline stability bands.",
            primary_label="Pro rate",
            primary_desc="Observed pro share in each bucket.",
            include_wilson=True,
            flagged_label="Flagged",
            flagged_desc="Buckets flagged by swing detector for abnormal deviation.",
            extra=[
                {
                    "label": "Baseline pro rate",
                    "description": "Expected day/time pro share baseline.",
                },
                {
                    "label": "Stable lower / stable upper",
                    "description": "Expected range around baseline for normal fluctuation.",
                },
            ],
        ),
        "procon_swings_shift_heatmap": {
            "summary": "Day/slot deviation heatmap.",
            "items": [
                {
                    "label": "Cell color",
                    "description": (
                        "Red cells are more pro-heavy than expected for that slot; "
                        "blue cells are more con-heavy."
                    ),
                },
                {
                    "label": "Slot outlier dots",
                    "description": "Highlighted cells that exceed detector outlier thresholds.",
                },
                {
                    "label": "Axes",
                    "description": (
                        "X-axis is slot-of-day; Y-axis is calendar date in chronological "
                        "top-down order (earliest at top)."
                    ),
                },
            ],
        },
        "procon_swings_day_hour_heatmap": {
            "summary": "Average pro-rate by weekday/hour.",
            "items": [
                {"label": "Cell color", "description": "Darker cells indicate higher pro rate."},
                {
                    "label": "Axes",
                    "description": (
                        "X-axis is hour of day; Y-axis is weekday in chronological "
                        "top-down order (Monday to Sunday)."
                    ),
                },
            ],
        },
        "procon_swings_time_of_day_profile": {
            "summary": "Pro-rate profile by slot-of-day.",
            "items": [
                {"label": "Bar height", "description": "Pro share in that slot-of-day bucket."},
                {"label": "X-axis", "description": "Slot start minute from midnight."},
            ],
        },
        "procon_swings_direction_runs": {
            "summary": "Contiguous pro/con directional runs over time.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Number of contiguous buckets in each directional run.",
                },
                {
                    "label": "Line",
                    "description": "Mean absolute pro-rate shift magnitude across the run.",
                },
                {"label": "X-axis", "description": "Run start timestamp."},
            ],
        },
        "procon_swings_null_distribution": {
            "summary": "Null distribution for swing extremes.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Maximum absolute pro-rate delta per null iteration.",
                },
                {"label": "X-axis", "description": "Simulation iteration index."},
            ],
        },
        "changepoints_hero_timeline": timebar(
            summary="Volume/pro-rate timeline with structural break markers.",
            primary_label="Pro rate",
            primary_desc="Observed pro share over time.",
            include_wilson=True,
            flagged_label="Flagged",
            flagged_desc="Detected changepoint locations.",
        ),
        "changepoints_magnitude": {
            "summary": "Changepoint magnitude ranking.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Absolute change magnitude at each detected break.",
                },
                {"label": "X-axis", "description": "Changepoint index/order."},
            ],
        },
        "changepoints_hour_hist": {
            "summary": "Changepoint timing histogram.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Number of changepoints occurring in each hour-of-day bin.",
                },
                {"label": "X-axis", "description": "Hour of day (0-23)."},
            ],
        },
        "off_hours_control_timeline": timebar(
            summary=(
                "Off-hours control timeline with primary baseline overlays "
                "(model when available, day-adjusted fallback otherwise)."
            ),
            primary_label="Pro rate",
            primary_desc=(
                "Observed pro share by bucket, with Wilson uncertainty and primary "
                "expected/control-band overlays."
            ),
            include_wilson=True,
            include_low_power=True,
            volume_label="Submission count",
            volume_desc="Total records per bucket for support context.",
            flagged_label="Robust primary alert",
            flagged_desc=(
                "Alert-eligible windows beyond the primary 99.8% control limits "
                "with tail-consistent FDR support and material effect size. Shaded spans "
                "mark contiguous robust-alert runs (lower or upper tail)."
            ),
        ),
        "off_hours_funnel_plot": {
            "summary": (
                "Funnel plot of pro share versus support with primary and global "
                "control references."
            ),
            "items": [
                {
                    "label": "Point",
                    "description": (
                        "Each point is one time bucket (x = known pro/con count, y = pro share). "
                        "Inference is strongest when windows are alert-eligible and not low-power."
                    ),
                },
                {
                    "label": "Control curves",
                    "description": (
                        "Curves show global-baseline expected range (95% and 99.8%). "
                        "Primary baseline is row-specific (model/day-adjusted) and is used "
                        "for robust-alert scoring and tooltip diagnostics."
                    ),
                },
                {
                    "label": "Color",
                    "description": (
                        "Off-hours-dominant windows are highlighted; red points mark "
                        "robust lower-tail alerts and pink triangles mark robust upper-tail "
                        "alerts (99.8% control-limit breach + tail-consistent FDR + material "
                        "effect size)."
                    ),
                },
                {
                    "label": "Y-axis scaling",
                    "description": (
                        "The pro-rate axis uses a tail-stretch transform so dense extreme-tail "
                        "regions remain readable instead of collapsing at chart boundaries."
                    ),
                },
            ],
        },
        "off_hours_primary_residual_timeline": timebar(
            summary=(
                "Primary-baseline residual timeline for inferentially tested off-hours "
                "windows with SPC/FDR channel markers."
            ),
            primary_label="Primary z-score",
            primary_desc=(
                "Standardized residual of observed pro count versus the primary expected "
                "pro-rate baseline (model/day-adjusted)."
            ),
            include_wilson=False,
            include_low_power=True,
            volume_label="Known Pro+Con count",
            volume_desc="Known pro/con records supporting each bucket.",
            flagged_label="Robust primary alert",
            flagged_desc=(
                "Alert-eligible windows meeting robust-primary criteria "
                "(99.8% control-limit breach + tail-consistent FDR + material effect size)."
            ),
            extra=[
                {
                    "label": "Day z-score",
                    "description": (
                        "Day-adjusted standardized residual shown as comparator context."
                    ),
                },
                {
                    "label": "Z references",
                    "description": "Reference lines at 0 and +/-3 sigma for residual context.",
                },
            ],
        ),
        "off_hours_primary_flag_channels": {
            "summary": (
                "Channelized flag accounting for tested off-hours windows in the primary bucket."
            ),
            "items": [
                {
                    "label": "Column",
                    "description": (
                        "Each column is a count of tested windows for one channel label on the x-axis."
                    ),
                },
                {
                    "label": "Channel meaning",
                    "description": (
                        "Tested off-hours windows = denominator; Primary 99.8% breach = SPC extreme tail; "
                        "Primary two-sided FDR-significant = multiplicity-adjusted test hits; "
                        "Any primary flag channel = SPC OR FDR; Both primary flag channels = SPC AND FDR; "
                        "Robust primary alerts = AND criteria plus material effect-size gate."
                    ),
                },
                {
                    "label": "Reading order",
                    "description": (
                        "Columns are ordered from broad denominator context to stricter overlap criteria."
                    ),
                },
            ],
        },
        "overview_position_volume_by_bucket": {
            "summary": "Stacked pro/con/other volume by time bucket.",
            "items": [
                {
                    "label": "Stacked columns",
                    "description": (
                        "Each bucket stacks Pro, Con, and Other testimony counts so composition "
                        "and total support are visible at once."
                    ),
                },
                {
                    "label": "Axes",
                    "description": (
                        "X-axis is bucket timestamp in report timezone; Y-axis is submission count."
                    ),
                },
            ],
        },
        "off_hours_date_hour_pro_heatmap": {
            "summary": "Date x hour heatmap for testimony position composition.",
            "items": [
                {
                    "label": "Cell color",
                    "description": (
                        "Pro share for that date/hour cell; low-power cells are marked in tooltip."
                    ),
                },
                {
                    "label": "Axes",
                    "description": (
                        "X-axis is hour of day; Y-axis is calendar date in chronological "
                        "top-down order (earliest at top)."
                    ),
                },
            ],
        },
        "off_hours_date_hour_primary_residual_heatmap": {
            "summary": (
                "Date x hour heatmap for primary-baseline standardized residuals "
                "across the full 24-hour timeline."
            ),
            "items": [
                {
                    "label": "Cell color",
                    "description": (
                        "Average support-window primary z-score for that date/hour cell "
                        "(blue = below baseline, warm = above baseline)."
                    ),
                },
                {
                    "label": "Support",
                    "description": (
                        "Tooltips show inferential tested-window counts and robust-alert counts so "
                        "isolated low-support cells are not over-weighted."
                    ),
                },
                {
                    "label": "Off-hours emphasis",
                    "description": (
                        "Off-hours hours are highlighted on the X-axis label to preserve "
                        "off-hours focus while retaining all-hour context."
                    ),
                },
                {
                    "label": "Axes",
                    "description": (
                        "X-axis is hour of day; Y-axis is calendar date in chronological "
                        "top-down order (earliest at top)."
                    ),
                },
            ],
        },
        "off_hours_date_hour_volume_heatmap": {
            "summary": "Date x hour heatmap for submission volume.",
            "items": [
                {"label": "Cell color", "description": "Submission count for that date/hour cell."},
                {
                    "label": "Axes",
                    "description": (
                        "X-axis is hour of day; Y-axis is calendar date in chronological "
                        "top-down order (earliest at top)."
                    ),
                },
            ],
        },
        "duplicates_exact_bucket_concentration": timebar(
            summary="Exact-duplicate observed versus expected burden over time.",
            primary_label="Observed duplicate rows",
            primary_desc="Line shows observed duplicate-row burden in each bucket.",
            include_low_power=False,
            include_wilson=False,
            volume_label="Rows",
            volume_desc="Total rows in each bucket.",
            extra=[
                {
                    "label": "Expected duplicate rows",
                    "description": "Model baseline expectation from the configured name-frequency baseline.",
                },
                {
                    "label": "Excess duplicate rows",
                    "description": "Observed minus expected duplicate burden (floored at zero).",
                },
            ],
        ),
        "duplicates_exact_metric_diagnostics": {
            "summary": "Observed versus expected diagnostics across collision metrics.",
            "items": [
                {
                    "label": "Metric columns",
                    "description": (
                        "pairs = same-name unordered pairs; excess_rows = n - unique names; "
                        "repeated_group_rows = rows in names appearing >=2 times."
                    ),
                },
                {
                    "label": "Observed",
                    "description": (
                        "Bar height is the observed value for each metric in the selected duplicate scope."
                    ),
                },
                {
                    "label": "Expected and quantiles",
                    "description": (
                        "Use tooltip/table columns `expected`, `expected_p05`, `expected_p50`, and "
                        "`expected_p95` to compare where observed lands under the baseline."
                    ),
                },
                {
                    "label": "Significance columns",
                    "description": (
                        "`z_score` and `p_value` indicate standardized effect size and tail probability "
                        "for each metric; interpret with `n_used`/`N_used` support context."
                    ),
                },
            ],
        },
        "duplicates_exact_per_name_anomalies": {
            "summary": "Per-name anomaly ranking with p/q values.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Repeat count for each canonical/display name in this hearing.",
                },
                {
                    "label": "Significance",
                    "description": "Lower q-values indicate stronger excess-versus-baseline evidence.",
                },
                {"label": "X-axis", "description": "Canonical/display names sorted by q then count."},
            ],
        },
        "duplicates_exact_top_name_timing_exact": {
            "summary": (
                "Exact-match top duplicate names shown as time points sized by per-position duplicate rows."
            ),
            "items": [
                {
                    "label": "Point",
                    "description": (
                        "Each point is one active-bucket occurrence for a ranked top exact-match "
                        "name-position pair (x = bucket time, y = name). Colors encode Pro/Con/Other "
                        "and point size scales duplicate rows for that position in the bucket."
                    ),
                },
                {
                    "label": "Y-axis order",
                    "description": (
                        "Names are ranked by total repeated rows (rank 1 = most repeated) and "
                        "paginated 10 at a time up to the top 50 names."
                    ),
                },
                {
                    "label": "Tooltip context",
                    "description": (
                        "Tooltips include exact-tier definition, rank, bucket span, Pro/Con split, "
                        "bucket duplicate rows, and total repeated rows."
                    ),
                },
            ],
        },
        "duplicates_exact_position_concentration": {
            "summary": "Position concentration test (Pro vs Con duplicate burden).",
            "items": [
                {
                    "label": "Left/Right rate bars",
                    "description": (
                        "Two bars per pair show duplicate-row rate in each compared position "
                        "(left label vs right label)."
                    ),
                },
                {
                    "label": "Pair tooltip diagnostics",
                    "description": (
                        "Tooltips include `rate_difference`, confidence bounds, `rate_ratio`, and "
                        "`permutation_p_value_one_sided` for effect-size and uncertainty context."
                    ),
                },
                {"label": "X-axis", "description": "Position comparison pair."},
            ],
        },
        "duplicates_exact_null_distribution": {
            "summary": "Monte Carlo null distribution for duplicate burden metrics.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Simulated duplicate burden metric under the configured baseline.",
                },
                {"label": "X-axis", "description": "Simulation iteration."},
            ],
        },
        "duplicates_exact_swing_impact": {
            "summary": "Sensitivity scenarios for effective Pro/Con counts.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Effective pro share under each collision-adjustment scenario.",
                },
                {"label": "X-axis", "description": "Scenario (raw, strict dedupe, excess-adjusted)."},
            ],
        },
        "sortedness_bucket_ratio": timebar(
            summary="Ordering behavior across time buckets.",
            primary_label="Alphabetical indicator",
            primary_desc="Line values near 1 indicate alphabetical ordering for bucket windows.",
            include_low_power=False,
            include_wilson=False,
            volume_label="Records",
            volume_desc="Bar height is record count in each bucket.",
        ),
        "sortedness_bucket_summary": {
            "summary": "Sortedness summary by bucket size.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Alphabetical ordering ratio for each bucket size.",
                },
                {"label": "X-axis", "description": "Bucket size in minutes."},
            ],
        },
        "sortedness_kendall_tau_summary": {
            "summary": "Kendall tau ordering strength by bucket size.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Average absolute Kendall tau for each bucket size.",
                },
                {"label": "X-axis", "description": "Bucket size in minutes."},
            ],
        },
        "sortedness_minute_spikes": {
            "summary": "Minute-level ordering spikes.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Records seen in each minute-level ordering sample.",
                },
                {"label": "X-axis", "description": "Minute bucket timestamp."},
            ],
        },
        "rare_names_unique_ratio": timebar(
            summary="Name uniqueness over time.",
            primary_label="Unique ratio",
            primary_desc="Share of submissions with distinct canonical names per bucket.",
            include_low_power=True,
            include_wilson=False,
            extra=[
                {
                    "label": "Threshold unique ratio",
                    "description": "Reference threshold used for unique-ratio anomaly signaling.",
                }
            ],
        ),
        "rare_names_weird_scores": {
            "summary": "Highest weirdness-score names.",
            "items": [
                {
                    "label": "Bar height",
                    "description": (
                        "Weirdness score of sampled names; "
                        "higher indicates atypical string shape."
                    ),
                },
                {"label": "X-axis", "description": "Sample names sorted by weirdness."},
            ],
        },
        "rare_names_singletons": timebar(
            summary="Singleton name composition over time.",
            primary_label="Con count",
            primary_desc="Line shows con-side count among singleton records.",
            include_low_power=False,
            include_wilson=False,
            volume_label="Pro count",
            volume_desc="Bars show pro-side count among singleton records.",
        ),
        "rare_names_rarity_timeline": timebar(
            summary="Rarity-score timeline.",
            primary_label="Rarity median",
            primary_desc="Median rarity score in each bucket.",
            include_low_power=True,
            include_wilson=False,
            extra=[
                {
                    "label": "Rarity p95",
                    "description": "95th percentile rarity score to show tail behavior.",
                }
            ],
        ),
        "org_anomalies_blank_rate": timebar(
            summary="Blank organization-rate trend with position splits.",
            primary_label="Blank org rate",
            primary_desc="Overall blank/null organization share per bucket.",
            include_wilson=True,
            extra=[
                {
                    "label": "Pro blank org rate",
                    "description": "Blank-org share among pro records.",
                },
                {
                    "label": "Con blank org rate",
                    "description": "Blank-org share among con records.",
                },
            ],
        ),
        "org_anomalies_position_rates": timebar(
            summary="Per-position blank-org rates by time bucket.",
            primary_label="Blank org rate",
            primary_desc="Position-specific blank organization share.",
            include_wilson=True,
        ),
        "org_anomalies_bursts": {
            "summary": "Organization burst concentration.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Burst count for organization-related minute windows.",
                },
                {"label": "X-axis", "description": "Minute bucket of organization burst sample."},
            ],
        },
        "org_anomalies_top_orgs": {
            "summary": "Most common organization values.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Total records linked to each normalized organization value.",
                },
                {"label": "X-axis", "description": "Organization value labels."},
            ],
        },
        "voter_registry_match_rates": timebar(
            summary="Conservative voter-linkage trend with matched-rate focus.",
            primary_label="Matched rate",
            primary_desc=("Share of rows classified as matched under conservative primary linkage."),
            include_wilson=True,
            flagged_label="Extreme match-rate anomaly",
            flagged_desc=(
                "Buckets outside global 99.8% matched-rate control limits "
                "(after low-power filtering), directionally marked as lower or upper."
            ),
            extra=[
                {
                    "label": "Pro match rate",
                    "description": "Matched-rate trajectory for Pro rows in each bucket.",
                },
                {
                    "label": "Con match rate",
                    "description": "Matched-rate trajectory for Con rows in each bucket.",
                },
                {
                    "label": "Global control references",
                    "description": (
                        "Expected matched rate and 95% control bounds under the hearing-wide "
                        "global linkage baseline."
                    ),
                },
            ],
        ),
        "voter_registry_linkage_by_position_rows": {
            "summary": "Unmatched-rate profile by position (row-level unit).",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Unmatched rate for each position using row-level counts.",
                },
                {"label": "X-axis", "description": "Normalized position label."},
            ],
        },
        "voter_registry_linkage_by_position_unique": {
            "summary": "Unmatched-rate profile by position (unique-name unit).",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Unmatched rate for each dominant position among unique names.",
                },
                {"label": "X-axis", "description": "Dominant position label for unique names."},
            ],
        },
        "voter_registry_unmatched_names": {
            "summary": "Top unmatched names by row count (chart shows top 10; table retains full tail).",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Count of unmatched rows for each display name.",
                },
                {"label": "X-axis", "description": "Display names for unmatched rows."},
            ],
        },
        "voter_registry_pairwise_tests": {
            "summary": "Pairwise unmatched-rate tests across positions.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Unmatched-rate difference between compared position pairs.",
                },
                {
                    "label": "X-axis",
                    "description": "Pair label (left vs right) by inference unit.",
                },
            ],
        },
        "voter_registry_sensitivity_modes": {
            "summary": "Conservative, balanced, and broad linkage sensitivity panel.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Unmatched rate under each linkage mode.",
                },
                {"label": "X-axis", "description": "Linkage mode."},
            ],
        },
        "periodicity_clockface": {
            "summary": "Clock-face minute concentration.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Observed event count at each minute-of-hour bin.",
                },
                {"label": "X-axis", "description": "Minute of hour (0-59)."},
            ],
        },
        "periodicity_autocorr": {
            "summary": "Autocorrelation by lag.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Autocorrelation coefficient at each lag in minutes.",
                },
                {"label": "X-axis", "description": "Lag length in minutes."},
            ],
        },
        "periodicity_spectrum": {
            "summary": "Top spectral periods.",
            "items": [
                {"label": "Bar height", "description": "Spectral power for each candidate period."},
                {"label": "X-axis", "description": "Detected period in minutes."},
            ],
        },
        "periodicity_rolling_fano": {
            "summary": "Rolling Fano overdispersion by window size.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Median rolling Fano factor for each window size.",
                },
                {"label": "X-axis", "description": "Rolling window size in minutes."},
            ],
        },
        "multivariate_score_timeline": timebar(
            summary="Multivariate anomaly score and support over time.",
            primary_label="Anomaly score",
            primary_desc="Combined feature-space anomaly score for each bucket.",
            include_wilson=False,
            include_low_power=True,
            extra=[
                {
                    "label": "Anomaly score percentile",
                    "description": "Percentile rank of anomaly score within this run.",
                }
            ],
        ),
        "multivariate_top_buckets": {
            "summary": "Top anomaly buckets (scatter).",
            "items": [
                {
                    "label": "Point position",
                    "description": "X-axis is bucket volume; Y-axis is anomaly score.",
                },
                {
                    "label": "Point color",
                    "description": "Color reflects anomaly-score percentile rank.",
                },
                {"label": "Point size", "description": "Bubble size scales with bucket volume."},
            ],
        },
        "multivariate_feature_projection": {
            "summary": "Feature projection scatter.",
            "items": [
                {
                    "label": "Point position",
                    "description": "X-axis is log volume; Y-axis is pro rate.",
                },
                {"label": "Point color", "description": "Color shows anomaly score intensity."},
                {"label": "Point size", "description": "Bubble size scales with bucket volume."},
            ],
        },
        "composite_score_timeline": timebar(
            summary="Composite risk score over time.",
            primary_label="Composite score",
            primary_desc="Aggregate score from multi-detector evidence overlap.",
            include_wilson=False,
            include_low_power=True,
        ),
        "composite_evidence_flags": {
            "summary": "Evidence-flag composition.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Count of windows containing each detector flag.",
                },
                {"label": "X-axis", "description": "Detector evidence flag token."},
            ],
        },
        "composite_high_priority": {
            "summary": "Highest-priority composite windows.",
            "items": [
                {"label": "Bar height", "description": "Composite score for top-ranked windows."},
                {"label": "X-axis", "description": "Window timestamp bucket."},
            ],
        },
    }
    return docs


def _extract_bucket_options(*frames: pd.DataFrame) -> list[int]:
    options: set[int] = set()
    for frame in frames:
        if frame.empty or "bucket_minutes" not in frame.columns:
            continue
        numeric = pd.to_numeric(frame["bucket_minutes"], errors="coerce").dropna()
        for value in numeric.astype(int).tolist():
            if value > 0:
                options.add(int(value))
    return sorted(options)


def _with_expected_columns(frame: pd.DataFrame, expected: list[str]) -> pd.DataFrame:
    working = frame.copy()
    for column in expected:
        if column not in working.columns:
            working[column] = pd.NA
    return working


def _build_bucketed_baseline_profiles(
    counts_per_minute: pd.DataFrame,
    bucket_minutes: list[int] | None = None,
) -> pd.DataFrame:
    expected = [
        "minute_bucket",
        "bucket_minutes",
        "n_total",
        "n_pro",
        "n_con",
        "pro_rate",
        "pro_rate_wilson_low",
        "pro_rate_wilson_high",
        "is_low_power",
    ]
    if counts_per_minute.empty or "minute_bucket" not in counts_per_minute.columns:
        return _with_expected_columns(pd.DataFrame(), expected)

    windows = sorted(
        {
            int(value)
            for value in (bucket_minutes or BASELINE_PROFILE_BUCKET_MINUTES)
            if int(value) > 0
        }
    )
    if not windows:
        return _with_expected_columns(pd.DataFrame(), expected)

    working = counts_per_minute.copy()
    working["minute_bucket"] = pd.to_datetime(working["minute_bucket"], errors="coerce")
    working = working.dropna(subset=["minute_bucket"])
    if working.empty:
        return _with_expected_columns(pd.DataFrame(), expected)

    for column in ["n_total", "n_pro", "n_con"]:
        if column not in working.columns:
            working[column] = 0
        working[column] = pd.to_numeric(working[column], errors="coerce").fillna(0.0)

    bucketed: list[pd.DataFrame] = []
    for minutes in windows:
        grouped = (
            working.assign(bucket_start=working["minute_bucket"].dt.floor(f"{int(minutes)}min"))
            .groupby("bucket_start", dropna=True)
            .agg(
                n_total=("n_total", "sum"),
                n_pro=("n_pro", "sum"),
                n_con=("n_con", "sum"),
            )
            .reset_index()
            .rename(columns={"bucket_start": "minute_bucket"})
            .sort_values("minute_bucket")
        )
        if grouped.empty:
            continue

        grouped["bucket_minutes"] = int(minutes)
        grouped["pro_rate"] = (grouped["n_pro"] / grouped["n_total"]).where(grouped["n_total"] > 0)
        grouped["pro_rate_wilson_low"], grouped["pro_rate_wilson_high"] = wilson_interval(
            successes=grouped["n_pro"],
            totals=grouped["n_total"],
        )
        grouped["is_low_power"] = low_power_mask(
            totals=grouped["n_total"],
            min_total=DEFAULT_LOW_POWER_MIN_TOTAL,
        )
        bucketed.append(grouped)

    if not bucketed:
        return _with_expected_columns(pd.DataFrame(), expected)

    combined = pd.concat(bucketed, ignore_index=True).sort_values(
        ["bucket_minutes", "minute_bucket"]
    )
    return _with_expected_columns(combined, expected)


def _build_bucketed_day_hour_profiles(
    baseline_bucket_profiles: pd.DataFrame,
    counts_per_hour: pd.DataFrame,
) -> pd.DataFrame:
    expected = [
        "bucket_minutes",
        "day_of_week",
        "hour",
        "n_total",
        "pro_rate",
        "pro_rate_wilson_low",
        "pro_rate_wilson_high",
        "is_low_power",
    ]

    if baseline_bucket_profiles.empty:
        if counts_per_hour.empty:
            return _with_expected_columns(pd.DataFrame(), expected)
        fallback = counts_per_hour.copy()
        fallback["bucket_minutes"] = 1
        return _with_expected_columns(fallback, expected)

    working = baseline_bucket_profiles.copy()
    if "minute_bucket" not in working.columns:
        return _with_expected_columns(pd.DataFrame(), expected)
    working["minute_bucket"] = pd.to_datetime(working["minute_bucket"], errors="coerce")
    working = working.dropna(subset=["minute_bucket"])
    if working.empty:
        return _with_expected_columns(pd.DataFrame(), expected)

    for column in ["n_total", "n_pro"]:
        if column not in working.columns:
            working[column] = 0
        working[column] = pd.to_numeric(working[column], errors="coerce").fillna(0.0)

    working["day_of_week"] = working["minute_bucket"].dt.day_name()
    working["hour"] = working["minute_bucket"].dt.hour

    grouped = (
        working.groupby(["bucket_minutes", "day_of_week", "hour"], dropna=True)
        .agg(
            n_total=("n_total", "sum"),
            n_pro=("n_pro", "sum"),
        )
        .reset_index()
        .sort_values(["bucket_minutes", "day_of_week", "hour"])
    )
    grouped["pro_rate"] = (grouped["n_pro"] / grouped["n_total"]).where(grouped["n_total"] > 0)
    grouped["pro_rate_wilson_low"], grouped["pro_rate_wilson_high"] = wilson_interval(
        successes=grouped["n_pro"],
        totals=grouped["n_total"],
    )
    grouped["is_low_power"] = low_power_mask(
        totals=grouped["n_total"],
        min_total=DEFAULT_LOW_POWER_MIN_TOTAL,
    )
    return _with_expected_columns(grouped, expected)


def _build_deadline_ramp_metrics(
    counts_per_minute: pd.DataFrame,
    *,
    cutoff_time: datetime,
    min_cell_n_for_rates: int,
) -> dict[str, Any]:
    if counts_per_minute.empty:
        return {
            "status": "unavailable",
            "reason": "Counts-per-minute artifact is empty.",
        }

    working = _with_expected_columns(
        counts_per_minute,
        ["minute_bucket", "n_total", "n_pro", "n_con"],
    ).copy()
    working["minute_bucket"] = pd.to_datetime(working["minute_bucket"], errors="coerce")
    working = working.dropna(subset=["minute_bucket"])
    if working.empty:
        return {
            "status": "unavailable",
            "reason": "No valid minute-bucket timestamps available for deadline ramp metrics.",
        }
    for column in ["n_total", "n_pro", "n_con"]:
        working[column] = pd.to_numeric(working[column], errors="coerce").fillna(0.0)

    cutoff = pd.Timestamp(cutoff_time)
    recent_mask = (working["minute_bucket"] > (cutoff - pd.Timedelta(minutes=60))) & (
        working["minute_bucket"] <= cutoff
    )
    prior_mask = (
        working["minute_bucket"] > (cutoff - pd.Timedelta(minutes=120))
    ) & (working["minute_bucket"] <= (cutoff - pd.Timedelta(minutes=60)))

    recent = working.loc[recent_mask]
    prior = working.loc[prior_mask]
    recent_total = float(recent["n_total"].sum())
    prior_total = float(prior["n_total"].sum())
    recent_pro = float(recent["n_pro"].sum())
    prior_pro = float(prior["n_pro"].sum())
    recent_con = float(recent["n_con"].sum())
    prior_con = float(prior["n_con"].sum())

    recent_pro_rate = recent_pro / recent_total if recent_total > 0 else None
    prior_pro_rate = prior_pro / prior_total if prior_total > 0 else None
    recent_con_rate = recent_con / recent_total if recent_total > 0 else None
    prior_con_rate = prior_con / prior_total if prior_total > 0 else None

    return {
        "status": "ok",
        "window_minutes": 60,
        "recent_window_start": _to_pacific_timestamp(
            cutoff - pd.Timedelta(minutes=60)
        ).isoformat(),
        "recent_window_end": _to_pacific_timestamp(cutoff).isoformat(),
        "prior_window_start": _to_pacific_timestamp(
            cutoff - pd.Timedelta(minutes=120)
        ).isoformat(),
        "prior_window_end": _to_pacific_timestamp(
            cutoff - pd.Timedelta(minutes=60)
        ).isoformat(),
        "recent_n_total": int(round(recent_total)),
        "prior_n_total": int(round(prior_total)),
        "recent_pro_rate": float(recent_pro_rate) if recent_pro_rate is not None else None,
        "prior_pro_rate": float(prior_pro_rate) if prior_pro_rate is not None else None,
        "recent_con_rate": float(recent_con_rate) if recent_con_rate is not None else None,
        "prior_con_rate": float(prior_con_rate) if prior_con_rate is not None else None,
        "recent_is_low_power": bool(recent_total < float(max(1, min_cell_n_for_rates))),
        "prior_is_low_power": bool(prior_total < float(max(1, min_cell_n_for_rates))),
        "ramp_ratio_recent_vs_prior": (
            float(recent_total / prior_total) if prior_total > 0 else None
        ),
        "pro_rate_delta_recent_minus_prior": (
            float(recent_pro_rate - prior_pro_rate)
            if recent_pro_rate is not None and prior_pro_rate is not None
            else None
        ),
    }


def _build_hearing_context_panel(
    counts_per_minute: pd.DataFrame,
    *,
    hearing_metadata: HearingMetadata | None,
    min_cell_n_for_rates: int,
) -> dict[str, Any]:
    if hearing_metadata is None:
        return {
            "status": "unavailable",
            "available": False,
            "reason": "No hearing metadata sidecar provided.",
            "process_markers": [],
            "deadline_ramp_metrics": {
                "status": "unavailable",
                "reason": "No sign_in_cutoff provided in hearing metadata.",
            },
        }

    process_markers = []
    for key, value in hearing_metadata.marker_times().items():
        marker_time = _to_pacific_timestamp(pd.Timestamp(value))
        if pd.isna(marker_time):
            continue
        process_markers.append(
            {
                "key": key,
                "label": key.replace("_", " "),
                "time_iso": marker_time.isoformat(),
            }
        )
    process_markers = sorted(process_markers, key=lambda item: item["time_iso"])
    sidecar_source = dict(hearing_metadata.source or {})
    sidecar_stats = dict(hearing_metadata.stats or {})
    meeting_start_iso = (
        _to_pacific_timestamp(pd.Timestamp(hearing_metadata.meeting_start)).isoformat()
        if hearing_metadata.meeting_start is not None
        else None
    )

    metadata_rows = [
        {
            "field": "hearing_id",
            "value": hearing_metadata.hearing_id,
        },
        {
            "field": "timezone",
            "value": PACIFIC_TIMEZONE_NAME,
        },
        {
            "field": "meeting_start",
            "value": meeting_start_iso,
        },
        {
            "field": "sign_in_open",
            "value": (
                _to_pacific_timestamp(pd.Timestamp(hearing_metadata.sign_in_open)).isoformat()
                if hearing_metadata.sign_in_open is not None
                else None
            ),
        },
        {
            "field": "sign_in_cutoff",
            "value": (
                _to_pacific_timestamp(pd.Timestamp(hearing_metadata.sign_in_cutoff)).isoformat()
                if hearing_metadata.sign_in_cutoff is not None
                else None
            ),
        },
        {
            "field": "written_testimony_deadline",
            "value": (
                _to_pacific_timestamp(
                    pd.Timestamp(hearing_metadata.written_testimony_deadline)
                ).isoformat()
                if hearing_metadata.written_testimony_deadline is not None
                else None
            ),
        },
    ]
    if sidecar_source:
        metadata_rows.extend(
            [
                {
                    "field": "short_bill_id",
                    "value": sidecar_source.get("short_bill_id"),
                },
                {
                    "field": "agenda_item_description",
                    "value": sidecar_source.get("agenda_item_description"),
                },
            ]
        )
    if sidecar_stats:
        metadata_rows.extend(
            [
                {
                    "field": "total_rows",
                    "value": sidecar_stats.get("total_rows"),
                },
                {
                    "field": "total_pro_pct",
                    "value": sidecar_stats.get("total_pro_pct"),
                },
                {
                    "field": "total_con_pct",
                    "value": sidecar_stats.get("total_con_pct"),
                },
            ]
        )

    if hearing_metadata.sign_in_cutoff is None:
        deadline_ramp_metrics = {
            "status": "unavailable",
            "reason": "No sign_in_cutoff provided in hearing metadata.",
        }
    else:
        deadline_ramp_metrics = _build_deadline_ramp_metrics(
            counts_per_minute,
            cutoff_time=hearing_metadata.sign_in_cutoff,
            min_cell_n_for_rates=min_cell_n_for_rates,
        )

    return {
        "status": "ok",
        "available": True,
        "hearing_id": hearing_metadata.hearing_id,
        "timezone": PACIFIC_TIMEZONE_NAME,
        "meeting_start": meeting_start_iso,
        "source_path": hearing_metadata.source_path,
        "source": sidecar_source,
        "stats": sidecar_stats,
        "process_markers": process_markers,
        "metadata_rows": metadata_rows,
        "deadline_ramp_metrics": deadline_ramp_metrics,
    }


def _load_table_map_from_results(
    results: dict[str, DetectorResult],
    artifacts: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    table_map: dict[str, pd.DataFrame] = {}
    for name, frame in artifacts.items():
        table_map[f"artifacts.{name}"] = frame.copy()
    for detector_name, result in results.items():
        for table_name, frame in result.tables.items():
            table_map[_table_key(detector_name, table_name)] = frame.copy()
    return table_map


def _load_table_map_from_disk(out_dir: Path) -> dict[str, pd.DataFrame]:
    table_map: dict[str, pd.DataFrame] = {}

    artifacts_dir = out_dir / "artifacts"
    if artifacts_dir.exists():
        artifact_candidates: dict[str, Path] = {}
        for path in artifacts_dir.iterdir():
            if path.suffix not in {".parquet", ".csv"}:
                continue
            existing = artifact_candidates.get(path.stem)
            if existing is None or (existing.suffix == ".csv" and path.suffix == ".parquet"):
                artifact_candidates[path.stem] = path
        for stem, path in sorted(artifact_candidates.items()):
            frame = _load_frame_from_candidates([path])
            table_map[f"artifacts.{stem}"] = frame

    tables_dir = out_dir / "tables"
    if tables_dir.exists():
        table_candidates: dict[str, Path] = {}
        for path in tables_dir.iterdir():
            if path.suffix not in {".parquet", ".csv"}:
                continue
            if "__" not in path.stem:
                continue
            detector_name, table_name = path.stem.split("__", 1)
            key = _table_key(detector_name, table_name)
            existing = table_candidates.get(key)
            if existing is None or (existing.suffix == ".csv" and path.suffix == ".parquet"):
                table_candidates[key] = path
        for key, path in sorted(table_candidates.items()):
            frame = _load_frame_from_candidates([path])
            table_map[key] = frame

    return table_map


def _build_interactive_chart_payload_v2(
    table_map: dict[str, pd.DataFrame],
    detector_summaries: dict[str, dict[str, Any]],
    *,
    default_dedup_mode: str | None = None,
    min_cell_n_for_rates: int = 25,
    hearing_metadata: HearingMetadata | None = None,
) -> dict[str, Any]:
    payload_started = perf_counter()
    counts_per_minute = _with_expected_columns(
        table_map.get("artifacts.counts_per_minute", pd.DataFrame()),
        [
            "minute_bucket",
            "n_total",
            "n_pro",
            "n_con",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "n_unique_names",
            "unique_ratio",
            "threshold_unique_ratio",
        ],
    )
    counts_per_hour = _with_expected_columns(
        table_map.get("artifacts.counts_per_hour", pd.DataFrame()),
        [
            "day_of_week",
            "hour",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
    )
    name_frequency = _with_expected_columns(
        table_map.get("artifacts.name_frequency", pd.DataFrame()),
        ["display_name", "canonical_name", "n", "n_pro", "n_con", "time_span_minutes"],
    )
    name_text_features = _with_expected_columns(
        table_map.get("artifacts.name_text_features", pd.DataFrame()),
        ["name_length"],
    )

    bursts_significant = _with_expected_columns(
        table_map.get(_table_key("bursts", "burst_significant_windows"), pd.DataFrame()),
        [
            "start_minute",
            "end_minute",
            "window_minutes",
            "bucket_minutes",
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
            "q_value",
            "is_significant",
        ],
    )
    bursts_tests = _with_expected_columns(
        table_map.get(_table_key("bursts", "burst_window_tests"), pd.DataFrame()),
        [
            "window_minutes",
            "bucket_minutes",
            "rate_ratio",
            "pro_rate",
            "baseline_pro_rate",
            "delta_pro_rate",
            "abs_delta_pro_rate",
            "is_low_power",
            "is_significant",
        ],
    )
    bursts_null = _with_expected_columns(
        table_map.get(_table_key("bursts", "burst_null_distribution"), pd.DataFrame()),
        ["window_minutes", "bucket_minutes", "iteration", "max_window_count"],
    )

    time_bucket_profiles = _with_expected_columns(
        table_map.get(_table_key("procon_swings", "time_bucket_profiles"), pd.DataFrame()),
        [
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "pro_rate",
            "baseline_pro_rate",
            "stable_lower",
            "stable_upper",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_flagged",
            "is_low_power",
        ],
    )
    day_bucket_profiles = _with_expected_columns(
        table_map.get(_table_key("procon_swings", "day_bucket_profiles"), pd.DataFrame()),
        [
            "date",
            "bucket_minutes",
            "slot_start_minute",
            "delta_from_slot_pro_rate",
            "n_total",
            "is_slot_outlier",
            "is_low_power",
        ],
    )
    pro_rate_by_hour = _with_expected_columns(
        table_map.get(_table_key("procon_swings", "pro_rate_by_hour"), pd.DataFrame()),
        [
            "day_of_week",
            "hour",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
    )
    time_of_day_profiles = _with_expected_columns(
        table_map.get(
            _table_key("procon_swings", "time_of_day_bucket_profiles"),
            pd.DataFrame(),
        ),
        [
            "bucket_minutes",
            "slot_start_minute",
            "n_total",
            "pro_rate",
            "baseline_pro_rate",
            "stable_lower",
            "stable_upper",
            "is_flagged",
            "is_low_power",
        ],
    )
    procon_direction_runs = _with_expected_columns(
        table_map.get(_table_key("procon_swings", "direction_runs"), pd.DataFrame()),
        [
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
        ],
    )
    swing_null = _with_expected_columns(
        table_map.get(_table_key("procon_swings", "swing_null_distribution"), pd.DataFrame()),
        ["window_minutes", "iteration", "max_abs_delta_pro_rate"],
    )

    all_changepoints = _with_expected_columns(
        table_map.get(_table_key("changepoints", "all_changepoints"), pd.DataFrame()),
        [
            "metric",
            "change_index",
            "change_minute",
            "mean_before",
            "mean_after",
            "delta",
            "abs_delta",
        ],
    )
    volume_changepoints = _with_expected_columns(
        table_map.get(_table_key("changepoints", "volume_changepoints"), pd.DataFrame()),
        ["change_minute"],
    )
    pro_rate_changepoints = _with_expected_columns(
        table_map.get(_table_key("changepoints", "pro_rate_changepoints"), pd.DataFrame()),
        ["change_minute"],
    )

    off_hours_hourly = _with_expected_columns(
        table_map.get(_table_key("off_hours", "hourly_distribution"), pd.DataFrame()),
        [
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
    )
    off_hours_date_hour = _with_expected_columns(
        table_map.get(_table_key("off_hours", "date_hour_distribution"), pd.DataFrame()),
        [
            "date",
            "day_of_week",
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "n_known",
            "n_unknown",
            "n_off_hours",
            "off_hours_fraction",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
    )
    off_hours_date_hour_primary_residual = _with_expected_columns(
        table_map.get(
            _table_key("off_hours", "date_hour_primary_residual_distribution"),
            pd.DataFrame(),
        ),
        [
            "bucket_minutes",
            "date",
            "day_of_week",
            "hour",
            "n_windows",
            "n_windows_alert_eligible",
            "n_windows_tested",
            "n_windows_low_power",
            "n_windows_primary_alert",
            "primary_alert_fraction_tested",
            "n_total",
            "n_known",
            "n_known_tested",
            "n_pro",
            "n_con",
            "off_hours_fraction",
            "pro_rate",
            "expected_pro_rate_primary",
            "delta_pro_rate_primary",
            "z_score_primary",
            "z_score_primary_median",
            "z_score_primary_abs_max",
            "is_low_power",
        ],
    )
    off_hours_window_control = _with_expected_columns(
        table_map.get(_table_key("off_hours", "window_control_profile"), pd.DataFrame()),
        [
            "bucket_start",
            "bucket_minutes",
            "event_date_key",
            "day_of_week",
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "n_known",
            "n_unknown",
            "n_off_hours",
            "off_hours_fraction",
            "is_off_hours_window",
            "is_pure_off_hours_window",
            "is_alert_off_hours_window",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "expected_pro_rate_day",
            "expected_pro_rate_model",
            "expected_pro_rate_primary",
            "expected_pro_rate_global",
            "baseline_source",
            "model_baseline_source",
            "primary_baseline_source",
            "is_model_baseline_available",
            "model_fit_method",
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_converged",
            "model_fit_aic",
            "model_fit_used_harmonics",
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
            "control_low_95_global",
            "control_high_95_global",
            "control_low_998_global",
            "control_high_998_global",
            "z_score_day",
            "z_score_model",
            "z_score_primary",
            "delta_pro_rate_day",
            "delta_pro_rate_model",
            "delta_pro_rate_primary",
            "p_value_day",
            "p_value_day_two_sided",
            "p_value_day_lower",
            "p_value_day_upper",
            "p_value_model",
            "p_value_model_two_sided",
            "p_value_model_lower",
            "p_value_model_upper",
            "p_value_primary",
            "p_value_primary_two_sided",
            "p_value_primary_lower",
            "p_value_primary_upper",
            "q_value_day",
            "q_value_day_lower",
            "q_value_day_upper",
            "q_value_day_two_sided",
            "q_value_model",
            "q_value_model_lower",
            "q_value_model_upper",
            "q_value_model_two_sided",
            "q_value_primary",
            "q_value_primary_lower",
            "q_value_primary_upper",
            "q_value_primary_two_sided",
            "is_significant_day",
            "is_significant_day_lower",
            "is_significant_day_upper",
            "is_significant_day_two_sided",
            "is_significant_model",
            "is_significant_model_lower",
            "is_significant_model_upper",
            "is_significant_model_two_sided",
            "is_significant_primary",
            "is_significant_primary_lower",
            "is_significant_primary_upper",
            "is_significant_primary_two_sided",
            "is_material_primary_shift",
            "is_material_primary_lower_shift",
            "is_material_primary_upper_shift",
            "is_primary_alert_window",
            "is_primary_lower_alert_window",
            "is_primary_upper_alert_window",
            "is_primary_two_sided_alert_window",
            "is_primary_spc_998_two_sided",
            "is_primary_fdr_two_sided",
            "is_primary_any_flag_channel",
            "is_primary_both_flag_channels",
            "is_below_day_control_95",
            "is_below_day_control_998",
            "is_above_day_control_95",
            "is_above_day_control_998",
            "is_below_model_control_95",
            "is_below_model_control_998",
            "is_above_model_control_95",
            "is_above_model_control_998",
            "is_below_primary_control_95",
            "is_below_primary_control_998",
            "is_above_primary_control_95",
            "is_above_primary_control_998",
            "is_outside_day_control_95",
            "is_outside_day_control_998",
            "is_outside_model_control_95",
            "is_outside_model_control_998",
            "is_outside_primary_control_95",
            "is_outside_primary_control_998",
            "is_below_global_control_95",
            "is_below_global_control_998",
        ],
    )
    off_hours_summary = _with_expected_columns(
        table_map.get(_table_key("off_hours", "off_hours_summary"), pd.DataFrame()),
        [
            "off_hours",
            "on_hours",
            "off_hours_ratio",
            "off_hours_pro_rate",
            "on_hours_pro_rate",
            "off_hours_pro_rate_wilson_low",
            "off_hours_pro_rate_wilson_high",
            "on_hours_pro_rate_wilson_low",
            "on_hours_pro_rate_wilson_high",
            "chi_square_p_value",
            "off_hours_is_low_power",
            "on_hours_is_low_power",
            "primary_bucket_minutes",
            "primary_baseline_method",
            "alert_off_hours_min_fraction",
            "primary_alert_min_abs_delta",
            "off_hours_windows_alert_eligible",
            "off_hours_windows_alert_eligible_low_power",
            "off_hours_windows_alert_eligible_tested_fraction",
            "off_hours_windows_alert_eligible_low_power_fraction",
            "off_hours_windows_tested",
            "off_hours_windows_below_day_control_95",
            "off_hours_windows_below_day_control_998",
            "off_hours_windows_below_model_control_95",
            "off_hours_windows_below_model_control_998",
            "off_hours_windows_below_primary_control_95",
            "off_hours_windows_below_primary_control_998",
            "off_hours_windows_above_primary_control_95",
            "off_hours_windows_above_primary_control_998",
            "off_hours_windows_significant_day",
            "off_hours_windows_significant_model",
            "off_hours_windows_significant_primary",
            "off_hours_windows_significant_primary_upper",
            "off_hours_windows_significant_primary_two_sided",
            "off_hours_windows_primary_spc_998_any",
            "off_hours_windows_primary_fdr_two_sided",
            "off_hours_windows_primary_flag_any",
            "off_hours_windows_primary_flag_both",
            "off_hours_windows_primary_spc_998_any_fraction",
            "off_hours_windows_primary_fdr_two_sided_fraction",
            "off_hours_windows_primary_flag_any_fraction",
            "off_hours_windows_primary_flag_both_fraction",
            "off_hours_windows_primary_alert",
            "off_hours_windows_primary_alert_fraction",
            "off_hours_primary_alert_run_count",
            "off_hours_primary_alert_max_run_windows",
            "off_hours_primary_alert_max_run_minutes",
            "off_hours_min_day_z",
            "off_hours_max_abs_day_z",
            "off_hours_min_model_z",
            "off_hours_max_abs_model_z",
            "off_hours_min_primary_z",
            "off_hours_max_abs_primary_z",
            "off_hours_min_primary_delta",
            "off_hours_max_abs_primary_delta",
            "off_hours_windows_model_available",
            "global_daytime_pro_rate",
            "day_adjusted_fdr_alpha",
            "model_fit_min_rows",
            "model_hour_harmonics",
            "primary_model_fit_method",
            "primary_model_fit_rows",
            "primary_model_fit_unique_days",
            "primary_model_fit_unique_hours",
            "primary_model_fit_converged",
            "primary_model_fit_aic",
        ],
    )
    off_hours_flag_channels = _with_expected_columns(
        table_map.get(_table_key("off_hours", "flag_channel_summary"), pd.DataFrame()),
        [
            "rank",
            "channel",
            "channel_label",
            "count",
            "share_of_tested",
        ],
    )

    dup_exact_methods = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "collision_methods"), pd.DataFrame()),
        [
            "scope",
            "baseline_source",
            "baseline_model",
            "uncertainty_model",
            "n_used",
            "N_used",
            "metric_primary",
            "metrics_reported",
            "baseline_degraded",
            "fallback_policy",
            "collision_key_mode",
            "normalization_version_hash",
            "stratification",
            "censored",
        ],
    )
    primary_dup_scope = (
        str(dup_exact_methods["scope"].iloc[0]).strip() if not dup_exact_methods.empty else "full_hearing"
    )
    primary_dup_metric = (
        str(dup_exact_methods["metric_primary"].iloc[0]).strip()
        if not dup_exact_methods.empty
        else "repeated_group_rows"
    )
    duplicate_scope_options = sorted(
        {
            str(value).strip()
            for value in dup_exact_methods.get("scope", pd.Series(dtype=str)).tolist()
            if str(value).strip()
        }
    )
    if not duplicate_scope_options:
        duplicate_scope_options = [primary_dup_scope]

    dup_exact_collision_overview = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "collision_overview"), pd.DataFrame()),
        [
            "scope",
            "metric",
            "observed",
            "expected",
            "expected_p05",
            "expected_p50",
            "expected_p95",
            "z_score",
            "p_value",
            "n_used",
            "N_used",
        ],
    )
    duplicate_metric_options = sorted(
        {
            str(value).strip()
            for value in dup_exact_collision_overview.get("metric", pd.Series(dtype=str)).tolist()
            if str(value).strip()
        }
    )
    if not duplicate_metric_options:
        duplicate_metric_options = [primary_dup_metric]
    dup_exact_metric_diagnostics = dup_exact_collision_overview[
        dup_exact_collision_overview["scope"].astype(str).str.len() > 0
    ].copy()

    dup_exact_collision_bucket = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "collision_by_bucket"), pd.DataFrame()),
        [
            "scope",
            "metric",
            "bucket_start",
            "bucket_minutes",
            "n_bucket",
            "n_used",
            "N_used",
            "n_unique_names",
            "n_pro",
            "n_con",
            "observed",
            "expected",
            "expected_p05",
            "expected_p95",
            "z_score",
            "p_value",
            "excess",
            "baseline_model",
            "baseline_source",
            "baseline_degraded",
            "is_low_power",
            "inference_status",
        ],
    )
    dup_exact_bucket = pd.DataFrame()
    if not dup_exact_collision_bucket.empty:
        dup_exact_bucket = dup_exact_collision_bucket.rename(
            columns={
                "n_bucket": "n_rows",
                "observed": "duplicate_rows",
                "expected": "expected_duplicate_rows",
                "excess": "excess_duplicate_rows",
            }
        ).copy()
        dup_exact_bucket["duplicate_row_rate"] = (
            dup_exact_bucket["duplicate_rows"] / dup_exact_bucket["n_rows"]
        ).where(dup_exact_bucket["n_rows"] > 0, 0.0)
    if dup_exact_bucket.empty:
        dup_exact_bucket = _with_expected_columns(
            table_map.get(_table_key("duplicates_exact", "duplicate_by_bucket"), pd.DataFrame()),
            [
                "bucket_start",
                "bucket_minutes",
                "n_rows",
                "n_unique_names",
                "n_pro",
                "n_con",
                "duplicate_rows",
                "duplicate_row_rate",
                "expected_duplicate_rows",
                "excess_duplicate_rows",
            ],
        )
        if dup_exact_bucket.empty:
            legacy_dup_exact_bucket = _with_expected_columns(
                table_map.get(_table_key("duplicates_exact", "repeated_same_bucket"), pd.DataFrame()),
                ["bucket_start", "bucket_minutes", "n", "n_pro", "n_con"],
            )
            if not legacy_dup_exact_bucket.empty:
                dup_exact_bucket = (
                    legacy_dup_exact_bucket.groupby(["bucket_start", "bucket_minutes"], dropna=False)
                    .agg(
                        n_rows=("n", "sum"),
                        n_pro=("n_pro", "sum"),
                        n_con=("n_con", "sum"),
                        duplicate_rows=("n", "sum"),
                    )
                    .reset_index()
                )
                dup_exact_bucket["n_unique_names"] = pd.NA
                dup_exact_bucket["duplicate_row_rate"] = (
                    dup_exact_bucket["duplicate_rows"] / dup_exact_bucket["n_rows"]
                ).where(dup_exact_bucket["n_rows"] > 0, 0.0)
                dup_exact_bucket["expected_duplicate_rows"] = pd.NA
                dup_exact_bucket["excess_duplicate_rows"] = dup_exact_bucket["duplicate_rows"]

    dup_exact_per_name = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "per_name_anomalies"), pd.DataFrame()),
        [
            "display_name",
            "canonical_name",
            "n",
            "n_pro",
            "n_con",
            "time_span_minutes",
            "expected_count",
            "p_value",
            "q_value",
            "is_significant",
            "within_5m_pairs",
            "within_15m_pairs",
            "temporal_p_value_within_5m",
            "temporal_p_value_min_gap",
        ],
    )
    if dup_exact_per_name.empty:
        per_name_display = _with_expected_columns(
            table_map.get(_table_key("duplicates_exact", "per_name_display"), pd.DataFrame()),
            [
                "scope",
                "display_name",
                "canonical_name",
                "observed_count",
                "n_pro",
                "n_con",
                "time_span_minutes",
                "expected_count",
                "p_value",
                "q_value",
                "is_significant",
            ],
        )
        if not per_name_display.empty:
            per_name_display = per_name_display[
                per_name_display["scope"].astype(str) == primary_dup_scope
            ].copy()
            dup_exact_per_name = per_name_display.rename(columns={"observed_count": "n"})
        else:
            dup_exact_per_name = _with_expected_columns(
                table_map.get(_table_key("duplicates_exact", "top_repeated_names"), pd.DataFrame()),
                ["display_name", "canonical_name", "n", "n_pro", "n_con", "time_span_minutes"],
            )
        dup_exact_per_name["expected_count"] = dup_exact_per_name.get("expected_count", pd.Series(dtype=float))
        dup_exact_per_name["p_value"] = dup_exact_per_name.get("p_value", pd.Series(dtype=float)).fillna(pd.NA)
        dup_exact_per_name["q_value"] = dup_exact_per_name.get("q_value", pd.Series(dtype=float)).fillna(pd.NA)
        is_significant_series = (
            dup_exact_per_name["is_significant"]
            if "is_significant" in dup_exact_per_name.columns
            else pd.Series(pd.NA, index=dup_exact_per_name.index, dtype="object")
        )
        dup_exact_per_name["is_significant"] = (
            pd.to_numeric(is_significant_series, errors="coerce").fillna(0).astype(bool)
        )
        dup_exact_per_name["within_5m_pairs"] = 0
        dup_exact_per_name["within_15m_pairs"] = 0
        dup_exact_per_name["temporal_p_value_within_5m"] = pd.NA
        dup_exact_per_name["temporal_p_value_min_gap"] = pd.NA
    dup_exact_top_name_timing = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "top_name_timing_by_mode"), pd.DataFrame()),
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
            "n_other",
            "first_seen",
            "last_seen",
        ],
    )

    dup_exact_position_tests = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "position_concentration_tests"), pd.DataFrame()),
        [
            "position_left",
            "position_right",
            "left_duplicate_row_rate",
            "right_duplicate_row_rate",
            "rate_difference",
            "rate_difference_ci_low",
            "rate_difference_ci_high",
            "rate_ratio",
            "permutation_p_value_one_sided",
            "left_is_low_power",
            "right_is_low_power",
        ],
    )
    dup_exact_null_distribution = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "null_distribution"), pd.DataFrame()),
        [
            "iteration",
            "duplicate_rows",
            "duplicate_row_rate",
            "duplicate_pairs",
            "n_names_ge2",
            "n_names_ge3",
            "n_names_ge5",
            "n_names_ge10",
            "max_count",
        ],
    )
    dup_exact_swing_impact = _with_expected_columns(
        table_map.get(_table_key("duplicates_exact", "swing_impact_scenarios"), pd.DataFrame()),
        [
            "scenario",
            "n_pro_effective",
            "n_con_effective",
            "pro_share",
        ],
    )

    sorted_bucket = _with_expected_columns(
        table_map.get(_table_key("sortedness", "bucket_ordering"), pd.DataFrame()),
        [
            "bucket_start",
            "bucket_minutes",
            "n_records",
            "is_alphabetical",
            "kendall_tau",
            "kendall_p_value",
            "abs_kendall_tau",
        ],
    )
    sorted_summary = _with_expected_columns(
        table_map.get(_table_key("sortedness", "bucket_ordering_summary"), pd.DataFrame()),
        [
            "bucket_minutes",
            "n_buckets",
            "avg_records_per_bucket",
            "alphabetical_ratio",
            "mean_kendall_tau",
            "mean_abs_kendall_tau",
            "max_abs_kendall_tau",
            "strong_ordering_ratio",
        ],
    )
    sorted_minute = _with_expected_columns(
        table_map.get(_table_key("sortedness", "minute_ordering"), pd.DataFrame()),
        [
            "minute_bucket",
            "n_records",
            "is_alphabetical",
            "kendall_tau",
            "kendall_p_value",
            "abs_kendall_tau",
        ],
    )

    rare_unique_ratio = _with_expected_columns(
        table_map.get(_table_key("rare_names", "unique_ratio_windows"), pd.DataFrame()),
        [
            "minute_bucket",
            "n_total",
            "n_unique_names",
            "unique_ratio",
            "threshold_unique_ratio",
            "is_low_power",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "bucket_minutes",
        ],
    )
    rare_weird = _with_expected_columns(
        table_map.get(_table_key("rare_names", "weird_names"), pd.DataFrame()),
        [
            "canonical_name",
            "sample_name",
            "weirdness_score",
            "name_length",
            "non_alpha_fraction",
            "name_entropy",
        ],
    )
    rare_singletons = _with_expected_columns(
        table_map.get(_table_key("rare_names", "singleton_names"), pd.DataFrame()),
        [
            "display_name",
            "canonical_name",
            "first_seen",
            "last_seen",
            "n_pro",
            "n_con",
            "time_span_minutes",
        ],
    )
    rare_rarity = _with_expected_columns(
        table_map.get(_table_key("rare_names", "rarity_by_minute"), pd.DataFrame()),
        [
            "minute_bucket",
            "n_total",
            "rarity_median",
            "rarity_p95",
            "is_low_power",
            "bucket_minutes",
        ],
    )

    org_blank_rates = _with_expected_columns(
        table_map.get(
            _table_key("org_anomalies", "organization_blank_rate_by_bucket"), pd.DataFrame()
        ),
        [
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "blank_org_rate",
            "blank_org_rate_wilson_low",
            "blank_org_rate_wilson_high",
            "pro_blank_org_rate",
            "con_blank_org_rate",
            "is_low_power",
            "pro_is_low_power",
            "con_is_low_power",
        ],
    )
    org_position_rates = _with_expected_columns(
        table_map.get(
            _table_key("org_anomalies", "organization_blank_rate_by_bucket_position"),
            pd.DataFrame(),
        ),
        [
            "bucket_start",
            "bucket_minutes",
            "position_normalized",
            "n_total",
            "blank_org_rate",
            "blank_org_rate_wilson_low",
            "blank_org_rate_wilson_high",
            "is_low_power",
        ],
    )
    org_bursts = _with_expected_columns(
        table_map.get(_table_key("org_anomalies", "organization_minute_bursts"), pd.DataFrame()),
        ["minute_bucket", "organization_clean", "n", "threshold"],
    )
    org_counts = _with_expected_columns(
        table_map.get(_table_key("org_anomalies", "organization_counts"), pd.DataFrame()),
        ["organization_clean", "n", "n_pro", "n_con", "first_seen", "last_seen"],
    )

    voter_bucket = _with_expected_columns(
        table_map.get(_table_key("voter_registry_match", "match_by_bucket"), pd.DataFrame()),
        [
            "bucket_start",
            "bucket_minutes",
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
            "n_pro",
            "n_con",
        ],
    )
    voter_position_rows = _with_expected_columns(
        table_map.get(_table_key("voter_registry_match", "linkage_by_position_rows"), pd.DataFrame()),
        [
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
        ],
    )
    voter_position_unique = _with_expected_columns(
        table_map.get(_table_key("voter_registry_match", "linkage_by_position_unique"), pd.DataFrame()),
        [
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
        ],
    )
    voter_pairwise = _with_expected_columns(
        table_map.get(_table_key("voter_registry_match", "position_pairwise_tests"), pd.DataFrame()),
        [
            "unit",
            "position_left",
            "position_right",
            "left_n_total",
            "left_n_unmatched",
            "left_unmatched_rate",
            "right_n_total",
            "right_n_unmatched",
            "right_unmatched_rate",
            "rate_difference",
            "odds_ratio",
            "p_value",
            "alpha",
            "is_significant",
            "inference_status",
        ],
    )
    voter_sensitivity_modes = _with_expected_columns(
        table_map.get(_table_key("voter_registry_match", "sensitivity_modes"), pd.DataFrame()),
        [
            "mode",
            "n_rows",
            "n_unmatched_rows",
            "unmatched_rate_rows",
            "n_unique_names",
            "n_unmatched_unique",
            "unmatched_rate_unique",
        ],
    )
    voter_unmatched = _with_expected_columns(
        table_map.get(_table_key("voter_registry_match", "unmatched_names"), pd.DataFrame()),
        [
            "display_name",
            "canonical_name",
            "n_rows",
            "n_pro",
            "n_con",
            "top_caveat",
            "best_similarity_score",
            "candidate_pool_size",
        ],
    )
    voter_bucket_position = _with_expected_columns(
        table_map.get(
            _table_key("voter_registry_match", "match_by_bucket_position"),
            pd.DataFrame(),
        ),
        [
            "bucket_start",
            "bucket_minutes",
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
        ],
    )

    # Compatibility aliases used by shared chart helpers/front-end logic.
    for frame in (voter_bucket, voter_position_rows, voter_position_unique, voter_bucket_position):
        if not frame.empty:
            if "match_rate" not in frame.columns and "matched_rate" in frame.columns:
                frame["match_rate"] = frame["matched_rate"]
            if "match_rate_wilson_low" not in frame.columns and "matched_rate_wilson_low" in frame.columns:
                frame["match_rate_wilson_low"] = frame["matched_rate_wilson_low"]
            if "match_rate_wilson_high" not in frame.columns and "matched_rate_wilson_high" in frame.columns:
                frame["match_rate_wilson_high"] = frame["matched_rate_wilson_high"]

    if "n_records" not in voter_unmatched.columns and "n_rows" in voter_unmatched.columns:
        voter_unmatched["n_records"] = voter_unmatched["n_rows"]
    if "display_name" not in voter_unmatched.columns:
        voter_unmatched["display_name"] = ""
    voter_unmatched["display_name"] = voter_unmatched["display_name"].fillna("").astype(str)
    if "canonical_name" in voter_unmatched.columns:
        canonical_display_names = (
            voter_unmatched["canonical_name"].fillna("").astype(str).map(_canonical_name_to_display_name)
        )
        voter_unmatched["display_name"] = voter_unmatched["display_name"].where(
            voter_unmatched["display_name"].str.strip() != "",
            canonical_display_names,
        )

    periodic_clockface = _with_expected_columns(
        table_map.get(_table_key("periodicity", "clockface_distribution"), pd.DataFrame()),
        [
            "minute_of_hour",
            "n_events",
            "expected_n_events_uniform",
            "deviation_from_uniform",
            "share",
            "z_score_uniform",
        ],
    )
    periodic_autocorr = _with_expected_columns(
        table_map.get(_table_key("periodicity", "autocorr"), pd.DataFrame()),
        ["lag_minutes", "autocorr", "abs_autocorr", "q_value", "is_significant"],
    )
    periodic_spectrum = _with_expected_columns(
        table_map.get(_table_key("periodicity", "spectrum_top"), pd.DataFrame()),
        ["period_minutes", "frequency_per_minute", "power", "q_value", "is_significant"],
    )
    periodic_fano_summary = _with_expected_columns(
        table_map.get(_table_key("periodicity", "rolling_fano_summary"), pd.DataFrame()),
        [
            "window_minutes",
            "n_windows",
            "median_fano_factor",
            "p95_fano_factor",
            "max_fano_factor",
            "high_fano_ratio",
        ],
    )

    multivariate_scores = _with_expected_columns(
        table_map.get(
            _table_key("multivariate_anomalies", "bucket_anomaly_scores"), pd.DataFrame()
        ),
        [
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "dup_name_fraction_weighted",
            "blank_org_rate",
            "anomaly_score",
            "anomaly_score_percentile",
            "is_anomaly",
            "is_low_power",
            "is_model_eligible",
            "log_n_total",
        ],
    )
    multivariate_top = _with_expected_columns(
        table_map.get(_table_key("multivariate_anomalies", "top_bucket_anomalies"), pd.DataFrame()),
        [
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "pro_rate",
            "anomaly_score",
            "anomaly_score_percentile",
            "is_anomaly",
            "is_low_power",
        ],
    )

    composite_ranked = _with_expected_columns(
        table_map.get(_table_key("composite_score", "ranked_windows"), pd.DataFrame()),
        [
            "minute_bucket",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "composite_score",
            "evidence_count",
            "burst_signal",
            "swing_signal",
            "changepoint_signal",
            "ml_anomaly_signal",
        ],
    )
    composite_evidence = _with_expected_columns(
        table_map.get(_table_key("composite_score", "evidence_bundle_windows"), pd.DataFrame()),
        ["minute_bucket", "evidence_flags"],
    )
    composite_high = _with_expected_columns(
        table_map.get(_table_key("composite_score", "high_priority_windows"), pd.DataFrame()),
        [
            "minute_bucket",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "composite_score",
            "burst_signal",
            "swing_signal",
            "changepoint_signal",
            "ml_anomaly_signal",
            "rarity_signal",
            "unique_signal",
        ],
    )

    for frame, column in [
        (counts_per_minute, "minute_bucket"),
        (bursts_significant, "start_minute"),
        (bursts_tests, "start_minute"),
        (time_bucket_profiles, "bucket_start"),
        (procon_direction_runs, "start_bucket"),
        (procon_direction_runs, "end_bucket"),
        (day_bucket_profiles, "date"),
        (all_changepoints, "change_minute"),
        (volume_changepoints, "change_minute"),
        (pro_rate_changepoints, "change_minute"),
        (off_hours_window_control, "bucket_start"),
        (dup_exact_bucket, "bucket_start"),
        (sorted_bucket, "bucket_start"),
        (sorted_minute, "minute_bucket"),
        (rare_unique_ratio, "minute_bucket"),
        (rare_singletons, "first_seen"),
        (rare_rarity, "minute_bucket"),
        (org_blank_rates, "bucket_start"),
        (org_position_rates, "bucket_start"),
        (org_bursts, "minute_bucket"),
        (voter_bucket, "bucket_start"),
        (voter_bucket_position, "bucket_start"),
        (multivariate_scores, "bucket_start"),
        (multivariate_top, "bucket_start"),
        (composite_ranked, "minute_bucket"),
        (composite_evidence, "minute_bucket"),
        (composite_high, "minute_bucket"),
    ]:
        if not frame.empty and column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")

    for frame in [bursts_significant, bursts_tests, bursts_null]:
        if (
            frame.empty
            or "window_minutes" not in frame.columns
            or "bucket_minutes" not in frame.columns
        ):
            continue
        window_minutes = pd.to_numeric(frame["window_minutes"], errors="coerce")
        bucket_minutes = pd.to_numeric(frame["bucket_minutes"], errors="coerce")
        frame["bucket_minutes"] = bucket_minutes.where(bucket_minutes.notna(), window_minutes)

    if not off_hours_window_control.empty:
        def _off_hours_bool(column_name: str) -> pd.Series:
            if column_name not in off_hours_window_control.columns:
                return pd.Series(False, index=off_hours_window_control.index, dtype=bool)
            return (
                pd.to_numeric(off_hours_window_control[column_name], errors="coerce")
                .fillna(0)
                .astype(int)
                .astype(bool)
            )

        lower_alert = _off_hours_bool("is_primary_alert_window")
        upper_alert = (
            _off_hours_bool("is_alert_off_hours_window")
            & (~_off_hours_bool("is_low_power"))
            & _off_hours_bool("is_above_primary_control_998")
            & (
                _off_hours_bool("is_significant_primary_upper")
                | _off_hours_bool("is_significant_primary_two_sided")
                | _off_hours_bool("is_primary_fdr_two_sided")
            )
            & _off_hours_bool("is_material_primary_upper_shift")
        )
        off_hours_window_control["is_primary_alert_window"] = lower_alert
        off_hours_window_control["is_primary_lower_alert_window"] = lower_alert
        off_hours_window_control["is_primary_upper_alert_window"] = upper_alert
        off_hours_window_control["is_primary_two_sided_alert_window"] = (
            lower_alert | upper_alert
        )

    if not voter_bucket.empty:
        voter_bucket["n_total"] = pd.to_numeric(
            voter_bucket.get("n_total"), errors="coerce"
        ).fillna(0)
        voter_bucket["n_matched_unique"] = pd.to_numeric(
            voter_bucket.get("n_matched_unique"), errors="coerce"
        ).fillna(0)
        voter_bucket["n_matched_ambiguous"] = pd.to_numeric(
            voter_bucket.get("n_matched_ambiguous"), errors="coerce"
        ).fillna(0)
        voter_bucket["matched_rate"] = pd.to_numeric(
            voter_bucket.get("matched_rate"), errors="coerce"
        )
        voter_bucket["n_matched"] = (
            voter_bucket["n_matched_unique"] + voter_bucket["n_matched_ambiguous"]
        )

        if not voter_bucket_position.empty:
            position_frame = voter_bucket_position.copy()
            position_frame["bucket_start"] = pd.to_datetime(
                position_frame.get("bucket_start"), errors="coerce"
            )
            position_frame["bucket_minutes"] = pd.to_numeric(
                position_frame.get("bucket_minutes"), errors="coerce"
            )
            position_frame["position_key"] = (
                position_frame.get("position_normalized", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
                .str.strip()
                .str.lower()
            )
            for position_key, prefix in (("pro", "pro"), ("con", "con")):
                position_subset = position_frame[
                    position_frame["position_key"] == position_key
                ].copy()
                if position_subset.empty:
                    continue
                position_subset = position_subset[
                    [
                        "bucket_start",
                        "bucket_minutes",
                        "matched_rate",
                        "matched_rate_wilson_low",
                        "matched_rate_wilson_high",
                    ]
                ].rename(
                    columns={
                        "matched_rate": f"matched_rate_{prefix}",
                        "matched_rate_wilson_low": f"matched_rate_{prefix}_wilson_low",
                        "matched_rate_wilson_high": f"matched_rate_{prefix}_wilson_high",
                    }
                )
                voter_bucket = voter_bucket.merge(
                    position_subset,
                    on=["bucket_start", "bucket_minutes"],
                    how="left",
                )
        for position_column in [
            "matched_rate_pro",
            "matched_rate_pro_wilson_low",
            "matched_rate_pro_wilson_high",
            "matched_rate_con",
            "matched_rate_con_wilson_low",
            "matched_rate_con_wilson_high",
        ]:
            if position_column not in voter_bucket.columns:
                voter_bucket[position_column] = np.nan

        n_total_all = float(pd.to_numeric(voter_bucket["n_total"], errors="coerce").sum())
        n_matched_all = float(
            pd.to_numeric(voter_bucket["n_matched"], errors="coerce").sum()
        )
        global_match_rate = (
            (n_matched_all / n_total_all) if n_total_all > 0 else float("nan")
        )
        voter_bucket["expected_match_rate_global"] = global_match_rate
        n_series = pd.to_numeric(voter_bucket["n_total"], errors="coerce").replace(0, np.nan)
        variance = global_match_rate * (1.0 - global_match_rate)
        std_error = np.sqrt(variance / n_series) if np.isfinite(global_match_rate) else np.nan
        voter_bucket["control_low_95_match_global"] = np.clip(
            global_match_rate - 1.96 * std_error, 0.0, 1.0
        )
        voter_bucket["control_high_95_match_global"] = np.clip(
            global_match_rate + 1.96 * std_error, 0.0, 1.0
        )
        voter_bucket["control_low_998_match_global"] = np.clip(
            global_match_rate - 3.0 * std_error, 0.0, 1.0
        )
        voter_bucket["control_high_998_match_global"] = np.clip(
            global_match_rate + 3.0 * std_error, 0.0, 1.0
        )
        voter_bucket["match_rate_delta_global"] = (
            pd.to_numeric(voter_bucket["matched_rate"], errors="coerce")
            - pd.to_numeric(voter_bucket["expected_match_rate_global"], errors="coerce")
        )
        voter_low_power = (
            pd.to_numeric(voter_bucket.get("is_low_power"), errors="coerce")
            .fillna(0)
            .astype(int)
            .astype(bool)
        )
        voter_bucket["is_match_rate_alert_lower"] = (
            (~voter_low_power)
            & pd.to_numeric(voter_bucket["matched_rate"], errors="coerce").notna()
            & (
                pd.to_numeric(voter_bucket["matched_rate"], errors="coerce")
                < pd.to_numeric(voter_bucket["control_low_998_match_global"], errors="coerce")
            )
        )
        voter_bucket["is_match_rate_alert_upper"] = (
            (~voter_low_power)
            & pd.to_numeric(voter_bucket["matched_rate"], errors="coerce").notna()
            & (
                pd.to_numeric(voter_bucket["matched_rate"], errors="coerce")
                > pd.to_numeric(voter_bucket["control_high_998_match_global"], errors="coerce")
            )
        )
        voter_bucket["is_match_rate_alert_any"] = (
            voter_bucket["is_match_rate_alert_lower"].astype(bool)
            | voter_bucket["is_match_rate_alert_upper"].astype(bool)
        )

    baseline_bucket_profiles = _build_bucketed_baseline_profiles(
        counts_per_minute=counts_per_minute,
        bucket_minutes=BASELINE_PROFILE_BUCKET_MINUTES,
    )
    baseline_day_hour_profiles = _build_bucketed_day_hour_profiles(
        baseline_bucket_profiles=baseline_bucket_profiles,
        counts_per_hour=counts_per_hour,
    )

    charts: dict[str, list[dict[str, Any]]] = {}

    charts["baseline_volume_pro_rate"] = _records_from_frame(
        baseline_bucket_profiles.sort_values(["bucket_minutes", "minute_bucket"]),
        columns=[
            "minute_bucket",
            "bucket_minutes",
            "n_total",
            "n_pro",
            "n_con",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
        max_rows=25_000,
    )
    charts["baseline_day_hour_volume"] = _records_from_frame(
        baseline_day_hour_profiles.sort_values(["bucket_minutes", "day_of_week", "hour"]),
        columns=[
            "bucket_minutes",
            "day_of_week",
            "hour",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
        max_rows=500,
    )
    charts["baseline_top_names"] = _records_from_frame(
        name_frequency.sort_values("n", ascending=False),
        columns=["display_name", "canonical_name", "n", "n_pro", "n_con", "time_span_minutes"],
        max_rows=200,
    )
    if not name_text_features.empty and "name_length" in name_text_features.columns:
        length_dist = (
            pd.to_numeric(name_text_features["name_length"], errors="coerce")
            .dropna()
            .astype(int)
            .value_counts()
            .sort_index()
            .rename_axis("name_length")
            .reset_index(name="n_names")
        )
    else:
        length_dist = pd.DataFrame()
    charts["baseline_name_length_distribution"] = _records_from_frame(
        length_dist,
        columns=["name_length", "n_names"],
        max_rows=200,
    )

    charts["bursts_hero_timeline"] = _records_from_frame(
        bursts_significant.sort_values(["start_minute", "window_minutes"]),
        columns=[
            "start_minute",
            "end_minute",
            "window_minutes",
            "bucket_minutes",
            "observed_count",
            "expected_count",
            "rate_ratio",
            "q_value",
            "is_significant",
        ],
        max_rows=25_000,
    )
    if not bursts_tests.empty and "window_minutes" in bursts_tests.columns:
        burst_sig_summary = (
            bursts_tests.assign(
                is_significant_bool=bursts_tests.get("is_significant", False).astype(bool),
            )
            .groupby("window_minutes", dropna=False)
            .agg(
                n_windows=("window_minutes", "size"),
                n_significant=("is_significant_bool", "sum"),
                median_rate_ratio=("rate_ratio", "median"),
            )
            .reset_index()
            .sort_values("window_minutes")
        )
        burst_sig_summary["bucket_minutes"] = pd.to_numeric(
            burst_sig_summary["window_minutes"],
            errors="coerce",
        )
    else:
        burst_sig_summary = pd.DataFrame()
    charts["bursts_significance_by_window"] = _records_from_frame(
        burst_sig_summary,
        columns=[
            "window_minutes",
            "bucket_minutes",
            "n_windows",
            "n_significant",
            "median_rate_ratio",
        ],
        max_rows=100,
    )
    charts["bursts_composition_shift"] = _records_from_frame(
        bursts_significant.sort_values(["start_minute", "window_minutes"]),
        columns=[
            "start_minute",
            "end_minute",
            "window_minutes",
            "bucket_minutes",
            "observed_count",
            "pro_rate",
            "baseline_pro_rate",
            "delta_pro_rate",
            "abs_delta_pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "q_value",
        ],
        max_rows=25_000,
    )
    charts["bursts_null_distribution"] = _records_from_frame(
        bursts_null.sort_values(["window_minutes", "iteration"]),
        columns=["window_minutes", "bucket_minutes", "iteration", "max_window_count"],
        max_rows=25_000,
    )

    charts["procon_swings_hero_bucket_trend"] = _records_from_frame(
        time_bucket_profiles.sort_values(["bucket_minutes", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "pro_rate",
            "baseline_pro_rate",
            "stable_lower",
            "stable_upper",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_flagged",
            "is_low_power",
        ],
        max_rows=25_000,
    )
    charts["procon_swings_shift_heatmap"] = _records_from_frame(
        day_bucket_profiles.sort_values(["bucket_minutes", "date", "slot_start_minute"]),
        columns=[
            "date",
            "bucket_minutes",
            "slot_start_minute",
            "delta_from_slot_pro_rate",
            "n_total",
            "is_slot_outlier",
            "is_low_power",
        ],
        max_rows=25_000,
    )
    charts["procon_swings_day_hour_heatmap"] = _records_from_frame(
        pro_rate_by_hour.sort_values(["day_of_week", "hour"]),
        columns=[
            "day_of_week",
            "hour",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
        max_rows=1_000,
    )
    charts["procon_swings_time_of_day_profile"] = _records_from_frame(
        time_of_day_profiles.sort_values(["bucket_minutes", "slot_start_minute"]),
        columns=[
            "bucket_minutes",
            "slot_start_minute",
            "n_total",
            "pro_rate",
            "baseline_pro_rate",
            "stable_lower",
            "stable_upper",
            "is_flagged",
            "is_low_power",
        ],
        max_rows=25_000,
    )
    charts["procon_swings_direction_runs"] = _records_from_frame(
        procon_direction_runs.sort_values(["bucket_minutes", "start_bucket"]),
        columns=[
            "bucket_minutes",
            "run_id",
            "run_direction",
            "start_bucket",
            "end_bucket",
            "run_length_buckets",
            "support_n",
            "mean_abs_delta_pro_rate",
            "max_abs_delta_pro_rate",
            "n_flagged_buckets",
            "n_low_power_buckets",
            "flagged_ratio",
            "low_power_ratio",
            "is_long_run",
        ],
        max_rows=10_000,
    )
    charts["procon_swings_null_distribution"] = _records_from_frame(
        swing_null.sort_values(["window_minutes", "iteration"]),
        columns=["window_minutes", "iteration", "max_abs_delta_pro_rate"],
        max_rows=25_000,
    )

    if not counts_per_minute.empty:
        change_minutes = set(
            pd.to_datetime(
                all_changepoints.get("change_minute", pd.Series(dtype="datetime64[ns]")),
                errors="coerce",
            )
            .dropna()
            .map(_serialize_value)
            .tolist()
        )
        changepoint_timeline = counts_per_minute.copy()
        changepoint_timeline["minute_bucket_serialized"] = changepoint_timeline[
            "minute_bucket"
        ].map(_serialize_value)
        changepoint_timeline["is_changepoint"] = changepoint_timeline[
            "minute_bucket_serialized"
        ].isin(change_minutes)
    else:
        changepoint_timeline = _with_expected_columns(
            pd.DataFrame(),
            [
                "minute_bucket",
                "n_total",
                "pro_rate",
                "pro_rate_wilson_low",
                "pro_rate_wilson_high",
                "is_low_power",
                "is_changepoint",
            ],
        )
    charts["changepoints_hero_timeline"] = _records_from_frame(
        changepoint_timeline.sort_values("minute_bucket"),
        columns=[
            "minute_bucket",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "is_changepoint",
        ],
        max_rows=25_000,
    )
    charts["changepoints_magnitude"] = _records_from_frame(
        all_changepoints.sort_values("abs_delta", ascending=False),
        columns=[
            "metric",
            "change_index",
            "change_minute",
            "mean_before",
            "mean_after",
            "delta",
            "abs_delta",
        ],
        max_rows=5_000,
    )
    if not all_changepoints.empty and "change_minute" in all_changepoints.columns:
        change_hour_hist = (
            all_changepoints.assign(change_hour=all_changepoints["change_minute"].dt.hour)
            .dropna(subset=["change_hour"])
            .groupby("change_hour", dropna=False)
            .size()
            .rename("n_changes")
            .reset_index()
            .sort_values("change_hour")
        )
    else:
        change_hour_hist = pd.DataFrame()
    charts["changepoints_hour_hist"] = _records_from_frame(
        change_hour_hist,
        columns=["change_hour", "n_changes"],
        max_rows=500,
    )

    charts["off_hours_hourly_profile"] = _records_from_frame(
        off_hours_hourly.sort_values("hour"),
        columns=[
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
        max_rows=500,
    )
    charts["off_hours_control_timeline"] = _records_from_frame(
        off_hours_window_control.sort_values(["bucket_minutes", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "event_date_key",
            "day_of_week",
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "n_known",
            "n_unknown",
            "n_off_hours",
            "off_hours_fraction",
            "is_off_hours_window",
            "is_pure_off_hours_window",
            "is_alert_off_hours_window",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "expected_pro_rate_day",
            "expected_pro_rate_model",
            "expected_pro_rate_primary",
            "expected_pro_rate_global",
            "baseline_source",
            "model_baseline_source",
            "primary_baseline_source",
            "is_model_baseline_available",
            "model_fit_method",
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_used_harmonics",
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
            "control_low_95_global",
            "control_high_95_global",
            "control_low_998_global",
            "control_high_998_global",
            "z_score_day",
            "z_score_model",
            "z_score_primary",
            "delta_pro_rate_day",
            "delta_pro_rate_model",
            "delta_pro_rate_primary",
            "p_value_day",
            "p_value_day_two_sided",
            "p_value_day_lower",
            "p_value_day_upper",
            "p_value_model",
            "p_value_model_two_sided",
            "p_value_model_lower",
            "p_value_model_upper",
            "p_value_primary",
            "p_value_primary_two_sided",
            "p_value_primary_lower",
            "p_value_primary_upper",
            "q_value_day",
            "q_value_day_lower",
            "q_value_day_upper",
            "q_value_day_two_sided",
            "q_value_model",
            "q_value_model_lower",
            "q_value_model_upper",
            "q_value_model_two_sided",
            "q_value_primary",
            "q_value_primary_lower",
            "q_value_primary_upper",
            "q_value_primary_two_sided",
            "is_significant_day",
            "is_significant_day_lower",
            "is_significant_day_upper",
            "is_significant_day_two_sided",
            "is_significant_model",
            "is_significant_model_lower",
            "is_significant_model_upper",
            "is_significant_model_two_sided",
            "is_significant_primary",
            "is_significant_primary_lower",
            "is_significant_primary_upper",
            "is_significant_primary_two_sided",
            "is_material_primary_shift",
            "is_material_primary_lower_shift",
            "is_material_primary_upper_shift",
            "is_primary_alert_window",
            "is_primary_lower_alert_window",
            "is_primary_upper_alert_window",
            "is_primary_two_sided_alert_window",
            "is_primary_spc_998_two_sided",
            "is_primary_fdr_two_sided",
            "is_primary_any_flag_channel",
            "is_primary_both_flag_channels",
            "is_below_day_control_95",
            "is_below_day_control_998",
            "is_above_day_control_95",
            "is_above_day_control_998",
            "is_below_model_control_95",
            "is_below_model_control_998",
            "is_above_model_control_95",
            "is_above_model_control_998",
            "is_below_primary_control_95",
            "is_below_primary_control_998",
            "is_above_primary_control_95",
            "is_above_primary_control_998",
            "is_outside_day_control_95",
            "is_outside_day_control_998",
            "is_outside_model_control_95",
            "is_outside_model_control_998",
            "is_outside_primary_control_95",
            "is_outside_primary_control_998",
            "is_below_global_control_95",
            "is_below_global_control_998",
        ],
        max_rows=100_000,
    )
    charts["off_hours_funnel_plot"] = _records_from_frame(
        off_hours_window_control.sort_values(["bucket_minutes", "n_known", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "n_known",
            "n_pro",
            "n_con",
            "off_hours_fraction",
            "is_off_hours_window",
            "is_pure_off_hours_window",
            "is_alert_off_hours_window",
            "pro_rate",
            "is_low_power",
            "expected_pro_rate_day",
            "expected_pro_rate_model",
            "expected_pro_rate_primary",
            "expected_pro_rate_global",
            "model_fit_method",
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_used_harmonics",
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
            "control_low_95_global",
            "control_high_95_global",
            "control_low_998_global",
            "control_high_998_global",
            "z_score_day",
            "z_score_model",
            "z_score_primary",
            "p_value_day",
            "p_value_day_two_sided",
            "p_value_model",
            "p_value_model_two_sided",
            "p_value_primary",
            "p_value_primary_two_sided",
            "q_value_day",
            "q_value_day_two_sided",
            "q_value_model",
            "q_value_model_two_sided",
            "q_value_primary",
            "q_value_primary_two_sided",
            "is_significant_day",
            "is_significant_day_two_sided",
            "is_significant_model",
            "is_significant_model_two_sided",
            "is_significant_primary",
            "is_significant_primary_lower",
            "is_significant_primary_upper",
            "is_significant_primary_two_sided",
            "is_material_primary_shift",
            "is_material_primary_lower_shift",
            "is_material_primary_upper_shift",
            "is_primary_alert_window",
            "is_primary_lower_alert_window",
            "is_primary_upper_alert_window",
            "is_primary_two_sided_alert_window",
            "is_primary_spc_998_two_sided",
            "is_primary_fdr_two_sided",
            "is_primary_any_flag_channel",
            "is_primary_both_flag_channels",
            "is_below_day_control_95",
            "is_below_day_control_998",
            "is_below_model_control_95",
            "is_below_model_control_998",
            "is_below_primary_control_95",
            "is_below_primary_control_998",
            "is_above_primary_control_95",
            "is_above_primary_control_998",
            "is_below_global_control_95",
            "is_below_global_control_998",
        ],
        max_rows=100_000,
    )
    overview_position_volume = off_hours_window_control.copy()
    if not overview_position_volume.empty:
        n_total = pd.to_numeric(overview_position_volume["n_total"], errors="coerce").fillna(0.0)
        n_pro = pd.to_numeric(overview_position_volume["n_pro"], errors="coerce").fillna(0.0)
        n_con = pd.to_numeric(overview_position_volume["n_con"], errors="coerce").fillna(0.0)
        n_unknown = pd.to_numeric(
            overview_position_volume["n_unknown"], errors="coerce"
        ).fillna(0.0)
        residual_other = (n_total - n_pro - n_con).clip(lower=0.0)
        overview_position_volume["n_other_position"] = n_unknown.where(
            n_unknown > 0, residual_other
        )
    charts["overview_position_volume_by_bucket"] = _records_from_frame(
        overview_position_volume.sort_values(["bucket_minutes", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "n_pro",
            "n_con",
            "n_other_position",
            "n_unknown",
            "n_known",
            "is_off_hours_window",
            "is_alert_off_hours_window",
            "is_low_power",
        ],
        max_rows=100_000,
    )
    off_hours_residual_timeline = off_hours_window_control.copy()
    if not off_hours_residual_timeline.empty:
        off_hours_residual_timeline["z_ref_zero"] = 0.0
        off_hours_residual_timeline["z_ref_pos3"] = 3.0
        off_hours_residual_timeline["z_ref_neg3"] = -3.0
    charts["off_hours_primary_residual_timeline"] = _records_from_frame(
        off_hours_residual_timeline.sort_values(["bucket_minutes", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "n_known",
            "n_pro",
            "n_con",
            "is_off_hours_window",
            "is_alert_off_hours_window",
            "is_low_power",
            "pro_rate",
            "expected_pro_rate_primary",
            "delta_pro_rate_primary",
            "z_score_primary",
            "z_score_day",
            "z_ref_zero",
            "z_ref_pos3",
            "z_ref_neg3",
            "p_value_primary",
            "p_value_primary_two_sided",
            "q_value_primary",
            "q_value_primary_two_sided",
            "is_primary_spc_998_two_sided",
            "is_primary_fdr_two_sided",
            "is_primary_any_flag_channel",
            "is_primary_both_flag_channels",
            "is_primary_alert_window",
            "is_primary_lower_alert_window",
            "is_primary_upper_alert_window",
            "is_primary_two_sided_alert_window",
            "primary_baseline_source",
            "is_model_baseline_available",
            "model_fit_method",
            "model_fit_rows",
            "model_fit_unique_days",
            "model_fit_unique_hours",
            "model_fit_used_harmonics",
        ],
        max_rows=100_000,
    )
    charts["off_hours_primary_flag_channels"] = _records_from_frame(
        off_hours_flag_channels.sort_values(["rank", "channel"]),
        columns=[
            "rank",
            "channel",
            "channel_label",
            "count",
            "share_of_tested",
        ],
        max_rows=50,
    )
    charts["off_hours_date_hour_pro_heatmap"] = _records_from_frame(
        off_hours_date_hour.sort_values(["date", "hour"]),
        columns=[
            "date",
            "day_of_week",
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "n_known",
            "n_unknown",
            "n_off_hours",
            "off_hours_fraction",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
        max_rows=20_000,
    )
    charts["off_hours_date_hour_primary_residual_heatmap"] = _records_from_frame(
        off_hours_date_hour_primary_residual.sort_values(["bucket_minutes", "date", "hour"]),
        columns=[
            "bucket_minutes",
            "date",
            "day_of_week",
            "hour",
            "n_windows",
            "n_windows_alert_eligible",
            "n_windows_tested",
            "n_windows_low_power",
            "n_windows_primary_alert",
            "primary_alert_fraction_tested",
            "n_total",
            "n_known",
            "n_known_tested",
            "n_pro",
            "n_con",
            "off_hours_fraction",
            "pro_rate",
            "expected_pro_rate_primary",
            "delta_pro_rate_primary",
            "z_score_primary",
            "z_score_primary_median",
            "z_score_primary_abs_max",
            "is_low_power",
        ],
        max_rows=20_000,
    )
    charts["off_hours_date_hour_volume_heatmap"] = _records_from_frame(
        off_hours_date_hour.sort_values(["date", "hour"]),
        columns=[
            "date",
            "day_of_week",
            "hour",
            "n_total",
            "n_pro",
            "n_con",
            "n_known",
            "n_unknown",
            "n_off_hours",
            "off_hours_fraction",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
        ],
        max_rows=20_000,
    )
    charts["off_hours_summary_compare"] = _records_from_frame(
        off_hours_summary,
        columns=[
            "off_hours",
            "on_hours",
            "off_hours_ratio",
            "off_hours_pro_rate",
            "on_hours_pro_rate",
            "off_hours_pro_rate_wilson_low",
            "off_hours_pro_rate_wilson_high",
            "on_hours_pro_rate_wilson_low",
            "on_hours_pro_rate_wilson_high",
            "chi_square_p_value",
            "off_hours_is_low_power",
            "on_hours_is_low_power",
            "primary_bucket_minutes",
            "primary_baseline_method",
            "alert_off_hours_min_fraction",
            "primary_alert_min_abs_delta",
            "off_hours_windows_alert_eligible",
            "off_hours_windows_alert_eligible_low_power",
            "off_hours_windows_alert_eligible_tested_fraction",
            "off_hours_windows_alert_eligible_low_power_fraction",
            "off_hours_windows_tested",
            "off_hours_windows_below_day_control_95",
            "off_hours_windows_below_day_control_998",
            "off_hours_windows_below_model_control_95",
            "off_hours_windows_below_model_control_998",
            "off_hours_windows_below_primary_control_95",
            "off_hours_windows_below_primary_control_998",
            "off_hours_windows_above_primary_control_95",
            "off_hours_windows_above_primary_control_998",
            "off_hours_windows_significant_day",
            "off_hours_windows_significant_model",
            "off_hours_windows_significant_primary",
            "off_hours_windows_significant_primary_upper",
            "off_hours_windows_significant_primary_two_sided",
            "off_hours_windows_primary_spc_998_any",
            "off_hours_windows_primary_fdr_two_sided",
            "off_hours_windows_primary_flag_any",
            "off_hours_windows_primary_flag_both",
            "off_hours_windows_primary_spc_998_any_fraction",
            "off_hours_windows_primary_fdr_two_sided_fraction",
            "off_hours_windows_primary_flag_any_fraction",
            "off_hours_windows_primary_flag_both_fraction",
            "off_hours_windows_primary_alert",
            "off_hours_windows_primary_alert_fraction",
            "off_hours_primary_alert_run_count",
            "off_hours_primary_alert_max_run_windows",
            "off_hours_primary_alert_max_run_minutes",
            "off_hours_min_day_z",
            "off_hours_max_abs_day_z",
            "off_hours_min_model_z",
            "off_hours_max_abs_model_z",
            "off_hours_min_primary_z",
            "off_hours_max_abs_primary_z",
            "off_hours_min_primary_delta",
            "off_hours_max_abs_primary_delta",
            "off_hours_windows_model_available",
            "global_daytime_pro_rate",
            "day_adjusted_fdr_alpha",
            "model_fit_min_rows",
            "model_hour_harmonics",
            "primary_model_fit_method",
            "primary_model_fit_rows",
            "primary_model_fit_unique_days",
            "primary_model_fit_unique_hours",
            "primary_model_fit_converged",
            "primary_model_fit_aic",
        ],
        max_rows=10,
    )

    charts["duplicates_exact_bucket_concentration"] = _records_from_frame(
        dup_exact_bucket.sort_values(["bucket_minutes", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "scope",
            "metric",
            "n_rows",
            "n_unique_names",
            "duplicate_rows",
            "duplicate_row_rate",
            "expected_duplicate_rows",
            "excess_duplicate_rows",
            "n_used",
            "N_used",
            "baseline_model",
            "baseline_source",
            "baseline_degraded",
            "n_pro",
            "n_con",
        ],
        max_rows=100_000,
    )
    charts["duplicates_exact_metric_diagnostics"] = _records_from_frame(
        dup_exact_metric_diagnostics.sort_values(["scope", "metric"]),
        columns=[
            "scope",
            "metric",
            "observed",
            "expected",
            "expected_p05",
            "expected_p50",
            "expected_p95",
            "z_score",
            "p_value",
            "n_used",
            "N_used",
        ],
        max_rows=50,
    )
    charts["duplicates_exact_per_name_anomalies"] = _records_from_frame(
        dup_exact_per_name.sort_values(["q_value", "p_value", "n"], ascending=[True, True, False]),
        columns=[
            "display_name",
            "canonical_name",
            "n",
            "n_pro",
            "n_con",
            "time_span_minutes",
            "expected_count",
            "p_value",
            "q_value",
            "is_significant",
            "within_5m_pairs",
            "within_15m_pairs",
            "temporal_p_value_within_5m",
            "temporal_p_value_min_gap",
        ],
        max_rows=15,
    )
    top_name_timing_sorted = dup_exact_top_name_timing.sort_values(
        ["match_mode", "rank", "bucket_minutes", "bucket_start", "name_key"]
    )
    top_name_timing_rows = _records_from_frame(
        top_name_timing_sorted,
        columns=[
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
            "n_other",
            "first_seen",
            "last_seen",
        ],
        max_rows=100_000,
    )
    top_name_timing_rank_rows = _records_from_frame(
        top_name_timing_sorted.drop_duplicates(
            subset=["scope", "match_mode", "rank", "name_key"],
            keep="first",
        ),
        columns=[
            "scope",
            "match_mode",
            "match_label",
            "match_definition",
            "rank",
            "name_key",
            "display_name",
            "total_repeated_rows",
        ],
        max_rows=100_000,
    )

    def _top_name_timing_rows_for_mode(match_mode: str) -> list[dict[str, Any]]:
        normalized_mode = str(match_mode).strip().lower()
        bucket_rows = [
            row
            for row in top_name_timing_rows
            if str(row.get("match_mode", "")).strip().lower() == normalized_mode
        ]
        rank_rows = [
            {**row, "row_kind": "name_rank"}
            for row in top_name_timing_rank_rows
            if str(row.get("match_mode", "")).strip().lower() == normalized_mode
        ]
        return bucket_rows + rank_rows

    charts["duplicates_exact_top_name_timing_exact"] = _top_name_timing_rows_for_mode("exact")
    charts["duplicates_exact_position_concentration"] = _records_from_frame(
        dup_exact_position_tests.assign(
            pair_label=(
                dup_exact_position_tests["position_left"].astype(str)
                + " vs "
                + dup_exact_position_tests["position_right"].astype(str)
            )
        ).sort_values("permutation_p_value_one_sided"),
        columns=[
            "pair_label",
            "position_left",
            "position_right",
            "left_duplicate_row_rate",
            "right_duplicate_row_rate",
            "rate_difference",
            "rate_difference_ci_low",
            "rate_difference_ci_high",
            "rate_ratio",
            "permutation_p_value_one_sided",
            "left_is_low_power",
            "right_is_low_power",
        ],
        max_rows=500,
    )
    charts["duplicates_exact_null_distribution"] = _records_from_frame(
        dup_exact_null_distribution.sort_values("iteration"),
        columns=[
            "iteration",
            "duplicate_rows",
            "duplicate_row_rate",
            "duplicate_pairs",
            "n_names_ge2",
            "n_names_ge3",
            "n_names_ge5",
            "n_names_ge10",
            "max_count",
        ],
        max_rows=25_000,
    )
    charts["duplicates_exact_swing_impact"] = _records_from_frame(
        dup_exact_swing_impact,
        columns=[
            "scenario",
            "n_pro_effective",
            "n_con_effective",
            "pro_share",
        ],
        max_rows=20,
    )

    # Compatibility aliases retained during contract migration.
    charts["duplicates_exact_top_names"] = charts["duplicates_exact_per_name_anomalies"]
    charts["duplicates_exact_position_switch"] = charts["duplicates_exact_per_name_anomalies"]

    charts["sortedness_bucket_ratio"] = _records_from_frame(
        sorted_bucket.sort_values(["bucket_minutes", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_records",
            "is_alphabetical",
            "kendall_tau",
            "kendall_p_value",
            "abs_kendall_tau",
        ],
        max_rows=25_000,
    )
    charts["sortedness_bucket_summary"] = _records_from_frame(
        sorted_summary.sort_values("bucket_minutes"),
        columns=[
            "bucket_minutes",
            "n_buckets",
            "avg_records_per_bucket",
            "alphabetical_ratio",
            "mean_kendall_tau",
            "mean_abs_kendall_tau",
            "max_abs_kendall_tau",
            "strong_ordering_ratio",
        ],
        max_rows=500,
    )
    charts["sortedness_kendall_tau_summary"] = _records_from_frame(
        sorted_summary.sort_values("bucket_minutes"),
        columns=[
            "bucket_minutes",
            "mean_kendall_tau",
            "mean_abs_kendall_tau",
            "max_abs_kendall_tau",
            "strong_ordering_ratio",
        ],
        max_rows=500,
    )
    charts["sortedness_minute_spikes"] = _records_from_frame(
        sorted_minute.sort_values("minute_bucket"),
        columns=[
            "minute_bucket",
            "n_records",
            "is_alphabetical",
            "kendall_tau",
            "kendall_p_value",
            "abs_kendall_tau",
        ],
        max_rows=25_000,
    )

    charts["rare_names_unique_ratio"] = _records_from_frame(
        rare_unique_ratio.sort_values("minute_bucket"),
        columns=[
            "minute_bucket",
            "bucket_minutes",
            "n_total",
            "n_unique_names",
            "unique_ratio",
            "threshold_unique_ratio",
            "is_low_power",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
        ],
        max_rows=25_000,
    )
    charts["rare_names_weird_scores"] = _records_from_frame(
        rare_weird.sort_values("weirdness_score", ascending=False),
        columns=[
            "canonical_name",
            "sample_name",
            "weirdness_score",
            "name_length",
            "non_alpha_fraction",
            "name_entropy",
        ],
        max_rows=1_000,
    )
    charts["rare_names_singletons"] = _records_from_frame(
        rare_singletons.sort_values("first_seen"),
        columns=[
            "display_name",
            "canonical_name",
            "first_seen",
            "last_seen",
            "n_pro",
            "n_con",
            "time_span_minutes",
        ],
        max_rows=25_000,
    )
    charts["rare_names_rarity_timeline"] = _records_from_frame(
        rare_rarity.sort_values("minute_bucket"),
        columns=[
            "minute_bucket",
            "bucket_minutes",
            "n_total",
            "rarity_median",
            "rarity_p95",
            "is_low_power",
        ],
        max_rows=25_000,
    )

    charts["org_anomalies_blank_rate"] = _records_from_frame(
        org_blank_rates.sort_values(["bucket_minutes", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "blank_org_rate",
            "blank_org_rate_wilson_low",
            "blank_org_rate_wilson_high",
            "pro_blank_org_rate",
            "con_blank_org_rate",
            "is_low_power",
            "pro_is_low_power",
            "con_is_low_power",
        ],
        max_rows=25_000,
    )
    charts["org_anomalies_position_rates"] = _records_from_frame(
        org_position_rates.sort_values(["bucket_minutes", "bucket_start", "position_normalized"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "position_normalized",
            "n_total",
            "blank_org_rate",
            "blank_org_rate_wilson_low",
            "blank_org_rate_wilson_high",
            "is_low_power",
        ],
        max_rows=25_000,
    )
    charts["org_anomalies_bursts"] = _records_from_frame(
        org_bursts.sort_values("minute_bucket"),
        columns=["minute_bucket", "organization_clean", "n", "threshold"],
        max_rows=5_000,
    )
    charts["org_anomalies_top_orgs"] = _records_from_frame(
        org_counts.sort_values("n", ascending=False),
        columns=["organization_clean", "n", "n_pro", "n_con", "first_seen", "last_seen"],
        max_rows=1_000,
    )

    charts["voter_registry_match_rates"] = _records_from_frame(
        voter_bucket.sort_values(["bucket_minutes", "bucket_start"]),
        columns=[
            "bucket_start",
            "bucket_minutes",
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
            "matched_rate_pro",
            "matched_rate_pro_wilson_low",
            "matched_rate_pro_wilson_high",
            "matched_rate_con",
            "matched_rate_con_wilson_low",
            "matched_rate_con_wilson_high",
            "expected_match_rate_global",
            "control_low_95_match_global",
            "control_high_95_match_global",
            "control_low_998_match_global",
            "control_high_998_match_global",
            "match_rate_delta_global",
            "is_match_rate_alert_lower",
            "is_match_rate_alert_upper",
            "is_match_rate_alert_any",
            "is_low_power",
            "n_pro",
            "n_con",
        ],
        max_rows=25_000,
    )
    charts["voter_registry_linkage_by_position_rows"] = _records_from_frame(
        voter_position_rows.sort_values("position_normalized"),
        columns=[
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
        ],
        max_rows=100,
    )
    charts["voter_registry_linkage_by_position_unique"] = _records_from_frame(
        voter_position_unique.sort_values("position_normalized"),
        columns=[
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
        ],
        max_rows=100,
    )
    charts["voter_registry_unmatched_names"] = _records_from_frame(
        voter_unmatched.sort_values("n_records", ascending=False),
        columns=[
            "display_name",
            "canonical_name",
            "n_records",
            "n_pro",
            "n_con",
            "top_caveat",
            "best_similarity_score",
            "candidate_pool_size",
        ],
        max_rows=10,
    )
    charts["voter_registry_pairwise_tests"] = _records_from_frame(
        voter_pairwise.assign(
            pair_label=(
                voter_pairwise["unit"].astype(str)
                + ": "
                + voter_pairwise["position_left"].astype(str)
                + " vs "
                + voter_pairwise["position_right"].astype(str)
            )
        ).sort_values(["unit", "p_value", "pair_label"]),
        columns=[
            "unit",
            "pair_label",
            "position_left",
            "position_right",
            "left_n_total",
            "left_n_unmatched",
            "left_unmatched_rate",
            "right_n_total",
            "right_n_unmatched",
            "right_unmatched_rate",
            "rate_difference",
            "odds_ratio",
            "p_value",
            "alpha",
            "is_significant",
            "inference_status",
        ],
        max_rows=250,
    )
    charts["voter_registry_sensitivity_modes"] = _records_from_frame(
        voter_sensitivity_modes.sort_values("mode"),
        columns=[
            "mode",
            "n_rows",
            "n_unmatched_rows",
            "unmatched_rate_rows",
            "n_unique_names",
            "n_unmatched_unique",
            "unmatched_rate_unique",
        ],
        max_rows=20,
    )
    charts["voter_registry_position_buckets"] = _records_from_frame(
        voter_bucket_position.sort_values(
            ["bucket_minutes", "bucket_start", "position_normalized"]
        ),
        columns=[
            "bucket_start",
            "bucket_minutes",
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
        ],
        max_rows=25_000,
    )
    # Compatibility aliases retained during contract migration.
    charts["voter_registry_match_by_position"] = charts["voter_registry_linkage_by_position_rows"]
    charts["voter_registry_match_tiers"] = charts["voter_registry_sensitivity_modes"]

    charts["periodicity_clockface"] = _records_from_frame(
        periodic_clockface.sort_values("minute_of_hour"),
        columns=[
            "minute_of_hour",
            "n_events",
            "expected_n_events_uniform",
            "deviation_from_uniform",
            "share",
            "z_score_uniform",
        ],
        max_rows=500,
    )
    charts["periodicity_autocorr"] = _records_from_frame(
        periodic_autocorr.sort_values("lag_minutes"),
        columns=["lag_minutes", "autocorr", "abs_autocorr", "q_value", "is_significant"],
        max_rows=5_000,
    )
    charts["periodicity_spectrum"] = _records_from_frame(
        periodic_spectrum.sort_values("power", ascending=False),
        columns=["period_minutes", "frequency_per_minute", "power", "q_value", "is_significant"],
        max_rows=5_000,
    )
    charts["periodicity_rolling_fano"] = _records_from_frame(
        periodic_fano_summary.sort_values("window_minutes"),
        columns=[
            "window_minutes",
            "n_windows",
            "median_fano_factor",
            "p95_fano_factor",
            "max_fano_factor",
            "high_fano_ratio",
        ],
        max_rows=500,
    )

    charts["multivariate_score_timeline"] = _records_from_frame(
        multivariate_scores.sort_values("bucket_start"),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "dup_name_fraction_weighted",
            "blank_org_rate",
            "anomaly_score",
            "anomaly_score_percentile",
            "is_anomaly",
            "is_low_power",
            "is_model_eligible",
        ],
        max_rows=25_000,
    )
    charts["multivariate_top_buckets"] = _records_from_frame(
        multivariate_top.sort_values("anomaly_score", ascending=False),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "n_total",
            "pro_rate",
            "anomaly_score",
            "anomaly_score_percentile",
            "is_anomaly",
            "is_low_power",
        ],
        max_rows=1_000,
    )
    charts["multivariate_feature_projection"] = _records_from_frame(
        multivariate_scores.sort_values("anomaly_score", ascending=False),
        columns=[
            "bucket_start",
            "bucket_minutes",
            "log_n_total",
            "pro_rate",
            "dup_name_fraction_weighted",
            "blank_org_rate",
            "anomaly_score",
            "anomaly_score_percentile",
            "is_anomaly",
        ],
        max_rows=25_000,
    )

    charts["composite_score_timeline"] = _records_from_frame(
        composite_ranked.sort_values("minute_bucket"),
        columns=[
            "minute_bucket",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "composite_score",
            "evidence_count",
            "burst_signal",
            "swing_signal",
            "changepoint_signal",
            "ml_anomaly_signal",
        ],
        max_rows=25_000,
    )
    if not composite_evidence.empty and "evidence_flags" in composite_evidence.columns:
        flag_counts: dict[str, int] = defaultdict(int)
        for raw in composite_evidence["evidence_flags"].fillna("").astype(str).tolist():
            for token in [item.strip() for item in raw.split(",") if item.strip()]:
                flag_counts[token] += 1
        evidence_flag_table = pd.DataFrame(
            [
                {"flag": name, "count": count}
                for name, count in sorted(
                    flag_counts.items(), key=lambda item: item[1], reverse=True
                )
            ]
        )
    else:
        evidence_flag_table = pd.DataFrame()
    charts["composite_evidence_flags"] = _records_from_frame(
        evidence_flag_table,
        columns=["flag", "count"],
        max_rows=1_000,
    )
    charts["composite_high_priority"] = _records_from_frame(
        composite_high.sort_values("composite_score", ascending=False),
        columns=[
            "minute_bucket",
            "n_total",
            "pro_rate",
            "pro_rate_wilson_low",
            "pro_rate_wilson_high",
            "is_low_power",
            "composite_score",
            "burst_signal",
            "swing_signal",
            "changepoint_signal",
            "ml_anomaly_signal",
            "rarity_signal",
            "unique_signal",
        ],
        max_rows=5_000,
    )

    analysis_definitions = registry_analysis_definitions()
    look_for_details = _detailed_what_to_look_for_by_analysis()
    analysis_help_docs = _build_analysis_help_docs(
        analysis_definitions=analysis_definitions,
        detailed_look_for=look_for_details,
    )
    chart_legend_docs = _default_chart_legend_docs()
    for chart_id in charts.keys():
        if chart_id not in chart_legend_docs:
            chart_legend_docs[chart_id] = _fallback_chart_legend_doc(chart_id)
    chart_help_docs = _build_chart_help_docs(chart_legend_docs=chart_legend_docs)
    analysis_catalog: list[dict[str, Any]] = []

    bucket_map: dict[str, list[int]] = {
        "baseline_profile": _extract_bucket_options(
            baseline_bucket_profiles,
            baseline_day_hour_profiles,
        ),
        "bursts": _extract_bucket_options(bursts_significant, bursts_tests),
        "procon_swings": _extract_bucket_options(
            time_bucket_profiles, day_bucket_profiles, time_of_day_profiles, procon_direction_runs
        ),
        "changepoints": [],
        "off_hours": _extract_bucket_options(off_hours_window_control),
        "duplicates_exact": _extract_bucket_options(dup_exact_bucket),
        "sortedness": _extract_bucket_options(sorted_bucket, sorted_summary),
        "rare_names": _extract_bucket_options(rare_unique_ratio, rare_rarity),
        "org_anomalies": _extract_bucket_options(org_blank_rates, org_position_rates),
        "voter_registry_match": _extract_bucket_options(voter_bucket, voter_bucket_position),
        "periodicity": [],
        "multivariate_anomalies": _extract_bucket_options(multivariate_scores),
        "composite_score": [],
    }
    standard_buckets = [int(value) for value in BASELINE_PROFILE_BUCKET_MINUTES]
    for definition in registry_analysis_definitions():
        analysis_id = str(definition["id"])
        current = {int(value) for value in bucket_map.get(analysis_id, []) if int(value) > 0}
        bucket_map[analysis_id] = sorted(current.union(standard_buckets))

    for definition in analysis_definitions:
        status, reason = analysis_registry_status(
            detector=definition.get("detector"),
            charts=charts,
            hero_chart_id=str(definition["hero_chart_id"]),
            detail_chart_ids=list(definition["detail_chart_ids"]),
            detector_summaries=detector_summaries,
        )
        analysis_catalog.append(
            {
                "id": definition["id"],
                "title": definition["title"],
                "detector": definition.get("detector"),
                "status": status,
                "reason": reason,
                "hero_chart_id": definition["hero_chart_id"],
                "detail_chart_ids": definition["detail_chart_ids"],
                "bucket_options": bucket_map.get(definition["id"], []),
                "group": definition.get("group", "detector_analysis"),
                "priority": int(definition.get("priority", 50)),
                "how_to_read": definition["how_to_read"],
                "what_to_look_for": definition["what_to_look_for"],
                "what_to_look_for_details": look_for_details.get(str(definition["id"]), []),
                "common_benign_causes": definition["common_benign_causes"],
                "help_sections": analysis_help_docs.get(str(definition["id"]), {}),
            }
        )

    analysis_allowlist = registry_configured_analysis_ids()
    if analysis_allowlist:
        allowset = set(analysis_allowlist)
        analysis_catalog = [
            analysis
            for analysis in analysis_catalog
            if str(analysis.get("id") or "").strip() in allowset
        ]
        allowlist_order = {analysis_id: index for index, analysis_id in enumerate(analysis_allowlist)}
        analysis_catalog.sort(
            key=lambda analysis: allowlist_order.get(str(analysis.get("id") or ""), len(allowlist_order))
        )
    visible_analysis_ids = [str(analysis.get("id") or "").strip() for analysis in analysis_catalog]
    visible_analysis_id_set = set(visible_analysis_ids)
    focus_analysis_ids = [
        analysis_id for analysis_id in analysis_allowlist if analysis_id in visible_analysis_id_set
    ]
    focus_mode = registry_focus_mode_for_analysis_ids(focus_analysis_ids)
    visible_chart_ids = {
        str(chart_id)
        for analysis in analysis_catalog
        for chart_id in [analysis.get("hero_chart_id"), *(analysis.get("detail_chart_ids") or [])]
        if isinstance(chart_id, str) and chart_id
    }
    supplemental_chart_ids = {
        "off_hours_hourly_profile",
        "off_hours_summary_compare",
        "off_hours_date_hour_pro_heatmap",
        "off_hours_date_hour_primary_residual_heatmap",
        "off_hours_date_hour_volume_heatmap",
        "overview_position_volume_by_bucket",
        "duplicates_exact_null_distribution",
        "duplicates_exact_top_names",
        "duplicates_exact_position_switch",
        "voter_registry_position_buckets",
        "voter_registry_match_by_position",
        "voter_registry_match_tiers",
    }
    retained_chart_ids = visible_chart_ids | supplemental_chart_ids
    charts = {
        chart_id: rows
        for chart_id, rows in charts.items()
        if chart_id in retained_chart_ids
    }
    chart_legend_docs = {
        chart_id: legend
        for chart_id, legend in chart_legend_docs.items()
        if chart_id in retained_chart_ids
    }
    chart_help_docs = {
        chart_id: help_doc
        for chart_id, help_doc in chart_help_docs.items()
        if chart_id in retained_chart_ids
    }

    global_bucket_options = sorted(
        {
            value
            for analysis in analysis_catalog
            for value in analysis.get("bucket_options", [])
            if isinstance(value, int)
        }
    )
    preferred_global = [
        value for value in (1, 5, 15, 30, 60, 120, 240) if value in global_bucket_options
    ]
    if preferred_global:
        global_bucket_options = preferred_global

    absolute_time_chart_ids = [
        "baseline_volume_pro_rate",
        "bursts_hero_timeline",
        "procon_swings_hero_bucket_trend",
        "changepoints_hero_timeline",
        "overview_position_volume_by_bucket",
        "off_hours_control_timeline",
        "off_hours_primary_residual_timeline",
        "duplicates_exact_bucket_concentration",
        "sortedness_bucket_ratio",
        "rare_names_unique_ratio",
        "org_anomalies_blank_rate",
        "org_anomalies_position_rates",
        "voter_registry_match_rates",
        "voter_registry_position_buckets",
        "multivariate_score_timeline",
        "composite_score_timeline",
    ]
    absolute_time_chart_ids = [
        chart_id for chart_id in absolute_time_chart_ids if charts.get(chart_id)
    ]

    resolved_default_dedup_mode = normalize_dedup_mode(
        default_dedup_mode,
        default=DEFAULT_DEDUP_MODE,
    )
    triage_views = build_investigation_views(table_map=table_map)
    investigation = triage_views.get(resolved_default_dedup_mode, triage_views.get("raw", {}))
    triage_summary = investigation.get("triage_summary", {})
    data_quality_panel = build_data_quality_panel(
        table_map=table_map,
        triage_views=triage_views,
        min_cell_n_for_rates=min_cell_n_for_rates,
    )
    hearing_context_panel = _build_hearing_context_panel(
        counts_per_minute,
        hearing_metadata=hearing_metadata,
        min_cell_n_for_rates=min_cell_n_for_rates,
    )

    timezone_name = PACIFIC_TIMEZONE_NAME
    process_markers = hearing_context_panel.get("process_markers", [])
    evidence_taxonomy = default_evidence_taxonomy()
    methodology = build_methodology_content(evidence_taxonomy=evidence_taxonomy)
    if "dup_exact_methods" in locals() and isinstance(dup_exact_methods, pd.DataFrame) and not dup_exact_methods.empty:
        baseline_models = sorted(
            {
                str(value)
                for value in dup_exact_methods.get("baseline_model", pd.Series(dtype=str)).tolist()
                if str(value).strip()
            }
        )
        baseline_sources = sorted(
            {
                str(value)
                for value in dup_exact_methods.get("baseline_source", pd.Series(dtype=str)).tolist()
                if str(value).strip()
            }
        )
        degraded = bool(
            pd.to_numeric(
                dup_exact_methods.get("baseline_degraded", pd.Series(dtype=float)),
                errors="coerce",
            )
            .fillna(0.0)
            .astype(float)
            .gt(0.0)
            .any()
        )
        methodology["definitions"].append(
            {
                "term": "Duplicate baseline runtime",
                "definition": (
                    "Duplicate-collision expectations were generated from runtime-selected "
                    f"sources/models: sources={','.join(baseline_sources) or 'unknown'}, "
                    f"models={','.join(baseline_models) or 'unknown'}."
                ),
            }
        )
        if degraded:
            methodology["caveats"].append(
                "Duplicate-collision baseline degraded during runtime; review methods metadata before inference."
            )
        methodology["duplicate_runtime"] = _records_from_frame(
            dup_exact_methods,
            columns=[
                "scope",
                "baseline_source",
                "baseline_model",
                "uncertainty_model",
                "n_used",
                "N_used",
                "metric_primary",
                "baseline_degraded",
                "fallback_policy",
                "collision_key_mode",
                "stratification",
            ],
            max_rows=20,
        )
    theme_options = default_theme_options()
    color_semantics = default_color_semantics()

    payload = {
        "version": 4,
        "analysis_catalog": analysis_catalog,
        "charts": charts,
        "chart_legend_docs": chart_legend_docs,
        "chart_help_docs": chart_help_docs,
        "triage_views": triage_views,
        "triage_summary": triage_summary,
        "data_quality_panel": data_quality_panel,
        "hearing_context_panel": hearing_context_panel,
        "controls": {
            "default_bucket_minutes": 30
            if 30 in global_bucket_options
            else (global_bucket_options[0] if global_bucket_options else None),
            "global_bucket_options": global_bucket_options,
            "zoom_sync_groups": {"absolute_time": absolute_time_chart_ids},
            "evidence_taxonomy": evidence_taxonomy,
            "methodology": methodology,
            "theme_options": theme_options,
            "default_theme": "light",
            "color_semantics": color_semantics,
            "dedup_modes": list(DEDUP_MODES),
            "default_dedup_mode": resolved_default_dedup_mode,
            "duplicate_collision_scope_default": primary_dup_scope,
            "duplicate_collision_metric_default": primary_dup_metric,
            "duplicate_collision_scope_options": duplicate_scope_options,
            "duplicate_collision_metric_options": duplicate_metric_options,
            "timezone": timezone_name,
            "timezone_label": timezone_name,
            "process_markers": process_markers,
            "focus_mode": focus_mode,
            "focus_analysis_ids": focus_analysis_ids,
        },
    }
    payload = _json_safe(payload)
    payload_build_ms = round((perf_counter() - payload_started) * 1000.0, 3)
    payload_json_bytes = len(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    )
    controls = payload.get("controls")
    if isinstance(controls, dict):
        controls["runtime"] = {
            "payload_build_ms": payload_build_ms,
            "payload_json_bytes": payload_json_bytes,
        }
    return payload


def _build_interactive_chart_payload(
    counts_per_minute: pd.DataFrame,
    volume_changepoints: pd.DataFrame,
    pro_rate_changepoints: pd.DataFrame,
    time_bucket_profiles: pd.DataFrame,
    day_bucket_profiles: pd.DataFrame,
    org_blank_rates: pd.DataFrame,
    voter_match_by_bucket: pd.DataFrame,
) -> dict[str, Any]:
    placeholder_table_map = {
        "artifacts.counts_per_minute": counts_per_minute,
        _table_key("changepoints", "volume_changepoints"): volume_changepoints,
        _table_key("changepoints", "pro_rate_changepoints"): pro_rate_changepoints,
        _table_key("procon_swings", "time_bucket_profiles"): time_bucket_profiles,
        _table_key("procon_swings", "day_bucket_profiles"): day_bucket_profiles,
        _table_key("org_anomalies", "organization_blank_rate_by_bucket"): org_blank_rates,
        _table_key("voter_registry_match", "match_by_bucket"): voter_match_by_bucket,
    }
    return _build_interactive_chart_payload_v2(
        table_map=placeholder_table_map,
        detector_summaries={},
    )


def _interactive_chart_payload_from_results(
    results: dict[str, DetectorResult],
    artifacts: dict[str, pd.DataFrame],
    *,
    default_dedup_mode: str | None = None,
    min_cell_n_for_rates: int = 25,
    hearing_metadata: HearingMetadata | None = None,
) -> dict[str, Any]:
    table_map = _load_table_map_from_results(results=results, artifacts=artifacts)
    detector_summaries = {name: result.summary for name, result in sorted(results.items())}
    return _build_interactive_chart_payload_v2(
        table_map=table_map,
        detector_summaries=detector_summaries,
        default_dedup_mode=default_dedup_mode,
        min_cell_n_for_rates=min_cell_n_for_rates,
        hearing_metadata=hearing_metadata,
    )


def _interactive_chart_payload_from_disk(
    out_dir: Path,
    *,
    default_dedup_mode: str | None = None,
    min_cell_n_for_rates: int = 25,
    hearing_metadata: HearingMetadata | None = None,
) -> dict[str, Any]:
    table_map = _load_table_map_from_disk(out_dir=out_dir)
    detector_summaries = _load_summaries_from_disk(out_dir)
    return _build_interactive_chart_payload_v2(
        table_map=table_map,
        detector_summaries=detector_summaries,
        default_dedup_mode=default_dedup_mode,
        min_cell_n_for_rates=min_cell_n_for_rates,
        hearing_metadata=hearing_metadata,
    )


def _rows_to_frame(rows: Any) -> pd.DataFrame:
    if not isinstance(rows, list) or not rows:
        return pd.DataFrame()
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            normalized_rows.append(_json_safe(row))
    if not normalized_rows:
        return pd.DataFrame()
    frame = pd.DataFrame(normalized_rows)
    for column in frame.columns:
        if frame[column].map(lambda value: isinstance(value, (list, dict))).any():
            frame[column] = frame[column].map(
                lambda value: (
                    json.dumps(_json_safe(value), ensure_ascii=False)
                    if isinstance(value, (list, dict))
                    else value
                )
            )
    return frame


def _write_investigation_artifacts(
    out_dir: Path,
    triage_summary: dict[str, Any],
    data_quality_panel: Any,
) -> None:
    summary_dir = out_dir / "summary"
    tables_dir = out_dir / "tables"
    summary_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)

    summary_payload = _json_safe(triage_summary if isinstance(triage_summary, dict) else {})
    (summary_dir / "investigation_summary.json").write_text(
        json.dumps(summary_payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    raw_vs_dedup_rows = []
    if isinstance(data_quality_panel, dict):
        candidate_rows = data_quality_panel.get("raw_vs_dedup_metrics", [])
        if isinstance(candidate_rows, list):
            raw_vs_dedup_rows = candidate_rows

    queue_table = _rows_to_frame(raw_vs_dedup_rows)
    for table_name, frame in {"data_quality__raw_vs_dedup_metrics": queue_table}.items():
        csv_path = tables_dir / f"{table_name}.csv"
        frame.to_csv(csv_path, index=False)
        if pq is not None:
            parquet_path = tables_dir / f"{table_name}.parquet"
            frame.to_parquet(parquet_path, index=False)


def render_report(
    results: dict[str, DetectorResult],
    artifacts: dict[str, pd.DataFrame],
    out_dir: Path,
    *,
    default_dedup_mode: str = DEFAULT_DEDUP_MODE,
    min_cell_n_for_rates: int = 25,
    hearing_metadata: HearingMetadata | None = None,
) -> Path:
    report_started = perf_counter()
    generated_at = datetime.now(ZoneInfo(PACIFIC_TIMEZONE_NAME)).isoformat()
    generated_at_label = PACIFIC_TIMEZONE_NAME
    env = _template_env()
    template = env.get_template("report.html.j2")

    detector_summaries = (
        {name: result.summary for name, result in sorted(results.items())}
        if results
        else _load_summaries_from_disk(out_dir)
    )
    artifact_rows = (
        {name: len(table) for name, table in sorted(artifacts.items())}
        if artifacts
        else _artifact_rows_from_disk(out_dir)
    )
    table_previews = (
        _table_previews_from_results(results)
        if results
        else _load_table_previews_from_disk(out_dir)
    )
    table_column_docs = _build_table_column_docs(
        table_previews=table_previews,
        artifact_rows=artifact_rows,
    )
    table_help_docs = _build_table_help_docs(table_column_docs=table_column_docs)
    interactive_started = perf_counter()
    interactive_charts = (
        _interactive_chart_payload_from_results(
            results=results,
            artifacts=artifacts,
            default_dedup_mode=default_dedup_mode,
            min_cell_n_for_rates=min_cell_n_for_rates,
            hearing_metadata=hearing_metadata,
        )
        if results
        else _interactive_chart_payload_from_disk(
            out_dir=out_dir,
            default_dedup_mode=default_dedup_mode,
            min_cell_n_for_rates=min_cell_n_for_rates,
            hearing_metadata=hearing_metadata,
        )
    )
    interactive_build_ms = round((perf_counter() - interactive_started) * 1000.0, 3)
    if isinstance(interactive_charts.get("controls"), dict):
        runtime_metrics = interactive_charts["controls"].get("runtime", {})
        if not isinstance(runtime_metrics, dict):
            runtime_metrics = {}
        runtime_metrics["interactive_payload_build_ms"] = interactive_build_ms
        interactive_charts["controls"]["runtime"] = runtime_metrics
    _write_investigation_artifacts(
        out_dir=out_dir,
        triage_summary=interactive_charts.get("triage_summary", {}),
        data_quality_panel=interactive_charts.get("data_quality_panel", {}),
    )

    detector_summaries_safe = _json_safe(detector_summaries)
    artifact_rows_safe = _json_safe(artifact_rows)
    table_previews_safe = _json_safe(table_previews)
    table_column_docs_safe = _json_safe(table_column_docs)
    table_help_docs_safe = _json_safe(table_help_docs)
    interactive_charts_safe = _json_safe(interactive_charts)
    report_data_root = out_dir / REPORT_DATA_DIRECTORY
    if report_data_root.exists():
        shutil.rmtree(report_data_root)
    legacy_report_data_path = out_dir / "report_data.json"
    if legacy_report_data_path.exists():
        legacy_report_data_path.unlink()

    report_data_payload, report_data_shards_json_bytes = _build_report_data_payload(
        out_dir=out_dir,
        artifact_rows_safe=artifact_rows_safe,
        detector_summaries_safe=detector_summaries_safe,
        table_previews_safe=table_previews_safe,
        table_column_docs_safe=table_column_docs_safe,
        table_help_docs_safe=table_help_docs_safe,
        interactive_charts_safe=interactive_charts_safe,
    )
    interactive_charts_for_template = report_data_payload.get("interactive_charts", {})
    report_data_path = out_dir / REPORT_DATA_FILENAME
    report_data_path.parent.mkdir(parents=True, exist_ok=True)
    report_data_json = json.dumps(
        report_data_payload,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    report_data_path.write_text(report_data_json, encoding="utf-8")
    report_assets = _copy_report_static_assets(out_dir)

    template_started = perf_counter()
    rendered = template.render(
        generated_at=generated_at,
        generated_at_label=generated_at_label,
        detector_summaries=detector_summaries_safe,
        artifact_rows=artifact_rows_safe,
        table_previews=table_previews_safe,
        table_column_docs=table_column_docs_safe,
        table_help_docs=table_help_docs_safe,
        interactive_charts=interactive_charts_for_template,
        report_data_url=REPORT_DATA_FILENAME,
        report_assets=report_assets,
        figure_files=sorted(path.name for path in (out_dir / "figures").glob("*")),
    )
    template_render_ms = round((perf_counter() - template_started) * 1000.0, 3)

    report_path = out_dir / "report.html"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    write_started = perf_counter()
    report_path.write_text(rendered, encoding="utf-8")
    report_write_ms = round((perf_counter() - write_started) * 1000.0, 3)

    runtime_metrics = {
        "generated_at": generated_at,
        "interactive_payload_build_ms": interactive_build_ms,
        "template_render_ms": template_render_ms,
        "report_write_ms": report_write_ms,
        "report_total_ms": round((perf_counter() - report_started) * 1000.0, 3),
        "report_html_bytes": int(report_path.stat().st_size),
        "report_data_json_bytes": len(report_data_json.encode("utf-8")),
        "report_data_shards_json_bytes": report_data_shards_json_bytes,
    }
    runtime_path = out_dir / "artifacts" / "report_runtime.json"
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_path.write_text(
        json.dumps(_json_safe(runtime_metrics), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return report_path
