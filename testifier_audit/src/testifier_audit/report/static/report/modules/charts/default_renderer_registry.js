import { ChartRendererRegistry } from "./renderer_registry.js";

function requiredFunction(name, value) {
  if (typeof value !== "function") {
    throw new Error("buildDefaultChartRendererRegistry requires function dependency: " + name);
  }
  return value;
}

function normalizePositionRows(rows) {
  return rows.map((row) => {
    const position = String((row || {}).position_normalized || "").trim();
    const normalized = position.toLowerCase();
    return Object.assign({}, row, {
      position_normalized:
        !position || normalized === "unknown" || normalized === "other" ? "Other" : position,
    });
  });
}

export const DEFAULT_TIME_BAR_LINE_OVERRIDES = Object.freeze({
  off_hours_control_timeline: {
    timeField: "bucket_start",
    barField: "n_total",
    lineField: "pro_rate",
    lineLow: "pro_rate_wilson_low",
    lineHigh: "pro_rate_wilson_high",
    lowPowerField: "is_low_power",
    flaggedField: "is_primary_two_sided_alert_window",
    inferentialWindowField: "is_alert_off_hours_window",
    sparseWhenLowSupport: true,
    sparseMinTestedPoints: 8,
    sparseMinTestedShare: 0.35,
    runOverlayField: "is_primary_two_sided_alert_window",
    extraLines: [
      "expected_pro_rate_primary",
      "control_low_95_primary",
      "control_high_95_primary",
      "expected_pro_rate_global",
    ],
    lineAxisName: "Pro rate",
    lineMin: 0,
    lineMax: 1,
  },
  off_hours_primary_residual_timeline: {
    timeField: "bucket_start",
    barField: "n_known",
    lineField: "z_score_primary",
    lowPowerField: "is_low_power",
    flaggedField: "is_primary_two_sided_alert_window",
    inferentialWindowField: "is_alert_off_hours_window",
    sparseWhenLowSupport: true,
    sparseMinTestedPoints: 8,
    sparseMinTestedShare: 0.35,
    runOverlayField: "is_primary_two_sided_alert_window",
    extraLines: ["z_score_day", "z_ref_zero", "z_ref_pos3", "z_ref_neg3"],
    barAxisName: "Known pro+con",
    lineAxisName: "Primary z-score",
  },
  org_anomalies_blank_rate: {
    timeField: "bucket_start",
    barField: null,
    stackedBarFields: [
      { field: "n_pro", name: "Pro volume", color: "contextLine", opacity: 0.44 },
      { field: "n_con", name: "Con volume", color: "alertLower", opacity: 0.44 },
    ],
    lineField: "blank_org_rate",
    lineLow: "blank_org_rate_wilson_low",
    lineHigh: "blank_org_rate_wilson_high",
    extraLines: ["pro_blank_org_rate", "con_blank_org_rate"],
    lowPowerField: "is_low_power",
    barAxisName: "Positioned rows",
    lineAxisName: "Blank org rate",
    lineMin: 0,
    lineMax: 1,
  },
  voter_registry_match_rates: {
    timeField: "bucket_start",
    barField: "n_total",
    lineField: "matched_rate",
    lineLow: "match_rate_wilson_low",
    lineHigh: "match_rate_wilson_high",
    adaptiveLineRange: true,
    adaptiveLineRangePadding: 0.12,
    adaptiveLineRangeMinSpan: 0.08,
    adaptiveLineRangeClampMin: 0,
    adaptiveLineRangeClampMax: 1,
    extraLines: [
      "matched_rate_pro",
      "matched_rate_con",
      "expected_match_rate_global",
      "control_low_95_match_global",
      "control_high_95_match_global",
    ],
    flaggedField: "is_match_rate_alert_any",
    lowPowerField: "is_low_power",
    lineAxisName: "Matched rate",
    lineMin: 0,
    lineMax: 1,
  },
  duplicates_exact_bucket_concentration: {
    timeField: "bucket_start",
    barField: null,
    stackedBarFields: [
      { field: "n_pro", name: "Pro volume", color: "contextLine", opacity: 0.44 },
      { field: "n_con", name: "Con volume", color: "alertLower", opacity: 0.44 },
    ],
    lineField: "duplicate_rows",
    lineSeriesName: "Observed (selected unit)",
    extraLines: [{ field: "expected_duplicate_rows", name: "Expected (report baseline)" }],
    lineAxisName: "Collision count",
    barAxisName: "Positioned rows",
  },
  vrdb_collision_evidence_pairs: {
    timeField: "bucket_start",
    barField: "n_rows",
    lineField: "observed_pairs",
    lineSeriesName: "Observed pair collisions",
    extraLines: [
      { field: "expected_pairs_mean", name: "Expected pairs (mean)" },
      { field: "expected_pairs_p95", name: "Expected pairs (p95)" },
      { field: "expected_pairs_p99", name: "Expected pairs (p99)" },
    ],
    lineAxisName: "Pair collisions",
    barAxisName: "Rows in bucket",
  },
  vrdb_collision_evidence_max_name_count: {
    timeField: "bucket_start",
    barField: "n_rows",
    lineField: "observed_max_name_count",
    lineSeriesName: "Observed max repeated-name count",
    extraLines: [
      { field: "expected_max_name_count_mean", name: "Expected max count (mean)" },
      { field: "expected_max_name_count_p95", name: "Expected max count (p95)" },
      { field: "expected_max_name_count_p99", name: "Expected max count (p99)" },
    ],
    lineAxisName: "Max repeated-name count",
    barAxisName: "Rows in bucket",
  },
  org_anomalies_position_rates: {
    timeField: "bucket_start",
    barField: null,
    stackedBarFields: [
      { field: "n_pro", name: "Pro volume", color: "contextLine", opacity: 0.44 },
      { field: "n_con", name: "Con volume", color: "alertLower", opacity: 0.44 },
    ],
    lineField: "pro_blank_org_rate",
    lineLow: "pro_blank_org_rate_wilson_low",
    lineHigh: "pro_blank_org_rate_wilson_high",
    lineSeriesName: "Pro blank org rate",
    extraLines: [{ field: "con_blank_org_rate", name: "Con blank org rate" }],
    extraLinesBeforeBounds: true,
    lowPowerField: "pro_is_low_power",
    barAxisName: "Positioned rows",
    lineAxisName: "Blank org rate by position",
    lineMin: 0,
    lineMax: 1,
  },
  voter_registry_position_buckets: {
    timeField: "bucket_start",
    barField: "n_total",
    lineField: "matched_rate",
    lineLow: "match_rate_wilson_low",
    lineHigh: "match_rate_wilson_high",
    extraLines: ["unmatched_rate"],
    lowPowerField: "is_low_power",
    lineAxisName: "Matched rate",
    lineMin: 0,
    lineMax: 1,
  },
  bursts_hero_timeline: {
    timeField: "start_minute",
    barField: "observed_count",
    lineField: "rate_ratio",
    barAxisLines: ["expected_count", "excess_count"],
    lineAxisName: "Rate ratio",
    barAxisName: "Observed count",
  },
  bursts_significance_by_window: {
    timeField: "start_minute",
    barField: "duration_minutes",
    lineField: "excess_count",
    extraLines: ["dominant_impact_count"],
    lowPowerField: "is_low_power",
    lineAxisName: "Excess submissions",
    barAxisName: "Burst duration (minutes)",
  },
  bursts_composition_shift: {
    timeField: "start_minute",
    barField: "dominant_impact_count",
    lineField: "net_position_impact",
    extraLines: ["pro_impact_count", "con_impact_count"],
    lowPowerField: "is_low_power",
    lineAxisName: "Net position impact (Pro minus Con)",
    barAxisName: "Dominant position impact",
  },
});

const BASE_CHART_RENDERER_IDS = Object.freeze([
  "off_hours_date_hour_pro_heatmap",
  "off_hours_date_hour_primary_residual_heatmap",
  "off_hours_date_hour_volume_heatmap",
  "off_hours_funnel_plot",
  "off_hours_primary_flag_channels",
  "overview_position_volume_by_bucket",
  "duplicates_exact_position_bucket_deviance",
  "off_hours_hourly_profile",
  "off_hours_summary_compare",
  "bursts_null_distribution",
  "duplicates_exact_top_names",
  "duplicates_exact_per_name_anomalies",
  "duplicates_exact_position_switch",
  "duplicates_exact_top_name_timing_exact",
  "duplicates_exact_metric_diagnostics",
  "duplicates_exact_null_distribution",
  "duplicates_exact_swing_impact",
  "vrdb_collision_evidence_overrun_names",
  "voter_registry_match_by_position",
  "voter_registry_linkage_by_position_rows",
  "voter_registry_linkage_by_position_unique",
  "voter_registry_pairwise_tests",
  "voter_registry_sensitivity_modes",
  "voter_registry_unmatched_names",
  "voter_registry_position_bounds",
  "voter_registry_match_tiers",
]);

export const DEFAULT_CHART_RENDERER_IDS = Object.freeze(
  Array.from(
    new Set(BASE_CHART_RENDERER_IDS.concat(Object.keys(DEFAULT_TIME_BAR_LINE_OVERRIDES)))
  ).sort()
);

export function buildDefaultChartRendererRegistry(deps) {
  const config = deps && typeof deps === "object" ? deps : {};
  const getState = typeof config.getState === "function" ? config.getState : () => ({});
  const normalizeReportMatchMode = requiredFunction(
    "normalizeReportMatchMode",
    config.normalizeReportMatchMode
  );
  const renderDateHourHeatmap = requiredFunction(
    "renderDateHourHeatmap",
    config.renderDateHourHeatmap
  );
  const renderDuplicateNamesByPosition = requiredFunction(
    "renderDuplicateNamesByPosition",
    config.renderDuplicateNamesByPosition
  );
  const renderDuplicatePositionBucketDeviance = requiredFunction(
    "renderDuplicatePositionBucketDeviance",
    config.renderDuplicatePositionBucketDeviance
  );
  const renderDuplicateTopNameTiming = requiredFunction(
    "renderDuplicateTopNameTiming",
    config.renderDuplicateTopNameTiming
  );
  const renderOffHoursFunnel = requiredFunction(
    "renderOffHoursFunnel",
    config.renderOffHoursFunnel
  );
  const renderOffHoursPrimaryFlagChannels = requiredFunction(
    "renderOffHoursPrimaryFlagChannels",
    config.renderOffHoursPrimaryFlagChannels
  );
  const renderOverviewPositionVolumeByBucket = requiredFunction(
    "renderOverviewPositionVolumeByBucket",
    config.renderOverviewPositionVolumeByBucket
  );
  const renderSimpleBar = requiredFunction("renderSimpleBar", config.renderSimpleBar);
  const renderTimeBarLine = requiredFunction("renderTimeBarLine", config.renderTimeBarLine);

  const registry = new ChartRendererRegistry();

  registry.register("off_hours_date_hour_pro_heatmap", (mount, rows) =>
    renderDateHourHeatmap(mount, rows, "pro_rate", "Pro rate", {
      scaleMode: "rate_diverging",
      force24HourSlots: true,
    })
  );
  registry.register("off_hours_date_hour_primary_residual_heatmap", (mount, rows) =>
    renderDateHourHeatmap(mount, rows, "z_score_primary", "Primary z-score", {
      scaleMode: "diverging",
      divergingPositiveWarm: true,
      force24HourSlots: true,
      highlightOffHoursAxis: true,
      offHoursAxisThreshold: 0.5,
    })
  );
  registry.register("off_hours_date_hour_volume_heatmap", (mount, rows) =>
    renderDateHourHeatmap(mount, rows, "n_total", "Submission count", {
      scaleMode: "volume",
      force24HourSlots: true,
      showMissingOverlay: true,
    })
  );
  registry.register("off_hours_funnel_plot", (mount, rows) =>
    renderOffHoursFunnel(mount, rows)
  );
  registry.register("off_hours_primary_flag_channels", (mount, rows) =>
    renderOffHoursPrimaryFlagChannels(mount, rows)
  );
  registry.register("overview_position_volume_by_bucket", (mount, rows) =>
    renderOverviewPositionVolumeByBucket(mount, rows)
  );
  registry.register("duplicates_exact_position_bucket_deviance", (mount, rows) =>
    renderDuplicatePositionBucketDeviance(mount, rows)
  );
  registry.register("off_hours_hourly_profile", (mount, rows) =>
    renderSimpleBar(mount, rows, "hour", "n_total", "submissions")
  );
  registry.register("off_hours_summary_compare", (mount, rows) =>
    renderSimpleBar(mount, rows, "off_hours", "off_hours_pro_rate", "pro rate", {
      observedSeriesName: "Off-hours pro rate",
      expectedField: "on_hours_pro_rate",
      expectedSeriesName: "On-hours pro rate",
    })
  );
  registry.register("bursts_null_distribution", (mount, rows) =>
    renderSimpleBar(mount, rows, "iteration", "max_window_count", "max count")
  );
  registry.registerMany(
    [
      "duplicates_exact_top_names",
      "duplicates_exact_per_name_anomalies",
      "duplicates_exact_position_switch",
    ],
    (mount, rows) => renderDuplicateNamesByPosition(mount, rows)
  );
  registry.register("duplicates_exact_top_name_timing_exact", (mount, rows) => {
    const stateSnapshot = getState() || {};
    return renderDuplicateTopNameTiming(
      mount,
      rows,
      normalizeReportMatchMode(
        stateSnapshot.activeDuplicateMatchMode,
        stateSnapshot.defaultDuplicateMatchMode || "strict"
      )
    );
  });
  registry.register("duplicates_exact_metric_diagnostics", (mount, rows) =>
    renderSimpleBar(mount, rows, "metric", "observed", "observed", {
      observedSeriesName: "Observed",
      expectedField: "expected",
      expectedSeriesName: "Expected",
      bandLowField: "expected_p05",
      bandHighField: "expected_p95",
      expectedBandName: "Expected p05-p95",
    })
  );
  registry.register("duplicates_exact_null_distribution", (mount, rows) =>
    renderSimpleBar(mount, rows, "iteration", "duplicate_rows", "duplicate rows")
  );
  registry.register("duplicates_exact_swing_impact", (mount, rows) =>
    renderSimpleBar(mount, rows, "scenario", "pro_share", "pro share", {
      observedSeriesName: "Scenario pro share",
    })
  );
  registry.register("vrdb_collision_evidence_overrun_names", (mount, rows) =>
    renderSimpleBar(mount, rows, "name_key", "observed_count", "observed count", {
      expectedField: "expected_count",
      expectedSeriesName: "Expected count",
      pageStateKey: "vrdbCollisionOverrunNamesPage",
      pageSize: 10,
      maxPages: 10,
      pageLabel: "Names",
    })
  );
  registry.register("voter_registry_match_by_position", (mount, rows) =>
    renderSimpleBar(mount, rows, "position_normalized", "match_rate", "match rate", {
      expectedField: "expected_match_rate_global",
      expectedSeriesName: "Expected global match rate",
    })
  );
  registry.registerMany(
    ["voter_registry_linkage_by_position_rows", "voter_registry_linkage_by_position_unique"],
    (mount, rows) =>
      renderSimpleBar(
        mount,
        normalizePositionRows(rows),
        "position_normalized",
        "unmatched_rate",
        "unmatched rate",
        {
          expectedField: "expected_unmatched_rate_global",
          expectedSeriesName: "Expected global unmatched rate",
        }
      )
  );
  registry.register("voter_registry_pairwise_tests", (mount, rows) =>
    renderSimpleBar(mount, rows, "pair_label", "rate_difference", "rate difference")
  );
  registry.register("voter_registry_sensitivity_modes", (mount, rows) =>
    renderSimpleBar(mount, rows, "mode", "unmatched_rate_rows", "unmatched rate")
  );
  registry.register("voter_registry_unmatched_names", (mount, rows) =>
    renderSimpleBar(mount, rows, "display_name", "n_records", "count", {
      pageStateKey: "voterUnmatchedNamesPage",
      pageSize: 10,
      maxPages: 10,
      pageLabel: "Names",
    })
  );
  registry.register("voter_registry_position_bounds", (mount, rows) =>
    renderSimpleBar(mount, rows, "position_normalized", "matched_rate_span", "matched-rate span")
  );
  registry.register("voter_registry_match_tiers", (mount, rows) => {
    const yField = rows.length && rows[0].record_rate !== undefined
      ? "record_rate"
      : "unmatched_rate_rows";
    const xField = rows.length && rows[0].match_tier !== undefined ? "match_tier" : "mode";
    return renderSimpleBar(mount, rows, xField, yField, "record rate");
  });

  Object.keys(DEFAULT_TIME_BAR_LINE_OVERRIDES).forEach((chartId) => {
    const timeBarLineConfig = DEFAULT_TIME_BAR_LINE_OVERRIDES[chartId];
    registry.register(chartId, (mount, rows) =>
      renderTimeBarLine(mount, rows, timeBarLineConfig)
    );
  });

  return registry;
}
