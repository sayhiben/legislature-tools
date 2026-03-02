import { toBool, toFiniteNumberOrNull } from "../shared/coercion.js";

export const TABLE_SEMANTIC_CLASS_NAMES = [
  "table-cell-semantic-alert",
  "table-cell-semantic-warn",
  "table-cell-semantic-context",
];

const offHoursSummaryAlertColumns = new Set([
  "off_hours_windows_primary_alert",
  "off_hours_windows_significant_primary",
  "off_hours_windows_significant_primary_two_sided",
  "off_hours_windows_below_primary_control_998",
  "off_hours_windows_above_primary_control_998",
  "off_hours_windows_primary_flag_any",
  "off_hours_windows_primary_flag_any_fraction",
  "off_hours_windows_primary_flag_both",
  "off_hours_windows_primary_flag_both_fraction",
]);

const offHoursSummaryLowPowerColumns = new Set([
  "off_hours_windows_alert_eligible_low_power",
  "off_hours_windows_alert_eligible_low_power_fraction",
  "off_hours_is_low_power",
  "on_hours_is_low_power",
]);

const offHoursSummaryContextColumns = new Set([
  "off_hours_windows_model_available",
  "off_hours_windows_alert_eligible_tested_fraction",
]);

export function semanticClassForTableCell(tableKey, field, value) {
  const key = String(tableKey || "").trim();
  const column = String(field || "").trim();
  if (!key || !column) {
    return "";
  }

  const numeric = toFiniteNumberOrNull(value);
  const truthy = toBool(value);
  const valueText = String(value === null || value === undefined ? "" : value)
    .trim()
    .toLowerCase();

  if (key === "cross_hearing_baseline.metric_comparators") {
    if (column === "support_tier") {
      if (valueText === "supported") {
        return "table-cell-semantic-context";
      }
      if (valueText === "descriptive_only") {
        return "table-cell-semantic-warn";
      }
      return "table-cell-semantic-alert";
    }
    if (column === "descriptive_only" || column === "low_power") {
      return truthy ? "table-cell-semantic-warn" : "";
    }
    if (column === "empirical_tail_p_two_sided" && numeric !== null && numeric <= 0.05) {
      return "table-cell-semantic-context";
    }
  }

  if (key === "off_hours.off_hours_summary") {
    if (offHoursSummaryAlertColumns.has(column)) {
      if (numeric !== null) {
        return numeric > 0 ? "table-cell-semantic-alert" : "";
      }
      return truthy ? "table-cell-semantic-alert" : "";
    }
    if (offHoursSummaryLowPowerColumns.has(column)) {
      if (numeric !== null) {
        return numeric > 0 ? "table-cell-semantic-warn" : "";
      }
      return truthy ? "table-cell-semantic-warn" : "";
    }
    if (offHoursSummaryContextColumns.has(column)) {
      if (numeric !== null) {
        return numeric > 0 ? "table-cell-semantic-context" : "";
      }
      if (!valueText) {
        return "";
      }
      if (valueText.includes("unavailable")) {
        return "table-cell-semantic-alert";
      }
      return valueText === "available" || valueText === "true" ? "table-cell-semantic-context" : "";
    }
    if (column === "primary_baseline_method" || column === "primary_model_fit_method") {
      if (!valueText) {
        return "";
      }
      if (valueText.includes("unavailable") || valueText.includes("failure") || valueText.includes("none")) {
        return "table-cell-semantic-alert";
      }
      return "";
    }
    if (column === "primary_model_fit_converged") {
      if (numeric === null) {
        return "table-cell-semantic-warn";
      }
      return numeric >= 1 ? "table-cell-semantic-context" : "table-cell-semantic-alert";
    }
  }

  if (key === "off_hours.model_fit_diagnostics") {
    if (column === "model_fit_available_fraction") {
      if (numeric === null) {
        return "table-cell-semantic-warn";
      }
      if (numeric >= 0.8) {
        return "table-cell-semantic-context";
      }
      if (numeric >= 0.4) {
        return "table-cell-semantic-warn";
      }
      return "table-cell-semantic-alert";
    }
    if (column === "model_fit_converged") {
      if (numeric === null) {
        return "table-cell-semantic-warn";
      }
      return numeric >= 1 ? "table-cell-semantic-context" : "table-cell-semantic-alert";
    }
    if (column === "model_fit_method") {
      if (!valueText) {
        return "";
      }
      if (valueText.includes("unavailable") || valueText.includes("failure")) {
        return "table-cell-semantic-alert";
      }
      return "";
    }
  }

  return "";
}
