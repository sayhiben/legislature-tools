export function filterRowsByDuplicateCollisionControls({
  chartId,
  rows,
  state,
  normalizeReportMatchMode,
}) {
  const subset = Array.isArray(rows) ? rows : [];
  if (!subset.length) {
    return subset;
  }
  const filterByMatchMode = (sourceRows, targetMode, fallbackMode) => {
    const candidateRows = Array.isArray(sourceRows) ? sourceRows : [];
    if (!candidateRows.length) {
      return candidateRows;
    }
    const hasMode = candidateRows.some((row) =>
      Object.prototype.hasOwnProperty.call(row || {}, "match_mode")
    );
    if (!hasMode) {
      return candidateRows;
    }
    const normalizedTarget = normalizeReportMatchMode(targetMode, fallbackMode);
    return candidateRows.filter(
      (row) => normalizeReportMatchMode((row || {}).match_mode, fallbackMode) === normalizedTarget
    );
  };

  if (chartId === "duplicates_exact_bucket_concentration") {
    const scoped = subset.filter((row) => String(row.scope || "") === String(state.activeDuplicateScope || ""));
    const scopedFallback = scoped.length ? scoped : subset;
    const modeFiltered = filterByMatchMode(
      scopedFallback,
      state.activeDuplicateMatchMode,
      state.defaultDuplicateMatchMode || "strict"
    );
    const modeFallback = modeFiltered.length ? modeFiltered : scopedFallback;
    const metered = modeFallback.filter(
      (row) => String(row.metric || "") === String(state.activeDuplicateMetric || "")
    );
    return metered.length ? metered : modeFallback;
  }

  if (chartId === "duplicates_exact_metric_diagnostics") {
    const scoped = subset.filter((row) => String(row.scope || "") === String(state.activeDuplicateScope || ""));
    return scoped.length ? scoped : subset;
  }

  if (chartId === "duplicates_exact_position_bucket_deviance") {
    const scoped = subset.filter((row) => {
      const scope = String((row || {}).scope || "").trim();
      return !scope || scope === String(state.activeDuplicateScope || "");
    });
    return scoped.length ? scoped : subset;
  }

  if (chartId === "duplicates_exact_top_name_timing_exact") {
    const scoped = subset.filter((row) => String(row.scope || "") === String(state.activeDuplicateScope || ""));
    const scopedFallback = scoped.length ? scoped : subset;
    return filterByMatchMode(
      scopedFallback,
      state.activeDuplicateMatchMode,
      state.defaultDuplicateMatchMode || "strict"
    );
  }

  if (
    chartId === "duplicates_exact_per_name_anomalies" ||
    chartId === "duplicates_exact_top_names" ||
    chartId === "duplicates_exact_position_switch"
  ) {
    const scoped = subset.filter((row) => {
      const scope = String((row || {}).scope || "").trim();
      return !scope || scope === String(state.activeDuplicateScope || "");
    });
    const scopedFallback = scoped.length ? scoped : subset;
    return filterByMatchMode(
      scopedFallback,
      state.activeDuplicateMatchMode,
      state.defaultDuplicateMatchMode || "strict"
    );
  }

  const voterModeChartIds = new Set([
    "voter_registry_match_rates",
    "voter_registry_linkage_by_position_rows",
    "voter_registry_linkage_by_position_unique",
    "voter_registry_pairwise_tests",
    "voter_registry_unmatched_names",
    "voter_registry_position_bounds",
    "voter_registry_position_buckets",
    "voter_registry_match_by_position",
    "voter_registry_match_tiers",
  ]);

  if (voterModeChartIds.has(chartId)) {
    return filterByMatchMode(subset, state.activeVoterMatchMode, state.defaultVoterMatchMode || "loose");
  }

  return subset;
}
