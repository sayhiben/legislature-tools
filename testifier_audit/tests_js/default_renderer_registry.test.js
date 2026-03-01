import assert from "node:assert/strict";
import test from "node:test";

import {
  buildDefaultChartRendererRegistry,
  DEFAULT_CHART_RENDERER_IDS,
  DEFAULT_TIME_BAR_LINE_OVERRIDES,
} from "../src/testifier_audit/report/static/report/modules/charts/default_renderer_registry.js";

function makeDependencies(overrides = {}) {
  const deps = {
    getState: () => ({}),
    normalizeReportMatchMode: (value, fallback) => value || fallback,
    renderDateHourHeatmap: () => true,
    renderDuplicateNamesByPosition: () => true,
    renderDuplicatePositionBucketDeviance: () => true,
    renderDuplicateTopNameTiming: () => true,
    renderEvidenceMatrix: () => true,
    renderOffHoursFunnel: () => true,
    renderOffHoursPrimaryFlagChannels: () => true,
    renderOverviewPositionVolumeByBucket: () => true,
    renderSimpleBar: () => true,
    renderTimeBarLine: () => true,
  };
  return Object.assign(deps, overrides);
}

test("default chart registry resolves all declared chart ids", () => {
  const registry = buildDefaultChartRendererRegistry(makeDependencies());
  DEFAULT_CHART_RENDERER_IDS.forEach((chartId) => {
    assert.equal(typeof registry.resolve(chartId), "function");
  });
});

test("duplicates timing renderer uses store snapshot for mode normalization", () => {
  const calls = {
    normalize: [],
    render: [],
  };
  const registry = buildDefaultChartRendererRegistry(
    makeDependencies({
      getState: () => ({
        activeDuplicateMatchMode: "loose",
        defaultDuplicateMatchMode: "strict",
      }),
      normalizeReportMatchMode: (value, fallback) => {
        calls.normalize.push([value, fallback]);
        return "strict";
      },
      renderDuplicateTopNameTiming: (_mount, _rows, mode) => {
        calls.render.push(mode);
        return "rendered";
      },
    })
  );

  const result = registry.render("duplicates_exact_top_name_timing_exact", {}, [], {});
  assert.equal(result.handled, true);
  assert.equal(result.result, "rendered");
  assert.deepEqual(calls.normalize, [["loose", "strict"]]);
  assert.deepEqual(calls.render, ["strict"]);
});

test("time override chart routes through renderTimeBarLine with expected config", () => {
  const seenConfigs = [];
  const registry = buildDefaultChartRendererRegistry(
    makeDependencies({
      renderTimeBarLine: (_mount, _rows, config) => {
        seenConfigs.push(config);
        return "time-rendered";
      },
    })
  );

  const result = registry.render("off_hours_control_timeline", {}, [{ n_total: 1 }], {});
  assert.equal(result.handled, true);
  assert.equal(result.result, "time-rendered");
  assert.equal(seenConfigs.length, 1);
  assert.deepEqual(seenConfigs[0], DEFAULT_TIME_BAR_LINE_OVERRIDES.off_hours_control_timeline);
});
