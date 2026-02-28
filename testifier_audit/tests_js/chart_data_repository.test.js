import assert from "node:assert/strict";
import test from "node:test";

import { ChartDataRepository } from "../src/testifier_audit/report/static/report/modules/data/chart_data_repository.js";
import { toFiniteNumberOrNull } from "../src/testifier_audit/report/static/report/modules/shared/coercion.js";

test("ChartDataRepository loads base and bucket shards with deduped in-flight requests", async () => {
  const calls = [];
  const repo = new ChartDataRepository({
    chartShardManifestByAnalysis: {
      off_hours: {
        base_url: "report_data/analyses/off_hours/base.json",
        bucket_urls: { "30": "report_data/analyses/off_hours/bucket-30m.json" },
        chart_bucket_options: { off_hours_control_timeline: [30] },
      },
    },
    chartBaseRowsMap: {},
    chartBucketRowsMap: new Map(),
    chartBucketOptionsByChart: new Map([["off_hours_control_timeline", [30]]]),
    toFiniteNumberOrNull,
    fetchJsonPayload: async (url) => {
      calls.push(url);
      if (url.endsWith("base.json")) {
        return {
          charts: {
            off_hours_control_timeline: [{ bucket_minutes: null, n_total: 10 }],
          },
        };
      }
      return {
        charts: {
          off_hours_control_timeline: [{ bucket_minutes: 30, n_total: 5 }],
        },
      };
    },
  });

  await Promise.all([
    repo.ensureAnalysisDataLoaded("off_hours", 30, 30),
    repo.ensureAnalysisDataLoaded("off_hours", 30, 30),
  ]);

  assert.equal(calls.length, 2);
  const rows = repo.getChartRows("off_hours_control_timeline", 30, 30);
  assert.equal(rows.length, 2);
  assert.equal(rows[0].n_total, 10);
  assert.equal(rows[1].n_total, 5);
});
