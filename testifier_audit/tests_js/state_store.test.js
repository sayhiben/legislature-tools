import assert from "node:assert/strict";
import test from "node:test";

import { ReportStateStore } from "../src/testifier_audit/report/static/report/modules/state/report_state_store.js";

test("ReportStateStore action methods mutate expected state fields", () => {
  const store = new ReportStateStore({
    activeBucket: null,
    activeDuplicateScope: "full_hearing",
    activeDuplicateMetric: "rows_anywhere",
    activeDuplicateMatchMode: "strict",
    activeVoterMatchMode: "loose",
    activeTocHeading: null,
    zoom: { minTime: null, maxTime: null, syncing: false, raf: null, pending: null },
  });

  store.setBucket(30);
  store.setDuplicateFilters({ scope: "window", metric: "names_anywhere", matchMode: "loose" });
  store.setVoterMatchMode("strict");
  store.setTocHeading("section-overview");
  store.setZoomRange(1000, 2000);

  assert.equal(store.activeBucket, 30);
  assert.equal(store.activeDuplicateScope, "window");
  assert.equal(store.activeDuplicateMetric, "names_anywhere");
  assert.equal(store.activeDuplicateMatchMode, "loose");
  assert.equal(store.activeVoterMatchMode, "strict");
  assert.equal(store.activeTocHeading, "section-overview");
  assert.equal(store.zoom.minTime, 1000);
  assert.equal(store.zoom.maxTime, 2000);

  store.resetZoom();
  assert.equal(store.zoom.minTime, null);
  assert.equal(store.zoom.maxTime, null);
});
