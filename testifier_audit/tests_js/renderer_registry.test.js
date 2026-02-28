import assert from "node:assert/strict";
import test from "node:test";

import { ChartRendererRegistry } from "../src/testifier_audit/report/static/report/modules/charts/renderer_registry.js";

test("ChartRendererRegistry resolves and dispatches handlers", () => {
  const registry = new ChartRendererRegistry();
  registry.register("chart_one", (_mount, rows) => rows.length > 0);
  registry.registerMany(["chart_two", "chart_three"], () => "ok");

  assert.equal(typeof registry.resolve("chart_one"), "function");
  assert.equal(registry.resolve("missing"), null);

  const rendered = registry.render("chart_one", {}, [1], {});
  assert.equal(rendered.handled, true);
  assert.equal(rendered.result, true);

  const missing = registry.render("missing", {}, [], {});
  assert.equal(missing.handled, false);
  assert.equal(missing.result, null);
});
