import assert from "node:assert/strict";
import test from "node:test";

import {
  parsePixelLikeValue,
  toBool,
  toEpochMillis,
  toFiniteNumberOrNull,
  toNumber,
} from "../src/testifier_audit/report/static/report/modules/shared/coercion.js";
import {
  formatDurationHumanized,
  formatPercent,
  formatRatio,
} from "../src/testifier_audit/report/static/report/modules/shared/formatting.js";

test("toNumber coerces numeric strings and handles non-finite values", () => {
  assert.equal(toNumber(4), 4);
  assert.equal(toNumber("5"), 5);
  assert.equal(toNumber("x"), 0);
  assert.equal(toNumber(null), 0);
});

test("toFiniteNumberOrNull and toBool normalize primitive inputs", () => {
  assert.equal(toFiniteNumberOrNull("7"), 7);
  assert.equal(toFiniteNumberOrNull(false), 0);
  assert.equal(toFiniteNumberOrNull("abc"), null);
  assert.equal(toBool("YES"), true);
  assert.equal(toBool("0"), false);
});

test("date and pixel coercion helpers return null for unsupported values", () => {
  const epoch = toEpochMillis("2026-02-01T00:00:00Z");
  assert.equal(Number.isFinite(epoch), true);
  assert.equal(toEpochMillis(""), null);
  assert.equal(parsePixelLikeValue("12px"), 12);
  assert.equal(parsePixelLikeValue("40%"), null);
});

test("formatting helpers preserve public report semantics", () => {
  assert.equal(formatPercent(0.1234, 1), "12.3%");
  assert.equal(formatPercent(null, 1), "-");
  assert.equal(formatRatio(5, 10, 0), "50%");
  assert.equal(formatRatio(1, 0, 1), "-");
  assert.equal(formatDurationHumanized(61 * 60 * 1000), "1h 1m");
});
