import assert from "node:assert/strict";
import test from "node:test";

import {
  parseBucketFromLocalStorage,
  parseBucketFromQueryParams,
  parseLinkedZoomFromQueryParams,
  parseOptionFromQueryParams,
  syncControlOverridesToUrl,
} from "../src/testifier_audit/report/static/report/modules/adapters/url_state.js";
import { toFiniteNumberOrNull } from "../src/testifier_audit/report/static/report/modules/shared/coercion.js";

function buildWindow(search = "") {
  const url = new URL(`http://example.test/${search ? "?" + search : ""}`);
  const historyCalls = [];
  return {
    URL,
    URLSearchParams,
    location: {
      href: url.href,
      pathname: url.pathname,
      search: url.search,
      hash: url.hash,
    },
    history: {
      state: null,
      replaceState: (_state, _title, nextUrl) => historyCalls.push(nextUrl),
    },
    localStorage: {
      _map: new Map(),
      getItem(key) {
        return this._map.has(key) ? this._map.get(key) : null;
      },
      setItem(key, value) {
        this._map.set(key, String(value));
      },
    },
    historyCalls,
  };
}

test("query and localStorage bucket parsing keep option guards", () => {
  const windowObj = buildWindow("bucket=30");
  windowObj.localStorage.setItem("bucket_key", "15");

  assert.equal(parseBucketFromQueryParams([15, 30], toFiniteNumberOrNull, windowObj), 30);
  assert.equal(
    parseBucketFromLocalStorage("bucket_key", [15, 30], toFiniteNumberOrNull, windowObj),
    15
  );
});

test("zoom and option query parsing normalize expected formats", () => {
  const windowObj = buildWindow("zoom_start=1700000000000&zoom_end=1700003600000&dup_scope=full_hearing");
  const zoom = parseLinkedZoomFromQueryParams(windowObj);
  assert.equal(zoom.min, 1700000000000);
  assert.equal(zoom.max, 1700003600000);
  assert.equal(parseOptionFromQueryParams(["dup_scope"], ["full_hearing"], windowObj), "full_hearing");
});

test("syncControlOverridesToUrl writes only non-default overrides", () => {
  const windowObj = buildWindow("");
  const state = {
    activeBucket: 60,
    defaultBucket: 30,
    activeDuplicateScope: "window",
    defaultDuplicateScope: "full_hearing",
    activeDuplicateMetric: "rows_anywhere",
    defaultDuplicateMetric: "rows_anywhere",
    activeDuplicateMatchMode: "loose",
    defaultDuplicateMatchMode: "strict",
    activeVoterMatchMode: "strict",
    defaultVoterMatchMode: "loose",
    zoom: { minTime: 1, maxTime: 2 },
  };

  syncControlOverridesToUrl(state, windowObj);
  assert.equal(windowObj.historyCalls.length, 1);
  assert.match(windowObj.historyCalls[0], /bucket=60/);
  assert.match(windowObj.historyCalls[0], /dup_scope=window/);
  assert.match(windowObj.historyCalls[0], /dup_match_mode=loose/);
  assert.match(windowObj.historyCalls[0], /voter_match_mode=strict/);
  assert.match(windowObj.historyCalls[0], /zoom_start=1/);
  assert.match(windowObj.historyCalls[0], /zoom_end=2/);
});
