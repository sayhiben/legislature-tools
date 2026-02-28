export function firstQueryParam(searchParams, names) {
  for (const name of names) {
    const value = searchParams.get(name);
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

export function parseZoomEpoch(value) {
  if (value === null || value === undefined) {
    return null;
  }
  const text = String(value).trim();
  if (!text) {
    return null;
  }

  const numeric = Number(text);
  if (Number.isFinite(numeric)) {
    const absolute = Math.abs(numeric);
    if (absolute >= 1e12) {
      return numeric;
    }
    if (absolute >= 1e9) {
      return numeric * 1000;
    }
    return null;
  }

  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? parsed : null;
}

export function parseBucketFromQueryParams(availableOptions, toFiniteNumberOrNull, windowObj = window) {
  if (!windowObj || typeof windowObj.URLSearchParams === "undefined") {
    return null;
  }
  const params = new windowObj.URLSearchParams(windowObj.location.search || "");
  const raw = firstQueryParam(params, ["bucket", "bucket_minutes", "linked_bucket_minutes"]);
  const parsed = toFiniteNumberOrNull(raw);
  if (parsed === null) {
    return null;
  }
  const rounded = Math.max(1, Math.round(parsed));
  if (!Array.isArray(availableOptions) || !availableOptions.length) {
    return rounded;
  }
  return availableOptions.includes(rounded) ? rounded : null;
}

export function parseBucketFromLocalStorage(
  storageKey,
  availableOptions,
  toFiniteNumberOrNull,
  windowObj = window
) {
  if (!windowObj || !windowObj.localStorage) {
    return null;
  }
  let raw = "";
  try {
    raw = String(windowObj.localStorage.getItem(storageKey) || "").trim();
  } catch (_error) {
    return null;
  }
  if (!raw) {
    return null;
  }
  const parsed = toFiniteNumberOrNull(raw);
  if (parsed === null) {
    return null;
  }
  const rounded = Math.max(1, Math.round(parsed));
  if (!Array.isArray(availableOptions) || !availableOptions.length) {
    return rounded;
  }
  return availableOptions.includes(rounded) ? rounded : null;
}

export function parseLinkedZoomFromQueryParams(windowObj = window) {
  if (!windowObj || typeof windowObj.URLSearchParams === "undefined") {
    return null;
  }
  const params = new windowObj.URLSearchParams(windowObj.location.search || "");
  const startRaw = firstQueryParam(params, [
    "zoom_start",
    "linked_zoom_start",
    "zoom_min",
    "zoom_min_time",
    "zoomStart",
  ]);
  const endRaw = firstQueryParam(params, [
    "zoom_end",
    "linked_zoom_end",
    "zoom_max",
    "zoom_max_time",
    "zoomEnd",
  ]);
  if (!startRaw || !endRaw) {
    return null;
  }

  const min = parseZoomEpoch(startRaw);
  const max = parseZoomEpoch(endRaw);
  if (!Number.isFinite(min) || !Number.isFinite(max) || max <= min) {
    return null;
  }
  return { min: min, max: max };
}

export function parseOptionFromQueryParams(names, options, windowObj = window) {
  if (!windowObj || typeof windowObj.URLSearchParams === "undefined") {
    return null;
  }
  if (!Array.isArray(options) || !options.length) {
    return null;
  }
  const params = new windowObj.URLSearchParams(windowObj.location.search || "");
  const raw = firstQueryParam(params, names);
  if (!raw) {
    return null;
  }
  return options.includes(raw) ? raw : null;
}

export function updateUrlQueryParams(mutator, windowObj = window) {
  if (
    !windowObj ||
    !windowObj.URL ||
    typeof windowObj.URLSearchParams === "undefined" ||
    !windowObj.history ||
    typeof windowObj.history.replaceState !== "function"
  ) {
    return;
  }
  const currentUrl = new windowObj.URL(windowObj.location.href);
  const params = new windowObj.URLSearchParams(currentUrl.search || "");
  mutator(params);
  const nextSearch = params.toString();
  const nextUrl =
    currentUrl.pathname +
    (nextSearch ? "?" + nextSearch : "") +
    (currentUrl.hash || "");
  const currentPath =
    windowObj.location.pathname + windowObj.location.search + windowObj.location.hash;
  if (nextUrl !== currentPath) {
    windowObj.history.replaceState(windowObj.history.state || null, "", nextUrl);
  }
}

export function syncControlOverridesToUrl(state, windowObj = window) {
  updateUrlQueryParams((params) => {
    ["bucket", "bucket_minutes", "linked_bucket_minutes"].forEach((key) => params.delete(key));
    ["dup_scope", "duplicate_scope"].forEach((key) => params.delete(key));
    ["dup_metric", "duplicate_metric"].forEach((key) => params.delete(key));
    ["dup_match_mode", "duplicate_match_mode"].forEach((key) => params.delete(key));
    ["voter_match_mode", "voter_mode"].forEach((key) => params.delete(key));
    if (
      Number.isFinite(state.activeBucket) &&
      Number.isFinite(state.defaultBucket) &&
      state.activeBucket !== state.defaultBucket
    ) {
      params.set("bucket", String(Math.round(state.activeBucket)));
    }
    if (
      typeof state.activeDuplicateScope === "string" &&
      state.activeDuplicateScope &&
      state.activeDuplicateScope !== state.defaultDuplicateScope
    ) {
      params.set("dup_scope", state.activeDuplicateScope);
    }
    if (
      typeof state.activeDuplicateMetric === "string" &&
      state.activeDuplicateMetric &&
      state.activeDuplicateMetric !== state.defaultDuplicateMetric
    ) {
      params.set("dup_metric", state.activeDuplicateMetric);
    }
    if (
      typeof state.activeDuplicateMatchMode === "string" &&
      state.activeDuplicateMatchMode &&
      state.activeDuplicateMatchMode !== state.defaultDuplicateMatchMode
    ) {
      params.set("dup_match_mode", state.activeDuplicateMatchMode);
    }
    if (
      typeof state.activeVoterMatchMode === "string" &&
      state.activeVoterMatchMode &&
      state.activeVoterMatchMode !== state.defaultVoterMatchMode
    ) {
      params.set("voter_match_mode", state.activeVoterMatchMode);
    }

    ["zoom_start", "linked_zoom_start", "zoom_min", "zoom_min_time", "zoomStart"].forEach(
      (key) => params.delete(key)
    );
    ["zoom_end", "linked_zoom_end", "zoom_max", "zoom_max_time", "zoomEnd"].forEach((key) =>
      params.delete(key)
    );
    if (
      Number.isFinite(state.zoom.minTime) &&
      Number.isFinite(state.zoom.maxTime) &&
      state.zoom.maxTime > state.zoom.minTime
    ) {
      params.set("zoom_start", String(Math.round(state.zoom.minTime)));
      params.set("zoom_end", String(Math.round(state.zoom.maxTime)));
    }
  }, windowObj);
}
