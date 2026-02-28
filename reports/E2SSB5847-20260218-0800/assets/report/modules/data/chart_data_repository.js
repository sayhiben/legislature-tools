export class ChartDataRepository {
  constructor({
    chartShardManifestByAnalysis,
    chartBaseRowsMap,
    chartBucketRowsMap,
    chartBucketOptionsByChart,
    toFiniteNumberOrNull,
    fetchJsonPayload,
  }) {
    this.chartShardManifestByAnalysis =
      chartShardManifestByAnalysis && typeof chartShardManifestByAnalysis === "object"
        ? chartShardManifestByAnalysis
        : {};
    this.chartBaseRowsMap =
      chartBaseRowsMap && typeof chartBaseRowsMap === "object" ? chartBaseRowsMap : {};
    this.chartBucketRowsMap = chartBucketRowsMap instanceof Map ? chartBucketRowsMap : new Map();
    this.chartBucketOptionsByChart =
      chartBucketOptionsByChart instanceof Map ? chartBucketOptionsByChart : new Map();
    this.toFiniteNumberOrNull = toFiniteNumberOrNull;
    this.fetchJsonPayload = fetchJsonPayload;
    this.loadedAnalysisBase = new Set();
    this.loadingAnalysisBase = new Map();
    this.loadedAnalysisBuckets = new Map();
    this.loadingAnalysisBuckets = new Map();
  }

  normalizeBucketKey(value) {
    const parsed = this.toFiniteNumberOrNull(value);
    if (parsed === null || parsed <= 0) {
      return null;
    }
    return String(Math.round(parsed));
  }

  chartRowsFromMap(payload) {
    return payload && typeof payload === "object" && !Array.isArray(payload) ? payload : {};
  }

  mergeChartShardPayload(payload, bucketKey) {
    const charts = this.chartRowsFromMap(payload ? payload.charts : null);
    Object.keys(charts).forEach((chartIdRaw) => {
      const chartId = String(chartIdRaw || "").trim();
      if (!chartId) {
        return;
      }
      const rows = Array.isArray(charts[chartIdRaw]) ? charts[chartIdRaw] : [];
      if (bucketKey === null) {
        this.chartBaseRowsMap[chartId] = rows;
        return;
      }
      let bucketMap = this.chartBucketRowsMap.get(chartId);
      if (!bucketMap) {
        bucketMap = new Map();
        this.chartBucketRowsMap.set(chartId, bucketMap);
      }
      bucketMap.set(bucketKey, rows);
    });
  }

  uniqueBucketOptions(rows) {
    const values = new Set();
    rows.forEach((row) => {
      const value = this.toFiniteNumberOrNull(row.bucket_minutes);
      if (value !== null) {
        values.add(value);
      }
    });
    return Array.from(values).sort((left, right) => left - right);
  }

  getChartBucketOptions(chartId, rows) {
    const manifestOptions = this.chartBucketOptionsByChart.get(chartId);
    if (Array.isArray(manifestOptions) && manifestOptions.length) {
      return manifestOptions.slice();
    }
    return this.uniqueBucketOptions(rows);
  }

  resolveBucketTarget(options, activeBucket, defaultBucket) {
    if (!options.length) {
      return { bucket: null, note: "" };
    }
    let target = activeBucket;
    if (!Number.isFinite(target)) {
      if (Number.isFinite(defaultBucket) && options.includes(defaultBucket)) {
        target = defaultBucket;
      } else {
        target = options.includes(30) ? 30 : options[0];
      }
    }
    if (!options.includes(target)) {
      const nearest = options
        .slice()
        .sort((left, right) => Math.abs(left - target) - Math.abs(right - target))[0];
      return {
        bucket: nearest,
        note:
          "Requested " +
          target +
          "m is unavailable for this chart; showing " +
          nearest +
          "m instead.",
      };
    }
    return { bucket: target, note: "" };
  }

  chartShardEntryForAnalysis(analysisId) {
    const key = String(analysisId || "").trim();
    if (!key) {
      return null;
    }
    const direct = this.chartShardManifestByAnalysis[key];
    if (direct && typeof direct === "object") {
      return direct;
    }
    return null;
  }

  analysisBucketSet(analysisId) {
    const key = String(analysisId || "").trim();
    let bucketSet = this.loadedAnalysisBuckets.get(key);
    if (!bucketSet) {
      bucketSet = new Set();
      this.loadedAnalysisBuckets.set(key, bucketSet);
    }
    return bucketSet;
  }

  analysisBucketPendingMap(analysisId) {
    const key = String(analysisId || "").trim();
    let pendingMap = this.loadingAnalysisBuckets.get(key);
    if (!pendingMap) {
      pendingMap = new Map();
      this.loadingAnalysisBuckets.set(key, pendingMap);
    }
    return pendingMap;
  }

  async ensureAnalysisBaseLoaded(analysisId) {
    const key = String(analysisId || "").trim();
    if (!key) {
      return;
    }
    const manifest = this.chartShardEntryForAnalysis(key);
    if (!manifest || typeof manifest.base_url !== "string" || !manifest.base_url.trim()) {
      this.loadedAnalysisBase.add(key);
      return;
    }
    if (this.loadedAnalysisBase.has(key)) {
      return;
    }
    const inFlight = this.loadingAnalysisBase.get(key);
    if (inFlight) {
      await inFlight;
      return;
    }
    const promise = (async () => {
      const payload = await this.fetchJsonPayload(manifest.base_url, "base analysis shard for " + key);
      this.mergeChartShardPayload(payload, null);
      this.loadedAnalysisBase.add(key);
    })();
    this.loadingAnalysisBase.set(key, promise);
    try {
      await promise;
    } finally {
      this.loadingAnalysisBase.delete(key);
    }
  }

  async ensureAnalysisBucketLoaded(analysisId, bucketMinutes) {
    const key = String(analysisId || "").trim();
    const bucketKey = this.normalizeBucketKey(bucketMinutes);
    if (!key || !bucketKey) {
      return;
    }
    const manifest = this.chartShardEntryForAnalysis(key);
    if (!manifest || typeof manifest !== "object") {
      return;
    }
    const bucketUrls =
      manifest.bucket_urls && typeof manifest.bucket_urls === "object" ? manifest.bucket_urls : {};
    const targetUrl = typeof bucketUrls[bucketKey] === "string" ? bucketUrls[bucketKey].trim() : "";
    if (!targetUrl) {
      return;
    }
    const loadedBuckets = this.analysisBucketSet(key);
    if (loadedBuckets.has(bucketKey)) {
      return;
    }
    const pendingMap = this.analysisBucketPendingMap(key);
    const inFlight = pendingMap.get(bucketKey);
    if (inFlight) {
      await inFlight;
      return;
    }
    const promise = (async () => {
      const payload = await this.fetchJsonPayload(targetUrl, "bucket " + bucketKey + "m shard for " + key);
      this.mergeChartShardPayload(payload, bucketKey);
      loadedBuckets.add(bucketKey);
    })();
    pendingMap.set(bucketKey, promise);
    try {
      await promise;
    } finally {
      pendingMap.delete(bucketKey);
    }
  }

  resolvedBucketsForAnalysis(analysisId, activeBucket, defaultBucket) {
    const manifest = this.chartShardEntryForAnalysis(analysisId);
    if (!manifest || typeof manifest !== "object") {
      return [];
    }
    const chartOptions =
      manifest.chart_bucket_options && typeof manifest.chart_bucket_options === "object"
        ? manifest.chart_bucket_options
        : {};
    const resolved = new Set();
    Object.keys(chartOptions).forEach((chartIdRaw) => {
      const chartId = String(chartIdRaw || "").trim();
      if (!chartId) {
        return;
      }
      const rows = Array.isArray(this.chartBaseRowsMap[chartId]) ? this.chartBaseRowsMap[chartId] : [];
      const options = this.getChartBucketOptions(chartId, rows);
      const selection = this.resolveBucketTarget(options, activeBucket, defaultBucket);
      if (Number.isFinite(selection.bucket)) {
        resolved.add(Math.round(selection.bucket));
      }
    });
    return Array.from(resolved).sort((left, right) => left - right);
  }

  analysisNeedsShardLoad(analysisId, activeBucket, defaultBucket) {
    const key = String(analysisId || "").trim();
    if (!key) {
      return false;
    }
    const manifest = this.chartShardEntryForAnalysis(key);
    if (!manifest || typeof manifest !== "object") {
      return false;
    }
    if (!this.loadedAnalysisBase.has(key)) {
      return true;
    }
    const buckets = this.resolvedBucketsForAnalysis(key, activeBucket, defaultBucket);
    if (!buckets.length) {
      return false;
    }
    const loadedBuckets = this.analysisBucketSet(key);
    return buckets.some((bucket) => !loadedBuckets.has(String(Math.round(bucket))));
  }

  async ensureAnalysisDataLoaded(analysisId, activeBucket, defaultBucket) {
    const key = String(analysisId || "").trim();
    if (!key) {
      return;
    }
    const manifest = this.chartShardEntryForAnalysis(key);
    if (!manifest || typeof manifest !== "object") {
      return;
    }
    await this.ensureAnalysisBaseLoaded(key);
    const buckets = this.resolvedBucketsForAnalysis(key, activeBucket, defaultBucket);
    if (!buckets.length) {
      return;
    }
    await Promise.all(buckets.map((bucket) => this.ensureAnalysisBucketLoaded(key, bucket)));
  }

  getChartRows(chartId, activeBucket, defaultBucket) {
    const baseRows = Array.isArray(this.chartBaseRowsMap[chartId]) ? this.chartBaseRowsMap[chartId] : [];
    const bucketMap = this.chartBucketRowsMap.get(chartId);
    if (!bucketMap || !bucketMap.size) {
      return baseRows;
    }
    const options = this.getChartBucketOptions(chartId, baseRows);
    if (!options.length) {
      const mergedRows = [];
      bucketMap.forEach((rows) => {
        if (Array.isArray(rows) && rows.length) {
          mergedRows.push(...rows);
        }
      });
      return baseRows.concat(mergedRows);
    }
    const selection = this.resolveBucketTarget(options, activeBucket, defaultBucket);
    if (!Number.isFinite(selection.bucket)) {
      return baseRows;
    }
    const key = String(Math.round(selection.bucket));
    const bucketRows = bucketMap.get(key);
    return baseRows.concat(Array.isArray(bucketRows) ? bucketRows : []);
  }
}
