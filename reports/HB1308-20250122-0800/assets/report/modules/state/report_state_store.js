export class ReportStateStore {
  constructor(initialState) {
    const source = initialState && typeof initialState === "object" ? initialState : {};
    Object.assign(this, source);
  }

  setBucket(bucketMinutes) {
    this.activeBucket = bucketMinutes;
    return this.activeBucket;
  }

  setZoomRange(minTime, maxTime) {
    if (!this.zoom || typeof this.zoom !== "object") {
      this.zoom = { minTime: null, maxTime: null, syncing: false, raf: null, pending: null };
    }
    this.zoom.minTime = minTime;
    this.zoom.maxTime = maxTime;
    return { minTime: this.zoom.minTime, maxTime: this.zoom.maxTime };
  }

  resetZoom() {
    return this.setZoomRange(null, null);
  }

  setDuplicateFilters({ scope, metric, matchMode }) {
    if (typeof scope === "string") {
      this.activeDuplicateScope = scope;
    }
    if (typeof metric === "string") {
      this.activeDuplicateMetric = metric;
    }
    if (typeof matchMode === "string") {
      this.activeDuplicateMatchMode = matchMode;
    }
    return {
      scope: this.activeDuplicateScope,
      metric: this.activeDuplicateMetric,
      matchMode: this.activeDuplicateMatchMode,
    };
  }

  setVoterMatchMode(matchMode) {
    if (typeof matchMode === "string") {
      this.activeVoterMatchMode = matchMode;
    }
    return this.activeVoterMatchMode;
  }

  setTocHeading(headingId) {
    this.activeTocHeading = headingId;
    return this.activeTocHeading;
  }

  snapshot() {
    return {
      activeBucket: this.activeBucket,
      defaultBucket: this.defaultBucket,
      activeDuplicateScope: this.activeDuplicateScope,
      activeDuplicateMetric: this.activeDuplicateMetric,
      activeDuplicateMatchMode: this.activeDuplicateMatchMode,
      activeVoterMatchMode: this.activeVoterMatchMode,
      activeTocHeading: this.activeTocHeading,
      zoom: this.zoom && typeof this.zoom === "object" ? { ...this.zoom } : null,
    };
  }
}
