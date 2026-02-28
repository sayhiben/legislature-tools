export class ChartRendererRegistry {
  constructor() {
    this.renderers = new Map();
  }

  register(chartId, renderFn) {
    const key = String(chartId || "").trim();
    if (!key || typeof renderFn !== "function") {
      return;
    }
    this.renderers.set(key, renderFn);
  }

  registerMany(chartIds, renderFn) {
    if (!Array.isArray(chartIds)) {
      return;
    }
    chartIds.forEach((chartId) => this.register(chartId, renderFn));
  }

  resolve(chartId) {
    const key = String(chartId || "").trim();
    if (!key) {
      return null;
    }
    return this.renderers.get(key) || null;
  }

  render(chartId, mount, rows, ctx) {
    const renderer = this.resolve(chartId);
    if (!renderer) {
      return { handled: false, result: null };
    }
    return {
      handled: true,
      result: renderer(mount, rows, ctx),
    };
  }
}
