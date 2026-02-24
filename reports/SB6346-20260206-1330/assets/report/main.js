(async function () {
  const sourceElement = document.getElementById("report-data-source");
  const sourceConfig = sourceElement ? JSON.parse(sourceElement.textContent || "{}") : {};
  const reportDataUrl =
    typeof sourceConfig.report_data_url === "string" && sourceConfig.report_data_url.trim()
      ? sourceConfig.report_data_url.trim()
      : "";
  async function loadReportData() {
    if (!reportDataUrl) {
      return {};
    }
    try {
      const response = await fetch(reportDataUrl);
      if (!response.ok) {
        throw new Error(
          "HTTP " + String(response.status || "") + " while loading " + reportDataUrl
        );
      }
      const payload = await response.json();
      return payload && typeof payload === "object" ? payload : {};
    } catch (error) {
      console.error("Failed to load report data payload.", error);
      const busyIndicator = document.getElementById("report-busy-indicator");
      const busyText = document.getElementById("report-busy-text");
      if (busyText) {
        busyText.textContent =
          "Unable to load report data. Serve this report directory over HTTP and refresh.";
      }
      if (busyIndicator) {
        busyIndicator.classList.remove("hidden");
      }
      return null;
    }
  }
  const reportData = await loadReportData();
  if (!reportData) {
    return;
  }
  const interactive = reportData.interactive_charts || {};
  const analysisCatalog = Array.isArray(interactive.analysis_catalog) ? interactive.analysis_catalog : [];
  const analysisById = new Map(analysisCatalog.map((analysis) => [String(analysis.id || ""), analysis]));
  const chartDataManifest =
    interactive.chart_data_manifest && typeof interactive.chart_data_manifest === "object"
      ? interactive.chart_data_manifest
      : {};
  const chartShardManifestByAnalysis =
    chartDataManifest.analysis && typeof chartDataManifest.analysis === "object"
      ? chartDataManifest.analysis
      : {};
  const chartBaseRowsMap =
    interactive.charts && typeof interactive.charts === "object"
      ? Object.assign({}, interactive.charts)
      : {};
  const chartBucketRowsMap = new Map();
  const chartBucketOptionsByChart = new Map();
  const chartToAnalysis = new Map();
  const loadedAnalysisBase = new Set();
  const loadingAnalysisBase = new Map();
  const loadedAnalysisBuckets = new Map();
  const loadingAnalysisBuckets = new Map();

  const rawChartToAnalysis =
    chartDataManifest.chart_to_analysis && typeof chartDataManifest.chart_to_analysis === "object"
      ? chartDataManifest.chart_to_analysis
      : {};
  Object.keys(rawChartToAnalysis).forEach((chartIdRaw) => {
    const chartId = String(chartIdRaw || "").trim();
    const analysisId = String(rawChartToAnalysis[chartIdRaw] || "").trim();
    if (!chartId || !analysisId) {
      return;
    }
    chartToAnalysis.set(chartId, analysisId);
  });
  analysisCatalog.forEach((analysis) => {
    const analysisId = String((analysis || {}).id || "").trim();
    if (!analysisId) {
      return;
    }
    const chartIds = [];
    const hero = String((analysis || {}).hero_chart_id || "").trim();
    if (hero) {
      chartIds.push(hero);
    }
    if (Array.isArray((analysis || {}).detail_chart_ids)) {
      analysis.detail_chart_ids.forEach((chartIdRaw) => {
        const chartId = String(chartIdRaw || "").trim();
        if (chartId) {
          chartIds.push(chartId);
        }
      });
    }
    chartIds.forEach((chartId) => {
      if (!chartToAnalysis.has(chartId)) {
        chartToAnalysis.set(chartId, analysisId);
      }
    });
  });
  Object.keys(chartShardManifestByAnalysis).forEach((analysisIdRaw) => {
    const analysisId = String(analysisIdRaw || "").trim();
    if (!analysisId) {
      return;
    }
    const entry = chartShardManifestByAnalysis[analysisIdRaw];
    const chartOptions =
      entry && typeof entry.chart_bucket_options === "object"
        ? entry.chart_bucket_options
        : {};
    Object.keys(chartOptions || {}).forEach((chartIdRaw) => {
      const chartId = String(chartIdRaw || "").trim();
      if (!chartId) {
        return;
      }
      const valuesRaw = chartOptions[chartIdRaw];
      const options = Array.isArray(valuesRaw)
        ? Array.from(
            new Set(
              valuesRaw
                .map((value) => {
                  const parsed =
                    typeof value === "number"
                      ? value
                      : typeof value === "string"
                        ? Number(value)
                        : null;
                  return Number.isFinite(parsed) && parsed > 0 ? Math.round(parsed) : null;
                })
                .filter((value) => value !== null)
            )
          ).sort((left, right) => left - right)
        : [];
      chartBucketOptionsByChart.set(chartId, options);
      if (!chartToAnalysis.has(chartId)) {
        chartToAnalysis.set(chartId, analysisId);
      }
    });
  });

  const triageViews = interactive.triage_views || {};
  const triageSummary = interactive.triage_summary || {};
  const dataQualityPanel = interactive.data_quality_panel || {};
  const hearingContextPanel = interactive.hearing_context_panel || {};
  const controls = interactive.controls || {};
  const focusAnalysisIds = Array.isArray(controls.focus_analysis_ids)
    ? controls.focus_analysis_ids
        .map((analysisId) => String(analysisId || "").trim())
        .filter((analysisId) => !!analysisId)
    : [];
  const isOffHoursFocusOnly =
    focusAnalysisIds.length === 1 && focusAnalysisIds[0] === "off_hours";
  const methodology = controls.methodology || {};
  const themeOptions = Array.isArray(controls.theme_options)
    ? controls.theme_options
    : [];
  const defaultTheme =
    typeof controls.default_theme === "string" && controls.default_theme.trim()
      ? controls.default_theme.trim()
      : "light";
  const colorSemantics =
    controls.color_semantics && typeof controls.color_semantics === "object"
      ? controls.color_semantics
      : {};
  const tableColumnDocs = reportData.table_column_docs || {};
  const tableHelpDocs = reportData.table_help_docs || {};
  const reportTimezone =
    typeof controls.timezone === "string" && controls.timezone.trim()
      ? controls.timezone.trim()
      : "America/Los_Angeles";
  const reportTimezoneLabel =
    typeof controls.timezone_label === "string" && controls.timezone_label.trim()
      ? controls.timezone_label.trim()
      : reportTimezone;
  const processMarkers = Array.isArray(controls.process_markers)
    ? controls.process_markers
    : [];
  const duplicateScopeOptions = Array.isArray(controls.duplicate_collision_scope_options)
    ? controls.duplicate_collision_scope_options
        .map((value) => String(value || "").trim())
        .filter((value) => !!value)
    : [];
  const duplicateMetricOptions = Array.isArray(controls.duplicate_collision_metric_options)
    ? controls.duplicate_collision_metric_options
        .map((value) => String(value || "").trim())
        .filter((value) => !!value)
    : [];
  const defaultDuplicateScope =
    typeof controls.duplicate_collision_scope_default === "string" &&
    duplicateScopeOptions.includes(controls.duplicate_collision_scope_default)
      ? controls.duplicate_collision_scope_default
      : duplicateScopeOptions[0] || "full_hearing";
  const defaultDuplicateMetric =
    typeof controls.duplicate_collision_metric_default === "string" &&
    duplicateMetricOptions.includes(controls.duplicate_collision_metric_default)
      ? controls.duplicate_collision_metric_default
      : duplicateMetricOptions[0] || "repeated_group_rows";

  const hasEcharts = typeof window.echarts !== "undefined";
  const hasTabulator = typeof window.Tabulator !== "undefined";

  const chartInstances = [];
  const mountedSections = new Set();
  const chartMounts = new Map();

  const state = {
    activeBucket: null,
    defaultBucket: null,
    activeDuplicateScope: defaultDuplicateScope,
    defaultDuplicateScope: defaultDuplicateScope,
    activeDuplicateMetric: defaultDuplicateMetric,
    defaultDuplicateMetric: defaultDuplicateMetric,
    cursorX: null,
    activeTocHeading: null,
    activeSectionControlKey: "",
    globalControlsExpandedMobile: false,
    renderToc: null,
    selectedWindowRange: null,
    zoom: {
      minTime: null,
      maxTime: null,
      syncing: false,
      raf: null,
      pending: null,
    },
    timeCharts: new Set(),
    absoluteTimeSet: new Set(
      ((controls.zoom_sync_groups || {}).absolute_time || []).map((id) => String(id || ""))
    ),
  };
  const linkedZoomFilterChartIds = new Set([
    "off_hours_funnel_plot",
    "off_hours_date_hour_pro_heatmap",
    "off_hours_date_hour_primary_residual_heatmap",
    "off_hours_date_hour_volume_heatmap",
    "procon_swings_shift_heatmap",
  ]);
  const DUPLICATE_TOP_NAME_TIMING_PAGE_SIZE = 10;
  const DUPLICATE_TOP_NAME_TIMING_MAX_PAGES = 10;
  const DUPLICATE_TOP_NAME_TIMING_MAX_NAMES =
    DUPLICATE_TOP_NAME_TIMING_PAGE_SIZE * DUPLICATE_TOP_NAME_TIMING_MAX_PAGES;
  const DUPLICATE_INLINE_TIMING_CHART_HEIGHT_PX = 136;
  const zonedDateTimeEpochCache = new Map();
  const semanticTokenCache = new Map();
  let sidebarFloatingControlsObserver = null;
  const fallbackColorSemantics = {
    light: {
      axisText: "#334155",
      axisLine: "#94a3b8",
      axisName: "#0f172a",
      markerLabel: "#334155",
      cursor: "#475569",
      series: {
        primary: "#0072b2",
        volume: "#94a3b8",
        context: "#009e73",
        interval: "#8b99a8",
        reference: "#475569",
      },
      alert: { lower: "#d55e00", upper: "#cc79a7" },
      state: { low_power: "#e69f00", outlier: "#56b4e9" },
      band: {
        alert_run: "rgba(213, 94, 0, 0.12)",
        comparator: "rgba(0, 114, 178, 0.1)",
      },
      heatmap: {
        rate_diverging: ["#2c7fb8", "#9ecae1", "#f7f7f7", "#fdd49e", "#d95f0e"],
        residual_diverging: ["#b13a00", "#f4a259", "#f5f7fa", "#82b1d8", "#1f6aa5"],
        volume_seq: ["#f8fafc", "#cbd5e1", "#475569"],
      },
      categorical_palette: [
        "#0072b2",
        "#009e73",
        "#e69f00",
        "#cc79a7",
        "#56b4e9",
        "#d55e00",
        "#8b99a8",
        "#475569",
      ],
    },
    dark: {
      axisText: "#d4deeb",
      axisLine: "#6f839b",
      axisName: "#e5edf7",
      markerLabel: "#d4deeb",
      cursor: "#b8c6d8",
      series: {
        primary: "#5ab0ff",
        volume: "#64748b",
        context: "#2fc79a",
        interval: "#a8b5c5",
        reference: "#94a3b8",
      },
      alert: { lower: "#ff8a3d", upper: "#f2a7d4" },
      state: { low_power: "#f2c14e", outlier: "#7cc7ff" },
      band: {
        alert_run: "rgba(255, 138, 61, 0.18)",
        comparator: "rgba(90, 176, 255, 0.14)",
      },
      heatmap: {
        rate_diverging: ["#6baed6", "#2e4c66", "#111827", "#6b4a2d", "#f4a259"],
        residual_diverging: ["#ff8a3d", "#c9723a", "#1e293b", "#5a8db8", "#8cc7ff"],
        volume_seq: ["#0f172a", "#334155", "#94a3b8"],
      },
      categorical_palette: [
        "#5ab0ff",
        "#2fc79a",
        "#f2c14e",
        "#f2a7d4",
        "#7cc7ff",
        "#ff8a3d",
        "#a8b5c5",
        "#94a3b8",
      ],
    },
  };

  function toNumber(value) {
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "string") {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : 0;
    }
    return 0;
  }

  function toFiniteNumberOrNull(value) {
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "boolean") {
      return value ? 1 : 0;
    }
    if (typeof value === "string") {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : null;
    }
    return null;
  }

  function toBool(value) {
    if (typeof value === "boolean") {
      return value;
    }
    if (typeof value === "number") {
      return value !== 0;
    }
    if (typeof value === "string") {
      const normalized = value.trim().toLowerCase();
      return normalized === "true" || normalized === "1" || normalized === "yes";
    }
    return false;
  }

  function activeThemeId() {
    const value = String(document.documentElement.getAttribute("data-theme") || "").trim();
    return value || "light";
  }

  function toArray(value, fallback) {
    if (!Array.isArray(value)) {
      return fallback.slice();
    }
    const normalized = value
      .map((entry) => (typeof entry === "string" ? entry.trim() : ""))
      .filter((entry) => !!entry);
    return normalized.length ? normalized : fallback.slice();
  }

  function resolveColorSemanticTheme(themeId) {
    const normalizedTheme = String(themeId || "").toLowerCase() === "dark" ? "dark" : "light";
    const fallback = fallbackColorSemantics[normalizedTheme] || fallbackColorSemantics.light;
    const provided =
      colorSemantics && typeof colorSemantics[normalizedTheme] === "object"
        ? colorSemantics[normalizedTheme]
        : {};
    const providedSeries =
      provided.series && typeof provided.series === "object" ? provided.series : {};
    const providedAlert =
      provided.alert && typeof provided.alert === "object" ? provided.alert : {};
    const providedState =
      provided.state && typeof provided.state === "object" ? provided.state : {};
    const providedBand =
      provided.band && typeof provided.band === "object" ? provided.band : {};
    const providedHeatmap =
      provided.heatmap && typeof provided.heatmap === "object" ? provided.heatmap : {};
    return {
      axisText:
        typeof provided.axisText === "string" && provided.axisText.trim()
          ? provided.axisText.trim()
          : fallback.axisText,
      axisLine:
        typeof provided.axisLine === "string" && provided.axisLine.trim()
          ? provided.axisLine.trim()
          : fallback.axisLine,
      axisName:
        typeof provided.axisName === "string" && provided.axisName.trim()
          ? provided.axisName.trim()
          : fallback.axisName,
      markerLabel:
        typeof provided.markerLabel === "string" && provided.markerLabel.trim()
          ? provided.markerLabel.trim()
          : fallback.markerLabel,
      cursor:
        typeof provided.cursor === "string" && provided.cursor.trim()
          ? provided.cursor.trim()
          : fallback.cursor,
      series: {
        primary:
          typeof providedSeries.primary === "string" && providedSeries.primary.trim()
            ? providedSeries.primary.trim()
            : fallback.series.primary,
        volume:
          typeof providedSeries.volume === "string" && providedSeries.volume.trim()
            ? providedSeries.volume.trim()
            : fallback.series.volume,
        context:
          typeof providedSeries.context === "string" && providedSeries.context.trim()
            ? providedSeries.context.trim()
            : fallback.series.context,
        interval:
          typeof providedSeries.interval === "string" && providedSeries.interval.trim()
            ? providedSeries.interval.trim()
            : fallback.series.interval,
        reference:
          typeof providedSeries.reference === "string" && providedSeries.reference.trim()
            ? providedSeries.reference.trim()
            : fallback.series.reference,
      },
      alert: {
        lower:
          typeof providedAlert.lower === "string" && providedAlert.lower.trim()
            ? providedAlert.lower.trim()
            : fallback.alert.lower,
        upper:
          typeof providedAlert.upper === "string" && providedAlert.upper.trim()
            ? providedAlert.upper.trim()
            : fallback.alert.upper,
      },
      state: {
        low_power:
          typeof providedState.low_power === "string" && providedState.low_power.trim()
            ? providedState.low_power.trim()
            : fallback.state.low_power,
        outlier:
          typeof providedState.outlier === "string" && providedState.outlier.trim()
            ? providedState.outlier.trim()
            : fallback.state.outlier,
      },
      band: {
        alert_run:
          typeof providedBand.alert_run === "string" && providedBand.alert_run.trim()
            ? providedBand.alert_run.trim()
            : fallback.band.alert_run,
        comparator:
          typeof providedBand.comparator === "string" && providedBand.comparator.trim()
            ? providedBand.comparator.trim()
            : fallback.band.comparator,
      },
      heatmap: {
        rate_diverging: toArray(
          providedHeatmap.rate_diverging,
          fallback.heatmap.rate_diverging
        ),
        residual_diverging: toArray(
          providedHeatmap.residual_diverging,
          fallback.heatmap.residual_diverging
        ),
        volume_seq: toArray(providedHeatmap.volume_seq, fallback.heatmap.volume_seq),
      },
      categoricalPalette: toArray(
        provided.categorical_palette,
        fallback.categorical_palette
      ),
    };
  }

  function currentChartTheme() {
    const surfaceTheme = activeThemeId();
    if (semanticTokenCache.has(surfaceTheme)) {
      return semanticTokenCache.get(surfaceTheme);
    }
    const semanticTheme = resolveColorSemanticTheme(surfaceTheme);
    const tokens = {
      axisText: semanticTheme.axisText,
      axisLine: semanticTheme.axisLine,
      axisName: semanticTheme.axisName,
      markerLabel: semanticTheme.markerLabel,
      cursor: semanticTheme.cursor,
      gridLine:
        surfaceTheme === "dark"
          ? "rgba(148, 163, 184, 0.18)"
          : "rgba(71, 85, 105, 0.16)",
      splitAreaBands:
        surfaceTheme === "dark"
          ? ["rgba(148, 163, 184, 0.035)", "rgba(148, 163, 184, 0.01)"]
          : ["rgba(71, 85, 105, 0.035)", "rgba(71, 85, 105, 0.012)"],
      primaryLine: semanticTheme.series.primary,
      volumeBar: semanticTheme.series.volume,
      volumeBarOpacity: surfaceTheme === "dark" ? 0.42 : 0.4,
      contextLine: semanticTheme.series.context,
      intervalBand: semanticTheme.series.interval,
      referenceLine: semanticTheme.series.reference,
      alertLower: semanticTheme.alert.lower,
      alertUpper: semanticTheme.alert.upper,
      alert: semanticTheme.alert.lower,
      lowPower: semanticTheme.state.low_power,
      outlierPoint: semanticTheme.state.outlier,
      alertBandFill: semanticTheme.band.alert_run,
      comparatorBandFill: semanticTheme.band.comparator,
      heatmapRateDiverging: semanticTheme.heatmap.rate_diverging,
      heatmapDiverging: semanticTheme.heatmap.residual_diverging,
      heatmapVolume: semanticTheme.heatmap.volume_seq,
      heatmapNoData:
        surfaceTheme === "dark" ? "rgba(148, 163, 184, 0.24)" : "rgba(148, 163, 184, 0.18)",
      heatmapNoDataBorder:
        surfaceTheme === "dark" ? "rgba(148, 163, 184, 0.34)" : "rgba(148, 163, 184, 0.24)",
      heatmapRate: [
        semanticTheme.heatmap.volume_seq[0],
        semanticTheme.series.context,
        semanticTheme.series.primary,
      ],
      barAccent: semanticTheme.series.primary,
      scatterDefault: semanticTheme.series.primary,
      shadowColor: surfaceTheme === "dark" ? "rgba(12, 18, 30, 0.45)" : "rgba(15, 23, 42, 0.24)",
      seriesPalette: [
        semanticTheme.series.primary,
        semanticTheme.series.context,
        semanticTheme.series.interval,
        semanticTheme.alert.lower,
        semanticTheme.alert.upper,
        semanticTheme.state.low_power,
        semanticTheme.state.outlier,
        semanticTheme.series.reference,
      ],
      categoricalPalette: semanticTheme.categoricalPalette,
    };
    semanticTokenCache.set(surfaceTheme, tokens);
    return tokens;
  }

  function colorForExtraLine(metricName, index) {
    const theme = currentChartTheme();
    const key = String(metricName || "").toLowerCase();
    if (!key) {
      return theme.seriesPalette[index % theme.seriesPalette.length];
    }
    if (key.includes("expected")) {
      return theme.contextLine;
    }
    if (key.includes("control_low") || key.includes("control_high")) {
      return theme.intervalBand;
    }
    if (key.includes("stable") || key.includes("threshold")) {
      return theme.referenceLine;
    }
    if (key.includes("z_ref_zero")) {
      return theme.referenceLine;
    }
    if (key.includes("z_ref_pos")) {
      return theme.intervalBand;
    }
    if (key.includes("z_ref_neg")) {
      return theme.intervalBand;
    }
    if (key.includes("baseline")) {
      return theme.contextLine;
    }
    return theme.seriesPalette[index % theme.seriesPalette.length];
  }

  function styleForExtraLine(metricName, index, chartId) {
    const key = String(metricName || "").toLowerCase();
    const chart = String(chartId || "").toLowerCase();
    const offHoursDense =
      chart === "off_hours_control_timeline" || chart === "off_hours_primary_residual_timeline";
    const base = {
      width: offHoursDense ? 0.95 : 1.1,
      type: index === 0 ? "dashed" : "solid",
      opacity: offHoursDense ? 0.56 : 0.78,
    };
    if (!key) {
      return base;
    }
    if (key.includes("expected_pro_rate_primary")) {
      return {
        width: offHoursDense ? 1.15 : 1.25,
        type: "solid",
        opacity: offHoursDense ? 0.68 : 0.82,
      };
    }
    if (key.includes("expected_pro_rate_day")) {
      return {
        width: 1,
        type: "dashed",
        opacity: offHoursDense ? 0.42 : 0.56,
      };
    }
    if (key.includes("control_low") || key.includes("control_high")) {
      return {
        width: offHoursDense ? 0.9 : 1,
        type: "dashed",
        opacity: offHoursDense ? 0.4 : 0.58,
      };
    }
    if (key.includes("z_ref_zero")) {
      return {
        width: 1,
        type: "solid",
        opacity: offHoursDense ? 0.56 : 0.68,
      };
    }
    if (key.includes("z_ref_pos") || key.includes("z_ref_neg")) {
      return {
        width: 0.9,
        type: "dotted",
        opacity: offHoursDense ? 0.4 : 0.52,
      };
    }
    if (key.includes("baseline")) {
      return {
        width: 1,
        type: "dashed",
        opacity: offHoursDense ? 0.46 : 0.62,
      };
    }
    return base;
  }

  function toEpochMillis(value) {
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (!value) {
      return null;
    }
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function applyReadableAxisStyle(axisConfig, axisKind) {
    if (!axisConfig || typeof axisConfig !== "object" || Array.isArray(axisConfig)) {
      return axisConfig;
    }
    const theme = currentChartTheme();
    const styled = Object.assign({}, axisConfig);

    styled.axisLabel = Object.assign(
      { color: theme.axisText, fontSize: 11 },
      styled.axisLabel || {}
    );
    if (toFiniteNumberOrNull(styled.axisLabel.margin) === null) {
      if (axisKind === "x") {
        const rotate = toFiniteNumberOrNull(styled.axisLabel.rotate);
        styled.axisLabel.margin = rotate !== null && Math.abs(rotate) >= 10 ? 16 : 12;
      } else {
        styled.axisLabel.margin = 8;
      }
    }

    const axisLine = Object.assign({}, styled.axisLine || {});
    axisLine.show = axisLine.show !== false;
    axisLine.lineStyle = Object.assign(
      { color: theme.axisLine, width: 1 },
      axisLine.lineStyle || {}
    );
    styled.axisLine = axisLine;

    const axisTick = Object.assign({}, styled.axisTick || {});
    axisTick.show = axisTick.show !== false;
    axisTick.lineStyle = Object.assign(
      { color: theme.axisLine },
      axisTick.lineStyle || {}
    );
    styled.axisTick = axisTick;

    const splitLine = Object.assign({}, styled.splitLine || {});
    splitLine.show = splitLine.show !== false;
    splitLine.lineStyle = Object.assign(
      { color: theme.gridLine, width: 1 },
      splitLine.lineStyle || {}
    );
    styled.splitLine = splitLine;

    const splitArea = Object.assign({}, styled.splitArea || {});
    if (splitArea.show) {
      const splitAreaStyle = Object.assign({}, splitArea.areaStyle || {});
      if (!Array.isArray(splitAreaStyle.color) || !splitAreaStyle.color.length) {
        splitAreaStyle.color = theme.splitAreaBands;
      }
      splitArea.areaStyle = splitAreaStyle;
    }
    styled.splitArea = splitArea;

    const hasName = typeof styled.name === "string" && styled.name.trim();
    if (hasName) {
      styled.nameTextStyle = Object.assign(
        { color: theme.axisName, fontSize: 12, fontWeight: 600 },
        styled.nameTextStyle || {}
      );
      if (!styled.nameLocation) {
        styled.nameLocation = "middle";
      }
      if (toFiniteNumberOrNull(styled.nameGap) === null) {
        styled.nameGap = axisKind === "x" ? 32 : 54;
      }
    }

    return styled;
  }

  function parsePixelLikeValue(value) {
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value !== "string") {
      return null;
    }
    const trimmed = value.trim();
    if (!trimmed || trimmed.endsWith("%")) {
      return null;
    }
    const parsed = Number(trimmed.replace(/px$/i, ""));
    return Number.isFinite(parsed) ? parsed : null;
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function truncateLegendText(label, maxChars) {
    const source = String(label || "");
    if (!Number.isFinite(maxChars) || maxChars < 4 || source.length <= maxChars) {
      return source;
    }
    return source.slice(0, Math.max(1, maxChars - 1)).trimEnd() + "…";
  }

  function computeLegendDockMode(mount) {
    const viewportWidth =
      typeof window !== "undefined" && Number.isFinite(window.innerWidth)
        ? window.innerWidth
        : 0;
    const hostWidth =
      mount && mount.host && Number.isFinite(mount.host.clientWidth) ? mount.host.clientWidth : 0;
    let chartWidth = hostWidth;
    if (!(chartWidth > 0) && mount && mount.chart && typeof mount.chart.getWidth === "function") {
      const chartApiWidth = toFiniteNumberOrNull(mount.chart.getWidth());
      if (chartApiWidth !== null && chartApiWidth > 0) {
        chartWidth = chartApiWidth;
      }
    }

    const wideViewport = viewportWidth >= 1200;
    const wideChart = chartWidth >= 960;
    return wideViewport && wideChart ? "right" : "bottom";
  }

  function applyDockedLegendLayout(option, mount) {
    if (!option || typeof option !== "object" || !option.legend) {
      return option;
    }
    const theme = currentChartTheme();
    const dockRight = computeLegendDockMode(mount) === "right";
    if (mount && typeof mount === "object") {
      mount.legendDockMode = dockRight ? "right" : "bottom";
    }
    const maxLegendChars = dockRight ? 24 : 34;

    const styleLegend = (legendConfig) => {
      if (!legendConfig || typeof legendConfig !== "object" || Array.isArray(legendConfig)) {
        return legendConfig;
      }
      const styledLegend = Object.assign({}, legendConfig);
      const baseFormatter =
        typeof styledLegend.formatter === "function" ? styledLegend.formatter : null;

      if (dockRight) {
        styledLegend.type = "scroll";
        styledLegend.orient = "vertical";
        styledLegend.right = 8;
        styledLegend.top = 24;
        styledLegend.bottom = 24;
        delete styledLegend.left;
      } else {
        if (!styledLegend.orient) {
          styledLegend.orient = "horizontal";
        }
        if (toFiniteNumberOrNull(styledLegend.bottom) === null && typeof styledLegend.bottom !== "string") {
          styledLegend.bottom = 0;
        }
      }

      if (toFiniteNumberOrNull(styledLegend.itemWidth) === null) {
        styledLegend.itemWidth = 14;
      }
      if (toFiniteNumberOrNull(styledLegend.itemHeight) === null) {
        styledLegend.itemHeight = 10;
      }
      if (toFiniteNumberOrNull(styledLegend.itemGap) === null) {
        styledLegend.itemGap = dockRight ? 8 : 10;
      }

      styledLegend.formatter = (name) => {
        const baseLabel = baseFormatter ? baseFormatter(name) : name;
        return truncateLegendText(baseLabel, maxLegendChars);
      };

      const legendTooltip =
        styledLegend.tooltip && typeof styledLegend.tooltip === "object"
          ? Object.assign({}, styledLegend.tooltip)
          : {};
      styledLegend.tooltip = Object.assign({}, legendTooltip, {
        show: true,
        confine: true,
        formatter: (params) => {
          const fullLabel =
            params && typeof params.name === "string"
              ? params.name
              : params && typeof params.value === "string"
                ? params.value
                : "";
          return escapeHtml(fullLabel);
        },
      });

      styledLegend.textStyle = Object.assign(
        {},
        styledLegend.textStyle || {},
        dockRight
          ? { width: 172, overflow: "truncate", ellipsis: "…", color: theme.axisText }
          : { color: theme.axisText }
      );

      return styledLegend;
    };

    const styled = Object.assign({}, option);
    styled.legend = Array.isArray(option.legend)
      ? option.legend.map((legendItem) => styleLegend(legendItem))
      : styleLegend(option.legend);

    if (dockRight && styled.grid && typeof styled.grid === "object") {
      if (Array.isArray(styled.grid)) {
        styled.grid = styled.grid.map((gridConfig) => {
          if (!gridConfig || typeof gridConfig !== "object") {
            return gridConfig;
          }
          const gridRight = parsePixelLikeValue(gridConfig.right);
          return Object.assign({}, gridConfig, {
            right: Math.max(gridRight === null ? 0 : gridRight, 228),
          });
        });
      } else {
        const gridRight = parsePixelLikeValue(styled.grid.right);
        styled.grid = Object.assign({}, styled.grid, {
          right: Math.max(gridRight === null ? 0 : gridRight, 228),
        });
      }
    }

    return styled;
  }

  function normalizeAxisArray(axisConfig) {
    if (!axisConfig) {
      return [];
    }
    return Array.isArray(axisConfig) ? axisConfig : [axisConfig];
  }

  function hasNamedXAxis(option) {
    return normalizeAxisArray(option && option.xAxis).some(
      (axis) => axis && typeof axis.name === "string" && axis.name.trim()
    );
  }

  function hasSliderDataZoom(option) {
    const zoomEntries = Array.isArray(option && option.dataZoom)
      ? option.dataZoom
      : option && option.dataZoom
        ? [option.dataZoom]
        : [];
    return zoomEntries.some((entry) => {
      if (!entry || typeof entry !== "object") {
        return false;
      }
      const zoomType = String(entry.type || "").toLowerCase();
      return zoomType === "slider";
    });
  }

  function hasVisualMap(option) {
    if (!option || typeof option !== "object") {
      return false;
    }
    if (Array.isArray(option.visualMap)) {
      return option.visualMap.length > 0;
    }
    return !!option.visualMap;
  }

  function hasRotatedXAxisLabels(option) {
    return normalizeAxisArray(option && option.xAxis).some((axis) => {
      if (!axis || typeof axis !== "object") {
        return false;
      }
      const axisLabel = axis.axisLabel;
      if (!axisLabel || typeof axisLabel !== "object") {
        return false;
      }
      const rotate = toFiniteNumberOrNull(axisLabel.rotate);
      return rotate !== null && Math.abs(rotate) >= 10;
    });
  }

  function legendDockIsBottom(option) {
    if (!option || !option.legend) {
      return false;
    }
    const legends = Array.isArray(option.legend) ? option.legend : [option.legend];
    return legends.some((legend) => {
      if (!legend || typeof legend !== "object") {
        return false;
      }
      const orient = String(legend.orient || "horizontal").toLowerCase();
      return orient !== "vertical";
    });
  }

  function reserveXAxisBottomSpace(option) {
    if (!option || typeof option !== "object" || !option.grid) {
      return option;
    }
    const hasName = hasNamedXAxis(option);
    const sliderZoom = hasSliderDataZoom(option);
    const visualMapEnabled = hasVisualMap(option);
    const rotatedXLabels = hasRotatedXAxisLabels(option);
    const legendAtBottom = legendDockIsBottom(option);

    let minBottom = 56;
    if (hasName) {
      minBottom += 16;
    }
    if (rotatedXLabels) {
      minBottom += 16;
    }
    if (sliderZoom) {
      minBottom += 42;
    }
    if (visualMapEnabled) {
      minBottom += 56;
    }
    if (legendAtBottom) {
      minBottom += 22;
    }

    const applyBottom = (gridConfig) => {
      if (!gridConfig || typeof gridConfig !== "object" || Array.isArray(gridConfig)) {
        return gridConfig;
      }
      const existingBottom = parsePixelLikeValue(gridConfig.bottom);
      if (existingBottom !== null || typeof gridConfig.bottom === "string") {
        return gridConfig;
      }
      return Object.assign({}, gridConfig, { bottom: minBottom });
    };

    const updated = Object.assign({}, option);
    updated.grid = Array.isArray(option.grid)
      ? option.grid.map((gridEntry) => applyBottom(gridEntry))
      : applyBottom(option.grid);
    return updated;
  }

  function ensureReadableAxes(option, mount) {
    if (!option || typeof option !== "object") {
      return option;
    }
    const styled = Object.assign({}, option);

    if (Array.isArray(styled.xAxis)) {
      styled.xAxis = styled.xAxis.map((axis) =>
        applyReadableAxisStyle(axis, "x")
      );
    } else if (styled.xAxis && typeof styled.xAxis === "object") {
      styled.xAxis = applyReadableAxisStyle(styled.xAxis, "x");
    }

    if (Array.isArray(styled.yAxis)) {
      styled.yAxis = styled.yAxis.map((axis) =>
        applyReadableAxisStyle(axis, "y")
      );
    } else if (styled.yAxis && typeof styled.yAxis === "object") {
      styled.yAxis = applyReadableAxisStyle(styled.yAxis, "y");
    }

    const withLegendLayout = applyDockedLegendLayout(styled, mount);
    return reserveXAxisBottomSpace(withLegendLayout);
  }

  function formatPercent(value, digits) {
    const parsed = toFiniteNumberOrNull(value);
    if (parsed === null) {
      return "-";
    }
    const precision = Number.isFinite(digits) ? digits : 1;
    return (parsed * 100).toFixed(precision) + "%";
  }

  function formatRatio(numerator, denominator, digits) {
    const num = toFiniteNumberOrNull(numerator);
    const den = toFiniteNumberOrNull(denominator);
    if (num === null || den === null || den <= 0) {
      return "-";
    }
    const precision = Number.isFinite(digits) ? digits : 1;
    return (num / den * 100).toFixed(precision) + "%";
  }

  function formatDateRange(startIso, endIso) {
    const start = toEpochMillis(startIso);
    const end = toEpochMillis(endIso);
    if (start === null || end === null) {
      return "-";
    }
    return formatEpochMillis(start) + " to " + formatEpochMillis(end);
  }

  function formatDurationHumanized(durationMs) {
    if (!Number.isFinite(durationMs) || durationMs <= 0) {
      return "";
    }
    const dayMs = 24 * 60 * 60 * 1000;
    const hourMs = 60 * 60 * 1000;
    const minuteMs = 60 * 1000;
    const days = Math.floor(durationMs / dayMs);
    const hours = Math.floor((durationMs % dayMs) / hourMs);
    const minutes = Math.floor((durationMs % hourMs) / minuteMs);
    const parts = [];
    if (days > 0) {
      parts.push(String(days) + "d");
    }
    if (hours > 0) {
      parts.push(String(hours) + "h");
    }
    if (minutes > 0 && days === 0) {
      parts.push(String(minutes) + "m");
    }
    return parts.join(" ");
  }

  function formatDateRangeHumanized(startIso, endIso) {
    const start = toEpochMillis(startIso);
    const end = toEpochMillis(endIso);
    if (start === null || end === null) {
      return {
        value: "-",
        meta: "",
      };
    }
    const value = formatZoomRangeEpochMillis(start) + " to " + formatZoomRangeEpochMillis(end);
    const durationText = formatDurationHumanized(Math.max(0, end - start));
    return {
      value: value,
      meta: durationText ? "Span: " + durationText + "." : "",
    };
  }

  function setTextById(id, value) {
    const element = document.getElementById(id);
    if (element) {
      element.textContent = String(value);
    }
  }

  function tablePreviewRows(detectorName, tableName) {
    const detectorTables =
      reportData.table_previews && typeof reportData.table_previews === "object"
        ? reportData.table_previews[detectorName]
        : null;
    const rows = detectorTables && typeof detectorTables === "object" ? detectorTables[tableName] : null;
    return Array.isArray(rows) ? rows : [];
  }

  function kpiPositionColor(positionLabel) {
    const normalized = String(positionLabel || "").trim().toLowerCase();
    const theme = currentChartTheme();
    if (normalized.includes("pro")) {
      return theme.contextLine;
    }
    if (normalized.includes("con")) {
      return theme.primaryLine;
    }
    if (normalized.includes("unknown")) {
      return theme.intervalBand;
    }
    return theme.referenceLine;
  }

  function renderKpiMiniPie(hostId, slices) {
    const host = document.getElementById(hostId);
    if (!host) {
      return;
    }
    host.innerHTML = "";
    const rows = Array.isArray(slices)
      ? slices.filter((slice) => toFiniteNumberOrNull((slice || {}).value) !== null)
      : [];
    const total = rows.reduce((acc, slice) => acc + Math.max(0, toNumber(slice.value)), 0);
    if (!(total > 0)) {
      return;
    }

    const parts = [];
    let cursor = 0;
    rows.forEach((slice) => {
      const value = Math.max(0, toNumber(slice.value));
      if (!(value > 0)) {
        return;
      }
      const start = (cursor / total) * 360;
      cursor += value;
      const end = (cursor / total) * 360;
      const color = typeof slice.color === "string" && slice.color.trim() ? slice.color.trim() : "#94a3b8";
      parts.push(color + " " + start + "deg " + end + "deg");
    });

    const pie = document.createElement("div");
    pie.className = "kpi-mini-pie";
    pie.style.background = "conic-gradient(" + parts.join(", ") + ")";
    host.appendChild(pie);
  }

  function renderKpiMiniBars(hostId, entries, options) {
    const host = document.getElementById(hostId);
    if (!host) {
      return;
    }
    host.innerHTML = "";
    const rows = Array.isArray(entries) ? entries : [];
    if (!rows.length) {
      return;
    }

    const maxValueOption = options ? toFiniteNumberOrNull(options.maxValue) : null;
    const maxValue =
      maxValueOption !== null && maxValueOption > 0
        ? maxValueOption
        : rows.reduce((acc, row) => Math.max(acc, Math.max(0, toNumber((row || {}).value))), 0);
    if (!(maxValue > 0)) {
      return;
    }
    const formatter =
      options && typeof options.valueFormatter === "function"
        ? options.valueFormatter
        : (value) => String(value);

    const list = document.createElement("div");
    list.className = "kpi-mini-bars";
    rows.forEach((row) => {
      const labelText = String((row || {}).label || "").trim();
      if (!labelText) {
        return;
      }
      const value = Math.max(0, toNumber((row || {}).value));
      const width = Math.max(0, Math.min(100, (value / maxValue) * 100));
      const color =
        typeof row.color === "string" && row.color.trim() ? row.color.trim() : kpiPositionColor(labelText);

      const wrapper = document.createElement("div");
      wrapper.className = "kpi-mini-bars-row";

      const label = document.createElement("span");
      label.className = "kpi-mini-bars-label";
      label.textContent = labelText;
      wrapper.appendChild(label);

      const track = document.createElement("div");
      track.className = "kpi-mini-bars-track";
      const fill = document.createElement("div");
      fill.className = "kpi-mini-bars-fill";
      fill.style.width = width.toFixed(1) + "%";
      fill.style.background = color;
      track.appendChild(fill);
      wrapper.appendChild(track);

      const valueLabel = document.createElement("span");
      valueLabel.className = "kpi-mini-bars-value";
      valueLabel.textContent = formatter(value);
      wrapper.appendChild(valueLabel);

      list.appendChild(wrapper);
    });

    if (!list.childElementCount) {
      return;
    }
    host.appendChild(list);
  }

  function duplicatePositionCounts(rows) {
    const counts = new Map([
      ["Pro", 0],
      ["Con", 0],
      ["Other", 0],
      ["Unknown", 0],
    ]);

    rows.forEach((row) => {
      const pro = Math.max(0, toNumber((row || {}).n_pro));
      const con = Math.max(0, toNumber((row || {}).n_con));
      const observed = Math.max(
        0,
        toNumber((row || {}).observed_count ?? (row || {}).n_records ?? (row || {}).n)
      );
      const other = Math.max(0, observed - pro - con);
      let bucket = "Unknown";
      if (pro >= con && pro >= other && pro > 0) {
        bucket = "Pro";
      } else if (con >= pro && con >= other && con > 0) {
        bucket = "Con";
      } else if (other > 0) {
        bucket = "Other";
      }
      counts.set(bucket, toNumber(counts.get(bucket)) + 1);
    });

    return ["Pro", "Con", "Other", "Unknown"]
      .map((label) => ({
        label: label,
        value: toNumber(counts.get(label)),
        color: kpiPositionColor(label),
      }))
      .filter((entry) => entry.value > 0);
  }

  function processMarkerDisplayLabel(marker) {
    const explicit = marker && typeof marker.label === "string" ? marker.label.trim() : "";
    if (explicit) {
      return explicit;
    }
    const key = marker && typeof marker.key === "string" ? marker.key : "";
    if (!key) {
      return "Process marker";
    }
    return key
      .replace(/_/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function processMarkerColor(markerKey) {
    const theme = currentChartTheme();
    if (markerKey === "meeting_start") {
      return theme.contextLine;
    }
    if (markerKey === "sign_in_open") {
      return theme.primaryLine;
    }
    if (markerKey === "sign_in_cutoff") {
      return theme.lowPower;
    }
    if (markerKey === "written_testimony_deadline") {
      return theme.alertLower;
    }
    return theme.axisText;
  }

  function buildProcessMarkerLines() {
    return processMarkers
      .map((marker) => {
        const markerTime = toEpochMillis(
          marker && (marker.time_iso || marker.timestamp || marker.time)
        );
        if (markerTime === null) {
          return null;
        }
        const markerKey = marker && typeof marker.key === "string" ? marker.key : "";
        return {
          xAxis: markerTime,
          lineStyle: {
            color: processMarkerColor(markerKey),
            width: 1.2,
            opacity: 0.88,
            type: "dashed",
          },
          label: {
            show: true,
            formatter: processMarkerDisplayLabel(marker),
            position: "insideEndTop",
            color: currentChartTheme().markerLabel,
            fontSize: 10,
          },
        };
      })
      .filter((entry) => !!entry);
  }

  function themeLabel(themeId) {
    if (themeId === "light") {
      return "Light";
    }
    if (themeId === "dark") {
      return "Dark";
    }
    return themeId;
  }

  function syncThemeButtons(activeTheme) {
    const buttons = Array.from(
      document.querySelectorAll('[data-theme-option]')
    );
    buttons.forEach((button) => {
      const option = String(button.getAttribute("data-theme-option") || "").trim();
      const isActive = option === activeTheme;
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-pressed", isActive ? "true" : "false");
    });
  }

  function applyTheme(themeId, persist) {
    const canPersist = persist !== false;
    const availableThemes = Array.from(
      new Set(
        themeOptions
          .map((entry) => (entry && typeof entry.id === "string" ? entry.id.trim() : ""))
          .filter((entry) => !!entry)
      )
    );
    if (!availableThemes.length) {
      availableThemes.push("light", "dark");
    }
    const resolved = availableThemes.includes(themeId) ? themeId : availableThemes[0];
    document.documentElement.setAttribute("data-theme", resolved);
    syncThemeButtons(resolved);
    if (!canPersist) {
      return;
    }
    try {
      window.localStorage.setItem("testifier_audit_theme", resolved);
    } catch (_error) {}
  }

  function initThemeControl() {
    const controlsRoot = document.getElementById("theme-controls");

    const options = Array.from(
      new Set(
        themeOptions
          .map((entry) => (entry && typeof entry.id === "string" ? entry.id.trim() : ""))
          .filter((entry) => !!entry)
      )
    );
    if (!options.length) {
      options.push("light", "dark");
    }

    let savedTheme = "";
    try {
      savedTheme = String(window.localStorage.getItem("testifier_audit_theme") || "").trim();
    } catch (_error) {}

    const selectedTheme = options.includes(savedTheme)
      ? savedTheme
      : options.includes(defaultTheme)
        ? defaultTheme
        : options[0];
    applyTheme(selectedTheme, false);

    if (!controlsRoot) {
      updateSidebarFloatingOffsets();
      return;
    }

    const buttons = Array.from(
      controlsRoot.querySelectorAll('[data-theme-option]')
    );
    buttons.forEach((button) => {
      const option = String(button.getAttribute("data-theme-option") || "").trim();
      const explicit = themeOptions.find(
        (entry) => entry && typeof entry.id === "string" && entry.id.trim() === option
      );
      button.setAttribute(
        "aria-label",
        explicit && typeof explicit.label === "string" && explicit.label.trim()
          ? explicit.label.trim()
          : themeLabel(option)
      );
      button.addEventListener("click", () => {
        if (!options.includes(option)) {
          return;
        }
        applyTheme(option, true);
        chartMounts.forEach((mount) => renderChartMount(mount));
        if (!isOffHoursFocusOnly) {
          renderTriageSummary();
        }
        scheduleChartResizeSequence();
      });
    });
    updateSidebarFloatingOffsets();
  }

  function createChartInstance(host) {
    if (!hasEcharts) {
      return null;
    }
    return window.echarts.init(host);
  }

  function initSidebarTooltips() {
    const tooltipTargets = Array.from(document.querySelectorAll("[data-tooltip]"));
    if (!tooltipTargets.length) {
      return;
    }

    if (typeof window.tippy === "function") {
      window.tippy(tooltipTargets, {
        content(reference) {
          return String(reference.getAttribute("data-tooltip") || "");
        },
        allowHTML: false,
        theme: "light-border",
        maxWidth: 320,
        delay: [120, 40],
        placement: "right",
      });
      return;
    }

    tooltipTargets.forEach((target) => {
      if (!target.getAttribute("title")) {
        target.setAttribute("title", String(target.getAttribute("data-tooltip") || ""));
      }
    });
  }

  function setListItems(host, items) {
    if (!host) {
      return;
    }
    host.innerHTML = "";
    const values = Array.isArray(items)
      ? items.map((item) => String(item || "").trim()).filter((item) => !!item)
      : [];
    if (!values.length) {
      const empty = document.createElement("li");
      empty.textContent = "No guidance available for this run.";
      host.appendChild(empty);
      return;
    }
    values.forEach((value) => {
      const item = document.createElement("li");
      item.textContent = value;
      host.appendChild(item);
    });
  }

  function clearStructuredHostClasses(container) {
    if (!container) {
      return;
    }
    container.classList.remove("structured-host");
    container.classList.remove("structured-host-key-value");
    container.classList.remove("structured-host-pairs");
  }

  function isBlankValue(value) {
    if (value === null || value === undefined) {
      return true;
    }
    if (typeof value === "string") {
      return !value.trim();
    }
    if (Array.isArray(value)) {
      return value.length === 0;
    }
    return false;
  }

  function formatStructuredValue(value) {
    if (value === null || value === undefined) {
      return "N/A";
    }
    if (typeof value === "boolean") {
      return value ? "Yes" : "No";
    }
    if (typeof value === "number") {
      if (!Number.isFinite(value)) {
        return "N/A";
      }
      if (Number.isInteger(value)) {
        return value.toLocaleString();
      }
      return value.toLocaleString(undefined, { maximumFractionDigits: 6 });
    }
    if (Array.isArray(value)) {
      if (!value.length) {
        return "N/A";
      }
      return value.map((entry) => formatStructuredValue(entry)).join(", ");
    }
    if (typeof value === "object") {
      return JSON.stringify(value);
    }
    const text = String(value).trim();
    return text || "N/A";
  }

  function appendStructuredEmptyMessage(container, message) {
    if (!container) {
      return;
    }
    const empty = document.createElement("p");
    empty.className = "structured-empty";
    empty.textContent = message || "No entries available for this run.";
    container.appendChild(empty);
  }

  function mountKeyValueList(container, rows, options) {
    if (!container) {
      return null;
    }
    clearStructuredHostClasses(container);
    container.classList.add("structured-host");
    container.classList.add("structured-host-key-value");
    container.innerHTML = "";

    const dataset = Array.isArray(rows) ? rows : [];
    if (!dataset.length) {
      appendStructuredEmptyMessage(container, "No metadata available for this run.");
      return { kind: "structured", data: [] };
    }

    const keyField =
      options && typeof options.keyField === "string" && options.keyField.trim()
        ? options.keyField.trim()
        : "key";
    const valueField =
      options && typeof options.valueField === "string" && options.valueField.trim()
        ? options.valueField.trim()
        : "value";
    const humanizeKeys = !!(options && options.humanizeKeys);
    const list = document.createElement("dl");
    list.className = "structured-kv-list";

    dataset.forEach((row) => {
      if (!row || typeof row !== "object") {
        return;
      }
      const keyRaw = row[keyField];
      const valueRaw = row[valueField];
      const keyText = String(keyRaw === null || keyRaw === undefined ? "" : keyRaw).trim();
      if (!keyText && isBlankValue(valueRaw)) {
        return;
      }

      const item = document.createElement("div");
      item.className = "structured-kv-item";

      const key = document.createElement("dt");
      key.textContent = humanizeKeys ? humanizeFieldName(keyText) : keyText || "entry";
      item.appendChild(key);

      const value = document.createElement("dd");
      value.textContent = formatStructuredValue(valueRaw);
      item.appendChild(value);

      list.appendChild(item);
    });

    if (!list.childElementCount) {
      appendStructuredEmptyMessage(container, "No metadata available for this run.");
      return { kind: "structured", data: dataset };
    }

    container.appendChild(list);
    return { kind: "structured", data: dataset };
  }

  function mountTextPairCards(container, rows, options) {
    if (!container) {
      return null;
    }
    clearStructuredHostClasses(container);
    container.classList.add("structured-host");
    container.classList.add("structured-host-pairs");
    container.innerHTML = "";

    const dataset = Array.isArray(rows) ? rows : [];
    if (!dataset.length) {
      appendStructuredEmptyMessage(container, "No entries available for this run.");
      return { kind: "structured", data: [] };
    }

    const titleField =
      options && typeof options.titleField === "string" && options.titleField.trim()
        ? options.titleField.trim()
        : "title";
    const bodyField =
      options && typeof options.bodyField === "string" && options.bodyField.trim()
        ? options.bodyField.trim()
        : "body";
    const list = document.createElement("div");
    list.className = "structured-pair-list";

    dataset.forEach((row) => {
      if (!row || typeof row !== "object") {
        return;
      }
      const titleText = String(row[titleField] === null || row[titleField] === undefined ? "" : row[titleField]).trim();
      const bodyText = String(row[bodyField] === null || row[bodyField] === undefined ? "" : row[bodyField]).trim();
      if (!titleText && !bodyText) {
        return;
      }

      const item = document.createElement("article");
      item.className = "structured-pair-item";

      const title = document.createElement("h4");
      title.className = "structured-pair-title";
      title.textContent = titleText || "entry";
      item.appendChild(title);

      const body = document.createElement("p");
      body.className = "structured-pair-body";
      body.textContent = bodyText || "N/A";
      item.appendChild(body);

      list.appendChild(item);
    });

    if (!list.childElementCount) {
      appendStructuredEmptyMessage(container, "No entries available for this run.");
      return { kind: "structured", data: dataset };
    }

    container.appendChild(list);
    return { kind: "structured", data: dataset };
  }

  function renderMethodologyPanel() {
    const definitionsHost = document.getElementById("methodology-definitions-host");
    const testsHost = document.getElementById("methodology-tests-used-host");
    const guardrailsHost = document.getElementById("methodology-guardrails-host");
    const multipleTestingHost = document.getElementById("methodology-multiple-testing-list");
    const caveatsHost = document.getElementById("methodology-caveats-list");
    const guidanceHost = document.getElementById("methodology-guidance-list");

    const definitions = Array.isArray(methodology.definitions) ? methodology.definitions : [];
    const testsUsed = Array.isArray(methodology.tests_used) ? methodology.tests_used : [];
    const guardrails = Array.isArray(methodology.ethical_guardrails)
      ? methodology.ethical_guardrails
      : [];
    const multipleTesting = Array.isArray(methodology.multiple_testing_policy)
      ? methodology.multiple_testing_policy
      : [];
    const caveats = Array.isArray(methodology.caveats) ? methodology.caveats : [];
    const guidance = Array.isArray(methodology.interpretation_guidance)
      ? methodology.interpretation_guidance
      : [];

    mountTextPairCards(definitionsHost, definitions, {
      titleField: "term",
      bodyField: "definition",
    });
    mountTable(testsHost, testsUsed, {
      pagination: false,
      maxHeight: "320px",
      tableKey: "methodology.tests_used",
    });
    mountTextPairCards(guardrailsHost, guardrails, {
      titleField: "standard",
      bodyField: "requirement",
    });
    setListItems(multipleTestingHost, multipleTesting);
    setListItems(caveatsHost, caveats);
    setListItems(guidanceHost, guidance);
  }

  function getRawTriageView() {
    const view = triageViews.raw;
    if (view && typeof view === "object") {
      return view;
    }
    return {
      triage_summary: triageSummary,
    };
  }

  function buildDateTimeFormatter(timezoneName) {
    const options = {
      timeZone: timezoneName,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    };
    try {
      return new Intl.DateTimeFormat(undefined, options);
    } catch (_error) {
      return new Intl.DateTimeFormat(
        undefined,
        Object.assign({}, options, { timeZone: "America/Los_Angeles" })
      );
    }
  }

  const reportDateTimeFormatter = buildDateTimeFormatter(reportTimezone);
  const zoomRangeDateTimeFormatter = (() => {
    try {
      return new Intl.DateTimeFormat("en-US", {
        timeZone: reportTimezone,
        month: "short",
        day: "numeric",
        year: "numeric",
        hour: "numeric",
        minute: "2-digit",
        hourCycle: "h23",
      });
    } catch (_error) {
      return new Intl.DateTimeFormat("en-US", {
        timeZone: "America/Los_Angeles",
        month: "short",
        day: "numeric",
        year: "numeric",
        hour: "numeric",
        minute: "2-digit",
        hourCycle: "h23",
      });
    }
  })();
  const zoomMonthAbbrevMap = {
    Jan: "Jan.",
    Feb: "Feb.",
    Mar: "Mar.",
    Apr: "Apr.",
    May: "May",
    Jun: "Jun.",
    Jul: "Jul.",
    Aug: "Aug.",
    Sep: "Sep.",
    Oct: "Oct.",
    Nov: "Nov.",
    Dec: "Dec.",
  };
  const meetingDateTimeFormatter = (() => {
    const options = {
      timeZone: reportTimezone,
      weekday: "short",
      month: "short",
      day: "numeric",
      year: "numeric",
      hour: "numeric",
      minute: "2-digit",
      timeZoneName: "short",
    };
    try {
      return new Intl.DateTimeFormat("en-US", options);
    } catch (_error) {
      return new Intl.DateTimeFormat(
        "en-US",
        Object.assign({}, options, { timeZone: "America/Los_Angeles" })
      );
    }
  })();

  function formatEpochMillis(epochMillis) {
    if (!Number.isFinite(epochMillis)) {
      return "";
    }
    return reportDateTimeFormatter.format(new Date(epochMillis));
  }

  function formatZoomRangeEpochMillis(epochMillis) {
    if (!Number.isFinite(epochMillis)) {
      return "";
    }
    const parts = zoomRangeDateTimeFormatter.formatToParts(new Date(epochMillis));
    let month = "";
    let day = "";
    let year = "";
    let hour = "";
    let minute = "";
    parts.forEach((part) => {
      if (!part || typeof part.type !== "string") {
        return;
      }
      if (part.type === "month") {
        month = String(part.value || "");
      } else if (part.type === "day") {
        day = String(part.value || "");
      } else if (part.type === "year") {
        year = String(part.value || "");
      } else if (part.type === "hour") {
        hour = String(part.value || "");
      } else if (part.type === "minute") {
        minute = String(part.value || "");
      }
    });

    const monthLabel = zoomMonthAbbrevMap[month] || (month ? month + "." : "");
    const hourNumber = Number.parseInt(hour, 10);
    if (!monthLabel || !day || !year || !Number.isFinite(hourNumber)) {
      return formatEpochMillis(epochMillis);
    }
    const minuteLabel = minute ? minute.padStart(2, "0") : "00";
    return (
      monthLabel +
      " " +
      day +
      ", " +
      year +
      ", " +
      String(hourNumber) +
      ":" +
      minuteLabel
    );
  }

  function formatTooltipValue(value) {
    const numeric = toFiniteNumberOrNull(value);
    if (numeric === null) {
      return "n/a";
    }
    const abs = Math.abs(numeric);
    if (abs >= 1000) {
      return numeric.toLocaleString(undefined, { maximumFractionDigits: 2 });
    }
    if (abs >= 1) {
      return numeric.toFixed(2);
    }
    if (abs === 0) {
      return "0";
    }
    return numeric.toFixed(4);
  }

  function bucketLabelFromValue(value) {
    const bucketMinutes = toFiniteNumberOrNull(value);
    return bucketMinutes !== null ? String(bucketMinutes) + "m" : null;
  }

  function bucketSelectorLabel(value) {
    const bucketMinutes = toFiniteNumberOrNull(value);
    if (bucketMinutes === null) {
      return "";
    }
    if (bucketMinutes >= 60 && bucketMinutes % 60 === 0) {
      return String(Math.round(bucketMinutes / 60)) + "h";
    }
    return String(Math.round(bucketMinutes)) + "m";
  }

  function normalizeHashId(rawValue) {
    const stripped = String(rawValue || "")
      .replace(/^#/, "")
      .trim();
    if (!stripped) {
      return "";
    }
    try {
      return decodeURIComponent(stripped);
    } catch (_error) {
      return stripped;
    }
  }

  function globalControlsOffsetPx(extraPixels) {
    const controlsPanel = document.getElementById("sidebar-global-controls");
    const panelHeight = controlsPanel ? controlsPanel.getBoundingClientRect().height : 0;
    const cssHeightRaw = getComputedStyle(document.documentElement).getPropertyValue(
      "--sidebar-global-controls-height"
    );
    const cssHeight = Number.parseFloat(cssHeightRaw);
    const baseHeight =
      panelHeight > 0
        ? panelHeight
        : Number.isFinite(cssHeight) && cssHeight > 0
          ? cssHeight
          : 84;
    const extra = toFiniteNumberOrNull(extraPixels);
    return Math.max(64, Math.round(baseHeight + (extra === null ? 0 : extra)));
  }

  function sectionViewControlsOffsetPx() {
    const panel = document.getElementById("section-view-controls-panel");
    if (!panel || panel.classList.contains("hidden")) {
      return 0;
    }
    const panelHeight = panel.getBoundingClientRect().height;
    if (panelHeight > 0) {
      return Math.round(panelHeight);
    }
    const cssHeightRaw = getComputedStyle(document.documentElement).getPropertyValue(
      "--section-view-controls-height"
    );
    const cssHeight = Number.parseFloat(cssHeightRaw);
    return Number.isFinite(cssHeight) && cssHeight > 0 ? Math.round(cssHeight) : 0;
  }

  function headerStackOffsetPx(extraPixels) {
    const extra = toFiniteNumberOrNull(extraPixels);
    const topStack = globalControlsOffsetPx(0) + sectionViewControlsOffsetPx();
    return Math.max(64, Math.round(topStack + (extra === null ? 0 : extra)));
  }

  function updateSidebarFloatingOffsets() {
    const rootStyle = document.documentElement ? document.documentElement.style : null;
    const controlsPanel = document.getElementById("sidebar-global-controls");
    if (!rootStyle || !controlsPanel) {
      return;
    }
    const panelHeight = controlsPanel.getBoundingClientRect().height;
    if (!(panelHeight > 0)) {
      return;
    }
    rootStyle.setProperty(
      "--sidebar-global-controls-height",
      String(Math.ceil(panelHeight)) + "px"
    );
    const sectionControlsPanel = document.getElementById("section-view-controls-panel");
    let sectionControlsHeight = 0;
    if (sectionControlsPanel && !sectionControlsPanel.classList.contains("hidden")) {
      sectionControlsHeight = sectionControlsPanel.getBoundingClientRect().height;
    }
    rootStyle.setProperty(
      "--section-view-controls-height",
      sectionControlsHeight > 0 ? String(Math.ceil(sectionControlsHeight)) + "px" : "0px"
    );
  }

  function initSidebarFloatingOffsetsObserver() {
    updateSidebarFloatingOffsets();
    if (sidebarFloatingControlsObserver || typeof window.ResizeObserver !== "function") {
      return;
    }
    const controlsPanel = document.getElementById("sidebar-global-controls");
    if (!controlsPanel) {
      return;
    }
    sidebarFloatingControlsObserver = new window.ResizeObserver(() => {
      updateSidebarFloatingOffsets();
    });
    sidebarFloatingControlsObserver.observe(controlsPanel);
    const sectionControlsPanel = document.getElementById("section-view-controls-panel");
    if (sectionControlsPanel) {
      sidebarFloatingControlsObserver.observe(sectionControlsPanel);
    }
  }

  function sectionControlKeyForHeading(headingId) {
    const normalized = normalizeHashId(headingId);
    if (!normalized) {
      return "";
    }
    const heading = document.getElementById(normalized);
    if (!heading) {
      return "";
    }
    const analysisSection = heading.closest("[data-analysis-id]");
    if (analysisSection) {
      return String(analysisSection.getAttribute("data-analysis-id") || "").trim();
    }
    const reportSection = heading.closest("section[id]");
    if (reportSection) {
      return String(reportSection.id || "").trim();
    }
    return "";
  }

  function updateSectionViewControlsForHeading(headingId) {
    const root = document.getElementById("section-view-controls-panel");
    if (!root) {
      return;
    }
    const panels = Array.from(root.querySelectorAll("[data-section-control-for]"));
    if (!panels.length) {
      root.classList.add("hidden");
      state.activeSectionControlKey = "";
      updateSidebarFloatingOffsets();
      return;
    }

    const key = sectionControlKeyForHeading(headingId);
    let activePanel = null;
    panels.forEach((panel) => {
      const panelKey = String(panel.getAttribute("data-section-control-for") || "").trim();
      const enabled = panel.getAttribute("data-section-control-enabled") !== "false";
      const isActive = enabled && !!panelKey && panelKey === key;
      panel.classList.toggle("hidden", !isActive);
      if (isActive) {
        activePanel = panel;
      }
    });

    if (!activePanel) {
      root.classList.add("hidden");
      state.activeSectionControlKey = "";
      updateSidebarFloatingOffsets();
      return;
    }

    root.classList.remove("hidden");

    const activeKey = String(activePanel.getAttribute("data-section-control-for") || "").trim();
    if (state.activeSectionControlKey !== activeKey) {
      activePanel.classList.remove("is-spotlight");
      // Force restart so each section switch replays the cue.
      void activePanel.offsetWidth;
      activePanel.classList.add("is-spotlight");
      window.setTimeout(() => {
        activePanel.classList.remove("is-spotlight");
      }, 980);
    }
    state.activeSectionControlKey = activeKey;
    updateSidebarFloatingOffsets();
  }

  function replaceUrlHashWithoutHistory(headingId) {
    const normalized = normalizeHashId(headingId);
    if (!normalized) {
      return;
    }
    const nextHash = "#" + normalized;
    if (window.location.hash === nextHash) {
      return;
    }
    if (window.history && typeof window.history.replaceState === "function") {
      try {
        const currentUrl = new window.URL(window.location.href);
        currentUrl.hash = nextHash;
        const nextUrl = currentUrl.pathname + currentUrl.search + currentUrl.hash;
        window.history.replaceState(window.history.state || null, "", nextUrl);
        return;
      } catch (_error) {}
    }
    window.location.hash = nextHash;
  }

  function updateUrlQueryParams(mutator) {
    if (
      !window.URL ||
      typeof window.URLSearchParams === "undefined" ||
      !window.history ||
      typeof window.history.replaceState !== "function"
    ) {
      return;
    }
    const currentUrl = new window.URL(window.location.href);
    const params = new window.URLSearchParams(currentUrl.search || "");
    mutator(params);
    const nextSearch = params.toString();
    const nextUrl =
      currentUrl.pathname +
      (nextSearch ? "?" + nextSearch : "") +
      (currentUrl.hash || "");
    const currentPath = window.location.pathname + window.location.search + window.location.hash;
    if (nextUrl !== currentPath) {
      window.history.replaceState(window.history.state || null, "", nextUrl);
    }
  }

  function syncControlOverridesToUrl() {
    updateUrlQueryParams((params) => {
      [
        "bucket",
        "bucket_minutes",
        "linked_bucket_minutes",
      ].forEach((key) => params.delete(key));
      ["dup_scope", "duplicate_scope"].forEach((key) => params.delete(key));
      ["dup_metric", "duplicate_metric"].forEach((key) => params.delete(key));
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

      [
        "zoom_start",
        "linked_zoom_start",
        "zoom_min",
        "zoom_min_time",
        "zoomStart",
      ].forEach((key) => params.delete(key));
      [
        "zoom_end",
        "linked_zoom_end",
        "zoom_max",
        "zoom_max_time",
        "zoomEnd",
      ].forEach((key) => params.delete(key));
      if (
        Number.isFinite(state.zoom.minTime) &&
        Number.isFinite(state.zoom.maxTime) &&
        state.zoom.maxTime > state.zoom.minTime
      ) {
        params.set("zoom_start", String(Math.round(state.zoom.minTime)));
        params.set("zoom_end", String(Math.round(state.zoom.maxTime)));
      }
    });
  }

  function setActiveTocHeading(headingId, syncUrl) {
    const normalized = normalizeHashId(headingId);
    if (!normalized || state.activeTocHeading === normalized) {
      return;
    }
    state.activeTocHeading = normalized;

    if (typeof state.renderToc === "function") {
      state.renderToc(normalized);
    } else {
      const links = Array.from(document.querySelectorAll('#report-toc a[href^="#"]'));
      links.forEach((link) => {
        const isActive = normalizeHashId(link.getAttribute("href")) === normalized;
        link.classList.toggle("is-active-link", isActive);
        if (isActive) {
          link.setAttribute("aria-current", "true");
        } else {
          link.removeAttribute("aria-current");
        }
      });
    }
    updateSectionViewControlsForHeading(normalized);

    if (syncUrl) {
      replaceUrlHashWithoutHistory(normalized);
    }
  }

  function seriesData(rows, xField, yField) {
    return rows
      .map((row) => {
        const x = toEpochMillis(row[xField]);
        const y = toFiniteNumberOrNull(row[yField]);
        return x === null ? null : [x, y];
      })
      .filter((entry) => entry !== null)
      .sort((left, right) => left[0] - right[0]);
  }

  function inferTimeField(rows) {
    if (!rows.length) {
      return null;
    }
    const candidates = [
      "minute_bucket",
      "bucket_start",
      "start_minute",
      "first_seen",
      "change_minute",
      "date",
    ];
    const row = rows[0] || {};
    for (const field of candidates) {
      if (Object.prototype.hasOwnProperty.call(row, field)) {
        if (field === "date" && Object.prototype.hasOwnProperty.call(row, "slot_start_minute")) {
          continue;
        }
        return field;
      }
    }
    return null;
  }

  function numericFields(row) {
    return Object.keys(row || {}).filter((key) => typeof row[key] === "number" && Number.isFinite(row[key]));
  }

  function setEmptyForChart(chartId, isEmpty) {
    const element = document.querySelector('[data-chart-empty-for="' + chartId + '"]');
    if (!element) {
      return;
    }
    element.classList.toggle("hidden", !isEmpty);
  }

  function setChartNote(chartId, text) {
    const element = document.querySelector('[data-chart-note-for="' + chartId + '"]');
    if (!element) {
      return;
    }
    element.textContent = text || "";
  }

  function ensureChartControlsHost(chartId) {
    const note = document.querySelector('[data-chart-note-for="' + chartId + '"]');
    if (!note || !note.parentElement) {
      return null;
    }
    const parent = note.parentElement;
    let controlsHost = parent.querySelector('[data-chart-controls-for="' + chartId + '"]');
    if (!controlsHost) {
      controlsHost = document.createElement("div");
      controlsHost.className = "chart-inline-controls hidden";
      controlsHost.setAttribute("data-chart-controls-for", chartId);
      parent.insertBefore(controlsHost, note);
    }
    return controlsHost;
  }

  function setChartControls(chartId, contentNode) {
    const host = ensureChartControlsHost(chartId);
    if (!host) {
      return;
    }
    host.replaceChildren();
    if (!contentNode) {
      host.classList.add("hidden");
      return;
    }
    host.appendChild(contentNode);
    host.classList.remove("hidden");
  }

  function composeChartNote(mount, fallbackNote) {
    const parts = [];
    if (mount && Number.isFinite(mount.activeBucket)) {
      parts.push("Bucket: " + mount.activeBucket + "m.");
    }
    if (mount && mount.customChartNote) {
      parts.push(mount.customChartNote);
    }
    if (fallbackNote) {
      parts.push(fallbackNote);
    }
    return parts.join(" ");
  }

  function resizeCharts() {
    chartInstances.forEach((instance) => {
      if (instance && typeof instance.resize === "function") {
        instance.resize();
      }
    });
  }

  function scheduleChartResizeSequence() {
    window.requestAnimationFrame(() => resizeCharts());
    window.setTimeout(() => resizeCharts(), 120);
    window.setTimeout(() => resizeCharts(), 260);
    scheduleLegendLayoutRerender();
  }

  let legendLayoutRerenderTimer = null;

  function rerenderChartsForLegendLayoutIfNeeded() {
    if (!hasEcharts || !chartMounts.size) {
      return;
    }
    let rerendered = false;
    chartMounts.forEach((mount) => {
      if (!mount || !mount.host) {
        return;
      }
      const expectedMode = computeLegendDockMode(mount);
      const currentMode =
        typeof mount.legendDockMode === "string" && mount.legendDockMode
          ? mount.legendDockMode
          : "";
      if (currentMode && currentMode !== expectedMode) {
        renderChartMount(mount);
        rerendered = true;
      }
    });
    if (rerendered) {
      updateCursorAcrossTimeCharts();
      updateZoomRangeLabel();
    }
  }

  function scheduleLegendLayoutRerender() {
    if (legendLayoutRerenderTimer !== null) {
      window.clearTimeout(legendLayoutRerenderTimer);
    }
    legendLayoutRerenderTimer = window.setTimeout(() => {
      legendLayoutRerenderTimer = null;
      rerenderChartsForLegendLayoutIfNeeded();
    }, 180);
  }

  let busyToken = 0;

  function setBusyIndicator(isVisible, text) {
    const root = document.getElementById("report-busy-indicator");
    if (!root) {
      return;
    }
    const label = document.getElementById("report-busy-text");
    if (label && text) {
      label.textContent = text;
    }
    root.classList.toggle("hidden", !isVisible);
  }

  function showDataLoadError(message, error) {
    if (error) {
      console.error(message, error);
    } else {
      console.error(message);
    }
    const busyIndicator = document.getElementById("report-busy-indicator");
    const busyText = document.getElementById("report-busy-text");
    if (busyText) {
      busyText.textContent = message;
    }
    if (busyIndicator) {
      busyIndicator.classList.remove("hidden");
    }
  }

  async function runWithBusyIndicator(text, action) {
    busyToken += 1;
    const token = busyToken;
    setBusyIndicator(true, text);
    clearAllChartInteractionState();
    await new Promise((resolve) => {
      window.requestAnimationFrame(() => {
        window.requestAnimationFrame(resolve);
      });
    });
    try {
      return await action();
    } catch (error) {
      showDataLoadError(
        "Unable to load report data files. Serve this report directory over HTTP and refresh.",
        error
      );
      return null;
    } finally {
      window.setTimeout(() => {
        if (token === busyToken) {
          setBusyIndicator(false);
        }
      }, 140);
    }
  }

  function normalizeBucketKey(value) {
    const parsed = toFiniteNumberOrNull(value);
    if (parsed === null || parsed <= 0) {
      return null;
    }
    return String(Math.round(parsed));
  }

  function chartRowsFromMap(payload) {
    return payload && typeof payload === "object" && !Array.isArray(payload)
      ? payload
      : {};
  }

  function setChartLoading(chartId, isLoading, noteText) {
    const host = document.querySelector('[data-chart-id="' + chartId + '"]');
    if (host) {
      host.classList.toggle("is-loading", !!isLoading);
      host.setAttribute("aria-busy", isLoading ? "true" : "false");
    }
    if (isLoading) {
      setEmptyForChart(chartId, false);
      setChartNote(chartId, noteText || "Loading chart data...");
    }
  }

  function setSectionLoading(section, isLoading, noteText) {
    if (!section) {
      return;
    }
    const hosts = Array.from(section.querySelectorAll("[data-chart-id]"));
    hosts.forEach((host) => {
      const chartId = String(host.getAttribute("data-chart-id") || "").trim();
      if (!chartId) {
        return;
      }
      setChartLoading(chartId, isLoading, noteText);
    });
  }

  async function fetchJsonPayload(url, contextLabel) {
    const target = String(url || "").trim();
    if (!target) {
      return {};
    }
    const response = await fetch(target);
    if (!response.ok) {
      throw new Error(
        "HTTP " + String(response.status || "") + " while loading " + contextLabel
      );
    }
    const payload = await response.json();
    return payload && typeof payload === "object" ? payload : {};
  }

  function mergeChartShardPayload(payload, bucketKey) {
    const charts = chartRowsFromMap(payload ? payload.charts : null);
    Object.keys(charts).forEach((chartIdRaw) => {
      const chartId = String(chartIdRaw || "").trim();
      if (!chartId) {
        return;
      }
      const rows = Array.isArray(charts[chartIdRaw]) ? charts[chartIdRaw] : [];
      if (bucketKey === null) {
        chartBaseRowsMap[chartId] = rows;
        return;
      }
      let bucketMap = chartBucketRowsMap.get(chartId);
      if (!bucketMap) {
        bucketMap = new Map();
        chartBucketRowsMap.set(chartId, bucketMap);
      }
      bucketMap.set(bucketKey, rows);
    });
  }

  function getChartBucketOptions(chartId, rows) {
    const manifestOptions = chartBucketOptionsByChart.get(chartId);
    if (Array.isArray(manifestOptions) && manifestOptions.length) {
      return manifestOptions.slice();
    }
    return uniqueBucketOptions(rows);
  }

  function resolveBucketTarget(options) {
    if (!options.length) {
      return { bucket: null, note: "" };
    }
    let target = state.activeBucket;
    if (!Number.isFinite(target)) {
      if (Number.isFinite(state.defaultBucket) && options.includes(state.defaultBucket)) {
        target = state.defaultBucket;
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

  function chartShardEntryForAnalysis(analysisId) {
    const key = String(analysisId || "").trim();
    if (!key) {
      return null;
    }
    const direct = chartShardManifestByAnalysis[key];
    if (direct && typeof direct === "object") {
      return direct;
    }
    return null;
  }

  async function ensureAnalysisBaseLoaded(analysisId) {
    const key = String(analysisId || "").trim();
    if (!key) {
      return;
    }
    const manifest = chartShardEntryForAnalysis(key);
    if (!manifest || typeof manifest.base_url !== "string" || !manifest.base_url.trim()) {
      loadedAnalysisBase.add(key);
      return;
    }
    if (loadedAnalysisBase.has(key)) {
      return;
    }
    const inFlight = loadingAnalysisBase.get(key);
    if (inFlight) {
      await inFlight;
      return;
    }
    const promise = (async () => {
      const payload = await fetchJsonPayload(
        manifest.base_url,
        "base analysis shard for " + key
      );
      mergeChartShardPayload(payload, null);
      loadedAnalysisBase.add(key);
    })();
    loadingAnalysisBase.set(key, promise);
    try {
      await promise;
    } finally {
      loadingAnalysisBase.delete(key);
    }
  }

  function analysisBucketSet(analysisId) {
    const key = String(analysisId || "").trim();
    let bucketSet = loadedAnalysisBuckets.get(key);
    if (!bucketSet) {
      bucketSet = new Set();
      loadedAnalysisBuckets.set(key, bucketSet);
    }
    return bucketSet;
  }

  function analysisBucketPendingMap(analysisId) {
    const key = String(analysisId || "").trim();
    let pendingMap = loadingAnalysisBuckets.get(key);
    if (!pendingMap) {
      pendingMap = new Map();
      loadingAnalysisBuckets.set(key, pendingMap);
    }
    return pendingMap;
  }

  async function ensureAnalysisBucketLoaded(analysisId, bucketMinutes) {
    const key = String(analysisId || "").trim();
    const bucketKey = normalizeBucketKey(bucketMinutes);
    if (!key || !bucketKey) {
      return;
    }
    const manifest = chartShardEntryForAnalysis(key);
    if (!manifest || typeof manifest !== "object") {
      return;
    }
    const bucketUrls =
      manifest.bucket_urls && typeof manifest.bucket_urls === "object"
        ? manifest.bucket_urls
        : {};
    const targetUrl =
      typeof bucketUrls[bucketKey] === "string" ? bucketUrls[bucketKey].trim() : "";
    if (!targetUrl) {
      return;
    }
    const loadedBuckets = analysisBucketSet(key);
    if (loadedBuckets.has(bucketKey)) {
      return;
    }
    const pendingMap = analysisBucketPendingMap(key);
    const inFlight = pendingMap.get(bucketKey);
    if (inFlight) {
      await inFlight;
      return;
    }
    const promise = (async () => {
      const payload = await fetchJsonPayload(
        targetUrl,
        "bucket " + bucketKey + "m shard for " + key
      );
      mergeChartShardPayload(payload, bucketKey);
      loadedBuckets.add(bucketKey);
    })();
    pendingMap.set(bucketKey, promise);
    try {
      await promise;
    } finally {
      pendingMap.delete(bucketKey);
    }
  }

  function resolvedBucketsForAnalysis(analysisId) {
    const manifest = chartShardEntryForAnalysis(analysisId);
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
      const rows = Array.isArray(chartBaseRowsMap[chartId]) ? chartBaseRowsMap[chartId] : [];
      const options = getChartBucketOptions(chartId, rows);
      const selection = resolveBucketTarget(options);
      if (Number.isFinite(selection.bucket)) {
        resolved.add(Math.round(selection.bucket));
      }
    });
    return Array.from(resolved).sort((left, right) => left - right);
  }

  function analysisNeedsShardLoad(analysisId) {
    const key = String(analysisId || "").trim();
    if (!key) {
      return false;
    }
    const manifest = chartShardEntryForAnalysis(key);
    if (!manifest || typeof manifest !== "object") {
      return false;
    }
    if (!loadedAnalysisBase.has(key)) {
      return true;
    }
    const buckets = resolvedBucketsForAnalysis(key);
    if (!buckets.length) {
      return false;
    }
    const loadedBuckets = analysisBucketSet(key);
    return buckets.some((bucket) => !loadedBuckets.has(String(Math.round(bucket))));
  }

  async function ensureAnalysisDataLoaded(analysisId) {
    const key = String(analysisId || "").trim();
    if (!key) {
      return;
    }
    const manifest = chartShardEntryForAnalysis(key);
    if (!manifest || typeof manifest !== "object") {
      return;
    }
    await ensureAnalysisBaseLoaded(key);
    const buckets = resolvedBucketsForAnalysis(key);
    if (!buckets.length) {
      return;
    }
    await Promise.all(buckets.map((bucket) => ensureAnalysisBucketLoaded(key, bucket)));
  }

  function getChartRows(chartId) {
    const baseRows = Array.isArray(chartBaseRowsMap[chartId]) ? chartBaseRowsMap[chartId] : [];
    const bucketMap = chartBucketRowsMap.get(chartId);
    if (!bucketMap || !bucketMap.size) {
      return baseRows;
    }
    const options = getChartBucketOptions(chartId, baseRows);
    if (!options.length) {
      const mergedRows = [];
      bucketMap.forEach((rows) => {
        if (Array.isArray(rows) && rows.length) {
          mergedRows.push(...rows);
        }
      });
      return baseRows.concat(mergedRows);
    }
    const selection = resolveBucketTarget(options);
    if (!Number.isFinite(selection.bucket)) {
      return baseRows;
    }
    const key = String(Math.round(selection.bucket));
    const bucketRows = bucketMap.get(key);
    return baseRows.concat(Array.isArray(bucketRows) ? bucketRows : []);
  }

  function uniqueBucketOptions(rows) {
    const values = new Set();
    rows.forEach((row) => {
      const value = toFiniteNumberOrNull(row.bucket_minutes);
      if (value !== null) {
        values.add(value);
      }
    });
    return Array.from(values).sort((left, right) => left - right);
  }

  function shouldRetainBucketlessRowsForChart(chartId) {
    return chartId === "duplicates_exact_top_name_timing_exact";
  }

  function filterRowsByBucket(rows, chartId) {
    const options = getChartBucketOptions(chartId, rows);
    if (!options.length) {
      return { rows: rows, bucket: null, options: options, note: "" };
    }

    const selection = resolveBucketTarget(options);
    const target = selection.bucket;
    const keepBucketlessRows = shouldRetainBucketlessRowsForChart(chartId);
    return {
      rows: rows.filter((row) => {
        const bucketValue = toFiniteNumberOrNull(row.bucket_minutes);
        if (bucketValue === target) {
          return true;
        }
        return keepBucketlessRows && bucketValue === null;
      }),
      bucket: target,
      options: options,
      note: selection.note,
    };
  }

  function shouldRetainBucketlessRowsForDuplicateTable(tableName) {
    return tableName === "top_name_timing_by_mode";
  }

  function filterRowsByDuplicateTableBucket(tableName, rows) {
    const sourceRows = Array.isArray(rows) ? rows : [];
    if (!sourceRows.length || !Number.isFinite(state.activeBucket)) {
      return {
        rows: sourceRows,
        applied: false,
        note: "",
      };
    }

    const hasBucketField = sourceRows.some((row) =>
      Object.prototype.hasOwnProperty.call(row || {}, "bucket_minutes")
    );
    if (!hasBucketField) {
      return {
        rows: sourceRows,
        applied: false,
        note: "",
      };
    }

    const keepBucketlessRows = shouldRetainBucketlessRowsForDuplicateTable(tableName);
    const filteredRows = sourceRows.filter((row) => {
      const bucketMinutes = toFiniteNumberOrNull((row || {}).bucket_minutes);
      if (bucketMinutes === state.activeBucket) {
        return true;
      }
      return keepBucketlessRows && bucketMinutes === null;
    });
    if (filteredRows.length) {
      return {
        rows: filteredRows,
        applied: true,
        note: "",
      };
    }

    return {
      rows: sourceRows,
      applied: false,
      note:
        "Selected bucket " +
        Math.round(state.activeBucket) +
        "m is unavailable for this preview table; showing all rows.",
    };
  }

  function hasActiveZoomRange() {
    return (
      Number.isFinite(state.zoom.minTime) &&
      Number.isFinite(state.zoom.maxTime) &&
      state.zoom.maxTime > state.zoom.minTime
    );
  }

  function parseShortOffsetMinutes(rawValue) {
    const value = String(rawValue || "")
      .replace(/\u2212/g, "-")
      .trim()
      .toUpperCase();
    if (!value || value === "GMT" || value === "UTC") {
      return 0;
    }
    const match = value.match(/^(?:GMT|UTC)\s*([+-])(\d{1,2})(?::?(\d{2}))?$/);
    if (!match) {
      return null;
    }
    const sign = match[1] === "-" ? -1 : 1;
    const hours = Number(match[2]);
    const minutes = Number(match[3] || "0");
    if (!Number.isFinite(hours) || !Number.isFinite(minutes)) {
      return null;
    }
    return sign * (hours * 60 + minutes);
  }

  function timezoneOffsetMinutesAt(epochMillis, timezoneName) {
    if (!Number.isFinite(epochMillis) || !timezoneName) {
      return null;
    }
    try {
      const parts = new Intl.DateTimeFormat("en-US", {
        timeZone: timezoneName,
        timeZoneName: "shortOffset",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      }).formatToParts(new Date(epochMillis));
      const zonePart = parts.find((part) => part.type === "timeZoneName");
      if (!zonePart || typeof zonePart.value !== "string") {
        return null;
      }
      return parseShortOffsetMinutes(zonePart.value);
    } catch (_error) {
      return null;
    }
  }

  function epochFromDateTimeInReportTimezone(dateValue, hourValue, minuteValue) {
    const dateText = String(dateValue || "").trim();
    const dateMatch = dateText.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    const hour = Math.max(0, Math.min(23, Math.round(toNumber(hourValue))));
    const minute = Math.max(0, Math.min(59, Math.round(toNumber(minuteValue))));
    if (!dateMatch) {
      return toEpochMillis(dateText);
    }
    const cacheKey =
      dateMatch[1] +
      "-" +
      dateMatch[2] +
      "-" +
      dateMatch[3] +
      "|" +
      String(hour) +
      "|" +
      String(minute) +
      "|" +
      reportTimezone;
    if (zonedDateTimeEpochCache.has(cacheKey)) {
      return zonedDateTimeEpochCache.get(cacheKey);
    }

    const year = Number(dateMatch[1]);
    const month = Number(dateMatch[2]);
    const day = Number(dateMatch[3]);
    if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
      return null;
    }

    const utcGuess = Date.UTC(year, month - 1, day, hour, minute, 0, 0);
    const firstOffset = timezoneOffsetMinutesAt(utcGuess, reportTimezone);
    let resolved =
      firstOffset === null ? null : utcGuess - firstOffset * 60 * 1000;
    if (resolved !== null) {
      const refinedOffset = timezoneOffsetMinutesAt(resolved, reportTimezone);
      if (refinedOffset !== null && refinedOffset !== firstOffset) {
        resolved = utcGuess - refinedOffset * 60 * 1000;
      }
    }

    zonedDateTimeEpochCache.set(cacheKey, resolved);
    return resolved;
  }

  function inferRowAbsoluteTimeRange(mount, row) {
    if (!row || typeof row !== "object") {
      return null;
    }

    const pointFields = [
      "bucket_start",
      "minute_bucket",
      "start_minute",
      "change_minute",
      "peak_bucket_start",
    ];
    for (const field of pointFields) {
      if (!Object.prototype.hasOwnProperty.call(row, field)) {
        continue;
      }
      const timestamp = toEpochMillis(row[field]);
      if (timestamp !== null) {
        return { start: timestamp, end: timestamp };
      }
    }

    const firstSeen = toEpochMillis(row.first_seen);
    const lastSeen = toEpochMillis(row.last_seen);
    if (firstSeen !== null || lastSeen !== null) {
      const start = firstSeen !== null ? firstSeen : lastSeen;
      const end = lastSeen !== null ? lastSeen : firstSeen;
      return {
        start: Math.min(start, end),
        end: Math.max(start, end),
      };
    }

    const dateText = String(row.date || "").trim();
    if (dateText) {
      const hour = toFiniteNumberOrNull(row.hour);
      if (hour !== null) {
        const start = epochFromDateTimeInReportTimezone(dateText, hour, 0);
        if (start !== null) {
          return { start: start, end: start + 60 * 60 * 1000 - 1 };
        }
      }

      const slotStartMinute = toFiniteNumberOrNull(row.slot_start_minute);
      if (slotStartMinute !== null) {
        const roundedMinute = Math.max(
          0,
          Math.min(24 * 60 - 1, Math.round(slotStartMinute))
        );
        const slotHour = Math.floor(roundedMinute / 60);
        const minuteOfHour = roundedMinute % 60;
        const start = epochFromDateTimeInReportTimezone(
          dateText,
          slotHour,
          minuteOfHour
        );
        if (start !== null) {
          const bucketMinutes = toFiniteNumberOrNull(row.bucket_minutes);
          const fallbackMinutes = Number.isFinite(mount && mount.activeBucket)
            ? mount.activeBucket
            : 60;
          const spanMinutes = Math.max(
            1,
            Math.round(bucketMinutes !== null ? bucketMinutes : fallbackMinutes)
          );
          return {
            start: start,
            end: start + spanMinutes * 60 * 1000 - 1,
          };
        }
      }

      const dayStart = epochFromDateTimeInReportTimezone(dateText, 0, 0);
      if (dayStart !== null) {
        return {
          start: dayStart,
          end: dayStart + 24 * 60 * 60 * 1000 - 1,
        };
      }
    }

    return null;
  }

  function chartUsesLinkedZoomRowFilter(mount, rows) {
    if (!mount || state.absoluteTimeSet.has(mount.chartId)) {
      return false;
    }
    if (linkedZoomFilterChartIds.has(mount.chartId)) {
      return true;
    }
    return Array.isArray(rows)
      ? rows.some((row) => inferRowAbsoluteTimeRange(mount, row) !== null)
      : false;
  }

  function overlapsWindow(startA, endA, startB, endB) {
    const leftStart = toFiniteNumberOrNull(startA);
    const leftEnd = toFiniteNumberOrNull(endA);
    const rightStart = toFiniteNumberOrNull(startB);
    const rightEnd = toFiniteNumberOrNull(endB);
    if (
      leftStart === null ||
      leftEnd === null ||
      rightStart === null ||
      rightEnd === null
    ) {
      return false;
    }
    const normalizedLeftStart = Math.min(leftStart, leftEnd);
    const normalizedLeftEnd = Math.max(leftStart, leftEnd);
    const normalizedRightStart = Math.min(rightStart, rightEnd);
    const normalizedRightEnd = Math.max(rightStart, rightEnd);
    return normalizedLeftStart <= normalizedRightEnd && normalizedRightStart <= normalizedLeftEnd;
  }

  function filterRowsByLinkedZoom(mount, rows) {
    if (!Array.isArray(rows) || !rows.length || !hasActiveZoomRange()) {
      return { rows: rows, note: "" };
    }
    if (!chartUsesLinkedZoomRowFilter(mount, rows)) {
      return { rows: rows, note: "" };
    }

    const start = state.zoom.minTime;
    const end = state.zoom.maxTime;
    const filtered = [];
    let scopedRows = 0;

    rows.forEach((row) => {
      const rowRange = inferRowAbsoluteTimeRange(mount, row);
      if (!rowRange) {
        return;
      }
      scopedRows += 1;
      if (overlapsWindow(rowRange.start, rowRange.end, start, end)) {
        filtered.push(row);
      }
    });

    if (!scopedRows) {
      return { rows: rows, note: "" };
    }

    const zoomPrefix =
      "Linked zoom filter: " +
      formatEpochMillis(start) +
      " to " +
      formatEpochMillis(end) +
      ".";
    if (!filtered.length) {
      return { rows: [], note: zoomPrefix + " No rows remain in this range." };
    }

    return {
      rows: filtered,
      note:
        zoomPrefix +
        " Showing " +
        filtered.length.toLocaleString() +
        "/" +
        scopedRows.toLocaleString() +
        " rows.",
    };
  }

  function appendCursorMarkLine(baseLines) {
    const theme = currentChartTheme();
    const lines = Array.isArray(baseLines) ? baseLines.slice() : [];
    if (Number.isFinite(state.cursorX)) {
      lines.push({
        xAxis: state.cursorX,
        lineStyle: { color: theme.cursor, width: 1.2, opacity: 0.95, type: "solid" },
        label: { show: false },
      });
    }
    if (!lines.length) {
      return { data: [] };
    }
    return {
      silent: true,
      symbol: ["none", "none"],
      data: lines,
    };
  }

  function updateCursorAcrossTimeCharts() {
    chartMounts.forEach((mount) => {
      if (!mount.isTimeSeries || !mount.chart || !mount.seriesId) {
        return;
      }
      const markLine = appendCursorMarkLine(mount.baseMarkLines || []);
      try {
        mount.chart.setOption({
          series: [{ id: mount.seriesId, markLine: markLine }],
        });
      } catch (_error) {}
    });

    const cursorNote = document.getElementById("cursor-sync-note");
    if (cursorNote) {
      cursorNote.textContent = "";
    }
  }

  function clearChartInteractionState(mount) {
    if (!mount || !mount.chart) {
      return;
    }
    try {
      mount.chart.dispatchAction({ type: "hideTip" });
    } catch (_error) {}
    try {
      mount.chart.dispatchAction({ type: "updateAxisPointer", currTrigger: "leave" });
    } catch (_error) {}
  }

  function clearAllChartInteractionState() {
    chartMounts.forEach((mount) => clearChartInteractionState(mount));
  }

  function extractCursorFromEvent(chart, params) {
    if (!params || !chart) {
      return null;
    }
    if (Object.prototype.hasOwnProperty.call(params, "value")) {
      const value = params.value;
      if (Array.isArray(value) && value.length) {
        const parsed = toEpochMillis(value[0]);
        if (parsed !== null) {
          return parsed;
        }
      }
      const parsedDirect = toEpochMillis(value);
      if (parsedDirect !== null) {
        return parsedDirect;
      }
    }

    if (!params.event) {
      return null;
    }
    const evt = params.event.event || params.event;
    if (!evt || !Number.isFinite(evt.offsetX) || !Number.isFinite(evt.offsetY)) {
      return null;
    }
    try {
      if (!chart.containPixel({ gridIndex: 0 }, [evt.offsetX, evt.offsetY])) {
        return null;
      }
      const converted = chart.convertFromPixel({ gridIndex: 0 }, [evt.offsetX, evt.offsetY]);
      const raw = Array.isArray(converted) ? converted[0] : converted;
      return toEpochMillis(raw);
    } catch (_error) {
      return null;
    }
  }

  function attachCursorHandlers(mount) {
    if (!mount.chart || !mount.isTimeSeries) {
      return;
    }
    mount.chart.on("click", (params) => {
      const xValue = extractCursorFromEvent(mount.chart, params);
      if (xValue !== null) {
        state.cursorX = xValue;
        updateCursorAcrossTimeCharts();
      }
    });
  }

  function extractFunnelCursorFromEvent(params) {
    if (!params) {
      return null;
    }
    const pointData = params && params.data && typeof params.data === "object" ? params.data : null;
    if (pointData && pointData.meta && typeof pointData.meta === "object") {
      const fromMeta = toEpochMillis(pointData.meta.bucketStart);
      if (fromMeta !== null) {
        return fromMeta;
      }
    }
    const valueCandidate = pointData && Object.prototype.hasOwnProperty.call(pointData, "value")
      ? pointData.value
      : params.value;
    if (Array.isArray(valueCandidate) && valueCandidate.length > 2) {
      const fromValue = toEpochMillis(valueCandidate[2]);
      if (fromValue !== null) {
        return fromValue;
      }
    }
    return null;
  }

  function attachFunnelCursorHandler(mount) {
    if (
      !mount ||
      !mount.chart ||
      mount.chartId !== "off_hours_funnel_plot" ||
      toBool(mount.hasFunnelClickHandler)
    ) {
      return;
    }
    mount.chart.on("click", (params) => {
      const timestamp = extractFunnelCursorFromEvent(params);
      if (timestamp === null) {
        return;
      }
      state.cursorX = timestamp;
      updateCursorAcrossTimeCharts();
    });
    mount.hasFunnelClickHandler = true;
  }

  function extentFromRows(rows, timeField) {
    const values = rows
      .map((row) => toEpochMillis(row[timeField]))
      .filter((value) => Number.isFinite(value));
    if (!values.length) {
      return null;
    }
    return {
      min: Math.min.apply(null, values),
      max: Math.max.apply(null, values),
    };
  }

  function firstQueryParam(searchParams, names) {
    for (const name of names) {
      const value = searchParams.get(name);
      if (typeof value === "string" && value.trim()) {
        return value.trim();
      }
    }
    return null;
  }

  function parseZoomEpoch(value) {
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

  function parseBucketFromQueryParams(availableOptions) {
    if (typeof window.URLSearchParams === "undefined") {
      return null;
    }
    const params = new window.URLSearchParams(window.location.search || "");
    const raw = firstQueryParam(params, [
      "bucket",
      "bucket_minutes",
      "linked_bucket_minutes",
    ]);
    const parsed = toFiniteNumberOrNull(raw);
    if (parsed === null) {
      return null;
    }
    const rounded = Math.max(1, Math.round(parsed));
    if (!Array.isArray(availableOptions) || !availableOptions.length) {
      return rounded;
    }
    if (availableOptions.includes(rounded)) {
      return rounded;
    }
    return null;
  }

  function parseLinkedZoomFromQueryParams() {
    if (typeof window.URLSearchParams === "undefined") {
      return null;
    }
    const params = new window.URLSearchParams(window.location.search || "");
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

  function parseDuplicateOptionFromQueryParams(names, options) {
    if (typeof window.URLSearchParams === "undefined") {
      return null;
    }
    if (!Array.isArray(options) || !options.length) {
      return null;
    }
    const params = new window.URLSearchParams(window.location.search || "");
    const raw = firstQueryParam(params, names);
    if (!raw) {
      return null;
    }
    return options.includes(raw) ? raw : null;
  }

  function collectAbsoluteTimeExtent() {
    let min = null;
    let max = null;
    chartMounts.forEach((mount) => {
      if (!mount || !mount.isAbsoluteTime || !mount.timeExtent) {
        return;
      }
      const extent = mount.timeExtent;
      if (!Number.isFinite(extent.min) || !Number.isFinite(extent.max)) {
        return;
      }
      min = min === null ? extent.min : Math.min(min, extent.min);
      max = max === null ? extent.max : Math.max(max, extent.max);
    });
    if (!Number.isFinite(min) || !Number.isFinite(max) || max <= min) {
      return null;
    }
    return { min: min, max: max };
  }

  function initializeLinkedZoomOnLoad() {
    if (!state.absoluteTimeSet.size) {
      return;
    }

    const fullExtent = collectAbsoluteTimeExtent();
    const queryRange = parseLinkedZoomFromQueryParams();
    if (!queryRange) {
      propagateZoom(null, null, null, true);
      return;
    }

    let min = queryRange.min;
    let max = queryRange.max;
    if (fullExtent) {
      min = Math.max(min, fullExtent.min);
      max = Math.min(max, fullExtent.max);
    }
    if (!Number.isFinite(min) || !Number.isFinite(max) || max <= min) {
      propagateZoom(null, null, null, true);
      return;
    }
    propagateZoom(min, max, null, false);
  }

  function extractZoomRange(chart, extent) {
    const option = chart.getOption ? chart.getOption() : {};
    const dataZoom = Array.isArray(option.dataZoom) ? option.dataZoom : [];

    for (const item of dataZoom) {
      const startValue = toFiniteNumberOrNull(item.startValue);
      const endValue = toFiniteNumberOrNull(item.endValue);
      if (startValue !== null && endValue !== null && endValue > startValue) {
        if (
          extent &&
          Number.isFinite(extent.min) &&
          Number.isFinite(extent.max) &&
          Math.abs(startValue - extent.min) < 1 &&
          Math.abs(endValue - extent.max) < 1
        ) {
          return { min: extent.min, max: extent.max, reset: true };
        }
        return { min: startValue, max: endValue, reset: false };
      }
    }

    if (!extent) {
      return null;
    }
    for (const item of dataZoom) {
      const start = toFiniteNumberOrNull(item.start);
      const end = toFiniteNumberOrNull(item.end);
      if (start === null || end === null) {
        continue;
      }
      if (start <= 0 && end >= 100) {
        return { min: extent.min, max: extent.max, reset: true };
      }
      const span = extent.max - extent.min;
      if (!(span > 0)) {
        continue;
      }
      const min = extent.min + (Math.max(0, Math.min(100, start)) / 100) * span;
      const max = extent.min + (Math.max(0, Math.min(100, end)) / 100) * span;
      if (max > min) {
        return { min: min, max: max, reset: false };
      }
    }
    return null;
  }

  function applyZoomToChart(mount) {
    if (!mount || !mount.chart || !mount.isAbsoluteTime) {
      return;
    }
    if (
      !Number.isFinite(state.zoom.minTime) ||
      !Number.isFinite(state.zoom.maxTime) ||
      state.zoom.maxTime <= state.zoom.minTime
    ) {
      mount.chart.dispatchAction({ type: "dataZoom", xAxisIndex: 0, start: 0, end: 100 });
      return;
    }
    mount.chart.dispatchAction({
      type: "dataZoom",
      xAxisIndex: 0,
      startValue: state.zoom.minTime,
      endValue: state.zoom.maxTime,
    });
  }

  function updateZoomRangeLabel() {
    const panel = document.getElementById("zoom-sync-panel");
    const label = document.getElementById("zoom-range-label");
    const statusChip = document.getElementById("zoom-status-chip");
    const resetButton = document.getElementById("zoom-reset-button");
    if (!panel || !label || !resetButton) {
      return;
    }
    if (!state.absoluteTimeSet.size) {
      panel.classList.add("hidden");
      panel.classList.remove("zoom-state-active");
      if (statusChip) {
        statusChip.classList.remove("is-active");
        statusChip.textContent = "All";
      }
      updateSidebarFloatingOffsets();
      return;
    }
    panel.classList.remove("hidden");

    const hasZoom = hasActiveZoomRange();
    document.body.classList.toggle("zoom-range-active", hasZoom);
    panel.classList.toggle("zoom-state-active", hasZoom);
    resetButton.disabled = !hasZoom;
    if (statusChip) {
      statusChip.classList.toggle("is-active", hasZoom);
      if (!hasZoom) {
        statusChip.textContent = "All";
      }
    }
    if (!hasZoom) {
      label.textContent = "Full timeline";
      updateSidebarFloatingOffsets();
      return;
    }

    const rangeText =
      formatZoomRangeEpochMillis(state.zoom.minTime) +
      " to " +
      formatZoomRangeEpochMillis(state.zoom.maxTime);
    if (statusChip) {
      statusChip.textContent = "Zoom";
    }
    label.textContent = rangeText;
    updateSidebarFloatingOffsets();
  }

  function propagateZoom(minTime, maxTime, sourceChartId, isReset) {
    if (state.zoom.syncing) {
      return;
    }
    const hasRange =
      Number.isFinite(minTime) && Number.isFinite(maxTime) && maxTime > minTime;
    if (!isReset && !hasRange) {
      return;
    }
    state.zoom.syncing = true;
    try {
      if (isReset) {
        state.zoom.minTime = null;
        state.zoom.maxTime = null;
      } else {
        state.zoom.minTime = minTime;
        state.zoom.maxTime = maxTime;
      }
      chartMounts.forEach((mount, chartId) => {
        if (!mount.isAbsoluteTime || chartId === sourceChartId) {
          return;
        }
        applyZoomToChart(mount);
      });
      rerenderLinkedZoomAwareCharts(sourceChartId);
    } finally {
      state.zoom.syncing = false;
      updateZoomRangeLabel();
      syncControlOverridesToUrl();
    }
  }

  function scheduleZoomSync(minTime, maxTime, sourceChartId, isReset) {
    state.zoom.pending = {
      minTime: minTime,
      maxTime: maxTime,
      sourceChartId: sourceChartId,
      isReset: isReset,
    };
    if (state.zoom.raf !== null) {
      return;
    }
    state.zoom.raf = window.requestAnimationFrame(() => {
      state.zoom.raf = null;
      const pending = state.zoom.pending;
      state.zoom.pending = null;
      if (!pending) {
        return;
      }
      propagateZoom(pending.minTime, pending.maxTime, pending.sourceChartId, pending.isReset);
    });
  }

  function attachZoomHandlers(mount) {
    if (!mount.chart || !mount.isAbsoluteTime) {
      return;
    }
    mount.chart.on("dataZoom", () => {
      if (state.zoom.syncing) {
        return;
      }
      const range = extractZoomRange(mount.chart, mount.timeExtent);
      if (!range) {
        return;
      }
      scheduleZoomSync(range.min, range.max, mount.chartId, range.reset);
    });
  }

  function chartTitleFor(chartId) {
    return String(chartId || "").replace(/_/g, " ");
  }

  function renderTimeBarLine(mount, rows, config) {
    const theme = currentChartTheme();
    const timeField = config.timeField;
    const barField = config.barField;
    const lineField = config.lineField;
    const lineLow = config.lineLow;
    const lineHigh = config.lineHigh;
    const extraLines = Array.isArray(config.extraLines) ? config.extraLines : [];
    const lowPowerField = config.lowPowerField;
    const flaggedField = config.flaggedField;
    const inferentialWindowField =
      typeof config.inferentialWindowField === "string"
        ? config.inferentialWindowField.trim()
        : "";
    const sparseWhenLowSupport = toBool(config.sparseWhenLowSupport);
    const sparseMinTestedPoints = Math.max(
      3,
      Math.round(toNumber(config.sparseMinTestedPoints || 8))
    );
    const sparseMinTestedShare = Math.max(
      0,
      Math.min(1, toNumber(config.sparseMinTestedShare || 0.35))
    );
    const adaptiveLineRange = toBool(config.adaptiveLineRange);
    const adaptiveLineRangePadding =
      toFiniteNumberOrNull(config.adaptiveLineRangePadding) === null
        ? 0.12
        : Math.max(0, toNumber(config.adaptiveLineRangePadding));
    const adaptiveLineRangeMinSpan =
      toFiniteNumberOrNull(config.adaptiveLineRangeMinSpan) === null
        ? 0.08
        : Math.max(0.02, toNumber(config.adaptiveLineRangeMinSpan));
    const adaptiveLineRangeClampMin = toFiniteNumberOrNull(config.adaptiveLineRangeClampMin);
    const adaptiveLineRangeClampMax = toFiniteNumberOrNull(config.adaptiveLineRangeClampMax);
    const runOverlayField =
      typeof config.runOverlayField === "string" ? config.runOverlayField.trim() : "";
    const denseOffHoursMarkers =
      mount.chartId === "off_hours_control_timeline" ||
      mount.chartId === "off_hours_primary_residual_timeline";

    const sorted = rows
      .map((row) => Object.assign({}, row, { __time: toEpochMillis(row[timeField]) }))
      .filter((row) => row.__time !== null)
      .sort((left, right) => left.__time - right.__time);

    if (!sorted.length) {
      return false;
    }

    mount.timeExtent = extentFromRows(sorted, "__time");
    mount.customChartNote = null;
    const mainSeriesId = "main-" + mount.chartId;

    const inferentialRows =
      lineField && inferentialWindowField
        ? sorted.filter(
            (row) =>
              toBool(row[inferentialWindowField]) &&
              toFiniteNumberOrNull(row[lineField]) !== null
          )
        : [];
    const testedInferentialRows =
      lineField && inferentialWindowField && lowPowerField
        ? inferentialRows.filter((row) => !toBool(row[lowPowerField]))
        : inferentialRows.slice();
    const inferentialCount = inferentialRows.length;
    const testedInferentialCount = testedInferentialRows.length;
    const testedInferentialShare =
      inferentialCount > 0 ? testedInferentialCount / inferentialCount : null;
    const sparseMode =
      sparseWhenLowSupport &&
      inferentialCount > 0 &&
      (testedInferentialCount < sparseMinTestedPoints ||
        (testedInferentialShare !== null && testedInferentialShare < sparseMinTestedShare));
    const noInferentialSupport =
      sparseWhenLowSupport && inferentialCount > 0 && testedInferentialCount === 0;

    const includeInPrimarySeries = (row) => {
      if (!lineField || !sparseMode) {
        return true;
      }
      if (inferentialWindowField && !toBool(row[inferentialWindowField])) {
        return false;
      }
      if (lowPowerField && toBool(row[lowPowerField])) {
        return false;
      }
      return true;
    };

    if (lineField && sparseWhenLowSupport) {
      if (inferentialCount === 0) {
        mount.customChartNote =
          "No alert-eligible windows were available for inferential off-hours testing in this bucket.";
      } else if (noInferentialSupport) {
        mount.customChartNote =
          "Descriptive-only: 0/" +
          inferentialCount.toLocaleString() +
          " alert-eligible windows were inferentially tested after low-power filtering.";
      } else if (sparseMode) {
        mount.customChartNote =
          "Sparse inferential support: showing tested windows as points with Wilson bounds (" +
          testedInferentialCount.toLocaleString() +
          "/" +
          inferentialCount.toLocaleString() +
          " tested).";
      }
    }

    const barData = barField
      ? sorted.map((row) => [row.__time, toFiniteNumberOrNull(row[barField])])
      : [];
    const lineData = lineField
      ? sorted.map((row) => [
          row.__time,
          includeInPrimarySeries(row) ? toFiniteNumberOrNull(row[lineField]) : null,
        ])
      : [];
    const lowData = lineLow
      ? sorted.map((row) => [
          row.__time,
          includeInPrimarySeries(row) ? toFiniteNumberOrNull(row[lineLow]) : null,
        ])
      : [];
    const highData = lineHigh
      ? sorted.map((row) => [
          row.__time,
          includeInPrimarySeries(row) ? toFiniteNumberOrNull(row[lineHigh]) : null,
        ])
      : [];
    const sparsePointData =
      sparseMode && lineField
        ? sorted
            .filter(
              (row) =>
                includeInPrimarySeries(row) &&
                toFiniteNumberOrNull(row[lineField]) !== null
            )
            .map((row) => [row.__time, toFiniteNumberOrNull(row[lineField])])
        : [];
    const sparseErrorbarData =
      sparseMode && lineLow && lineHigh
        ? sorted
            .filter(
              (row) =>
                includeInPrimarySeries(row) &&
                toFiniteNumberOrNull(row[lineLow]) !== null &&
                toFiniteNumberOrNull(row[lineHigh]) !== null
            )
            .map((row) => [
              row.__time,
              toFiniteNumberOrNull(row[lineLow]),
              toFiniteNumberOrNull(row[lineHigh]),
            ])
        : [];

    const lowPowerData = lowPowerField
      ? sorted
          .filter((row) => toBool(row[lowPowerField]) && toFiniteNumberOrNull(row[lineField]) !== null)
          .map((row) => [row.__time, toFiniteNumberOrNull(row[lineField])])
      : [];
    const robustLowerData = [];
    const robustUpperData = [];
    const genericFlagData = [];
    const spcOnlyData = [];
    const fdrOnlyData = [];
    const classifyDirection = (row, value) => {
      if (toBool(row.is_match_rate_alert_lower)) {
        return "lower";
      }
      if (toBool(row.is_match_rate_alert_upper)) {
        return "upper";
      }
      if (toBool(row.is_primary_lower_alert_window)) {
        return "lower";
      }
      if (toBool(row.is_primary_upper_alert_window)) {
        return "upper";
      }
      if (toBool(row.is_material_primary_lower_shift) || toBool(row.is_significant_primary_lower)) {
        return "lower";
      }
      if (toBool(row.is_material_primary_upper_shift) || toBool(row.is_significant_primary_upper)) {
        return "upper";
      }
      if (toFiniteNumberOrNull(row.match_rate_delta_global) !== null) {
        return toNumber(row.match_rate_delta_global) < 0 ? "lower" : "upper";
      }
      if (value !== null && value < 0) {
        return "lower";
      }
      return "upper";
    };
    if (lineField) {
      sorted.forEach((row) => {
        const value = toFiniteNumberOrNull(row[lineField]);
        if (value === null) {
          return;
        }
        const point = [row.__time, value];
        const isLowPowerRow = lowPowerField ? toBool(row[lowPowerField]) : false;
        const isSpcOnly =
          !isLowPowerRow &&
          toBool(row.is_primary_spc_998_two_sided) &&
          !toBool(row.is_primary_fdr_two_sided);
        const isFdrOnly =
          !isLowPowerRow &&
          toBool(row.is_primary_fdr_two_sided) &&
          !toBool(row.is_primary_spc_998_two_sided);
        if (isSpcOnly) {
          spcOnlyData.push(point);
        }
        if (isFdrOnly) {
          fdrOnlyData.push(point);
        }
        if (!flaggedField || !toBool(row[flaggedField])) {
          return;
        }
        if (isLowPowerRow) {
          genericFlagData.push(point);
          return;
        }
        const direction = classifyDirection(row, value);
        if (direction === "lower") {
          robustLowerData.push(point);
        } else if (direction === "upper") {
          robustUpperData.push(point);
        } else {
          genericFlagData.push(point);
        }
      });
    }
    const bucketByTime = new Map();
    sorted.forEach((row) => {
      if (bucketByTime.has(row.__time)) {
        return;
      }
      const bucketMinutes = toFiniteNumberOrNull(row.bucket_minutes);
      if (bucketMinutes !== null) {
        bucketByTime.set(row.__time, bucketMinutes);
      }
    });

    const resolveAdaptiveLineAxisRange = (fields) => {
      const candidateFields = Array.from(
        new Set(
          (Array.isArray(fields) ? fields : [])
            .map((field) => (typeof field === "string" ? field.trim() : ""))
            .filter((field) => !!field)
        )
      );
      if (!candidateFields.length) {
        return null;
      }
      const values = [];
      sorted.forEach((row) => {
        candidateFields.forEach((field) => {
          const value = toFiniteNumberOrNull(row[field]);
          if (value !== null) {
            values.push(value);
          }
        });
      });
      if (!values.length) {
        return null;
      }

      let minValue = Math.min.apply(null, values);
      let maxValue = Math.max.apply(null, values);
      if (!(Number.isFinite(minValue) && Number.isFinite(maxValue))) {
        return null;
      }

      if (maxValue > minValue) {
        const span = maxValue - minValue;
        const padding = Math.max(span * adaptiveLineRangePadding, adaptiveLineRangeMinSpan * 0.2);
        minValue -= padding;
        maxValue += padding;
      } else {
        minValue -= adaptiveLineRangeMinSpan / 2;
        maxValue += adaptiveLineRangeMinSpan / 2;
      }

      if (adaptiveLineRangeClampMin !== null) {
        minValue = Math.max(adaptiveLineRangeClampMin, minValue);
      }
      if (adaptiveLineRangeClampMax !== null) {
        maxValue = Math.min(adaptiveLineRangeClampMax, maxValue);
      }

      if (!(maxValue > minValue)) {
        return null;
      }

      if (maxValue - minValue < adaptiveLineRangeMinSpan) {
        const midpoint = (minValue + maxValue) / 2;
        minValue = midpoint - adaptiveLineRangeMinSpan / 2;
        maxValue = midpoint + adaptiveLineRangeMinSpan / 2;
        if (adaptiveLineRangeClampMin !== null && minValue < adaptiveLineRangeClampMin) {
          maxValue += adaptiveLineRangeClampMin - minValue;
          minValue = adaptiveLineRangeClampMin;
        }
        if (adaptiveLineRangeClampMax !== null && maxValue > adaptiveLineRangeClampMax) {
          minValue -= maxValue - adaptiveLineRangeClampMax;
          maxValue = adaptiveLineRangeClampMax;
        }
        if (adaptiveLineRangeClampMin !== null) {
          minValue = Math.max(adaptiveLineRangeClampMin, minValue);
        }
        if (adaptiveLineRangeClampMax !== null) {
          maxValue = Math.min(adaptiveLineRangeClampMax, maxValue);
        }
      }

      if (!(maxValue > minValue)) {
        return null;
      }
      return { min: minValue, max: maxValue };
    };

    const lineAxisBase = {
      type: "value",
      name: config.lineAxisName || "Value",
    };
    const adaptiveLineAxisFields = Array.isArray(config.adaptiveLineRangeFields)
      ? config.adaptiveLineRangeFields
      : [lineField, lineLow, lineHigh].concat(extraLines);
    const adaptiveLineAxisRange = adaptiveLineRange
      ? resolveAdaptiveLineAxisRange(adaptiveLineAxisFields)
      : null;
    const configuredLineMin = toFiniteNumberOrNull(config.lineMin);
    const configuredLineMax = toFiniteNumberOrNull(config.lineMax);
    const lineAxisMin = adaptiveLineAxisRange ? adaptiveLineAxisRange.min : configuredLineMin;
    const lineAxisMax = adaptiveLineAxisRange ? adaptiveLineAxisRange.max : configuredLineMax;
    if (lineAxisMin !== null) {
      lineAxisBase.min = lineAxisMin;
    }
    if (lineAxisMax !== null) {
      lineAxisBase.max = lineAxisMax;
    }
    if (adaptiveLineAxisRange || toBool(config.lineScale)) {
      lineAxisBase.scale = true;
    }

    const option = {
      animation: false,
      color: theme.seriesPalette,
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross" },
        formatter: (params) => {
          const entries = Array.isArray(params) ? params : [params];
          if (!entries.length) {
            return "";
          }
          const first = entries[0] || {};
          const rawValue = first.value;
          const axisRaw = Array.isArray(rawValue) ? rawValue[0] : first.axisValue;
          const timestamp = toEpochMillis(axisRaw);
          const lines = [];
          if (timestamp !== null) {
            lines.push(
              "<strong>Time (" +
                reportTimezoneLabel +
                "):</strong> " +
                formatEpochMillis(timestamp)
            );
            const bucketFromTime = bucketByTime.has(timestamp)
              ? bucketByTime.get(timestamp)
              : Number.isFinite(mount.activeBucket)
                ? mount.activeBucket
                : null;
            const bucketLabel = bucketLabelFromValue(bucketFromTime);
            if (bucketLabel) {
              lines.push("<strong>Bucket:</strong> " + bucketLabel);
            }
          }
          entries.forEach((entry) => {
            const entryValue = Array.isArray(entry.value) ? entry.value[1] : entry.value;
            lines.push(
              (entry.marker || "") +
                "<strong>" +
                String(entry.seriesName || "value") +
                ":</strong> " +
                formatTooltipValue(entryValue)
            );
          });
          return lines.join("<br/>");
        },
      },
      legend: { bottom: 0 },
      grid: { left: 56, right: 56, top: 18, bottom: 88 },
      xAxis: { type: "time", name: "Time (" + reportTimezoneLabel + ")" },
      yAxis: [
        barField
          ? { type: "value", name: config.barAxisName || "Volume" }
          : Object.assign({}, lineAxisBase),
        Object.assign({}, lineAxisBase),
      ],
      dataZoom: [
        {
          type: "inside",
          xAxisIndex: [0],
          startValue: Number.isFinite(state.zoom.minTime) ? state.zoom.minTime : undefined,
          endValue: Number.isFinite(state.zoom.maxTime) ? state.zoom.maxTime : undefined,
        },
        {
          type: "slider",
          xAxisIndex: [0],
          bottom: 14,
          startValue: Number.isFinite(state.zoom.minTime) ? state.zoom.minTime : undefined,
          endValue: Number.isFinite(state.zoom.maxTime) ? state.zoom.maxTime : undefined,
        },
      ],
      series: [],
    };

      if (barField) {
      option.series.push({
        id: mainSeriesId,
        name: config.barSeriesName || "Volume",
        type: "bar",
        yAxisIndex: 0,
        data: barData,
        barMaxWidth: 11,
        itemStyle: { color: config.barColor || theme.volumeBar, opacity: theme.volumeBarOpacity },
        markLine: appendCursorMarkLine(mount.baseMarkLines || []),
      });
    } else {
      option.series.push({
        id: mainSeriesId,
        name: config.lineSeriesName || "Value",
        type: "line",
        yAxisIndex: 0,
        data: lineData,
        showSymbol: false,
        lineStyle: { color: config.lineColor || theme.primaryLine, width: 1.6 },
        markLine: appendCursorMarkLine(mount.baseMarkLines || []),
      });
    }

    if (lineField) {
      if (sparseMode) {
        option.series.push({
          name: (config.lineSeriesName || "Value") + " (tested windows)",
            type: "scatter",
            yAxisIndex: barField ? 1 : 0,
            data: sparsePointData,
            symbolSize: 7,
            itemStyle: { color: config.lineColor || theme.primaryLine, opacity: 0.9 },
          });
        if (sparseErrorbarData.length) {
          option.series.push({
            name: "Wilson low (tested)",
            type: "line",
            yAxisIndex: barField ? 1 : 0,
            data: sparseErrorbarData.map((row) => [row[0], row[1]]),
            showSymbol: false,
            lineStyle: {
              color: theme.intervalBand,
              width: denseOffHoursMarkers ? 0.9 : 1,
              opacity: denseOffHoursMarkers ? 0.52 : 0.7,
            },
          });
          option.series.push({
            name: "Wilson high (tested)",
            type: "line",
            yAxisIndex: barField ? 1 : 0,
            data: sparseErrorbarData.map((row) => [row[0], row[2]]),
            showSymbol: false,
            lineStyle: {
              color: theme.intervalBand,
              width: denseOffHoursMarkers ? 0.9 : 1,
              opacity: denseOffHoursMarkers ? 0.52 : 0.7,
            },
          });
        }
      } else {
        option.series.push({
          name: config.lineSeriesName || "Value",
          type: "line",
          yAxisIndex: barField ? 1 : 0,
          data: lineData,
          showSymbol: false,
          lineStyle: { color: config.lineColor || theme.primaryLine, width: 1.6 },
        });
      }
    }
    if (lineLow && !sparseMode) {
      option.series.push({
        name: "Wilson low",
        type: "line",
        yAxisIndex: barField ? 1 : 0,
        data: lowData,
        showSymbol: false,
        lineStyle: {
          color: theme.intervalBand,
          width: denseOffHoursMarkers ? 0.9 : 1,
          opacity: denseOffHoursMarkers ? 0.52 : 0.7,
        },
      });
    }
    if (lineHigh && !sparseMode) {
      option.series.push({
        name: "Wilson high",
        type: "line",
        yAxisIndex: barField ? 1 : 0,
        data: highData,
        showSymbol: false,
        lineStyle: {
          color: theme.intervalBand,
          width: denseOffHoursMarkers ? 0.9 : 1,
          opacity: denseOffHoursMarkers ? 0.52 : 0.7,
        },
      });
    }

    extraLines.forEach((field, index) => {
      const extraLineStyle = styleForExtraLine(field, index, mount.chartId);
      option.series.push({
        name: field.replace(/_/g, " "),
        type: "line",
        yAxisIndex: barField ? 1 : 0,
        data: sorted.map((row) => [row.__time, toFiniteNumberOrNull(row[field])]),
        showSymbol: false,
        lineStyle: {
          color: colorForExtraLine(field, index),
          width: extraLineStyle.width,
          type: extraLineStyle.type,
          opacity: extraLineStyle.opacity,
        },
      });
    });

    if (runOverlayField && lineField) {
      const runIntervals = [];
      let runStart = null;
      let prevTime = null;
      let prevBucketMinutes = null;

      const closeRun = () => {
        if (runStart === null || prevTime === null) {
          return;
        }
        const endPaddingMs =
          Math.max(1, toNumber(prevBucketMinutes || mount.activeBucket || 1)) * 60 * 1000;
        runIntervals.push({
          start: runStart,
          end: prevTime + endPaddingMs,
        });
        runStart = null;
        prevTime = null;
        prevBucketMinutes = null;
      };

      sorted.forEach((row) => {
        const flagged = toBool(row[runOverlayField]);
        const currentTime = row.__time;
        const currentBucketMinutes = toFiniteNumberOrNull(row.bucket_minutes);
        const expectedGapMs =
          Math.max(1, toNumber(currentBucketMinutes || mount.activeBucket || 1)) * 60 * 1000 * 1.5;

        if (!flagged) {
          closeRun();
          return;
        }

        const isContiguous =
          prevTime !== null && Number.isFinite(currentTime - prevTime)
            ? currentTime - prevTime <= expectedGapMs
            : false;

        if (runStart === null || !isContiguous) {
          closeRun();
          runStart = currentTime;
        }
        prevTime = currentTime;
        prevBucketMinutes = currentBucketMinutes;
      });
      closeRun();

      if (runIntervals.length) {
        option.series.push({
          name: "Robust alert run span",
          type: "line",
          yAxisIndex: barField ? 1 : 0,
          data: sorted.map((row) => [row.__time, null]),
          showSymbol: false,
          tooltip: { show: false },
          lineStyle: { opacity: 0 },
          markArea: {
            silent: true,
            itemStyle: { color: theme.alertBandFill },
            data: runIntervals.map((interval) => [
              { xAxis: interval.start },
              { xAxis: interval.end },
            ]),
          },
        });
      }
    }

    if (robustLowerData.length) {
      option.series.push({
        name: "Robust lower-tail alert",
        type: "scatter",
        yAxisIndex: barField ? 1 : 0,
        data: robustLowerData,
        symbol: "diamond",
        symbolSize: denseOffHoursMarkers ? 9 : 10,
        itemStyle: {
          color: theme.alertLower,
          borderColor: theme.axisLine,
          borderWidth: denseOffHoursMarkers ? 1.25 : 1.5,
          opacity: denseOffHoursMarkers ? 0.94 : 0.98,
        },
      });
    }
    if (robustUpperData.length) {
      option.series.push({
        name: "Robust upper-tail alert",
        type: "scatter",
        yAxisIndex: barField ? 1 : 0,
        data: robustUpperData,
        symbol: "triangle",
        symbolSize: denseOffHoursMarkers ? 9 : 10,
        itemStyle: {
          color: theme.alertUpper,
          borderColor: theme.axisLine,
          borderWidth: denseOffHoursMarkers ? 1.25 : 1.5,
          opacity: denseOffHoursMarkers ? 0.94 : 0.98,
        },
      });
    }
    if (spcOnlyData.length) {
      option.series.push({
        name: "SPC-only flag",
        type: "scatter",
        yAxisIndex: barField ? 1 : 0,
        data: spcOnlyData,
        symbol: "rect",
        symbolSize: denseOffHoursMarkers ? 7 : 8,
        itemStyle: {
          color: "rgba(0,0,0,0)",
          borderColor: theme.referenceLine,
          borderWidth: denseOffHoursMarkers ? 1.2 : 1.5,
          opacity: denseOffHoursMarkers ? 0.9 : 1,
        },
      });
    }
    if (fdrOnlyData.length) {
      option.series.push({
        name: "FDR-only flag",
        type: "scatter",
        yAxisIndex: barField ? 1 : 0,
        data: fdrOnlyData,
        symbol: "circle",
        symbolSize: denseOffHoursMarkers ? 7 : 8,
        itemStyle: {
          color: "rgba(0,0,0,0)",
          borderColor: theme.contextLine,
          borderWidth: denseOffHoursMarkers ? 1.2 : 1.5,
          opacity: denseOffHoursMarkers ? 0.9 : 1,
        },
      });
    }
    if (genericFlagData.length) {
      option.series.push({
        name: "Flagged",
        type: "scatter",
        yAxisIndex: barField ? 1 : 0,
        data: genericFlagData,
        symbol: "diamond",
        symbolSize: denseOffHoursMarkers ? 8 : 9,
        itemStyle: {
          color: theme.alertLower,
          borderColor: theme.axisLine,
          borderWidth: denseOffHoursMarkers ? 1 : 1.2,
          opacity: denseOffHoursMarkers ? 0.86 : 0.92,
        },
      });
    }
    if (lowPowerData.length) {
      option.series.push({
        name: "Low-power",
        type: "scatter",
        yAxisIndex: barField ? 1 : 0,
        data: lowPowerData,
        symbol: "triangle",
        symbolRotate: 180,
        symbolSize: denseOffHoursMarkers ? 7 : 8,
        itemStyle: {
          color: theme.lowPower,
          borderColor: theme.axisLine,
          borderWidth: denseOffHoursMarkers ? 0.9 : 1,
          opacity: denseOffHoursMarkers ? 0.9 : 0.96,
        },
      });
    }

    mount.chart.setOption(ensureReadableAxes(option, mount), true);
    mount.seriesId = mainSeriesId;
    mount.isTimeSeries = true;
    mount.isAbsoluteTime = state.absoluteTimeSet.has(mount.chartId);
    return true;
  }

  function renderShiftHeatmap(mount, rows) {
    const theme = currentChartTheme();
    const subset = rows
      .map((row) => ({
        date: String(row.date || ""),
        slot: toNumber(row.slot_start_minute),
        value: toFiniteNumberOrNull(row.delta_from_slot_pro_rate),
        outlier: toBool(row.is_slot_outlier),
      }))
      .filter((row) => row.date && row.value !== null);

    if (!subset.length) {
      return false;
    }

    const dates = Array.from(new Set(subset.map((row) => row.date))).sort();
    const slots = Array.from(new Set(subset.map((row) => row.slot))).sort((a, b) => a - b);
    const dateMap = new Map(dates.map((value, index) => [value, index]));
    const slotMap = new Map(slots.map((value, index) => [value, index]));

    const values = [];
    const outliers = [];
    subset.forEach((row) => {
      const x = slotMap.get(row.slot);
      const y = dateMap.get(row.date);
      if (typeof x !== "number" || typeof y !== "number") {
        return;
      }
      values.push([x, y, row.value]);
      if (row.outlier) {
        outliers.push([x, y, row.value]);
      }
    });

    const maxAbs = Math.max(
      0.05,
      ...values.map((entry) => Math.abs(toNumber(entry[2]))).filter((value) => Number.isFinite(value))
    );

    const option = {
      animation: false,
      tooltip: {
          position: "top",
          formatter: (params) => {
            if (!Array.isArray(params.value)) {
              return "";
            }
            const xIndex = params.value[0];
            const yIndex = params.value[1];
            const slotMinutes = slots[xIndex];
            const hour = Math.floor(slotMinutes / 60);
            const minute = slotMinutes % 60;
            const bucketLabel = bucketLabelFromValue(mount.activeBucket);
            return (
              "<strong>Time (" +
              reportTimezoneLabel +
              "):</strong> " +
              dates[yIndex] +
              " " +
              String(hour).padStart(2, "0") +
              ":" +
              String(minute).padStart(2, "0") +
              (bucketLabel ? "<br/><strong>Bucket:</strong> " + bucketLabel : "") +
              "<br/><strong>Delta:</strong> " +
              toNumber(params.value[2]).toFixed(4)
            );
          },
        },
      grid: { left: 72, right: 34, top: 20, bottom: 62 },
      xAxis: {
          type: "category",
          name: "Time of day (" + reportTimezoneLabel + ")",
          data: slots.map((slotMinutes) => {
            const hour = Math.floor(slotMinutes / 60);
            const minute = slotMinutes % 60;
            return String(hour).padStart(2, "0") + ":" + String(minute).padStart(2, "0");
          }),
          axisLabel: { interval: Math.ceil(slots.length / 18) },
          splitArea: { show: true },
        },
      yAxis: {
          type: "category",
          name: "Date",
          data: dates,
          inverse: true,
          splitArea: { show: true },
        },
      visualMap: {
          min: -maxAbs,
          max: maxAbs,
          calculable: true,
          orient: "horizontal",
          left: "center",
          bottom: 6,
          inRange: { color: theme.heatmapDiverging },
        },
      series: [
          {
            name: "shift",
            type: "heatmap",
            data: values,
            emphasis: { itemStyle: { shadowBlur: 10, shadowColor: theme.shadowColor } },
          },
          {
            name: "slot outlier",
            type: "scatter",
            data: outliers,
            symbolSize: 10,
            itemStyle: {
              color: theme.outlierPoint,
              borderColor: theme.axisLine,
              borderWidth: 1,
            },
          },
        ],
    };

    mount.chart.setOption(ensureReadableAxes(option, mount), true);

    mount.isTimeSeries = false;
    mount.isAbsoluteTime = false;
    return true;
  }

  function renderDayHourHeatmap(mount, rows, valueField) {
    const theme = currentChartTheme();
    const isProportion = String(valueField || "").toLowerCase() === "pro_rate";
    const subset = rows
      .map((row) => ({
        day: String(row.day_of_week || ""),
        hour: toNumber(row.hour),
        value: toFiniteNumberOrNull(row[valueField]),
      }))
      .filter((row) => row.day && row.value !== null);
    if (!subset.length) {
      return false;
    }

    const dayOrder = [
      "Monday",
      "Tuesday",
      "Wednesday",
      "Thursday",
      "Friday",
      "Saturday",
      "Sunday",
    ];
    const days = Array.from(new Set(subset.map((row) => row.day))).sort(
      (left, right) => dayOrder.indexOf(left) - dayOrder.indexOf(right)
    );
    const hours = Array.from(new Set(subset.map((row) => row.hour))).sort((a, b) => a - b);
    const dayMap = new Map(days.map((value, index) => [value, index]));
    const hourMap = new Map(hours.map((value, index) => [value, index]));

    const values = subset
      .map((row) => {
        const x = hourMap.get(row.hour);
        const y = dayMap.get(row.day);
        if (typeof x !== "number" || typeof y !== "number") {
          return null;
        }
        return [x, y, row.value];
      })
      .filter((entry) => entry !== null);

    const numericValues = values
      .map((entry) => toFiniteNumberOrNull(entry[2]))
      .filter((value) => value !== null);
    if (!numericValues.length) {
      return false;
    }
    let minValue = isProportion ? 0 : Math.min.apply(null, numericValues);
    let maxValue = isProportion ? 1 : Math.max.apply(null, numericValues);
    if (!isProportion && minValue === maxValue) {
      maxValue = minValue + 1;
    }

    const option = {
      animation: false,
      tooltip: {
          position: "top",
          formatter: (params) => {
            if (!Array.isArray(params.value)) {
              return "";
            }
            const bucketLabel = bucketLabelFromValue(mount.activeBucket);
            return (
              "<strong>Time (" +
              reportTimezoneLabel +
              "):</strong> " +
              days[params.value[1]] +
              " " +
              String(hours[params.value[0]]).padStart(2, "0") +
              ":00" +
              (bucketLabel ? "<br/><strong>Bucket:</strong> " + bucketLabel : "") +
              "<br/><strong>Value:</strong> " +
              toNumber(params.value[2]).toFixed(4)
            );
          },
        },
      grid: { left: 76, right: 30, top: 18, bottom: 56 },
      xAxis: {
          type: "category",
          name: "Hour (" + reportTimezoneLabel + ")",
          data: hours.map((hour) => String(hour).padStart(2, "0")),
          splitArea: { show: true },
        },
      yAxis: {
          type: "category",
          name: "Day of week",
          data: days,
          inverse: true,
          splitArea: { show: true },
        },
      visualMap: {
          min: minValue,
          max: maxValue,
          orient: "horizontal",
          left: "center",
          bottom: 6,
          calculable: true,
          text: isProportion ? ["Pro-leaning", "Con-leaning"] : undefined,
          inRange: isProportion
            ? { color: theme.heatmapRateDiverging || theme.heatmapDiverging }
            : { color: theme.heatmapRate },
        },
      series: [
          {
            type: "heatmap",
            data: values,
            emphasis: { itemStyle: { shadowBlur: 8, shadowColor: theme.shadowColor } },
          },
        ],
    };
    mount.chart.setOption(ensureReadableAxes(option, mount), true);
    mount.isTimeSeries = false;
    mount.isAbsoluteTime = false;
    return true;
  }

  function renderDateHourHeatmap(mount, rows, valueField, valueLabel, options) {
    const theme = currentChartTheme();
    const heatmapOptions =
      options && typeof options === "object" ? options : {};
    const scaleMode = String(heatmapOptions.scaleMode || "").toLowerCase();
    const isProportionField = String(valueField || "").toLowerCase() === "pro_rate";
    const highlightOffHoursAxis = toBool(heatmapOptions.highlightOffHoursAxis);
    const offHoursAxisThreshold = Math.min(
      1,
      Math.max(
        0,
        toFiniteNumberOrNull(heatmapOptions.offHoursAxisThreshold) === null
          ? 0.5
          : toNumber(heatmapOptions.offHoursAxisThreshold)
      )
    );
    const force24HourSlots = toBool(heatmapOptions.force24HourSlots);
    const showMissingOverlay =
      toBool(heatmapOptions.showMissingOverlay) ||
      String(valueField || "").toLowerCase() === "n_total";
    const divergingPositiveWarm = toBool(heatmapOptions.divergingPositiveWarm);
    const subset = rows
      .map((row) => ({
        date: String(row.date || ""),
        hour: toNumber(row.hour),
        value: toFiniteNumberOrNull(row[valueField]),
        nTotal: toFiniteNumberOrNull(row.n_total),
        nKnown: toFiniteNumberOrNull(row.n_known),
        nWindows: toFiniteNumberOrNull(row.n_windows),
        nWindowsTested: toFiniteNumberOrNull(row.n_windows_tested),
        nWindowsPrimaryAlert: toFiniteNumberOrNull(row.n_windows_primary_alert),
        expectedPrimary: toFiniteNumberOrNull(row.expected_pro_rate_primary),
        deltaPrimary: toFiniteNumberOrNull(row.delta_pro_rate_primary),
        zPrimaryMedian: toFiniteNumberOrNull(row.z_score_primary_median),
        zPrimaryAbsMax: toFiniteNumberOrNull(row.z_score_primary_abs_max),
        offHoursFraction: toFiniteNumberOrNull(row.off_hours_fraction),
        proRate: toFiniteNumberOrNull(row.pro_rate),
        lowPower: toBool(row.is_low_power),
      }))
      .filter((row) => row.date && Number.isFinite(row.hour));
    if (!subset.length) {
      return false;
    }

    const dates = Array.from(new Set(subset.map((row) => row.date))).sort();
    const hours = force24HourSlots
      ? Array.from({ length: 24 }, (_unused, index) => index)
      : Array.from(new Set(subset.map((row) => row.hour))).sort((a, b) => a - b);
    const dateMap = new Map(dates.map((value, index) => [value, index]));
    const hourMap = new Map(hours.map((value, index) => [value, index]));
    const cellLookup = new Map();

    const values = [];
    const lowPowerPoints = [];
    const highlightedHours = new Set();
    subset.forEach((row) => {
      const x = hourMap.get(row.hour);
      const y = dateMap.get(row.date);
      if (typeof x !== "number" || typeof y !== "number") {
        return;
      }
      const key = String(row.date) + "|" + String(row.hour);
      cellLookup.set(key, row);
      if (row.value === null) {
        return;
      }
      values.push([x, y, row.value]);
      if (row.lowPower) {
        lowPowerPoints.push([x, y, row.value]);
      }
      if (
        highlightOffHoursAxis &&
        row.offHoursFraction !== null &&
        row.offHoursFraction >= offHoursAxisThreshold
      ) {
        highlightedHours.add(row.hour);
      }
    });

    const isDiverging = scaleMode === "diverging";
    const isRateDiverging = scaleMode === "rate_diverging";
    const isVolume =
      scaleMode === "volume" ||
      String(valueField || "").toLowerCase() === "n_total" ||
      String(valueField || "").toLowerCase() === "n_known";
    const isRate =
      scaleMode === "rate" ||
      isRateDiverging ||
      (!isDiverging && isProportionField);
    const numericValues = values
      .map((entry) => toFiniteNumberOrNull(entry[2]))
      .filter((value) => value !== null);
    if (!numericValues.length) {
      return false;
    }
    let minValue = isRate || isVolume ? 0 : Math.min.apply(null, numericValues);
    let maxValue = isRate ? 1 : Math.max.apply(null, numericValues);
    if (isDiverging) {
      const maxAbs = Math.max.apply(
        null,
        numericValues.map((value) => Math.abs(toNumber(value)))
      );
      const boundedMaxAbs = Number.isFinite(maxAbs) && maxAbs > 0 ? maxAbs : 1;
      minValue = -boundedMaxAbs;
      maxValue = boundedMaxAbs;
    } else if (isVolume && maxValue <= 0) {
      maxValue = 1;
    } else if (!isRate && minValue === maxValue) {
      maxValue = minValue + 1;
    }
    const divergingColors = divergingPositiveWarm
      ? (theme.heatmapDiverging || []).slice().reverse()
      : theme.heatmapDiverging;
    const missingCells = [];
    if (showMissingOverlay && dates.length && hours.length) {
      dates.forEach((dateValue) => {
        hours.forEach((hourValue) => {
          const key = String(dateValue) + "|" + String(hourValue);
          const details = cellLookup.get(key);
          const hasValue = details && details.value !== null;
          if (hasValue) {
            return;
          }
          const x = hourMap.get(hourValue);
          const y = dateMap.get(dateValue);
          if (typeof x === "number" && typeof y === "number") {
            missingCells.push([x, y, 0]);
          }
        });
      });
    }
    const bucketLabel = bucketLabelFromValue(mount.activeBucket);

    const option = {
      animation: false,
      tooltip: {
          position: "top",
          formatter: (params) => {
            if (!Array.isArray(params.value)) {
              return "";
            }
            const xIndex = params.value[0];
            const yIndex = params.value[1];
            const hour = hours[xIndex];
            const date = dates[yIndex];
            const key = String(date) + "|" + String(hour);
            const details = cellLookup.get(key);
            const lines = [
              "<strong>Time (" +
                reportTimezoneLabel +
                "):</strong> " +
                String(date) +
                " " +
                String(hour).padStart(2, "0") +
                ":00",
            ];
            if (bucketLabel) {
              lines.push("<strong>Bucket:</strong> " + bucketLabel);
            }
            const hasCellValue = details && details.value !== null;
            lines.push(
              "<strong>" +
                String(valueLabel || valueField) +
                ":</strong> " +
                (hasCellValue ? formatTooltipValue(params.value[2]) : "No data")
            );
            if (details && details.nTotal !== null) {
              lines.push("<strong>Total:</strong> " + Math.round(details.nTotal).toLocaleString());
            }
            if (details && details.nKnown !== null) {
              lines.push("<strong>Known Pro+Con:</strong> " + Math.round(details.nKnown));
            }
            if (details && details.nWindows !== null) {
              lines.push("<strong>Windows:</strong> " + Math.round(details.nWindows));
            }
            if (details && details.nWindowsTested !== null) {
              lines.push(
                "<strong>Tested windows:</strong> " +
                  Math.round(details.nWindowsTested)
              );
            }
            if (details && details.nWindowsPrimaryAlert !== null) {
              lines.push(
                "<strong>Robust alerts:</strong> " +
                  Math.round(details.nWindowsPrimaryAlert)
              );
            }
            if (details && details.proRate !== null && valueField !== "pro_rate") {
              lines.push("<strong>Pro rate:</strong> " + formatPercent(details.proRate));
            }
            if (details && details.offHoursFraction !== null) {
              lines.push(
                "<strong>Off-hours fraction:</strong> " + formatPercent(details.offHoursFraction)
              );
            }
            if (
              details &&
              details.expectedPrimary !== null &&
              valueField !== "expected_pro_rate_primary"
            ) {
              lines.push(
                "<strong>Expected pro rate:</strong> " +
                  formatPercent(details.expectedPrimary)
              );
            }
            if (
              details &&
              details.deltaPrimary !== null &&
              valueField !== "delta_pro_rate_primary"
            ) {
              lines.push(
                "<strong>Delta vs primary:</strong> " +
                  formatTooltipValue(details.deltaPrimary)
              );
            }
            if (details && details.zPrimaryMedian !== null && valueField !== "z_score_primary") {
              lines.push(
                "<strong>Median primary z:</strong> " +
                  formatTooltipValue(details.zPrimaryMedian)
              );
            }
            if (details && details.zPrimaryAbsMax !== null) {
              lines.push(
                "<strong>Max |primary z|:</strong> " +
                  formatTooltipValue(details.zPrimaryAbsMax)
              );
            }
            if (details && details.lowPower) {
              lines.push("<strong>Low-power:</strong> yes");
            }
            return lines.join("<br/>");
          },
        },
      grid: { left: 86, right: 54, top: 18, bottom: 86 },
      xAxis: {
          type: "category",
          name: "Hour (" + reportTimezoneLabel + ")",
          data: hours.map((hour) => String(hour).padStart(2, "0")),
          axisLabel: {
            formatter: (value) => {
              const parsedHour = Number(value);
              const label = String(value || "").padStart(2, "0");
              if (highlightOffHoursAxis && highlightedHours.has(parsedHour)) {
                return "{offHours|" + label + "}";
              }
              return label;
            },
            rich: highlightOffHoursAxis
              ? {
                  offHours: {
                    color: theme.alertLower,
                    fontWeight: 700,
                  },
                }
              : undefined,
          },
          splitArea: { show: true },
        },
      yAxis: {
          type: "category",
          name: "Date",
          data: dates,
          inverse: true,
          splitArea: { show: true },
        },
      visualMap: {
          min: minValue,
          max: maxValue,
          orient: "horizontal",
          left: "center",
          bottom: 8,
          calculable: true,
          text:
            isRate && (isRateDiverging || isProportionField)
              ? ["Pro-leaning", "Con-leaning"]
              : isDiverging
                ? divergingPositiveWarm
                  ? ["Positive residual", "Negative residual"]
                  : ["Negative residual", "Positive residual"]
              : undefined,
          inRange: isRate
            ? isRateDiverging || isProportionField
              ? { color: theme.heatmapRateDiverging || theme.heatmapDiverging }
              : { color: theme.heatmapRate }
            : isDiverging
              ? { color: divergingColors }
              : { color: theme.heatmapVolume },
        },
      series: [
          {
            name: "no-data",
            type: "heatmap",
            data: missingCells,
            silent: true,
            itemStyle: {
              color: theme.heatmapNoData,
              borderColor: theme.heatmapNoDataBorder,
              borderWidth: 0.6,
            },
            emphasis: { disabled: true },
            z: 0,
          },
          {
            name: valueLabel || valueField,
            type: "heatmap",
            data: values,
            emphasis: { itemStyle: { shadowBlur: 8, shadowColor: theme.shadowColor } },
            z: 1,
          },
          {
            name: "low-power",
            type: "scatter",
            data: lowPowerPoints,
            symbol: "triangle",
            symbolRotate: 180,
            symbolSize: 9,
            itemStyle: {
              color: theme.lowPower,
              borderColor: theme.axisLine,
              borderWidth: 1,
            },
          },
        ],
    };

    mount.chart.setOption(ensureReadableAxes(option, mount), true);

    mount.isTimeSeries = false;
    mount.isAbsoluteTime = false;
    return true;
  }

  function renderOffHoursFunnel(mount, rows) {
    const theme = currentChartTheme();
    const subset = rows
      .map((row) => ({
        bucketStart: toEpochMillis(row.bucket_start),
        bucketMinutes: toFiniteNumberOrNull(row.bucket_minutes),
        nTotal: toFiniteNumberOrNull(row.n_total),
        nKnown: toFiniteNumberOrNull(row.n_known),
        proRate: toFiniteNumberOrNull(row.pro_rate),
        expectedGlobal: toFiniteNumberOrNull(row.expected_pro_rate_global),
        expectedPrimary: toFiniteNumberOrNull(row.expected_pro_rate_primary),
        low95Primary: toFiniteNumberOrNull(row.control_low_95_primary),
        high95Primary: toFiniteNumberOrNull(row.control_high_95_primary),
        low998Primary: toFiniteNumberOrNull(row.control_low_998_primary),
        high998Primary: toFiniteNumberOrNull(row.control_high_998_primary),
        low95Global: toFiniteNumberOrNull(row.control_low_95_global),
        high95Global: toFiniteNumberOrNull(row.control_high_95_global),
        low998Global: toFiniteNumberOrNull(row.control_low_998_global),
        high998Global: toFiniteNumberOrNull(row.control_high_998_global),
        zDay: toFiniteNumberOrNull(row.z_score_day),
        zPrimary: toFiniteNumberOrNull(row.z_score_primary),
        deltaPrimary: toFiniteNumberOrNull(row.delta_pro_rate_primary),
        qPrimary: toFiniteNumberOrNull(
          Object.prototype.hasOwnProperty.call(row, "q_value_primary_lower")
            ? row.q_value_primary_lower
            : row.q_value_primary
        ),
        qPrimaryTwoSided: toFiniteNumberOrNull(row.q_value_primary_two_sided),
        significantPrimaryLower: toBool(
          Object.prototype.hasOwnProperty.call(row, "is_significant_primary_lower")
            ? row.is_significant_primary_lower
            : row.is_significant_primary
        ),
        significantPrimaryUpper: toBool(row.is_significant_primary_upper),
        significantPrimaryTwoSided: toBool(row.is_significant_primary_two_sided),
        materialPrimaryLowerShift: toBool(row.is_material_primary_lower_shift),
        materialPrimaryUpperShift: toBool(row.is_material_primary_upper_shift),
        belowPrimary998: toBool(row.is_below_primary_control_998),
        abovePrimary998: toBool(row.is_above_primary_control_998),
        alertOffHoursWindow: toBool(row.is_alert_off_hours_window),
        primaryAlertWindow: toBool(row.is_primary_alert_window),
        primaryLowerAlertWindow: toBool(
          Object.prototype.hasOwnProperty.call(row, "is_primary_lower_alert_window")
            ? row.is_primary_lower_alert_window
            : row.is_primary_alert_window
        ),
        primaryUpperAlertWindow: toBool(row.is_primary_upper_alert_window),
        primaryTwoSidedAlertWindow: toBool(
          Object.prototype.hasOwnProperty.call(row, "is_primary_two_sided_alert_window")
            ? row.is_primary_two_sided_alert_window
            : row.is_primary_alert_window
        ),
        primarySpcTwoSided: toBool(row.is_primary_spc_998_two_sided),
        primaryFdrTwoSided: toBool(row.is_primary_fdr_two_sided),
        modelAvailable: toBool(row.is_model_baseline_available),
        primaryBaselineSource:
          typeof row.primary_baseline_source === "string" ? row.primary_baseline_source : "",
        lowPower: toBool(row.is_low_power),
        offHoursWindow: toBool(row.is_off_hours_window),
        pureOffHoursWindow: toBool(row.is_pure_off_hours_window),
        inferentialEligible:
          toBool(row.is_alert_off_hours_window) && !toBool(row.is_low_power),
      }))
      .filter((row) => row.nKnown !== null && row.nKnown > 0 && row.proRate !== null)
      .slice(0, 60000);
    if (!subset.length) {
      return false;
    }

    const sorted = subset.slice().sort((left, right) => left.nKnown - right.nKnown);
    const tailStretchEpsilon = 1e-3;
    const clampRateForDisplay = (value) => {
      if (value === null || value === undefined) {
        return null;
      }
      return Math.max(
        tailStretchEpsilon,
        Math.min(1 - tailStretchEpsilon, toNumber(value))
      );
    };
    const toTailStretchScale = (value) => {
      const clamped = clampRateForDisplay(value);
      if (clamped === null) {
        return null;
      }
      return Math.log(clamped / (1 - clamped));
    };
    const fromTailStretchScale = (value) => {
      const numeric = toFiniteNumberOrNull(value);
      if (numeric === null) {
        return null;
      }
      return 1 / (1 + Math.exp(-numeric));
    };

    const curveByN = new Map();
    sorted.forEach((row) => {
      const nKey = Math.round(row.nKnown);
      if (curveByN.has(nKey)) {
        return;
      }
      curveByN.set(nKey, row);
    });
    const curveRows = Array.from(curveByN.entries())
      .map((entry) => ({ n: entry[0], row: entry[1] }))
      .sort((left, right) => left.n - right.n);

    const expectedSeries = curveRows
      .filter((entry) => entry.row.expectedGlobal !== null)
      .map((entry) => [entry.n, toTailStretchScale(entry.row.expectedGlobal)]);
    const low95Series = curveRows
      .filter((entry) => entry.row.low95Global !== null)
      .map((entry) => [entry.n, toTailStretchScale(entry.row.low95Global)]);
    const high95Series = curveRows
      .filter((entry) => entry.row.high95Global !== null)
      .map((entry) => [entry.n, toTailStretchScale(entry.row.high95Global)]);
    const low998Series = curveRows
      .filter((entry) => entry.row.low998Global !== null)
      .map((entry) => [entry.n, toTailStretchScale(entry.row.low998Global)]);
    const high998Series = curveRows
      .filter((entry) => entry.row.high998Global !== null)
      .map((entry) => [entry.n, toTailStretchScale(entry.row.high998Global)]);

    const scaledValues = sorted
      .map((row) => toTailStretchScale(row.proRate))
      .filter((value) => value !== null);
    const minScaled = scaledValues.length ? Math.min(...scaledValues) : -2.2;
    const maxScaledRaw = scaledValues.length ? Math.max(...scaledValues) : 2.2;
    const maxScaled = maxScaledRaw > minScaled ? maxScaledRaw : minScaled + 0.3;
    const padding = Math.max(0.15, (maxScaled - minScaled) * 0.08);
    const yMinScaled = minScaled - padding;
    const yMaxScaled = maxScaled + padding;

    const toPoint = (row) => ({
      value: [row.nKnown, toTailStretchScale(row.proRate), row.bucketStart],
      meta: row,
    });
    const classifyDirection = (row) => {
      if (row.materialPrimaryLowerShift || row.significantPrimaryLower || row.belowPrimary998) {
        return "lower";
      }
      if (row.materialPrimaryUpperShift || row.significantPrimaryUpper || row.abovePrimary998) {
        return "upper";
      }
      if (row.deltaPrimary !== null) {
        return row.deltaPrimary < 0 ? "lower" : "upper";
      }
      return "upper";
    };
    const offHoursPoints = sorted.filter((row) => row.offHoursWindow).map(toPoint);
    const inferentialOffHoursPoints = sorted
      .filter((row) => row.inferentialEligible)
      .map(toPoint);
    const onHoursPoints = sorted.filter((row) => !row.offHoursWindow).map(toPoint);
    const lowPowerPoints = sorted.filter((row) => row.lowPower).map(toPoint);
    const robustLowerPoints = [];
    const robustUpperPoints = [];
    const spcOnlyPoints = [];
    const fdrOnlyPoints = [];
    sorted.forEach((row) => {
      if (row.inferentialEligible && row.primarySpcTwoSided && !row.primaryFdrTwoSided) {
        spcOnlyPoints.push(toPoint(row));
      }
      if (row.inferentialEligible && row.primaryFdrTwoSided && !row.primarySpcTwoSided) {
        fdrOnlyPoints.push(toPoint(row));
      }
      if (!row.inferentialEligible) {
        return;
      }
      const direction = classifyDirection(row);
      const robustLower =
        row.primaryLowerAlertWindow ||
        (row.significantPrimaryTwoSided &&
          row.belowPrimary998 &&
          row.materialPrimaryLowerShift);
      const robustUpper =
        row.primaryUpperAlertWindow ||
        (row.significantPrimaryTwoSided &&
          row.abovePrimary998 &&
          row.materialPrimaryUpperShift) ||
        (row.primaryTwoSidedAlertWindow && direction === "upper");
      if (robustLower && direction === "lower") {
        robustLowerPoints.push(toPoint(row));
      } else if (robustUpper && direction === "upper") {
        robustUpperPoints.push(toPoint(row));
      }
    });
    const bucketLabel = bucketLabelFromValue(mount.activeBucket);

    const option = {
      animation: false,
      color: theme.seriesPalette,
      tooltip: {
          formatter: (params) => {
            const payload = params && params.data && params.data.meta ? params.data.meta : null;
            const dataValue = Array.isArray(params.value) ? params.value : [];
            const nKnown = payload ? payload.nKnown : toFiniteNumberOrNull(dataValue[0]);
            const proRate = payload ? payload.proRate : fromTailStretchScale(dataValue[1]);
            const lines = [];
            if (nKnown !== null) {
              lines.push(
                "<strong>Known Pro+Con (n):</strong> " + Math.round(nKnown).toLocaleString()
              );
            }
            if (payload && payload.nTotal !== null) {
              lines.push("<strong>Total rows:</strong> " + Math.round(payload.nTotal).toLocaleString());
            }
            if (proRate !== null) {
              lines.push("<strong>Pro rate:</strong> " + formatPercent(proRate));
            }
            if (payload && payload.bucketStart !== null) {
              lines.push(
                "<strong>Time (" +
                  reportTimezoneLabel +
                  "):</strong> " +
                  formatEpochMillis(payload.bucketStart)
              );
            }
            if (payload && payload.bucketMinutes !== null) {
              lines.push(
                "<strong>Bucket:</strong> " + String(Math.round(payload.bucketMinutes)) + "m"
              );
            } else if (bucketLabel) {
              lines.push("<strong>Bucket:</strong> " + bucketLabel);
            }
            if (payload && payload.expectedGlobal !== null) {
              lines.push("<strong>Global baseline:</strong> " + formatPercent(payload.expectedGlobal));
            }
            if (payload && payload.expectedPrimary !== null) {
              lines.push(
                "<strong>Primary baseline:</strong> " + formatPercent(payload.expectedPrimary)
              );
            }
            if (payload && payload.zDay !== null) {
              lines.push("<strong>Day-adjusted z:</strong> " + toNumber(payload.zDay).toFixed(2));
            }
            if (payload && payload.zPrimary !== null) {
              lines.push(
                "<strong>Primary-baseline z:</strong> " +
                  toNumber(payload.zPrimary).toFixed(2)
              );
            }
            if (payload && payload.deltaPrimary !== null) {
              lines.push(
                "<strong>Primary delta (obs-exp):</strong> " +
                  formatPercent(payload.deltaPrimary)
              );
            }
            if (payload && payload.qPrimary !== null) {
              lines.push(
                "<strong>Primary lower-tail q:</strong> " +
                  toNumber(payload.qPrimary).toExponential(2)
              );
            }
            if (payload && payload.qPrimaryTwoSided !== null) {
              lines.push(
                "<strong>Primary two-sided q:</strong> " +
                  toNumber(payload.qPrimaryTwoSided).toExponential(2)
              );
            }
            if (payload && payload.offHoursWindow) {
              lines.push("<strong>Window class:</strong> off-hours");
            }
            if (payload && payload.alertOffHoursWindow) {
              lines.push("<strong>Alert-eligible window:</strong> yes");
            }
            if (payload && payload.inferentialEligible) {
              lines.push("<strong>Inferentially eligible:</strong> yes");
            } else if (payload && payload.alertOffHoursWindow) {
              lines.push("<strong>Inferentially eligible:</strong> no (low-power)");
            }
            if (payload && payload.primaryBaselineSource) {
              lines.push(
                "<strong>Primary source:</strong> " +
                  String(payload.primaryBaselineSource).replace(/_/g, " ")
              );
            }
            if (payload && payload.modelAvailable) {
              lines.push("<strong>Model baseline:</strong> available");
            }
            if (payload && payload.significantPrimaryLower) {
              lines.push("<strong>Primary lower-tail FDR:</strong> yes");
            }
            if (payload && payload.significantPrimaryUpper) {
              lines.push("<strong>Primary upper-tail FDR:</strong> yes");
            }
            if (payload && payload.significantPrimaryTwoSided) {
              lines.push("<strong>Primary two-sided FDR:</strong> yes");
            }
            if (payload && payload.materialPrimaryLowerShift) {
              lines.push("<strong>Material lower shift:</strong> yes");
            }
            if (payload && payload.materialPrimaryUpperShift) {
              lines.push("<strong>Material upper shift:</strong> yes");
            }
            if (
              payload &&
              (
                payload.primaryAlertWindow ||
                payload.primaryLowerAlertWindow ||
                payload.primaryUpperAlertWindow ||
                payload.primaryTwoSidedAlertWindow
              )
            ) {
              lines.push("<strong>Robust primary alert:</strong> yes");
            }
            if (payload && payload.abovePrimary998) {
              lines.push("<strong>Above primary 99.8% upper band:</strong> yes");
            }
            if (payload && payload.lowPower) {
              lines.push("<strong>Low-power:</strong> yes");
            }
            return lines.join("<br/>");
          },
        },
      grid: { left: 66, right: 36, top: 18, bottom: 60 },
      legend: {
          bottom: 0,
          selected: {
            "Global 99.8% lower": false,
            "Global 99.8% upper": false,
          },
        },
      xAxis: {
          type: "log",
          name: "Known Pro+Con Count (n)",
          min: "dataMin",
        },
      yAxis: {
          type: "value",
          name: "Pro rate (tail-stretched)",
          min: yMinScaled,
          max: yMaxScaled,
          axisLabel: {
            formatter: (value) => {
              const rawRate = fromTailStretchScale(value);
              return rawRate === null ? "-" : formatPercent(rawRate);
            },
          },
        },
      series: [
          {
            name: "On-hours/mixed windows",
            type: "scatter",
            data: onHoursPoints,
            symbolSize: 6.5,
            itemStyle: { color: theme.referenceLine, opacity: 0.34 },
          },
          {
            name: "Off-hours windows",
            type: "scatter",
            data: offHoursPoints,
            symbolSize: 7,
            itemStyle: { color: theme.primaryLine, opacity: 0.52 },
          },
          {
            name: "Inferentially tested off-hours",
            type: "scatter",
            data: inferentialOffHoursPoints,
            symbolSize: 7.5,
            itemStyle: { color: theme.contextLine, opacity: 0.82 },
          },
          {
            name: "SPC-only flag",
            type: "scatter",
            data: spcOnlyPoints,
            symbol: "rect",
            symbolSize: 7,
            itemStyle: {
              color: "rgba(0,0,0,0)",
              borderColor: theme.referenceLine,
              borderWidth: 1.2,
              opacity: 0.9,
            },
          },
          {
            name: "FDR-only flag",
            type: "scatter",
            data: fdrOnlyPoints,
            symbol: "circle",
            symbolSize: 7,
            itemStyle: {
              color: "rgba(0,0,0,0)",
              borderColor: theme.contextLine,
              borderWidth: 1.2,
              opacity: 0.9,
            },
          },
          {
            name: "Robust lower-tail alert",
            type: "scatter",
            data: robustLowerPoints,
            symbol: "diamond",
            symbolSize: 9,
            itemStyle: {
              color: theme.alertLower,
              borderColor: theme.axisLine,
              borderWidth: 1.35,
              opacity: 0.94,
            },
          },
          {
            name: "Robust upper-tail alert",
            type: "scatter",
            data: robustUpperPoints,
            symbol: "triangle",
            symbolSize: 9,
            itemStyle: {
              color: theme.alertUpper,
              borderColor: theme.axisLine,
              borderWidth: 1.35,
              opacity: 0.94,
            },
          },
          {
            name: "Low-power windows",
            type: "scatter",
            data: lowPowerPoints,
            symbol: "triangle",
            symbolRotate: 180,
            symbolSize: 7,
            itemStyle: {
              color: theme.lowPower,
              borderColor: theme.axisLine,
              borderWidth: 0.9,
              opacity: 0.88,
            },
          },
          {
            name: "Global expected rate",
            type: "line",
            data: expectedSeries,
            showSymbol: false,
            lineStyle: { color: theme.referenceLine, width: 1.1, type: "solid", opacity: 0.62 },
          },
          {
            name: "Global 95% lower",
            type: "line",
            data: low95Series,
            showSymbol: false,
            lineStyle: { color: theme.intervalBand, width: 0.95, type: "dashed", opacity: 0.43 },
          },
          {
            name: "Global 95% upper",
            type: "line",
            data: high95Series,
            showSymbol: false,
            lineStyle: { color: theme.intervalBand, width: 0.95, type: "dashed", opacity: 0.43 },
          },
          {
            name: "Global 99.8% lower",
            type: "line",
            data: low998Series,
            showSymbol: false,
            lineStyle: { color: theme.intervalBand, width: 0.85, type: "dotted", opacity: 0.28 },
          },
          {
            name: "Global 99.8% upper",
            type: "line",
            data: high998Series,
            showSymbol: false,
            lineStyle: { color: theme.intervalBand, width: 0.85, type: "dotted", opacity: 0.28 },
          },
        ],
    };

    mount.chart.setOption(ensureReadableAxes(option, mount), true);

    mount.isTimeSeries = false;
    mount.isAbsoluteTime = false;
    return true;
  }

  function renderOverviewPositionVolumeByBucket(mount, rows) {
    const theme = currentChartTheme();
    const points = rows
      .map((row) => {
        const bucketStart = toEpochMillis(row.bucket_start);
        const bucketMinutes = toFiniteNumberOrNull(row.bucket_minutes);
        const nPro = Math.max(0, toNumber(row.n_pro));
        const nCon = Math.max(0, toNumber(row.n_con));
        const nOtherRaw =
          Object.prototype.hasOwnProperty.call(row, "n_other_position")
            ? row.n_other_position
            : row.n_unknown;
        const nOther = Math.max(0, toNumber(nOtherRaw));
        const nTotalRaw = toFiniteNumberOrNull(row.n_total);
        const nTotal = nTotalRaw !== null ? Math.max(0, nTotalRaw) : nPro + nCon + nOther;
        return {
          bucketStart,
          bucketMinutes,
          nPro,
          nCon,
          nOther,
          nTotal,
        };
      })
      .filter((row) => row.bucketStart !== null)
      .sort((left, right) => left.bucketStart - right.bucketStart)
      .slice(0, 60000);
    if (!points.length) {
      return false;
    }

    mount.timeExtent = extentFromRows(points, "bucketStart");
    mount.customChartNote = null;
    const mainSeriesId = "main-" + mount.chartId;

    const seriesFor = (field) =>
      points.map((point) => ({
        value: [point.bucketStart, point[field]],
        meta: point,
      }));

    const option = {
      animation: false,
      grid: { left: 66, right: 28, top: 24, bottom: 88 },
      legend: { bottom: 0 },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        appendToBody: true,
        confine: false,
        formatter: (params) => {
          const items = Array.isArray(params) ? params : [];
          const first = items.length ? items[0] : null;
          const meta = first && first.data && first.data.meta ? first.data.meta : null;
          if (!meta) {
            return "";
          }
          const lines = [];
          if (meta.bucketStart !== null) {
            lines.push(
              "<strong>Bucket start (" +
                reportTimezoneLabel +
                "):</strong> " +
                formatEpochMillis(meta.bucketStart)
            );
          }
          if (meta.bucketStart !== null && meta.bucketMinutes !== null) {
            lines.push(
              "<strong>Bucket end:</strong> " +
                formatEpochMillis(
                  meta.bucketStart + Math.max(1, Math.round(meta.bucketMinutes)) * 60 * 1000 - 1
                )
            );
            lines.push(
              "<strong>Bucket:</strong> " + String(Math.round(meta.bucketMinutes)) + "m"
            );
          } else {
            const bucketLabel = bucketLabelFromValue(mount.activeBucket);
            if (bucketLabel) {
              lines.push("<strong>Bucket:</strong> " + bucketLabel);
            }
          }
          lines.push("<strong>Pro:</strong> " + Math.round(meta.nPro).toLocaleString());
          lines.push("<strong>Con:</strong> " + Math.round(meta.nCon).toLocaleString());
          lines.push("<strong>Other:</strong> " + Math.round(meta.nOther).toLocaleString());
          lines.push("<strong>Total:</strong> " + Math.round(meta.nTotal).toLocaleString());
          if (meta.nTotal > 0) {
            lines.push(
              "<strong>Shares (Pro/Con/Other):</strong> " +
                formatPercent(meta.nPro / meta.nTotal) +
                " / " +
                formatPercent(meta.nCon / meta.nTotal) +
                " / " +
                formatPercent(meta.nOther / meta.nTotal)
            );
          }
          return lines.join("<br/>");
        },
      },
      xAxis: {
        type: "time",
        name: "Time (" + reportTimezoneLabel + ")",
        axisLabel: {
          formatter: (value) => formatEpochMillis(value),
        },
      },
      yAxis: {
        type: "value",
        name: "Submission count",
        min: 0,
      },
      dataZoom: [
        {
          type: "inside",
          xAxisIndex: [0],
          startValue: Number.isFinite(state.zoom.minTime) ? state.zoom.minTime : undefined,
          endValue: Number.isFinite(state.zoom.maxTime) ? state.zoom.maxTime : undefined,
        },
        {
          type: "slider",
          xAxisIndex: [0],
          bottom: 14,
          startValue: Number.isFinite(state.zoom.minTime) ? state.zoom.minTime : undefined,
          endValue: Number.isFinite(state.zoom.maxTime) ? state.zoom.maxTime : undefined,
        },
      ],
      series: [
        {
          id: mainSeriesId,
          name: "Pro",
          type: "bar",
          stack: "position-volume",
          data: seriesFor("nPro"),
          itemStyle: { color: theme.contextLine },
          emphasis: { focus: "series" },
          barMaxWidth: 20,
          markLine: appendCursorMarkLine(mount.baseMarkLines || []),
        },
        {
          name: "Con",
          type: "bar",
          stack: "position-volume",
          data: seriesFor("nCon"),
          itemStyle: { color: theme.alertLower },
          emphasis: { focus: "series" },
          barMaxWidth: 20,
        },
        {
          name: "Other",
          type: "bar",
          stack: "position-volume",
          data: seriesFor("nOther"),
          itemStyle: { color: theme.referenceLine },
          emphasis: { focus: "series" },
          barMaxWidth: 20,
        },
      ],
    };

    mount.chart.setOption(ensureReadableAxes(option, mount), true);
    mount.seriesId = mainSeriesId;
    mount.isTimeSeries = true;
    mount.isAbsoluteTime = state.absoluteTimeSet.has(mount.chartId);
    return true;
  }

  const simpleBarCategoricalChartIds = new Set([
    "composite_evidence_flags",
    "duplicates_exact_swing_impact",
    "off_hours_primary_flag_channels",
    "voter_registry_linkage_by_position_rows",
    "voter_registry_linkage_by_position_unique",
    "voter_registry_sensitivity_modes",
    "voter_registry_match_tiers",
    "voter_registry_match_by_position",
  ]);
  const simpleBarRankedChartIds = new Set([
    "baseline_top_names",
    "baseline_name_length_distribution",
    "changepoints_hour_hist",
    "changepoints_magnitude",
    "duplicates_exact_per_name_anomalies",
    "duplicates_exact_metric_diagnostics",
    "duplicates_exact_temporal_burst",
    "duplicates_exact_top_names",
    "duplicates_exact_position_switch",
    "duplicates_exact_position_concentration",
    "off_hours_hourly_profile",
    "off_hours_model_fit_diagnostics",
    "off_hours_summary_compare",
    "org_anomalies_bursts",
    "org_anomalies_top_orgs",
    "periodicity_clockface",
    "periodicity_spectrum",
    "rare_names_weird_scores",
    "sortedness_minute_spikes",
    "voter_registry_pairwise_tests",
    "voter_registry_unmatched_names",
  ]);
  const simpleBarNullDiagnosticChartIds = new Set([
    "bursts_null_distribution",
    "bursts_significance_by_window",
    "duplicates_exact_null_distribution",
    "procon_swings_null_distribution",
    "periodicity_rolling_fano",
  ]);
  const simpleBarRatioReferenceChartIds = new Set([
    "composite_high_priority",
    "duplicates_exact_position_concentration",
    "voter_registry_pairwise_tests",
    "periodicity_autocorr",
    "procon_swings_time_of_day_profile",
    "sortedness_bucket_summary",
    "sortedness_kendall_tau_summary",
  ]);
  const simpleBarDirectionalReferenceChartIds = new Set([
    "duplicates_exact_position_concentration",
    "periodicity_autocorr",
    "procon_swings_time_of_day_profile",
    "sortedness_bucket_summary",
    "sortedness_kendall_tau_summary",
    "voter_registry_pairwise_tests",
  ]);

  function renderSimpleBar(mount, rows, xField, yField, title) {
    const theme = currentChartTheme();
    const subset = rows
      .map((row) => ({
        raw: row,
        x: row[xField],
        y: toFiniteNumberOrNull(row[yField]),
      }))
      .filter((row) => row.x !== undefined && row.x !== null && row.y !== null)
      .slice(0, 200);
    if (!subset.length) {
      return false;
    }
    const bucketLabel = bucketLabelFromValue(mount.activeBucket);
    const isTimeField = [
      "minute_bucket",
      "bucket_start",
      "first_seen",
      "start_minute",
      "change_minute",
      "date",
    ].includes(xField);
    const xAxisLabel = isTimeField
      ? "Time (" + reportTimezoneLabel + ")"
      : humanizeFieldName(xField);
    const yAxisLabel = title ? String(title) : humanizeFieldName(yField);

    let barSeriesData = subset.map((row) => row.y);
    const markLineData = [];

    if (simpleBarCategoricalChartIds.has(mount.chartId)) {
      const palette = Array.isArray(theme.categoricalPalette) && theme.categoricalPalette.length
        ? theme.categoricalPalette
        : theme.seriesPalette;
      const colorByLabel = new Map();
      subset.forEach((row) => {
        const label = String(row.x);
        if (colorByLabel.has(label)) {
          return;
        }
        const nextColor = palette[colorByLabel.size % palette.length] || theme.barAccent;
        colorByLabel.set(label, nextColor);
      });
      barSeriesData = subset.map((row) => ({
        value: row.y,
        itemStyle: {
          color: colorByLabel.get(String(row.x)) || theme.barAccent,
          opacity: 0.9,
        },
      }));
    } else if (simpleBarRankedChartIds.has(mount.chartId)) {
      const ranked = subset
        .map((row, index) => ({ index: index, value: row.y }))
        .sort((left, right) => right.value - left.value);
      const topCount = Math.max(1, Math.min(6, Math.ceil(subset.length * 0.15)));
      const topIndexSet = new Set(ranked.slice(0, topCount).map((entry) => entry.index));
      barSeriesData = subset.map((row, index) => ({
        value: row.y,
        itemStyle: {
          color: topIndexSet.has(index) ? theme.alertLower : theme.barAccent,
          opacity: topIndexSet.has(index) ? 0.94 : 0.56,
        },
      }));
    } else if (simpleBarNullDiagnosticChartIds.has(mount.chartId)) {
      barSeriesData = subset.map((row) => ({
        value: row.y,
        itemStyle: {
          color: theme.referenceLine,
          opacity: 0.58,
        },
      }));
      const first = subset[0].raw || {};
      const observedField = [
        "observed_value",
        "observed_max",
        "observed_count",
        "observed",
        "actual",
      ].find((field) => toFiniteNumberOrNull(first[field]) !== null);
      const thresholdField = [
        "critical_value",
        "significance_threshold",
        "threshold",
        "alpha_threshold",
      ].find((field) => toFiniteNumberOrNull(first[field]) !== null);
      const observedValue = observedField ? toFiniteNumberOrNull(first[observedField]) : null;
      const thresholdValue = thresholdField
        ? toFiniteNumberOrNull(first[thresholdField])
        : null;
      if (thresholdValue !== null) {
        markLineData.push({
          name: "Threshold",
          yAxis: thresholdValue,
          lineStyle: { color: theme.referenceLine, type: "dashed", width: 1.2, opacity: 0.8 },
          label: { formatter: "threshold", color: theme.axisText, fontSize: 10 },
        });
      }
      if (observedValue !== null) {
        markLineData.push({
          name: "Observed",
          yAxis: observedValue,
          lineStyle: { color: theme.alertLower, type: "solid", width: 1.2, opacity: 0.9 },
          label: { formatter: "observed", color: theme.axisText, fontSize: 10 },
        });
      }
    } else if (simpleBarRatioReferenceChartIds.has(mount.chartId)) {
      const referenceByChartId = {
        composite_high_priority: 0.8,
        duplicates_exact_position_concentration: 0.0,
        periodicity_autocorr: 0.0,
        procon_swings_time_of_day_profile: 0.5,
        sortedness_bucket_summary: 0.5,
        sortedness_kendall_tau_summary: 0.0,
        voter_registry_pairwise_tests: 0.0,
      };
      const referenceValue = Object.prototype.hasOwnProperty.call(referenceByChartId, mount.chartId)
        ? referenceByChartId[mount.chartId]
        : null;
      const useDirectionalSplit =
        simpleBarDirectionalReferenceChartIds.has(mount.chartId) &&
        Number.isFinite(referenceValue);
      barSeriesData = subset.map((row) => {
        if (!useDirectionalSplit) {
          return {
            value: row.y,
            itemStyle: { color: theme.barAccent, opacity: 0.8 },
          };
        }
        return {
          value: row.y,
          itemStyle: {
            color: row.y >= referenceValue ? theme.alertUpper : theme.alertLower,
            opacity: 0.86,
          },
        };
      });
      if (Number.isFinite(referenceValue)) {
        markLineData.push({
          name: "Reference",
          yAxis: referenceValue,
          lineStyle: { color: theme.referenceLine, type: "dashed", width: 1.2, opacity: 0.85 },
          label: { formatter: "reference", color: theme.axisText, fontSize: 10 },
        });
      }
    }

    const seriesEntry = {
      type: "bar",
      data: barSeriesData,
    };
    if (markLineData.length) {
      seriesEntry.markLine = {
        symbol: ["none", "none"],
        data: markLineData,
      };
    }

    const option = {
      animation: false,
      tooltip: {
          trigger: "axis",
          axisPointer: { type: "shadow" },
          formatter: (params) => {
            const entries = Array.isArray(params) ? params : [params];
            if (!entries.length) {
              return "";
            }
            const first = entries[0];
            const categoryRaw = first.axisValueLabel || first.axisValue || "";
            const timestamp = toEpochMillis(categoryRaw);
            const lines = [];
            if (timestamp !== null) {
              lines.push(
                "<strong>Time (" +
                  reportTimezoneLabel +
                  "):</strong> " +
                  formatEpochMillis(timestamp)
              );
            } else {
              lines.push("<strong>" + xAxisLabel + ":</strong> " + String(categoryRaw));
            }
            if (bucketLabel) {
              lines.push("<strong>Bucket:</strong> " + bucketLabel);
            }
            entries.forEach((entry) => {
              lines.push(
                (entry.marker || "") +
                  "<strong>" +
                  String(entry.seriesName || title || "value") +
                  ":</strong> " +
                  formatTooltipValue(entry.value)
              );
            });
            return lines.join("<br/>");
          },
        },
      grid: { left: 64, right: 20, top: 26, bottom: 86 },
      xAxis: {
          type: "category",
          name: xAxisLabel,
          data: subset.map((row) => String(row.x)),
          axisLabel: { interval: 0, rotate: 34, color: theme.axisText },
        },
      yAxis: { type: "value", name: yAxisLabel, axisLabel: { color: theme.axisText } },
      series: [seriesEntry],
    };
    mount.chart.setOption(ensureReadableAxes(option, mount), true);
    mount.isTimeSeries = false;
    mount.isAbsoluteTime = false;
    return true;
  }

  function renderDuplicatePositionConcentration(mount, rows) {
    const theme = currentChartTheme();
    const subset = rows
      .map((row) => {
        const fallbackLabel =
          String(row.position_left || "").trim() + " vs " + String(row.position_right || "").trim();
        const pairLabel =
          row.pair_label !== undefined && row.pair_label !== null && String(row.pair_label).trim()
            ? String(row.pair_label)
            : fallbackLabel.trim();
        const leftRate = toFiniteNumberOrNull(row.left_duplicate_row_rate);
        const rightRate = toFiniteNumberOrNull(row.right_duplicate_row_rate);
        const rateDifference = toFiniteNumberOrNull(row.rate_difference);
        if (!pairLabel || (leftRate === null && rightRate === null && rateDifference === null)) {
          return null;
        }
        return {
          raw: row,
          pairLabel: pairLabel,
          leftPosition: String(row.position_left || "left"),
          rightPosition: String(row.position_right || "right"),
          leftRate: leftRate,
          rightRate: rightRate,
          rateDifference: rateDifference,
          absDifference: Math.abs(rateDifference !== null ? rateDifference : 0),
        };
      })
      .filter((row) => row !== null)
      .sort((left, right) => right.absDifference - left.absDifference)
      .slice(0, 10);
    if (!subset.length) {
      return false;
    }

    const bucketLabel = bucketLabelFromValue(mount.activeBucket);
    const rateValues = subset
      .flatMap((row) => [row.leftRate, row.rightRate])
      .filter((value) => value !== null);
    const boundedRates =
      rateValues.length && rateValues.every((value) => value >= 0 && value <= 1);
    const leftSeriesName = "Left position rate";
    const rightSeriesName = "Right position rate";

    const option = {
      animation: false,
      legend: {
        top: 0,
        data: [leftSeriesName, rightSeriesName],
      },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        formatter: (params) => {
          const entries = Array.isArray(params) ? params : [params];
          if (!entries.length) {
            return "";
          }
          const dataIndex = Number(toNumber(entries[0].dataIndex || 0));
          const row = subset[Math.max(0, Math.min(subset.length - 1, dataIndex))];
          const lines = ["<strong>Pair:</strong> " + escapeHtml(row.pairLabel)];
          if (bucketLabel) {
            lines.push("<strong>Bucket:</strong> " + bucketLabel);
          }
          lines.push(
            "<strong>" +
              escapeHtml(row.leftPosition) +
              " rate:</strong> " +
              formatTooltipValue(row.leftRate)
          );
          lines.push(
            "<strong>" +
              escapeHtml(row.rightPosition) +
              " rate:</strong> " +
              formatTooltipValue(row.rightRate)
          );
          if (row.rateDifference !== null) {
            lines.push(
              "<strong>Rate difference (left - right):</strong> " +
                formatTooltipValue(row.rateDifference)
            );
          }
          const ciLow = toFiniteNumberOrNull(row.raw.rate_difference_ci_low);
          const ciHigh = toFiniteNumberOrNull(row.raw.rate_difference_ci_high);
          if (ciLow !== null || ciHigh !== null) {
            lines.push(
              "<strong>Rate difference CI:</strong> [" +
                formatTooltipValue(ciLow) +
                ", " +
                formatTooltipValue(ciHigh) +
                "]"
            );
          }
          const rateRatio = toFiniteNumberOrNull(row.raw.rate_ratio);
          if (rateRatio !== null) {
            lines.push("<strong>Rate ratio:</strong> " + formatTooltipValue(rateRatio));
          }
          const permP = toFiniteNumberOrNull(row.raw.permutation_p_value_one_sided);
          if (permP !== null) {
            lines.push("<strong>Permutation p (one-sided):</strong> " + formatTooltipValue(permP));
          }
          return lines.join("<br/>");
        },
      },
      grid: { left: 64, right: 24, top: 48, bottom: 88 },
      xAxis: {
        type: "category",
        name: "Position comparison pair",
        data: subset.map((row) => row.pairLabel),
        axisLabel: { interval: 0, rotate: 30, color: theme.axisText },
      },
      yAxis: {
        type: "value",
        name: boundedRates ? "Duplicate row rate" : "Duplicate burden rate",
        axisLabel: { color: theme.axisText },
        min: boundedRates ? 0 : null,
        max: boundedRates ? 1 : null,
      },
      series: [
        {
          name: leftSeriesName,
          type: "bar",
          data: subset.map((row) => row.leftRate),
          itemStyle: { color: theme.alertLower, opacity: 0.86 },
        },
        {
          name: rightSeriesName,
          type: "bar",
          data: subset.map((row) => row.rightRate),
          itemStyle: { color: theme.alertUpper, opacity: 0.82 },
        },
      ],
    };
    mount.chart.setOption(ensureReadableAxes(option, mount), true);
    mount.isTimeSeries = false;
    mount.isAbsoluteTime = false;
    return true;
  }

  function renderOffHoursModelFitDiagnostics(mount, rows) {
    const theme = currentChartTheme();
    const subset = rows
      .map((row) => {
        const bucketMinutes = toFiniteNumberOrNull(row.bucket_minutes);
        const totalRaw = toFiniteNumberOrNull(row.model_fit_window_count);
        const availableRaw = toFiniteNumberOrNull(row.model_fit_available_windows);
        const fractionRaw = toFiniteNumberOrNull(row.model_fit_available_fraction);
        if (
          bucketMinutes === null ||
          (totalRaw === null && availableRaw === null && fractionRaw === null)
        ) {
          return null;
        }
        return {
          raw: row,
          bucketMinutes: bucketMinutes,
          totalRaw: totalRaw,
          availableRaw: availableRaw,
          fractionRaw: fractionRaw,
          totalCount: totalRaw === null ? 0 : totalRaw,
          availableCount: availableRaw === null ? 0 : availableRaw,
        };
      })
      .filter((row) => row !== null)
      .sort((left, right) => left.bucketMinutes - right.bucketMinutes)
      .slice(0, 24);
    if (!subset.length) {
      return false;
    }

    const categories = subset.map((row) => String(Math.round(row.bucketMinutes)) + "m");
    const hasFractionSeries = subset.some((row) => row.fractionRaw !== null);
    const lineSeriesName = "Model-available fraction";
    const series = [
      {
        name: "Total windows",
        type: "bar",
        yAxisIndex: 0,
        data: subset.map((row) => row.totalCount),
        itemStyle: { color: theme.volumeBar, opacity: 0.64 },
      },
      {
        name: "Model-available windows",
        type: "bar",
        yAxisIndex: 0,
        data: subset.map((row) => row.availableCount),
        itemStyle: { color: theme.barAccent, opacity: 0.86 },
      },
    ];
    if (hasFractionSeries) {
      series.push({
        name: lineSeriesName,
        type: "line",
        yAxisIndex: 1,
        data: subset.map((row) => row.fractionRaw),
        showSymbol: true,
        symbolSize: 6,
        lineStyle: { color: theme.primaryLine, width: 1.8, opacity: 0.9 },
        itemStyle: { color: theme.primaryLine },
      });
    }

    const option = {
      animation: false,
      legend: {
        top: 0,
        data: series.map((entry) => entry.name),
      },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        formatter: (params) => {
          const entries = Array.isArray(params) ? params : [params];
          if (!entries.length) {
            return "";
          }
          const dataIndex = Number(toNumber(entries[0].dataIndex || 0));
          const row = subset[Math.max(0, Math.min(subset.length - 1, dataIndex))];
          const lines = ["<strong>Bucket:</strong> " + escapeHtml(categories[dataIndex] || "")];
          lines.push(
            "<strong>Total windows:</strong> " + formatTooltipValue(row.totalRaw)
          );
          lines.push(
            "<strong>Model-available windows:</strong> " + formatTooltipValue(row.availableRaw)
          );
          lines.push(
            "<strong>Model-available fraction:</strong> " + formatTooltipValue(row.fractionRaw)
          );
          const method = String(row.raw.model_fit_method || "").trim();
          if (method) {
            lines.push("<strong>Model fit method:</strong> " + escapeHtml(method));
          }
          const converged = toFiniteNumberOrNull(row.raw.model_fit_converged);
          if (converged !== null) {
            lines.push("<strong>Model converged:</strong> " + (converged > 0 ? "yes" : "no"));
          }
          return lines.join("<br/>");
        },
      },
      grid: { left: 64, right: 66, top: 54, bottom: 84 },
      xAxis: {
        type: "category",
        name: "Bucket (minutes)",
        data: categories,
        axisLabel: { interval: 0, rotate: 0, color: theme.axisText },
      },
      yAxis: [
        {
          type: "value",
          name: "Window count",
          min: 0,
          axisLabel: { color: theme.axisText },
        },
        {
          type: "value",
          name: "Available fraction",
          min: 0,
          max: 1,
          axisLabel: {
            color: theme.axisText,
            formatter: (value) => formatPercent(value),
          },
          splitLine: { show: false },
        },
      ],
      series: series,
    };
    mount.chart.setOption(ensureReadableAxes(option, mount), true);
    mount.isTimeSeries = false;
    mount.isAbsoluteTime = false;
    return true;
  }

  function renderScatter(mount, rows, xField, yField, colorField, sizeField) {
    const theme = currentChartTheme();
    const subset = rows
      .map((row) => {
        const x = toFiniteNumberOrNull(row[xField]);
        const y = toFiniteNumberOrNull(row[yField]);
        const c = colorField ? toFiniteNumberOrNull(row[colorField]) : null;
        const s = sizeField ? toFiniteNumberOrNull(row[sizeField]) : null;
        if (x === null || y === null) {
          return null;
        }
        return { x: x, y: y, c: c, s: s };
      })
      .filter((row) => row !== null)
      .slice(0, 15000);

    if (!subset.length) {
      return false;
    }

    const dataPoints = subset.map((row) => [row.x, row.y, row.c, row.s]);
    const colorValues = subset
      .map((row) => row.c)
      .filter((value) => value !== null)
      .sort((left, right) => left - right);
    const colorMin = colorValues.length ? colorValues[0] : 0;
    const colorMaxRaw = colorValues.length ? colorValues[colorValues.length - 1] : 1;
    const colorMax = colorMaxRaw > colorMin ? colorMaxRaw : colorMin + 1;
    const topPercentileThreshold = colorValues.length
      ? colorValues[Math.max(0, Math.floor(colorValues.length * 0.95) - 1)]
      : null;
    const topPercentilePoints =
      topPercentileThreshold === null
        ? []
        : subset
            .filter((row) => row.c !== null && row.c >= topPercentileThreshold)
            .map((row) => [row.x, row.y, row.c, row.s]);

    const visualMap = colorField
      ? {
          min: colorMin,
          max: colorMax,
          dimension: 2,
          orient: "horizontal",
          left: "center",
          bottom: 6,
          calculable: true,
          inRange: {
            color: [theme.volumeBar, theme.primaryLine, theme.alertLower],
          },
        }
      : null;

    const option = {
      animation: false,
      tooltip: {
          formatter: (params) => {
            if (!Array.isArray(params.value)) {
              return "";
            }
            return (
              "<strong>x:</strong> " +
              toNumber(params.value[0]).toFixed(4) +
              "<br/><strong>y:</strong> " +
              toNumber(params.value[1]).toFixed(4)
            );
          },
        },
      grid: { left: 62, right: 24, top: 24, bottom: colorField ? 80 : 44 },
      xAxis: { type: "value", name: humanizeFieldName(xField) },
      yAxis: { type: "value", name: humanizeFieldName(yField) },
      visualMap: visualMap,
      series: [
          {
            type: "scatter",
            data: dataPoints,
            symbolSize: (value) => {
              if (!sizeField || value[3] === null) {
                return 7;
              }
              return Math.max(5, Math.min(18, Math.sqrt(Math.max(1, toNumber(value[3])))));
            },
            itemStyle: { color: colorField ? undefined : theme.scatterDefault, opacity: 0.74 },
          },
        ],
    };
    if (topPercentilePoints.length) {
      option.series.push({
        name: "Top-percentile anomalies",
        type: "scatter",
        data: topPercentilePoints,
        symbolSize: (value) => {
          if (!sizeField || value[3] === null) {
            return 9;
          }
          return Math.max(7, Math.min(20, Math.sqrt(Math.max(1, toNumber(value[3]))) + 2));
        },
        tooltip: { show: false },
        itemStyle: {
          color: "rgba(0,0,0,0)",
          borderColor: theme.alertLower,
          borderWidth: 1.8,
          opacity: 1,
        },
        z: 4,
      });
    }

    mount.chart.setOption(ensureReadableAxes(option, mount), true);

    mount.isTimeSeries = false;
    mount.isAbsoluteTime = false;
    return true;
  }

  function renderDuplicateTopNameTiming(mount, rows, matchModeOverride) {
    const theme = currentChartTheme();
    const modeMeta = {
      exact: {
        label: "Exact (last + first)",
        fallbackDefinition: "Exact match on last-name and first-name tokens.",
      },
      medium: {
        label: "Medium (last + nickname root)",
        fallbackDefinition: "Matches last-name with nickname-root first-name normalization.",
      },
      loose: {
        label: "Loose (last + first initial)",
        fallbackDefinition: "Matches last-name with first-name initial only.",
      },
    };
    const normalizeMode = (value) => String(value || "").trim().toLowerCase();
    const targetMode = normalizeMode(matchModeOverride || "");
    const filtered = rows.filter((row) => {
      if (!targetMode) {
        return true;
      }
      return normalizeMode(row.match_mode) === targetMode;
    });
    if (!filtered.length) {
      return false;
    }

    const resolvedMode = targetMode || normalizeMode(filtered[0].match_mode);
    const modeInfo = Object.prototype.hasOwnProperty.call(modeMeta, resolvedMode)
      ? modeMeta[resolvedMode]
      : {
          label: String(filtered[0].match_label || "Top timing"),
          fallbackDefinition: "",
        };
    const keyedNames = new Map();
    filtered.forEach((row) => {
      const key = String(row.name_key || "").trim();
      if (!key) {
        return;
      }
      const rank = toFiniteNumberOrNull(row.rank);
      const numericRank = rank === null ? Number.POSITIVE_INFINITY : Math.round(rank);
      const displayName = String(row.display_name || row.name_key || "").trim();
      const totalRepeatedRows = toFiniteNumberOrNull(row.total_repeated_rows);
      const existing = keyedNames.get(key);
      if (
        !existing ||
        numericRank < existing.rank ||
        (numericRank === existing.rank && toNumber(totalRepeatedRows) > toNumber(existing.totalRepeatedRows))
      ) {
        keyedNames.set(key, {
          key: key,
          rank: numericRank,
          displayName: displayName,
          totalRepeatedRows: totalRepeatedRows,
        });
      }
    });
    const topNames = Array.from(keyedNames.values())
      .sort((left, right) => {
        const rankDelta = left.rank - right.rank;
        if (rankDelta !== 0) {
          return rankDelta;
        }
        const countDelta = toNumber(right.totalRepeatedRows) - toNumber(left.totalRepeatedRows);
        if (countDelta !== 0) {
          return countDelta;
        }
        return String(left.displayName).localeCompare(String(right.displayName));
      })
      .slice(0, DUPLICATE_TOP_NAME_TIMING_MAX_NAMES);
    if (!topNames.length) {
      return false;
    }
    const rankedTopNames = topNames.map((entry, index) => ({
      key: entry.key,
      rank: entry.rank,
      displayName: entry.displayName,
      totalRepeatedRows: entry.totalRepeatedRows,
      displayRank: index + 1,
    }));

    const totalPages = Math.max(
      1,
      Math.ceil(rankedTopNames.length / DUPLICATE_TOP_NAME_TIMING_PAGE_SIZE)
    );
    const requestedPage = Number.isFinite(mount.topNameTimingPage)
      ? Math.round(mount.topNameTimingPage)
      : 0;
    const pageIndex = Math.max(0, Math.min(totalPages - 1, requestedPage));
    mount.topNameTimingPage = pageIndex;
    const pageStart = pageIndex * DUPLICATE_TOP_NAME_TIMING_PAGE_SIZE;
    const pageEndExclusive = Math.min(
      rankedTopNames.length,
      pageStart + DUPLICATE_TOP_NAME_TIMING_PAGE_SIZE
    );
    const pageNames = rankedTopNames.slice(pageStart, pageEndExclusive);
    if (!pageNames.length) {
      return false;
    }
    const displayRankByKey = new Map(pageNames.map((entry) => [entry.key, entry.displayRank]));

    const yLabelByKey = new Map(
      pageNames.map((entry) => [
        entry.key,
        String(entry.displayRank) + ". " + entry.displayName,
      ])
    );
    const yCategories = pageNames.map((entry) => yLabelByKey.get(entry.key) || entry.displayName);
    const visibleKeys = new Set(pageNames.map((entry) => entry.key));
    const points = [];
    filtered.forEach((row) => {
      const key = String(row.name_key || "").trim();
      if (!visibleKeys.has(key)) {
        return;
      }
      const timestamp = toEpochMillis(row.bucket_start);
      const duplicateRows = toFiniteNumberOrNull(row.duplicate_rows);
      if (timestamp === null || duplicateRows === null) {
        return;
      }
      const nPro = Math.max(0, Math.round(toNumber(row.n_pro)));
      const nCon = Math.max(0, Math.round(toNumber(row.n_con)));
      const reportedOther = toFiniteNumberOrNull(row.n_other);
      let nOther =
        reportedOther === null
          ? Math.max(0, Math.round(duplicateRows) - nPro - nCon)
          : Math.max(0, Math.round(reportedOther));
      if (nPro + nCon + nOther === 0) {
        nOther = Math.max(1, Math.round(duplicateRows));
      }
      const bucketPositionRows = [
        { position: "Pro", rows: nPro },
        { position: "Con", rows: nCon },
        { position: "Other", rows: nOther },
      ].filter((entry) => entry.rows > 0);
      bucketPositionRows.forEach((entry) => {
        points.push({
          value: [timestamp, yLabelByKey.get(key), entry.rows],
          name_key: key,
          display_name: String(row.display_name || key),
          rank: toFiniteNumberOrNull(displayRankByKey.get(key)),
          total_repeated_rows: toFiniteNumberOrNull(row.total_repeated_rows),
          bucket_minutes: toFiniteNumberOrNull(row.bucket_minutes),
          duplicate_rows: duplicateRows,
          position: entry.position,
          position_rows: entry.rows,
          n_pro: nPro,
          n_con: nCon,
          n_other: nOther,
          match_label: String(row.match_label || modeInfo.label || ""),
          match_definition: String(
            row.match_definition || modeInfo.fallbackDefinition || ""
          ),
        });
      });
    });
    if (!points.length) {
      return false;
    }

    const positionValues = points
      .map((point) => toFiniteNumberOrNull(point.position_rows))
      .filter((value) => value !== null);
    const minRows = positionValues.length ? Math.min(...positionValues) : 1;
    const maxRowsRaw = positionValues.length ? Math.max(...positionValues) : 1;
    const maxRows = maxRowsRaw > minRows ? maxRowsRaw : minRows + 1;
    const symbolSizeFor = (dupRaw) => {
      const duplicateRows = Math.max(1, toNumber(dupRaw));
      if (!(maxRows > minRows)) {
        return 12;
      }
      const ratio = Math.max(0, Math.min(1, (duplicateRows - minRows) / (maxRows - minRows)));
      return 8 + ratio * 20;
    };
    const orderedPoints = points
      .slice()
      .sort((left, right) => {
        const sizeDelta = toNumber(right.position_rows) - toNumber(left.position_rows);
        if (sizeDelta !== 0) {
          return sizeDelta;
        }
        const timeDelta = toNumber(left.value[0]) - toNumber(right.value[0]);
        if (timeDelta !== 0) {
          return timeDelta;
        }
        return String(left.display_name || "").localeCompare(String(right.display_name || ""));
      });
    const positionColors = {
      Pro: theme.contextLine,
      Con: theme.alertLower,
      Other: theme.referenceLine,
    };
    const definition = String(points[0].match_definition || modeInfo.fallbackDefinition || "").trim();
    const bucketLabel = bucketLabelFromValue(mount.activeBucket);
    const maxLabelChars = yCategories.reduce(
      (maxChars, label) => Math.max(maxChars, String(label || "").length),
      0
    );
    const hostWidth =
      mount && mount.host && Number.isFinite(mount.host.clientWidth)
        ? mount.host.clientWidth
        : 0;
    const compact = hostWidth > 0 && hostWidth < 700;
    const axisLabelWidth = compact ? 110 : 150;
    const leftPadding = compact
      ? Math.max(186, Math.min(254, 122 + Math.min(maxLabelChars, 34) * 3.4))
      : Math.max(236, Math.min(338, 142 + Math.min(maxLabelChars, 42) * 4.2));
    const yNameGap = compact ? 84 : 96;

    const option = {
      animation: false,
      grid: { left: leftPadding, right: 30, top: 28, bottom: 78 },
      tooltip: {
        trigger: "item",
        appendToBody: true,
        confine: false,
        formatter: (params) => {
          const raw = params && typeof params.data === "object" ? params.data : {};
          const value = Array.isArray(params.value) ? params.value : [];
          const timestamp = toFiniteNumberOrNull(value[0]);
          const rank = toFiniteNumberOrNull(raw.rank);
          const duplicateRows = toFiniteNumberOrNull(raw.duplicate_rows);
          const totalRepeatedRows = toFiniteNumberOrNull(raw.total_repeated_rows);
          const bucketMinutes = toFiniteNumberOrNull(raw.bucket_minutes);
          const nPro = toFiniteNumberOrNull(raw.n_pro);
          const nCon = toFiniteNumberOrNull(raw.n_con);
          const nOther = toFiniteNumberOrNull(raw.n_other);
          const positionRows = toFiniteNumberOrNull(raw.position_rows);
          const position = String(raw.position || "").trim();
          const tierDefinition = String(raw.match_definition || "").trim();
          const bucketEnd =
            timestamp !== null && bucketMinutes !== null
              ? formatEpochMillis(timestamp + Math.max(1, Math.round(bucketMinutes)) * 60 * 1000 - 1)
              : "";
          const lines = [
            "<strong>" + escapeHtml(String(raw.match_label || modeInfo.label || "")) + "</strong>",
            "<strong>Name:</strong> " + escapeHtml(String(raw.display_name || raw.name_key || "")),
          ];
          if (rank !== null) {
            lines.push("<strong>Rank:</strong> " + Number(rank).toLocaleString());
          }
          if (timestamp !== null) {
            lines.push("<strong>Bucket start:</strong> " + formatEpochMillis(timestamp));
            if (bucketEnd) {
              lines.push("<strong>Bucket end:</strong> " + bucketEnd);
            }
          }
          if (bucketLabel && bucketMinutes === null) {
            lines.push("<strong>Bucket:</strong> " + bucketLabel);
          }
          if (duplicateRows !== null) {
            lines.push(
              "<strong>Duplicate rows (bucket):</strong> " + Number(duplicateRows).toLocaleString()
            );
          }
          if (positionRows !== null) {
            lines.push(
              "<strong>Position rows (bucket):</strong> " + Number(positionRows).toLocaleString()
            );
          }
          if (position) {
            lines.push("<strong>Position:</strong> " + escapeHtml(position));
          }
          if (totalRepeatedRows !== null) {
            lines.push(
              "<strong>Total repeated rows (name):</strong> " + Number(totalRepeatedRows).toLocaleString()
            );
          }
          if (nPro !== null || nCon !== null || nOther !== null) {
            lines.push(
              "<strong>Pro/Con/Other:</strong> " +
                Number(nPro || 0).toLocaleString() +
                "/" +
                Number(nCon || 0).toLocaleString() +
                "/" +
                Number(nOther || 0).toLocaleString()
            );
          }
          if (tierDefinition) {
            lines.push("<strong>Tier definition:</strong> " + escapeHtml(tierDefinition));
          }
          return lines.join("<br/>");
        },
      },
      xAxis: {
        type: "time",
        name: "Time (" + reportTimezoneLabel + ")",
        axisLabel: {
          color: theme.axisText,
          formatter: (value) => formatEpochMillis(value),
        },
        axisLine: { lineStyle: { color: theme.axisLine } },
        splitLine: { show: false },
      },
      yAxis: {
        type: "category",
        name: "Top names",
        nameGap: yNameGap,
        data: yCategories,
        inverse: true,
        axisLabel: {
          color: theme.axisText,
          margin: 12,
          width: axisLabelWidth,
          overflow: "truncate",
          formatter: (value) => {
            const text = String(value || "");
            return text.length > 38 ? text.slice(0, 35) + "..." : text;
          },
        },
        axisTick: { show: false },
        axisLine: { lineStyle: { color: theme.axisLine } },
      },
      series: [
        {
          name: modeInfo.label,
          type: "scatter",
          data: orderedPoints,
          symbolSize: (value, params) => {
            if (params && params.data && params.data.position_rows !== undefined) {
              return symbolSizeFor(params.data.position_rows);
            }
            return symbolSizeFor(Array.isArray(value) ? value[2] : null);
          },
          itemStyle: {
            opacity: 0.82,
            color: (params) => {
              const raw = params && params.data && typeof params.data === "object" ? params.data : {};
              const position = String(raw.position || "").trim();
              return positionColors[position] || theme.primaryLine;
            },
          },
          emphasis: {
            itemStyle: {
              borderColor: theme.axisLine,
              borderWidth: 1.1,
              opacity: 0.95,
            },
          },
        },
      ],
    };
    mount.chart.setOption(ensureReadableAxes(option, mount), true);
    const pageLabelStart = pageStart + 1;
    const pageLabelEnd = pageEndExclusive;
    if (totalPages > 1) {
      const controls = document.createElement("div");
      controls.className = "chart-inline-controls-row";

      const previousButton = document.createElement("button");
      previousButton.type = "button";
      previousButton.className = "control-button";
      previousButton.textContent = "Prev 10";
      previousButton.disabled = pageIndex <= 0;
      previousButton.addEventListener("click", () => {
        if (mount.topNameTimingPage <= 0) {
          return;
        }
        mount.topNameTimingPage = Math.max(0, mount.topNameTimingPage - 1);
        void renderChartMount(mount);
      });

      const status = document.createElement("span");
      status.className = "tiny-note chart-inline-controls-status";
      status.textContent =
        "Names " +
        pageLabelStart +
        "-" +
        pageLabelEnd +
        " of " +
        rankedTopNames.length +
        " (page " +
        (pageIndex + 1) +
        "/" +
        totalPages +
        ")";

      const nextButton = document.createElement("button");
      nextButton.type = "button";
      nextButton.className = "control-button";
      nextButton.textContent = "Next 10";
      nextButton.disabled = pageIndex >= totalPages - 1;
      nextButton.addEventListener("click", () => {
        if (mount.topNameTimingPage >= totalPages - 1) {
          return;
        }
        mount.topNameTimingPage = Math.min(totalPages - 1, mount.topNameTimingPage + 1);
        void renderChartMount(mount);
      });

      controls.appendChild(previousButton);
      controls.appendChild(status);
      controls.appendChild(nextButton);
      setChartControls(mount.chartId, controls);
    }
    mount.customChartNote =
      "Point size scales per-position duplicate rows in each active bucket occurrence. Colors show Pro/Con/Other." +
      (definition ? " Tier: " + definition : "") +
      " Showing names " +
      pageLabelStart +
      "-" +
      pageLabelEnd +
      " of " +
      rankedTopNames.length +
      ".";
    mount.isTimeSeries = false;
    mount.isAbsoluteTime = false;
    return true;
  }

  function renderAutoChart(mount, rows) {
    if (!rows.length) {
      return false;
    }

    if (mount.chartId === "procon_swings_shift_heatmap") {
      return renderShiftHeatmap(mount, rows);
    }
    if (mount.chartId === "procon_swings_day_hour_heatmap") {
      return renderDayHourHeatmap(mount, rows, "pro_rate");
    }
    if (mount.chartId === "baseline_day_hour_volume") {
      return renderDayHourHeatmap(mount, rows, "n_total");
    }
    if (mount.chartId === "off_hours_date_hour_pro_heatmap") {
      return renderDateHourHeatmap(mount, rows, "pro_rate", "Pro rate", {
        scaleMode: "rate_diverging",
        force24HourSlots: true,
      });
    }
    if (mount.chartId === "off_hours_date_hour_primary_residual_heatmap") {
      return renderDateHourHeatmap(
        mount,
        rows,
        "z_score_primary",
        "Primary z-score",
        {
          scaleMode: "diverging",
          divergingPositiveWarm: true,
          force24HourSlots: true,
          highlightOffHoursAxis: true,
          offHoursAxisThreshold: 0.5,
        }
      );
    }
    if (mount.chartId === "off_hours_date_hour_volume_heatmap") {
      return renderDateHourHeatmap(mount, rows, "n_total", "Submission count", {
        scaleMode: "volume",
        force24HourSlots: true,
        showMissingOverlay: true,
      });
    }
    if (mount.chartId === "off_hours_funnel_plot") {
      return renderOffHoursFunnel(mount, rows);
    }
    if (mount.chartId === "off_hours_primary_flag_channels") {
      return renderSimpleBar(
        mount,
        rows,
        "channel_label",
        "count",
        "Window count"
      );
    }
    if (mount.chartId === "off_hours_model_fit_diagnostics") {
      return renderOffHoursModelFitDiagnostics(mount, rows);
    }
    if (mount.chartId === "overview_position_volume_by_bucket") {
      return renderOverviewPositionVolumeByBucket(mount, rows);
    }

    const timeOverrides = {
      baseline_volume_pro_rate: {
        timeField: "minute_bucket",
        barField: "n_total",
        lineField: "pro_rate",
        lineLow: "pro_rate_wilson_low",
        lineHigh: "pro_rate_wilson_high",
        lowPowerField: "is_low_power",
        lineAxisName: "Pro rate",
        lineMin: 0,
        lineMax: 1,
      },
      procon_swings_hero_bucket_trend: {
        timeField: "bucket_start",
        barField: "n_total",
        lineField: "pro_rate",
        lineLow: "pro_rate_wilson_low",
        lineHigh: "pro_rate_wilson_high",
        lowPowerField: "is_low_power",
        flaggedField: "is_flagged",
        extraLines: ["baseline_pro_rate", "stable_lower", "stable_upper"],
        lineAxisName: "Pro rate",
        lineMin: 0,
        lineMax: 1,
      },
      changepoints_hero_timeline: {
        timeField: "minute_bucket",
        barField: "n_total",
        lineField: "pro_rate",
        lineLow: "pro_rate_wilson_low",
        lineHigh: "pro_rate_wilson_high",
        lowPowerField: "is_low_power",
        flaggedField: "is_changepoint",
        lineAxisName: "Pro rate",
        lineMin: 0,
        lineMax: 1,
      },
      off_hours_control_timeline: {
        timeField: "bucket_start",
        barField: "n_total",
        lineField: "pro_rate",
        lineLow: "pro_rate_wilson_low",
        lineHigh: "pro_rate_wilson_high",
        lowPowerField: "is_low_power",
        flaggedField: "is_primary_two_sided_alert_window",
        inferentialWindowField: "is_alert_off_hours_window",
        sparseWhenLowSupport: true,
        sparseMinTestedPoints: 8,
        sparseMinTestedShare: 0.35,
        runOverlayField: "is_primary_two_sided_alert_window",
        extraLines: [
          "expected_pro_rate_primary",
          "control_low_95_primary",
          "control_high_95_primary",
          "expected_pro_rate_day",
        ],
        lineAxisName: "Pro rate",
        lineMin: 0,
        lineMax: 1,
      },
      off_hours_primary_residual_timeline: {
        timeField: "bucket_start",
        barField: "n_known",
        lineField: "z_score_primary",
        lowPowerField: "is_low_power",
        flaggedField: "is_primary_two_sided_alert_window",
        inferentialWindowField: "is_alert_off_hours_window",
        sparseWhenLowSupport: true,
        sparseMinTestedPoints: 8,
        sparseMinTestedShare: 0.35,
        runOverlayField: "is_primary_two_sided_alert_window",
        extraLines: ["z_score_day", "z_ref_zero", "z_ref_pos3", "z_ref_neg3"],
        barAxisName: "Known pro+con",
        lineAxisName: "Primary z-score",
      },
      rare_names_unique_ratio: {
        timeField: "minute_bucket",
        barField: "n_total",
        lineField: "unique_ratio",
        extraLines: ["threshold_unique_ratio"],
        lowPowerField: "is_low_power",
        lineAxisName: "Unique ratio",
        lineMin: 0,
        lineMax: 1,
      },
      org_anomalies_blank_rate: {
        timeField: "bucket_start",
        barField: "n_total",
        lineField: "blank_org_rate",
        lineLow: "blank_org_rate_wilson_low",
        lineHigh: "blank_org_rate_wilson_high",
        extraLines: ["pro_blank_org_rate", "con_blank_org_rate"],
        lowPowerField: "is_low_power",
        lineAxisName: "Blank org rate",
        lineMin: 0,
        lineMax: 1,
      },
      voter_registry_match_rates: {
        timeField: "bucket_start",
        barField: "n_total",
        lineField: "matched_rate",
        lineLow: "match_rate_wilson_low",
        lineHigh: "match_rate_wilson_high",
        adaptiveLineRange: true,
        adaptiveLineRangePadding: 0.12,
        adaptiveLineRangeMinSpan: 0.08,
        adaptiveLineRangeClampMin: 0,
        adaptiveLineRangeClampMax: 1,
        extraLines: [
          "matched_rate_pro",
          "matched_rate_con",
          "expected_match_rate_global",
          "control_low_95_match_global",
          "control_high_95_match_global",
        ],
        flaggedField: "is_match_rate_alert_any",
        lowPowerField: "is_low_power",
        lineAxisName: "Matched rate",
        lineMin: 0,
        lineMax: 1,
      },
      multivariate_score_timeline: {
        timeField: "bucket_start",
        barField: "n_total",
        lineField: "anomaly_score",
        extraLines: ["anomaly_score_percentile"],
        lowPowerField: "is_low_power",
        lineAxisName: "Anomaly score",
      },
      procon_swings_direction_runs: {
        timeField: "start_bucket",
        barField: "run_length_buckets",
        lineField: "mean_abs_delta_pro_rate",
        lineAxisName: "Mean abs delta",
        barAxisName: "Run length",
      },
      composite_score_timeline: {
        timeField: "minute_bucket",
        barField: "n_total",
        lineField: "composite_score",
        lowPowerField: "is_low_power",
        lineAxisName: "Composite score",
      },
      duplicates_exact_bucket_concentration: {
        timeField: "bucket_start",
        barField: "n_rows",
        lineField: "duplicate_rows",
        extraLines: ["expected_duplicate_rows", "excess_duplicate_rows"],
        lineAxisName: "Duplicate rows",
        barAxisName: "Total rows",
      },
      sortedness_bucket_ratio: {
        timeField: "bucket_start",
        barField: "n_records",
        lineField: "is_alphabetical",
        lineAxisName: "Alphabetical (0/1)",
        lineMin: 0,
        lineMax: 1,
      },
      org_anomalies_position_rates: {
        timeField: "bucket_start",
        barField: "n_total",
        lineField: "blank_org_rate",
        lineLow: "blank_org_rate_wilson_low",
        lineHigh: "blank_org_rate_wilson_high",
        lowPowerField: "is_low_power",
        lineAxisName: "Blank org rate",
        lineMin: 0,
        lineMax: 1,
      },
      voter_registry_position_buckets: {
        timeField: "bucket_start",
        barField: "n_total",
        lineField: "matched_rate",
        lineLow: "match_rate_wilson_low",
        lineHigh: "match_rate_wilson_high",
        extraLines: ["unmatched_rate"],
        lowPowerField: "is_low_power",
        lineAxisName: "Matched rate",
        lineMin: 0,
        lineMax: 1,
      },
      rare_names_singletons: {
        timeField: "first_seen",
        barField: "n_pro",
        lineField: "n_con",
        lineAxisName: "Counts",
        barAxisName: "Pro count",
      },
      rare_names_rarity_timeline: {
        timeField: "minute_bucket",
        barField: "n_total",
        lineField: "rarity_median",
        extraLines: ["rarity_p95"],
        lowPowerField: "is_low_power",
        lineAxisName: "Rarity",
      },
      bursts_hero_timeline: {
        timeField: "start_minute",
        barField: "observed_count",
        lineField: "rate_ratio",
        lineAxisName: "Rate ratio",
        barAxisName: "Observed count",
      },
      bursts_composition_shift: {
        timeField: "start_minute",
        barField: "observed_count",
        lineField: "abs_delta_pro_rate",
        lowPowerField: "is_low_power",
        lineAxisName: "Abs pro-rate delta",
        barAxisName: "Observed count",
        lineMin: 0,
      },
    };

    if (Object.prototype.hasOwnProperty.call(timeOverrides, mount.chartId)) {
      return renderTimeBarLine(mount, rows, timeOverrides[mount.chartId]);
    }

    if (mount.chartId === "multivariate_feature_projection") {
      return renderScatter(mount, rows, "log_n_total", "pro_rate", "anomaly_score", "n_total");
    }
    if (mount.chartId === "multivariate_top_buckets") {
      return renderScatter(mount, rows, "n_total", "anomaly_score", "anomaly_score_percentile", "n_total");
    }
    if (mount.chartId === "composite_evidence_flags") {
      return renderSimpleBar(mount, rows, "flag", "count", "count");
    }
    if (mount.chartId === "off_hours_hourly_profile") {
      return renderSimpleBar(mount, rows, "hour", "n_total", "submissions");
    }
    if (mount.chartId === "off_hours_summary_compare") {
      return renderSimpleBar(mount, rows, "off_hours", "off_hours_pro_rate", "pro rate");
    }
    if (mount.chartId === "periodicity_clockface") {
      return renderSimpleBar(mount, rows, "minute_of_hour", "n_events", "events");
    }
    if (mount.chartId === "periodicity_autocorr") {
      return renderSimpleBar(mount, rows, "lag_minutes", "autocorr", "autocorr");
    }
    if (mount.chartId === "periodicity_spectrum") {
      return renderSimpleBar(mount, rows, "period_minutes", "power", "power");
    }
    if (mount.chartId === "periodicity_rolling_fano") {
      return renderSimpleBar(
        mount,
        rows,
        "window_minutes",
        "median_fano_factor",
        "median fano factor"
      );
    }
    if (mount.chartId === "baseline_top_names") {
      return renderSimpleBar(mount, rows, "display_name", "n", "count");
    }
    if (mount.chartId === "baseline_name_length_distribution") {
      return renderSimpleBar(mount, rows, "name_length", "n_names", "names");
    }
    if (mount.chartId === "bursts_significance_by_window") {
      return renderSimpleBar(mount, rows, "window_minutes", "n_significant", "significant windows");
    }
    if (mount.chartId === "bursts_null_distribution") {
      return renderSimpleBar(mount, rows, "iteration", "max_window_count", "max count");
    }
    if (mount.chartId === "procon_swings_null_distribution") {
      return renderSimpleBar(mount, rows, "iteration", "max_abs_delta_pro_rate", "max abs delta");
    }
    if (mount.chartId === "changepoints_magnitude") {
      return renderSimpleBar(mount, rows, "change_index", "abs_delta", "abs delta");
    }
    if (mount.chartId === "changepoints_hour_hist") {
      return renderSimpleBar(mount, rows, "change_hour", "n_changes", "changes");
    }
    if (mount.chartId === "duplicates_exact_top_names") {
      return renderSimpleBar(mount, rows, "display_name", "n", "count");
    }
    if (mount.chartId === "duplicates_exact_per_name_anomalies") {
      return renderSimpleBar(mount, rows, "display_name", "n", "repeat count");
    }
    if (mount.chartId === "duplicates_exact_top_name_timing_exact") {
      return renderDuplicateTopNameTiming(mount, rows, "exact");
    }
    if (mount.chartId === "duplicates_exact_metric_diagnostics") {
      return renderSimpleBar(mount, rows, "metric", "observed", "observed");
    }
    if (mount.chartId === "duplicates_exact_position_switch") {
      return renderSimpleBar(mount, rows, "display_name", "n", "count");
    }
    if (mount.chartId === "duplicates_exact_position_concentration") {
      return renderDuplicatePositionConcentration(mount, rows);
    }
    if (mount.chartId === "duplicates_exact_null_distribution") {
      return renderSimpleBar(mount, rows, "iteration", "duplicate_rows", "duplicate rows");
    }
    if (mount.chartId === "duplicates_exact_temporal_burst") {
      return renderSimpleBar(mount, rows, "canonical_name", "within_5m_pairs", "within 5m pairs");
    }
    if (mount.chartId === "duplicates_exact_swing_impact") {
      return renderSimpleBar(mount, rows, "scenario", "pro_share", "pro share");
    }
    if (mount.chartId === "sortedness_bucket_summary") {
      return renderSimpleBar(mount, rows, "bucket_minutes", "alphabetical_ratio", "alphabetical ratio");
    }
    if (mount.chartId === "sortedness_kendall_tau_summary") {
      return renderSimpleBar(
        mount,
        rows,
        "bucket_minutes",
        "mean_abs_kendall_tau",
        "mean abs kendall tau"
      );
    }
    if (mount.chartId === "sortedness_minute_spikes") {
      return renderSimpleBar(mount, rows, "minute_bucket", "n_records", "records");
    }
    if (mount.chartId === "rare_names_weird_scores") {
      return renderSimpleBar(mount, rows, "sample_name", "weirdness_score", "weirdness");
    }
    if (mount.chartId === "org_anomalies_bursts") {
      return renderSimpleBar(mount, rows, "minute_bucket", "n", "count");
    }
    if (mount.chartId === "org_anomalies_top_orgs") {
      return renderSimpleBar(mount, rows, "organization_clean", "n", "count");
    }
    if (mount.chartId === "voter_registry_match_by_position") {
      return renderSimpleBar(mount, rows, "position_normalized", "match_rate", "match rate");
    }
    if (mount.chartId === "voter_registry_linkage_by_position_rows") {
      return renderSimpleBar(mount, rows, "position_normalized", "unmatched_rate", "unmatched rate");
    }
    if (mount.chartId === "voter_registry_linkage_by_position_unique") {
      return renderSimpleBar(mount, rows, "position_normalized", "unmatched_rate", "unmatched rate");
    }
    if (mount.chartId === "voter_registry_pairwise_tests") {
      return renderSimpleBar(mount, rows, "pair_label", "rate_difference", "rate difference");
    }
    if (mount.chartId === "voter_registry_sensitivity_modes") {
      return renderSimpleBar(mount, rows, "mode", "unmatched_rate_rows", "unmatched rate");
    }
    if (mount.chartId === "voter_registry_unmatched_names") {
      return renderSimpleBar(mount, rows, "display_name", "n_records", "count");
    }
    if (mount.chartId === "voter_registry_match_tiers") {
      const yField = rows.length && rows[0].record_rate !== undefined
        ? "record_rate"
        : "unmatched_rate_rows";
      const xField = rows.length && rows[0].match_tier !== undefined ? "match_tier" : "mode";
      return renderSimpleBar(mount, rows, xField, yField, "record rate");
    }
    if (mount.chartId === "procon_swings_time_of_day_profile") {
      return renderSimpleBar(mount, rows, "slot_start_minute", "pro_rate", "pro rate");
    }
    if (mount.chartId === "composite_high_priority") {
      return renderSimpleBar(mount, rows, "minute_bucket", "composite_score", "score");
    }

    const timeField = inferTimeField(rows);
    if (timeField) {
      const fields = numericFields(rows[0]);
      const lineField = fields.find((field) => field !== "bucket_minutes") || null;
      if (lineField) {
        return renderTimeBarLine(mount, rows, {
          timeField: timeField,
          barField: null,
          lineField: lineField,
          lineAxisName: lineField,
        });
      }
    }

    const row = rows[0] || {};
    const numeric = numericFields(row);
    const stringField = Object.keys(row).find(
      (key) => typeof row[key] === "string" || typeof row[key] === "boolean"
    );

    if (stringField && numeric.length) {
      return renderSimpleBar(mount, rows, stringField, numeric[0], numeric[0]);
    }

    if (numeric.length >= 2) {
      return renderScatter(mount, rows, numeric[0], numeric[1], null, null);
    }

    return false;
  }

  function humanizeFieldName(field) {
    return String(field || "")
      .replace(/_/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function fallbackColumnDescription(field) {
    const normalized = String(field || "").trim();
    const label = humanizeFieldName(normalized);
    if (!normalized) {
      return "Column value from detector output.";
    }
    if (normalized.startsWith("n_")) {
      return "Count of " + humanizeFieldName(normalized.slice(2)) + " in this row grouping.";
    }
    if (normalized.endsWith("_rate")) {
      return "Proportion metric for " + label + " on a 0 to 1 scale.";
    }
    if (normalized.endsWith("_ratio")) {
      return "Ratio metric for " + label + ".";
    }
    if (normalized.endsWith("_wilson_low")) {
      return "Lower Wilson confidence bound for " + humanizeFieldName(normalized.replace(/_wilson_low$/, "")) + ".";
    }
    if (normalized.endsWith("_wilson_high")) {
      return "Upper Wilson confidence bound for " + humanizeFieldName(normalized.replace(/_wilson_high$/, "")) + ".";
    }
    if (normalized.startsWith("is_")) {
      return "Boolean indicator for " + label + ".";
    }
    if (
      normalized.includes("minute") ||
      normalized.includes("hour") ||
      normalized.endsWith("_time") ||
      normalized.endsWith("_date")
    ) {
      return "Time coordinate for " + label + ".";
    }
    return "Detector output field for " + label + ".";
  }

  function tableColumnsFromRows(rows) {
    const columns = [];
    const seen = new Set();
    (Array.isArray(rows) ? rows : []).forEach((row) => {
      Object.keys(row || {}).forEach((field) => {
        if (seen.has(field)) {
          return;
        }
        seen.add(field);
        columns.push(field);
      });
    });
    return columns;
  }

  function renderColumnGlossary(parent, tableKey, rows) {
    if (!parent) {
      return;
    }
    const fields = tableColumnsFromRows(rows);
    if (!fields.length) {
      return;
    }
    const docsByTable = tableKey && tableColumnDocs[tableKey] ? tableColumnDocs[tableKey] : {};

    const glossary = document.createElement("section");
    glossary.className = "column-glossary";

    const title = document.createElement("p");
    title.className = "column-glossary-title";
    title.innerHTML = "<strong>Column glossary:</strong> what each field means in this table.";
    glossary.appendChild(title);

    const list = document.createElement("dl");
    list.className = "column-glossary-grid";
    fields.forEach((field) => {
      const wrapper = document.createElement("div");
      wrapper.className = "column-glossary-item";

      const key = document.createElement("dt");
      const code = document.createElement("code");
      code.textContent = field;
      key.appendChild(code);

      const value = document.createElement("dd");
      value.textContent = docsByTable[field] || fallbackColumnDescription(field);

      wrapper.appendChild(key);
      wrapper.appendChild(value);
      list.appendChild(wrapper);
    });
    glossary.appendChild(list);
    parent.appendChild(glossary);
  }

  function fallbackTableHelp(tableKey, rows) {
    const columns = tableColumnsFromRows(rows);
    const hasRates = columns.some((name) => name.endsWith("_rate") || name.includes("ratio"));
    const hasCounts = columns.some((name) => name.startsWith("n_") || name === "count" || name === "n");
    const hasTime = columns.some(
      (name) =>
        name.includes("minute") ||
        name.includes("hour") ||
        name.includes("bucket") ||
        name.includes("date")
    );
    const context = [];
    if (hasRates) {
      context.push("rate/proportion fields");
    }
    if (hasCounts) {
      context.push("count/volume fields");
    }
    if (hasTime) {
      context.push("time keys");
    }
    const contextText = context.length ? context.join(", ") : "detector-specific fields";
    const tableLabel = humanizeFieldName((tableKey || "table").replace(/\./g, " "));

    return {
      what_is_this:
        "This table preview shows row-level values for " +
        tableLabel +
        ", including " +
        contextText +
        ". It is the direct evidence layer behind chart summaries and is the best place to verify exact records.",
      why_it_matters:
        "The table is the audit source of truth behind chart aggregates. It confirms whether chart patterns are backed by real support and helps catch low-power or contradictory rows that can mislead visual interpretation.",
      how_to_interpret:
        "Sort and filter around flagged windows or categories, then compare neighboring rows to distinguish random outliers from consistent structure. Read identifier/time keys first, then volume and rate fields, then flags and derived scores.",
      what_to_look_for:
        "Look for multiple indicators moving together, especially when elevated values persist across adjacent rows with adequate support. Repeated combinations are stronger evidence than one extreme field in one row.",
      momentary_high_low:
        "A single high/low row can be event noise or low-support variance; verify in nearby rows and linked charts. Momentary highs often map to reminders or queue releases, while momentary lows often map to normal lulls or ingest timing.",
      extended_high_low:
        "Sustained high/low runs across many rows suggest process-level behavior and deserve higher confidence. Extended highs can indicate durable mobilization or process skew; extended lows can indicate reduced activity or missing data segments.",
      column_highlight:
        "Primary columns in preview: " +
        (columns.slice(0, 6).join(", ") || "none") +
        ". Use the glossary below for per-column definitions before drawing conclusions.",
    };
  }

  function renderTableHelpCard(parent, tableKey, rows) {
    if (!parent) {
      return;
    }
    const explicit = tableKey && tableHelpDocs[tableKey] ? tableHelpDocs[tableKey] : null;
    const help = explicit || fallbackTableHelp(tableKey, rows);

    const details = document.createElement("details");
    details.className = "table-help-card";

    const summary = document.createElement("summary");
    summary.textContent = "Table Help";
    details.appendChild(summary);

    const body = document.createElement("div");
    body.className = "table-help-body";

    const fields = [
      ["What is this?", help.what_is_this],
      ["Why this data matters", help.why_it_matters],
      ["How do I interpret this data?", help.how_to_interpret],
      ["What do I look for?", help.what_to_look_for],
      ["What could a momentary high/low mean?", help.momentary_high_low],
      ["What could an extended high/low mean?", help.extended_high_low],
      ["Column focus", help.column_highlight],
    ];
    fields.forEach((entry) => {
      if (!entry[1]) {
        return;
      }
      const p = document.createElement("p");
      p.innerHTML = "<strong>" + entry[0] + ":</strong> " + entry[1];
      body.appendChild(p);
    });

    renderColumnGlossary(body, tableKey, rows);
    details.appendChild(body);
    parent.appendChild(details);
  }

  const tableSemanticClassNames = [
    "table-cell-semantic-alert",
    "table-cell-semantic-warn",
    "table-cell-semantic-context",
  ];
  const offHoursSummaryAlertColumns = new Set([
    "off_hours_windows_primary_alert",
    "off_hours_windows_primary_alert_fraction",
    "off_hours_primary_alert_run_count",
    "off_hours_primary_alert_max_run_minutes",
    "off_hours_windows_significant_primary",
    "off_hours_windows_significant_primary_upper",
    "off_hours_windows_significant_primary_two_sided",
    "off_hours_windows_below_primary_control_998",
    "off_hours_windows_above_primary_control_998",
    "off_hours_windows_primary_flag_any",
    "off_hours_windows_primary_flag_any_fraction",
    "off_hours_windows_primary_flag_both",
    "off_hours_windows_primary_flag_both_fraction",
  ]);
  const offHoursSummaryLowPowerColumns = new Set([
    "off_hours_windows_alert_eligible_low_power",
    "off_hours_windows_alert_eligible_low_power_fraction",
    "off_hours_is_low_power",
    "on_hours_is_low_power",
  ]);
  const offHoursSummaryContextColumns = new Set([
    "off_hours_windows_model_available",
    "off_hours_windows_alert_eligible_tested_fraction",
  ]);

  function clearSemanticCellClasses(element) {
    if (!element) {
      return;
    }
    tableSemanticClassNames.forEach((name) => element.classList.remove(name));
  }

  function semanticClassForTableCell(tableKey, field, value) {
    const key = String(tableKey || "").trim();
    const column = String(field || "").trim();
    if (!key || !column) {
      return "";
    }

    const numeric = toFiniteNumberOrNull(value);
    const truthy = toBool(value);
    const valueText = String(value === null || value === undefined ? "" : value)
      .trim()
      .toLowerCase();

    if (key === "off_hours.off_hours_summary") {
      if (offHoursSummaryAlertColumns.has(column)) {
        if (numeric !== null) {
          return numeric > 0 ? "table-cell-semantic-alert" : "";
        }
        return truthy ? "table-cell-semantic-alert" : "";
      }
      if (offHoursSummaryLowPowerColumns.has(column)) {
        if (numeric !== null) {
          return numeric > 0 ? "table-cell-semantic-warn" : "";
        }
        return truthy ? "table-cell-semantic-warn" : "";
      }
      if (offHoursSummaryContextColumns.has(column)) {
        if (numeric !== null) {
          return numeric > 0 ? "table-cell-semantic-context" : "";
        }
        if (!valueText) {
          return "";
        }
        if (valueText.includes("unavailable")) {
          return "table-cell-semantic-alert";
        }
        return valueText === "available" || valueText === "true"
          ? "table-cell-semantic-context"
          : "";
      }
      if (column === "primary_baseline_method" || column === "primary_model_fit_method") {
        if (!valueText) {
          return "";
        }
        if (
          valueText.includes("unavailable") ||
          valueText.includes("failure") ||
          valueText.includes("none")
        ) {
          return "table-cell-semantic-alert";
        }
        return "";
      }
      if (column === "primary_model_fit_converged") {
        if (numeric === null) {
          return "table-cell-semantic-warn";
        }
        return numeric >= 1 ? "table-cell-semantic-context" : "table-cell-semantic-alert";
      }
    }

    if (key === "off_hours.model_fit_diagnostics") {
      if (column === "model_fit_available_fraction") {
        if (numeric === null) {
          return "table-cell-semantic-warn";
        }
        if (numeric >= 0.8) {
          return "table-cell-semantic-context";
        }
        if (numeric >= 0.4) {
          return "table-cell-semantic-warn";
        }
        return "table-cell-semantic-alert";
      }
      if (column === "model_fit_converged") {
        if (numeric === null) {
          return "table-cell-semantic-warn";
        }
        return numeric >= 1 ? "table-cell-semantic-context" : "table-cell-semantic-alert";
      }
      if (column === "model_fit_method") {
        if (!valueText) {
          return "";
        }
        if (valueText.includes("unavailable") || valueText.includes("failure")) {
          return "table-cell-semantic-alert";
        }
        return "";
      }
    }

    return "";
  }

  function mountTable(container, rows, options) {
    const dataset = Array.isArray(rows) ? rows : [];
    if (!container || !dataset.length) {
      if (container) {
        clearStructuredHostClasses(container);
        container.innerHTML = "";
      }
      return null;
    }
    clearStructuredHostClasses(container);
    const tableKey =
      options && typeof options.tableKey === "string" ? options.tableKey.trim() : "";
    const tableOptions = Object.assign({}, options || {});
    if (Object.prototype.hasOwnProperty.call(tableOptions, "tableKey")) {
      delete tableOptions.tableKey;
    }
    const defaultTableMaxHeightPx = 340;
    const tableHeightBumpPx = 64;
    const requestedMaxHeight = Object.prototype.hasOwnProperty.call(tableOptions, "maxHeight")
      ? tableOptions.maxHeight
      : defaultTableMaxHeightPx;
    const requestedMaxHeightPx = parsePixelLikeValue(requestedMaxHeight);
    if (requestedMaxHeightPx !== null) {
      tableOptions.maxHeight = `${Math.max(
        160,
        Math.round(requestedMaxHeightPx + tableHeightBumpPx)
      )}px`;
    }

    const columns = Array.from(
      new Set(dataset.flatMap((row) => Object.keys(row || {})))
    ).map((field) => ({
      title: field,
      field: field,
      headerFilter: "input",
      formatter: (cell) => {
        const value = cell && typeof cell.getValue === "function" ? cell.getValue() : null;
        const element = cell && typeof cell.getElement === "function" ? cell.getElement() : null;
        clearSemanticCellClasses(element);
        const semanticClass = semanticClassForTableCell(tableKey, field, value);
        if (semanticClass && element) {
          element.classList.add(semanticClass);
        }
        if (value === null || value === undefined) {
          return "";
        }
        return String(value);
      },
    }));

    if (hasTabulator) {
      container.innerHTML = "";
      const table = new window.Tabulator(
        container,
        Object.assign(
          {
            data: dataset,
            columns: columns,
            layout: "fitDataStretch",
            reactiveData: false,
            pagination: true,
            paginationSize: 8,
            paginationCounter: "rows",
            maxHeight: "340px",
            placeholder: "No rows",
            movableColumns: true,
          },
          tableOptions
        )
      );
      return {
        kind: "tabulator",
        table: table,
        data: dataset,
      };
    }

    const table = document.createElement("table");
    table.style.borderCollapse = "collapse";
    table.style.width = "100%";

    const thead = document.createElement("thead");
    const headerRow = document.createElement("tr");
    columns.forEach((column) => {
      const th = document.createElement("th");
      th.textContent = column.title;
      th.style.border = "1px solid #cbd5e1";
      th.style.padding = "0.35rem 0.45rem";
      th.style.background = "#f8fafc";
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    const rowClickHandler =
      options && typeof options.rowClick === "function" ? options.rowClick : null;
    dataset.forEach((row) => {
      const tr = document.createElement("tr");
      columns.forEach((column) => {
        const td = document.createElement("td");
        const value = row[column.field];
        td.textContent = value === null || value === undefined ? "" : String(value);
        const semanticClass = semanticClassForTableCell(tableKey, column.field, value);
        if (semanticClass) {
          td.classList.add(semanticClass);
        }
        td.style.border = "1px solid #e2e8f0";
        td.style.padding = "0.3rem 0.45rem";
        tr.appendChild(td);
      });
      if (rowClickHandler) {
        tr.style.cursor = "pointer";
        tr.tabIndex = 0;
        tr.addEventListener("click", (event) => {
          rowClickHandler(event, {
            getData: () => row,
            getElement: () => tr,
          });
        });
        tr.addEventListener("keydown", (event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            rowClickHandler(event, {
              getData: () => row,
              getElement: () => tr,
            });
          }
        });
      }
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);

    container.innerHTML = "";
    container.appendChild(table);
    return {
      kind: "html",
      table: null,
      data: dataset,
    };
  }

  function normalizeDuplicateNameLookupKey(value) {
    const normalized = String(value || "").trim().toUpperCase();
    if (!normalized) {
      return "";
    }
    return normalized.replace(/\|+$/g, "");
  }

  function formatMinutesAsDayHourMinute(rawMinutes) {
    const parsed = toFiniteNumberOrNull(rawMinutes);
    if (parsed === null || parsed < 0) {
      return "-";
    }
    const totalMinutes = Math.max(0, Math.round(parsed));
    const days = Math.floor(totalMinutes / (24 * 60));
    const hours = Math.floor((totalMinutes % (24 * 60)) / 60);
    const minutes = totalMinutes % 60;
    return String(days) + "d " + String(hours) + "h " + String(minutes) + "m";
  }

  function duplicateNameCountForRow(row) {
    const observed = toFiniteNumberOrNull((row || {}).observed_count);
    if (observed !== null) {
      return Math.max(0, Math.round(observed));
    }
    const legacyCount = toFiniteNumberOrNull((row || {}).n);
    return legacyCount === null ? 0 : Math.max(0, Math.round(legacyCount));
  }

  function duplicateNameDisplayForRow(row) {
    const displayName = String((row || {}).display_name || "").trim();
    if (displayName) {
      return displayName;
    }
    const canonical = String((row || {}).canonical_name || (row || {}).name_key || "").trim();
    return canonical || "Unknown";
  }

  function buildUnifiedDuplicateNameRows(detectorTables) {
    const sourcePriorities = {
      per_name_tests: 3,
      per_name_display: 2,
      per_name_anomalies: 1,
    };
    const sources = [
      ["per_name_tests", Array.isArray(detectorTables.per_name_tests) ? detectorTables.per_name_tests : []],
      ["per_name_display", Array.isArray(detectorTables.per_name_display) ? detectorTables.per_name_display : []],
      ["per_name_anomalies", Array.isArray(detectorTables.per_name_anomalies) ? detectorTables.per_name_anomalies : []],
    ];

    const mergedByKey = new Map();
    sources.forEach((entry) => {
      const sourceName = entry[0];
      const rows = entry[1];
      const sourcePriority = sourcePriorities[sourceName] || 0;
      rows.forEach((row) => {
        const signIns = duplicateNameCountForRow(row);
        if (signIns < 2) {
          return;
        }
        const scope = String((row || {}).scope || "").trim();
        const canonicalName = String((row || {}).canonical_name || (row || {}).name_key || "").trim();
        const displayName = duplicateNameDisplayForRow(row);
        const canonicalLookupKey = normalizeDuplicateNameLookupKey(canonicalName);
        const displayLookupKey = normalizeDuplicateNameLookupKey(displayName);
        const lookupKey = canonicalLookupKey || displayLookupKey;
        if (!lookupKey) {
          return;
        }
        const nPro = Math.max(0, Math.round(toNumber((row || {}).n_pro)));
        const nCon = Math.max(0, Math.round(toNumber((row || {}).n_con)));
        const timeSpanMinutes = toFiniteNumberOrNull((row || {}).time_span_minutes);
        const mapKey = String(scope || "all") + "::" + lookupKey;
        const candidate = {
          mapKey: mapKey,
          scope: scope,
          lookupKey: lookupKey,
          displayLookupKey: displayLookupKey,
          canonicalName: canonicalName,
          displayName: displayName,
          signIns: signIns,
          nPro: nPro,
          nCon: nCon,
          timeSpanMinutes: timeSpanMinutes,
          sourcePriority: sourcePriority,
        };
        const existing = mergedByKey.get(mapKey);
        if (!existing) {
          mergedByKey.set(mapKey, candidate);
          return;
        }
        if (candidate.signIns > existing.signIns) {
          mergedByKey.set(mapKey, candidate);
          return;
        }
        if (candidate.signIns === existing.signIns && candidate.sourcePriority > existing.sourcePriority) {
          mergedByKey.set(mapKey, candidate);
          return;
        }
        if (
          candidate.signIns === existing.signIns &&
          candidate.sourcePriority === existing.sourcePriority &&
          candidate.timeSpanMinutes !== null &&
          existing.timeSpanMinutes === null
        ) {
          mergedByKey.set(mapKey, candidate);
        }
      });
    });

    const mergedRows = Array.from(mergedByKey.values());
    const scopedRows = mergedRows.filter(
      (row) => row.scope && row.scope === String(state.activeDuplicateScope || "")
    );
    const activeRows = scopedRows.length ? scopedRows : mergedRows;
    activeRows.sort((left, right) => {
      const signInDelta = toNumber(right.signIns) - toNumber(left.signIns);
      if (signInDelta !== 0) {
        return signInDelta;
      }
      return String(left.displayName || "").localeCompare(String(right.displayName || ""));
    });

    return activeRows.map((row) => ({
      __row_id: row.mapKey,
      __lookup_key: row.lookupKey,
      __display_lookup_key: row.displayLookupKey,
      __scope: row.scope,
      Name: row.displayName,
      "# Sign-ins": row.signIns,
      "# Pro": row.nPro,
      "# Con": row.nCon,
      "Submission period": formatMinutesAsDayHourMinute(row.timeSpanMinutes),
    }));
  }

  function buildDuplicateNameTimingLookup(detectorTables) {
    const lookup = new Map();
    const addEntry = (lookupKey, entry) => {
      const key = String(lookupKey || "").trim();
      if (!key) {
        return;
      }
      const list = lookup.get(key) || [];
      list.push(entry);
      lookup.set(key, list);
    };

    const repeatedRowsRaw = Array.isArray(detectorTables.repeated_same_bucket)
      ? detectorTables.repeated_same_bucket
      : [];
    const repeatedRowsSelection = filterRowsByDuplicateTableBucket(
      "repeated_same_bucket",
      repeatedRowsRaw
    );
    repeatedRowsSelection.rows.forEach((row) => {
      const lookupKey = normalizeDuplicateNameLookupKey((row || {}).canonical_name);
      const timestamp = toEpochMillis((row || {}).bucket_start);
      const nTotal = toFiniteNumberOrNull((row || {}).n);
      if (!lookupKey || timestamp === null || nTotal === null || nTotal <= 0) {
        return;
      }
      const nPro = Math.max(0, Math.round(toNumber((row || {}).n_pro)));
      const nCon = Math.max(0, Math.round(toNumber((row || {}).n_con)));
      const reportedOther = toFiniteNumberOrNull((row || {}).n_unknown);
      const nOther =
        reportedOther === null
          ? Math.max(0, Math.round(nTotal) - nPro - nCon)
          : Math.max(0, Math.round(reportedOther));
      addEntry(lookupKey, {
        timestamp: timestamp,
        nTotal: Math.max(1, Math.round(nTotal)),
        nPro: nPro,
        nCon: nCon,
        nOther: nOther,
        bucketMinutes: toFiniteNumberOrNull((row || {}).bucket_minutes),
      });
    });

    const topTimingRaw = Array.isArray(detectorTables.top_name_timing_by_mode)
      ? detectorTables.top_name_timing_by_mode
      : [];
    const topTimingSelection = filterRowsByDuplicateTableBucket("top_name_timing_by_mode", topTimingRaw);
    const topTimingRows = topTimingSelection.rows.filter((row) => {
      const scope = String((row || {}).scope || "").trim();
      if (scope && scope !== String(state.activeDuplicateScope || "")) {
        return false;
      }
      const matchMode = String((row || {}).match_mode || "").trim().toLowerCase();
      return !matchMode || matchMode === "exact";
    });
    topTimingRows.forEach((row) => {
      const lookupKey = normalizeDuplicateNameLookupKey((row || {}).name_key || (row || {}).canonical_name);
      if (!lookupKey || (lookup.get(lookupKey) || []).length) {
        return;
      }
      const timestamp = toEpochMillis((row || {}).bucket_start);
      const nTotal = toFiniteNumberOrNull((row || {}).duplicate_rows);
      if (timestamp === null || nTotal === null || nTotal <= 0) {
        return;
      }
      const nPro = Math.max(0, Math.round(toNumber((row || {}).n_pro)));
      const nCon = Math.max(0, Math.round(toNumber((row || {}).n_con)));
      const reportedOther = toFiniteNumberOrNull((row || {}).n_other);
      const nOther =
        reportedOther === null
          ? Math.max(0, Math.round(nTotal) - nPro - nCon)
          : Math.max(0, Math.round(reportedOther));
      addEntry(lookupKey, {
        timestamp: timestamp,
        nTotal: Math.max(1, Math.round(nTotal)),
        nPro: nPro,
        nCon: nCon,
        nOther: nOther,
        bucketMinutes: toFiniteNumberOrNull((row || {}).bucket_minutes),
      });
    });

    lookup.forEach((entries, key) => {
      entries.sort((left, right) => toNumber(left.timestamp) - toNumber(right.timestamp));
      const deduped = [];
      const seen = new Set();
      entries.forEach((entry) => {
        const signature = [
          toNumber(entry.timestamp),
          toNumber(entry.nTotal),
          toNumber(entry.nPro),
          toNumber(entry.nCon),
          toNumber(entry.nOther),
        ].join("|");
        if (seen.has(signature)) {
          return;
        }
        seen.add(signature);
        deduped.push(entry);
      });
      lookup.set(key, deduped);
    });

    return {
      rowsByLookupKey: lookup,
      bucketNote: repeatedRowsSelection.note || topTimingSelection.note || "",
    };
  }

  function ensureDuplicateInlineDetailScaffold(detailElement) {
    if (!detailElement) {
      return null;
    }
    let summary = detailElement.querySelector(".duplicate-name-inline-summary");
    if (!summary) {
      summary = document.createElement("p");
      summary.className = "duplicate-name-inline-summary";
      detailElement.appendChild(summary);
    }
    let note = detailElement.querySelector(".duplicate-name-inline-note");
    if (!note) {
      note = document.createElement("p");
      note.className = "duplicate-name-inline-note";
      detailElement.appendChild(note);
    }
    let legend = detailElement.querySelector(".duplicate-name-inline-legend");
    if (!legend) {
      legend = document.createElement("div");
      legend.className = "duplicate-name-inline-legend";
      detailElement.appendChild(legend);
    }
    let chartHost = detailElement.querySelector(".duplicate-name-inline-chart");
    if (!chartHost) {
      chartHost = document.createElement("div");
      chartHost.className = "duplicate-name-inline-chart";
      chartHost.style.height = String(DUPLICATE_INLINE_TIMING_CHART_HEIGHT_PX) + "px";
      detailElement.appendChild(chartHost);
    }
    return {
      summary: summary,
      note: note,
      legend: legend,
      chartHost: chartHost,
    };
  }

  function disposeDuplicateInlineTimingChart(chartHost) {
    if (!chartHost) {
      return;
    }
    const chart = chartHost.__duplicateInlineChart;
    if (!chart || typeof chart.dispose !== "function") {
      return;
    }
    try {
      chart.dispose();
    } catch (_error) {}
    chartHost.__duplicateInlineChart = null;
  }

  function renderDuplicateInlineTimingChart(chartHost, displayName, timelineRows) {
    if (!chartHost) {
      return;
    }
    if (!Array.isArray(timelineRows) || !timelineRows.length || !hasEcharts) {
      disposeDuplicateInlineTimingChart(chartHost);
      return;
    }
    const theme = currentChartTheme();
    const positionColors = {
      Pro: theme.contextLine,
      Con: theme.alertLower,
      Other: theme.referenceLine,
    };

    const points = [];
    timelineRows.forEach((entry) => {
      const timestamp = toFiniteNumberOrNull((entry || {}).timestamp);
      if (timestamp === null) {
        return;
      }
      [
        ["Pro", (entry || {}).nPro],
        ["Con", (entry || {}).nCon],
        ["Other", (entry || {}).nOther],
      ].forEach((positionEntry) => {
        const position = positionEntry[0];
        const positionRows = Math.max(0, Math.round(toNumber(positionEntry[1])));
        if (positionRows <= 0) {
          return;
        }
        points.push({
          value: [timestamp, String(displayName || "Name"), positionRows],
          position: position,
          position_rows: positionRows,
          n_total: Math.max(1, Math.round(toNumber((entry || {}).nTotal))),
          n_pro: Math.max(0, Math.round(toNumber((entry || {}).nPro))),
          n_con: Math.max(0, Math.round(toNumber((entry || {}).nCon))),
          n_other: Math.max(0, Math.round(toNumber((entry || {}).nOther))),
          bucket_minutes: toFiniteNumberOrNull((entry || {}).bucketMinutes),
        });
      });
    });

    if (!points.length) {
      disposeDuplicateInlineTimingChart(chartHost);
      return;
    }

    let chart = chartHost.__duplicateInlineChart;
    if (!chart || typeof chart.setOption !== "function" || (chart.isDisposed && chart.isDisposed())) {
      chart = window.echarts.init(chartHost);
      chartHost.__duplicateInlineChart = chart;
    }

    const pointSizes = points.map((point) => Math.max(1, toNumber(point.position_rows)));
    const minRows = pointSizes.length ? Math.min(...pointSizes) : 1;
    const maxRowsRaw = pointSizes.length ? Math.max(...pointSizes) : 1;
    const maxRows = maxRowsRaw > minRows ? maxRowsRaw : minRows + 1;
    const symbolSizeFor = (valueRaw) => {
      const value = Math.max(1, toNumber(valueRaw));
      if (!(maxRows > minRows)) {
        return 12;
      }
      const ratio = Math.max(0, Math.min(1, (value - minRows) / (maxRows - minRows)));
      return 8 + ratio * 14;
    };

    const option = {
      animation: false,
      grid: { left: 62, right: 22, top: 18, bottom: 48 },
      tooltip: {
        trigger: "item",
        appendToBody: true,
        confine: false,
        formatter: (params) => {
          const raw = params && typeof params.data === "object" ? params.data : {};
          const timestamp = toFiniteNumberOrNull(Array.isArray(params.value) ? params.value[0] : null);
          const bucketMinutes = toFiniteNumberOrNull(raw.bucket_minutes);
          const lines = ["<strong>" + escapeHtml(String(displayName || "")) + "</strong>"];
          if (timestamp !== null) {
            lines.push("<strong>Bucket start:</strong> " + formatEpochMillis(timestamp));
            if (bucketMinutes !== null) {
              const bucketEnd = timestamp + Math.max(1, Math.round(bucketMinutes)) * 60 * 1000 - 1;
              lines.push("<strong>Bucket end:</strong> " + formatEpochMillis(bucketEnd));
            }
          }
          lines.push(
            "<strong>Position rows:</strong> " + Number(toNumber(raw.position_rows)).toLocaleString()
          );
          lines.push(
            "<strong>Pro/Con/Other:</strong> " +
              Number(toNumber(raw.n_pro)).toLocaleString() +
              "/" +
              Number(toNumber(raw.n_con)).toLocaleString() +
              "/" +
              Number(toNumber(raw.n_other)).toLocaleString()
          );
          lines.push(
            "<strong>Total repeated rows (bucket):</strong> " +
              Number(toNumber(raw.n_total)).toLocaleString()
          );
          return lines.join("<br/>");
        },
      },
      xAxis: {
        type: "time",
        name: "Time (" + reportTimezoneLabel + ")",
        axisLabel: {
          color: theme.axisText,
          formatter: (value) => formatEpochMillis(value),
        },
        axisLine: { lineStyle: { color: theme.axisLine } },
      },
      yAxis: {
        type: "category",
        data: [String(displayName || "Name")],
        axisLabel: { show: false },
        axisTick: { show: false },
        axisLine: { show: false },
        splitLine: { show: false },
      },
      series: [
        {
          type: "scatter",
          data: points,
          symbolSize: (value, params) => {
            const sizeValue =
              params && params.data && params.data.position_rows !== undefined
                ? params.data.position_rows
                : Array.isArray(value)
                  ? value[2]
                  : null;
            return symbolSizeFor(sizeValue);
          },
          itemStyle: {
            opacity: 0.86,
            color: (params) => {
              const raw = params && params.data && typeof params.data === "object" ? params.data : {};
              return positionColors[String(raw.position || "").trim()] || theme.primaryLine;
            },
          },
          emphasis: {
            itemStyle: {
              borderColor: theme.axisLine,
              borderWidth: 1.0,
              opacity: 0.98,
            },
          },
        },
      ],
    };

    chart.setOption(
      ensureReadableAxes(option, { chartId: "duplicates_exact_inline_timing", host: chartHost }),
      true
    );
    chart.resize();
  }

  function ensureDuplicateNameInlineDetailElement(rowElement) {
    if (!rowElement) {
      return null;
    }
    const tagName = String(rowElement.tagName || "").toUpperCase();
    if (tagName === "TR") {
      let detailRow = rowElement.nextElementSibling;
      if (!detailRow || !detailRow.classList.contains("duplicate-name-inline-detail-row")) {
        detailRow = document.createElement("tr");
        detailRow.className = "duplicate-name-inline-detail-row hidden";
        const detailCell = document.createElement("td");
        detailCell.colSpan = Math.max(1, rowElement.children.length);
        const detailElement = document.createElement("div");
        detailElement.className = "duplicate-name-inline-detail";
        detailCell.appendChild(detailElement);
        detailRow.appendChild(detailCell);
        if (rowElement.parentNode) {
          rowElement.parentNode.insertBefore(detailRow, rowElement.nextSibling);
        }
      }
      const detailElement = detailRow.querySelector(".duplicate-name-inline-detail");
      return {
        rowElement: rowElement,
        detailRow: detailRow,
        detailElement: detailElement,
      };
    }

    rowElement.classList.add("duplicate-name-row-expandable");
    let detailElement = rowElement.querySelector(":scope > .duplicate-name-inline-detail");
    if (!detailElement) {
      detailElement = document.createElement("div");
      detailElement.className = "duplicate-name-inline-detail";
      rowElement.appendChild(detailElement);
    }
    return {
      rowElement: rowElement,
      detailRow: null,
      detailElement: detailElement,
    };
  }

  function collapseDuplicateNameInlineDetail(activeState) {
    if (!activeState || !activeState.current) {
      return;
    }
    const current = activeState.current;
    if (current.rowElement) {
      current.rowElement.classList.remove("is-expanded");
    }
    if (current.detailRow) {
      current.detailRow.classList.add("hidden");
    }
    if (current.detailElement) {
      current.detailElement.classList.remove("is-open");
      const chartHost = current.detailElement.querySelector(".duplicate-name-inline-chart");
      disposeDuplicateInlineTimingChart(chartHost);
    }
    activeState.current = null;
  }

  function renderUnifiedDuplicateNameTable(container, detectorTables, detectorKey) {
    const rows = buildUnifiedDuplicateNameRows(detectorTables);
    if (!rows.length) {
      return false;
    }

    const details = document.createElement("details");
    details.className = "table-group";
    details.open = true;

    const summary = document.createElement("summary");
    summary.textContent = "per_name_duplicates";
    details.appendChild(summary);

    const wrap = document.createElement("div");
    wrap.className = "table-wrap";

    const host = document.createElement("div");
    host.className = "table-host";

    const displayRows = rows.map((row) => ({
      Name: row.Name,
      "# Sign-ins": row["# Sign-ins"],
      "# Pro": row["# Pro"],
      "# Con": row["# Con"],
      "Submission period": row["Submission period"],
    }));
    const tableKey = detectorKey ? detectorKey + ".per_name_duplicates" : "duplicates_exact.per_name_duplicates";
    renderTableHelpCard(wrap, tableKey, displayRows);
    wrap.appendChild(host);
    details.appendChild(wrap);
    container.appendChild(details);

    const timingLookup = buildDuplicateNameTimingLookup(detectorTables);
    const activeInlineDetail = { current: null };
    const onRowClick = (_event, rowComponent) => {
      const rowData =
        rowComponent && typeof rowComponent.getData === "function" ? rowComponent.getData() : null;
      const rowElement =
        rowComponent && typeof rowComponent.getElement === "function"
          ? rowComponent.getElement()
          : null;
      if (!rowData || !rowElement) {
        return;
      }
      if (activeInlineDetail.current && activeInlineDetail.current.rowElement === rowElement) {
        collapseDuplicateNameInlineDetail(activeInlineDetail);
        return;
      }
      collapseDuplicateNameInlineDetail(activeInlineDetail);

      const detailRefs = ensureDuplicateNameInlineDetailElement(rowElement);
      if (!detailRefs || !detailRefs.detailElement) {
        return;
      }
      const scaffold = ensureDuplicateInlineDetailScaffold(detailRefs.detailElement);
      if (!scaffold) {
        return;
      }

      const lookupKey = normalizeDuplicateNameLookupKey(rowData.__lookup_key);
      const displayLookupKey = normalizeDuplicateNameLookupKey(rowData.__display_lookup_key);
      let timelineRows = lookupKey ? timingLookup.rowsByLookupKey.get(lookupKey) || [] : [];
      if (!timelineRows.length && displayLookupKey) {
        timelineRows = timingLookup.rowsByLookupKey.get(displayLookupKey) || [];
      }
      const bucketLabel = bucketLabelFromValue(state.activeBucket);
      const displayName = String(rowData.Name || "").trim();
      const nSignIns = Math.max(0, Math.round(toNumber(rowData["# Sign-ins"])));
      scaffold.summary.textContent =
        displayName +
        " · " +
        nSignIns.toLocaleString() +
        " sign-ins (" +
        Math.max(0, Math.round(toNumber(rowData["# Pro"]))).toLocaleString() +
        " Pro / " +
        Math.max(0, Math.round(toNumber(rowData["# Con"]))).toLocaleString() +
        " Con)";

      scaffold.legend.innerHTML = "";
      [
        ["Pro", currentChartTheme().contextLine],
        ["Con", currentChartTheme().alertLower],
        ["Other", currentChartTheme().referenceLine],
      ].forEach((entry) => {
        const item = document.createElement("span");
        item.className = "duplicate-name-inline-legend-item";
        const swatch = document.createElement("span");
        swatch.className = "duplicate-name-inline-legend-swatch";
        swatch.style.backgroundColor = entry[1];
        const label = document.createElement("span");
        label.textContent = entry[0];
        item.appendChild(swatch);
        item.appendChild(label);
        scaffold.legend.appendChild(item);
      });

      if (!timelineRows.length) {
        scaffold.note.textContent =
          "No repeated-bucket timing points are available for this name at the current bucket view" +
          (bucketLabel ? " (" + bucketLabel + ")" : "") +
          ".";
        scaffold.chartHost.classList.add("hidden");
        disposeDuplicateInlineTimingChart(scaffold.chartHost);
      } else {
        scaffold.note.textContent =
          "Timing row shows bucket-level repeated sign-ins for this name" +
          (bucketLabel ? " at " + bucketLabel : "") +
          " (" +
          reportTimezoneLabel +
          "). Point size scales per-position rows; colors map to Pro/Con/Other." +
          (timingLookup.bucketNote ? " " + timingLookup.bucketNote : "");
        scaffold.chartHost.classList.remove("hidden");
        renderDuplicateInlineTimingChart(scaffold.chartHost, displayName, timelineRows);
      }

      detailRefs.rowElement.classList.add("is-expanded");
      detailRefs.detailElement.classList.add("is-open");
      if (detailRefs.detailRow) {
        detailRefs.detailRow.classList.remove("hidden");
      }
      activeInlineDetail.current = detailRefs;
    };

    mountTable(host, rows, {
      paginationSize: 8,
      maxHeight: "380px",
      tableKey: tableKey,
      layout: "fitData",
      initialSort: [{ column: "# Sign-ins", dir: "desc" }],
      rowClick: onRowClick,
      columns: [
        { title: "Name", field: "Name", headerFilter: "input", minWidth: 230 },
        { title: "# Sign-ins", field: "# Sign-ins", headerFilter: "input", hozAlign: "right" },
        { title: "# Pro", field: "# Pro", headerFilter: "input", hozAlign: "right" },
        { title: "# Con", field: "# Con", headerFilter: "input", hozAlign: "right" },
        {
          title: "Submission period",
          field: "Submission period",
          headerFilter: "input",
          minWidth: 170,
        },
        { title: "__row_id", field: "__row_id", visible: false, headerFilter: false },
        { title: "__lookup_key", field: "__lookup_key", visible: false, headerFilter: false },
        {
          title: "__display_lookup_key",
          field: "__display_lookup_key",
          visible: false,
          headerFilter: false,
        },
        { title: "__scope", field: "__scope", visible: false, headerFilter: false },
      ],
    });

    return true;
  }

  function renderTriageSummary() {
    const view = getRawTriageView();
    const summary = view.triage_summary || {};
    const totalSubmissions = Math.max(0, Math.round(toNumber(summary.total_submissions || 0)));
    const proRateRaw = toFiniteNumberOrNull(summary.overall_pro_rate);
    const conRateRaw = toFiniteNumberOrNull(summary.overall_con_rate);
    const proRate = proRateRaw === null ? 0 : Math.max(0, Math.min(1, proRateRaw));
    const conRate = conRateRaw === null ? 0 : Math.max(0, Math.min(1, conRateRaw));
    let proCount = Math.max(0, Math.round(totalSubmissions * proRate));
    let conCount = Math.max(0, Math.round(totalSubmissions * conRate));
    if (proCount + conCount > totalSubmissions && totalSubmissions > 0) {
      const denom = Math.max(1, proCount + conCount);
      proCount = Math.round((totalSubmissions * proCount) / denom);
      conCount = totalSubmissions - proCount;
    }
    const otherCount = Math.max(0, totalSubmissions - proCount - conCount);

    setTextById("triage-total-submissions", totalSubmissions.toLocaleString());
    const proConMetaParts = [
      "Pro " + proCount.toLocaleString() + " (" + formatPercent(proRate, 1) + ")",
      "Con " + conCount.toLocaleString() + " (" + formatPercent(conRate, 1) + ")",
    ];
    if (otherCount > 0) {
      proConMetaParts.push("Other " + otherCount.toLocaleString());
    }
    setTextById("triage-total-procon-meta", proConMetaParts.join(" · "));
    renderKpiMiniPie("triage-procon-pie", [
      { label: "Pro", value: proCount, color: currentChartTheme().contextLine },
      { label: "Con", value: conCount, color: currentChartTheme().primaryLine },
      { label: "Other", value: otherCount, color: currentChartTheme().referenceLine },
    ]);

    const range = formatDateRangeHumanized(summary.date_range_start, summary.date_range_end);
    setTextById("triage-date-range", range.value);
    setTextById("triage-date-range-meta", range.meta);

    const duplicateRows = tablePreviewRows("duplicates_exact", "per_name_display");
    const topRepeatedNames = Array.isArray(summary.top_repeated_names) ? summary.top_repeated_names : [];
    const duplicateSourceRows = duplicateRows.length ? duplicateRows : topRepeatedNames;
    setTextById("triage-duplicate-names-total", duplicateSourceRows.length.toLocaleString());
    setTextById(
      "triage-duplicate-names-meta",
      duplicateRows.length
        ? "Repeated canonical names in duplicates table."
        : "Fallback from triage summary (top repeated names only)."
    );
    renderKpiMiniBars("triage-duplicate-position-bars", duplicatePositionCounts(duplicateSourceRows), {
      valueFormatter: (value) => Math.round(value).toLocaleString(),
    });

    const voterOverviewRows = tablePreviewRows("voter_registry_match", "linkage_overview");
    const voterOverview = voterOverviewRows.length ? voterOverviewRows[0] || {} : {};
    const matchedRate = toFiniteNumberOrNull(
      voterOverview.matched_rate_rows !== undefined
        ? voterOverview.matched_rate_rows
        : voterOverview.matched_rate_unique
    );
    setTextById(
      "triage-voter-match-rate",
      matchedRate === null ? "-" : formatPercent(matchedRate, 1)
    );
    const matchedRows = Math.max(
      0,
      Math.round(
        toNumber(voterOverview.n_matched_unique_rows) + toNumber(voterOverview.n_matched_ambiguous_rows)
      )
    );
    const totalRows = Math.max(0, Math.round(toNumber(voterOverview.n_rows)));
    setTextById(
      "triage-voter-match-meta",
      totalRows > 0
        ? matchedRows.toLocaleString() + " / " + totalRows.toLocaleString() + " matched rows."
        : "Voter match data unavailable for this run."
    );
    const voterPositionRows = tablePreviewRows("voter_registry_match", "linkage_by_position_rows")
      .filter((row) => String((row || {}).unit || "rows").toLowerCase() === "rows")
      .map((row) => {
        const label = String((row || {}).position_normalized || "Unknown");
        return {
          label: label,
          value: Math.max(0, Math.min(1, toNumber((row || {}).matched_rate))),
          color: kpiPositionColor(label),
        };
      });
    const positionOrder = {
      Pro: 0,
      Con: 1,
      Other: 2,
      Unknown: 3,
    };
    voterPositionRows.sort(
      (left, right) =>
        toNumber(positionOrder[left.label] !== undefined ? positionOrder[left.label] : 99) -
        toNumber(positionOrder[right.label] !== undefined ? positionOrder[right.label] : 99)
    );
    renderKpiMiniBars("triage-voter-match-position-bars", voterPositionRows, {
      maxValue: 1,
      valueFormatter: (value) => formatPercent(value, 0),
    });
  }

  function renderDataQualityPanel() {
    const summaryHost = document.getElementById("data-quality-summary");
    const warningHost = document.getElementById("data-quality-warning-host");
    const metricsHost = document.getElementById("data-quality-dedup-metrics-host");

    const panelStatus = String(dataQualityPanel.status || "ok");
    const warnings = Array.isArray(dataQualityPanel.warnings) ? dataQualityPanel.warnings : [];
    const metrics = Array.isArray(dataQualityPanel.triage_raw_vs_dedup_metrics)
      ? dataQualityPanel.triage_raw_vs_dedup_metrics
      : [];

    if (summaryHost) {
      summaryHost.classList.toggle("ok", panelStatus === "ok");
      summaryHost.textContent =
        typeof dataQualityPanel.summary === "string" && dataQualityPanel.summary.trim()
          ? dataQualityPanel.summary
          : "Data-quality checks unavailable for this run.";
    }
    if (warningHost) {
      mountTable(warningHost, warnings, {
        pagination: false,
        maxHeight: "260px",
        tableKey: "data_quality.warnings",
      });
    }
    if (metricsHost) {
      mountTable(metricsHost, metrics, {
        pagination: false,
        maxHeight: "260px",
        tableKey: "data_quality.raw_vs_dedup_metrics",
      });
    }
  }

  function renderHearingContextPanel() {
    const summaryHost = document.getElementById("hearing-context-summary");
    const metadataHost = document.getElementById("hearing-context-metadata-host");
    const rampHost = document.getElementById("hearing-deadline-ramp-host");

    const isAvailable = !!(hearingContextPanel && hearingContextPanel.available);
    const metadataRows = Array.isArray(hearingContextPanel.metadata_rows)
      ? hearingContextPanel.metadata_rows
      : [];
    const ramp = hearingContextPanel.deadline_ramp_metrics || {};

    if (summaryHost) {
      if (!isAvailable) {
        const reason =
          typeof hearingContextPanel.reason === "string" && hearingContextPanel.reason.trim()
            ? hearingContextPanel.reason
            : "No hearing metadata sidecar was provided for this run.";
        summaryHost.textContent = reason;
      } else {
        const hearingId =
          typeof hearingContextPanel.hearing_id === "string" &&
          hearingContextPanel.hearing_id.trim()
            ? hearingContextPanel.hearing_id.trim()
            : "unknown";
        summaryHost.textContent =
          "Hearing " +
          hearingId +
          " (" +
          reportTimezoneLabel +
          "). Process markers are overlaid on linked absolute-time charts.";
      }
    }

    if (metadataHost) {
      mountKeyValueList(metadataHost, metadataRows, {
        keyField: "field",
        valueField: "value",
        humanizeKeys: true,
      });
    }

    const rampRows = [];
    if (ramp && typeof ramp === "object") {
      Object.keys(ramp)
        .forEach((key) => {
          rampRows.push({
            metric: key,
            value: ramp[key],
          });
        });
    }
    if (rampHost) {
      mountKeyValueList(rampHost, rampRows, {
        keyField: "metric",
        valueField: "value",
        humanizeKeys: true,
      });
    }
  }

  function renderInvestigationTables() {
    if (isOffHoursFocusOnly) {
      return;
    }
    const view = getRawTriageView();
    const summary = view.triage_summary || {};
    const forensicsNamesHost = document.getElementById("forensics-top-names-host");
    const taxonomyHost = document.getElementById("methodology-evidence-taxonomy-host");
    const artifactRowsHost = document.getElementById("methodology-artifact-rows-host");

    const topNames = Array.isArray(summary.top_repeated_names)
      ? summary.top_repeated_names
      : [];
    const topNamesRows = topNames.map((row) => ({
      Name:
        typeof (row || {}).display_name === "string" && row.display_name.trim()
          ? row.display_name.trim()
          : typeof (row || {}).canonical_name === "string" && row.canonical_name.trim()
            ? row.canonical_name.trim()
            : "",
      "Total Sign-ins": Number(toNumber((row || {}).n_records ?? (row || {}).n)),
      "# Pro": Number(toNumber((row || {}).n_pro)),
      "# Con": Number(toNumber((row || {}).n_con)),
    }));
    mountTable(forensicsNamesHost, topNamesRows, {
      paginationSize: 8,
      maxHeight: "340px",
      layout: "fitData",
      tableKey: "triage.top_repeated_names",
    });

    const taxonomyRows = Array.isArray(methodology.evidence_taxonomy)
      ? methodology.evidence_taxonomy
      : Array.isArray(controls.evidence_taxonomy)
        ? controls.evidence_taxonomy
        : [];
    mountTable(taxonomyHost, taxonomyRows, {
      pagination: false,
      maxHeight: "280px",
      tableKey: "methodology.evidence_taxonomy",
    });

    const artifactRows = Object.entries(reportData.artifact_rows || {})
      .map((entry) => ({
        artifact: entry[0],
        rows: toNumber(entry[1]),
      }))
      .sort((left, right) => {
        const rowDelta = Number(toNumber(right.rows)) - Number(toNumber(left.rows));
        if (rowDelta !== 0) {
          return rowDelta;
        }
        return String(left.artifact).localeCompare(String(right.artifact));
      });
    mountTable(artifactRowsHost, artifactRows, {
      paginationSize: 10,
      maxHeight: "300px",
      tableKey: "report.artifact_rows",
    });
    renderMethodologyPanel();
  }

  function renderTablesForAnalysis(section, analysis) {
    const container = section.querySelector('[data-analysis-tables-for="' + analysis.id + '"]');
    if (!container) {
      return;
    }
    Array.from(container.querySelectorAll(".duplicate-name-inline-chart")).forEach((host) => {
      disposeDuplicateInlineTimingChart(host);
    });
    container.innerHTML = "";

    const detectorKey = analysis.detector;
    const detectorTables = detectorKey ? (reportData.table_previews || {})[detectorKey] || {} : {};
    if (analysis.id === "duplicates_exact") {
      const methodRows = Array.isArray(detectorTables.collision_methods)
        ? detectorTables.collision_methods
        : [];
      const scopedRows = methodRows.filter(
        (row) => String((row || {}).scope || "") === String(state.activeDuplicateScope || "")
      );
      const activeRows = scopedRows.length ? scopedRows : methodRows;
      const degraded = activeRows.some((row) => toBool((row || {}).baseline_degraded));
      if (degraded) {
        const methodRow = activeRows.length ? activeRows[0] || {} : {};
        const warning = document.createElement("div");
        warning.className = "warning-banner";
        const source = String(methodRow.baseline_source || "unknown");
        const model = String(methodRow.baseline_model || "unknown");
        const scopeLabel = String(state.activeDuplicateScope || "unknown").replace(/_/g, " ");
        warning.textContent =
          "Duplicate baseline degraded for " +
          scopeLabel +
          " scope. Source=" +
          source +
          ", model=" +
          model +
          ". Interpret duplicate expectations descriptively.";
        container.appendChild(warning);
      }
      renderUnifiedDuplicateNameTable(container, detectorTables, detectorKey);
    }
    let tableNames = Object.keys(detectorTables).sort();
    if (analysis.id === "duplicates_exact") {
      const mergedDuplicateNameSourceTables = new Set([
        "per_name_anomalies",
        "per_name_display",
        "per_name_tests",
      ]);
      tableNames = tableNames.filter((name) => !mergedDuplicateNameSourceTables.has(name));
    }
    if (isOffHoursFocusOnly && analysis.id === "off_hours") {
      const preferred = [
        "off_hours_summary",
        "model_fit_diagnostics",
        "flag_channel_summary",
        "flagged_window_diagnostics",
        "window_control_profile",
        "date_hour_primary_residual_distribution",
        "date_hour_distribution",
      ];
      tableNames = preferred.filter((name) =>
        Object.prototype.hasOwnProperty.call(detectorTables, name)
      );
    }

    if (analysis.id === "composite_score" && Array.isArray(reportData.evidence_bundle_preview || [])) {
      const evidenceDetails = document.createElement("details");
      evidenceDetails.className = "table-group";
      evidenceDetails.open = true;
      const evidenceSummary = document.createElement("summary");
      evidenceSummary.textContent = "evidence_bundle_preview";
      evidenceDetails.appendChild(evidenceSummary);
      const evidenceWrap = document.createElement("div");
      evidenceWrap.className = "table-wrap";
      const evidenceHost = document.createElement("div");
      evidenceHost.className = "table-host";
      const evidenceRows = reportData.evidence_bundle_preview || [];
      renderTableHelpCard(
        evidenceWrap,
        "composite_score.evidence_bundle_preview",
        evidenceRows
      );
      evidenceWrap.appendChild(evidenceHost);
      evidenceDetails.appendChild(evidenceWrap);
      mountTable(evidenceHost, evidenceRows, {
        paginationSize: 10,
        maxHeight: "380px",
        tableKey: "composite_score.evidence_bundle_preview",
      });
      container.appendChild(evidenceDetails);
    }

    if (analysis.id === "rare_names") {
      const rarityTables = [
        ["rarity_coverage_preview", reportData.rarity_coverage_preview || []],
        ["rarity_unmatched_first_preview", reportData.rarity_unmatched_first_preview || []],
        ["rarity_unmatched_last_preview", reportData.rarity_unmatched_last_preview || []],
      ];
      const rarityTableKeys = {
        rarity_coverage_preview: "rare_names.rarity_coverage_preview",
        rarity_unmatched_first_preview: "rare_names.rarity_unmatched_first_preview",
        rarity_unmatched_last_preview: "rare_names.rarity_unmatched_last_preview",
      };
      rarityTables.forEach((entry, index) => {
        const rows = Array.isArray(entry[1]) ? entry[1] : [];
        if (!rows.length) {
          return;
        }
        const details = document.createElement("details");
        details.className = "table-group";
        details.open = index === 0;
        const summary = document.createElement("summary");
        summary.textContent = entry[0];
        details.appendChild(summary);
        const wrap = document.createElement("div");
        wrap.className = "table-wrap";
        const host = document.createElement("div");
        host.className = "table-host";
        renderTableHelpCard(wrap, rarityTableKeys[entry[0]] || "", rows);
        wrap.appendChild(host);
        details.appendChild(wrap);
        mountTable(host, rows, {
          paginationSize: 8,
          maxHeight: "320px",
          tableKey: rarityTableKeys[entry[0]] || "",
        });
        container.appendChild(details);
      });
    }

    if (analysis.id === "periodicity" && Array.isArray(reportData.clockface_top_preview || [])) {
      const rows = reportData.clockface_top_preview || [];
      if (rows.length) {
        const details = document.createElement("details");
        details.className = "table-group";
        details.open = true;
        const summary = document.createElement("summary");
        summary.textContent = "clockface_top_preview";
        details.appendChild(summary);
        const wrap = document.createElement("div");
        wrap.className = "table-wrap";
        const host = document.createElement("div");
        host.className = "table-host";
        renderTableHelpCard(wrap, "periodicity.clockface_top_preview", rows);
        wrap.appendChild(host);
        details.appendChild(wrap);
        mountTable(host, rows, {
          paginationSize: 8,
          maxHeight: "300px",
          tableKey: "periodicity.clockface_top_preview",
        });
        container.appendChild(details);
      }
    }

    tableNames.forEach((tableName, index) => {
      const sourceRows = detectorTables[tableName] || [];
      let rows = sourceRows;
      let tableBucketNote = "";
      let tableTitle = tableName;
      if (analysis.id === "duplicates_exact") {
        const bucketFiltered = filterRowsByDuplicateTableBucket(tableName, sourceRows);
        rows = bucketFiltered.rows;
        tableBucketNote = bucketFiltered.note;
        if (bucketFiltered.applied && Number.isFinite(state.activeBucket)) {
          tableTitle = tableName + " (" + Math.round(state.activeBucket) + "m)";
        }
      }

      const details = document.createElement("details");
      details.className = "table-group";
      details.open = index === 0 && container.childElementCount === 0;

      const summary = document.createElement("summary");
      summary.textContent = tableTitle;
      details.appendChild(summary);

      const wrap = document.createElement("div");
      wrap.className = "table-wrap";
      details.appendChild(wrap);

      if (tableBucketNote) {
        const note = document.createElement("p");
        note.className = "tiny-note";
        note.textContent = tableBucketNote;
        wrap.appendChild(note);
      }

      const host = document.createElement("div");
      host.className = "table-host";
      wrap.appendChild(host);

      const tableKey = detectorKey ? detectorKey + "." + tableName : tableName;
      renderTableHelpCard(wrap, tableKey, rows);
      mountTable(host, rows, {
        paginationSize: 8,
        maxHeight: "340px",
        tableKey: tableKey,
      });

      container.appendChild(details);
    });

    if (!container.childElementCount) {
      const empty = document.createElement("p");
      empty.className = "empty-message";
      empty.textContent = "No preview tables available for this analysis.";
      container.appendChild(empty);
    }
  }

  async function renderChartMount(mount) {
    if (!mount || !mount.chartId) {
      return;
    }
    const analysisId = String(chartToAnalysis.get(mount.chartId) || "").trim();
    if (analysisId) {
      const needsLoad = analysisNeedsShardLoad(analysisId);
      if (needsLoad) {
        setChartLoading(mount.chartId, true, "Loading chart data...");
      }
      try {
        await ensureAnalysisDataLoaded(analysisId);
      } catch (error) {
        setChartLoading(mount.chartId, false);
        setEmptyForChart(mount.chartId, true);
        setChartNote(mount.chartId, "Unable to load chart data for this section.");
        setChartControls(mount.chartId, null);
        showDataLoadError(
          "Unable to load report data files. Serve this report directory over HTTP and refresh.",
          error
        );
        return;
      }
      if (needsLoad) {
        setChartLoading(mount.chartId, false);
      }
    }

    setChartControls(mount.chartId, null);
    const rawRows = getChartRows(mount.chartId);
    const duplicateScopedRows = filterRowsByDuplicateCollisionControls(mount.chartId, rawRows);
    if (!duplicateScopedRows.length) {
      setEmptyForChart(mount.chartId, true);
      setChartNote(mount.chartId, "");
      return;
    }

    const bucketSelection = filterRowsByBucket(duplicateScopedRows, mount.chartId);
    const zoomSelection = filterRowsByLinkedZoom(mount, bucketSelection.rows);
    const rows = zoomSelection.rows;
    mount.activeBucket = bucketSelection.bucket;
    const preRenderNote = [bucketSelection.note, zoomSelection.note]
      .map((value) => String(value || "").trim())
      .filter((value) => !!value)
      .join(" ");
    setChartNote(mount.chartId, preRenderNote);

    if (!rows.length) {
      setEmptyForChart(mount.chartId, true);
      return;
    }
    setEmptyForChart(mount.chartId, false);

    if (!hasEcharts) {
      return;
    }

    clearChartInteractionState(mount);
    const didRender = renderAutoChart(mount, rows);
    setEmptyForChart(mount.chartId, !didRender);
    if (!didRender) {
      return;
    }
    setChartNote(mount.chartId, composeChartNote(mount, preRenderNote));
    if (mount.isAbsoluteTime) {
      applyZoomToChart(mount);
    }
  }

  async function mountChartHost(host) {
    const chartId = String(host.getAttribute("data-chart-id") || "").trim();
    if (!chartId || chartMounts.has(chartId)) {
      return;
    }

    const mount = {
      chartId: chartId,
      host: host,
      chart: createChartInstance(host),
      isTimeSeries: false,
      isAbsoluteTime: false,
      legendDockMode: "",
      hasFunnelClickHandler: false,
      seriesId: null,
      baseMarkLines: state.absoluteTimeSet.has(chartId) ? buildProcessMarkerLines() : [],
      customChartNote: null,
      timeExtent: null,
      activeBucket: null,
      topNameTimingPage: 0,
    };
    chartMounts.set(chartId, mount);
    if (mount.chart) {
      chartInstances.push(mount.chart);
    }

    await renderChartMount(mount);

    if (mount.chart && mount.isTimeSeries) {
      attachCursorHandlers(mount);
      attachZoomHandlers(mount);
      state.timeCharts.add(chartId);
    } else {
      attachFunnelCursorHandler(mount);
    }
  }

  async function mountSection(section, analysis) {
    if (!section || !analysis) {
      return;
    }
    const sectionId = String(analysis.id || "");
    if (!sectionId || mountedSections.has(sectionId)) {
      return;
    }

    const needsLoad = analysisNeedsShardLoad(sectionId);
    if (needsLoad) {
      setSectionLoading(section, true, "Loading analysis data...");
    }
    try {
      await ensureAnalysisDataLoaded(sectionId);
    } catch (error) {
      setSectionLoading(section, false);
      const hosts = Array.from(section.querySelectorAll("[data-chart-id]"));
      hosts.forEach((host) => {
        const chartId = String(host.getAttribute("data-chart-id") || "").trim();
        if (!chartId) {
          return;
        }
        setEmptyForChart(chartId, true);
        setChartNote(chartId, "Unable to load chart data for this section.");
      });
      showDataLoadError(
        "Unable to load report data files. Serve this report directory over HTTP and refresh.",
        error
      );
      return;
    }

    const hosts = Array.from(section.querySelectorAll("[data-chart-id]"));
    await Promise.all(hosts.map((host) => mountChartHost(host)));
    if (needsLoad) {
      setSectionLoading(section, false);
    }
    renderTablesForAnalysis(section, analysis);
    mountedSections.add(sectionId);
  }

  async function rerenderBucketAwareCharts() {
    const analysisIds = new Set();
    chartMounts.forEach((mount) => {
      const chartRows = getChartRows(mount.chartId);
      const options = getChartBucketOptions(mount.chartId, chartRows);
      if (!options.length) {
        return;
      }
      const analysisId = String(chartToAnalysis.get(mount.chartId) || "").trim();
      if (analysisId) {
        analysisIds.add(analysisId);
      }
    });
    await Promise.all(
      Array.from(analysisIds).map((analysisId) => ensureAnalysisDataLoaded(analysisId))
    );
    const renders = [];
    chartMounts.forEach((mount) => {
      const rows = getChartRows(mount.chartId);
      if (!rows.length) {
        return;
      }
      if (!getChartBucketOptions(mount.chartId, rows).length) {
        return;
      }
      renders.push(renderChartMount(mount));
    });
    await Promise.all(renders);
  }

  function rerenderBucketAwareTables() {
    if (!mountedSections.has("duplicates_exact")) {
      return;
    }
    const duplicateSection = document.querySelector('[data-analysis-id="duplicates_exact"]');
    const duplicateAnalysis = analysisById.get("duplicates_exact");
    if (!duplicateSection || !duplicateAnalysis) {
      return;
    }
    renderTablesForAnalysis(duplicateSection, duplicateAnalysis);
  }

  function rerenderLinkedZoomAwareCharts(sourceChartId) {
    chartMounts.forEach((mount, chartId) => {
      if (!mount || chartId === sourceChartId || state.absoluteTimeSet.has(chartId)) {
        return;
      }
      const rows = getChartRows(chartId);
      if (!rows.length) {
        return;
      }
      if (!chartUsesLinkedZoomRowFilter(mount, rows)) {
        return;
      }
      renderChartMount(mount);
    });
  }

  function filterRowsByDuplicateCollisionControls(chartId, rows) {
    const subset = Array.isArray(rows) ? rows : [];
    if (!subset.length) {
      return subset;
    }
    if (chartId === "duplicates_exact_bucket_concentration") {
      const scoped = subset.filter(
        (row) => String(row.scope || "") === String(state.activeDuplicateScope || "")
      );
      const scopedFallback = scoped.length ? scoped : subset;
      const metered = scopedFallback.filter(
        (row) => String(row.metric || "") === String(state.activeDuplicateMetric || "")
      );
      return metered.length ? metered : scopedFallback;
    }
    if (chartId === "duplicates_exact_metric_diagnostics") {
      const scoped = subset.filter(
        (row) => String(row.scope || "") === String(state.activeDuplicateScope || "")
      );
      return scoped.length ? scoped : subset;
    }
    if (chartId === "duplicates_exact_top_name_timing_exact") {
      const scoped = subset.filter(
        (row) => String(row.scope || "") === String(state.activeDuplicateScope || "")
      );
      return scoped.length ? scoped : subset;
    }
    return subset;
  }

  async function rerenderDuplicateCollisionCharts() {
    const targets = new Set([
      "duplicates_exact_bucket_concentration",
      "duplicates_exact_metric_diagnostics",
      "duplicates_exact_top_name_timing_exact",
    ]);
    const renders = [];
    chartMounts.forEach((mount, chartId) => {
      if (!mount || !targets.has(chartId)) {
        return;
      }
      renders.push(renderChartMount(mount));
    });
    await Promise.all(renders);
  }

  function initDuplicateCollisionControls() {
    const panel = document.getElementById("duplicate-collision-panel");
    const scopeSelect = document.getElementById("duplicate-scope-select");
    const metricSelect = document.getElementById("duplicate-metric-select");
    const scopeLabel = panel ? panel.querySelector('label[for="duplicate-scope-select"]') : null;
    if (!panel || !scopeSelect || !metricSelect) {
      return;
    }

    const scopeOptions = duplicateScopeOptions.length
      ? duplicateScopeOptions
      : [state.defaultDuplicateScope];
    const metricOptions = duplicateMetricOptions.length
      ? duplicateMetricOptions
      : [state.defaultDuplicateMetric];
    if (!scopeOptions.length || !metricOptions.length) {
      panel.setAttribute("data-section-control-enabled", "false");
      panel.classList.add("hidden");
      updateSectionViewControlsForHeading(state.activeTocHeading || window.location.hash);
      return;
    }
    panel.setAttribute("data-section-control-enabled", "true");
    panel.classList.add("hidden");

    let savedScope = "";
    let savedMetric = "";
    try {
      savedScope = String(window.localStorage.getItem("testifier_audit_dup_scope") || "").trim();
      savedMetric = String(window.localStorage.getItem("testifier_audit_dup_metric") || "").trim();
    } catch (_error) {}
    const queryScope = parseDuplicateOptionFromQueryParams(
      ["dup_scope", "duplicate_scope"],
      scopeOptions
    );
    const queryMetric = parseDuplicateOptionFromQueryParams(
      ["dup_metric", "duplicate_metric"],
      metricOptions
    );
    state.activeDuplicateScope = queryScope
      ? queryScope
      : scopeOptions.includes(savedScope)
        ? savedScope
        : state.defaultDuplicateScope;
    state.activeDuplicateMetric = queryMetric
      ? queryMetric
      : metricOptions.includes(savedMetric)
        ? savedMetric
        : state.defaultDuplicateMetric;
    if (!scopeOptions.includes(state.activeDuplicateScope)) {
      state.activeDuplicateScope = scopeOptions[0];
    }
    if (!metricOptions.includes(state.activeDuplicateMetric)) {
      state.activeDuplicateMetric = metricOptions[0];
    }

    scopeSelect.innerHTML = "";
    scopeOptions.forEach((value) => {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = value.replace(/_/g, " ");
      scopeSelect.appendChild(option);
    });
    scopeSelect.value = state.activeDuplicateScope;
    const hideScopeControl = scopeOptions.length === 1 && scopeOptions[0] === "full_hearing";
    scopeSelect.classList.toggle("hidden", hideScopeControl);
    scopeSelect.disabled = hideScopeControl;
    if (scopeLabel) {
      scopeLabel.classList.toggle("hidden", hideScopeControl);
    }

    metricSelect.innerHTML = "";
    metricOptions.forEach((value) => {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = value.replace(/_/g, " ");
      metricSelect.appendChild(option);
    });
    metricSelect.value = state.activeDuplicateMetric;

    const onChange = () => {
      state.activeDuplicateScope = scopeSelect.value;
      state.activeDuplicateMetric = metricSelect.value;
      try {
        window.localStorage.setItem("testifier_audit_dup_scope", state.activeDuplicateScope);
        window.localStorage.setItem("testifier_audit_dup_metric", state.activeDuplicateMetric);
      } catch (_error) {}
      syncControlOverridesToUrl();
      runWithBusyIndicator("Applying duplicate collision view...", async () => {
        await rerenderDuplicateCollisionCharts();
        const duplicateSection = document.querySelector('[data-analysis-id="duplicates_exact"]');
        const duplicateAnalysis = analysisById.get("duplicates_exact");
        if (duplicateSection && duplicateAnalysis) {
          renderTablesForAnalysis(duplicateSection, duplicateAnalysis);
        }
      });
    };
    scopeSelect.addEventListener("change", onChange);
    metricSelect.addEventListener("change", onChange);
    updateSectionViewControlsForHeading(state.activeTocHeading || window.location.hash);
  }

  function initBucketTabs() {
    const root = document.getElementById("bucket-sync-tabs");
    const panel = document.getElementById("bucket-sync-panel");
    const optionsRaw = Array.isArray(controls.global_bucket_options) ? controls.global_bucket_options : [];
    const options = Array.from(
      new Set(optionsRaw.map((value) => toFiniteNumberOrNull(value)).filter((value) => value !== null))
    ).sort((left, right) => left - right);

    if (!root || !panel || !options.length) {
      if (panel) {
        panel.classList.add("hidden");
      }
      updateSidebarFloatingOffsets();
      return;
    }

    panel.classList.remove("hidden");
    const preferredDefault = toFiniteNumberOrNull(controls.default_bucket_minutes);
    state.defaultBucket =
      preferredDefault !== null && options.includes(preferredDefault)
        ? preferredDefault
        : options.includes(30)
          ? 30
          : options[0];
    const queryBucket = parseBucketFromQueryParams(options);
    state.activeBucket =
      queryBucket !== null && options.includes(queryBucket)
        ? queryBucket
        : state.defaultBucket;

    root.innerHTML = "";
    options.forEach((bucket) => {
      const tab = document.createElement("button");
      tab.type = "button";
      tab.className = "bucket-tab";
      tab.setAttribute("role", "tab");
      tab.setAttribute("data-bucket-minutes", String(Math.round(bucket)));
      tab.setAttribute("aria-selected", state.activeBucket === bucket ? "true" : "false");
      tab.textContent = bucketSelectorLabel(bucket);
      tab.title = String(Math.round(bucket)) + "m";
      tab.addEventListener("click", () => {
        state.activeBucket = bucket;
        Array.from(root.querySelectorAll(".bucket-tab")).forEach((node) => {
          const value = toFiniteNumberOrNull(node.getAttribute("data-bucket-minutes"));
          node.setAttribute("aria-selected", value === bucket ? "true" : "false");
        });
        syncControlOverridesToUrl();
        runWithBusyIndicator("Applying " + bucket + "m bucket...", async () => {
          await rerenderBucketAwareCharts();
          rerenderBucketAwareTables();
        });
      });
      root.appendChild(tab);
    });
    updateSidebarFloatingOffsets();
  }

  function initZoomControls() {
    const panel = document.getElementById("zoom-sync-panel");
    const resetButton = document.getElementById("zoom-reset-button");
    if (!panel || !resetButton) {
      return;
    }
    if (!state.absoluteTimeSet.size) {
      panel.classList.add("hidden");
      updateSidebarFloatingOffsets();
      return;
    }
    panel.classList.remove("hidden");
    const resetZoom = () => {
      runWithBusyIndicator("Resetting linked zoom...", () => {
        propagateZoom(null, null, null, true);
      });
    };
    resetButton.addEventListener("click", resetZoom);
    updateZoomRangeLabel();
    updateSidebarFloatingOffsets();
  }

  function initGlobalControlsCollapse() {
    const panel = document.getElementById("sidebar-global-controls");
    const toggleButton = document.getElementById("global-controls-toggle");
    if (!panel || !toggleButton) {
      return;
    }

    const isMobile = () => window.matchMedia("(max-width: 820px)").matches;
    let previousIsMobile = isMobile();
    state.globalControlsExpandedMobile = !previousIsMobile;

    const applyState = () => {
      const mobile = isMobile();
      const expanded = mobile ? state.globalControlsExpandedMobile : true;
      panel.classList.toggle("is-collapsed", mobile && !expanded);
      toggleButton.setAttribute("aria-expanded", expanded ? "true" : "false");
      toggleButton.setAttribute(
        "aria-label",
        expanded ? "Hide global controls" : "Show global controls"
      );
      updateSidebarFloatingOffsets();
    };

    applyState();

    toggleButton.addEventListener("click", () => {
      if (!isMobile()) {
        return;
      }
      state.globalControlsExpandedMobile = !state.globalControlsExpandedMobile;
      applyState();
      scheduleChartResizeSequence();
    });

    window.addEventListener("resize", () => {
      const mobile = isMobile();
      if (mobile !== previousIsMobile) {
        previousIsMobile = mobile;
        state.globalControlsExpandedMobile = !mobile;
      }
      applyState();
    });
  }

  function initSidebarToggle() {
    const shell = document.getElementById("page-shell");
    const sidebar = document.getElementById("toc-sidebar");
    const toggle = document.getElementById("sidebar-toggle");
    const launcher = document.getElementById("sidebar-launcher");
    const backdrop = document.getElementById("sidebar-backdrop");
    if (!shell || !sidebar || !toggle || !launcher || !backdrop) {
      return;
    }

    const isMobile = () => window.matchMedia("(max-width: 1220px)").matches;
    let previousIsMobile = isMobile();
    const applyState = (open) => {
      shell.classList.toggle("sidebar-open", open);
      toggle.setAttribute("aria-expanded", open ? "true" : "false");
      launcher.setAttribute("aria-expanded", open ? "true" : "false");
      toggle.setAttribute("aria-label", open ? "Hide menu" : "Show menu");
      toggle.textContent = open ? "←" : "→";
      updateSidebarFloatingOffsets();
      scheduleChartResizeSequence();
    };

    applyState(!previousIsMobile);

    toggle.addEventListener("click", () => {
      applyState(!shell.classList.contains("sidebar-open"));
    });
    launcher.addEventListener("click", () => applyState(true));
    backdrop.addEventListener("click", () => applyState(false));

    window.addEventListener("resize", () => {
      const nowMobile = isMobile();
      if (nowMobile !== previousIsMobile) {
        previousIsMobile = nowMobile;
        applyState(!nowMobile);
      } else {
        updateSidebarFloatingOffsets();
        scheduleChartResizeSequence();
      }
    });
  }

  function initSidebarToc() {
    const sidebar = document.getElementById("toc-sidebar");
    const tocRoot = document.getElementById("report-toc");
    const contentRoot = document.getElementById("toc-content");
    if (!sidebar || !tocRoot || !contentRoot) {
      return;
    }

    const topHeadings = Array.from(contentRoot.querySelectorAll("h2.toc-heading"));
    if (!topHeadings.length) {
      sidebar.classList.add("hidden");
      return;
    }

    const slugify = (value) =>
      String(value || "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-+|-+$/g, "");

    const usedIds = new Set(
      Array.from(document.querySelectorAll("[id]"))
        .map((node) => String(node.id || "").trim())
        .filter((id) => !!id)
    );

    const ensureHeadingId = (heading, fallbackPrefix) => {
      if (!heading) {
        return "";
      }
      const existing = String(heading.id || "").trim();
      if (existing) {
        usedIds.add(existing);
        return existing;
      }
      const slug = slugify(heading.textContent || "");
      const base = slug ? fallbackPrefix + "-" + slug : fallbackPrefix + "-item";
      let candidate = base;
      let counter = 2;
      while (usedIds.has(candidate)) {
        candidate = base + "-" + counter;
        counter += 1;
      }
      heading.id = candidate;
      usedIds.add(candidate);
      return candidate;
    };

    const topEntries = topHeadings
      .map((heading, index) => {
        const topId = ensureHeadingId(heading, "section-" + (index + 1));
        const sectionRoot = heading.closest("section");
        const children = sectionRoot
          ? Array.from(sectionRoot.querySelectorAll("h3, h4"))
              .filter((child) => {
                const childText = String(child.textContent || "").trim();
                return !!childText;
              })
              .map((child, childIndex) => {
                ensureHeadingId(child, topId + "-sub-" + (childIndex + 1));
                return child;
              })
          : [];
        return {
          heading: heading,
          topId: topId,
          sectionRoot: sectionRoot,
          children: children,
        };
      })
      .filter((entry) => !!entry.topId);

    if (!topEntries.length) {
      sidebar.classList.add("hidden");
      return;
    }

    const topById = new Map(topEntries.map((entry) => [entry.topId, entry]));
    const childToTop = new Map();
    topEntries.forEach((entry) => {
      entry.children.forEach((child) => {
        childToTop.set(child.id, entry.topId);
      });
    });

    const trackedHeadings = topEntries.flatMap((entry) => [entry.heading].concat(entry.children));
    if (!trackedHeadings.length) {
      sidebar.classList.add("hidden");
      return;
    }

    const topForHeading = (headingId) => {
      const normalized = normalizeHashId(headingId);
      if (!normalized) {
        return topEntries[0].topId;
      }
      if (topById.has(normalized)) {
        return normalized;
      }
      const parentTop = childToTop.get(normalized);
      if (parentTop && topById.has(parentTop)) {
        return parentTop;
      }
      return topEntries[0].topId;
    };

    const renderToc = (activeHeadingId) => {
      const activeHeading = normalizeHashId(activeHeadingId) || topEntries[0].topId;
      const activeTop = topForHeading(activeHeading);
      const list = document.createElement("ul");
      list.className = "toc-list";

      topEntries.forEach((entry) => {
        const topItem = document.createElement("li");
        topItem.className = "toc-list-item";
        const topLink = document.createElement("a");
        topLink.className = "toc-link";
        topLink.href = "#" + entry.topId;
        topLink.textContent = String(entry.heading.textContent || "").trim() || entry.topId;

        const topIsActive = entry.topId === activeHeading;
        topLink.classList.toggle("is-active-link", topIsActive);
        if (topIsActive) {
          topLink.setAttribute("aria-current", "true");
        }
        topItem.appendChild(topLink);

        if (entry.topId === activeTop && entry.children.length) {
          const childList = document.createElement("ul");
          childList.className = "toc-list-children";
          entry.children.forEach((child) => {
            const childItem = document.createElement("li");
            childItem.className = "toc-list-item";
            const childLink = document.createElement("a");
            childLink.className = "toc-link toc-link-child";
            childLink.href = "#" + child.id;
            childLink.textContent = String(child.textContent || "").trim() || child.id;
            const childIsActive = child.id === activeHeading;
            childLink.classList.toggle("is-active-link", childIsActive);
            if (childIsActive) {
              childLink.setAttribute("aria-current", "true");
            }
            childItem.appendChild(childLink);
            childList.appendChild(childItem);
          });
          topItem.appendChild(childList);
        }

        list.appendChild(topItem);
      });

      tocRoot.innerHTML = "";
      tocRoot.appendChild(list);
    };

    state.renderToc = renderToc;

    const pickActiveHeading = () => {
      const topOffset = headerStackOffsetPx(16);
      const lastHeading = trackedHeadings[trackedHeadings.length - 1];
      const doc = document.documentElement;
      const hashHeadingId = normalizeHashId(window.location.hash);
      const hashHeading = hashHeadingId ? document.getElementById(hashHeadingId) : null;
      if (hashHeading && trackedHeadings.includes(hashHeading)) {
        const rect = hashHeading.getBoundingClientRect();
        if (rect.top >= -24 && rect.top <= window.innerHeight * 1.5) {
          return hashHeading.id;
        }
      }
      if (window.scrollY + window.innerHeight >= doc.scrollHeight - 4) {
        return lastHeading ? lastHeading.id : "";
      }

      let active = trackedHeadings[0];
      let foundAboveOffset = false;
      trackedHeadings.forEach((heading) => {
        if (heading.getBoundingClientRect().top <= topOffset) {
          active = heading;
          foundAboveOffset = true;
        }
      });
      if (!foundAboveOffset) {
        const nearest = trackedHeadings
          .slice()
          .sort(
            (left, right) =>
              Math.abs(left.getBoundingClientRect().top - topOffset) -
              Math.abs(right.getBoundingClientRect().top - topOffset)
          )[0];
        active = nearest || active;
      }
      return active ? active.id : "";
    };

    tocRoot.addEventListener("click", async (event) => {
      const link = event.target.closest('a[href^="#"]');
      if (!link) {
        return;
      }
      if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
        return;
      }

      const headingId = normalizeHashId(link.getAttribute("href"));
      const target = headingId ? document.getElementById(headingId) : null;
      if (!target) {
        return;
      }
      event.preventDefault();

      const targetSection = target.closest("[data-analysis-id]");
      if (targetSection) {
        const allSections = Array.from(document.querySelectorAll("[data-analysis-id]"));
        for (const section of allSections) {
          const analysisId = String(section.getAttribute("data-analysis-id") || "");
          await mountSection(section, analysisById.get(analysisId));
          if (section === targetSection) {
            break;
          }
        }
      }
      const alignToTarget = (behavior) => {
        const nextTop = Math.max(
          0,
          target.getBoundingClientRect().top + window.scrollY - headerStackOffsetPx(12)
        );
        window.scrollTo({ top: nextTop, behavior: behavior });
      };
      alignToTarget("smooth");
      window.setTimeout(() => alignToTarget("auto"), 320);
      window.setTimeout(() => alignToTarget("auto"), 920);
      setActiveTocHeading(headingId, true);
    });

    let ticking = false;
    const updateActiveHeading = () => {
      const headingId = pickActiveHeading();
      if (headingId) {
        setActiveTocHeading(headingId, true);
      }
    };
    const onScroll = () => {
      if (ticking) {
        return;
      }
      ticking = true;
      window.requestAnimationFrame(() => {
        ticking = false;
        updateActiveHeading();
      });
    };

    window.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("hashchange", () => {
      const headingId = normalizeHashId(window.location.hash);
      if (headingId && document.getElementById(headingId)) {
        setActiveTocHeading(headingId, false);
      }
    });

    const initialHash = normalizeHashId(window.location.hash);
    if (initialHash && document.getElementById(initialHash)) {
      setActiveTocHeading(initialHash, false);
    } else {
      updateActiveHeading();
    }
  }

  function collectDetectorStats() {
    const summaries = reportData.detector_summaries || {};
    return Object.keys(summaries)
      .sort()
      .map((detectorName) => {
        const summary = summaries[detectorName] || {};
        const flaggedKeys = [
          "n_significant_windows",
          "n_anomaly_buckets",
          "n_high_priority_windows",
          "n_time_bucket_flags",
          "n_day_slot_outliers",
        ];
        const lowPowerKeys = [
          "n_low_power_windows",
          "n_low_power_buckets",
          "n_low_power_match_buckets",
          "n_low_power_time_buckets",
          "n_low_power_day_slots",
        ];
        return {
          detectorName: detectorName,
          flagged: flaggedKeys.reduce((acc, key) => (Object.prototype.hasOwnProperty.call(summary, key) ? toNumber(summary[key]) : acc), 0),
          lowPower: lowPowerKeys.reduce((acc, key) => (Object.prototype.hasOwnProperty.call(summary, key) ? toNumber(summary[key]) : acc), 0),
        };
      });
  }

  function updateReportTimezoneSummary() {
    const host = document.getElementById("report-timezone-summary");
    if (!host) {
      return;
    }
    host.textContent = "All times in this report are shown in " + reportTimezoneLabel + ".";
  }

  function normalizeBillShortName(value) {
    const source = String(value || "")
      .replace(/_/g, " ")
      .trim();
    if (!source) {
      return "";
    }
    const collapsed = source.replace(/\s+/g, " ");
    const canonical = collapsed.match(/^([A-Za-z]{1,4})[-\s]?(\d{3,5}[A-Za-z]?)$/);
    if (canonical) {
      return canonical[1].toUpperCase() + " " + canonical[2].toUpperCase();
    }
    return collapsed;
  }

  function escapeRegexLiteral(value) {
    return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  function hearingContextSource() {
    return hearingContextPanel && hearingContextPanel.source && typeof hearingContextPanel.source === "object"
      ? hearingContextPanel.source
      : {};
  }

  function readHearingContextString(fieldCandidates) {
    const candidates = Array.isArray(fieldCandidates)
      ? fieldCandidates
          .map((value) => String(value || "").trim())
          .filter((value) => !!value)
      : [];
    if (!candidates.length) {
      return "";
    }

    const source = hearingContextSource();
    for (const key of candidates) {
      const value = source[key];
      if (typeof value === "string" && value.trim()) {
        return value.trim();
      }
    }

    for (const key of candidates) {
      const value = hearingContextPanel ? hearingContextPanel[key] : null;
      if (typeof value === "string" && value.trim()) {
        return value.trim();
      }
    }

    const lowered = new Set(candidates.map((value) => value.toLowerCase()));
    const metadataRows = Array.isArray(hearingContextPanel.metadata_rows)
      ? hearingContextPanel.metadata_rows
      : [];
    for (const row of metadataRows) {
      const field = String(row && row.field ? row.field : "")
        .trim()
        .toLowerCase();
      if (!field || !lowered.has(field)) {
        continue;
      }
      const value = row ? row.value : null;
      if (value === null || value === undefined) {
        continue;
      }
      const text = String(value).trim();
      if (text) {
        return text;
      }
    }
    return "";
  }

  function deriveBillShortName() {
    const fromContext = normalizeBillShortName(readHearingContextString(["short_bill_id"]));
    if (fromContext) {
      return fromContext;
    }

    const hearingId = String(hearingContextPanel.hearing_id || "").trim();
    if (hearingId) {
      const prefix = hearingId.split("-")[0] || "";
      const fromPrefix = normalizeBillShortName(prefix);
      if (fromPrefix) {
        return fromPrefix;
      }
      const tokenMatch = hearingId.match(/([A-Za-z]{1,4}[-\s]?\d{3,5}[A-Za-z]?)/);
      if (tokenMatch) {
        const fromToken = normalizeBillShortName(tokenMatch[1]);
        if (fromToken) {
          return fromToken;
        }
      }
    }

    return "";
  }

  function deriveAgendaItemDescription() {
    return readHearingContextString(["agenda_item_description", "agenda_description"]);
  }

  function deriveBillLongTitle() {
    return readHearingContextString(["bill_title"]);
  }

  function deriveCommitteeName() {
    return readHearingContextString(["committee_name", "committee"]);
  }

  function deriveChamberName() {
    return readHearingContextString(["chamber"]);
  }

  function deriveMeetingStartEpochMillis() {
    const rawMeetingStart = readHearingContextString(["meeting_start"]);
    return toEpochMillis(rawMeetingStart);
  }

  function stripLeadingBillPrefix(description, billShortName) {
    const source = String(description || "").replace(/\s+/g, " ").trim();
    if (!source) {
      return "";
    }
    const canonicalShortName = normalizeBillShortName(billShortName);
    if (!canonicalShortName) {
      return source;
    }
    let stripped = source;
    const canonical = canonicalShortName.match(/^([A-Za-z]{1,4})\s+(\d{3,5}[A-Za-z]?)$/);
    if (canonical) {
      const prefixPattern = new RegExp(
        "^\\s*" +
          escapeRegexLiteral(canonical[1]) +
          "\\s*[-\\s]*" +
          escapeRegexLiteral(canonical[2]) +
          "\\b\\s*[:\\-–—]?\\s*",
        "i"
      );
      stripped = stripped.replace(prefixPattern, "").trim();
    }
    if (!stripped) {
      return "";
    }
    const compactShort = canonicalShortName.toLowerCase().replace(/[^a-z0-9]+/g, "");
    const compactStripped = stripped.toLowerCase().replace(/[^a-z0-9]+/g, "");
    if (compactShort && compactStripped === compactShort) {
      return "";
    }
    return stripped;
  }

  function buildCombinedBillHeading(shortBillName, agendaDescription) {
    const shortLabel = normalizeBillShortName(shortBillName);
    const description = String(agendaDescription || "").replace(/\s+/g, " ").trim();
    if (shortLabel && description) {
      const dedupedDescription = stripLeadingBillPrefix(description, shortLabel);
      if (dedupedDescription) {
        return shortLabel + ": " + dedupedDescription;
      }
      return shortLabel;
    }
    if (shortLabel) {
      return shortLabel;
    }
    return description;
  }

  function buildSidebarAuditHeading(shortBillName) {
    const shortLabel = normalizeBillShortName(shortBillName);
    if (shortLabel) {
      return shortLabel + ": Sign-in Audit";
    }
    return "Sign-in Audit";
  }

  function formatMeetingDateTime(epochMillis) {
    if (!Number.isFinite(epochMillis)) {
      return "";
    }
    return meetingDateTimeFormatter.format(new Date(epochMillis));
  }

  function buildMeetingContextSummary(epochMillis, chamber, committee) {
    const parts = [];
    const meetingLabel = formatMeetingDateTime(epochMillis);
    if (meetingLabel) {
      parts.push(meetingLabel);
    }
    const chamberCommittee = [String(chamber || "").trim(), String(committee || "").trim()]
      .filter((value) => !!value)
      .join(" · ");
    if (chamberCommittee) {
      parts.push(chamberCommittee);
    }
    return parts.join(" · ");
  }

  function applySidebarBillMeta() {
    const headerTitleHost = document.getElementById("header-bill-title");
    const headerContextHost = document.getElementById("header-context-meta");
    const headerBillTitleHost = document.getElementById("header-bill-long-title");
    const sidebarTitleHost = document.getElementById("sidebar-report-title");
    const sidebarMeetingHost = document.getElementById("sidebar-meeting-meta");

    const billShortName = deriveBillShortName();
    const agendaDescription = deriveAgendaItemDescription();
    const combinedTitle = buildCombinedBillHeading(billShortName, agendaDescription);
    const meetingStartEpochMillis = deriveMeetingStartEpochMillis();
    const committeeName = deriveCommitteeName();
    const chamberName = deriveChamberName();
    const billTitleFull = deriveBillLongTitle();
    const headerContextSummary = buildMeetingContextSummary(
      meetingStartEpochMillis,
      chamberName,
      committeeName
    );
    const sidebarMeetingSummary = formatMeetingDateTime(meetingStartEpochMillis);
    const truncatedBillTitle = truncateLegendText(billTitleFull, 128);

    if (headerTitleHost) {
      if (combinedTitle) {
        headerTitleHost.textContent = combinedTitle;
        headerTitleHost.classList.remove("hidden");
      } else {
        headerTitleHost.textContent = "";
        headerTitleHost.classList.add("hidden");
      }
    }

    if (headerContextHost) {
      if (headerContextSummary) {
        headerContextHost.textContent = headerContextSummary;
        headerContextHost.classList.remove("hidden");
      } else {
        headerContextHost.textContent = "";
        headerContextHost.classList.add("hidden");
      }
    }

    if (headerBillTitleHost) {
      const normalizedCombinedTitle = combinedTitle
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, " ")
        .trim();
      const normalizedBillTitle = billTitleFull
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, " ")
        .trim();
      const shouldShowBillTitle =
        !!truncatedBillTitle &&
        (!normalizedBillTitle ||
          !normalizedCombinedTitle ||
          normalizedBillTitle !== normalizedCombinedTitle);
      if (shouldShowBillTitle) {
        headerBillTitleHost.textContent = truncatedBillTitle;
        headerBillTitleHost.title = billTitleFull;
        headerBillTitleHost.classList.remove("hidden");
      } else {
        headerBillTitleHost.textContent = "";
        headerBillTitleHost.removeAttribute("title");
        headerBillTitleHost.classList.add("hidden");
      }
    }

    if (sidebarTitleHost) {
      sidebarTitleHost.textContent = buildSidebarAuditHeading(billShortName);
    }

    if (sidebarMeetingHost) {
      if (sidebarMeetingSummary) {
        sidebarMeetingHost.textContent = sidebarMeetingSummary;
        sidebarMeetingHost.classList.remove("hidden");
      } else {
        sidebarMeetingHost.textContent = "";
        sidebarMeetingHost.classList.add("hidden");
      }
    }

    const title = String(document.title || "").trim();
    const titleToken = normalizeBillShortName(billShortName) || combinedTitle;
    if (!titleToken) {
      return;
    }
    if (!title) {
      document.title = "Testifier Audit Report - " + titleToken;
      return;
    }
    if (!title.includes(titleToken)) {
      document.title = title + " - " + titleToken;
    }
  }

  function buildKpis() {
    const artifactRows = Object.values(reportData.artifact_rows || {}).map((value) => toNumber(value));
    const detectorStats = collectDetectorStats();

    const setText = (id, value) => {
      const element = document.getElementById(id);
      if (element) {
        element.textContent = String(value);
      }
    };
    const setMeta = (id, value) => {
      const element = document.getElementById(id);
      if (element) {
        element.textContent = value || "";
      }
    };

    if (isOffHoursFocusOnly) {
      const artifactEntries = Object.entries(reportData.artifact_rows || {}).filter((entry) =>
        String(entry[0] || "").startsWith("off_hours.")
      );
      const offHoursArtifactFallback = artifactEntries.reduce(
        (acc, entry) => acc + toNumber(entry[1]),
        0
      );
      const offHoursSummaryRows = getChartRows("off_hours_summary_compare");
      const offHoursSummary = offHoursSummaryRows.length ? offHoursSummaryRows[0] : {};
      const offHoursRecords = toNumber(offHoursSummary.off_hours);
      const primaryBucket = toFiniteNumberOrNull(offHoursSummary.primary_bucket_minutes);
      const offHoursTimelineRows = getChartRows("off_hours_control_timeline");
      const primaryBucketRows = offHoursTimelineRows.filter((row) => {
        const rowBucket = toFiniteNumberOrNull(row.bucket_minutes);
        if (primaryBucket === null || rowBucket === null) {
          return false;
        }
        return rowBucket === primaryBucket;
      });
      let lowPowerOffHoursWindows = primaryBucketRows.filter((row) => {
        const alertWindow = Object.prototype.hasOwnProperty.call(row, "is_alert_off_hours_window")
          ? toBool(row.is_alert_off_hours_window)
          : toBool(row.is_off_hours_window);
        return alertWindow && toBool(row.is_low_power);
      }).length;
      const robustAlerts = toNumber(offHoursSummary.off_hours_windows_primary_alert);
      const twoSidedFlags = toNumber(
        offHoursSummary.off_hours_windows_significant_primary_two_sided
      );
      const lowerTailFlags = toNumber(offHoursSummary.off_hours_windows_significant_primary);
      const fallbackBreaches = toNumber(
        offHoursSummary.off_hours_windows_below_primary_control_998
      );
      const alertEligibleSummary = toFiniteNumberOrNull(
        offHoursSummary.off_hours_windows_alert_eligible
      );
      const alertEligibleFallback = primaryBucketRows.filter((row) => {
        const alertWindow = Object.prototype.hasOwnProperty.call(row, "is_alert_off_hours_window")
          ? toBool(row.is_alert_off_hours_window)
          : toBool(row.is_off_hours_window);
        return alertWindow && toFiniteNumberOrNull(row.pro_rate) !== null;
      }).length;
      const alertEligibleWindows = Math.max(
        0,
        Math.round(
          alertEligibleSummary !== null ? alertEligibleSummary : alertEligibleFallback
        )
      );
      const lowPowerSummary = toFiniteNumberOrNull(
        offHoursSummary.off_hours_windows_alert_eligible_low_power
      );
      if (lowPowerSummary !== null) {
        lowPowerOffHoursWindows = Math.max(0, Math.round(lowPowerSummary));
      }
      const testedSummary = toFiniteNumberOrNull(offHoursSummary.off_hours_windows_tested);
      const testedOffHoursWindows = Math.max(
        0,
        Math.round(
          testedSummary !== null
            ? testedSummary
            : Math.max(0, alertEligibleWindows - lowPowerOffHoursWindows)
        )
      );
      const offHoursRatio = toFiniteNumberOrNull(offHoursSummary.off_hours_ratio);
      const onHoursRecords = toNumber(offHoursSummary.on_hours);
      const totalRecords = Math.max(
        0,
        Math.round((offHoursRecords > 0 ? offHoursRecords : 0) + Math.max(0, onHoursRecords))
      );
      const primaryBaselineMethod =
        typeof offHoursSummary.primary_baseline_method === "string"
          ? offHoursSummary.primary_baseline_method.replace(/_/g, " ")
          : "";
      const flaggedKpiValue =
        robustAlerts > 0
          ? robustAlerts
          : twoSidedFlags > 0
            ? twoSidedFlags
            : lowerTailFlags > 0
              ? lowerTailFlags
              : fallbackBreaches;

      setText(
        "kpi-artifacts",
        Math.round(offHoursRecords > 0 ? offHoursRecords : offHoursArtifactFallback).toLocaleString()
      );
      setText("kpi-detectors", "1");
      setText("kpi-flagged", Math.round(flaggedKpiValue).toLocaleString());
      setText("kpi-low-power", Math.round(lowPowerOffHoursWindows).toLocaleString());

      setMeta(
        "kpi-artifacts-meta",
        offHoursRatio !== null && totalRecords > 0
          ? "Share of all records: " +
              formatPercent(offHoursRatio, 1) +
              " (" +
              totalRecords.toLocaleString() +
              " total)."
          : ""
      );
      setMeta(
        "kpi-detectors-meta",
        primaryBaselineMethod ? "Primary baseline: " + primaryBaselineMethod + "." : ""
      );
      setMeta(
        "kpi-flagged-meta",
        alertEligibleWindows > 0
          ? "Tested windows: " +
              testedOffHoursWindows.toLocaleString() +
              "/" +
              alertEligibleWindows.toLocaleString() +
              " (" +
              formatRatio(testedOffHoursWindows, alertEligibleWindows, 1) +
              ")."
          : "No alert-eligible windows in the primary bucket."
      );
      setMeta(
        "kpi-low-power-meta",
        alertEligibleWindows > 0
          ? "Low-power among alert-eligible: " +
              lowPowerOffHoursWindows.toLocaleString() +
              "/" +
              alertEligibleWindows.toLocaleString() +
              " (" +
              formatRatio(lowPowerOffHoursWindows, alertEligibleWindows, 1) +
              ")."
          : "No alert-eligible windows in the primary bucket."
      );

      const tierHost = document.getElementById("off-hours-evidence-tier");
      if (tierHost) {
        let tierClass = "tier-none";
        let tierText = "Evidence tier: No off-hours support";
        if (offHoursRecords > 0 && alertEligibleWindows > 0) {
          if (robustAlerts > 0) {
            tierClass = "tier-strong";
            tierText =
              "Evidence tier: Strong (" +
              Math.round(robustAlerts).toLocaleString() +
              " robust primary alerts)";
          } else {
            tierClass = "tier-descriptive";
            tierText = "Evidence tier: Descriptive-only (no robust primary alerts)";
          }
        }
        tierHost.classList.remove("hidden", "tier-strong", "tier-descriptive", "tier-none");
        tierHost.classList.add(tierClass);
        tierHost.textContent = tierText;
      }

      const inferenceBanner = document.getElementById("off-hours-inference-banner");
      if (inferenceBanner) {
        const noInferentialSupport =
          alertEligibleWindows > 0 && testedOffHoursWindows === 0;
        inferenceBanner.classList.toggle("hidden", !noInferentialSupport);
        if (noInferentialSupport) {
          inferenceBanner.classList.remove("ok");
          inferenceBanner.textContent =
            "Inferential scan unavailable in the primary bucket: 0/" +
            alertEligibleWindows.toLocaleString() +
            " alert-eligible windows passed low-power support. Treat off-hours results as descriptive-only for this run.";
        } else {
          inferenceBanner.textContent = "";
        }
      }
      return;
    }

    setMeta("kpi-artifacts-meta", "");
    setMeta("kpi-detectors-meta", "");
    setMeta("kpi-flagged-meta", "");
    setMeta("kpi-low-power-meta", "");

    setText(
      "kpi-artifacts",
      artifactRows.reduce((acc, value) => acc + value, 0).toLocaleString()
    );
    setText("kpi-detectors", detectorStats.length.toLocaleString());
    setText(
      "kpi-flagged",
      detectorStats.reduce((acc, item) => acc + toNumber(item.flagged), 0).toLocaleString()
    );
    setText(
      "kpi-low-power",
      detectorStats.reduce((acc, item) => acc + toNumber(item.lowPower), 0).toLocaleString()
    );
  }

  async function ensureHeaderDataLoaded() {
    const requiredAnalysisIds = new Set();
    if (isOffHoursFocusOnly) {
      ["off_hours_summary_compare", "off_hours_control_timeline"].forEach((chartId) => {
        const analysisId = String(chartToAnalysis.get(chartId) || "").trim();
        if (analysisId) {
          requiredAnalysisIds.add(analysisId);
        }
      });
      requiredAnalysisIds.add("off_hours");
    }
    if (!requiredAnalysisIds.size) {
      return;
    }
    await Promise.all(
      Array.from(requiredAnalysisIds).map((analysisId) => ensureAnalysisDataLoaded(analysisId))
    );
  }

  async function mountAllSections() {
    const sections = Array.from(document.querySelectorAll("[data-analysis-id]"));
    for (const section of sections) {
      const analysisId = String(section.getAttribute("data-analysis-id") || "");
      await mountSection(section, analysisById.get(analysisId));
    }

    const overviewHosts = Array.from(
      document.querySelectorAll("#section-overview [data-chart-id]")
    );
    if (!overviewHosts.length) {
      return;
    }
    await Promise.all(overviewHosts.map((host) => mountChartHost(host)));
  }

  async function ensureWindowDrilldownDataLoaded() {
    if (isOffHoursFocusOnly) {
      return;
    }
    const requiredAnalyses = [
      "baseline_profile",
      "bursts",
      "procon_swings",
      "duplicates_exact",
      "rare_names",
    ];
    const available = requiredAnalyses.filter((analysisId) =>
      !!chartShardEntryForAnalysis(analysisId)
    );
    await Promise.all(available.map((analysisId) => ensureAnalysisDataLoaded(analysisId)));
  }

  applySidebarBillMeta();
  updateReportTimezoneSummary();
  await ensureHeaderDataLoaded();
  buildKpis();
  initThemeControl();
  initSidebarTooltips();
  if (!isOffHoursFocusOnly) {
    renderDataQualityPanel();
    renderHearingContextPanel();
    renderTriageSummary();
    await ensureWindowDrilldownDataLoaded();
    renderInvestigationTables();
  }
  initSidebarToggle();
  initGlobalControlsCollapse();
  initBucketTabs();
  initDuplicateCollisionControls();
  initZoomControls();
  initSidebarFloatingOffsetsObserver();
  initSidebarToc();
  await runWithBusyIndicator("Loading report sections...", async () => {
    await mountAllSections();
  });
  initializeLinkedZoomOnLoad();
  syncControlOverridesToUrl();
  updateCursorAcrossTimeCharts();
  updateZoomRangeLabel();
  scheduleChartResizeSequence();

  window.addEventListener("resize", () => {
    updateSidebarFloatingOffsets();
    scheduleChartResizeSequence();
  });
})();
