from __future__ import annotations

from typing import Any

from testifier_audit.report.rendering.constants import _SCATTER_CHART_IDS

def _detailed_what_to_look_for_by_analysis() -> dict[str, list[str]]:
    return {
        "baseline_profile": [
            (
                "Short, isolated spikes in volume with no matching shift in pro rate "
                "or corroborating detector flags are often random campaign pulses "
                "rather than systemic manipulation."
            ),
            (
                "Extended level shifts (for example, 60-240 minutes) in both volume "
                "and composition, especially when Wilson bands tighten, suggest a "
                "meaningful regime change worth cross-checking against changepoints "
                "and composite evidence."
            ),
            (
                "Very low overnight volume can create dramatic percentage swings; "
                "prioritize windows where elevated rates persist after local volume "
                "recovers into daytime traffic."
            ),
        ],
        "bursts": [
            (
                "Single-window rate-ratio peaks can be benign; stronger signals are "
                "contiguous runs of elevated rate ratios that recur at multiple window "
                "sizes (for example 5m and 30m both elevated)."
            ),
            (
                "High observed counts with low q-values in sustained windows imply "
                "concentration beyond baseline expectation, especially when these "
                "bursts overlap with duplicate-name or swing anomalies."
            ),
            (
                "Suppressed or unusually flat burst activity can also be informative "
                "if baseline volume is high; a lack of natural variability may "
                "indicate synchronized intake behavior or batching."
            ),
        ],
        "procon_swings": [
            (
                "Brief pro-rate jumps with wide Wilson intervals typically indicate "
                "low-support noise; treat them as weak unless adjacent buckets move "
                "in the same direction with tighter intervals."
            ),
            (
                "Extended daytime streaks of positive or negative shifts (multiple "
                "contiguous buckets) can indicate directional mobilization, queueing "
                "effects, or operational gating; confirm with day/hour and "
                "time-of-day panels."
            ),
            (
                "Large off-hours directional blocks that reverse at wake-hour "
                "transitions may indicate temporally segmented participation behavior, "
                "including potential strategic timing by one side."
            ),
        ],
        "changepoints": [
            (
                "Look for clustered breakpoints across both volume and pro rate; "
                "multi-metric co-occurrence is usually more meaningful than a "
                "solitary break in one metric."
            ),
            (
                "Large absolute deltas with sustained post-break behavior "
                "(not immediate reversion) indicate structural transitions rather "
                "than transient spikes."
            ),
            (
                "Repeated changes at similar hours across days can reflect "
                "operational schedules; treat as lower risk unless change magnitudes "
                "are extreme and detector corroboration is strong."
            ),
        ],
        "off_hours": [
            (
                "Prioritize robust primary alerts (below primary 99.8% lower limit, "
                "lower-tail FDR-significant, and materially negative delta at "
                "adequate support); avoid interpreting low-n windows even if raw "
                "rates look extreme."
            ),
            (
                "If tested off-hours windows are zero after low-power filtering, treat "
                "the section as descriptive-only and avoid inferential conclusions."
            ),
            (
                "Use the funnel view to compare primary and global expected bands; "
                "treat primary-baseline breaches as the decision metric and global "
                "bands as context."
            ),
            (
                "In date-hour heatmaps, repeated overnight blocks across multiple "
                "dates are stronger than a single-night dip; corroborate with burst, "
                "periodicity, and duplicate detectors before escalation."
            ),
        ],
        "duplicates_exact": [
            (
                "Short bursts of repeated names in tiny windows may occur during "
                "legitimate group actions; concern rises when concentration repeats "
                "across multiple larger buckets."
            ),
            (
                "Names that appear repeatedly while switching pro/con positions are "
                "higher-priority review targets because they indicate inconsistent "
                "stance representation under one canonical identity."
            ),
            (
                "Persistent duplicate concentration during otherwise stable baseline "
                "periods can imply scripted submissions or queue replay effects "
                "rather than organic participation."
            ),
        ],
        "sortedness": [
            (
                "Single alphabetical spikes in small buckets can be accidental; "
                "repeated elevated alphabetical ratios across 15m-120m buckets "
                "suggest process-level ordering behavior."
            ),
            (
                "Sustained ordered streaks during high-volume windows are unusual "
                "for organic arrivals and may imply batch uploads, sorted lists, or "
                "deterministic queue processing."
            ),
            (
                "Low sortedness is expected for organic traffic, so abrupt "
                "transitions from unsorted to highly sorted and back are more "
                "informative than consistently modest ratios."
            ),
        ],
        "rare_names": [
            (
                "Short-lived unique-ratio increases during low volume can be "
                "misleading; investigate when unique-ratio elevation persists into "
                "higher-support windows."
            ),
            (
                "Concurrent rises in weirdness scores, singleton concentration, and "
                "rarity quantiles indicate novelty concentration beyond normal "
                "lexical drift."
            ),
            (
                "Extended rarity suppression (unusually low novelty) can also be "
                "noteworthy in broad public hearings and may suggest repeated "
                "template populations."
            ),
        ],
        "org_anomalies": [
            (
                "Blank-organization spikes in low-support windows are weak evidence; "
                "prioritize wide windows where blank rate rises and Wilson bands "
                "remain narrow."
            ),
            (
                "Divergence between pro and con blank-org rates over sustained "
                "periods can indicate side-specific form behavior, campaign guidance, "
                "or data-entry heterogeneity."
            ),
            (
                "Sharp blank-rate reversals around specific times may indicate UX "
                "changes, batch imports, or conditional form paths and should be "
                "checked against operational logs."
            ),
        ],
        "voter_registry_match": [
            (
                "Interpret primary linkage through conservative outcomes "
                "(matched unique, matched ambiguous, unmatched) and keep unmatched "
                "language scoped to the WA active voter file."
            ),
            (
                "Compare unmatched-rate differences at both row and unique-name units; "
                "focus on persistent differences that also remain visible in the "
                "position-bounds span panel."
            ),
            (
                "Use the rows-vs-unique position-bounds span panel to assess linkage "
                "assumption sensitivity; wide spans indicate directionality is fragile."
            ),
        ],
        "periodicity": [
            (
                "Minor periodic peaks are normal in outreach-driven datasets; "
                "stronger signals appear when clock-face concentration, "
                "autocorrelation peaks, and spectrum peaks align."
            ),
            (
                "Narrow high-power peaks at specific periods (for example near exact "
                "campaign cadence intervals) can indicate automation or tightly "
                "scheduled reminders."
            ),
            (
                "Extended suppression of expected periodic structure in otherwise "
                "campaign-heavy contexts may imply missing intervals or "
                "preprocessing artifacts."
            ),
        ],
        "multivariate_anomalies": [
            (
                "Single high anomaly buckets with low support can be model-noise; "
                "prioritize consecutive high-score windows with model eligibility and "
                "corroborating detector evidence."
            ),
            (
                "Joint excursions in volume, duplicate fraction, blank-org rate, and "
                "pro-rate shape are stronger than any one feature spike in isolation."
            ),
            (
                "Extended high-percentile stretches can indicate sustained "
                "behavioral mode changes; inspect top buckets and feature projection "
                "for which dimensions drive score elevation."
            ),
        ],
        "composite_score": [
            (
                "High composite windows are most useful when evidence-count is high "
                "and signals come from independent detectors rather than one "
                "detector repeated across scales."
            ),
            (
                "Short isolated composite spikes can still be benign; extended "
                "elevated runs with overlapping burst/swing/changepoint/ML evidence "
                "are higher-priority review candidates."
            ),
            (
                "Very low composite scores during known high-activity periods can "
                "reveal under-sensitive detector settings or data-quality gaps and "
                "should trigger configuration review."
            ),
        ],
    }


def _analysis_help_hints() -> dict[str, dict[str, str]]:
    return {
        "baseline_profile": {
            "primary_metric": "baseline volume and composition drift",
            "momentary_high": (
                "a short notice event, reminder blast, or temporary queue release"
            ),
            "momentary_low": (
                "normal minute-level quiet periods or ingest timing jitter"
            ),
            "extended_high": (
                "a sustained participation regime shift that can affect all downstream "
                "detectors"
            ),
            "extended_low": (
                "potential ingestion gaps, hearing lulls, or sustained reduced campaign "
                "activity"
            ),
        },
        "bursts": {
            "primary_metric": "observed-vs-expected burst intensity",
            "momentary_high": (
                "legitimate synchronized outreach or one-off reminder cascades"
            ),
            "momentary_low": (
                "normal random fluctuation when expected baseline is already elevated"
            ),
            "extended_high": (
                "repeated concentration windows that deserve correlation with duplicate "
                "and swing signals"
            ),
            "extended_low": (
                "suppressed variance that can indicate workflow smoothing or batching"
            ),
        },
        "procon_swings": {
            "primary_metric": (
                "directional pro/con ratio movement relative to expected bands"
            ),
            "momentary_high": "small-sample randomness, especially in low-power buckets",
            "momentary_low": (
                "brief balancing waves where opposite-side submissions cluster together"
            ),
            "extended_high": (
                "persistent directional mobilization or process-side skew in intake "
                "timing"
            ),
            "extended_low": (
                "prolonged suppression of one side that may indicate queueing or "
                "campaign fatigue"
            ),
        },
        "changepoints": {
            "primary_metric": "structural breaks in level or composition",
            "momentary_high": (
                "single regime boundaries caused by predictable hearing state "
                "transitions"
            ),
            "momentary_low": (
                "noisy micro-fluctuations that do not persist across adjacent windows"
            ),
            "extended_high": (
                "multi-break episodes indicating stable before/after behavioral regimes"
            ),
            "extended_low": "a relatively stationary process with fewer systemic shifts",
        },
        "off_hours": {
            "primary_metric": "model-aware off-hours composition shift with volume context",
            "momentary_high": (
                "short overnight swings that stay within primary control limits after "
                "day/hour adjustment"
            ),
            "momentary_low": (
                "small support windows where apparent extremes are likely sampling noise"
            ),
            "extended_high": (
                "repeating robust primary alerts across adjacent windows or nights at "
                "moderate/high support"
            ),
            "extended_low": (
                "stable overnight behavior that remains inside expected primary "
                "control bands"
            ),
        },
        "duplicates_exact": {
            "primary_metric": "exact repeated-name concentration",
            "momentary_high": (
                "household/shared-name collisions or small coordinated batches"
            ),
            "momentary_low": "normal diversity of distinct names in organic intake",
            "extended_high": (
                "repeat-name patterns likely to influence authenticity and weighting "
                "assumptions"
            ),
            "extended_low": "healthy name diversity with limited exact repetition pressure",
        },
        "sortedness": {
            "primary_metric": "alphabetical/ordered submission behavior",
            "momentary_high": (
                "small sorted snippets caused by chance or local administrative handling"
            ),
            "momentary_low": "expected unsorted arrivals from organic user behavior",
            "extended_high": "batch-oriented or deterministic ordering processes across windows",
            "extended_low": (
                "persistent organic ordering noise without process-level sorting artifacts"
            ),
        },
        "rare_names": {
            "primary_metric": "novelty, uniqueness, and rarity concentration",
            "momentary_high": (
                "brief novelty spikes from campaign expansion to new participants"
            ),
            "momentary_low": (
                "common-name clustering or temporary shrinkage in participant diversity"
            ),
            "extended_high": (
                "sustained lexical novelty requiring cross-check against lookup coverage"
            ),
            "extended_low": "repeated-name dominance or limited participant turnover",
        },
        "org_anomalies": {
            "primary_metric": "blank/null organization usage and split behavior",
            "momentary_high": "form UX friction or temporary omission guidance in outreach",
            "momentary_low": "short windows where organization prompts were more salient",
            "extended_high": (
                "systemic metadata sparsity that can bias affiliation interpretation"
            ),
            "extended_low": "more complete organization capture across participation streams",
        },
        "voter_registry_match": {
            "primary_metric": "conservative matched/unmatched composition with uncertainty accounting",
            "momentary_high": "brief matched concentration that may reflect clean registry overlap",
            "momentary_low": "short-lived unmatched growth in sparse buckets",
            "extended_high": "stable conservative matched coverage across windows",
            "extended_low": (
                "persistent unmatched dominance requiring normalization and source review"
            ),
        },
        "periodicity": {
            "primary_metric": "recurring timing structure across minute and lag spaces",
            "momentary_high": "single reminder cycles or one-time timed campaign sends",
            "momentary_low": "flat/noisy slots where periodic patterns are not dominant",
            "extended_high": (
                "repeated cadence signatures that may indicate automation or strict "
                "scheduling"
            ),
            "extended_low": "weak periodic structure consistent with more organic arrival timing",
        },
        "multivariate_anomalies": {
            "primary_metric": "joint anomaly score across multiple behavioral features",
            "momentary_high": (
                "single-bucket feature coincidence without sustained corroboration"
            ),
            "momentary_low": "brief reversion to feature-space baseline",
            "extended_high": (
                "multi-feature regime changes needing manual validation and context "
                "checks"
            ),
            "extended_low": "feature combinations staying near historically typical mixtures",
        },
        "composite_score": {
            "primary_metric": "cross-detector evidence overlap and prioritization",
            "momentary_high": "short-lived detector agreement around a local event",
            "momentary_low": "isolated detector activity without consensus evidence",
            "extended_high": (
                "durable multi-detector agreement that should drive investigation "
                "priority"
            ),
            "extended_low": "broad detector disagreement suggesting mostly baseline behavior",
        },
    }


def _build_analysis_help_docs(
    analysis_definitions: list[dict[str, Any]],
    detailed_look_for: dict[str, list[str]],
) -> dict[str, dict[str, str]]:
    hints = _analysis_help_hints()
    docs: dict[str, dict[str, str]] = {}

    for definition in analysis_definitions:
        analysis_id = str(definition["id"])
        title = str(definition["title"])
        hint = hints.get(analysis_id, {})
        detail_points = detailed_look_for.get(analysis_id, [])
        detail_excerpt = " ".join(detail_points[:3]).strip()
        detail_suffix = (
            detail_excerpt
            if detail_excerpt
            else "Prioritize patterns that persist across adjacent windows and align "
            "with at least one independent detector signal."
        )

        primary_metric = hint.get("primary_metric", "this detector's primary signal")
        momentary_high = hint.get("momentary_high", "a local transient event")
        momentary_low = hint.get("momentary_low", "short-term random variation")
        extended_high = hint.get("extended_high", "a sustained process-level shift")
        extended_low = hint.get("extended_low", "a stable low-intensity regime")

        docs[analysis_id] = {
            "what_is_this": (
                f"{title} focuses on {primary_metric}. "
                "This section combines a hero chart, supporting charts, and tables to "
                "separate one-off noise from meaningful sustained behavior. "
                "Treat it as a detector notebook: start broad, then drill into "
                "specific windows with evidence context."
            ),
            "why_it_matters": (
                "This data matters because it changes how confident you should be in "
                "an anomaly narrative. Strong claims should come from persistent, "
                "well-supported patterns rather than isolated spikes. "
                "It also prevents both over-calling benign fluctuations and missing "
                "slow-burn anomalies that only emerge over longer runs."
            ),
            "how_to_interpret": (
                "Read the hero chart first for the dominant temporal structure, then "
                "use detail charts to test whether the signal repeats across scales, "
                "dayparts, or subgroup splits. Use tables to verify exact values and "
                "support counts behind flagged windows. "
                "When uncertainty bands or low-power markers are present, discount "
                "single-window jumps unless they recur with stronger support."
            ),
            "what_to_look_for": (
                f"{definition['what_to_look_for']} "
                f"{detail_suffix} "
                "Investigation priority should increase when multiple independent views "
                "tell the same story at the same time."
            ),
            "momentary_high_low": (
                "Momentary highs can indicate "
                f"{momentary_high}. Momentary lows can indicate {momentary_low}. "
                "Treat both cautiously when low-power flags are present. "
                "A practical rule: do not escalate on a single bucket unless a nearby "
                "table row and at least one companion chart support the same direction."
            ),
            "extended_high_low": (
                f"Extended highs can indicate {extended_high}. "
                f"Extended lows can indicate {extended_low}. "
                "Persistence across adjacent windows and corroborating detectors raises "
                "confidence that the shift is meaningful. "
                "Extended runs deserve timeline annotation and root-cause notes so later "
                "reviewers can separate operational context from suspicious behavior."
            ),
        }

    return docs


def _chart_family(chart_id: str) -> str:
    chart_id_norm = str(chart_id or "")
    if chart_id_norm in _SCATTER_CHART_IDS or "funnel" in chart_id_norm:
        return "scatter"
    if "heatmap" in chart_id_norm:
        return "heatmap"
    if any(
        token in chart_id_norm
        for token in ("timeline", "rates", "ratio", "trend", "bucket", "profile")
    ):
        return "timeseries"
    return "categorical"


def _build_chart_help_docs(
    chart_legend_docs: dict[str, dict[str, Any]],
) -> dict[str, dict[str, str]]:
    docs: dict[str, dict[str, str]] = {}
    for chart_id, legend_doc in sorted(chart_legend_docs.items()):
        summary = str(legend_doc.get("summary") or "").strip()
        legend_items = legend_doc.get("items", [])
        labels = ", ".join(
            str(item.get("label", "")).strip() for item in legend_items if item
        )
        family = _chart_family(chart_id)

        if family == "heatmap":
            docs[chart_id] = {
                "what_is_this": (
                    f"{summary} This is a matrix view where color encodes magnitude "
                    "across paired axes such as date/hour or slot/day. "
                    "Each cell is a compact summary of one intersection, so the chart "
                    "is optimized for pattern shape over exact per-cell precision."
                ),
                "why_it_matters": (
                    "Heatmaps reveal spatially contiguous patterns that line charts can "
                    "hide, especially repeated daypart behavior and slot-level drift. "
                    "They are especially useful for finding regime-like blocks that "
                    "persist across many adjacent cells."
                ),
                "how_to_interpret": (
                    "Scan for contiguous blocks before focusing on single cells. "
                    "Compare high-intensity and low-intensity regions with bucket "
                    "support and related detector outputs. "
                    "Then check whether color transitions occur at meaningful "
                    "boundaries such as day changes, hearing windows, or slot shifts."
                ),
                "what_to_look_for": (
                    "Look for coherent blocks, repeated stripes, or abrupt regime "
                    "boundaries that persist across adjacent rows/columns. "
                    "Short isolated hot/cold cells are weaker evidence; long bands or "
                    "rectangles are stronger. "
                    f"Legend components: {labels}."
                ),
                "momentary_high_low": (
                    "A single hot/cold cell can reflect transient activity or low "
                    "support. Interpret isolated cells cautiously, especially if they "
                    "do not repeat in neighboring slots. "
                    "Momentary highs can map to one reminder wave; momentary lows can "
                    "map to ordinary quiet periods."
                ),
                "extended_high_low": (
                    "Extended hot/cold regions typically indicate sustained behavioral "
                    "mode shifts. Persistence across multiple dates/slots is stronger "
                    "evidence than one transition point. "
                    "Extended hot regions may indicate durable mobilization or process "
                    "bias; extended cold regions may indicate suppression or inactivity."
                ),
            }
            continue

        if family == "scatter":
            docs[chart_id] = {
                "what_is_this": (
                    f"{summary} This scatter plot maps each bucket as a point in "
                    "feature space, often with color and size as additional signals. "
                    "It is a relationship view, showing joint behavior rather than a "
                    "single metric over time."
                ),
                "why_it_matters": (
                    "Scatter views expose joint-feature structure, clusters, and "
                    "outliers that are not visible in one-dimensional summaries. "
                    "They help determine whether anomalies are isolated outliers or "
                    "part of a broader feature-space regime."
                ),
                "how_to_interpret": (
                    "Read axis meaning first, then evaluate whether outliers are "
                    "isolated or part of a cluster. Use color/size encodings to "
                    "understand confidence and support. "
                    "Cross-reference extreme points with time-based charts to determine "
                    "whether they are single events or repeated states."
                ),
                "what_to_look_for": (
                    "Look for detached point clouds, extreme tails, and dense anomaly "
                    "clusters that align with flagged windows. "
                    "A compact cluster far from baseline often carries more weight than "
                    "one far-away point with low support. "
                    f"Legend components: {labels}."
                ),
                "momentary_high_low": (
                    "A single extreme point may be a one-off event or model artifact. "
                    "Validate with timeline charts and table support counts. "
                    "Momentary lows are usually returns toward baseline and are often "
                    "benign unless paired with abrupt nearby outliers."
                ),
                "extended_high_low": (
                    "Large persistent outlier clusters imply broad feature-space drift. "
                    "Extended low-intensity clustering implies stable baseline behavior. "
                    "Sustained dual-cluster structure can indicate mixed populations or "
                    "alternating operational modes."
                ),
            }
            continue

        if family == "timeseries":
            docs[chart_id] = {
                "what_is_this": (
                    f"{summary} This time-aligned view shows how the measured signal "
                    "changes across chronological buckets. "
                    "It is the primary lens for identifying sequence, duration, and "
                    "coincidence with external events."
                ),
                "why_it_matters": (
                    "Time-series structure distinguishes transient spikes from sustained "
                    "regime changes and helps align detector evidence by timestamp. "
                    "Without duration context, it is easy to overreact to one-bucket "
                    "noise and miss broad shifts."
                ),
                "how_to_interpret": (
                    "Read left to right, compare volume with rate/score overlays, and "
                    "pay attention to uncertainty bounds and low-power markers where "
                    "available. "
                    "When zoomed in, verify whether local extremes persist across "
                    "neighboring buckets and remain visible at wider scales."
                ),
                "what_to_look_for": (
                    "Look for repeated peaks, troughs, trend breaks, and persistent "
                    "drifts across adjacent windows. "
                    "Patterns that recur at the same daypart across dates are usually "
                    "stronger than one isolated wave. "
                    f"Legend components: {labels}."
                ),
                "momentary_high_low": (
                    "Short highs/lows can reflect event timing, random variance, or "
                    "small-sample effects. Confirm with neighboring buckets before "
                    "treating them as material anomalies. "
                    "A momentary high near a known outreach time can be benign; a "
                    "momentary low during expected peak periods may indicate data lag."
                ),
                "extended_high_low": (
                    "Extended highs/lows are stronger indicators of behavioral shifts, "
                    "especially when they persist across multiple bucket sizes and "
                    "coincide with corroborating detector outputs. "
                    "Extended highs may indicate sustained mobilization or systematic "
                    "bias; extended lows may indicate prolonged inactivity or missing "
                    "segments."
                ),
            }
            continue

        docs[chart_id] = {
            "what_is_this": (
                f"{summary} This categorical/ranked chart compares values across "
                "labels, groups, or parameter settings. "
                "It emphasizes composition and concentration instead of chronology."
            ),
            "why_it_matters": (
                "Category comparisons show concentration, imbalance, and dominance "
                "patterns that can explain why timeline signals moved. "
                "They are often the fastest way to identify which subgroup is driving "
                "a detector outcome."
            ),
            "how_to_interpret": (
                "Sort by magnitude, compare head vs tail behavior, and relate category "
                "concentration to corresponding detector windows. "
                "Check both absolute values and relative spacing so you can distinguish "
                "true concentration from a uniformly low baseline."
            ),
            "what_to_look_for": (
                "Look for heavy concentration in a few categories, abrupt drop-offs, "
                "or rare categories with disproportionately high values. "
                "A long flat tail with one or two dominant bars often indicates a "
                "targeted driver worth validating in tables. "
                f"Legend components: {labels}."
            ),
            "momentary_high_low": (
                "A single dominant category may come from one campaign event or local "
                "data artifact. Check whether the dominance repeats over time. "
                "Momentary category suppression can also happen when total volume is "
                "temporarily low."
            ),
            "extended_high_low": (
                "Persistent dominance/absence across many categories can indicate "
                "structural participation effects rather than random variation. "
                "Extended concentration deserves follow-up to determine whether it is "
                "policy-driven outreach, operational process, or suspicious patterning."
            ),
        }

    return docs


def _fallback_chart_legend_doc(chart_id: str) -> dict[str, Any]:
    return {
        "summary": "Legend semantics for this chart.",
        "items": [
            {
                "label": "Primary series",
                "description": f"Main plotted signal for {chart_id.replace('_', ' ')}.",
            },
            {
                "label": "Axes",
                "description": (
                    "X-axis encodes time/category context; "
                    "Y-axis encodes magnitude or rate."
                ),
            },
        ],
    }


def _default_chart_legend_docs() -> dict[str, dict[str, Any]]:
    def timebar(
        *,
        summary: str,
        primary_label: str,
        primary_desc: str,
        include_wilson: bool = False,
        include_low_power: bool = True,
        flagged_label: str | None = None,
        flagged_desc: str | None = None,
        extra: list[dict[str, str]] | None = None,
        volume_label: str = "Volume",
        volume_desc: str = "Bars show record volume in each time bucket.",
    ) -> dict[str, Any]:
        items: list[dict[str, str]] = [
            {"label": volume_label, "description": volume_desc},
            {"label": primary_label, "description": primary_desc},
        ]
        if include_wilson:
            items.extend(
                [
                    {
                        "label": "Wilson low / Wilson high",
                        "description": (
                            "Confidence band for the proportion metric; "
                            "wider bands indicate higher uncertainty."
                        ),
                    }
                ]
            )
        if extra:
            items.extend(extra)
        if flagged_label and flagged_desc:
            items.append({"label": flagged_label, "description": flagged_desc})
        if include_low_power:
            items.append(
                {
                    "label": "Low-power",
                    "description": (
                        "Markers for buckets with insufficient support where rates "
                        "can swing from noise."
                    ),
                }
            )
        return {"summary": summary, "items": items}

    docs: dict[str, dict[str, Any]] = {
        "baseline_volume_pro_rate": timebar(
            summary="Baseline trend of submissions and pro share.",
            primary_label="Pro rate",
            primary_desc="Line shows pro-position share per bucket.",
            include_wilson=True,
        ),
        "baseline_day_hour_volume": {
            "summary": "Day/hour baseline heatmap.",
            "items": [
                {
                    "label": "Cell color",
                    "description": (
                        "Darker cells indicate higher submission volume for that "
                        "weekday/hour."
                    ),
                },
                {"label": "X/Y axes", "description": "X-axis is hour of day; Y-axis is weekday."},
            ],
        },
        "baseline_top_names": {
            "summary": "Top-frequency names.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Total submissions associated with each displayed name.",
                },
                {
                    "label": "X-axis names",
                    "description": "Most frequent names (trimmed to top slice for readability).",
                },
            ],
        },
        "baseline_name_length_distribution": {
            "summary": "Name-length histogram view.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Count of names with the corresponding character length.",
                },
                {"label": "X-axis", "description": "Normalized name length in characters."},
            ],
        },
        "bursts_hero_timeline": timebar(
            summary="Observed burst counts with burst intensity overlay.",
            primary_label="Rate ratio",
            primary_desc="Observed-to-expected count ratio per tested window.",
            include_wilson=False,
            include_low_power=False,
            volume_label="Observed count",
            volume_desc="Bars show observed submissions in each burst window.",
        ),
        "bursts_significance_by_window": {
            "summary": "Burst significance by window size.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Number of significant windows for each tested window size.",
                },
                {"label": "X-axis", "description": "Window size in minutes."},
            ],
        },
        "bursts_composition_shift": timebar(
            summary="Burst composition shift over time.",
            primary_label="Absolute pro-rate shift",
            primary_desc="Absolute deviation of burst-window pro rate from run baseline.",
            include_wilson=False,
            include_low_power=True,
            volume_label="Observed count",
            volume_desc="Observed submissions in each burst window.",
            extra=[
                {
                    "label": "Baseline pro rate",
                    "description": "Run-level baseline pro share for composition comparison.",
                },
                {
                    "label": "Delta pro rate",
                    "description": "Signed burst-window pro-rate shift from baseline.",
                },
            ],
        ),
        "bursts_null_distribution": {
            "summary": "Burst null simulation output.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Maximum simulated count observed in each null iteration.",
                },
                {"label": "X-axis", "description": "Simulation iteration index."},
            ],
        },
        "procon_swings_hero_bucket_trend": timebar(
            summary="Pro-rate trend against baseline stability bands.",
            primary_label="Pro rate",
            primary_desc="Observed pro share in each bucket.",
            include_wilson=True,
            flagged_label="Flagged",
            flagged_desc="Buckets flagged by swing detector for abnormal deviation.",
            extra=[
                {
                    "label": "Baseline pro rate",
                    "description": "Expected day/time pro share baseline.",
                },
                {
                    "label": "Stable lower / stable upper",
                    "description": "Expected range around baseline for normal fluctuation.",
                },
            ],
        ),
        "procon_swings_shift_heatmap": {
            "summary": "Day/slot deviation heatmap.",
            "items": [
                {
                    "label": "Cell color",
                    "description": (
                        "Red cells are more pro-heavy than expected for that slot; "
                        "blue cells are more con-heavy."
                    ),
                },
                {
                    "label": "Slot outlier dots",
                    "description": "Highlighted cells that exceed detector outlier thresholds.",
                },
                {
                    "label": "Axes",
                    "description": (
                        "X-axis is slot-of-day; Y-axis is calendar date in chronological "
                        "top-down order (earliest at top)."
                    ),
                },
            ],
        },
        "procon_swings_day_hour_heatmap": {
            "summary": "Average pro-rate by weekday/hour.",
            "items": [
                {"label": "Cell color", "description": "Darker cells indicate higher pro rate."},
                {
                    "label": "Axes",
                    "description": (
                        "X-axis is hour of day; Y-axis is weekday in chronological "
                        "top-down order (Monday to Sunday)."
                    ),
                },
            ],
        },
        "procon_swings_time_of_day_profile": {
            "summary": "Pro-rate profile by slot-of-day.",
            "items": [
                {"label": "Bar height", "description": "Pro share in that slot-of-day bucket."},
                {"label": "X-axis", "description": "Slot start minute from midnight."},
            ],
        },
        "procon_swings_direction_runs": {
            "summary": "Contiguous pro/con directional runs over time.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Number of contiguous buckets in each directional run.",
                },
                {
                    "label": "Line",
                    "description": "Mean absolute pro-rate shift magnitude across the run.",
                },
                {"label": "X-axis", "description": "Run start timestamp."},
            ],
        },
        "procon_swings_null_distribution": {
            "summary": "Null distribution for swing extremes.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Maximum absolute pro-rate delta per null iteration.",
                },
                {"label": "X-axis", "description": "Simulation iteration index."},
            ],
        },
        "changepoints_hero_timeline": timebar(
            summary="Volume/pro-rate timeline with structural break markers.",
            primary_label="Pro rate",
            primary_desc="Observed pro share over time.",
            include_wilson=True,
            flagged_label="Flagged",
            flagged_desc="Detected changepoint locations.",
        ),
        "changepoints_magnitude": {
            "summary": "Changepoint magnitude ranking.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Absolute change magnitude at each detected break.",
                },
                {"label": "X-axis", "description": "Changepoint index/order."},
            ],
        },
        "changepoints_hour_hist": {
            "summary": "Changepoint timing histogram.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Number of changepoints occurring in each hour-of-day bin.",
                },
                {"label": "X-axis", "description": "Hour of day (0-23)."},
            ],
        },
        "off_hours_control_timeline": timebar(
            summary=(
                "Off-hours control timeline with primary baseline overlays "
                "(model when available, day-adjusted fallback otherwise)."
            ),
            primary_label="Pro rate",
            primary_desc=(
                "Observed pro share by bucket, with Wilson uncertainty and primary "
                "expected/control-band overlays."
            ),
            include_wilson=True,
            include_low_power=True,
            volume_label="Submission count",
            volume_desc="Total records per bucket for support context.",
            flagged_label="Robust lower-tail alert / Robust upper-tail alert",
            flagged_desc=(
                "Alert-eligible windows beyond the primary 99.8% control limits "
                "with tail-consistent FDR support and material effect size. Shaded spans "
                "mark contiguous robust-alert runs (lower or upper tail)."
            ),
            extra=[
                {
                    "label": "Wilson interval area",
                    "description": "Shaded region between Wilson low and Wilson high.",
                },
                {
                    "label": "expected pro rate primary",
                    "description": (
                        "Primary expected pro share (model when available, day-adjusted fallback otherwise)."
                    ),
                },
                {
                    "label": "control low 95 primary / control high 95 primary",
                    "description": "Primary 95% control limits around the primary baseline.",
                },
                {
                    "label": "expected pro rate day",
                    "description": "Day-adjusted expected pro share comparator.",
                },
                {
                    "label": "expected pro rate global",
                    "description": "Hearing-level expected pro share comparator.",
                },
                {
                    "label": "control low 95 global / control high 95 global",
                    "description": "Global 95% control limits for contextual comparison.",
                },
                {
                    "label": "SPC-only flag",
                    "description": (
                        "Window passed the SPC 99.8% control-limit channel but not the FDR channel."
                    ),
                },
                {
                    "label": "FDR-only flag",
                    "description": (
                        "Window passed the two-sided FDR channel but not the SPC 99.8% channel."
                    ),
                },
                {
                    "label": "Robust alert run span",
                    "description": "Shaded x-range for contiguous robust-alert runs.",
                },
            ],
        ),
        "off_hours_funnel_plot": {
            "summary": (
                "Funnel plot of pro share versus support with primary and global "
                "control references."
            ),
            "items": [
                {
                    "label": "On-hours/mixed windows",
                    "description": (
                        "Reference windows outside off-hours dominance (or mixed windows)."
                    ),
                },
                {
                    "label": "Off-hours windows",
                    "description": (
                        "All off-hours-dominant windows regardless of inferential support."
                    ),
                },
                {
                    "label": "Inferentially tested off-hours",
                    "description": (
                        "Off-hours windows that are alert-eligible and not low-power."
                    ),
                },
                {
                    "label": "SPC-only flag",
                    "description": "SPC 99.8% channel hit without two-sided FDR channel support.",
                },
                {
                    "label": "FDR-only flag",
                    "description": "Two-sided FDR channel hit without SPC 99.8% channel support.",
                },
                {
                    "label": "Robust lower-tail alert",
                    "description": (
                        "Robust lower-tail primary alert (99.8% lower breach + FDR support + material effect)."
                    ),
                },
                {
                    "label": "Robust upper-tail alert",
                    "description": (
                        "Robust upper-tail primary alert (99.8% upper breach + FDR support + material effect)."
                    ),
                },
                {
                    "label": "Low-power windows",
                    "description": "Alert-eligible windows excluded from inferential claims by low support.",
                },
                {
                    "label": "Global expected rate",
                    "description": "Global expected pro share as a function of support.",
                },
                {
                    "label": "Global 95% lower / Global 95% upper",
                    "description": "Global 95% control envelope.",
                },
                {
                    "label": "Global 99.8% lower / Global 99.8% upper",
                    "description": "Global 99.8% extreme-tail control envelope.",
                },
                {
                    "label": "Y-axis scaling",
                    "description": (
                        "The pro-rate axis uses a tail-stretch transform so dense extreme-tail "
                        "regions remain readable instead of collapsing at chart boundaries."
                    ),
                },
            ],
        },
        "off_hours_primary_residual_timeline": timebar(
            summary=(
                "Primary-baseline residual timeline for inferentially tested off-hours "
                "windows with SPC/FDR channel markers."
            ),
            primary_label="Primary z-score",
            primary_desc=(
                "Standardized residual of observed pro count versus the primary expected "
                "pro-rate baseline (model/day-adjusted)."
            ),
            include_wilson=False,
            include_low_power=True,
            volume_label="Known Pro+Con count",
            volume_desc="Known pro/con records supporting each bucket.",
            flagged_label="Robust lower-tail alert / Robust upper-tail alert",
            flagged_desc=(
                "Alert-eligible windows meeting robust-primary criteria "
                "(99.8% control-limit breach + tail-consistent FDR + material effect size)."
            ),
            extra=[
                {
                    "label": "z score day",
                    "description": (
                        "Day-adjusted standardized residual shown as comparator context."
                    ),
                },
                {
                    "label": "z ref zero",
                    "description": "Zero-residual reference line.",
                },
                {
                    "label": "z ref pos3 / z ref neg3",
                    "description": "Reference lines at +/-3 sigma.",
                },
                {
                    "label": "SPC-only flag",
                    "description": (
                        "Window passed the SPC 99.8% control-limit channel but not the FDR channel."
                    ),
                },
                {
                    "label": "FDR-only flag",
                    "description": (
                        "Window passed the two-sided FDR channel but not the SPC 99.8% channel."
                    ),
                },
                {
                    "label": "Robust alert run span",
                    "description": "Shaded x-range for contiguous robust-alert runs.",
                },
            ],
        ),
        "off_hours_primary_flag_channels": {
            "summary": (
                "Channelized flag accounting for tested off-hours windows in the primary bucket."
            ),
            "items": [
                {
                    "label": "Column",
                    "description": (
                        "Each column is a count of tested windows for one channel label on the x-axis."
                    ),
                },
                {
                    "label": "Channel meaning",
                    "description": (
                        "Tested off-hours windows = denominator; Primary 99.8% breach = SPC extreme tail; "
                        "Primary two-sided FDR-significant = multiplicity-adjusted test hits; "
                        "Any primary flag channel = SPC OR FDR; Both primary flag channels = SPC AND FDR; "
                        "Robust primary alerts = AND criteria plus material effect-size gate."
                    ),
                },
                {
                    "label": "Reading order",
                    "description": (
                        "Columns are ordered from broad denominator context to stricter overlap criteria."
                    ),
                },
            ],
        },
        "overview_position_volume_by_bucket": {
            "summary": "Stacked pro/con/other volume by time bucket.",
            "items": [
                {
                    "label": "Stacked columns",
                    "description": (
                        "Each bucket stacks Pro, Con, and Other testimony counts so composition "
                        "and total support are visible at once."
                    ),
                },
                {
                    "label": "Pro-share lines",
                    "description": (
                        "Solid line is observed Pro share in each bucket; dashed line at 50% "
                        "is the neutral split reference to highlight when sentiment is majority "
                        "Pro versus majority Con."
                    ),
                },
                {
                    "label": "Axes",
                    "description": (
                        "X-axis is bucket timestamp in report timezone; Y-axis is submission count."
                    ),
                },
            ],
        },
        "off_hours_date_hour_pro_heatmap": {
            "summary": "Date x hour heatmap for testimony position composition.",
            "items": [
                {
                    "label": "Cell color",
                    "description": (
                        "Pro share for that date/hour cell; low-power cells are marked in tooltip."
                    ),
                },
                {
                    "label": "Axes",
                    "description": (
                        "X-axis is hour of day; Y-axis is calendar date in chronological "
                        "top-down order (earliest at top)."
                    ),
                },
            ],
        },
        "off_hours_date_hour_primary_residual_heatmap": {
            "summary": (
                "Date x hour heatmap for primary-baseline standardized residuals "
                "across the full 24-hour timeline."
            ),
            "items": [
                {
                    "label": "Cell color",
                    "description": (
                        "Average support-window primary z-score for that date/hour cell "
                        "(blue = below baseline, warm = above baseline)."
                    ),
                },
                {
                    "label": "Support",
                    "description": (
                        "Tooltips show inferential tested-window counts and robust-alert counts so "
                        "isolated low-support cells are not over-weighted."
                    ),
                },
                {
                    "label": "Off-hours emphasis",
                    "description": (
                        "Off-hours hours are highlighted on the X-axis label to preserve "
                        "off-hours focus while retaining all-hour context."
                    ),
                },
                {
                    "label": "Axes",
                    "description": (
                        "X-axis is hour of day; Y-axis is calendar date in chronological "
                        "top-down order (earliest at top)."
                    ),
                },
            ],
        },
        "off_hours_date_hour_volume_heatmap": {
            "summary": "Date x hour heatmap for submission volume.",
            "items": [
                {"label": "Cell color", "description": "Submission count for that date/hour cell."},
                {
                    "label": "Axes",
                    "description": (
                        "X-axis is hour of day; Y-axis is calendar date in chronological "
                        "top-down order (earliest at top)."
                    ),
                },
            ],
        },
        "duplicates_exact_bucket_concentration": timebar(
            summary=(
                "Observed versus expected duplicated-anywhere presence over time "
                "(rows or distinct names)."
            ),
            primary_label="Observed duplicated-anywhere count",
            primary_desc=(
                "Line shows the selected unit count in each bucket for names that are "
                "duplicated anywhere in the timeline."
            ),
            include_low_power=False,
            include_wilson=False,
            volume_label="Rows",
            volume_desc="Total rows in each bucket.",
            extra=[
                {
                    "label": "Expected duplicated-anywhere count",
                    "description": (
                        "Volume-share expectation for the selected unit "
                        "(bucket rows * global duplicated-anywhere share)."
                    ),
                },
            ],
        ),
        "duplicates_exact_metric_diagnostics": {
            "summary": "Observed versus expected diagnostics across collision metrics.",
            "items": [
                {
                    "label": "Metric columns",
                    "description": (
                        "pairs = same-name unordered pairs; excess_rows = n - unique names; "
                        "repeated_group_rows = rows in names appearing >=2 times."
                    ),
                },
                {
                    "label": "Observed",
                    "description": (
                        "Bar height is the observed value for each metric in the selected duplicate scope."
                    ),
                },
                {
                    "label": "Expected and quantiles",
                    "description": (
                        "Use tooltip/table columns `expected`, `expected_p05`, `expected_p50`, and "
                        "`expected_p95` to compare where observed lands under the baseline."
                    ),
                },
                {
                    "label": "Significance columns",
                    "description": (
                        "`z_score` and `p_value` indicate standardized effect size and tail probability "
                        "for each metric; interpret with `n_used`/`N_used` support context."
                    ),
                },
            ],
        },
        "duplicates_exact_per_name_anomalies": {
            "summary": "Per-name duplicate counts with stacked Pro/Con bars.",
            "items": [
                {
                    "label": "Series",
                    "description": (
                        "Each name has stacked Pro and Con duplicate counts in one bar."
                    ),
                },
                {
                    "label": "Pagination",
                    "description": (
                        "Names are paginated 10 at a time up to 10 pages (top 100 names by "
                        "duplicate sign-ins)."
                    ),
                },
                {
                    "label": "Significance context",
                    "description": (
                        "Rows retain p/q columns in tooltips/tables; lower q-values indicate "
                        "stronger excess-versus-baseline evidence."
                    ),
                },
                {
                    "label": "X-axis",
                    "description": "Canonical/display names sorted by significance then side count.",
                },
            ],
        },
        "duplicates_exact_top_name_timing_exact": {
            "summary": (
                "Top duplicate names shown as time points sized by per-position submission rows."
            ),
            "items": [
                {
                    "label": "Point",
                    "description": (
                        "Each point is one active-bucket occurrence for a ranked top "
                        "name-position pair (x = bucket time, y = name). Colors encode Pro/Con/Other "
                        "and point size scales submission rows for that position in the bucket. "
                        "Names are eligible only when they have at least one duplicate in the hearing."
                    ),
                },
                {
                    "label": "Y-axis order",
                    "description": (
                        "Names are ranked by total sign-ins among duplicate-eligible names "
                        "(rank 1 = highest) and "
                        "paginated 10 at a time up to the top 200 names."
                    ),
                },
                {
                    "label": "Tooltip context",
                    "description": (
                        "Tooltips include match definition, rank, bucket span, Pro/Con split, "
                        "bucket submission rows, and total sign-ins for the name."
                    ),
                },
            ],
        },
        "duplicates_exact_null_distribution": {
            "summary": "Monte Carlo null distribution for duplicate burden metrics.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Simulated duplicate burden metric under the configured baseline.",
                },
                {"label": "X-axis", "description": "Simulation iteration."},
            ],
        },
        "duplicates_exact_swing_impact": {
            "summary": "Sensitivity scenarios for effective Pro/Con counts.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Effective pro share under each collision-adjustment scenario.",
                },
                {"label": "X-axis", "description": "Scenario (raw, strict dedupe, excess-adjusted)."},
            ],
        },
        "sortedness_bucket_ratio": timebar(
            summary="Ordering behavior across time buckets.",
            primary_label="Alphabetical indicator",
            primary_desc="Line values near 1 indicate alphabetical ordering for bucket windows.",
            include_low_power=False,
            include_wilson=False,
            volume_label="Records",
            volume_desc="Bar height is record count in each bucket.",
        ),
        "sortedness_bucket_summary": {
            "summary": "Sortedness summary by bucket size.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Alphabetical ordering ratio for each bucket size.",
                },
                {"label": "X-axis", "description": "Bucket size in minutes."},
            ],
        },
        "sortedness_kendall_tau_summary": {
            "summary": "Kendall tau ordering strength by bucket size.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Average absolute Kendall tau for each bucket size.",
                },
                {"label": "X-axis", "description": "Bucket size in minutes."},
            ],
        },
        "sortedness_minute_spikes": {
            "summary": "Minute-level ordering spikes.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Records seen in each minute-level ordering sample.",
                },
                {"label": "X-axis", "description": "Minute bucket timestamp."},
            ],
        },
        "rare_names_unique_ratio": timebar(
            summary="Name uniqueness over time.",
            primary_label="Unique ratio",
            primary_desc="Share of submissions with distinct canonical names per bucket.",
            include_low_power=True,
            include_wilson=False,
            extra=[
                {
                    "label": "Threshold unique ratio",
                    "description": "Reference threshold used for unique-ratio anomaly signaling.",
                }
            ],
        ),
        "rare_names_weird_scores": {
            "summary": "Highest weirdness-score names.",
            "items": [
                {
                    "label": "Bar height",
                    "description": (
                        "Weirdness score of sampled names; "
                        "higher indicates atypical string shape."
                    ),
                },
                {"label": "X-axis", "description": "Sample names sorted by weirdness."},
            ],
        },
        "rare_names_singletons": timebar(
            summary="Singleton name composition over time.",
            primary_label="Con count",
            primary_desc="Line shows con-side count among singleton records.",
            include_low_power=False,
            include_wilson=False,
            volume_label="Pro count",
            volume_desc="Bars show pro-side count among singleton records.",
        ),
        "rare_names_rarity_timeline": timebar(
            summary="Rarity-score timeline.",
            primary_label="Rarity median",
            primary_desc="Median rarity score in each bucket.",
            include_low_power=True,
            include_wilson=False,
            extra=[
                {
                    "label": "Rarity p95",
                    "description": "95th percentile rarity score to show tail behavior.",
                }
            ],
        ),
        "org_anomalies_blank_rate": timebar(
            summary="Blank organization-rate trend with position splits.",
            primary_label="Blank org rate",
            primary_desc="Overall blank/null organization share per bucket.",
            include_wilson=True,
            extra=[
                {
                    "label": "Pro blank org rate",
                    "description": "Blank-org share among pro records.",
                },
                {
                    "label": "Con blank org rate",
                    "description": "Blank-org share among con records.",
                },
            ],
        ),
        "org_anomalies_position_rates": timebar(
            summary="Per-position blank-org rates by time bucket.",
            primary_label="Blank org rate",
            primary_desc="Position-specific blank organization share.",
            include_wilson=True,
        ),
        "org_anomalies_bursts": {
            "summary": "Organization burst concentration.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Burst count for organization-related minute windows.",
                },
                {"label": "X-axis", "description": "Minute bucket of organization burst sample."},
            ],
        },
        "org_anomalies_top_orgs": {
            "summary": "Most common organization values.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Total records linked to each normalized organization value.",
                },
                {"label": "X-axis", "description": "Organization value labels."},
            ],
        },
        "voter_registry_match_rates": timebar(
            summary="Conservative voter-linkage trend with matched-rate focus.",
            primary_label="Matched rate",
            primary_desc=("Share of rows classified as matched under conservative primary linkage."),
            include_wilson=True,
            extra=[
                {
                    "label": "Pro match rate",
                    "description": "Matched-rate trajectory for Pro rows in each bucket.",
                },
                {
                    "label": "Con match rate",
                    "description": "Matched-rate trajectory for Con rows in each bucket.",
                },
                {
                    "label": "Expected global match rate",
                    "description": (
                        "Hearing-wide expected matched rate under the global linkage baseline."
                    ),
                },
                {
                    "label": "Global control low 95",
                    "description": "Lower 95% global control reference for matched rate.",
                },
                {
                    "label": "Global control high 95",
                    "description": "Upper 95% global control reference for matched rate.",
                },
                {
                    "label": "Robust lower-tail alert",
                    "description": (
                        "Buckets with materially low matched rate after low-power filtering."
                    ),
                },
                {
                    "label": "Robust upper-tail alert",
                    "description": (
                        "Buckets with materially high matched rate after low-power filtering."
                    ),
                },
            ],
        ),
        "voter_registry_linkage_by_position_rows": {
            "summary": "Unmatched-rate profile by position (row-level unit).",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Unmatched rate for each position using row-level counts.",
                },
                {"label": "X-axis", "description": "Normalized position label."},
            ],
        },
        "voter_registry_linkage_by_position_unique": {
            "summary": "Unmatched-rate profile by position (unique-name unit).",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Unmatched rate for each dominant position among unique names.",
                },
                {"label": "X-axis", "description": "Dominant position label for unique names."},
            ],
        },
        "voter_registry_unmatched_names": {
            "summary": (
                "Top unmatched names by row count (chart keeps top 50 and pages through up to 10 pages; "
                "table preview shows first 50 rows)."
            ),
            "items": [
                {
                    "label": "Bar height",
                    "description": "Count of unmatched rows for each display name.",
                },
                {"label": "X-axis", "description": "Display names for unmatched rows."},
            ],
        },
        "voter_registry_pairwise_tests": {
            "summary": "Pairwise unmatched-rate tests across positions.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Unmatched-rate difference between compared position pairs.",
                },
                {
                    "label": "X-axis",
                    "description": "Pair label (left vs right) by inference unit.",
                },
            ],
        },
        "voter_registry_sensitivity_modes": {
            "summary": "Conservative, balanced, and broad linkage sensitivity panel.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Unmatched rate under each linkage mode.",
                },
                {"label": "X-axis", "description": "Linkage mode."},
            ],
        },
        "periodicity_clockface": {
            "summary": "Clock-face minute concentration.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Observed event count at each minute-of-hour bin.",
                },
                {"label": "X-axis", "description": "Minute of hour (0-59)."},
            ],
        },
        "periodicity_autocorr": {
            "summary": "Autocorrelation by lag.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Autocorrelation coefficient at each lag in minutes.",
                },
                {"label": "X-axis", "description": "Lag length in minutes."},
            ],
        },
        "periodicity_spectrum": {
            "summary": "Top spectral periods.",
            "items": [
                {"label": "Bar height", "description": "Spectral power for each candidate period."},
                {"label": "X-axis", "description": "Detected period in minutes."},
            ],
        },
        "periodicity_rolling_fano": {
            "summary": "Rolling Fano overdispersion by window size.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Median rolling Fano factor for each window size.",
                },
                {"label": "X-axis", "description": "Rolling window size in minutes."},
            ],
        },
        "multivariate_score_timeline": timebar(
            summary="Multivariate anomaly score and support over time.",
            primary_label="Anomaly score",
            primary_desc="Combined feature-space anomaly score for each bucket.",
            include_wilson=False,
            include_low_power=True,
            extra=[
                {
                    "label": "Anomaly score percentile",
                    "description": "Percentile rank of anomaly score within this run.",
                }
            ],
        ),
        "multivariate_top_buckets": {
            "summary": "Top anomaly buckets (scatter).",
            "items": [
                {
                    "label": "Point position",
                    "description": "X-axis is bucket volume; Y-axis is anomaly score.",
                },
                {
                    "label": "Point color",
                    "description": "Color reflects anomaly-score percentile rank.",
                },
                {"label": "Point size", "description": "Bubble size scales with bucket volume."},
            ],
        },
        "multivariate_feature_projection": {
            "summary": "Feature projection scatter.",
            "items": [
                {
                    "label": "Point position",
                    "description": "X-axis is log volume; Y-axis is pro rate.",
                },
                {"label": "Point color", "description": "Color shows anomaly score intensity."},
                {"label": "Point size", "description": "Bubble size scales with bucket volume."},
            ],
        },
        "composite_score_timeline": timebar(
            summary="Composite risk score over time.",
            primary_label="Composite score",
            primary_desc="Aggregate score from multi-detector evidence overlap.",
            include_wilson=False,
            include_low_power=True,
        ),
        "composite_evidence_flags": {
            "summary": "Evidence-flag composition.",
            "items": [
                {
                    "label": "Bar height",
                    "description": "Count of windows containing each detector flag.",
                },
                {"label": "X-axis", "description": "Detector evidence flag token."},
            ],
        },
        "composite_high_priority": {
            "summary": "Highest-priority composite windows.",
            "items": [
                {"label": "Bar height", "description": "Composite score for top-ranked windows."},
                {"label": "X-axis", "description": "Window timestamp bucket."},
            ],
        },
    }
    return docs
