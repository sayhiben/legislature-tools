from __future__ import annotations

from typing import Any

from testifier_audit.report.rendering.constants import _SCATTER_CHART_IDS

def _detailed_what_to_look_for_by_analysis() -> dict[str, list[str]]:
    return {
        "bursts": [
            (
                "Prioritize merged burst windows that last multiple minutes; "
                "single-minute spikes can be routine campaign timing noise."
            ),
            (
                "Use excess-count and position-impact rows to see whether Pro, Con, "
                "or both sides drive the burst and by how many submissions."
            ),
            (
                "When duration and excess remain elevated across adjacent windows, "
                "treat the burst as sustained and correlate with other analyses."
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
                "dates are stronger than a single-night dip; corroborate with burst "
                "and duplicate detectors before escalation."
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
        "org_anomalies": [
            (
                "Start with sustained changes in overall blank-organization rate "
                "across adjacent windows instead of isolated single-bucket spikes."
            ),
            (
                "Compare Pro and Con blank-organization rates and prioritize gaps "
                "that persist across multiple neighboring buckets."
            ),
            (
                "Treat low-support windows as descriptive context; use higher-support "
                "periods for stronger conclusions about rate differences."
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
    }


def _analysis_help_hints() -> dict[str, dict[str, str]]:
    return {
        "bursts": {
            "primary_metric": "burst duration, excess volume, and position impact counts",
            "momentary_high": (
                "legitimate synchronized outreach or one-off reminder cascades"
            ),
            "momentary_low": (
                "short-lived spikes with little excess and no clear position dominance"
            ),
            "extended_high": (
                "multi-window burst periods with persistent excess and stable position "
                "impact direction"
            ),
            "extended_low": (
                "baseline activity without concentrated burst windows"
            ),
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
        "org_anomalies": {
            "primary_metric": "blank organization rate overall and by position",
            "momentary_high": "a short-lived blank-rate bump in a sparse window",
            "momentary_low": "a temporary completeness improvement in a small bucket",
            "extended_high": (
                "persistent blank-rate elevation that can reduce organization attribution quality"
            ),
            "extended_low": "sustained lower blank rates with more complete organization capture",
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
        "bursts_hero_timeline": timebar(
            summary="Detected burst windows with timing, intensity, and dominant position impact.",
            primary_label="Rate ratio",
            primary_desc="Observed-to-expected count ratio per tested window.",
            include_wilson=False,
            include_low_power=True,
            volume_label="Observed count",
            volume_desc="Bars show observed submissions in each burst window.",
        ),
        "bursts_significance_by_window": {
            "summary": "Burst duration and excess volume over time.",
            "items": [
                {
                    "label": "Duration",
                    "description": (
                        "Bar height shows merged burst duration in minutes for each burst period."
                    ),
                },
                {
                    "label": "Excess submissions",
                    "description": "Line values show observed minus expected submissions.",
                },
                {"label": "X-axis", "description": "Burst start time."},
            ],
        },
        "bursts_composition_shift": timebar(
            summary="Position-specific burst impact over time.",
            primary_label="Net position impact",
            primary_desc="Pro impact minus Con impact count in each burst window.",
            include_wilson=False,
            include_low_power=True,
            volume_label="Dominant position impact",
            volume_desc="Absolute count impact for the dominant affected position.",
            extra=[
                {
                    "label": "Pro impact count",
                    "description": "Observed Pro count minus expected Pro count in the burst window.",
                },
                {
                    "label": "Con impact count",
                    "description": "Observed Con count minus expected Con count in the burst window.",
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
                    "label": "expected pro rate global",
                    "description": "Hearing-level expected pro share comparator.",
                },
                {
                    "label": "Inferentially tested windows",
                    "description": (
                        "Alert-eligible windows that are not low-power; SPC/FDR channels are evaluated "
                        "on this tested subset."
                    ),
                },
                {
                    "label": "SPC-only flag",
                    "description": (
                        "SPC = Statistical Process Control. Window crossed the primary 99.8% control "
                        "channel but did not also pass the FDR channel."
                    ),
                },
                {
                    "label": "FDR-only flag",
                    "description": (
                        "FDR = False Discovery Rate control on multiple tested windows. Window passed "
                        "the two-sided FDR channel but not the SPC 99.8% channel."
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
                        "Off-hours windows that are alert-eligible and not low-power; only these "
                        "feed SPC/FDR inferential channels."
                    ),
                },
                {
                    "label": "SPC-only flag",
                    "description": (
                        "SPC = Statistical Process Control. Window hit the primary 99.8% channel "
                        "without two-sided FDR support."
                    ),
                },
                {
                    "label": "FDR-only flag",
                    "description": (
                        "FDR = False Discovery Rate control. Window hit the two-sided FDR channel "
                        "without SPC 99.8% channel support."
                    ),
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
                        "Solid line is observed Pro share in each bucket; dashed horizontal "
                        "line at 50% is the neutral split reference; dashed Wilson low/high "
                        "bounds show 95% uncertainty for the Pro-share estimate."
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
            volume_label="Positioned rows",
            volume_desc="Stacked Pro/Con row counts in each bucket (desaturated context bars).",
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
        "duplicates_exact_position_bucket_deviance": timebar(
            summary=(
                "Position-level observed versus baseline duplicate counts over time, split by "
                "position and metric."
            ),
            primary_label="Observed duplicate count",
            primary_desc=(
                "Solid lines show observed duplicate count in each position bucket for the selected "
                "position/metric series."
            ),
            include_low_power=True,
            include_wilson=False,
            volume_label="Rows in position bucket",
            volume_desc="Bar height is total rows available in each position bucket.",
            extra=[
                {
                    "label": "<Position> baseline",
                    "description": (
                        "Dashed line for expected duplicate count under the position-aware baseline "
                        "for the same position/metric series."
                    ),
                },
                {
                    "label": "<Position> low-power",
                    "description": (
                        "Triangle markers on observed points where support is too sparse for robust "
                        "inference."
                    ),
                },
            ],
        ),
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
        "org_anomalies_blank_rate": timebar(
            summary="Blank organization-rate trend with position splits.",
            primary_label="Blank org rate",
            primary_desc="Overall blank/null organization share per bucket.",
            include_wilson=True,
            volume_label="Positioned rows",
            volume_desc="Stacked Pro/Con row counts in each bucket (desaturated context bars).",
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
            summary="Per-position blank-org rates (Pro/Con/Other) by time bucket.",
            primary_label="Pro blank org rate",
            primary_desc="Blank organization share among Pro records in each bucket.",
            include_wilson=True,
            volume_label="Positioned rows",
            volume_desc="Stacked Pro/Con row counts in each bucket (desaturated context bars).",
            extra=[
                {
                    "label": "Con blank org rate",
                    "description": "Blank organization share among Con records.",
                },
                {
                    "label": "Other blank org rate",
                    "description": "Blank organization share among non-Pro/Con records.",
                },
            ],
        ),
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
                "Top unmatched names by row count (chart keeps top 100 and pages 10 names per page, "
                "up to 10 pages; table preview shows first 50 rows)."
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
    }
    return docs
