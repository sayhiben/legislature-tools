# Detecting Anomalous Participation Patterns in Washington Legislative Sign‑In Data

## Project goals, threat model, and operational constraints

Your project is best framed as an **anomaly discovery and triage system**, not a “bot detector.” In unsupervised anomaly detection and fraud detection, the central difficulty is that “abnormal” does not uniquely imply “malicious,” and true malicious events are typically rare and adaptive. citeturn0search1turn3search2 This means the tool should aim to (a) quantify *how unusual* a pattern is relative to comparable hearings, and (b) present *evidence bundles* that help investigators decide whether a pattern is consistent with ordinary mobilization, data quality artifacts, or potential manipulation. citeturn0search1turn3search2

The Washington legislative “Committee Sign In” system supports multiple public participation actions (register to testify, note a position for the record, submit written testimony), each with materially different timing windows and user incentives. citeturn8view1turn14view0 In particular, official guidance makes two constraints crucial for your modeling:

- **Sign‑in is allowed as soon as a bill is placed on an agenda and closes one hour before the meeting starts**, which can create multi‑day registration windows and deadline effects. citeturn8view0turn8view1turn14view0  
- **Written testimony can be submitted up to 24 hours after the meeting start**, which creates an additional “after-the-start” participation band that should not be lumped into the same temporal model as pre‑meeting sign‑ins. citeturn8view0turn8view1turn14view0  

The system also states that collected information (registration, positions, comments) is part of the legislative record and treated as public information (subject to applicable protections). citeturn14view0turn9search5 This matters because public systems attract both genuine transparency-driven participation and strategic attempts to influence perceived public sentiment; your analytics should explicitly anticipate both. citeturn3search2turn3search3

In your provided sample export (Export.csv), you have the fields **Count**, **Name**, **Organization**, **Position**, and **Time Signed In** at **minute resolution**. Minute resolution is workable for population-level burst and scheduling diagnostics, but it prevents certain classic “human vs bot” timing signatures (sub-second regularity, keystroke-delay distributions, etc.). In anomaly detection terms, you are primarily looking for **collective anomalies** (organized patterns across many rows) and **contextual anomalies** (records that are unusual given time-of-day / deadline / committee context), more than isolated single-row outliers. citeturn0search1

## Critical review of current analyses and what likely needs adjustment

This section answers: *What about our current analyses seems questionable, incorrect, or in need of adjustment to improve its accuracy or applicability?*

### Off-hours and “pro‑rated shifts” are easy to misinterpret without hearing-relative time

Because sign‑in can open as soon as an agenda item is posted and closes one hour before the hearing, “off-hours” spikes (late night, early morning) can be completely consistent with normal civic behavior: people sign in after work, after advocacy alerts, or shortly before a deadline—especially when a hearing is the next morning. citeturn8view0turn8view1turn14view0  
If your current dashboard treats “off-hours activity” as inherently suspicious, it risks systematically flagging routine deadline behavior as anomalous.

**Adjustment:** redefine time features **relative to the hearing start time** and **relative to the sign‑in opening time** (when the item appears on the agenda), rather than absolute clock time alone. This aligns your features to the actual process constraints and makes cross-hearing comparisons meaningful. citeturn8view0turn8view1

### Voter-roll “miss rate” is not intrinsically a bot signal (and can be structurally biased)

The Legislature explicitly states that **anyone can testify** and that people can participate in multiple ways. citeturn8view0 A “miss” against a registered voter list can therefore occur for many non-nefarious reasons: non-registered residents, new movers, non-residents with a stake (industry groups, national advocacy), name variants, and matching error.

**Adjustment:** if voter-roll linkage remains a goal, treat it as **a probabilistic linkage problem with uncertainty**, not a binary match/miss indicator. Classic record linkage work (Fellegi–Sunter) and practical government matching guidance emphasize that linkage requires modeling match likelihood, not exact equality. citeturn1search4turn1search5  
Additionally, linkage-quality metrics (false match / false non-match sensitivity) should be visible in the dashboard to prevent overconfident interpretation. citeturn1search4turn1search5

### Minute resolution invalidates some “inter-arrival” and regularity metrics unless redefined

With minute-resolution timestamps, many consecutive rows can share the same timestamp. In your sample export, that is empirically true (large same-minute clusters). Metrics that assume continuous-time event arrival (or second-level precision) can become artifacts of the rounding and should not be interpreted as behavioral regularity.

**Adjustment:** compute regularity on a **minute-indexed count series** (counts per minute) and use methods designed for binned event counts, including dispersion testing, changepoint analysis, and burst detection over discrete time. citeturn0search1turn5search4turn0search2

### Data duplication and missingness look like first-order issues you should surface and correct

In the provided export, **Organization is missing for the vast majority of rows** and there are **exact duplicate records** (same Name/Organization/Position/Time Signed In) as well as **large within-minute repetitions of the same name**. These may reflect (a) real repeated sign-ins, (b) data extraction duplication, or (c) upstream system quirks. Regardless, they can distort all higher-level anomaly metrics if not handled explicitly.

**Adjustment:** your pipeline should implement and dashboard should show:
- exact-duplicate rates (by full row key),
- within-minute repetition counts,
- missingness by field,
- analyses both **with and without deduplication**, because each answers a different question (“what is in the public export” vs “how many unique sign-in attempts are plausible”).  
Fraud/anomaly work commonly fails when data quality artifacts are treated as behavioral signals. citeturn3search2turn0search1

### Statistical validity issues: multiple comparisons and “anomaly score inflation”

As the dashboard grows, you’ll run many tests/metrics per hearing. Without correction, you will inevitably generate false positives. The false discovery rate (FDR) approach introduced by **entity["people","Yoav Benjamini","statistician fdr"]** and **entity["people","Yosef Hochberg","statistician multiple testing"]** is a widely used, practical way to manage expected false discoveries in multiple testing. citeturn1search10turn1search6

**Adjustment:** design the system so that “red flags” are either (a) FDR-managed statistical findings, or (b) clearly labeled heuristic indicators, and never mix them without a clear legend and methodology section. citeturn1search10turn0search1

## Research-backed methodologies that map well to legislative sign-in data

This section answers: *What often-performed analyses, charting techniques, or other statistical work is typically employed to discover the anomalous patterns we’re looking for—and how can we use those to improve the work?*

### Baseline modeling for binned event counts

A common starting point is modeling sign-ins as a count process over time bins (minutes). A naive Poisson model assumes equal mean and variance; real-world human activity is usually overdispersed due to diurnal cycles, deadlines, and bursty attention patterns. citeturn4search4turn3search5  
Two practical upgrades are:

- **Non-homogeneous (time-varying) rate models**, which incorporate known rhythms (time of day, day of week, deadline proximity). Work on human activity shows that apparent “heavy tails” can emerge from circadian/weekly cycles plus cascades of activity. citeturn3search5turn3search0  
- **Negative binomial or other overdispersion-aware count models** when variability exceeds Poisson assumptions. citeturn4search4turn4search5turn4search12  

In your context, baseline modeling is what turns “that spike looks big” into “that spike is in the 99.5th percentile compared to hearings with similar size, committee, and time-to-deadline.”

### Burst detection for coordinated waves

Burst detection is a classic technique for identifying intervals where the event rate rises sharply above baseline. The burst detection algorithm proposed by **entity["people","Jon Kleinberg","computer scientist burst detection"]** was designed to find bursty structure in document/event streams and is directly relevant to sign-in surges. citeturn0search2turn0search6  
For your tool, burst detection is valuable because it (a) localizes suspicious intervals, and (b) provides a principled way to compare “bursty-ness” across hearings of different total sizes. citeturn0search2turn0search1

### Changepoint detection for regime shifts

Changepoint detection looks for abrupt shifts in the underlying data-generating process (rate, variance, or composition). Surveys of offline changepoint detection emphasize decomposing methods into a cost function, a search strategy, and constraints on the number of changes—useful guidance when you operationalize this in production. citeturn5search4turn0search3  
In legislative sign-in data, changepoints are especially useful for detecting:
- “new mobilization wave begins,”
- “deadline-driven ramp-up starts,”
- “stance composition flips” (Pro vs Con share changes materially).

### Statistical process control for abnormal shifts

Statistical process control (SPC) techniques—especially CUSUM—are designed to detect small persistent shifts in a monitored process. CUSUM is explicitly described as a method for change detection and is often used to detect drift or aberrance in sequential streams. citeturn1search15turn1search7  
Translating to your setting: if you define a baseline expected minute-rate curve (by hour-of-day and proximity to cut-off), CUSUM or related methods can flag sustained deviations, not just single spikes. citeturn1search15turn5search4

### Robust outlier detection in feature space

Once you compute a per-hearing feature vector (peakiness, concentration, duplication rates, stance volatility, etc.), you can apply unsupervised outlier detection methods that are commonly used when labels are unavailable:

- Isolation Forest isolates anomalies via random partitioning and is widely used for unsupervised anomaly scoring. citeturn6search8turn6search16  
- Local Outlier Factor (LOF) measures local density deviation, helpful if “normal” differs across subpopulations (e.g., committee types). citeturn6search6turn6search10  
- One-class support estimation methods support “learn normal boundary then flag outside” approaches often used when you have many normal examples and few confirmed anomalies. citeturn6search13turn6search9  

These help with global comparative analysis, but they must be paired with interpretability (feature contributions, drill-down evidence) to avoid opaque “risk scores.” citeturn0search1turn3search2

### Record linkage and near-duplicate clustering for names and organizations

Because many anomalies you care about involve repeated or synthetic identities, you need high-quality deduplication and similarity matching:

- The Fellegi–Sunter framework is foundational for probabilistic record linkage when you lack unique IDs. citeturn1search4turn1search0  
- Practical evaluations of string comparators (including Jaro–Winkler-style methods) show how to choose and validate name similarity metrics in administrative data contexts. citeturn1search5turn1search9  

For your domain, the key is to treat “same name string” and “same person” as different hypotheses, each with different error profiles—and quantify both.

image_group{"layout":"carousel","aspect_ratio":"16:9","query":["CUSUM control chart example","changepoint detection time series illustration","Kleinberg burst detection visualization","record linkage Fellegi Sunter diagram"],"num_per_query":1}

### Permutation tests and Monte Carlo calibration for hearing-specific significance

Because each hearing has its own size and time window, “textbook p-values” are often less useful than **hearing-conditioned resampling**. Randomization/permutation tests are a standard way to build empirical null distributions when assumptions (independence, stationarity) are questionable. citeturn7search2turn7search3  
For example: to test whether Pro/Con sign-ins are unusually clustered in a short time span, you can permute stance labels within the hearing (or within hour blocks) and compare the observed clustering statistic to the permuted distribution.

## Additional analyses that would materially strengthen detection power

This section answers: *What additional analyses should we perform?* It prioritizes analyses that (a) reduce false positives, (b) produce interpretable evidence, and (c) scale to many hearings.

### Hearing-relative time analysis and deadline effects

Add a first-class time axis: **minutes-to-cutoff** (cutoff = hearing start minus 60 minutes). The Legislature’s own guidance makes this the natural behavioral reference point. citeturn8view0turn8view1turn14view0  
For each hearing, compute:
- distribution of sign-ins by minutes-to-cutoff (and optionally minutes-since-open),
- “last-hour fraction,” “last-15-minutes fraction,” and “last-5-minutes fraction,”
- a ramp-rate metric (how quickly sign-ins accelerate as cutoff approaches),
- stance-specific deadline behavior (does one stance disproportionately arrive in the final window?).

These features often discriminate organic advocacy alerts (sharp ramp near deadline) from simplistic automation (uniform rate or mechanical periodicity throughout the open window). citeturn3search5turn3search2

### Concentration and manipulation-resistant volume metrics

Raw counts are easy to game. Add concentration measures that highlight whether a small set of repeated identities dominate:

- share of rows contributed by the top-k names,
- Gini/Lorenz-like concentration over name frequencies (even if you do not publicize identities),
- maximum same-minute repetition per name, and
- entropy of names/orgs within bursts (do bursts involve many unique identities or a repeated small set?).  

Fraud detection literature repeatedly emphasizes that useful detectors often leverage *distributional shape* and *collective structure* rather than single-record oddities. citeturn3search2turn0search1

### “Template” and synthetic-text indicators for Organization

In your sample export, Organization is frequently blank. That is not suspicious by itself; it is an expected behavior when fields are optional. What is more useful is detecting:
- repeated uncommon exact strings appearing in bursts,
- high rates of “placeholder-like” tokens (“N/A”, “none”, single letters) within a localized window,
- organizations that look like addresses or random strings, and whether they correlate with specific bursts.

Treat these as weak indicators that gain strength only when corroborated by timing and duplication anomalies. citeturn0search1turn3search2

### Stance dynamics beyond “overall Pro/Con ratio”

Overall stance share is a coarse measure. Add:
- **stance volatility over time** (rolling Pro share),
- changepoints in stance share, and
- burst-local stance imbalance (do bursts skew strongly to one side?).  

Because the platform explicitly supports “position noted for the legislative record,” stance-only participation may dominate and might behave differently from live testimony signups. citeturn14view0turn13view0

### Cross-hearing identity graphs

When you scale to many hearings, build a bipartite graph of (identity token) ↔ (bill/hearing). Then compute:
- unusually high-degree identities (appear in many unrelated hearings),
- tightly repeated cohorts (same set of names appears together repeatedly),
- stance consistency vs opportunism across topics.

Graph-level changepoint or community structure shifts can indicate organized campaigns. This is where your system becomes “global anomaly intelligence,” not just a per-list dashboard. citeturn0search1turn3search3

### Explicit model of “three explanations”

For each flagged phenomenon, classify it (even heuristically) into:
1) **Data quality artifact** (duplicates, extraction problems, missingness spikes),  
2) **Legitimate mobilization** (deadline ramps, advocacy alerts),  
3) **Potential manipulation / automation** (repeated identities in bursts, unnatural periodicity, excessive exact duplication).  

Fraud detection work stresses that operational value comes from placing flags into actionable categories, not just scoring. citeturn3search2turn0search1

## Dashboard and UX redesign recommendations

This section answers: *What is missing from our dashboard? UX strengths/weaknesses? Should the order change? How to handle noisy charts?*

### What the dashboard is currently good at

From the screenshot, the dashboard appears to already emphasize:
- an overall time-series of activity,
- distributional views (histograms / rank-like curves),
- heatmap-like density views,
- and at least one tabular drill-down.

That’s directionally aligned with a good “overview then drill-down” approach. The information-seeking mantra (“overview first, zoom and filter, then details on demand”) is a strong fit for investigative anomaly dashboards. citeturn2search13turn2search4

### Where it is currently weakest

The main weakness is likely **interpretability and calibration**: charts can show variation, but users still need a disciplined answer to “is this unusual *for this kind of hearing*?” A dashboard that lacks *comparative baselines* will force users into eyeballing, which increases false positives and reduces trust. citeturn0search1turn3search2

Second, the screenshot suggests **high-noise minute-level plots**. Human activity is naturally bursty and non-stationary; without smoothing, bin controls, or contextual overlays, users may over-index on spikes. citeturn3search0turn3search5

Third, the dashboard likely under-explains process constraints (when sign-in opens/closes, the one-hour cutoff, written-testimony window). Those constraints should be placed adjacent to time charts so users do not mistake deadline clusters for anomalies. citeturn8view0turn14view0turn8view1

### Reordering the page for investigative flow

A structure that usually works better for anomaly triage:

1) **Hearing context panel**: committee, bill, hearing start, sign-in open time, cutoff time, participation modes included. citeturn8view0turn8view1turn13view0  
2) **Data quality panel**: missingness, duplicate rates, rows included/excluded by dedup rules.  
3) **“What’s unusual?” summary**: percentile-based flags and short sentences explaining *why*. citeturn3search2turn0search1  
4) **Time evidence**: minute counts with smoothing + burst intervals + changepoints. citeturn0search2turn5search4turn1search15  
5) **Identity evidence**: duplicates, near-duplicates, cohort clusters. citeturn1search4turn1search5  
6) **Stance evidence**: stance share over time, stance-by-burst. citeturn14view0turn8view1  
7) **Details on demand**: sortable tables, export, audit trail.

This sequencing is essentially Shneiderman’s mantra operationalized for investigations. citeturn2search13turn2search4

### How to fix noisy charts without hiding signal

For each “too noisy” chart type, these are high-yield remedies:

- **Minute-level time series:** provide a bin-size toggle (1m / 5m / 15m / 60m) and default to a slightly aggregated view; overlay changepoints and burst intervals rather than relying on visual inspection. citeturn5search4turn0search2  
- **Spiky histograms / long tails:** offer log-scale on the y-axis and show cumulative distribution (CDF) as an alternative view.  
- **Heatmaps:** normalize by row/column (e.g., show within-day percent) to avoid size dominating pattern; also add a “difference from baseline” heatmap when aggregate norms exist.  
- **Rank-frequency curves:** add reference bands (median, 90th percentile) from the global corpus so users can interpret whether the curve is extreme.

These changes align with graphical perception research: users read position and length more accurately than area and clutter, and reducing overplotting improves quantitative judgment. citeturn2search1turn2search11

### Missing dashboard features that will increase trust

- **Baseline comparator for every major chart** (e.g., “Compared to hearings with 10k–50k sign-ins, this hearing is in the 98th percentile for within-minute duplication”).  
- **Methodology drawer**: a concise explanation of each metric, plus cautions.  
- **Auditability**: clear statement of deduplication rules, filters, and transformations used, because investigators will need reproducibility. citeturn0search1turn3search2  
- **Uncertainty display** when using probabilistic linkage (voter rolls, name linkage). citeturn1search4turn1search5  
- **Ethical guardrails**: labels like “statistical irregularity” rather than “fraud,” plus guidance that flags are investigatory leads. citeturn3search2turn0search1  

## Scaling to many testifier lists, global aggregation, and comparative analytics

This section answers: *How to aggregate globally? How to compare datasets meaningfully?*

### Build a per-hearing feature store with robust normalization

For each hearing/list, compute a feature vector in several families:

- **Volume & window:** total sign-ins, active minutes, open-window duration, cutoff proximity stats. citeturn8view0turn8view1  
- **Temporal structure:** peak minute count, fraction in top 1/5/15 minutes, burst count and burst strength, changepoint count, dispersion/overdispersion measures. citeturn0search2turn5search4turn4search4  
- **Identity structure:** exact-duplicate rate, near-duplicate cluster sizes, top-k concentration metrics. citeturn1search4turn1search5  
- **Stance structure:** overall stance shares, stance volatility, burst-local stance skew. citeturn14view0turn8view1  
- **Data quality:** missingness rates by field, parsing anomalies.

Then store global distributions of each feature by relevant strata: committee, chamber, bill topic, hearing time-of-day, and sign-in window duration. This is essential because “normal” differs by context. citeturn0search1turn3search5

### Global digestibility: percentile ranks and “difference from typical” panels

For users, raw features are less interpretable than percentiles. A practical global presentation:

- show each feature’s **percentile rank** within its peer group (“99th percentile for exact duplicates among hearings of similar size and window”),  
- show a small **“why flagged” sentence** that references the top-two contributing features,  
- provide a “compare to typical” overlay on key plots (median curve + shaded 10–90% band).

This is also where you can introduce an overall “irregularity index,” but it should be decomposable into its components (time vs identity vs data quality). citeturn0search1turn3search2

### Comparing two datasets/hearings

To compare hearing A vs hearing B meaningfully, do it at two levels:

1) **Distributional comparisons** (time-of-day distribution, minute-count distribution, stance-over-time series). Two-sample tests like KS can be used cautiously as screening tools, but effect sizes and visual overlays are more interpretable for end users. citeturn4search17turn4search13  
2) **Feature-space comparisons**: compare their feature vectors using robust distance metrics and show which features differ most (e.g., A has 5× higher within-minute duplication, but similar burst strength).

If you apply many pairwise tests, manage multiplicity with FDR control; otherwise you will end up “finding” differences everywhere. citeturn1search10turn1search6

### Calibration: empirical nulls and resampling

When you report “unusual,” prefer calibration by:
- **peer-group baselines** (global corpus percentiles), and
- **within-hearing permutation tests** (Monte Carlo), especially for stance clustering or name clustering within bursts. citeturn7search2turn7search3  

This makes anomaly claims more defensible than relying on fragile parametric assumptions in a non-stationary civic participation setting. citeturn3search5turn4search4

## Appendix with dashboard help text and anomaly interpretation guide

The screenshot text is too low-resolution to reliably read every label, but the visible layout strongly suggests a structure of: (a) top-line KPIs, (b) an overall time series, (c) heatmaps and distribution plots, and (d) one or more drill-down tables. The help text below is designed to map to those visible components and to the fields present in your export. Where your dashboard uses different names, you can keep the content and swap the label.

### Top summary KPIs

**Total sign-ins (rows)**  
Purpose: establishes scale; drives what “normal variability” should look like. In fraud/anomaly settings, scale affects false positive rates and expected extreme values. citeturn3search2turn0search1  
How to read: larger totals naturally create higher peaks per minute even without manipulation.  
Anomalous patterns: none by itself; interpret only relative to window length and baseline bins.  
Cross-check: compare to active minutes and hearing window duration. citeturn8view0turn8view1

**Unique names**  
Purpose: approximates participation breadth.  
How to read: “unique names / total rows” is a crude repetition ratio; it becomes meaningful only after dedup rules are explicit. citeturn1search4turn1search5  
Anomalous patterns: very low uniqueness can reflect mass repetition, but can also be export duplication; confirm with exact-duplicate table.  
Cross-check: exact-duplicate rate, within-minute repetitions.

**Organization completion rate**  
Purpose: shows how usable the org field is as a signal.  
How to read: low completion is common if org is optional; treat org signals as conditional on completion.  
Anomalous patterns: sudden shifts in completion rate within a burst can indicate templated or automated form-filling, but also could reflect a specific organization’s mobilization. citeturn0search1turn3search2  
Cross-check: burst windows + org-string repetition.

**Pro / Con / Other totals**  
Purpose: overall stance distribution.  
How to read: useful as context, but overall stance is often less diagnostic than stance *over time*.  
Anomalous patterns: extreme imbalance is common in contentious bills; do not treat as bot evidence without timing/identity corroboration. citeturn3search2turn0search1  
Cross-check: stance volatility and burst-local stance skew.

### Main time series: sign-ins per minute (or per chosen bin)

**Sign-ins over time (line/area plot)**  
Purpose: identifies bursts, ramps to deadline, and multi-wave mobilizations.  
How to read: interpret against known platform rules: sign-in can open days ahead and closes 1 hour before start, so expect ramps near cutoff and possibly after-start activity for written testimony if included. citeturn8view0turn8view1turn14view0  
What to look for:  
- “deadline wall”: sharp rise in final hour (often normal), citeturn8view0turn8view1  
- repeated identical spike shapes across multiple days (could be scheduled automation), citeturn0search1turn3search2  
- long sustained high-rate plateau at off-hours (more suspicious than a single spike, but still check context).  
To reduce noise: offer bin-size controls and overlay burst intervals/changepoints instead of relying on raw spikiness. citeturn5search4turn0search2  
How to increase confidence: cross-reference with within-minute repetition and name entropy inside flagged intervals.

### Heatmap-like density view

**Time-of-day × date (or hour × day) heatmap**  
Purpose: shows when participation clusters by clock time.  
How to read: a typical pattern may show daytime and evening activity; “off-hours” can be normal given multi-day windows. citeturn8view0turn3search5turn3search0  
Anomalous patterns:  
- strong, repeated vertical stripes at exactly the same clock-minute across days (suggests scheduled scripts),  
- bursts concentrated at unusual hours *without* a deadline explanation.  
De-noising: normalize each day to percentages; optionally show “difference from baseline” once you have many hearings.

### Distribution plots that often appear in anomaly dashboards

**Minute-count distribution (histogram of counts per minute)**  
Purpose: quantifies “peakiness” (how much activity concentrates into a small number of minutes).  
How to read: long right tail is common in mobilizations; interpret against overdispersion models. citeturn4search4turn3search5  
Anomalous patterns: an unusually sharp cutoff or “comb-like” distribution (counts frequently equal to a few repeated values) can indicate batching artifacts.  
Cross-check: burst detection intervals and changepoint results. citeturn0search2turn5search4

**Rank-frequency curve (e.g., names by frequency, organizations by frequency)**  
Purpose: detects concentration (few entities dominating).  
How to read: steep drop-off means most names appear once and few repeat; interpret only after dedup. citeturn1search4turn1search5  
Anomalous patterns: unusually heavy head (top names appearing extremely often) may reflect repeated sign-in or export duplication.  
De-noising: log-scale y-axis and annotate top-1/top-10 shares.

**Cumulative sign-ins (CDF over time)**  
Purpose: makes deadline effects obvious (slope changes).  
How to read: a sudden steepening near cutoff is expected; multiple steepening segments suggest multiple mobilization waves. citeturn8view0turn8view1  
Anomalous patterns: sustained linear growth at a constant slope for hours can be more consistent with automation than humans, but validate against hearing context. citeturn3search2turn0search1  
Cross-check: periodicity/regularity tests on binned counts.

### Stance-specific views

**Stacked/overlaid Pro vs Con over time**  
Purpose: shows who mobilizes when, not just how many.  
How to read: interpret with stance mode: many users choose “position noted” and may not testify live; stance waves can be driven by organizational alerts. citeturn14view0turn13view0  
Anomalous patterns: abrupt stance flips not aligned with known agenda/deadline changes can indicate coordinated counter-mobilization or manipulation; changepoint detection helps here. citeturn5search4turn0search3  
De-noising: plot rolling stance share (e.g., 30-minute window) with confidence bands from resampling. citeturn7search1turn7search2

### Drill-down tables

If your dashboard includes tables similar to what is typical for this domain, here is recommended help text per common table type and column.

**Raw records table** (likely columns: Count, Name, Organization, Position, Time Signed In)  
- **Count**: usually a row index from the export, not an analytical feature; warn users not to treat it as time-order unless verified.  
- **Name**: free-text identity token; may contain variants, nicknames, reordered names; should be analyzed with canonicalization and record linkage rather than exact matching alone. citeturn1search4turn1search5  
- **Organization**: optional and noisy; missingness should be displayed; interpret as self-reported affiliation, not verified membership.  
- **Position**: categorical stance (“Pro”, “Con”, “Other”); interpret in light of the system’s “position noted” pathway. citeturn14view0turn13view0  
- **Time Signed In**: minute-resolution timestamp; for behavior analysis, convert to (minutes-to-cutoff) and (minutes-since-open) where possible. citeturn8view0turn8view1

**Exact-duplicate groups table** (recommended columns: Name, Organization, Position, Time Signed In, Duplicate Count, % of Total)  
Purpose: separates “data artifact or repeat sign-in” from broader mobilization.  
How to read: high duplicate counts at the same minute are more suspicious than repeated appearances spread across days, but both require context.  
Anomalous pattern examples:  
- many exact duplicates with identical timestamps clustered in a burst window,  
- duplicates concentrated in off-hours without deadline explanation.  
Cross-check: whether deduplication materially changes the time series.

**Near-duplicate / record-linkage clusters table** (recommended columns: Canonical Name, Variants, Cluster Size, Similarity Score Range, Stance Consistency, Time Span)  
Purpose: captures attempts to evade exact matching (typos, nicknames) or messy data. citeturn1search4turn1search5  
What to look for: clusters that appear suddenly in a narrow window, high cluster size with many variants, or clusters with inconsistent stance (could indicate shared identity tokens).  
Caution: name similarity can create false merges for common names; show uncertainty (match probability), not just a cluster label. citeturn1search4turn1search5

**Burst interval table** (recommended columns: Start, End, Peak Rate, Total in Burst, % of Total, Pro Share in Burst, Top Repeated Names)  
Purpose: turns time-series anomalies into discrete investigation units.  
How to read: a burst is not inherently malicious; it is “elevated attention.” The question is whether the burst’s identity structure is broad (many unique names) or narrow (repetition), and whether timing aligns with cutoff. citeturn0search2turn3search5turn8view0  
Cross-check: whether changepoints occur at burst boundaries. citeturn5search4turn0search3

### “How to interpret flags” guidance users should see in-product

A short, user-facing rubric that reduces misinterpretation:

- **Single metric flags are weak**; require at least two corroborating indicators across different families (time + identity, or time + data quality, etc.). citeturn0search1turn3search2  
- **Deadline-aligned bursts are often normal** given sign-in timing rules; treat as suspicious only if identity repetition inside the burst is extreme or mechanically patterned. citeturn8view0turn8view1turn0search2  
- **Label outputs as “irregularity” not “fraud.”** Fraud detection literature warns that operational harm comes from overconfident classification without ground truth. citeturn3search2turn0search1