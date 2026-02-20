1) High-level critique: the dashboard is “profiling” instead of “investigating”

What it’s doing well
	•	You’ve got a broad sweep of distributions and relationships.
	•	It’s visually consistent (mostly one palette) and has a navigation sidebar.
	•	It likely surfaces basic issues (missingness, cardinality, heavy tails).

The problem for this domain

For manipulation detection, the questions are not “What’s the distribution of field X?” but:
	•	Where are the suspicious windows?
	•	Which records/clusters drive those windows?
	•	What specific signals make them suspicious?
	•	How strong is the evidence (effect size + uncertainty), and how does it compare to baseline?

Profiling reports bury those answers under dozens of generic plots (many of which will be misleading for sequential IDs, sparse org fields, etc.). The result: high cognitive load, low actionability.

Fix: Reframe the dashboard around an investigation flow:
	1.	Find anomalous windows
	2.	Explain anomalous windows (composition, pro/con shift, duplicate/near-dup concentration)
	3.	Drill down to the actual rows/names
	4.	Provide a ranked queue for review/export

⸻

2) “Code review” issues and improvements

A. Information architecture (major)

Issue: The dashboard reads like a linear dump of charts. There’s no clear hierarchy: “here are the 5 things you should look at first.”

Improve:
	•	Add an Executive Summary header at the top with:
	•	Total submissions, date range
	•	Overall Pro/Con rate
	•	Top 5 burst windows (with timestamps + counts + p/q-values)
	•	Top 5 pro/con swing windows (timestamps + delta + n)
	•	Top 10 repeated names + top 5 near-dup clusters
	•	Off-hours % volume + off-hours pro-rate vs baseline
	•	Add an Anomalies Table directly below summary:
	•	start_time, end_time, count, expected, z, q_value, pro_rate, delta_pro_rate, dup_fraction, near_dup_fraction, name_weirdness_mean
	•	sortable, filterable, with “drilldown” links

This makes it usable: you land on the page and immediately see what matters.

⸻

B. Relevance filtering (major)

Issue: Generic profiling visuals for:
	•	id (sequential)
	•	“correlations” between id and time (expected monotonic)
	•	missingness histograms (org almost blank; this isn’t “interesting” unless it changes over time)

These plots add noise, not signal.

Improve:
	•	Treat id as an index. Show only:
	•	monotonicity check: % non-monotonic time vs id
	•	gaps/duplicates: missing IDs, repeated IDs (if any)
	•	Replace “correlation matrices” with domain-relevant relationships, e.g.:
	•	count_per_minute vs pro_rate (windowed)
	•	burst_score vs dup_fraction
	•	burst_score vs near_dup_cluster_fraction
	•	Only show missingness if it’s time-varying (e.g., org field suddenly filled during a window).

⸻

C. Visual encoding / color semantics (major)

Issue: Nearly everything is blue. That works for EDA, but for anomaly detection you need semantic color:
	•	Pro vs Con should be visually distinct everywhere.
	•	“Normal” vs “Flagged” should be visually distinct.
	•	Divergence plots should use diverging palettes.

Improve:
	•	Standardize:
	•	Pro = one hue, Con = another (consistent across all charts)
	•	Flags = red/orange overlay, normal = neutral
	•	Add legends that actually explain semantics (not just value ranges).

⸻

D. Time-series readability (major)

From what I can see, your time series plots are dense and “mountain-y.” With minute bins, this is inevitable.

Improve:
	•	Add multiple resolutions:
	•	minute-level (for detection)
	•	5-min, 15-min, hourly (for readability)
	•	Overlay:
	•	baseline (rolling median / STL trend)
	•	confidence band / threshold line (control limits)
	•	explicit annotations for flagged windows (shaded bands)
	•	Add “brush to zoom” and a linked drilldown table for the selected interval.

If you do only one big time-series chart, make it:
	•	counts/minute (stacked Pro/Con)
	•	with shaded anomaly windows

⸻

E. Metrics need uncertainty + effect sizes (major)

Issue: Many dashboards show raw ratios and counts without telling you if the change is meaningful or just small-n noise.

Improve:
For every windowed rate chart (pro_rate, unique_fraction, etc.) show:
	•	the rate
	•	an uncertainty interval (Wilson / Beta posterior)
	•	a baseline comparison (Δ and standardized effect)

And in the anomalies table:
	•	report n and an adjusted significance (q-value) if you’re scanning many windows.

⸻

F. Drilldown and explainability (major)

Issue: Profilers don’t connect “this looks weird” to “here are the exact records causing it.”

Improve:
For each flagged window provide a drilldown panel that shows:
	•	Top repeated names (exact) in that window
	•	Near-duplicate clusters active in that window (with examples)
	•	Distribution of name “weirdness” features vs baseline
	•	Organization nonblank values in that window (if relevant)
	•	Export: download rows for that window as CSV

This is where your dashboard becomes investigative rather than descriptive.

⸻

G. “Duplicate report” / repeated sections (major bug)

The screenshot looks like the entire report appears twice. If true:
	•	fix the rendering loop / layout duplication
	•	add section anchors and collapse/expand sections to reduce scroll fatigue

⸻

3) What I would add to increase signal-to-noise

Below are additional analyses that tend to separate “organic civic engagement” from “automation/coordinated posting” using only your fields.

1) Burst composition analysis (adds meaningful context)

Not just “there was a spike,” but “what was inside the spike.”

For every detected burst window compute:
	•	dup_fraction_exact = % rows whose canonical name appears ≥2 in that window
	•	near_dup_fraction = % rows belonging to a near-duplicate cluster (within window)
	•	unique_name_fraction = unique / total
	•	name_weirdness_mean and 95th percentile (entropy, digits, punctuation, length)
	•	pro_rate and Δ vs baseline
	•	org_nonblank_rate

Then rank bursts by a composite suspiciousness score.
This usually produces a very actionable top-10 list.

Dashboard add: a scatter plot of windows:
	•	x = burst z-score (or q-value rank)
	•	y = near_dup_fraction (or unique_name_fraction)
	•	point color = pro_rate delta
	•	point size = total submissions
This instantly highlights “bursty + weird composition” windows.

⸻

2) Inter-arrival regularity / “clockwork” signature

Even with minute resolution you can detect regimes like:
	•	steady 1/min, 2/min for long stretches
	•	unnaturally low variance in counts within a period

Implement:
	•	rolling Fano factor (variance/mean) on counts-per-minute
(Poisson-ish organic ≈ 1; scripted can be <<1 or show periodic artifacts)
	•	autocorrelation peaks
	•	spectral peaks (FFT) on counts series

Dashboard add: “regularity score over time” with flagged regimes.

⸻

3) Pro/Con sequence “runs” tests (order-based)

If the data is in true submission order, the sequence of Pro/Con can be tested.

Compute:
	•	Wald–Wolfowitz runs test for randomness on the Pro/Con sequence
	•	windowed runs statistic: detect segments with unusually long runs (all Pro)
	•	Markov transition matrix in windows (P(Pro→Pro), etc.)

Why it helps: coordinated scripts often submit long homogeneous runs, or alternate deterministically.

Dashboard add: for flagged windows, show:
	•	run length distribution vs baseline

⸻

4) Name distribution sanity checks vs external name frequencies (very high signal if done carefully)

Bots using random name lists often produce:
	•	too many extremely rare last names
	•	abnormal first-name distribution
	•	odd initial-letter distributions

Method:
	•	Use SSA first-name frequency and Census surname frequency as reference tables.
	•	Score each name with -log P(first) - log P(last) (approx improbability).
	•	Track average improbability over time; bursts with very high improbability are suspicious.

Dashboard add:
	•	“name improbability over time” with top windows
	•	top improbable names list (with caveats)

This can be one of your best detectors if implemented conservatively (many legitimate rare names exist; you’re looking for shifts and concentrations).

⸻

5) Near-duplicate clusters concentrated in time

Near-dups are more suspicious when:
	•	cluster members occur in a tight window
	•	cluster spans many minutes/hours but at regular cadence

For each cluster compute:
	•	size
	•	active span (max_time - min_time)
	•	concentration (e.g., % of cluster within shortest 30-min interval)
	•	cadence metrics (gaps distribution)

Dashboard add:
	•	cluster table sorted by “time concentration score”
	•	click cluster → show member names + timestamps + positions

⸻

6) “Within-minute alphabetical ordering” / export artifacts

If within the same minute the names appear sorted alphabetically more than chance, that can indicate:
	•	batch uploads
	•	post-processing sort
	•	scripted generation output order

Compute for each minute bucket:
	•	Kendall tau between record order and name order
Aggregate minutes with tau near 1.

Dashboard add: percent of minutes “sortedness > threshold” and list top minutes.

(Interpret carefully—this can also be an export quirk.)

⸻

7) Off-hours deviation: volume and composition

Off-hours by itself isn’t suspicious; what matters is:
	•	off-hours volume spikes + composition shifts (higher improbability, more unique names, more near-dups, extreme pro_rate)

Dashboard add:
	•	hour-of-week heatmap for volume
	•	hour-of-week heatmap for pro_rate (with n thresholds)
	•	hour-of-week heatmap for near_dup_fraction / weirdness

⸻

8) Robust “suspicion scoring” with explainability

Build a composite score at both:
	•	window level (for triage)
	•	record/cluster level (for drilldown)

Score components might include:
	•	burst z-score / q-value
	•	pro_rate delta / q-value
	•	membership in near-dup cluster
	•	exact-dup count
	•	name improbability percentile
	•	weirdness percentile
	•	off-hours indicator

Dashboard add:
	•	top 100 records with “reasons” (which components fired)
	•	top 20 windows with same

This is how you turn many weak signals into a usable review queue.

⸻

4) Concrete dashboard layout recommendation (practical and readable)

Page 1: “Triage”
	1.	KPI cards
	2.	Counts/min (stacked Pro/Con) with anomaly overlays
	3.	Anomalous windows table (sortable)
	4.	Off-hours summary + hour-of-week heatmap
	5.	Top clusters/names summary

Page 2: “Window drilldown”
	•	Time brush selector
	•	For selected window:
	•	raw rows table (download)
	•	exact dup names
	•	near-dup clusters (with examples)
	•	pro/con runs summary
	•	name improbability + weirdness distributions vs baseline

Page 3: “Name/Cluster forensics”
	•	Cluster table
	•	Cluster graph / connected components
	•	Name feature distributions
	•	External frequency scoring summaries

Page 4: “Methodology / thresholds”
	•	definitions, tests used, multiple testing correction, caveats

This keeps the system defensible and reviewer-friendly.

⸻

5) Small but high-impact polish items
	•	Put the timezone (and DST handling) explicitly on every time axis.
	•	Use consistent binning rules (minute/5-min/15-min) and label them.
	•	Avoid giant correlation matrices for this dataset; they mislead more than help.
	•	For any heatmap/rate plot, enforce minimum n per cell; otherwise you’ll chase noise.
	•	Add “data quality” warnings that are actually meaningful here:
	•	invalid/missing positions
	•	unparsable names
	•	duplicate IDs
	•	non-monotonic timestamps vs ID

⸻

If you want a sharper “line-by-line” critique

The screenshot is extremely narrow (the text labels aren’t readable), so I can’t reliably comment on specific field names/metrics you already included. If you can export/share the report HTML (or a higher-res screenshot), I can do a true code-review style pass that references specific panels (“Panel X is misleading because…”, “rename metric Y…”, etc.).

But even from what’s visible, the main improvement is clear: shift from generic profiling panels to an anomaly triage + drilldown workflow, and add composition-based anomaly metrics (near-dup concentration, name improbability/weirdness, runs tests) to increase signal-to-noise.