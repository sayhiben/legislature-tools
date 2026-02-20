# Detecting Potential Automation and Manipulation in Legislative Testifier CSV Data

## Data context and what can be inferred from your fields

Your dataset (auto-increment ID, “Last, First” name string, mostly blank organization, Pro/Con position, and a minute-resolution submission timestamp) fits a common “event log” pattern: each row is one discrete action, with light identity information and one categorical stance. That structure supports **statistical anomaly detection** (identifying time windows, name patterns, and stance dynamics that look inconsistent with expected human-driven participation), but it usually cannot support **attribution** (proving who did it, whether it was a bot vs. a coordinated human effort, or whether it was one actor vs. many). citeturn24view0

In modern influence and “astroturfing” research, a repeated theme is that inauthentic campaigns try to **mimic ordinary behavior**, which makes “bot vs. human” an unreliable dichotomy; many campaigns mix automation, scheduling, and paid or coordinated human labor. In that literature, the most reliable signals are often **group-level coordination patterns** and **time-structured routines** rather than “single weird record” indicators. citeturn24view0

Minute-resolution timestamps are a meaningful limitation. They preserve diurnal patterns and medium-scale bursts (minutes to hours), but they erase within-minute spacing and can create many ties in high-volume periods. This reduces the power of classic continuous-time tests (e.g., inter-arrival distributions) and shifts the analysis toward **count-by-minute time series**, **window scans**, and **change-point** methods. citeturn6search6turn5search9

## Baselines for organic participation in time and stance

A core pitfall in this domain is treating “bursts” or “clusters” as evidence of automation. Empirically, many human activities are **bursty**—periods of intense action separated by inactivity—often deviating strongly from simple random-arrival (Poisson) assumptions. The classic explanation is that human action is shaped by priority queues, schedules, and attention cycles, producing heavy-tailed inter-event times and bursty sessions. citeturn39view0turn40view0

Online civic actions (like petition signing) show the same blend: (a) circadian (daily) rhythms and (b) bursty dynamics at multiple time scales. A large study of online petitions reports clear **bursty behavior** and **circadian patterns** in signing time series, and also emphasizes that characterizing burstiness often requires measures that consider ordering and non-stationarity. citeturn6search6turn25view0turn8view0

This matters because a “perfectly normal” grassroots mobilization (e.g., an email blast, a social media post, a news story, a hearing agenda item) can produce (i) sharp bursts, (ii) stance-skewed windows, and (iii) off-hour activity. Consequently, the strongest analyses in this setting are those that (1) build explicit null models, (2) compare multiple independent signals, and (3) localize suspicion to specific windows or clusters rather than concluding “the whole dataset is botted.” citeturn25view0turn24view0

## Timestamp-based anomaly analyses for automation, scheduling, and coordination

### Event-rate dispersion and baseline modeling

A practical first step is to convert the log to a minute-indexed series. Let \(y_t\) be the **count of submissions** in minute \(t\). Many anomaly methods then compare observed variability to a baseline random model.

Under a simple Poisson count model, the mean equals the variance; departures are often summarized by the **index of dispersion** \(I = \mathrm{Var}(y_t)/\mathrm{E}(y_t)\). Under Poisson-like randomness \(I \approx 1\); \(I \gg 1\) indicates overdispersion (extra clustering/bursts), while \(I \ll 1\) indicates unusually regular timing. citeturn5search7turn5search21

Because real human participation is usually non-stationary (time-of-day, day-of-week, deadlines), a more defensible baseline is a **Poisson regression / GLM** with seasonal structure, e.g.:
- \(y_t \sim \text{Poisson}(\lambda_t)\)
- \(\log \lambda_t = \beta_0 + f(\text{hour-of-day}) + g(\text{day-of-week}) + h(\text{date trend})\)

Then examine residuals (or standardized residuals) for extreme positive spikes (bursts) or long runs of unusually low variance (regularity). Overdispersion is common in practice and can be tested/handled with alternative count models (e.g., negative binomial; more flexible models exist for both over- and under-dispersion). citeturn5search21turn5search4

A key automation-adjacent signature here is **under-dispersion / unnatural regularity** (e.g., “exactly 1 submission every minute for 5 hours”), which is less consistent with human burstiness and more consistent with scheduled scripts. Conversely, extreme overdispersion can be either viral mobilization or batch injection; you need follow-on analyses to disambiguate. citeturn39view0turn6search6

### Burst and cluster localization in time

Once you have \(y_t\), you can localize anomalous windows using established burst/scan frameworks:

**Kleinberg burst detection.** Kleinberg’s model is widely used to identify “bursts” in event streams; it formalizes bursts as state transitions in an automaton with different intensity levels and produces a hierarchical (nested) burst description. In practice, it’s useful when you want a principled way to segment “background vs. burst levels” over time. citeturn16view0turn25view1

**Scan statistics (temporal scan).** Scan statistics search over many candidate windows and ask whether any window has an unusually high concentration of events compared to expectation—classically used in outbreak detection and broadly reviewed in the scan-statistics literature. Even though many scan frameworks are presented for space-time, the core idea applies directly to **time-only** windows (your case): scan over all windows of lengths \(L \in \{5, 10, 30, 60, 180\}\) minutes (and more), compute a likelihood-ratio (or expectation-based) score for “elevated rate,” and identify the most significant windows. citeturn31view0turn19view0turn30view0

A pragmatic implementation detail: because scanning many windows is itself a multiple-testing problem, you either (a) use Monte Carlo/permutation calibration (common in scan settings) or (b) use error-rate control like FDR when you treat many windows as “tests.” citeturn31view0turn33view0

image_group{"layout":"carousel","aspect_ratio":"16:9","query":["Kleinberg burst detection example plot","CUSUM control chart example","scan statistic temporal window diagram","time series autocorrelation periodic pattern example"],"num_per_query":1}

### Stance dynamics and “Pro/Con ratio swings”

Because you have a binary stance field, you can detect unusual stance shifts without needing any name-based inference.

For each time window \(W\), let \(n_W\) be submissions and \(k_W\) be “Pro” (or “Con”). A natural model is:
- \(k_W \sim \text{Binomial}(n_W, p_W)\)

You can then ask whether \(p_W\) is stable over time or shows sharp structural shifts. Logistic regression is a standard way to model a binary outcome probability as a function of covariates (hour, day, event phase), and it supports tests for whether time-of-day or specific intervals significantly change the odds of Pro vs. Con. citeturn35view0turn34view0

For localized detection rather than a single global model, two complementary approaches are common:

- **Windowed proportion tests:** Compare \(p_W\) in a candidate suspicious window to a baseline (previous day, same hour-of-day, or the full dataset). For small windows, exact or small-sample methods can be appropriate; for large \(n_W\), standard approximations work well. citeturn34view0turn4search1  
- **Change-point detection / CUSUM:** CUSUM methods were developed for sequential change detection and are widely used to detect shifts in a process over time. In your setting, you can apply a CUSUM-style detector to either (a) \(y_t\) (volume) or (b) the sequence of stances (coded 0/1) to detect points where the underlying mean/proportion changes. citeturn2search0turn2search27

A high-value diagnostic specifically tied to suspected manipulation is **off-hours stance skew**: if the overall stream shows human-like circadian volume, but one stance’s probability concentrates in a narrow off-hour band (or vice versa), that can indicate scheduled or shift-based activity. Coordination research on astroturfing campaigns reports that campaign activity can display distinct daily/weekly routines (e.g., clustered in office hours), which is exactly the kind of pattern you can test with stance-time interaction models. citeturn24view0turn6search6

### Regularity, periodicity, and motif-like temporal patterns

Bots and scripted campaigns frequently reveal **regular intervals**, repeated motifs, or unusually synchronized timing patterns—properties that can be studied even when you only have timestamps (and not IP/user agent).

In social bot research, temporal-pattern mining (motifs, bursts, discords, and dynamic clustering) has been used to separate bots from humans, and “unusually synchronous” timing across accounts is treated as a key bot/coordination signature. While your data lacks account IDs, analogous patterns can appear at the aggregate level (e.g., repeated bursts at the same minute-of-hour across multiple days). citeturn28view0turn27view0

Concrete analyses that map well to your CSV constraints:

- **Autocorrelation / spectral peaks on \(y_t\):** Look for strong periodic components at 5, 10, 15, 30, 60 minutes (common scheduler cadences). Repeating spikes at exact fixed intervals are less consistent with organic burstiness. citeturn28view0turn39view0  
- **Runs of low-variance activity:** Identify long segments where the count per minute rarely deviates (e.g., alternating 0/1 with nearly constant rate). Human-driven civic mobilization tends to be bursty with variable gaps; excessively smooth regularity can be suspicious. citeturn39view0turn6search6  
- **“Clock-face” analysis:** Plot counts by minute-of-hour (0–59). If the system records actual server submission time, strong spikes at specific minute values (e.g., always at :00 or :30) may indicate scheduling. (This should be interpreted cautiously; platform batching/queueing can also create artifacts.) citeturn5search9turn28view0  

### Data-order and “unusually sorted” diagnostics

Because you have an auto-increment ID, you can test whether record order reflects submission order. If IDs correlate tightly with time, that’s expected for insert-at-submit logging; if not, you may be looking at a **sorted export** or a **post-processed dataset**.

Order-based anomalies that can matter for automation suspicion:

- **Within-minute ordering:** For minutes with many submissions (same timestamp), check whether names appear alphabetically (or follow another systematic pattern) in the row order. Strong alphabetical ordering inside tight time buckets can indicate batch insertion or export sorting rather than true arrival order. citeturn24view0turn9view0  
- **Time monotonicity breaks:** Identify segments where IDs increase but timestamps go backward (or jump). Some may be timezone/DST issues, but systematic reorderings can undermine other time-based inferences and should be documented. citeturn23view0turn6search6  

## Name-field analyses for duplication, obfuscation, and synthetic identities

### Canonicalization and data standardization

Name-based work is extremely sensitive to formatting and noise. Record-linkage literature emphasizes that standardization is a prerequisite for meaningful matching (e.g., punctuation, spacing, casing, diacritics, suffixes). citeturn12view0turn9view0

For your specific “Last, First” field, canonicalization typically includes:
- splitting into last / first (and optional middle/initials/suffix if present),
- uppercasing or casefolding consistently,
- removing punctuation that doesn’t change identity (“O’CONNOR” vs “OCONNOR” as a comparison variant),
- preserving alternate forms as separate features (because punctuation/spacing can itself be used to evade deduping). citeturn12view0turn10view0

### Duplicate detection and probabilistic record linkage

Duplicate names are not inherently suspicious: extremely common names will repeat often, and rare names repeating can be strong evidence of duplication or of a real person appearing multiple times—without extra identifiers you cannot know which. Record linkage explicitly frames this as a probabilistic problem: matching on a common name (“John Smith”) provides weak evidence; matching on a rare name is much stronger. citeturn12view0turn9view0

A robust deduplication approach is to treat this as **within-file record linkage** (“deduplication/unduplication”), using the Fellegi–Sunter framework: create comparison features for candidate pairs and then classify likely-matches vs non-matches, often with blocking to keep computation feasible on large files. citeturn12view0turn9view0

Because your non-name fields are sparse, your comparison features are mostly string-similarity features. Standard string metrics used in record linkage include:

- **Jaro–Winkler** similarity, designed for names and emphasizing shared prefixes (useful for typographic variants and transpositions). citeturn10view0  
- **Edit distance (Levenshtein)** and other edit-distance family methods. citeturn11view0turn3search3  
- Comparative evaluations in name-matching tasks find that hybrid approaches (token weighting + name-oriented similarity like Jaro–Winkler) can perform well, and that Jaro–Winkler can be a strong fast baseline in practice. citeturn36view0turn11view0  

Deliverable you want from this step is not just “a deduped list,” but a **distribution of cluster sizes** (how many near-duplicates exist) and their relationship to time and stance (see the joint analyses section). citeturn12view0turn24view0

### Nicknames, variant spellings, and obfuscation strategies

Nicknames and spelling variants matter for two different reasons:
1) they create false negatives in deduplication if you only use exact matching, and  
2) they can be used intentionally to evade naive duplicate detection (“Robert” vs “Bob”; swapping initials; adding/removing middle initials). citeturn12view0turn10view0

Record-linkage practice explicitly flags these issues: people may use nicknames, include or omit middle names, and vary how they report identifying information. Your analysis should therefore treat “nickname-like variation rate” and “initial usage rate” as measurable quantities, especially inside suspicious time windows. citeturn12view0turn23view0

A concrete way to operationalize this in your dataset:

- Build a canonical “first-name core” feature (e.g., strip trailing punctuation, collapse repeated spaces, map common nicknames using a lookup table if you choose to include one).  
- Measure, per time window, the share of records that (a) are initials-only, (b) contain uncommon punctuation patterns, or (c) match a near-duplicate cluster when using spell-tolerant metrics but not when using exact matching. citeturn10view0turn36view0turn12view0

### Rare-name prevalence and synthetic-name suspicion

Name rarity is one of the few “external baseline” handles you have. Two widely used public baselines are:

- The entity["organization","Social Security Administration","us federal agency"] first-name data (from Social Security card applications for U.S. births after 1879), with documented limitations (not edited; variant spellings not combined; privacy thresholds suppress very low counts). citeturn23view0  
- The entity["organization","United States Census Bureau","us federal statistical agency"] surname tabulations (e.g., 2010 surname files made available via the Census API; the publicly summarized product includes surnames occurring at least 100 times and explicitly notes that it is aggregated/tabulated, not individual-level). citeturn21view0  

A practical rarity workflow:

1) Assign each first name a frequency rank/percentile from SSA (acknowledging this is a births-based baseline, not a voter-adult baseline). citeturn23view0  
2) Assign each surname a frequency rank/percentile from the Census surname distribution (noting that it truncates rare surnames under the “100+ occurrences” framing if you use that particular tabulation). citeturn21view0  
3) Define a simple rarity score such as:  
\[
R = -\log \big(\max(\epsilon, p(\text{first}))\cdot \max(\epsilon, p(\text{last}))\big)
\]
and then study \(R\) over time and by stance. citeturn23view0turn21view0  

What you are looking for is not “many rare names” per se—real civic participation includes many rare names—but **time-localized and stance-localized shifts** in rarity, such as:
- a burst window where the median rarity jumps sharply,
- a window where near-duplicate clustering drops to near-zero but rarity spikes (consistent with synthetic one-off names),
- or conversely, a window where many “rare” names share unnatural structural properties (same length patterns, unusual capitalization, repeated letter patterns), which can happen when names are generated from templates. citeturn24view0turn28view0  

Because name frequency baselines reflect population structure imperfectly (especially across ethnicity, age, and geography), rarity-based flags must be treated as **ranking signals**, not as proofs, and should be cross-validated with the time-structure and duplicate-structure analyses. citeturn12view0turn23view0

## Joint analyses combining name patterns, timing, and Pro/Con position

The highest-confidence signals typically come from **convergence**: a suspicious time window (timestamp analysis) that also exhibits suspicious name structure (name analysis) and suspicious stance dynamics (Pro/Con shifts).

### Time-localized duplicate and near-duplicate clusters

After clustering names via record linkage, compute per cluster:

- size (number of records in the cluster),
- time span (max timestamp − min timestamp),
- stance consistency (all Pro, all Con, or mixed),
- concentration (e.g., how many occur in the same minute or same 10-minute window). citeturn12view0turn10view0  

A particularly salient pattern for automation suspicion is: **many near-duplicate clusters that “turn on” in a tight window** and share the same stance. That can reflect scripted resubmissions, cut-and-paste sign-ins, or coordinated entry. citeturn24view0turn16view0

### Coordination-style signatures without account IDs

Even without user accounts, you can borrow “coordination detection” concepts by defining coordination in terms of event similarity + time proximity.

In astroturfing detection, “coordinated” behavior is often defined by posting the same message within a short time window, and one-minute thresholds are used operationally in some coordination analyses. Translating that idea to your dataset, you can define coordination edges such as:

- same (or near-duplicate) name cluster activity within Δt minutes,  
- bursts where many records share an unusual name feature (e.g., initials-only first names) within Δt,  
- or repeated micro-patterns like “exactly 30 consecutive minutes with identical stance and near-constant volume.” citeturn24view0turn28view0  

You are not claiming “these are the same actor” (you cannot), but you can quantify whether the stream exhibits **implausibly synchronized** structure compared to null models. citeturn24view0turn31view0

### Stance-dependent diurnal routines

Build two diurnal profiles:

- \(y^{\text{Pro}}_{h}\): average Pro submissions per hour-of-day  
- \(y^{\text{Con}}_{h}\): average Con submissions per hour-of-day  

and test:
- whether the shapes differ (distributional comparison), and  
- whether one side’s activity concentrates in “work shift” blocks or other routine windows.

Coordination research reports that campaign-like activity can differ from typical user activity by time-of-day and day-of-week patterns (e.g., office-hour concentration). Your dataset can test for that by comparing stance-time interactions (logistic regression), and by checking whether detected bursts are disproportionately one-sided. citeturn24view0turn35view0turn6search6

## Calibration, statistical validity, and how to report results responsibly

### Use null models and permutation calibration

Because your process is non-stationary and heavily aggregated (minute granularity), calibration by permutation/simulation is often more trustworthy than relying solely on asymptotic p-values.

Two common null strategies:

- **Permutation tests:** Shuffle stance labels within constrained strata (e.g., within each day, or within each hour-of-day across days) to preserve diurnal volume but break any stance-time coupling; then recompute your “max window” statistics to obtain an empirical reference distribution. Randomization/permutation methods trace back to early experimental design work and are widely used as assumption-light inference tools. citeturn7search6turn7search20  
- **Time-rescaling / model-based simulation:** Fit a baseline intensity model \(\hat{\lambda}_t\) (Poisson regression or similar), simulate event times (or counts) from the fitted model, and compare observed burst statistics to simulated ones. In point-process settings, the time-rescaling theorem provides a foundation for transforming and assessing point process models via goodness-of-fit diagnostics. (With minute data you’d use discrete analogs or simulate counts per minute.) citeturn37view0turn38view0turn5search9  

### Control false discoveries across many tests

Scanning over many windows, many similarity thresholds, and many name-features creates a multiple-comparisons problem.

A practical approach is to treat flagged windows/clusters as “discoveries” and control the **false discovery rate (FDR)** using the Benjamini–Hochberg procedure, which was introduced specifically to provide a more powerful error-rate control than family-wise corrections in large-scale testing. citeturn33view0

### Produce evidence bundles, not a single score

A defensible final output is usually a ranked set of **suspect windows** or **suspect clusters**, each with an “evidence bundle,” e.g.:

- time evidence: burst/scan score, dispersion/regularity metrics, periodicity indicators,  
- stance evidence: window-level Pro/Con deviation from baseline, change-point signals,  
- name evidence: near-duplicate cluster counts, rarity distribution shift, initials/nickname anomalies.

This mirrors best practice in coordination and anomaly research: detection is strongest when multiple independent signatures line up, and weakest when it depends on a single noisy heuristic. citeturn24view0turn6search6turn16view0

### Document sensitivity and bias risks

Name-based rarity and string-structure heuristics can unintentionally correlate with demographic, linguistic, and cultural patterns (e.g., diacritics, compound surnames, transliteration variance). Record-linkage guidance emphasizes privacy and practical considerations, and your reporting should treat name-based signals as supportive rather than dispositive—especially if the downstream use is consequential. citeturn12view0turn23view0