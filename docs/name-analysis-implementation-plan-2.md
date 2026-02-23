## Refined, compiled plan for duplicate-name and voter-roll name matching analyses

### 0) Guiding principle: what you *can* and *cannot* infer
Because you do **not** have stable person identifiers (address, DOB, voter ID, email, phone, etc.), you cannot observe “the same human signed in twice.” What you *can* measure:

1. **Name-string reuse** under explicit normalization rules  
2. **Excess reuse** relative to defensible baselines (population collision expectations; cross-hearing expectations)  
3. **Asymmetry by position** (duplicates/miss rates/time bursts disproportionately concentrated in Pro vs Con)  
4. **Sensitivity of the headline Pro/Con ratio** to plausible deduplication assumptions  

Pre-commit to language like:
- “statistically anomalous,” “inconsistent with baseline assumptions,” “requires further investigation,”  
and avoid:
- “fake,” “fraud,” “bots,” “manipulation,” when referring to individuals.

This is not just PR—this is how you prevent the analysis from becoming statistically or legally brittle.

---

### 1) Define the estimands (the quantities you will report)
Make these explicit in your documentation and UI.

#### 1.1 Core duplication estimands
Compute each under multiple “name keys” (see Section 2):
- **D1: Duplicate-row rate**: fraction of rows whose name key appears ≥ 2 times  
- **D2: Duplicate-pair count**: \(\sum_i \binom{c_i}{2}\) where \(c_i\) is count for name \(i\)  
  - This is extremely useful because it aligns cleanly with baseline math and gives more weight to heavy repeaters.
- **D3: Tail metrics**: max \(c_i\), and counts of names with \(c_i \ge 2,3,5,10\)  
- **D4: Concentration**: share of all rows contributed by top 1% (or top 10) repeat-name groups; optionally Gini/Lorenz  

Report overall and by **Position**.

#### 1.2 Voter-roll linkage estimands
Under an explicitly defined matching policy:
- **M1: Match outcome counts** by position: `No match / Ambiguous / Unique / Fuzzy-high-confidence / Fuzzy-ambiguous`  
- **M2: Miss rate** by position, at two units:
  - **Row-level** (what the public dataset shows)
  - **Unique-name-level** (deduped by a medium key; reduces duplicate-row distortion)
- **M3: Weighted match rate**: weight matches by inverse name frequency (rare-name matches count more)

#### 1.3 “Swing potential” estimands (what users care about)
For Pro/Con ratio:
- **S1: Raw ratio** (rows)
- **S2: Dedup ratio** under each name key
- **S3: Bounded interval** for Pro share under two extremes:
  - **Upper bound**: assume duplicates are distinct people (collisions)  
  - **Lower bound**: assume within-position duplicates are repeat sign-ins  
- **S4: Rarity-weighted estimate**: probabilistically downweight common-name repeats and upweight rare-name repeats (see Section 5.4)

---

### 2) Data preparation and canonicalization (foundation you must get right)

#### 2.1 Preserve raw; generate structured name fields
For every record, store:
- `raw_name`, `raw_org`, `raw_position`, `raw_datetime`
- Parsed name components:
  - `last`, `first`, optional `middle`, `suffix`, plus “particles” handling (De/Van/La/etc.)

Canonicalization transforms (store both raw and canonical):
- Casefold
- Unicode normalize
- Whitespace/punctuation normalize
- Apostrophes/hyphens preserved *and* folded variants generated
- Suffix normalization (“Jr.” → “JR”)
- **Do not** over-aggressively collapse multi-part surnames.

#### 2.2 Define multiple name keys (run everything across keys)
This is non-negotiable: it’s how you keep the analysis honest.

Recommended keys:
- **Key S (Strict)**: last + first + middle/initial + suffix  
- **Key M (Medium)**: last + first  
- **Key L (Loose)**: last + first-initial (mostly for blocking / stress-testing collisions)  
- **Key N (Nickname-expanded)**: last + canonical-first (using your nickname table)

Optionally (for fuzzy candidate generation only):
- **Key P (Phonetic)**: Double Metaphone on last name (and possibly first), *not* as a final key

**Important**: treat Key N and phonetic components as **separate sensitivity tiers**, not a single “final truth.”

#### 2.3 Position & time normalization
- Position: map to `{Pro, Con, Other, Unknown}` + preserve original string
- Datetime:
  - store timezone assumption explicitly
  - derive: `minute_index_since_first_signin`, `date`, and `minute_bin`
- Create hearing-level context features:
  - sign-ins per minute overall and by position

---

### 3) First-pass descriptive analysis (no inference yet)
Purpose: establish what is true *in the data* before baseline comparisons.

For each key (S/M/N) compute overall and by position:
- counts: rows, unique names, duplicate rows, duplicate pairs, max count
- distribution of counts-per-name (1,2,3,…)
- “top repeaters” table with **name rarity context** (Section 4), plus position/time summaries

Also compute:
- **Position concordance** for repeated names:
  - For each name: counts by position
  - concordant-pairs = \(\sum_{pos} \binom{c_{pos}}{2}\)
  - discordant-pairs = total pairs − concordant pairs

---

### 4) Build the voter-roll baseline (collision expectations and rarity tiers)

#### 4.1 Build a voter-roll name frequency table (for each key you use)
From the voter file, compute:
- \(f_i\) = count of name \(i\)
- \(p_i = f_i / N\) where \(N\) is total voters after cleaning
- rarity percentiles / tiers (e.g., top 1%, 1–5%, 5–20%, bottom 80%)

Do this for:
- full name (Key M and Key N)
- optionally last-only and first-only for diagnostics (but don’t overinterpret)

#### 4.2 Hearing-level null model via Monte Carlo (primary baseline)
For a hearing with \(n\) rows:
- Simulate drawing \(n\) individuals from the voter-name distribution  
  - With replacement is usually fine if \(n \ll N\); also run without replacement as sensitivity.
- For each simulation, compute the same duplication metrics: D1–D4.

Outputs:
- expected mean and **empirical 95% interval** (use quantiles)
- empirical p-value: fraction of simulations with metric ≥ observed
- standardized effect size: \((obs - mean)/sd\) as an optional summary

This is the backbone “is this hearing unusual?” test.

#### 4.3 Name-level null model (per-name anomaly probabilities)
For each name \(i\) observed \(c_i\) times in the hearing:
- Under H0 (distinct individuals drawn from population), \(C_i\) is approximately:
  - Hypergeometric (without replacement) or Binomial (with replacement)
  - Poisson is a good approximation for rare names
- Compute tail probability \(P(C_i \ge c_i)\)

**Multiple testing policy**
- Exploratory ranking: **Benjamini–Hochberg FDR** (q=0.05 or q=0.10), show raw p and q
- Confirmatory subsets (if ever): **Holm** (or gating: overall test first, then partitioned, then a limited set of names)

#### 4.4 Baseline mismatch caveat + mitigation
Testifiers are not random voters. Therefore:
- Treat voter-roll baseline as **one lens**, not the only one.
- Add an empirical baseline if possible: duplicate metrics across many hearings/bills, stratified by hearing size and year/session.

---

### 5) Position-focused inference: “is duplication skewed and does it matter?”

#### 5.1 Duplicate rate by position vs expected
For each position group separately (Pro/Con/Other):
- run the Monte Carlo baseline using that subgroup’s \(n_{pos}\)
- compare observed vs expected duplicate metrics

#### 5.2 Between-position tests (skew)
Avoid naive row-level independence assumptions because duplicates cluster by name.

Recommended hierarchy:
1) **Primary unit = unique name** (dedup by Key M)  
2) Secondary unit = rows, but with cluster-aware uncertainty

Tests:
- **Permutation test (preferred)**: shuffle position labels across rows while holding names fixed; recompute difference in duplicate metrics (or concordant-pair metrics).
- **Contingency tests** (chi-squared / Fisher) as a simple check, but interpret carefully.

Report:
- effect sizes (risk difference, RR, OR) + CIs (bootstrap is fine)

#### 5.3 Concordance ratio test
Logic:
- Under null (duplicate strings are collisions among distinct people), positions for occurrences should behave like independent draws from overall position mix.
- Compare observed concordant vs discordant duplicate pairs to expectation (analytically or via permutation).
- Elevated concordance on one side = signal that repetition may be boosting that side.

#### 5.4 Swing potential: bound and estimate Pro/Con sensitivity
Produce a standard “Pro/Con robustness panel”:

- **Raw** counts (rows)
- **Dedup** counts under Key S / Key M / Key N
- **Bounds**:
  - Upper: treat duplicates as different people (no adjustment)
  - Lower: within-position duplicates collapse to 1
- **Rarity-weighted dedup (recommended middle estimate)**:
  - For each duplicate name \(i\), estimate “collision vs repeat” evidence using voter frequency \(p_i\) and the observed count \(c_i\) (via tail probability or q-value).
  - Map evidence into a weight \(w_i \in [0,1]\) that represents “effective unique persons contributed by this cluster.”
  - Publish the mapping, and show sensitivity bands.

This gives a defensible middle ground between “all collisions” and “all repeats.”

---

### 6) Voter-file linkage and miss-rate analysis (done responsibly)

#### 6.1 Define linkage tiers and outcomes (taxonomy)
For each testifier name (preferably deduped by Key M first), compute outcomes:
- **No match**
- **Ambiguous** (multiple candidates)
- **Unique**
- **Fuzzy-high-confidence**
- **Fuzzy-ambiguous**

This prevents “miss rate” from conflating “no such name exists” with “name is common.”

#### 6.2 Matching pipeline (transparent and conservative)
**Deterministic first (high precision)**
- exact match on Key M (and Key S when available)
- consistent suffix/punctuation handling

**Candidate generation for fuzzy (high recall but controlled)**
- block on last name exact or phonetic (Double Metaphone on last)
- within block: compare first name with nickname expansion + Jaro–Winkler / edit distance
- do not allow global fuzzy matching without blocking

**Threshold policy**
- Predefine thresholds for “high-confidence fuzzy”
- keep a buffer “review zone” (don’t force ambiguous fuzzy matches into “matched”)

#### 6.3 Compute miss rates by position with correct uncertainty
Compute miss rates at:
- row level
- unique-name level

Compare across positions using:
- chi-squared / Fisher for simple reporting, and/or
- logistic regression with cluster-robust SEs (cluster by standardized name)

Always report effect sizes + CIs.

#### 6.4 Interpret miss rates cautiously (required caveats)
Misses can reflect:
- out-of-state participants
- unregistered eligible residents
- data-entry variability
- voter roll staleness

Therefore: treat miss-rate asymmetry as **corroborating evidence**, strongest when aligned with:
- excess duplicates beyond baseline
- temporal bursts
- rare-name repeat anomalies

---

### 7) Temporal anomaly detection (minute-resolution can still be powerful)

#### 7.1 Hearing-level arrival patterns
Compute per-minute counts:
- overall
- by position
- by “unmatched” status
- by “duplicate-involved” status

Visualize:
- cumulative curves by position (stair-steps reveal bursts)
- per-minute bar/line
- heatmap: minute × position with intensity

#### 7.2 Burst detection (two-tier approach)
**Tier A (simple, explainable)**
- sliding windows (e.g., 5-min / 15-min) z-scores vs hearing-average
- index of dispersion (variance/mean across minute bins)

**Tier B (optional advanced)**
- Kleinberg burst detection to identify high-intensity states
- temporal scan statistic for formal cluster localization

Always show sensitivity to parameters.

#### 7.3 Name-level time clustering
For each repeated name:
- min inter-arrival time
- number of occurrences within 5/15 minutes
- time span between first and last occurrence

Inference via permutation:
- keep overall time intensity fixed; randomly permute timestamps among names (optionally within position); recompute clustering stats.

#### 7.4 “Correction window” sensitivity
Add a sensitivity variant:
- collapse repeated (name key + position) within X minutes into one “corrected entry”
- report how duplication metrics and swing potential change as X ∈ {0, 5, 10, 30}

---

### 8) Composite anomaly scoring and investigation workflow (transparent, not magical)

#### 8.1 Build a feature set per name cluster
For each name (or fuzzy cluster), compute:
- rarity tier / voter frequency
- occurrence count and per-name tail p-value/q-value
- position concentration (entropy or “all on one side”)
- time clustering features
- voter match outcome class
- organization consistency when available

#### 8.2 Scoring approach
Prefer transparent scoring:
- rank-normalize each feature → weighted sum (weights documented), or
- “flag count” (how many dimensions exceed thresholds)

Avoid opaque ML unless you have labeled ground truth.

#### 8.3 Output artifacts
- “Top anomalous clusters” table for internal review
- Public-facing: aggregate rates + distributions; if names must be shown, include:
  - rarity tier
  - q-value / null probability
  - explicit caveat: “name-string anomaly, not identity proof”

---

### 9) Visualization and reporting package (what to show so anomalies are obvious)
For each hearing, generate a consistent dashboard:

#### 9.1 Core summary panels
1) **Counts & ratios**: rows by position, raw Pro/Con  
2) **Dedup sensitivity**: Pro/Con under Key S/M/N + bounds + rarity-weighted estimate  
3) **Duplication vs expected**: observed vs Monte Carlo null (percentile, p-value)  
4) **Match funnel**: match outcomes by position (deterministic + fuzzy tiers)  

#### 9.2 High-value visuals
- **Null histogram**: simulated duplicate-metric distribution with observed line  
- **Count-per-name distribution**: CCDF or histogram, faceted by position  
- **Rarity vs repetition scatter**: x = voter frequency (log), y = count, color by position  
- **Lorenz/Pareto curve**: share of rows contributed by top repeaters  
- **Timeline strip**: top N anomalous names, tick marks by time, colored by position  
- **Heatmap**: minute × position, overlay unmatched/duplicate bursts

---

### 10) Robustness, sensitivity, and “anti-p-hacking” design
Build a **specification curve** style report.

Vary:
- name key (S/M/N)
- fuzzy threshold(s)
- nickname on/off
- correction window X
- voter baseline: with vs without replacement
- inclusion/exclusion of top K common names (stress test)

For each spec, report:
- excess duplication percentile/p-value
- position skew effect size
- swing potential range

---

### 11) Common failure modes (and how this plan prevents them)

1) **Homonyms mistaken for repeats**  
   - Mitigation: voter-frequency baseline + per-name tail probabilities + rarity tiers  

2) **Cultural/common-name bias creating false positives**  
   - Mitigation: frequency adjustment; treat common names as high-ambiguity; don’t headline them  

3) **Over-normalization merges distinct people**  
   - Mitigation: multiple keys; strict vs medium; nickname tier reported separately  

4) **Under-normalization splits same person**  
   - Mitigation: nickname tier + conservative fuzzy matching inside blocks  

5) **Row-level dependence breaks tests**  
   - Mitigation: unique-name-level primary inference; permutation tests; cluster-aware CIs  

6) **Multiple testing produces spurious “top anomalies”**  
   - Mitigation: BH-FDR for screening; Holm for confirmatory subsets  

7) **Baseline mismatch (testifiers ≠ random voters)**  
   - Mitigation: voter baseline as one lens; add cross-hearing baseline where possible  

8) **Defamation/privacy risk**  
   - Mitigation: aggregate-first reporting; neutral language; show uncertainty and ambiguity; consider hashing names publicly  

---

### Recommended execution order (milestones)
1) **Canonicalization + key generation** (unit-tested; reproducible)  
2) **Descriptive duplication dashboards** under Key S/M/N  
3) **Voter-roll frequency table + rarity tiers**  
4) **Monte Carlo baseline** (overall + by position)  
5) **Per-name anomaly table** + BH-FDR  
6) **Position skew tests** (permutation + effect sizes) + swing potential panel  
7) **Voter linkage pipeline** + miss-rate panel (row + unique-name)  
8) **Temporal burst + time clustering** analyses  
9) **Composite scoring + sensitivity/spec curve report**  
10) **Public-facing narrative templates + limitations** (consistent language)