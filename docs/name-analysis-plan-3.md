# Statistical Analysis Plan: Anomalous Duplicate Detection in Legislative Testimony Sign-In Data

---

## 1. Problem Framing and Null Hypothesis Structure

The core question is whether the observed rate of duplicate names in the testifier list — overall and partitioned by position — exceeds what would be expected by chance given the natural frequency of name collisions in the population.

This means every analysis below should be framed as a comparison between **observed** and **expected** rates, not raw counts. A list with 47 entries for "Maria Garcia" is only suspicious if the base rate of "Maria Garcia" in the population doesn't support it. This framing is what separates rigorous analysis from cherry-picking.

**Global null hypothesis (H₀):** The distribution of name duplicates in the testifier list is consistent with random sampling from the population (proxied by the voter roll), and there is no significant difference in duplicate rates across positions.

---

## 2. Data Preparation and Name Normalization

Before any analysis, you need a deterministic normalization pipeline applied identically to both the testifier list and the voter roll. Every downstream result depends on this being consistent.

### 2.1 Exact-Match Canonicalization
- Case-fold to lowercase
- Strip all non-alphabetic characters except hyphens and spaces (handles "O'Brien" vs "OBrien" vs "O Brien")
- Collapse multiple whitespace to single space
- Trim leading/trailing whitespace
- Standardize surname prefixes: "mc donald" → "mcdonald", "van der" handling, etc.
- Normalize suffixes: strip or standardize Jr/Sr/II/III

### 2.2 Nickname Expansion
Using your nickname lookup, for each first name, generate a **canonical name set** — a cluster of names that should be treated as equivalent. For example, {William, Will, Bill, Billy, Willy, Liam} all map to a single canonical cluster ID.

**Critical decision:** You must decide whether to do analysis at **two tiers** (and I strongly recommend you do):
- **Tier 1 — Exact match** after canonicalization only
- **Tier 2 — Fuzzy match** using nickname expansion + phonetic similarity

Run every downstream analysis at both tiers and report both. This lets readers assess how sensitive your conclusions are to matching assumptions. If the signal is strong, it should appear at Tier 1. If it only appears at Tier 2, the finding is weaker and you should say so.

### 2.3 Phonetic Encoding (Optional Tier 3)
For additional fuzzy matching, apply Metaphone or Double Metaphone encoding to both first and last names. This catches spelling variants ("Erikson" / "Erickson", "Meier" / "Meyer") that aren't nicknames. Soundex is too coarse for this purpose — Double Metaphone is preferable.

**Failure mode to avoid:** Do not union all three tiers into one mega-match. Each tier must be analyzed and reported independently, or you lose the ability to reason about match confidence.

---

## 3. Establishing the Population Baseline from Voter Rolls

The voter roll serves as your **expected distribution of name frequencies** in the relevant population. This is the foundation of every statistical test.

### 3.1 Name Frequency Distribution
From the voter roll, compute:
- **P(name):** The probability that a randomly selected individual has a given (last, first) name pair — i.e., count(name) / total_voters
- The full frequency distribution of these probabilities (how many names appear once, twice, 10 times, 1000 times)
- Separately: P(last_name) and P(first_name) marginal distributions

### 3.2 Expected Duplicate Rate Under Random Sampling
Given a sample of size *n* drawn from the voter roll distribution, the expected number of exact name collisions follows from the **birthday problem generalization**. For a testifier list of size *n*, the expected number of names appearing exactly *k* times is:

$$E[X_k] = \sum_{name} \binom{n}{k} P(name)^k (1 - P(name))^{n-k}$$

Or more practically, you can compute the expected collision rate by simulation: draw *n* names from the voter roll distribution (with replacement) many times (10,000+ iterations), and record the distribution of duplicate counts. This gives you a full **null distribution** of duplicates under random sampling, which you compare your observed data to.

**This is the single most important baseline in the entire analysis.** Without it, you cannot distinguish "there are a lot of duplicate names" from "of course there are, these are common names."

### 3.3 Stratified Baseline
Compute the above separately for different name-frequency strata:
- **Rare names** (appear ≤5 times in voter roll): duplicates here are much more suspicious
- **Common names** (appear 100+ times): duplicates here are expected
- **Medium names** (in between)

A duplicate "James Johnson" is very different from a duplicate "Xiomara Thistlethwaite." Your analysis must account for this.

---

## 4. Core Statistical Analyses

### 4.1 Analysis 1: Overall Duplicate Rate vs. Expected

**Method:** Compare the observed number of duplicate names (names appearing 2+ times) in the testifier list against the null distribution generated in §3.2.

**Test:** Compute an empirical p-value: What fraction of your simulated samples produced as many or more duplicates than observed? This is essentially a **Monte Carlo permutation test**.

**Report:**
- Observed duplicate count and rate
- Expected duplicate count (mean of null distribution) with 95% confidence interval
- Empirical p-value
- Effect size: (observed - expected) / std(null distribution), i.e., how many standard deviations above expected

**Failure mode:** If the testifier population is demographically different from the voter roll (e.g., skewed toward a particular ethnic group with more common name patterns), the baseline will be miscalibrated. Acknowledge this limitation explicitly. You can partially mitigate it by using only the name frequency distribution (not demographic weighting), since you don't have demographic data on testifiers.

### 4.2 Analysis 2: Duplicate Rate by Position (Pro/Con/Other)

This is the key partitioned analysis. If manipulation is occurring, you'd expect one position to show anomalous duplicate rates while others don't.

**Method:** For each position, treat the subset of testifiers as an independent sample and repeat Analysis 1. Then compare positions to each other.

**Test 1 — Within-position:** For each position, compute the empirical p-value of its duplicate rate against the null distribution (scaled to that position's sample size).

**Test 2 — Between-position:** Use a **chi-squared test of homogeneity** or **Fisher's exact test** to compare duplicate rates across positions. The contingency table is:

| | Unique names | Duplicate instances |
|---|---|---|
| Pro | a | b |
| Con | c | d |
| Other | e | f |

A significant result means the positions have different duplicate rates, which is itself noteworthy regardless of directionality.

**Test 3 — Rate ratio:** Compute the ratio of duplicate rates (e.g., Pro duplicate rate / Con duplicate rate) with a confidence interval via bootstrap. A ratio significantly different from 1.0 is evidence of asymmetry.

**Failure mode:** Unequal sample sizes across positions will affect power. If Pro has 5,000 entries and Con has 200, the Con subset may lack statistical power to detect anomalies. Report confidence intervals, not just point estimates, to make this visible.

### 4.3 Analysis 3: Per-Name Anomaly Scoring

Not all duplicates are equally suspicious. A name appearing 3 times when it's one of the most common names in the state is expected. The same name appearing 3 times when it's unique in the voter roll is extremely suspicious.

**Method:** For each name that appears *k* times in the testifier list, compute the probability of observing *k* or more occurrences under the null (random sampling from voter roll distribution). This is:

$$p_i = P(X_i \geq k_i) = 1 - \text{CDF}_{\text{Binomial}}(k_i - 1;\ n,\ P_i)$$

where *P_i* is the name's frequency in the voter roll and *n* is the testifier list size.

This gives each duplicate name an individual anomaly score (a p-value). Names with very small p-values are the most suspicious.

**Multiple testing correction:** You're computing this for many names simultaneously. Apply **Benjamini-Hochberg FDR correction** at q = 0.05 to identify which names are statistically significant after correction. Do not use Bonferroni — it's too conservative for this setting and you'll lose real signal.

**Output:** A ranked table of anomalous names with their occurrence count, expected occurrence, individual p-value, and FDR-adjusted p-value, partitioned by position.

### 4.4 Analysis 4: Temporal Clustering of Duplicates

If someone is signing in the same name multiple times, the entries may cluster in time (submitted in quick succession) rather than being spread across the hearing.

**Method:** For each duplicate name, compute the **inter-arrival times** between its appearances. Compare the distribution of these inter-arrival times against what you'd expect if the appearances were uniformly distributed across the hearing's time window.

**Test:** For names with 3+ occurrences, apply a **Kolmogorov-Smirnov test** against a uniform distribution over the hearing time range, or compute the coefficient of variation of the inter-arrival times. Tight temporal clustering (e.g., the same name appearing 5 times within 2 minutes) is far more suspicious than the same name spread across 3 hours.

**Aggregate version:** Across all duplicate names, compare the median inter-arrival time for duplicates vs. the expected inter-arrival time under uniform distribution. Do this separately by position.

**Visualization:** A strip chart or timeline showing all entries for each flagged duplicate name, with minute-resolution timestamps on the x-axis. This immediately reveals whether duplicates are temporally clustered.

### 4.5 Analysis 5: Voter Roll Match Rate by Position

**Method:** For each testifier, determine whether the name matches an entry in the voter roll (at both Tier 1 and Tier 2 matching). Compute the match rate per position.

**Hypothesis:** If one position has a significantly lower voter roll match rate, this could indicate fabricated names (which wouldn't match real voters) or out-of-state participants. Conversely, a very high match rate with unusually high duplicates could suggest names were drawn from the voter roll.

**Test:** Chi-squared test of homogeneity on:

| | Matched | Unmatched |
|---|---|---|
| Pro | a | b |
| Con | c | d |

**Important caveat:** A name match to the voter roll does not confirm identity — it only confirms the name exists. "John Smith" will always match. The informative signal is in the *interaction* between match rate and name rarity. A rare name that matches is more informative than a common name that matches.

**Refined version:** Compute a **weighted match rate** where each match is weighted by 1/P(name) — i.e., matches on rare names count more. Compare this weighted rate across positions. This is much more discriminating than the raw match rate.

### 4.6 Analysis 6: Name Frequency Distribution Shape Comparison

**Method:** Compare the full shape of the name-occurrence distribution (how many names appear once, twice, three times, etc.) between the testifier list and the null expectation.

Under benign conditions, you'd expect approximately a **Poisson-like** distribution of name occurrences, with parameters determined by the name frequency distribution and sample size. If there is stuffing, you'll see an **excess tail** — more names at high occurrence counts than the model predicts.

**Test:** Fit a **zero-truncated Poisson** or **negative binomial** model to the expected distribution, then test whether the observed distribution's tail exceeds the model using a **goodness-of-fit test** (chi-squared or likelihood ratio). Do this overall and per-position.

**Visualization:** A histogram of name occurrence counts (x = number of times a name appears, y = number of distinct names with that count), overlaid with the expected distribution under the null model. A visible excess in the right tail is the visual signature of stuffing.

---

## 5. Composite Anomaly Framework

Each of the above analyses captures one signal dimension. Names that are suspicious on multiple dimensions simultaneously are far more likely to represent genuine manipulation.

Define a composite anomaly flag for each duplicate name based on:

1. **Name rarity score** (how rare the name is in the voter roll)
2. **Occurrence excess** (how many times it appears vs. expected)
3. **Temporal clustering** (how tightly its appearances cluster in time)
4. **Position concentration** (are all duplicates on the same position, or split?)
5. **Voter roll match status**

A name that is rare, appears many times, is temporally clustered, all on one position, and matches the voter roll is maximally suspicious. You can rank names by the number of flags they trigger (a simple scoring approach), or use a more formal method like **Mahalanobis distance** across the feature dimensions if you want a continuous anomaly score.

---

## 6. Recommended Visualizations and Tables

### Tables
1. **Summary statistics table:** Total testifiers, unique names, duplicate count, duplicate rate — overall and by position. With expected values and confidence intervals from the null model.
2. **Top anomalous names table:** Ranked by FDR-adjusted p-value. Columns: name (redacted or hashed if privacy is a concern), occurrence count, expected count, p-value, adjusted p-value, position breakdown, temporal spread, voter roll match.
3. **Position comparison table:** Duplicate rate, weighted match rate, mean anomaly score — by position, with statistical test results.

### Visualizations
4. **Observed vs. expected duplicate histogram** (§4.6): The single most important visualization. Shows whether the tail of the occurrence distribution is fatter than expected.
5. **Null distribution with observed value marked** (§4.1): A histogram of simulated duplicate counts with a vertical line at the observed value. Instantly communicates whether the observed count is within or outside the normal range.
6. **Position-partitioned version of #5:** Same visualization, one panel per position. This is where asymmetry between positions becomes visually obvious.
7. **Timeline strip chart** (§4.4): One row per flagged duplicate name, dots at each timestamp. Reveals temporal clustering at a glance.
8. **Cumulative sign-in curve by position:** Time on x-axis, cumulative count on y-axis, one line per position. If one position shows sudden jumps (batch submissions), this will be visible as stair-step patterns.
9. **Name rarity vs. occurrence count scatter plot:** Each duplicate name as a point. X = log(voter roll frequency), Y = occurrence count in testifier list. Points in the upper-left (rare names, many occurrences) are the most suspicious. Color by position.
10. **Match rate comparison bar chart:** Voter roll match rate by position, with confidence intervals. Simple and direct.
11. **Heatmap of sign-in density:** Time (minute bins) on x-axis, position on y-axis, color intensity = sign-in rate. Reveals whether one position has suspicious temporal spikes.

---

## 7. Key Failure Modes and Mitigations

| Failure Mode | Risk | Mitigation |
|---|---|---|
| **Demographic mismatch** between voter roll and testifier population | Miscalibrated baseline — could inflate or deflate expected duplicates | Acknowledge limitation. Perform sensitivity analysis using subsets of the voter roll (e.g., by county if testifier geography is known). |
| **Nickname lookup incompleteness** | Missed fuzzy matches or false merges | Run at both Tier 1 and Tier 2. If conclusions differ, report both and explain why. |
| **Legitimate repeat testimony** | Some hearings allow sign-in for multiple bills or sessions | Verify hearing rules. If re-sign-in is permitted, this must be stated as a limitation. Check whether timestamps align with different sessions. |
| **Common name swamping** | A few very common names dominate aggregate statistics | Always use the per-name anomaly scoring (§4.3) alongside aggregates. Report results with and without the top 20 most common names. |
| **Multiple testing inflation** | Many simultaneous tests produce false positives | Apply FDR correction to per-name tests. For aggregate tests (§4.1, §4.2), pre-register your test plan (this document) to avoid p-hacking accusations. |
| **Small sample sizes** in position subgroups | Low statistical power, unstable estimates | Report confidence intervals everywhere. Use Fisher's exact test instead of chi-squared when cell counts are small (<5). |
| **Confirmation bias** | You stated you "highly suspect" manipulation — this can unconsciously influence analysis choices | Define the full analysis plan before looking at results (which you're doing now). Report all results, including null findings. Use two-sided tests. |
| **Privacy and defamation risk** | Publishing individual names as "suspicious" based on statistical flags | Never claim any individual committed fraud. Report aggregated statistics. If individual names are shown, present the statistical evidence neutrally ("this name appears X times, which has a probability of Y under the null model"). |

---

## 8. Reporting Framework

Structure the final report as:

1. **Methodology** — describe the null model, matching tiers, and tests (this plan)
2. **Baseline** — present the voter roll name frequency distribution and expected duplicate rates
3. **Aggregate findings** — overall duplicate rates vs. expected, by position
4. **Per-name findings** — anomalous names after FDR correction
5. **Temporal analysis** — clustering patterns
6. **Voter roll cross-reference** — match rates and weighted match rates
7. **Sensitivity analysis** — how results change across Tier 1/2/3 matching, with and without common names, etc.
8. **Limitations** — demographic mismatch, nickname coverage, inability to confirm identity from name alone

Every quantitative claim should be accompanied by a confidence interval or p-value. Every visualization should include the null expectation for comparison. The language should be careful: "statistically anomalous" rather than "fraudulent," since your data can identify patterns inconsistent with random sampling but cannot prove intent.

---

This plan gives you a methodologically defensible, multi-angle analysis. The strongest possible finding would be convergent evidence across multiple analyses: excess duplicates concentrated in one position, on rare names, temporally clustered, with the affected position showing a different voter roll match profile. Any single analysis alone could have an innocent explanation; the combination is what builds a compelling case.