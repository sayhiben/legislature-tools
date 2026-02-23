# Analytical Plan: Detecting Anomalous Duplicate Sign-ins in Legislative Testifier Lists

## Framing the Core Problem

Before diving into methods, it's important to establish what we're trying to distinguish: **benign duplicates** (two people sharing a name, common names appearing naturally) versus **fraudulent duplicates** (one person signing in multiple times, possibly with slight name variations). The analysis must be honest about what it can and cannot prove — we can establish statistical anomalies and raise red flags, but name matching alone cannot definitively identify fraud. This framing matters for how you present results.

---

## Part 1: Establishing a Name Frequency Baseline from Voter Rolls

### 1.1 Name Rarity Distribution

The central question for any duplicate name in the testifier list is: *how likely is it that two distinct real people share this name in Washington State?*

Using the voter rolls, compute the **surname frequency distribution** and the **full name frequency distribution** separately. For each unique name in the voter rolls, record its raw count and its per-capita frequency (count / total registered voters). This gives you a reference population.

The key metric is the **expected number of coincidental co-occurrences**: given that *k* people with a given name exist in the voter population, if you drew a random sample of testifiers, what's the probability that two or more would appear by chance? This is a hypergeometric sampling problem. If the testifier pool is small relative to the voter population, you can approximate it with a Poisson or binomial model.

Formally, for a name with frequency *p* in the voter population, if *n* total people testify, the expected number of appearances of that name is *np* and the variance is *np(1-p)*. A name appearing *k* times when the expected value is *μ = np* can be assessed with a **one-tailed Poisson test** or a binomial exact test to produce a p-value for "is this appearance count anomalously high given the name's rarity?"

Common names like "Smith, John" have high *p*, so multiple appearances are less surprising. Rare names have low *p*, so even two appearances may be statistically significant.

### 1.2 Name Rarity Tiers

Categorize names into rarity tiers based on voter roll frequency — for example: very common (top 1%), common (1–5%), uncommon (5–20%), rare (bottom 80%). This tiering will be a critical covariate in all downstream analyses. A duplicate "rare" name is far more anomalous than a duplicate "very common" name.

### 1.3 Surname vs. Full Name Analysis

Treat these separately. Surname duplicates are common and uninformative. Full name (Last, First) duplicates are more meaningful. However, note that the voter roll represents the full voting-eligible population of WA — not all will appear in any given hearing. The "effective population" that would plausibly testify on any given bill is much smaller, which makes the prior probability of coincidental duplicates even lower, strengthening the significance of anomalies for rare names.

---

## Part 2: Testifier List — Duplicate Name Analysis

### 2.1 Exact Duplicate Detection

The simplest baseline: normalize names (strip extra whitespace, standardize case, remove punctuation) and count exact duplicates within the testifier list. For each duplicate group, record:

- Name
- Count of appearances
- All associated positions (Pro/Con/Other)
- All associated timestamps
- Time deltas between consecutive sign-ins

**Time delta analysis** is particularly important. If the same name appears twice and the timestamps are 90 seconds apart, this is far more suspicious than appearances separated by hours (which might suggest a clerical error or legitimately two people). Construct a distribution of time deltas for all duplicate pairs and look for a bimodal distribution — one cluster near zero (suspicious rapid re-sign-in) and one distributed widely (likely coincidental or clerical). A **kernel density estimate (KDE)** of time deltas is a good visualization here.

### 2.2 Fuzzy Duplicate Detection

Since someone signing in fraudulently twice may vary their name slightly, you need fuzzy matching. The recommended approach is a **multi-stage blocking and matching pipeline**:

**Stage 1 — Blocking**: Reduce the comparison space by grouping names that share the same first three characters of the surname (or use Soundex/Double Metaphone phonetic encoding). This avoids O(n²) comparisons. Double Metaphone is preferred over Soundex for English names as it handles more phonetic edge cases.

**Stage 2 — Similarity Scoring**: For candidate pairs within each block, compute multiple string similarity metrics and combine them:
- **Jaro-Winkler distance**: Particularly well-suited for names; it gives extra weight to prefix agreement. Use this as your primary metric.
- **Levenshtein edit distance**: Good for catching typos.
- **Token sort ratio**: Useful if name components appear in different order (e.g., "Smith, John" vs. "John Smith" in mixed-format data).

**Stage 3 — Nickname Resolution**: Before computing string similarity, normalize first names through your nickname lookup table. Expand each first name to its full canonical set of variants (e.g., "Bill" → {Bill, William, Will, Willy, Billy}). Two names match on the first-name component if their canonical sets intersect. This should happen *before* Jaro-Winkler is applied.

**Stage 4 — Composite Scoring**: Combine surname similarity, first-name match (boolean after nickname expansion, or Jaro-Winkler if no nickname match), and any available metadata (organization field, if populated) into a **logistic regression or weighted composite score**. Without labeled ground truth, you'll need to manually calibrate thresholds — plan for a manual review step of borderline cases.

**Failure mode to avoid**: Fuzzy matching between a large dataset of names will produce many false positives, especially for common names. Always stratify your fuzzy match results by name rarity tier. A fuzzy match between two "John W." variants is meaningless; the same match for an unusual surname is highly meaningful.

### 2.3 Position-Stratified Duplicate Analysis

This is the core political question. For each duplicate or fuzzy-matched pair, record both the shared position and any position discordance:

- **Concordant duplicates**: Both appearances carry the same position (Pro/Pro or Con/Con). These are the primary fraud signal — they directly inflate one side's count.
- **Discordant duplicates**: Appearances carry different positions. These may indicate different people with the same name, or someone changing their mind (less likely to indicate fraud, but still worth flagging).

Compute the **concordance ratio** = (concordant duplicates) / (all duplicates). Under a null model where duplicate names are coincidental (different people), position assignments should be approximately independent and distributed proportionally to the overall Pro/Con ratio in the hearing. If the overall hearing is 60% Pro, a randomly selected pair of same-name individuals would be both-Pro with probability 0.36, both-Con with probability 0.16, and discordant with probability 0.48. A highly elevated concordance rate — especially for one specific position — is a strong statistical signal.

Test this with a **chi-squared goodness-of-fit test** or a **binomial exact test** against the null hypothesis of independent position assignment. Compute this separately for Pro-concordant and Con-concordant duplicates.

### 2.4 Temporal Clustering Analysis

Sign-in times provide a powerful secondary signal. Construct a **temporal sign-in sequence** and look for:

- **Burst patterns**: Are duplicate names clustered in time, suggesting coordinated sign-in? Use a **scan statistic** (Kulldorff's spatial scan statistic adapted to 1D time) to detect windows of elevated duplicate sign-in activity.
- **Regular interval duplicates**: If someone is systematically signing in multiple times, the inter-arrival times between their re-sign-ins may be suspiciously regular. This can be tested with a **Kolmogorov-Smirnov test** against a uniform distribution of sign-in times (under the null of random signing order).
- **Sign-in order vs. position**: Plot the cumulative proportion of Pro vs. Con sign-ins over time. A sudden step-change (many Pro or Con entries in a short burst) may indicate coordinated bulk sign-in, which is distinct from — but correlated with — name duplication fraud.

---

## Part 3: Voter Roll Match Rate Analysis ("Miss Rate")

### 3.1 Exact Match Rate

For each testifier, attempt an exact normalized match to the voter rolls. Record match/no-match per entry, and compute the **overall match rate**. Then compute the match rate **partitioned by position** (Pro, Con, Other). Under the null hypothesis that all groups are drawn from the same underlying population (WA registered voters), match rates should be approximately equal across positions. A significant difference in match rates by position is a meaningful anomaly.

Test this with a **two-proportion z-test** or a **chi-squared test of independence** (position × matched/unmatched). With three positions, use a 3×2 contingency table. Report the odds ratio for each position relative to the baseline, with confidence intervals.

### 3.2 Fuzzy Match Rate

Apply the same fuzzy matching pipeline (with nickname expansion) to produce fuzzy match rates by position. This will naturally be higher than the exact rate. Report both rates and the "gap" between them. A large fuzzy-vs-exact gap for a particular position may indicate systematic name entry inconsistencies (e.g., nicknames used more often by one group).

### 3.3 Interpreting Miss Rates — Critical Caveats

The miss rate has multiple legitimate causes that must be accounted for before interpreting any anomaly:

- **Out-of-state residents**: Some testifiers may be registered in other states (lobbyists, federal employees, advocates from other states). These will never match WA voter rolls. If you have organization data, out-of-state entities may partially explain misses.
- **Non-citizens with standing**: Some people may legally testify but not be WA registered voters.
- **Name entry errors**: Testifiers self-enter (or staff enters) names, introducing transcription variability.
- **Voter roll currency**: Voter rolls have a snapshot date; some people may have recently moved, died, or registered since the roll was generated.

Because of these confounds, the **absolute miss rate** is less meaningful than **relative differences in miss rate across positions**. Your analysis should focus on whether one position has a systematically higher miss rate than others, adjusted for observable covariates where possible.

---

## Part 4: Composite Anomaly Scoring

After running the above analyses, construct a **composite anomaly score** for each name (or name cluster) in the testifier list. This score should combine:

1. **Appearance count vs. expected count** (from the Poisson/binomial test, expressed as a z-score or -log10(p))
2. **Name rarity** (rare names with duplicates score higher)
3. **Concordance of positions** across duplicate appearances (concordant = more suspicious)
4. **Temporal proximity** of duplicate sign-ins (closer in time = more suspicious)
5. **Voter roll match status** (an unmatched name that appears multiple times is more suspicious)

You can combine these into a simple **unweighted sum of rank-standardized component scores**, which is interpretable and defensible, or use a **principal component analysis (PCA)** to derive a composite dimension that captures the most variance across these features — though PCA requires more careful interpretation. For reporting purposes, the transparent weighted-sum approach is usually preferable because it's auditable.

Rank all duplicate name groups by composite score and produce a sorted table for manual review. Flag everything above a threshold (e.g., 2 standard deviations above the mean score) for deeper investigation.

---

## Part 5: Visualizations

**Distribution charts:**
- KDE plot of name frequencies in voter rolls vs. in the testifier list, to see if testifier names are anomalously concentrated in rare-name space.
- Histogram of duplicate counts by name rarity tier (faceted by position) to immediately show where duplicates cluster.

**Temporal visualizations:**
- Swim-lane timeline of all sign-ins, colored by position, with duplicate names highlighted. This immediately reveals temporal clustering.
- Scatter plot of time delta between duplicate appearances (x-axis) vs. name rarity (y-axis), with point size encoding number of appearances and color encoding position concordance.

**Positional balance charts:**
- Before/after bar chart: show Pro/Con/Other ratio with all entries included vs. with duplicates removed (or de-duplicated to one entry per name). If there's a large shift, this is your headline finding.
- Stacked bar chart of concordant vs. discordant duplicates by position, compared to the expected baseline under the null.

**Match rate visualizations:**
- Grouped bar chart of exact and fuzzy match rates by position, with error bars representing 95% confidence intervals from a proportion test. If the CIs don't overlap between positions, that's a strong visual signal.

**Ranked anomaly table:**
- A sortable table of the top N anomalous name groups, showing: name, rarity tier, appearance count, positions, timestamps, time delta, voter roll match status, and composite anomaly score. This is the primary investigative tool.

---

## Common Failure Modes and Mitigations

**Inflation through name normalization errors**: If you normalize "Smith, Jr., John" differently from "Smith, John" you'll miss a duplicate. Build a robust normalization pipeline that handles suffixes (Jr., Sr., III), middle names/initials, and hyphenated surnames *before* any matching. Define your normalization rules explicitly and document them.

**Multiple testing problem**: You'll run many individual statistical tests (one per unique name, effectively). Apply a **Benjamini-Hochberg false discovery rate (FDR) correction** rather than a Bonferroni correction — Bonferroni is too conservative for this use case and will mask real signals. Report both raw p-values and FDR-adjusted q-values.

**Ecological fallacy in position analysis**: Finding that the Con group has more duplicates doesn't mean every Con-group duplicate is fraudulent. Be careful to report aggregate patterns rather than implying individual guilt, especially in a public-facing tool.

**Recency of voter rolls**: If the voter rolls are even six months old, thousands of registrations may be missing. This inflates miss rates universally. Document the roll snapshot date and note this limitation prominently.

**Self-referential fuzzy matching**: When fuzzy-matching within the testifier list, make sure you exclude self-matches (a name matching itself) and treat the comparison as undirected pairs, not directed, to avoid double-counting.

**Rarity bias in composite scoring**: Rare names will almost always score highest. Make sure your anomaly score accounts for this so you don't simply produce a list of unusual names rather than genuinely suspicious entries. The name-frequency-adjusted Poisson test component handles this if implemented correctly.

---

## Summary of Analyses to Perform, in Recommended Order

1. Voter roll name frequency analysis — establish your rarity baseline
2. Testifier list normalization and exact duplicate detection
3. Poisson/binomial significance tests for each duplicate name, using rarity baseline
4. FDR correction across all tests
5. Position concordance analysis for duplicate pairs, with chi-squared test
6. Temporal clustering analysis (KDE of time deltas, scan statistic)
7. Fuzzy matching pipeline (blocking → Jaro-Winkler + nickname expansion → composite score)
8. Voter roll match rate analysis, exact and fuzzy, partitioned by position
9. Two-proportion z-tests for match rate differences by position
10. Composite anomaly scoring and ranked anomaly table
11. Pre/post deduplication balance chart (headline visualization)

This plan is methodologically defensible, uses well-established statistical approaches, accounts for the major confounders, and produces both granular investigative outputs and high-level summary visualizations appropriate for public reporting.