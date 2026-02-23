# Statistical methodology for detecting anomalous duplicate names in testimony data

**A rigorous, simulation-centered approach can determine whether duplicate names in legislative hearing sign-in lists exceed chance expectations and concentrate disproportionately in one position category.** The methodology combines probabilistic record linkage, birthday-problem mathematics applied to name frequency distributions, Monte Carlo null models, and temporal anomaly detection—all calibrated against Washington state voter roll data. This plan draws on forensic statistics, election forensics precedents (notably the FCC net neutrality fake comments investigation), and established record linkage theory to produce results defensible in public policy and legal contexts.

The core analytical question breaks into three testable hypotheses: (1) the total number of duplicate names exceeds what random sampling from the population would produce, (2) duplicates concentrate disproportionately in one position category (Pro or Con), and (3) temporal patterns suggest coordinated rather than organic sign-in behavior.

---

## Phase 1: Data cleaning, name standardization, and duplicate detection

Before any statistical testing, raw name data must be rigorously standardized. The sign-in list's "Name (Last, First)" field should be parsed into separate last name and first name fields, converted to uppercase, stripped of punctuation (apostrophes, hyphens stored separately), and cleared of suffixes (Jr., Sr., III) and titles (Dr., Mr.). Hyphenated surnames like "Smith-Jones" should be retained in original form but also decomposed into components for matching against both "Smith" and "Jones."

**String similarity scoring** should follow a multi-level comparison framework. The Jaro-Winkler metric is the consensus best performer for personal names, originally developed by Census Bureau researchers for exactly this purpose. Its formula adds a prefix bonus to the base Jaro similarity: **JW(s₁, s₂) = Jaro(s₁, s₂) + ℓ × 0.1 × (1 − Jaro(s₁, s₂))**, where ℓ is the common prefix length (up to 4 characters). Standard thresholds are **≥ 0.95** for near-exact matches, **≥ 0.90** for high-confidence fuzzy matches, and **≥ 0.85** for moderate-confidence matches. Levenshtein edit distance (≤ 1 for short names, ≤ 2 for longer names) and phonetic algorithms (Double Metaphone for multi-ethnic coverage, Soundex for surnames) serve as complementary layers.

The **Fellegi-Sunter probabilistic record linkage model** provides the theoretical backbone. For each name-pair comparison, the model computes a match weight: **ω = log₂(m/u)**, where m is the probability of agreement given a true match and u is the probability of agreement given a non-match. The critical innovation for name-only linkage is **term-frequency adjustment**: agreement on a rare name like "Xiulan Zhao" receives far higher weight than agreement on "John Smith," because u (the probability two random non-matching people share a rare name) is minuscule. Modern implementations like Splink and fastLink estimate m and u parameters via Expectation-Maximization and produce posterior match probabilities with Bayesian uncertainty quantification.

For nickname handling, a lookup-table approach maps common variants to canonical forms (Bill/William, Bob/Robert, Dick/Richard, etc.) before comparison. Available dictionaries include the carltonnorthern/nicknames GitHub repository (US given names with diminutives), the LDC American English Nickname Collection (331,237 mappings from government records), and Deron Meranda's Census-based nicknames.txt. A 2023 study in BMC Medical Informatics found that nickname consolidation raised the F1 score from **0.807 to 0.905** while maintaining precision. The sensitivity analysis (Phase 6) should run all tests both with and without nickname expansion to quantify its impact.

**Recommended tools**: In R, the `fastLink` package implements Fellegi-Sunter with EM estimation and provides `dedupe.ids` for duplicate groups. In Python, the `recordlinkage` library offers flexible blocking, Jaro-Winkler comparison, and unsupervised Bernoulli EM classification. For string distances alone, R's `stringdist` and Python's `jellyfish` packages compute all major metrics efficiently.

---

## Phase 2: Establishing name frequency baselines from the voter roll

The Washington state voter registration database—freely available from the Secretary of State under RCW 29A.08.710, containing approximately **5 million records** with name, address, gender, date of birth, and registration date—provides the population baseline for expected name collision rates.

The central quantity is the **random match probability (RMP)**: **RMP = Σᵢ pᵢ²**, where pᵢ is the proportion of the voter roll with full name i. This is equivalent to the probability that two randomly selected individuals share the same name. Using the birthday problem generalization for non-uniform distributions, the **expected number of duplicate pairs among N testifiers** is:

> **E[duplicate pairs] = C(N, 2) × Σᵢ pᵢ²  =  N(N−1)/2 × RMP**

This formula follows directly from linearity of expectation applied to pairwise indicator variables. Non-uniform name distributions always increase collision probability relative to the uniform case—a critical result from Mase (1992) on the birthday problem with unequal probabilities, applied to Japanese surname data. The expected number of distinct names appearing two or more times is: **E[duplicate names] = Σₓ [1 − (1−pₓ)ᴺ − N·pₓ·(1−pₓ)^(N−1)]**.

Joint name probabilities should be estimated directly from full-name frequencies in the voter roll wherever possible. The independence assumption P(first, last) ≈ P(first) × P(last) systematically underestimates collision rates because ethnic correlations between first and last names concentrate probability mass (e.g., "José García" is far more common than independence predicts). Direct joint estimation is therefore both more accurate and more conservative—if excess duplicates are detected even under direct estimation, the finding is robust.

For names absent from the voter roll, Laplace smoothing (adding a small count) or the forensic genetics convention of **5/(2n)** for rare types provides a floor probability that avoids zero-probability artifacts.

---

## Phase 3: Statistical tests for excess duplicates

Three complementary testing frameworks should be applied, with the simulation approach serving as the primary method.

**Monte Carlo simulation test (primary)**. This is the most defensible approach because it makes no distributional assumptions and naturally handles non-uniform name frequencies. The procedure: (1) estimate name frequency distribution {p̂ₓ} from the voter roll; (2) for each of B ≥ 10,000 simulations, draw N names from this distribution with replacement and count duplicate pairs; (3) construct the empirical null distribution of the duplicate count; (4) compute **p = (1 + #{simulated ≥ observed}) / (1 + B)**. The 2.5th and 97.5th percentiles of the simulated distribution provide a 95% confidence interval for expected duplicates. To ensure p-value precision within ±0.01 at 99% confidence, approximately 17,000 simulations suffice. This test is easy to explain to non-statisticians: "We simulated what would happen if people signed in randomly 10,000 times, and the actual data has more duplicates than any simulation."

**Poisson exact test (analytical check)**. When each individual name probability is small, the total number of matching pairs is approximately **Poisson(λ)** where **λ = C(N,2) × RMP**. The one-sided Poisson test computes P(X ≥ D_obs | X ~ Poisson(λ)). This provides a rapid analytical cross-check; concordance between Poisson and Monte Carlo p-values strengthens the finding.

**Chi-squared goodness-of-fit (distributional test)**. For each name x, the expected count is E(x) = N × pₓ, and the test statistic **χ² = Σₓ (O(x) − E(x))² / E(x)** tests whether the entire name distribution is consistent with random draws. This detects anomalies beyond just duplicates—unusual names, missing expected names—but requires merging rare names to satisfy the expected-count ≥ 5 rule.

---

## Phase 4: Partitioned analysis comparing Pro, Con, and Other positions

The critical question—whether duplicates concentrate in one position—requires direct comparison of duplicate rates across groups. Set up a **2×2 contingency table** (duplicate status × position) for the primary Pro vs. Con comparison.

**Fisher's exact test** is preferred because duplicate counts are likely small, making chi-squared approximations unreliable. Fisher's test computes exact p-values from the hypergeometric distribution and is valid for all sample sizes. For the three-way comparison (Pro/Con/Other), the Fisher-Freeman-Halton exact test generalizes to r×c tables.

**Effect size measures** are essential alongside p-values. The **odds ratio** OR = (a×d)/(b×c) quantifies how much higher the odds of duplication are in one group versus the other, with 95% CI = exp(ln(OR) ± 1.96 × √(1/a + 1/b + 1/c + 1/d)). The **relative risk** RR = [a/(a+b)] / [c/(c+d)] is more directly interpretable: "Pro testifiers are RR times as likely to have a duplicate name." Since this is cross-sectional data, relative risk is the more appropriate measure.

**Permutation test for position-specific concentration** provides the most robust inference. Fix all testifier names as observed, randomly shuffle position labels (keeping marginal totals fixed) across B ≥ 10,000 permutations, and recompute the difference in duplicate rates. The p-value is the fraction of permuted differences exceeding the observed difference. This approach conditions on the observed names (automatically handling non-uniform frequency) and controls Type I error exactly under the exchangeability assumption.

A **hierarchical testing strategy** reduces multiple testing burden: first test whether overall duplicates exceed expectation (Phase 3); only if significant, proceed to the partitioned test (Phase 4). This gates the second test on the first, avoiding unnecessary correction. For pairwise comparisons among three groups (Pro vs. Con, Pro vs. Other, Con vs. Other), apply **Bonferroni correction** at α/3 = 0.0167, or preferably Holm's step-down procedure, which dominates Bonferroni under all conditions. In a legal/policy context where false accusations carry high cost, family-wise error rate control (Bonferroni/Holm) is more appropriate than the less conservative Benjamini-Hochberg FDR procedure.

**Bootstrap confidence intervals** for the difference in duplicate rates provide intuitive uncertainty quantification: resample Pro and Con groups separately with replacement, compute the difference in duplicate rates for each of 10,000 resamples, and report the 2.5th–97.5th percentile interval. If this interval excludes zero, the difference is significant at the 5% level.

---

## Phase 5: Temporal clustering and voter roll match rate analysis

### Temporal analysis at minute resolution

Under the null hypothesis of organic, independent arrivals, sign-in events should approximate a **homogeneous Poisson process** with exponentially distributed interarrival times. Three detection methods apply.

**Interarrival time analysis** tests whether gaps between consecutive sign-ins follow an exponential distribution. Compute interarrival times separately for Pro and Con streams. Apply a Kolmogorov-Smirnov goodness-of-fit test against the exponential distribution. An excess of very short gaps (clustering) rejects the null. The **index of dispersion** (variance/mean of event counts per time bin) equals 1 for Poisson data; overdispersion signals clustering.

**Kleinberg's burst detection algorithm** models the event stream as a hidden Markov model with states representing different arrival rates, identifying periods when activity shifts to a higher-intensity state. Applied separately to Pro and Con streams, it localizes temporal bursts. Available in Python (`burst_detection`) and R (`bursts`).

**Kulldorff's temporal scan statistic** slides windows of varying length across the timeline, computing likelihood ratios comparing observed to expected counts. Implemented in the SaTScan software, it identifies the most likely temporal cluster and provides Monte Carlo p-values. The **two-sample Kolmogorov-Smirnov test** directly compares Pro and Con temporal distributions: the test statistic D = max|F_Pro(t) − F_Con(t)| is sensitive to differences in both timing and shape, and a significant result means the two groups arrived with different temporal patterns.

Indicators of coordinated activity include temporal bursts concentrated in one position group, regular spacing suggesting an organized queue, and different temporal distributions for Pro versus Con (KS test significant). Organic patterns show roughly uniform arrival rates, similar patterns across groups, and temporally interleaved sign-ins.

### Voter roll match rate analysis

Match all testifier names against the WA voter roll and compute match rates by position. Washington's registration rate exceeds **80%** of voting-age residents, so expected match rates for genuine Washington residents providing real names should be approximately **75–85%** (accounting for matching failures, name variations, and unregistered individuals).

The **two-proportion z-test** compares match rates: **z = (p̂₁ − p̂₂) / √[p̂(1−p̂)(1/n₁ + 1/n₂)]**. For small samples, Fisher's exact test is appropriate. Confidence intervals for the difference use the Agresti-Caffo method. A significantly lower match rate in one group could indicate fabricated names (which would not appear on voter rolls), though alternative explanations must be considered: out-of-state participants, minors, non-citizens, or name format differences affecting matching accuracy.

---

## Phase 6: Sensitivity analysis and robustness assessment

The credibility of findings depends on demonstrating that conclusions hold across reasonable methodological choices. Five parameter dimensions require systematic variation.

**String matching thresholds** have the greatest impact. Run all tests at Jaro-Winkler thresholds of 0.80, 0.85, 0.90, 0.95, and 1.00 (exact match only). **Nickname inclusion** is the second most consequential parameter—run with and without nickname expansion and report both results. **Population baseline** should be tested using the full statewide voter roll, county-specific subsets, and (if available) age-restricted subpopulations. For the Monte Carlo simulation, verify that analytical and simulated expected values agree within simulation error as a calibration check.

Present sensitivity results as a summary table showing test statistics and p-values across all parameter combinations. Conclusions that persist across all reasonable parameter choices are robust; findings that appear only at specific settings should be flagged as fragile. A **specification curve analysis**—running the analysis under all defensible combinations of methodological choices and displaying the full distribution of results—provides the strongest evidence of robustness or fragility.

---

## Phase 7: Visualization strategy for communicating results

Seven core visualizations communicate the complete analysis.

**Null distribution histogram with observed value**. Plot the Monte Carlo distribution of simulated duplicate counts as a histogram, with a vertical red line marking the observed value and shading beyond it representing the p-value. This is the single most important visualization—it makes the statistical test immediately intuitive. Annotate with the observed count, expected count, and p-value.

**Log-log rank-frequency plot** of name frequencies from the voter roll, overlaid with the observed testifier name distribution. Deviations from the expected Zipfian pattern highlight specific names appearing more often than population frequencies predict.

**Timeline strip plot** showing each sign-in as a point on a time axis, colored by position. Temporal clustering and differential arrival patterns become immediately visible. Supplement with separate interarrival-time histograms for Pro and Con.

**Forest plot of effect sizes** displaying the odds ratio (or relative risk) for duplicate concentration in Pro vs. Con, with confidence intervals. If multiple hearings or bills are analyzed, stack them vertically for cross-hearing comparison.

**Side-by-side bar chart** comparing duplicate rates and voter roll match rates across Pro, Con, and Other groups, with error bars showing 95% confidence intervals.

**Sensitivity analysis heatmap** with string-matching threshold on one axis and analysis variant (with/without nicknames, statewide/county baseline) on the other, cell values showing p-values or effect sizes, using diverging color to indicate significance.

For policymaker audiences, use natural frequency formats ("out of 1,000 random groups this size, only 3 would show this many duplicates"), annotate charts with plain-language explanations, and follow Tufte's principles: maximize the data-ink ratio, eliminate chartjunk, and use small multiples for group comparisons. Design all visualizations to be legible in grayscale for printed documents.

---

## Critical failure modes and how to guard against them

**Cultural name frequency bias** is the single most dangerous pitfall. Surnames like Kim, Lee, Garcia, Nguyen, and Patel are orders of magnitude more common than average. A system calibrated to average surname frequency will generate massive false positives for these names. The mitigation is term-frequency adjustment in matching weights and using direct joint-name frequencies from the voter roll rather than independence assumptions.

**The prosecutor's fallacy** occurs when P(name match | innocent) is confused with P(innocent | match). If the probability that two random people share the name "John Smith" is 1 in 10,000, it does not follow that a "John Smith" appearing twice has only a 1/10,000 chance of being two different people. Bayesian reasoning with explicit prior odds is essential. The forensic statistics community (ENFSI guidelines) recommends reporting **likelihood ratios** rather than posterior probabilities.

**The base rate fallacy** compounds this problem. Even a 99% accurate duplicate detector, when applied to thousands of name pairs with a very low true-duplicate base rate, will produce mostly false positives. Always compute the **positive predictive value**, not just sensitivity and specificity.

**Selection bias** affects the baseline comparison. Legislative hearing testifiers are not a random sample from the voter roll—they are self-selected, politically engaged, potentially mobilized by organizations, and geographically concentrated near the capitol. Comparing testifier name frequencies to statewide voter roll frequencies without adjustment is imperfect, though this bias cuts against finding anomalies (political activists tend to have more diverse demographics, not less).

**Confirmation bias** in forensic analysis is well-documented. The PCAST Report (2016) emphasizes that forensic analysts are susceptible to cognitive bias affecting data collection, analysis, and interpretation. Mitigations include: pre-registering the analysis plan before examining data, conducting blind analysis where possible (analyzing without knowing which hearings are "suspicious"), actively seeking innocent explanations for every anomalous finding ("devil's advocate" analysis), and independent replication.

**Multiple testing** across many individual names, multiple hearings, or multiple test statistics inflates false positive rates. Use Bonferroni or Holm correction for pre-specified comparisons and Benjamini-Hochberg FDR control for exploratory screening. Report both corrected and uncorrected p-values.

**Simpson's paradox** can cause aggregate trends to reverse when stratified by confounders. An overall anomalous duplicate rate might disappear when stratified by hearing topic, time period, or cultural background. Always examine both aggregate and stratified results.

---

## Conclusion: A defensible analysis rests on four pillars

The methodology achieves rigor through **simulation-based inference** (Monte Carlo null models making no distributional assumptions), **multiple independent lines of evidence** (name frequency analysis, temporal patterns, voter roll matching), **comprehensive sensitivity analysis** (varying all consequential parameters), and **explicit acknowledgment of limitations** (what the analysis cannot prove). The most important principle: statistical anomaly detection identifies patterns warranting investigation, not proof of fraud. The analysis should state clearly that excess duplicates are consistent with manipulation but do not prove it—legitimate same-name individuals exist, data entry errors create artifacts, and coordinated but genuine advocacy can produce temporal clustering.

For maximum legal and policy defensibility, the analyst should pre-register the analysis plan, use conservative error-rate controls (Bonferroni/Holm), prioritize precision over recall (a higher match probability threshold of ≥ 0.90 avoids false accusations), report effect sizes alongside p-values, and present all sensitivity analyses transparently. The FCC net neutrality fake comments investigation—where the New York Attorney General used name analysis, temporal clustering, and identity verification to demonstrate that 18 million of 22 million public comments were fraudulent—provides the closest legal precedent and validates this general analytical framework.

Key methodological references include Fellegi and Sunter (1969) in JASA for probabilistic record linkage theory; Enamorado, Fifield, and Imai (2019) in the American Political Science Review for the fastLink implementation; Mase (1992) in the Annals of the Institute of Statistical Mathematics for the birthday problem with unequal name frequencies; Kleinberg (2003) for burst detection; and Kulldorff (1997) for temporal scan statistics. The New York Attorney General's 2021 report "Fake Comments: How U.S. Companies and Partisans Hack Democracy" documents the most directly relevant applied precedent.