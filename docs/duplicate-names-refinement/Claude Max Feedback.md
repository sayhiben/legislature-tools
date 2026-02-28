# Statistical Review: Duplicate-Name Detector Methodology

**Date:** 2026-02-27
**Subject:** Review of `DuplicatesExactDetector` implementation and methodology memo

---

This is an impressively thorough and well-documented system. The level of self-awareness in Section 11 is commendable — you've already identified many of the right pressure points. This critique is organized from most consequential to least, focusing on statistical correctness, methodological soundness, and inferential validity.

---

## 1. The Hearing-Empirical Baseline Is Circular for Anomaly Detection

This is the most fundamental issue. When `collision_baseline_source = hearing_empirical`, the null distribution is estimated from the very data being tested. If duplicates are genuinely anomalous (i.e., someone submitted their name many times), those inflated counts enter the baseline histogram, inflating expected collisions and *reducing your power to detect the very thing you're looking for*.

This is not just a theoretical concern — it's a bias that runs in exactly the wrong direction. The more manipulation is present, the more the empirical baseline accommodates it, and the less significant your test becomes.

**Recommendation:** Treat the hearing-empirical path as descriptive only and never use it for inferential claims. When the registry baseline degrades to hearing-empirical, the `baseline_degraded` flag should additionally suppress all p-values and significance flags — not just annotate provenance. If you must keep it, consider a leave-one-out or jackknife variant where each name's expected count is computed from the histogram *excluding that name's contribution*, though this only partially addresses the problem.

---

## 2. Multinomial Model Assumption vs. Actual Data-Generating Process

The multinomial (iid draws with replacement from a fixed probability vector) is your workhorse model. Two issues:

### a) Independence assumption

Legislative hearing sign-ins are not independent draws from a population urn. People sign in because they care about the bill. Name frequencies in the testifier pool are driven by self-selection on the bill topic, organized advocacy campaigns, geographic proximity to the hearing, and demographic factors — none of which are captured by the voter registry frequency distribution. The registry tells you how common a name is *in the population*, but the testifier pool is a heavily non-random subset. This means your null model is wrong in a way that's hard to sign: organized communities may have correlated name frequencies that inflate collisions even without any duplicate submissions.

**Recommendation:** You should explicitly characterize this as a *reference model* rather than a generative model of the sign-in process. Consider calibration studies: run your detector on hearings where you have ground truth (no duplicates possible, or known duplicate counts) to assess the false positive rate empirically. Without this calibration, the p-values are not interpretable at their nominal levels.

### b) Population mismatch

The registry histogram reflects all registered voters. Testifiers are a non-representative subset. Name frequency distributions differ across demographic groups, and the testifier pool's demographic composition varies by bill. This is a form of *informative sampling* that your stratification by birth decade only partially addresses.

---

## 3. Multiple Testing Is Under-Controlled

You correctly apply BH-FDR to per-name tests within each scope. But the system as a whole conducts many families of tests:

- Scope-level collision metrics (pairs, excess_rows, repeated_group_rows) × scopes
- Bucket-level collision tests × buckets × scopes
- Per-name tests (BH-controlled within scope)
- Position permutation tests
- Temporal permutation tests per name

These are not independent test families — they share the same data and largely test related hypotheses. The risk is that a user scanning across all outputs will find *something* significant by chance and treat it as confirmatory.

**Recommendation:** At minimum, adopt a hierarchical testing framework: test scope-level metrics first as gatekeeping tests; only proceed to bucket-level and per-name tests if scope-level evidence exceeds a threshold. Alternatively, consider a closed testing procedure or use a global FDR approach (e.g., Benjamini-Yekutieli for dependent tests) across all hypothesis tests in a single detector run. At the very least, clearly report the total number of tests conducted so users can mentally calibrate.

---

## 4. Empirical P-Value Precision and One-Sidedness

### a) Precision ceiling

With a hard cap of 1000 draws at scope level and 250 at bucket level, your minimum achievable empirical p-value is approximately 1/1001 ≈ 0.001 and 1/251 ≈ 0.004, respectively. For bucket-level tests, this is coarse — you cannot distinguish moderately significant from highly significant results. If you're reporting these alongside BH-corrected per-name q-values (which can be much smaller), the resolution mismatch may confuse interpretation.

**Recommendation:** Either increase draw budgets for scope-level tests (10,000 is standard for publication-quality Monte Carlo p-values), or supplement with saddlepoint or normal approximations for the tail and report the MC p-value as a bound: "p < 1/n\_draws" rather than a point estimate.

### b) One-sidedness is defensible but should be explicit

For detecting *excess* duplication (the manipulation hypothesis), upper-tail tests are appropriate. But your z-scores are signed — a large negative z indicates suspicious under-duplication. You should either commit to one-sided inference throughout (and not report z-scores that invite two-sided interpretation) or provide two-sided p-values. The current hybrid (one-sided p, signed z) invites misinterpretation.

---

## 5. Stratified Hypergeometric Approximation via Rounding

Section 5.5 describes converting continuous probabilities to integer histogram counts via rounding. This is problematic for several reasons:

- Rounding destroys probability mass in the tails, where rare names live — and rare-name collisions are precisely the signal you care about.
- Class compression (binning rounded counts) further smooths the distribution.
- The approximation error is neither bounded nor characterized.

**Recommendation:** For the hypergeometric stratified path, implement direct finite-population sampling rather than going through the histogram intermediary. Specifically: maintain the per-key population counts as integers (which they are in the registry) and sample directly using multivariate hypergeometric draws. If computational cost is the concern, use Poisson approximation to the hypergeometric for keys with small `c_j/N` (which is most of them), and exact hypergeometric only for high-frequency keys.

---

## 6. Draw-Budget Scaling Heuristic

The `sqrt(n/400)` scaling factor for Monte Carlo budgets is unprincipled. It reduces draws for small hearings (where variance is highest and you most need simulation precision) and provides more draws for large hearings (where CLT-based analytic approximations are most reliable and you least need simulation).

This is backwards from an efficiency standpoint. Small-n problems have discrete, lumpy null distributions where analytic approximations are poorest — those are exactly the cases that benefit most from Monte Carlo.

**Recommendation:** Either use a fixed draw count (simplest), or scale draws *inversely* with the quality of analytic approximation. A practical approach: use analytic expectations + Poisson/normal approximation for large n (say n > 100), and reserve the full Monte Carlo budget for small n. The current minimum floor of 48 draws is also too low for reliable quantile estimation — 200+ is a reasonable minimum for estimating 5th/95th percentiles.

---

## 7. Low-Power Thresholds Need Calibration

Fixed cutoffs for `low_power_min_unique_names` and `low_power_min_expected_duplicates` are heuristic. The problem is that "low power" is a statement about a specific alternative hypothesis, and you haven't defined what alternative you're trying to detect.

**Recommendation:** Define a minimum detectable effect size (e.g., "we want 80% power to detect a 50% excess in duplicate rows over the null expectation") and derive the thresholds from that. For the binomial per-name test, this is straightforward power analysis. For the collision metrics, you can calibrate via simulation: inject known duplicate counts into synthetic data and measure detection rates as a function of n and effect size.

---

## 8. Temporal Permutation Tests Lack Multiplicity Control

Section 8.3 notes that temporal per-name p-values receive no BH correction. Given that these are computed for every name with ≥2 timestamps, this is a large family. In a hearing with hundreds of repeated names, you'd expect several to show significant temporal clustering by chance alone.

**Recommendation:** Apply BH correction across all temporal tests within a hearing, or (better) use a hierarchical approach: first test whether the overall distribution of minimum gaps across all names is shifted relative to the null, and only drill into per-name temporals if the global test is significant.

---

## 9. Smaller Issues

**RNG streams:** A single shared RNG means that adding or removing one stochastic component changes the draw sequence for all downstream components. This makes results fragile to code changes. Use independent `numpy.random.Generator` instances with derived seeds (e.g., `SeedSequence.spawn()`) for each sub-method.

**Scope fallback semantics:** When voter-match assignments are unavailable, `matched_only` silently becomes the full hearing. This is a semantic violation that could produce misleading results. Disable the scope entirely and report it as unavailable rather than silently substituting.

**Position claim eligibility:** Requiring *all* positions to be non-low-power is conservative but defensible. If you relax it, you'd need to account for differential power across positions in your claim language, which is complex. I'd keep the current strict gating.

**Detector vs. report baselines:** The volume-share expectation in the report layer is a different statistical quantity than the collision null expectation. This must be communicated to end users — ideally with different visual language (e.g., "proportional share baseline" vs. "collision null baseline") rather than just a footnote in a methods table.

---

## Summary Assessment

The core statistical machinery is sound in its definitions and internal consistency. The main risks are inferential validity risks: the hearing-empirical baseline circularity, the population mismatch between registry and testifier pool, and the under-controlled multiplicity across the full test battery. The Monte Carlo implementation is competent but has efficiency inversions in the draw-budget logic. The system would benefit most from empirical calibration against ground-truth hearings and a clearer separation between descriptive metrics and inferential claims.
