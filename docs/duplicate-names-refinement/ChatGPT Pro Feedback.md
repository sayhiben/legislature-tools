# Statistical critique of the Duplicate-Name Detector

My overall assessment: **the combinatorial math is mostly sound, but the inferential system is not yet well-calibrated for the real scientific question**. In its current form, I would treat this as a **descriptive / anomaly-screening framework**, not as a fully valid inferential engine for claims about “excess duplicate submissions.”

The strongest parts are the metric definitions, the analytic occupancy formulas, and the engineering around provenance. The weakest parts are the **choice of null**, **self-referential fallback behavior**, **error-control gaps**, and a few places where the report layer is statistically inconsistent with the detector layer.

## Bottom-line verdict

### What looks correct
- Your three collision metrics are well-defined and internally coherent:
  - `pairs = Σ C(X_g,2)`
  - `excess_rows = n - U`
  - `repeated_group_rows = n - S`
- The multinomial and hypergeometric expectations in Sections 5.2–5.3 are correct.
- The multinomial Monte Carlo sampler by histogram class is a valid simulation strategy for the compressed histogram representation.
- The empirical p-value formula with the `+1` correction is standard and defensible.
- The memo is unusually good at surfacing provenance, degradation, and effective-vs-requested settings.

### What is materially deficient
1. **The null model is not aligned with the actual data-generating process.**
2. **`hearing_empirical` is not a valid inferential baseline for the same hearing.**
3. **Some p-values are either not calibrated or are only partially calibrated.**
4. **The report layer contains at least one mathematically incorrect expectation.**
5. **Several fallbacks preserve execution but break statistical meaning.**

---

## 1. The biggest conceptual problem: you are testing name-collision, not repeat-submission

Your implementation is statistically about **name occupancy under a baseline name distribution**. That is not the same thing as **same person submitting multiple times**.

These are different phenomena:
- **Homonymy**: multiple distinct people share a name key.
- **Repeat submission**: one person submits multiple rows.
- **Name normalization collision**: canonicalization collapses distinct legal names into one key.
- **Selection effects**: hearings attract subpopulations with very different name distributions from the WA voter registry.

So the system is valid for the estimand:

> “How much more concentrated are observed name-key collisions than expected under a given name-frequency baseline?”

It is **not** valid for the stronger estimand:

> “How many duplicate submissions are likely to come from the same individual?”

### Recommendation
Split the problem into two layers:

**Layer A: collision burden**  
A descriptive / inferential measure of how many repeated **name keys** occur.

**Layer B: suspicious repeat-submission burden**  
A separate model that uses features associated with same-person repetition:
- same normalized name
- same position
- short time gaps / burstiness
- identical or near-identical metadata if available
- same voter match / same address / same email / same city if available

A good practical design:
1. Use name-count anomaly as a **screen**.
2. Condition on count and test:
   - **same-position concentration**
   - **temporal burstiness**
   - any available metadata concordance
3. Combine evidence into a composite suspiciousness score or posterior probability.

As written, your system risks users interpreting “excess duplicate names” as “likely repeated people,” which is too strong.

---

## 2. `hearing_empirical` is statistically unsuitable as an inferential baseline for the same hearing

This is the single most important technical flaw.

If you build the baseline from the observed hearing itself and then ask whether the hearing has “too many” collisions relative to that baseline, you have created a self-referential null.

### Why this is mathematically bad

Let observed counts be \(x_i\) over the names seen in the hearing, with \(n=\sum_i x_i\), and define the empirical baseline \(p_i=x_i/n\).

#### For `pairs`
Observed:
\[
O_{\text{pairs}} = \sum_i \binom{x_i}{2}
= \frac{1}{2}\left(\sum_i x_i^2 - n\right)
\]

Under multinomial resampling from the same empirical distribution:
\[
E_{\text{pairs}} = \binom{n}{2}\sum_i p_i^2
= \frac{n-1}{2n}\sum_i x_i^2
\]

Subtract:
\[
E_{\text{pairs}} - O_{\text{pairs}}
= \frac{n^2 - \sum_i x_i^2}{2n} \ge 0
\]

So under this self-baseline, **expected pair collisions are always at least observed pair collisions**, with equality only in the degenerate all-one-name case.

#### For `excess_rows`
Observed unique names are exactly the names already seen:
\[
U_{\text{obs}} = K
\]

But under resampling from the empirical distribution:
\[
E[U] = \sum_i \left(1-(1-p_i)^n\right) < K
\]
unless one category has probability 1.

Therefore:
\[
E[\text{excess\_rows}] = n - E[U] > n-K = \text{observed excess\_rows}
\]

So again, the self-baseline **systematically expects more duplication than was actually observed**.

#### For hypergeometric with hearing empirical baseline
If the hearing empirical histogram is treated as a finite population of size \(N=n\) and you sample \(n\) without replacement, the sample is the whole population. The null becomes essentially deterministic; you get the observed composition back.

That makes inferential output either vacuous or degenerate.

### Corrective action
When `collision_baseline_source = hearing_empirical`:
- Do **not** emit inferential p-values or q-values.
- Do **not** emit z-scores as if meaningful.
- Set an explicit status such as:
  - `inferential_status = descriptive_only`
  - `p_value = NA`
  - `q_value = NA`
  - `reason = self_referential_baseline`

If you want an empirical baseline, use one of:
- leave-one-hearing-out historical baseline
- committee/session-specific historical baseline
- cross-fitted baseline from all other hearings
- hierarchical empirical Bayes baseline estimated out-of-sample

---

## 3. The registry baseline is also mis-specified for hearing attendance

Using the WA voter registry as the population baseline is better than the same-hearing empirical baseline, but it is still not the true data-generating process.

Hearing attendees are not a simple random sample from the WA registry. They are selected by:
- issue area
- committee
- geography
- advocacy networks
- age
- political engagement
- possibly non-voter / non-resident / organization participation

That creates **extra-multinomial variation** relative to a fixed statewide name distribution. If you ignore that, you will tend to **underestimate variance** and overstate significance.

### Recommendation: use a hierarchical hearing-attendee baseline
A much better model is a **Dirichlet-multinomial** or related overdispersed occupancy model fitted to historical hearings.

For hearing \(h\):
\[
p_h \sim \text{Dirichlet}(\alpha \pi_h), \quad
X_h \mid p_h \sim \text{Multinomial}(n_h, p_h)
\]

Where \(\pi_h\) can depend on:
- committee
- chamber
- session year
- topic cluster
- sign-in position distribution
- matched/unmatched status

This solves two problems:
1. It uses a hearing-relevant baseline.
2. It captures between-hearing heterogeneity, which your current multinomial null does not.

If a full Dirichlet-multinomial is too heavy, a strong intermediate step is:
- estimate baseline key frequencies from historical hearings
- use empirical-Bayes smoothing
- calibrate posterior predictive collision metrics by simulation

---

## 4. `unmatched_only` with a voter-registry baseline is especially problematic

If a scope is defined as rows not matched to the voter registry, then the WA voter registry is usually the wrong baseline for that scope.

For unmatched rows:
- some names may be absent from registry
- some may be non-voters, out-of-state, or organizations
- registry-based \(p_i\) may be zero or near-zero for many legitimate names

That can produce spuriously tiny per-name p-values and distorted expected counts.

### Recommendation
- Do **not** use VRDB per-name inference for `unmatched_only` unless you have a principled unmatched baseline.
- For unmatched scopes, use:
  - historical unmatched sign-in data
  - a separate public-name corpus
  - or mark the scope descriptive-only

---

## 5. Stratification is underpowered and partly endogenous

`birth_decade` stratification is directionally good, but the implementation described has a major inferential weakness:

> scope-level stratum weights are estimated by proportional assignment of observed counts across a name's stratum distribution

This means the same observed names are used both to:
1. estimate the mixture weights, and
2. test whether those names are overrepresented.

That is a form of double use of the data. It shrinks the null toward the observed sample and invalidates exact p-values.

### Better alternatives
**Best**
- Use independently observed strata from matched participants:
  - actual matched birth decade
  - actual geography
  - actual sex if available and appropriate

**Acceptable**
- Estimate stratum weights from a separate sample:
  - matched-only subset
  - historical hearings of the same type
  - external demographic prior

**If you must estimate from the same hearing**
- Use leave-one-name-out or cross-fitting for per-name tests:
  - when testing name \(i\), estimate mixture weights without that name’s rows

Also: uncertainty in estimated mixture weights should be propagated. Right now the p-values appear to treat them as fixed.

---

## 6. The hypergeometric path adds complexity without much gain, and its current implementation is incomplete

If the hearing sample size is tiny relative to the population baseline size, the multinomial approximation is nearly exact. In a statewide registry, this is often the case.

Given that:
- hypergeometric simulation is not implemented for the collision null,
- stratified hypergeometric uses a rounding approximation,
- uncertainty bands disappear or degrade,

the hypergeometric branch currently seems more costly than valuable.

### Recommendation
Either:

**Option A: make multinomial the only inferential baseline**  
This is likely acceptable whenever \(n/N\) is very small.

**Option B: implement the finite-population model directly**  
If you keep hypergeometric:
- simulate directly from multivariate hypergeometric structures
- do not round probabilities back into a pseudo-histogram
- return proper MC quantiles and p-values

At present, the rounded probability-to-count histogram is an approximation I would not trust in the long tail.

---

## 7. Multiple testing control is incomplete

Current state:
- BH only for per-name tests within scope
- no comparable control for:
  - collision metrics across scopes/metrics
  - bucket tests
  - temporal tests
  - position-related claims
  - report-layer interactively inspected families

### Recommendation: define hypothesis families explicitly
A defensible hierarchy:

**Family 1: scope-level global collision tests**  
Pick one primary metric; I would recommend `excess_rows` as primary because it is the most interpretable.

**Family 2: per-name tests**  
Run only if Family 1 is significant for that scope.

**Family 3: within-name follow-up tests**  
Temporal burstiness, same-position concentration, etc., only for names selected from Family 2.

Use:
- BH if you are comfortable with its assumptions
- BY if you want guaranteed FDR under arbitrary dependence
- or permutation/maxT approaches if you want a stronger frequentist guarantee

---

## 8. The position permutation p-value is not valid as a one-sided p-value unless direction was pre-specified

You describe:
1. compute observed Pro–Con difference
2. choose the one-sided tail based on the observed sign

That is not a valid one-sided test unless the direction was specified before seeing the data.

### Correction
Use one of:
- two-sided permutation p-value based on \(|T|\)
- pre-specify the direction and keep it fixed
- report both one-sided tails transparently

### Additional issue: bootstrap CI
A “binomial resampling” CI is too naive here because duplicate-row indicators are not independent Bernoulli trials; they are induced by name clusters.

Use instead:
- cluster bootstrap by name key
- or permutation-based confidence intervals by test inversion

Also consider permuting within time buckets or agenda phases, because position and time can be confounded.

---

## 9. Temporal diagnostics need both multiplicity control and a better null

Problems:
- no BH/FDR across names
- three temporal statistics per name
- only tested for names with repeated occurrences
- null may not preserve relevant structure like position, agenda phase, or bursty site traffic

### Recommendation
At minimum:
- test temporal diagnostics only for names passing the per-name count screen
- apply BH across tested names for each temporal family, or use a hierarchical procedure

A better null would condition on:
- hearing-wide submission intensity over time
- position
- possibly bucket or agenda phase

A strong alternative is a scan-statistic or inhomogeneous Poisson-process approach:
- estimate baseline intensity \(\lambda(t)\)
- test whether a name’s repeated submissions are more clustered than expected under \(\lambda(t)\)

---

## 10. Monte Carlo budgets are too heuristic and too small for some uses

With \(B\) draws, Monte Carlo standard error of an estimated p-value is roughly:
\[
\sqrt{\hat p(1-\hat p)/B}
\]

For \(\hat p = 0.05\):
- \(B=250\) gives MCSE around 0.014
- \(B=1000\) gives MCSE around 0.007

That is coarse if users interpret thresholds around 0.05 or compare many buckets.

### Recommendation
Use precision-targeted sequential simulation, not fixed budgets:
- keep simulating until MCSE is below a target
- or until the confidence interval around \(\hat p\) is entirely above/below the decision threshold
- then stop

Also:
- report `n_draws`
- report MCSE or a Monte Carlo confidence interval for p
- treat quantiles from tiny draw counts as unstable

---

## 11. Returning `z=0, p=1` when inference is skipped is misleading

If Monte Carlo was not run, the correct state is:
- not estimated
- not inferentially available

It is not “exactly null-consistent.”

### Correction
Use explicit missing values and status fields:
- `p_value = NA`
- `z_score = NA`
- `expected_p05/p50/p95 = NA`
- `inferential_status = analytic_only_no_uncertainty` or `low_power_skipped`

The current behavior risks users interpreting “p=1” as evidence against anomaly, when it actually means “not computed.”

---

## 12. The report-layer `names_anywhere` expectation is mathematically wrong

You define:
- observed `unit_observed_names` = number of distinct duplicated-anywhere names appearing in bucket
- expected `unit_expected_names` = `bucket_n_rows * global_duplicated_names / total_rows_in_scope`

That linear formula is not the correct expectation for distinct names in bucket.

If duplicated name \(i\) appears \(m_i\) times globally, and a row lands in bucket \(b\) with probability \(w_b\), then:
\[
P(\text{name } i \text{ appears at least once in bucket}) = 1 - (1-w_b)^{m_i}
\]

So:
\[
E[\text{names in bucket}] = \sum_i \left[1-(1-w_b)^{m_i}\right]
\]

If you want fixed bucket size without replacement:
\[
E[\text{names in bucket}] = \sum_i \left[1-\frac{\binom{n-m_i}{n_b}}{\binom{n}{n_b}}\right]
\]

Your current formula \(w_b \times \text{global duplicated names}\) ignores multiplicities \(m_i\); it is not correct under the implied allocation model.

### Correction
For `names_anywhere`, use the exact occupancy expectation above, or simulate a permutation/allocation null that preserves multiplicities.

For `rows_anywhere`, the linear expectation is fine:
\[
E[\text{duplicated rows in bucket}] = n_b \cdot D / n
\]
where \(D\) is total duplicated rows.

---

## 13. Fallback semantics should preserve meaning, not just runtime

Several fallbacks are statistically unsafe:
- `matched_only` falling back to full hearing when assignments are missing
- `exclude_non_person_from_inference=True` but falling back to full frame if filter empties
- registry failure degrading to `hearing_empirical` while still emitting inferential-looking outputs

These are execution-friendly but inferentially dangerous.

### Recommendation
Unavailable scopes should be unavailable:
- `scope_status = unavailable_missing_match_assignments`
- `scope_status = unavailable_no_person_rows`
- `baseline_status = degraded_descriptive_only`

Do not silently reinterpret the estimand.

---

## 14. Key-mode inconsistency weakens interpretability

If the detector’s inferential baseline is built on one key definition and the timing/report views use another, users will conflate results across incompatible entities.

### Recommendation
- Pick one primary inferential key
- Carry it consistently through detector, timing, and reporting
- Treat strict/medium/loose/nickname as sensitivity analyses, not interchangeable UI modes

For duplicate-submission concerns:
- strict key as primary
- medium/loose only as sensitivity

---

## 15. Your tests validate algebra and contract stability, not statistical calibration

The current tests demonstrate:
- formulas agree with implementation
- Monte Carlo approximately matches analytic expectation
- payload contracts are stable

They do not establish:
- Type I error under realistic hearing-generation processes
- FDR under dependence and adaptive scope use
- power to detect injected repeat-submission patterns
- robustness to match uncertainty and missing strata

### Recommendation: add a calibration study
Simulate hearings from realistic baselines and inject controlled anomalies.

Evaluate:
- empirical false positive rate by scope
- empirical FDR for per-name calls
- power for repeated same-person submissions with:
  - same name
  - same position
  - bursty timing
- sensitivity to match-coverage loss
- sensitivity to loose vs strict keys

---

## What I would change first (priority order)

1. Disable inferential claims under `hearing_empirical` and degraded-to-empirical modes.
2. Fix the report-layer `names_anywhere` expectation.
3. Make unavailable scopes truly unavailable; do not silently redefine them.
4. Change the position permutation test to a valid two-sided procedure.
5. Adopt a single primary endpoint (`excess_rows`) and a formal hypothesis-family structure.
6. Move from statewide fixed multinomial to a historical hearing-based overdispersed baseline.
7. Replace heuristic MC budgets with precision-targeted simulation.
8. Add calibration experiments with synthetic duplicate injection.

---

## Final judgment

- Accuracy / correctness of the core occupancy math: good.
- Correctness of the inferential claims as currently framed: mixed.
- Appropriateness for the scientific question: limited.

In its current form, I would describe this system as:

> a well-engineered collision-analysis framework with valid combinatorial bookkeeping, but only partially valid statistical inference for hearing duplicate-name anomalies, and not yet a reliable inferential system for duplicate-person submissions.

The two most serious issues are:
- self-referential empirical baselines
- null-model mismatch between statewide name frequencies and hearing-specific participation processes

The most concrete outright bug is:
- the report-layer expected value for `names_anywhere` is mathematically wrong

The most concrete inference bug is:
- the sign-adaptive one-sided position permutation p-value
