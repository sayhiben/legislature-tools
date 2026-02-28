I read the current-state memo first and treated it as the authoritative description of behavior, then reconciled the two review memos against it. The two critiques mostly agree on substance: the core counting math is solid, but the product needs tighter claim language, stricter inferential guardrails, cleaner fallback semantics, and better calibration before it should speak confidently about anomalies.  ￼  ￼  ￼

A few places needed an explicit resolution:
	•	Keep one-sided upper-tail inference for collision and per-name screening, because the tool’s stated job is to flag over-concentration under a reference baseline, not to test for any deviation in either direction.
	•	Switch the position permutation test to two-sided, because the current implementation chooses the one-sided direction after seeing the sign, which is not valid.
	•	Do not build a public-facing “same-person duplicate estimate.” Split the product into:
	1.	collision burden on name keys, and
	2.	secondary submission-pattern signals from public data such as timing and position concentration.
	•	Make strict key the inferential default. Keep nickname-based loose matching as a sensitivity analysis only. Do not add phonetic or typo matching; nickname dictionary expansion is the only matching extension in scope.
	•	Treat the current VRDB-based multinomial model as a reference baseline, not as a faithful generative model of hearing attendance. Add a hearing-relevant historical baseline later.  ￼  ￼  ￼

Below is the sequenced backlog I would hand to engineering.

⸻

1) P0 — Establish the statistical product contract and public claim language

Why this exists
Right now the system mixes collision metrics, per-name significance, position/temporal diagnostics, and report-layer concentration charts in a way that can sound more definitive than the public data supports. Both critiques recommend reframing the system as an anomaly screen / reference-model comparison rather than a duplicate-person or intent detector, which matches your stated goal.  ￼  ￼  ￼

Goals
	•	Define a product-wide contract for what the tool does and does not claim.
	•	Separate:
	•	Duplicate-name collision burden
	•	Submission-pattern signals
	•	Unavailable / descriptive-only states
	•	Make every public output declare its baseline and inferential status.

Specific changes
	•	Add explicit enums/fields such as:
	•	claim_class: collision_signal, submission_pattern_signal, descriptive_only, unavailable
	•	inferential_status: reference_model_inference, descriptive_only, unavailable
	•	baseline_label: statewide_registry_reference, historical_hearing_reference, same_hearing_empirical, proportional_share_baseline
	•	Rewrite public-facing labels/tooltips so they say things like:
	•	“higher than expected under the selected reference baseline”
	•	“signal for follow-up”
	•	“not proof of duplicate-person submissions”
	•	Remove any copy that implies identity, motive, or certainty.
	•	Explicitly document that public data can flag suspicious concentration patterns, but not determine intent or person-level duplication.

Acceptance criteria
	•	No public text describes outputs as proof of repeated-person submissions or intent.
	•	Every chart/table/exported record includes baseline labeling and inferential status.
	•	The methodology page has a plain-language section titled something like “What this tool can and cannot conclude.”

Test / verification
	•	Snapshot tests for report labels and payload fields.
	•	Copy audit on at least three representative reports.
	•	Regression test ensuring legacy tables do not reintroduce stronger language.

⸻

2) P0 — Make hearing_empirical and degraded baselines descriptive-only; replace fake null outputs with NA

Why this exists
The current memo says hearing_empirical is a supported baseline path and that when Monte Carlo samples do not exist the system can emit deterministic expectations with z=0 and p=1. Both are statistically misleading. The reviews are aligned that same-hearing empirical baselines are circular for anomaly detection, and skipped inference must not masquerade as evidence for the null.  ￼  ￼  ￼

Goals
	•	Make self-referential or degraded baselines descriptive-only.
	•	Replace “not computed” with NA, never p=1, z=0, or false certainty.
	•	Propagate this consistently across scope, bucket, and per-name outputs.

Specific changes
	•	If collision_baseline_source = hearing_empirical, set baseline-driven inferential fields to null/NA:
	•	p_value
	•	q_value
	•	z_score
	•	expected_p05/p50/p95
	•	significant
	•	Do the same when registry failure degrades to hearing empirical.
	•	Add reason codes such as:
	•	self_referential_baseline
	•	baseline_degraded_to_empirical
	•	analytic_only_no_uncertainty
	•	low_power_skipped
	•	Keep raw observed metrics and descriptive expected values if useful, but clearly mark them as non-inferential.
	•	Ensure summary and legacy compatibility tables follow the same semantics.

Acceptance criteria
	•	In hearing-empirical and degraded-to-empirical modes, no baseline-derived p/q/z/quantile fields are populated.
	•	A consumer cannot confuse “not inferentially evaluated” with “non-significant.”
	•	Report UI shows a descriptive-only badge or equivalent.

Test / verification
	•	Unit tests for hearing_empirical, degraded registry fallback, analytic-only mode, and zero-draw cases.
	•	Schema tests for nullability and reason codes.
	•	Snapshot tests verifying UI badges/tooltips.

⸻

3) P0 — Preserve scope semantics; remove silent fallback that redefines the estimand

Why this exists
The current implementation says matched_only can silently become a copy of the full hearing when voter-match assignments are missing, and that non-person filtering can fall back to the full working frame if it empties the inference frame. Both reviews correctly flag this as semantically unsafe: execution survives, but the scope no longer means what the user thinks it means.  ￼  ￼  ￼

Goals
	•	Preserve the meaning of full_hearing, matched_only, and unmatched_only.
	•	Mark unavailable scopes as unavailable, rather than silently substituting different data.
	•	Make scope availability explicit in both API and UI.

Specific changes
	•	If match assignments are missing or malformed:
	•	matched_only => unavailable
	•	unmatched_only => unavailable
	•	If exclude_non_person_from_inference=True empties the scope:
	•	mark scope unavailable
	•	do not fall back to the full working frame
	•	Add scope_status fields such as:
	•	available
	•	unavailable_missing_match_assignments
	•	unavailable_no_person_rows
	•	unavailable_no_rows_after_filtering
	•	Update frontend controls so unavailable scopes are hidden or disabled with a reason.

Acceptance criteria
	•	No scope can silently alias another scope.
	•	matched_only and unmatched_only never emit results unless their inputs are truly available.
	•	Users can see why a scope is unavailable.

Test / verification
	•	Unit tests for missing assignments, empty post-filter scopes, and malformed assignment payloads.
	•	Frontend integration tests for disabled/hidden scope controls.
	•	Regression tests ensuring no fallback copies appear in outputs.

⸻

4) P0 — Fix the names_anywhere bucket expectation and clearly separate detector nulls from report baselines

Why this exists
The current memo explicitly says the report-layer bucket concentration chart rebuilds expectations from payload logic and uses unit_expected_names = bucket_n_rows * global_duplicated_names / total_rows_in_scope. The ChatGPT review correctly identifies this as mathematically wrong for distinct names-in-bucket; Claude also flags the detector/report baseline mismatch as something users can easily conflate.  ￼  ￼  ￼

Goals
	•	Correct the names_anywhere expectation.
	•	Preserve rows_anywhere if its current linear expectation is intended.
	•	Make it impossible to confuse:
	•	detector-native collision nulls
	•	report-layer proportional allocation baselines

Specific changes
	•	Replace unit_expected_names with an occupancy-based expectation using duplicated-name multiplicities.
	•	Exact fixed-bucket-size version preferred:
E[\text{distinct duplicated names in bucket}] = \sum_i \left[1 - \frac{\binom{n-m_i}{n_b}}{\binom{n}{n_b}}\right]
	•	If implementation simplicity wins, use a clearly documented approximation and test it.
	•	Keep unit_expected_rows linear if that is the desired proportional-share quantity.
	•	Rename the report baseline to something like:
	•	“proportional allocation baseline”
	•	not “expected duplicates” without qualification
	•	Add tooltip text explaining that this chart does not use the same null as detector-level collision inference.

Acceptance criteria
	•	names_anywhere expectation matches closed-form toy examples.
	•	Report labels distinguish proportional-share expectations from collision-null expectations.
	•	No chart or tooltip implies these are interchangeable baselines.

Test / verification
	•	Unit tests on small exact examples where the correct expectation is computable by hand.
	•	Regression tests for payload builder outputs.
	•	UI snapshot tests for labeling.

⸻

5) P1 — Formalize the hypothesis families, primary endpoint, and multiple-testing control

Why this exists
Current behavior applies BH only to per-name tests within scope, while scope metrics, bucket tests, position tests, and temporal tests are not governed by a single family structure. Both reviews call out incomplete multiplicity control. The cleanest repair is to reduce the number of primary inferential claims and gate follow-ups.  ￼  ￼  ￼

Goals
	•	Define exactly what counts as a primary inferential claim.
	•	Control false discoveries across the full detector workflow.
	•	Reduce exploratory outputs being mistaken for confirmatory findings.

Specific changes
	•	Choose excess_rows as the primary scope-level endpoint.
	•	Treat pairs and repeated_group_rows as secondary descriptive/exploratory metrics unless explicitly promoted later.
	•	Define families:
	•	Family A: scope-level excess_rows across available scopes
	•	recommended adjustment: Holm
	•	Family B: bucket-level follow-ups within Family A discoveries
	•	recommended adjustment: BH across buckets within scope
	•	Family C: per-name upper-tail tests within Family A discoveries
	•	recommended adjustment: BH within scope
	•	Family D: temporal follow-ups only for Family C discoveries
	•	recommended adjustment: BH within temporal family
	•	Position: one corrected test per eligible scope; if multiple scopes are tested, adjust across scopes
	•	Add metadata:
	•	family_id
	•	adjustment_method
	•	n_tests_in_family
	•	eligible_by_gate

Acceptance criteria
	•	Every inferential p/q-value belongs to a declared family.
	•	Downstream tests only run when their gate conditions are met.
	•	The summary/export includes total test counts and adjustment methods.

Test / verification
	•	Unit tests for family construction and gating.
	•	Integration tests showing downstream tables disappear or become descriptive when Family A does not pass.
	•	Golden-file tests for family metadata.

⸻

6) P1 — Repair the position concentration method

Why this exists
The current memo says the position permutation test chooses a one-sided p-value based on the observed sign and uses a binomial bootstrap CI. The reviews agree this is the clearest inferential bug in the current implementation. Claude also argues the current strict eligibility gating is defensible; I agree and would keep it for now.  ￼  ￼  ￼

Goals
	•	Make the position test valid.
	•	Keep claim eligibility conservative.
	•	Use an interval method that respects dependence induced by name clusters.

Specific changes
	•	Replace the sign-adaptive one-sided permutation p-value with a two-sided permutation test based on |rate_diff|.
	•	Keep current strict eligibility gating unless calibration proves a better alternative.
	•	Replace binomial bootstrap CI with one of:
	•	cluster bootstrap by name key (recommended first)
	•	permutation-inverted interval (later, if needed)
	•	If time-confounding proves material, add an optional stratified permutation path within coarse time buckets or agenda phases.

Acceptance criteria
	•	No p-value direction is chosen after observing the effect sign.
	•	Position intervals are no longer based on independent Bernoulli resampling of rows.
	•	Public copy describes this as a position imbalance signal, not proof of manipulation.

Test / verification
	•	Null simulations showing approximately uniform p-values under exchangeability.
	•	Coverage study for cluster bootstrap intervals.
	•	Regression tests for claim-eligibility gating.

⸻

7) P1 — Repair temporal diagnostics with gating, multiplicity control, and a better null

Why this exists
The current memo says temporal permutation p-values are computed for names with repeated timestamps and are not BH-corrected across names. Both reviews flag multiplicity problems, and ChatGPT also asks for a null that better preserves hearing-wide structure.  ￼  ￼  ￼

Goals
	•	Stop temporal p-values from operating as an unbounded exploratory family.
	•	Preserve more of the hearing’s empirical time structure in the null.
	•	Position temporal diagnostics as follow-up signals, not first-line claims.

Specific changes
	•	Only run temporal inferential tests for names that pass the per-name screen in Family C.
	•	Apply BH correction across names within each temporal family:
	•	minimum gap
	•	within-5-minute count
	•	within-15-minute count
	•	Upgrade the null so it preserves the empirical hearing submission intensity curve; if feasible, condition on position or other coarse strata.
	•	When data are too sparse to support conditioned temporal nulls, downgrade to descriptive-only temporal summaries.

Acceptance criteria
	•	No name gets an inferential temporal p-value unless it passes the configured gate.
	•	Temporal outputs include family-specific q-values or are labeled descriptive-only.
	•	Null resampling preserves the overall hearing time intensity by construction.

Test / verification
	•	Null simulations for temporal FDR.
	•	Injected-burst scenarios to measure power.
	•	Regression tests for gating and q-value propagation.

⸻

8) P1 — Overhaul Monte Carlo precision, stopping rules, and RNG reproducibility

Why this exists
The current draw-budget heuristic scales with sqrt(n/400), has low minimum floors, and shares a single RNG stream across components. Both reviews criticize the budget logic, and Claude also points out order-sensitivity from a shared RNG. The current memo confirms those implementation choices.  ￼  ￼  ￼

Goals
	•	Make simulation precision target-driven rather than heuristic.
	•	Improve reproducibility under code changes.
	•	Surface Monte Carlo uncertainty honestly.

Specific changes
	•	Replace fixed heuristic budgets with sequential Monte Carlo:
	•	run in batches
	•	stop when MCSE is below a target or the decision boundary is resolved
	•	Use a higher minimum draw floor for quantiles and threshold-adjacent p-values.
	•	Report:
	•	n_draws
	•	mcse
	•	optionally a Monte Carlo confidence interval or p < 1/(B+1) style bound
	•	Spawn independent RNG streams per sub-method using SeedSequence.spawn() or equivalent.
	•	Remove any remaining code path that converts “simulation skipped” into a fake exact null output.

Acceptance criteria
	•	Small hearings no longer receive the least simulation effort by default.
	•	Independent components do not perturb each other’s random sequences.
	•	Simulation-backed outputs report their precision.

Test / verification
	•	Unit tests for stopping rules and MCSE calculations.
	•	Reproducibility tests showing scope-level results do not change when unrelated stochastic sub-methods are added or removed.
	•	Performance tests to keep runtime within acceptable bounds.

⸻

9) P1 — Lock the entity-definition policy: strict key primary, nickname mode sensitivity-only

Why this exists
The current memo says detector inference can use one collision key while timing/report views use other keys, creating interpretability drift. The ChatGPT review recommends a single primary inferential key; your explicit constraint is that matching should not expand beyond nickname lookups.  ￼  ￼

Goals
	•	Make the inferential entity definition stable and understandable.
	•	Preserve nickname support without broad fuzzy matching.
	•	Align report views with detector semantics.

Specific changes
	•	Set strict key as the default inferential key for collision tests.
	•	Keep medium/nickname modes as sensitivity analyses, not interchangeable primary views.
	•	Ensure top-name timing and per-name report modes either:
	•	use the same key as the active detector mode, or
	•	are explicitly labeled “sensitivity view”
	•	Introduce a versioned nickname dictionary process:
	•	add nicknames via curated lookup updates only
	•	no phonetic matching
	•	no edit distance / typo correction

Acceptance criteria
	•	The user can always tell which key produced an inferential result.
	•	No UI control silently swaps the grouping entity.
	•	Nickname expansion is the only matching flexibility added.

Test / verification
	•	Regression tests on key resolution and mode labeling.
	•	Snapshot tests for “primary vs sensitivity” badges.
	•	Fixture tests for curated nickname additions.

⸻

10) P2 — Add a hearing-relevant historical reference baseline and out-of-sample stratification

Why this exists
Both critiques agree the statewide VRDB baseline is not the hearing attendance process. ChatGPT goes further and recommends a historical hearing-attendee / overdispersed baseline and non-endogenous strata weights; Claude recommends explicitly calling the current model a reference model and empirically calibrating it. The best synthesis is: keep the current registry model as a labeled reference comparator, but build a better hearing-relevant baseline for default inference.  ￼  ￼  ￼

Goals
	•	Replace the “statewide voter registry as default inferential truth” idea.
	•	Use out-of-sample historical hearings to better match attendance composition.
	•	Eliminate same-hearing estimation of strata weights.

Specific changes
	•	Build a historical baseline keyed by:
	•	committee
	•	chamber
	•	session/year
	•	topic cluster if available
	•	Exclude the target hearing from baseline training.
	•	Implement an overdispersed predictive model:
	•	Dirichlet-multinomial or
	•	empirical-Bayes smoothed historical frequencies with posterior predictive simulation
	•	Estimate birth-decade mixture weights from:
	•	historical matched hearings
	•	or a held-out matched subset
	•	never from the same hearing under test
	•	Keep VRDB as a comparator or fallback, but not the only inferential default.

Acceptance criteria
	•	The baseline training set never includes the target hearing.
	•	Stratification weights are externally estimated or stratification is disabled.
	•	Calibration shows better false-positive control than the current statewide reference model.

Test / verification
	•	Data lineage tests for leave-one-hearing-out exclusion.
	•	Simulation and backtesting against historical hearings.
	•	Comparative calibration report: legacy VRDB vs historical baseline.

⸻

11) P2 — Rationalize unmatched_only inference and simplify/complete hypergeometric support

Why this exists
The ChatGPT review is right that unmatched_only combined with a voter-registry baseline is especially fragile, and both reviews criticize the stratified hypergeometric approximation that rounds probabilities back into histogram counts. The current memo confirms that this approximation exists and that full hypergeometric uncertainty support is incomplete.  ￼  ￼  ￼

Goals
	•	Stop unsupported inferential paths from appearing authoritative.
	•	Remove or quarantine approximate hypergeometric inference until it is correct.
	•	Make unmatched_only honest about baseline limitations.

Specific changes
	•	Until a dedicated unmatched baseline exists:
	•	unmatched_only should be descriptive-only or unavailable for per-name / collision inference
	•	Disable inferential hypergeometric paths that rely on rounded probability-to-histogram reconstruction.
	•	If finite-population support is genuinely needed later, implement:
	•	direct multivariate hypergeometric sampling from integer counts, or
	•	a justified approximation such as Poisson for small fractions
	•	Keep analytic hypergeometric expectations only if they are clearly labeled descriptive or expectation-only.

Acceptance criteria
	•	No inferential p-value is emitted for unmatched_only under a VRDB-only baseline without a dedicated unmatched reference population.
	•	No rounded-histogram hypergeometric Monte Carlo is exposed as calibrated inference.
	•	Config/UI make these limits explicit.

Test / verification
	•	Mode-guard tests for unmatched-only inference.
	•	Exact toy-case tests for any future direct hypergeometric sampler.
	•	Regression tests ensuring disabled paths stay disabled.

⸻

12) P2 — Build a calibration harness and operating-characteristics study; use it to set low-power thresholds

Why this exists
The current test suite validates formulas, invariants, and payload contracts, but not Type I error, FDR, power, coverage, or robustness under realistic hearing-generation scenarios. Both reviews say calibration is the missing evidence base, and Claude explicitly calls out the current low-power thresholds as heuristic rather than target-driven.  ￼  ￼  ￼

Goals
	•	Turn “statistically plausible” into “empirically calibrated.”
	•	Derive low-power thresholds from operating targets, not fixed intuition.
	•	Create a release gate for future methodological changes.

Specific changes
	•	Build a synthetic hearing generator with scenarios covering:
	•	clean null under hearing-specific name distributions
	•	homonym-heavy but legitimate attendance
	•	repeated-name injections
	•	same-position concentration
	•	temporal bursts
	•	match-coverage loss
	•	strict vs nickname sensitivity
	•	stratification error
	•	Measure:
	•	scope-level Type I error
	•	per-name FDR
	•	power under injected anomalies
	•	CI coverage
	•	effect of low-power gates
	•	Use results to set:
	•	low_power_min_unique_names
	•	low_power_min_expected_duplicates
	•	family-specific gating thresholds
	•	Produce a versioned calibration report as a release artifact.

Acceptance criteria
	•	Thresholds are backed by documented operating-characteristic targets.
	•	Any change to baseline/modeling/inference code can be checked against calibration benchmarks.
	•	The repo includes a reproducible calibration script/report.

Test / verification
	•	CI smoke suite on reduced simulation settings.
	•	Periodic full calibration run in release workflow.
	•	Regression thresholds that must be met before shipping inferential changes.

⸻

Recommended implementation order

I would schedule this in three milestones:

Milestone A — Safe public semantics and correctness patch
	1.	Ticket 1
	2.	Ticket 2
	3.	Ticket 3
	4.	Ticket 4

This is the minimum bar before any public-facing release that uses inferential language.

Milestone B — Defensible screening engine
5. Ticket 5
6. Ticket 6
7. Ticket 7
8. Ticket 8
9. Ticket 9

This gets you to a much more coherent “signal detection” system.

Milestone C — Better baseline and calibration
10. Ticket 10
11. Ticket 11
12. Ticket 12

This is where the methodology becomes materially stronger rather than just safer.

⸻

Coverage of the critiques

This plan covers all of the unique substantive issues raised in the two review memos:
	•	self-referential hearing baseline → 2
	•	degraded-baseline semantics / fake p=1 outputs → 2
	•	fallback scopes that change meaning → 3
	•	mathematically wrong names_anywhere expectation → 4
	•	incomplete multiplicity control → 5
	•	invalid sign-adaptive position p-value → 6
	•	temporal multiplicity and null conditioning → 7
	•	Monte Carlo budget / MCSE / RNG concerns → 8
	•	key-mode inconsistency and matching policy → 9
	•	statewide registry mismatch to hearing attendance → 10
	•	endogenous stratification weights → 10
	•	unmatched_only baseline problem → 11
	•	hypergeometric rounding/implementation gap → 11
	•	calibration, power, and low-power thresholding → 12  ￼  ￼

My strongest recommendation is to treat Tickets 1–4 as blocking. They are the changes that most directly align the product with your intent: generate careful, understandable signals from incomplete public data without overstating what the data can prove.