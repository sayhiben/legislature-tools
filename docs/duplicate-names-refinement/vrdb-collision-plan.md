Yes. The cleanest approach is to add VRDB-derived name-frequency expectations as a new null-model layer and keep your existing analyses intact as separate evidence families.

The operating rule

VRDB should answer one specific question:

Given a submission volume N, and a realistic Washington name distribution, how many normalized name-string collisions should occur by chance?

It should not replace your current analyses that answer different questions, such as:
	•	whether a batch resembles the registry distribution;
	•	whether names look copied from the registry;
	•	whether timing, content, geography, or other fields suggest coordination.

Those stay. VRDB collision modeling becomes an additional signal.

⸻

Plan

1) Freeze the analysis contract before any coding

Write a one-page spec that assigns each analysis family a single claim.

Use something like this:
	•	VRDB collision baseline
	•	Claim: “Observed duplicate name-string collisions are higher/lower than expected under a VRDB-derived null.”
	•	Registry similarity / divergence
	•	Claim: “The batch’s name distribution is unusually close to or far from VRDB.”
	•	Behavioral / temporal / content analyses
	•	Claim: “Submission timing/content/metadata show or do not show coordination.”
	•	Identity-linked analyses
	•	Claim: separate, and only if you actually have linkage fields beyond name strings.

This prevents the most common failure: a useful VRDB feature quietly becoming the “master explanation” for everything else.

Deliverable: a short spec with named evidence families, exact question answered, and allowed interpretation.

⸻

2) Standardize normalization once, and use it everywhere

Before modeling anything, build a single normalization layer shared by:
	•	the VRDB ingest;
	•	the submission data;
	•	any duplicate counting code;
	•	any reporting output.

At minimum, create:
	•	full_name_key
	•	first_name_key
	•	last_name_key

Normalization should handle:
	•	case-folding;
	•	whitespace collapse;
	•	punctuation/apostrophes/hyphens policy;
	•	suffix removal policy;
	•	“LAST, FIRST” reordering if needed;
	•	optional nickname handling only if you explicitly decide to do it.

Keep both:
	•	the raw name string for audit;
	•	the normalized key for modeling.

Important: the primary duplicate null should be built on normalized full-name strings, not first or last name alone.
First-only and last-only models are useful as explanatory side panels, but they should not drive the main duplicate-collision baseline.

Deliverable: versioned normalization function plus test cases.

⸻

3) Build versioned VRDB probability tables

Treat VRDB as a probability source, not just a file you sample from ad hoc.

Create frequency tables for normalized names:
	•	statewide;
	•	county-level if reliable;
	•	city-level only where data quality and denominator size support it.

For each geography level, compute:
	•	count(name_key)
	•	p(name_key)
	•	optionally p(first_name_key) and p(last_name_key)

Also keep baseline variants, not just one table:
	•	Variant A: statewide VRDB
	•	Variant B: county/city-conditioned VRDB
	•	Variant C: alternative registry universe if relevant, such as active-only vs broader registrant set

Use backoff / shrinkage, not hard failure, for sparse geographies:
	•	city → county → statewide

If a city-level distribution is too sparse or the city field is messy, shrink toward county or statewide rather than pretending the city estimate is precise.

Deliverable: a versioned VRDB probability artifact, with provenance and denominator notes.

⸻

4) Implement a dedicated collision-null engine

This is the part that is probably missing today.

For each analysis slice — overall hearing, position bucket, time bucket, city bucket, or any other slice — compute expectations from the VRDB name distribution.

For a slice with N_b submissions, compute:
	•	Expected duplicate pairs
\mathbb{E}[\text{pairs}_b] = \binom{N_b}{2}\sum_i p_i^2

Also compute:
	•	expected count for specific names: N_b p_i
	•	observed duplicate pairs
	•	observed max repeat count for a single name
	•	observed number of unique names

Then add Monte Carlo simulation for the full null distribution, so you can report:
	•	median expected pairs;
	•	95th and 99th percentile;
	•	tail probability of the observed value;
	•	the same for “max repeated name” if useful.

Two rules matter here:
	1.	Use each bucket’s own N_b
Never linearly allocate or rescale a global duplicate expectation across buckets.
	2.	Treat this as a string-collision null
Do not let downstream reporting silently convert it into “person duplication” or “identity fraud.”

Deliverable: a vrdb_collision module that produces expected-pairs metrics and Monte Carlo bands per slice.

⸻

5) Integrate it as a sidecar, not as a replacement

Do not modify existing detectors in place for the first rollout.

Add a new evidence table, something like:
	•	slice_id
	•	slice_type
	•	N
	•	baseline_variant
	•	observed_pairs
	•	expected_pairs_mean
	•	expected_pairs_p95
	•	expected_pairs_p99
	•	tail_prob_pairs
	•	observed_max_name_count
	•	expected_max_name_count_mean
	•	tail_prob_max_name
	•	top_overrun_names
	•	normalization_version
	•	vrdb_version

Then join this to your current outputs. Do not overwrite them.

Concretely:
	•	keep current divergence metrics;
	•	keep current timing/content/metadata metrics;
	•	add VRDB collision metrics under a new namespace;
	•	show them side by side in dashboards and reports.

For v1, avoid a single merged score unless you already have a calibrated ensemble framework.
The safer move is an evidence matrix, not a “super-score.”

Deliverable: additive join into the existing pipeline, with zero changes to existing columns or thresholds.

⸻

6) Define how disagreement between analyses is handled

This is the piece that keeps VRDB from overriding other work.

Write explicit rules for disagreement:
	•	High VRDB collision anomaly, normal registry similarity
	•	Interpretation: too many repeated names given plausible frequencies, even if overall distribution shape looks ordinary.
	•	High registry similarity, normal collision anomaly
	•	Interpretation: names may resemble VRDB composition, but repeat levels are not unusual.
	•	Both high
	•	Interpretation: stronger combined evidence.
	•	Both normal, other behavioral signals high
	•	Interpretation: name evidence is not carrying the case; other analyses remain primary.

The key policy:
	•	VRDB collision evidence may add concern, but it does not suppress other flags.
	•	Existing analyses may add concern, but they do not erase a VRDB extreme.

That is how you avoid replacement or override.

Deliverable: analyst guidance for interpreting concordant and discordant signals.

⸻

7) Validate against real controls before using it operationally

Run the new module against three sets:
	1.	Normal historical hearings
	•	to estimate real-world false positive behavior;
	2.	Known or suspected problematic hearings
	•	to see whether VRDB collision adds separable signal;
	3.	Synthetic sanity checks
	•	random draws from VRDB-based priors should land where the null says they should.

Validation checks should include:
	•	statewide vs county/city-conditioned variants;
	•	active-only vs broader VRDB denominator variants if relevant;
	•	sensitivity to normalization choices;
	•	small-bucket behavior;
	•	city-field quality issues.

The goal is not to prove the VRDB null is perfect.
The goal is to show that the signal is stable enough to be informative and that its uncertainty is visible.

Deliverable: backtest memo with false positive behavior and sensitivity bands.

⸻

8) Add reporting language that cannot be misread

Your report layer needs hard wording constraints.

Use language like:
	•	“duplicate name-string collisions”
	•	“higher than expected under a VRDB-derived null”
	•	“consistent/inconsistent with chance given observed volume”

Do not use language like:
	•	“duplicate people”
	•	“fake identities”
	•	“impossible frequency”
	•	“proof of fraud”

unless some separate identity-linkage analysis justifies that stronger claim.

Also require the report to name:
	•	VRDB version used;
	•	normalization version used;
	•	baseline variant used;
	•	whether the slice used city, county, or statewide backoff.

Deliverable: fixed terminology and report templates.

⸻

What should stay exactly as-is

Assuming your other analyses already exist, do not replace these with VRDB collision modeling:
	•	registry-similarity tests;
	•	overfit-to-registry tests;
	•	timing bursts / submission coordination tests;
	•	metadata/content clustering;
	•	any non-name anomaly measures.

Those are answering different questions.
The right structure is:
	1.	collision null
	2.	distribution similarity
	3.	behavioral / temporal
	4.	content / metadata
	5.	identity-linked signals if available

Not one winner, but multiple orthogonal lines of evidence.

⸻

The minimum viable implementation

If you want the smallest version that is still correct, do this first:
	1.	Shared name normalization.
	2.	Statewide VRDB probability table over normalized full names.
	3.	Per-slice expected duplicate pairs using \binom{N_b}{2}\sum p_i^2.
	4.	Monte Carlo p95/p99 bands.
	5.	Additive evidence table joined to current outputs.
	6.	No composite score yet.

That gets the core math right without disturbing your current framework.

⸻

Acceptance criteria

You can treat this as done when all of these are true:
	•	Existing analyses still run unchanged.
	•	VRDB collision metrics appear as new, separate outputs.
	•	Expectations are computed per slice using that slice’s N.
	•	Reports clearly distinguish name-string collisions from identity claims.
	•	At least one conservative baseline variant is included.
	•	Historical controls show reasonable false positive behavior.
	•	Analysts can see disagreement across evidence families instead of having it collapsed away.

⸻

Recommended sequencing

The best order is:
	1.	spec and interpretation guardrails
	2.	normalization
	3.	VRDB probability tables
	4.	collision engine
	5.	additive integration
	6.	backtesting
	7.	reporting rollout

That order minimizes the chance of building a mathematically correct module that later gets misinterpreted.

Next useful artifact would be a technical spec with schema, function signatures, and test cases.