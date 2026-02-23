# Statistical Plan for Duplicate-Name Analysis in WA Legislative Hearing Testifier Sign‑Ins

## Framing the problem as estimands rather than “duplicates”

With only four fields—**Name (last, first)**, **Organization (often blank)**, **Position (Pro/Con/Other)**, and **Datetime (minute resolution)**—you do not have stable person-identifiers. That means you can’t directly observe “the same human signed in twice.” What you *can* observe is **name-string reuse** and **patterns consistent with repeated sign-ins**, and then quantify how surprising those patterns are under defensible null models.

A strong plan starts by defining a small set of estimands (quantities you intend to estimate), and then producing results under **multiple identification assumptions** (because ambiguity is unavoidable). The core estimands that map cleanly onto your goal are:

- **E1: Name-string duplication rate** (overall and by position): the proportion of rows that share the same standardized name with ≥1 other row.
- **E2: Excess duplication relative to baseline**: how much larger duplication is than you’d expect *even if every row corresponds to a distinct person*, due purely to **homonyms** (different people sharing the same name).
- **E3: Position skew in duplication**: whether repetition is disproportionately concentrated in Pro vs Con vs Other (and how much that can change the apparent Pro/Con ratio).
- **E4: Voter-file match “miss rate” by position**: the rate at which testifier names fail to match the registered voter list, under a specified matching pipeline and confidence level.
- **E5: Time-structure anomaly**: whether repeated names are unusually clustered in time (e.g., repeated sign-ins for the same name within a short window), which would be less consistent with independent distinct individuals.

You should treat any outputs as **anomaly detection and sensitivity analysis**, not proof of intent, and explicitly separate:
- “**Observed name reuse**”
- “**Estimated unique persons under assumptions**”
- “**How much the Pro/Con ratio could shift** under plausible identification models”

This framing keeps your tool statistically defensible and reduces the risk of overclaiming from ambiguous identifiers.

## Data preparation and canonicalization pipeline

The single biggest driver of false results in name-based analysis is inconsistency in parsing and standardization. Your first milestone is a reproducible canonicalization pipeline that produces **multiple standardized keys** (not one), so downstream analyses can be run at different strictness levels.

You should create a structured, auditable name representation:

- Parsed fields: `last`, `first`, `middle` (if present), `suffix` (Jr/Sr/II/III), `particles` (De/Van/La), plus a `raw_name` preserved exactly.
- Canonical transforms (store all):
  - **Casefolding**, Unicode normalization, diacritic folding (but keep a diacritic-preserving version too).
  - Punctuation and whitespace normalization: hyphens, apostrophes, multiple spaces.
  - Standardize common formatting artifacts: `LAST, FIRST M.` vs `FIRST M LAST`.
  - Suffix normalization (`JR`, `Jr.` → `JR`).
  - Handle multi-part surnames carefully (“De La Cruz”, “Van Buren”)—do *not* blindly drop particles.
  - Create tokens and initials: first-initial, middle-initial, last-initial.

Then define multiple “name keys,” from strict to loose, for sensitivity analysis:

- **Key S (strict)**: `last + first + middle/initial + suffix` (after standardization).
- **Key M (medium)**: `last + first` (standardized, punctuation folded).
- **Key L (loose)**: `last + first_initial` (useful for blocking and for bounding worst-case collisions).
- **Key N (nickname-expanded)**: `last + canonical_first` where canonical_first is derived from nickname mapping (and optionally symmetrical mapping, e.g., William ↔ Bill).

You should also generate a **name rarity table** from the voter file for each key definition you intend to use: for each key, compute `freq_in_voterfile` and `rank_percentile`. That rarity signal becomes critical later for separating likely repeated sign-ins from common-name collisions.

Finally, canonicalize the other columns:

- **Position**: enforce a closed set (Pro/Con/Other/Unknown) and record the raw label too.
- **Datetime**:
  - normalize timezone assumptions (and store an absolute timestamp if possible),
  - compute derived fields: `date`, `minute_index_in_hearing` (time since first sign-in), `sign_in_rate_per_minute` (overall contextual intensity).

## Defining “duplicate” in a way that remains statistically honest

You need to present duplicate-related results in layers, because there is no single “truth” definition without person identifiers.

### Observable duplication layers

For each chosen key (S/M/L/N), compute:

- `rows_total`
- `unique_names` (unique keys)
- `rows_in_names_with_count>=2`
- `duplicate_rows = rows_total - unique_names` (for that key)
- `names_with_count>=2`
- Distribution of counts per name: `count=1,2,3,...`, plus tail summaries (max, 95th percentile)

Produce these overall and by position.

### Ambiguity-aware interpretation layer

A critical addition is to quantify how likely a “duplicate name” is to represent **different individuals with the same name** versus **repeat sign-ins**. With only names, the best you can do is inference based on:

- **Name frequency in the voter file** (rarity)
- **Co-occurring organization text** (when present)
- **Time clustering** (repeat sign-ins close together are less consistent with multiple independent individuals)
- **Position switching** (same name registering Pro and Con may indicate either different people or manipulation; interpretation should depend on name rarity)

A practical methodology is to attach to each name key a **collision risk score** derived from voter-file frequency, such as:

- `collision_risk = 1 - exp(-λ)` where `λ` is the expected number of *distinct individuals* in a sample that share that name (estimated from voter-file frequency and sample size; see baseline section).
- Or simply provide `freq_in_voterfile` and percentile and let viewers interpret.

Then your reporting tool can label repeated names as:
- **High ambiguity** (very common in voter file)
- **Medium ambiguity**
- **Low ambiguity** (rare name repeats are more anomalous)

This avoids the common failure mode of treating all duplicate strings as equal evidence.

## Establishing a baseline using the registered voter list

Your voter list is uniquely valuable because it allows you to model **how many duplicate name strings you’d expect even with perfectly honest, one-person-one-entry sign-ins**, purely because names are not unique.

You’ll want two baseline models: a population-collision model and a per-name plausibility model.

### Population-collision baseline via simulation or occupancy modeling

Let:
- `N` = number of individuals in voter file (after cleaning)
- `n` = number of testifier sign-in rows (or unique inferred people, depending on the scenario)
- `p(name)` = empirical name distribution from the voter file under your chosen key

Under a null hypothesis **H0 (no repeat sign-ins; each row is a different person)**, the sign-in list is approximately a sample of `n` individuals from a population with distribution `p(name)`. If you sample individuals uniformly from the voter file, name collisions arise naturally.

Compute expected quantities under H0:
- expected `unique_names`
- expected number of names appearing 2+ times
- expected max count for any single name
- expected duplicate_rows

Because exact formulas can get messy with a highly non-uniform distribution, the most robust approach is:

- **Monte Carlo baseline**: repeatedly sample `n` individuals from the voter file (without replacement if treating it as a true subset; with replacement as an approximation when `n << N`), compute duplication metrics for each draw, and form empirical null distributions.

Then compare observed metrics to the baseline:
- standardized scores (z-like) where appropriate
- empirical p-values (how often the simulated metric ≥ observed)

You should run this baseline at multiple key strictness levels (M and N at minimum), because sensitivity to canonicalization is itself a result worth surfacing.

### Per-name plausibility using hypergeometric/binomial tails

For a specific name key with voter-file frequency `f` (i.e., `f` people share that standardized name in the voter list), under H0 and a uniform sample of size `n` from the voter file, the count `K` of that name appearing is approximately:

- `K ~ Hypergeometric(N, f, n)` (without replacement), or
- `K ~ Binomial(n, f/N)` (when `N` is large and `n` is small relative to `N`)

This gives you a powerful anomaly score:
- For each name with observed count `c` in the testifier list, compute `P(K >= c)` under H0.
- Names with extremely small tail probabilities (after multiple-testing correction) are candidates for “excess repetition beyond collision expectation.”

This approach directly attacks one of the biggest conceptual traps: **“5 entries for John Smith”** is far less surprising than **“5 entries for a very rare name.”**

### Making the baseline fairer than “random voters”

A realistic caveat: testifiers are not random voters. They may differ by age, geography, civic engagement, ethnicity, etc., which affects name distributions and match rates.

To mitigate baseline mismatch, include at least one alternative null:

- **Empirical hearing-based baseline**: collect duplication statistics across many hearings/bills (if you can) to create a comparative distribution (“this hearing is in the 99.5th percentile of excess duplication compared to similar hearings”).
- Consider stratification if you later acquire additional covariates (district, county, etc.). If you never have covariates, you should explicitly report that the voter-file baseline is **an approximation** and treat it as one lens among several.

## Matching testifier names to the voter file and computing miss rate by position

You want a “miss rate” partitioned by position. Doing this responsibly requires (a) a defined linkage pipeline and (b) explicit reporting of ambiguity and confidence.

### Linkage tiers and match outcomes

For each testifier row (or better, each unique name key), compute match outcomes against the voter list:

- **No match**: no candidate in voter file.
- **Unique match**: exactly one candidate in voter file under deterministic rules.
- **Ambiguous match**: multiple candidates (common names) under deterministic rules.
- **Fuzzy match**: one or more candidates under fuzzy rules with a score distribution.

Because you only have names, “unique match” is only meaningful when the name is rare enough that the voter list yields a single candidate (or nearly so). For common names, you should treat linkage as ambiguous.

### Deterministic matching first (high precision)

Start with conservative exact matching on Key M and Key S (where possible):
- exact after canonicalization and suffix normalization
- consider a second pass allowing single-character middle-initial differences (if you have middle initials sometimes)

Compute miss rate by position at both:
- **row level** (each sign-in row)
- **unique-name level** (deduped by a chosen key)

Row-level miss rates can be distorted by duplicates; unique-name-level miss rates reduce that distortion.

### Nickname and fuzzy matching as controlled sensitivity analysis

Nicknames and fuzzy matching can dramatically increase false positives unless constrained. The defensible approach is:

- Use fuzzy matching only inside **blocking windows**, e.g.:
  - exact last name match, fuzzy first name match, or
  - phonetic last name match (Soundex/Metaphone-style), exact/near first name match
- Apply nickname mapping as a **candidate generator**, not an automatic match:
  - e.g., if first name is “Bill,” allow candidate first name “William,” then evaluate string similarity plus last name agreement.
- Produce a match confidence score and define thresholds:
  - **High-confidence** (suitable for miss-rate reporting)
  - **Medium-confidence** (report separately)
  - **Low-confidence** (exclude from “matched” counts)

Because you are not writing code yet, the planning deliverable here is a specification: what constitutes a match, what constitutes ambiguous, and how those categories roll up into miss-rate reporting.

### Miss-rate comparisons by position with uncertainty

To compare miss rates across Pro/Con/Other, you should avoid naive tests that assume independent rows if duplicates exist.

A statistically cleaner plan:
- Primary analysis at the **unique-name level** (deduped by a medium key), which reduces repeated-row dependence.
- Secondary analysis at the **row level** (reflecting the public-facing sign-in ratio), but adjusted with robust uncertainty (e.g., cluster by standardized name).

For each position:
- report miss rate with a confidence interval (and clearly state the unit: rows vs unique names)
- compare positions using effect sizes (rate differences and ratios), not just p-values

Interpretation discipline is important: a higher miss rate in one position could reflect out-of-state participants, non-registered residents, data-entry differences, or genuine manipulation. Your downstream anomaly section should only treat it as corroborating evidence when it aligns with other signals (excess repetition, time clustering, rare-name repeats).

## Statistical anomaly detection focused on “excess repetition” and “position impact”

This is where you connect duplication signals to the hypothesis that repeated sign-ins could swing the Pro/Con ratio—while staying honest about uncertainty.

### Excess repetition tests at multiple levels

Run three complementary anomaly lenses:

**Hearing-level excess duplication**
- Compare observed duplicate metrics to the voter-file Monte Carlo baseline:
  - excess duplicate_rows
  - excess names_with_count>=2
  - excess max count

Report where the observed value falls in the null distribution (percentile) rather than relying only on a single p-value.

**Name-level excess repetition**
- Per name, compute tail probabilities under the hypergeometric/binomial model (as described above).
- Correct for multiple comparisons (since you test many names). Plan to use a false discovery rate control so the “top anomalous names” list isn’t dominated by chance.

**Time-structure anomalies**
Because you have minute-resolution timestamps, time adds independent evidentiary value:
- For each repeated name, compute:
  - minimum time gap between sign-ins
  - number of sign-ins within short windows (e.g., within 5 minutes, 15 minutes)
- Compare observed clustering against a permutation null:
  - keep the overall sign-in intensity pattern fixed, but randomly reassign times to names (or randomly permute timestamps within position)
- Names with unusually tight bursts are more consistent with repeated sign-ins by the same actor (or coordinated submission) than with multiple independent individuals sharing a name.

### Position skew and “swing potential” analysis

Your core user-facing question is not just “are there duplicates,” but “could duplicates materially change Pro/Con balance?”

Plan a scenario-based “swing potential” module:

- Compute **raw counts** by position (rows).
- Compute **unique-key counts** by position under multiple key definitions (Key S, M, N).
- Compute a **bounded interval** for the Pro share under uncertainty:
  - **Upper bound**: assume every duplicate string is a distinct person (worst case for “manipulation” inference; duplicates don’t change the ratio).
  - **Lower bound**: assume every repeated name within a position is the same person signing multiple times (max impact on ratio).
  - A **rarity-weighted estimate**: downweight duplicates for common names (assume more are collisions) and upweight duplicates for rare names (assume more are repeat sign-ins). This can be formalized as an uncertainty model using voter-file frequency.

Then quantify:
- change in Pro share from raw to deduped under each model
- confidence/credibility intervals if you implement probabilistic weighting later

You should also test whether repetition is disproportionately concentrated in one position:
- Compare the distribution of counts-per-name across positions.
- Compare duplicate rates by position with cluster-aware uncertainty (unique-name level as primary; row level as secondary).
- Flag if one side has a heavy tail (a few names contributing many rows), which is a recognizable anomaly signature.

## Reporting outputs, visualizations, and common failure modes

### Tables and summary panels to make results interpretable

A reporting tool should present a small set of consistent summary tables for every hearing:

- **Counts and ratios panel**
  - rows by position
  - unique names by position (for each key strictness)
  - Pro/Con ratio under each dedup scenario

- **Duplication metrics panel**
  - duplicate_rows and % duplicated (overall and by position)
  - number of names with count ≥2, ≥3, ≥5
  - max count for one name
  - concentration metrics (e.g., what fraction of all rows come from the top 1%, top 5% repeaters)

- **Voter match panel**
  - matched / ambiguous / unmatched counts (by position)
  - miss rate (row level and unique-name level)
  - miss rate under deterministic-only vs deterministic+nickname vs deterministic+fuzzy (each clearly labeled as a different rule set)

### Visualizations that highlight anomalies without overclaiming

Use visuals that separate “string behavior” from “identity claims”:

- **Count-per-name distribution** by position (histogram or complementary cumulative distribution). Heavy tails jump out.
- **Lorenz/Pareto-style concentration plot**: cumulative share of rows contributed by the top repeated names (overall and by position).
- **Rarity vs repetition scatter**: x = voter-file frequency (or percentile), y = count in hearing. Rare-and-high is the suspicious quadrant.
- **Time heatmap**: minutes on x-axis, position on y-axis, intensity by sign-ins; overlay markers for repeated-name bursts.
- **Name-level timeline strips for top repeaters**: for the most repeated names (especially rare ones), show tick marks over time colored by position.
- **Sankey or stacked bars** for match outcomes by position: matched vs ambiguous vs unmatched.

A key design principle: whenever you show “top duplicated names,” display the **name frequency in the voter file** beside it so viewers can immediately gauge ambiguity.

### Failure modes and mitigations

The most common ways these analyses go wrong—and how your plan prevents them:

- **Confusing homonyms with repeat sign-ins**  
  Mitigation: voter-file baseline; rarity scoring; per-name tail probabilities; never presenting duplicates as “same person” without qualification.

- **Over-normalization collapsing distinct people** (e.g., dropping particles, mishandling hyphens, erasing meaningful surname structure)  
  Mitigation: preserve multiple keys; audit transformations; keep diacritic-preserving forms; treat name parsing as a first-class component.

- **Under-normalization splitting the same person** (Bill vs William, punctuation variants)  
  Mitigation: nickname-expanded key; fuzzy matching only as sensitivity analysis with thresholds and blocking.

- **Row-level inference distorted by duplicate rows**  
  Mitigation: report both row-level and unique-name-level metrics; use cluster-aware comparisons; treat unique-name as the primary inferential unit.

- **Multiple testing leading to spurious “top anomalies”**  
  Mitigation: apply FDR control for name-level anomaly lists; emphasize effect sizes and rarity context; provide stability checks across key definitions.

- **Baseline mismatch (testifiers are not random voters)**  
  Mitigation: treat voter baseline as one lens; supplement with cross-hearing baseline if you can; phrase conclusions as “excess relative to baseline assumptions.”

- **Minute-resolution timestamp artifacts** (simultaneous entries, ordering ambiguity, time zone uncertainty)  
  Mitigation: analyze time clustering with appropriate tolerance windows; avoid over-interpreting exact order; store and document timezone assumptions.

- **Position label inconsistencies or strategic switching**  
  Mitigation: enforce controlled vocabulary; analyze “position switching within the same name” as a separate signal and interpret through name rarity.

---

If you implement this plan, your tool will be able to say, with statistical discipline:  
1) how much name reuse exists,  
2) how much is explainable by name collisions alone,  
3) whether repetition is position-skewed and time-clustered in ways that are unusual, and  
4) how sensitive the Pro/Con ratio is to plausible deduplication assumptions—without pretending that names are unique identifiers.