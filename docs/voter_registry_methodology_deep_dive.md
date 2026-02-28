## Scope and Provenance
This brief documents the full voter-registry-linked analytical chain in the current workspace implementation, spanning matching, duplicate-collision inference, shared collision math, and report/UI transformations that can alter interpretation.

Primary implementation sources:
- Matching detector: [voter_registry_match.py:51](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:51)
- Duplicate detector: [duplicates_exact.py:86](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:86)
- Collision baseline math: [collision_baseline.py:150](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/collision_baseline.py:150)
- Name linkage: [linkage.py:26](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/linkage.py:26)
- Statistical tests: [stat_tests.py:12](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/stat_tests.py:12)
- Proportion stats (Wilson/low-power): [proportion_stats.py:15](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/proportion_stats.py:15)
- Registry SQL accessors: [vrdb_postgres.py:665](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/io/vrdb_postgres.py:665)
- Detector wiring: [registry.py:14](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/registry.py:14)
- Pass-2 orchestration/output tables: [pass2_deep_dive.py:222](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/pipeline/pass2_deep_dive.py:222)
- Report payload builder: [builder.py:317](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:317)
- Front-end filtering/controls: [main.js:10625](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/static/report/main.js:10625)
- Defaults: [default.yaml:57](/Users/sayhiben/dev/legislature-tools/testifier_audit/configs/default.yaml:57), [default.yaml:100](/Users/sayhiben/dev/legislature-tools/testifier_audit/configs/default.yaml:100)
- Voter-enabled config: [voter_registry_enabled.yaml:57](/Users/sayhiben/dev/legislature-tools/testifier_audit/configs/voter_registry_enabled.yaml:57), [voter_registry_enabled.yaml:100](/Users/sayhiben/dev/legislature-tools/testifier_audit/configs/voter_registry_enabled.yaml:100)

Test evidence referenced in this handoff:
- [test_voter_registry_match.py:28](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_voter_registry_match.py:28)
- [test_duplicates_exact.py:125](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_duplicates_exact.py:125)
- [test_collision_baseline_math.py:21](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_collision_baseline_math.py:21)
- [test_report_chart_payload.py:507](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_report_chart_payload.py:507)
- [test_external_methodology_handoff_e2e.py:21](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_external_methodology_handoff_e2e.py:21)

## End-to-End Data Flow
1. Detector instantiation and configuration binding happens in [registry.py:40](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/registry.py:40). `VoterRegistryMatchDetector` is created before `DuplicatesExactDetector` ([registry.py:41](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/registry.py:41), [registry.py:58](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/registry.py:58)).
2. `run_detectors` iterates detectors, writes each summary and each table artifact, and injects every table back into shared feature context keyed as `<detector>.<table>` ([pass2_deep_dive.py:255](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/pipeline/pass2_deep_dive.py:255), [pass2_deep_dive.py:266](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/pipeline/pass2_deep_dive.py:266)).
3. Because of this ordering, `duplicates_exact` can consume `voter_registry_match.match_assignments` when partitioning matched/unmatched scopes ([duplicates_exact.py:485](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:485), [duplicates_exact.py:510](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:510)).
4. Report payload builder ingests these persisted detector tables and emits chart-facing records, control defaults/options, and methodology metadata ([builder.py:317](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:317), [builder.py:4044](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:4044)).
5. Front-end JS applies additional runtime filters by bucket, scope, and match mode before rendering ([main.js:10456](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/static/report/main.js:10456), [main.js:10625](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/static/report/main.js:10625)).

## Voter Registry Matching: Algorithm and Outputs
Algorithm pipeline:
1. Input validation: requires `canonical_name`, `position_normalized`, `minute_bucket` ([voter_registry_match.py:472](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:472)).
2. Registry lookup pull:
- Exact canonical-name counts via `fetch_matching_voter_names` ([voter_registry_match.py:512](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:512), [vrdb_postgres.py:702](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/io/vrdb_postgres.py:702)).
- Last-name candidate pool via `fetch_voter_candidates_by_last_name` ([voter_registry_match.py:518](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:518), [vrdb_postgres.py:717](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/io/vrdb_postgres.py:717)).
- Registry row count metadata via `count_registry_rows` ([voter_registry_match.py:525](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:525), [vrdb_postgres.py:906](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/io/vrdb_postgres.py:906)).
3. Name linkage classification in `classify_name_linkage`:
- Exact match tier (`match_tier="exact"`) gives `primary_outcome` matched unique/ambiguous based on `n_registry_rows` ([linkage.py:93](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/linkage.py:93), [linkage.py:95](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/linkage.py:95)).
- Nickname-root exact equivalence (`match_tier="nickname_exact"`) is also treated as matched in primary outcome ([linkage.py:113](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/linkage.py:113), [linkage.py:123](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/linkage.py:123)).
- Fuzzy candidates are scored by RapidFuzz and tiered `strong_fuzzy`/`weak_fuzzy` by thresholds ([linkage.py:174](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/linkage.py:174), [linkage.py:212](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/linkage.py:212)).
- Important: fuzzy tiers do not update `primary_outcome`; they only affect `balanced_outcome`/`broad_outcome` ([linkage.py:219](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/linkage.py:219), [linkage.py:227](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/linkage.py:227), [linkage.py:243](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/linkage.py:243)).
4. Detector-level reporting modes:
- `strict_outcome`: exact only ([voter_registry_match.py:632](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:632), [voter_registry_match.py:634](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:634)).
- `loose_outcome`: exact + nickname_exact ([voter_registry_match.py:639](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:639), [voter_registry_match.py:641](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:641)).
5. Primary mode selection for summary/report defaults:
- Candidate options are strict/loose from available sensitivity rows ([voter_registry_match.py:970](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:970)).
- Default report mode is `loose` when available ([voter_registry_match.py:37](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:37), [voter_registry_match.py:980](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:980)).

Runtime outputs:
- Summary fields include `primary_match_mode`, `primary_outcome_column`, counts/rates, and caveats ([voter_registry_match.py:1031](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:1031)).
- Tables: `linkage_overview`, `linkage_by_position_rows/unique`, `position_pairwise_tests`, `sensitivity_modes`, `match_assignments`, `match_by_bucket`, `match_by_bucket_position`, `unmatched_names`, plus dual-bounds tables when enabled ([voter_registry_match.py:1056](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:1056)).

## Statistical Methods in Matching Layer
Implemented methods:
1. Wilson intervals for matched and unmatched rates.
- Formula used:
  \[
  \hat p = k/n,\quad
  c = \frac{\hat p + z^2/(2n)}{1+z^2/n},\quad
  h = \frac{z\sqrt{(\hat p(1-\hat p)+z^2/(4n))/n}}{1+z^2/n}
  \]
  interval = \([c-h, c+h]\) clipped to \([0,1]\).
- Implemented in [proportion_stats.py:15](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/proportion_stats.py:15), used in match detector at [voter_registry_match.py:225](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:225), [voter_registry_match.py:359](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:359), [voter_registry_match.py:800](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:800).
2. Low-power masking for counts below threshold (`n_total < low_power_min_total`).
- Implemented in [proportion_stats.py:51](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/proportion_stats.py:51), applied in [voter_registry_match.py:237](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:237), [voter_registry_match.py:371](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:371), [voter_registry_match.py:413](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:413).
3. Pairwise position testing uses Fisher exact on unmatched counts.
- `scipy.stats.fisher_exact` wrapper with rate difference and Wilson bounds for each arm in [stat_tests.py:74](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/stat_tests.py:74).
- Applied by detector in [voter_registry_match.py:283](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:283).

Inferential vs descriptive flags:
- Pairwise rows are flagged `descriptive_only` when either side is low-power ([voter_registry_match.py:313](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:313)).
- Other matching-layer tables carry low-power fields but do not perform multiplicity adjustment.

Dual-bounds mode:
- Runs matching twice (`active_only=True` and `False`) and exposes lower/upper spans in summary and `position_bounds` ([voter_registry_match.py:1246](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:1246), [voter_registry_match.py:1084](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:1084), [voter_registry_match.py:1173](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:1173)).

## Duplicate Collision Detector: Core Model
Collision metrics from bucket or scope name-count vectors \(x_i\):
- `pairs = \sum_i x_i(x_i-1)/2`
- `excess_rows = \sum_i \max(x_i-1,0)`
- `repeated_group_rows = \sum_{i: x_i\ge 2} x_i`
Implemented in [collision_baseline.py:125](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/collision_baseline.py:125).

Detector core settings and defaults:
- Constructor defaults include `collision_baseline_source="hearing_empirical"`, `collision_baseline_model="multinomial"`, `collision_uncertainty_mode="monte_carlo"`, `per_name_significance_model="binomial_tail"`, `monte_carlo_draws=20000`, `bh_fdr_q=0.10`, low-power thresholds, and position-baseline parameters ([duplicates_exact.py:96](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:96)).
- Config defaults mirror this in [default.yaml:63](/Users/sayhiben/dev/legislature-tools/testifier_audit/configs/default.yaml:63) through [default.yaml:85](/Users/sayhiben/dev/legislature-tools/testifier_audit/configs/default.yaml:85).
- Voter-enabled config switches collision baseline source to `vrdb_full_histogram` and failure policy to `fail` ([voter_registry_enabled.yaml:63](/Users/sayhiben/dev/legislature-tools/testifier_audit/configs/voter_registry_enabled.yaml:63), [voter_registry_enabled.yaml:68](/Users/sayhiben/dev/legislature-tools/testifier_audit/configs/voter_registry_enabled.yaml:68)).

Primary runtime artifacts and decision fields:
- `collision_methods` holds baseline source/model, uncertainty mode, fallback/degraded status, stratification, and scope-level `n_used/N_used` ([duplicates_exact.py:1775](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1775)).
- `collision_overview` holds observed vs expected + z/p by metric/scope ([duplicates_exact.py:1619](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1619)).
- `collision_by_bucket` and `collision_by_bucket_position` carry bucket-level expected/observed plus `is_low_power` and `inference_status` ([duplicates_exact.py:1947](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1947), [duplicates_exact.py:2043](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:2043)).
- `per_name_tests` holds per-name p/q/significance and expected counts ([duplicates_exact.py:1717](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1717)).

## Baseline Construction and Fallback Logic
Baseline source selection:
1. `hearing_empirical`: histogram built from observed hearing name counts ([duplicates_exact.py:521](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:521)).
2. `vrdb_full_histogram`: DB-level histogram via SQL CTE grouping key counts ([duplicates_exact.py:530](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:530), [vrdb_postgres.py:835](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/io/vrdb_postgres.py:835)).
3. `vrdb_full_keys`: DB key frequencies then converted to histogram ([duplicates_exact.py:537](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:537), [vrdb_postgres.py:791](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/io/vrdb_postgres.py:791)).

Fallback policy:
- If source requires DB but DB URL is absent or query fails, behavior depends on `collision_baseline_failure_policy`:
  - `fail`: raise runtime error.
  - `degrade`: fallback to hearing empirical and mark degraded.
- Logic in [duplicates_exact.py:524](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:524), [duplicates_exact.py:551](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:551).

Baseline model formulas:
1. Multinomial analytic expectations:
- \(p_i = c_i/N\) for each histogram class count \(c_i\) with multiplicity \(n_i\).
- \(E[\text{pairs}] = \binom{n}{2}\sum_i n_i p_i^2\)
- \(E[\text{unique}] = \sum_i n_i(1-(1-p_i)^n)\)
- \(E[\text{singletons}] = \sum_i n_i\,n p_i(1-p_i)^{n-1}\)
- \(E[\text{excess}] = n-E[\text{unique}]\)
- \(E[\text{repeated}] = n-E[\text{singletons}]\)
Implemented in [collision_baseline.py:166](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/collision_baseline.py:166) through [collision_baseline.py:176](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/collision_baseline.py:176).
2. Hypergeometric analytic expectations:
- Uses log-combination probabilities for zero/one success occupancy per key class and same-name pair probability under finite population without replacement.
- Implemented in [collision_baseline.py:180](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/collision_baseline.py:180) through [collision_baseline.py:198](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/collision_baseline.py:198).
- Requires \(n\le N\) or raises ([collision_baseline.py:162](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/collision_baseline.py:162)).

Stratified baseline path:
- `birth_decade` strata from SQL CASE expression ([vrdb_postgres.py:92](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/io/vrdb_postgres.py:92), [vrdb_postgres.py:96](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/io/vrdb_postgres.py:96), [vrdb_postgres.py:864](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/io/vrdb_postgres.py:864)).
- Scope-level mixture probabilities are built from stratum-specific name frequencies weighted by scope-specific stratum weights ([duplicates_exact.py:608](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:608)).

## Monte Carlo and Permutation Procedures
Collision null simulation:
1. Scope-level Monte Carlo uses draw budget from `_collision_monte_carlo_draw_budget` with size-based scaling and caps ([duplicates_exact.py:240](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:240)).
2. Bucket-level Monte Carlo budgets are suppressed to zero for guaranteed low-power buckets (`n < low_power_min_unique_names` or expected primary < threshold) ([duplicates_exact.py:251](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:251)).
3. Scope-level hard cap is 1000 draws ([duplicates_exact.py:1569](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1569)); bucket-level hard cap is 250 ([duplicates_exact.py:1847](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1847)).
4. Hypergeometric mode does not generate Monte Carlo nulls in shared baseline utility (`simulate_collision_null_from_histogram` returns empty unless multinomial) ([collision_baseline.py:367](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/collision_baseline.py:367)).

Stratified Monte Carlo:
- If stratification is active and multinomial, detector can run a dedicated stratified sampler drawing stratum allocations with `rng.multinomial` and within-stratum key sampling (`rng.choice`) ([duplicates_exact.py:708](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:708), [duplicates_exact.py:738](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:738)).

Empirical p-values from Monte Carlo null:
- One-sided empirical p uses \((\#\{null \ge observed\}+1)/(m+1)\) ([stat_tests.py:37](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/stat_tests.py:37)).
- Collision summary z and p from null samples are computed in [collision_baseline.py:404](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/collision_baseline.py:404).

Permutation and bootstrap procedures:
1. Position permutation test for Pro vs Con duplicate-row-rate difference:
- Permutes position labels while preserving Pro/Con totals and computes one-sided permutation p-value with +1 correction.
- Implemented in [duplicates_exact.py:857](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:857), [duplicates_exact.py:874](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:874).
2. Bootstrap CI for rate difference uses parametric binomial bootstrap at `n_boot=4000` ([duplicates_exact.py:877](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:877), [stat_tests.py:47](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/stat_tests.py:47)).
3. Temporal permutation per name:
- Draws capped at `min(temporal_permutation_draws, 1000)` and cached by sample size.
- Tests min gap and counts of gaps within 5 and 15 minutes via empirical one-sided p-values.
- Implemented in [duplicates_exact.py:920](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:920), [duplicates_exact.py:926](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:926), [duplicates_exact.py:961](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:961).

## Per-Name Inference and Multiple Testing
Per-name null parameterization:
1. `population_probability` is estimated from either stratified mixture probabilities or per-name population counts divided by `N_used` ([duplicates_exact.py:1656](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1656), [duplicates_exact.py:1670](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1670)).
2. `expected_count = population_probability * n_scope` ([duplicates_exact.py:1675](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1675)).

P-value models:
1. Binomial upper-tail model:
- \(p_i = P(X\ge k_i),\ X\sim \text{Binomial}(n_{scope}, \pi_i)\)
- Vectorized path in [duplicates_exact.py:370](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:370), scalar fallback in [stat_tests.py:109](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/stat_tests.py:109).
2. Hypergeometric upper-tail model:
- \(p_i = P(X\ge k_i),\ X\sim \text{Hypergeometric}(N, K_i, n_{scope})\)
- Vectorized path in [duplicates_exact.py:407](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:407), scalar fallback in [stat_tests.py:130](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/stat_tests.py:130).

Multiple testing:
- Benjamini-Hochberg adjusted q-values are computed across all names in each scope frame ([duplicates_exact.py:1712](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1712)).
- BH implementation uses reverse monotone pass: \(q_{(i)}=\min(q_{(i+1)}, p_{(i)}m/i)\), clipped to \([0,1]\) ([stat_tests.py:20](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/stat_tests.py:20)).
- Significance flag is `q_value <= bh_fdr_q` ([duplicates_exact.py:1713](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1713)).

## Low-Power Policy and Inference Gating
Matching layer:
- Low power for matched/unmatched rates and pairwise comparisons is driven by `low_power_min_total` (default 30 via `DEFAULT_LOW_POWER_MIN_TOTAL`) ([proportion_stats.py:6](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/proportion_stats.py:6), [voter_registry_match.py:65](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:65)).

Duplicates layer:
1. Bucket-level gate:
- `low_power = (n_unique < low_power_min_unique_names) OR (expected_primary_bucket < low_power_min_expected_duplicates)` ([duplicates_exact.py:1939](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1939)).
- `inference_status = descriptive_only` when low power ([duplicates_exact.py:1970](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1970)).
2. Position-bucket gate uses analogous criteria and sets `inference_status` per row ([duplicates_exact.py:2038](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:2038), [duplicates_exact.py:2065](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:2065)).
3. Position-claim gate for summary:
- Requires multinomial model, non-empty metrics, no low-power rows, and interval draws available ([duplicates_exact.py:354](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:354)).
- Claim status/result fields are `position_claim_eligible` and `position_claim_reason` in summary ([duplicates_exact.py:2460](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:2460)).

Report-layer low-power handling:
- Voter match alerts are suppressed when `is_low_power` is true in payload-level alert booleans ([builder.py:2331](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:2331)).

## Scope Partitioning via Voter Match Assignments
Scope logic for duplicates detector:
1. Default scope always includes `full_hearing` ([duplicates_exact.py:480](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:480)).
2. If requested scopes include `matched_only` and/or `unmatched_only`, detector reads `voter_registry_match.match_assignments` from features ([duplicates_exact.py:485](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:485)).
3. Name partitioning uses `primary_outcome_selected` when present, otherwise `primary_outcome` ([duplicates_exact.py:495](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:495)).
4. Matched outcomes are `matched_unique`/`matched_ambiguous`; unmatched is explicit `unmatched` ([duplicates_exact.py:47](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:47), [duplicates_exact.py:503](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:503)).

Runtime fields carrying partition decisions:
- `collision_methods.scope`, `n_used`, `N_used` ([duplicates_exact.py:1777](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1777)).
- Summary: `collision_scope_primary`, `n_records`, `n_used`, `N_used` ([duplicates_exact.py:2451](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:2451)).

E2E validation of partitioning contract:
- [test_external_methodology_handoff_e2e.py:100](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_external_methodology_handoff_e2e.py:100) asserts scope-level `n_used` equals full/matched/unmatched derived from assignments.

## Report-Layer Transformations (Critical for Interpretation)
Detector-native vs report-transformed distinction:
1. Detector-native collision bucket metrics are `observed`, `expected`, `excess`, `z_score`, `p_value` by collision metric (`pairs`, `excess_rows`, `repeated_group_rows`) in `collision_by_bucket` ([duplicates_exact.py:1959](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1959), [duplicates_exact.py:1963](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1963)).
2. Report payload replaces this with derived `duplicates_exact_bucket_concentration` units `rows_anywhere` and `names_anywhere` built from per-name timing/per-name totals and volume-share scaling ([builder.py:1150](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:1150), [builder.py:1368](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:1368), [builder.py:1415](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:1415)).

Key payload transformations:
1. `global_duplicated_rows` and `global_duplicated_names` are aggregated from per-name mode tables and/or timing tables, then expected per-bucket values are scaled as:
- `unit_expected_rows = n_rows * global_duplicated_rows / total_rows_in_scope`
- `unit_expected_names = n_rows * global_duplicated_names / total_rows_in_scope`
([builder.py:1241](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:1241), [builder.py:1368](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:1368), [builder.py:1374](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:1374)).
2. Legacy detector expected values are fallback only (`legacy_expected_rows`) when transformed expecteds are unavailable ([builder.py:1380](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:1380)).
3. Chart controls expose transformed metric names with explicit labels in UI (`Rows (duplicated anywhere)`, `Names (duplicated anywhere)`) ([main.js:10850](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/static/report/main.js:10850)).
4. JS filtering then subsets by active duplicate scope, match mode, and transformed metric id ([main.js:10646](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/static/report/main.js:10646), [main.js:10658](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/static/report/main.js:10658)).

Voter payload transformations:
1. Report builder computes global matched-rate control limits not present in detector output:
- `expected_match_rate_global = sum(n_matched)/sum(n_total)`
- `SE = sqrt(p(1-p)/n_bucket)`
- 95%: `p ± 1.96*SE`, 99.8% proxy: `p ± 3*SE`
- bucket alerts set when bucket rate breaches 99.8 bands and not low-power.
Implemented in [builder.py:2308](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:2308), [builder.py:2315](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:2315), [builder.py:2337](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:2337).
2. If `position_bounds` table is absent, payload derives fallback bounds from rows vs unique tables, with `unit="rows_vs_unique"` and derived inference status ([builder.py:1997](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:1997), [builder.py:249](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:249)).

Mode normalization in report/UI:
- JS maps `strict|exact|medium -> strict` and `loose|nickname -> loose` ([main.js:3486](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/static/report/main.js:3486)).
- Duplicate and voter mode selectors are wired to those normalized options only ([main.js:186](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/static/report/main.js:186), [main.js:206](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/static/report/main.js:206)).

## Methodological Risks and Implementation Caveats
### Critical Findings
1. Matching-mode semantics split across layers.
- Internal linkage emits `primary_outcome`/`balanced_outcome`/`broad_outcome` (`conservative`/`balanced`/`broad` concept), but report-facing matching summary defaults from strict/loose only ([voter_registry_match.py:28](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:28), [voter_registry_match.py:33](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:33), [voter_registry_match.py:980](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:980)).
- `primary_match_mode` config is parsed into `self.primary_match_mode` but report default still picks strict/loose path, not this field ([voter_registry_match.py:66](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:66), [voter_registry_match.py:87](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:87), [voter_registry_match.py:980](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:980)).
2. Fuzzy-link evidence is structurally asymmetric.
- `strong_fuzzy`/`weak_fuzzy` affect `balanced_outcome`/`broad_outcome`, but `primary_outcome` remains unmatched, while strict/loose reporting is constructed from `primary_outcome` + tier masks. This can hide fuzzy evidence in report modes ([linkage.py:219](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/linkage.py:219), [linkage.py:227](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/linkage.py:227), [voter_registry_match.py:632](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py:632)).
3. Baseline switching and degradation materially changes inferential regime.
- Runtime may silently degrade to hearing empirical (unless `fail`), changing both null model and population interpretation ([duplicates_exact.py:524](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:524), [duplicates_exact.py:555](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:555)).
4. Stratification weights are partially observation-driven.
- Scope-level stratum weights are derived from observed hearing composition allocated through registry stratum frequencies, not fixed external priors. This may partially “condition on observed outcomes” in null construction ([duplicates_exact.py:571](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:571), [duplicates_exact.py:597](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:597)).
5. Bucket-level expected-vs-observed caching has a likely statistical bug for p/z reuse.
- `metric_summary_by_n` is cached by `n_bucket`; `summary_for_n` is computed with the first bucket’s observed metric values for that n, then later buckets reuse cached `p_value`/`z_score` regardless of their own observed values ([duplicates_exact.py:1825](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1825), [duplicates_exact.py:1919](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1919), [duplicates_exact.py:1945](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py:1945)).
6. Report-level duplicate expectations are transformed away from detector null expectations.
- `rows_anywhere`/`names_anywhere` use scaled expectations from global duplicated totals by scope/mode, not detector collision null expectations (`expected` in `collision_by_bucket`) ([builder.py:1368](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:1368), [builder.py:1400](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:1400)).
7. Voter control limits are normal-approximation overlays added in payload layer.
- These are not detector-native inference outputs and are unadjusted for multiple bucket scanning ([builder.py:2315](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:2315), [builder.py:2337](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/rendering/payload/builder.py:2337)).

Additional caveats:
- Hypergeometric collision mode currently has analytic expectations only; null-simulation uncertainty diagnostics become unavailable ([collision_baseline.py:367](/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/collision_baseline.py:367)).
- Per-name BH assumes dependence conditions not explicitly evaluated in code.

## Questions for External Statistician
1. Is the multinomial collision null (with optional registry histogram) an appropriate first-order null for this operational process, or should a clustered/inhomogeneous process be modeled explicitly?
2. Does the hearing-conditioned stratification weighting (`_scope_stratum_weights`) induce unacceptable post-treatment bias in null expectations?
3. Should `rows_anywhere`/`names_anywhere` report metrics be framed as descriptive concentration metrics rather than inferential null tests?
4. Should bucket-level p-values be recomputed per bucket observed metric (rather than cached by `n_bucket`), and should any bucket-wise multiplicity correction be applied?
5. Is BH-FDR appropriate for per-name tests given dependency structure among names and potential shared temporal shocks?
6. Are low-power thresholds (`low_power_min_unique_names=25`, `low_power_min_expected_duplicates=5.0`, matching `low_power_min_total=30`) calibrated to practical power targets?
7. Should dual-bounds voter matching (`active_only` true/false) be interpreted as sensitivity bounds, and how should downstream inference combine them?
8. Are 95%/99.8% normal control bands on match rates defensible under bucket heterogeneity and non-stationarity?
9. Is one-sided testing direction (collision excess, temporal clustering) aligned with intended causal or evidentiary claims?
10. What robustness suite should be mandatory before alert interpretation (alternative nulls, bootstrap under dependence, synthetic stress injections)?

### Statistician Feedback Checklist
- Null-model suitability: assess multinomial/hypergeometric assumptions against real submission generation processes.
- Dependence/multiple testing: evaluate BH validity and whether hierarchical or empirical-Bayes alternatives are preferable.
- Calibration of low-power thresholds: recommend thresholds tied to minimum detectable effect sizes.
- Interpretability and causal-overreach guardrails: validate language boundaries between anomaly evidence and causal claims.
- Preferred robustness checks: specify required perturbation, placebo, and simulation calibration experiments.

## Validation/Stress-Test Agenda
Executed test commands and outcomes:
1. `python -m pytest /Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_voter_registry_match.py`
- Result: 8 passed, 3 warnings.
- Surface covered: matching-mode behavior, nickname strict-vs-loose semantics, bucket outputs, dual-bounds summaries/tables.
- Anchor tests: [test_voter_registry_match.py:237](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_voter_registry_match.py:237), [test_voter_registry_match.py:503](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_voter_registry_match.py:503).
2. `python -m pytest /Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_duplicates_exact.py`
- Result: 17 passed.
- Surface covered: baseline fallback policy, stratification degradation/effective use, Monte Carlo draw budgeting, low-power simulation skips, position baseline/claim gating.
- Anchor tests: [test_duplicates_exact.py:125](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_duplicates_exact.py:125), [test_duplicates_exact.py:325](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_duplicates_exact.py:325), [test_duplicates_exact.py:387](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_duplicates_exact.py:387), [test_duplicates_exact.py:846](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_duplicates_exact.py:846).
3. `python -m pytest /Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_collision_baseline_math.py`
- Result: 8 passed.
- Surface covered: analytic expectation invariants/bounds, scaling checks, Monte Carlo-vs-analytic calibration, probability-histogram conversion.
- Anchor tests: [test_collision_baseline_math.py:21](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_collision_baseline_math.py:21), [test_collision_baseline_math.py:101](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_collision_baseline_math.py:101).
4. `python -m pytest /Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_report_chart_payload.py -k "duplicates_exact or voter_registry"`
- Result: 6 passed, 10 deselected, 138 warnings.
- Surface covered: transformed duplicate bucket concentration contract, mode/scope metric rows, voter unmatched top-50 cap, voter mode+bucket merge integrity.
- Anchor tests: [test_report_chart_payload.py:507](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_report_chart_payload.py:507), [test_report_chart_payload.py:1033](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_report_chart_payload.py:1033), [test_report_chart_payload.py:1438](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_report_chart_payload.py:1438).
5. `python -m pytest /Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_external_methodology_handoff_e2e.py`
- Result: 1 passed.
- Surface covered: cross-detector handoff where voter assignments drive duplicate scope partition totals.
- Anchor test: [test_external_methodology_handoff_e2e.py:21](/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_external_methodology_handoff_e2e.py:21).

Recommended stress-test expansions for statistician review:
1. Recompute bucket p/z per bucket observed value (no `n_bucket` cache reuse) and compare alert rank stability.
2. Evaluate alternative nulls: mixed Poisson-multinomial or time-varying name-probability models.
3. Simulate dependence-aware FDR controls for per-name tests (e.g., BY, Storey-q, knockoff-style approximations where feasible).
4. Perform calibration checks for report-layer control bands under empirically resampled buckets.
5. Compare detector-native and report-transformed duplicate expectations side-by-side in a fixed benchmark hearing.
