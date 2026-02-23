# Duplicate Name + Voter Registry Analyses v2 (Rewrite/Refactor) Implementation Plan

## Summary
This rewrite will replace the current disabled duplicate/voter analyses with a collision-aware, uncertainty-explicit design that is statistically defensible for name-only data. The final system will:
- Keep the existing detector-first report architecture.
- Re-enable duplicate and voter analyses with stronger methodology.
- Separate primary metrics (conservative matching) from sensitivity metrics (fuzzy/near matching).
- Quantify anomaly evidence as “excess over expected under name-collision baseline,” not as identity proof.

Raw-data grounding from `/Users/sayhiben/dev/legislature-tools/data/raw` supports this priority: simple normalized duplicate-row rates are already high in large hearings (about 23.7% to 29.4% in three recent files), so baseline-corrected inference is mandatory.

---

## Expert Plan Synthesis (Strengths/Weaknesses by Document)

| Plan | Strengths | Weaknesses | Final Adoption |
|---|---|---|---|
| `name-analysis-plan-1.md` | Best estimand framing; strong ambiguity treatment; strong simulation + per-name tail framework; good reporting discipline | Less concrete about implementation contracts and thresholds | Adopt as primary conceptual backbone |
| `name-analysis-plan-2.md` | Practical pipeline design; useful position and temporal ideas; good investigative visual concepts | Composite score can become opaque; some test choices are less robust than permutation/simulation alternatives | Adopt operational pieces, avoid opaque single-score primacy |
| `name-analysis-plan-3.md` | Clear null hypotheses; two-tier matching; Monte Carlo + BH FDR framing is strong | Some distributional modeling adds complexity without clear gain for current data constraints | Adopt most of it, simplify where unnecessary |
| `name-analysis-plan-4.md` | Concise phased execution and intuitive visuals | Some heuristics are too strong (for example, near-certain identity assumptions from org/time) | Adopt structure, reject hard certainty heuristics |
| `name-analysis-plan-5.md` | Good baseline emphasis and reporting pragmatism | Static thresholds and ad hoc collapse rules risk biasing results | Adopt Monte Carlo emphasis, reject static collapse defaults |
| `name-analysis-plan-6.md` | Strong inferential rigor (simulation, permutation, effect sizes, sensitivity) | Over-conservative FWER stance can under-detect in exploratory/public-interest setting | Adopt robustness scaffolding; use FDR for discovery lists |
| `name-analysis-plan-7.md` | Deep theory on non-uniform collisions and linkage caveats; strong warnings about base-rate fallacy | Overly heavyweight for current feature set; recommends machinery (full FS/Kleinberg) beyond necessary complexity | Adopt key theory/caveats, defer heavyweight models |

---

## Final Statistical Specification

## 1) Data model and canonicalization
- Define a single shared canonicalization pipeline for all duplicate and voter analyses.
- Produce these deterministic keys for every row:
- `key_strict`: canonical last + canonical first + middle initial + suffix.
- `key_medium`: canonical last + canonical first.
- `key_loose`: canonical last + first initial.
- `key_nickname`: canonical last + canonical nickname-root(first).
- Keep raw name and parse diagnostics for auditability.
- Tag non-person/low-quality name rows (for example organization-like strings) and exclude them from inferential tests while still reporting counts.

## 2) Collision baseline using voter roll
- Primary baseline universe: WA voter records with `StatusCode=Active` from `/Users/sayhiben/dev/legislature-tools/data/raw/20260202_VRDB_Extract.txt` (92.56% of rows in current extract).
- Estimate empirical full-name probabilities from canonicalized active voter names.
- Use Monte Carlo null as primary inferential engine:
- For hearing size `n`, simulate `B=20,000` draws from empirical name distribution.
- Compute null distributions for duplicate metrics.
- Derive empirical one-sided p-values and simulation intervals.
- Analytical check: per-name Binomial/Poisson-tail approximation for speed and sanity cross-check.

## 3) Duplicate-name estimands and tests
- Report both row-level and unique-name-level metrics.
- Core estimands:
- Duplicate-row rate: rows belonging to names with count >=2 divided by total rows.
- Duplicate-name prevalence: names with count >=2 divided by unique names.
- Excess duplicate burden: observed minus expected under Monte Carlo baseline.
- Position-partitioned duplicate burden for Pro/Con/Other.
- Position concentration test:
- Primary: permutation test that shuffles position labels while preserving position totals and observed name multiplicities.
- Effect sizes: rate differences and rate ratios with bootstrap confidence intervals.
- Per-name anomaly list:
- For each name with observed count >=2, compute tail probability under null.
- Apply Benjamini-Hochberg at `q<=0.10`.
- Include rarity context and ambiguity indicators.
- Temporal anomaly add-on:
- For repeated names, compute minimum inter-arrival and short-window burst counts (5 and 15 minutes).
- Use timestamp permutation null to estimate burst significance.
- Treat temporal signals as corroborating evidence, not standalone proof.

## 4) Voter linkage estimands and tests
- Primary matching mode (chosen): conservative.
- Primary matched definition: exact canonical full-name agreement plus nickname-equivalent first-name with exact canonical last-name.
- Sensitivity-only tiers:
- Balanced: include strong fuzzy.
- Broad: include strong + weak fuzzy.
- Maintain explicit outcomes per row/name:
- `matched_unique`, `matched_ambiguous`, `unmatched`, plus tier used.
- Miss-rate estimands:
- Row-level miss rate by position.
- Unique-name-level miss rate by position (primary inferential unit).
- Position comparisons:
- Two-proportion comparisons with Wilson/Newcombe intervals.
- Fisher exact fallback for sparse cells.
- Report effect sizes and uncertainty, not only p-values.
- Keep language neutral: “unmatched to WA active voter file,” not fraud claims.

## 5) Swing-impact module
- Provide three scenario views for Pro/Con share:
- Raw rows.
- Strict unique-name dedupe.
- Excess-only collision adjustment by position (`observed duplicates - expected duplicates`, floored at zero).
- Purpose: quantify potential directional impact range without over-attributing identity.

---

## Important Public API / Interface / Type Changes

## Detector outputs
- Add explicit typed payload contracts for v2 name analyses in `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/contracts.py`.
- New/updated structures:
- Duplicate summary block with observed/expected/p-values/effect sizes.
- Position-partitioned duplicate block with low-power flags.
- Per-name anomaly table with `p_value`, `q_value`, rarity fields, and temporal burst indicators.
- Voter linkage summary block with match tiers, ambiguity counts, miss rates by position, and pairwise differences with CIs.
- Sensitivity block comparing conservative/balanced/broad outcomes.

## Analysis registry and report wiring
- Update `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/analysis_registry.py` as single source of truth.
- Re-enable:
- `duplicates_exact`
- `duplicates_near` (repurposed as sensitivity/near-match support analysis)
- `voter_registry_match`
- Update `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/render.py` with chart payload contracts and explanatory docs.
- Update `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/templates/report.html.j2` and partials for new chart/table sections.
- Update `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/static/report/main.js` for chart rendering and tooltips tied to new payload keys.

## Shared analysis modules (new)
- Introduce reusable modules to remove duplicated logic between detectors:
- `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/canonicalize.py`
- `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/nickname_map.py`
- `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/collision_baseline.py`
- `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/linkage.py`
- `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/names/stat_tests.py`

---

## Implementation Phases (Decision-Complete)

## Phase 0: Contract lock + fixtures
- Freeze current detector output snapshots and chart IDs to plan intentional deltas.
- Add deterministic fixtures from existing raw samples for large and small hearings.

## Phase 1: Canonicalization core
- Implement one canonical parser/normalizer used by both duplicate and voter detectors.
- Add parse diagnostics and non-person-name tagging.
- Wire nickname-root expansion from existing nickname config assets.

## Phase 2: Collision baseline engine
- Build active-voter full-name frequency table generation.
- Add Monte Carlo sampler and per-name tail-probability helpers.
- Add deterministic seed policy based on hearing identifier for reproducibility.

## Phase 3: Duplicate detector rewrite
- Rewrite `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_exact.py` to output:
- Overall observed-vs-expected duplicate metrics.
- Position-stratified duplicate metrics with permutation results.
- Per-name anomaly table with BH q-values.
- Temporal burst corroboration metrics.
- Refactor `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/duplicates_near.py` to focus on sensitivity/near-match diagnostics and ambiguity surfacing, not primary claims.

## Phase 4: Voter linkage detector rewrite
- Rewrite `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/detectors/voter_registry_match.py` for conservative primary outputs.
- Keep strong/weak fuzzy as sensitivity panels only.
- Add explicit ambiguous-match accounting and position-partitioned miss-rate inference.

## Phase 5: Report integration
- Update registry, payload builder, template sections, and frontend chart config together.
- Add chart docs that clearly separate descriptive vs inferential outputs and low-power behavior.
- Keep timezone conventions and global bucket/zoom behavior consistent with existing report UX contracts.

## Phase 6: Test + integration hardening
- Update and add unit/contract/integration tests (detailed below).
- Run CI scripts and targeted coverage checks.
- Validate no non-finite values in payload and no frontend runtime errors.

## Phase 7: Enable for focused runs
- Finalize `ANALYSES_TO_PERFORM` in `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/analysis_registry.py` with the rewritten analyses enabled.
- Rebuild sample report and capture stitched screenshot for UX verification.

---

## Visualizations and Tables to Implement

- Observed vs expected duplicate burden histogram with observed marker.
- Position-partitioned observed vs expected duplicate burden panel.
- Name rarity vs observed multiplicity scatter with significance encoding (`q` tiers).
- Per-name anomaly table (sortable) with observed count, expected count, `p`, `q`, position mix, min gap.
- Duplicate impact scenario bars (raw vs strict dedupe vs excess-adjusted).
- Voter linkage funnel by position (`matched_unique`, `ambiguous`, `unmatched`).
- Miss rate by position with confidence intervals and pairwise difference table.
- Sensitivity comparison chart across conservative/balanced/broad linkage modes.
- Temporal burst panel for repeated names (min-gap and short-window counts, with null comparison).

---

## Test Cases and Scenarios

## Unit tests
- Canonicalization equivalence: punctuation, diacritics, suffixes, token order, nickname roots.
- Candidate blocking and ambiguity behavior in linkage.
- Monte Carlo null reproducibility with fixed seed.
- Per-name tail probability and BH correction correctness.
- Low-power gate behavior for sparse subgroups.

## Detector tests
- Update `/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_bucketed_detectors.py`.
- Update `/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_duplicates_near.py`.
- Update `/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_voter_registry_match.py`.
- Add regression fixtures for:
- Rare-name repeated bursts.
- Common-name benign collisions.
- Position-imbalanced duplicate concentration.
- Ambiguous voter matches.

## Payload/render contract tests
- Update `/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_report_chart_payload.py`.
- Update `/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_report_render_helpers.py`.
- Verify chart IDs, section docs, and finite-safe serialization.

## Integration and UX checks
- Keep `/Users/sayhiben/dev/legislature-tools/testifier_audit/tests/test_pipeline_integration.py` parity coverage with at least one run containing rewritten analyses.
- Run:
- `/Users/sayhiben/dev/legislature-tools/testifier_audit/scripts/ci/lint.sh`
- `/Users/sayhiben/dev/legislature-tools/testifier_audit/scripts/ci/test.sh`
- Generate and visually validate report using:
- `/Users/sayhiben/dev/legislature-tools/testifier_audit/scripts/report/run_unified_report_and_capture.sh`

---

## Failure Modes and Mitigations (Built into Design)

- Common-name collision inflation: mitigated by voter-frequency baseline and per-name tail tests.
- Overclaiming identity from names: mitigated by explicit ambiguity classes and neutral language.
- False certainty from fuzzy matching: mitigated by conservative primary mode and sensitivity-only fuzzy tiers.
- Multiple testing inflation: mitigated by BH FDR (`q<=0.10`) for per-name lists.
- Sparse subgroup instability: mitigated by low-power flags and descriptive-only fallback.
- Demographic mismatch between testifiers and voter baseline: mitigated by explicit caveats and hearing-level comparative context where available.
- Temporal false positives from global surges: mitigated by permutation null that preserves hearing time structure.
- Contract drift across report surfaces: mitigated by synchronized updates to registry/render/template/frontend and contract tests.

---

## Assumptions and Defaults (Locked)

- Primary voter baseline scope: `StatusCode=Active` records.
- Primary linkage mode: Conservative.
- Inferential discovery threshold for per-name anomalies: BH `q<=0.10`.
- Fuzzy linkage tiers remain sensitivity-only in public-facing primary metrics.
- Inference unit for position miss-rate comparisons: unique-name level primary, row-level secondary.
- Monte Carlo draws per hearing: 20,000 by default.
- Low-power rule: inferential position claims only when subgroup size and expected duplicate support are adequate; otherwise descriptive-only with explicit flag.
- Timezone handling remains Pacific (`America/Los_Angeles`) and consistent with current report conventions.
- No feature flags or legacy compatibility shims are added; this is a direct v2 replacement in pre-production.

---

## Acceptance Criteria

- Rewritten duplicate and voter analyses are enabled and run from registry-defined focused scope.
- Report sections render with no JS/runtime payload errors and no non-finite JSON values.
- Outputs clearly distinguish observed, expected, excess, and uncertainty.
- Per-name anomaly outputs are multiplicity-corrected and ambiguity-tagged.
- Position miss-rate comparisons include effect sizes and confidence intervals.
- Unit, contract, and integration tests pass, including updated pipeline integration parity.
