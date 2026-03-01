# Duplicate Names Refinement - Implementation Notes

## Work Item ID Policy
- Keep `DUP-xxx` numbers stable as permanent identifiers.
- Do not renumber work items to reflect execution order.
- Track real execution order in this notes file so references in commits/PRs/discussion remain stable.

## Implementation Sequence (Actual + Planned)
Execution order is tracked here independently from ticket numbering.

### Completed (actual)
1. `DUP-001` (Done, 2026-02-28)
2. `DUP-008` (Done, 2026-02-28)
3. `DUP-009` (Done, 2026-02-28)
4. `DUP-010` (Done, 2026-02-28)
5. `DUP-002` (Done, 2026-02-28)
6. `DUP-003` (Done, 2026-02-28)
7. `DUP-004` (Done, 2026-02-28)

### Current
1. `DUP-005` (next in-progress target)

### Planned next order (subject to reprioritization)
1. `DUP-005`
2. `DUP-007`
3. `DUP-006`
4. `DUP-011`
5. `DUP-012`
6. `DUP-013`
7. `DUP-014`
8. `DUP-015`
9. `DUP-016`
10. `DUP-017`
11. `DUP-018`
12. `DUP-019`
13. `DUP-020`
14. `DUP-021`

## Scope Covered
These notes capture implementation takeaways from **DUP-001**, **DUP-008**, **DUP-009**, **DUP-010**, **DUP-002**, **DUP-003**, and **DUP-004**, including code-level contract decisions, QA observations, and planning impacts for upcoming work items.

Date: 2026-02-28  
Work item: DUP-001 (P0)

## What Was Implemented
- Added a detector-facing statistical contract in duplicate outputs:
  - `summary.statistical_contract`
  - `collision_methods` baseline/status/claim metadata
- Standardized semantics:
  - primary estimand: name-key collision burden vs reference baseline
  - baseline semantics: reference model (not the data-generating process)
  - explicit non-goals: no identity/intent/IP/per-person duplication claims
- Added report-level contract payload:
  - `interactive_charts.controls.duplicate_statistical_contract`
  - includes interpretation/limitations callouts plus chart/table declarations
- Updated duplicate analysis voice and labels:
  - "Duplicate-Name Collisions" language
  - calibrated wording ("higher than expected under baseline", "follow-up signal")
  - removed manipulation/identity certainty language from duplicate report surfaces
- Added UI callouts and declaration notes:
  - chart/table baseline source
  - inferential status
  - gating constraints
- Added tests for:
  - detector contract fields
  - payload contract presence and row-level metadata
  - report rendering copy/callouts and integration coverage

## Verification Notes
- Full test suite passed after implementation (`scripts/ci/test.sh`).
- Lint passed (`scripts/ci/lint.sh`).
- Rerender + live Playwright QA completed on ESSB 6346 (desktop + mobile):
  - duplicate contract callout present
  - duplicate chart declarations visible (baseline/status/gating)
  - no failed data requests; only expected local `favicon.ico` 404
- Additional manual QA checks were also run on representative hearings for language/declaration consistency.

## Problem Areas Encountered
- Report payload path gotcha:
  - duplicate contract is under `interactive_charts.controls`, not a top-level `interactive_payload` node.
  - This should be documented in future payload consumers/tests to avoid false "missing field" conclusions.
- Generated report artifacts are easy to accidentally dirty while QA:
  - rerender/serve cycles modify `reports/<hearing>/...`.
  - restore generated artifacts before finalizing source-only code changes unless explicitly requested.
- External frozen fixture drift surfaced during full CI:
  - `tests/fixtures/methodology/external/expected/febrl_dataset1_duplicates_fast.json`
  - required deterministic expectation updates for `Con` p50 fields.
  - This was not directly DUP-001 copy work but blocked green CI.

## Guardrails For Remaining Work
- Preserve the DUP-001 contract object and callout structure even when later work changes inferential logic.
- Do not reintroduce ambiguous terms:
  - avoid "duplicate submissions", "manipulated", "proof", or identity/intent assertions.
- Every duplicate chart/table should continue to declare:
  - baseline source/label
  - inferential status
  - gating/low-power constraints
- If a later ticket suppresses p/q/z (for descriptive-only/unavailable), keep contract fields present and explicit rather than omitting sections.

## Recommendations For Upcoming Tickets
- DUP-008 and DUP-009 should explicitly preserve DUP-001 disclosure surfaces while changing inferential availability.
- DUP-010 should reuse DUP-001 declaration plumbing when introducing additional baseline families so users can always tell which expectation is being shown.

---

## DUP-008 Implementation Addendum

Date: 2026-02-28  
Work item: DUP-008 (P0)

## What Was Implemented
- Added explicit inferential reason taxonomy to duplicates detector outputs:
  - `reference_model_inference_available`
  - `self_referential_baseline`
  - `degraded_to_self_referential_baseline`
  - `analytic_only_no_null_samples`
  - `low_power_support`
- Added scope-level inferential resolution in detector runtime:
  - non-inferential scope states now emit `inferential_status` + `inferential_reason`
  - inferential metrics are masked to `NA` when scope is descriptive-only/unavailable
- Removed fake-null defaults in non-inferential paths:
  - no more sentinel `p=1` / `z=0` for skipped inference paths
  - per-name and temporal inferential fields are nulled when status is non-inferential
- Propagated status/reason into payload tables and declarations:
  - payload builder now includes/fills `inferential_reason`
  - duplicate chart/table contract metadata includes reason context
  - statistical contract payload remains present in descriptive-only/unavailable states
- Added detector logging for non-inferential scope execution to improve run-time observability.

## Tests Added/Updated
- `tests/test_duplicates_exact.py`
  - updated degraded-baseline assertions to require descriptive-only + reason propagation
  - new tests for self-referential suppression and analytic-only/no-null-sample suppression
  - per-name tested totals test adjusted to use inferential-capable setup with mocked histogram source
- `tests/test_report_chart_payload.py`
  - assertions for reason propagation + inferential-field masking in descriptive-only outputs
  - new payload test for unavailable status masking (`p/q/significance` and temporal p-values)
- Full suite run via `./testifier_audit/scripts/ci/test.sh` passed:
  - `323 passed` (warnings only; no failures)

## Runtime / Report Validation
- Exercised ESSB 6346 with both run modes:
  - unified run: `scripts/report/run_unified_report.sh`
  - rerender-only: `python -m testifier_audit.cli report ...`
- Playwright MCP checks completed for both target viewports:
  - desktop `1728x1117`
  - mobile `390x844`
- Verified in report:
  - duplicate statistical contract callout remains visible
  - baseline / inferential-status / gating declarations remain visible on duplicate charts/tables
  - bucket, theme, zoom/reset controls work and persist expected URL state
  - no console warnings/errors during validation run; data requests returned `200`

## Issue Discovered During QA
- Shared marker synchronization works from off-hours funnel interactions, but automated click attempts on duplicate timing scatter (`duplicates_exact_top_name_timing_exact`) did not move linked timeline markers in this run.
- This behavior was observed during QA of DUP-008 outputs and appears orthogonal to inferential masking logic; leave as follow-up unless DUP-009 work touches linked-marker event handling.

## Remaining Work / Handoff
- Move to `DUP-009` (scope semantics and unavailable-scope handling).
- Preserve DUP-008 inferential status/reason fields as orthogonal metadata when implementing `scope_status`/`scope_reason`.

---

## DUP-009 Implementation Addendum

Date: 2026-02-28  
Work item: DUP-009 (P0)

## What Was Implemented
- Preserved explicit duplicate scope semantics in detector runtime:
  - no silent aliasing from `matched_only`/`unmatched_only` to `full_hearing`
  - added scope availability taxonomy:
    - `scope_status`: `available` | `unavailable`
    - `scope_reason`: `available` | `unavailable_missing_match_assignments` | `unavailable_no_person_rows` | `unavailable_no_rows_after_filtering`
- Kept scope availability separate from inferential status:
  - inferential unavailability for unavailable scopes uses `inferential_reason=scope_unavailable`
  - DUP-008 inferential taxonomy remains orthogonal metadata (not overloaded for scope semantics)
- Removed fallback behavior that changed estimand:
  - eliminated `infer = working.copy()` fallback when inference frame is empty after person filtering
  - in unavailable scopes, inferential metrics remain null-masked under existing DUP-008 masking rules
- Propagated scope availability metadata across detector outputs:
  - `collision_methods`, `collision_overview`, `per_name_tests`, `per_name_display`,
    `temporal_burst_signals`, `collision_by_bucket`, `collision_by_bucket_position`
  - summary now includes `scope_status`, `scope_reason`, and `scope_availability`
- Added runtime observability:
  - profiling counter `detector.duplicates_exact.scope.unavailable_count`
- Updated report payload/build behavior:
  - payload normalization now carries `scope_status`/`scope_reason`
  - duplicate scope controls include `duplicate_collision_scope_availability`
  - unavailable scopes are excluded from `duplicate_collision_scope_options`
  - default scope resolves to an available scope (fallback to primary scope only if option list is empty)
  - duplicate methodology/runtime entries now include scope status/reason and caveats for unavailable scopes
- Updated frontend duplicate controls:
  - reads `duplicate_collision_scope_availability`
  - surfaces unavailable-scope reason text on scope control label/select tooltips when applicable

## Tests Added/Updated
- `tests/test_duplicates_exact.py`
  - added regressions for:
    - missing match assignments => `matched_only` unavailable
    - malformed assignments => scoped unavailable responses
    - no-person rows after filtering => unavailable, no full-frame fallback
    - requested scope empty after filtering => unavailable
  - expanded collision-methods contract assertions for `scope_status`/`scope_reason`
- `tests/test_report_chart_payload.py`
  - added payload regression for duplicate scope controls:
    - unavailable scopes excluded from options
    - default scope resolves to available entry
    - availability reasons preserved and shown in methodology caveats

## Runtime / Report Validation
- ESSB 6346 end-to-end run completed:
  - `scripts/report/run_unified_report.sh` with hearing metadata and VRDB extract
  - rerender pass via `python -m testifier_audit.cli report`
- Playwright MCP validation completed on:
  - desktop `1728x1117`
  - mobile `390x844`
- Verified:
  - duplicate controls remain stable (mode/unit toggles, bucket sync)
  - no console errors/warnings
  - no failed data requests (all chart/data fetches `200`)

## Issue Discovered During QA
- ESSB 6346 run currently exposes only `full_hearing` as an available duplicate scope.
- This is expected for the current detector configuration (`detector.duplicates_exact.scope.count = 1`) and not a scope-fallback regression.
- Resulting UI behavior is intentional for this run: scope selector stays hidden because there are no alternate available scopes to choose from.

## Remaining Work / Handoff
- Move to `DUP-010` (report-layer baseline math + baseline-family separation).
- Preserve DUP-009 scope-availability semantics when introducing additional baseline families and labels.

---

## DUP-010 Implementation Addendum

Date: 2026-02-28  
Work item: DUP-010 (P0)

## What Was Implemented
- Replaced report-layer `names_anywhere` expectation with occupancy-based math that uses hearing multiplicities:
  - expected distinct names in a bucket now uses
    `sum(1 - C(N-c, n) / C(N, n))` across duplicated-name multiplicities per scope/match mode
  - retained explicit fallback path (`row_share_fallback_missing_multiplicity`) when multiplicity profiles are unavailable
- Preserved `rows_anywhere` as report-layer proportional-share expectation and made it explicit in labels/metadata.
- Added explicit baseline-family metadata on duplicate bucket chart rows:
  - report-layer fields:
    - `report_baseline_family`
    - `report_baseline_label`
    - `report_baseline_method`
    - `report_baseline_method_label`
    - `unit_expected_names_method`
  - detector-side context fields:
    - `detector_baseline_family`
    - `detector_baseline_family_label`
    - `detector_baseline_label`
- Updated duplicate chart UX labeling:
  - expected series label now renders as `Expected (report baseline)` (not generic/unlabeled expected duplicates)
  - duplicate chart note now states report baseline and detector baseline context together
  - names unit note shows occupancy method; rows unit shows proportional-share method
- Updated explanatory/report copy to align with new semantics:
  - duplicate analysis registry “how to read” text now distinguishes rows vs names baseline logic
  - duplicate chart help docs now document report baseline split and detector/VRDB distinction
  - column docs updated for expected-fields semantics and new baseline metadata fields
- Added additional methodology caveat text documenting separation between report-layer expected lines and detector/VRDB collision baselines.

## Tests Added/Updated
- `tests/test_report_chart_payload.py`
  - updated existing expected values where `names_anywhere` switched from linear share to occupancy
  - added hand-checkable occupancy test (`N=6, n=2, counts=[3,2]`) to validate exact expectation
  - added assertions for baseline-family metadata and method fields in duplicate bucket rows
- Focused suites passed:
  - `tests/test_report_chart_payload.py`
  - `tests/test_analysis_registry.py`
  - `tests/test_report_render_helpers.py`
- Full suite passed via `./testifier_audit/scripts/ci/test.sh`:
  - `329 passed` (warnings only; no failures)

## Runtime / Report Validation
- ESSB 6346 exercised in both run modes:
  - unified run: `scripts/report/run_unified_report.sh`
  - rerender: `python -m testifier_audit.cli report ...`
- Playwright MCP validation completed for:
  - desktop `1728x1117`
  - mobile `390x844`
- Verified:
  - duplicate unit toggle updates baseline note correctly:
    - rows => proportional-share wording
    - names => occupancy wording
  - duplicate declaration content still includes baseline/inferential/gating disclosures from DUP-001
  - no console errors/warnings
  - no failed data requests (data/chart fetches all `200`)

## Issues Discovered During Implementation
- Relative path invocation for `run_unified_report.sh` failed in this environment (`Submissions CSV not found`) while absolute paths succeeded.
- Multiplicity-aware occupancy requires per-name counts; fallback labeling was kept explicit when those counts are unavailable in payload build inputs.

## Remaining Work / Handoff
- Move to `DUP-002`.
- Keep DUP-010 baseline-family naming consistent when subsequent tickets modify duplicate chart/table presentation logic.

---

## DUP-002 Implementation Addendum

Date: 2026-02-28  
Work item: DUP-002 (P0)

## What Was Implemented
- Added a shared, versioned normalization layer:
  - new module: `src/testifier_audit/names/normalization.py`
  - emits:
    - `full_name_key` (policy-defined full-name collision key; strict key form)
    - `first_name_key`
    - `last_name_key`
    - `normalization_version`
    - `normalization_version_hash`
  - centralizes composition + canonicalization for component-based inputs (`compose_person_name`) and raw name strings (`normalize_name_record`)
- Updated submission-side normalization call site:
  - `preprocess/names.py` now uses shared normalization module and writes new key/version fields to the working frame while preserving legacy canonical/collision fields.
- Updated VRDB normalization/import path to use the same normalization module:
  - `io/vrdb_postgres.py`
  - `normalize_vrdb_chunk(...)` now canonicalizes VRDB rows via shared normalization (instead of VRDB-specific token normalization logic)
  - persisted VRDB normalization metadata:
    - `full_name_key`, `first_name_key`, `last_name_key`
    - `name_normalized`
    - `normalization_version`, `normalization_version_hash`
- Updated VRDB schema/indexes/backfill:
  - new columns + indexes added in `ensure_voter_registry_schema(...)`
  - legacy-row backfill populates new key/version fields
  - importer version bumped to `vrdb_extract_v3` to reflect schema/normalization changes
- Updated VRDB import orchestration/CLI:
  - `cli import-vrdb` now passes names config normalization settings (`nickname_map_path`, `normalize_unicode`, `strip_punctuation`) into VRDB import
  - CLI output now includes normalization version + hash
- Updated VRDB key-query defaults for collision-oriented helpers to use full-name key by default:
  - `fetch_matching_voter_keys` default `key_column="full_name_key"`
  - `fetch_voter_name_key_frequencies` / `fetch_voter_name_key_count_histogram` / `fetch_voter_name_key_stratum_frequencies` defaults now `full_name_key`
  - `fetch_matching_voter_names(...)` remains explicit on `canonical_name` for voter-linkage behavior
- Propagated normalization version metadata into duplicates detector/report contracts:
  - `duplicates_exact.collision_methods` now includes `normalization_version` + `normalization_version_hash`
  - detector summary now includes same version metadata
  - payload builder/runtime includes `normalization_version` as expected duplicate methods metadata field.

## Tests Added/Updated
- `tests/test_names.py`
  - asserts new shared key/version columns in submission preprocessing output
- `tests/test_vrdb_postgres.py`
  - asserts new VRDB normalized key/version fields (`full_name_key`, `first_name_key`, `last_name_key`, normalization metadata)
  - updated import tests for new VRDB import result metadata
  - extended parity test to include new key fields between submission canonicalization and VRDB chunk normalization
- `tests/test_cli.py`
  - updated `import-vrdb` command test fixture/signature for new normalization parameters and output metadata.
- Focused suites run:
  - `tests/test_names.py`
  - `tests/test_vrdb_postgres.py`
  - `tests/test_cli.py`
  - `tests/test_duplicates_exact.py`
  - `tests/test_report_chart_payload.py`
- Full suite run via `./testifier_audit/scripts/ci/test.sh`:
  - `329 passed`
- Lint run via `./testifier_audit/scripts/ci/lint.sh` passed.

## Runtime / Report Validation (ESSB 6346)
- Executed:
  - unified report run on ESSB 6346 (skip-import mode for final QA run after migration-lock issue triage)
  - report rerender (`python -m testifier_audit.cli report ...`)
  - Playwright MCP validation on:
    - desktop `1728x1117`
    - mobile `390x844`
- Verified:
  - duplicate unit toggle (`rows` ↔ `names`) updates baseline wording as expected
  - names unit note explicitly uses occupancy method wording
  - bucket synchronization and responsive controls worked in both viewports
  - data requests returned `200` for report payload shards
  - only console error was expected local `favicon.ico` `404`.

## Issues Discovered During Implementation
- First post-schema-change VRDB import can be expensive:
  - `ensure_voter_registry_schema` backfill over large `voter_registry` table triggered long-running update.
- Interrupting that one-time backfill left an active DB backend holding relation locks, which blocked subsequent report runs until backend termination.
- Mitigation used during QA run:
  - terminated lingering backend
  - validated ESSB run path using `--skip-vrdb-import --skip-submissions-import` once imports/data were already present.

## Remaining Work / Handoff
- Move to `DUP-003` (versioned VRDB probability artifacts and geography-aware backoff).
- Preserve DUP-002 shared normalization layer as the single source for submission + VRDB key generation; do not introduce sidecar-only normalizers in DUP-003+.

---

## DUP-003 Implementation Addendum

Date: 2026-02-28  
Work item: DUP-003 (P0)

## What Was Implemented
- Added a dedicated VRDB probability artifact builder module:
  - `src/testifier_audit/io/vrdb_probability_artifacts.py`
  - supports versioned artifact generation with:
    - probability rows (state/county/city-conditioned where supported)
    - deterministic geography backoff rows (`city -> county -> state`)
    - metadata with provenance + checksums
- Implemented denominator variants:
  - `all_registrants`
  - `active_only`
- Implemented conservative geography backoff policy with deterministic metadata:
  - requested and effective geography level/value
  - fallback step count
  - fallback reason and county city-coverage signal
- Extended VRDB ingest/schema for geography fields used by conditioned baselines:
  - `reg_city`
  - `county_code`
  - importer version bumped from `vrdb_extract_v3` to `vrdb_extract_v4`
- Added CLI entrypoint:
  - `python -m testifier_audit.cli build-vrdb-probability-artifacts ...`
  - emits artifact CSVs + metadata JSON with SHA256 hashes.

## Tests Added/Updated
- Added `tests/test_vrdb_probability_artifacts.py`:
  - deterministic threshold/backoff behavior
  - active-only vs all-registrants denominator separation
  - reproducible checksum output across repeated rebuilds
  - threshold validation guardrails
- Updated `tests/test_vrdb_postgres.py`:
  - geography normalization assertions (`reg_city`, `county_code`)
  - schema/index assertions for new geography columns
- Updated `tests/test_cli.py`:
  - command coverage for `build-vrdb-probability-artifacts`
  - missing-DB-URL validation for the new command
- Verification runs:
  - focused suites for new/modified modules passed
  - full suite passed via `./testifier_audit/scripts/ci/test.sh`:
    - `335 passed`
  - lint passed via `./testifier_audit/scripts/ci/lint.sh`.

## Runtime / Report Validation (ESSB 6346)
- Executed unified run with explicit ESSB source and skip-import mode:
  - `scripts/report/run_unified_report.sh --skip-imports .../ESSB6346-20260224-0800.csv ...`
- Executed report rerender:
  - `python -m testifier_audit.cli report --out ../reports/ESSB6346-20260224-0800 ...`
- Playwright MCP checks completed on:
  - desktop `1728x1117`
  - mobile `390x844`
- Verified:
  - sidebar/menu toggle works on desktop/mobile
  - global controls expand/collapse works on mobile
  - bucket switch updates linked notes and retains chart-level fallback messaging where bucket is unavailable
  - theme toggle works
  - no failed report-data requests (all report-data shard fetches `200`)
  - only console error observed was expected local `favicon.ico` `404`.

## Artifact Build Evidence
- Extract-backed full artifact generation completed (chunked):
  - `output/dup003/vrdb_name_probabilities.csv` (30,421,343 rows)
  - `output/dup003/vrdb_geo_backoff.csv` (1,471 rows)
  - `output/dup003/vrdb_probability_artifacts.json`
- Recorded checksums:
  - probability rows SHA256: `2b08287b3ca5883b2e4a8bee80dfcbfffb7aa4113b04d4cd07eae95a3276080d`
  - backoff rows SHA256: `4fa89a74779939771a4ce7a7171b69b7e039f71aecc973eff4b110b1e8e40ce9`.

## Issues Discovered During Implementation
- Legacy local DB snapshots may not yet include DUP-002 key columns (`full_name_key` et al), which blocks the DB-backed artifact builder path until the one-time schema/backfill completes.
- The one-time `ensure_voter_registry_schema` backfill can run for a long duration on large existing `voter_registry` tables.
- Mitigation used for DUP-003 runtime verification in this environment:
  - used extract-backed artifact generation path (same normalization + artifact logic) to validate end-to-end output and reproducibility without waiting on the full DB migration.

## Remaining Work / Handoff
- Move to `DUP-004` (dedicated VRDB collision-null engine per analysis slice).
- Reuse DUP-003 artifacts as the sole probability/backoff source for DUP-004 slice-level expectation calculations.

---

## DUP-004 Implementation Addendum

Date: 2026-02-28  
Work item: DUP-004 (P0)

## What Was Implemented
- Added a dedicated VRDB collision-null engine module:
  - `src/testifier_audit/io/vrdb_collision_null.py`
  - key functionality:
    - `compute_vrdb_collision_null_for_slices(...)` for per-slice collision-null computation
    - `load_vrdb_probability_artifacts(...)` and `write_vrdb_collision_null_tables(...)` helpers
- Implemented per-slice null-model computation with slice-local `N`:
  - observed outputs:
    - `observed_pairs`
    - `n_unique_names`
    - `observed_max_name_count`
  - expected outputs:
    - `expected_pairs_analytic`
    - `expected_pairs_closed_form` (`C(N,2) * Σ p_i^2`)
    - Monte Carlo summary metrics:
      - `expected_pairs_mean`
      - `expected_pairs_median`
      - `expected_pairs_p95`
      - `expected_pairs_p99`
      - `tail_prob_pairs`
    - optional max-repeat reference metrics with availability flags
  - inferential metadata:
    - `inferential_status`
    - `inferential_reason`
    - Monte Carlo draw accounting fields
- Added additive evidence-family labeling to all outputs:
  - `evidence_family = "vrdb_collision_null"`
- Implemented expected-per-name outputs:
  - top observed names per slice with:
    - `observed_count`
    - `expected_count`
    - `overrun_count`
    - `expected_share`
- Added runtime logging per slice for traceability during ESSB execution.

## Tests Added/Updated
- Added `tests/test_vrdb_collision_null.py` with coverage for DUP-004 acceptance criteria:
  - closed-form toy-case expected-pairs check
  - simulation parity check vs multinomial expectation
  - regression proving bucket expectations are not derived by linear rescale from full-hearing expectation
  - geography backoff resolution check (requested vs effective geography)
- Verification runs:
  - focused suite: `python -m pytest tests/test_vrdb_collision_null.py -q` passed
  - lint: `./testifier_audit/scripts/ci/lint.sh` passed
  - full suite: `./testifier_audit/scripts/ci/test.sh` passed:
    - `339 passed` (warnings only; no failures)

## Runtime / Report Validation (ESSB 6346)
- Unified run executed:
  - `scripts/report/run_unified_report.sh --skip-imports ...ESSB6346-20260224-0800.csv ...20260202_VRDB_Extract.txt ...ESSB6346-20260224-0800.hearing.yaml`
  - log captured at:
    - `output/run_logs/dup004_essb6346_unified.log`
- Rerender executed:
  - `python -m testifier_audit.cli report --out ../reports/ESSB6346-20260224-0800 ...`
  - log captured at:
    - `output/run_logs/dup004_essb6346_rerender.log`
- Direct DUP-004 engine exercise against ESSB slices completed:
  - used DUP-003 probability/backoff artifacts from `output/dup003/`
  - computed full hearing + 60-minute slices
  - logs captured at:
    - `output/run_logs/dup004_essb6346_null_engine.log`
  - outputs written:
    - `output/dup004/essb6346_vrdb_collision_metrics.csv`
    - `output/dup004/essb6346_vrdb_collision_expected_names.csv`
  - observed summary from run:
    - `probability_rows=4,974,281` (state/full-name/all-registrants slice of artifact)
    - `metrics_rows=136`
    - `expected_name_rows=2,040`
    - full-hearing row: `n_rows=129,971`, `observed_pairs=64,758`, `expected_pairs_analytic=2,369.304`
- Playwright MCP checks completed:
  - desktop `1728x1117`
  - mobile `390x844`
  - verified:
    - sidebar/menu toggle
    - global controls expand/collapse on mobile
    - bucket-switch propagation across chart note/caveat surfaces
    - theme toggle behavior
    - report-data shard requests all `200`
  - only console error observed: expected local `favicon.ico` `404`.

## Issues Discovered During Implementation
- ESSB time parsing in the ad hoc engine-exercise script produced a pandas warning about format inference.
  - impact was limited to runtime validation scripting and did not affect committed module behavior.
- Some burst charts intentionally fall back to `60m` when requested `30m` is unavailable; this is expected and surfaced explicitly in chart notes.

## Remaining Work / Handoff
- Move to `DUP-005` (integrate VRDB collision metrics as additive sidecar evidence family).
- Keep DUP-004 outputs additive and isolated; do not replace existing detector outputs when wiring report/payload integration.
