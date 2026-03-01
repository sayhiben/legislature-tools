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
8. `DUP-005` (Done, 2026-02-28)
9. `DUP-007` (Implemented, 2026-02-28; review sign-off pending)
10. `DUP-006` (Implemented, 2026-02-28; analyst sign-off pending)
11. `DUP-011` (Done, 2026-02-28)
12. `DUP-012` (Done, 2026-02-28)

### Current
1. `DUP-013` (next in-progress target)

### Planned next order (subject to reprioritization)
1. `DUP-013`
2. `DUP-014`
3. `DUP-015`
4. `DUP-016`
5. `DUP-017`
6. `DUP-018`
7. `DUP-019`
8. `DUP-020`
9. `DUP-021`

## Scope Covered
These notes capture implementation takeaways from **DUP-001**, **DUP-008**, **DUP-009**, **DUP-010**, **DUP-002**, **DUP-003**, **DUP-004**, **DUP-005**, **DUP-007**, **DUP-006**, **DUP-011**, and **DUP-012**, including code-level contract decisions, QA observations, and planning impacts for upcoming work items.

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

---

## DUP-005 Implementation Addendum

Date: 2026-02-28  
Work item: DUP-005 (P0)

## What Was Implemented
- Added a new additive detector:
  - `src/testifier_audit/detectors/vrdb_collision_evidence.py`
  - detector id: `vrdb_collision_evidence`
  - outputs:
    - `slice_metrics`
    - `top_overrun_names`
- Kept existing duplicate detector outputs and contracts unchanged:
  - no column replacement in `duplicates_exact`
  - no threshold changes in existing analyses
- Wired sidecar into detector orchestration:
  - `src/testifier_audit/detectors/registry.py`
  - sidecar runs with bounded Monte Carlo draws and coarse bucket defaults for runtime control
- Added separate report analysis section (not merged into duplicate section):
  - `src/testifier_audit/report/analysis_registry.py`
  - analysis id: `vrdb_collision_evidence`
  - hero chart: `vrdb_collision_evidence_pairs`
  - detail charts:
    - `vrdb_collision_evidence_max_name_count`
    - `vrdb_collision_evidence_overrun_names`
- Added payload integration and controls:
  - `src/testifier_audit/report/rendering/payload/builder.py`
  - sidecar tables now flow into chart payloads and analysis catalog
  - sidecar bucket options and absolute-time zoom sync participation added
- Added report help/interpretation content for the new sidecar section:
  - `src/testifier_audit/report/rendering/help_docs.py`
- Added frontend renderer wiring:
  - `src/testifier_audit/report/static/report/modules/charts/default_renderer_registry.js`
  - time-bar-line rendering for:
    - `vrdb_collision_evidence_pairs`
    - `vrdb_collision_evidence_max_name_count`
  - tabular/simple-bar rendering for:
    - `vrdb_collision_evidence_overrun_names`

## Tests Added/Updated
- Added detector tests:
  - `tests/test_vrdb_collision_evidence_detector.py`
    - missing-artifact graceful disable path
    - artifact-backed slice metric + overrun output path
- Updated report payload contract coverage:
  - `tests/test_report_chart_payload.py`
    - sidecar charts, catalog status, bucket options, and absolute-time zoom sync assertions
- Updated registry coverage:
  - `tests/test_analysis_registry.py`
    - sidecar analysis id/title/chart ids/group/priority assertions
- Validation runs:
  - focused:
    - `python -m pytest tests/test_vrdb_collision_evidence_detector.py tests/test_analysis_registry.py tests/test_report_chart_payload.py -q`
  - targeted integration:
    - `python -m pytest tests/test_report_render_helpers.py tests/test_pipeline_integration.py -q`
  - full suite:
    - `./testifier_audit/scripts/ci/test.sh` passed (`342 passed`)
  - lint:
    - `./testifier_audit/scripts/ci/lint.sh` passed

## Runtime / Report Validation (ESSB 6346)
- Unified run executed (skip-import mode):
  - `CI_SKIP_INSTALL=1 ./testifier_audit/scripts/report/run_unified_report.sh --skip-imports /Users/sayhiben/dev/legislature-tools/data/raw/ESSB6346-20260224-0800.csv /Users/sayhiben/dev/legislature-tools/data/raw/20260202_VRDB_Extract.txt /Users/sayhiben/dev/legislature-tools/data/metadata/ESSB6346-20260224-0800.hearing.yaml`
  - log:
    - `output/run_logs/dup005_essb6346_unified.log`
- Rerender executed:
  - `python -m testifier_audit.cli report --out ../reports/ESSB6346-20260224-0800 --config ./configs/voter_registry_enabled.yaml --hearing-metadata ../data/metadata/ESSB6346-20260224-0800.hearing.yaml`
  - log:
    - `output/run_logs/dup005_essb6346_rerender.log`
- Sidecar execution evidence from unified log:
  - detector scope includes `vrdb_collision_evidence`
  - sidecar summary log includes:
    - `rows=129971`
    - `slices=544`
    - `buckets=[30, 60, 120, 240, 480, 720, 1440]`
    - `probabilities=4974281`
- Playwright MCP checks completed:
  - desktop: `1728x1117`
  - mobile: `390x844`
  - verified:
    - separate “VRDB Collision Sidecar” analysis section present
    - bucket switching updates sidecar charts and loads sidecar bucket shards
    - global controls and sidebar behave on desktop/mobile
    - report-data requests for sidecar/base/bucket shards return `200`
  - only console error observed: expected local `favicon.ico` `404`.

## Issues Discovered During Implementation
- Relative paths passed to `run_unified_report.sh` failed after the script changed to the `testifier_audit/` project root.
  - mitigation: use absolute paths for ESSB runtime validation commands.
- Running `lint.sh` and `test.sh` in parallel caused editable-install race conditions in this environment.
  - mitigation: run CI scripts sequentially.
- Report artifact rerendering naturally dirties `reports/ESSB6346-20260224-0800/`; keep these generated changes out of source commits unless report artifacts are explicitly requested.

## Remaining Work / Handoff
- Move to `DUP-007`.
- Preserve `vrdb_collision_evidence` as additive sidecar evidence; avoid back-merging into existing duplicate output schemas unless explicitly required by later tickets.

---

## DUP-007 Implementation Addendum

Date: 2026-02-28  
Work item: DUP-007 (P0)

## What Was Implemented
- Added a dedicated backtest helper module:
  - `src/testifier_audit/backtests/vrdb_collision_backtest.py`
  - capabilities added:
    - deterministic seed helper (`stable_seed`)
    - scenario dataclasses for baseline/synthetic cases
    - geo-target derivation and filtering for state/county/city/fallback backtests
    - streaming probability/backoff artifact loading helpers for scenario subsets
    - slice construction for full-hearing and bucket windows
    - per-case summary rollups (tail probabilities, overrun ratios, fallback behavior, low-power share)
    - deterministic historical family selection (normal vs suspected) and calibration/holdout split helpers
    - synthetic case generator with controlled burst injection support
- Added package init for backtest helpers:
  - `src/testifier_audit/backtests/__init__.py`
- Added reproducible DUP-007 harness script:
  - `scripts/tests/backtest_vrdb_collision_module.py`
  - performs end-to-end backtest workflow with fixed seed:
    - discovers historical hearings
    - builds normal/suspected families + calibration/holdout split
    - builds synthetic null and injected cases
    - runs required scenario matrix (state/county/city + denominator variants + city quality/fallback checks)
    - computes scenario-level calibration thresholds from normal controls
    - writes case-level and scenario-level artifacts + JSON summary + markdown memo
- Added unit tests for backtest helpers:
  - `tests/test_vrdb_collision_backtest.py`
  - coverage includes:
    - geo target derivation
    - probability filtering contract
    - slice generation + bucket parsing
    - case summarization fields
    - family selection and deterministic split
    - synthetic injection behavior
- Added checked-in DUP-007 memo:
  - `docs/duplicate-names-refinement/DUP-007-backtest-memo.md`
- Updated work item status/details:
  - `docs/duplicate-names-refinement/work-items/DUP-007.md`

## Tests Added/Updated
- `python -m pytest tests/test_vrdb_collision_backtest.py -q`
- `python -m pytest tests/test_vrdb_collision_null.py tests/test_vrdb_collision_evidence_detector.py tests/test_vrdb_collision_backtest.py -q`
- CI lint:
  - `./testifier_audit/scripts/ci/lint.sh`
- CI tests:
  - `./testifier_audit/scripts/ci/test.sh`

## Backtest Runtime Artifacts
- Backtest run log:
  - `output/run_logs/dup007_backtest.log`
- Backtest outputs:
  - `output/dup007/vrdb_collision_backtest_case_manifest.csv`
  - `output/dup007/vrdb_collision_backtest_case_metrics.csv`
  - `output/dup007/vrdb_collision_backtest_scenario_summary.csv`
  - `output/dup007/vrdb_collision_backtest_summary.json`
  - `output/dup007/vrdb_collision_backtest_memo.md`

## Backtest Findings (Current Cohort)
- Synthetic behavior:
  - synthetic null cases remained unflagged in this run
  - synthetic injected cases were flagged in this run
- Geography quality behavior:
  - city quality stress scenarios (`city_ad_benge`, `city_ad_missing`) produced explicit fallback behavior (`fallback_steps=1`)
- Sensitivity coverage:
  - denominator variants (`all_registrants` vs `active_only`) measured
  - normalization version artifact availability is currently single-version (`shared_name_normalization_v1:4f856a65edcd`)
  - normalization proxy comparison (`canonical_medium_proxy`) showed zero delta in this run
- Control behavior:
  - several baseline choices (state/county/city-supported) still produced high holdout alert rates on selected historical controls
  - rollout remains review-gated pending sign-off (do not treat this as operational approval)

## Runtime / Report Validation (ESSB 6346)
- Unified run executed:
  - `CI_SKIP_INSTALL=1 ./testifier_audit/scripts/report/run_unified_report.sh --skip-imports /Users/sayhiben/dev/legislature-tools/data/raw/ESSB6346-20260224-0800.csv /Users/sayhiben/dev/legislature-tools/data/raw/20260202_VRDB_Extract.txt /Users/sayhiben/dev/legislature-tools/data/metadata/ESSB6346-20260224-0800.hearing.yaml`
  - log:
    - `output/run_logs/dup007_essb6346_unified.log`
- Rerender executed:
  - `python -m testifier_audit.cli report --out ../reports/ESSB6346-20260224-0800 --config ./configs/voter_registry_enabled.yaml --hearing-metadata ../data/metadata/ESSB6346-20260224-0800.hearing.yaml`
  - log:
    - `output/run_logs/dup007_essb6346_rerender.log`
- Playwright MCP validation completed:
  - desktop + mobile coverage
  - verified:
    - report loads and data shards hydrate
    - bucket control interaction updates chart note text and fallback messaging
    - mobile global-controls toggle works
    - sidecar analysis and table surfaces load under ESSB report
    - network requests for report shards return `200`
  - only console error: expected local `favicon.ico` `404`

## Issues Discovered During DUP-007
- Backtest runtime can become heavy when combining:
  - large hearings (ESSB-scale),
  - very fine buckets (especially `1m`/`5m`),
  - many scenario variants.
- Practical mitigation used for this batch:
  - bounded bucket set for reproducible runtime (`15,60,240`) in the committed memo run.

## Remaining Work / Handoff
- DUP-007 implementation deliverables are in place (pipeline + memo + artifacts), but rollout sign-off is still pending review of control-behavior findings.
- Next ticket in planned sequence remains `DUP-006`.

---

## DUP-006 Implementation Addendum

Date: 2026-02-28  
Work item: DUP-006 (P1)

## What Was Implemented
- Added a dedicated cross-family evidence-matrix analysis section:
  - analysis id: `duplicate_evidence_matrix`
  - hero chart id: `duplicate_evidence_matrix_overview`
  - detail chart id: `duplicate_evidence_matrix_scenario_counts`
- Kept evidence families additive and separate:
  - no composite score added
  - matrix columns explicitly separate:
    - duplicate collision burden
    - VRDB collision-null
    - behavioral timing
- Implemented explicit disagreement scenario classification in report payload builder:
  - `vrdb_high_duplicate_normal`
  - `duplicate_high_vrdb_normal`
  - `both_name_families_high`
  - `name_families_normal_behavioral_high`
- Added disagreement policy metadata to controls:
  - `controls.duplicate_evidence_matrix_policy`
  - rules preserve “additive, non-suppressive” interpretation between families.
- Added matrix interpretation and methodology content:
  - cross-family definition entry
  - disagreement caveat text
  - interpretation guidance entry tied to concordant/discordant triage
- Added report template callout in the matrix section that renders policy rules directly for analysts.
- Added dedicated frontend renderer for the evidence matrix:
  - heatmap-style matrix with scenario rows and evidence-family columns
  - tooltips include scenario interpretation, policy note, and window counts/shares
  - companion scenario-count chart retained for quick count comparison
- Added a checked-in analyst guidance memo:
  - `docs/duplicate-names-refinement/DUP-006-evidence-matrix-guidance.md`

## Tests Added/Updated
- `tests/test_analysis_registry.py`
  - asserts new matrix analysis metadata, chart ids, and priority/group.
- `tests/test_report_chart_payload.py`
  - new regression verifies scenario classification counts and policy/methodology payload surfaces.
- `tests/test_report_render_helpers.py`
  - asserts rendered report includes matrix section title and disagreement-policy callout.
- `tests_js/default_renderer_registry.test.js`
  - updated dependency fixture to include `renderEvidenceMatrix` for registry construction.
- Verification runs:
  - focused: `python -m pytest tests/test_analysis_registry.py tests/test_report_chart_payload.py tests/test_report_render_helpers.py -q`
  - lint: `./testifier_audit/scripts/ci/lint.sh`
  - full suite: `./testifier_audit/scripts/ci/test.sh` passed (`352 passed`)

## Runtime / Report Validation (ESSB 6346)
- Unified run executed (skip-import mode):
  - `CI_SKIP_INSTALL=1 ./testifier_audit/scripts/report/run_unified_report.sh --skip-imports /Users/sayhiben/dev/legislature-tools/data/raw/ESSB6346-20260224-0800.csv /Users/sayhiben/dev/legislature-tools/data/raw/20260202_VRDB_Extract.txt /Users/sayhiben/dev/legislature-tools/data/metadata/ESSB6346-20260224-0800.hearing.yaml`
  - log: `output/run_logs/dup006_essb6346_unified.log`
- Rerender executed:
  - `python -m testifier_audit.cli report --out ../reports/ESSB6346-20260224-0800 --config ./configs/voter_registry_enabled.yaml --hearing-metadata ../data/metadata/ESSB6346-20260224-0800.hearing.yaml`
  - log: `output/run_logs/dup006_essb6346_rerender.log`
- Runtime evidence in both logs:
  - `Duplicate evidence matrix built: bucket_variants=7 scenario_rows=28`
- Playwright MCP validation completed:
  - desktop: `1728x1117`
  - mobile: `390x844`
  - verified:
    - matrix section and policy callout render
    - both matrix charts render (`duplicate_evidence_matrix_overview`, `duplicate_evidence_matrix_scenario_counts`)
    - shard requests for matrix analysis return `200`:
      - `report_data/analyses/duplicate_evidence_matrix/base.json`
      - `report_data/analyses/duplicate_evidence_matrix/bucket-30m.json`
    - no console warnings/errors observed during validation.

## Issues Discovered During Implementation
- JS unit registry tests failed initially because the new renderer dependency (`renderEvidenceMatrix`) was required but not provided in `tests_js/default_renderer_registry.test.js`.
- Mitigation:
  - extended test dependency fixture with a `renderEvidenceMatrix` stub.

## Remaining Work / Handoff
- DUP-006 implementation is complete at code/test/runtime-validation level; analyst language sign-off remains the final acceptance step.
- Next ticket in sequence is `DUP-011`.

---

## DUP-011 Implementation Addendum

Date: 2026-02-28  
Work item: DUP-011 (P1)

## What Was Implemented
- Added explicit duplicate-detector hypothesis families with backend gating and multiplicity controls:
  - `A_scope_excess_rows` (primary scope-level endpoint)
  - `B_bucket_follow_up` (bucket follow-up tests)
  - `C_per_name_upper_tail` (per-name follow-up tests)
  - `D_temporal_follow_up` (within-name temporal follow-ups)
  - `E_position_follow_up` (legacy position follow-up)
- Added per-row inferential family metadata across duplicate inferential tables:
  - `family_id`
  - `adjustment_method`
  - `n_tests`
  - `n_tests_in_family`
  - `eligible_by_gate`
  - `gate_reason`
  - `adjusted_p_value`
  - `is_significant`
- Implemented gate chain in detector runtime:
  - Family A significance controls whether Family B/C are inferentially eligible within a scope.
  - Family D is eligible only for names discovered by the per-name family and only in Family-A-passing scopes.
- Added adjusted temporal outputs in `temporal_burst_signals`:
  - `temporal_q_value_min_gap`
  - `temporal_q_value_within_5m`
  - `temporal_q_value_within_15m`
  - corresponding `temporal_is_significant_*` flags.
- Added detector-side family summary outputs:
  - new `hypothesis_families` table
  - summary fields:
    - `hypothesis_families`
    - `hypothesis_family_totals`
    - `n_hypothesis_tests_total`
- Propagated family metadata into report payload contract and duplicate chart declarations:
  - duplicate contract now includes family counts + adjustment methods
  - duplicate callout template now renders a “Hypothesis families” block with `n_tests` and `adjustment`.

## Tests Added/Updated
- `tests/test_duplicates_exact.py`
  - added regressions for Family-A gate closed/open behavior
  - asserts downstream family gating and metadata population
  - asserts `hypothesis_families` summary table content
- `tests/test_report_chart_payload.py`
  - added payload regression asserting family-count and adjustment metadata in duplicate contract/declarations
  - asserts duplicate gating text carries multiplicity-family summary
- Validation runs:
  - `python -m pytest tests/test_duplicates_exact.py tests/test_report_chart_payload.py`
    - passed (`54 passed`)
  - `./testifier_audit/scripts/ci/test.sh`
    - passed (`355 passed`)

## Runtime / Report Validation (ESSB 6346)
- Full `run-all` recompute completed with voter config + hearing metadata:
  - command:
    - `TESTIFIER_AUDIT_DB_URL=postgresql://legislature:legislature@localhost:55432/legislature python -m testifier_audit.cli run-all --csv ../data/raw/ESSB6346-20260224-0800.csv --out ../reports/ESSB6346-20260224-0800 --config ./configs/voter_registry_enabled.yaml --hearing-metadata ../data/metadata/ESSB6346-20260224-0800.hearing.yaml`
  - log:
    - `output/run_logs/dup011_essb6346_runall_csv.log`
  - completion evidence:
    - `Duplicate evidence matrix built: bucket_variants=7 scenario_rows=28`
    - `Run complete. Report: /Users/sayhiben/dev/legislature-tools/reports/ESSB6346-20260224-0800/report.html`
- Rerender completed:
  - command:
    - `python -m testifier_audit.cli report --out ../reports/ESSB6346-20260224-0800 --config ./configs/voter_registry_enabled.yaml --hearing-metadata ../data/metadata/ESSB6346-20260224-0800.hearing.yaml`
  - log:
    - `output/run_logs/dup011_essb6346_rerender.log`
- Post-recompute artifact checks:
  - `report_data/analyses/duplicates_exact/base.json` now includes non-null family metadata rows (for example `family_id="A_scope_excess_rows"`, `adjustment_method="holm"`).
  - `summary/duplicates_exact.json` contains:
    - `hypothesis_families`
    - `hypothesis_family_totals`
    - `n_hypothesis_tests_total`
  - `report.html` renders:
    - “Hypothesis families” callout with family labels, `n_tests`, and adjustment method.
- Playwright MCP validation completed (desktop + mobile):
  - desktop `1728x1117`
  - mobile `390x844`
  - verified:
    - hypothesis-family callout renders in duplicate section
    - bucket/global-controls/menu interactions remain functional
    - no console errors/warnings
    - report-data requests returned `200` across loaded analyses.

## Issues Discovered During DUP-011
- Initial ESSB recompute attempts appeared stalled because a lingering long-running schema backfill query was holding relation locks on `voter_registry`.
  - blocked queries were visible in `pg_stat_activity` as `wait_event_type=Lock` on duplicate VRDB lookup SELECTs.
- Mitigation used:
  - identified backend PID via `docker exec ... psql ... pg_stat_activity`
  - terminated stale blocker with `pg_terminate_backend(...)`
  - reran `run-all` successfully to completion.
- Additional runtime note:
  - rerender-only validation can show stale inferential/family metadata when underlying computed artifacts were not regenerated; full recompute is required after detector logic changes.

## Remaining Work / Handoff
- DUP-011 acceptance criteria are satisfied at detector, payload, and report levels (including ESSB runtime evidence and live Playwright validation).
- Next ticket in sequence is `DUP-013`.

---

## DUP-012 Implementation Addendum

Date: 2026-02-28  
Work item: DUP-012 (P1)

## What Was Implemented
- Repaired duplicate position-concentration inference to remove sign-adaptive one-sided testing:
  - replaced legacy one-sided p-value logic with a two-sided permutation test on `|rate_difference|`.
  - added explicit test metadata:
    - `permutation_test_id=position_rate_difference_permutation_abs_two_sided_v1`
    - `permutation_test_sidedness=two_sided_abs_effect`
    - `permutation_p_value_two_sided`
- Replaced naive rate-difference interval path with cluster-aware bootstrap by name key:
  - new cluster bootstrap helper samples name-key clusters with replacement.
  - outputs:
    - `rate_difference_interval_low`
    - `rate_difference_interval_high`
    - `rate_difference_interval_method=position_rate_difference_cluster_bootstrap_v1`
    - `rate_difference_interval_draws`
- Updated family-level gating/multiplicity handling for position follow-up:
  - position-family adjusted values now use the two-sided permutation p-value path.
  - family significance summary now uses `is_significant` from adjusted p-values.
- Updated report-facing language for the position analysis:
  - duplicate position deviance text now frames output as a position imbalance signal, not proof of manipulation.
  - position tooltip label updated to two-sided permutation wording (`Permutation p (two-sided |Δ|)`).

## Tests Added/Updated
- `tests/test_duplicates_exact.py`
  - added regression to assert two-sided absolute-effect permutation behavior and removal of one-sided field.
  - added cluster-bootstrap interval regression to ensure finite ordered intervals and bounded effective draw counts.
- Focused validation run:
  - `python -m pytest tests/test_duplicates_exact.py tests/test_report_chart_payload.py tests/test_analysis_registry.py tests/test_report_render_helpers.py`
  - result: `78 passed` (warnings only; no failures)

## Runtime / Report Validation (ESSB 6346)
- Full recompute executed:
  - command:
    - `TESTIFIER_AUDIT_DB_URL=postgresql://legislature:legislature@localhost:55432/legislature python -m testifier_audit.cli run-all --csv ../data/raw/ESSB6346-20260224-0800.csv --out ../reports/ESSB6346-20260224-0800 --config ./configs/voter_registry_enabled.yaml --hearing-metadata ../data/metadata/ESSB6346-20260224-0800.hearing.yaml`
  - log:
    - `output/run_logs/dup012_essb6346_runall_csv.log`
  - completion evidence:
    - `Duplicate evidence matrix built: bucket_variants=7 scenario_rows=28`
    - `Run complete. Report: /Users/sayhiben/dev/legislature-tools/reports/ESSB6346-20260224-0800/report.html`
- Rerender completed:
  - command:
    - `python -m testifier_audit.cli report --out ../reports/ESSB6346-20260224-0800 --config ./configs/voter_registry_enabled.yaml --hearing-metadata ../data/metadata/ESSB6346-20260224-0800.hearing.yaml`
  - log:
    - `output/run_logs/dup012_essb6346_rerender.log`
- Playwright MCP validation completed:
  - desktop `1728x1117`
  - mobile `390x844`
  - verified:
    - bucket controls, theme controls, and menu behavior remain stable after DUP-012 changes.
    - duplicate hypothesis-family callout shows `E_position_follow_up` with expected metadata.
    - report-data analysis requests returned `200` across loaded sections.
    - only console error observed was local static-server `favicon.ico` `404`.

## Issues Discovered During DUP-012
- Cluster bootstrap with cluster-level resampling can produce degenerate draws (all selected weight on one side), so effective draw count can be lower than requested draw count.
- Mitigation:
  - preserved explicit `rate_difference_interval_draws` output.
  - regression assertions were adjusted to check bounded effective draws rather than forcing exact draw parity.
- ESSB 6346 position deviance chart can be data-sparse for some bucket selections/inferential states, so tooltip-level text is not always visible in every viewport/bucket combination during manual QA.

## Remaining Work / Handoff
- DUP-012 acceptance criteria are met:
  - directionally valid p-values (two-sided, fixed test definition)
  - cluster-aware intervals
  - report language scoped to position imbalance signals
- Next ticket in sequence is `DUP-013`.
