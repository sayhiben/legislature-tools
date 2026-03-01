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

### Current
1. `DUP-010` (next in-progress target)

### Planned next order (subject to reprioritization)
1. `DUP-010`
2. `DUP-002`
3. `DUP-003`
4. `DUP-004`
5. `DUP-005`
6. `DUP-007`
7. `DUP-006`
8. `DUP-011`
9. `DUP-012`
10. `DUP-013`
11. `DUP-014`
12. `DUP-015`
13. `DUP-016`
14. `DUP-017`
15. `DUP-018`
16. `DUP-019`
17. `DUP-020`
18. `DUP-021`

## Scope Covered
These notes capture implementation takeaways from **DUP-001**, **DUP-008**, and **DUP-009**, including code-level contract decisions, QA observations, and planning impacts for upcoming work items.

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
