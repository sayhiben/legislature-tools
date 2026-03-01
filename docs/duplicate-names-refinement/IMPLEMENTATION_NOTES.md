# Duplicate Names Refinement - Implementation Notes

## Work Item ID Policy
- Keep `DUP-xxx` numbers stable as permanent identifiers.
- Do not renumber work items to reflect execution order.
- Track real execution order in this notes file so references in commits/PRs/discussion remain stable.

## Implementation Sequence (Actual + Planned)
Execution order is tracked here independently from ticket numbering.

### Completed (actual)
1. `DUP-001` (Done, 2026-02-28)

### Current
1. `DUP-008` (next in-progress target)

### Planned next order (subject to reprioritization)
1. `DUP-009`
2. `DUP-010`
3. `DUP-002`
4. `DUP-003`
5. `DUP-004`
6. `DUP-005`
7. `DUP-007`
8. `DUP-006`
9. `DUP-011`
10. `DUP-012`
11. `DUP-013`
12. `DUP-014`
13. `DUP-015`
14. `DUP-016`
15. `DUP-017`
16. `DUP-018`
17. `DUP-019`
18. `DUP-020`
19. `DUP-021`

## Scope Covered
These notes capture implementation takeaways from **DUP-001** (statistical contract and report voice rules), including code-level contract decisions, QA observations, and planning impacts for upcoming work items.

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
