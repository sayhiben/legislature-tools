# Plan v2: Investigation-First Modernization (Pre-Production)

## Summary
This plan delivers a complete end-to-end roadmap for moving the project from detector-heavy profiling to investigation-first anomaly triage and forensics, while preserving scientific defensibility and usability.

Pre-production policy for this plan:
- Do not spend effort on feature flags.
- Do not spend effort on backward compatibility or legacy-renderer support.
- Optimize for correctness, clarity, and iteration speed toward the first publishable version.

## Current State Evaluation
- Strengths:
  - Broad detector coverage exists (bursts, swings, changepoints, duplicates, near-duplicates, periodicity, multivariate, composite).
  - Wilson interval and low-power concepts are present in several detectors.
  - Interactive report stack is functioning (ECharts + Tabulator) with bucket and zoom sync foundations.
  - CI baseline is healthy.
- Primary gaps:
  - Information architecture is detector-first and verbose instead of triage-first.
  - Signal provenance is not strictly separated between statistical and heuristic evidence.
  - Drilldown to causative rows and explicit export queues are not first-class.
  - Data-quality artifact lens vs behavior lens is not explicit (raw vs dedup).
  - Hearing-relative process context is missing.
  - UX issues remain: sidebar resize distortion, heavy redraw without visible progress, confusing status badge.

## Guiding Principles
1. Investigation workflow over profiling dump.
2. Evidence governance: show what is statistically calibrated vs heuristic.
3. Explainability over opaque scoring.
4. Raw and dedup perspectives both visible for key metrics.
5. Maintainability through modular report architecture.
6. No compatibility overhead while pre-production.

## Scope and Deliverables
This plan includes all requested feedback-driven work:
- Investigation-first IA and navigation.
- Executive summary and anomaly queue contracts.
- Full drilldown and export workflows.
- UX and usability fixes in TODO.
- Statistical calibration governance and caveats.
- Added analyses named in feedback documents:
  - Burst composition.
  - Inter-arrival regularity (Fano/autocorr/FFT).
  - Pro/Con runs tests.
  - Name improbability over time.
  - Near-dup time concentration.
  - Within-minute sortedness with Kendall tau.
  - Off-hours composition heatmaps.
  - Explainable window and record/cluster scoring.
- Hearing-relative context support via optional metadata sidecar.
- Data quality panel and raw-vs-dedup dual-lens reporting.
- Probabilistic voter linkage uncertainty framing.
- Cross-hearing feature store and percentile normalization.
- Methodology/guardrails page and reviewer-facing transparency.

## Core Interface and Contract Changes

### Config
Update `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/config.py` and YAMLs:
- `input.hearing_metadata_path: str | None`
- `report.min_cell_n_for_rates: int` (default 25)
- `report.default_dedup_mode: "raw" | "exact_row_dedup" | "side_by_side"` (default `side_by_side`)
- `composite.window_tier_thresholds.high` / `.medium`
- `composite.record_tier_thresholds.high` / `.medium`

### CLI
Update `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/cli.py`:
- Add `--hearing-metadata` on `profile`, `detect`, `run-all`.
- Add `--dedup-mode` on `run-all` and `report`.

### Unified Script
Update `/Users/sayhiben/dev/legislature-tools/testifier_audit/scripts/report/run_unified_report.sh`:
- Optional arg 3 for hearing metadata path.
- Optional `DEDUP_MODE` env.

### Payload
Update `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/render.py`:
- Add `triage_summary`, `window_evidence_queue`, `record_evidence_queue`, `cluster_evidence_queue`.
- Add `data_quality_panel`, `hearing_context_panel`.
- Add `controls.evidence_taxonomy`, `controls.dedup_modes`.
- Add `analysis_catalog[*].group`, `analysis_catalog[*].priority`.

### New output artifacts
- `summary/investigation_summary.json`
- `summary/feature_vector.json`
- `tables/triage__window_evidence_queue.(csv|parquet)`
- `tables/triage__record_evidence_queue.(csv|parquet)`
- `tables/triage__cluster_evidence_queue.(csv|parquet)`
- `tables/data_quality__raw_vs_dedup_metrics.(csv|parquet)`

## Evidence Governance Specification
Every queue item and detector-derived signal included in triage must include:
- `evidence_kind`: `stat_fdr | calibrated_empirical | heuristic`
- `support_n`
- `effect_size` (when applicable)
- `p_value` / `q_value` (when applicable)
- `is_low_power`
- `caveat_flags`

Tiering rules (default):
- `high`: score >= 0.80, support_n >= 25, and includes at least one `stat_fdr` or `calibrated_empirical` contributor.
- `medium`: score >= 0.60 and support_n >= 25.
- `watch`: all others.
- Any `is_low_power=true` row is capped at `medium`.

Required explanation labels:
- `primary_explanation`: `data_quality_artifact | legitimate_mobilization | potential_manipulation | mixed | insufficient_evidence`
- `secondary_explanation`: same enum or `none`

## Architecture Refactor Plan
Decompose report generation to reduce monolith risk:
- `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/contracts.py`
- `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/analysis_registry.py`
- `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/triage_builder.py`
- `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/quality_builder.py`
- `/Users/sayhiben/dev/legislature-tools/testifier_audit/src/testifier_audit/report/help_registry.py`

Design constraints:
- Detectors produce analyzable tables.
- Triage builder normalizes and ranks evidence.
- Template handles interaction only.
- New analyses are registry additions, not structural template rewrites.

## Multi-Phase Implementation Plan

## Phase 0: Foundations
1. Introduce typed contracts for triage evidence.
2. Implement shared scoring and explanation helpers.
3. Add runtime instrumentation for payload size and render timings.
4. Extract analysis registry out of `render.py`.

Why:
- Stabilizes future changes and reduces accidental signal mixing.

Where:
- `.../report/contracts.py`, `.../report/analysis_registry.py`, `.../report/triage_builder.py`, `.../report/render.py`.

Tests:
- `test_evidence_contract.py`
- `test_triage_builder.py`
- `test_report_runtime_contracts.py`

## Phase 1: UX Bug Fixes and Interaction Reliability
1. Remove noisy `ready` status badge from section header UI.
2. Fix sidebar toggle resize distortion by resizing charts after layout changes.
3. Add bucket-change loading spinner/progress indicator.
4. Add explicit global zoom reset and linked zoom-range controls.
5. Show timezone and bucket-size labels consistently on charts/tooltips.

Why:
- Directly addresses known UX bugs and trust issues.

Where:
- `.../report/templates/report.html.j2`

Tests:
- `test_report_render_helpers.py`
- `test_report_runtime_contracts.py`

## Phase 2: Investigation-First Report IA and Drilldown
1. Implement report pages/tabs:
   - `Triage`
   - `Window Drilldown`
   - `Name/Cluster Forensics`
   - `Methodology`
2. Executive summary contract:
   - total submissions, date range, overall pro/con rate.
   - top 5 burst windows.
   - top 5 swing windows.
   - top repeated names + top near-dup clusters.
   - off-hours summary.
3. Required anomaly table fields:
   - `window_id`, `start_time`, `end_time`, `count`, `expected`, `z`, `q_value`,
     `pro_rate`, `delta_pro_rate`, `dup_fraction`, `near_dup_fraction`,
     `name_weirdness_mean`, `support_n`, `evidence_tier`, `primary_explanation`.
4. Drilldown flow:
   - click queue row -> highlight time span across linked charts.
   - render causative raw rows, exact dup names, near-dup clusters, runs summary,
     and improbability/weirdness distribution comparison.
5. Export flows:
   - `download selected window rows`
   - `download top evidence windows`
   - `download top evidence records`

Where:
- `.../report/triage_builder.py`
- `.../report/templates/report.html.j2`
- `.../report/render.py`

Tests:
- `test_triage_summary_contract.py`
- `test_window_queue_schema.py`
- `test_drilldown_exports.py`
- `test_report_layout_contract.py`

## Phase 3: Analysis Pack A (All Named Additions)
1. Burst composition detector.
2. Regularity detector (rolling Fano + autocorrelation + FFT peaks).
3. Pro/Con runs detector.
4. Name improbability detector over time using external frequencies.
5. Near-dup time concentration metrics.
6. Sortedness enhancement with Kendall tau.
7. Off-hours composition package with hour-of-week heatmaps.
8. Explainable suspicion scoring for window and record/cluster queues.

Where:
- `.../detectors/burst_composition.py`
- `.../detectors/regularity.py`
- `.../detectors/runs.py`
- `.../detectors/name_improbability.py`
- `.../detectors/duplicates_near.py`
- `.../detectors/sortedness.py`
- `.../detectors/composite_score.py`
- `.../detectors/registry.py`
- `.../report/render.py`

Tests:
- `test_burst_composition.py`
- `test_regularity_detector.py`
- `test_runs_detector.py`
- `test_name_improbability.py`
- updates to `test_duplicates_near.py`, `test_statistics_detectors.py`, `test_composite_score.py`, `test_pipeline_integration.py`.

## Phase 4: Data Quality and Dual-Lens Reporting
1. Add explicit raw vs dedup mode support in triage and panels.
2. Add side-by-side metrics where dedup materially changes interpretation.
3. Add data quality warning panel with high-value warnings only:
   - invalid/missing positions
   - unparsable names
   - duplicate IDs
   - non-monotonic timestamps vs ID
   - time-varying missingness spikes.
4. Move non-actionable profiling metrics out of primary triage view.

Where:
- `.../features/dedup.py`
- `.../report/quality_builder.py`
- `.../report/render.py`
- `.../report/templates/report.html.j2`

Tests:
- `test_dedup_lenses.py`
- `test_data_quality_panel.py`
- updates to payload and render contract tests.

## Phase 5: Hearing-Relative Context and Process-Aware Features
1. Add metadata sidecar ingestion and validation.
2. Add hearing-relative features:
   - minutes to cutoff
   - minutes since sign-in open
   - minutes since meeting start.
3. Add process markers/overlays on charts (open, cutoff, meeting start).
4. Add deadline-ramp metrics and stance-by-deadline behaviors.

Where:
- `.../io/hearing_metadata.py`
- `.../preprocess/time.py`
- `.../cli.py`
- `.../scripts/report/run_unified_report.sh`
- `.../report/render.py`

Tests:
- `test_hearing_metadata.py`
- updates to `test_time.py`, `test_cli.py`, and payload/render tests.

Example sidecar schema:
```yaml
schema_version: 1
hearing_id: SB6346
timezone: America/Los_Angeles
meeting_start: 2026-02-06T13:30:00-08:00
sign_in_open: 2026-02-03T09:00:00-08:00
sign_in_cutoff: 2026-02-06T12:30:00-08:00
written_testimony_deadline: 2026-02-07T13:30:00-08:00
```

## Phase 6: Probabilistic Voter Linkage and Uncertainty
1. Replace binary framing with probabilistic match tiers:
   - exact
   - strong fuzzy
   - weak fuzzy
   - unmatched.
2. Add match confidence and uncertainty caveat summaries.
3. Ensure voter signals are treated as supporting evidence, not standalone attribution.

Where:
- `.../detectors/voter_registry_match.py`
- `.../report/render.py`
- `.../report/templates/report.html.j2`

Tests:
- update `test_voter_registry_match.py`
- add uncertainty rendering checks.

## Phase 7: Cross-Hearing Feature Store and Comparative Baselines
1. Emit per-report feature vectors.
2. Build baseline aggregator script over `reports/` corpus.
3. Add percentile overlays and comparator bands.
4. Add cross-hearing name/cluster forensics cues.

Where:
- `.../scripts/report/build_global_baselines.py`
- `.../report/render.py`
- `.../report/templates/report.html.j2`

Tests:
- `test_global_baselines.py`
- payload contract test updates.

## Phase 8: Methodology, Guardrails, and Publish Readiness
1. Build a dedicated methodology page:
   - definitions
   - tests used
   - multiple testing policy
   - caveats
   - interpretation guidance.
2. Add ethical guardrail copy standards:
   - "statistical irregularity" language
   - explicit anti-attribution caveats.
3. Optional dark mode with preserved semantic color meanings.
4. Improve screenshot capture UX to avoid false duplication interpretation in stitched output.

Where:
- `.../report/help_registry.py`
- `.../report/templates/report.html.j2`
- `.../scripts/report/capture_report_screenshot.py`

Tests:
- methodology and copy guardrail tests.
- screenshot script argument behavior tests.

## Performance and Quality Gates
- Payload size target on reference dataset: <= 8 MB.
- First render target on reference dataset: <= 3.0s.
- Bucket-switch rerender target: <= 1.5s.
- No uncaught browser console errors during QA checklist.
- Keep coverage >= 80% overall.

## Acceptance Test Matrix (End-to-End)
1. Empty/sparse dataset renders all sections with explicit reasons.
2. Low-support dataset shows low-power downgrades and caveats.
3. Burst-heavy synthetic dataset ranks composition-aware windows correctly.
4. Clockwork synthetic dataset triggers regularity indicators.
5. Long homogeneous Pro sequence triggers runs indicators.
6. Name-randomized synthetic data triggers improbability shifts.
7. Near-dup concentrated synthetic data triggers concentration metrics.
8. Off-hours composition shifts render with minimum-n masking.
9. Raw-vs-dedup toggle changes duplication-sensitive metrics predictably.
10. Metadata absent is handled gracefully with disabled context panel.
11. Metadata present enables hearing-relative overlays.
12. Probabilistic voter linkage exposes uncertainty fields and caveats.
13. Drilldown selection links charts + tables correctly.
14. Window and record export files include required provenance columns.
15. Comparative baseline overlays appear when baseline corpus exists.
16. Sidebar toggle never leaves distorted charts.
17. Zoom reset restores shared extent.
18. Spinner appears during bucket redraw.
19. Desktop and mobile layouts pass visual checks.
20. CI scripts pass:
   - `/Users/sayhiben/dev/legislature-tools/testifier_audit/scripts/ci/lint.sh`
   - `/Users/sayhiben/dev/legislature-tools/testifier_audit/scripts/ci/test.sh`

## Explicit Assumptions
- Project remains pre-production for an extended period.
- Fast iteration is more valuable than preserving historical internal interfaces.
- If future production constraints require compatibility or staged rollout controls,
  those concerns will be added in a later plan revision.
