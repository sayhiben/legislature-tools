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
5. Remove legacy inlined registry helpers from `render.py` after parity tests pass (new task).

Why:
- Stabilizes future changes and reduces accidental signal mixing.

Where:
- `.../report/contracts.py`, `.../report/analysis_registry.py`, `.../report/triage_builder.py`, `.../report/render.py`.

Tests:
- `test_evidence_contract.py`
- `test_triage_builder.py`
- `test_report_runtime_contracts.py`
- `test_analysis_registry.py` (added to lock registry extraction/parity behavior)
- `test_pipeline_integration.py` (used as regression check after render registry cleanup)

Status (2026-02-21): Complete
- Implemented `report/contracts.py` with typed triage evidence contracts and validation.
- Implemented `report/triage_builder.py` shared scoring/tiering/explanation/caveat helpers.
- Added payload and render runtime instrumentation:
  `controls.runtime.payload_build_ms`, `controls.runtime.payload_json_bytes`,
  `controls.runtime.interactive_payload_build_ms`, and `artifacts/report_runtime.json`.
- Extracted analysis registry/status into `report/analysis_registry.py` and removed legacy duplicated
  registry helpers from `report/render.py`.
- Validation:
  `python -m ruff check src/testifier_audit/report tests/test_analysis_registry.py tests/test_evidence_contract.py tests/test_triage_builder.py tests/test_report_runtime_contracts.py tests/test_report_render_helpers.py tests/test_pipeline_integration.py`
  and
  `python -m pytest tests/test_analysis_registry.py tests/test_evidence_contract.py tests/test_triage_builder.py tests/test_report_runtime_contracts.py tests/test_report_chart_payload.py tests/test_report_render_helpers.py tests/test_pipeline_integration.py`
  (24 passed).

Lessons learned to carry into later phases:
- Keep one source of truth per report contract domain (registry, evidence contracts, scoring helpers).
- Avoid adding large new behavior directly in `render.py`; add focused modules and wire them in.
- Couple structural refactors with both unit-contract coverage and integration parity checks.
- Keep runtime metrics in payload/artifacts to guard performance while IA grows in Phases 1-4.

## Phase 1: UX Bug Fixes and Interaction Reliability
1. Remove noisy `ready` status badge from section header UI.
2. Fix sidebar toggle resize distortion by resizing charts after layout changes.
3. Add bucket-change loading spinner/progress indicator.
4. Add explicit global zoom reset and linked zoom-range controls.
5. Show timezone and bucket-size labels consistently on charts/tooltips
   (read from `controls.timezone`/`controls.timezone_label` when present; fallback `UTC`).

Why:
- Directly addresses known UX bugs and trust issues.

Where:
- `.../report/templates/report.html.j2`

Tests:
- `test_report_render_helpers.py`
- `test_report_runtime_contracts.py`

Status (2026-02-21): Complete
- Implemented in `report/templates/report.html.j2`:
  - `ready` badges are no longer rendered; non-ready badges remain visible.
  - Sidebar open/close and viewport resize now trigger multi-pass chart resize sequencing to prevent
    distortion after layout transitions.
  - Added a global busy indicator for bucket changes (`runWithBusyIndicator`) with explicit progress text.
  - Added linked zoom controls in the sidebar:
    - `Reset linked zoom` action.
    - live linked zoom-range label synchronized to shared zoom state.
  - Added consistent timezone/bucket context:
    - time-aware tooltip labels include timezone and bucket where available.
    - chart notes now include timezone and active bucket metadata for mounted time-series charts.
- Implemented in `report/render.py`:
  - Added explicit payload controls defaults: `controls.timezone = "UTC"` and
    `controls.timezone_label = "UTC"` so template timezone labeling has a deterministic contract.
- Regression/contract coverage updates:
  - Updated `tests/test_report_render_helpers.py` to assert:
    - zoom controls and zoom-range runtime hooks are present,
    - busy indicator hooks are present,
    - layout resize sequencing hook is present,
    - and `status-ready` styling is removed from rendered output.
  - Validation run:
    - `python -m ruff check testifier_audit/tests/test_report_render_helpers.py testifier_audit/src/testifier_audit/report/render.py`
    - `python -m pytest testifier_audit/tests/test_report_render_helpers.py testifier_audit/tests/test_report_runtime_contracts.py` (11 passed).
    - `python -m pytest testifier_audit/tests/test_report_chart_payload.py` (payload contract assertions include timezone controls).
    - `python -m ruff check src tests --select F`
    - `python -m pytest tests/test_pipeline_integration.py` (1 passed)
    - `./scripts/ci/test.sh` (122 passed).

Gap/Scope note discovered during implementation:
- Timezone is currently explicitly fixed to `UTC` in payload controls for deterministic labeling.
  This is intentional to avoid Phase 5 scope creep; wiring authoritative hearing timezone through
  metadata ingestion/payload remains part of Phase 5 context work.

Lessons learned to carry into later phases:
- Keep UX interaction state explicit in the sidebar (global controls, range state, and loading
  feedback) rather than implicit in chart internals.
- Preserve payload-driven UI contracts (`controls.*`) for shared runtime behavior; avoid hardcoding
  UI assumptions in template logic.
- Treat layout-transition reliability as a first-class chart concern; preserve sequenced resize
  behavior when extending IA in Phase 2.

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

Status (2026-02-21): Complete
- Completed in this iteration:
  - Implemented investigation-first IA blocks in `report/templates/report.html.j2`:
    - Added workflow tabs/anchors for `Triage`, `Window Drilldown`, `Name/Cluster Forensics`,
      and `Methodology`.
    - Added queue tables and investigation summary KPI cards in `Triage`.
    - Added drilldown tables and linked row-click behavior in `Window Drilldown`.
    - Added export actions:
      - `download selected window rows`
      - `download top evidence windows`
      - `download top evidence records`
  - Implemented Phase 2 triage contracts in `report/triage_builder.py`:
    - Added `build_investigation_view(...)` to derive:
      - `triage_summary`
      - `window_evidence_queue`
      - `record_evidence_queue`
      - `cluster_evidence_queue`
    - Window queue now emits required fields:
      `window_id`, `start_time`, `end_time`, `count`, `expected`, `z`, `q_value`,
      `pro_rate`, `delta_pro_rate`, `dup_fraction`, `near_dup_fraction`,
      `name_weirdness_mean`, `support_n`, `evidence_tier`, `primary_explanation`.
  - Integrated Phase 2 payload/runtime contracts in `report/render.py`:
    - Added triage payload keys to interactive payload.
    - Added `controls.evidence_taxonomy` and `controls.dedup_modes`.
    - Added `analysis_catalog[*].group` and `analysis_catalog[*].priority`.
  - Added/updated output artifacts written during report render:
    - `summary/investigation_summary.json`
    - `summary/feature_vector.json`
    - `tables/triage__window_evidence_queue.(csv|parquet)`
    - `tables/triage__record_evidence_queue.(csv|parquet)`
    - `tables/triage__cluster_evidence_queue.(csv|parquet)`
  - Added Phase 2 test coverage:
    - `tests/test_triage_summary_contract.py`
    - `tests/test_window_queue_schema.py`
    - `tests/test_drilldown_exports.py`
    - `tests/test_report_layout_contract.py`
    - plus payload/integration updates in:
      - `tests/test_report_chart_payload.py`
      - `tests/test_pipeline_integration.py`
      - `tests/test_analysis_registry.py`
  - Hardened drilldown row-click behavior in fallback table mode (`mountTable(...)`):
    - When Tabulator is unavailable, HTML table rows now honor `rowClick` callbacks.
    - Added keyboard activation (`Enter`/`Space`) and focusability for clickable fallback rows.
  - Completed end-to-end UX QA pass on real report output (`reports/SB6346-20260206-1330/report.html`):
    - Verified section/hash navigation (`#window-drilldown`) and triage row-click -> drilldown selection sync.
    - Verified linked bucket selector propagation (example: `15m` applied across mounted bucket-aware charts).
    - Verified browser console cleanliness in Playwright (`0` errors/warnings).
    - Captured QA screenshots in `reports/SB6346-20260206-1330/screenshots/`:
      - `report-fullpage-desktop-20260221.png`
      - `report-viewport-top-desktop-20260221.png`
      - `report-viewport-middle-desktop-20260221.png`
      - `report-viewport-bottom-desktop-20260221.png`
      - `report-viewport-top-mobile-20260221.png`
      - `report-viewport-middle-mobile-20260221.png`
      - `report-viewport-bottom-mobile-20260221.png`

Scope refinement applied (to avoid Phase 3/4 creep):
- Drilldown currently uses causative timeline rows available in current payload/artifacts
  (`baseline_volume_pro_rate`) rather than introducing a new raw submission row contract in this phase.
  Exact duplicate rows, near-dup clusters, runs-style summary, and rarity/weirdness comparison are included
  without adding new detector families.
- Keep per-submission “causative raw rows” as a deferred enhancement; do not add a larger payload contract
  until we explicitly prioritize it and accept payload-size/runtime tradeoffs in a later phase.

Carry-forward lessons from Phase 2:
- Keep investigation-first navigation explicit and stable:
  `Triage` -> `Window Drilldown` -> `Name/Cluster Forensics` -> `Methodology`.
- Treat triage queues/summary and drilldown behavior as cross-module contracts:
  update builder, payload wiring, template runtime, and tests together.
- Preserve drilldown row-click parity across table runtimes; fallback HTML tables must support both
  mouse click and keyboard activation.
- Close report IA changes with real-dataset QA artifacts (desktop/mobile screenshots + console checks)
  in addition to contract/integration tests.

## Phase 3: Analysis Pack A (All Named Additions)
1. Burst composition detector. (completed in this tranche)
2. Regularity detector (rolling Fano + autocorrelation + FFT peaks). (completed in this tranche)
3. Pro/Con runs detector. (completed in this tranche)
4. Name improbability detector over time using external frequencies. (completed; existing `rare_names` contract validated)
5. Near-dup time concentration metrics. (completed in this tranche)
6. Sortedness enhancement with Kendall tau. (completed in this tranche)
7. Off-hours composition package with hour-of-week heatmaps. (completed in this tranche)
8. Explainable suspicion scoring for window and record/cluster queues. (completed in this tranche)

Status (2026-02-21): Complete
- Completed detector/report tranche:
  - `bursts`: completed burst-composition contract by extending burst-window outputs:
    - burst window composition fields:
      `n_pro`, `n_con`, `pro_rate`, `baseline_pro_rate`,
      `delta_pro_rate`, `abs_delta_pro_rate`, Wilson bounds, and low-power flags.
    - summary metrics:
      `max_abs_delta_pro_rate`, `max_significant_abs_delta_pro_rate`,
      `n_significant_composition_shifts`.
  - `duplicates_near`: added time-concentration contracts:
    - `cluster_time_concentration`
    - `cluster_time_concentration_summary`
    - cluster-level fields in `cluster_summary`:
      `n_active_buckets`, `peak_bucket_start`, `peak_bucket_records`, `peak_bucket_fraction`,
      `concentration_hhi`.
  - `sortedness`: added Kendall tau ordering strength metrics to bucket/minute outputs and summary:
    - per-bucket `kendall_tau`, `kendall_p_value`, `abs_kendall_tau`
    - per-bucket-size summary `mean_kendall_tau`, `mean_abs_kendall_tau`,
      `max_abs_kendall_tau`, `strong_ordering_ratio`.
  - `off_hours`: added weekday/hour composition table:
    - `hour_of_week_distribution` with `day_of_week`, `day_of_week_index`, `hour`,
      `n_total`, `n_pro`, `n_con`, `n_off_hours`, `off_hours_fraction`,
      `pro_rate`, Wilson bounds, and low-power flags.
  - `periodicity`: completed regularity contract by adding rolling Fano outputs while
    reusing existing autocorrelation + FFT outputs:
    - `rolling_fano`
    - `rolling_fano_summary`
    - summary metrics:
      `max_rolling_fano_factor`, `median_rolling_fano_factor`,
      `n_high_fano_windows`, `high_fano_threshold`.
  - `procon_swings`: added contiguous directional runs contracts:
    - `direction_runs`
    - `direction_runs_summary`
    - summary metrics:
      `n_direction_runs`, `n_long_direction_runs`,
      `max_direction_run_length`, `max_direction_run_mean_abs_delta`.
  - `rare_names`: validated existing external-frequency improbability timeline contract:
    - external frequency lookups (`first_name_frequency_path`/`last_name_frequency_path`)
    - over-time rarity table (`rarity_by_minute`)
    - report chart wiring (`rare_names_rarity_timeline`).
  - Explainable queue scoring:
    - `triage_builder` now emits queue-level scoring explanation fields for
      window/record/cluster queues:
      `score_primary_driver`, `score_detector_breakdown`, `score_signal_breakdown`.
  - Report wiring completed for new Phase 3 contracts:
    - Added new chart hosts + metadata + render routing:
      - `bursts_composition_shift`
      - `off_hours_day_hour_heatmap`
      - `duplicates_near_time_concentration`
      - `sortedness_kendall_tau_summary`
      - `periodicity_rolling_fano`
      - `procon_swings_direction_runs`
    - Updated analysis registry detail chart sets for `bursts`, `off_hours`,
      `duplicates_near`, and `sortedness`; extended `periodicity` and `procon_swings`
      detail charts for regularity/runs coverage.
    - Added column glossary docs for new detector and triage fields.

Scope refinement applied (to avoid Phase 3 overreach):
- Reused and extended existing detector modules (`duplicates_near`, `sortedness`, `off_hours`)
  instead of introducing parallel new detector families for these capabilities.
  This keeps Phase 3 DRY/YAGNI and avoids unnecessary registry/template churn.
- Implemented burst composition as a `bursts` extension (volume + composition in the same
  burst-window contract) rather than adding a standalone burst-composition detector module.
- Implemented regularity as a `periodicity` extension (rolling Fano + existing autocorr/FFT)
  instead of adding a parallel detector family. This preserves existing detector governance,
  avoids duplicate chart wiring, and keeps Phase 3 focused on contract completion.
- Implemented runs as a `procon_swings` extension (directional run segmentation over existing
  bucket profiles) rather than adding a standalone runs detector module.
- Removed duplicate-work expectation for a standalone name-improbability detector module:
  existing `rare_names` external-frequency rarity timeline already fulfills this Phase 3 intent.

Where (implemented in this tranche):
- `.../detectors/bursts.py`
- `.../detectors/duplicates_near.py`
- `.../detectors/sortedness.py`
- `.../detectors/off_hours.py`
- `.../detectors/periodicity.py`
- `.../detectors/procon_swings.py`
- `.../report/triage_builder.py`
- `.../report/analysis_registry.py`
- `.../report/render.py`
- `.../report/templates/report.html.j2`
- `.../detectors/registry.py`

Tests (implemented/updated in this tranche):
- `tests/test_duplicates_near.py`
- `tests/test_bucketed_detectors.py`
- `tests/test_off_hours_detector.py`
- `tests/test_statistics_detectors.py`
- `tests/test_triage_builder.py`
- `tests/test_window_queue_schema.py`
- plus validation against:
  - `tests/test_analysis_registry.py`
  - `tests/test_rarity.py`
  - `tests/test_rarity_baselines.py`
  - `tests/test_report_chart_payload.py`
  - `tests/test_report_render_helpers.py`
  - `tests/test_pipeline_integration.py`

Validation run:
- `python -m ruff check src/testifier_audit/detectors/duplicates_near.py src/testifier_audit/detectors/sortedness.py src/testifier_audit/detectors/off_hours.py src/testifier_audit/report/analysis_registry.py src/testifier_audit/report/triage_builder.py src/testifier_audit/report/render.py tests/test_duplicates_near.py tests/test_bucketed_detectors.py tests/test_off_hours_detector.py tests/test_window_queue_schema.py tests/test_triage_builder.py`
- `python -m pytest tests/test_duplicates_near.py tests/test_bucketed_detectors.py tests/test_off_hours_detector.py tests/test_window_queue_schema.py tests/test_triage_builder.py tests/test_report_chart_payload.py tests/test_report_render_helpers.py tests/test_pipeline_integration.py` (24 passed)
- `./scripts/ci/lint.sh` (passed)
- `./scripts/ci/test.sh` (127 passed)
- Additional validation for regularity tranche:
  - `python -m ruff check src/testifier_audit/detectors/periodicity.py src/testifier_audit/detectors/registry.py src/testifier_audit/report/analysis_registry.py src/testifier_audit/report/render.py tests/test_statistics_detectors.py`
  - `python -m pytest tests/test_statistics_detectors.py tests/test_report_chart_payload.py tests/test_report_render_helpers.py tests/test_analysis_registry.py` (25 passed)
- Additional validation for runs + improbability verification tranche:
  - `python -m ruff check src/testifier_audit/detectors/procon_swings.py src/testifier_audit/report/analysis_registry.py src/testifier_audit/report/render.py tests/test_statistics_detectors.py`
  - `python -m pytest tests/test_statistics_detectors.py tests/test_report_chart_payload.py tests/test_report_render_helpers.py tests/test_analysis_registry.py` (26 passed)
  - `python -m pytest tests/test_rarity.py tests/test_rarity_baselines.py` (6 passed)
  - `python -m pytest tests/test_pipeline_integration.py` (1 passed)
- Additional validation for burst composition tranche:
  - `python -m ruff check src/testifier_audit/detectors/bursts.py src/testifier_audit/report/analysis_registry.py src/testifier_audit/report/render.py tests/test_statistics_detectors.py`
  - `python -m pytest tests/test_statistics_detectors.py tests/test_report_chart_payload.py tests/test_report_render_helpers.py tests/test_analysis_registry.py tests/test_pipeline_integration.py` (28 passed)

Remaining Phase 3 implementation focus:
- None. Phase 3 implementation is complete.

Lessons learned to carry into later phases:
- Extend existing detector families before introducing new detector modules; only add standalone
  modules when methodology is materially different from current contracts.
- Keep rate/composition anomaly outputs defensible by preserving low-power gating and interval
  context in detector outputs and chart payloads.
- Treat chart additions as cross-module contract work:
  update `analysis_registry.py`, `render.py`, `report.html.j2`, and payload tests in one change.
- Resolve roadmap duplication early (for example, name-improbability overlap with rarity pipeline)
  to avoid spending effort on parallel implementations that do not improve signal quality.
- Keep completion criteria explicit in phase docs (`pending` vs `completed`) so multi-tranche work
  remains auditable and avoids hidden carry-over scope.

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

Status (2026-02-21): Complete
- Completed in this phase:
  - Added dual-lens dedup infrastructure:
    - new `features/dedup.py` helpers for mode normalization, count-column mapping, and dedup-safe
      minute-count enrichment.
    - extended `features/aggregates.py::build_counts_per_minute(...)` with dedup-aware minute metrics:
      `n_total_dedup`, `n_pro_dedup`, `n_con_dedup`, `dup_name_fraction_dedup`,
      `dedup_drop_fraction`, and `dedup_multiplier`.
  - Implemented triage dual-lens contracts in `report/triage_builder.py`:
    - `build_investigation_view(..., dedup_mode=...)` now supports `raw`, `exact_row_dedup`,
      and `side_by_side`.
    - added `build_investigation_views(...)` payload-ready map across all three lenses.
    - side-by-side queue rows now include explicit deltas (`count_*`, `pro_rate_*`, `dup_fraction_*`).
  - Implemented `report/quality_builder.py` with:
    - high-value warning generation for:
      - invalid/missing positions
      - unparsable/missing names
      - duplicate IDs
      - non-monotonic timestamps vs ID
      - time-varying organization missingness spikes
    - raw-vs-dedup material-delta metrics for triage context.
  - Wired Phase 4 payload/runtime contracts in `report/render.py`:
    - replaced `pending_phase4` placeholder with computed `data_quality_panel`.
    - added `triage_views` payload key and `controls.default_dedup_mode`.
    - preserved backward-compatible top-level active-lens keys:
      `triage_summary`, `window_evidence_queue`, `record_evidence_queue`, `cluster_evidence_queue`.
    - added artifact export:
      `tables/data_quality__raw_vs_dedup_metrics.(csv|parquet)`.
  - Added report-level config + wiring for dual-lens/data-quality controls:
    - new config fields in `config.py` and YAMLs:
      - `report.min_cell_n_for_rates` (default `25`)
      - `report.default_dedup_mode` (default `side_by_side`)
    - threaded through `pipeline/run_all.py`, CLI (`run-all` and `report`), and `render_report(...)`.
    - added unified script override (`DEDUP_MODE`) in
      `scripts/report/run_unified_report.sh`.
  - Updated `report/templates/report.html.j2`:
    - added triage lens selector (`raw` / `exact_row_dedup` / `side_by_side`) with live rerender.
    - added Data Quality warning card + Raw-vs-Dedup metrics card.
    - export actions now read from active lens queues.
    - moved profiling-only `artifact_rows` table out of detector analysis cards and into
      `Methodology` (`methodology-artifact-rows-host`) to keep triage/investigation paths focused on
      actionable evidence.
  - Refined primary triage metrics to reduce non-actionable noise:
    - `report/quality_builder.py` now emits `triage_raw_vs_dedup_metrics` as material-change rows only
      (with single-row fallback when no material deltas exist).
- Added/updated tests:
  - Added:
    - `tests/test_dedup_lenses.py`
    - `tests/test_data_quality_panel.py`
  - Updated:
    - `tests/test_report_chart_payload.py`
    - `tests/test_report_render_helpers.py`
    - `tests/test_pipeline_integration.py`
    - `tests/test_cli.py`
    - `tests/test_config.py`
  - Validation run:
    - `python -m pytest testifier_audit/tests/test_data_quality_panel.py testifier_audit/tests/test_report_render_helpers.py testifier_audit/tests/test_report_chart_payload.py testifier_audit/tests/test_pipeline_integration.py` (16 passed)
    - `python -m ruff check tests/test_report_render_helpers.py tests/test_data_quality_panel.py`
    - `./scripts/ci/lint.sh` (passed)
    - `./scripts/ci/test.sh` (138 passed)

Scope refinement decisions:
- To avoid Phase 4/5 scope creep, `exact_row_dedup` currently uses the highest-fidelity
  payload-available proxy: canonical-name collapse within minute buckets.
  This is surfaced in panel lens notes and can be upgraded later if/when row-level dedup contracts
  are introduced.
- Record/cluster queues remain lens-switchable (`raw`, `exact_row_dedup`, `side_by_side`) without
  extra side-by-side delta columns; window queue side-by-side deltas were kept as the Phase 4
  sufficient contract because those windows are the primary prioritization surface.

Lessons learned to carry into later phases:
- Keep configuration-driven controls end-to-end:
  when thresholds/modes affect interpretation (for example dedup lens defaults and min support for
  warning rates), thread config values through CLI/pipeline/render contracts instead of embedding
  local defaults in downstream builders.
- Preserve investigator-first information hierarchy:
  keep triage focused on high-signal, actionable tables; move profiling/completeness tables to
  methodology contexts unless they directly influence prioritization.
- Prefer lens-switchable views over schema multiplication:
  if a queue can reuse the same schema across lenses, avoid introducing parallel side-by-side delta
  columns unless the additional fields materially improve prioritization decisions.

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
