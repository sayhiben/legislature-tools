# Legislature Tools

Utilities for ingesting and analyzing public Washington State Legislature participation data.

## Repository layout
- `testifier_audit/`: Python application for anomaly analysis and report generation.
- `data/raw/`: local raw source files (ignored by git).
- `reports/`: cached rendered reports published to GitHub Pages.
- `output/`: local working artifacts/screenshots (not committed).
- `anomaly-detection-guidance.md`: detector and statistical guidance.
- `initial-implementation-plan.txt`: implementation plan notes.
- `IMPLEMENTATION-PLAN-v2.md`: current comprehensive implementation roadmap.
- `AGENTS.md`: repository-specific operating guidance for agents and contributors.

## Current Development Posture
- The project is intentionally pre-production and iterating toward a first publishable version.
- During this pre-production phase, prioritize correctness, usability, and iteration speed over
  feature flags or backward-compatibility scaffolding.
- The current roadmap source of truth is `IMPLEMENTATION-PLAN-v2.md`.

## Lessons Learned (Phase 0)
- Keep report contracts and registries modular and typed; avoid growing `report/render.py` as a
  mixed source-of-truth module.
- Maintain exactly one source of truth for analysis definitions/status to avoid silent drift across
  payload, template, and tests.
- Add contract-level tests plus at least one integration parity test whenever core report contracts
  move.
- Keep runtime observability first-class: preserve payload/render timing metrics and payload byte
  size reporting to catch regressions early.

## Lessons Learned (Phase 1 UX Reliability)
- Avoid showing routine `ready` status badges; reserve badges for exceptional states
  (`empty`/`disabled`) to reduce visual noise.
- Sidebar layout changes can distort ECharts unless resize is sequenced after transition; use a
  multi-pass resize pattern on toggle/viewport changes.
- Bucket rerenders should show explicit progress feedback so users can distinguish redraw latency
  from a frozen UI.
- Keep shared controls contract-driven: global zoom reset/range and timezone labels should be backed
  by payload controls rather than ad-hoc template assumptions.

## Lessons Learned (Phase 2 Investigation IA)
- Keep the report workflow explicit and investigation-first: `Triage` ->
  `Window Drilldown` -> `Name/Cluster Forensics` -> `Methodology`.
- Treat triage queues and summary fields as payload contracts; update renderer, template, and
  contract tests together whenever queue schema changes.
- Preserve drilldown click behavior across both table runtimes:
  Tabulator mode and fallback HTML table mode (including keyboard activation).
- Keep Phase 2 drilldown data lightweight by reusing existing timeline artifacts; defer
  per-submission raw-row payload expansion until explicitly prioritized with payload-size/runtime
  tradeoff acceptance.
- Close report UX changes with a real-dataset QA pass (desktop/mobile screenshots + browser console
  checks), not only unit tests.

## Lessons Learned (Phase 3 Analysis Pack A)
- Favor extending mature detector families instead of creating parallel modules when intent is
  additive:
  - burst composition on top of `bursts`
  - rolling-Fano regularity on top of `periodicity`
  - directional runs on top of `procon_swings`
- Keep composition/rate outputs inference-safe by carrying Wilson intervals and low-power flags in
  detector tables and chart payloads.
- Treat name-improbability-over-time as covered by the external-frequency rarity pipeline in
  `rare_names` unless a new methodology is explicitly required.
- For every new analysis chart, update registry definitions, payload wiring, template rendering, and
  contract tests together to prevent contract drift.

## Lessons Learned (Phase 4 Data Quality and Dual-Lens Reporting)
- Keep dedup-lens behavior contract-driven in payload controls:
  default lens selection belongs in `controls.default_dedup_mode` and lens views belong in
  `triage_views`.
- Keep triage actionable by default:
  prioritize high-value quality warnings and material raw-vs-dedup deltas in the `Triage` section,
  while relocating profiling-only coverage tables to `Methodology`.
- Treat quality warning sensitivity as configuration, not template logic:
  thread `report.min_cell_n_for_rates` through config/CLI/pipeline into report quality builders.
- Limit Phase 4 side-by-side expansion to the primary prioritization surface (window queue deltas)
  unless there is an explicit need to mirror side-by-side deltas for record/cluster queues.

## Lessons Learned (Phase 5 Hearing-Relative Context)
- Keep hearing metadata optional but fail fast when provided:
  parse/validate sidecars at CLI boundaries so invalid timezone/timestamp/schema issues are
  surfaced before long pipeline runs.
- Preserve robust timestamp parsing for sidecars:
  accept both ISO strings and YAML-native datetime values so metadata remains portable across
  authoring tools.
- Treat hearing timing as an explicit contract:
  infer process markers from the hearing submissions dataset + metadata sidecar, not VRDB extract
  filenames (extract timestamps represent export timing, not hearing timing).
- Reuse existing minute-level artifacts for deadline context:
  implement deadline-ramp and stance-by-deadline summaries in `hearing_context_panel` without
  introducing a parallel detector family.

## Lessons Learned (Phase 6 Probabilistic Voter Linkage)
- Treat voter linkage as probabilistic and tiered (`exact`, `strong_fuzzy`, `weak_fuzzy`,
  `unmatched`) rather than binary matched/unmatched framing.
- Keep uncertainty explicit in detector outputs and report payloads:
  emit `match_confidence`, expected-match metrics, and uncertainty summaries/caveats.
- Keep attribution guardrails explicit in both methodology copy and payload semantics:
  voter linkage is supporting context, not standalone attribution evidence.
- Keep detector/report parity strict for voter-linkage changes:
  update `analysis_registry.py`, `render.py`, `report.html.j2`, and contract tests together.

## Lessons Learned (Phase 7 Cross-Hearing Baselines)
- Keep `summary/feature_vector.json` schema-versioned and stable so each report run can feed the
  corpus baseline process without custom adapters.
- Build corpus baselines from existing report artifacts (`feature_vector.json` with
  `investigation_summary.json` backfill) rather than adding a separate feature-store service.
- Keep comparator overlays constrained to high-value charts first to preserve readability and avoid
  broad runtime/render complexity.
- Keep cross-hearing payloads deterministic:
  always emit `cross_hearing_baseline` with `available=false` fallback fields when corpus baselines
  are absent.

## Primary workflow
From `testifier_audit/`, the recommended end-to-end run is:

```bash
./scripts/report/run_unified_report.sh \
  /Users/sayhiben/dev/legislature-tools/data/raw/SB6346-20260206-1330.csv \
  /Users/sayhiben/dev/legislature-tools/data/raw/20260202_VRDB_Extract.txt

# Optional: include hearing metadata sidecar for process markers + hearing-relative context
./scripts/report/run_unified_report.sh \
  /Users/sayhiben/dev/legislature-tools/data/raw/SB6346-20260206-1330.csv \
  /Users/sayhiben/dev/legislature-tools/data/raw/20260202_VRDB_Extract.txt \
  /Users/sayhiben/dev/legislature-tools/output/hearing_metadata/SB6346-20260206-1330.hearing.yaml

# Rebuild cross-hearing comparative baselines (run from repo root)
python /Users/sayhiben/dev/legislature-tools/testifier_audit/scripts/report/build_global_baselines.py
```

This imports submissions + voter registry data into Postgres, runs all detectors, and writes a
single report directory under `reports/<csv-stem>/`.

## GitHub Pages hosting
- Workflow: `.github/workflows/pages.yml`
- Index generator: `testifier_audit/scripts/report/build_reports_index.py`
- Published root: `reports/` (includes `reports/index.html` and `reports/.nojekyll`)

After enabling **Settings -> Pages -> Source: GitHub Actions**, reports are available at:

- `https://<github-user>.github.io/<repo>/`
- `https://<github-user>.github.io/<repo>/<report-id>/report.html`

## Application details
See `testifier_audit/README.md` for full setup, configs, and commands.
