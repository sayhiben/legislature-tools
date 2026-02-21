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

## Primary workflow
From `testifier_audit/`, the recommended end-to-end run is:

```bash
./scripts/report/run_unified_report.sh \
  /Users/sayhiben/dev/legislature-tools/data/raw/SB6346-20260206-1330.csv \
  /Users/sayhiben/dev/legislature-tools/data/raw/20260202_VRDB_Extract.txt
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
