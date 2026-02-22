# AGENTS.md

Repository-specific guidance for AI/code agents.

## Project Intent
- Analyze WA State Legislature public participation/testifier exports.
- Detect anomalous patterns in timing, pro/con ratios, names, organization fields, periodicity,
  and multi-detector evidence.
- Publish a single detector-first interactive report per dataset run.

## Pre-Production Policy
- This project is currently pre-production and expected to remain pre-production for an extended period.
- Do not implement feature flagging, rollout gates, or legacy renderer paths unless explicitly requested.
- Do not spend effort on backward compatibility shims, migration compatibility layers, or preserving old internal contracts by default.
- Optimize for rapid iteration toward the first publishable version; the user will explicitly indicate when production-readiness constraints should be introduced.

## Roadmap Source of Truth
- The current implementation roadmap is:
  - `/Users/sayhiben/dev/legislature-tools/IMPLEMENTATION-PLAN-v2.md`
- When proposing or executing substantial changes, align with this plan unless the user explicitly overrides it.

## Durable Engineering Lessons
- Treat `src/testifier_audit/report/analysis_registry.py` as the single source of truth for
  analysis definitions and run/publish status.
- Keep detector/report contracts in dedicated modules (`report/contracts.py`,
  `report/triage_builder.py`) rather than re-encoding shape logic in templates.
- When adding or modifying detector charts, update all four surfaces together:
  `report/analysis_registry.py`, `report/render.py`, `report/templates/report.html.j2`, and
  payload/render tests.
- Preserve runtime instrumentation fields unless intentionally revised:
  `controls.runtime.payload_build_ms`, `controls.runtime.payload_json_bytes`,
  `controls.runtime.interactive_payload_build_ms`, and `artifacts/report_runtime.json`.
- For structural report changes, require both focused unit/contract tests and at least one
  integration parity test (`tests/test_pipeline_integration.py`).

## Off-Hours Analysis Lessons (Current)
- Do not conflate statistical concepts across views:
  Wilson intervals quantify binomial proportion uncertainty for a cell/window;
  control limits and standardized residuals are separate diagnostics.
- Keep low-power handling explicit:
  retain low-power flags, gate inferential claims on support, and allow descriptive-only runs when
  alert-eligible windows fail support criteria.
- Prefer model-aware primary baselines (when available) with explicit day/hour fallback paths.
- Treat persistence and neighborhood structure as first-class evidence:
  repeated adjacent-window signals are stronger than isolated single-cell spikes.
- Keep one CSV per report run by default; comparative analysis must be explicitly requested and
  rendered as a separate comparative output.
- Avoid hidden focus flags in orchestration; use an explicit list of analyses to run/publish so
  unused analyses are not computed during focused development runs.

## Report UX Lessons (Current)
- All report times are Pacific (`America/Los_Angeles`) for WA-focused analysis; communicate this
  once in report summary context instead of repeating timezone labels everywhere.
- Keep zoom behavior contract-consistent:
  default to full timeline unless URL params provide an override, and apply zoom to every chart
  with absolute-time data (timeseries, date/hour heatmaps, funnel/scatter, and bucketed tables).
- When zoom is active, show persistent state in the UI and provide a clear reset action.
- Persist user controls (`bucket`, theme, palette) via URL/localStorage so views are shareable and
  restore correctly on load.
- Ensure chart palettes/themes are centrally sourced from selected ECharts theme/palette values;
  avoid hardcoded chart colors, including heatmaps and visualMap gradients.
- Keep chart readability resilient:
  right-side legends on wide layouts, responsive legend reflow on narrow/mobile layouts, explicit
  axis labels, and enough grid spacing so x-axis labels do not collide with zoom/visualMap UI.
- On bucket/theme rerender, avoid stale ECharts state when series/data shapes change:
  rebuild option payloads from the active filtered dataset and clear/replace chart options so
  tooltip/dataIndex lookups do not reference removed series.
- For date/hour heatmaps, use chronological reading order (top-to-bottom dates, left-to-right
  hours), keep consistent 24-hour slot display, and align bucketing behavior with global controls.
- Cross-chart interaction should stay bidirectional:
  clicking funnel/scatter points should move the shared timeline marker in time-series charts.

## Fast Onboarding Checklist (10-15 minutes)
1. Read:
   - `/Users/sayhiben/dev/legislature-tools/README.md`
   - `/Users/sayhiben/dev/legislature-tools/testifier_audit/README.md`
   - `/Users/sayhiben/dev/legislature-tools/testifier_audit/configs/default.yaml`
2. Validate environment:
   - `python --version` (3.11+)
   - `docker --version`
   - `docker compose version`
3. Install app in editable mode:
   - `cd /Users/sayhiben/dev/legislature-tools/testifier_audit`
   - `python -m pip install -e ".[dev]"`
4. Run baseline checks:
   - `./scripts/ci/lint.sh`
   - `./scripts/ci/test.sh`

## Canonical Directories
- `/Users/sayhiben/dev/legislature-tools/testifier_audit/`: app source, detectors, reports, tests.
- `/Users/sayhiben/dev/legislature-tools/data/raw/`: local raw inputs (git-ignored).
- `/Users/sayhiben/dev/legislature-tools/reports/`: cached rendered reports (tracked and published).
- `/Users/sayhiben/dev/legislature-tools/output/`: temporary local artifacts (do not commit).

## Data Flow and Orchestration
- Submissions CSV import:
  - `/Users/sayhiben/dev/legislature-tools/testifier_audit/scripts/db/import_submissions.sh`
- VRDB extract import:
  - `/Users/sayhiben/dev/legislature-tools/testifier_audit/scripts/vrdb/import_vrdb.sh`
- Unified pipeline:
  - `/Users/sayhiben/dev/legislature-tools/testifier_audit/scripts/report/run_unified_report.sh`
- Unified output:
  - `/Users/sayhiben/dev/legislature-tools/reports/<csv-stem>/`

## Import Memoization (Important)
- Raw files do not need to be re-imported on every report run when their contents are unchanged.
- Importers now memoize by file checksum (not filename), because filenames can be reused with
  different data.
- Tracking table:
  - `data_imports` in Postgres
  - records import kind, target table, file hash, importer version, status, and row counts.
- Behavior:
  - repeated import with same checksum is skipped
  - use `--force` on import commands to bypass memoization intentionally.

## Primary Runbook
Run from `/Users/sayhiben/dev/legislature-tools/testifier_audit` unless noted.

```bash
# End-to-end import + analysis + report
./scripts/report/run_unified_report.sh \
  /Users/sayhiben/dev/legislature-tools/data/raw/SB6346-20260206-1330.csv \
  /Users/sayhiben/dev/legislature-tools/data/raw/20260202_VRDB_Extract.txt

# Optional: include hearing metadata sidecar for process markers and hearing-relative context
./scripts/report/run_unified_report.sh \
  /Users/sayhiben/dev/legislature-tools/data/raw/SB6346-20260206-1330.csv \
  /Users/sayhiben/dev/legislature-tools/data/raw/20260202_VRDB_Extract.txt \
  /Users/sayhiben/dev/legislature-tools/output/hearing_metadata/SB6346-20260206-1330.hearing.yaml

# Preferred visual-regression capture: generate report + capture known-good stitched screenshot
./scripts/report/run_unified_report_and_capture.sh \
  /Users/sayhiben/dev/legislature-tools/data/raw/SB6346-20260206-1330.csv \
  /Users/sayhiben/dev/legislature-tools/data/raw/20260202_VRDB_Extract.txt \
  /Users/sayhiben/dev/legislature-tools/output/hearing_metadata/SB6346-20260206-1330.hearing.yaml

# Local CI parity
./scripts/ci/lint.sh
./scripts/ci/test.sh
./scripts/ci/run.sh

# Rebuild reports index (run from repo root)
python /Users/sayhiben/dev/legislature-tools/testifier_audit/scripts/report/build_reports_index.py

# Rebuild cross-hearing baseline corpus (run from repo root, after report generation)
python /Users/sayhiben/dev/legislature-tools/testifier_audit/scripts/report/build_global_baselines.py
```

## Runtime and Infrastructure
- PostgreSQL runs in `/Users/sayhiben/dev/legislature-tools/testifier_audit/docker-compose.yml`.
- Default DB URL:
  - `postgresql://legislature:legislature@localhost:55432/legislature`
- Unified script auto-starts Postgres with `docker compose up -d postgres`.

## Configuration Truths
- Default config:
  - `/Users/sayhiben/dev/legislature-tools/testifier_audit/configs/default.yaml`
- Voter-enabled config:
  - `/Users/sayhiben/dev/legislature-tools/testifier_audit/configs/voter_registry_enabled.yaml`
- Optional hearing metadata sidecar path:
  - `input.hearing_metadata_path`
- Keep bucket windows consistent across detectors, payload, and UI:
  - `1, 5, 15, 30, 60, 120, 240`
- If bucket options change, update:
  1. config YAML
  2. payload builder (`report/render.py`)
  3. report template runtime behavior (`report/templates/report.html.j2`)
  4. contract tests (`tests/test_report_chart_payload.py` and related tests)

## Testing Policy (80%+ Coverage Goal)
- Target goals:
  - overall coverage: `>= 80%`
  - changed files/modules: `>= 80%` whenever practical
- Required before push:
  - `./scripts/ci/test.sh`
- Coverage validation command (recommended for feature work):
  - `python -m pytest --cov=src/testifier_audit --cov-report=term-missing --cov-fail-under=80`
- If a bug is fixed, add a regression test in the nearest relevant suite.
- Do not weaken assertions to “make tests pass” unless behavior intentionally changed.

## Linting Best Practices
- CI currently enforces:
  - `python -m ruff check src tests --select F`
- Local preferred standard before commit:
  1. `python -m ruff check src tests`
  2. `python -m ruff format src tests`
  3. rerun targeted tests
- Keep line length at 100 and avoid reintroducing broad ignores.
- Use auto-fix selectively and review diffs for behavioral changes.

## Report Frontend Architecture (Current Decisions)
- Stack: ECharts + Tabulator + static template (`report/templates/report.html.j2`).
- Layout: detector-first, hero chart first, then interpretation/help, then details/tables.
- Sidebar behavior:
  - fixed and collapsible desktop
  - hamburger collapsed by default on mobile
  - active section tracking and URL hash updates
  - global bucket selector in sidebar
- Chart behavior:
  - eager mount (`mountAllSections()`), not lazy mount
  - all time-series charts and time-bucketed tables support bucketing/rollups via the global bucket selector
  - synchronized zoom range across all time-series charts
  - apply linked zoom to all charts with absolute-time rows whenever possible:
    timeseries use zoomed x-range, heatmaps/time-grids truncate out-of-range buckets, and funnel/scatter
    views drop points outside the selected window
  - synchronized vertical cursor across time-series charts that follows mouse X position
  - synchronized click marker across time-series charts; clicking any chart moves/places the shared X marker
- Static image gallery / PhotoSwipe UI is intentionally not used in report page.
- Every analysis section should render at least one chart.
- Empty/disabled analyses must remain visible with explicit reason.

## Statistical and Interpretation Conventions
- Use Wilson intervals and low-power flags on proportion series where applicable.
- Keep low-power interpretation visible globally and in section context.
- Each chart/table should include guidance on:
  - what it is
  - why it matters
  - how to read it
  - what anomalies to look for
  - momentary vs extended highs/lows
  - legend or column definitions

## Payload and Template Contracts
- Interactive payload version: `2`.
- Payload contains:
  - `analysis_catalog`
  - `charts`
  - `cross_hearing_baseline`
  - `controls` (`global_bucket_options`, `default_bucket_minutes`, `zoom_sync_groups`)
  - chart/table legend/help docs
- JSON must be finite-safe:
  - no `NaN`, `Infinity`, or `-Infinity` in rendered payload.

## UX and Usability Evaluation Guide
After major report/template changes, perform this checklist:
1. Generate a fresh report with unified script.
2. Serve locally from report directory:
   - `cd /Users/sayhiben/dev/legislature-tools/reports/<csv-stem>`
   - `python -m http.server 8774`
3. Validate desktop and mobile layouts:
   - desktop around `1728x1117`
   - mobile around `390x844`
4. Verify interaction quality:
   - sidebar navigation and active section highlighting
   - global bucket selection updates all time-series charts and time-bucketed tables
   - cursor linking follows shared x-axis position across time-series charts
   - clicking any time-series chart moves/places the shared X marker across time-series charts
   - zooming one time-series chart syncs global zoom range to all time-series charts
   - zoom selection also updates non-timeseries time-based charts (for example date/hour heatmaps and funnel plots)
5. Confirm no visual overlap:
   - hero charts are readable and not vertically clipped
   - tables are full-width and not squished
6. Open browser console and verify no runtime JSON/JS errors.
7. Capture screenshots into:
   - `/Users/sayhiben/dev/legislature-tools/reports/<csv-stem>/screenshots/`
   - include full-page plus top/middle/bottom viewport shots.

## Reliable Screenshot Capture (Very Tall Pages)
- Dependency:
  - `brew install playwright-cli` (preferred)
- Do not rely on Chromium full-page screenshot for long reports; it can repeat content at
  ~16,384px intervals on very tall pages.
- Note: repeated fixed sidebar visuals in stitched screenshots can be a capture artifact from tiled
  viewport stitching and do not necessarily indicate duplicated report content.
- Preferred one-command workflow for visual regression checks:
  - `/Users/sayhiben/dev/legislature-tools/testifier_audit/scripts/report/run_unified_report_and_capture.sh`
  - runs `run_unified_report.sh`, then captures a fully loaded stitched screenshot from `report.html`
    using the known-good defaults (`--wait-ms 12000`, `--settle-ms 600`, sidebar open, all
    `<details>` expanded).
- Use the chunked capture script:
  - `/Users/sayhiben/dev/legislature-tools/testifier_audit/scripts/report/capture_report_screenshot.py`
- Script behavior:
  - accepts a report URL or local report path
  - if a local path is provided, starts a temporary localhost server automatically
  - opens sidebar + expands all `<details>` by default
  - hides fixed report chrome (sidebar/menu/busy indicator) by default during tiling to reduce
    false duplicate-content interpretation in stitched output
  - prints per-tile progress and writes capture diagnostics JSON next to the PNG
  - captures viewport tiles and stitches into one PNG
- Example command:
  - `python /Users/sayhiben/dev/legislature-tools/testifier_audit/scripts/report/capture_report_screenshot.py /Users/sayhiben/dev/legislature-tools/reports/<csv-stem>/report.html /Users/sayhiben/dev/legislature-tools/reports/<csv-stem>/screenshots/report-full-expanded-sidebar-<YYYYMMDD>-stitched.png --width 1920 --height 1400 --wait-ms 12000 --settle-ms 600`
- Optional flags:
  - `--keep-fixed-chrome` (opt out of fixed-chrome hiding; can reintroduce repeated-sidebar artifacts)
  - `--no-open-sidebar`
  - `--no-expand-details`
  - `--max-tiles <N>` (bounds runtime and intentionally truncates output with warning metadata)
  - `--command-timeout-sec <seconds>` (fails fast if any Playwright CLI command hangs)
  - `--keep-tiles` (keeps intermediate tiles under `output/playwright/tiles/`)

## CI and GitHub Pages
- Test/lint workflow:
  - `/Users/sayhiben/dev/legislature-tools/.github/workflows/testifier-audit-ci.yml`
  - triggers on `testifier_audit/**` and workflow file changes.
- Pages workflow:
  - `/Users/sayhiben/dev/legislature-tools/.github/workflows/pages.yml`
  - publishes `reports/` on push to `main`
  - regenerates `reports/index.html` before deploy.

## Git and Data Hygiene
- Never commit raw source extracts from `data/raw/`.
- Commit `reports/` cached artifacts when requested.
- Do not commit `/Users/sayhiben/dev/legislature-tools/output/`.
- `.gitignore` currently ignores `data/raw/` and `output/playwright/`.

## Known Regression Traps
- Do not reintroduce lazy-loading assumptions in tests unless lazy loading is explicitly restored.
- If CI fails, use `gh` to inspect run logs before patching.
- If report renders empty, first check for JSON parse failures from non-finite values.

## Nickname Dataset Maintenance
- Source details:
  - `/Users/sayhiben/dev/legislature-tools/testifier_audit/configs/nicknames.SOURCE.md`
- Regenerator:
  - `/Users/sayhiben/dev/legislature-tools/testifier_audit/scripts/data/update_nicknames.py`
- Keep upstream source pin + local override mapping synchronized.

## Practical Tips for Future Agents
- Prefer targeted, test-backed changes over broad rewrites.
- When adding a detector/chart:
  1. generate detector table/summary outputs
  2. wire chart IDs and docs in payload
  3. ensure template hosts render correctly
  4. add or update contract/integration tests
  5. validate UX with screenshots and console checks
- Keep local commands and CI behavior aligned; scripts in `scripts/ci/` are the source of truth.
