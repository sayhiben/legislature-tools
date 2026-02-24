# AGENTS.md

Repository-specific guidance for AI/code agents.

## Purpose
- Analyze WA State Legislature public participation/testifier exports.
- Detect anomalous timing/composition patterns with explicit statistical caveats.
- Publish one detector-first interactive report per dataset run.

## Project Status
- This project is pre-production and expected to remain pre-production for an extended period.
- Prioritize correctness, interpretability, UX reliability, and iteration speed.
- Do not add feature flags, rollout gates, legacy renderer paths, or compatibility shims unless explicitly requested.

## Canonical Locations
- App source/tests: `./testifier_audit/`
- Raw local inputs (git-ignored): `./data/raw/`
- Hearing metadata sidecars: `./data/metadata/`
- Published reports: `./reports/`
- Local temporary artifacts (not committed): `./output/`

## Core Engineering Directives
- Keep `src/testifier_audit/report/analysis_registry.py` as the single source of truth for analysis
  definitions and run/publish status.
- Keep report contract logic in dedicated modules (`report/contracts.py`,
  `report/triage_builder.py`) rather than re-encoding shape logic in templates.
- When adding/modifying detector charts, update all four surfaces in one change:
  1. `report/analysis_registry.py`
  2. `report/render.py`
  3. `report/templates/report.html.j2`
  4. payload/render tests
- Preserve runtime instrumentation fields unless intentionally revised:
  - `controls.runtime.payload_build_ms`
  - `controls.runtime.payload_json_bytes`
  - `controls.runtime.interactive_payload_build_ms`
  - `artifacts/report_runtime.json`
- For structural report changes, include focused contract tests and at least one integration parity
  test (`tests/test_pipeline_integration.py`).

## Data Flow and Runbook
Run from repo root unless noted.

```bash
# Fast path for UI/template-only changes (no recompute):
# rerender report HTML/assets from existing report_data + tables in reports/<csv-stem>
cd ./testifier_audit
python -m testifier_audit.cli report \
  --out ../reports/SB6346-20260206-1330 \
  --config ./configs/voter_registry_enabled.yaml \
  --hearing-metadata ../data/metadata/SB6346-20260206-1330.hearing.yaml

# End-to-end import + analysis + report
./testifier_audit/scripts/report/run_unified_report.sh \
  ./data/raw/SB6346-20260206-1330.csv \
  ./data/raw/20260202_VRDB_Extract.txt

# Optional sidecar for hearing-relative context
./testifier_audit/scripts/report/run_unified_report.sh \
  ./data/raw/SB6346-20260206-1330.csv \
  ./data/raw/20260202_VRDB_Extract.txt \
  ./data/metadata/SB6346-20260206-1330.hearing.yaml

# Preferred frontend manual-check flow (Playwright MCP first; screenshots optional)
python -m http.server 8777 --directory ./reports/SB6346-20260206-1330
# Then drive Playwright MCP to: http://127.0.0.1:8777/report.html
# If Playwright fails with "Opening in existing browser session":
playwright-cli close-all
playwright-cli kill-all

# Local CI parity
./testifier_audit/scripts/ci/lint.sh
./testifier_audit/scripts/ci/test.sh
./testifier_audit/scripts/ci/run.sh
```

From repo root:

```bash
python ./testifier_audit/scripts/report/build_reports_index.py
python ./testifier_audit/scripts/report/build_global_baselines.py
```

### Recompute Policy
- Default to template/assets-only rerender (`python -m testifier_audit.cli report`) for visual,
  layout, copy, or interaction-only changes.
- Do **not** run `run_unified_report.sh`/`run-all` unless report data is stale.
- Treat report data as stale when any of the following is true:
  - required cached artifacts are missing/corrupt (`report_data/`, `summary/`, `tables/`,
    `artifacts/report_runtime.json`);
  - source inputs changed (submissions CSV, VRDB extract, or hearing metadata values that affect
    computed outputs);
  - detector/pipeline/config/contract changes alter computed payload values (not just rendering).

## Import Memoization
- Imports memoize by file checksum (not filename).
- Tracking table: `data_imports` in Postgres.
- Repeat import with same checksum is skipped.
- Use `--force` to intentionally bypass memoization.

## Analysis and Interpretation Requirements
- Keep statistical concepts separate in charts/tables:
  - Wilson intervals quantify binomial uncertainty.
  - Control limits and standardized residuals are different diagnostics.
- Keep low-power handling explicit:
  - retain low-power flags,
  - gate inferential claims on support,
  - allow descriptive-only output when alert-eligible windows fail support thresholds.
- Prefer model-aware primary baselines with explicit day/hour fallback behavior.
- Treat persistence and neighborhood structure as first-class evidence:
  repeated adjacent-window signals are stronger than isolated spikes.
- Default to one CSV per report run; comparative analysis must be explicitly requested and rendered
  as separate comparative output.
- Avoid hidden focus flags in orchestration. Use explicit lists of analyses to run/publish.

## Report UX Requirements
- Use Pacific time (`America/Los_Angeles`) for WA-focused reports and communicate timezone once in
  summary context.
- Keep zoom behavior contract-consistent:
  - default to full timeline unless URL params override,
  - apply zoom to all absolute-time views (timeseries, heatmaps/time-grids, funnel/scatter,
    bucketed tables),
  - provide persistent zoom state and clear reset action.
- Persist user controls (`bucket`, theme, palette) via URL/localStorage.
- Source chart colors from selected ECharts theme/palette values; avoid hardcoded chart colors
  (including heatmaps/visualMap gradients).
- Preserve readability across breakpoints:
  right-side legends on wide layouts, responsive legend reflow on narrow layouts, explicit axis
  labels, and sufficient grid spacing.
- On bucket/theme rerender with shape changes, rebuild options from filtered data and clear/replace
  ECharts options so stale data indexes are not reused.
- Keep date/hour heatmaps chronological (dates top-to-bottom, hours left-to-right) with full 24-hour
  slots.
- Keep cross-chart interactions bidirectional:
  clicking funnel/scatter points updates the shared timeseries marker.

## Configuration and Payload Contracts
- Default config: `./testifier_audit/configs/default.yaml`
- Voter-enabled config:
  `./testifier_audit/configs/voter_registry_enabled.yaml`
- Optional sidecar path: `input.hearing_metadata_path`
- Keep bucket windows aligned across detector/payload/UI: `1, 5, 15, 30, 60, 120, 240`
- If bucket options change, update config, payload builder, template runtime behavior, and contract
  tests in the same change.
- Interactive payload version is `2` and must remain finite-safe (`no NaN/Infinity/-Infinity`).

## Testing and QA Expectations
- Coverage targets:
  - overall `>= 80%`
  - changed modules `>= 80%` when practical
- Required before push:
  - `./testifier_audit/scripts/ci/test.sh`
- Recommended coverage command:
  - `cd ./testifier_audit && python -m pytest --cov=src/testifier_audit --cov-report=term-missing --cov-fail-under=80`
- Bug fixes require regression tests in the nearest relevant suite.
- Do not weaken assertions solely to make tests pass unless behavior intentionally changed.

For major report/template changes, run a UX pass:
1. Generate a fresh report.
2. Serve the generated report locally and validate in Playwright MCP at desktop (`~1728x1117`)
   and mobile (`~390x844`) viewports.
3. Verify sidebar navigation, bucket sync, cursor sync, click-marker sync, and zoom sync across
   both timeseries and non-timeseries absolute-time charts.
4. Confirm no visual overlap/clipping, no console JSON/JS errors, and no failing data requests in
   Playwright network logs.
5. Capture screenshots only when needed for review artifacts or when explicitly requested.

## Playwright MCP Manual QA Guidance
- Prefer live Playwright MCP interaction over screenshot-only analysis for frontend validation.
- Standard manual-check loop:
  1. Build/rerender report artifacts.
  2. Start a local static server:
     - `python -m http.server 8777 --directory ./reports/<csv-stem>`
  3. In Playwright MCP, navigate to:
     - `http://127.0.0.1:8777/report.html`
  4. Run both viewport checks:
     - desktop: `1728x1117`
     - mobile: `390x844`
  5. Verify controls and interactions:
     - sidebar toggle, bucket/theme controls, zoom/reset, linked marker/cursor behavior
  6. Check diagnostics:
     - console errors/warnings and failed network requests
- If Playwright cannot launch because an existing session/profile lock is detected:
  - `playwright-cli close-all`
  - `playwright-cli kill-all`
  - retry MCP navigation
- If browser binaries are missing:
  - `playwright-cli install-browser`

## Screenshot Capture Guidance (Secondary)
- Use screenshot capture as supporting evidence, not as the primary frontend QA method.
- Preferred helper:
  - `./testifier_audit/scripts/report/run_unified_report_and_capture.sh`
- Do not rely on Chromium full-page capture for very tall pages; it can duplicate segments around
  ~16,384px.
- Use chunked capture script when needed:
  - `./testifier_audit/scripts/report/capture_report_screenshot.py`

## Runtime and Infrastructure
- PostgreSQL is defined in
  `./testifier_audit/docker-compose.yml`.
- Default DB URL:
  - `postgresql://legislature:legislature@localhost:55432/legislature`
- Unified run script auto-starts Postgres with `docker compose up -d postgres`.

## CI and Publishing
- CI workflow:
  - `./.github/workflows/testifier-audit-ci.yml`
- Pages workflow:
  - `./.github/workflows/pages.yml`
- Pages publishes from `reports/` on push to `main` and rebuilds `reports/index.html`.

## Data and Git Hygiene
- Never commit raw source extracts from `data/raw/`.
- Commit `reports/` cached artifacts only when requested.
- Do not commit `./output/`.
- Keep local commands aligned with scripts in `./testifier_audit/scripts/ci/`.

## Known Pitfalls
- Do not reintroduce lazy-loading assumptions in tests unless lazy loading is intentionally restored.
- If report output appears empty, first check for JSON parse failures caused by non-finite values.
- If CI fails, inspect logs before patching.

## Nickname Dataset Maintenance
- Source notes:
  - `./testifier_audit/configs/nicknames.SOURCE.md`
- Regenerator:
  - `./testifier_audit/scripts/data/update_nicknames.py`
- Keep upstream source pin and local override mapping synchronized.
