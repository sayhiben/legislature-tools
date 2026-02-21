# Testifier Audit

`testifier_audit` analyzes WA Legislature sign-in/testifier exports and generates a detector-first
HTML report for anomaly review.

## Current Development Posture
- This project is in pre-production and is expected to remain in pre-production while we iterate.
- Prioritize rapid, test-backed improvements to accuracy, interpretability, and UX.
- Do not treat feature-flagging or backward-compatibility scaffolding as default work during this phase.
- The current end-to-end roadmap is documented in:
  - `/Users/sayhiben/dev/legislature-tools/IMPLEMENTATION-PLAN-v2.md`

## What this app covers
- Baseline profile diagnostics (volume, day/hour heatmaps, name distributions).
- Burst detection and calibrated significance windows.
- Pro/Con ratio swing detection across bucket sizes.
- Volume and Pro-rate changepoints.
- Off-hours concentration checks.
- Exact and near-duplicate name detection.
- Alphabetical/sortedness pattern detection.
- Rare-name and singleton concentration checks.
- Organization blank/null and concentration anomalies.
- Voter-registry match-rate analysis (overall, by position, by time bucket).
- Periodicity diagnostics (clock-face, autocorrelation, spectrum).
- Multivariate anomaly scoring.
- Composite priority scoring from multiple detector signals.

## Primary workflow (recommended)
Run the unified script. It imports submissions + VRDB into Postgres, executes all analyses, and
writes one consolidated report directory under `../reports/<csv-stem>/`.

```bash
cd /Users/sayhiben/dev/legislature-tools/testifier_audit
./scripts/report/run_unified_report.sh \
  /Users/sayhiben/dev/legislature-tools/data/raw/SB6346-20260206-1330.csv \
  /Users/sayhiben/dev/legislature-tools/data/raw/20260202_VRDB_Extract.txt
```

Result:
- `../reports/SB6346-20260206-1330/report.html`
- `../reports/SB6346-20260206-1330/{tables,summary,figures,artifacts,screenshots}`

## Local setup
```bash
cd /Users/sayhiben/dev/legislature-tools/testifier_audit
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]

docker compose up -d postgres
export TESTIFIER_AUDIT_DB_URL="postgresql://legislature:legislature@localhost:55432/legislature"
```

## CLI commands
```bash
# Import submissions CSV into normalized PostgreSQL tables
python -m testifier_audit.cli import-submissions \
  --csv /Users/sayhiben/dev/legislature-tools/data/raw/SB6346-20260206-1330.csv \
  --db-url "$TESTIFIER_AUDIT_DB_URL"

# Import VRDB extract with upsert semantics
python -m testifier_audit.cli import-vrdb \
  --extract /Users/sayhiben/dev/legislature-tools/data/raw/20260202_VRDB_Extract.txt \
  --db-url "$TESTIFIER_AUDIT_DB_URL"

# Full pipeline (profile + detect + report)
python -m testifier_audit.cli run-all \
  --config /Users/sayhiben/dev/legislature-tools/testifier_audit/configs/voter_registry_enabled.yaml \
  --out /Users/sayhiben/dev/legislature-tools/reports/SB6346-20260206-1330
```

## Configuration highlights
- Default config: `configs/default.yaml`
- Voter-registry-enabled config: `configs/voter_registry_enabled.yaml`
- Supported detector bucket windows:
  - `windows.scan_window_minutes: [1,5,15,30,60,120,240]`
  - `windows.analysis_bucket_minutes: [1,5,15,30,60,120,240]`
- Input hydration:
  - `input.mode: csv | postgres`
  - `input.db_url`, `input.submissions_table`, `input.source_file`
- Voter matching:
  - `voter_registry.enabled`, `voter_registry.db_url`, `voter_registry.table_name`,
    `voter_registry.active_only`, `voter_registry.match_bucket_minutes`

## Report UX stack
- Interactive charts: ECharts
- Interactive tables: Tabulator
- Linked global bucket selector (1/5/15/30/60/120/240)
- Linked time cursor and synchronized zoom for absolute-time charts
- Detector-first sections with interpretation/help text and low-power context

## GitHub Pages publishing
- Workflow: `../.github/workflows/pages.yml`
- Index builder: `scripts/report/build_reports_index.py`
- Published root: `../reports/`

When Pages is enabled (GitHub Actions source), reports are available at:
- `https://<github-user>.github.io/<repo>/`
- `https://<github-user>.github.io/<repo>/<report-id>/report.html`

## Local quality checks
```bash
cd /Users/sayhiben/dev/legislature-tools/testifier_audit
./scripts/ci/lint.sh
./scripts/ci/test.sh
./scripts/ci/run.sh
```

## Data and artifacts
- Keep raw source files in `../data/raw/` (git-ignored).
- Cached report outputs are tracked in `../reports/`.
- Ephemeral local captures live in `../output/` (not committed).
