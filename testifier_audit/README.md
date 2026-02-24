# Testifier Audit

`testifier_audit` analyzes WA Legislature sign-in/testifier exports and generates a detector-first
HTML report for anomaly review.

## Development Posture
- This app is pre-production.
- Prioritize rapid, test-backed improvements to accuracy, interpretability, and UX.
- Avoid feature flags and backward-compatibility scaffolding unless explicitly requested.
- Development direction and guardrails: `./AGENTS.md`

## What This App Covers
- Burst detection and calibrated significance windows.
- Pro/Con ratio swings across bucket sizes.
- Volume and Pro-rate changepoints.
- Off-hours concentration checks.
- Exact and near-duplicate name detection.
- Alphabetical/sortedness pattern detection.
- Rare-name and singleton concentration checks.
- Organization blank/null and concentration anomalies.
- Voter-registry match-rate analysis (overall, by position, and by time bucket).
- Periodicity diagnostics (clock-face, autocorrelation, spectrum).
- Multivariate and composite scoring built from detector evidence.

## Prerequisites
- Python 3.11+
- Docker + Docker Compose

## Primary Workflow (Recommended)
Run from repo root.

```bash
./testifier_audit/scripts/report/run_unified_report.sh \
  ./data/raw/SB6346-20260206-1330.csv \
  ./data/raw/20260202_VRDB_Extract.txt

# Optional hearing metadata sidecar
./testifier_audit/scripts/report/run_unified_report.sh \
  ./data/raw/SB6346-20260206-1330.csv \
  ./data/raw/20260202_VRDB_Extract.txt \
  ./data/metadata/SB6346-20260206-1330.hearing.yaml

# Preferred visual-regression flow (run + stitched capture)
./testifier_audit/scripts/report/run_unified_report_and_capture.sh \
  ./data/raw/SB6346-20260206-1330.csv \
  ./data/raw/20260202_VRDB_Extract.txt \
  ./data/metadata/SB6346-20260206-1330.hearing.yaml
```

Outputs are written to `./reports/<csv-stem>/`.

## Local Setup

```bash
cd ./testifier_audit
cp .env.example .env
# Edit .env as needed

make setup-env

export TESTIFIER_AUDIT_DB_URL="postgresql://legislature:legislature@localhost:55432/legislature"
export DATABASE_URL="$TESTIFIER_AUDIT_DB_URL"
export TESTIFIER_AUDIT_SKIP_DOCKER_POSTGRES=1
```

Lifecycle:

```bash
make start
make stop
make restart
make status
make reset-db
```

Schema helpers:

```bash
./scripts/db/apply_schema.sh "$TESTIFIER_AUDIT_DB_URL"
./scripts/db/extract_schema.sh "$TESTIFIER_AUDIT_DB_URL" ./sql/schema.sql
```

## CLI Commands

```bash
# Download testifier CSV + hearing metadata sidecar from WA CSI
python -m testifier_audit.cli download-csi-testifiers \
  "SB 6005" \
  --csv-out-dir ./data/raw \
  --metadata-out-dir ./data/metadata

# Import submissions CSV
python -m testifier_audit.cli import-submissions \
  --csv ./data/raw/SB6346-20260206-1330.csv \
  --db-url "$TESTIFIER_AUDIT_DB_URL"

# Import VRDB extract
python -m testifier_audit.cli import-vrdb \
  --extract ./data/raw/20260202_VRDB_Extract.txt \
  --db-url "$TESTIFIER_AUDIT_DB_URL"

# Full pipeline (profile + detect + report)
python -m testifier_audit.cli run-all \
  --config ./testifier_audit/configs/voter_registry_enabled.yaml \
  --hearing-metadata ./data/metadata/SB6346-20260206-1330.hearing.yaml \
  --out ./reports/SB6346-20260206-1330
```

## Configuration Highlights
- Default config: `configs/default.yaml`
- Voter-enabled config: `configs/voter_registry_enabled.yaml`
- Input modes: `csv | postgres` via `input.mode`
- Optional sidecar path: `input.hearing_metadata_path`
- Bucket windows (must stay aligned across detector/payload/UI):
  - `windows.scan_window_minutes: [1,5,15,30,60,120,240]`
  - `windows.analysis_bucket_minutes: [1,5,15,30,60,120,240]`

## Report Stack
- Charts: ECharts
- Tables: Tabulator
- Global bucket selector and synchronized time-zoom/cursor behavior
- Detector-first section layout with interpretation/help content

## Local Quality Checks

```bash
cd ./testifier_audit
./scripts/ci/lint.sh
./scripts/ci/test.sh
./scripts/ci/run.sh
```

## Data and Artifacts
- Raw source files belong in `./data/raw/` (git-ignored).
- Cached report outputs in `./reports/` are tracked and published.
- Ephemeral local captures belong in `./output/` (not committed).

## Additional Guidance
Development contracts, guardrails, and QA expectations are documented in:
`./AGENTS.md`.
