# Testifier Audit

`testifier_audit` analyzes WA State Legislature public participation CSV exports for timing, stance, and name-pattern anomalies.

## Implemented in this iteration
- Config-driven CLI: `profile`, `detect`, `report`, `run-all`
- Two-pass workflow: profile artifacts then detector suite
- Statistical detectors:
  - Burst windows (Poisson scan + BH-FDR + optional calibration)
  - Pro/Con swing windows (windowed rate tests + BH-FDR + optional calibration)
  - Volume and pro-rate changepoints
  - Permutation-calibrated p-values with BH-FDR columns for calibrated detectors
  - Periodicity/autocorrelation/spectrum peaks with simulation-calibrated significance
- Calibration modes:
  - `global`: stationary Poisson/Binomial null simulation
  - `hour_of_day`: stratified null simulation preserving diurnal structure
  - `day_of_week_hour`: stratified null simulation preserving weekly diurnal structure
- Name/identity detectors:
  - Exact duplicates and same-minute repeats
  - Blocked fuzzy near-duplicates with cluster extraction
  - Optional first/last-name rarity enrichment from external frequency tables
- Additional detectors:
  - Off-hours patterns
  - Periodicity/autocorrelation/spectrum
  - Ordering/sortedness diagnostics
  - Rare/unique/weird name windows
  - Voter-registry match rates (overall, by position, and by time bucket)
  - Multivariate bucket anomaly scoring (IsolationForest over volume/rate/duplication/org features)
  - Organization blank/null rate shifts over time (total + Pro/Con partitions at 1/5/15/30/60/120 minute buckets)
  - Organization concentration patterns
  - Composite ranking of suspicious windows with evidence-bundle exports
- HTML reporting with detector summaries, artifact counts, table previews, and figures
- Detector overlay figures (`counts_with_anomalies`, `pro_rate_with_anomalies`)
- Pro/Con stability visualizations (`pro_rate_heatmap_day_hour`, `pro_rate_shift_heatmap_15m`, `pro_rate_shift_heatmap_30m`, `pro_rate_shift_heatmap_60m`, `pro_rate_bucket_trends`, `pro_rate_time_of_day_profiles`)
- Organization blank-rate visualization (`organization_blank_rates`)
- Calibration null-distribution figures (`bursts_null_distribution`, `swing_null_distribution`)
- Periodicity figures (`periodicity_autocorr`, `periodicity_spectrum`, `periodicity_clockface`)
- Multivariate anomaly figure (`multivariate_anomaly_scores`)
- Dockerized runtime with an internal virtualenv at `/opt/venv`

## Quick start (local)
```bash
cd /Users/sayhiben/dev/legislature-tools/testifier_audit
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]

# Build profile artifacts and figures
testifier-audit profile \
  --csv /Users/sayhiben/dev/legislature-tools/data/raw/SB6346-20260206-1330.csv \
  --out /Users/sayhiben/dev/legislature-tools/testifier_audit/out

# Run detectors using saved profile artifacts
testifier-audit detect \
  --csv /Users/sayhiben/dev/legislature-tools/data/raw/SB6346-20260206-1330.csv \
  --out /Users/sayhiben/dev/legislature-tools/testifier_audit/out

# Rebuild complete outputs
testifier-audit run-all \
  --csv /Users/sayhiben/dev/legislature-tools/data/raw/SB6346-20260206-1330.csv \
  --out /Users/sayhiben/dev/legislature-tools/testifier_audit/out

# Prepare rarity lookups from raw baseline files (SSA/Census-style)
testifier-audit prepare-rarity-baselines \
  --out-dir /Users/sayhiben/dev/legislature-tools/testifier_audit/configs \
  --first-raw /path/to/first_names_raw.csv \
  --last-raw /path/to/last_names_raw.csv \
  --write-config /Users/sayhiben/dev/legislature-tools/testifier_audit/configs/default.yaml

# Run CI checks locally (same scripts used by GitHub Actions)
./scripts/ci/lint.sh
./scripts/ci/test.sh
./scripts/ci/run.sh

# Unified single-page report (imports submissions + VRDB, then runs all analyses together)
./scripts/report/run_unified_report.sh \
  /Users/sayhiben/dev/legislature-tools/data/raw/SB6346-20260206-1330.csv \
  /Users/sayhiben/dev/legislature-tools/data/raw/20260202_VRDB_Extract.txt
```

## Voter registry matching workflow
```bash
cd /Users/sayhiben/dev/legislature-tools/testifier_audit
export TESTIFIER_AUDIT_DB_URL="postgresql://legislature:legislature@localhost:55432/legislature"

# Import/update voter registry rows (pipe-delimited WA VRDB extract)
./scripts/vrdb/import_vrdb.sh /Users/sayhiben/dev/legislature-tools/data/raw/20260202_VRDB_Extract.txt

# Enable matching in config (or set via another config file)
# voter_registry:
#   enabled: true
#   db_url: "postgresql://legislature:legislature@localhost:5432/legislature"
#   table_name: "voter_registry"
#   active_only: true
#   match_bucket_minutes: 30

# Run analysis with voter matching enabled
testifier-audit run-all \
  --csv /Users/sayhiben/dev/legislature-tools/data/raw/SB6346-20260206-1330.csv \
  --out /Users/sayhiben/dev/legislature-tools/testifier_audit/out

# Or run the full unified workflow in one command (recommended)
./scripts/report/run_unified_report.sh \
  /Users/sayhiben/dev/legislature-tools/data/raw/SB6346-20260206-1330.csv \
  /Users/sayhiben/dev/legislature-tools/data/raw/20260202_VRDB_Extract.txt
```

## PostgreSQL hydration workflow
```bash
cd /Users/sayhiben/dev/legislature-tools/testifier_audit
docker compose up -d postgres
export TESTIFIER_AUDIT_DB_URL="postgresql://legislature:legislature@localhost:55432/legislature"

# Import legislature submission CSV into normalized Postgres table
./scripts/db/import_submissions.sh /Users/sayhiben/dev/legislature-tools/data/raw/SB6346-20260206-1330.csv

# Switch pipeline input to Postgres in config:
# input:
#   mode: "postgres"
#   db_url: "postgresql://legislature:legislature@localhost:55432/legislature"
#   submissions_table: "public_submissions"
#   source_file: "SB6346-20260206-1330.csv"

# Run full analysis hydrated from Postgres (no --csv required in postgres mode)
testifier-audit run-all --out /Users/sayhiben/dev/legislature-tools/testifier_audit/out
```

## Quick start (docker)
```bash
cd /Users/sayhiben/dev/legislature-tools/testifier_audit
docker build -t testifier-audit .
docker run --rm -it \
  -v "/Users/sayhiben/dev/legislature-tools:/workspace" \
  -w /workspace/testifier_audit \
  testifier-audit \
  run-all --csv /workspace/data/raw/SB6346-20260206-1330.csv --out /workspace/testifier_audit/out
```

## Output layout
- `out/artifacts/`: pass-1 feature tables
- `out/tables/`: per-detector analysis tables
- `out/summary/`: per-detector JSON summaries
- `out/flags/`: optional record-level score/flag tables
- `out/figures/`: generated PNG figures
- `out/report.html`: rendered report

## Notes
- CSV ingest is BOM-safe (`utf-8-sig`).
- Default config is at `configs/default.yaml`.
- Calibration is controlled via `calibration.enabled`, `calibration.mode`, `calibration.iterations`, and `calibration.support_alpha`.
- Detector significance policy is configurable via `calibration.significance_policy` (`parametric_fdr`, `permutation_fdr`, `either_fdr`).
- Bucketed duplicate/sortedness analysis is controlled via `windows.analysis_bucket_minutes` (default: `1,5,15,30,60,120`).
- Periodicity calibration is controlled via `periodicity.*` (lags/period range/top peaks/iterations/FDR alpha).
- Optional rarity enrichment is controlled via `rarity.enabled`, `rarity.first_name_frequency_path`, and `rarity.last_name_frequency_path`.
- `prepare-rarity-baselines` writes canonical lookup files with columns `name,count,probability` and can update config rarity paths.
- Baseline prep supports source profiles (`ssa_first`, `census_last`, `generic`) and optional explicit column overrides.
- Relative config paths are resolved from the config file directory for portability.
- Input hydration is controlled by `input.mode` (`csv` or `postgres`), `input.db_url`, `input.submissions_table`, and optional `input.source_file`.
- Submissions import command: `testifier-audit import-submissions --csv <path-to-csv>` (normalized upsert into Postgres).
- VRDB import command: `testifier-audit import-vrdb --extract <path-to-vrdb-txt>` (uses upsert on `voter_key`).
- Voter registry matching is controlled via `voter_registry.enabled`, `voter_registry.db_url`, `voter_registry.table_name`, `voter_registry.active_only`, and `voter_registry.match_bucket_minutes`.
- Voter matching outputs include `voter_registry_match__match_overview`, `voter_registry_match__match_by_position`, `voter_registry_match__match_by_bucket`, and figure `voter_registry_match_rates`.
- `./scripts/report/run_unified_report.sh` keeps submissions + voter-registry analyses on one report page (`out/report.html`) by importing both sources and running one consolidated `run-all`.
- Multivariate anomaly detector is controlled via `multivariate_anomaly.*` and outputs `multivariate_anomalies__bucket_anomaly_scores`, `multivariate_anomalies__top_bucket_anomalies`, and figure `multivariate_anomaly_scores`.
- Organization blank-rate outputs include `org_anomalies__organization_blank_rate_by_bucket`, `org_anomalies__organization_blank_rate_by_bucket_position`, and figure `organization_blank_rates`.
- Nickname mappings are in `configs/nicknames.csv`; source and regeneration details are in `configs/nicknames.SOURCE.md` and `scripts/data/update_nicknames.py`.
