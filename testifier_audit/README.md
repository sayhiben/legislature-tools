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
  - Organization concentration patterns
  - Composite ranking of suspicious windows with evidence-bundle exports
- HTML reporting with detector summaries, artifact counts, table previews, and figures
- Detector overlay figures (`counts_with_anomalies`, `pro_rate_with_anomalies`)
- Calibration null-distribution figures (`bursts_null_distribution`, `swing_null_distribution`)
- Periodicity figures (`periodicity_autocorr`, `periodicity_spectrum`, `periodicity_clockface`)
- Dockerized runtime with an internal virtualenv at `/opt/venv`

## Quick start (local)
```bash
cd /Users/sayhiben/dev/legislature-tools/testifier_audit
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]

# Build profile artifacts and figures
testifier-audit profile \
  --csv /Users/sayhiben/dev/legislature-tools/SB6346-20260206-1330.csv \
  --out /Users/sayhiben/dev/legislature-tools/testifier_audit/out

# Run detectors using saved profile artifacts
testifier-audit detect \
  --csv /Users/sayhiben/dev/legislature-tools/SB6346-20260206-1330.csv \
  --out /Users/sayhiben/dev/legislature-tools/testifier_audit/out

# Rebuild complete outputs
testifier-audit run-all \
  --csv /Users/sayhiben/dev/legislature-tools/SB6346-20260206-1330.csv \
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
```

## Quick start (docker)
```bash
cd /Users/sayhiben/dev/legislature-tools/testifier_audit
docker build -t testifier-audit .
docker run --rm -it \
  -v "/Users/sayhiben/dev/legislature-tools:/workspace" \
  -w /workspace/testifier_audit \
  testifier-audit \
  run-all --csv /workspace/SB6346-20260206-1330.csv --out /workspace/testifier_audit/out
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
- Periodicity calibration is controlled via `periodicity.*` (lags/period range/top peaks/iterations/FDR alpha).
- Optional rarity enrichment is controlled via `rarity.enabled`, `rarity.first_name_frequency_path`, and `rarity.last_name_frequency_path`.
- `prepare-rarity-baselines` writes canonical lookup files with columns `name,count,probability` and can update config rarity paths.
- Baseline prep supports source profiles (`ssa_first`, `census_last`, `generic`) and optional explicit column overrides.
- Relative config paths are resolved from the config file directory for portability.
