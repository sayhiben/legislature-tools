# Legislature Tools

Utilities for ingesting and analyzing public Washington State Legislature participation data.

## Repository Structure
- `testifier_audit/`: Python app for anomaly analysis and report generation.
- `data/raw/`: local raw source files (git-ignored).
- `data/metadata/`: hearing metadata sidecars (`*.hearing.yaml`).
- `reports/`: cached rendered reports published via GitHub Pages.
- `output/`: local working artifacts and screenshots (not committed).
- `docs/anomaly-detection-guidance.md`: methodology notes.
- `AGENTS.md`: repository development guidance for humans and AI/code agents.

## Development Posture
- This project is intentionally pre-production.
- Prioritize correctness, interpretability, and iteration speed.
- Do not add feature flags, rollout gates, or backward-compatibility scaffolding unless explicitly requested.

## Quick Start
From repo root:

```bash
python -m pip install -e "./testifier_audit[dev]"

./testifier_audit/scripts/report/run_unified_report.sh \
  ./data/raw/SB6346-20260206-1330.csv \
  ./data/raw/20260202_VRDB_Extract.txt

# Optional hearing sidecar
./testifier_audit/scripts/report/run_unified_report.sh \
  ./data/raw/SB6346-20260206-1330.csv \
  ./data/raw/20260202_VRDB_Extract.txt \
  ./data/metadata/SB6346-20260206-1330.hearing.yaml
```

## Local CI and Publishing
From repo root:

```bash
./testifier_audit/scripts/ci/lint.sh
./testifier_audit/scripts/ci/test.sh
./testifier_audit/scripts/ci/run.sh
```

From repo root:

```bash
python ./testifier_audit/scripts/report/build_reports_index.py
python ./testifier_audit/scripts/report/build_global_baselines.py
```

GitHub Pages is published from `reports/` by `.github/workflows/pages.yml`.

## Additional Documentation
- App setup, workflow, CLI, and config: `./testifier_audit/README.md`
- Development contracts and guardrails: `./AGENTS.md`
