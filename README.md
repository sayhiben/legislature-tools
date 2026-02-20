# Legislature Tools

Utilities for ingesting and analyzing public Washington State Legislature participation data.

## Repository layout
- `testifier_audit/`: Python application for anomaly analysis and report generation.
- `data/raw/`: local raw source files (ignored by git).
- `reports/`: cached rendered reports published to GitHub Pages.
- `output/`: local working artifacts/screenshots (not committed).
- `anomaly-detection-guidance.md`: detector and statistical guidance.
- `initial-implementation-plan.txt`: implementation plan notes.

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
