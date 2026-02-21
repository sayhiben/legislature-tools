#!/usr/bin/env python3
"""Build cross-hearing baseline summaries from the reports corpus."""

from __future__ import annotations

from pathlib import Path

from testifier_audit.report.global_baselines import (
    GLOBAL_BASELINES_FILENAME,
    build_global_baselines_from_reports_dir,
    write_global_baselines,
)


def project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def main() -> None:
    root = project_root()
    reports_dir = root / "reports"
    payload = build_global_baselines_from_reports_dir(reports_dir)
    output_path = write_global_baselines(
        reports_dir=reports_dir,
        payload=payload,
        output_filename=GLOBAL_BASELINES_FILENAME,
    )
    report_count = int(payload.get("report_count") or 0)
    print(f"Wrote {output_path} ({report_count} report(s) indexed)")


if __name__ == "__main__":
    main()
