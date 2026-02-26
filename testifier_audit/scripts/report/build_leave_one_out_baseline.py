#!/usr/bin/env python3
"""Build a leave-one-out cross-hearing baseline payload for a target report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from testifier_audit.report.global_baselines import (
    LEAVE_ONE_OUT_BASELINE_FILENAME,
    build_leave_one_out_baseline_from_reports_dir,
)


def project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    root = project_root()
    parser = argparse.ArgumentParser(
        description="Build leave-one-out baseline comparators for one report."
    )
    parser.add_argument(
        "--reports-dir",
        default=str(root / "reports"),
        help="Root reports directory containing report subdirectories.",
    )
    parser.add_argument(
        "--report-id",
        required=True,
        help="Target report directory name (for example: SB1234-20260224-1330).",
    )
    parser.add_argument(
        "--exclude-report-id",
        action="append",
        default=[],
        help="Additional report_id to exclude from the comparison corpus. Repeatable.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help=(
            "Optional output JSON path. Defaults to "
            "reports/<report-id>/summary/cross_hearing_baseline_loo.json."
        ),
    )
    return parser.parse_args(argv)


def _default_output_path(*, reports_dir: Path, report_id: str) -> Path:
    return reports_dir / report_id / "summary" / LEAVE_ONE_OUT_BASELINE_FILENAME


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    reports_dir = Path(args.reports_dir).resolve()
    report_id = str(args.report_id).strip()
    output_path = (
        Path(str(args.output)).resolve()
        if args.output
        else _default_output_path(reports_dir=reports_dir, report_id=report_id).resolve()
    )

    payload = build_leave_one_out_baseline_from_reports_dir(
        reports_dir=reports_dir,
        target_report_id=report_id,
        excluded_report_ids=list(args.exclude_report_id or []),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    availability = "available" if bool(payload.get("available")) else "unavailable"
    reason = str(payload.get("reason") or "").strip()
    if reason:
        availability = f"{availability}:{reason}"
    comparison_count = int(payload.get("report_count") or 0)
    print(
        f"Wrote {output_path} "
        f"(target={report_id} status={availability} comparisons={comparison_count})"
    )


if __name__ == "__main__":
    main()
