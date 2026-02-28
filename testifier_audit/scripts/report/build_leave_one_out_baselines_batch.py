#!/usr/bin/env python3
"""Build leave-one-out cross-hearing baseline payloads for one or more reports."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Sequence

from testifier_audit.report.global_baselines import (
    write_leave_one_out_baselines_from_reports_dir,
)


def project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _read_report_ids_from_file(path: Path) -> list[str]:
    report_ids: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        candidate = str(line).strip()
        if not candidate or candidate.startswith("#"):
            continue
        report_ids.append(candidate)
    return report_ids


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    root = project_root()
    parser = argparse.ArgumentParser(
        description="Build leave-one-out baseline comparators for many reports in one process."
    )
    parser.add_argument(
        "--reports-dir",
        default=str(root / "reports"),
        help="Root reports directory containing report subdirectories.",
    )
    parser.add_argument(
        "--report-id",
        action="append",
        default=[],
        help="Target report directory name. Repeatable.",
    )
    parser.add_argument(
        "--report-id-file",
        default=None,
        help="Optional file containing newline-delimited target report IDs.",
    )
    parser.add_argument(
        "--all-reports",
        action="store_true",
        help="Build leave-one-out payloads for every report found under --reports-dir.",
    )
    parser.add_argument(
        "--exclude-report-id",
        action="append",
        default=[],
        help="Additional report_id to exclude from each comparison corpus. Repeatable.",
    )
    parser.add_argument(
        "--cohort-strategy",
        default="hierarchical",
        choices=["hierarchical"],
        help="Cohort strategy for cohort LOO channel (default: hierarchical).",
    )
    parser.add_argument(
        "--failure-output",
        default=None,
        help="Optional path for a JSON failure list (report_id + reason).",
    )
    return parser.parse_args(argv)


def _log(level: str, message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} [loo-batch][{level}] {message}")


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    reports_dir = Path(args.reports_dir).resolve()
    ids_from_args = [str(value).strip() for value in list(args.report_id or []) if str(value).strip()]

    ids_from_file: list[str] = []
    if args.report_id_file:
        ids_from_file = _read_report_ids_from_file(Path(str(args.report_id_file)).resolve())

    if bool(args.all_reports):
        target_report_ids: list[str] | None = None
    else:
        seen: set[str] = set()
        ordered: list[str] = []
        for report_id in [*ids_from_args, *ids_from_file]:
            if report_id in seen:
                continue
            seen.add(report_id)
            ordered.append(report_id)
        target_report_ids = ordered
        if not target_report_ids:
            raise SystemExit("No target reports specified. Provide --report-id, --report-id-file, or --all-reports.")

    written_paths, failures = write_leave_one_out_baselines_from_reports_dir(
        reports_dir=reports_dir,
        target_report_ids=target_report_ids,
        excluded_report_ids=list(args.exclude_report_id or []),
        cohort_strategy=str(args.cohort_strategy or "hierarchical"),
    )

    for path in written_paths:
        _log("info", f"Wrote {path}")

    if args.failure_output:
        failure_output = Path(str(args.failure_output)).resolve()
        failure_output.parent.mkdir(parents=True, exist_ok=True)
        failure_output.write_text(
            json.dumps(failures, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    _log(
        "info",
        f"Batch LOO complete: written={len(written_paths)} failures={len(failures)} "
        f"reports_dir={reports_dir}",
    )

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
