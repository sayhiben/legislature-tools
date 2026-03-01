from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = PROJECT_ROOT.parent
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(SRC_ROOT))

from testifier_audit.backtests.duplicate_low_power_calibration import (  # noqa: E402
    build_calibration_report_markdown,
    default_scenarios,
    default_targets,
    run_duplicate_low_power_calibration,
)

LOGGER = logging.getLogger("dup021.calibration")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=REPO_ROOT / "output" / "dup021",
        help="Directory for calibration artifacts.",
    )
    parser.add_argument(
        "--scenario-replicates",
        type=int,
        default=24,
        help="Replicates per scenario.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=6346,
        help="Global deterministic seed.",
    )
    parser.add_argument(
        "--bucket-minutes",
        type=int,
        default=30,
        help="Bucket size for bucket-family calibration.",
    )
    parser.add_argument(
        "--scope-draws",
        type=int,
        default=256,
        help="Scope null Monte Carlo draws.",
    )
    parser.add_argument(
        "--bucket-draws",
        type=int,
        default=128,
        help="Bucket null Monte Carlo draws.",
    )
    parser.add_argument(
        "--position-permutations",
        type=int,
        default=400,
        help="Position permutation draws.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level.",
    )
    return parser.parse_args()


def run() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    scenarios = default_scenarios()
    targets = default_targets()
    artifacts = run_duplicate_low_power_calibration(
        scenarios=scenarios,
        scenario_replicates=int(max(int(args.scenario_replicates), 1)),
        seed=int(args.seed),
        bucket_minutes=int(max(int(args.bucket_minutes), 1)),
        scope_draws=int(max(int(args.scope_draws), 16)),
        bucket_draws=int(max(int(args.bucket_draws), 16)),
        position_permutations=int(max(int(args.position_permutations), 32)),
    )

    now = datetime.now(tz=UTC)
    report_markdown = build_calibration_report_markdown(
        artifacts=artifacts,
        scenarios=scenarios,
        targets=targets,
    )

    case_csv = out_dir / "duplicate_low_power_calibration_cases.csv"
    bucket_csv = out_dir / "duplicate_low_power_calibration_bucket_details.csv"
    grid_csv = out_dir / "duplicate_low_power_calibration_threshold_grid.csv"
    summary_json = out_dir / "duplicate_low_power_calibration_summary.json"
    report_md = out_dir / "duplicate_low_power_calibration_report.md"

    artifacts.case_summary.to_csv(case_csv, index=False)
    artifacts.bucket_details.to_csv(bucket_csv, index=False)
    artifacts.threshold_grid.to_csv(grid_csv, index=False)
    report_md.write_text(report_markdown, encoding="utf-8")

    payload = {
        "generated_at": now.isoformat(),
        "seed": int(args.seed),
        "scenario_replicates": int(args.scenario_replicates),
        "bucket_minutes": int(args.bucket_minutes),
        "scope_draws": int(args.scope_draws),
        "bucket_draws": int(args.bucket_draws),
        "position_permutations": int(args.position_permutations),
        "recommendations": asdict(artifacts.recommendations),
        "benchmark_summary": artifacts.benchmark_summary,
        "artifacts": {
            "case_csv": str(case_csv),
            "bucket_csv": str(bucket_csv),
            "threshold_grid_csv": str(grid_csv),
            "report_markdown": str(report_md),
            "summary_json": str(summary_json),
        },
    }
    summary_json.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    LOGGER.info("Wrote duplicate low-power case metrics: %s", case_csv)
    LOGGER.info("Wrote duplicate low-power bucket details: %s", bucket_csv)
    LOGGER.info("Wrote duplicate low-power threshold grid: %s", grid_csv)
    LOGGER.info("Wrote duplicate low-power report: %s", report_md)
    LOGGER.info("Wrote duplicate low-power summary JSON: %s", summary_json)
    LOGGER.info("Recommended thresholds: %s", payload["recommendations"])
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
