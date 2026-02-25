from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(SRC_ROOT))

from testifier_audit.config import AppConfig, load_config  # noqa: E402
from testifier_audit.detectors.registry import default_detectors  # noqa: E402
from testifier_audit.names.collision_baseline import (  # noqa: E402
    histogram_from_name_counts,
    simulate_collision_null_from_histogram,
)
from testifier_audit.pipeline.pass1_profile import prepare_base_dataframe  # noqa: E402


@dataclass(frozen=True)
class SyntheticScenario:
    scenario_id: str
    n_rows: int
    position_probabilities: dict[str, float]
    n_names: int
    concentration: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=PROJECT_ROOT / "configs" / "default.yaml",
        help="Configuration file used for detector defaults.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=PROJECT_ROOT.parent / "output" / "calibration",
        help="Directory for calibration artifacts.",
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        action="append",
        default=[],
        help="Optional additional historical sign-in CSV path (repeatable).",
    )
    parser.add_argument(
        "--input-glob",
        type=str,
        action="append",
        default=[],
        help="Optional CSV glob pattern for historical inputs (repeatable).",
    )
    parser.add_argument(
        "--max-historical-csvs",
        type=int,
        default=0,
        help="Optional cap for historical CSV count (0 keeps all discovered files).",
    )
    parser.add_argument(
        "--synthetic-replicates",
        type=int,
        default=120,
        help="Replicates per synthetic scenario.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="RNG seed for deterministic corpus generation.",
    )
    parser.add_argument(
        "--position-interval-draws",
        type=int,
        default=0,
        help="Optional override for name_analysis.position_interval_draws (0 keeps config default).",
    )
    parser.add_argument(
        "--coverage-min",
        type=float,
        default=0.93,
        help="Minimum acceptable overall empirical coverage for eligible rows.",
    )
    parser.add_argument(
        "--coverage-max",
        type=float,
        default=0.97,
        help="Maximum acceptable overall empirical coverage for eligible rows.",
    )
    parser.add_argument(
        "--subgroup-coverage-min",
        type=float,
        default=0.90,
        help="Minimum acceptable subgroup empirical coverage for powered subgroups.",
    )
    parser.add_argument(
        "--subgroup-coverage-max",
        type=float,
        default=0.99,
        help="Maximum acceptable subgroup empirical coverage for powered subgroups.",
    )
    parser.add_argument(
        "--subgroup-min-evaluable",
        type=int,
        default=30,
        help="Minimum eligible rows required to evaluate subgroup coverage thresholds.",
    )
    parser.add_argument(
        "--fail-on-thresholds",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Exit non-zero when coverage gates fail.",
    )
    return parser.parse_args()


def _synthetic_scenarios() -> tuple[SyntheticScenario, ...]:
    return (
        SyntheticScenario(
            scenario_id="near_uniform",
            n_rows=320,
            position_probabilities={"Pro": 0.50, "Con": 0.45, "Unknown": 0.05},
            n_names=260,
            concentration="uniform",
        ),
        SyntheticScenario(
            scenario_id="high_collision_concentration",
            n_rows=320,
            position_probabilities={"Pro": 0.45, "Con": 0.45, "Unknown": 0.10},
            n_names=90,
            concentration="high_collision",
        ),
        SyntheticScenario(
            scenario_id="extreme_position_imbalance",
            n_rows=320,
            position_probabilities={"Pro": 0.90, "Con": 0.08, "Unknown": 0.02},
            n_names=180,
            concentration="moderate",
        ),
        SyntheticScenario(
            scenario_id="sparse_positions",
            n_rows=120,
            position_probabilities={"Pro": 0.70, "Con": 0.22, "Unknown": 0.08},
            n_names=120,
            concentration="moderate",
        ),
        SyntheticScenario(
            scenario_id="unknown_prevalence",
            n_rows=260,
            position_probabilities={"Pro": 0.20, "Con": 0.20, "Unknown": 0.60},
            n_names=180,
            concentration="moderate",
        ),
    )


def _name_probabilities(*, n_names: int, concentration: str) -> np.ndarray:
    if n_names <= 0:
        return np.asarray([], dtype=float)
    if concentration == "uniform":
        return np.full(n_names, 1.0 / float(n_names), dtype=float)
    if concentration == "high_collision":
        weights = np.ones(n_names, dtype=float)
        head = min(6, n_names)
        weights[:head] = np.asarray([36, 24, 14, 8, 5, 3], dtype=float)[:head]
        return weights / float(weights.sum())
    weights = np.ones(n_names, dtype=float)
    head = min(5, n_names)
    weights[:head] = np.asarray([8, 6, 4, 3, 2], dtype=float)[:head]
    return weights / float(weights.sum())


def _sample_synthetic_positions(
    *,
    scenario: SyntheticScenario,
    rng: np.random.Generator,
) -> np.ndarray:
    n_rows = int(max(int(scenario.n_rows), 0))
    if n_rows <= 0:
        return np.asarray([], dtype=object)
    position_labels = list(scenario.position_probabilities.keys())
    position_probs = np.asarray(list(scenario.position_probabilities.values()), dtype=float)
    position_probs = position_probs / float(position_probs.sum())
    return rng.choice(position_labels, size=n_rows, replace=True, p=position_probs).astype(object)


def _build_synthetic_frame(
    *,
    scenario: SyntheticScenario,
    replicate_index: int,
    positions: np.ndarray,
    rng: np.random.Generator,
) -> pd.DataFrame:
    n_rows = int(positions.size)
    if n_rows <= 0:
        return pd.DataFrame()
    n_names = int(max(int(scenario.n_names), 1))
    name_pool = np.asarray(
        [f"SYNL{idx:04d}|SYNF{idx:04d}" for idx in range(n_names)],
        dtype=object,
    )
    name_probs = _name_probabilities(n_names=n_names, concentration=scenario.concentration)
    names = rng.choice(name_pool, size=n_rows, replace=True, p=name_probs)

    base_timestamp = pd.Timestamp("2026-01-01 00:00:00") + pd.Timedelta(days=int(replicate_index))
    timestamps = [base_timestamp + pd.Timedelta(minutes=offset) for offset in range(n_rows)]
    frame = pd.DataFrame(
        {
            "id": np.arange(1, n_rows + 1, dtype=int),
            "canonical_name": names.astype(str),
            "name_display": np.char.replace(names.astype(str), "|", ", ").tolist(),
            "position_normalized": positions.astype(str),
            "timestamp": pd.to_datetime(timestamps, errors="coerce"),
            "minute_bucket": pd.to_datetime(timestamps, errors="coerce"),
            "is_person_name": True,
        }
    )
    return frame


def _iter_historical_paths(args: argparse.Namespace) -> list[Path]:
    discovered: set[Path] = set()
    for candidate in args.input_csv:
        discovered.add(Path(candidate).resolve())
    for pattern in args.input_glob:
        for matched in sorted(Path(PROJECT_ROOT.parent).glob(pattern)):
            if matched.is_file():
                discovered.add(matched.resolve())
    historical = sorted(path for path in discovered if path.suffix.lower() == ".csv" and path.exists())
    limit = int(max(int(args.max_historical_csvs), 0))
    if limit > 0:
        return historical[:limit]
    return historical


def _load_historical_frame(csv_path: Path, config: AppConfig) -> pd.DataFrame:
    run_config = config.model_copy(deep=True)
    run_config.input.mode = "csv"
    frame = prepare_base_dataframe(csv_path=csv_path, config=run_config)
    if "id" not in frame.columns:
        frame["id"] = np.arange(1, len(frame) + 1, dtype=int)
    return frame


def _build_duplicates_detector(config: AppConfig):
    detectors = default_detectors(config)
    for detector in detectors:
        if detector.name == "duplicates_exact":
            return detector
    raise RuntimeError("duplicates_exact detector is not configured.")


def _coverage_or_nan(values: pd.Series) -> float:
    if values.empty:
        return float("nan")
    return float(values.mean())


def main() -> None:
    args = parse_args()
    config = load_config(Path(args.config).resolve())
    config.name_analysis.collision_uncertainty_mode = "analytic_only"
    config.windows.analysis_bucket_minutes = [30]
    config.windows.scan_window_minutes = [30]
    config.windows.swing_window_minutes = 30
    config.name_analysis.position_permutation_draws = min(
        int(config.name_analysis.position_permutation_draws),
        250,
    )
    config.name_analysis.temporal_permutation_draws = min(
        int(config.name_analysis.temporal_permutation_draws),
        250,
    )
    if int(args.position_interval_draws) > 0:
        config.name_analysis.position_interval_draws = int(args.position_interval_draws)

    detector = _build_duplicates_detector(config)
    rng = np.random.default_rng(int(args.seed))
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    evaluation_rows: list[dict[str, object]] = []
    case_count = 0

    for historical_csv in _iter_historical_paths(args):
        frame = _load_historical_frame(historical_csv, config=config)
        result = detector.run(df=frame, features={})
        metrics = result.tables.get("position_duplicate_metrics", pd.DataFrame())
        summary = result.summary
        for row in metrics.itertuples(index=False):
            lower = float(getattr(row, "expected_duplicate_row_rate_p05", 0.0))
            upper = float(getattr(row, "expected_duplicate_row_rate_p95", 0.0))
            observed = float(getattr(row, "duplicate_row_rate", 0.0))
            evaluable = bool(summary.get("position_claim_eligible", False)) and (
                str(getattr(row, "inference_status", "")) == "tested"
            )
            in_interval = bool(lower <= observed <= upper) if evaluable else False
            evaluation_rows.append(
                {
                    "source_type": "historical",
                    "source_id": historical_csv.name,
                    "case_index": case_count,
                    "position_normalized": str(getattr(row, "position_normalized", "Unknown")),
                    "n_rows": int(getattr(row, "n_rows", 0)),
                    "duplicate_rows": int(getattr(row, "duplicate_rows", 0)),
                    "duplicate_row_rate": observed,
                    "expected_duplicate_row_rate_p05": lower,
                    "expected_duplicate_row_rate_p95": upper,
                    "interval_method_id": str(getattr(row, "interval_method_id", "")),
                    "interval_draws_effective": int(getattr(row, "interval_draws_effective", 0)),
                    "is_low_power": bool(getattr(row, "is_low_power", True)),
                    "inference_status": str(getattr(row, "inference_status", "descriptive_only")),
                    "position_claim_eligible": bool(summary.get("position_claim_eligible", False)),
                    "position_claim_reason": str(summary.get("position_claim_reason", "")),
                    "evaluable": bool(evaluable),
                    "in_interval": bool(in_interval),
                }
            )
        case_count += 1

    synthetic_replicates = int(max(int(args.synthetic_replicates), 0))
    for scenario in _synthetic_scenarios():
        for replicate_idx in range(synthetic_replicates):
            positions = _sample_synthetic_positions(scenario=scenario, rng=rng)
            fit_frame = _build_synthetic_frame(
                scenario=scenario,
                replicate_index=replicate_idx,
                positions=positions,
                rng=rng,
            )
            result = detector.run(df=fit_frame, features={})
            metrics = result.tables.get("position_duplicate_metrics", pd.DataFrame())
            summary = result.summary
            fit_counts = fit_frame["canonical_name"].astype(str).value_counts(dropna=False)
            fit_histogram = histogram_from_name_counts(fit_counts)
            for row in metrics.itertuples(index=False):
                lower = float(getattr(row, "expected_duplicate_row_rate_p05", 0.0))
                upper = float(getattr(row, "expected_duplicate_row_rate_p95", 0.0))
                n_rows = int(getattr(row, "n_rows", 0))
                holdout = simulate_collision_null_from_histogram(
                    n_rows=n_rows,
                    histogram=fit_histogram,
                    draws=1,
                    rng=rng,
                    baseline_model="multinomial",
                    max_draws=1,
                )
                observed_rows = (
                    int(pd.to_numeric(holdout["repeated_group_rows"], errors="coerce").fillna(0).iloc[0])
                    if not holdout.empty
                    else 0
                )
                observed_rate = (float(observed_rows) / float(n_rows)) if n_rows > 0 else 0.0
                position = str(getattr(row, "position_normalized", "Unknown"))
                evaluable = bool(summary.get("position_claim_eligible", False)) and (
                    str(getattr(row, "inference_status", "")) == "tested"
                )
                in_interval = bool(lower <= observed_rate <= upper) if evaluable else False
                evaluation_rows.append(
                    {
                        "source_type": "synthetic",
                        "source_id": str(scenario.scenario_id),
                        "case_index": case_count,
                        "position_normalized": position,
                        "n_rows": n_rows,
                        "duplicate_rows": int(observed_rows),
                        "duplicate_row_rate": float(observed_rate),
                        "expected_duplicate_row_rate_p05": lower,
                        "expected_duplicate_row_rate_p95": upper,
                        "interval_method_id": str(getattr(row, "interval_method_id", "")),
                        "interval_draws_effective": int(
                            getattr(row, "interval_draws_effective", 0)
                        ),
                        "is_low_power": bool(getattr(row, "is_low_power", True)),
                        "inference_status": str(
                            getattr(row, "inference_status", "descriptive_only")
                        ),
                        "position_claim_eligible": bool(
                            summary.get("position_claim_eligible", False)
                        ),
                        "position_claim_reason": str(summary.get("position_claim_reason", "")),
                        "evaluable": bool(evaluable),
                        "in_interval": bool(in_interval),
                    }
                )
            case_count += 1

    evaluations = pd.DataFrame(evaluation_rows)
    evaluations.to_csv(out_dir / "position_interval_evaluations.csv", index=False)

    evaluable = evaluations[evaluations["evaluable"].astype(bool)].copy()
    overall_coverage = _coverage_or_nan(evaluable["in_interval"].astype(float))
    overall_n = int(len(evaluable))
    coverage_min = float(args.coverage_min)
    coverage_max = float(args.coverage_max)
    overall_pass = bool(
        overall_n > 0 and math.isfinite(overall_coverage) and coverage_min <= overall_coverage <= coverage_max
    )

    subgroup_rows: list[dict[str, object]] = []
    subgroup_min = int(max(int(args.subgroup_min_evaluable), 1))
    subgroup_min_cov = float(args.subgroup_coverage_min)
    subgroup_max_cov = float(args.subgroup_coverage_max)
    subgroup_pass = True
    if not evaluable.empty:
        grouped = evaluable.groupby("position_normalized", dropna=False)
        for position, frame in grouped:
            n = int(len(frame))
            coverage = _coverage_or_nan(frame["in_interval"].astype(float))
            powered = n >= subgroup_min
            in_band = bool(
                powered
                and math.isfinite(coverage)
                and subgroup_min_cov <= coverage <= subgroup_max_cov
            )
            if powered and not in_band:
                subgroup_pass = False
            subgroup_rows.append(
                {
                    "position_normalized": str(position),
                    "n_evaluable": n,
                    "coverage": coverage,
                    "powered": powered,
                    "pass_band": in_band if powered else None,
                }
            )

    subgroup = pd.DataFrame(subgroup_rows).sort_values("position_normalized")
    subgroup.to_csv(out_dir / "coverage_by_position.csv", index=False)

    unique_method_ids = sorted(set(evaluable.get("interval_method_id", pd.Series(dtype=str)).astype(str)))
    method_id_valid = (
        len(unique_method_ids) == 1
        and unique_method_ids[0] == "position_duplicate_interval_multinomial_mc_v1"
    )
    thresholds_pass = bool(overall_pass and subgroup_pass and method_id_valid)
    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "config_path": str(Path(args.config).resolve()),
        "position_interval_nominal": float(config.name_analysis.position_interval_nominal),
        "position_interval_draws": int(config.name_analysis.position_interval_draws),
        "position_claim_min_rows_per_position": int(
            config.name_analysis.position_claim_min_rows_per_position
        ),
        "coverage_thresholds": {
            "overall_min": coverage_min,
            "overall_max": coverage_max,
            "subgroup_min": subgroup_min_cov,
            "subgroup_max": subgroup_max_cov,
            "subgroup_min_evaluable": subgroup_min,
        },
        "corpus": {
            "n_cases": int(case_count),
            "n_rows_total": int(len(evaluations)),
            "n_evaluable_rows": overall_n,
            "historical_inputs": sorted(
                {str(value) for value in evaluations[evaluations["source_type"] == "historical"]["source_id"]}
            ),
            "synthetic_scenarios": sorted(
                {str(value) for value in evaluations[evaluations["source_type"] == "synthetic"]["source_id"]}
            ),
        },
        "coverage": {
            "overall": overall_coverage,
            "overall_pass": overall_pass,
            "subgroup_pass": subgroup_pass,
            "method_id_valid": method_id_valid,
            "pass": thresholds_pass,
        },
        "method_ids_evaluable": unique_method_ids,
    }
    (out_dir / "calibration_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    if bool(args.fail_on_thresholds) and not thresholds_pass:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
