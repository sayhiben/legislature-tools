from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = PROJECT_ROOT.parent
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(SRC_ROOT))

from testifier_audit.backtests.vrdb_collision_backtest import (  # noqa: E402
    BaselineScenario,
    SyntheticScenario,
    case_stats_from_frame,
    filter_probability_rows,
    load_backoff_rows,
    load_probability_rows_for_scenarios,
    safe_numeric_median,
    select_historical_case_families,
    slice_rows_for_case,
    split_case_ids,
    stable_seed,
    summarize_case_metrics,
    synthetic_case_frame,
    threshold_feasibility_scan,
    wilson_interval,
)
from testifier_audit.config import AppConfig, load_config  # noqa: E402
from testifier_audit.io.vrdb_collision_null import compute_vrdb_collision_null_for_slices  # noqa: E402
from testifier_audit.pipeline.pass1_profile import prepare_base_dataframe  # noqa: E402

LOGGER = logging.getLogger("dup007.backtest")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=PROJECT_ROOT / "configs" / "voter_registry_enabled.yaml",
        help="Config used to preprocess historical hearings.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=REPO_ROOT / "output" / "dup007",
        help="Directory for backtest artifacts.",
    )
    parser.add_argument(
        "--probability-csv",
        type=Path,
        default=REPO_ROOT / "output" / "dup003" / "vrdb_name_probabilities.csv",
        help="VRDB probability artifact CSV/Parquet.",
    )
    parser.add_argument(
        "--backoff-csv",
        type=Path,
        default=REPO_ROOT / "output" / "dup003" / "vrdb_geo_backoff.csv",
        help="VRDB geography backoff artifact CSV/Parquet.",
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        action="append",
        default=[],
        help="Optional explicit hearing CSV path (repeatable).",
    )
    parser.add_argument(
        "--input-glob",
        type=str,
        action="append",
        default=["data/raw/*.csv"],
        help="Glob(s) for historical hearing CSV discovery relative to repo root.",
    )
    parser.add_argument(
        "--max-historical-csvs",
        type=int,
        default=0,
        help="Optional cap for discovered historical hearings (0 keeps all).",
    )
    parser.add_argument(
        "--min-historical-rows",
        type=int,
        default=250,
        help="Minimum cleaned name rows for a hearing to be considered in backtest families.",
    )
    parser.add_argument(
        "--historical-normal-count",
        type=int,
        default=6,
        help="Number of historical normal controls to include.",
    )
    parser.add_argument(
        "--historical-suspect-count",
        type=int,
        default=3,
        help="Number of historical suspected hearings to include.",
    )
    parser.add_argument(
        "--force-suspect",
        type=str,
        action="append",
        default=["ESSB6346-20260224-0800", "SB6346-20260206-1330"],
        help="Hearing stem to force into suspected family (repeatable).",
    )
    parser.add_argument(
        "--bucket-minutes",
        type=str,
        default="1,5,15,30,60,120,240",
        help="Comma-separated bucket minute options.",
    )
    parser.add_argument(
        "--small-bucket-minutes",
        type=int,
        default=5,
        help="Bucket used for small-bucket alert share reporting.",
    )
    parser.add_argument(
        "--monte-carlo-draws",
        type=int,
        default=256,
        help="Monte Carlo draws per slice.",
    )
    parser.add_argument(
        "--top-name-limit",
        type=int,
        default=25,
        help="Top-name rows retained inside collision-null computation.",
    )
    parser.add_argument(
        "--tail-alpha",
        type=float,
        default=0.01,
        help="Default tail probability alert threshold.",
    )
    parser.add_argument(
        "--calibration-target-fpr",
        type=float,
        default=0.10,
        help="Target calibration false-positive rate used to derive holdout threshold.",
    )
    parser.add_argument(
        "--holdout-fraction",
        type=float,
        default=0.40,
        help="Holdout split fraction for historical families.",
    )
    parser.add_argument(
        "--max-holdout-normal-alert-rate",
        type=float,
        default=0.20,
        help="Maximum acceptable holdout-normal alert rate for threshold-feasibility scan.",
    )
    parser.add_argument(
        "--min-synthetic-injected-alert-rate",
        type=float,
        default=0.80,
        help="Minimum acceptable synthetic-injected alert rate for threshold-feasibility scan.",
    )
    parser.add_argument(
        "--minimum-inferential-holdout-normal-cases",
        type=int,
        default=5,
        help=(
            "Minimum inferential holdout-normal cases required before a scenario is marked "
            "operating-point evaluable."
        ),
    )
    parser.add_argument(
        "--synthetic-replicates",
        type=int,
        default=4,
        help="Replicates per synthetic scenario.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=6346,
        help="Global fixed seed for deterministic split and synthetic generation.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING).",
    )
    return parser.parse_args()


def _parse_bucket_minutes(raw: str) -> list[int]:
    values: list[int] = []
    for token in str(raw or "").split(","):
        text = str(token or "").strip()
        if not text:
            continue
        try:
            minute = int(text)
        except ValueError:
            continue
        if minute > 0:
            values.append(minute)
    deduped = sorted({value for value in values if value > 0})
    return deduped or [5, 15, 60]


def _iter_historical_paths(args: argparse.Namespace) -> list[Path]:
    discovered: set[Path] = set()
    for candidate in args.input_csv:
        path = Path(candidate)
        if path.exists() and path.suffix.lower() == ".csv":
            discovered.add(path.resolve())
    for pattern in args.input_glob:
        for matched in sorted(REPO_ROOT.glob(str(pattern))):
            if matched.is_file() and matched.suffix.lower() == ".csv":
                discovered.add(matched.resolve())

    paths = sorted(discovered)
    limit = int(max(int(args.max_historical_csvs), 0))
    if limit > 0:
        return paths[:limit]
    return paths


def _hearing_metadata_path_for_csv(csv_path: Path) -> Path | None:
    candidate = REPO_ROOT / "data" / "metadata" / f"{csv_path.stem}.hearing.yaml"
    return candidate if candidate.exists() else None


def _load_hearing_frame(csv_path: Path, config: AppConfig) -> pd.DataFrame:
    run_config = config.model_copy(deep=True)
    run_config.input.mode = "csv"
    metadata_path = _hearing_metadata_path_for_csv(csv_path)
    run_config.input.hearing_metadata_path = str(metadata_path) if metadata_path else ""

    frame = prepare_base_dataframe(csv_path=csv_path, config=run_config)
    # Keep both default and proxy normalization columns for sensitivity checks.
    columns = ["full_name_key", "canonical_key_medium", "timestamp"]
    for column in columns:
        if column not in frame.columns:
            frame[column] = "" if column != "timestamp" else pd.NaT
    frame = frame.loc[:, columns].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce", utc=True)
    return frame


def _default_baseline_scenarios() -> tuple[BaselineScenario, ...]:
    return (
        BaselineScenario(
            scenario_id="state_wa_all_default",
            baseline_variant="all_registrants",
            requested_geo_level="state",
            requested_geo_value="WA",
            normalization_mode="default",
        ),
        BaselineScenario(
            scenario_id="state_wa_all_canonical_proxy",
            baseline_variant="all_registrants",
            requested_geo_level="state",
            requested_geo_value="WA",
            normalization_mode="canonical_medium_proxy",
        ),
        BaselineScenario(
            scenario_id="state_wa_active_default",
            baseline_variant="active_only",
            requested_geo_level="state",
            requested_geo_value="WA",
            normalization_mode="default",
        ),
        BaselineScenario(
            scenario_id="county_ki_all_default",
            baseline_variant="all_registrants",
            requested_geo_level="county",
            requested_geo_value="KI",
            normalization_mode="default",
        ),
        BaselineScenario(
            scenario_id="city_ki_seattle_all_default",
            baseline_variant="all_registrants",
            requested_geo_level="city",
            requested_geo_value="KI|SEATTLE",
            normalization_mode="default",
        ),
        BaselineScenario(
            scenario_id="city_ad_benge_all_default",
            baseline_variant="all_registrants",
            requested_geo_level="city",
            requested_geo_value="AD|BENGE",
            normalization_mode="default",
        ),
        BaselineScenario(
            scenario_id="city_ad_missing_all_default",
            baseline_variant="all_registrants",
            requested_geo_level="city",
            requested_geo_value="AD|",
            normalization_mode="default",
        ),
    )


def _default_synthetic_scenarios() -> tuple[SyntheticScenario, ...]:
    return (
        SyntheticScenario(
            scenario_id="synthetic_null_large",
            n_rows=800,
            span_minutes=720,
            injection_fraction=0.0,
            injection_burst_minutes=0,
        ),
        SyntheticScenario(
            scenario_id="synthetic_null_sparse",
            n_rows=140,
            span_minutes=180,
            injection_fraction=0.0,
            injection_burst_minutes=0,
        ),
        SyntheticScenario(
            scenario_id="synthetic_injected_mild",
            n_rows=800,
            span_minutes=720,
            injection_fraction=0.03,
            injection_burst_minutes=15,
        ),
        SyntheticScenario(
            scenario_id="synthetic_injected_strong",
            n_rows=800,
            span_minutes=720,
            injection_fraction=0.10,
            injection_burst_minutes=5,
        ),
    )


def _historical_case_stats(paths: list[Path], config: AppConfig) -> tuple[pd.DataFrame, dict[str, Path]]:
    stats_rows: list[dict[str, object]] = []
    path_by_case_id: dict[str, Path] = {}
    for index, path in enumerate(paths, start=1):
        case_id = path.stem
        try:
            frame = _load_hearing_frame(path, config)
            stats = case_stats_from_frame(case_id=case_id, frame=frame, name_column="full_name_key")
            stats_rows.append(stats)
            path_by_case_id[case_id] = path
            LOGGER.info(
                "historical %s/%s case=%s n_rows=%s pairs_ratio=%.6f",
                index,
                len(paths),
                case_id,
                stats["n_rows"],
                stats["duplicate_pairs_ratio"],
            )
        except Exception as exc:  # pragma: no cover - defensive for local data quality issues
            LOGGER.warning("Skipping historical hearing %s due to error: %s", path, exc)
    return pd.DataFrame(stats_rows), path_by_case_id


def _load_selected_historical_frames(
    *,
    selected_case_ids: list[str],
    path_by_case_id: dict[str, Path],
    config: AppConfig,
) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for case_id in selected_case_ids:
        path = path_by_case_id.get(case_id)
        if path is None:
            continue
        frame = _load_hearing_frame(path, config)
        frames[case_id] = frame
    return frames


def _build_case_manifest(
    *,
    historical_family_map: pd.DataFrame,
    historical_calibration_ids: set[str],
    historical_holdout_ids: set[str],
    synthetic_cases: pd.DataFrame,
    path_by_case_id: dict[str, Path],
) -> pd.DataFrame:
    historical_manifest = historical_family_map.copy()
    if not historical_manifest.empty:
        historical_manifest["split"] = np.where(
            historical_manifest["case_id"].isin(historical_calibration_ids),
            "calibration",
            "holdout",
        )
        historical_manifest["source_kind"] = "historical"
        historical_manifest["source_path"] = historical_manifest["case_id"].map(
            lambda token: str(path_by_case_id.get(str(token), ""))
        )

    pieces = [historical_manifest, synthetic_cases]
    non_empty = [piece for piece in pieces if not piece.empty]
    if not non_empty:
        return pd.DataFrame(columns=["case_id", "family", "split", "source_kind", "source_path"])
    out = pd.concat(non_empty, ignore_index=True)
    out["case_id"] = out["case_id"].astype(str)
    return out.sort_values(["source_kind", "family", "case_id"]).reset_index(drop=True)


def _build_synthetic_cases(
    *,
    synthetic_scenarios: tuple[SyntheticScenario, ...],
    synthetic_replicates: int,
    seed: int,
    probability_rows: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    if probability_rows.empty:
        raise ValueError("Cannot build synthetic cases without state-level probability rows.")

    top_name = (
        probability_rows.sort_values(["probability", "count", "name_key"], ascending=[False, False, True])
        .iloc[0]["name_key"]
    )

    case_rows: list[dict[str, object]] = []
    frame_map: dict[str, pd.DataFrame] = {}
    for scenario in synthetic_scenarios:
        for replicate in range(int(max(synthetic_replicates, 0))):
            case_id = f"{scenario.scenario_id}__r{replicate:02d}"
            case_seed = stable_seed(seed, "synthetic", scenario.scenario_id, replicate)
            start_timestamp = pd.Timestamp("2026-01-01T00:00:00Z") + pd.Timedelta(days=replicate)
            frame = synthetic_case_frame(
                probability_rows=probability_rows,
                n_rows=int(scenario.n_rows),
                case_seed=case_seed,
                start_timestamp=start_timestamp,
                span_minutes=int(scenario.span_minutes),
                injection_name_key=str(top_name),
                injection_fraction=float(scenario.injection_fraction),
                injection_burst_minutes=int(scenario.injection_burst_minutes),
            )
            # Keep proxy normalization path available for scenario parity checks.
            frame["canonical_key_medium"] = frame["full_name_key"].astype(str)
            family = "synthetic_injected" if float(scenario.injection_fraction) > 0.0 else "synthetic_null"
            case_rows.append(
                {
                    "case_id": case_id,
                    "family": family,
                    "split": "synthetic",
                    "source_kind": "synthetic",
                    "source_path": scenario.scenario_id,
                    "synthetic_scenario_id": scenario.scenario_id,
                    "synthetic_replicate": replicate,
                }
            )
            frame_map[case_id] = frame
    return pd.DataFrame(case_rows), frame_map


def _slice_rows_for_scenario(
    *,
    scenario: BaselineScenario,
    case_manifest: pd.DataFrame,
    case_frames: dict[str, pd.DataFrame],
    bucket_minutes: list[int],
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for row in case_manifest.itertuples(index=False):
        case_id = str(row.case_id)
        frame = case_frames.get(case_id)
        if frame is None or frame.empty:
            continue
        name_column = "full_name_key"
        if str(scenario.normalization_mode).strip().lower() == "canonical_medium_proxy":
            name_column = "canonical_key_medium"
        slice_rows = slice_rows_for_case(
            case_id=case_id,
            frame=frame,
            bucket_minutes=bucket_minutes,
            baseline_variant=scenario.baseline_variant,
            requested_geo_level=scenario.requested_geo_level,
            requested_geo_value=scenario.requested_geo_value,
            name_column=name_column,
            name_key_type="full_name_key",
        )
        if not slice_rows.empty:
            rows.append(slice_rows)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def _scenario_probability_subset(
    *,
    probability_rows: pd.DataFrame,
    scenario: BaselineScenario,
) -> pd.DataFrame:
    from testifier_audit.backtests.vrdb_collision_backtest import required_geo_targets

    targets = required_geo_targets(
        requested_geo_level=scenario.requested_geo_level,
        requested_geo_value=scenario.requested_geo_value,
    )
    target_keys = {f"{level}|{value}" for level, value in targets}
    return filter_probability_rows(
        probability_rows=probability_rows,
        baseline_variants={scenario.baseline_variant},
        name_key_type="full_name_key",
        geo_target_keys=target_keys,
    )


def _calibration_thresholds(
    *,
    case_results: pd.DataFrame,
    calibration_target_fpr: float,
    fallback_tail_alpha: float,
) -> dict[str, float]:
    thresholds: dict[str, float] = {}
    for scenario_id, group in case_results.groupby("scenario_id", dropna=False):
        calibration_controls = group[
            (group["family"] == "historical_normal")
            & (group["split"] == "calibration")
            & (group["has_metrics"])
        ]
        if calibration_controls.empty:
            thresholds[str(scenario_id)] = float(fallback_tail_alpha)
            continue
        values = pd.to_numeric(calibration_controls["full_tail_prob_pairs"], errors="coerce").dropna()
        if values.empty:
            thresholds[str(scenario_id)] = float(fallback_tail_alpha)
            continue
        quantile = float(max(min(float(calibration_target_fpr), 0.99), 0.01))
        thresholds[str(scenario_id)] = float(values.quantile(quantile))
    return thresholds


def _scenario_summary(
    *,
    case_results: pd.DataFrame,
    thresholds: dict[str, float],
    default_tail_alpha: float,
    max_holdout_normal_alert_rate: float,
    min_synthetic_injected_alert_rate: float,
    minimum_inferential_holdout_normal_cases: int,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for scenario_id, group in case_results.groupby("scenario_id", dropna=False):
        scenario_token = str(scenario_id)
        threshold = float(thresholds.get(scenario_token, default_tail_alpha))

        working = group.copy()
        working["full_tail_prob_pairs"] = pd.to_numeric(working["full_tail_prob_pairs"], errors="coerce")
        working["full_tail_prob_max_name"] = pd.to_numeric(working["full_tail_prob_max_name"], errors="coerce")
        working["full_pairs_ratio"] = pd.to_numeric(working["full_pairs_ratio"], errors="coerce")
        working["has_metrics"] = working["has_metrics"].astype(bool)
        inferential = working["full_inferential_status"].fillna("").astype(str).eq("inferential")

        def _alert_stats(mask: pd.Series, *, cutoff: float) -> dict[str, float]:
            frame = working[mask & working["has_metrics"]]
            if frame.empty:
                return {
                    "n": 0.0,
                    "alerts": 0.0,
                    "rate": float("nan"),
                    "wilson_lo": float("nan"),
                    "wilson_hi": float("nan"),
                }
            alerts = int((frame["full_tail_prob_pairs"] <= float(cutoff)).sum())
            n_rows = int(len(frame))
            lo, hi = wilson_interval(successes=alerts, trials=n_rows)
            return {
                "n": float(n_rows),
                "alerts": float(alerts),
                "rate": float(alerts / n_rows),
                "wilson_lo": float(lo),
                "wilson_hi": float(hi),
            }

        calibration_normal = (working["family"] == "historical_normal") & (working["split"] == "calibration")
        holdout_normal = (working["family"] == "historical_normal") & (working["split"] == "holdout")
        holdout_suspect = (working["family"] == "historical_suspect") & (working["split"] == "holdout")
        synthetic_null = working["family"] == "synthetic_null"
        synthetic_injected = working["family"] == "synthetic_injected"
        calibration_normal_stats = _alert_stats(calibration_normal, cutoff=threshold)
        holdout_normal_stats = _alert_stats(holdout_normal, cutoff=threshold)
        holdout_suspect_stats = _alert_stats(holdout_suspect, cutoff=threshold)
        synthetic_null_stats = _alert_stats(synthetic_null, cutoff=threshold)
        synthetic_injected_stats = _alert_stats(synthetic_injected, cutoff=threshold)
        calibration_normal_inferential_stats = _alert_stats(calibration_normal & inferential, cutoff=threshold)
        holdout_normal_inferential_stats = _alert_stats(holdout_normal & inferential, cutoff=threshold)
        holdout_suspect_inferential_stats = _alert_stats(holdout_suspect & inferential, cutoff=threshold)
        synthetic_null_inferential_stats = _alert_stats(synthetic_null & inferential, cutoff=threshold)
        synthetic_injected_inferential_stats = _alert_stats(synthetic_injected & inferential, cutoff=threshold)
        default_holdout_normal_stats = _alert_stats(holdout_normal, cutoff=float(default_tail_alpha))
        default_holdout_suspect_stats = _alert_stats(holdout_suspect, cutoff=float(default_tail_alpha))
        feasibility = threshold_feasibility_scan(
            holdout_normal_tail_probs=(
                pd.to_numeric(
                    working.loc[holdout_normal & working["has_metrics"], "full_tail_prob_pairs"],
                    errors="coerce",
                )
                .dropna()
                .to_numpy(dtype=float)
            ),
            synthetic_injected_tail_probs=(
                pd.to_numeric(
                    working.loc[synthetic_injected & working["has_metrics"], "full_tail_prob_pairs"],
                    errors="coerce",
                )
                .dropna()
                .to_numpy(dtype=float)
            ),
            max_holdout_normal_alert_rate=max_holdout_normal_alert_rate,
            min_synthetic_injected_alert_rate=min_synthetic_injected_alert_rate,
            candidate_thresholds=(
                pd.to_numeric(working.loc[working["has_metrics"], "full_tail_prob_pairs"], errors="coerce")
                .dropna()
                .to_numpy(dtype=float)
            ),
        )
        inferential_feasibility = threshold_feasibility_scan(
            holdout_normal_tail_probs=(
                pd.to_numeric(
                    working.loc[holdout_normal & inferential & working["has_metrics"], "full_tail_prob_pairs"],
                    errors="coerce",
                )
                .dropna()
                .to_numpy(dtype=float)
            ),
            synthetic_injected_tail_probs=(
                pd.to_numeric(
                    working.loc[
                        synthetic_injected & inferential & working["has_metrics"],
                        "full_tail_prob_pairs",
                    ],
                    errors="coerce",
                )
                .dropna()
                .to_numpy(dtype=float)
            ),
            max_holdout_normal_alert_rate=max_holdout_normal_alert_rate,
            min_synthetic_injected_alert_rate=min_synthetic_injected_alert_rate,
            candidate_thresholds=(
                pd.to_numeric(
                    working.loc[inferential & working["has_metrics"], "full_tail_prob_pairs"],
                    errors="coerce",
                )
                .dropna()
                .to_numpy(dtype=float)
            ),
        )
        inferential_evaluable = (
            int(holdout_normal_inferential_stats["n"]) >= int(minimum_inferential_holdout_normal_cases)
            and int(synthetic_injected_inferential_stats["n"]) >= 1
        )
        inferential_meets_targets = (
            inferential_evaluable
            and float(holdout_normal_inferential_stats["rate"]) <= float(max_holdout_normal_alert_rate)
            and float(synthetic_injected_inferential_stats["rate"]) >= float(min_synthetic_injected_alert_rate)
        )

        rows.append(
            {
                "scenario_id": scenario_token,
                "baseline_variant": str(working["baseline_variant"].iloc[0]),
                "requested_geo_level": str(working["requested_geo_level"].iloc[0]),
                "requested_geo_value": str(working["requested_geo_value"].iloc[0]),
                "normalization_mode": str(working["normalization_mode"].iloc[0]),
                "calibrated_threshold": threshold,
                "n_cases_with_metrics": int(working["has_metrics"].sum()),
                "calibration_normal_n": int(calibration_normal_stats["n"]),
                "calibration_normal_alerts": int(calibration_normal_stats["alerts"]),
                "calibration_normal_alert_rate": calibration_normal_stats["rate"],
                "calibration_normal_wilson_lo": calibration_normal_stats["wilson_lo"],
                "calibration_normal_wilson_hi": calibration_normal_stats["wilson_hi"],
                "calibration_normal_inferential_n": int(calibration_normal_inferential_stats["n"]),
                "calibration_normal_inferential_alerts": int(calibration_normal_inferential_stats["alerts"]),
                "calibration_normal_inferential_alert_rate": calibration_normal_inferential_stats["rate"],
                "calibration_normal_inferential_wilson_lo": calibration_normal_inferential_stats["wilson_lo"],
                "calibration_normal_inferential_wilson_hi": calibration_normal_inferential_stats["wilson_hi"],
                "holdout_normal_n": int(holdout_normal_stats["n"]),
                "holdout_normal_alerts": int(holdout_normal_stats["alerts"]),
                "holdout_normal_alert_rate": holdout_normal_stats["rate"],
                "holdout_normal_wilson_lo": holdout_normal_stats["wilson_lo"],
                "holdout_normal_wilson_hi": holdout_normal_stats["wilson_hi"],
                "holdout_normal_inferential_n": int(holdout_normal_inferential_stats["n"]),
                "holdout_normal_inferential_alerts": int(holdout_normal_inferential_stats["alerts"]),
                "holdout_normal_inferential_alert_rate": holdout_normal_inferential_stats["rate"],
                "holdout_normal_inferential_wilson_lo": holdout_normal_inferential_stats["wilson_lo"],
                "holdout_normal_inferential_wilson_hi": holdout_normal_inferential_stats["wilson_hi"],
                "holdout_suspect_n": int(holdout_suspect_stats["n"]),
                "holdout_suspect_alerts": int(holdout_suspect_stats["alerts"]),
                "holdout_suspect_alert_rate": holdout_suspect_stats["rate"],
                "holdout_suspect_wilson_lo": holdout_suspect_stats["wilson_lo"],
                "holdout_suspect_wilson_hi": holdout_suspect_stats["wilson_hi"],
                "holdout_suspect_inferential_n": int(holdout_suspect_inferential_stats["n"]),
                "holdout_suspect_inferential_alerts": int(holdout_suspect_inferential_stats["alerts"]),
                "holdout_suspect_inferential_alert_rate": holdout_suspect_inferential_stats["rate"],
                "holdout_suspect_inferential_wilson_lo": holdout_suspect_inferential_stats["wilson_lo"],
                "holdout_suspect_inferential_wilson_hi": holdout_suspect_inferential_stats["wilson_hi"],
                "synthetic_null_n": int(synthetic_null_stats["n"]),
                "synthetic_null_alerts": int(synthetic_null_stats["alerts"]),
                "synthetic_null_alert_rate": synthetic_null_stats["rate"],
                "synthetic_null_wilson_lo": synthetic_null_stats["wilson_lo"],
                "synthetic_null_wilson_hi": synthetic_null_stats["wilson_hi"],
                "synthetic_null_inferential_n": int(synthetic_null_inferential_stats["n"]),
                "synthetic_null_inferential_alerts": int(synthetic_null_inferential_stats["alerts"]),
                "synthetic_null_inferential_alert_rate": synthetic_null_inferential_stats["rate"],
                "synthetic_null_inferential_wilson_lo": synthetic_null_inferential_stats["wilson_lo"],
                "synthetic_null_inferential_wilson_hi": synthetic_null_inferential_stats["wilson_hi"],
                "synthetic_injected_n": int(synthetic_injected_stats["n"]),
                "synthetic_injected_alerts": int(synthetic_injected_stats["alerts"]),
                "synthetic_injected_alert_rate": synthetic_injected_stats["rate"],
                "synthetic_injected_wilson_lo": synthetic_injected_stats["wilson_lo"],
                "synthetic_injected_wilson_hi": synthetic_injected_stats["wilson_hi"],
                "synthetic_injected_inferential_n": int(synthetic_injected_inferential_stats["n"]),
                "synthetic_injected_inferential_alerts": int(synthetic_injected_inferential_stats["alerts"]),
                "synthetic_injected_inferential_alert_rate": synthetic_injected_inferential_stats["rate"],
                "synthetic_injected_inferential_wilson_lo": synthetic_injected_inferential_stats["wilson_lo"],
                "synthetic_injected_inferential_wilson_hi": synthetic_injected_inferential_stats["wilson_hi"],
                "default_holdout_normal_alert_rate": default_holdout_normal_stats["rate"],
                "default_holdout_suspect_alert_rate": default_holdout_suspect_stats["rate"],
                "threshold_operating_point_feasible": bool(feasibility["feasible"]),
                "threshold_operating_point_feasible_count": int(feasibility["feasible_count"]),
                "threshold_operating_point_feasible_min": float(feasibility["feasible_min_threshold"]),
                "threshold_operating_point_feasible_max": float(feasibility["feasible_max_threshold"]),
                "threshold_operating_point_inferential_feasible": bool(inferential_feasibility["feasible"]),
                "threshold_operating_point_inferential_feasible_count": int(
                    inferential_feasibility["feasible_count"]
                ),
                "threshold_operating_point_inferential_feasible_min": float(
                    inferential_feasibility["feasible_min_threshold"]
                ),
                "threshold_operating_point_inferential_feasible_max": float(
                    inferential_feasibility["feasible_max_threshold"]
                ),
                "inferential_operating_evaluable": bool(inferential_evaluable),
                "inferential_operating_targets_met": bool(inferential_meets_targets),
                "median_full_tail_prob_pairs": safe_numeric_median(
                    working.loc[working["has_metrics"], "full_tail_prob_pairs"]
                ),
                "median_full_tail_prob_max_name": safe_numeric_median(
                    working.loc[working["has_metrics"], "full_tail_prob_max_name"]
                ),
                "median_full_pairs_ratio": safe_numeric_median(
                    working.loc[working["has_metrics"], "full_pairs_ratio"]
                ),
                "median_small_bucket_alert_share": safe_numeric_median(
                    working.loc[working["has_metrics"], "small_bucket_alert_share"]
                ),
                "median_bucket_low_power_share": safe_numeric_median(
                    working.loc[working["has_metrics"], "bucket_low_power_share"]
                ),
                "max_fallback_steps": int(
                    pd.to_numeric(working.loc[working["has_metrics"], "fallback_steps_max"], errors="coerce")
                    .fillna(0)
                    .max()
                ),
            }
        )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(["scenario_id"]).reset_index(drop=True)


def _normalization_sensitivity(case_results: pd.DataFrame) -> list[dict[str, object]]:
    if case_results.empty:
        return []
    subset = case_results[
        (case_results["baseline_variant"] == "all_registrants")
        & (case_results["requested_geo_level"] == "state")
        & (case_results["requested_geo_value"] == "WA")
        & (case_results["normalization_mode"].isin(["default", "canonical_medium_proxy"]))
        & (case_results["has_metrics"])
    ].copy()
    if subset.empty:
        return []

    pivot = subset.pivot_table(
        index=["case_id", "family", "split"],
        columns="normalization_mode",
        values="full_tail_prob_pairs",
        aggfunc="first",
    )
    if "default" not in pivot.columns or "canonical_medium_proxy" not in pivot.columns:
        return []

    diff = (pivot["canonical_medium_proxy"] - pivot["default"]).dropna()
    if diff.empty:
        return []

    return [
        {
            "comparison": "state_wa_all default vs canonical_medium_proxy",
            "n_cases": int(diff.size),
            "median_delta_tail_prob_pairs": float(diff.median()),
            "p90_abs_delta_tail_prob_pairs": float(np.quantile(np.abs(diff.to_numpy(dtype=float)), 0.90)),
        }
    ]


def _build_memo(
    *,
    now: datetime,
    args: argparse.Namespace,
    case_manifest: pd.DataFrame,
    scenario_summary: pd.DataFrame,
    thresholds: dict[str, float],
    normalization_findings: list[dict[str, object]],
    probability_rows: pd.DataFrame,
) -> str:
    lines: list[str] = []
    lines.append("# DUP-007 VRDB Collision Backtest Memo")
    lines.append("")
    lines.append(f"Generated: {now.isoformat()}")
    lines.append(f"Seed: {int(args.seed)}")
    lines.append("")

    lines.append("## Scope")
    lines.append("- Families: historical normal controls, historical suspected hearings, synthetic null, synthetic injected.")
    lines.append(
        "- Required comparisons covered: state/county/city-conditioned baselines, denominator variants "
        "(`all_registrants` vs `active_only`), small-bucket behavior, geography quality edge cases, tail behavior."
    )
    lines.append(
        "- Normalization sensitivity: compared default full-name normalization against a canonical-medium proxy "
        "for the `state_wa_all` scenario."
    )
    lines.append("")

    lines.append("## Data Footprint")
    by_family = case_manifest.groupby("family", dropna=False)["case_id"].nunique().to_dict()
    for family in sorted(by_family):
        lines.append(f"- {family}: {int(by_family[family])} cases")

    normalization_versions = sorted(
        {
            value
            for value in probability_rows.get("normalization_version", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .tolist()
            if value
        }
    )
    vrdb_versions = sorted(
        {
            value
            for value in probability_rows.get("vrdb_version", pd.Series(dtype=str)).fillna("").astype(str).tolist()
            if value
        }
    )
    lines.append(f"- Probability normalization versions observed: {normalization_versions or ['(missing)']}")
    lines.append(f"- VRDB versions observed: {vrdb_versions or ['(missing)']}")
    lines.append("")

    lines.append("## Calibration and Holdout")
    lines.append(
        "- Historical families are split deterministically into calibration and holdout subsets. "
        "Calibration thresholds target the configured normal-control false-positive rate."
    )
    lines.append(
        "- Threshold-feasibility target: "
        + f"holdout_normal <= {float(args.max_holdout_normal_alert_rate):.3f} and "
        + f"synthetic_injected >= {float(args.min_synthetic_injected_alert_rate):.3f}."
    )
    lines.append(
        "- Inferential evaluability gate: "
        + f"holdout_normal_inferential_n >= {int(args.minimum_inferential_holdout_normal_cases)} "
        + "and synthetic_injected_inferential_n >= 1."
    )
    if thresholds:
        for scenario_id in sorted(thresholds):
            lines.append(f"- {scenario_id}: calibrated threshold={thresholds[scenario_id]:.6f}")
    lines.append("")

    lines.append("## Scenario Outcomes")
    if scenario_summary.empty:
        lines.append("- No scenario outcomes were produced.")
    else:
        for row in scenario_summary.itertuples(index=False):
            lines.append(
                "- "
                + f"{row.scenario_id}: holdout_normal={row.holdout_normal_alert_rate:.3f} "
                + f"(n={int(row.holdout_normal_n)}, wilson=[{row.holdout_normal_wilson_lo:.3f}, {row.holdout_normal_wilson_hi:.3f}]), "
                + f"holdout_normal_inferential={row.holdout_normal_inferential_alert_rate:.3f} "
                + f"(n={int(row.holdout_normal_inferential_n)}), "
                + f"holdout_suspect={row.holdout_suspect_alert_rate:.3f} "
                + f"(n={int(row.holdout_suspect_n)}), "
                + f"synthetic_null={row.synthetic_null_alert_rate:.3f} "
                + f"(n={int(row.synthetic_null_n)}), "
                + f"synthetic_injected={row.synthetic_injected_alert_rate:.3f} "
                + f"(n={int(row.synthetic_injected_n)}), "
                + f"threshold_feasible={bool(row.threshold_operating_point_feasible)} "
                + f"(count={int(row.threshold_operating_point_feasible_count)}), "
                + f"inferential_threshold_feasible={bool(row.threshold_operating_point_inferential_feasible)} "
                + f"(count={int(row.threshold_operating_point_inferential_feasible_count)}), "
                + f"inferential_evaluable={bool(row.inferential_operating_evaluable)}, "
                + f"inferential_targets_met={bool(row.inferential_operating_targets_met)}, "
                + f"median_pairs_ratio={row.median_full_pairs_ratio:.3f}, "
                + f"max_fallback_steps={int(row.max_fallback_steps)}"
            )
    lines.append("")

    lines.append("## Normalization Sensitivity")
    if not normalization_findings:
        lines.append("- No normalization sensitivity comparison rows were available.")
    else:
        for finding in normalization_findings:
            lines.append(
                "- "
                + f"{finding['comparison']}: n_cases={finding['n_cases']}, "
                + f"median_delta={finding['median_delta_tail_prob_pairs']:.6f}, "
                + f"p90_abs_delta={finding['p90_abs_delta_tail_prob_pairs']:.6f}"
            )
    lines.append("")

    lines.append("## Notes")
    lines.append(
        "- This memo evaluates stability of VRDB collision evidence as a screening signal. "
        "It does not claim the VRDB null is a fully generative attendance model."
    )
    lines.append(
        "- The canonical-medium normalization path is a sensitivity proxy and is intentionally labeled as such."
    )
    lines.append("")
    return "\n".join(lines)


def run() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    config = load_config(Path(args.config))
    bucket_minutes = _parse_bucket_minutes(args.bucket_minutes)
    baseline_scenarios = _default_baseline_scenarios()
    synthetic_scenarios = _default_synthetic_scenarios()

    LOGGER.info("Discovering historical hearings")
    historical_paths = _iter_historical_paths(args)
    if not historical_paths:
        raise ValueError("No historical hearing CSV files discovered for backtest.")
    LOGGER.info("Historical hearings discovered: %s", len(historical_paths))

    historical_stats, path_by_case_id = _historical_case_stats(historical_paths, config)
    if historical_stats.empty:
        raise ValueError("Historical case statistics are empty; cannot proceed.")

    family_map = select_historical_case_families(
        case_stats=historical_stats,
        normal_count=int(args.historical_normal_count),
        suspect_count=int(args.historical_suspect_count),
        min_rows=int(args.min_historical_rows),
        force_suspect_case_ids=tuple(str(value) for value in args.force_suspect),
    )
    if family_map.empty:
        raise ValueError("No eligible historical cases were selected for normal/suspect families.")

    normal_case_ids = family_map[family_map["family"] == "historical_normal"]["case_id"].astype(str).tolist()
    suspect_case_ids = family_map[family_map["family"] == "historical_suspect"]["case_id"].astype(str).tolist()

    normal_calibration, normal_holdout = split_case_ids(
        case_ids=normal_case_ids,
        seed=stable_seed(args.seed, "normal"),
        holdout_fraction=float(args.holdout_fraction),
    )
    suspect_calibration, suspect_holdout = split_case_ids(
        case_ids=suspect_case_ids,
        seed=stable_seed(args.seed, "suspect"),
        holdout_fraction=float(args.holdout_fraction),
    )
    historical_calibration_ids = set(normal_calibration) | set(suspect_calibration)
    historical_holdout_ids = set(normal_holdout) | set(suspect_holdout)

    LOGGER.info(
        "Historical family split: normal=%s suspect=%s calibration=%s holdout=%s",
        len(normal_case_ids),
        len(suspect_case_ids),
        len(historical_calibration_ids),
        len(historical_holdout_ids),
    )

    historical_frames = _load_selected_historical_frames(
        selected_case_ids=sorted(family_map["case_id"].astype(str).tolist()),
        path_by_case_id=path_by_case_id,
        config=config,
    )

    probability_rows = load_probability_rows_for_scenarios(
        probability_path=Path(args.probability_csv),
        scenarios=baseline_scenarios,
        name_key_type="full_name_key",
    )
    if probability_rows.empty:
        raise ValueError("No probability rows loaded for configured baseline scenarios.")

    state_all_rows = filter_probability_rows(
        probability_rows=probability_rows,
        baseline_variants={"all_registrants"},
        name_key_type="full_name_key",
        geo_target_keys={"state|WA"},
    )
    synthetic_cases, synthetic_frames = _build_synthetic_cases(
        synthetic_scenarios=synthetic_scenarios,
        synthetic_replicates=int(args.synthetic_replicates),
        seed=int(args.seed),
        probability_rows=state_all_rows,
    )

    backoff_rows = load_backoff_rows(
        backoff_path=Path(args.backoff_csv),
        baseline_variants={scenario.baseline_variant for scenario in baseline_scenarios},
    )

    case_frames: dict[str, pd.DataFrame] = {}
    case_frames.update(historical_frames)
    case_frames.update(synthetic_frames)

    manifest = _build_case_manifest(
        historical_family_map=family_map,
        historical_calibration_ids=historical_calibration_ids,
        historical_holdout_ids=historical_holdout_ids,
        synthetic_cases=synthetic_cases,
        path_by_case_id=path_by_case_id,
    )

    case_rows: list[dict[str, object]] = []
    for scenario in baseline_scenarios:
        scenario_probability = _scenario_probability_subset(
            probability_rows=probability_rows,
            scenario=scenario,
        )
        LOGGER.info(
            "Scenario %s: baseline=%s geo=%s:%s normalization=%s probability_rows=%s",
            scenario.scenario_id,
            scenario.baseline_variant,
            scenario.requested_geo_level,
            scenario.requested_geo_value,
            scenario.normalization_mode,
            len(scenario_probability),
        )

        if scenario_probability.empty:
            metrics_rows = pd.DataFrame()
        else:
            slice_rows = _slice_rows_for_scenario(
                scenario=scenario,
                case_manifest=manifest,
                case_frames=case_frames,
                bucket_minutes=bucket_minutes,
            )
            metrics_rows, _ = compute_vrdb_collision_null_for_slices(
                slice_rows=slice_rows,
                probability_rows=scenario_probability,
                backoff_rows=backoff_rows,
                monte_carlo_draws=int(args.monte_carlo_draws),
                random_seed=int(args.seed),
                top_name_limit=int(args.top_name_limit),
            )

        for manifest_row in manifest.itertuples(index=False):
            summary = summarize_case_metrics(
                metrics_rows=metrics_rows,
                case_id=str(manifest_row.case_id),
                family=str(manifest_row.family),
                scenario_id=scenario.scenario_id,
                baseline_variant=scenario.baseline_variant,
                requested_geo_level=scenario.requested_geo_level,
                requested_geo_value=scenario.requested_geo_value,
                normalization_mode=scenario.normalization_mode,
                tail_alpha=float(args.tail_alpha),
                small_bucket_minutes=int(args.small_bucket_minutes),
            )
            summary["split"] = str(manifest_row.split)
            summary["source_kind"] = str(manifest_row.source_kind)
            summary["source_path"] = str(manifest_row.source_path)
            case_rows.append(summary)

    case_results = pd.DataFrame(case_rows)
    if case_results.empty:
        raise ValueError("Backtest produced no case-level summary rows.")

    thresholds = _calibration_thresholds(
        case_results=case_results,
        calibration_target_fpr=float(args.calibration_target_fpr),
        fallback_tail_alpha=float(args.tail_alpha),
    )
    case_results["calibrated_threshold"] = case_results["scenario_id"].map(
        lambda token: float(thresholds.get(str(token), float(args.tail_alpha)))
    )
    case_results["calibrated_alert"] = (
        pd.to_numeric(case_results["full_tail_prob_pairs"], errors="coerce")
        .le(pd.to_numeric(case_results["calibrated_threshold"], errors="coerce"))
    )
    case_results["default_alert"] = (
        pd.to_numeric(case_results["full_tail_prob_pairs"], errors="coerce")
        .le(float(args.tail_alpha))
    )

    scenario_summary = _scenario_summary(
        case_results=case_results,
        thresholds=thresholds,
        default_tail_alpha=float(args.tail_alpha),
        max_holdout_normal_alert_rate=float(args.max_holdout_normal_alert_rate),
        min_synthetic_injected_alert_rate=float(args.min_synthetic_injected_alert_rate),
        minimum_inferential_holdout_normal_cases=int(args.minimum_inferential_holdout_normal_cases),
    )
    normalization_findings = _normalization_sensitivity(case_results)

    now = datetime.now(tz=UTC)
    memo_text = _build_memo(
        now=now,
        args=args,
        case_manifest=manifest,
        scenario_summary=scenario_summary,
        thresholds=thresholds,
        normalization_findings=normalization_findings,
        probability_rows=probability_rows,
    )

    case_csv = out_dir / "vrdb_collision_backtest_case_metrics.csv"
    summary_csv = out_dir / "vrdb_collision_backtest_scenario_summary.csv"
    manifest_csv = out_dir / "vrdb_collision_backtest_case_manifest.csv"
    memo_md = out_dir / "vrdb_collision_backtest_memo.md"
    summary_json = out_dir / "vrdb_collision_backtest_summary.json"

    case_results.to_csv(case_csv, index=False)
    scenario_summary.to_csv(summary_csv, index=False)
    manifest.to_csv(manifest_csv, index=False)
    memo_md.write_text(memo_text, encoding="utf-8")

    payload = {
        "generated_at": now.isoformat(),
        "seed": int(args.seed),
        "bucket_minutes": bucket_minutes,
        "tail_alpha": float(args.tail_alpha),
        "calibration_target_fpr": float(args.calibration_target_fpr),
        "max_holdout_normal_alert_rate": float(args.max_holdout_normal_alert_rate),
        "min_synthetic_injected_alert_rate": float(args.min_synthetic_injected_alert_rate),
        "minimum_inferential_holdout_normal_cases": int(args.minimum_inferential_holdout_normal_cases),
        "monte_carlo_draws": int(args.monte_carlo_draws),
        "historical_paths_considered": int(len(historical_paths)),
        "historical_case_stats_rows": int(len(historical_stats)),
        "manifest_rows": int(len(manifest)),
        "case_result_rows": int(len(case_results)),
        "scenario_rows": int(len(scenario_summary)),
        "baseline_scenarios": [asdict(item) for item in baseline_scenarios],
        "synthetic_scenarios": [asdict(item) for item in synthetic_scenarios],
        "thresholds": thresholds,
        "normalization_sensitivity": normalization_findings,
        "artifacts": {
            "case_csv": str(case_csv),
            "scenario_summary_csv": str(summary_csv),
            "manifest_csv": str(manifest_csv),
            "summary_json": str(summary_json),
            "memo_markdown": str(memo_md),
        },
    }
    summary_json.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    LOGGER.info("Wrote case metrics: %s", case_csv)
    LOGGER.info("Wrote scenario summary: %s", summary_csv)
    LOGGER.info("Wrote memo: %s", memo_md)
    LOGGER.info("Wrote summary JSON: %s", summary_json)
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
