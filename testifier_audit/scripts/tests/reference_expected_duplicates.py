from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(SRC_ROOT))

from testifier_audit.config import NamesConfig, TimeConfig  # noqa: E402
from testifier_audit.detectors.duplicates_exact import DuplicatesExactDetector  # noqa: E402
from testifier_audit.preprocess.names import add_name_features  # noqa: E402
from testifier_audit.preprocess.position import normalize_position  # noqa: E402
from testifier_audit.preprocess.time import add_time_features  # noqa: E402


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_names_from_submission(csv_path: Path, nickname_map_path: str) -> pd.Series:
    frame = pd.read_csv(csv_path)
    if "canonical_name" in frame.columns:
        canonical = frame["canonical_name"].fillna("").astype(str)
        if "is_person_name" in frame.columns:
            person_mask = frame["is_person_name"].astype(bool)
            canonical = canonical[person_mask]
        return canonical[canonical != ""]

    if "Name" in frame.columns:
        name_column = "Name"
    elif "name" in frame.columns:
        name_column = "name"
    else:
        raise ValueError("Expected a Name/name/canonical_name column in submission CSV")

    names = add_name_features(
        df=frame.rename(columns={name_column: "name"}),
        config=NamesConfig(nickname_map_path=nickname_map_path),
    )
    person_mask = names["is_person_name"].astype(bool)
    canonical = names["canonical_name"].fillna("").astype(str)
    canonical = canonical[person_mask]
    canonical = canonical[canonical != ""]
    canonical = canonical[canonical != "|"]
    return canonical


def _observed_metrics(counts: np.ndarray) -> dict[str, float]:
    pairs = float(np.sum(counts * np.maximum(counts - 1.0, 0.0) / 2.0))
    excess_rows = float(np.sum(np.maximum(counts - 1.0, 0.0)))
    repeated_group_rows = float(np.sum(counts[counts >= 2.0]))
    return {
        "pairs": pairs,
        "excess_rows": excess_rows,
        "repeated_group_rows": repeated_group_rows,
    }


def _detector_frame_from_submission(csv_path: Path, nickname_map_path: str) -> pd.DataFrame:
    raw = pd.read_csv(csv_path)
    frame = raw.copy()
    if "id" not in frame.columns:
        frame["id"] = np.arange(1, len(frame) + 1, dtype=int)

    if "canonical_name" in frame.columns:
        frame["canonical_name"] = frame["canonical_name"].fillna("").astype(str)
        frame = frame[frame["canonical_name"] != ""].copy()
        if "name_display" not in frame.columns:
            frame["name_display"] = frame["canonical_name"].astype(str).str.replace("|", ", ", regex=False)
        if "position_normalized" not in frame.columns:
            position_column = "Position" if "Position" in frame.columns else "position"
            frame["position"] = frame.get(position_column, "")
            frame = normalize_position(frame)
        if "timestamp" not in frame.columns or "minute_bucket" not in frame.columns:
            time_column = "Time Signed In" if "Time Signed In" in frame.columns else "time_signed_in"
            frame["time_signed_in"] = frame.get(time_column, "")
            frame = add_time_features(frame, config=TimeConfig())
        return frame

    if "Name" in frame.columns:
        name_column = "Name"
    elif "name" in frame.columns:
        name_column = "name"
    else:
        raise ValueError("Expected Name/name/canonical_name column in submission CSV")

    position_column = "Position" if "Position" in frame.columns else "position"
    time_column = "Time Signed In" if "Time Signed In" in frame.columns else "time_signed_in"
    prepared = pd.DataFrame(
        {
            "id": frame["id"],
            "name": frame[name_column],
            "organization": frame.get("Organization", ""),
            "position": frame.get(position_column, ""),
            "time_signed_in": frame.get(time_column, ""),
        }
    )
    prepared = add_name_features(
        df=prepared,
        config=NamesConfig(nickname_map_path=nickname_map_path),
    )
    prepared = normalize_position(prepared)
    prepared = add_time_features(prepared, config=TimeConfig())
    prepared["name_display"] = (
        prepared.get("name_display", prepared["canonical_name"])
        .fillna(prepared["canonical_name"])
        .astype(str)
    )
    return prepared


def _position_interval_reference_rows(
    *,
    submissions_csv: Path,
    nickname_map_path: str,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    frame = _detector_frame_from_submission(submissions_csv, nickname_map_path=nickname_map_path)
    detector = DuplicatesExactDetector(
        top_n=200,
        bucket_minutes=[30],
        collision_uncertainty_mode="analytic_only",
        collision_scope_primary="full_hearing",
        collision_scope_overlays=[],
        collision_baseline_source="hearing_empirical",
        collision_baseline_model="multinomial",
        position_interval_nominal=0.95,
        position_interval_draws=5000,
        position_claim_min_rows_per_position=25,
        random_seed=42,
    )
    result = detector.run(df=frame, features={})
    position_metrics = result.tables.get("position_duplicate_metrics", pd.DataFrame()).copy()
    if position_metrics.empty:
        rows: list[dict[str, object]] = []
    else:
        cols = [
            "position_normalized",
            "n_rows",
            "duplicate_rows",
            "duplicate_row_rate",
            "expected_duplicate_rows",
            "expected_duplicate_rows_p05",
            "expected_duplicate_rows_p50",
            "expected_duplicate_rows_p95",
            "expected_duplicate_row_rate",
            "expected_duplicate_row_rate_p05",
            "expected_duplicate_row_rate_p50",
            "expected_duplicate_row_rate_p95",
            "interval_method_id",
            "interval_draws_effective",
            "is_low_power",
            "inference_status",
        ]
        rows = (
            position_metrics[cols]
            .sort_values("position_normalized")
            .to_dict(orient="records")
        )
    summary = {
        "position_interval_nominal": float(result.summary.get("position_interval_nominal", 0.95)),
        "position_interval_method_id": str(
            result.summary.get("position_interval_method_id", detector.POSITION_INTERVAL_METHOD_ID)
        ),
        "position_claim_eligible": bool(result.summary.get("position_claim_eligible", False)),
        "position_claim_reason": str(result.summary.get("position_claim_reason", "")),
    }
    return rows, summary


def _expected_metrics_from_probabilities(n_rows: int, probabilities: np.ndarray) -> dict[str, float]:
    n = int(max(int(n_rows), 0))
    if n <= 0 or probabilities.size == 0:
        return {
            "pairs": 0.0,
            "excess_rows": 0.0,
            "repeated_group_rows": 0.0,
        }

    p = probabilities[np.isfinite(probabilities) & (probabilities > 0.0)]
    if p.size == 0:
        return {
            "pairs": 0.0,
            "excess_rows": 0.0,
            "repeated_group_rows": 0.0,
        }
    p = p / float(p.sum())

    if n <= 1:
        return {
            "pairs": 0.0,
            "excess_rows": 0.0,
            "repeated_group_rows": 0.0,
        }

    expected_pairs = float(math.comb(n, 2) * float(np.sum(p**2)))
    expected_repeated = float(n - (n * float(np.sum(p * np.power(1.0 - p, n - 1)))))
    expected_excess = float(n - float(np.sum(1.0 - np.power(1.0 - p, n))))

    return {
        "pairs": max(expected_pairs, 0.0),
        "excess_rows": max(expected_excess, 0.0),
        "repeated_group_rows": max(expected_repeated, 0.0),
    }


def build_reference_expected_duplicates(
    *,
    submissions_csv: Path,
    nickname_map_path: str,
    dataset_id: str,
    dataset_version: str,
    source_sha256: str,
) -> dict[str, object]:
    canonical_names = _canonical_names_from_submission(
        submissions_csv,
        nickname_map_path=nickname_map_path,
    )
    counts = canonical_names.value_counts(dropna=False).to_numpy(dtype=float)

    n_rows = int(canonical_names.shape[0])
    n_unique = int(len(counts))
    observed = _observed_metrics(counts)
    probabilities = counts / float(max(n_rows, 1)) if n_rows > 0 else np.asarray([], dtype=float)
    expected = _expected_metrics_from_probabilities(n_rows=n_rows, probabilities=probabilities)
    position_rows, position_summary = _position_interval_reference_rows(
        submissions_csv=submissions_csv,
        nickname_map_path=nickname_map_path,
    )

    metric_rows = []
    for metric in ("repeated_group_rows", "excess_rows", "pairs"):
        metric_rows.append(
            {
                "scope": "full_hearing",
                "metric": metric,
                "observed": float(observed[metric]),
                "expected": float(expected[metric]),
            }
        )

    return {
        "source": {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "source_sha256": source_sha256,
            "fixture_sha256": sha256_file(submissions_csv),
        },
        "reference_method": {
            "id": "expected_duplicates_probability_formula_v1",
            "description": (
                "Independent expected-collision formulas computed from empirical name probabilities."
            ),
            "generated_at_utc": datetime.now(UTC).isoformat(),
        },
        "summary": {
            "n_rows": n_rows,
            "n_unique_names": n_unique,
            "observed": observed,
            "expected": expected,
        },
        "position_interval_summary": position_summary,
        "position_duplicate_metrics_rows": position_rows,
        "collision_overview_rows": metric_rows,
        "tolerances": {
            "float_atol": 1e-9,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--submissions-csv", type=Path, required=True)
    parser.add_argument("--nickname-map-path", type=str, required=True)
    parser.add_argument("--dataset-id", type=str, required=True)
    parser.add_argument("--dataset-version", type=str, required=True)
    parser.add_argument("--source-sha256", type=str, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = build_reference_expected_duplicates(
        submissions_csv=args.submissions_csv,
        nickname_map_path=args.nickname_map_path,
        dataset_id=args.dataset_id,
        dataset_version=args.dataset_version,
        source_sha256=args.source_sha256,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
