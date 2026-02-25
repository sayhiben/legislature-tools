from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(SRC_ROOT))

from testifier_audit.config import NamesConfig  # noqa: E402
from testifier_audit.preprocess.names import add_name_features  # noqa: E402

_REC_ID_PATTERN = re.compile(r"rec-(\d+)-")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _extract_record_key(value: object) -> int | None:
    match = _REC_ID_PATTERN.search(str(value or ""))
    if not match:
        return None
    return int(match.group(1))


def _canonical_submission_rows(submissions_csv: Path, nickname_map_path: str) -> pd.DataFrame:
    raw = pd.read_csv(submissions_csv)
    names = add_name_features(
        df=raw.rename(columns={"Name": "name"}),
        config=NamesConfig(nickname_map_path=nickname_map_path),
    )
    rows = raw.copy()
    rows["canonical_name"] = names["canonical_name"].fillna("").astype(str)
    rows = rows[(rows["canonical_name"] != "") & (rows["canonical_name"] != "|")].copy()
    rows["record_key"] = rows["rec_id"].map(_extract_record_key)
    rows = rows[rows["record_key"].notna()].copy()
    rows["record_key"] = rows["record_key"].astype(int)
    return rows


def _registry_record_keys(registry_csv: Path) -> set[int]:
    registry = pd.read_csv(registry_csv)
    keys = {
        _extract_record_key(value)
        for value in registry.get("rec_id", pd.Series(dtype=str)).tolist()
    }
    return {int(value) for value in keys if value is not None}


def build_reference_voter_ground_truth(
    *,
    submissions_csv: Path,
    registry_csv: Path,
    nickname_map_path: str,
    dataset_id: str,
    dataset_version: str,
    source_sha256: str,
) -> dict[str, object]:
    rows = _canonical_submission_rows(
        submissions_csv=submissions_csv,
        nickname_map_path=nickname_map_path,
    )
    registry_keys = _registry_record_keys(registry_csv)
    truth_match = rows["record_key"].isin(registry_keys)

    n_rows = int(len(rows))
    n_truth_matched_rows = int(truth_match.sum())
    n_truth_unmatched_rows = int((~truth_match).sum())

    return {
        "source": {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "source_sha256": source_sha256,
            "submissions_fixture_sha256": _sha256_file(submissions_csv),
            "registry_fixture_sha256": _sha256_file(registry_csv),
        },
        "reference_method": {
            "id": "voter_linkage_ground_truth_from_febrl_rec_id_v1",
            "description": (
                "Ground truth labels derived from FEBRL rec_id key parity "
                "between registry and submissions."
            ),
            "generated_at_utc": datetime.now(UTC).isoformat(),
        },
        "truth_summary": {
            "n_rows_evaluable": n_rows,
            "n_truth_matched_rows": n_truth_matched_rows,
            "n_truth_unmatched_rows": n_truth_unmatched_rows,
            "truth_match_rate_rows": (
                float(n_truth_matched_rows) / float(n_rows) if n_rows else 0.0
            ),
            "truth_unmatched_rate_rows": (
                float(n_truth_unmatched_rows) / float(n_rows) if n_rows else 0.0
            ),
        },
        "evaluation_contract": {
            "modes": ["strict", "loose"],
            "predicted_match_outcomes": ["matched_unique", "matched_ambiguous"],
        },
        "quality_thresholds_by_mode": {
            "strict": {
                "min_precision": 0.99,
                "min_recall": 0.49,
                "min_f1": 0.65,
                "max_false_positive_rate": 0.01,
            },
            "loose": {
                "min_precision": 0.99,
                "min_recall": 0.49,
                "min_f1": 0.65,
                "max_false_positive_rate": 0.01,
            },
        },
        "tolerances": {
            "float_atol": 1e-9,
        },
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--submissions-csv", type=Path, required=True)
    parser.add_argument("--registry-csv", type=Path, required=True)
    parser.add_argument("--nickname-map-path", type=str, required=True)
    parser.add_argument("--dataset-id", type=str, required=True)
    parser.add_argument("--dataset-version", type=str, required=True)
    parser.add_argument("--source-sha256", type=str, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    payload = build_reference_voter_ground_truth(
        submissions_csv=args.submissions_csv,
        registry_csv=args.registry_csv,
        nickname_map_path=args.nickname_map_path,
        dataset_id=args.dataset_id,
        dataset_version=args.dataset_version,
        source_sha256=args.source_sha256,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
