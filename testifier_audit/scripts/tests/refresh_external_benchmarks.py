from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from reference_expected_duplicates import build_reference_expected_duplicates
from reference_voter_linkage import build_reference_voter_linkage
from reference_voter_ground_truth import build_reference_voter_ground_truth


PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = PROJECT_ROOT / "tests" / "fixtures" / "methodology" / "external"
FEBRL_ROOT = FIXTURE_ROOT / "febrl"
EXPECTED_ROOT = FIXTURE_ROOT / "expected"
NICKNAME_MAP_PATH = str((PROJECT_ROOT / "configs" / "nicknames.csv").resolve())

RECORDLINKAGE_DATASET_URLS = {
    "dataset1": "https://raw.githubusercontent.com/J535D165/recordlinkage/master/recordlinkage/datasets/febrl/dataset1.csv",
    "dataset2": "https://raw.githubusercontent.com/J535D165/recordlinkage/master/recordlinkage/datasets/febrl/dataset2.csv",
    "dataset4a": "https://raw.githubusercontent.com/J535D165/recordlinkage/master/recordlinkage/datasets/febrl/dataset4a.csv",
    "dataset4b": "https://raw.githubusercontent.com/J535D165/recordlinkage/master/recordlinkage/datasets/febrl/dataset4b.csv",
}


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _extract_record_key(series: pd.Series) -> pd.Series:
    keys = series.fillna("").astype(str).str.extract(r"rec-(\d+)", expand=True)[0]
    return pd.to_numeric(keys, errors="coerce").fillna(-1).astype(int)


def _load_febrl_from_wheel(wheel_path: Path, dataset_filename: str) -> tuple[pd.DataFrame, bytes]:
    member = f"recordlinkage/datasets/febrl/{dataset_filename}"
    with zipfile.ZipFile(wheel_path) as archive:
        payload = archive.read(member)
    frame = pd.read_csv(
        io.BytesIO(payload),
        sep=",",
        skipinitialspace=True,
        dtype=str,
    )
    return frame, payload


def _to_submission_fixture(frame: pd.DataFrame, *, start_time: datetime) -> pd.DataFrame:
    working = frame.copy().reset_index(drop=True)
    surname = working.get("surname", pd.Series([""] * len(working))).fillna("").astype(str).str.strip()
    given = (
        working.get("given_name", pd.Series([""] * len(working))).fillna("").astype(str).str.strip()
    )

    timestamps = [start_time + timedelta(minutes=index) for index in range(len(working))]
    position = ["Pro" if index % 2 == 0 else "Con" for index in range(len(working))]

    fixture = pd.DataFrame(
        {
            "Group": "Testifying",
            "Name": surname + ", " + given,
            "Organization": "",
            "Position": position,
            "Time Signed In": [
                timestamp.strftime("%-m/%-d/%Y %-I:%M %p") for timestamp in timestamps
            ],
            "rec_id": working.get("rec_id", pd.Series([""] * len(working))).fillna("").astype(str),
        }
    )
    return fixture


def _to_registry_fixture(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy().reset_index(drop=True)
    surname = working.get("surname", pd.Series([""] * len(working))).fillna("").astype(str).str.strip()
    given = (
        working.get("given_name", pd.Series([""] * len(working))).fillna("").astype(str).str.strip()
    )
    return pd.DataFrame(
        {
            "rec_id": working.get("rec_id", pd.Series([""] * len(working))).fillna("").astype(str),
            "Name": surname + ", " + given,
        }
    )


def _select_keys(frame: pd.DataFrame, *, key_limit: int) -> pd.DataFrame:
    keys = _extract_record_key(frame.get("rec_id", pd.Series(dtype=str)))
    working = frame.copy()
    working["_key"] = keys
    valid = sorted({int(key) for key in working["_key"].tolist() if int(key) >= 0})
    selected = set(valid[:key_limit])
    return working[working["_key"].isin(selected)].drop(columns=["_key"]).reset_index(drop=True)


def _source_version_from_wheel(wheel_path: Path) -> str:
    match = re.search(r"recordlinkage-([\d\.]+)", wheel_path.name)
    return match.group(1) if match else "unknown"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fixture-root",
        type=Path,
        default=FIXTURE_ROOT,
        help="Fixture root; defaults to tests/fixtures/methodology/external.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fixture_root = args.fixture_root.resolve()
    febrl_root = fixture_root / "febrl"
    expected_root = fixture_root / "expected"
    febrl_root.mkdir(parents=True, exist_ok=True)
    expected_root.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        subprocess.run(
            [sys.executable, "-m", "pip", "download", "--no-deps", "recordlinkage", "-q"],
            cwd=temp_path,
            check=True,
        )
        wheel_path = next(temp_path.glob("recordlinkage-*.whl"))
        source_version = _source_version_from_wheel(wheel_path)

        dataset1, dataset1_bytes = _load_febrl_from_wheel(wheel_path, "dataset1.csv")
        dataset2, dataset2_bytes = _load_febrl_from_wheel(wheel_path, "dataset2.csv")
        dataset4a, dataset4a_bytes = _load_febrl_from_wheel(wheel_path, "dataset4a.csv")
        dataset4b, dataset4b_bytes = _load_febrl_from_wheel(wheel_path, "dataset4b.csv")

    duplicates_fast_raw = _select_keys(dataset1, key_limit=160)
    duplicates_extended_raw = _select_keys(dataset2, key_limit=700)
    voter_registry_raw = _select_keys(dataset4a, key_limit=280)
    voter_submission_raw = _select_keys(dataset4b, key_limit=340)

    duplicates_fast_fixture = _to_submission_fixture(
        duplicates_fast_raw,
        start_time=datetime(2026, 2, 3, 8, 0),
    )
    duplicates_extended_fixture = _to_submission_fixture(
        duplicates_extended_raw,
        start_time=datetime(2026, 2, 4, 8, 0),
    )
    voter_submissions_fixture = _to_submission_fixture(
        voter_submission_raw,
        start_time=datetime(2026, 2, 5, 8, 0),
    )
    voter_registry_fixture = _to_registry_fixture(voter_registry_raw)

    duplicates_fast_csv = febrl_root / "febrl_dataset1_duplicates_fast.csv"
    duplicates_extended_csv = febrl_root / "febrl_dataset2_duplicates_extended.csv"
    voter_submissions_csv = febrl_root / "febrl_dataset4_submissions_fast.csv"
    voter_registry_csv = febrl_root / "febrl_dataset4_registry_fast.csv"

    duplicates_fast_fixture.to_csv(duplicates_fast_csv, index=False)
    duplicates_extended_fixture.to_csv(duplicates_extended_csv, index=False)
    voter_submissions_fixture.to_csv(voter_submissions_csv, index=False)
    voter_registry_fixture.to_csv(voter_registry_csv, index=False)

    duplicates_fast_expected = build_reference_expected_duplicates(
        submissions_csv=duplicates_fast_csv,
        nickname_map_path=NICKNAME_MAP_PATH,
        dataset_id="febrl_dataset1_duplicates_fast",
        dataset_version=source_version,
        source_sha256=sha256_bytes(dataset1_bytes),
    )
    duplicates_extended_expected = build_reference_expected_duplicates(
        submissions_csv=duplicates_extended_csv,
        nickname_map_path=NICKNAME_MAP_PATH,
        dataset_id="febrl_dataset2_duplicates_extended",
        dataset_version=source_version,
        source_sha256=sha256_bytes(dataset2_bytes),
    )
    voter_fast_expected = build_reference_voter_linkage(
        submissions_csv=voter_submissions_csv,
        registry_csv=voter_registry_csv,
        nickname_map_path=NICKNAME_MAP_PATH,
        dataset_id="febrl_dataset4_voter_fast",
        dataset_version=source_version,
        source_sha256=sha256_bytes(dataset4a_bytes + dataset4b_bytes),
        bucket_minutes=[30],
    )
    voter_ground_truth_expected = build_reference_voter_ground_truth(
        submissions_csv=voter_submissions_csv,
        registry_csv=voter_registry_csv,
        nickname_map_path=NICKNAME_MAP_PATH,
        dataset_id="febrl_dataset4_voter_fast",
        dataset_version=source_version,
        source_sha256=sha256_bytes(dataset4a_bytes + dataset4b_bytes),
    )

    duplicates_fast_expected_path = expected_root / "febrl_dataset1_duplicates_fast.json"
    duplicates_extended_expected_path = expected_root / "febrl_dataset2_duplicates_extended.json"
    voter_fast_expected_path = expected_root / "febrl_dataset4_voter_fast.json"
    voter_ground_truth_expected_path = expected_root / "febrl_dataset4_voter_fast_ground_truth.json"

    _write_json(duplicates_fast_expected_path, duplicates_fast_expected)
    _write_json(duplicates_extended_expected_path, duplicates_extended_expected)
    _write_json(voter_fast_expected_path, voter_fast_expected)
    _write_json(voter_ground_truth_expected_path, voter_ground_truth_expected)

    duplicates_manifest = {
        "source": {
            "provider": "recordlinkage",
            "dataset_urls": RECORDLINKAGE_DATASET_URLS,
            "dataset_version": source_version,
            "license": "BSD-3-Clause",
        },
        "cases": [
            {
                "case_id": "febrl_dataset1_duplicates_fast",
                "tier": "default",
                "input_csv": str(duplicates_fast_csv.relative_to(fixture_root)),
                "expected_json": str(duplicates_fast_expected_path.relative_to(fixture_root)),
                "fixture_sha256": sha256_file(duplicates_fast_csv),
            },
            {
                "case_id": "febrl_dataset2_duplicates_extended",
                "tier": "extended",
                "input_csv": str(duplicates_extended_csv.relative_to(fixture_root)),
                "expected_json": str(duplicates_extended_expected_path.relative_to(fixture_root)),
                "fixture_sha256": sha256_file(duplicates_extended_csv),
            },
        ],
    }
    voter_manifest = {
        "source": {
            "provider": "recordlinkage",
            "dataset_urls": RECORDLINKAGE_DATASET_URLS,
            "dataset_version": source_version,
            "license": "BSD-3-Clause",
        },
        "cases": [
            {
                "case_id": "febrl_dataset4_voter_fast",
                "tier": "default",
                "submissions_csv": str(voter_submissions_csv.relative_to(fixture_root)),
                "registry_csv": str(voter_registry_csv.relative_to(fixture_root)),
                "expected_json": str(voter_fast_expected_path.relative_to(fixture_root)),
                "ground_truth_json": str(voter_ground_truth_expected_path.relative_to(fixture_root)),
                "submissions_fixture_sha256": sha256_file(voter_submissions_csv),
                "registry_fixture_sha256": sha256_file(voter_registry_csv),
            }
        ],
    }

    _write_json(expected_root / "duplicates_manifest.json", duplicates_manifest)
    _write_json(expected_root / "voter_manifest.json", voter_manifest)


if __name__ == "__main__":
    main()
