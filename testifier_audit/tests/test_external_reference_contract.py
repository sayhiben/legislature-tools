from __future__ import annotations

import hashlib
import math
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from testifier_audit.config import NamesConfig
from testifier_audit.features.rarity import normalize_name_token
from testifier_audit.names.nickname_map import load_nickname_map
from testifier_audit.preprocess.names import add_name_features
from tests._methodology_assertions import assert_frame_matches_records, assert_mapping_subset
from tests._methodology_fixture_loader import fixture_path, load_fixture_json

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NICKNAME_MAP_PATH = str((PROJECT_ROOT / "configs" / "nicknames.csv").resolve())
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MATCHED_OUTCOMES = {"matched_unique", "matched_ambiguous"}
_REC_ID_PATTERN = re.compile(r"rec-(\d+)-")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _split_canonical_name(value: str) -> tuple[str, str]:
    raw = str(value or "")
    if "|" not in raw:
        return "", ""
    last, first = raw.split("|", 1)
    return last.strip(), first.strip()


def _nickname_root(first_name: str, nickname_map: dict[str, str]) -> str:
    normalized = normalize_name_token(first_name)
    if not normalized:
        return ""
    return nickname_map.get(normalized, normalized)


def _canonical_names_from_submission_csv(csv_path: Path) -> pd.Series:
    frame = pd.read_csv(csv_path)
    names = add_name_features(
        df=frame.rename(columns={"Name": "name"}),
        config=NamesConfig(nickname_map_path=NICKNAME_MAP_PATH),
    )
    person_mask = names["is_person_name"].astype(bool)
    canonical = names["canonical_name"].fillna("").astype(str)
    canonical = canonical[person_mask]
    canonical = canonical[(canonical != "") & (canonical != "|")]
    return canonical


def _observed_collision_metrics(counts: np.ndarray) -> dict[str, float]:
    pairs = float(np.sum(counts * np.maximum(counts - 1.0, 0.0) / 2.0))
    excess_rows = float(np.sum(np.maximum(counts - 1.0, 0.0)))
    repeated_group_rows = float(np.sum(counts[counts >= 2.0]))
    return {
        "pairs": pairs,
        "excess_rows": excess_rows,
        "repeated_group_rows": repeated_group_rows,
    }


def _expected_collision_metrics(n_rows: int, probabilities: np.ndarray) -> dict[str, float]:
    n = int(max(int(n_rows), 0))
    if n <= 1 or probabilities.size == 0:
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

    expected_pairs = float(math.comb(n, 2) * float(np.sum(p**2)))
    expected_repeated = float(n - (n * float(np.sum(p * np.power(1.0 - p, n - 1)))))
    expected_excess = float(n - float(np.sum(1.0 - np.power(1.0 - p, n))))

    return {
        "pairs": max(expected_pairs, 0.0),
        "excess_rows": max(expected_excess, 0.0),
        "repeated_group_rows": max(expected_repeated, 0.0),
    }


def _extract_record_key(value: object) -> int | None:
    match = _REC_ID_PATTERN.search(str(value or ""))
    if not match:
        return None
    return int(match.group(1))


def _truth_summary(*, submissions_csv: Path, registry_csv: Path) -> dict[str, float | int]:
    submissions_raw = pd.read_csv(submissions_csv)
    names = add_name_features(
        df=submissions_raw.rename(columns={"Name": "name"}),
        config=NamesConfig(nickname_map_path=NICKNAME_MAP_PATH),
    )
    rows = submissions_raw.copy()
    rows["canonical_name"] = names["canonical_name"].fillna("").astype(str)
    rows = rows[(rows["canonical_name"] != "") & (rows["canonical_name"] != "|")].copy()
    rows["record_key"] = rows["rec_id"].map(_extract_record_key)
    rows = rows[rows["record_key"].notna()].copy()
    rows["record_key"] = rows["record_key"].astype(int)

    registry = pd.read_csv(registry_csv)
    registry_keys = {
        key
        for key in (_extract_record_key(value) for value in registry.get("rec_id", []))
        if key is not None
    }
    truth_match = rows["record_key"].isin(registry_keys)
    n_rows = int(len(rows))
    n_truth_matched_rows = int(truth_match.sum())
    n_truth_unmatched_rows = int((~truth_match).sum())
    return {
        "n_rows_evaluable": n_rows,
        "n_truth_matched_rows": n_truth_matched_rows,
        "n_truth_unmatched_rows": n_truth_unmatched_rows,
        "truth_match_rate_rows": (float(n_truth_matched_rows) / float(n_rows)) if n_rows else 0.0,
        "truth_unmatched_rate_rows": (float(n_truth_unmatched_rows) / float(n_rows)) if n_rows else 0.0,
    }


def _prepare_submissions_with_minute_bucket(csv_path: Path) -> pd.DataFrame:
    frame = pd.read_csv(csv_path)
    names = add_name_features(
        df=frame.rename(columns={"Name": "name"}),
        config=NamesConfig(nickname_map_path=NICKNAME_MAP_PATH),
    )
    prepared = frame.copy()
    prepared["canonical_name"] = names["canonical_name"].fillna("").astype(str)
    parsed = pd.to_datetime(prepared["Time Signed In"], errors="coerce")
    localized = parsed.dt.tz_localize("America/Los_Angeles", ambiguous="NaT", nonexistent="NaT")
    prepared["minute_bucket"] = localized.dt.tz_convert("UTC").dt.floor("min")
    prepared = prepared[(prepared["canonical_name"] != "") & (prepared["canonical_name"] != "|")].copy()
    prepared = prepared.dropna(subset=["minute_bucket"]).copy()
    return prepared


def _registry_name_counts(registry_csv: Path) -> dict[str, int]:
    frame = pd.read_csv(registry_csv)
    names = add_name_features(
        df=frame.rename(columns={"Name": "name"}),
        config=NamesConfig(nickname_map_path=NICKNAME_MAP_PATH),
    )
    canonical = names["canonical_name"].fillna("").astype(str)
    canonical = canonical[(canonical != "") & (canonical != "|")]
    counts = canonical.value_counts(dropna=False)
    return {str(name): int(count) for name, count in counts.items()}


def _independent_voter_assignments(
    *,
    submission_names: list[str],
    exact_lookup: dict[str, int],
    nickname_map: dict[str, str],
) -> pd.DataFrame:
    by_last_root: dict[tuple[str, str], list[tuple[str, int]]] = {}
    for canonical_name, n_registry_rows in exact_lookup.items():
        last_name, first_name = _split_canonical_name(canonical_name)
        first_root = _nickname_root(first_name, nickname_map)
        if not last_name or not first_root:
            continue
        by_last_root.setdefault((last_name, first_root), []).append(
            (canonical_name, int(n_registry_rows))
        )

    rows: list[dict[str, str]] = []
    for canonical_name in submission_names:
        if canonical_name in exact_lookup:
            rows_count = int(exact_lookup[canonical_name])
            exact_outcome = "matched_unique" if rows_count == 1 else "matched_ambiguous"
            rows.append(
                {
                    "canonical_name": canonical_name,
                    "strict_outcome": exact_outcome,
                    "loose_outcome": exact_outcome,
                }
            )
            continue

        last_name, first_name = _split_canonical_name(canonical_name)
        first_root = _nickname_root(first_name, nickname_map)
        candidates = sorted(by_last_root.get((last_name, first_root), []), key=lambda item: item[0])
        loose_outcome = "unmatched"
        if candidates:
            selected_registry_rows = int(candidates[0][1])
            is_ambiguous = len(candidates) > 1 or selected_registry_rows > 1
            loose_outcome = "matched_ambiguous" if is_ambiguous else "matched_unique"
        rows.append(
            {
                "canonical_name": canonical_name,
                "strict_outcome": "unmatched",
                "loose_outcome": loose_outcome,
            }
        )
    return pd.DataFrame(rows)


def _mode_summary(
    *,
    merged: pd.DataFrame,
    assignments: pd.DataFrame,
    match_mode: str,
) -> dict[str, float | int]:
    outcome_column = f"{match_mode}_outcome"
    outcomes = ("matched_unique", "matched_ambiguous", "unmatched")
    row_counts = merged[outcome_column].value_counts().reindex(outcomes, fill_value=0)
    unique_counts = assignments[outcome_column].value_counts().reindex(outcomes, fill_value=0)

    n_rows = int(len(merged))
    n_unique_names = int(len(assignments))
    n_matched_unique_rows = int(row_counts["matched_unique"])
    n_matched_ambiguous_rows = int(row_counts["matched_ambiguous"])
    n_unmatched_rows = int(row_counts["unmatched"])
    n_matched_rows = n_matched_unique_rows + n_matched_ambiguous_rows
    return {
        "n_rows": n_rows,
        "n_unique_names": n_unique_names,
        "n_matched_unique_rows": n_matched_unique_rows,
        "n_matched_ambiguous_rows": n_matched_ambiguous_rows,
        "n_unmatched_rows": n_unmatched_rows,
        "matched_rate_rows": (float(n_matched_rows) / float(n_rows)) if n_rows else 0.0,
        "unmatched_rate_rows": (float(n_unmatched_rows) / float(n_rows)) if n_rows else 0.0,
        "n_matched_unique_unique": int(unique_counts["matched_unique"]),
        "n_matched_ambiguous_unique": int(unique_counts["matched_ambiguous"]),
        "n_unmatched_unique": int(unique_counts["unmatched"]),
    }


def _match_by_bucket_rows(
    *,
    merged: pd.DataFrame,
    match_mode: str,
    bucket_minutes: int,
) -> list[dict[str, Any]]:
    outcome_column = f"{match_mode}_outcome"
    grouped = (
        merged.assign(
            bucket_start=pd.to_datetime(merged["minute_bucket"], errors="coerce").dt.floor(
                f"{int(bucket_minutes)}min"
            )
        )
        .groupby("bucket_start", dropna=False)
        .agg(
            n_total=("canonical_name", "count"),
            n_matched_unique=(outcome_column, lambda series: int((series == "matched_unique").sum())),
            n_matched_ambiguous=(outcome_column, lambda series: int((series == "matched_ambiguous").sum())),
            n_unmatched=(outcome_column, lambda series: int((series == "unmatched").sum())),
        )
        .reset_index()
        .sort_values("bucket_start")
    )
    grouped["match_mode"] = match_mode
    grouped["bucket_minutes"] = int(bucket_minutes)
    grouped["matched_rate"] = (
        (grouped["n_matched_unique"] + grouped["n_matched_ambiguous"]) / grouped["n_total"]
    ).where(grouped["n_total"] > 0, 0.0)
    grouped["unmatched_rate"] = (grouped["n_unmatched"] / grouped["n_total"]).where(
        grouped["n_total"] > 0,
        0.0,
    )
    grouped["bucket_start"] = pd.to_datetime(grouped["bucket_start"], errors="coerce").dt.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    return grouped.to_dict(orient="records")


def test_external_manifests_and_expected_artifacts_have_provenance_and_valid_checksums() -> None:
    duplicates_manifest = load_fixture_json("external", "expected", "duplicates_manifest.json")
    voter_manifest = load_fixture_json("external", "expected", "voter_manifest.json")

    for manifest in (duplicates_manifest, voter_manifest):
        source = manifest["source"]
        assert source["provider"] == "recordlinkage"
        assert source["license"] == "BSD-3-Clause"
        assert set(source["dataset_urls"].keys()) == {"dataset1", "dataset2", "dataset4a", "dataset4b"}
        for url in source["dataset_urls"].values():
            assert str(url).startswith(
                "https://raw.githubusercontent.com/J535D165/recordlinkage/"
            )

    for case in duplicates_manifest["cases"]:
        csv_path = fixture_path("external", *str(case["input_csv"]).split("/"))
        expected_path = fixture_path("external", *str(case["expected_json"]).split("/"))
        fixture_sha = _sha256_file(csv_path)
        expected = load_fixture_json("external", *str(case["expected_json"]).split("/"))

        assert fixture_sha == str(case["fixture_sha256"])
        assert expected_path.exists()
        assert expected["source"]["dataset_id"] == case["case_id"]
        assert expected["source"]["dataset_version"] == duplicates_manifest["source"]["dataset_version"]
        assert expected["source"]["fixture_sha256"] == fixture_sha
        assert _SHA256_RE.match(str(expected["source"]["source_sha256"])) is not None
        assert expected["reference_method"]["id"] == "expected_duplicates_probability_formula_v1"
        assert (
            expected["position_interval_summary"]["position_interval_method_id"]
            == "position_duplicate_interval_multinomial_mc_v1"
        )
        assert "position_duplicate_metrics_rows" in expected

    for case in voter_manifest["cases"]:
        submissions_path = fixture_path("external", *str(case["submissions_csv"]).split("/"))
        registry_path = fixture_path("external", *str(case["registry_csv"]).split("/"))
        expected_linkage = load_fixture_json("external", *str(case["expected_json"]).split("/"))
        expected_truth = load_fixture_json("external", *str(case["ground_truth_json"]).split("/"))

        submissions_sha = _sha256_file(submissions_path)
        registry_sha = _sha256_file(registry_path)

        assert submissions_sha == str(case["submissions_fixture_sha256"])
        assert registry_sha == str(case["registry_fixture_sha256"])

        for expected in (expected_linkage, expected_truth):
            assert expected["source"]["dataset_id"] == case["case_id"]
            assert expected["source"]["dataset_version"] == voter_manifest["source"]["dataset_version"]
            assert expected["source"]["submissions_fixture_sha256"] == submissions_sha
            assert expected["source"]["registry_fixture_sha256"] == registry_sha
            assert _SHA256_RE.match(str(expected["source"]["source_sha256"])) is not None

        assert expected_linkage["reference_method"]["id"] == "voter_linkage_aggregation_reference_v1"
        assert (
            expected_truth["reference_method"]["id"]
            == "voter_linkage_ground_truth_from_febrl_rec_id_v1"
        )


def test_external_duplicate_expected_payloads_match_independent_formula_recalculation() -> None:
    manifest = load_fixture_json("external", "expected", "duplicates_manifest.json")
    for case in manifest["cases"]:
        csv_path = fixture_path("external", *str(case["input_csv"]).split("/"))
        expected = load_fixture_json("external", *str(case["expected_json"]).split("/"))

        canonical_names = _canonical_names_from_submission_csv(csv_path)
        counts = canonical_names.value_counts(dropna=False).to_numpy(dtype=float)
        n_rows = int(canonical_names.shape[0])
        observed = _observed_collision_metrics(counts)
        probabilities = counts / float(max(n_rows, 1)) if n_rows > 0 else np.asarray([], dtype=float)
        expected_metrics = _expected_collision_metrics(n_rows=n_rows, probabilities=probabilities)

        atol = float(expected["tolerances"]["float_atol"])
        assert_mapping_subset(
            actual=expected["summary"],
            expected_subset={
                "n_rows": n_rows,
                "n_unique_names": int(len(counts)),
            },
            float_tolerance=atol,
        )
        assert_mapping_subset(
            actual=expected["summary"]["observed"],
            expected_subset=observed,
            float_tolerance=atol,
        )
        assert_mapping_subset(
            actual=expected["summary"]["expected"],
            expected_subset=expected_metrics,
            float_tolerance=atol,
        )

        independent_rows = [
            {
                "scope": "full_hearing",
                "metric": "repeated_group_rows",
                "observed": float(observed["repeated_group_rows"]),
                "expected": float(expected_metrics["repeated_group_rows"]),
            },
            {
                "scope": "full_hearing",
                "metric": "excess_rows",
                "observed": float(observed["excess_rows"]),
                "expected": float(expected_metrics["excess_rows"]),
            },
            {
                "scope": "full_hearing",
                "metric": "pairs",
                "observed": float(observed["pairs"]),
                "expected": float(expected_metrics["pairs"]),
            },
        ]
        assert_frame_matches_records(
            actual=pd.DataFrame(expected["collision_overview_rows"]),
            expected_records=independent_rows,
            columns=("scope", "metric", "observed", "expected"),
            sort_by=("scope", "metric"),
            float_tolerance=atol,
        )
        position_summary = expected["position_interval_summary"]
        assert float(position_summary["position_interval_nominal"]) == 0.95
        assert (
            str(position_summary["position_interval_method_id"])
            == "position_duplicate_interval_multinomial_mc_v1"
        )
        assert isinstance(position_summary["position_claim_eligible"], bool)
        assert isinstance(position_summary["position_claim_reason"], str)

        position_rows = pd.DataFrame(expected["position_duplicate_metrics_rows"])
        if not position_rows.empty:
            required_columns = {
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
            }
            assert required_columns.issubset(position_rows.columns)
            assert set(position_rows["interval_method_id"].astype(str)) == {
                "position_duplicate_interval_multinomial_mc_v1"
            }
            interval_numeric = position_rows[
                [
                    "expected_duplicate_rows_p05",
                    "expected_duplicate_rows_p50",
                    "expected_duplicate_rows_p95",
                    "expected_duplicate_row_rate_p05",
                    "expected_duplicate_row_rate_p50",
                    "expected_duplicate_row_rate_p95",
                ]
            ]
            assert np.isfinite(interval_numeric.to_numpy(dtype=float)).all()


def test_external_voter_ground_truth_payload_matches_independent_rec_id_truth_summary() -> None:
    manifest = load_fixture_json("external", "expected", "voter_manifest.json")
    case = manifest["cases"][0]
    submissions_csv = fixture_path("external", *str(case["submissions_csv"]).split("/"))
    registry_csv = fixture_path("external", *str(case["registry_csv"]).split("/"))
    expected = load_fixture_json("external", *str(case["ground_truth_json"]).split("/"))

    summary = _truth_summary(submissions_csv=submissions_csv, registry_csv=registry_csv)
    assert_mapping_subset(
        actual=expected["truth_summary"],
        expected_subset=summary,
        float_tolerance=float(expected["tolerances"]["float_atol"]),
    )

    for mode, thresholds in expected["quality_thresholds_by_mode"].items():
        assert str(mode) in {"strict", "loose"}
        assert 0.0 <= float(thresholds["min_precision"]) <= 1.0
        assert 0.0 <= float(thresholds["min_recall"]) <= 1.0
        assert 0.0 <= float(thresholds["min_f1"]) <= 1.0
        assert 0.0 <= float(thresholds["max_false_positive_rate"]) <= 1.0


def test_external_voter_linkage_expected_payload_matches_independent_strict_loose_aggregation() -> None:
    manifest = load_fixture_json("external", "expected", "voter_manifest.json")
    case = manifest["cases"][0]
    submissions_csv = fixture_path("external", *str(case["submissions_csv"]).split("/"))
    registry_csv = fixture_path("external", *str(case["registry_csv"]).split("/"))
    expected = load_fixture_json("external", *str(case["expected_json"]).split("/"))

    submissions = _prepare_submissions_with_minute_bucket(submissions_csv)
    exact_lookup = _registry_name_counts(registry_csv)
    nickname_map = load_nickname_map(NICKNAME_MAP_PATH)

    submission_names = sorted(set(submissions["canonical_name"].astype(str)))
    assignments = _independent_voter_assignments(
        submission_names=submission_names,
        exact_lookup=exact_lookup,
        nickname_map=nickname_map,
    )

    merged = submissions.merge(assignments, on="canonical_name", how="left")
    for mode in ("strict", "loose"):
        outcome_column = f"{mode}_outcome"
        merged[outcome_column] = merged[outcome_column].fillna("unmatched").astype(str)
        assignments[outcome_column] = assignments[outcome_column].fillna("unmatched").astype(str)

    atol = float(expected["tolerances"]["float_atol"])
    summary_by_mode = {
        mode: _mode_summary(merged=merged, assignments=assignments, match_mode=mode)
        for mode in ("strict", "loose")
    }
    for mode in ("strict", "loose"):
        assert_mapping_subset(
            actual=expected["summary_by_mode"][mode],
            expected_subset=summary_by_mode[mode],
            float_tolerance=atol,
        )

    sensitivity_rows = []
    for mode in ("strict", "loose"):
        summary = summary_by_mode[mode]
        n_unique = int(summary["n_unique_names"])
        matched_unique_unique = int(summary["n_matched_unique_unique"])
        matched_ambiguous_unique = int(summary["n_matched_ambiguous_unique"])
        unmatched_unique = int(summary["n_unmatched_unique"])
        sensitivity_rows.append(
            {
                "mode": mode,
                "match_mode": mode,
                **summary,
                "matched_rate_unique": (
                    float(matched_unique_unique + matched_ambiguous_unique) / float(n_unique)
                    if n_unique
                    else 0.0
                ),
                "unmatched_rate_unique": (
                    float(unmatched_unique) / float(n_unique) if n_unique else 0.0
                ),
            }
        )
    assert_frame_matches_records(
        actual=pd.DataFrame(expected["sensitivity_modes"]),
        expected_records=sensitivity_rows,
        columns=(
            "mode",
            "match_mode",
            "n_rows",
            "n_unique_names",
            "n_matched_unique_rows",
            "n_matched_ambiguous_rows",
            "n_unmatched_rows",
            "matched_rate_rows",
            "unmatched_rate_rows",
            "n_matched_unique_unique",
            "n_matched_ambiguous_unique",
            "n_unmatched_unique",
            "matched_rate_unique",
            "unmatched_rate_unique",
        ),
        sort_by=("mode",),
        float_tolerance=atol,
    )

    bucket_minutes = sorted(
        {
            int(value)
            for value in pd.DataFrame(expected["match_by_bucket"])["bucket_minutes"]
            .dropna()
            .astype(int)
            .tolist()
        }
    )
    bucket_rows: list[dict[str, Any]] = []
    for mode in ("strict", "loose"):
        for bucket in bucket_minutes:
            bucket_rows.extend(
                _match_by_bucket_rows(
                    merged=merged,
                    match_mode=mode,
                    bucket_minutes=bucket,
                )
            )

    assert_frame_matches_records(
        actual=pd.DataFrame(expected["match_by_bucket"]),
        expected_records=bucket_rows,
        columns=(
            "match_mode",
            "bucket_minutes",
            "bucket_start",
            "n_total",
            "n_matched_unique",
            "n_matched_ambiguous",
            "n_unmatched",
            "matched_rate",
            "unmatched_rate",
        ),
        sort_by=("match_mode", "bucket_minutes", "bucket_start"),
        datetime_columns=("bucket_start",),
        float_tolerance=atol,
    )

    for mode in ("strict", "loose"):
        outcome_column = f"{mode}_outcome"
        predicted_match_rate = float(merged[outcome_column].isin(_MATCHED_OUTCOMES).mean())
        assert np.isclose(
            predicted_match_rate,
            float(expected["summary_by_mode"][mode]["matched_rate_rows"]),
            rtol=0.0,
            atol=atol,
        )
