from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(SRC_ROOT))

from testifier_audit.config import NamesConfig  # noqa: E402
from testifier_audit.names.linkage import LinkageThresholds, classify_name_linkage  # noqa: E402
from testifier_audit.names.nickname_map import load_nickname_map  # noqa: E402
from testifier_audit.preprocess.names import add_name_features  # noqa: E402

PRIMARY_OUTCOMES = ("matched_unique", "matched_ambiguous", "unmatched")
WA_TIMEZONE = "America/Los_Angeles"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_position(series: pd.Series) -> pd.Series:
    values = series.fillna("").astype(str).str.strip().str.upper()
    return values.map({"PRO": "Pro", "CON": "Con"}).fillna("Unknown")


def _parse_signed_in_utc(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    localized = parsed.dt.tz_localize(WA_TIMEZONE, ambiguous="NaT", nonexistent="NaT")
    return localized.dt.tz_convert("UTC").dt.floor("min")


def _prepare_names_frame(csv_path: Path, nickname_map_path: str) -> pd.DataFrame:
    frame = pd.read_csv(csv_path)

    if "canonical_name" in frame.columns:
        prepared = frame.copy()
        if "position_normalized" not in prepared.columns:
            if "position" in prepared.columns:
                prepared["position_normalized"] = _normalize_position(prepared["position"])
            elif "Position" in prepared.columns:
                prepared["position_normalized"] = _normalize_position(prepared["Position"])
            else:
                prepared["position_normalized"] = "Unknown"
        if "minute_bucket" not in prepared.columns:
            if "Time Signed In" in prepared.columns:
                prepared["minute_bucket"] = _parse_signed_in_utc(prepared["Time Signed In"])
            else:
                prepared["minute_bucket"] = pd.NaT
        prepared["canonical_name"] = prepared["canonical_name"].fillna("").astype(str)
        return prepared

    if "Name" in frame.columns:
        name_column = "Name"
    elif "name" in frame.columns:
        name_column = "name"
    else:
        raise ValueError("Expected Name/name/canonical_name column")

    names = add_name_features(
        df=frame.rename(columns={name_column: "name"}),
        config=NamesConfig(nickname_map_path=nickname_map_path),
    )

    names["canonical_name"] = names["canonical_name"].fillna("").astype(str)
    if "Position" in frame.columns:
        names["position_normalized"] = _normalize_position(frame["Position"])
    elif "position" in frame.columns:
        names["position_normalized"] = _normalize_position(frame["position"])
    else:
        names["position_normalized"] = "Unknown"

    if "Time Signed In" in frame.columns:
        names["minute_bucket"] = _parse_signed_in_utc(frame["Time Signed In"])
    elif "minute_bucket" in frame.columns:
        names["minute_bucket"] = pd.to_datetime(frame["minute_bucket"], errors="coerce").dt.floor("min")
    else:
        names["minute_bucket"] = pd.NaT

    return names


def _candidate_lookup(registry: pd.DataFrame) -> dict[str, list[dict[str, object]]]:
    lookup: dict[str, list[dict[str, object]]] = {}
    canonical_series = registry["canonical_name"].fillna("").astype(str)
    counts = canonical_series.value_counts(dropna=False)

    for canonical_name, count in counts.items():
        if not canonical_name or "|" not in canonical_name:
            continue
        last_name, first_name = canonical_name.split("|", 1)
        last_name = last_name.strip()
        first_name = first_name.strip()
        if not last_name:
            continue
        lookup.setdefault(last_name, []).append(
            {
                "canonical_name": canonical_name,
                "canonical_first": first_name,
                "n_registry_rows": int(count),
            }
        )
    return lookup


def build_reference_voter_linkage(
    *,
    submissions_csv: Path,
    registry_csv: Path,
    nickname_map_path: str,
    dataset_id: str,
    dataset_version: str,
    source_sha256: str,
    bucket_minutes: list[int],
) -> dict[str, object]:
    submissions = _prepare_names_frame(submissions_csv, nickname_map_path=nickname_map_path)
    registry = _prepare_names_frame(registry_csv, nickname_map_path=nickname_map_path)

    submissions = submissions.dropna(subset=["minute_bucket"]).copy()
    submissions = submissions[submissions["canonical_name"].isin(["", "|"]) == False].copy()

    registry_canonical = registry["canonical_name"].fillna("").astype(str)
    registry_canonical = registry_canonical[(registry_canonical != "") & (registry_canonical != "|")]

    exact_lookup = registry_canonical.value_counts(dropna=False).to_dict()
    candidate_lookup = _candidate_lookup(registry)

    submission_names = sorted(
        set(submissions["canonical_name"].fillna("").astype(str).tolist()) - {"", "|"}
    )

    assignments = classify_name_linkage(
        submission_names=submission_names,
        exact_lookup={str(k): int(v) for k, v in exact_lookup.items()},
        candidate_lookup_by_last=candidate_lookup,
        nickname_map=load_nickname_map(nickname_map_path),
        thresholds=LinkageThresholds(
            strong_fuzzy_min_score=92.0,
            weak_fuzzy_min_score=84.0,
            ambiguous_score_gap=2.0,
        ),
    )

    assignments["strict_outcome"] = "unmatched"
    strict_mask = assignments["match_tier"] == "exact"
    assignments.loc[strict_mask, "strict_outcome"] = assignments.loc[strict_mask, "primary_outcome"]

    assignments["loose_outcome"] = "unmatched"
    loose_mask = assignments["match_tier"].isin({"exact", "nickname_exact"})
    assignments.loc[loose_mask, "loose_outcome"] = assignments.loc[loose_mask, "primary_outcome"]

    merged = submissions.merge(assignments, on="canonical_name", how="left")
    for outcome_col in ("strict_outcome", "loose_outcome"):
        merged[outcome_col] = merged[outcome_col].fillna("unmatched").astype(str)

    assignments = assignments.copy()
    for outcome_col in ("strict_outcome", "loose_outcome"):
        assignments[outcome_col] = assignments[outcome_col].fillna("unmatched").astype(str)

    sensitivity_rows: list[dict[str, object]] = []
    match_by_bucket_rows: list[dict[str, object]] = []
    summary_by_mode: dict[str, dict[str, object]] = {}

    for match_mode, outcome_col in (("strict", "strict_outcome"), ("loose", "loose_outcome")):
        row_counts = merged[outcome_col].value_counts().reindex(PRIMARY_OUTCOMES, fill_value=0)
        unique_counts = assignments[outcome_col].value_counts().reindex(PRIMARY_OUTCOMES, fill_value=0)

        n_rows = int(len(merged))
        n_unique_names = int(len(assignments))
        n_matched_unique_rows = int(row_counts["matched_unique"])
        n_matched_ambiguous_rows = int(row_counts["matched_ambiguous"])
        n_unmatched_rows = int(row_counts["unmatched"])
        n_matched_rows = n_matched_unique_rows + n_matched_ambiguous_rows

        summary_by_mode[match_mode] = {
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

        sensitivity_rows.append(
            {
                "mode": match_mode,
                "match_mode": match_mode,
                **summary_by_mode[match_mode],
                "matched_rate_unique": (
                    float(unique_counts["matched_unique"] + unique_counts["matched_ambiguous"])
                    / float(n_unique_names)
                    if n_unique_names
                    else 0.0
                ),
                "unmatched_rate_unique": (
                    float(unique_counts["unmatched"]) / float(n_unique_names)
                    if n_unique_names
                    else 0.0
                ),
            }
        )

        for bucket in bucket_minutes:
            bucketed = merged.copy()
            bucketed["bucket_start"] = pd.to_datetime(bucketed["minute_bucket"], errors="coerce").dt.floor(
                f"{int(bucket)}min"
            )
            grouped = (
                bucketed.groupby("bucket_start", dropna=False)
                .agg(
                    n_total=("canonical_name", "count"),
                    n_matched_unique=(outcome_col, lambda s: int((s == "matched_unique").sum())),
                    n_matched_ambiguous=(outcome_col, lambda s: int((s == "matched_ambiguous").sum())),
                    n_unmatched=(outcome_col, lambda s: int((s == "unmatched").sum())),
                )
                .reset_index()
                .sort_values("bucket_start")
            )
            grouped["match_mode"] = match_mode
            grouped["bucket_minutes"] = int(bucket)
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
            match_by_bucket_rows.extend(grouped.to_dict(orient="records"))

    return {
        "source": {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "source_sha256": source_sha256,
            "submissions_fixture_sha256": sha256_file(submissions_csv),
            "registry_fixture_sha256": sha256_file(registry_csv),
        },
        "reference_method": {
            "id": "voter_linkage_aggregation_reference_v1",
            "description": (
                "Independent aggregation over classify_name_linkage outcomes for strict/loose modes."
            ),
            "generated_at_utc": datetime.now(UTC).isoformat(),
        },
        "summary_by_mode": summary_by_mode,
        "sensitivity_modes": sensitivity_rows,
        "match_by_bucket": match_by_bucket_rows,
        "tolerances": {
            "float_atol": 1e-9,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--submissions-csv", type=Path, required=True)
    parser.add_argument("--registry-csv", type=Path, required=True)
    parser.add_argument("--nickname-map-path", type=str, required=True)
    parser.add_argument("--dataset-id", type=str, required=True)
    parser.add_argument("--dataset-version", type=str, required=True)
    parser.add_argument("--source-sha256", type=str, required=True)
    parser.add_argument("--bucket-minutes", type=int, nargs="+", required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = build_reference_voter_linkage(
        submissions_csv=args.submissions_csv,
        registry_csv=args.registry_csv,
        nickname_map_path=args.nickname_map_path,
        dataset_id=args.dataset_id,
        dataset_version=args.dataset_version,
        source_sha256=args.source_sha256,
        bucket_minutes=list(args.bucket_minutes),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
