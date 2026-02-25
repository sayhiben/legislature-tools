from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import yaml

from testifier_audit.io.csi_testifiers import (
    CSIDownloadError,
    download_csi_testifier_csv,
    download_csi_testifier_csv_by_agenda_item,
)
from testifier_audit.io.hearing_metadata import PACIFIC_TIMEZONE_NAME
from testifier_audit.io.meeting_bill_index import run_build_meeting_bill_index

DEFAULT_INDEX_JSON = Path("data/metadata/wa_meeting_bill_index.json")
DEFAULT_INDEX_CSV = Path("data/metadata/wa_meeting_bill_index.csv")
DEFAULT_CSV_OUT_DIR = Path("data/raw")
DEFAULT_METADATA_OUT_DIR = Path("data/metadata")
DEFAULT_MANIFEST_OUT = Path("data/metadata/baseline_sample_manifest.json")


@dataclass(frozen=True)
class BaselineSampleCandidate:
    agenda_id: str
    agenda_item_id: str
    bill_id: str
    item_description: str
    hearing_type_description: str
    meeting_start: datetime
    revised_date: str
    chamber: str
    committee_name: str

    @property
    def key(self) -> tuple[str, str]:
        return self.agenda_id, (self.agenda_item_id or self.bill_id)

    @property
    def session_year(self) -> int:
        return int(self.meeting_start.year)

    def to_dict(self) -> dict[str, Any]:
        return {
            "agenda_id": self.agenda_id,
            "agenda_item_id": self.agenda_item_id,
            "bill_id": self.bill_id,
            "item_description": self.item_description,
            "hearing_type_description": self.hearing_type_description,
            "meeting_start": self.meeting_start.isoformat(),
            "revised_date": self.revised_date,
            "session_year": int(self.session_year),
            "chamber": self.chamber,
            "committee_name": self.committee_name,
        }


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _extract_bill_number(bill_id: str) -> str:
    matches = re.findall(r"\d{3,6}", bill_id)
    if not matches:
        return ""
    return matches[-1]


def _normalize_chamber(value: str) -> str:
    normalized = _safe_text(value).lower()
    if normalized.startswith("sen"):
        return "Senate"
    if normalized.startswith("hou"):
        return "House"
    return _safe_text(value) or "Unknown"


def _normalize_committee(value: str) -> str:
    return _safe_text(value) or "Unknown Committee"


def _parse_meeting_start(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    pacific = ZoneInfo(PACIFIC_TIMEZONE_NAME)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=pacific)
    return parsed.astimezone(pacific)


def _load_index_payload(index_path: Path) -> dict[str, Any]:
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        return {"rows": payload}
    return {"rows": []}


def _extract_index_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            out.append(dict(row))
    return out


def _index_contains_session_years(
    *,
    rows: Sequence[Mapping[str, Any]],
    session_years: Sequence[int],
) -> bool:
    target_years = set(int(year) for year in session_years)
    available_years: set[int] = set()
    for row in rows:
        meeting_start = _parse_meeting_start(_safe_text(row.get("meeting_date")))
        if meeting_start is not None:
            available_years.add(int(meeting_start.year))
    if not target_years:
        return True
    return target_years.issubset(available_years)


def derive_recent_session_years(
    *,
    session_count: int = 3,
    reference_date: date | None = None,
) -> list[int]:
    if session_count < 1:
        raise ValueError("session_count must be >= 1")
    anchor = reference_date or date.today()
    return [int(anchor.year - offset) for offset in range(session_count)]


def _session_bounds(
    *,
    session_years: Sequence[int],
    reference_date: date | None = None,
) -> tuple[date, date]:
    anchor = reference_date or date.today()
    min_year = min(int(year) for year in session_years)
    max_year = max(int(year) for year in session_years)
    start = date(min_year, 1, 1)
    if anchor.year == max_year:
        end = anchor
    else:
        end = date(max_year, 12, 31)
    return start, end


def _collect_sampled_keys_from_sidecars(metadata_dir: Path) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    if not metadata_dir.exists():
        return keys
    for sidecar_path in metadata_dir.glob("*.hearing.yaml"):
        try:
            payload = yaml.safe_load(sidecar_path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError:
            continue
        if not isinstance(payload, dict):
            continue
        source = payload.get("source", {})
        if not isinstance(source, dict):
            continue
        agenda_id = _safe_text(source.get("meeting_family_id"))
        agenda_item_id = _safe_text(source.get("agenda_item_id"))
        if agenda_id and agenda_item_id:
            keys.add((agenda_id, agenda_item_id))
    return keys


def _collect_sampled_keys_from_manifest(manifest_path: Path) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    if not manifest_path.exists():
        return keys
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return keys
    if not isinstance(payload, dict):
        return keys
    successes = payload.get("successes", [])
    if not isinstance(successes, list):
        return keys
    for entry in successes:
        if not isinstance(entry, dict):
            continue
        agenda_id = _safe_text(entry.get("agenda_id"))
        agenda_item_id = _safe_text(entry.get("agenda_item_id"))
        if agenda_id and agenda_item_id:
            keys.add((agenda_id, agenda_item_id))
    return keys


def collect_sampled_keys(
    *,
    metadata_dirs: Sequence[Path],
    manifest_path: Path | None,
) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    for metadata_dir in metadata_dirs:
        keys.update(_collect_sampled_keys_from_sidecars(metadata_dir))
    if manifest_path is not None:
        keys.update(_collect_sampled_keys_from_manifest(manifest_path))
    return keys


def build_candidate_pool(
    *,
    rows: Sequence[Mapping[str, Any]],
    session_years: Sequence[int],
    sampled_keys: set[tuple[str, str]],
) -> list[BaselineSampleCandidate]:
    target_years = set(int(year) for year in session_years)
    deduped: dict[tuple[str, str], BaselineSampleCandidate] = {}
    for row in rows:
        agenda_id = _safe_text(row.get("agenda_id"))
        agenda_item_id = _safe_text(row.get("agenda_item_id"))
        bill_id = _safe_text(row.get("bill_id"))
        if not (agenda_id and bill_id):
            continue
        sampled_key = (agenda_id, agenda_item_id) if agenda_item_id else None
        if sampled_key is not None and sampled_key in sampled_keys:
            continue
        dedupe_key = (agenda_id, (agenda_item_id or bill_id))

        meeting_start = _parse_meeting_start(_safe_text(row.get("meeting_date")))
        if meeting_start is None:
            continue
        if int(meeting_start.year) not in target_years:
            continue

        candidate = BaselineSampleCandidate(
            agenda_id=agenda_id,
            agenda_item_id=agenda_item_id,
            bill_id=bill_id,
            item_description=_safe_text(row.get("item_description")),
            hearing_type_description=_safe_text(row.get("hearing_type_description")),
            meeting_start=meeting_start,
            revised_date=_safe_text(row.get("revised_date")),
            chamber=_normalize_chamber(_safe_text(row.get("agency"))),
            committee_name=_normalize_committee(_safe_text(row.get("committee_name"))),
        )
        prior = deduped.get(dedupe_key)
        if prior is None or candidate.revised_date > prior.revised_date:
            deduped[dedupe_key] = candidate
    return list(deduped.values())


def sample_candidates(
    *,
    candidates: Sequence[BaselineSampleCandidate],
    sample_size: int,
    seed: int | None = None,
) -> list[BaselineSampleCandidate]:
    if sample_size <= 0:
        return []
    rng = random.Random(seed)
    by_stratum: dict[tuple[str, str], list[BaselineSampleCandidate]] = {}
    for candidate in candidates:
        stratum = (candidate.chamber, candidate.committee_name)
        by_stratum.setdefault(stratum, []).append(candidate)
    for bucket in by_stratum.values():
        rng.shuffle(bucket)

    strata = list(by_stratum.keys())
    rng.shuffle(strata)
    selected: list[BaselineSampleCandidate] = []

    chamber_to_strata: dict[str, list[tuple[str, str]]] = {}
    for stratum in strata:
        chamber_to_strata.setdefault(stratum[0], []).append(stratum)
    chamber_order = [chamber for chamber, group in chamber_to_strata.items() if group]
    rng.shuffle(chamber_order)

    if sample_size >= len(chamber_order):
        for chamber in chamber_order:
            options = chamber_to_strata.get(chamber, [])
            rng.shuffle(options)
            for stratum in options:
                bucket = by_stratum.get(stratum, [])
                if bucket:
                    selected.append(bucket.pop())
                    break
            if len(selected) >= sample_size:
                return selected

    while len(selected) < sample_size:
        available_strata = [stratum for stratum in strata if by_stratum.get(stratum)]
        if not available_strata:
            break
        rng.shuffle(available_strata)
        progress = False
        for stratum in available_strata:
            bucket = by_stratum.get(stratum, [])
            if not bucket:
                continue
            selected.append(bucket.pop())
            progress = True
            if len(selected) >= sample_size:
                break
        if not progress:
            break
    return selected


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _materialize_candidates(
    *,
    candidates: Sequence[BaselineSampleCandidate],
    csv_out_dir: Path,
    metadata_out_dir: Path,
    rate_limit_seconds: float,
    timeout_seconds: float,
    max_retries: int,
    retry_backoff_seconds: float,
    overwrite: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    successes: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for index, candidate in enumerate(candidates):
        if index > 0 and rate_limit_seconds > 0:
            time.sleep(rate_limit_seconds)
        try:
            if candidate.agenda_item_id:
                result = download_csi_testifier_csv_by_agenda_item(
                    bill_query=candidate.bill_id,
                    meeting_family_id=candidate.agenda_id,
                    agenda_item_id=candidate.agenda_item_id,
                    meeting_start=candidate.meeting_start,
                    short_bill_id=candidate.bill_id,
                    bill_number=_extract_bill_number(candidate.bill_id),
                    bill_title=candidate.item_description,
                    committee_name=candidate.committee_name,
                    chamber=candidate.chamber,
                    agenda_item_description=candidate.item_description,
                    csv_out_dir=csv_out_dir,
                    metadata_out_dir=metadata_out_dir,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    retry_backoff_seconds=retry_backoff_seconds,
                    overwrite=overwrite,
                )
            else:
                # Fallback path for index rows that omit agenda_item_id.
                try:
                    result = download_csi_testifier_csv(
                        bill_query=candidate.bill_id,
                        csv_out_dir=csv_out_dir,
                        metadata_out_dir=metadata_out_dir,
                        meeting_family_id=candidate.agenda_id,
                        timeout_seconds=timeout_seconds,
                        max_retries=max_retries,
                        retry_backoff_seconds=retry_backoff_seconds,
                        overwrite=overwrite,
                    )
                except CSIDownloadError as exc:
                    # If the service search no longer returns this meeting family id,
                    # retry using bill query only.
                    if "was not returned for query" not in str(exc):
                        raise
                    result = download_csi_testifier_csv(
                        bill_query=candidate.bill_id,
                        csv_out_dir=csv_out_dir,
                        metadata_out_dir=metadata_out_dir,
                        timeout_seconds=timeout_seconds,
                        max_retries=max_retries,
                        retry_backoff_seconds=retry_backoff_seconds,
                        overwrite=overwrite,
                    )
            successes.append(
                {
                    **candidate.to_dict(),
                    "agenda_id": str(result.meeting_family_id),
                    "agenda_item_id": str(result.agenda_item_id),
                    "csv_path": str(result.csv_path.resolve()),
                    "csv_sha256": _sha256_file(result.csv_path),
                    "metadata_path": str(result.metadata_path.resolve()),
                    "metadata_sha256": _sha256_file(result.metadata_path),
                    "total_rows": int(result.total_rows),
                    "testifying_rows": int(result.testifying_rows),
                    "not_testifying_rows": int(result.not_testifying_rows),
                }
            )
        except CSIDownloadError as exc:
            failures.append({**candidate.to_dict(), "error": str(exc)})
    return successes, failures


def _ensure_index(
    *,
    index_json_path: Path,
    index_csv_path: Path,
    session_years: Sequence[int],
    timeout_seconds: float,
    refresh_index: bool,
    reference_date: date | None = None,
) -> tuple[dict[str, Any], bool]:
    index_exists = index_json_path.exists()
    index_payload: dict[str, Any] = {"rows": []}
    if index_exists:
        index_payload = _load_index_payload(index_json_path)

    if refresh_index or not index_exists:
        should_refresh = True
    else:
        should_refresh = not _index_contains_session_years(
            rows=_extract_index_rows(index_payload),
            session_years=session_years,
        )
    if not should_refresh:
        return index_payload, False

    start_date, end_date = _session_bounds(
        session_years=session_years,
        reference_date=reference_date,
    )
    rebuilt = run_build_meeting_bill_index(
        start_date=start_date,
        end_date=end_date,
        output_json=index_json_path,
        output_csv=index_csv_path,
        include_revised=True,
        timeout_seconds=timeout_seconds,
    )
    return rebuilt, True


def sample_unsampled_baseline_corpus(
    *,
    sample_size: int,
    session_count: int = 3,
    index_json_path: Path = DEFAULT_INDEX_JSON,
    index_csv_path: Path = DEFAULT_INDEX_CSV,
    csv_out_dir: Path = DEFAULT_CSV_OUT_DIR,
    metadata_out_dir: Path = DEFAULT_METADATA_OUT_DIR,
    manifest_path: Path = DEFAULT_MANIFEST_OUT,
    sampled_metadata_dirs: Sequence[Path] = (),
    refresh_index: bool = False,
    seed: int | None = None,
    timeout_seconds: float = 30.0,
    max_retries: int = 3,
    retry_backoff_seconds: float = 1.5,
    rate_limit_seconds: float = 1.0,
    overwrite: bool = False,
    reference_date: date | None = None,
) -> dict[str, Any]:
    requested = max(0, int(sample_size))
    session_years = derive_recent_session_years(
        session_count=max(1, int(session_count)),
        reference_date=reference_date,
    )
    unique_metadata_dirs = [metadata_out_dir]
    for path in sampled_metadata_dirs:
        if path not in unique_metadata_dirs:
            unique_metadata_dirs.append(path)

    index_payload, index_refreshed = _ensure_index(
        index_json_path=index_json_path,
        index_csv_path=index_csv_path,
        session_years=session_years,
        timeout_seconds=float(timeout_seconds),
        refresh_index=bool(refresh_index),
        reference_date=reference_date,
    )
    rows = _extract_index_rows(index_payload)
    sampled_keys = collect_sampled_keys(
        metadata_dirs=unique_metadata_dirs,
        manifest_path=manifest_path if manifest_path.exists() else None,
    )
    candidates = build_candidate_pool(
        rows=rows,
        session_years=session_years,
        sampled_keys=sampled_keys,
    )
    selected = sample_candidates(
        candidates=candidates,
        sample_size=requested,
        seed=seed,
    )
    successes, failures = _materialize_candidates(
        candidates=selected,
        csv_out_dir=csv_out_dir,
        metadata_out_dir=metadata_out_dir,
        rate_limit_seconds=max(0.0, float(rate_limit_seconds)),
        timeout_seconds=float(timeout_seconds),
        max_retries=max(0, int(max_retries)),
        retry_backoff_seconds=max(0.1, float(retry_backoff_seconds)),
        overwrite=bool(overwrite),
    )

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "session_years": [int(year) for year in session_years],
        "sample_size_requested": int(requested),
        "sample_size_selected": int(len(selected)),
        "sample_size_downloaded": int(len(successes)),
        "sample_size_failed": int(len(failures)),
        "seed": seed,
        "rate_limit_seconds": float(max(0.0, rate_limit_seconds)),
        "index_json_path": str(index_json_path),
        "index_csv_path": str(index_csv_path),
        "index_refreshed": bool(index_refreshed),
        "index_row_count": int(len(rows)),
        "candidate_pool_count": int(len(candidates)),
        "existing_sampled_key_count": int(len(sampled_keys)),
        "sampled_metadata_dirs": [str(path) for path in unique_metadata_dirs],
        "selected_candidates": [candidate.to_dict() for candidate in selected],
        "successes": successes,
        "failures": failures,
    }


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sample N unsampled bill-hearing agenda items from the most recent legislative "
            "sessions and materialize CSI CSV + hearing metadata sidecars."
        )
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        required=True,
        help="Number of unsampled hearings to select and download.",
    )
    parser.add_argument(
        "--session-count",
        type=int,
        default=3,
        help="Number of most-recent session years to include.",
    )
    parser.add_argument(
        "--index-json",
        default=str(DEFAULT_INDEX_JSON),
        help="Meeting/bill index JSON path used as candidate source.",
    )
    parser.add_argument(
        "--index-csv",
        default=str(DEFAULT_INDEX_CSV),
        help="Meeting/bill index CSV path written when index is refreshed.",
    )
    parser.add_argument(
        "--csv-out-dir",
        default=str(DEFAULT_CSV_OUT_DIR),
        help="Directory where sampled CSV files will be written.",
    )
    parser.add_argument(
        "--metadata-out-dir",
        default=str(DEFAULT_METADATA_OUT_DIR),
        help="Directory where sampled hearing metadata sidecars will be written.",
    )
    parser.add_argument(
        "--manifest-out",
        default=str(DEFAULT_MANIFEST_OUT),
        help="Output JSON manifest path for this sampling run.",
    )
    parser.add_argument(
        "--sampled-metadata-dir",
        action="append",
        default=[],
        help=(
            "Additional metadata directory with existing sidecars to treat as already sampled. "
            "Repeatable."
        ),
    )
    parser.add_argument(
        "--refresh-index",
        action="store_true",
        help="Force rebuilding the meeting/bill index before sampling.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed for reproducible sampling.",
    )
    parser.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=1.0,
        help="Sleep interval between sampled downloads.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds for index/build and CSI calls.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Retry count for transient CSI errors.",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=1.5,
        help="Base retry backoff seconds for CSI requests.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow overwriting existing sampled CSV/sidecar outputs.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    index_json_path = Path(args.index_json).resolve()
    index_csv_path = Path(args.index_csv).resolve()
    csv_out_dir = Path(args.csv_out_dir).resolve()
    metadata_out_dir = Path(args.metadata_out_dir).resolve()
    manifest_path = Path(args.manifest_out).resolve()
    sampled_metadata_dirs = [Path(path).resolve() for path in args.sampled_metadata_dir]
    manifest = sample_unsampled_baseline_corpus(
        sample_size=int(args.sample_size),
        session_count=int(args.session_count),
        index_json_path=index_json_path,
        index_csv_path=index_csv_path,
        csv_out_dir=csv_out_dir,
        metadata_out_dir=metadata_out_dir,
        manifest_path=manifest_path,
        sampled_metadata_dirs=sampled_metadata_dirs,
        refresh_index=bool(args.refresh_index),
        seed=args.seed,
        timeout_seconds=float(args.timeout_seconds),
        max_retries=int(args.max_retries),
        retry_backoff_seconds=float(args.retry_backoff_seconds),
        rate_limit_seconds=float(args.rate_limit_seconds),
        overwrite=bool(args.overwrite),
    )
    write_manifest(manifest_path, manifest)
    print(
        "Sampled baseline corpus: "
        f"selected={int(manifest['sample_size_selected'])} "
        f"downloaded={int(manifest['sample_size_downloaded'])} "
        f"failed={int(manifest['sample_size_failed'])}"
    )
    print(f"Wrote manifest: {manifest_path}")


if __name__ == "__main__":
    main()
