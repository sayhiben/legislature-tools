from __future__ import annotations

import argparse
import hashlib
import json
import logging
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
    download_csi_testifier_csv_by_meeting_family,
)
from testifier_audit.io.hearing_metadata import PACIFIC_TIMEZONE_NAME
from testifier_audit.io.wa_committee_service import (
    CommitteeMeetingItem,
    fetch_committee_meeting_items,
    fetch_committee_meetings,
)

DEFAULT_INDEX_JSON = Path("data/metadata/wa_committee_meetings_cache.json")
DEFAULT_INDEX_CSV = Path("data/metadata/wa_meeting_bill_index.csv")
DEFAULT_MEETING_ITEMS_CACHE_DIR = Path("data/metadata/wa_committee_meeting_items")
DEFAULT_MEETINGS_CACHE_MAX_AGE_HOURS = 12.0
DEFAULT_CSV_OUT_DIR = Path("data/raw")
DEFAULT_METADATA_OUT_DIR = Path("data/metadata")
DEFAULT_MANIFEST_OUT = Path("data/metadata/baseline_sample_manifest.json")
LOGGER = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class BaselineMeetingCandidate:
    agenda_id: str
    meeting_start: datetime
    revised_date: str
    chamber: str
    committee_name: str

    @property
    def session_year(self) -> int:
        return int(self.meeting_start.year)

    def to_dict(self) -> dict[str, Any]:
        return {
            "agenda_id": self.agenda_id,
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


def _parse_utc_timestamp(value: str) -> datetime | None:
    raw = _safe_text(value)
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_meeting_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        agenda_id = _safe_text(row.get("agenda_id"))
        meeting_date = _safe_text(row.get("meeting_date"))
        if not agenda_id or not meeting_date:
            continue
        normalized_row = {
            "agenda_id": agenda_id,
            "meeting_date": meeting_date,
            "revised_date": _safe_text(row.get("revised_date")),
            "agency": _safe_text(row.get("agency")),
            "committee_acronym": _safe_text(row.get("committee_acronym")),
            "committee_name": _safe_text(row.get("committee_name")),
        }
        prior = deduped.get(agenda_id)
        if prior is None or normalized_row["revised_date"] > prior["revised_date"]:
            deduped[agenda_id] = normalized_row
    normalized = list(deduped.values())
    normalized.sort(
        key=lambda row: (
            str(row.get("meeting_date") or ""),
            str(row.get("agenda_id") or ""),
        )
    )
    return normalized


def _meeting_row_from_service(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "agenda_id": _safe_text(row.get("agenda_id")),
        "meeting_date": _safe_text(row.get("meeting_date")),
        "revised_date": _safe_text(row.get("revised_date")),
        "agency": _safe_text(row.get("agency")),
        "committee_acronym": _safe_text(row.get("committee_acronym")),
        "committee_name": _safe_text(row.get("committee_name")),
    }


def _build_meetings_cache_payload(
    *,
    start_date: date,
    end_date: date,
    rows: Sequence[Mapping[str, Any]],
    max_age_hours: float,
) -> dict[str, Any]:
    normalized_rows = [_meeting_row_from_service(row) for row in _normalize_meeting_rows(rows)]
    return {
        "schema_version": 1,
        "retrieved_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "source": {
            "service": "wa_committee_meeting_service",
            "endpoint": "GetCommitteeMeetings",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "include_revised": False,
        },
        "cache": {
            "max_age_hours": float(max_age_hours),
        },
        "row_count": int(len(normalized_rows)),
        "rows": normalized_rows,
    }


def _load_json_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"rows": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"rows": []}
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        return {"rows": payload}
    return {"rows": []}


def _meetings_cache_is_stale(
    *,
    payload: Mapping[str, Any],
    max_age_hours: float,
    now_utc: datetime | None = None,
) -> bool:
    if max_age_hours <= 0:
        return False
    retrieved_at = _parse_utc_timestamp(_safe_text(payload.get("retrieved_at_utc")))
    if retrieved_at is None:
        return True
    reference = now_utc or datetime.now(tz=timezone.utc)
    age_hours = (reference - retrieved_at).total_seconds() / 3600.0
    return age_hours > float(max_age_hours)


def _build_meeting_pool(
    *,
    rows: Sequence[Mapping[str, Any]],
    session_years: Sequence[int],
) -> list[BaselineMeetingCandidate]:
    target_years = set(int(year) for year in session_years)
    deduped: dict[str, BaselineMeetingCandidate] = {}
    for row in rows:
        agenda_id = _safe_text(row.get("agenda_id"))
        meeting_start = _parse_meeting_start(_safe_text(row.get("meeting_date")))
        if not agenda_id or meeting_start is None:
            continue
        if int(meeting_start.year) not in target_years:
            continue
        candidate = BaselineMeetingCandidate(
            agenda_id=agenda_id,
            meeting_start=meeting_start,
            revised_date=_safe_text(row.get("revised_date")),
            chamber=_normalize_chamber(_safe_text(row.get("agency"))),
            committee_name=_normalize_committee(_safe_text(row.get("committee_name"))),
        )
        prior = deduped.get(agenda_id)
        if prior is None or candidate.revised_date > prior.revised_date:
            deduped[agenda_id] = candidate
    return list(deduped.values())


def _meeting_items_cache_path(*, meeting_items_cache_dir: Path, agenda_id: str) -> Path:
    return meeting_items_cache_dir / f"{agenda_id}.json"


def _normalize_meeting_item_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        bill_id = _safe_text(row.get("bill_id"))
        if not bill_id:
            continue
        out.append(
            {
                "agenda_item_id": _safe_text(row.get("agenda_item_id")),
                "hearing_type_description": _safe_text(row.get("hearing_type_description")),
                "bill_id": bill_id,
                "item_description": _safe_text(row.get("item_description")),
            }
        )
    return out


def _meeting_item_row_from_service(item: CommitteeMeetingItem) -> dict[str, Any]:
    return {
        "agenda_item_id": _safe_text(item.agenda_item_id),
        "hearing_type_description": _safe_text(item.hearing_type_description),
        "bill_id": _safe_text(item.bill_id),
        "item_description": _safe_text(item.item_description),
    }


def _load_meeting_items_cache_rows(cache_path: Path) -> list[dict[str, Any]] | None:
    if not cache_path.exists():
        return None
    payload = _load_json_payload(cache_path)
    raw_rows = payload.get("rows") if isinstance(payload, Mapping) else None
    if not isinstance(raw_rows, list):
        return None
    rows = _extract_index_rows(payload)
    return _normalize_meeting_item_rows(rows)


def _write_meeting_items_cache(
    *,
    cache_path: Path,
    agenda_id: str,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "retrieved_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "agenda_id": str(agenda_id),
        "row_count": int(len(rows)),
        "rows": list(rows),
    }
    cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _load_or_fetch_meeting_items_rows(
    *,
    agenda_id: str,
    meeting_items_cache_dir: Path,
    timeout_seconds: float,
) -> tuple[list[dict[str, Any]], bool]:
    cache_path = _meeting_items_cache_path(
        meeting_items_cache_dir=meeting_items_cache_dir,
        agenda_id=agenda_id,
    )
    cached_rows = _load_meeting_items_cache_rows(cache_path)
    if cached_rows is not None:
        LOGGER.debug(
            "Meeting-items cache hit agenda_id=%s rows=%s cache_path=%s",
            agenda_id,
            len(cached_rows),
            cache_path,
        )
        return cached_rows, False

    LOGGER.info(
        "Meeting-items request start agenda_id=%s timeout=%.1fs",
        agenda_id,
        float(timeout_seconds),
    )
    fetched = fetch_committee_meeting_items(
        agenda_id=agenda_id,
        timeout_seconds=timeout_seconds,
    )
    fetched_rows = _normalize_meeting_item_rows(
        [_meeting_item_row_from_service(item) for item in fetched]
    )
    _write_meeting_items_cache(
        cache_path=cache_path,
        agenda_id=agenda_id,
        rows=fetched_rows,
    )
    LOGGER.info(
        "Meeting-items request complete agenda_id=%s rows=%s cache_path=%s",
        agenda_id,
        len(fetched_rows),
        cache_path,
    )
    return fetched_rows, True


def _ensure_index(
    *,
    index_json_path: Path,
    index_csv_path: Path,
    session_years: Sequence[int],
    timeout_seconds: float,
    refresh_index: bool,
    reference_date: date | None = None,
    meetings_cache_max_age_hours: float = DEFAULT_MEETINGS_CACHE_MAX_AGE_HOURS,
) -> tuple[dict[str, Any], bool]:
    del index_csv_path
    meetings_payload = _load_json_payload(index_json_path)
    meetings_rows = _extract_index_rows(meetings_payload)
    cache_stale = _meetings_cache_is_stale(
        payload=meetings_payload,
        max_age_hours=float(meetings_cache_max_age_hours),
    )
    has_year_coverage = _index_contains_session_years(
        rows=meetings_rows,
        session_years=session_years,
    )
    should_refresh = (
        bool(refresh_index) or (not meetings_rows) or cache_stale or (not has_year_coverage)
    )
    LOGGER.info(
        "Meetings index evaluation rows=%s refresh_requested=%s cache_stale=%s "
        "has_year_coverage=%s max_age_hours=%.1f",
        len(meetings_rows),
        bool(refresh_index),
        bool(cache_stale),
        bool(has_year_coverage),
        float(meetings_cache_max_age_hours),
    )
    if not should_refresh:
        LOGGER.info("Using cached meetings index path=%s", index_json_path)
        return meetings_payload, False

    start_date, end_date = _session_bounds(
        session_years=session_years,
        reference_date=reference_date,
    )
    LOGGER.info(
        "Refreshing meetings index path=%s begin=%s end=%s",
        index_json_path,
        start_date.isoformat(),
        end_date.isoformat(),
    )
    fetched_meetings = fetch_committee_meetings(
        begin_date=start_date,
        end_date=end_date,
        revised=False,
        timeout_seconds=float(timeout_seconds),
    )
    rows: list[dict[str, Any]] = []
    for meeting in fetched_meetings:
        rows.append(
            {
                "agenda_id": _safe_text(meeting.agenda_id),
                "meeting_date": _safe_text(meeting.meeting_date),
                "revised_date": _safe_text(meeting.revised_date),
                "agency": _safe_text(meeting.agency),
                "committee_acronym": _safe_text(meeting.acronym),
                "committee_name": _safe_text(meeting.long_name or meeting.committee_name),
            }
        )
    refreshed_payload = _build_meetings_cache_payload(
        start_date=start_date,
        end_date=end_date,
        rows=rows,
        max_age_hours=float(meetings_cache_max_age_hours),
    )
    index_json_path.parent.mkdir(parents=True, exist_ok=True)
    index_json_path.write_text(
        json.dumps(refreshed_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    LOGGER.info(
        "Wrote refreshed meetings index path=%s row_count=%s",
        index_json_path,
        len(rows),
    )
    return refreshed_payload, True


def _select_candidates_from_meetings(
    *,
    meetings: Sequence[BaselineMeetingCandidate],
    sample_size: int,
    sampled_keys: set[tuple[str, str]],
    meeting_items_cache_dir: Path,
    timeout_seconds: float,
    seed: int | None,
    max_uncached_meeting_item_fetches: int,
) -> tuple[list[BaselineSampleCandidate], dict[str, int]]:
    rng = random.Random(seed)
    cached_meetings: list[BaselineMeetingCandidate] = []
    uncached_meetings: list[BaselineMeetingCandidate] = []
    for meeting in meetings:
        cache_path = _meeting_items_cache_path(
            meeting_items_cache_dir=meeting_items_cache_dir,
            agenda_id=meeting.agenda_id,
        )
        if cache_path.exists():
            cached_meetings.append(meeting)
        else:
            uncached_meetings.append(meeting)
    rng.shuffle(cached_meetings)
    rng.shuffle(uncached_meetings)
    meeting_order = [*cached_meetings, *uncached_meetings]
    LOGGER.info(
        "Candidate selection start sample_size=%s meetings_total=%s cached=%s uncached=%s "
        "uncached_fetch_budget=%s",
        int(sample_size),
        len(meeting_order),
        len(cached_meetings),
        len(uncached_meetings),
        int(max_uncached_meeting_item_fetches),
    )

    selected: list[BaselineSampleCandidate] = []
    in_run_fallback_keys: set[tuple[str, str]] = set()
    seen_sampled_keys = set(sampled_keys)

    cache_hits = 0
    cache_misses = 0
    meetings_examined = 0
    meetings_without_items = 0
    meetings_skipped_fetch_budget = 0
    uncached_fetches = 0

    for meeting_index, meeting in enumerate(meeting_order, start=1):
        if len(selected) >= sample_size:
            break
        cache_path = _meeting_items_cache_path(
            meeting_items_cache_dir=meeting_items_cache_dir,
            agenda_id=meeting.agenda_id,
        )
        cache_exists = cache_path.exists()
        if (not cache_exists) and uncached_fetches >= max_uncached_meeting_item_fetches:
            LOGGER.debug(
                "Skipping meeting agenda_id=%s due to uncached-fetch budget exhaustion "
                "(used=%s budget=%s)",
                meeting.agenda_id,
                int(uncached_fetches),
                int(max_uncached_meeting_item_fetches),
            )
            meetings_skipped_fetch_budget += 1
            continue

        LOGGER.info(
            "Evaluating meeting %s/%s agenda_id=%s cached=%s selected_so_far=%s/%s",
            meeting_index,
            len(meeting_order),
            meeting.agenda_id,
            bool(cache_exists),
            len(selected),
            int(sample_size),
        )
        items_rows, was_fetched = _load_or_fetch_meeting_items_rows(
            agenda_id=meeting.agenda_id,
            meeting_items_cache_dir=meeting_items_cache_dir,
            timeout_seconds=float(timeout_seconds),
        )
        if was_fetched:
            cache_misses += 1
            uncached_fetches += 1
        else:
            cache_hits += 1
        meetings_examined += 1

        eligible_items: list[dict[str, Any]] = []
        for row in items_rows:
            bill_id = _safe_text(row.get("bill_id"))
            agenda_item_id = _safe_text(row.get("agenda_item_id"))
            if not bill_id:
                continue
            if agenda_item_id:
                if (meeting.agenda_id, agenda_item_id) in seen_sampled_keys:
                    continue
            else:
                fallback_key = (meeting.agenda_id, bill_id)
                if fallback_key in in_run_fallback_keys:
                    continue
            eligible_items.append(dict(row))

        if not eligible_items:
            meetings_without_items += 1
            LOGGER.debug(
                "Meeting agenda_id=%s has no eligible items after sampled-key filtering",
                meeting.agenda_id,
            )
            continue

        chosen_row = rng.choice(eligible_items)
        chosen_bill_id = _safe_text(chosen_row.get("bill_id"))
        chosen_agenda_item_id = _safe_text(chosen_row.get("agenda_item_id"))
        if chosen_agenda_item_id:
            seen_sampled_keys.add((meeting.agenda_id, chosen_agenda_item_id))
        else:
            in_run_fallback_keys.add((meeting.agenda_id, chosen_bill_id))

        selected.append(
            BaselineSampleCandidate(
                agenda_id=meeting.agenda_id,
                agenda_item_id=chosen_agenda_item_id,
                bill_id=chosen_bill_id,
                item_description=_safe_text(chosen_row.get("item_description")),
                hearing_type_description=_safe_text(chosen_row.get("hearing_type_description")),
                meeting_start=meeting.meeting_start,
                revised_date=meeting.revised_date,
                chamber=meeting.chamber,
                committee_name=meeting.committee_name,
            )
        )
        LOGGER.info(
            "Selected candidate %s/%s agenda_id=%s agenda_item_id=%s bill_id=%s",
            len(selected),
            int(sample_size),
            meeting.agenda_id,
            chosen_agenda_item_id or "(missing)",
            chosen_bill_id,
        )

    stats = {
        "meeting_items_cache_hits": int(cache_hits),
        "meeting_items_cache_misses": int(cache_misses),
        "meeting_items_uncached_fetches": int(uncached_fetches),
        "meetings_examined_count": int(meetings_examined),
        "meetings_without_eligible_items_count": int(meetings_without_items),
        "meetings_skipped_fetch_budget_count": int(meetings_skipped_fetch_budget),
    }
    LOGGER.info(
        "Candidate selection complete selected=%s/%s stats=%s",
        len(selected),
        int(sample_size),
        stats,
    )
    return selected, stats


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
        candidate_number = index + 1
        total_candidates = len(candidates)
        LOGGER.info(
            "Downloading candidate %s/%s agenda_id=%s agenda_item_id=%s bill_id=%s",
            candidate_number,
            total_candidates,
            candidate.agenda_id,
            candidate.agenda_item_id or "(missing)",
            candidate.bill_id,
        )
        if index > 0 and rate_limit_seconds > 0:
            LOGGER.info(
                "Rate-limit sleep %.2fs before candidate %s/%s",
                float(rate_limit_seconds),
                candidate_number,
                total_candidates,
            )
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
                    LOGGER.info(
                        "Attempting meeting-family fallback agenda_id=%s bill_id=%s",
                        candidate.agenda_id,
                        candidate.bill_id,
                    )
                    result = download_csi_testifier_csv_by_meeting_family(
                        bill_query=candidate.bill_id,
                        meeting_family_id=candidate.agenda_id,
                        chamber=candidate.chamber,
                        meeting_start=candidate.meeting_start,
                        short_bill_id=candidate.bill_id,
                        csv_out_dir=csv_out_dir,
                        metadata_out_dir=metadata_out_dir,
                        bill_number=_extract_bill_number(candidate.bill_id),
                        bill_title=candidate.item_description,
                        committee_name=candidate.committee_name,
                        timeout_seconds=timeout_seconds,
                        max_retries=max_retries,
                        retry_backoff_seconds=retry_backoff_seconds,
                        overwrite=overwrite,
                    )
                except CSIDownloadError as exc:
                    LOGGER.warning(
                        "Meeting-family fallback failed agenda_id=%s bill_id=%s: %s. "
                        "Falling back to search-based download.",
                        candidate.agenda_id,
                        candidate.bill_id,
                        exc,
                    )
                    result = download_csi_testifier_csv(
                        bill_query=candidate.bill_id,
                        csv_out_dir=csv_out_dir,
                        metadata_out_dir=metadata_out_dir,
                        meeting_year=candidate.session_year,
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
            LOGGER.info(
                "Download success candidate %s/%s agenda_id=%s agenda_item_id=%s total_rows=%s",
                candidate_number,
                total_candidates,
                result.meeting_family_id,
                result.agenda_item_id,
                int(result.total_rows),
            )
        except CSIDownloadError as exc:
            LOGGER.error(
                "Download failed candidate %s/%s agenda_id=%s agenda_item_id=%s bill_id=%s: %s",
                candidate_number,
                total_candidates,
                candidate.agenda_id,
                candidate.agenda_item_id or "(missing)",
                candidate.bill_id,
                exc,
            )
            failures.append({**candidate.to_dict(), "error": str(exc)})
    return successes, failures


def sample_unsampled_baseline_corpus(
    *,
    sample_size: int,
    session_count: int = 3,
    index_json_path: Path = DEFAULT_INDEX_JSON,
    index_csv_path: Path = DEFAULT_INDEX_CSV,
    meeting_items_cache_dir: Path = DEFAULT_MEETING_ITEMS_CACHE_DIR,
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
    meetings_cache_max_age_hours: float = DEFAULT_MEETINGS_CACHE_MAX_AGE_HOURS,
    max_meeting_items_fetches: int | None = None,
    overwrite: bool = False,
    reference_date: date | None = None,
) -> dict[str, Any]:
    requested = max(0, int(sample_size))
    uncached_fetch_budget = (
        int(requested)
        if max_meeting_items_fetches is None
        else max(0, int(max_meeting_items_fetches))
    )
    session_years = derive_recent_session_years(
        session_count=max(1, int(session_count)),
        reference_date=reference_date,
    )
    LOGGER.info(
        "Baseline sampling run start requested=%s session_years=%s seed=%s "
        "rate_limit_seconds=%.2f timeout_seconds=%.1f max_retries=%s",
        int(requested),
        session_years,
        seed,
        float(max(0.0, rate_limit_seconds)),
        float(timeout_seconds),
        int(max_retries),
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
        meetings_cache_max_age_hours=float(meetings_cache_max_age_hours),
    )
    meeting_rows = _extract_index_rows(index_payload)
    meetings = _build_meeting_pool(
        rows=meeting_rows,
        session_years=session_years,
    )
    LOGGER.info(
        "Meeting pool ready rows=%s candidates=%s index_refreshed=%s",
        len(meeting_rows),
        len(meetings),
        bool(index_refreshed),
    )
    sampled_keys = collect_sampled_keys(
        metadata_dirs=unique_metadata_dirs,
        manifest_path=manifest_path if manifest_path.exists() else None,
    )
    LOGGER.info(
        "Existing sampled-key coverage=%s metadata_dirs=%s",
        len(sampled_keys),
        [str(path) for path in unique_metadata_dirs],
    )
    selected, selection_stats = _select_candidates_from_meetings(
        meetings=meetings,
        sample_size=requested,
        sampled_keys=sampled_keys,
        meeting_items_cache_dir=meeting_items_cache_dir,
        timeout_seconds=float(timeout_seconds),
        seed=seed,
        max_uncached_meeting_item_fetches=uncached_fetch_budget,
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
    LOGGER.info(
        "Baseline sampling run complete selected=%s downloaded=%s failed=%s",
        len(selected),
        len(successes),
        len(failures),
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
        "meetings_cache_max_age_hours": float(max(0.0, meetings_cache_max_age_hours)),
        "max_meeting_items_fetches": int(uncached_fetch_budget),
        "index_json_path": str(index_json_path),
        "index_csv_path": str(index_csv_path),
        "meeting_items_cache_dir": str(meeting_items_cache_dir),
        "index_refreshed": bool(index_refreshed),
        "index_row_count": int(len(meeting_rows)),
        "candidate_pool_count": int(len(meetings)),
        "existing_sampled_key_count": int(len(sampled_keys)),
        "sampled_metadata_dirs": [str(path) for path in unique_metadata_dirs],
        "selected_candidates": [candidate.to_dict() for candidate in selected],
        "selection_stats": selection_stats,
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
        help="Cached GetCommitteeMeetings JSON path used as candidate source.",
    )
    parser.add_argument(
        "--index-csv",
        default=str(DEFAULT_INDEX_CSV),
        help="Legacy argument retained for compatibility; value is no longer used.",
    )
    parser.add_argument(
        "--meeting-items-cache-dir",
        default=str(DEFAULT_MEETING_ITEMS_CACHE_DIR),
        help=(
            "Directory containing per-meeting GetCommitteeMeetingItems cache files. "
            "Missing meeting caches are fetched on demand."
        ),
    )
    parser.add_argument(
        "--meetings-cache-max-age-hours",
        type=float,
        default=float(DEFAULT_MEETINGS_CACHE_MAX_AGE_HOURS),
        help="Max age for GetCommitteeMeetings cache before it is refreshed.",
    )
    parser.add_argument(
        "--max-meeting-items-fetches",
        type=int,
        default=None,
        help=(
            "Maximum uncached GetCommitteeMeetingItems fetches allowed in a single run. "
            "Defaults to sample-size when omitted."
        ),
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
        help="Force refreshing the cached GetCommitteeMeetings payload before sampling.",
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
    meeting_items_cache_dir = Path(args.meeting_items_cache_dir).resolve()
    csv_out_dir = Path(args.csv_out_dir).resolve()
    metadata_out_dir = Path(args.metadata_out_dir).resolve()
    manifest_path = Path(args.manifest_out).resolve()
    sampled_metadata_dirs = [Path(path).resolve() for path in args.sampled_metadata_dir]
    manifest = sample_unsampled_baseline_corpus(
        sample_size=int(args.sample_size),
        session_count=int(args.session_count),
        index_json_path=index_json_path,
        index_csv_path=index_csv_path,
        meeting_items_cache_dir=meeting_items_cache_dir,
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
        meetings_cache_max_age_hours=float(args.meetings_cache_max_age_hours),
        max_meeting_items_fetches=args.max_meeting_items_fetches,
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
