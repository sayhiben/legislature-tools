from __future__ import annotations

import csv
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import OpenerDirector, Request, build_opener
from zoneinfo import ZoneInfo

import yaml

from testifier_audit.io.hearing_metadata import PACIFIC_TIMEZONE_NAME

CSI_BASE_URL = "https://app.leg.wa.gov/csi/Home"
RETRIABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_USER_AGENT = "wa-leg-testifier-audit/0.1 (+https://app.leg.wa.gov/csi)"

CSV_COLUMNS = ("Group", "Name", "Organization", "Position", "Time Signed In")
TESTIFYING_GROUP_LABEL = "Testifying"
NOT_TESTIFYING_GROUP_LABEL = "Not Testifying"


class CSIDownloadError(RuntimeError):
    """Raised when CSI testifier download or parsing fails."""


@dataclass(frozen=True)
class CSIMeeting:
    leg_id: str
    committee_id: str
    committee_name: str
    chamber: str
    meeting_family_id: str
    bill_title: str
    bill_number: str
    short_bill_id: str
    chamber_abbr: str
    meeting_date_time_formatted: str
    meeting_start: datetime


@dataclass(frozen=True)
class CSIAgendaItem:
    agenda_item_family_id: str
    agenda_item_id: str
    description: str


@dataclass(frozen=True)
class CSITestifierRow:
    group: str
    name: str
    organization: str
    position: str
    time_signed_in: str


@dataclass(frozen=True)
class CSIDownloadResult:
    search_query: str
    csv_path: Path
    metadata_path: Path
    short_bill_id: str
    bill_title: str
    meeting_family_id: str
    agenda_item_family_id: str
    agenda_item_id: str
    meeting_start: datetime
    testifying_rows: int
    not_testifying_rows: int

    @property
    def total_rows(self) -> int:
        return self.testifying_rows + self.not_testifying_rows


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _normalize_match_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _sanitize_bill_token(value: str) -> str:
    token = re.sub(r"[^A-Z0-9]+", "", value.upper())
    return token or "BILL"


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    return _normalize_whitespace(str(value))


def _parse_meeting_start(raw_value: str) -> datetime:
    if not raw_value:
        raise CSIDownloadError("Search payload missing MeetingDateTime")
    try:
        parsed = datetime.fromisoformat(raw_value)
    except ValueError as exc:
        raise CSIDownloadError(f"Invalid MeetingDateTime value: {raw_value}") from exc

    pacific = ZoneInfo(PACIFIC_TIMEZONE_NAME)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=pacific)
    return parsed.astimezone(pacific)


def _meeting_from_payload(payload: Mapping[str, Any]) -> CSIMeeting:
    return CSIMeeting(
        leg_id=_coerce_text(payload.get("LegId")),
        committee_id=_coerce_text(payload.get("CommitteeId")),
        committee_name=_coerce_text(payload.get("CommitteeName")),
        chamber=_coerce_text(payload.get("Chamber")),
        meeting_family_id=_coerce_text(payload.get("MeetingFamilyId")),
        bill_title=_coerce_text(payload.get("BillTitle")),
        bill_number=_coerce_text(payload.get("BillNumber")),
        short_bill_id=_coerce_text(payload.get("ShortBillId")),
        chamber_abbr=_coerce_text(payload.get("ChamberAbbr")),
        meeting_date_time_formatted=_coerce_text(payload.get("MeetingDateTimeFormatted")),
        meeting_start=_parse_meeting_start(_coerce_text(payload.get("MeetingDateTime"))),
    )


def _build_search_url(query: str, top: int) -> str:
    escaped_query = query.lower().replace("'", "''")
    filter_clause = (
        f"(substringof('{escaped_query}',tolower(cast(shortBillId, 'Edm.String')))) "
        f"or (substringof('{escaped_query}',tolower(cast(billTitle, 'Edm.String')))) "
        f"or (substringof('{escaped_query}',tolower(cast(committeeName, 'Edm.String')))) "
        f"or (substringof('{escaped_query}',tolower(cast(meetingDateTimeFormatted, 'Edm.String'))))"
    )
    query_string = "&".join(
        [
            "$inlinecount=allpages",
            f"$filter={quote(filter_clause, safe='()$,')}",
            f"$orderby={quote('meetingDateTimeFormatted desc')}",
            "$skip=0",
            f"$top={top}",
        ]
    )
    return f"{CSI_BASE_URL}/SearchMeetings/?{query_string}"


def _decode_response_body(body: bytes) -> str:
    return body.decode("utf-8", errors="replace")


def _request_text_with_retries(
    *,
    opener: OpenerDirector,
    url: str,
    logger: logging.Logger,
    timeout_seconds: float,
    max_retries: int,
    retry_backoff_seconds: float,
    user_agent: str,
    accept: str,
) -> str:
    attempt = 0
    while True:
        attempt += 1
        start = time.monotonic()
        request = Request(url, headers={"User-Agent": user_agent, "Accept": accept})
        try:
            with opener.open(request, timeout=timeout_seconds) as response:
                body = response.read()
                elapsed = time.monotonic() - start
                status = int(getattr(response, "status", response.getcode()))
                logger.info(
                    "HTTP %s %s (attempt=%s bytes=%s elapsed=%.2fs)",
                    status,
                    url,
                    attempt,
                    len(body),
                    elapsed,
                )
                return _decode_response_body(body)
        except HTTPError as exc:
            elapsed = time.monotonic() - start
            status = int(exc.code)
            body_excerpt = _decode_response_body(exc.read())[:240].replace("\n", " ")
            retries_used = attempt - 1
            if status in RETRIABLE_STATUS_CODES and retries_used < max_retries:
                delay = retry_backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "HTTP %s for %s (attempt=%s elapsed=%.2fs). Retrying in %.1fs.",
                    status,
                    url,
                    attempt,
                    elapsed,
                    delay,
                )
                if body_excerpt:
                    logger.debug("Response excerpt: %s", body_excerpt)
                time.sleep(delay)
                continue
            raise CSIDownloadError(
                f"Request failed with HTTP {status} for {url}. Response excerpt: {body_excerpt}"
            ) from exc
        except URLError as exc:
            elapsed = time.monotonic() - start
            retries_used = attempt - 1
            if retries_used < max_retries:
                delay = retry_backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "Network error for %s (attempt=%s elapsed=%.2fs): %s. Retrying in %.1fs.",
                    url,
                    attempt,
                    elapsed,
                    exc,
                    delay,
                )
                time.sleep(delay)
                continue
            raise CSIDownloadError(f"Request failed for {url}: {exc}") from exc


def _parse_search_meetings(text: str) -> list[CSIMeeting]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise CSIDownloadError("SearchMeetings response is not valid JSON") from exc

    items = payload.get("Items")
    if not isinstance(items, list):
        raise CSIDownloadError("SearchMeetings response missing list field 'Items'")

    meetings: list[CSIMeeting] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        meetings.append(_meeting_from_payload(item))
    return meetings


def _rank_meeting_for_query(meeting: CSIMeeting, query: str) -> int:
    query_text = _normalize_whitespace(query).lower()
    query_token = _normalize_match_token(query)
    short_bill_token = _normalize_match_token(meeting.short_bill_id)
    bill_number_token = _normalize_match_token(meeting.bill_number)

    score = 0
    if query_token:
        if query_token == short_bill_token:
            score = max(score, 100)
        elif query_token == bill_number_token:
            score = max(score, 95)
        elif short_bill_token.startswith(query_token):
            score = max(score, 90)
        elif query_token in short_bill_token:
            score = max(score, 80)
        elif query_token in _normalize_match_token(meeting.bill_title):
            score = max(score, 70)
        elif query_token in _normalize_match_token(meeting.committee_name):
            score = max(score, 60)

    if query_text:
        if query_text in meeting.bill_title.lower():
            score = max(score, 70)
        elif query_text in meeting.short_bill_id.lower():
            score = max(score, 80)
        elif query_text in meeting.committee_name.lower():
            score = max(score, 60)
    return score


def _select_meeting(
    *,
    meetings: list[CSIMeeting],
    bill_query: str,
    meeting_index: int,
    meeting_family_id: str | None,
) -> CSIMeeting:
    if not meetings:
        raise CSIDownloadError(f"No meetings matched search query: {bill_query!r}")

    if meeting_family_id:
        for meeting in meetings:
            if meeting.meeting_family_id == meeting_family_id:
                return meeting
        raise CSIDownloadError(
            f"Meeting family id {meeting_family_id!r} was not returned for query {bill_query!r}"
        )

    rankings = [_rank_meeting_for_query(meeting, bill_query) for meeting in meetings]
    best_score = max(rankings)
    if best_score > 0:
        candidates = [
            meeting
            for meeting, score in zip(meetings, rankings, strict=True)
            if score == best_score
        ]
    else:
        candidates = meetings

    if meeting_index >= len(candidates):
        candidate_count = len(candidates)
        raise CSIDownloadError(
            f"Requested meeting-index {meeting_index} but only {candidate_count} matching "
            f"meeting(s) are available for query {bill_query!r}"
        )
    return candidates[meeting_index]


def _parse_onclick_agenda_ids(onclick_value: str) -> tuple[str | None, str | None]:
    numbers = re.findall(r"\d+", onclick_value)
    if len(numbers) < 3:
        return None, None
    # getTestimonyTypes(..., meetingFamilyId, agendaItemFamilyId, agendaItemId)
    return numbers[-2], numbers[-1]


class _AgendaItemsParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.items: list[CSIAgendaItem] = []
        self._capture_text = False
        self._current_attrs: dict[str, str] = {}
        self._text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "button":
            return
        attributes = {key: value or "" for key, value in attrs}
        classes = set(attributes.get("class", "").split())
        if "agendaItem" not in classes and "selectedAgendaItem" not in classes:
            return
        self._capture_text = True
        self._current_attrs = attributes
        self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._capture_text:
            self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "button" or not self._capture_text:
            return
        button_id = self._current_attrs.get("id", "")
        onclick = self._current_attrs.get("onclick", "")
        family_id_match = re.search(r"agendaItem-(\d+)", button_id)
        agenda_item_family_id = family_id_match.group(1) if family_id_match else ""
        onclick_family_id, agenda_item_id = _parse_onclick_agenda_ids(onclick)
        if onclick_family_id:
            agenda_item_family_id = onclick_family_id

        description = _normalize_whitespace("".join(self._text_parts))
        if agenda_item_family_id and agenda_item_id:
            self.items.append(
                CSIAgendaItem(
                    agenda_item_family_id=agenda_item_family_id,
                    agenda_item_id=agenda_item_id,
                    description=description,
                )
            )
        self._capture_text = False
        self._current_attrs = {}
        self._text_parts = []


def parse_agenda_items(html_text: str) -> list[CSIAgendaItem]:
    parser = _AgendaItemsParser()
    parser.feed(html_text)
    return parser.items


def _rank_agenda_item(agenda_item: CSIAgendaItem, meeting: CSIMeeting) -> int:
    description_token = _normalize_match_token(agenda_item.description)
    bill_token = _normalize_match_token(meeting.short_bill_id)
    number_token = _normalize_match_token(meeting.bill_number)

    score = 0
    if bill_token:
        if description_token.startswith(bill_token):
            score = max(score, 100)
        elif bill_token in description_token:
            score = max(score, 90)
    if number_token and number_token in description_token:
        score = max(score, 80)
    return score


def _select_agenda_item(
    *,
    agenda_items: list[CSIAgendaItem],
    meeting: CSIMeeting,
    agenda_index: int,
    agenda_item_id: str | None,
) -> CSIAgendaItem:
    if not agenda_items:
        raise CSIDownloadError(
            f"No agenda items found for chamber={meeting.chamber} "
            f"meetingFamilyId={meeting.meeting_family_id}"
        )

    if agenda_item_id:
        for item in agenda_items:
            if item.agenda_item_id == agenda_item_id:
                return item
        raise CSIDownloadError(
            f"Agenda item id {agenda_item_id!r} was not returned for "
            f"meetingFamilyId={meeting.meeting_family_id}"
        )

    rankings = [_rank_agenda_item(item, meeting) for item in agenda_items]
    best_score = max(rankings)
    if best_score > 0:
        candidates = [
            item for item, score in zip(agenda_items, rankings, strict=True) if score == best_score
        ]
    elif len(agenda_items) == 1:
        candidates = agenda_items
    else:
        choices = ", ".join(item.description for item in agenda_items[:5])
        raise CSIDownloadError(
            "Could not infer the target agenda item for bill "
            f"{meeting.short_bill_id!r}. Pass --agenda-item-id to disambiguate. "
            f"First options: {choices}"
        )

    if agenda_index >= len(candidates):
        raise CSIDownloadError(
            f"Requested agenda-index {agenda_index} but only {len(candidates)} matching agenda "
            f"item(s) were found for bill {meeting.short_bill_id!r}"
        )
    return candidates[agenda_index]


class _DataJsonPayloadParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.payloads: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "div":
            return
        attributes = {key: value or "" for key, value in attrs}
        div_id = attributes.get("id")
        is_target_div = div_id in {"testifyingDataTable", "notTestifyingDataTable"}
        if is_target_div and "data-json" in attributes:
            self.payloads[div_id] = attributes["data-json"]


def _parse_group_rows(raw_payload: str, group_label: str) -> list[CSITestifierRow]:
    if not raw_payload.strip():
        return []
    try:
        payload = json.loads(unescape(raw_payload))
    except json.JSONDecodeError as exc:
        raise CSIDownloadError(f"Invalid data-json payload for {group_label}") from exc

    if not isinstance(payload, list):
        raise CSIDownloadError(f"Expected list payload for {group_label}")

    rows: list[CSITestifierRow] = []
    for item in payload:
        if not isinstance(item, Mapping):
            continue
        rows.append(
            CSITestifierRow(
                group=group_label,
                name=_coerce_text(item.get("Name")),
                organization=_coerce_text(item.get("Organization")),
                position=_coerce_text(item.get("Position")),
                time_signed_in=_coerce_text(item.get("TimeSignedIn")),
            )
        )
    return rows


def parse_testifier_rows(html_text: str) -> tuple[list[CSITestifierRow], list[CSITestifierRow]]:
    parser = _DataJsonPayloadParser()
    parser.feed(html_text)

    testifying_rows = _parse_group_rows(
        parser.payloads.get("testifyingDataTable", ""),
        TESTIFYING_GROUP_LABEL,
    )
    not_testifying_rows = _parse_group_rows(
        parser.payloads.get("notTestifyingDataTable", ""),
        NOT_TESTIFYING_GROUP_LABEL,
    )
    return testifying_rows, not_testifying_rows


def _build_output_stem(meeting: CSIMeeting) -> str:
    bill_token = _sanitize_bill_token(meeting.short_bill_id or meeting.bill_number)
    return f"{bill_token}-{meeting.meeting_start:%Y%m%d}-{meeting.meeting_start:%H%M}"


def _write_csv(path: Path, rows: list[CSITestifierRow]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(CSV_COLUMNS)
        for row in rows:
            writer.writerow(
                [
                    row.group,
                    row.name,
                    row.organization,
                    row.position,
                    row.time_signed_in,
                ]
            )


def _build_sidecar_stats(
    *,
    testifying_rows: list[CSITestifierRow],
    not_testifying_rows: list[CSITestifierRow],
) -> dict[str, int | float]:
    combined_rows = [*testifying_rows, *not_testifying_rows]
    total_pro = 0
    total_con = 0
    total_other = 0

    for row in combined_rows:
        position = _normalize_whitespace(row.position).lower()
        if position == "pro":
            total_pro += 1
        elif position == "con":
            total_con += 1
        elif position == "other":
            total_other += 1

    total_rows = len(combined_rows)
    if total_rows > 0:
        total_pro_pct = (total_pro / total_rows) * 100.0
        total_con_pct = (total_con / total_rows) * 100.0
        total_other_pct = (total_other / total_rows) * 100.0
    else:
        total_pro_pct = 0.0
        total_con_pct = 0.0
        total_other_pct = 0.0

    return {
        "total_rows": total_rows,
        "total_testifying": len(testifying_rows),
        "total_not_testifying": len(not_testifying_rows),
        "total_pro": total_pro,
        "total_con": total_con,
        "total_other": total_other,
        "total_pro_pct": total_pro_pct,
        "total_con_pct": total_con_pct,
        "total_other_pct": total_other_pct,
    }


def _build_sidecar_payload(
    *,
    search_query: str,
    meeting: CSIMeeting,
    agenda_item: CSIAgendaItem,
    stats: Mapping[str, int | float],
) -> dict[str, Any]:
    hearing_id = (
        f"{_sanitize_bill_token(meeting.short_bill_id or meeting.bill_number)}-"
        f"{meeting.meeting_family_id}-{agenda_item.agenda_item_id}"
    )
    return {
        "schema_version": 1,
        "hearing_id": hearing_id,
        "timezone": PACIFIC_TIMEZONE_NAME,
        "meeting_start": meeting.meeting_start.isoformat(),
        "stats": dict(stats),
        "source": {
            "provider": "wa_leg_csi",
            "search_query": search_query,
            "meeting_family_id": meeting.meeting_family_id,
            "agenda_item_family_id": agenda_item.agenda_item_family_id,
            "agenda_item_id": agenda_item.agenda_item_id,
            "agenda_item_description": agenda_item.description,
            "short_bill_id": meeting.short_bill_id,
            "bill_number": meeting.bill_number,
            "bill_title": meeting.bill_title,
            "committee_name": meeting.committee_name,
            "chamber": meeting.chamber,
            "downloaded_at": datetime.now(tz=timezone.utc).isoformat(),
        },
    }


def _write_sidecar(path: Path, payload: Mapping[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(dict(payload), handle, sort_keys=False, allow_unicode=False)


def download_csi_testifier_csv(
    *,
    bill_query: str,
    csv_out_dir: Path,
    metadata_out_dir: Path,
    meeting_index: int = 0,
    agenda_index: int = 0,
    meeting_family_id: str | None = None,
    agenda_item_id: str | None = None,
    top: int = 100,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
    overwrite: bool = True,
    user_agent: str = DEFAULT_USER_AGENT,
    logger: logging.Logger | None = None,
) -> CSIDownloadResult:
    query = _normalize_whitespace(bill_query)
    if not query:
        raise CSIDownloadError("bill_query must be a non-empty string")
    if top < 1:
        raise CSIDownloadError("top must be >= 1")

    active_logger = logger or logging.getLogger(__name__)
    opener = build_opener()

    search_url = _build_search_url(query=query, top=top)
    active_logger.info("Searching CSI meetings for query=%r", query)
    search_text = _request_text_with_retries(
        opener=opener,
        url=search_url,
        logger=active_logger,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        user_agent=user_agent,
        accept="application/json",
    )
    meetings = _parse_search_meetings(search_text)
    meeting = _select_meeting(
        meetings=meetings,
        bill_query=query,
        meeting_index=meeting_index,
        meeting_family_id=meeting_family_id,
    )
    active_logger.info(
        "Selected meeting: short_bill_id=%s meeting_family_id=%s chamber=%s meeting_start=%s",
        meeting.short_bill_id,
        meeting.meeting_family_id,
        meeting.chamber,
        meeting.meeting_start.isoformat(),
    )

    agenda_query = urlencode(
        {
            "chamber": meeting.chamber.lower(),
            "meetingFamilyId": meeting.meeting_family_id,
        }
    )
    agenda_url = f"{CSI_BASE_URL}/GetAgendaItems/?{agenda_query}"
    active_logger.info(
        "Loading agenda items for chamber=%s meeting_family_id=%s",
        meeting.chamber.lower(),
        meeting.meeting_family_id,
    )
    agenda_html = _request_text_with_retries(
        opener=opener,
        url=agenda_url,
        logger=active_logger,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        user_agent=user_agent,
        accept="text/html",
    )
    agenda_items = parse_agenda_items(agenda_html)
    agenda_item = _select_agenda_item(
        agenda_items=agenda_items,
        meeting=meeting,
        agenda_index=agenda_index,
        agenda_item_id=agenda_item_id,
    )
    active_logger.info(
        "Selected agenda item: agenda_item_id=%s agenda_item_family_id=%s description=%r",
        agenda_item.agenda_item_id,
        agenda_item.agenda_item_family_id,
        agenda_item.description,
    )

    testifiers_query = urlencode({"agendaItemId": agenda_item.agenda_item_id})
    testifiers_url = f"{CSI_BASE_URL}/GetOtherTestifiers/?{testifiers_query}"
    active_logger.info("Fetching testifiers for agenda_item_id=%s", agenda_item.agenda_item_id)
    testifiers_html = _request_text_with_retries(
        opener=opener,
        url=testifiers_url,
        logger=active_logger,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        user_agent=user_agent,
        accept="text/html",
    )
    testifying_rows, not_testifying_rows = parse_testifier_rows(testifiers_html)

    combined_rows = [*testifying_rows, *not_testifying_rows]
    active_logger.info(
        "Parsed testifier rows: testifying=%s not_testifying=%s total=%s",
        len(testifying_rows),
        len(not_testifying_rows),
        len(combined_rows),
    )

    output_stem = _build_output_stem(meeting)
    csv_path = csv_out_dir / f"{output_stem}.csv"
    metadata_path = metadata_out_dir / f"{output_stem}.hearing.yaml"

    csv_out_dir.mkdir(parents=True, exist_ok=True)
    metadata_out_dir.mkdir(parents=True, exist_ok=True)

    if not overwrite and csv_path.exists():
        raise CSIDownloadError(f"CSV already exists and overwrite is disabled: {csv_path}")
    if not overwrite and metadata_path.exists():
        raise CSIDownloadError(
            f"Hearing metadata sidecar already exists and overwrite is disabled: {metadata_path}"
        )

    if overwrite and csv_path.exists():
        active_logger.warning("Overwriting existing CSV: %s", csv_path)
    if overwrite and metadata_path.exists():
        active_logger.warning("Overwriting existing hearing metadata sidecar: %s", metadata_path)

    _write_csv(csv_path, combined_rows)
    sidecar_stats = _build_sidecar_stats(
        testifying_rows=testifying_rows,
        not_testifying_rows=not_testifying_rows,
    )
    sidecar_payload = _build_sidecar_payload(
        search_query=query,
        meeting=meeting,
        agenda_item=agenda_item,
        stats=sidecar_stats,
    )
    _write_sidecar(metadata_path, sidecar_payload)
    active_logger.info("Wrote CSV: %s", csv_path)
    active_logger.info("Wrote hearing metadata sidecar: %s", metadata_path)

    return CSIDownloadResult(
        search_query=query,
        csv_path=csv_path,
        metadata_path=metadata_path,
        short_bill_id=meeting.short_bill_id,
        bill_title=meeting.bill_title,
        meeting_family_id=meeting.meeting_family_id,
        agenda_item_family_id=agenda_item.agenda_item_family_id,
        agenda_item_id=agenda_item.agenda_item_id,
        meeting_start=meeting.meeting_start,
        testifying_rows=len(testifying_rows),
        not_testifying_rows=len(not_testifying_rows),
    )
