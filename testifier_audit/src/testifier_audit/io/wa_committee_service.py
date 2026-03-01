from __future__ import annotations

import logging
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from testifier_audit.io.http_rate_limit import wait_for_global_http_slot

SERVICE_BASE_URL = "https://wslwebservices.leg.wa.gov/CommitteeMeetingService.asmx"
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_USER_AGENT = "wa-leg-testifier-audit/0.1 (+https://app.leg.wa.gov/csi)"
LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CommitteeMeeting:
    agenda_id: str
    agency: str
    acronym: str
    committee_name: str
    long_name: str
    meeting_date: str
    revised_date: str
    notes: str


@dataclass(frozen=True)
class CommitteeMeetingItem:
    agenda_item_id: str
    hearing_type_description: str
    bill_id: str
    item_description: str


def _local_name(tag: str) -> str:
    return str(tag or "").split("}", 1)[-1]


def _child_text(element: ET.Element, child_name: str) -> str:
    for child in list(element):
        if _local_name(child.tag) == child_name:
            return str(child.text or "").strip()
    return ""


def _request_xml(*, operation: str, params: dict[str, Any], timeout_seconds: float) -> str:
    query = urlencode({key: str(value) for key, value in params.items() if value is not None})
    url = f"{SERVICE_BASE_URL}/{operation}?{query}" if query else f"{SERVICE_BASE_URL}/{operation}"
    LOGGER.info(
        "Committee service request start %s timeout=%.1fs",
        url,
        float(timeout_seconds),
    )
    request = Request(
        url,
        headers={
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "application/xml, text/xml",
        },
    )
    started = time.monotonic()
    wait_for_global_http_slot()
    with urlopen(request, timeout=timeout_seconds) as response:
        payload = response.read()
        status = int(getattr(response, "status", response.getcode()))
    elapsed = time.monotonic() - started
    LOGGER.info(
        "Committee service response %s %s (bytes=%s elapsed=%.2fs)",
        status,
        url,
        len(payload),
        elapsed,
    )
    return payload.decode("utf-8", errors="replace")


def parse_committee_meetings_xml(xml_text: str) -> list[CommitteeMeeting]:
    if not xml_text.strip():
        return []
    root = ET.fromstring(xml_text)
    meetings: list[CommitteeMeeting] = []
    for element in root.iter():
        if _local_name(element.tag) != "CommitteeMeeting":
            continue
        meetings.append(
            CommitteeMeeting(
                agenda_id=_child_text(element, "AgendaId"),
                agency=_child_text(element, "Agency"),
                acronym=_child_text(element, "Acronym"),
                committee_name=_child_text(element, "Name"),
                long_name=_child_text(element, "LongName"),
                meeting_date=_child_text(element, "Date"),
                revised_date=_child_text(element, "RevisedDate"),
                notes=_child_text(element, "Notes"),
            )
        )
    return meetings


def parse_committee_meeting_items_xml(xml_text: str) -> list[CommitteeMeetingItem]:
    if not xml_text.strip():
        return []
    root = ET.fromstring(xml_text)
    items: list[CommitteeMeetingItem] = []
    for element in root.iter():
        if _local_name(element.tag) != "CommitteeMeetingItem":
            continue
        items.append(
            CommitteeMeetingItem(
                agenda_item_id=_child_text(element, "AgendaItemId"),
                hearing_type_description=_child_text(element, "HearingTypeDescription"),
                bill_id=_child_text(element, "BillId"),
                item_description=_child_text(element, "ItemDescription"),
            )
        )
    return items


def fetch_committee_meetings(
    *,
    begin_date: date,
    end_date: date,
    revised: bool = False,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> list[CommitteeMeeting]:
    operation = "GetRevisedCommitteeMeetings" if revised else "GetCommitteeMeetings"
    xml_text = _request_xml(
        operation=operation,
        params={"beginDate": begin_date.isoformat(), "endDate": end_date.isoformat()},
        timeout_seconds=timeout_seconds,
    )
    return parse_committee_meetings_xml(xml_text)


def fetch_committee_meeting_items(
    *,
    agenda_id: str,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> list[CommitteeMeetingItem]:
    xml_text = _request_xml(
        operation="GetCommitteeMeetingItems",
        params={"agendaId": str(agenda_id)},
        timeout_seconds=timeout_seconds,
    )
    return parse_committee_meeting_items_xml(xml_text)


def build_meeting_bill_index(
    *,
    begin_date: date,
    end_date: date,
    include_revised: bool = True,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> list[dict[str, Any]]:
    meetings = fetch_committee_meetings(
        begin_date=begin_date,
        end_date=end_date,
        revised=False,
        timeout_seconds=timeout_seconds,
    )
    if include_revised:
        meetings.extend(
            fetch_committee_meetings(
                begin_date=begin_date,
                end_date=end_date,
                revised=True,
                timeout_seconds=timeout_seconds,
            )
        )
    deduped_meetings: dict[str, CommitteeMeeting] = {}
    for meeting in meetings:
        agenda_id = str(meeting.agenda_id or "").strip()
        if not agenda_id:
            continue
        deduped_meetings[agenda_id] = meeting

    rows: list[dict[str, Any]] = []
    for agenda_id in sorted(deduped_meetings):
        meeting = deduped_meetings[agenda_id]
        items = fetch_committee_meeting_items(
            agenda_id=agenda_id,
            timeout_seconds=timeout_seconds,
        )
        for item in items:
            if not str(item.bill_id or "").strip():
                continue
            rows.append(
                {
                    "agenda_id": agenda_id,
                    "agenda_item_id": str(item.agenda_item_id or "").strip(),
                    "bill_id": str(item.bill_id or "").strip(),
                    "item_description": str(item.item_description or "").strip(),
                    "hearing_type_description": str(item.hearing_type_description or "").strip(),
                    "meeting_date": str(meeting.meeting_date or "").strip(),
                    "revised_date": str(meeting.revised_date or "").strip(),
                    "agency": str(meeting.agency or "").strip(),
                    "committee_acronym": str(meeting.acronym or "").strip(),
                    "committee_name": str(
                        meeting.long_name or meeting.committee_name or ""
                    ).strip(),
                }
            )
    rows.sort(
        key=lambda row: (
            str(row.get("meeting_date") or ""),
            str(row.get("agenda_id") or ""),
            str(row.get("agenda_item_id") or ""),
            str(row.get("bill_id") or ""),
        )
    )
    return rows
