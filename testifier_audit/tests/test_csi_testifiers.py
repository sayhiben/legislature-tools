from __future__ import annotations

import json
import logging
from datetime import datetime
from urllib.parse import unquote
from zoneinfo import ZoneInfo

import pytest
import yaml

from testifier_audit.io.csi_testifiers import (
    CSIAgendaItem,
    CSIDownloadError,
    CSIMeeting,
    CSITestifierRow,
    _build_output_stem,
    _build_sidecar_payload,
    _build_sidecar_stats,
    _derive_lookup_query,
    _request_text_with_retries,
    _select_agenda_item,
    download_csi_testifier_csv,
    download_csi_testifier_csv_by_agenda_item,
    download_csi_testifier_csv_by_meeting_family,
    parse_agenda_items,
    parse_testifier_rows,
)


def _sample_meeting() -> CSIMeeting:
    return CSIMeeting(
        leg_id="135565",
        committee_id="438",
        committee_name="Transportation",
        chamber="Senate",
        meeting_family_id="34001",
        bill_title="Transportation funding and appropriations",
        bill_number="6005",
        short_bill_id="SB 6005",
        chamber_abbr="S",
        meeting_date_time_formatted="02/24/26 04:00 PM",
        meeting_start=datetime(2026, 2, 24, 16, 0, tzinfo=ZoneInfo("America/Los_Angeles")),
    )


def test_parse_agenda_items_extracts_family_and_item_ids() -> None:
    html = """
    <ul>
      <li>
        <button class="agendaItem" id="agendaItem-170647"
          onclick="WSLApp.Testimony.getTestimonyTypes($(this), 'senate',34001, 170647, 28435)">
          SB 6225 Transportation funding bonds
        </button>
      </li>
      <li>
        <button class="agendaItem" id="agendaItem-170646"
          onclick="WSLApp.Testimony.getTestimonyTypes($(this), 'senate',34001, 170646, 28434)">
          SB 6005 Transportation budget, supp.
        </button>
      </li>
    </ul>
    """

    items = parse_agenda_items(html)

    assert len(items) == 2
    assert items[0].agenda_item_family_id == "170647"
    assert items[0].agenda_item_id == "28435"
    assert items[0].description == "SB 6225 Transportation funding bonds"
    assert items[1].agenda_item_family_id == "170646"
    assert items[1].agenda_item_id == "28434"


def test_parse_testifier_rows_combines_testifying_and_not_testifying_groups() -> None:
    testifying_payload = (
        "[{&quot;Name&quot;:&quot;Doe, Jane&quot;,&quot;Organization&quot;:&quot;Org A&quot;,"
        "&quot;Position&quot;:&quot;Pro&quot;,"
        "&quot;TimeSignedIn&quot;:&quot;2/23/2026 9:00 AM&quot;}]"
    )
    not_testifying_payload = (
        "[{&quot;Name&quot;:&quot;Roe, John&quot;,&quot;Organization&quot;:null,"
        "&quot;Position&quot;:&quot;Con&quot;,"
        "&quot;TimeSignedIn&quot;:&quot;2/23/2026 9:30 AM&quot;}]"
    )
    html = (
        '<div id="testifyingDataTable" '
        f'data-json="{testifying_payload}"></div>'
        '<div id="notTestifyingDataTable" '
        f'data-json="{not_testifying_payload}"></div>'
    )

    testifying, not_testifying = parse_testifier_rows(html)

    assert len(testifying) == 1
    assert testifying[0].group == "Testifying"
    assert testifying[0].name == "Doe, Jane"
    assert testifying[0].organization == "Org A"
    assert testifying[0].position == "Pro"
    assert testifying[0].time_signed_in == "2/23/2026 9:00 AM"

    assert len(not_testifying) == 1
    assert not_testifying[0].group == "Not Testifying"
    assert not_testifying[0].name == "Roe, John"
    assert not_testifying[0].organization == ""
    assert not_testifying[0].position == "Con"
    assert not_testifying[0].time_signed_in == "2/23/2026 9:30 AM"


def test_select_agenda_item_prefers_short_bill_match() -> None:
    meeting = _sample_meeting()
    agenda_items = [
        CSIAgendaItem(
            agenda_item_family_id="170647",
            agenda_item_id="28435",
            description="SB 6225 Transportation funding bonds",
        ),
        CSIAgendaItem(
            agenda_item_family_id="170646",
            agenda_item_id="28434",
            description="SB 6005 Transportation budget, supp.",
        ),
    ]

    selected = _select_agenda_item(
        agenda_items=agenda_items,
        meeting=meeting,
        agenda_index=0,
        agenda_item_id=None,
    )

    assert selected.agenda_item_id == "28434"
    assert selected.agenda_item_family_id == "170646"


def test_build_output_stem_uses_sanitized_short_bill_and_24h_time() -> None:
    stem = _build_output_stem(_sample_meeting())

    assert stem == "SB6005-20260224-1600"


def test_build_sidecar_payload_includes_high_level_stats() -> None:
    testifying_rows = [
        CSITestifierRow(
            group="Testifying",
            name="A, One",
            organization="Org A",
            position="Pro",
            time_signed_in="2/23/2026 9:00 AM",
        ),
        CSITestifierRow(
            group="Testifying",
            name="B, Two",
            organization="Org B",
            position="Other",
            time_signed_in="2/23/2026 9:01 AM",
        ),
    ]
    not_testifying_rows = [
        CSITestifierRow(
            group="Not Testifying",
            name="C, Three",
            organization="Org C",
            position="Con",
            time_signed_in="2/23/2026 9:02 AM",
        ),
        CSITestifierRow(
            group="Not Testifying",
            name="D, Four",
            organization="Org D",
            position="PRO",
            time_signed_in="2/23/2026 9:03 AM",
        ),
    ]

    stats = _build_sidecar_stats(
        testifying_rows=testifying_rows,
        not_testifying_rows=not_testifying_rows,
    )
    payload = _build_sidecar_payload(
        search_query="SB 6005",
        meeting=_sample_meeting(),
        agenda_item=CSIAgendaItem(
            agenda_item_family_id="170646",
            agenda_item_id="28434",
            description="SB 6005 Transportation budget, supp.",
        ),
        stats=stats,
    )

    assert payload["stats"] == {
        "total_rows": 4,
        "total_testifying": 2,
        "total_not_testifying": 2,
        "total_pro": 2,
        "total_con": 1,
        "total_other": 1,
        "total_pro_pct": 50.0,
        "total_con_pct": 25.0,
        "total_other_pct": 25.0,
    }
    assert payload["meeting_start"] == "2026-02-24T16:00:00-08:00"
    assert payload["sign_in_cutoff"] == "2026-02-24T15:00:00-08:00"


def test_download_by_agenda_item_uses_single_testifier_request(monkeypatch, tmp_path) -> None:
    calls: list[str] = []

    def _fake_request(**kwargs: object) -> str:
        url = str(kwargs["url"])
        calls.append(url)
        payload = (
            "[{&quot;Name&quot;:&quot;Doe, Jane&quot;,&quot;Organization&quot;:&quot;Org A&quot;,"
            "&quot;Position&quot;:&quot;Pro&quot;,"
            "&quot;TimeSignedIn&quot;:&quot;2/23/2026 9:00 AM&quot;}]"
        )
        return f'<div id="testifyingDataTable" data-json="{payload}"></div>'

    monkeypatch.setattr(
        "testifier_audit.io.csi_testifiers._request_text_with_retries",
        _fake_request,
    )

    result = download_csi_testifier_csv_by_agenda_item(
        bill_query="SB 6005",
        meeting_family_id="34001",
        agenda_item_id="28434",
        meeting_start=datetime(2026, 2, 24, 16, 0),
        short_bill_id="SB 6005",
        bill_number="6005",
        bill_title="Transportation funding and appropriations",
        committee_name="Transportation",
        chamber="Senate",
        agenda_item_description="SB 6005 Transportation budget, supp.",
        csv_out_dir=tmp_path / "raw",
        metadata_out_dir=tmp_path / "metadata",
    )

    assert len(calls) == 1
    assert "GetOtherTestifiers" in calls[0]
    assert "agendaItemId=28434" in calls[0]
    assert result.meeting_family_id == "34001"
    assert result.agenda_item_id == "28434"
    assert result.total_rows == 1

    sidecar = yaml.safe_load(result.metadata_path.read_text(encoding="utf-8"))
    assert sidecar["source"]["meeting_family_id"] == "34001"
    assert sidecar["source"]["agenda_item_id"] == "28434"


def test_download_by_meeting_family_resolves_agenda_then_fetches_testifiers(
    monkeypatch,
    tmp_path,
) -> None:
    calls: list[str] = []

    def _fake_request(**kwargs: object) -> str:
        url = str(kwargs["url"])
        calls.append(url)
        if "GetAgendaItems" in url:
            return """
            <ul>
              <li>
                <button class="agendaItem" id="agendaItem-170647"
                  onclick="WSLApp.Testimony.getTestimonyTypes($(this),
                  'senate',34001, 170647, 28435)">
                  SB 6225 Transportation funding bonds
                </button>
              </li>
              <li>
                <button class="agendaItem" id="agendaItem-170646"
                  onclick="WSLApp.Testimony.getTestimonyTypes($(this),
                  'senate',34001, 170646, 28434)">
                  SB 6005 Transportation budget, supp.
                </button>
              </li>
            </ul>
            """
        payload = (
            "[{&quot;Name&quot;:&quot;Doe, Jane&quot;,&quot;Organization&quot;:&quot;Org A&quot;,"
            "&quot;Position&quot;:&quot;Pro&quot;,"
            "&quot;TimeSignedIn&quot;:&quot;2/23/2026 9:00 AM&quot;}]"
        )
        return f'<div id="testifyingDataTable" data-json="{payload}"></div>'

    monkeypatch.setattr(
        "testifier_audit.io.csi_testifiers._request_text_with_retries",
        _fake_request,
    )

    result = download_csi_testifier_csv_by_meeting_family(
        bill_query="SB 6005",
        meeting_family_id="34001",
        chamber="Senate",
        meeting_start=datetime(2026, 2, 24, 16, 0),
        short_bill_id="SB 6005",
        bill_number="6005",
        bill_title="Transportation funding and appropriations",
        committee_name="Transportation",
        csv_out_dir=tmp_path / "raw",
        metadata_out_dir=tmp_path / "metadata",
    )

    assert len(calls) == 2
    assert "GetAgendaItems" in calls[0]
    assert "meetingFamilyId=34001" in calls[0]
    assert "chamber=senate" in calls[0]
    assert "GetOtherTestifiers" in calls[1]
    assert result.meeting_family_id == "34001"
    assert result.agenda_item_id == "28434"


def test_request_text_with_retries_uses_global_rate_limit(monkeypatch) -> None:
    class _FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def read(self) -> bytes:
            return b"ok"

        def getcode(self) -> int:
            return 200

    class _FakeOpener:
        def open(self, request, timeout):
            return _FakeResponse()

    call_count = {"value": 0}

    def _fake_wait() -> None:
        call_count["value"] = int(call_count["value"]) + 1

    monkeypatch.setattr("testifier_audit.io.csi_testifiers.wait_for_global_http_slot", _fake_wait)

    payload = _request_text_with_retries(
        opener=_FakeOpener(),
        url="https://example.com",
        logger=logging.getLogger(__name__),
        timeout_seconds=10.0,
        max_retries=0,
        retry_backoff_seconds=0.1,
        user_agent="test-agent",
        accept="application/json",
    )

    assert payload == "ok"
    assert call_count["value"] == 1


def test_derive_lookup_query_uses_numeric_token_for_short_bill_ids() -> None:
    assert _derive_lookup_query("SB 6005") == "6005"
    assert _derive_lookup_query("2SHB 2479") == "2479"
    assert _derive_lookup_query("transportation funding and appropriations") == (
        "transportation funding and appropriations"
    )


def test_download_csi_testifier_csv_uses_numeric_lookup_token(monkeypatch, tmp_path) -> None:
    calls: list[str] = []

    def _fake_request(**kwargs: object) -> str:
        url = str(kwargs["url"])
        calls.append(url)
        if "SearchMeetings" in url:
            return json.dumps(
                {
                    "Items": [
                        {
                            "LegId": "135565",
                            "CommitteeId": "438",
                            "CommitteeName": "Transportation",
                            "Chamber": "Senate",
                            "MeetingFamilyId": "34001",
                            "BillTitle": "Transportation funding and appropriations",
                            "BillNumber": "6005",
                            "ShortBillId": "SB 6005",
                            "ChamberAbbr": "S",
                            "MeetingDateTimeFormatted": "02/24/26 04:00 PM",
                            "MeetingDateTime": "2026-02-24T16:00:00-08:00",
                        }
                    ]
                }
            )
        if "GetAgendaItems" in url:
            return """
            <ul>
              <li>
                <button class="agendaItem" id="agendaItem-170646"
                  onclick="WSLApp.Testimony.getTestimonyTypes($(this),
                  'senate',34001, 170646, 28434)">
                  SB 6005 Transportation budget, supp.
                </button>
              </li>
            </ul>
            """
        payload = (
            "[{&quot;Name&quot;:&quot;Doe, Jane&quot;,&quot;Organization&quot;:&quot;Org A&quot;,"
            "&quot;Position&quot;:&quot;Pro&quot;,"
            "&quot;TimeSignedIn&quot;:&quot;2/23/2026 9:00 AM&quot;}]"
        )
        return f'<div id="testifyingDataTable" data-json="{payload}"></div>'

    monkeypatch.setattr(
        "testifier_audit.io.csi_testifiers._request_text_with_retries",
        _fake_request,
    )

    result = download_csi_testifier_csv(
        bill_query="SB 6005",
        csv_out_dir=tmp_path / "raw",
        metadata_out_dir=tmp_path / "metadata",
    )

    decoded_search_url = unquote(calls[0]).lower()
    assert "searchmeetings" in decoded_search_url
    assert "substringof('6005'" in decoded_search_url
    assert "substringof('sb 6005'" not in decoded_search_url
    assert result.meeting_family_id == "34001"


def test_download_csi_testifier_csv_meeting_year_filter(monkeypatch, tmp_path) -> None:
    def _fake_request(**kwargs: object) -> str:
        url = str(kwargs["url"])
        if "SearchMeetings" in url:
            return json.dumps(
                {
                    "Items": [
                        {
                            "LegId": "2025001",
                            "CommitteeId": "438",
                            "CommitteeName": "Transportation",
                            "Chamber": "Senate",
                            "MeetingFamilyId": "34001",
                            "BillTitle": "Transportation funding and appropriations",
                            "BillNumber": "6005",
                            "ShortBillId": "SB 6005",
                            "ChamberAbbr": "S",
                            "MeetingDateTimeFormatted": "02/24/25 04:00 PM",
                            "MeetingDateTime": "2025-02-24T16:00:00-08:00",
                        },
                        {
                            "LegId": "2026001",
                            "CommitteeId": "438",
                            "CommitteeName": "Transportation",
                            "Chamber": "Senate",
                            "MeetingFamilyId": "34002",
                            "BillTitle": "Transportation funding and appropriations",
                            "BillNumber": "6005",
                            "ShortBillId": "SB 6005",
                            "ChamberAbbr": "S",
                            "MeetingDateTimeFormatted": "02/24/26 04:00 PM",
                            "MeetingDateTime": "2026-02-24T16:00:00-08:00",
                        },
                    ]
                }
            )
        if "GetAgendaItems" in url and "meetingFamilyId=34002" in url:
            return """
            <ul>
              <li>
                <button class="agendaItem" id="agendaItem-170646"
                  onclick="WSLApp.Testimony.getTestimonyTypes($(this),
                  'senate',34002, 170646, 28434)">
                  SB 6005 Transportation budget, supp.
                </button>
              </li>
            </ul>
            """
        payload = (
            "[{&quot;Name&quot;:&quot;Doe, Jane&quot;,&quot;Organization&quot;:&quot;Org A&quot;,"
            "&quot;Position&quot;:&quot;Pro&quot;,"
            "&quot;TimeSignedIn&quot;:&quot;2/23/2026 9:00 AM&quot;}]"
        )
        return f'<div id="testifyingDataTable" data-json="{payload}"></div>'

    monkeypatch.setattr(
        "testifier_audit.io.csi_testifiers._request_text_with_retries",
        _fake_request,
    )

    result = download_csi_testifier_csv(
        bill_query="SB 6005",
        csv_out_dir=tmp_path / "raw",
        metadata_out_dir=tmp_path / "metadata",
        meeting_year=2026,
    )
    assert result.meeting_family_id == "34002"
    assert result.meeting_start.year == 2026

    with pytest.raises(CSIDownloadError, match="in year 2024"):
        download_csi_testifier_csv(
            bill_query="SB 6005",
            csv_out_dir=tmp_path / "raw",
            metadata_out_dir=tmp_path / "metadata",
            meeting_year=2024,
        )
