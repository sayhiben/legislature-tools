from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from testifier_audit.io.csi_testifiers import (
    CSIAgendaItem,
    CSIMeeting,
    CSITestifierRow,
    _build_output_stem,
    _build_sidecar_payload,
    _build_sidecar_stats,
    _select_agenda_item,
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
