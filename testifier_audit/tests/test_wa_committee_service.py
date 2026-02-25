from __future__ import annotations

from datetime import date

from testifier_audit.io import wa_committee_service as committee_service


def test_parse_committee_meetings_xml_extracts_rows() -> None:
    xml_text = """
    <Envelope xmlns=\"http://schemas.xmlsoap.org/soap/envelope/\">
      <Body>
        <GetCommitteeMeetingsResponse xmlns=\"http://tempuri.org/\">
          <GetCommitteeMeetingsResult>
            <CommitteeMeeting>
              <AgendaId>34001</AgendaId>
              <Agency>Senate</Agency>
              <Acronym>TRAN</Acronym>
              <Name>Transportation</Name>
              <LongName>Senate Transportation</LongName>
              <Date>2026-02-10T10:30:00</Date>
              <RevisedDate>2026-02-10T11:00:00</RevisedDate>
              <Notes>test note</Notes>
            </CommitteeMeeting>
          </GetCommitteeMeetingsResult>
        </GetCommitteeMeetingsResponse>
      </Body>
    </Envelope>
    """
    rows = committee_service.parse_committee_meetings_xml(xml_text)
    assert len(rows) == 1
    row = rows[0]
    assert row.agenda_id == "34001"
    assert row.agency == "Senate"
    assert row.acronym == "TRAN"
    assert row.long_name == "Senate Transportation"


def test_parse_committee_meeting_items_xml_extracts_rows() -> None:
    xml_text = """
    <Root>
      <CommitteeMeetingItem>
        <AgendaItemId>28434</AgendaItemId>
        <HearingTypeDescription>Public Hearing</HearingTypeDescription>
        <BillId>SB 6005</BillId>
        <ItemDescription>Transportation budget</ItemDescription>
      </CommitteeMeetingItem>
    </Root>
    """
    rows = committee_service.parse_committee_meeting_items_xml(xml_text)
    assert len(rows) == 1
    row = rows[0]
    assert row.agenda_item_id == "28434"
    assert row.bill_id == "SB 6005"
    assert row.item_description == "Transportation budget"


def test_build_meeting_bill_index_joins_meetings_and_items(monkeypatch) -> None:
    meetings = [
        committee_service.CommitteeMeeting(
            agenda_id="34001",
            agency="Senate",
            acronym="TRAN",
            committee_name="Transportation",
            long_name="Senate Transportation",
            meeting_date="2026-02-10T10:30:00",
            revised_date="2026-02-10T11:00:00",
            notes="",
        )
    ]
    items = [
        committee_service.CommitteeMeetingItem(
            agenda_item_id="28434",
            hearing_type_description="Public Hearing",
            bill_id="SB 6005",
            item_description="Transportation budget",
        ),
        committee_service.CommitteeMeetingItem(
            agenda_item_id="28435",
            hearing_type_description="Work Session",
            bill_id="",
            item_description="non-bill row",
        ),
    ]

    def _fetch_meetings(**kwargs):
        if kwargs.get("revised"):
            return []
        return meetings

    monkeypatch.setattr(committee_service, "fetch_committee_meetings", _fetch_meetings)
    monkeypatch.setattr(
        committee_service,
        "fetch_committee_meeting_items",
        lambda **_kwargs: items,
    )

    rows = committee_service.build_meeting_bill_index(
        begin_date=date(2026, 2, 1),
        end_date=date(2026, 2, 28),
        include_revised=True,
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["agenda_id"] == "34001"
    assert row["agenda_item_id"] == "28434"
    assert row["bill_id"] == "SB 6005"
    assert row["committee_name"] == "Senate Transportation"


def test_request_xml_uses_global_rate_limit(monkeypatch) -> None:
    class _FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def read(self) -> bytes:
            return b"<Root />"

    call_count = {"value": 0}

    def _fake_wait() -> None:
        call_count["value"] = int(call_count["value"]) + 1

    def _fake_urlopen(request, timeout):
        return _FakeResponse()

    monkeypatch.setattr(committee_service, "wait_for_global_http_slot", _fake_wait)
    monkeypatch.setattr(committee_service, "urlopen", _fake_urlopen)

    payload = committee_service._request_xml(
        operation="GetCommitteeMeetings",
        params={"beginDate": "2026-01-01", "endDate": "2026-01-02"},
        timeout_seconds=10.0,
    )

    assert payload == "<Root />"
    assert call_count["value"] == 1
