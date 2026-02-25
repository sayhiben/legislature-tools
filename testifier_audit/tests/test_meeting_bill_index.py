from __future__ import annotations

from datetime import date
from pathlib import Path
import json

from testifier_audit.io import meeting_bill_index as module


def test_run_build_meeting_bill_index_writes_json_and_csv(
    monkeypatch,
    tmp_path: Path,
) -> None:
    rows = [
        {
            "agenda_id": "34001",
            "agenda_item_id": "28434",
            "bill_id": "SB 6005",
            "item_description": "Transportation budget",
            "hearing_type_description": "Public Hearing",
            "meeting_date": "2026-02-10T10:30:00",
            "revised_date": "",
            "agency": "Senate",
            "committee_acronym": "TRAN",
            "committee_name": "Senate Transportation",
        }
    ]
    monkeypatch.setattr(module, "build_meeting_bill_index", lambda **_kwargs: rows)

    output_json = tmp_path / "index.json"
    output_csv = tmp_path / "index.csv"
    payload = module.run_build_meeting_bill_index(
        start_date=date(2026, 2, 1),
        end_date=date(2026, 2, 28),
        output_json=output_json,
        output_csv=output_csv,
        include_revised=True,
    )

    assert payload["row_count"] == 1
    assert payload["rows_sha256"] == module.sha256_json_rows(rows)
    assert output_json.exists()
    assert output_csv.exists()
    loaded = json.loads(output_json.read_text(encoding="utf-8"))
    assert loaded["row_count"] == 1
    assert "GetRevisedCommitteeMeetings" in loaded["source"]["endpoints"]
    csv_lines = output_csv.read_text(encoding="utf-8").splitlines()
    assert csv_lines[0].startswith("agenda_id,agenda_item_id,bill_id")
    assert "SB 6005" in csv_lines[1]


def test_build_index_payload_omits_revised_endpoint_when_disabled() -> None:
    payload = module.build_index_payload(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
        include_revised=False,
        rows=[],
    )
    assert payload["source"]["include_revised"] is False
    assert payload["source"]["endpoints"] == ["GetCommitteeMeetings", "GetCommitteeMeetingItems"]


def test_parse_args_supports_cli_flags() -> None:
    args = module.parse_args(
        [
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-31",
            "--skip-revised",
            "--timeout-seconds",
            "5.5",
        ]
    )
    assert str(args.start_date) == "2026-01-01"
    assert str(args.end_date) == "2026-01-31"
    assert bool(args.skip_revised) is True
    assert float(args.timeout_seconds) == 5.5
    assert str(args.output_json).endswith("wa_meeting_bill_index.json")
    assert str(args.output_csv).endswith("wa_meeting_bill_index.csv")


def test_main_invokes_runner_and_prints_paths(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    calls: dict[str, object] = {}

    def _run_build(**kwargs):
        calls.update(kwargs)
        return {"row_count": 3}

    monkeypatch.setattr(module, "run_build_meeting_bill_index", _run_build)
    out_json = tmp_path / "idx.json"
    out_csv = tmp_path / "idx.csv"
    module.main(
        [
            "--start-date",
            "2026-02-01",
            "--end-date",
            "2026-02-28",
            "--output-json",
            str(out_json),
            "--output-csv",
            str(out_csv),
            "--skip-revised",
        ]
    )

    captured = capsys.readouterr()
    assert calls["start_date"] == date(2026, 2, 1)
    assert calls["end_date"] == date(2026, 2, 28)
    assert calls["include_revised"] is False
    assert str(calls["output_json"]) == str(out_json.resolve())
    assert str(calls["output_csv"]) == str(out_csv.resolve())
    assert "Wrote meeting/bill index JSON" in captured.out
    assert "Wrote meeting/bill index CSV" in captured.out
