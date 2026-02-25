from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

from testifier_audit.io.baseline_corpus_sampler import (
    build_candidate_pool,
    derive_recent_session_years,
    sample_candidates,
    sample_unsampled_baseline_corpus,
)
from testifier_audit.io.csi_testifiers import CSIDownloadError, CSIDownloadResult


def test_derive_recent_session_years_uses_anchor_year() -> None:
    years = derive_recent_session_years(session_count=3, reference_date=date(2026, 2, 25))
    assert years == [2026, 2025, 2024]


def test_build_candidate_pool_filters_sampled_and_years() -> None:
    rows = [
        {
            "agenda_id": "100",
            "agenda_item_id": "200",
            "bill_id": "SB 1000",
            "item_description": "sample 1",
            "hearing_type_description": "Public Hearing",
            "meeting_date": "2026-02-10T10:30:00",
            "revised_date": "2026-02-10T11:00:00",
            "agency": "Senate",
            "committee_name": "Transportation",
        },
        {
            "agenda_id": "101",
            "agenda_item_id": "201",
            "bill_id": "HB 2000",
            "item_description": "sample 2",
            "hearing_type_description": "Public Hearing",
            "meeting_date": "2023-02-10T10:30:00",
            "revised_date": "2023-02-10T11:00:00",
            "agency": "House",
            "committee_name": "Appropriations",
        },
        {
            "agenda_id": "102",
            "agenda_item_id": "202",
            "bill_id": "SB 3000",
            "item_description": "sample 3",
            "hearing_type_description": "Public Hearing",
            "meeting_date": "2025-02-10T10:30:00",
            "revised_date": "2025-02-10T11:00:00",
            "agency": "Senate",
            "committee_name": "Ways & Means",
        },
    ]
    candidates = build_candidate_pool(
        rows=rows,
        session_years=[2026, 2025, 2024],
        sampled_keys={("100", "200")},
    )

    assert len(candidates) == 1
    assert candidates[0].agenda_id == "102"
    assert candidates[0].agenda_item_id == "202"


def test_build_candidate_pool_keeps_rows_with_missing_agenda_item_id() -> None:
    rows = [
        {
            "agenda_id": "500",
            "agenda_item_id": "",
            "bill_id": "SB 5000",
            "item_description": "sample",
            "hearing_type_description": "Public Hearing",
            "meeting_date": "2026-02-10T10:30:00",
            "revised_date": "2026-02-10T11:00:00",
            "agency": "Senate",
            "committee_name": "Transportation",
        }
    ]
    candidates = build_candidate_pool(
        rows=rows,
        session_years=[2026, 2025, 2024],
        sampled_keys=set(),
    )

    assert len(candidates) == 1
    assert candidates[0].agenda_id == "500"
    assert candidates[0].agenda_item_id == ""
    assert candidates[0].bill_id == "SB 5000"


def test_sample_candidates_prefers_cross_chamber_coverage() -> None:
    pacific = ZoneInfo("America/Los_Angeles")
    candidates = [
        {
            "agenda_id": "1",
            "agenda_item_id": "11",
            "bill_id": "HB 1000",
            "item_description": "",
            "hearing_type_description": "",
            "meeting_date": "2026-01-01T10:00:00",
            "revised_date": "",
            "agency": "House",
            "committee_name": "Labor",
        },
        {
            "agenda_id": "2",
            "agenda_item_id": "22",
            "bill_id": "HB 1001",
            "item_description": "",
            "hearing_type_description": "",
            "meeting_date": "2026-01-02T10:00:00",
            "revised_date": "",
            "agency": "House",
            "committee_name": "Appropriations",
        },
        {
            "agenda_id": "3",
            "agenda_item_id": "33",
            "bill_id": "SB 2000",
            "item_description": "",
            "hearing_type_description": "",
            "meeting_date": "2026-01-03T10:00:00",
            "revised_date": "",
            "agency": "Senate",
            "committee_name": "Transportation",
        },
    ]
    pool = build_candidate_pool(
        rows=candidates,
        session_years=[2026, 2025, 2024],
        sampled_keys=set(),
    )
    selected = sample_candidates(candidates=pool, sample_size=3, seed=7)

    assert len(selected) == 3
    chambers = {candidate.chamber for candidate in selected}
    assert chambers == {"House", "Senate"}
    assert all(candidate.meeting_start.tzinfo == pacific for candidate in selected)


def test_sample_unsampled_baseline_corpus_downloads_only_unsampled(
    monkeypatch,
    tmp_path: Path,
) -> None:
    index_path = tmp_path / "index.json"
    index_csv = tmp_path / "index.csv"
    csv_out_dir = tmp_path / "raw"
    metadata_out_dir = tmp_path / "metadata"
    manifest_path = tmp_path / "manifest.json"

    rows = [
        {
            "agenda_id": "100",
            "agenda_item_id": "200",
            "bill_id": "SB 1000",
            "item_description": "first bill",
            "hearing_type_description": "Public Hearing",
            "meeting_date": "2026-02-10T10:30:00",
            "revised_date": "2026-02-10T11:00:00",
            "agency": "Senate",
            "committee_name": "Transportation",
        },
        {
            "agenda_id": "101",
            "agenda_item_id": "201",
            "bill_id": "HB 2000",
            "item_description": "second bill",
            "hearing_type_description": "Public Hearing",
            "meeting_date": "2025-02-10T10:30:00",
            "revised_date": "2025-02-10T11:00:00",
            "agency": "House",
            "committee_name": "Appropriations",
        },
        {
            "agenda_id": "102",
            "agenda_item_id": "202",
            "bill_id": "SB 3000",
            "item_description": "third bill",
            "hearing_type_description": "Public Hearing",
            "meeting_date": "2024-02-10T10:30:00",
            "revised_date": "2024-02-10T11:00:00",
            "agency": "Senate",
            "committee_name": "Ways & Means",
        },
    ]
    index_payload = {"schema_version": 1, "rows": rows}
    index_path.write_text(json.dumps(index_payload), encoding="utf-8")

    metadata_out_dir.mkdir(parents=True, exist_ok=True)
    existing_sidecar = {
        "schema_version": 1,
        "source": {"meeting_family_id": "100", "agenda_item_id": "200"},
    }
    (metadata_out_dir / "existing.hearing.yaml").write_text(
        yaml.safe_dump(existing_sidecar, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )

    sleep_calls: list[float] = []

    def _fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    def _fake_download(**kwargs: object) -> CSIDownloadResult:
        agenda_item_id = str(kwargs["agenda_item_id"])
        meeting_start = kwargs["meeting_start"]
        assert isinstance(meeting_start, datetime)
        csv_path = Path(str(kwargs["csv_out_dir"])) / f"{agenda_item_id}.csv"
        metadata_path = Path(str(kwargs["metadata_out_dir"])) / f"{agenda_item_id}.hearing.yaml"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.write_text("Group,Name,Organization,Position,Time Signed In\n", encoding="utf-8")
        metadata_path.write_text(
            yaml.safe_dump(
                {
                    "schema_version": 1,
                    "source": {
                        "meeting_family_id": str(kwargs["meeting_family_id"]),
                        "agenda_item_id": agenda_item_id,
                    },
                },
                sort_keys=False,
                allow_unicode=False,
            ),
            encoding="utf-8",
        )
        return CSIDownloadResult(
            search_query=str(kwargs["bill_query"]),
            csv_path=csv_path,
            metadata_path=metadata_path,
            short_bill_id=str(kwargs["short_bill_id"]),
            bill_title=str(kwargs.get("bill_title") or ""),
            meeting_family_id=str(kwargs["meeting_family_id"]),
            agenda_item_family_id=str(kwargs.get("agenda_item_family_id") or ""),
            agenda_item_id=agenda_item_id,
            meeting_start=meeting_start,
            testifying_rows=1,
            not_testifying_rows=0,
        )

    monkeypatch.setattr(
        "testifier_audit.io.baseline_corpus_sampler.download_csi_testifier_csv_by_agenda_item",
        _fake_download,
    )
    monkeypatch.setattr("testifier_audit.io.baseline_corpus_sampler.time.sleep", _fake_sleep)

    manifest = sample_unsampled_baseline_corpus(
        sample_size=2,
        session_count=3,
        index_json_path=index_path,
        index_csv_path=index_csv,
        csv_out_dir=csv_out_dir,
        metadata_out_dir=metadata_out_dir,
        manifest_path=manifest_path,
        refresh_index=False,
        seed=42,
        rate_limit_seconds=0.2,
        reference_date=date(2026, 2, 25),
    )

    assert manifest["sample_size_selected"] == 2
    assert manifest["sample_size_downloaded"] == 2
    assert manifest["sample_size_failed"] == 0
    selected_ids = {entry["agenda_item_id"] for entry in manifest["selected_candidates"]}
    assert "200" not in selected_ids
    assert selected_ids == {"201", "202"}
    assert sleep_calls == [0.2]


def test_sample_unsampled_baseline_corpus_falls_back_when_agenda_item_missing(
    monkeypatch,
    tmp_path: Path,
) -> None:
    index_path = tmp_path / "index.json"
    index_csv = tmp_path / "index.csv"
    csv_out_dir = tmp_path / "raw"
    metadata_out_dir = tmp_path / "metadata"
    manifest_path = tmp_path / "manifest.json"
    rows = [
        {
            "agenda_id": "700",
            "agenda_item_id": "",
            "bill_id": "SB 7000",
            "item_description": "fallback bill",
            "hearing_type_description": "Public Hearing",
            "meeting_date": "2026-02-10T10:30:00",
            "revised_date": "2026-02-10T11:00:00",
            "agency": "Senate",
            "committee_name": "Transportation",
        }
    ]
    index_path.write_text(json.dumps({"schema_version": 1, "rows": rows}), encoding="utf-8")

    called: dict[str, bool | int] = {"fallback_calls": 0, "direct": False}

    def _fake_direct(**kwargs: object) -> CSIDownloadResult:
        called["direct"] = True
        raise AssertionError("direct path should not be used when agenda_item_id is missing")

    def _fake_fallback(**kwargs: object) -> CSIDownloadResult:
        called["fallback_calls"] = int(called["fallback_calls"]) + 1
        if kwargs.get("meeting_family_id"):
            raise CSIDownloadError("Meeting family id '700' was not returned for query 'SB 7000'")
        csv_path = Path(str(kwargs["csv_out_dir"])) / "SB7000-20260210-1030.csv"
        metadata_path = Path(str(kwargs["metadata_out_dir"])) / "SB7000-20260210-1030.hearing.yaml"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.write_text("Group,Name,Organization,Position,Time Signed In\n", encoding="utf-8")
        metadata_path.write_text(
            yaml.safe_dump(
                {
                    "schema_version": 1,
                    "source": {"meeting_family_id": "700", "agenda_item_id": "70001"},
                },
                sort_keys=False,
                allow_unicode=False,
            ),
            encoding="utf-8",
        )
        return CSIDownloadResult(
            search_query="SB 7000",
            csv_path=csv_path,
            metadata_path=metadata_path,
            short_bill_id="SB 7000",
            bill_title="fallback bill",
            meeting_family_id="700",
            agenda_item_family_id="",
            agenda_item_id="70001",
            meeting_start=datetime(2026, 2, 10, 10, 30, tzinfo=ZoneInfo("America/Los_Angeles")),
            testifying_rows=1,
            not_testifying_rows=0,
        )

    monkeypatch.setattr(
        "testifier_audit.io.baseline_corpus_sampler.download_csi_testifier_csv_by_agenda_item",
        _fake_direct,
    )
    monkeypatch.setattr(
        "testifier_audit.io.baseline_corpus_sampler.download_csi_testifier_csv",
        _fake_fallback,
    )

    manifest = sample_unsampled_baseline_corpus(
        sample_size=1,
        session_count=1,
        index_json_path=index_path,
        index_csv_path=index_csv,
        csv_out_dir=csv_out_dir,
        metadata_out_dir=metadata_out_dir,
        manifest_path=manifest_path,
        refresh_index=False,
        seed=1,
        rate_limit_seconds=0.0,
        reference_date=date(2026, 2, 25),
    )

    assert called["fallback_calls"] == 2
    assert called["direct"] is False
    assert manifest["sample_size_downloaded"] == 1
    assert manifest["successes"][0]["agenda_item_id"] == "70001"
