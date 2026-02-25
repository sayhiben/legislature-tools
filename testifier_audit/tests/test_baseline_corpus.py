from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from testifier_audit.io import baseline_corpus as module
from testifier_audit.io.csi_testifiers import CSIDownloadError, CSIDownloadResult


def _write_index(path: Path) -> None:
    payload = {
        "rows": [
            {"bill_id": "SB 6005", "agenda_id": "34001", "agenda_item_id": "28434"},
            {"bill_id": "SB 7000", "agenda_id": "34002", "agenda_item_id": "28435"},
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_materialize_baseline_corpus_records_success_and_failure(
    monkeypatch,
    tmp_path: Path,
) -> None:
    index_path = tmp_path / "index.json"
    _write_index(index_path)
    csv_out = tmp_path / "csv"
    meta_out = tmp_path / "meta"

    def _download(**kwargs):
        bill_query = str(kwargs["bill_query"])
        if bill_query == "SB 7000":
            raise CSIDownloadError("forced failure")
        csv_out.mkdir(parents=True, exist_ok=True)
        meta_out.mkdir(parents=True, exist_ok=True)
        csv_path = csv_out / "SB6005-20260224-1600.csv"
        metadata_path = meta_out / "SB6005-20260224-1600.hearing.yaml"
        csv_path.write_text("Group,Name,Organization,Position,Time Signed In\n", encoding="utf-8")
        metadata_path.write_text("schema_version: 1\n", encoding="utf-8")
        return CSIDownloadResult(
            search_query=bill_query,
            csv_path=csv_path,
            metadata_path=metadata_path,
            short_bill_id="SB 6005",
            bill_title="Example",
            meeting_family_id="34001",
            agenda_item_family_id="170646",
            agenda_item_id="28434",
            meeting_start=datetime(2026, 2, 24, 16, 0, tzinfo=ZoneInfo("America/Los_Angeles")),
            testifying_rows=1,
            not_testifying_rows=0,
        )

    monkeypatch.setattr(module, "download_csi_testifier_csv", _download)

    manifest = module.materialize_baseline_corpus(
        index_path=index_path,
        csv_out_dir=csv_out,
        metadata_out_dir=meta_out,
    )
    assert manifest["requested_rows"] == 2
    assert manifest["downloaded_rows"] == 1
    assert manifest["failed_rows"] == 1
    assert len(manifest["successes"]) == 1
    assert len(manifest["failures"]) == 1
    assert manifest["successes"][0]["csv_sha256"]
    assert manifest["failures"][0]["bill_id"] == "SB 7000"


def test_write_manifest_persists_json(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    manifest = {"schema_version": 1, "downloaded_rows": 3}
    module.write_manifest(path, manifest)
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded["downloaded_rows"] == 3


def test_parse_args_supports_cli_flags() -> None:
    args = module.parse_args(
        [
            "--index-json",
            "idx.json",
            "--csv-out-dir",
            "csv-out",
            "--metadata-out-dir",
            "meta-out",
            "--manifest-out",
            "manifest.json",
            "--limit",
            "12",
            "--overwrite",
            "--top",
            "50",
            "--timeout-seconds",
            "9.0",
            "--max-retries",
            "2",
        ]
    )
    assert str(args.index_json) == "idx.json"
    assert str(args.csv_out_dir) == "csv-out"
    assert str(args.metadata_out_dir) == "meta-out"
    assert str(args.manifest_out) == "manifest.json"
    assert int(args.limit) == 12
    assert bool(args.overwrite) is True
    assert int(args.top) == 50
    assert float(args.timeout_seconds) == 9.0
    assert int(args.max_retries) == 2


def test_read_index_rows_returns_empty_for_non_list_rows(tmp_path: Path) -> None:
    path = tmp_path / "index.json"
    path.write_text(json.dumps({"rows": "invalid"}), encoding="utf-8")
    rows = module.read_index_rows(path)
    assert rows == []


def test_materialize_baseline_corpus_applies_limit_and_skips_missing_bill(
    monkeypatch,
    tmp_path: Path,
) -> None:
    index_path = tmp_path / "index.json"
    index_path.write_text(
        json.dumps(
            {
                "rows": [
                    {"bill_id": "", "agenda_id": "34000", "agenda_item_id": "28000"},
                    {"bill_id": "SB 6005", "agenda_id": "34001", "agenda_item_id": "28434"},
                    {"bill_id": "SB 7000", "agenda_id": "34002", "agenda_item_id": "28435"},
                ]
            }
        ),
        encoding="utf-8",
    )
    csv_out = tmp_path / "csv"
    meta_out = tmp_path / "meta"
    seen: list[dict[str, object]] = []

    def _download(**kwargs):
        seen.append(kwargs)
        csv_out.mkdir(parents=True, exist_ok=True)
        meta_out.mkdir(parents=True, exist_ok=True)
        csv_path = csv_out / "SB6005-20260224-1600.csv"
        metadata_path = meta_out / "SB6005-20260224-1600.hearing.yaml"
        csv_path.write_text("Group,Name,Organization,Position,Time Signed In\n", encoding="utf-8")
        metadata_path.write_text("schema_version: 1\n", encoding="utf-8")
        return CSIDownloadResult(
            search_query=str(kwargs["bill_query"]),
            csv_path=csv_path,
            metadata_path=metadata_path,
            short_bill_id="SB 6005",
            bill_title="Example",
            meeting_family_id="34001",
            agenda_item_family_id="170646",
            agenda_item_id="28434",
            meeting_start=datetime(2026, 2, 24, 16, 0, tzinfo=ZoneInfo("America/Los_Angeles")),
            testifying_rows=1,
            not_testifying_rows=0,
        )

    monkeypatch.setattr(module, "download_csi_testifier_csv", _download)

    manifest = module.materialize_baseline_corpus(
        index_path=index_path,
        csv_out_dir=csv_out,
        metadata_out_dir=meta_out,
        limit=2,
        top=-10,
        max_retries=-3,
    )
    assert manifest["requested_rows"] == 2
    assert manifest["downloaded_rows"] == 1
    assert manifest["failed_rows"] == 0
    assert len(seen) == 1
    assert int(seen[0]["top"]) == 1
    assert int(seen[0]["max_retries"]) == 0


def test_main_writes_manifest_and_prints(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    calls: dict[str, object] = {}

    def _materialize(**kwargs):
        calls.update(kwargs)
        return {
            "schema_version": 1,
            "downloaded_rows": 2,
            "failed_rows": 1,
            "successes": [],
            "failures": [],
        }

    def _write_manifest(path: Path, manifest: dict[str, object]) -> None:
        calls["manifest_path"] = path
        calls["manifest"] = manifest

    monkeypatch.setattr(module, "materialize_baseline_corpus", _materialize)
    monkeypatch.setattr(module, "write_manifest", _write_manifest)

    index_json = tmp_path / "idx.json"
    index_json.write_text("{}", encoding="utf-8")
    csv_out = tmp_path / "csv"
    meta_out = tmp_path / "meta"
    manifest_out = tmp_path / "manifest.json"
    module.main(
        [
            "--index-json",
            str(index_json),
            "--csv-out-dir",
            str(csv_out),
            "--metadata-out-dir",
            str(meta_out),
            "--manifest-out",
            str(manifest_out),
        ]
    )

    captured = capsys.readouterr()
    assert str(calls["index_path"]) == str(index_json.resolve())
    assert str(calls["csv_out_dir"]) == str(csv_out.resolve())
    assert str(calls["metadata_out_dir"]) == str(meta_out.resolve())
    assert str(calls["manifest_path"]) == str(manifest_out.resolve())
    assert "Materialized baseline corpus" in captured.out
    assert "Wrote manifest" in captured.out
