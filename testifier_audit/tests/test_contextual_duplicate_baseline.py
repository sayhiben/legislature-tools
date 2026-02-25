from __future__ import annotations

import json
from pathlib import Path

from testifier_audit.io import contextual_duplicate_baseline as module


def _write_csv(path: Path) -> None:
    path.write_text(
        "Group,Name,Organization,Position,Time Signed In\n"
        "Testifying,\"Doe, Jane\",Org A,Pro,2026-02-24 16:00:00\n"
        "Testifying,\"Doe, Jane\",Org A,Con,2026-02-24 16:01:00\n"
        "Testifying,\"Smith, John\",Org B,Pro,2026-02-24 16:02:00\n",
        encoding="utf-8",
    )


def _write_sidecar(path: Path) -> None:
    path.write_text(
        "schema_version: 1\n"
        "hearing_id: SB6005\n"
        "timezone: America/Los_Angeles\n"
        "meeting_start: 2026-02-24T16:00:00-08:00\n"
        "source:\n"
        "  committee_name: Senate Transportation\n"
        "  chamber: Senate\n",
        encoding="utf-8",
    )


def _write_nickname_map(path: Path) -> None:
    path.write_text("alias,canonical\nBOB,ROBERT\n", encoding="utf-8")


def test_build_contextual_duplicate_baseline_payload_emits_rows(tmp_path: Path) -> None:
    csv_dir = tmp_path / "csv"
    metadata_dir = tmp_path / "metadata"
    csv_dir.mkdir()
    metadata_dir.mkdir()
    csv_path = csv_dir / "SB6005-20260224-1600.csv"
    _write_csv(csv_path)
    _write_sidecar(metadata_dir / "SB6005-20260224-1600.hearing.yaml")
    nicknames = tmp_path / "nicknames.csv"
    _write_nickname_map(nicknames)

    payload = module.build_contextual_duplicate_baseline_payload(
        csv_dir=csv_dir,
        metadata_dir=metadata_dir,
        nickname_map_path=nicknames,
        bucket_minutes=[5],
    )

    assert payload["csv_file_count"] == 1
    assert payload["window_row_count"] > 0
    rows = payload["rows"]
    assert rows
    levels = {str(row["level"]) for row in rows}
    assert "bucket" in levels
    assert "hour_weekday_bucket" in levels


def test_write_contextual_outputs(tmp_path: Path) -> None:
    payload = {
        "schema_version": 1,
        "rows": [
            {
                "level": "bucket",
                "committee": "",
                "chamber": "",
                "hour_bin": -1,
                "weekday_bin": -1,
                "bucket_minutes": 5,
                "n_windows": 10,
                "n_rows_total": 200,
                "duplicate_row_rate_mean": 0.1,
                "duplicate_row_rate_median": 0.1,
                "median_n_rows": 20.0,
                "shrink_k": 30.0,
            }
        ],
    }
    json_path = tmp_path / "baseline.json"
    csv_path = tmp_path / "baseline.csv"
    module.write_contextual_baseline_json(json_path, payload)
    module.write_contextual_baseline_csv(csv_path, payload["rows"])

    loaded = json.loads(json_path.read_text(encoding="utf-8"))
    assert loaded["schema_version"] == 1
    csv_lines = csv_path.read_text(encoding="utf-8").splitlines()
    assert csv_lines[0].startswith("level,committee,chamber")


def test_parse_args_supports_cli_flags() -> None:
    args = module.parse_args(
        [
            "--csv-dir",
            "csv-dir",
            "--metadata-dir",
            "meta-dir",
            "--nickname-map-path",
            "nick.csv",
            "--output-json",
            "baseline.json",
            "--output-csv",
            "baseline.csv",
            "--bucket-minutes",
            "5",
            "15",
        ]
    )
    assert str(args.csv_dir) == "csv-dir"
    assert str(args.metadata_dir) == "meta-dir"
    assert str(args.nickname_map_path) == "nick.csv"
    assert str(args.output_json) == "baseline.json"
    assert str(args.output_csv) == "baseline.csv"
    assert [int(value) for value in args.bucket_minutes] == [5, 15]


def test_load_sidecar_context_handles_missing_and_invalid_source(tmp_path: Path) -> None:
    missing = tmp_path / "missing.hearing.yaml"
    assert module.load_sidecar_context(missing) == ("", "")

    invalid = tmp_path / "invalid.hearing.yaml"
    invalid.write_text("source: not-a-dict\n", encoding="utf-8")
    assert module.load_sidecar_context(invalid) == ("", "")


def test_build_window_rows_returns_empty_for_missing_required_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "bad.csv"
    csv_path.write_text("Group,Organization,Position\nTestifying,Org A,Pro\n", encoding="utf-8")
    rows = module.build_window_rows(
        csv_path=csv_path,
        metadata_dir=tmp_path,
        nickname_map={},
        bucket_minutes=[5],
    )
    assert rows == []


def test_build_window_rows_returns_empty_for_invalid_timestamps(tmp_path: Path) -> None:
    csv_path = tmp_path / "bad_time.csv"
    csv_path.write_text(
        "Group,Name,Organization,Position,Time Signed In\n"
        "Testifying,\"Doe, Jane\",Org A,Pro,not-a-time\n",
        encoding="utf-8",
    )
    rows = module.build_window_rows(
        csv_path=csv_path,
        metadata_dir=tmp_path,
        nickname_map={},
        bucket_minutes=[5],
    )
    assert rows == []


def test_build_contextual_duplicate_baseline_payload_handles_no_windows(tmp_path: Path) -> None:
    csv_dir = tmp_path / "csv"
    metadata_dir = tmp_path / "metadata"
    csv_dir.mkdir()
    metadata_dir.mkdir()
    csv_path = csv_dir / "invalid.csv"
    csv_path.write_text("Group,Organization,Position\nTestifying,Org A,Pro\n", encoding="utf-8")
    nicknames = tmp_path / "nicknames.csv"
    _write_nickname_map(nicknames)

    payload = module.build_contextual_duplicate_baseline_payload(
        csv_dir=csv_dir,
        metadata_dir=metadata_dir,
        nickname_map_path=nicknames,
        bucket_minutes=[5],
    )
    assert payload["csv_file_count"] == 1
    assert payload["window_row_count"] == 0
    assert payload["rows"] == []


def test_main_writes_outputs_and_filters_non_dict_rows(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    calls: dict[str, object] = {}

    def _build_payload(**kwargs):
        calls.update(kwargs)
        return {
            "schema_version": 1,
            "rows": [
                {
                    "level": "bucket",
                    "committee": "",
                    "chamber": "",
                    "hour_bin": -1,
                    "weekday_bin": -1,
                    "bucket_minutes": 5,
                    "n_windows": 1,
                    "n_rows_total": 2,
                    "duplicate_row_rate_mean": 0.5,
                    "duplicate_row_rate_median": 0.5,
                    "median_n_rows": 2.0,
                    "shrink_k": 10.0,
                },
                "not-a-row",
            ],
        }

    def _write_json(path: Path, payload: dict[str, object]) -> None:
        calls["json_path"] = path
        calls["json_payload"] = payload

    def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
        calls["csv_path"] = path
        calls["csv_rows"] = rows

    monkeypatch.setattr(module, "build_contextual_duplicate_baseline_payload", _build_payload)
    monkeypatch.setattr(module, "write_contextual_baseline_json", _write_json)
    monkeypatch.setattr(module, "write_contextual_baseline_csv", _write_csv)

    csv_dir = tmp_path / "csv"
    meta_dir = tmp_path / "meta"
    nicknames = tmp_path / "nicknames.csv"
    csv_dir.mkdir()
    meta_dir.mkdir()
    _write_nickname_map(nicknames)
    out_json = tmp_path / "out.json"
    out_csv = tmp_path / "out.csv"
    module.main(
        [
            "--csv-dir",
            str(csv_dir),
            "--metadata-dir",
            str(meta_dir),
            "--nickname-map-path",
            str(nicknames),
            "--output-json",
            str(out_json),
            "--output-csv",
            str(out_csv),
            "--bucket-minutes",
            "0",
            "5",
            "-1",
        ]
    )

    captured = capsys.readouterr()
    assert str(calls["csv_dir"]) == str(csv_dir.resolve())
    assert str(calls["metadata_dir"]) == str(meta_dir.resolve())
    assert str(calls["nickname_map_path"]) == str(nicknames.resolve())
    assert list(calls["bucket_minutes"]) == [5]
    assert str(calls["json_path"]) == str(out_json.resolve())
    assert str(calls["csv_path"]) == str(out_csv.resolve())
    assert len(calls["csv_rows"]) == 1
    assert "Wrote contextual baseline JSON" in captured.out
    assert "Wrote contextual baseline CSV" in captured.out
