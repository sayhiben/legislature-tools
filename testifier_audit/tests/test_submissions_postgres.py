from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from testifier_audit.config import ColumnsConfig
from testifier_audit.io import submissions_postgres as submissions_module
from testifier_audit.io.import_tracking import ImportTrackingRecord
from testifier_audit.io.submissions_postgres import (
    _detect_csv_encoding,
    _iter_submission_chunks,
    _normalize_position,
    _normalize_text,
    _normalize_upper_text,
    _parse_signed_at,
    _split_name,
    _upsert_submission_rows,
    ensure_submission_schema,
    import_submission_csv_to_postgres,
    load_submission_records_from_postgres,
    normalize_submission_chunk,
)


def _columns() -> ColumnsConfig:
    return ColumnsConfig(
        id="Count",
        name="Name",
        organization="Organization",
        position="Position",
        time_signed_in="Time Signed In",
    )


class _FakeSQLText(str):
    def format(self, *args: object, **kwargs: object) -> "_FakeSQLText":
        text = str(self)
        for value in args:
            text = text.replace("{}", str(value), 1)
        for key, value in kwargs.items():
            text = text.replace("{" + key + "}", str(value))
        return _FakeSQLText(text)


class _FakeSQLModule:
    @staticmethod
    def SQL(text: str) -> _FakeSQLText:
        return _FakeSQLText(text)

    @staticmethod
    def Identifier(name: str) -> str:
        return f'"{name}"'


class _FakeCursor:
    def __init__(
        self,
        fetchall_batches: list[list[tuple[Any, ...]]] | None = None,
    ) -> None:
        self.executed: list[tuple[str, object | None]] = []
        self.executemany_calls: list[tuple[str, list[dict[str, object | None]]]] = []
        self._fetchall_batches = list(fetchall_batches or [])

    def execute(self, query: object, params: object | None = None) -> None:
        self.executed.append((str(query), params))

    def executemany(self, query: object, payload: list[dict[str, object | None]]) -> None:
        self.executemany_calls.append((str(query), payload))

    def fetchall(self) -> list[tuple[Any, ...]]:
        if not self._fetchall_batches:
            return []
        return self._fetchall_batches.pop(0)

    def fetchone(self) -> tuple[Any, ...] | None:
        if not self._fetchall_batches:
            return None
        batch = self._fetchall_batches.pop(0)
        if not batch:
            return None
        return batch[0]

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor
        self.commit_count = 0

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.commit_count += 1

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False


class _FakePsycopg:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection
        self.connect_calls: list[str] = []

    def connect(self, db_url: str) -> _FakeConnection:
        self.connect_calls.append(db_url)
        return self._connection


def _fake_psycopg_bundle(
    *,
    fetchall_batches: list[list[tuple[Any, ...]]] | None = None,
) -> tuple[_FakePsycopg, _FakeSQLModule, _FakeConnection, _FakeCursor]:
    cursor = _FakeCursor(fetchall_batches=fetchall_batches)
    conn = _FakeConnection(cursor=cursor)
    psycopg = _FakePsycopg(connection=conn)
    return psycopg, _FakeSQLModule(), conn, cursor


def test_text_and_position_normalization_helpers_cover_edge_cases() -> None:
    assert _normalize_text("  Foo \n\tBar  ") == "Foo Bar"
    assert _normalize_upper_text("  Foo \n\tBar  ") == "FOO BAR"
    assert _split_name("Doe, Jane A") == ("DOE", "JANE A")
    assert _split_name("Single") == ("SINGLE", "")
    assert _split_name("") == ("", "")
    assert _split_name("Jane Doe") == ("DOE", "JANE")
    assert _normalize_position("pro") == "Pro"
    assert _normalize_position("Con") == "Con"
    assert _normalize_position("neutral") == "Unknown"


def test_parse_signed_at_handles_naive_and_aware_inputs() -> None:
    naive = pd.Series(["2/3/2026 5:07 PM", "2026-02-03 17:08"])
    naive_out = _parse_signed_at(naive, timezone="America/Los_Angeles")
    assert str(naive_out.dt.tz) == "America/Los_Angeles"
    assert naive_out.notna().all()

    aware = pd.Series(
        [
            pd.Timestamp("2026-02-03 17:07:00+00:00"),
            pd.Timestamp("2026-02-03 17:08:00+00:00"),
        ]
    )
    aware_out = aware.dt.tz_convert("America/Los_Angeles")
    assert str(aware_out.dt.tz) == "America/Los_Angeles"
    assert aware_out.notna().all()


def test_detect_csv_encoding_covers_bom_cp1252_and_utf8(tmp_path: Path) -> None:
    bom_path = tmp_path / "bom.csv"
    bom_path.write_bytes(b"\xef\xbb\xbfA,B\n1,2\n")
    assert _detect_csv_encoding(bom_path) == "utf-8-sig"

    cp1252_path = tmp_path / "cp1252.csv"
    cp1252_path.write_bytes("A,B\nJos\xe9,1\n".encode("cp1252"))
    assert _detect_csv_encoding(cp1252_path, probe_bytes=8) == "cp1252"

    utf8_path = tmp_path / "utf8.csv"
    utf8_path.write_text("A,B\nJane,1\n", encoding="utf-8")
    assert _detect_csv_encoding(utf8_path) == "utf-8"


def test_iter_submission_chunks_forwards_expected_read_csv_arguments(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("A,B\n1,2\n", encoding="utf-8")
    captured: dict[str, object] = {}
    sentinel = [pd.DataFrame({"A": ["1"], "B": ["2"]})]

    def _fake_read_csv(path: Path, **kwargs: object) -> list[pd.DataFrame]:
        captured["path"] = path
        captured["kwargs"] = kwargs
        return sentinel

    monkeypatch.setattr(submissions_module, "_detect_csv_encoding", lambda _path: "cp1252")
    monkeypatch.setattr(submissions_module.pd, "read_csv", _fake_read_csv)

    out = _iter_submission_chunks(csv_path, chunk_size=1234)
    assert out == sentinel
    assert captured["path"] == csv_path
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["encoding"] == "cp1252"
    assert kwargs["chunksize"] == 1234


def test_normalize_submission_chunk_empty_returns_expected_columns() -> None:
    out = normalize_submission_chunk(
        chunk=pd.DataFrame(),
        source_file="sample.csv",
        columns=_columns(),
        timezone="America/Los_Angeles",
        row_number_offset=0,
    )
    assert out.empty
    assert "submission_key" in out.columns
    assert "minute_bucket" in out.columns


def test_normalize_submission_chunk_derives_expected_fields() -> None:
    chunk = pd.DataFrame(
        {
            "Count": ["1", "2"],
            "Name": ["Doe, Jane", "Smith, John"],
            "Organization": ["", "Org A"],
            "Position": ["Pro", "Con"],
            "Time Signed In": ["2/3/2026 5:07 PM", "2/3/2026 5:08 PM"],
        }
    )

    out = normalize_submission_chunk(
        chunk=chunk,
        source_file="SB6346-20260206-1330.csv",
        columns=_columns(),
        timezone="America/Los_Angeles",
        row_number_offset=10,
    )

    assert len(out) == 2
    assert out.loc[0, "submission_key"] == "SB6346-20260206-1330.csv:11"
    assert out.loc[1, "submission_key"] == "SB6346-20260206-1330.csv:12"
    assert out.loc[0, "name_last"] == "DOE"
    assert out.loc[0, "name_first"] == "JANE"
    assert bool(out.loc[0, "organization_is_blank"]) is True
    assert bool(out.loc[1, "organization_is_blank"]) is False
    assert out.loc[0, "position_normalized"] == "Pro"
    assert out.loc[1, "position_normalized"] == "Con"
    assert pd.notna(out.loc[0, "signed_at"])
    assert pd.notna(out.loc[0, "minute_bucket"])


def test_ensure_submission_schema_executes_create_statement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    psycopg, fake_sql, conn, cursor = _fake_psycopg_bundle()
    monkeypatch.setattr(submissions_module, "_load_psycopg", lambda: (psycopg, fake_sql))

    ensure_submission_schema(conn=conn, table_name="public_submissions")

    assert len(cursor.executed) == 1
    statement, _params = cursor.executed[0]
    assert "CREATE TABLE IF NOT EXISTS" in statement
    assert '"public_submissions"' in statement


def test_upsert_submission_rows_handles_empty_and_non_empty_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    psycopg, fake_sql, conn, cursor = _fake_psycopg_bundle()
    monkeypatch.setattr(submissions_module, "_load_psycopg", lambda: (psycopg, fake_sql))

    assert (
        _upsert_submission_rows(
            conn=conn,
            table_name="public_submissions",
            rows=pd.DataFrame(),
        )
        == 0
    )
    assert cursor.executemany_calls == []

    rows = pd.DataFrame(
        [
            {
                "submission_key": "file.csv:1",
                "source_file": "file.csv",
                "source_row_number": 1,
                "source_hash": "abc",
                "source_id": "1",
                "name_raw": "Doe, Jane",
                "name_clean": "DOE, JANE",
                "name_last": "DOE",
                "name_first": "JANE",
                "organization_raw": "",
                "organization_clean": "",
                "organization_is_blank": True,
                "position_raw": "Pro",
                "position_normalized": "Pro",
                "time_signed_in_raw": "2/3/2026 5:07 PM",
                "signed_at": pd.Timestamp("2026-02-03 17:07:00", tz="America/Los_Angeles"),
                "minute_bucket": pd.Timestamp("2026-02-03 17:07:00", tz="America/Los_Angeles"),
            },
            {
                "submission_key": "file.csv:2",
                "source_file": "file.csv",
                "source_row_number": 2,
                "source_hash": "def",
                "source_id": None,
                "name_raw": "Smith, John",
                "name_clean": "SMITH, JOHN",
                "name_last": "SMITH",
                "name_first": "JOHN",
                "organization_raw": "Org",
                "organization_clean": "ORG",
                "organization_is_blank": False,
                "position_raw": "Con",
                "position_normalized": "Con",
                "time_signed_in_raw": "2/3/2026 5:08 PM",
                "signed_at": pd.NaT,
                "minute_bucket": pd.NaT,
            },
        ]
    )
    count = _upsert_submission_rows(conn=conn, table_name="public_submissions", rows=rows)
    assert count == 2
    assert len(cursor.executemany_calls) == 1
    query, payload = cursor.executemany_calls[0]
    assert "INSERT INTO" in query
    assert len(payload) == 2
    assert pd.isna(payload[1]["signed_at"])
    assert pd.isna(payload[1]["minute_bucket"])


def test_import_submission_csv_to_postgres_aggregates_metrics(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("unused", encoding="utf-8")
    psycopg, fake_sql, conn, _cursor = _fake_psycopg_bundle()
    monkeypatch.setattr(submissions_module, "_load_psycopg", lambda: (psycopg, fake_sql))
    monkeypatch.setattr(
        submissions_module,
        "ensure_submission_schema",
        lambda conn, table_name: None,
    )
    monkeypatch.setattr(submissions_module, "ensure_import_tracking_schema", lambda conn: None)
    monkeypatch.setattr(submissions_module, "find_completed_import", lambda **kwargs: None)
    monkeypatch.setattr(
        submissions_module,
        "record_import_result",
        lambda **kwargs: 1,
    )

    chunk_one = pd.DataFrame({"a": ["1", "2"]})
    chunk_two = pd.DataFrame({"a": ["3"]})
    monkeypatch.setattr(
        submissions_module,
        "_iter_submission_chunks",
        lambda path, chunk_size: [chunk_one, chunk_two],
    )

    normalized_one = pd.DataFrame(
        {
            "organization_is_blank": [True, False],
            "signed_at": [
                pd.Timestamp("2026-02-03 17:07:00", tz="America/Los_Angeles"),
                pd.NaT,
            ],
        }
    )
    normalized_two = pd.DataFrame(columns=["organization_is_blank", "signed_at"])
    calls: list[int] = []

    def _fake_normalize(
        chunk: pd.DataFrame,
        source_file: str,
        columns: ColumnsConfig,
        timezone: str,
        row_number_offset: int,
    ) -> pd.DataFrame:
        calls.append(row_number_offset)
        return normalized_one if len(calls) == 1 else normalized_two

    monkeypatch.setattr(submissions_module, "normalize_submission_chunk", _fake_normalize)
    monkeypatch.setattr(
        submissions_module,
        "_upsert_submission_rows",
        lambda conn, table_name, rows: len(rows),
    )

    result = import_submission_csv_to_postgres(
        csv_path=csv_path,
        db_url="postgresql://example",
        columns=_columns(),
        timezone="America/Los_Angeles",
        table_name="public_submissions",
        chunk_size=1000,
        source_file="override.csv",
    )

    assert result.source_file == "override.csv"
    assert result.rows_processed == 3
    assert result.rows_upserted == 2
    assert result.rows_blank_organization == 1
    assert result.rows_invalid_timestamp == 1
    assert result.file_hash
    assert conn.commit_count == 3
    assert calls == [0, 2]
    assert psycopg.connect_calls == ["postgresql://example"]


def test_import_submission_csv_to_postgres_skips_when_checksum_seen(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("unused", encoding="utf-8")

    psycopg, fake_sql, conn, _cursor = _fake_psycopg_bundle()
    monkeypatch.setattr(submissions_module, "_load_psycopg", lambda: (psycopg, fake_sql))
    monkeypatch.setattr(
        submissions_module,
        "ensure_submission_schema",
        lambda conn, table_name: None,
    )
    monkeypatch.setattr(submissions_module, "ensure_import_tracking_schema", lambda conn: None)
    monkeypatch.setattr(
        submissions_module,
        "find_completed_import",
        lambda **kwargs: ImportTrackingRecord(
            import_id=7,
            import_kind="submissions_csv",
            target_table="public_submissions",
            source_file="sample.csv",
            file_hash="abc",
            importer_version="submissions_csv_v1",
            status="completed",
            rows_processed=10,
            rows_upserted=10,
        ),
    )
    record_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        submissions_module,
        "record_import_result",
        lambda **kwargs: (record_calls.append(kwargs) or 8),
    )
    monkeypatch.setattr(
        submissions_module,
        "_iter_submission_chunks",
        lambda path, chunk_size: pytest.fail("memoized import should skip chunk iteration"),
    )

    result = import_submission_csv_to_postgres(
        csv_path=csv_path,
        db_url="postgresql://example",
        columns=_columns(),
        timezone="America/Los_Angeles",
        table_name="public_submissions",
        chunk_size=1000,
        source_file="sample.csv",
    )

    assert result.import_skipped is True
    assert result.rows_processed == 0
    assert result.rows_upserted == 0
    assert result.previous_import_id == 7
    assert record_calls and record_calls[0]["status"] == "skipped"


def test_import_submission_csv_to_postgres_force_bypasses_checksum_skip(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("unused", encoding="utf-8")

    psycopg, fake_sql, conn, _cursor = _fake_psycopg_bundle()
    monkeypatch.setattr(submissions_module, "_load_psycopg", lambda: (psycopg, fake_sql))
    monkeypatch.setattr(
        submissions_module,
        "ensure_submission_schema",
        lambda conn, table_name: None,
    )
    monkeypatch.setattr(submissions_module, "ensure_import_tracking_schema", lambda conn: None)
    monkeypatch.setattr(
        submissions_module,
        "find_completed_import",
        lambda **kwargs: ImportTrackingRecord(
            import_id=9,
            import_kind="submissions_csv",
            target_table="public_submissions",
            source_file="sample.csv",
            file_hash="abc",
            importer_version="submissions_csv_v1",
            status="completed",
            rows_processed=10,
            rows_upserted=10,
        ),
    )
    monkeypatch.setattr(
        submissions_module,
        "_iter_submission_chunks",
        lambda path, chunk_size: [pd.DataFrame({"a": ["1"]})],
    )
    monkeypatch.setattr(
        submissions_module,
        "normalize_submission_chunk",
        lambda **kwargs: pd.DataFrame(
            {
                "organization_is_blank": [False],
                "signed_at": [pd.Timestamp("2026-02-03 17:07:00", tz="America/Los_Angeles")],
            }
        ),
    )
    monkeypatch.setattr(submissions_module, "_upsert_submission_rows", lambda **kwargs: 1)
    statuses: list[str] = []
    monkeypatch.setattr(
        submissions_module,
        "record_import_result",
        lambda **kwargs: (statuses.append(str(kwargs["status"])) or 10),
    )

    result = import_submission_csv_to_postgres(
        csv_path=csv_path,
        db_url="postgresql://example",
        columns=_columns(),
        timezone="America/Los_Angeles",
        table_name="public_submissions",
        chunk_size=1000,
        source_file="sample.csv",
        force=True,
    )

    assert result.import_skipped is False
    assert result.rows_upserted == 1
    assert statuses == ["completed"]


def test_load_submission_records_from_postgres_with_and_without_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [("1", "Doe, Jane", "", "Pro", "2/3/2026 5:07 PM")]
    psycopg, fake_sql, _conn, cursor = _fake_psycopg_bundle(fetchall_batches=[rows, []])
    monkeypatch.setattr(submissions_module, "_load_psycopg", lambda: (psycopg, fake_sql))

    loaded = load_submission_records_from_postgres(
        db_url="postgresql://example",
        table_name="public_submissions",
        source_file="SB6346.csv",
    )
    assert list(loaded.columns) == ["id", "name", "organization", "position", "time_signed_in"]
    assert len(loaded) == 1
    assert loaded.loc[0, "name"] == "Doe, Jane"
    assert cursor.executed[0][1] == ["SB6346.csv"]
    assert "WHERE source_file = %s" in cursor.executed[0][0]

    empty = load_submission_records_from_postgres(
        db_url="postgresql://example",
        table_name="public_submissions",
        source_file=None,
    )
    assert empty.empty
    assert list(empty.columns) == ["id", "name", "organization", "position", "time_signed_in"]


def test_import_submission_csv_rejects_too_small_chunk_size() -> None:
    with pytest.raises(ValueError):
        import_submission_csv_to_postgres(
            csv_path=Path("unused.csv"),
            db_url="postgresql://unused",
            columns=_columns(),
            timezone="America/Los_Angeles",
            chunk_size=999,
        )
