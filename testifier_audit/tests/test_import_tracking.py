from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from testifier_audit.io import import_tracking as tracking


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
    def __init__(self, fetchone_values: list[tuple[Any, ...] | None] | None = None) -> None:
        self.executed: list[tuple[str, object | None]] = []
        self._fetchone_values = list(fetchone_values or [])

    def execute(self, query: object, params: object | None = None) -> None:
        self.executed.append((str(query), params))

    def fetchone(self) -> tuple[Any, ...] | None:
        if not self._fetchone_values:
            return None
        return self._fetchone_values.pop(0)

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_compute_file_sha256_is_stable(tmp_path: Path) -> None:
    payload = "hello-checksum\n"
    file_path = tmp_path / "sample.txt"
    file_path.write_text(payload, encoding="utf-8")
    first = tracking.compute_file_sha256(file_path)
    second = tracking.compute_file_sha256(file_path)
    assert first == second
    assert len(first) == 64


def test_ensure_import_tracking_schema_executes_create_statement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor()
    conn = _FakeConnection(cursor=cursor)
    monkeypatch.setattr(tracking, "_load_psycopg", lambda: _FakeSQLModule())

    tracking.ensure_import_tracking_schema(conn=conn, table_name="data_imports")

    assert len(cursor.executed) == 1
    statement, _params = cursor.executed[0]
    assert "CREATE TABLE IF NOT EXISTS" in statement
    assert '"data_imports"' in statement


def test_find_completed_import_returns_record_when_match_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row = (
        9,
        "submissions_csv",
        "public_submissions",
        "sample.csv",
        "abc",
        "submissions_csv_v1",
        "completed",
        100,
        100,
    )
    cursor = _FakeCursor(fetchone_values=[row])
    conn = _FakeConnection(cursor=cursor)
    monkeypatch.setattr(tracking, "_load_psycopg", lambda: _FakeSQLModule())

    out = tracking.find_completed_import(
        conn=conn,
        import_kind="submissions_csv",
        target_table="public_submissions",
        file_hash="abc",
        importer_version="submissions_csv_v1",
    )

    assert out is not None
    assert out.import_id == 9
    assert out.rows_processed == 100
    assert out.rows_upserted == 100
    assert cursor.executed


def test_find_completed_import_returns_none_when_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(fetchone_values=[None])
    conn = _FakeConnection(cursor=cursor)
    monkeypatch.setattr(tracking, "_load_psycopg", lambda: _FakeSQLModule())

    out = tracking.find_completed_import(
        conn=conn,
        import_kind="vrdb_extract",
        target_table="voter_registry",
        file_hash="abc",
        importer_version="vrdb_extract_v1",
    )

    assert out is None


def test_record_import_result_inserts_row_and_returns_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(fetchone_values=[(42,)])
    conn = _FakeConnection(cursor=cursor)
    monkeypatch.setattr(tracking, "_load_psycopg", lambda: _FakeSQLModule())

    import_id = tracking.record_import_result(
        conn=conn,
        import_kind="submissions_csv",
        target_table="public_submissions",
        source_file="sample.csv",
        file_hash="abc",
        file_size_bytes=123,
        importer_version="submissions_csv_v1",
        status="completed",
        rows_processed=100,
        rows_upserted=100,
        message="ok",
        metadata={"force": False},
    )

    assert import_id == 42
    assert len(cursor.executed) == 1
    statement, params = cursor.executed[0]
    assert "INSERT INTO" in statement
    assert params is not None


def test_record_import_result_rejects_invalid_status() -> None:
    cursor = _FakeCursor(fetchone_values=[(1,)])
    conn = _FakeConnection(cursor=cursor)
    with pytest.raises(ValueError):
        tracking.record_import_result(
            conn=conn,
            import_kind="submissions_csv",
            target_table="public_submissions",
            source_file="sample.csv",
            file_hash="abc",
            file_size_bytes=123,
            importer_version="submissions_csv_v1",
            status="running",
            rows_processed=0,
            rows_upserted=0,
        )
