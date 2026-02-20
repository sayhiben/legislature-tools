from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from testifier_audit.io import vrdb_postgres as vrdb_module
from testifier_audit.io.vrdb_postgres import (
    _chunk_values,
    _detect_vrdb_encoding,
    _fallback_voter_key,
    _iter_vrdb_chunks,
    _resolve_column,
    _upsert_vrdb_rows,
    count_registry_rows,
    ensure_voter_registry_schema,
    fetch_matching_voter_names,
    import_vrdb_extract_to_postgres,
    normalize_vrdb_chunk,
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
        fetchone_values: list[tuple[Any, ...] | None] | None = None,
    ) -> None:
        self.executed: list[tuple[str, object | None]] = []
        self.executemany_calls: list[tuple[str, list[dict[str, object | None]]]] = []
        self._fetchall_batches = list(fetchall_batches or [])
        self._fetchone_values = list(fetchone_values or [])

    def execute(self, query: object, params: object | None = None) -> None:
        self.executed.append((str(query), params))

    def executemany(self, query: object, payload: list[dict[str, object | None]]) -> None:
        self.executemany_calls.append((str(query), payload))

    def fetchall(self) -> list[tuple[Any, ...]]:
        if not self._fetchall_batches:
            return []
        return self._fetchall_batches.pop(0)

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
    fetchone_values: list[tuple[Any, ...] | None] | None = None,
) -> tuple[_FakePsycopg, _FakeSQLModule, _FakeConnection, _FakeCursor]:
    cursor = _FakeCursor(fetchall_batches=fetchall_batches, fetchone_values=fetchone_values)
    conn = _FakeConnection(cursor=cursor)
    psycopg = _FakePsycopg(connection=conn)
    return psycopg, _FakeSQLModule(), conn, cursor


def test_resolve_column_and_chunk_values_helpers() -> None:
    assert _resolve_column(["StateVoterID", "FName"], ("statevoterid",)) == "StateVoterID"
    assert _resolve_column(["foo"], ("StateVoterID",)) is None
    assert list(_chunk_values(["a", "b", "c", "d"], chunk_size=3)) == [["a", "b", "c"], ["d"]]


def test_fallback_voter_key_uses_name_birthyear_suffix_fingerprint() -> None:
    frame = pd.DataFrame(
        {
            "canonical_last": ["DOE"],
            "canonical_first": ["JANE"],
            "birth_year": ["1980"],
            "name_suffix": ["JR"],
        }
    )
    out = _fallback_voter_key(frame)
    assert out.iloc[0].startswith("NAME:")
    assert len(out.iloc[0]) == 45


def test_normalize_vrdb_chunk_standardizes_and_filters_names() -> None:
    chunk = pd.DataFrame(
        {
            "StateVoterID": ["12345", "", "77777"],
            "FName": ["Jane", "John", ""],
            "LName": ["Doe", "Smith", "NoFirst"],
            "MName": ["A", "", "X"],
            "Birthyear": ["1980", "1975", "1990"],
            "StatusCode": ["Active", "Active", "Inactive"],
        }
    )

    normalized = normalize_vrdb_chunk(chunk=chunk, source_file="sample.txt")
    assert len(normalized) == 2
    assert set(normalized["canonical_name"]) == {"DOE|JANE", "SMITH|JOHN"}
    assert "STATE:12345" in set(normalized["voter_key"])
    fallback_keys = [
        value for value in normalized["voter_key"].tolist() if value.startswith("NAME:")
    ]
    assert len(fallback_keys) == 1
    assert normalized["source_file"].nunique() == 1
    assert normalized["source_file"].iloc[0] == "sample.txt"


def test_normalize_vrdb_chunk_empty_and_missing_columns_paths() -> None:
    empty = normalize_vrdb_chunk(chunk=pd.DataFrame(), source_file="empty.txt")
    assert empty.empty
    assert "canonical_name" in empty.columns

    with pytest.raises(ValueError, match="first and last name columns"):
        normalize_vrdb_chunk(chunk=pd.DataFrame({"OnlyCol": ["x"]}), source_file="bad.txt")


def test_normalize_vrdb_chunk_without_optional_columns_applies_defaults() -> None:
    chunk = pd.DataFrame(
        {
            "FirstName": [" Jane ", " "],
            "LastName": [" Doe ", "MissingFirst"],
            "StatusCode": ["Active", "Inactive"],
        }
    )
    out = normalize_vrdb_chunk(chunk=chunk, source_file="simple.txt")
    assert len(out) == 1
    assert out.loc[out.index[0], "state_voter_id"] == ""
    assert out.loc[out.index[0], "middle_name"] == ""
    assert out.loc[out.index[0], "name_suffix"] == ""
    assert out.loc[out.index[0], "birth_year"] == ""
    assert out.loc[out.index[0], "canonical_name"] == "DOE|JANE"


def test_detect_vrdb_encoding_covers_bom_cp1252_and_utf8(tmp_path: Path) -> None:
    bom_path = tmp_path / "bom.txt"
    bom_path.write_bytes(b"\xef\xbb\xbfStateVoterID|FName|LName\n1|Jane|Doe\n")
    assert _detect_vrdb_encoding(bom_path) == "utf-8-sig"

    cp1252_path = tmp_path / "cp1252.txt"
    cp1252_path.write_bytes("StateVoterID|FName|LName\n1|Jos\xe9|Garc\xeda\n".encode("cp1252"))
    assert _detect_vrdb_encoding(cp1252_path, probe_bytes=8) == "cp1252"

    utf8_path = tmp_path / "utf8.txt"
    utf8_path.write_text("StateVoterID|FName|LName\n1|Jane|Doe\n", encoding="utf-8")
    assert _detect_vrdb_encoding(utf8_path) == "utf-8-sig"


def test_iter_vrdb_chunks_forwards_expected_read_csv_arguments(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    extract_path = tmp_path / "extract.txt"
    extract_path.write_text("StateVoterID|FName|LName\n1|Jane|Doe\n", encoding="utf-8")
    sentinel = [pd.DataFrame({"StateVoterID": ["1"], "FName": ["Jane"], "LName": ["Doe"]})]
    captured: dict[str, object] = {}

    def _fake_read_csv(path: Path, **kwargs: object) -> list[pd.DataFrame]:
        captured["path"] = path
        captured["kwargs"] = kwargs
        return sentinel

    monkeypatch.setattr(vrdb_module, "_detect_vrdb_encoding", lambda _path: "cp1252")
    monkeypatch.setattr(vrdb_module.pd, "read_csv", _fake_read_csv)
    out = _iter_vrdb_chunks(extract_path, chunk_size=4321)
    assert out == sentinel
    assert captured["path"] == extract_path
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["encoding"] == "cp1252"
    assert kwargs["sep"] == "|"
    assert kwargs["chunksize"] == 4321


def test_ensure_voter_registry_schema_executes_create_statement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    psycopg, fake_sql, conn, cursor = _fake_psycopg_bundle()
    monkeypatch.setattr(vrdb_module, "_load_psycopg", lambda: (psycopg, fake_sql))

    ensure_voter_registry_schema(conn=conn, table_name="voter_registry")

    assert len(cursor.executed) == 1
    statement, _params = cursor.executed[0]
    assert "CREATE TABLE IF NOT EXISTS" in statement
    assert '"voter_registry"' in statement


def test_upsert_vrdb_rows_handles_empty_and_non_empty_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    psycopg, fake_sql, conn, cursor = _fake_psycopg_bundle()
    monkeypatch.setattr(vrdb_module, "_load_psycopg", lambda: (psycopg, fake_sql))

    assert _upsert_vrdb_rows(conn=conn, table_name="voter_registry", rows=pd.DataFrame()) == 0
    assert cursor.executemany_calls == []

    rows = pd.DataFrame(
        [
            {
                "voter_key": "STATE:1",
                "state_voter_id": "1",
                "first_name": "Jane",
                "middle_name": "",
                "last_name": "Doe",
                "name_suffix": "",
                "birth_year": "1980",
                "status_code": "Active",
                "canonical_first": "JANE",
                "canonical_last": "DOE",
                "canonical_name": "DOE|JANE",
                "source_file": "extract.txt",
                "source_hash": "abc",
            },
            {
                "voter_key": "NAME:hash",
                "state_voter_id": "",
                "first_name": "John",
                "middle_name": None,
                "last_name": "Smith",
                "name_suffix": "",
                "birth_year": "",
                "status_code": "",
                "canonical_first": "JOHN",
                "canonical_last": "SMITH",
                "canonical_name": "SMITH|JOHN",
                "source_file": "extract.txt",
                "source_hash": "def",
            },
        ]
    )
    count = _upsert_vrdb_rows(conn=conn, table_name="voter_registry", rows=rows)
    assert count == 2
    assert len(cursor.executemany_calls) == 1
    _query, payload = cursor.executemany_calls[0]
    assert len(payload) == 2
    assert payload[1]["middle_name"] is None


def test_import_vrdb_extract_to_postgres_aggregates_metrics(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    extract_path = tmp_path / "extract.txt"
    extract_path.write_text("unused", encoding="utf-8")

    psycopg, fake_sql, conn, _cursor = _fake_psycopg_bundle()
    monkeypatch.setattr(vrdb_module, "_load_psycopg", lambda: (psycopg, fake_sql))
    monkeypatch.setattr(vrdb_module, "ensure_voter_registry_schema", lambda conn, table_name: None)

    chunk_one = pd.DataFrame({"row": ["1", "2"]})
    chunk_two = pd.DataFrame({"row": ["3"]})
    monkeypatch.setattr(
        vrdb_module,
        "_iter_vrdb_chunks",
        lambda path, chunk_size: [chunk_one, chunk_two],
    )

    normalized_one = pd.DataFrame(
        {
            "state_voter_id": ["1", ""],
            "canonical_name": ["DOE|JANE", "SMITH|JOHN"],
        }
    )
    normalized_two = pd.DataFrame(columns=["state_voter_id", "canonical_name"])
    calls: list[int] = []

    def _fake_normalize(chunk: pd.DataFrame, source_file: str) -> pd.DataFrame:
        calls.append(len(chunk))
        return normalized_one if len(calls) == 1 else normalized_two

    monkeypatch.setattr(vrdb_module, "normalize_vrdb_chunk", _fake_normalize)
    monkeypatch.setattr(vrdb_module, "_upsert_vrdb_rows", lambda conn, table_name, rows: len(rows))

    result = import_vrdb_extract_to_postgres(
        extract_path=extract_path,
        db_url="postgresql://example",
        table_name="voter_registry",
        chunk_size=1000,
    )

    assert result.source_file == "extract.txt"
    assert result.rows_processed == 3
    assert result.rows_upserted == 2
    assert result.rows_with_state_voter_id == 1
    assert result.rows_with_canonical_name == 2
    assert conn.commit_count == 2
    assert calls == [2, 1]
    assert psycopg.connect_calls == ["postgresql://example"]


def test_import_vrdb_extract_rejects_too_small_chunk_size() -> None:
    with pytest.raises(ValueError):
        import_vrdb_extract_to_postgres(
            extract_path=Path("unused.txt"),
            db_url="postgresql://unused",
            chunk_size=999,
        )


def test_fetch_matching_voter_names_empty_input_short_circuit() -> None:
    result = fetch_matching_voter_names(
        db_url="postgresql://unused",
        table_name="voter_registry",
        canonical_names=[],
        active_only=True,
    )
    assert result.empty
    assert list(result.columns) == ["canonical_name", "n_registry_rows"]


def test_fetch_matching_voter_names_queries_chunks_and_supports_active_only_toggle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    psycopg, fake_sql, _conn, cursor = _fake_psycopg_bundle(
        fetchall_batches=[[("DOE|JANE", 2)], [("SMITH|JOHN", 1)]]
    )
    monkeypatch.setattr(vrdb_module, "_load_psycopg", lambda: (psycopg, fake_sql))
    monkeypatch.setattr(
        vrdb_module,
        "_chunk_values",
        lambda values, chunk_size=10_000: [["DOE|JANE"], ["SMITH|JOHN"]],
    )

    out = fetch_matching_voter_names(
        db_url="postgresql://example",
        table_name="voter_registry",
        canonical_names=["DOE|JANE", "SMITH|JOHN"],
        active_only=False,
    )
    assert set(out["canonical_name"]) == {"DOE|JANE", "SMITH|JOHN"}
    assert len(cursor.executed) == 2
    assert "LOWER(status_code) = 'active'" not in cursor.executed[0][0]

    cursor.executed.clear()
    cursor._fetchall_batches = [[("DOE|JANE", 2)]]
    monkeypatch.setattr(
        vrdb_module,
        "_chunk_values",
        lambda values, chunk_size=10_000: [["DOE|JANE"]],
    )
    fetch_matching_voter_names(
        db_url="postgresql://example",
        table_name="voter_registry",
        canonical_names=["DOE|JANE"],
        active_only=True,
    )
    assert "LOWER(status_code) = 'active'" in cursor.executed[0][0]


def test_count_registry_rows_handles_none_and_non_none_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    psycopg, fake_sql, _conn, cursor = _fake_psycopg_bundle(fetchone_values=[(5,), (None,), None])
    monkeypatch.setattr(vrdb_module, "_load_psycopg", lambda: (psycopg, fake_sql))

    assert count_registry_rows("postgresql://example", "voter_registry", active_only=True) == 5
    assert "LOWER(status_code) = 'active'" in cursor.executed[0][0]

    assert count_registry_rows("postgresql://example", "voter_registry", active_only=False) == 0
    assert "LOWER(status_code) = 'active'" not in cursor.executed[1][0]

    assert count_registry_rows("postgresql://example", "voter_registry", active_only=False) == 0


def test_iter_vrdb_chunks_falls_back_to_cp1252(tmp_path: Path) -> None:
    extract_path = tmp_path / "vrdb_cp1252.txt"
    extract_path.write_bytes("StateVoterID|FName|LName\n1|Jos\xe9|Garc\xeda\n".encode("cp1252"))

    chunks = list(_iter_vrdb_chunks(path=extract_path, chunk_size=1000))

    assert len(chunks) == 1
    assert chunks[0].loc[0, "FName"] == "José"
    assert chunks[0].loc[0, "LName"] == "García"
