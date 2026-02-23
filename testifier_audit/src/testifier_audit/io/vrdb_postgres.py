from __future__ import annotations

import codecs
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from typing import Iterable

import pandas as pd

from testifier_audit.features.rarity import normalize_name_token
from testifier_audit.io.import_tracking import (
    compute_file_sha256,
    ensure_import_tracking_schema,
    find_completed_import,
    record_import_result,
)

ID_COLUMN_CANDIDATES = (
    "StateVoterID",
    "statevoterid",
    "voter_id",
    "voterid",
)
FIRST_COLUMN_CANDIDATES = ("FName", "FirstName", "first_name", "First")
MIDDLE_COLUMN_CANDIDATES = ("MName", "MiddleName", "middle_name", "Middle")
LAST_COLUMN_CANDIDATES = ("LName", "LastName", "last_name", "Last")
SUFFIX_COLUMN_CANDIDATES = ("NameSuffix", "Suffix", "name_suffix")
BIRTH_YEAR_COLUMN_CANDIDATES = ("Birthyear", "BirthYear", "birth_year")
STATUS_COLUMN_CANDIDATES = ("StatusCode", "status_code", "Status")
IMPORT_KIND_VRDB = "vrdb_extract"
VRDB_IMPORTER_VERSION = "vrdb_extract_v2"
ALLOWED_NAME_KEY_COLUMNS = frozenset(
    {
        "canonical_name",
        "canonical_key_strict",
        "canonical_key_medium",
        "canonical_key_loose",
        "canonical_key_nickname",
    }
)


@dataclass(frozen=True)
class VRDBImportResult:
    source_file: str
    table_name: str
    rows_processed: int
    rows_upserted: int
    rows_with_state_voter_id: int
    rows_with_canonical_name: int
    chunk_size: int
    file_hash: str = ""
    import_skipped: bool = False
    skip_reason: str | None = None
    previous_import_id: int | None = None


def _resolve_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    lowered = {column.lower(): column for column in columns}
    for candidate in candidates:
        match = lowered.get(candidate.lower())
        if match:
            return match
    return None


def _load_psycopg():
    try:
        import psycopg
        from psycopg import sql
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "psycopg is required for VRDB PostgreSQL operations. "
            "Install with: pip install 'psycopg[binary]'"
        ) from exc
    return psycopg, sql


def _fallback_voter_key(frame: pd.DataFrame) -> pd.Series:
    basis = (
        frame["canonical_last"].fillna("")
        + "|"
        + frame["canonical_first"].fillna("")
        + "|"
        + frame["birth_year"].fillna("")
        + "|"
        + frame["name_suffix"].fillna("")
    )
    return basis.map(lambda value: "NAME:" + sha1(value.encode("utf-8")).hexdigest())


def normalize_vrdb_chunk(chunk: pd.DataFrame, source_file: str) -> pd.DataFrame:
    if chunk.empty:
        return pd.DataFrame(
            columns=[
                "voter_key",
                "state_voter_id",
                "first_name",
                "middle_name",
                "last_name",
                "name_suffix",
                "birth_year",
                "status_code",
                "canonical_first",
                "canonical_last",
                "canonical_name",
                "canonical_middle_initial",
                "canonical_suffix",
                "canonical_key_strict",
                "canonical_key_medium",
                "canonical_key_loose",
                "canonical_key_nickname",
                "source_file",
                "source_hash",
            ]
        )

    columns = list(chunk.columns)
    first_col = _resolve_column(columns, FIRST_COLUMN_CANDIDATES)
    last_col = _resolve_column(columns, LAST_COLUMN_CANDIDATES)
    if first_col is None or last_col is None:
        raise ValueError(
            "VRDB extract must contain first and last name columns. "
            f"Found columns: {', '.join(columns)}"
        )

    state_id_col = _resolve_column(columns, ID_COLUMN_CANDIDATES)
    middle_col = _resolve_column(columns, MIDDLE_COLUMN_CANDIDATES)
    suffix_col = _resolve_column(columns, SUFFIX_COLUMN_CANDIDATES)
    birth_year_col = _resolve_column(columns, BIRTH_YEAR_COLUMN_CANDIDATES)
    status_col = _resolve_column(columns, STATUS_COLUMN_CANDIDATES)

    out = pd.DataFrame(index=chunk.index)
    out["state_voter_id"] = (
        chunk[state_id_col].fillna("").astype(str).str.strip() if state_id_col is not None else ""
    )
    out["first_name"] = chunk[first_col].fillna("").astype(str).str.strip()
    out["middle_name"] = chunk[middle_col].fillna("").astype(str).str.strip() if middle_col else ""
    out["last_name"] = chunk[last_col].fillna("").astype(str).str.strip()
    out["name_suffix"] = chunk[suffix_col].fillna("").astype(str).str.strip() if suffix_col else ""
    out["birth_year"] = (
        chunk[birth_year_col].fillna("").astype(str).str.strip() if birth_year_col else ""
    )
    out["status_code"] = chunk[status_col].fillna("").astype(str).str.strip() if status_col else ""
    out["canonical_first"] = out["first_name"].map(normalize_name_token)
    out["canonical_last"] = out["last_name"].map(normalize_name_token)
    out["canonical_name"] = out["canonical_last"] + "|" + out["canonical_first"]
    out["canonical_middle_initial"] = (
        out["middle_name"].map(normalize_name_token).str[:1].fillna("").astype(str)
    )
    out["canonical_suffix"] = out["name_suffix"].map(normalize_name_token)
    out["canonical_key_strict"] = (
        out["canonical_last"]
        + "|"
        + out["canonical_first"]
        + "|"
        + out["canonical_middle_initial"]
        + "|"
        + out["canonical_suffix"]
    )
    out["canonical_key_medium"] = out["canonical_last"] + "|" + out["canonical_first"]
    out["canonical_key_loose"] = out["canonical_last"] + "|" + out["canonical_first"].str[:1]
    out["canonical_key_nickname"] = out["canonical_key_medium"]
    out["source_file"] = source_file

    fingerprint = (
        out["state_voter_id"].fillna("")
        + "|"
        + out["canonical_last"].fillna("")
        + "|"
        + out["canonical_first"].fillna("")
        + "|"
        + out["birth_year"].fillna("")
        + "|"
        + out["status_code"].fillna("")
    )
    out["source_hash"] = fingerprint.map(lambda value: sha1(value.encode("utf-8")).hexdigest())
    out["voter_key"] = out["state_voter_id"].map(
        lambda value: f"STATE:{value}" if str(value).strip() else ""
    )

    missing_key = out["voter_key"] == ""
    out.loc[missing_key, "voter_key"] = _fallback_voter_key(out.loc[missing_key])

    has_name = (out["canonical_last"] != "") & (out["canonical_first"] != "")
    return out[has_name].copy()


def _detect_vrdb_encoding(path: Path, probe_bytes: int = 1 << 20) -> str:
    with path.open("rb") as handle:
        prefix = handle.read(3)
        if prefix.startswith(codecs.BOM_UTF8):
            return "utf-8-sig"

    decoder = codecs.getincrementaldecoder("utf-8")()
    with path.open("rb") as handle:
        while True:
            block = handle.read(probe_bytes)
            if not block:
                break
            try:
                decoder.decode(block)
            except UnicodeDecodeError:
                return "cp1252"
    try:
        decoder.decode(b"", final=True)
    except UnicodeDecodeError:
        return "cp1252"
    return "utf-8-sig"


def _iter_vrdb_chunks(path: Path, chunk_size: int) -> Iterable[pd.DataFrame]:
    encoding = _detect_vrdb_encoding(path)
    return pd.read_csv(
        path,
        sep="|",
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        chunksize=chunk_size,
        encoding=encoding,
        low_memory=False,
    )


def ensure_voter_registry_schema(conn, table_name: str) -> None:
    _psycopg, sql = _load_psycopg()
    create_table_statement = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {table_name} (
          voter_key TEXT PRIMARY KEY,
          state_voter_id TEXT,
          first_name TEXT NOT NULL,
          middle_name TEXT,
          last_name TEXT NOT NULL,
          name_suffix TEXT,
          birth_year TEXT,
          status_code TEXT,
          canonical_first TEXT NOT NULL,
          canonical_last TEXT NOT NULL,
          canonical_name TEXT NOT NULL,
          canonical_middle_initial TEXT NOT NULL DEFAULT '',
          canonical_suffix TEXT NOT NULL DEFAULT '',
          canonical_key_strict TEXT NOT NULL DEFAULT '',
          canonical_key_medium TEXT NOT NULL DEFAULT '',
          canonical_key_loose TEXT NOT NULL DEFAULT '',
          canonical_key_nickname TEXT NOT NULL DEFAULT '',
          source_file TEXT NOT NULL,
          source_hash TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    ).format(table_name=sql.Identifier(table_name))
    add_missing_columns = (
        sql.SQL(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS canonical_middle_initial TEXT "
            "NOT NULL DEFAULT ''"
        ),
        sql.SQL(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS canonical_suffix TEXT "
            "NOT NULL DEFAULT ''"
        ),
        sql.SQL(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS canonical_key_strict TEXT "
            "NOT NULL DEFAULT ''"
        ),
        sql.SQL(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS canonical_key_medium TEXT "
            "NOT NULL DEFAULT ''"
        ),
        sql.SQL(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS canonical_key_loose TEXT "
            "NOT NULL DEFAULT ''"
        ),
        sql.SQL(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS canonical_key_nickname TEXT "
            "NOT NULL DEFAULT ''"
        ),
    )
    add_missing_indexes = (
        sql.SQL("CREATE INDEX IF NOT EXISTS {idx_canonical} ON {table_name} (canonical_name)").format(
            idx_canonical=sql.Identifier(f"{table_name}_canonical_name_idx"),
            table_name=sql.Identifier(table_name),
        ),
        sql.SQL("CREATE INDEX IF NOT EXISTS {idx_status} ON {table_name} (status_code)").format(
            idx_status=sql.Identifier(f"{table_name}_status_code_idx"),
            table_name=sql.Identifier(table_name),
        ),
        sql.SQL(
            "CREATE INDEX IF NOT EXISTS {idx_key_strict} ON {table_name} (canonical_key_strict)"
        ).format(
            idx_key_strict=sql.Identifier(f"{table_name}_canonical_key_strict_idx"),
            table_name=sql.Identifier(table_name),
        ),
        sql.SQL(
            "CREATE INDEX IF NOT EXISTS {idx_key_medium} ON {table_name} (canonical_key_medium)"
        ).format(
            idx_key_medium=sql.Identifier(f"{table_name}_canonical_key_medium_idx"),
            table_name=sql.Identifier(table_name),
        ),
        sql.SQL(
            "CREATE INDEX IF NOT EXISTS {idx_key_loose} ON {table_name} (canonical_key_loose)"
        ).format(
            idx_key_loose=sql.Identifier(f"{table_name}_canonical_key_loose_idx"),
            table_name=sql.Identifier(table_name),
        ),
        sql.SQL(
            "CREATE INDEX IF NOT EXISTS {idx_key_nickname} ON {table_name} (canonical_key_nickname)"
        ).format(
            idx_key_nickname=sql.Identifier(f"{table_name}_canonical_key_nickname_idx"),
            table_name=sql.Identifier(table_name),
        ),
    )
    backfill_new_keys = sql.SQL(
        """
        UPDATE {table_name}
        SET
          canonical_middle_initial = COALESCE(
            NULLIF(TRIM(canonical_middle_initial), ''),
            LEFT(COALESCE(LOWER(REGEXP_REPLACE(middle_name, '[^a-z0-9]+', '', 'g')), ''), 1)
          ),
          canonical_suffix = COALESCE(
            NULLIF(TRIM(canonical_suffix), ''),
            COALESCE(LOWER(REGEXP_REPLACE(name_suffix, '[^a-z0-9]+', '', 'g')), '')
          ),
          canonical_key_medium = COALESCE(
            NULLIF(TRIM(canonical_key_medium), ''),
            COALESCE(canonical_name, '')
          ),
          canonical_key_loose = COALESCE(
            NULLIF(TRIM(canonical_key_loose), ''),
            COALESCE(canonical_last, '') || '|' || LEFT(COALESCE(canonical_first, ''), 1)
          ),
          canonical_key_nickname = COALESCE(
            NULLIF(TRIM(canonical_key_nickname), ''),
            COALESCE(canonical_name, '')
          ),
          canonical_key_strict = COALESCE(
            NULLIF(TRIM(canonical_key_strict), ''),
            COALESCE(canonical_last, '')
            || '|'
            || COALESCE(canonical_first, '')
            || '|'
            || COALESCE(
              NULLIF(TRIM(canonical_middle_initial), ''),
              LEFT(COALESCE(LOWER(REGEXP_REPLACE(middle_name, '[^a-z0-9]+', '', 'g')), ''), 1)
            )
            || '|'
            || COALESCE(
              NULLIF(TRIM(canonical_suffix), ''),
              COALESCE(LOWER(REGEXP_REPLACE(name_suffix, '[^a-z0-9]+', '', 'g')), '')
            )
          )
        WHERE
          canonical_key_medium = ''
          OR canonical_key_loose = ''
          OR canonical_key_nickname = ''
          OR canonical_key_strict = '';
        """
    ).format(table_name=sql.Identifier(table_name))
    with conn.cursor() as cursor:
        cursor.execute(create_table_statement)
        for alter_stmt in add_missing_columns:
            cursor.execute(alter_stmt.format(table_name=sql.Identifier(table_name)))
        for index_stmt in add_missing_indexes:
            cursor.execute(index_stmt)
        cursor.execute(backfill_new_keys)


def _upsert_vrdb_rows(conn, table_name: str, rows: pd.DataFrame) -> int:
    if rows.empty:
        return 0
    _psycopg, sql = _load_psycopg()
    query = sql.SQL(
        """
        INSERT INTO {table_name} (
          voter_key,
          state_voter_id,
          first_name,
          middle_name,
          last_name,
          name_suffix,
          birth_year,
          status_code,
          canonical_first,
          canonical_last,
          canonical_name,
          canonical_middle_initial,
          canonical_suffix,
          canonical_key_strict,
          canonical_key_medium,
          canonical_key_loose,
          canonical_key_nickname,
          source_file,
          source_hash
        )
        VALUES (
          %(voter_key)s,
          %(state_voter_id)s,
          %(first_name)s,
          %(middle_name)s,
          %(last_name)s,
          %(name_suffix)s,
          %(birth_year)s,
          %(status_code)s,
          %(canonical_first)s,
          %(canonical_last)s,
          %(canonical_name)s,
          %(canonical_middle_initial)s,
          %(canonical_suffix)s,
          %(canonical_key_strict)s,
          %(canonical_key_medium)s,
          %(canonical_key_loose)s,
          %(canonical_key_nickname)s,
          %(source_file)s,
          %(source_hash)s
        )
        ON CONFLICT (voter_key)
        DO UPDATE SET
          state_voter_id = EXCLUDED.state_voter_id,
          first_name = EXCLUDED.first_name,
          middle_name = EXCLUDED.middle_name,
          last_name = EXCLUDED.last_name,
          name_suffix = EXCLUDED.name_suffix,
          birth_year = EXCLUDED.birth_year,
          status_code = EXCLUDED.status_code,
          canonical_first = EXCLUDED.canonical_first,
          canonical_last = EXCLUDED.canonical_last,
          canonical_name = EXCLUDED.canonical_name,
          canonical_middle_initial = EXCLUDED.canonical_middle_initial,
          canonical_suffix = EXCLUDED.canonical_suffix,
          canonical_key_strict = EXCLUDED.canonical_key_strict,
          canonical_key_medium = EXCLUDED.canonical_key_medium,
          canonical_key_loose = EXCLUDED.canonical_key_loose,
          canonical_key_nickname = EXCLUDED.canonical_key_nickname,
          source_file = EXCLUDED.source_file,
          source_hash = EXCLUDED.source_hash,
          updated_at = NOW()
        """
    ).format(table_name=sql.Identifier(table_name))

    payload = rows.where(pd.notna(rows), None).to_dict(orient="records")
    with conn.cursor() as cursor:
        cursor.executemany(query, payload)
    return len(payload)


def import_vrdb_extract_to_postgres(
    extract_path: Path,
    db_url: str,
    table_name: str = "voter_registry",
    chunk_size: int = 50_000,
    force: bool = False,
) -> VRDBImportResult:
    if chunk_size < 1_000:
        raise ValueError("chunk_size must be >= 1000")

    psycopg, _sql = _load_psycopg()
    source_file = extract_path.name
    file_hash = compute_file_sha256(extract_path)
    file_size_bytes = int(extract_path.stat().st_size)

    rows_processed = 0
    rows_upserted = 0
    rows_with_state_voter_id = 0
    rows_with_canonical_name = 0

    with psycopg.connect(db_url) as conn:
        ensure_voter_registry_schema(conn=conn, table_name=table_name)
        ensure_import_tracking_schema(conn=conn)
        conn.commit()

        prior = find_completed_import(
            conn=conn,
            import_kind=IMPORT_KIND_VRDB,
            target_table=table_name,
            file_hash=file_hash,
            importer_version=VRDB_IMPORTER_VERSION,
        )
        if prior is not None and not force:
            skip_reason = (
                "checksum already imported "
                f"(import_id={prior.import_id}, rows_upserted={prior.rows_upserted})"
            )
            record_import_result(
                conn=conn,
                import_kind=IMPORT_KIND_VRDB,
                target_table=table_name,
                source_file=source_file,
                file_hash=file_hash,
                file_size_bytes=file_size_bytes,
                importer_version=VRDB_IMPORTER_VERSION,
                status="skipped",
                rows_processed=0,
                rows_upserted=0,
                message=skip_reason,
                metadata={"previous_import_id": prior.import_id},
            )
            conn.commit()
            return VRDBImportResult(
                source_file=source_file,
                table_name=table_name,
                rows_processed=0,
                rows_upserted=0,
                rows_with_state_voter_id=0,
                rows_with_canonical_name=0,
                chunk_size=chunk_size,
                file_hash=file_hash,
                import_skipped=True,
                skip_reason=skip_reason,
                previous_import_id=prior.import_id,
            )

        for chunk in _iter_vrdb_chunks(extract_path, chunk_size=chunk_size):
            rows_processed += len(chunk)
            normalized = normalize_vrdb_chunk(chunk=chunk, source_file=source_file)
            if normalized.empty:
                continue

            rows_with_state_voter_id += int((normalized["state_voter_id"] != "").sum())
            rows_with_canonical_name += int((normalized["canonical_name"] != "|").sum())
            rows_upserted += _upsert_vrdb_rows(conn=conn, table_name=table_name, rows=normalized)
            conn.commit()

        record_import_result(
            conn=conn,
            import_kind=IMPORT_KIND_VRDB,
            target_table=table_name,
            source_file=source_file,
            file_hash=file_hash,
            file_size_bytes=file_size_bytes,
            importer_version=VRDB_IMPORTER_VERSION,
            status="completed",
            rows_processed=rows_processed,
            rows_upserted=rows_upserted,
            metadata={"force": bool(force)},
        )
        conn.commit()

    return VRDBImportResult(
        source_file=source_file,
        table_name=table_name,
        rows_processed=rows_processed,
        rows_upserted=rows_upserted,
        rows_with_state_voter_id=rows_with_state_voter_id,
        rows_with_canonical_name=rows_with_canonical_name,
        chunk_size=chunk_size,
        file_hash=file_hash,
    )


def _chunk_values(values: list[str], chunk_size: int = 10_000) -> Iterable[list[str]]:
    for idx in range(0, len(values), chunk_size):
        yield values[idx : idx + chunk_size]


def _validated_name_key_column(column: str) -> str:
    normalized = str(column or "").strip()
    if normalized not in ALLOWED_NAME_KEY_COLUMNS:
        raise ValueError(
            f"Unsupported name key column: {normalized!r}. "
            f"Expected one of: {', '.join(sorted(ALLOWED_NAME_KEY_COLUMNS))}"
        )
    return normalized


def fetch_matching_voter_keys(
    db_url: str,
    table_name: str,
    key_values: list[str],
    *,
    key_column: str = "canonical_name",
    active_only: bool = True,
) -> pd.DataFrame:
    resolved_column = _validated_name_key_column(key_column)
    if not key_values:
        return pd.DataFrame(columns=[resolved_column, "n_registry_rows"])

    psycopg, sql = _load_psycopg()
    rows: list[tuple[str, int]] = []
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cursor:
            for chunk in _chunk_values(key_values, chunk_size=10_000):
                where_clause = sql.SQL("{} = ANY(%s)").format(sql.Identifier(resolved_column))
                if active_only:
                    where_clause = sql.SQL("{} AND LOWER(status_code) = 'active'").format(
                        where_clause
                    )
                query = sql.SQL(
                    "SELECT {key_column}, COUNT(*)::INT AS n_registry_rows "
                    "FROM {table_name} WHERE {where_clause} GROUP BY {key_column}"
                ).format(
                    key_column=sql.Identifier(resolved_column),
                    table_name=sql.Identifier(table_name),
                    where_clause=where_clause,
                )
                cursor.execute(query, (chunk,))
                rows.extend(cursor.fetchall())
    if not rows:
        return pd.DataFrame(columns=[resolved_column, "n_registry_rows"])
    return pd.DataFrame(rows, columns=[resolved_column, "n_registry_rows"])


def fetch_matching_voter_names(
    db_url: str,
    table_name: str,
    canonical_names: list[str],
    active_only: bool = True,
) -> pd.DataFrame:
    return fetch_matching_voter_keys(
        db_url=db_url,
        table_name=table_name,
        key_values=canonical_names,
        key_column="canonical_name",
        active_only=active_only,
    )


def fetch_voter_candidates_by_last_name(
    db_url: str,
    table_name: str,
    canonical_lasts: list[str],
    active_only: bool = True,
) -> pd.DataFrame:
    if not canonical_lasts:
        return pd.DataFrame(
            columns=[
                "canonical_last",
                "canonical_first",
                "canonical_name",
                "canonical_middle_initial",
                "canonical_suffix",
                "canonical_key_strict",
                "canonical_key_medium",
                "n_registry_rows",
            ]
        )

    psycopg, sql = _load_psycopg()
    rows: list[tuple[str, str, str, str, str, str, str, int]] = []
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cursor:
            for chunk in _chunk_values(canonical_lasts, chunk_size=10_000):
                where_clause = sql.SQL("canonical_last = ANY(%s)")
                if active_only:
                    where_clause = sql.SQL("{} AND LOWER(status_code) = 'active'").format(
                        where_clause
                    )
                query = sql.SQL(
                    "SELECT canonical_last, canonical_first, canonical_name, "
                    "canonical_middle_initial, canonical_suffix, canonical_key_strict, "
                    "canonical_key_medium, "
                    "COUNT(*)::INT AS n_registry_rows "
                    "FROM {table_name} WHERE {where_clause} "
                    "GROUP BY canonical_last, canonical_first, canonical_name, "
                    "canonical_middle_initial, canonical_suffix, canonical_key_strict, "
                    "canonical_key_medium"
                ).format(
                    table_name=sql.Identifier(table_name),
                    where_clause=where_clause,
                )
                cursor.execute(query, (chunk,))
                rows.extend(cursor.fetchall())

    if not rows:
        return pd.DataFrame(
            columns=[
                "canonical_last",
                "canonical_first",
                "canonical_name",
                "canonical_middle_initial",
                "canonical_suffix",
                "canonical_key_strict",
                "canonical_key_medium",
                "n_registry_rows",
            ]
        )
    return pd.DataFrame(
        rows,
        columns=[
            "canonical_last",
            "canonical_first",
            "canonical_name",
            "canonical_middle_initial",
            "canonical_suffix",
            "canonical_key_strict",
            "canonical_key_medium",
            "n_registry_rows",
        ],
    )


def fetch_voter_name_key_frequencies(
    db_url: str,
    table_name: str,
    *,
    key_column: str = "canonical_key_medium",
    active_only: bool = True,
) -> pd.DataFrame:
    resolved_column = _validated_name_key_column(key_column)
    psycopg, sql = _load_psycopg()
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cursor:
            where_clause = sql.SQL("TRUE")
            if active_only:
                where_clause = sql.SQL("LOWER(status_code) = 'active'")
            query = sql.SQL(
                "SELECT {key_column}, COUNT(*)::INT AS n_registry_rows "
                "FROM {table_name} WHERE {where_clause} GROUP BY {key_column}"
            ).format(
                key_column=sql.Identifier(resolved_column),
                table_name=sql.Identifier(table_name),
                where_clause=where_clause,
            )
            cursor.execute(query)
            rows = cursor.fetchall()
    if not rows:
        return pd.DataFrame(columns=[resolved_column, "n_registry_rows"])
    return pd.DataFrame(rows, columns=[resolved_column, "n_registry_rows"])


def count_registry_rows(db_url: str, table_name: str, active_only: bool = True) -> int:
    psycopg, sql = _load_psycopg()
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cursor:
            where_sql = (
                sql.SQL(" WHERE LOWER(status_code) = 'active'") if active_only else sql.SQL("")
            )
            query = sql.SQL("SELECT COUNT(*)::BIGINT FROM {table_name}{where_sql}").format(
                table_name=sql.Identifier(table_name),
                where_sql=where_sql,
            )
            cursor.execute(query)
            value = cursor.fetchone()
    return int(value[0]) if value and value[0] is not None else 0
