from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

DEFAULT_IMPORT_TRACKING_TABLE = "data_imports"
TERMINAL_IMPORT_STATUSES = {"completed", "failed", "skipped"}


@dataclass(frozen=True)
class ImportTrackingRecord:
    import_id: int
    import_kind: str
    target_table: str
    source_file: str
    file_hash: str
    importer_version: str
    status: str
    rows_processed: int
    rows_upserted: int


def _load_psycopg():
    try:
        from psycopg import sql
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "psycopg is required for import tracking. "
            "Install with: pip install 'psycopg[binary]'"
        ) from exc
    return sql


def compute_file_sha256(path: Path, block_size: int = 1 << 20) -> str:
    hasher = sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(block_size)
            if not block:
                break
            hasher.update(block)
    return hasher.hexdigest()


def ensure_import_tracking_schema(
    conn,
    table_name: str = DEFAULT_IMPORT_TRACKING_TABLE,
) -> None:
    sql = _load_psycopg()
    statement = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {table_name} (
          import_id BIGSERIAL PRIMARY KEY,
          import_kind TEXT NOT NULL,
          target_table TEXT NOT NULL,
          source_file TEXT NOT NULL,
          file_hash TEXT NOT NULL,
          file_size_bytes BIGINT NOT NULL,
          importer_version TEXT NOT NULL,
          status TEXT NOT NULL,
          rows_processed BIGINT NOT NULL DEFAULT 0,
          rows_upserted BIGINT NOT NULL DEFAULT 0,
          message TEXT,
          metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS {idx_lookup}
          ON {table_name} (import_kind, target_table, file_hash, importer_version, status);
        CREATE INDEX IF NOT EXISTS {idx_created_at}
          ON {table_name} (created_at DESC);
        """
    ).format(
        table_name=sql.Identifier(table_name),
        idx_lookup=sql.Identifier(f"{table_name}_lookup_idx"),
        idx_created_at=sql.Identifier(f"{table_name}_created_at_idx"),
    )
    with conn.cursor() as cursor:
        cursor.execute(statement)


def find_completed_import(
    conn,
    import_kind: str,
    target_table: str,
    file_hash: str,
    importer_version: str,
    table_name: str = DEFAULT_IMPORT_TRACKING_TABLE,
) -> ImportTrackingRecord | None:
    sql = _load_psycopg()
    query = sql.SQL(
        """
        SELECT
          import_id,
          import_kind,
          target_table,
          source_file,
          file_hash,
          importer_version,
          status,
          rows_processed,
          rows_upserted
        FROM {table_name}
        WHERE
          import_kind = %s
          AND target_table = %s
          AND file_hash = %s
          AND importer_version = %s
          AND status = 'completed'
        ORDER BY created_at DESC, import_id DESC
        LIMIT 1
        """
    ).format(table_name=sql.Identifier(table_name))

    with conn.cursor() as cursor:
        cursor.execute(query, (import_kind, target_table, file_hash, importer_version))
        row = cursor.fetchone()

    if not row:
        return None

    return ImportTrackingRecord(
        import_id=int(row[0]),
        import_kind=str(row[1]),
        target_table=str(row[2]),
        source_file=str(row[3]),
        file_hash=str(row[4]),
        importer_version=str(row[5]),
        status=str(row[6]),
        rows_processed=int(row[7]),
        rows_upserted=int(row[8]),
    )


def record_import_result(
    conn,
    import_kind: str,
    target_table: str,
    source_file: str,
    file_hash: str,
    file_size_bytes: int,
    importer_version: str,
    status: str,
    rows_processed: int,
    rows_upserted: int,
    message: str | None = None,
    metadata: dict[str, Any] | None = None,
    table_name: str = DEFAULT_IMPORT_TRACKING_TABLE,
) -> int:
    if status not in TERMINAL_IMPORT_STATUSES:
        raise ValueError(f"Unsupported import status: {status}")

    sql = _load_psycopg()
    query = sql.SQL(
        """
        INSERT INTO {table_name} (
          import_kind,
          target_table,
          source_file,
          file_hash,
          file_size_bytes,
          importer_version,
          status,
          rows_processed,
          rows_upserted,
          message,
          metadata
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        RETURNING import_id
        """
    ).format(table_name=sql.Identifier(table_name))

    metadata_payload = json.dumps(metadata or {}, sort_keys=True)
    with conn.cursor() as cursor:
        cursor.execute(
            query,
            (
                import_kind,
                target_table,
                source_file,
                file_hash,
                int(file_size_bytes),
                importer_version,
                status,
                int(rows_processed),
                int(rows_upserted),
                message,
                metadata_payload,
            ),
        )
        record = cursor.fetchone()

    return int(record[0]) if record and record[0] is not None else 0
