from __future__ import annotations

import codecs
import re
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from typing import Iterable

import pandas as pd

from testifier_audit.config import ColumnsConfig
from testifier_audit.io.schema import normalize_columns

POSITION_MAP = {
    "PRO": "Pro",
    "CON": "Con",
}
WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class SubmissionImportResult:
    source_file: str
    table_name: str
    rows_processed: int
    rows_upserted: int
    rows_blank_organization: int
    rows_invalid_timestamp: int
    chunk_size: int


def _load_psycopg():
    try:
        import psycopg
        from psycopg import sql
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "psycopg is required for PostgreSQL operations. "
            "Install with: pip install 'psycopg[binary]'"
        ) from exc
    return psycopg, sql


def _normalize_text(value: str) -> str:
    return WHITESPACE_RE.sub(" ", str(value).strip())


def _normalize_upper_text(value: str) -> str:
    return _normalize_text(value).upper()


def _split_name(value: str) -> tuple[str, str]:
    if "," in value:
        last, first = value.split(",", 1)
        return _normalize_upper_text(last), _normalize_upper_text(first)

    parts = [part for part in value.split(" ") if part]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return _normalize_upper_text(parts[0]), ""
    return _normalize_upper_text(parts[-1]), _normalize_upper_text(" ".join(parts[:-1]))


def _normalize_position(value: str) -> str:
    return POSITION_MAP.get(_normalize_upper_text(value), "Unknown")


def _parse_signed_at(values: pd.Series, timezone: str) -> pd.Series:
    parsed = pd.to_datetime(
        values,
        format="%m/%d/%Y %I:%M %p",
        errors="coerce",
    )
    missing_mask = parsed.isna()
    if missing_mask.any():
        parsed.loc[missing_mask] = pd.to_datetime(values.loc[missing_mask], errors="coerce")

    if parsed.dt.tz is None:
        parsed = parsed.dt.tz_localize(
            timezone,
            nonexistent="shift_forward",
            ambiguous="NaT",
        )
    else:
        parsed = parsed.dt.tz_convert(timezone)
    return parsed


def _detect_csv_encoding(path: Path, probe_bytes: int = 1 << 20) -> str:
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
    return "utf-8"


def _iter_submission_chunks(path: Path, chunk_size: int) -> Iterable[pd.DataFrame]:
    encoding = _detect_csv_encoding(path)
    return pd.read_csv(
        path,
        encoding=encoding,
        chunksize=chunk_size,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        low_memory=False,
    )


def normalize_submission_chunk(
    chunk: pd.DataFrame,
    source_file: str,
    columns: ColumnsConfig,
    timezone: str,
    row_number_offset: int,
) -> pd.DataFrame:
    if chunk.empty:
        return pd.DataFrame(
            columns=[
                "submission_key",
                "source_file",
                "source_row_number",
                "source_hash",
                "source_id",
                "name_raw",
                "name_clean",
                "name_last",
                "name_first",
                "organization_raw",
                "organization_clean",
                "organization_is_blank",
                "position_raw",
                "position_normalized",
                "time_signed_in_raw",
                "signed_at",
                "minute_bucket",
            ]
        )

    normalized = normalize_columns(df=chunk, columns=columns)
    out = pd.DataFrame(index=normalized.index)
    out["source_file"] = source_file
    out["source_row_number"] = pd.RangeIndex(
        start=row_number_offset + 1,
        stop=row_number_offset + 1 + len(normalized),
        step=1,
    ).astype("int64")

    out["source_id"] = normalized["id"].fillna("").astype(str).map(_normalize_text)
    out["name_raw"] = normalized["name"].fillna("").astype(str).map(_normalize_text)
    out["name_clean"] = out["name_raw"].map(_normalize_upper_text)

    split_values = out["name_raw"].map(_split_name)
    out["name_last"] = split_values.str[0]
    out["name_first"] = split_values.str[1]

    out["organization_raw"] = normalized["organization"].fillna("").astype(str).map(_normalize_text)
    out["organization_clean"] = out["organization_raw"].map(_normalize_upper_text)
    out["organization_is_blank"] = out["organization_clean"] == ""

    out["position_raw"] = normalized["position"].fillna("").astype(str).map(_normalize_text)
    out["position_normalized"] = out["position_raw"].map(_normalize_position)

    out["time_signed_in_raw"] = (
        normalized["time_signed_in"].fillna("").astype(str).map(_normalize_text)
    )
    out["signed_at"] = _parse_signed_at(out["time_signed_in_raw"], timezone=timezone)
    out["minute_bucket"] = out["signed_at"].dt.floor("min")

    hash_input = (
        out["source_file"].astype(str)
        + "|"
        + out["source_row_number"].astype(str)
        + "|"
        + out["source_id"].astype(str)
        + "|"
        + out["name_raw"].astype(str)
        + "|"
        + out["organization_raw"].astype(str)
        + "|"
        + out["position_raw"].astype(str)
        + "|"
        + out["time_signed_in_raw"].astype(str)
    )
    out["source_hash"] = hash_input.map(lambda value: sha1(value.encode("utf-8")).hexdigest())
    out["submission_key"] = out["source_file"] + ":" + out["source_row_number"].astype(str)
    return out


def ensure_submission_schema(conn, table_name: str) -> None:
    _psycopg, sql = _load_psycopg()
    statement = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {table_name} (
          submission_key TEXT PRIMARY KEY,
          source_file TEXT NOT NULL,
          source_row_number BIGINT NOT NULL,
          source_hash TEXT NOT NULL,
          source_id TEXT,
          name_raw TEXT NOT NULL,
          name_clean TEXT NOT NULL,
          name_last TEXT,
          name_first TEXT,
          organization_raw TEXT,
          organization_clean TEXT,
          organization_is_blank BOOLEAN NOT NULL,
          position_raw TEXT,
          position_normalized TEXT NOT NULL,
          time_signed_in_raw TEXT,
          signed_at TIMESTAMPTZ,
          minute_bucket TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS {idx_source_file} ON {table_name} (source_file);
        CREATE INDEX IF NOT EXISTS {idx_source_row_number} ON {table_name} (source_row_number);
        CREATE INDEX IF NOT EXISTS {idx_minute_bucket} ON {table_name} (minute_bucket);
        CREATE INDEX IF NOT EXISTS {idx_position_normalized} ON {table_name} (position_normalized);
        CREATE INDEX IF NOT EXISTS {idx_org_blank} ON {table_name} (organization_is_blank);
        """
    ).format(
        table_name=sql.Identifier(table_name),
        idx_source_file=sql.Identifier(f"{table_name}_source_file_idx"),
        idx_source_row_number=sql.Identifier(f"{table_name}_source_row_number_idx"),
        idx_minute_bucket=sql.Identifier(f"{table_name}_minute_bucket_idx"),
        idx_position_normalized=sql.Identifier(f"{table_name}_position_normalized_idx"),
        idx_org_blank=sql.Identifier(f"{table_name}_organization_is_blank_idx"),
    )
    with conn.cursor() as cursor:
        cursor.execute(statement)


def _upsert_submission_rows(conn, table_name: str, rows: pd.DataFrame) -> int:
    if rows.empty:
        return 0

    _psycopg, sql = _load_psycopg()
    query = sql.SQL(
        """
        INSERT INTO {table_name} (
          submission_key,
          source_file,
          source_row_number,
          source_hash,
          source_id,
          name_raw,
          name_clean,
          name_last,
          name_first,
          organization_raw,
          organization_clean,
          organization_is_blank,
          position_raw,
          position_normalized,
          time_signed_in_raw,
          signed_at,
          minute_bucket
        )
        VALUES (
          %(submission_key)s,
          %(source_file)s,
          %(source_row_number)s,
          %(source_hash)s,
          %(source_id)s,
          %(name_raw)s,
          %(name_clean)s,
          %(name_last)s,
          %(name_first)s,
          %(organization_raw)s,
          %(organization_clean)s,
          %(organization_is_blank)s,
          %(position_raw)s,
          %(position_normalized)s,
          %(time_signed_in_raw)s,
          %(signed_at)s,
          %(minute_bucket)s
        )
        ON CONFLICT (submission_key)
        DO UPDATE SET
          source_hash = EXCLUDED.source_hash,
          source_id = EXCLUDED.source_id,
          name_raw = EXCLUDED.name_raw,
          name_clean = EXCLUDED.name_clean,
          name_last = EXCLUDED.name_last,
          name_first = EXCLUDED.name_first,
          organization_raw = EXCLUDED.organization_raw,
          organization_clean = EXCLUDED.organization_clean,
          organization_is_blank = EXCLUDED.organization_is_blank,
          position_raw = EXCLUDED.position_raw,
          position_normalized = EXCLUDED.position_normalized,
          time_signed_in_raw = EXCLUDED.time_signed_in_raw,
          signed_at = EXCLUDED.signed_at,
          minute_bucket = EXCLUDED.minute_bucket,
          updated_at = NOW()
        """
    ).format(table_name=sql.Identifier(table_name))

    payload = rows.where(pd.notna(rows), None).to_dict(orient="records")
    with conn.cursor() as cursor:
        cursor.executemany(query, payload)
    return len(payload)


def import_submission_csv_to_postgres(
    csv_path: Path,
    db_url: str,
    columns: ColumnsConfig,
    timezone: str,
    table_name: str = "public_submissions",
    chunk_size: int = 50_000,
    source_file: str | None = None,
) -> SubmissionImportResult:
    if chunk_size < 1_000:
        raise ValueError("chunk_size must be >= 1000")

    psycopg, _sql = _load_psycopg()
    source_file_value = source_file or csv_path.name
    rows_processed = 0
    rows_upserted = 0
    rows_blank_organization = 0
    rows_invalid_timestamp = 0

    with psycopg.connect(db_url) as conn:
        ensure_submission_schema(conn=conn, table_name=table_name)
        conn.commit()

        for chunk in _iter_submission_chunks(csv_path, chunk_size=chunk_size):
            normalized = normalize_submission_chunk(
                chunk=chunk,
                source_file=source_file_value,
                columns=columns,
                timezone=timezone,
                row_number_offset=rows_processed,
            )
            rows_processed += len(chunk)
            if normalized.empty:
                continue

            rows_blank_organization += int(normalized["organization_is_blank"].sum())
            rows_invalid_timestamp += int(normalized["signed_at"].isna().sum())
            rows_upserted += _upsert_submission_rows(
                conn=conn, table_name=table_name, rows=normalized
            )
            conn.commit()

    return SubmissionImportResult(
        source_file=source_file_value,
        table_name=table_name,
        rows_processed=rows_processed,
        rows_upserted=rows_upserted,
        rows_blank_organization=rows_blank_organization,
        rows_invalid_timestamp=rows_invalid_timestamp,
        chunk_size=chunk_size,
    )


def load_submission_records_from_postgres(
    db_url: str,
    table_name: str = "public_submissions",
    source_file: str | None = None,
) -> pd.DataFrame:
    psycopg, sql = _load_psycopg()
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cursor:
            where_sql = sql.SQL("")
            params: list[object] = []
            if source_file:
                where_sql = sql.SQL(" WHERE source_file = %s")
                params.append(source_file)

            query = sql.SQL(
                """
                SELECT
                  COALESCE(NULLIF(source_id, ''), source_row_number::TEXT) AS id,
                  name_raw AS name,
                  organization_raw AS organization,
                  position_raw AS position,
                  time_signed_in_raw AS time_signed_in
                FROM {table_name}
                {where_sql}
                ORDER BY source_file, source_row_number
                """
            ).format(
                table_name=sql.Identifier(table_name),
                where_sql=where_sql,
            )
            cursor.execute(query, params)
            rows = cursor.fetchall()

    if not rows:
        return pd.DataFrame(columns=["id", "name", "organization", "position", "time_signed_in"])
    return pd.DataFrame(
        rows,
        columns=["id", "name", "organization", "position", "time_signed_in"],
    )
