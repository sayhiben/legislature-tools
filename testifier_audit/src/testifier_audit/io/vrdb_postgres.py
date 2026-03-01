from __future__ import annotations

import codecs
import logging
import re
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from time import perf_counter
from typing import Iterable

import pandas as pd

from testifier_audit.io.import_tracking import (
    compute_file_sha256,
    ensure_import_tracking_schema,
    find_completed_import,
    record_import_result,
)
from testifier_audit.names.nickname_map import load_nickname_map
from testifier_audit.names.normalization import (
    compose_person_name,
    normalization_version,
    normalization_version_hash,
    normalize_name_record,
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
REG_CITY_COLUMN_CANDIDATES = ("RegCity", "reg_city", "RegistrationCity", "City")
COUNTY_CODE_COLUMN_CANDIDATES = ("CountyCode", "county_code", "County")
IMPORT_KIND_VRDB = "vrdb_extract"
VRDB_IMPORTER_VERSION = "vrdb_extract_v4"
ALLOWED_NAME_KEY_COLUMNS = frozenset(
    {
        "full_name_key",
        "first_name_key",
        "last_name_key",
        "canonical_name",
        "canonical_key_strict",
        "canonical_key_medium",
        "canonical_key_loose",
        "canonical_key_nickname",
        "collision_key_strict",
        "collision_key_medium",
        "collision_key_loose",
    }
)
ALLOWED_STRATIFICATION_MODES = frozenset({"none", "birth_decade"})
ACTIVE_STATUS_VALUE = "Active"
INACTIVE_STATUS_VALUE = "Inactive"
LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class VRDBImportResult:
    source_file: str
    table_name: str
    rows_processed: int
    rows_upserted: int
    rows_with_state_voter_id: int
    rows_with_canonical_name: int
    normalization_version: str
    normalization_version_hash: str
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


def _validated_stratification_mode(stratification: str) -> str:
    value = str(stratification or "none").strip().lower()
    if value not in ALLOWED_STRATIFICATION_MODES:
        allowed = ", ".join(sorted(ALLOWED_STRATIFICATION_MODES))
        raise ValueError(f"Unsupported stratification mode: {stratification!r}. Allowed: {allowed}.")
    return value


def _stratification_sql_expression(stratification: str, sql):
    mode = _validated_stratification_mode(stratification)
    if mode == "none":
        return sql.SQL("'all'")
    if mode == "birth_decade":
        return sql.SQL(
            "CASE "
            "WHEN birth_year ~ '^[0-9]{4}$' THEN SUBSTRING(birth_year, 1, 3) || '0s' "
            "ELSE 'unknown' "
            "END"
        )
    # Keep this explicit for readability if new modes are added later.
    raise ValueError(f"Unsupported stratification mode: {stratification!r}")


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


def _normalize_status_code_series(series: pd.Series) -> pd.Series:
    normalized = series.fillna("").astype(str).str.strip()
    lowered = normalized.str.lower()
    normalized = normalized.mask(lowered == "active", ACTIVE_STATUS_VALUE)
    normalized = normalized.mask(lowered == "inactive", INACTIVE_STATUS_VALUE)
    return normalized


def _normalize_geo_city_series(series: pd.Series) -> pd.Series:
    city = series.fillna("").astype(str).str.strip().str.upper()
    city = city.str.replace(r"[^A-Z0-9]+", " ", regex=True)
    city = city.str.replace(r"\s+", " ", regex=True).str.strip()
    city = city.mask(city.isin({"UNKNOWN", "UNK", "NA", "N A", "NONE", "NULL"}), "")
    return city


def _normalize_geo_county_code_series(series: pd.Series) -> pd.Series:
    county = series.fillna("").astype(str).str.upper()
    county = county.map(lambda value: re.sub(r"[^A-Z0-9]+", "", value))
    county = county.mask(county.isin({"UNKNOWN", "UNK", "NA", "NONE", "NULL"}), "")
    return county


def normalize_vrdb_chunk(
    chunk: pd.DataFrame,
    source_file: str,
    *,
    nickname_map: dict[str, str] | None = None,
    normalize_unicode: bool = True,
    strip_punctuation: bool = True,
    normalization_version_value: str | None = None,
    normalization_version_hash_value: str | None = None,
) -> pd.DataFrame:
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
                "reg_city",
                "county_code",
                "canonical_first",
                "canonical_last",
                "canonical_name",
                "canonical_middle_initial",
                "canonical_suffix",
                "canonical_key_strict",
                "canonical_key_medium",
                "canonical_key_loose",
                "canonical_key_nickname",
                "collision_key_strict",
                "collision_key_medium",
                "collision_key_loose",
                "full_name_key",
                "first_name_key",
                "last_name_key",
                "name_normalized",
                "normalization_version",
                "normalization_version_hash",
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
    reg_city_col = _resolve_column(columns, REG_CITY_COLUMN_CANDIDATES)
    county_code_col = _resolve_column(columns, COUNTY_CODE_COLUMN_CANDIDATES)

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
    out["status_code"] = _normalize_status_code_series(chunk[status_col]) if status_col else ""
    out["reg_city"] = (
        _normalize_geo_city_series(chunk[reg_city_col]) if reg_city_col is not None else ""
    )
    out["county_code"] = (
        _normalize_geo_county_code_series(chunk[county_code_col])
        if county_code_col is not None
        else ""
    )
    nickname_map_value = nickname_map or {}
    resolved_version_hash = str(normalization_version_hash_value or "").strip() or normalization_version_hash(
        normalize_unicode=normalize_unicode,
        strip_punctuation=strip_punctuation,
        nickname_map=nickname_map_value,
    )
    resolved_version = str(normalization_version_value or "").strip() or normalization_version(
        normalize_unicode=normalize_unicode,
        strip_punctuation=strip_punctuation,
        nickname_map=nickname_map_value,
    )
    canonicalized = pd.Series(
        (
            normalize_name_record(
                compose_person_name(
                    first_name=first_name,
                    middle_name=middle_name,
                    last_name=last_name,
                    suffix=name_suffix,
                ),
                nickname_map=nickname_map_value,
                normalize_unicode=normalize_unicode,
                strip_punctuation=strip_punctuation,
                normalization_version_value=resolved_version,
                normalization_version_hash_value=resolved_version_hash,
            )
            for first_name, middle_name, last_name, name_suffix in zip(
                out["first_name"].tolist(),
                out["middle_name"].tolist(),
                out["last_name"].tolist(),
                out["name_suffix"].tolist(),
                strict=False,
            )
        ),
        index=out.index,
        dtype=object,
    )
    canonicalized_name = canonicalized.map(lambda item: item.canonicalized)
    out["canonical_first"] = canonicalized_name.map(lambda item: item.first_primary)
    out["canonical_last"] = canonicalized_name.map(lambda item: item.last)
    out["canonical_name"] = canonicalized_name.map(lambda item: item.canonical_key_medium)
    out["canonical_middle_initial"] = canonicalized_name.map(lambda item: item.middle_initial)
    out["canonical_suffix"] = canonicalized_name.map(lambda item: item.suffix_normalized)
    out["canonical_key_strict"] = canonicalized_name.map(lambda item: item.canonical_key_strict)
    out["canonical_key_medium"] = canonicalized_name.map(lambda item: item.canonical_key_medium)
    out["canonical_key_loose"] = canonicalized_name.map(lambda item: item.canonical_key_loose)
    out["canonical_key_nickname"] = canonicalized_name.map(lambda item: item.canonical_key_nickname)
    out["collision_key_strict"] = canonicalized_name.map(lambda item: item.collision_key_strict)
    out["collision_key_medium"] = canonicalized_name.map(lambda item: item.collision_key_medium)
    out["collision_key_loose"] = canonicalized_name.map(lambda item: item.collision_key_loose)
    out["full_name_key"] = canonicalized.map(lambda item: item.full_name_key)
    out["first_name_key"] = canonicalized.map(lambda item: item.first_name_key)
    out["last_name_key"] = canonicalized.map(lambda item: item.last_name_key)
    out["name_normalized"] = canonicalized_name.map(lambda item: item.name_normalized)
    out["normalization_version"] = resolved_version
    out["normalization_version_hash"] = resolved_version_hash
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

    has_name = (
        out["last_name"].fillna("").astype(str).str.strip() != ""
    ) & (out["first_name"].fillna("").astype(str).str.strip() != "")
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
    started = perf_counter()
    _psycopg, sql = _load_psycopg()
    LOGGER.info("[vrdb-import] Ensuring voter_registry schema for table=%s", table_name)
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
          reg_city TEXT NOT NULL DEFAULT '',
          county_code TEXT NOT NULL DEFAULT '',
          canonical_first TEXT NOT NULL,
          canonical_last TEXT NOT NULL,
          canonical_name TEXT NOT NULL,
          canonical_middle_initial TEXT NOT NULL DEFAULT '',
          canonical_suffix TEXT NOT NULL DEFAULT '',
          canonical_key_strict TEXT NOT NULL DEFAULT '',
          canonical_key_medium TEXT NOT NULL DEFAULT '',
          canonical_key_loose TEXT NOT NULL DEFAULT '',
          canonical_key_nickname TEXT NOT NULL DEFAULT '',
          collision_key_strict TEXT NOT NULL DEFAULT '',
          collision_key_medium TEXT NOT NULL DEFAULT '',
          collision_key_loose TEXT NOT NULL DEFAULT '',
          full_name_key TEXT NOT NULL DEFAULT '',
          first_name_key TEXT NOT NULL DEFAULT '',
          last_name_key TEXT NOT NULL DEFAULT '',
          name_normalized TEXT NOT NULL DEFAULT '',
          normalization_version TEXT NOT NULL DEFAULT '',
          normalization_version_hash TEXT NOT NULL DEFAULT '',
          source_file TEXT NOT NULL,
          source_hash TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    ).format(table_name=sql.Identifier(table_name))
    add_missing_columns = (
        sql.SQL(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS reg_city TEXT "
            "NOT NULL DEFAULT ''"
        ),
        sql.SQL(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS county_code TEXT "
            "NOT NULL DEFAULT ''"
        ),
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
        sql.SQL(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS collision_key_strict TEXT "
            "NOT NULL DEFAULT ''"
        ),
        sql.SQL(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS collision_key_medium TEXT "
            "NOT NULL DEFAULT ''"
        ),
        sql.SQL(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS collision_key_loose TEXT "
            "NOT NULL DEFAULT ''"
        ),
        sql.SQL(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS full_name_key TEXT "
            "NOT NULL DEFAULT ''"
        ),
        sql.SQL(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS first_name_key TEXT "
            "NOT NULL DEFAULT ''"
        ),
        sql.SQL(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS last_name_key TEXT "
            "NOT NULL DEFAULT ''"
        ),
        sql.SQL(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS name_normalized TEXT "
            "NOT NULL DEFAULT ''"
        ),
        sql.SQL(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS normalization_version TEXT "
            "NOT NULL DEFAULT ''"
        ),
        sql.SQL(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS normalization_version_hash TEXT "
            "NOT NULL DEFAULT ''"
        ),
    )
    add_missing_indexes = (
        sql.SQL("CREATE INDEX IF NOT EXISTS {idx_canonical} ON {table_name} (canonical_name)").format(
            idx_canonical=sql.Identifier(f"{table_name}_canonical_name_idx"),
            table_name=sql.Identifier(table_name),
        ),
        sql.SQL(
            "CREATE INDEX IF NOT EXISTS {idx_canonical_last} ON {table_name} (canonical_last)"
        ).format(
            idx_canonical_last=sql.Identifier(f"{table_name}_canonical_last_idx"),
            table_name=sql.Identifier(table_name),
        ),
        sql.SQL("CREATE INDEX IF NOT EXISTS {idx_status} ON {table_name} (status_code)").format(
            idx_status=sql.Identifier(f"{table_name}_status_code_idx"),
            table_name=sql.Identifier(table_name),
        ),
        sql.SQL("CREATE INDEX IF NOT EXISTS {idx_county_code} ON {table_name} (county_code)").format(
            idx_county_code=sql.Identifier(f"{table_name}_county_code_idx"),
            table_name=sql.Identifier(table_name),
        ),
        sql.SQL("CREATE INDEX IF NOT EXISTS {idx_reg_city} ON {table_name} (reg_city)").format(
            idx_reg_city=sql.Identifier(f"{table_name}_reg_city_idx"),
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
        sql.SQL(
            "CREATE INDEX IF NOT EXISTS {idx_collision_strict} ON {table_name} (collision_key_strict)"
        ).format(
            idx_collision_strict=sql.Identifier(f"{table_name}_collision_key_strict_idx"),
            table_name=sql.Identifier(table_name),
        ),
        sql.SQL(
            "CREATE INDEX IF NOT EXISTS {idx_collision_medium} ON {table_name} (collision_key_medium)"
        ).format(
            idx_collision_medium=sql.Identifier(f"{table_name}_collision_key_medium_idx"),
            table_name=sql.Identifier(table_name),
        ),
        sql.SQL(
            "CREATE INDEX IF NOT EXISTS {idx_collision_loose} ON {table_name} (collision_key_loose)"
        ).format(
            idx_collision_loose=sql.Identifier(f"{table_name}_collision_key_loose_idx"),
            table_name=sql.Identifier(table_name),
        ),
        sql.SQL("CREATE INDEX IF NOT EXISTS {idx_full_name} ON {table_name} (full_name_key)").format(
            idx_full_name=sql.Identifier(f"{table_name}_full_name_key_idx"),
            table_name=sql.Identifier(table_name),
        ),
        sql.SQL(
            "CREATE INDEX IF NOT EXISTS {idx_first_name} ON {table_name} (first_name_key)"
        ).format(
            idx_first_name=sql.Identifier(f"{table_name}_first_name_key_idx"),
            table_name=sql.Identifier(table_name),
        ),
        sql.SQL("CREATE INDEX IF NOT EXISTS {idx_last_name} ON {table_name} (last_name_key)").format(
            idx_last_name=sql.Identifier(f"{table_name}_last_name_key_idx"),
            table_name=sql.Identifier(table_name),
        ),
    )
    with conn.cursor() as cursor:
        cursor.execute(create_table_statement)
        LOGGER.info("[vrdb-import] Schema step complete: table exists/created")
        for alter_stmt in add_missing_columns:
            cursor.execute(alter_stmt.format(table_name=sql.Identifier(table_name)))
        LOGGER.info(
            "[vrdb-import] Schema step complete: missing columns ensured count=%d",
            len(add_missing_columns),
        )
        for index_stmt in add_missing_indexes:
            cursor.execute(index_stmt)
        LOGGER.info(
            "[vrdb-import] Schema step complete: indexes ensured count=%d",
            len(add_missing_indexes),
        )
    LOGGER.info(
        "[vrdb-import] Schema ensure complete table=%s elapsed_ms=%.1f",
        table_name,
        (perf_counter() - started) * 1000.0,
    )


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
          reg_city,
          county_code,
          canonical_first,
          canonical_last,
          canonical_name,
          canonical_middle_initial,
          canonical_suffix,
          canonical_key_strict,
          canonical_key_medium,
          canonical_key_loose,
          canonical_key_nickname,
          collision_key_strict,
          collision_key_medium,
          collision_key_loose,
          full_name_key,
          first_name_key,
          last_name_key,
          name_normalized,
          normalization_version,
          normalization_version_hash,
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
          %(reg_city)s,
          %(county_code)s,
          %(canonical_first)s,
          %(canonical_last)s,
          %(canonical_name)s,
          %(canonical_middle_initial)s,
          %(canonical_suffix)s,
          %(canonical_key_strict)s,
          %(canonical_key_medium)s,
          %(canonical_key_loose)s,
          %(canonical_key_nickname)s,
          %(collision_key_strict)s,
          %(collision_key_medium)s,
          %(collision_key_loose)s,
          %(full_name_key)s,
          %(first_name_key)s,
          %(last_name_key)s,
          %(name_normalized)s,
          %(normalization_version)s,
          %(normalization_version_hash)s,
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
          reg_city = EXCLUDED.reg_city,
          county_code = EXCLUDED.county_code,
          canonical_first = EXCLUDED.canonical_first,
          canonical_last = EXCLUDED.canonical_last,
          canonical_name = EXCLUDED.canonical_name,
          canonical_middle_initial = EXCLUDED.canonical_middle_initial,
          canonical_suffix = EXCLUDED.canonical_suffix,
          canonical_key_strict = EXCLUDED.canonical_key_strict,
          canonical_key_medium = EXCLUDED.canonical_key_medium,
          canonical_key_loose = EXCLUDED.canonical_key_loose,
          canonical_key_nickname = EXCLUDED.canonical_key_nickname,
          collision_key_strict = EXCLUDED.collision_key_strict,
          collision_key_medium = EXCLUDED.collision_key_medium,
          collision_key_loose = EXCLUDED.collision_key_loose,
          full_name_key = EXCLUDED.full_name_key,
          first_name_key = EXCLUDED.first_name_key,
          last_name_key = EXCLUDED.last_name_key,
          name_normalized = EXCLUDED.name_normalized,
          normalization_version = EXCLUDED.normalization_version,
          normalization_version_hash = EXCLUDED.normalization_version_hash,
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
    nickname_map_path: str | None = None,
    normalize_unicode: bool = True,
    strip_punctuation: bool = True,
    force: bool = False,
) -> VRDBImportResult:
    if chunk_size < 1_000:
        raise ValueError("chunk_size must be >= 1000")
    import_started = perf_counter()

    psycopg, _sql = _load_psycopg()
    source_file = extract_path.name
    file_hash = compute_file_sha256(extract_path)
    file_size_bytes = int(extract_path.stat().st_size)
    LOGGER.info(
        "[vrdb-import] Starting import source_file=%s table=%s "
        "file_size_bytes=%d chunk_size=%d force=%s",
        source_file,
        table_name,
        file_size_bytes,
        chunk_size,
        bool(force),
    )

    rows_processed = 0
    rows_upserted = 0
    rows_with_state_voter_id = 0
    rows_with_canonical_name = 0
    nickname_map = load_nickname_map(str(nickname_map_path or "")) if nickname_map_path else {}
    resolved_normalization_version_hash = normalization_version_hash(
        normalize_unicode=normalize_unicode,
        strip_punctuation=strip_punctuation,
        nickname_map=nickname_map,
        nickname_map_path=nickname_map_path,
    )
    resolved_normalization_version = normalization_version(
        normalize_unicode=normalize_unicode,
        strip_punctuation=strip_punctuation,
        nickname_map=nickname_map,
        nickname_map_path=nickname_map_path,
    )
    LOGGER.info(
        "[vrdb-import] Normalization settings version=%s version_hash=%s nickname_map_entries=%d",
        resolved_normalization_version,
        resolved_normalization_version_hash,
        len(nickname_map),
    )

    with psycopg.connect(db_url) as conn:
        LOGGER.info("[vrdb-import] Connected to database; ensuring schema + import tracking tables")
        ensure_voter_registry_schema(conn=conn, table_name=table_name)
        ensure_import_tracking_schema(conn=conn)
        conn.commit()
        LOGGER.info("[vrdb-import] Schema/import-tracking ensure committed")

        LOGGER.info("[vrdb-import] Checking checksum/importer-version skip status")
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
            LOGGER.info("[vrdb-import] Import skipped: %s", skip_reason)
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
                normalization_version=resolved_normalization_version,
                normalization_version_hash=resolved_normalization_version_hash,
                chunk_size=chunk_size,
                file_hash=file_hash,
                import_skipped=True,
                skip_reason=skip_reason,
                previous_import_id=prior.import_id,
            )

        LOGGER.info("[vrdb-import] Beginning chunked upsert loop")
        chunk_index = 0
        for chunk in _iter_vrdb_chunks(extract_path, chunk_size=chunk_size):
            chunk_index += 1
            chunk_started = perf_counter()
            chunk_rows = int(len(chunk))
            rows_processed += chunk_rows
            normalized = normalize_vrdb_chunk(
                chunk=chunk,
                source_file=source_file,
                nickname_map=nickname_map,
                normalize_unicode=normalize_unicode,
                strip_punctuation=strip_punctuation,
                normalization_version_value=resolved_normalization_version,
                normalization_version_hash_value=resolved_normalization_version_hash,
            )
            normalized_rows = int(len(normalized))
            if normalized.empty:
                LOGGER.info(
                    "[vrdb-import] Chunk %d processed: chunk_rows=%d normalized_rows=0 "
                    "rows_processed=%d rows_upserted=%d elapsed_s=%.1f",
                    chunk_index,
                    chunk_rows,
                    rows_processed,
                    rows_upserted,
                    perf_counter() - import_started,
                )
                continue

            rows_with_state_voter_id += int((normalized["state_voter_id"] != "").sum())
            rows_with_canonical_name += int((normalized["canonical_name"] != "|").sum())
            chunk_upserted = _upsert_vrdb_rows(conn=conn, table_name=table_name, rows=normalized)
            rows_upserted += chunk_upserted
            conn.commit()
            LOGGER.info(
                "[vrdb-import] Chunk %d processed: chunk_rows=%d normalized_rows=%d "
                "upserted_rows=%d rows_processed=%d rows_upserted=%d elapsed_s=%.1f "
                "chunk_elapsed_ms=%.1f",
                chunk_index,
                chunk_rows,
                normalized_rows,
                int(chunk_upserted),
                rows_processed,
                rows_upserted,
                perf_counter() - import_started,
                (perf_counter() - chunk_started) * 1000.0,
            )

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
        LOGGER.info(
            "[vrdb-import] Import completed rows_processed=%d rows_upserted=%d "
            "rows_with_state_voter_id=%d rows_with_canonical_name=%d elapsed_s=%.1f",
            rows_processed,
            rows_upserted,
            rows_with_state_voter_id,
            rows_with_canonical_name,
            perf_counter() - import_started,
        )

    return VRDBImportResult(
        source_file=source_file,
        table_name=table_name,
        rows_processed=rows_processed,
        rows_upserted=rows_upserted,
        rows_with_state_voter_id=rows_with_state_voter_id,
        rows_with_canonical_name=rows_with_canonical_name,
        normalization_version=resolved_normalization_version,
        normalization_version_hash=resolved_normalization_version_hash,
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
    key_column: str = "full_name_key",
    active_only: bool = True,
) -> pd.DataFrame:
    resolved_column = _validated_name_key_column(key_column)
    normalized_key_values = sorted({str(value or "").strip() for value in key_values if str(value or "").strip()})
    if not normalized_key_values:
        return pd.DataFrame(columns=[resolved_column, "n_registry_rows"])

    psycopg, sql = _load_psycopg()
    rows: list[tuple[str, int]] = []
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cursor:
            for chunk in _chunk_values(normalized_key_values, chunk_size=10_000):
                where_clause = sql.SQL("{} = ANY(%s)").format(sql.Identifier(resolved_column))
                if active_only:
                    where_clause = sql.SQL("{} AND status_code = {}").format(
                        where_clause,
                        sql.SQL(f"'{ACTIVE_STATUS_VALUE}'"),
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
    normalized_lasts = sorted({str(value or "").strip() for value in canonical_lasts if str(value or "").strip()})
    if not normalized_lasts:
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
            where_clause = sql.SQL("canonical_last = ANY(%s)")
            if active_only:
                where_clause = sql.SQL("{} AND status_code = {}").format(
                    where_clause,
                    sql.SQL(f"'{ACTIVE_STATUS_VALUE}'"),
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
            cursor.execute(query, (normalized_lasts,))
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
    key_column: str = "full_name_key",
    active_only: bool = True,
) -> pd.DataFrame:
    resolved_column = _validated_name_key_column(key_column)
    psycopg, sql = _load_psycopg()
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cursor:
            where_clause = sql.SQL("TRUE")
            if active_only:
                where_clause = sql.SQL("status_code = {}").format(
                    sql.SQL(f"'{ACTIVE_STATUS_VALUE}'")
                )
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


def fetch_voter_name_key_count_histogram(
    db_url: str,
    table_name: str,
    *,
    key_column: str = "full_name_key",
    active_only: bool = True,
) -> pd.DataFrame:
    resolved_column = _validated_name_key_column(key_column)
    psycopg, sql = _load_psycopg()
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cursor:
            where_clause = sql.SQL("TRUE")
            if active_only:
                where_clause = sql.SQL("status_code = {}").format(
                    sql.SQL(f"'{ACTIVE_STATUS_VALUE}'")
                )
            query = sql.SQL(
                "WITH key_counts AS ("
                "  SELECT {key_column} AS key_value, COUNT(*)::BIGINT AS name_count "
                "  FROM {table_name} "
                "  WHERE {where_clause} "
                "  GROUP BY {key_column}"
                ") "
                "SELECT name_count::BIGINT AS name_count, "
                "       COUNT(*)::BIGINT AS n_keys "
                "FROM key_counts "
                "GROUP BY name_count "
                "ORDER BY name_count"
            ).format(
                key_column=sql.Identifier(resolved_column),
                table_name=sql.Identifier(table_name),
                where_clause=where_clause,
            )
            cursor.execute(query)
            rows = cursor.fetchall()
    if not rows:
        return pd.DataFrame(columns=["name_count", "n_keys", "N"])
    out = pd.DataFrame(rows, columns=["name_count", "n_keys"])
    out["name_count"] = pd.to_numeric(out["name_count"], errors="coerce").fillna(0).astype(int)
    out["n_keys"] = pd.to_numeric(out["n_keys"], errors="coerce").fillna(0).astype(int)
    out = out[(out["name_count"] > 0) & (out["n_keys"] > 0)].copy()
    n_population = int((out["name_count"] * out["n_keys"]).sum())
    out["N"] = n_population
    return out.sort_values("name_count").reset_index(drop=True)


def fetch_voter_name_key_stratum_frequencies(
    db_url: str,
    table_name: str,
    *,
    key_column: str = "full_name_key",
    stratification: str = "birth_decade",
    active_only: bool = True,
) -> pd.DataFrame:
    resolved_column = _validated_name_key_column(key_column)
    psycopg, sql = _load_psycopg()
    strat_expr = _stratification_sql_expression(stratification, sql)
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cursor:
            where_clause = sql.SQL("TRUE")
            if active_only:
                where_clause = sql.SQL("status_code = {}").format(
                    sql.SQL(f"'{ACTIVE_STATUS_VALUE}'")
                )
            query = sql.SQL(
                "SELECT {key_column} AS name_key, "
                "{strat_expr} AS stratum, "
                "COUNT(*)::BIGINT AS n_registry_rows "
                "FROM {table_name} "
                "WHERE {where_clause} "
                "GROUP BY {key_column}, {strat_expr}"
            ).format(
                key_column=sql.Identifier(resolved_column),
                strat_expr=strat_expr,
                table_name=sql.Identifier(table_name),
                where_clause=where_clause,
            )
            cursor.execute(query)
            rows = cursor.fetchall()
    if not rows:
        return pd.DataFrame(columns=["name_key", "stratum", "n_registry_rows"])
    out = pd.DataFrame(rows, columns=["name_key", "stratum", "n_registry_rows"])
    out["name_key"] = out["name_key"].fillna("").astype(str).str.strip()
    out["stratum"] = out["stratum"].fillna("unknown").astype(str).str.strip()
    out["n_registry_rows"] = (
        pd.to_numeric(out["n_registry_rows"], errors="coerce").fillna(0).astype(int)
    )
    return out[(out["name_key"] != "") & (out["n_registry_rows"] > 0)].reset_index(drop=True)


def count_registry_rows(db_url: str, table_name: str, active_only: bool = True) -> int:
    psycopg, sql = _load_psycopg()
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cursor:
            where_sql = (
                sql.SQL(" WHERE status_code = {}").format(sql.SQL(f"'{ACTIVE_STATUS_VALUE}'"))
                if active_only
                else sql.SQL("")
            )
            query = sql.SQL("SELECT COUNT(*)::BIGINT FROM {table_name}{where_sql}").format(
                table_name=sql.Identifier(table_name),
                where_sql=where_sql,
            )
            cursor.execute(query)
            value = cursor.fetchone()
    return int(value[0]) if value and value[0] is not None else 0
