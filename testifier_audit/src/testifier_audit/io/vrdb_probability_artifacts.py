from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

from testifier_audit.io.import_tracking import compute_file_sha256
from testifier_audit.io.vrdb_postgres import (
    ACTIVE_STATUS_VALUE,
    VRDB_IMPORTER_VERSION,
    _load_psycopg,
    _normalize_geo_city_series,
    _normalize_geo_county_code_series,
    _normalize_status_code_series,
)

logger = logging.getLogger(__name__)

VRDB_PROBABILITY_ARTIFACT_SCHEMA_VERSION = 1
DEFAULT_BASELINE_VARIANTS: tuple[str, ...] = ("all_registrants", "active_only")
ALLOWED_BASELINE_VARIANTS: frozenset[str] = frozenset(DEFAULT_BASELINE_VARIANTS)
DEFAULT_KEY_COLUMNS: tuple[str, ...] = ("full_name_key",)
MARGINAL_KEY_COLUMNS: tuple[str, ...] = ("first_name_key", "last_name_key")
ALLOWED_KEY_COLUMNS: frozenset[str] = frozenset((*DEFAULT_KEY_COLUMNS, *MARGINAL_KEY_COLUMNS))


@dataclass(frozen=True)
class VRDBProbabilityArtifactBuildResult:
    probability_rows_path: Path
    backoff_rows_path: Path
    metadata_path: Path
    probability_row_count: int
    backoff_row_count: int
    probability_rows_sha256: str
    backoff_rows_sha256: str
    vrdb_version: str
    normalization_version: str


@dataclass
class _VariantAccumulator:
    state_denominator: int = 0
    county_denominator: Counter[str] = field(default_factory=Counter)
    county_city_numerator: Counter[str] = field(default_factory=Counter)
    city_denominator: Counter[tuple[str, str]] = field(default_factory=Counter)
    state_key_counts: dict[str, Counter[str]] = field(default_factory=dict)
    county_key_counts: dict[str, Counter[tuple[str, str]]] = field(default_factory=dict)
    city_key_counts: dict[str, Counter[tuple[str, str, str]]] = field(default_factory=dict)


def _normalize_text_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def _validate_baseline_variants(variants: Sequence[str] | None) -> tuple[str, ...]:
    resolved = tuple(str(value or "").strip() for value in (variants or DEFAULT_BASELINE_VARIANTS))
    if not resolved:
        raise ValueError("At least one baseline variant is required.")
    invalid = [value for value in resolved if value not in ALLOWED_BASELINE_VARIANTS]
    if invalid:
        allowed = ", ".join(sorted(ALLOWED_BASELINE_VARIANTS))
        raise ValueError(
            f"Unsupported baseline variant(s): {', '.join(invalid)}. Allowed variants: {allowed}."
        )
    return tuple(dict.fromkeys(resolved))


def _validate_key_columns(
    key_columns: Sequence[str] | None,
    *,
    include_marginals: bool,
) -> tuple[str, ...]:
    if key_columns is None:
        resolved = list(DEFAULT_KEY_COLUMNS)
        if include_marginals:
            resolved.extend(MARGINAL_KEY_COLUMNS)
    else:
        resolved = [str(value or "").strip() for value in key_columns]
    normalized = tuple(dict.fromkeys(value for value in resolved if value))
    if not normalized:
        raise ValueError("At least one name-key column is required.")
    invalid = [value for value in normalized if value not in ALLOWED_KEY_COLUMNS]
    if invalid:
        allowed = ", ".join(sorted(ALLOWED_KEY_COLUMNS))
        raise ValueError(f"Unsupported key columns: {', '.join(invalid)}. Allowed columns: {allowed}.")
    return normalized


def _city_geo_value(county_code: str, city: str) -> str:
    return f"{county_code}|{city}" if city else f"{county_code}|"


def _clean_probability_rows(rows: list[dict[str, object]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "name_key",
                "name_key_type",
                "count",
                "probability",
                "denominator",
                "geo_level",
                "geo_value",
                "effective_geo_level",
                "effective_geo_value",
                "baseline_variant",
                "vrdb_version",
                "normalization_version",
            ]
        )
    frame = frame.copy()
    frame["name_key"] = _normalize_text_series(frame["name_key"])
    frame["name_key_type"] = _normalize_text_series(frame["name_key_type"])
    frame["count"] = pd.to_numeric(frame["count"], errors="coerce").fillna(0).astype(int)
    frame["denominator"] = pd.to_numeric(frame["denominator"], errors="coerce").fillna(0).astype(int)
    frame["probability"] = pd.to_numeric(frame["probability"], errors="coerce").fillna(0.0)
    frame["geo_level"] = _normalize_text_series(frame["geo_level"])
    frame["geo_value"] = _normalize_text_series(frame["geo_value"])
    frame["effective_geo_level"] = _normalize_text_series(frame["effective_geo_level"])
    frame["effective_geo_value"] = _normalize_text_series(frame["effective_geo_value"])
    frame["baseline_variant"] = _normalize_text_series(frame["baseline_variant"])
    frame["vrdb_version"] = _normalize_text_series(frame["vrdb_version"])
    frame["normalization_version"] = _normalize_text_series(frame["normalization_version"])
    frame = frame[(frame["name_key"] != "") & (frame["count"] > 0) & (frame["denominator"] > 0)].copy()
    frame = frame.sort_values(
        ["baseline_variant", "name_key_type", "geo_level", "geo_value", "name_key"],
        ascending=[True, True, True, True, True],
    ).reset_index(drop=True)
    return frame


def _clean_backoff_rows(rows: list[dict[str, object]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "baseline_variant",
                "requested_geo_level",
                "requested_geo_value",
                "requested_denominator",
                "effective_geo_level",
                "effective_geo_value",
                "effective_denominator",
                "fallback_steps",
                "backoff_reason",
                "county_city_coverage",
            ]
        )
    frame = frame.copy()
    frame["baseline_variant"] = _normalize_text_series(frame["baseline_variant"])
    frame["requested_geo_level"] = _normalize_text_series(frame["requested_geo_level"])
    frame["requested_geo_value"] = _normalize_text_series(frame["requested_geo_value"])
    frame["requested_denominator"] = (
        pd.to_numeric(frame["requested_denominator"], errors="coerce").fillna(0).astype(int)
    )
    frame["effective_geo_level"] = _normalize_text_series(frame["effective_geo_level"])
    frame["effective_geo_value"] = _normalize_text_series(frame["effective_geo_value"])
    frame["effective_denominator"] = (
        pd.to_numeric(frame["effective_denominator"], errors="coerce").fillna(0).astype(int)
    )
    frame["fallback_steps"] = pd.to_numeric(frame["fallback_steps"], errors="coerce").fillna(0).astype(int)
    frame["backoff_reason"] = _normalize_text_series(frame["backoff_reason"])
    frame["county_city_coverage"] = (
        pd.to_numeric(frame["county_city_coverage"], errors="coerce").fillna(0.0)
    )
    frame = frame.sort_values(
        ["baseline_variant", "requested_geo_level", "requested_geo_value"],
        ascending=[True, True, True],
    ).reset_index(drop=True)
    return frame


def _resolve_snapshot_versions(
    *,
    normalization_versions: Counter[str],
    source_files: set[str],
) -> tuple[str, list[str], str, list[str]]:
    normalization_versions_sorted = sorted(
        [value for value in normalization_versions if value],
        key=lambda value: (-int(normalization_versions[value]), value),
    )
    if not normalization_versions_sorted:
        normalization_version = "unknown"
    elif len(normalization_versions_sorted) == 1:
        normalization_version = normalization_versions_sorted[0]
    else:
        normalization_version = "mixed"

    source_file_list = sorted(value for value in source_files if value)
    source_file_hash = sha256(
        json.dumps(source_file_list, ensure_ascii=True, sort_keys=True).encode("utf-8")
    ).hexdigest()
    vrdb_version = f"{VRDB_IMPORTER_VERSION}:{source_file_hash[:12]}"

    return normalization_version, normalization_versions_sorted, vrdb_version, source_file_list


def _prepare_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    source = chunk.copy()
    required_columns = [
        "full_name_key",
        "first_name_key",
        "last_name_key",
        "status_code",
        "county_code",
        "reg_city",
        "normalization_version",
        "source_file",
    ]
    for column in required_columns:
        if column not in source.columns:
            source[column] = ""

    out = pd.DataFrame(index=source.index)
    for column in required_columns:
        out[column] = _normalize_text_series(source[column])
    out["status_code"] = _normalize_status_code_series(out["status_code"])
    out["county_code"] = _normalize_geo_county_code_series(out["county_code"])
    out["reg_city"] = _normalize_geo_city_series(out["reg_city"])
    return out[out["full_name_key"] != ""].copy()


def _update_counter_from_series(counter: Counter, series: pd.Series) -> None:
    for key, value in series.items():
        counter[key] += int(value)


def _update_counter_from_groupby(counter: Counter, series: pd.Series) -> None:
    for key, value in series.items():
        counter[key] += int(value)


def _update_variant_accumulator(
    *,
    accumulator: _VariantAccumulator,
    frame: pd.DataFrame,
    key_columns: tuple[str, ...],
) -> None:
    if frame.empty:
        return

    accumulator.state_denominator += int(len(frame))

    county_frame = frame[frame["county_code"] != ""]
    _update_counter_from_series(accumulator.county_denominator, county_frame["county_code"].value_counts())

    city_frame = county_frame[county_frame["reg_city"] != ""]
    _update_counter_from_series(
        accumulator.county_city_numerator,
        city_frame["county_code"].value_counts(),
    )
    _update_counter_from_groupby(
        accumulator.city_denominator,
        city_frame.groupby(["county_code", "reg_city"], dropna=False).size(),
    )

    for key_column in key_columns:
        key_frame = frame[[key_column, "county_code", "reg_city"]].copy()
        key_frame[key_column] = _normalize_text_series(key_frame[key_column])
        key_frame = key_frame[key_frame[key_column] != ""]
        if key_frame.empty:
            continue

        state_counter = accumulator.state_key_counts.setdefault(key_column, Counter())
        county_counter = accumulator.county_key_counts.setdefault(key_column, Counter())
        city_counter = accumulator.city_key_counts.setdefault(key_column, Counter())

        _update_counter_from_series(state_counter, key_frame[key_column].value_counts())

        county_key_frame = key_frame[key_frame["county_code"] != ""]
        if not county_key_frame.empty:
            _update_counter_from_groupby(
                county_counter,
                county_key_frame.groupby(["county_code", key_column], dropna=False).size(),
            )

        city_key_frame = county_key_frame[county_key_frame["reg_city"] != ""]
        if not city_key_frame.empty:
            _update_counter_from_groupby(
                city_counter,
                city_key_frame.groupby(["county_code", "reg_city", key_column], dropna=False).size(),
            )


def build_vrdb_probability_artifact_tables(
    *,
    chunks: Iterable[pd.DataFrame],
    baseline_variants: Sequence[str] | None = None,
    include_marginals: bool = False,
    key_columns: Sequence[str] | None = None,
    state_geo_value: str = "WA",
    min_county_denominator: int = 1_000,
    min_city_denominator: int = 250,
    min_city_coverage: float = 0.75,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    if int(min_county_denominator) < 1:
        raise ValueError("min_county_denominator must be >= 1")
    if int(min_city_denominator) < 1:
        raise ValueError("min_city_denominator must be >= 1")
    if float(min_city_coverage) < 0.0 or float(min_city_coverage) > 1.0:
        raise ValueError("min_city_coverage must be between 0 and 1")

    resolved_variants = _validate_baseline_variants(baseline_variants)
    resolved_key_columns = _validate_key_columns(key_columns, include_marginals=include_marginals)
    resolved_state_geo_value = str(state_geo_value or "").strip() or "WA"

    accumulators: dict[str, _VariantAccumulator] = {
        variant: _VariantAccumulator(
            state_key_counts={column: Counter() for column in resolved_key_columns},
            county_key_counts={column: Counter() for column in resolved_key_columns},
            city_key_counts={column: Counter() for column in resolved_key_columns},
        )
        for variant in resolved_variants
    }

    normalization_versions: Counter[str] = Counter()
    source_files: set[str] = set()

    chunk_index = 0
    for chunk in chunks:
        chunk_index += 1
        prepared = _prepare_chunk(chunk)
        if prepared.empty:
            logger.info("VRDB artifact build: skipped empty chunk %s", chunk_index)
            continue

        normalization_versions.update(
            value
            for value in _normalize_text_series(prepared["normalization_version"]).tolist()
            if value
        )
        source_files.update(
            value for value in _normalize_text_series(prepared["source_file"]).tolist() if value
        )

        for variant in resolved_variants:
            if variant == "active_only":
                variant_frame = prepared[prepared["status_code"] == ACTIVE_STATUS_VALUE]
            else:
                variant_frame = prepared
            if variant_frame.empty:
                continue
            _update_variant_accumulator(
                accumulator=accumulators[variant],
                frame=variant_frame,
                key_columns=resolved_key_columns,
            )
        logger.info("VRDB artifact build: processed chunk %s (%s rows)", chunk_index, len(prepared))

    normalization_version, normalization_versions_present, vrdb_version, source_file_list = (
        _resolve_snapshot_versions(
            normalization_versions=normalization_versions,
            source_files=source_files,
        )
    )

    probability_rows: list[dict[str, object]] = []
    backoff_rows: list[dict[str, object]] = []

    for variant in resolved_variants:
        accumulator = accumulators[variant]
        state_denominator = int(accumulator.state_denominator)
        if state_denominator <= 0:
            continue

        county_denominator = {k: int(v) for k, v in accumulator.county_denominator.items() if str(k)}
        city_denominator = {
            (county, city): int(value)
            for (county, city), value in accumulator.city_denominator.items()
            if county and city
        }
        county_city_coverage = {
            county: float(accumulator.county_city_numerator.get(county, 0)) / float(denominator)
            for county, denominator in county_denominator.items()
            if denominator > 0
        }

        reliable_counties = {
            county
            for county, denominator in county_denominator.items()
            if denominator >= int(min_county_denominator)
        }
        reliable_cities = {
            (county, city)
            for (county, city), denominator in city_denominator.items()
            if county in reliable_counties
            and denominator >= int(min_city_denominator)
            and county_city_coverage.get(county, 0.0) >= float(min_city_coverage)
        }

        for key_column in resolved_key_columns:
            state_counter = accumulator.state_key_counts.get(key_column, Counter())
            for name_key, count in state_counter.items():
                count_value = int(count)
                if count_value <= 0:
                    continue
                probability_rows.append(
                    {
                        "name_key": name_key,
                        "name_key_type": key_column,
                        "count": count_value,
                        "probability": float(count_value) / float(state_denominator),
                        "denominator": state_denominator,
                        "geo_level": "state",
                        "geo_value": resolved_state_geo_value,
                        "effective_geo_level": "state",
                        "effective_geo_value": resolved_state_geo_value,
                        "baseline_variant": variant,
                        "vrdb_version": vrdb_version,
                        "normalization_version": normalization_version,
                    }
                )

            county_counter = accumulator.county_key_counts.get(key_column, Counter())
            for (county, name_key), count in county_counter.items():
                if county not in reliable_counties:
                    continue
                denominator = int(county_denominator.get(county, 0))
                count_value = int(count)
                if denominator <= 0 or count_value <= 0:
                    continue
                probability_rows.append(
                    {
                        "name_key": name_key,
                        "name_key_type": key_column,
                        "count": count_value,
                        "probability": float(count_value) / float(denominator),
                        "denominator": denominator,
                        "geo_level": "county",
                        "geo_value": county,
                        "effective_geo_level": "county",
                        "effective_geo_value": county,
                        "baseline_variant": variant,
                        "vrdb_version": vrdb_version,
                        "normalization_version": normalization_version,
                    }
                )

            city_counter = accumulator.city_key_counts.get(key_column, Counter())
            for (county, city, name_key), count in city_counter.items():
                if (county, city) not in reliable_cities:
                    continue
                denominator = int(city_denominator.get((county, city), 0))
                count_value = int(count)
                if denominator <= 0 or count_value <= 0:
                    continue
                geo_value = _city_geo_value(county, city)
                probability_rows.append(
                    {
                        "name_key": name_key,
                        "name_key_type": key_column,
                        "count": count_value,
                        "probability": float(count_value) / float(denominator),
                        "denominator": denominator,
                        "geo_level": "city",
                        "geo_value": geo_value,
                        "effective_geo_level": "city",
                        "effective_geo_value": geo_value,
                        "baseline_variant": variant,
                        "vrdb_version": vrdb_version,
                        "normalization_version": normalization_version,
                    }
                )

        backoff_rows.append(
            {
                "baseline_variant": variant,
                "requested_geo_level": "state",
                "requested_geo_value": resolved_state_geo_value,
                "requested_denominator": state_denominator,
                "effective_geo_level": "state",
                "effective_geo_value": resolved_state_geo_value,
                "effective_denominator": state_denominator,
                "fallback_steps": 0,
                "backoff_reason": "state_supported",
                "county_city_coverage": 1.0,
            }
        )

        for county in sorted(county_denominator):
            requested_denominator = int(county_denominator[county])
            if county in reliable_counties:
                effective_geo_level = "county"
                effective_geo_value = county
                effective_denominator = requested_denominator
                fallback_steps = 0
                backoff_reason = "county_supported"
            else:
                effective_geo_level = "state"
                effective_geo_value = resolved_state_geo_value
                effective_denominator = state_denominator
                fallback_steps = 1
                backoff_reason = "county_denominator_below_threshold"

            backoff_rows.append(
                {
                    "baseline_variant": variant,
                    "requested_geo_level": "county",
                    "requested_geo_value": county,
                    "requested_denominator": requested_denominator,
                    "effective_geo_level": effective_geo_level,
                    "effective_geo_value": effective_geo_value,
                    "effective_denominator": effective_denominator,
                    "fallback_steps": fallback_steps,
                    "backoff_reason": backoff_reason,
                    "county_city_coverage": county_city_coverage.get(county, 0.0),
                }
            )

            missing_city_requested_denominator = int(
                max(0, requested_denominator - int(accumulator.county_city_numerator.get(county, 0)))
            )
            if county in reliable_counties:
                missing_city_effective_level = "county"
                missing_city_effective_value = county
                missing_city_effective_denominator = requested_denominator
                missing_city_fallback_steps = 1
            else:
                missing_city_effective_level = "state"
                missing_city_effective_value = resolved_state_geo_value
                missing_city_effective_denominator = state_denominator
                missing_city_fallback_steps = 2
            backoff_rows.append(
                {
                    "baseline_variant": variant,
                    "requested_geo_level": "city",
                    "requested_geo_value": _city_geo_value(county, ""),
                    "requested_denominator": missing_city_requested_denominator,
                    "effective_geo_level": missing_city_effective_level,
                    "effective_geo_value": missing_city_effective_value,
                    "effective_denominator": missing_city_effective_denominator,
                    "fallback_steps": missing_city_fallback_steps,
                    "backoff_reason": "missing_city",
                    "county_city_coverage": county_city_coverage.get(county, 0.0),
                }
            )

        for county, city in sorted(city_denominator):
            requested_denominator = int(city_denominator[(county, city)])
            coverage = county_city_coverage.get(county, 0.0)
            geo_value = _city_geo_value(county, city)
            if (county, city) in reliable_cities:
                effective_geo_level = "city"
                effective_geo_value = geo_value
                effective_denominator = requested_denominator
                fallback_steps = 0
                backoff_reason = "city_supported"
            elif county in reliable_counties:
                effective_geo_level = "county"
                effective_geo_value = county
                effective_denominator = int(county_denominator.get(county, 0))
                fallback_steps = 1
                if coverage < float(min_city_coverage):
                    backoff_reason = "city_coverage_below_threshold"
                else:
                    backoff_reason = "city_denominator_below_threshold"
            else:
                effective_geo_level = "state"
                effective_geo_value = resolved_state_geo_value
                effective_denominator = state_denominator
                fallback_steps = 2
                if int(county_denominator.get(county, 0)) < int(min_county_denominator):
                    backoff_reason = "county_denominator_below_threshold"
                elif coverage < float(min_city_coverage):
                    backoff_reason = "city_coverage_below_threshold"
                else:
                    backoff_reason = "city_denominator_below_threshold"

            backoff_rows.append(
                {
                    "baseline_variant": variant,
                    "requested_geo_level": "city",
                    "requested_geo_value": geo_value,
                    "requested_denominator": requested_denominator,
                    "effective_geo_level": effective_geo_level,
                    "effective_geo_value": effective_geo_value,
                    "effective_denominator": effective_denominator,
                    "fallback_steps": fallback_steps,
                    "backoff_reason": backoff_reason,
                    "county_city_coverage": coverage,
                }
            )

    probability_df = _clean_probability_rows(probability_rows)
    backoff_df = _clean_backoff_rows(backoff_rows)

    metadata = {
        "schema_version": VRDB_PROBABILITY_ARTIFACT_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "baseline_variants": list(resolved_variants),
        "key_columns": list(resolved_key_columns),
        "include_marginals": bool(include_marginals),
        "state_geo_value": resolved_state_geo_value,
        "min_county_denominator": int(min_county_denominator),
        "min_city_denominator": int(min_city_denominator),
        "min_city_coverage": float(min_city_coverage),
        "vrdb_version": vrdb_version,
        "vrdb_importer_version": VRDB_IMPORTER_VERSION,
        "normalization_version": normalization_version,
        "normalization_versions_present": normalization_versions_present,
        "source_files": source_file_list,
        "probability_row_count": int(len(probability_df)),
        "backoff_row_count": int(len(backoff_df)),
    }
    return probability_df, backoff_df, metadata


def _iter_vrdb_probability_source_chunks(
    *,
    db_url: str,
    table_name: str,
    chunk_size: int,
) -> Iterable[pd.DataFrame]:
    if int(chunk_size) < 1_000:
        raise ValueError("chunk_size must be >= 1000")

    _psycopg, sql = _load_psycopg()
    query = sql.SQL(
        "SELECT full_name_key, first_name_key, last_name_key, status_code, county_code, reg_city, "
        "normalization_version, source_file "
        "FROM {table_name}"
    ).format(table_name=sql.Identifier(table_name))

    with _psycopg.connect(db_url) as conn:
        with conn.cursor(name="vrdb_probability_artifact_cursor") as cursor:
            cursor.itersize = int(chunk_size)
            cursor.execute(query)
            columns = [str(col.name) for col in cursor.description] if cursor.description else []
            while True:
                rows = cursor.fetchmany(int(chunk_size))
                if not rows:
                    break
                yield pd.DataFrame(rows, columns=columns)


def write_vrdb_probability_artifacts(
    *,
    probability_rows: pd.DataFrame,
    backoff_rows: pd.DataFrame,
    metadata: dict[str, object],
    probability_rows_path: Path,
    backoff_rows_path: Path,
    metadata_path: Path,
) -> VRDBProbabilityArtifactBuildResult:
    probability_rows_path.parent.mkdir(parents=True, exist_ok=True)
    backoff_rows_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    probability_rows.to_csv(probability_rows_path, index=False, float_format="%.12g")
    backoff_rows.to_csv(backoff_rows_path, index=False, float_format="%.12g")

    probability_rows_sha256 = compute_file_sha256(probability_rows_path)
    backoff_rows_sha256 = compute_file_sha256(backoff_rows_path)

    metadata_payload = dict(metadata)
    metadata_payload["probability_rows_path"] = str(probability_rows_path)
    metadata_payload["backoff_rows_path"] = str(backoff_rows_path)
    metadata_payload["probability_rows_sha256"] = probability_rows_sha256
    metadata_payload["backoff_rows_sha256"] = backoff_rows_sha256

    metadata_path.write_text(
        json.dumps(metadata_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return VRDBProbabilityArtifactBuildResult(
        probability_rows_path=probability_rows_path,
        backoff_rows_path=backoff_rows_path,
        metadata_path=metadata_path,
        probability_row_count=int(len(probability_rows)),
        backoff_row_count=int(len(backoff_rows)),
        probability_rows_sha256=probability_rows_sha256,
        backoff_rows_sha256=backoff_rows_sha256,
        vrdb_version=str(metadata_payload.get("vrdb_version") or ""),
        normalization_version=str(metadata_payload.get("normalization_version") or ""),
    )


def build_and_write_vrdb_probability_artifacts_from_postgres(
    *,
    db_url: str,
    table_name: str,
    probability_rows_path: Path,
    backoff_rows_path: Path,
    metadata_path: Path,
    chunk_size: int = 250_000,
    baseline_variants: Sequence[str] | None = None,
    include_marginals: bool = False,
    key_columns: Sequence[str] | None = None,
    state_geo_value: str = "WA",
    min_county_denominator: int = 1_000,
    min_city_denominator: int = 250,
    min_city_coverage: float = 0.75,
) -> VRDBProbabilityArtifactBuildResult:
    logger.info(
        "Building VRDB probability artifacts from table %s (chunk_size=%s)",
        table_name,
        chunk_size,
    )
    probability_rows, backoff_rows, metadata = build_vrdb_probability_artifact_tables(
        chunks=_iter_vrdb_probability_source_chunks(
            db_url=db_url,
            table_name=table_name,
            chunk_size=int(chunk_size),
        ),
        baseline_variants=baseline_variants,
        include_marginals=include_marginals,
        key_columns=key_columns,
        state_geo_value=state_geo_value,
        min_county_denominator=int(min_county_denominator),
        min_city_denominator=int(min_city_denominator),
        min_city_coverage=float(min_city_coverage),
    )
    result = write_vrdb_probability_artifacts(
        probability_rows=probability_rows,
        backoff_rows=backoff_rows,
        metadata=metadata,
        probability_rows_path=probability_rows_path,
        backoff_rows_path=backoff_rows_path,
        metadata_path=metadata_path,
    )
    logger.info(
        "VRDB probability artifacts written: %s rows, %s backoff rows",
        result.probability_row_count,
        result.backoff_row_count,
    )
    return result
