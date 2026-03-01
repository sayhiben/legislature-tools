from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from testifier_audit.io.vrdb_probability_artifacts import (
    build_vrdb_probability_artifact_tables,
    write_vrdb_probability_artifacts,
)


def _fixture_rows() -> pd.DataFrame:
    rows: list[dict[str, str]] = []

    rows.extend(
        {
            "full_name_key": "DOE|JANE||",
            "first_name_key": "JANE",
            "last_name_key": "DOE",
            "status_code": "Active",
            "county_code": "AD",
            "reg_city": "Ritzville",
            "normalization_version": "shared_name_normalization_v1:abc123",
            "source_file": "vrdb_extract_20260202.txt",
        }
        for _ in range(4)
    )
    rows.extend(
        {
            "full_name_key": "SMITH|JOHN||",
            "first_name_key": "JOHN",
            "last_name_key": "SMITH",
            "status_code": "active",
            "county_code": "AD",
            "reg_city": "Ritzville",
            "normalization_version": "shared_name_normalization_v1:abc123",
            "source_file": "vrdb_extract_20260202.txt",
        }
        for _ in range(2)
    )
    rows.extend(
        {
            "full_name_key": "DOE|JANE||",
            "first_name_key": "JANE",
            "last_name_key": "DOE",
            "status_code": "Inactive",
            "county_code": "AD",
            "reg_city": "Ritzville",
            "normalization_version": "shared_name_normalization_v1:abc123",
            "source_file": "vrdb_extract_20260202.txt",
        }
        for _ in range(1)
    )
    rows.extend(
        {
            "full_name_key": "BROWN|ALEX||",
            "first_name_key": "ALEX",
            "last_name_key": "BROWN",
            "status_code": "Inactive",
            "county_code": "AD",
            "reg_city": "Ritzville",
            "normalization_version": "shared_name_normalization_v1:abc123",
            "source_file": "vrdb_extract_20260202.txt",
        }
        for _ in range(1)
    )
    rows.extend(
        {
            "full_name_key": "DOE|JANE||",
            "first_name_key": "JANE",
            "last_name_key": "DOE",
            "status_code": "Active",
            "county_code": "AD",
            "reg_city": "Lind",
            "normalization_version": "shared_name_normalization_v1:abc123",
            "source_file": "vrdb_extract_20260202.txt",
        }
        for _ in range(1)
    )
    rows.extend(
        {
            "full_name_key": "JONES|PAT||",
            "first_name_key": "PAT",
            "last_name_key": "JONES",
            "status_code": "Active",
            "county_code": "AD",
            "reg_city": "",
            "normalization_version": "shared_name_normalization_v1:abc123",
            "source_file": "vrdb_extract_20260202.txt",
        }
        for _ in range(3)
    )
    rows.extend(
        {
            "full_name_key": "LEE|KAI||",
            "first_name_key": "KAI",
            "last_name_key": "LEE",
            "status_code": "Active",
            "county_code": "BE",
            "reg_city": "Smalltown",
            "normalization_version": "shared_name_normalization_v1:abc123",
            "source_file": "vrdb_extract_20260202.txt",
        }
        for _ in range(2)
    )
    rows.extend(
        {
            "full_name_key": "KING|SAM||",
            "first_name_key": "SAM",
            "last_name_key": "KING",
            "status_code": "Active",
            "county_code": "BE",
            "reg_city": "Smalltown",
            "normalization_version": "shared_name_normalization_v1:abc123",
            "source_file": "vrdb_extract_20260202.txt",
        }
        for _ in range(1)
    )
    rows.extend(
        {
            "full_name_key": "REED|JAY||",
            "first_name_key": "JAY",
            "last_name_key": "REED",
            "status_code": "Active",
            "county_code": "BE",
            "reg_city": "",
            "normalization_version": "shared_name_normalization_v1:abc123",
            "source_file": "vrdb_extract_20260202.txt",
        }
        for _ in range(1)
    )

    return pd.DataFrame(rows)


def test_build_vrdb_probability_artifact_tables_applies_backoff_thresholds() -> None:
    probability_rows, backoff_rows, metadata = build_vrdb_probability_artifact_tables(
        chunks=[_fixture_rows()],
        min_county_denominator=5,
        min_city_denominator=4,
        min_city_coverage=0.65,
    )

    assert metadata["probability_row_count"] == len(probability_rows)
    assert metadata["backoff_row_count"] == len(backoff_rows)
    assert {"all_registrants", "active_only"}.issubset(set(probability_rows["baseline_variant"]))

    city_supported = backoff_rows[
        (backoff_rows["baseline_variant"] == "all_registrants")
        & (backoff_rows["requested_geo_level"] == "city")
        & (backoff_rows["requested_geo_value"] == "AD|RITZVILLE")
    ]
    assert len(city_supported) == 1
    assert city_supported.iloc[0]["effective_geo_level"] == "city"

    city_fallback_to_county = backoff_rows[
        (backoff_rows["baseline_variant"] == "all_registrants")
        & (backoff_rows["requested_geo_level"] == "city")
        & (backoff_rows["requested_geo_value"] == "AD|LIND")
    ]
    assert len(city_fallback_to_county) == 1
    assert city_fallback_to_county.iloc[0]["effective_geo_level"] == "county"

    city_fallback_to_state = backoff_rows[
        (backoff_rows["baseline_variant"] == "all_registrants")
        & (backoff_rows["requested_geo_level"] == "city")
        & (backoff_rows["requested_geo_value"] == "BE|SMALLTOWN")
    ]
    assert len(city_fallback_to_state) == 1
    assert city_fallback_to_state.iloc[0]["effective_geo_level"] == "state"

    county_fallback_to_state = backoff_rows[
        (backoff_rows["baseline_variant"] == "all_registrants")
        & (backoff_rows["requested_geo_level"] == "county")
        & (backoff_rows["requested_geo_value"] == "BE")
    ]
    assert len(county_fallback_to_state) == 1
    assert county_fallback_to_state.iloc[0]["effective_geo_level"] == "state"

    # Unreliable geographies should not emit dedicated probability rows.
    assert not (
        (probability_rows["baseline_variant"] == "all_registrants")
        & (probability_rows["geo_level"] == "city")
        & (probability_rows["geo_value"] == "AD|LIND")
    ).any()
    assert not (
        (probability_rows["baseline_variant"] == "all_registrants")
        & (probability_rows["geo_level"] == "county")
        & (probability_rows["geo_value"] == "BE")
    ).any()


def test_build_vrdb_probability_artifact_tables_separates_variant_denominators() -> None:
    probability_rows, _backoff_rows, _metadata = build_vrdb_probability_artifact_tables(
        chunks=[_fixture_rows()],
        min_county_denominator=5,
        min_city_denominator=4,
        min_city_coverage=0.65,
    )

    state_rows = probability_rows[
        (probability_rows["geo_level"] == "state")
        & (probability_rows["name_key_type"] == "full_name_key")
        & (probability_rows["name_key"] == "DOE|JANE||")
    ]
    assert set(state_rows["baseline_variant"]) == {"all_registrants", "active_only"}
    by_variant = {
        row["baseline_variant"]: int(row["denominator"])
        for row in state_rows[["baseline_variant", "denominator"]].to_dict(orient="records")
    }
    assert by_variant["all_registrants"] == 16
    assert by_variant["active_only"] == 14


def test_write_vrdb_probability_artifacts_reproducible_checksums(tmp_path: Path) -> None:
    probability_rows_a, backoff_rows_a, metadata_a = build_vrdb_probability_artifact_tables(
        chunks=[_fixture_rows()],
        min_county_denominator=5,
        min_city_denominator=4,
        min_city_coverage=0.65,
    )
    probability_rows_b, backoff_rows_b, metadata_b = build_vrdb_probability_artifact_tables(
        chunks=[_fixture_rows()],
        min_county_denominator=5,
        min_city_denominator=4,
        min_city_coverage=0.65,
    )

    result_a = write_vrdb_probability_artifacts(
        probability_rows=probability_rows_a,
        backoff_rows=backoff_rows_a,
        metadata=metadata_a,
        probability_rows_path=tmp_path / "run_a" / "vrdb_name_probabilities.csv",
        backoff_rows_path=tmp_path / "run_a" / "vrdb_geo_backoff.csv",
        metadata_path=tmp_path / "run_a" / "vrdb_probability_artifacts.json",
    )
    result_b = write_vrdb_probability_artifacts(
        probability_rows=probability_rows_b,
        backoff_rows=backoff_rows_b,
        metadata=metadata_b,
        probability_rows_path=tmp_path / "run_b" / "vrdb_name_probabilities.csv",
        backoff_rows_path=tmp_path / "run_b" / "vrdb_geo_backoff.csv",
        metadata_path=tmp_path / "run_b" / "vrdb_probability_artifacts.json",
    )

    assert result_a.probability_rows_sha256 == result_b.probability_rows_sha256
    assert result_a.backoff_rows_sha256 == result_b.backoff_rows_sha256
    assert result_a.vrdb_version == result_b.vrdb_version
    assert result_a.normalization_version == result_b.normalization_version


def test_build_vrdb_probability_artifact_tables_validates_thresholds() -> None:
    with pytest.raises(ValueError, match="min_county_denominator"):
        build_vrdb_probability_artifact_tables(chunks=[_fixture_rows()], min_county_denominator=0)

    with pytest.raises(ValueError, match="min_city_denominator"):
        build_vrdb_probability_artifact_tables(chunks=[_fixture_rows()], min_city_denominator=0)

    with pytest.raises(ValueError, match="min_city_coverage"):
        build_vrdb_probability_artifact_tables(chunks=[_fixture_rows()], min_city_coverage=1.5)
