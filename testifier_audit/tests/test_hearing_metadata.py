from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from testifier_audit.io.hearing_metadata import load_hearing_metadata, parse_hearing_metadata


def test_load_hearing_metadata_parses_and_normalizes_timezone(tmp_path: Path) -> None:
    sidecar = {
        "schema_version": 1,
        "hearing_id": "SB6346",
        "timezone": "America/Los_Angeles",
        "meeting_start": "2026-02-06T13:30:00-08:00",
        "sign_in_open": "2026-02-03T09:00:00-08:00",
        "sign_in_cutoff": "2026-02-06T12:30:00-08:00",
    }
    path = tmp_path / "hearing.yaml"
    path.write_text(yaml.safe_dump(sidecar), encoding="utf-8")

    metadata = load_hearing_metadata(path)

    assert metadata is not None
    assert metadata.hearing_id == "SB6346"
    assert metadata.timezone == "America/Los_Angeles"
    assert metadata.sign_in_cutoff is not None
    assert metadata.sign_in_cutoff.isoformat().startswith("2026-02-06T12:30:00")


def test_load_hearing_metadata_accepts_yaml_native_datetime_values(tmp_path: Path) -> None:
    path = tmp_path / "hearing.yaml"
    path.write_text(
        "\n".join(
            [
                "schema_version: 1",
                "hearing_id: SB6346",
                "timezone: America/Los_Angeles",
                "meeting_start: 2026-02-06T13:30:00-08:00",
                "sign_in_cutoff: 2026-02-06T12:30:00-08:00",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    metadata = load_hearing_metadata(path)

    assert metadata is not None
    assert metadata.meeting_start is not None
    assert metadata.sign_in_cutoff is not None


def test_parse_hearing_metadata_rejects_invalid_timezone() -> None:
    payload = {
        "schema_version": 1,
        "hearing_id": "SB6346",
        "timezone": "Mars/Olympus",
        "meeting_start": "2026-02-06T13:30:00-08:00",
    }
    with pytest.raises(ValueError, match="invalid hearing metadata timezone"):
        parse_hearing_metadata(payload)


def test_parse_hearing_metadata_rejects_open_after_cutoff() -> None:
    payload = {
        "schema_version": 1,
        "hearing_id": "SB6346",
        "timezone": "America/Los_Angeles",
        "sign_in_open": "2026-02-06T13:00:00-08:00",
        "sign_in_cutoff": "2026-02-06T12:30:00-08:00",
    }
    with pytest.raises(ValueError, match="sign_in_open must be <= sign_in_cutoff"):
        parse_hearing_metadata(payload)


def test_parse_hearing_metadata_localizes_naive_timestamps() -> None:
    payload = {
        "schema_version": 1,
        "hearing_id": "SB6346",
        "timezone": "America/Los_Angeles",
        "meeting_start": "2026-02-06T13:30:00",
    }

    metadata = parse_hearing_metadata(payload)

    assert metadata.meeting_start is not None
    assert metadata.meeting_start.tzinfo is not None
