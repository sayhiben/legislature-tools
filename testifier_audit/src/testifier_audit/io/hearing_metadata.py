from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd
import yaml

PACIFIC_TIMEZONE_NAME = "America/Los_Angeles"


@dataclass(frozen=True)
class HearingMetadata:
    schema_version: int
    hearing_id: str
    timezone: str
    meeting_start: datetime | None
    sign_in_open: datetime | None
    sign_in_cutoff: datetime | None
    written_testimony_deadline: datetime | None
    source_path: str | None = None

    def marker_times(self) -> dict[str, datetime]:
        markers: dict[str, datetime] = {}
        if self.sign_in_open is not None:
            markers["sign_in_open"] = self.sign_in_open
        if self.sign_in_cutoff is not None:
            markers["sign_in_cutoff"] = self.sign_in_cutoff
        if self.meeting_start is not None:
            markers["meeting_start"] = self.meeting_start
        if self.written_testimony_deadline is not None:
            markers["written_testimony_deadline"] = self.written_testimony_deadline
        return markers


def _require_string(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"hearing metadata field '{field_name}' must be a non-empty string")
    return value.strip()


def _validate_timezone(timezone_name: str) -> str:
    try:
        ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"invalid hearing metadata timezone: {timezone_name}") from exc
    return timezone_name


def _parse_timestamp(
    raw_value: Any,
    *,
    field_name: str,
    timezone_name: str,
) -> datetime | None:
    if raw_value is None:
        return None
    if isinstance(raw_value, pd.Timestamp):
        parsed = raw_value
    elif isinstance(raw_value, datetime):
        parsed = pd.Timestamp(raw_value)
    elif isinstance(raw_value, str) and raw_value.strip():
        try:
            parsed = pd.Timestamp(raw_value.strip())
        except Exception as exc:
            raise ValueError(f"invalid datetime for hearing metadata field '{field_name}'") from exc
    else:
        raise ValueError(
            "hearing metadata field "
            f"'{field_name}' must be an ISO datetime string, datetime, or null"
        )

    if parsed.tzinfo is None:
        parsed = parsed.tz_localize(timezone_name, nonexistent="shift_forward", ambiguous="NaT")
    else:
        parsed = parsed.tz_convert(timezone_name)
    if pd.isna(parsed):
        raise ValueError(f"invalid timezone-localized datetime for field '{field_name}'")
    return parsed.to_pydatetime()


def parse_hearing_metadata(
    payload: Mapping[str, Any],
    *,
    source_path: Path | None = None,
) -> HearingMetadata:
    schema_version = int(payload.get("schema_version", 1))
    if schema_version != 1:
        raise ValueError(f"unsupported hearing metadata schema_version: {schema_version}")

    hearing_id = _require_string(payload.get("hearing_id"), field_name="hearing_id")
    timezone_name = _validate_timezone(
        str(payload.get("timezone") or PACIFIC_TIMEZONE_NAME).strip() or PACIFIC_TIMEZONE_NAME
    )

    meeting_start = _parse_timestamp(
        payload.get("meeting_start"),
        field_name="meeting_start",
        timezone_name=timezone_name,
    )
    sign_in_open = _parse_timestamp(
        payload.get("sign_in_open"),
        field_name="sign_in_open",
        timezone_name=timezone_name,
    )
    sign_in_cutoff = _parse_timestamp(
        payload.get("sign_in_cutoff"),
        field_name="sign_in_cutoff",
        timezone_name=timezone_name,
    )
    written_deadline = _parse_timestamp(
        payload.get("written_testimony_deadline"),
        field_name="written_testimony_deadline",
        timezone_name=timezone_name,
    )

    if all(
        value is None
        for value in (meeting_start, sign_in_open, sign_in_cutoff, written_deadline)
    ):
        raise ValueError("hearing metadata must provide at least one process timestamp")
    if sign_in_open is not None and sign_in_cutoff is not None and sign_in_open > sign_in_cutoff:
        raise ValueError("hearing metadata sign_in_open must be <= sign_in_cutoff")

    return HearingMetadata(
        schema_version=schema_version,
        hearing_id=hearing_id,
        timezone=timezone_name,
        meeting_start=meeting_start,
        sign_in_open=sign_in_open,
        sign_in_cutoff=sign_in_cutoff,
        written_testimony_deadline=written_deadline,
        source_path=str(source_path) if source_path else None,
    )


def load_hearing_metadata(path: str | Path | None) -> HearingMetadata | None:
    if not path:
        return None
    source_path = Path(path).resolve()
    with source_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, Mapping):
        raise ValueError("hearing metadata file must contain a mapping/object")
    return parse_hearing_metadata(payload=payload, source_path=source_path)
