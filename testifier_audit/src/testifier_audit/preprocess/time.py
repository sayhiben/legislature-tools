from __future__ import annotations

import pandas as pd

from testifier_audit.config import TimeConfig
from testifier_audit.io.hearing_metadata import HearingMetadata

FREQ_MAP = {
    "minute": "min",
    "hour": "h",
}


def add_time_features(
    df: pd.DataFrame,
    config: TimeConfig,
    hearing_metadata: HearingMetadata | None = None,
) -> pd.DataFrame:
    working = df.copy()
    # WA-only deployment: keep all bucketing/daypart features in configured Pacific time.
    # Sidecar timezone metadata is preserved for context, but does not override analysis timezone.
    timezone_name = config.timezone
    # WA legislature exports commonly use `m/d/YYYY h:mm AM/PM`;
    # parse this first for speed and consistency.
    timestamps = pd.to_datetime(
        working["time_signed_in"],
        format="%m/%d/%Y %I:%M %p",
        errors="coerce",
    )
    if timestamps.isna().all():
        timestamps = pd.to_datetime(working["time_signed_in"], errors="coerce")
    if timestamps.isna().all():
        raise ValueError("No valid timestamps found in time_signed_in column")

    if timestamps.dt.tz is None:
        timestamps = timestamps.dt.tz_localize(
            timezone_name,
            nonexistent="shift_forward",
            ambiguous="NaT",
        )
    else:
        timestamps = timestamps.dt.tz_convert(timezone_name)

    floor_alias = FREQ_MAP.get(config.floor, "min")
    working["timestamp"] = timestamps
    working["minute_bucket"] = timestamps.dt.floor(floor_alias)
    working["hour"] = timestamps.dt.hour
    working["day_of_week"] = timestamps.dt.dayofweek
    working["date"] = timestamps.dt.date

    start = config.off_hours_start
    end = config.off_hours_end
    if start <= end:
        working["is_off_hours"] = working["hour"].between(start, end)
    else:
        working["is_off_hours"] = (working["hour"] >= start) | (working["hour"] <= end)

    working["minutes_to_cutoff"] = pd.Series(float("nan"), index=working.index, dtype="float64")
    working["minutes_since_sign_in_open"] = pd.Series(
        float("nan"), index=working.index, dtype="float64"
    )
    working["minutes_since_meeting_start"] = pd.Series(
        float("nan"), index=working.index, dtype="float64"
    )

    if hearing_metadata is not None:
        if hearing_metadata.sign_in_cutoff is not None:
            working["minutes_to_cutoff"] = (
                hearing_metadata.sign_in_cutoff - working["timestamp"]
            ).dt.total_seconds() / 60.0
        if hearing_metadata.sign_in_open is not None:
            working["minutes_since_sign_in_open"] = (
                working["timestamp"] - hearing_metadata.sign_in_open
            ).dt.total_seconds() / 60.0
        if hearing_metadata.meeting_start is not None:
            working["minutes_since_meeting_start"] = (
                working["timestamp"] - hearing_metadata.meeting_start
            ).dt.total_seconds() / 60.0
    return working
