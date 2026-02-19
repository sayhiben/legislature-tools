from __future__ import annotations

import pandas as pd

from testifier_audit.config import TimeConfig


FREQ_MAP = {
    "minute": "min",
    "hour": "h",
}


def add_time_features(df: pd.DataFrame, config: TimeConfig) -> pd.DataFrame:
    working = df.copy()
    # WA legislature exports commonly use `m/d/YYYY h:mm AM/PM`; parse this first for speed and consistency.
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
            config.timezone,
            nonexistent="shift_forward",
            ambiguous="NaT",
        )
    else:
        timestamps = timestamps.dt.tz_convert(config.timezone)

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
    return working
