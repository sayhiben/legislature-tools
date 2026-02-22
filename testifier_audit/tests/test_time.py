from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from testifier_audit.config import TimeConfig
from testifier_audit.io.hearing_metadata import HearingMetadata
from testifier_audit.preprocess.time import add_time_features


def test_add_time_features_builds_minute_bucket_and_off_hours() -> None:
    df = pd.DataFrame({"time_signed_in": ["2/3/2026 1:15 AM", "2/3/2026 6:15 AM"]})
    out = add_time_features(df=df, config=TimeConfig(off_hours_start=0, off_hours_end=5))

    assert out["minute_bucket"].notna().all()
    assert out.loc[0, "timestamp"].tz is not None
    assert out.loc[0, "is_off_hours"]
    assert not out.loc[1, "is_off_hours"]
    assert out["minutes_to_cutoff"].isna().all()
    assert out["minutes_since_sign_in_open"].isna().all()
    assert out["minutes_since_meeting_start"].isna().all()


def test_add_time_features_populates_hearing_relative_minutes() -> None:
    df = pd.DataFrame({"time_signed_in": ["2/6/2026 12:00 PM", "2/6/2026 1:00 PM"]})
    metadata = HearingMetadata(
        schema_version=1,
        hearing_id="SB6346",
        timezone="America/Los_Angeles",
        meeting_start=datetime(2026, 2, 6, 13, 30, tzinfo=ZoneInfo("America/Los_Angeles")),
        sign_in_open=datetime(2026, 2, 3, 9, 0, tzinfo=ZoneInfo("America/Los_Angeles")),
        sign_in_cutoff=datetime(2026, 2, 6, 12, 30, tzinfo=ZoneInfo("America/Los_Angeles")),
        written_testimony_deadline=None,
    )

    out = add_time_features(
        df=df,
        config=TimeConfig(timezone="America/Los_Angeles", off_hours_start=0, off_hours_end=5),
        hearing_metadata=metadata,
    )

    assert out.loc[0, "minutes_to_cutoff"] == 30.0
    assert out.loc[1, "minutes_to_cutoff"] == -30.0
    assert out.loc[0, "minutes_since_meeting_start"] == -90.0
    assert out.loc[1, "minutes_since_meeting_start"] == -30.0
