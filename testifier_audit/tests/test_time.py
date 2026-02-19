from __future__ import annotations

import pandas as pd

from testifier_audit.config import TimeConfig
from testifier_audit.preprocess.time import add_time_features


def test_add_time_features_builds_minute_bucket_and_off_hours() -> None:
    df = pd.DataFrame({"time_signed_in": ["2/3/2026 1:15 AM", "2/3/2026 6:15 AM"]})
    out = add_time_features(df=df, config=TimeConfig(off_hours_start=0, off_hours_end=5))

    assert out["minute_bucket"].notna().all()
    assert out.loc[0, "timestamp"].tz is not None
    assert out.loc[0, "is_off_hours"]
    assert not out.loc[1, "is_off_hours"]
