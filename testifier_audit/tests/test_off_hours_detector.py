from __future__ import annotations

import pandas as pd

from testifier_audit.detectors.off_hours import OffHoursDetector


def test_off_hours_detector_emits_wilson_and_low_power_columns() -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "position_normalized": ["Pro", "Con", "Pro", "Con", "Pro", "Con"],
            "is_off_hours": [True, True, False, False, False, False],
            "hour": [2, 2, 10, 10, 11, 11],
        }
    )

    result = OffHoursDetector().run(df=df, features={})

    summary_table = result.tables["off_hours_summary"]
    hourly = result.tables["hourly_distribution"]

    assert "off_hours_pro_rate_wilson_low" in summary_table.columns
    assert "off_hours_pro_rate_wilson_high" in summary_table.columns
    assert "off_hours_is_low_power" in summary_table.columns
    assert "pro_rate_wilson_low" in hourly.columns
    assert "pro_rate_wilson_high" in hourly.columns
    assert "is_low_power" in hourly.columns
    assert result.summary["off_hours_is_low_power"] is True
