from __future__ import annotations

from pathlib import Path

import pandas as pd

from testifier_audit.config import load_config
from testifier_audit.detectors.registry import default_detectors
from testifier_audit.features.aggregates import build_counts_per_hour, build_counts_per_minute, build_name_frequency
from testifier_audit.features.text_features import build_name_text_features


def test_default_detectors_run_without_errors() -> None:
    cfg_path = Path(__file__).resolve().parents[1] / "configs/default.yaml"
    cfg = load_config(cfg_path)

    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "canonical_name": ["DOE|JANE", "DOE|JANE", "SMITH|JOHN", "SMYTH|JOHN"],
            "name_display": ["DOE, JANE", "DOE, JANE", "SMITH, JOHN", "SMYTH, JOHN"],
            "name_normalized": ["Doe, Jane", "Doe, Jane", "Smith, John", "Smyth, John"],
            "position_normalized": ["Pro", "Con", "Pro", "Pro"],
            "minute_bucket": pd.to_datetime(
                [
                    "2026-02-03 17:00:00-08:00",
                    "2026-02-03 17:00:00-08:00",
                    "2026-02-03 17:01:00-08:00",
                    "2026-02-03 17:02:00-08:00",
                ]
            ),
            "timestamp": pd.to_datetime(
                [
                    "2026-02-03 17:00:00-08:00",
                    "2026-02-03 17:00:00-08:00",
                    "2026-02-03 17:01:00-08:00",
                    "2026-02-03 17:02:00-08:00",
                ]
            ),
            "is_off_hours": [False, False, False, False],
            "organization": ["", "", "Org A", "Org A"],
            "last": ["DOE", "DOE", "SMITH", "SMYTH"],
            "first": ["JANE", "JANE", "JOHN", "JOHN"],
            "first_canonical": ["JANE", "JANE", "JOHN", "JOHN"],
            "hour": [17, 17, 17, 17],
            "day_of_week": [1, 1, 1, 1],
            "name": ["Doe, Jane", "Doe, Jane", "Smith, John", "Smyth, John"],
        }
    )

    features = {
        "counts_per_minute": build_counts_per_minute(df),
        "counts_per_hour": build_counts_per_hour(df),
        "name_frequency": build_name_frequency(df),
        "name_text_features": build_name_text_features(df),
    }

    results = [detector.run(df, features) for detector in default_detectors(cfg)]
    assert results
    assert all(result.detector for result in results)
