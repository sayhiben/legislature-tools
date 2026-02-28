from __future__ import annotations

import pandas as pd

from testifier_audit.io.rarity_baselines import normalize_frequency_baseline


def test_normalize_frequency_baseline_aggregates_names() -> None:
    raw = pd.DataFrame(
        {
            "First Name": ["Jane", "JANE", "John", "X Æ A-12", ""],
            "Count": [100, 25, 125, 2, 999],
        }
    )
    normalized, used_name_col, used_value_col = normalize_frequency_baseline(
        table=raw,
        name_column="First Name",
        value_column="Count",
        min_weight=1.0,
    )

    assert used_name_col == "First Name"
    assert used_value_col == "Count"
    assert list(normalized.columns) == ["name", "count", "probability"]
    assert "JANE" in set(normalized["name"])
    assert "JOHN" in set(normalized["name"])
    assert abs(float(normalized["probability"].sum()) - 1.0) < 1e-9
    jane_count = float(normalized.loc[normalized["name"] == "JANE", "count"].iloc[0])
    assert jane_count == 125.0
