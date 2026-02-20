from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from testifier_audit.proportion_stats import low_power_mask, wilson_half_width, wilson_interval


def test_wilson_interval_returns_expected_shape_and_bounds() -> None:
    lower, upper = wilson_interval(
        successes=pd.Series([0, 5, 10]),
        totals=pd.Series([0, 10, 10]),
    )

    assert lower.shape == (3,)
    assert upper.shape == (3,)
    assert np.isnan(lower[0])
    assert np.isnan(upper[0])
    assert lower[1] == pytest.approx(0.2365930905, abs=1e-5)
    assert upper[1] == pytest.approx(0.7634069095, abs=1e-5)
    assert lower[2] <= upper[2]


def test_wilson_half_width_monotonically_shrinks_with_more_samples() -> None:
    half_width = wilson_half_width(
        successes=pd.Series([5, 50]),
        totals=pd.Series([10, 100]),
    )
    assert half_width[0] > half_width[1]


def test_low_power_mask_flags_small_or_missing_totals() -> None:
    mask = low_power_mask(
        totals=pd.Series([0, 10, 30, np.nan, 45]),
        min_total=30,
    )
    assert mask.tolist() == [True, True, False, True, False]
