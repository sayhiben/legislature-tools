from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from scipy.stats import binom, binomtest

from testifier_audit.detectors.off_hours_statistics import (
    apply_bh_fdr,
    control_limits,
    exact_binomial_tail_p_values,
    standardized_residual,
)


def test_control_limits_matches_expected_formula_values() -> None:
    lower, upper = control_limits(
        pd.Series([0.5]),
        pd.Series([100]),
        z=1.96,
    )
    assert lower.iloc[0] == pytest.approx(0.402, abs=1e-3)
    assert upper.iloc[0] == pytest.approx(0.598, abs=1e-3)

    lower_invalid, upper_invalid = control_limits(
        pd.Series([0.5]),
        pd.Series([0]),
        z=1.96,
    )
    assert pd.isna(lower_invalid.iloc[0])
    assert pd.isna(upper_invalid.iloc[0])


def test_standardized_residual_computes_expected_z_scores() -> None:
    z_scores = standardized_residual(
        observed_successes=pd.Series([60, 40]),
        totals=pd.Series([100, 100]),
        expected_rate=pd.Series([0.5, 0.5]),
    )
    assert z_scores.iloc[0] == pytest.approx(2.0, abs=1e-9)
    assert z_scores.iloc[1] == pytest.approx(-2.0, abs=1e-9)


def test_exact_binomial_tail_p_values_match_scipy_reference() -> None:
    p_lower, p_upper, p_two_sided, valid = exact_binomial_tail_p_values(
        observed_successes=pd.Series([2]),
        totals=pd.Series([10]),
        expected_rate=pd.Series([0.5]),
    )

    assert bool(valid.iloc[0]) is True
    assert p_lower.iloc[0] == pytest.approx(float(binom.cdf(2, 10, 0.5)), abs=1e-12)
    assert p_upper.iloc[0] == pytest.approx(float(binom.sf(1, 10, 0.5)), abs=1e-12)
    assert p_two_sided.iloc[0] == pytest.approx(
        float(binomtest(2, 10, p=0.5, alternative="two-sided").pvalue),
        abs=1e-12,
    )


def test_apply_bh_fdr_handles_missing_values() -> None:
    p_values = pd.Series([0.001, 0.02, np.nan, 0.5], index=["a", "b", "c", "d"])
    q_values, is_significant = apply_bh_fdr(p_values, alpha=0.05)

    assert q_values.index.tolist() == ["a", "b", "c", "d"]
    assert pd.isna(q_values.loc["c"])
    assert bool(is_significant.loc["c"]) is False
    assert bool(is_significant.loc["a"]) is True
    assert bool(is_significant.loc["b"]) is True
    assert bool(is_significant.loc["d"]) is False
