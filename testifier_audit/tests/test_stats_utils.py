from __future__ import annotations

import numpy as np
import pytest

from testifier_audit.detectors.stats import (
    benjamini_hochberg,
    empirical_tail_p_values,
    rolling_sum,
    simulate_binomial_max_abs_delta,
    simulate_binomial_max_abs_delta_hourly,
    simulate_binomial_max_abs_delta_probability_series,
    simulate_poisson_max_rolling_sums,
    simulate_poisson_max_rolling_sums_hourly,
    simulate_poisson_max_rolling_sums_stratified,
)


def test_stats_basic_helpers_handle_edge_cases() -> None:
    rejected, q_values = benjamini_hochberg(np.array([], dtype=float), alpha=0.1)
    assert rejected.size == 0
    assert q_values.size == 0

    with pytest.raises(ValueError, match="window must be >= 1"):
        rolling_sum(np.array([1.0, 2.0]), window=0)
    assert rolling_sum(np.array([1.0, 2.0]), window=5).size == 0

    assert empirical_tail_p_values(np.array([], dtype=float), np.array([1.0])).size == 0
    np.testing.assert_allclose(
        empirical_tail_p_values(np.array([0.1, 0.2]), np.array([], dtype=float)),
        np.array([1.0, 1.0]),
    )


def test_poisson_simulation_utilities_cover_nontrivial_paths() -> None:
    rng = np.random.default_rng(7)

    assert simulate_poisson_max_rolling_sums(0, 1.0, window=2, iterations=5, rng=rng).size == 0

    maxima = simulate_poisson_max_rolling_sums(30, -2.5, window=5, iterations=6, rng=rng)
    assert maxima.shape == (6,)
    assert np.all(maxima >= 0.0)

    hour_index = np.array([0, 1, 2, 23, 24, -3], dtype=int)
    hourly_rates = np.linspace(0.1, 2.4, 24)
    hourly_maxima = simulate_poisson_max_rolling_sums_hourly(
        hour_index=hour_index,
        hourly_rates=hourly_rates,
        window=2,
        iterations=4,
        rng=rng,
    )
    assert hourly_maxima.shape == (4,)

    stratified_maxima = simulate_poisson_max_rolling_sums_stratified(
        lambdas_per_minute=np.array([-1.0, 0.0, 0.5, 2.5, 4.0]),
        window=2,
        iterations=4,
        rng=rng,
    )
    assert stratified_maxima.shape == (4,)
    assert np.all(np.isfinite(stratified_maxima))


def test_binomial_simulation_utilities_cover_edge_and_success_paths() -> None:
    rng = np.random.default_rng(17)

    totals = np.array([2, 3, 1, 4, 2], dtype=int)
    assert (
        simulate_binomial_max_abs_delta(
            totals_per_minute=totals,
            baseline_probability=0.6,
            window=3,
            min_window_total=999,
            iterations=5,
            rng=rng,
        ).size
        == 0
    )

    maxima, expected = simulate_binomial_max_abs_delta_probability_series(
        totals_per_minute=np.array([10, 12, 8, 9, 11, 10]),
        probabilities_per_minute=np.array([0.2, 0.3, 0.35, 0.55, 0.6, 0.5]),
        window=3,
        min_window_total=20,
        iterations=5,
        rng=rng,
    )
    assert maxima.shape == (5,)
    assert expected.shape == (4,)
    assert np.all((expected >= 0.0) & (expected <= 1.0))

    hourly_maxima, hourly_expected = simulate_binomial_max_abs_delta_hourly(
        totals_per_minute=np.array([5, 6, 7, 8, 9]),
        hourly_probabilities=np.array([0.3, 0.4, 0.5, 0.6, 0.7]),
        window=2,
        min_window_total=5,
        iterations=4,
        rng=rng,
    )
    assert hourly_maxima.shape == (4,)
    assert hourly_expected.shape == (4,)

    empty_maxima, empty_expected = simulate_binomial_max_abs_delta_probability_series(
        totals_per_minute=np.array([1, 1]),
        probabilities_per_minute=np.array([0.5, 0.5]),
        window=3,
        min_window_total=1,
        iterations=4,
        rng=rng,
    )
    assert empty_maxima.size == 0
    assert empty_expected.size == 0
