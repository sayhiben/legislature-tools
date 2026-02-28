from __future__ import annotations

from time import perf_counter

import numpy as np

from testifier_audit.profiling import record_runtime_counter, record_runtime_timing


def benjamini_hochberg(p_values: np.ndarray, alpha: float) -> tuple[np.ndarray, np.ndarray]:
    """Return rejection mask and q-values using Benjamini-Hochberg FDR control."""
    if p_values.size == 0:
        return np.array([], dtype=bool), np.array([], dtype=float)

    clipped = np.clip(p_values.astype(float), 0.0, 1.0)
    m = float(clipped.size)
    order = np.argsort(clipped)
    ranked_p = clipped[order]
    rank = np.arange(1, clipped.size + 1, dtype=float)

    ranked_q = ranked_p * (m / rank)
    ranked_q = np.minimum.accumulate(ranked_q[::-1])[::-1]
    ranked_q = np.clip(ranked_q, 0.0, 1.0)

    q_values = np.empty_like(ranked_q)
    q_values[order] = ranked_q
    rejected = q_values <= alpha
    return rejected.astype(bool), q_values.astype(float)


def rolling_sum(values: np.ndarray, window: int) -> np.ndarray:
    """Efficient rolling sum for 1D arrays."""
    if window <= 0:
        raise ValueError("window must be >= 1")
    if values.size < window:
        return np.array([], dtype=float)

    cumulative = np.cumsum(values, dtype=float)
    previous = np.concatenate(([0.0], cumulative[:-window]))
    return cumulative[window - 1 :] - previous


def empirical_tail_p_values(observed: np.ndarray, null_samples: np.ndarray) -> np.ndarray:
    """Compute one-sided empirical tail probabilities with +1 smoothing."""
    if observed.size == 0:
        return np.array([], dtype=float)
    if null_samples.size == 0:
        return np.ones(observed.size, dtype=float)

    sorted_null = np.sort(null_samples.astype(float))
    n = float(sorted_null.size)
    idx = np.searchsorted(sorted_null, observed.astype(float), side="left")
    tail = sorted_null.size - idx
    return (tail + 1.0) / (n + 1.0)


def simulate_poisson_max_rolling_sums(
    n_minutes: int,
    rate_per_minute: float,
    window: int,
    iterations: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Null maxima from Poisson baseline simulations."""
    started = perf_counter()
    maxima = np.array([], dtype=float)
    try:
        if iterations <= 0 or n_minutes <= 0 or window > n_minutes:
            return np.array([], dtype=float)

        maxima = np.zeros(iterations, dtype=float)
        lam = max(rate_per_minute, 0.0)
        for idx in range(iterations):
            sim = rng.poisson(lam=lam, size=n_minutes).astype(float)
            roll = rolling_sum(sim, window)
            maxima[idx] = float(roll.max()) if roll.size else 0.0
        return maxima
    finally:
        record_runtime_timing(
            "simulation.poisson_max_rolling_sums",
            (perf_counter() - started) * 1000.0,
        )
        record_runtime_counter("simulation.poisson_max_rolling_sums.calls", 1)
        record_runtime_counter(
            "simulation.poisson_max_rolling_sums.iterations",
            max(int(iterations), 0),
        )
        record_runtime_counter(
            "simulation.poisson_max_rolling_sums.n_minutes",
            max(int(n_minutes), 0),
        )
        record_runtime_counter(
            "simulation.poisson_max_rolling_sums.output_samples",
            int(maxima.size),
        )


def simulate_poisson_max_rolling_sums_hourly(
    hour_index: np.ndarray,
    hourly_rates: np.ndarray,
    window: int,
    iterations: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Null maxima from Poisson simulation using hour-of-day specific rates."""
    lambdas = hourly_rates[np.clip(hour_index.astype(int), 0, 23)]
    return simulate_poisson_max_rolling_sums_stratified(
        lambdas_per_minute=lambdas,
        window=window,
        iterations=iterations,
        rng=rng,
    )


def simulate_poisson_max_rolling_sums_stratified(
    lambdas_per_minute: np.ndarray,
    window: int,
    iterations: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Null maxima from Poisson simulations with per-minute expected rates."""
    started = perf_counter()
    maxima = np.array([], dtype=float)
    n_minutes = int(lambdas_per_minute.size)
    try:
        if iterations <= 0 or n_minutes == 0 or window > n_minutes:
            return np.array([], dtype=float)

        lambdas = np.clip(lambdas_per_minute.astype(float), 0.0, None)
        maxima = np.zeros(iterations, dtype=float)
        for idx in range(iterations):
            sim = rng.poisson(lam=lambdas).astype(float)
            roll = rolling_sum(sim, window)
            maxima[idx] = float(roll.max()) if roll.size else 0.0
        return maxima
    finally:
        record_runtime_timing(
            "simulation.poisson_max_rolling_sums_stratified",
            (perf_counter() - started) * 1000.0,
        )
        record_runtime_counter("simulation.poisson_max_rolling_sums_stratified.calls", 1)
        record_runtime_counter(
            "simulation.poisson_max_rolling_sums_stratified.iterations",
            max(int(iterations), 0),
        )
        record_runtime_counter(
            "simulation.poisson_max_rolling_sums_stratified.n_minutes",
            max(n_minutes, 0),
        )
        record_runtime_counter(
            "simulation.poisson_max_rolling_sums_stratified.output_samples",
            int(maxima.size),
        )


def simulate_binomial_max_abs_delta(
    totals_per_minute: np.ndarray,
    baseline_probability: float,
    window: int,
    min_window_total: int,
    iterations: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Null maxima from Binomial baseline simulations for pro/con window swings."""
    started = perf_counter()
    maxima = np.array([], dtype=float)
    n_minutes = int(totals_per_minute.size)
    try:
        if iterations <= 0 or n_minutes == 0 or window > n_minutes:
            return np.array([], dtype=float)

        totals = totals_per_minute.astype(int)
        baseline = float(np.clip(baseline_probability, 1e-9, 1.0 - 1e-9))

        total_roll = rolling_sum(totals.astype(float), window)
        valid_mask = total_roll >= float(min_window_total)
        if not valid_mask.any():
            return np.array([], dtype=float)

        maxima = np.zeros(iterations, dtype=float)
        for idx in range(iterations):
            sim_pro = rng.binomial(n=totals, p=baseline).astype(float)
            pro_roll = rolling_sum(sim_pro, window)
            rates = np.divide(
                pro_roll,
                total_roll,
                out=np.zeros_like(pro_roll, dtype=float),
                where=total_roll > 0,
            )
            abs_delta = np.abs(rates[valid_mask] - baseline)
            maxima[idx] = float(abs_delta.max()) if abs_delta.size else 0.0
        return maxima
    finally:
        record_runtime_timing(
            "simulation.binomial_max_abs_delta",
            (perf_counter() - started) * 1000.0,
        )
        record_runtime_counter("simulation.binomial_max_abs_delta.calls", 1)
        record_runtime_counter(
            "simulation.binomial_max_abs_delta.iterations",
            max(int(iterations), 0),
        )
        record_runtime_counter(
            "simulation.binomial_max_abs_delta.n_minutes",
            max(n_minutes, 0),
        )
        record_runtime_counter(
            "simulation.binomial_max_abs_delta.output_samples",
            int(maxima.size),
        )


def simulate_binomial_max_abs_delta_hourly(
    totals_per_minute: np.ndarray,
    hourly_probabilities: np.ndarray,
    window: int,
    min_window_total: int,
    iterations: int,
    rng: np.random.Generator,
) -> tuple[np.ndarray, np.ndarray]:
    """Null maxima from Binomial simulation using hour-of-day specific probabilities."""
    return simulate_binomial_max_abs_delta_probability_series(
        totals_per_minute=totals_per_minute,
        probabilities_per_minute=hourly_probabilities,
        window=window,
        min_window_total=min_window_total,
        iterations=iterations,
        rng=rng,
    )


def simulate_binomial_max_abs_delta_probability_series(
    totals_per_minute: np.ndarray,
    probabilities_per_minute: np.ndarray,
    window: int,
    min_window_total: int,
    iterations: int,
    rng: np.random.Generator,
) -> tuple[np.ndarray, np.ndarray]:
    """Null maxima from Binomial simulation with per-minute expected probabilities."""
    started = perf_counter()
    maxima = np.array([], dtype=float)
    expected_rate = np.array([], dtype=float)
    n_minutes = int(totals_per_minute.size)
    try:
        if iterations <= 0 or n_minutes == 0 or window > n_minutes:
            return np.array([], dtype=float), np.array([], dtype=float)

        totals = totals_per_minute.astype(int)
        probs = np.clip(probabilities_per_minute.astype(float), 1e-9, 1.0 - 1e-9)

        total_roll = rolling_sum(totals.astype(float), window)
        valid_mask = total_roll >= float(min_window_total)
        if not valid_mask.any():
            return np.array([], dtype=float), np.array([], dtype=float)

        expected_pro = totals * probs
        expected_roll = rolling_sum(expected_pro, window)
        expected_rate = np.divide(
            expected_roll,
            total_roll,
            out=np.zeros_like(expected_roll, dtype=float),
            where=total_roll > 0,
        )

        maxima = np.zeros(iterations, dtype=float)
        for idx in range(iterations):
            sim_pro = rng.binomial(n=totals, p=probs).astype(float)
            pro_roll = rolling_sum(sim_pro, window)
            rates = np.divide(
                pro_roll,
                total_roll,
                out=np.zeros_like(pro_roll, dtype=float),
                where=total_roll > 0,
            )
            abs_delta = np.abs(rates[valid_mask] - expected_rate[valid_mask])
            maxima[idx] = float(abs_delta.max()) if abs_delta.size else 0.0

        return maxima, expected_rate
    finally:
        record_runtime_timing(
            "simulation.binomial_max_abs_delta_probability_series",
            (perf_counter() - started) * 1000.0,
        )
        record_runtime_counter(
            "simulation.binomial_max_abs_delta_probability_series.calls",
            1,
        )
        record_runtime_counter(
            "simulation.binomial_max_abs_delta_probability_series.iterations",
            max(int(iterations), 0),
        )
        record_runtime_counter(
            "simulation.binomial_max_abs_delta_probability_series.n_minutes",
            max(n_minutes, 0),
        )
        record_runtime_counter(
            "simulation.binomial_max_abs_delta_probability_series.output_samples",
            int(maxima.size),
        )
