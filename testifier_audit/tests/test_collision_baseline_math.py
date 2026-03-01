from __future__ import annotations

import math

import numpy as np
import pandas as pd

from testifier_audit.names import collision_baseline as collision_baseline_module
from testifier_audit.names.collision_baseline import (
    expected_collision_metrics,
    expected_collision_metrics_from_probabilities,
    histogram_from_name_counts,
    histogram_from_probabilities,
    simulate_collision_null_from_histogram,
    summarize_collision_observed_vs_null,
)
from testifier_audit.profiling import RuntimeProfiler, activate_runtime_profiler


def _hist_from_counts(values: list[int]) -> pd.DataFrame:
    return histogram_from_name_counts(pd.Series(values, dtype=int))


def test_expected_collision_metrics_are_zero_for_n_zero_and_one() -> None:
    histogram = _hist_from_counts([1, 1, 2, 3, 5, 8, 13])
    for baseline_model in ("multinomial", "hypergeometric"):
        for n_rows in (0, 1):
            expected = expected_collision_metrics(
                n_rows=n_rows,
                histogram=histogram,
                baseline_model=baseline_model,
            )
            assert expected["pairs"] == 0.0
            assert expected["excess_rows"] == 0.0
            assert expected["repeated_group_rows"] == 0.0


def test_expected_collision_metrics_respect_bounds() -> None:
    histogram = _hist_from_counts([1, 1, 1, 2, 2, 4, 7, 10])
    for baseline_model, n_rows in (("multinomial", 50), ("hypergeometric", 20)):
        expected = expected_collision_metrics(
            n_rows=n_rows,
            histogram=histogram,
            baseline_model=baseline_model,
        )
        assert 0.0 <= expected["pairs"]
        assert 0.0 <= expected["excess_rows"] <= max(n_rows - 1, 0)
        assert 0.0 <= expected["repeated_group_rows"] <= n_rows


def test_expected_pairs_scale_with_choose_n_2() -> None:
    histogram = _hist_from_counts([50, 30, 20, 10, 5])
    expected_10 = expected_collision_metrics(
        n_rows=10,
        histogram=histogram,
        baseline_model="multinomial",
    )
    expected_20 = expected_collision_metrics(
        n_rows=20,
        histogram=histogram,
        baseline_model="multinomial",
    )
    ratio = expected_20["pairs"] / expected_10["pairs"]
    target = math.comb(20, 2) / math.comb(10, 2)
    assert np.isclose(ratio, target, rtol=1e-12, atol=0.0)


def test_expected_repeated_group_rows_is_nonlinear_in_n() -> None:
    histogram = _hist_from_counts([500, 300] + [1] * 2000)
    expected_500 = expected_collision_metrics(
        n_rows=500,
        histogram=histogram,
        baseline_model="multinomial",
    )["repeated_group_rows"]
    expected_1000 = expected_collision_metrics(
        n_rows=1000,
        histogram=histogram,
        baseline_model="multinomial",
    )["repeated_group_rows"]
    assert not np.isclose(expected_1000, 2.0 * expected_500, rtol=1e-3, atol=1e-3)


def test_observed_only_baseline_inflates_collision_expectations() -> None:
    full_population_hist = _hist_from_counts([500, 300] + [1] * 2000)
    observed_only_hist = _hist_from_counts([500, 300])
    n_rows = 1000

    expected_full = expected_collision_metrics(
        n_rows=n_rows,
        histogram=full_population_hist,
        baseline_model="multinomial",
    )
    expected_observed_only = expected_collision_metrics(
        n_rows=n_rows,
        histogram=observed_only_hist,
        baseline_model="multinomial",
    )

    assert expected_observed_only["pairs"] > expected_full["pairs"]
    assert expected_observed_only["repeated_group_rows"] > expected_full["repeated_group_rows"]
    assert expected_observed_only["excess_rows"] > expected_full["excess_rows"]


def test_analytic_expectations_match_multinomial_monte_carlo() -> None:
    histogram = _hist_from_counts([80, 40, 25, 15, 10, 5, 3, 2, 1, 1, 1])
    n_rows = 120
    expected = expected_collision_metrics(
        n_rows=n_rows,
        histogram=histogram,
        baseline_model="multinomial",
    )
    samples = simulate_collision_null_from_histogram(
        n_rows=n_rows,
        histogram=histogram,
        draws=1500,
        max_draws=1500,
        rng=np.random.default_rng(123),
        baseline_model="multinomial",
    )
    assert not samples.empty

    for metric in ("pairs", "excess_rows", "repeated_group_rows"):
        mean_value = float(samples[metric].mean())
        expected_value = float(expected[metric])
        relative_error = abs(mean_value - expected_value) / max(expected_value, 1.0)
        assert relative_error < 0.20


def test_expected_collision_metrics_from_probabilities_respects_n_one_invariants() -> None:
    probabilities = np.asarray([0.7, 0.2, 0.1], dtype=float)
    assert expected_collision_metrics_from_probabilities(n_rows=0, probabilities=probabilities) == {
        "pairs": 0.0,
        "excess_rows": 0.0,
        "repeated_group_rows": 0.0,
    }
    assert expected_collision_metrics_from_probabilities(n_rows=1, probabilities=probabilities) == {
        "pairs": 0.0,
        "excess_rows": 0.0,
        "repeated_group_rows": 0.0,
    }


def test_histogram_from_probabilities_builds_nonempty_distribution() -> None:
    probabilities = np.asarray([0.5, 0.25, 0.25], dtype=float)
    histogram = histogram_from_probabilities(probabilities=probabilities, n_population=1000)
    assert not histogram.empty
    assert set(histogram.columns) == {"name_count", "n_keys", "N"}
    assert int(histogram["N"].iloc[0]) > 0


def test_collision_simulation_emits_runtime_profile_events() -> None:
    histogram = _hist_from_counts([50, 40, 20, 10, 5, 2, 1, 1])
    profiler = RuntimeProfiler()
    with activate_runtime_profiler(profiler):
        samples = simulate_collision_null_from_histogram(
            n_rows=120,
            histogram=histogram,
            draws=25,
            max_draws=25,
            rng=np.random.default_rng(2026),
            baseline_model="multinomial",
        )
    assert not samples.empty

    profile = profiler.to_dict()
    timings = profile["timings"]
    counters = profile["counters"]
    assert "simulation.collision_null_from_histogram" in timings
    assert timings["simulation.collision_null_from_histogram"]["calls"] == 1
    assert counters["simulation.collision_null_from_histogram.draws_effective"] == 25


def test_collision_simulation_precision_target_can_stop_before_draw_cap() -> None:
    histogram = _hist_from_counts([120, 80, 60, 40, 20, 10, 5, 2, 1, 1])
    samples = simulate_collision_null_from_histogram(
        n_rows=120,
        histogram=histogram,
        draws=2000,
        max_draws=2000,
        min_draws=40,
        target_p_mcse=0.005,
        decision_p_threshold=0.05,
        rng=np.random.default_rng(44),
        baseline_model="multinomial",
        tail_observed={
            "pairs": 1e9,
            "excess_rows": 1e9,
            "repeated_group_rows": 1e9,
        },
    )
    assert not samples.empty
    assert 40 <= len(samples) < 2000


def test_summarize_collision_observed_vs_null_emits_precision_metadata() -> None:
    null_samples = pd.DataFrame(
        {
            "pairs": [0.0, 0.0, 1.0, 2.0, 3.0],
            "excess_rows": [0.0, 1.0, 1.0, 2.0, 2.0],
            "repeated_group_rows": [0.0, 1.0, 2.0, 2.0, 3.0],
        }
    )
    summary = summarize_collision_observed_vs_null(
        observed={"pairs": 2.0, "excess_rows": 2.0, "repeated_group_rows": 2.0},
        expected={"pairs": 1.0, "excess_rows": 1.0, "repeated_group_rows": 1.0},
        null_samples=null_samples,
    )
    assert not summary.empty
    for column in (
        "monte_carlo_draws_effective",
        "monte_carlo_quantile_resolution",
        "monte_carlo_p_value_mcse",
        "monte_carlo_p_value_ci_low",
        "monte_carlo_p_value_ci_high",
    ):
        assert column in summary.columns
    assert (summary["monte_carlo_draws_effective"].astype(int) == len(null_samples)).all()


def test_collision_simulation_heavy_calls_use_parallel_executor(
    monkeypatch,
) -> None:
    class _FakeProcessPoolExecutor:
        entered = 0
        exited = 0
        map_calls = 0

        def __init__(self, *, max_workers: int):
            self.max_workers = int(max_workers)

        def __enter__(self):
            _FakeProcessPoolExecutor.entered += 1
            return self

        def __exit__(self, exc_type, exc, tb):
            _FakeProcessPoolExecutor.exited += 1
            return False

        def map(self, fn, iterable):
            _FakeProcessPoolExecutor.map_calls += 1
            return [fn(item) for item in iterable]

    monkeypatch.setattr(collision_baseline_module, "ProcessPoolExecutor", _FakeProcessPoolExecutor)
    monkeypatch.setattr(collision_baseline_module.os, "cpu_count", lambda: 4)
    monkeypatch.setattr(collision_baseline_module, "_COLLISION_NULL_PARALLEL_MIN_DRAWS", 8)
    monkeypatch.setattr(collision_baseline_module, "_COLLISION_NULL_PARALLEL_MIN_ROWS", 2)
    monkeypatch.setattr(collision_baseline_module, "_COLLISION_NULL_PARALLEL_CHUNK_SIZE", 4)
    monkeypatch.setattr(collision_baseline_module, "_COLLISION_NULL_PARALLEL_MAX_WORKERS", 4)

    histogram = _hist_from_counts([120, 80, 40, 20, 10, 5, 2, 1, 1])
    samples = simulate_collision_null_from_histogram(
        n_rows=100,
        histogram=histogram,
        draws=16,
        max_draws=16,
        rng=np.random.default_rng(1234),
        baseline_model="multinomial",
    )

    assert len(samples) == 16
    assert _FakeProcessPoolExecutor.entered == 1
    assert _FakeProcessPoolExecutor.exited == 1
    assert _FakeProcessPoolExecutor.map_calls == 1


def test_collision_simulation_parallel_and_single_worker_are_deterministic(
    monkeypatch,
) -> None:
    class _FakeProcessPoolExecutor:
        def __init__(self, *, max_workers: int):
            self.max_workers = int(max_workers)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def map(self, fn, iterable):
            return [fn(item) for item in iterable]

    monkeypatch.setattr(collision_baseline_module, "ProcessPoolExecutor", _FakeProcessPoolExecutor)
    monkeypatch.setattr(collision_baseline_module.os, "cpu_count", lambda: 4)
    monkeypatch.setattr(collision_baseline_module, "_COLLISION_NULL_PARALLEL_MIN_DRAWS", 8)
    monkeypatch.setattr(collision_baseline_module, "_COLLISION_NULL_PARALLEL_MIN_ROWS", 2)
    monkeypatch.setattr(collision_baseline_module, "_COLLISION_NULL_PARALLEL_CHUNK_SIZE", 4)

    histogram = _hist_from_counts([90, 60, 30, 15, 8, 3, 2, 1, 1])

    monkeypatch.setattr(collision_baseline_module, "_COLLISION_NULL_PARALLEL_MAX_WORKERS", 1)
    serial_samples = simulate_collision_null_from_histogram(
        n_rows=80,
        histogram=histogram,
        draws=24,
        max_draws=24,
        rng=np.random.default_rng(777),
        baseline_model="multinomial",
    )

    monkeypatch.setattr(collision_baseline_module, "_COLLISION_NULL_PARALLEL_MAX_WORKERS", 4)
    parallel_samples = simulate_collision_null_from_histogram(
        n_rows=80,
        histogram=histogram,
        draws=24,
        max_draws=24,
        rng=np.random.default_rng(777),
        baseline_model="multinomial",
    )

    pd.testing.assert_frame_equal(serial_samples, parallel_samples, check_exact=True)
