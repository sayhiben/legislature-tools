from __future__ import annotations

from testifier_audit.io import http_rate_limit


def test_wait_for_global_http_slot_enforces_interval(monkeypatch) -> None:
    http_rate_limit._reset_global_http_rate_limiter_for_tests()

    clock = {"now": 0.0}
    sleep_calls: list[float] = []

    def _fake_monotonic() -> float:
        return float(clock["now"])

    def _fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        clock["now"] = float(clock["now"]) + float(seconds)

    monkeypatch.setattr(http_rate_limit.time, "monotonic", _fake_monotonic)
    monkeypatch.setattr(http_rate_limit.time, "sleep", _fake_sleep)

    http_rate_limit.wait_for_global_http_slot(min_interval_seconds=1.0)
    clock["now"] = 0.25
    http_rate_limit.wait_for_global_http_slot(min_interval_seconds=1.0)

    assert sleep_calls == [0.75]


def test_wait_for_global_http_slot_noop_when_interval_zero(monkeypatch) -> None:
    http_rate_limit._reset_global_http_rate_limiter_for_tests()

    sleep_calls: list[float] = []
    monkeypatch.setattr(http_rate_limit.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    http_rate_limit.wait_for_global_http_slot(min_interval_seconds=0.0)
    assert sleep_calls == []
