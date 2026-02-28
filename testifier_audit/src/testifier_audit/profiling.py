from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from time import perf_counter
from typing import Any
import math


@dataclass
class _TimingStat:
    calls: int = 0
    total_ms: float = 0.0
    min_ms: float = float("inf")
    max_ms: float = 0.0

    def add(self, elapsed_ms: float) -> None:
        value = float(elapsed_ms)
        if not math.isfinite(value):
            return
        value = max(value, 0.0)
        self.calls += 1
        self.total_ms += value
        self.min_ms = min(self.min_ms, value)
        self.max_ms = max(self.max_ms, value)

    def to_dict(self) -> dict[str, float | int]:
        if self.calls <= 0:
            return {
                "calls": 0,
                "total_ms": 0.0,
                "avg_ms": 0.0,
                "min_ms": 0.0,
                "max_ms": 0.0,
            }
        total_ms = round(float(self.total_ms), 3)
        avg_ms = round(float(self.total_ms / float(self.calls)), 3)
        min_ms = round(float(self.min_ms), 3) if math.isfinite(self.min_ms) else 0.0
        max_ms = round(float(self.max_ms), 3)
        return {
            "calls": int(self.calls),
            "total_ms": total_ms,
            "avg_ms": avg_ms,
            "min_ms": min_ms,
            "max_ms": max_ms,
        }


class RuntimeProfiler:
    def __init__(self) -> None:
        self._timings: dict[str, _TimingStat] = {}
        self._counters: dict[str, float] = {}

    def add_timing(self, key: str, elapsed_ms: float) -> None:
        normalized = str(key or "").strip()
        if not normalized:
            return
        stat = self._timings.get(normalized)
        if stat is None:
            stat = _TimingStat()
            self._timings[normalized] = stat
        stat.add(elapsed_ms)

    def add_counter(self, key: str, value: float = 1.0) -> None:
        normalized = str(key or "").strip()
        if not normalized:
            return
        numeric = float(value)
        if not math.isfinite(numeric):
            return
        self._counters[normalized] = float(self._counters.get(normalized, 0.0) + numeric)

    def to_dict(self) -> dict[str, Any]:
        timings = {key: stat.to_dict() for key, stat in sorted(self._timings.items())}
        counters = {
            key: int(value)
            if float(value).is_integer()
            else round(float(value), 6)
            for key, value in sorted(self._counters.items())
        }
        total_timed_ms = round(float(sum(stat.total_ms for stat in self._timings.values())), 3)
        return {
            "timings": timings,
            "counters": counters,
            "total_timed_ms": total_timed_ms,
            "timed_event_count": int(sum(stat.calls for stat in self._timings.values())),
        }


_ACTIVE_RUNTIME_PROFILER: ContextVar[RuntimeProfiler | None] = ContextVar(
    "testifier_audit_active_runtime_profiler",
    default=None,
)


def get_active_runtime_profiler() -> RuntimeProfiler | None:
    return _ACTIVE_RUNTIME_PROFILER.get()


@contextmanager
def activate_runtime_profiler(profiler: RuntimeProfiler):
    token = _ACTIVE_RUNTIME_PROFILER.set(profiler)
    try:
        yield profiler
    finally:
        _ACTIVE_RUNTIME_PROFILER.reset(token)


def record_runtime_timing(key: str, elapsed_ms: float) -> None:
    profiler = get_active_runtime_profiler()
    if profiler is None:
        return
    profiler.add_timing(key=key, elapsed_ms=elapsed_ms)


def record_runtime_counter(key: str, value: float = 1.0) -> None:
    profiler = get_active_runtime_profiler()
    if profiler is None:
        return
    profiler.add_counter(key=key, value=value)


@contextmanager
def profile_runtime_block(key: str):
    started = perf_counter()
    try:
        yield
    finally:
        record_runtime_timing(key=key, elapsed_ms=(perf_counter() - started) * 1000.0)
