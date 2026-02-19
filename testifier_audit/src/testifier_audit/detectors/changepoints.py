from __future__ import annotations

import numpy as np
import pandas as pd

from testifier_audit.detectors.base import Detector, DetectorResult

try:  # pragma: no cover
    import ruptures as rpt
except ImportError:  # pragma: no cover
    rpt = None


def _detect_with_ruptures(values: np.ndarray, min_segment: int, penalty_scale: float) -> list[int]:
    if rpt is None:
        return []
    if values.size < (min_segment * 2):
        return []
    if np.allclose(values, values[0]):
        return []

    signal = values.reshape(-1, 1)
    variance = float(np.var(values))
    penalty = penalty_scale * max(variance, 1.0) * np.log(values.size)

    algo = rpt.Pelt(model="l2", min_size=min_segment).fit(signal)
    breakpoints = algo.predict(pen=penalty)
    return [int(idx) for idx in breakpoints[:-1] if 0 < idx < values.size]


def _detect_fallback(values: np.ndarray, min_segment: int) -> list[int]:
    if values.size < (min_segment * 2):
        return []
    if np.allclose(values, values[0]):
        return []

    scores = np.zeros(values.size, dtype=float)
    for idx in range(min_segment, values.size - min_segment):
        left = values[idx - min_segment : idx]
        right = values[idx : idx + min_segment]
        scores[idx] = abs(float(right.mean() - left.mean()))

    threshold = float(scores.mean() + (2.5 * scores.std()))
    candidate_indices = np.where(scores >= threshold)[0].tolist()

    selected: list[int] = []
    for idx in candidate_indices:
        if not selected or (idx - selected[-1]) >= min_segment:
            selected.append(int(idx))
    return selected


def _build_changepoint_table(
    values: np.ndarray,
    minutes: np.ndarray,
    indices: list[int],
    metric: str,
    context_window: int,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    total = values.size
    for idx in indices:
        left_start = max(0, idx - context_window)
        right_end = min(total, idx + context_window)
        left = values[left_start:idx]
        right = values[idx:right_end]
        if left.size == 0 or right.size == 0:
            continue

        mean_before = float(left.mean())
        mean_after = float(right.mean())
        rows.append(
            {
                "metric": metric,
                "change_index": int(idx),
                "change_minute": minutes[idx],
                "mean_before": mean_before,
                "mean_after": mean_after,
                "delta": mean_after - mean_before,
                "abs_delta": abs(mean_after - mean_before),
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "metric",
                "change_index",
                "change_minute",
                "mean_before",
                "mean_after",
                "delta",
                "abs_delta",
            ]
        )
    return pd.DataFrame(rows).sort_values("change_minute")


class ChangePointsDetector(Detector):
    name = "changepoints"

    def __init__(self, min_segment_minutes: int, penalty_scale: float) -> None:
        self.min_segment_minutes = min_segment_minutes
        self.penalty_scale = penalty_scale

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        counts = features.get("counts_per_minute", pd.DataFrame())
        if counts.empty:
            empty = pd.DataFrame()
            return DetectorResult(
                detector=self.name,
                summary={
                    "n_volume_changepoints": 0,
                    "n_pro_rate_changepoints": 0,
                    "method": "none",
                },
                tables={
                    "volume_changepoints": empty,
                    "pro_rate_changepoints": empty,
                    "all_changepoints": empty,
                },
            )

        minute_bucket = counts["minute_bucket"].to_numpy()
        volume = counts["n_total"].astype(float).to_numpy()

        baseline_pro_rate = float(counts["n_pro"].sum() / max(counts["n_total"].sum(), 1.0))
        pro_rate = counts["pro_rate"].fillna(baseline_pro_rate).astype(float).to_numpy()

        volume_indices = _detect_with_ruptures(volume, self.min_segment_minutes, self.penalty_scale)
        pro_indices = _detect_with_ruptures(pro_rate, self.min_segment_minutes, self.penalty_scale)

        method = "ruptures" if rpt is not None else "fallback"
        if not volume_indices:
            volume_indices = _detect_fallback(volume, self.min_segment_minutes)
            if method == "ruptures":
                method = "ruptures+fallback"
        if not pro_indices:
            pro_indices = _detect_fallback(pro_rate, self.min_segment_minutes)
            if method == "ruptures":
                method = "ruptures+fallback"

        volume_table = _build_changepoint_table(
            volume,
            minute_bucket,
            volume_indices,
            metric="n_total",
            context_window=self.min_segment_minutes,
        )
        pro_table = _build_changepoint_table(
            pro_rate,
            minute_bucket,
            pro_indices,
            metric="pro_rate",
            context_window=self.min_segment_minutes,
        )

        all_changes = pd.concat([volume_table, pro_table], ignore_index=True)
        if not all_changes.empty:
            all_changes = all_changes.sort_values(["change_minute", "metric"]) 

        summary = {
            "n_volume_changepoints": int(len(volume_table)),
            "n_pro_rate_changepoints": int(len(pro_table)),
            "max_volume_abs_delta": float(volume_table["abs_delta"].max()) if not volume_table.empty else 0.0,
            "max_pro_rate_abs_delta": float(pro_table["abs_delta"].max()) if not pro_table.empty else 0.0,
            "method": method,
        }

        return DetectorResult(
            detector=self.name,
            summary=summary,
            tables={
                "volume_changepoints": volume_table,
                "pro_rate_changepoints": pro_table,
                "all_changepoints": all_changes,
            },
        )
