from __future__ import annotations

import pandas as pd
from scipy.stats import kendalltau

from testifier_audit.detectors.base import Detector, DetectorResult


class SortednessDetector(Detector):
    name = "sortedness"
    DEFAULT_BUCKET_MINUTES = [1, 5, 15, 30, 60, 120, 240]

    def __init__(self, bucket_minutes: list[int] | None = None) -> None:
        buckets = bucket_minutes or self.DEFAULT_BUCKET_MINUTES
        self.bucket_minutes = sorted({int(value) for value in buckets if int(value) > 0})

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        if df.empty:
            empty = pd.DataFrame(columns=["metric", "value"])
            empty_bucket_ordering = pd.DataFrame(
                columns=[
                    "bucket_minutes",
                    "bucket_start",
                    "n_records",
                    "is_alphabetical",
                    "kendall_tau",
                    "kendall_p_value",
                    "abs_kendall_tau",
                ]
            )
            return DetectorResult(
                detector=self.name,
                summary={
                    "id_timestamp_corr": 0.0,
                    "alphabetical_minute_ratio": 0.0,
                    "max_bucket_abs_kendall_tau": 0.0,
                    "mean_bucket_abs_kendall_tau": 0.0,
                },
                tables={
                    "sortedness_metrics": empty,
                    "minute_ordering": pd.DataFrame(),
                    "bucket_ordering": empty_bucket_ordering,
                    "bucket_ordering_summary": pd.DataFrame(),
                },
            )

        working = df.copy()
        id_num = pd.to_numeric(working["id"], errors="coerce")

        timestamp_rank = working["timestamp"].rank(method="average")
        name_rank = (working["last"].fillna("") + "|" + working["first"].fillna("")).rank(
            method="average"
        )

        id_timestamp_corr = (
            float(id_num.corr(timestamp_rank, method="spearman"))
            if id_num.notna().sum() >= 2
            else 0.0
        )
        id_name_corr = (
            float(id_num.corr(name_rank, method="spearman")) if id_num.notna().sum() >= 2 else 0.0
        )

        timestamp_diff = working["timestamp"].diff()
        time_monotonic_breaks = int((timestamp_diff < pd.Timedelta(0)).sum())

        bucket_frames: list[pd.DataFrame] = []
        minute_bucket_series = pd.to_datetime(working["minute_bucket"], errors="coerce")
        for bucket_minutes in self.bucket_minutes:
            floor_rule = f"{int(bucket_minutes)}min"
            bucket_start = minute_bucket_series.dt.floor(floor_rule)
            grouped = (
                working.assign(bucket_start=bucket_start)
                .dropna(subset=["bucket_start"])
                .groupby("bucket_start", dropna=True, sort=True)
            )
            rows: list[dict[str, object]] = []
            for bucket_value, bucket_df in grouped:
                if len(bucket_df) < 3:
                    continue
                names = bucket_df["name_display"].fillna("").astype(str).tolist()
                alphabetical = names == sorted(names)
                working_bucket = bucket_df.copy()
                sort_columns = [
                    column
                    for column in ("timestamp", "id", "name_display")
                    if column in working_bucket.columns
                ]
                if sort_columns:
                    working_bucket = working_bucket.sort_values(sort_columns, kind="stable")
                alphabetical_rank = (
                    working_bucket["name_display"].fillna("").astype(str).rank(method="average")
                )
                arrival_order = pd.Series(
                    range(len(working_bucket)),
                    index=working_bucket.index,
                    dtype=float,
                )
                tau, tau_p_value = kendalltau(
                    arrival_order.to_numpy(dtype=float),
                    alphabetical_rank.to_numpy(dtype=float),
                )
                if pd.isna(tau):
                    tau = 0.0
                if pd.isna(tau_p_value):
                    tau_p_value = 1.0
                rows.append(
                    {
                        "bucket_minutes": int(bucket_minutes),
                        "bucket_start": bucket_value,
                        "n_records": int(len(bucket_df)),
                        "is_alphabetical": bool(alphabetical),
                        "kendall_tau": float(tau),
                        "kendall_p_value": float(tau_p_value),
                        "abs_kendall_tau": float(abs(float(tau))),
                    }
                )
            if rows:
                bucket_frames.append(pd.DataFrame(rows))

        bucket_ordering = (
            pd.concat(bucket_frames, ignore_index=True)
            if bucket_frames
            else pd.DataFrame(
                columns=[
                    "bucket_minutes",
                    "bucket_start",
                    "n_records",
                    "is_alphabetical",
                    "kendall_tau",
                    "kendall_p_value",
                    "abs_kendall_tau",
                ]
            )
        )
        bucket_ordering_summary = pd.DataFrame()
        if not bucket_ordering.empty:
            bucket_ordering_summary = (
                bucket_ordering.groupby("bucket_minutes", dropna=True)
                .agg(
                    n_buckets=("bucket_start", "count"),
                    avg_records_per_bucket=("n_records", "mean"),
                    alphabetical_ratio=("is_alphabetical", "mean"),
                    mean_kendall_tau=("kendall_tau", "mean"),
                    mean_abs_kendall_tau=("abs_kendall_tau", "mean"),
                    max_abs_kendall_tau=("abs_kendall_tau", "max"),
                    strong_ordering_ratio=(
                        "abs_kendall_tau",
                        lambda series: float(
                            (pd.to_numeric(series, errors="coerce").fillna(0.0) >= 0.8).mean()
                        ),
                    ),
                )
                .reset_index()
                .sort_values("bucket_minutes")
            )

        minute_ordering = bucket_ordering[bucket_ordering["bucket_minutes"] == 1].copy()
        if not minute_ordering.empty:
            minute_ordering = minute_ordering.rename(columns={"bucket_start": "minute_bucket"})[
                [
                    "minute_bucket",
                    "n_records",
                    "is_alphabetical",
                    "kendall_tau",
                    "kendall_p_value",
                    "abs_kendall_tau",
                ]
            ]

        alphabetical_ratio = (
            float(minute_ordering["is_alphabetical"].mean()) if not minute_ordering.empty else 0.0
        )
        max_bucket_alphabetical_ratio = (
            float(bucket_ordering_summary["alphabetical_ratio"].max())
            if not bucket_ordering_summary.empty
            else 0.0
        )
        max_bucket_abs_kendall_tau = (
            float(bucket_ordering_summary["max_abs_kendall_tau"].max())
            if not bucket_ordering_summary.empty
            else 0.0
        )
        mean_bucket_abs_kendall_tau = (
            float(bucket_ordering_summary["mean_abs_kendall_tau"].mean())
            if not bucket_ordering_summary.empty
            else 0.0
        )

        metrics = pd.DataFrame(
            [
                {"metric": "rows", "value": float(len(working))},
                {"metric": "id_timestamp_corr", "value": id_timestamp_corr},
                {"metric": "id_name_corr", "value": id_name_corr},
                {"metric": "time_monotonic_breaks", "value": float(time_monotonic_breaks)},
                {"metric": "alphabetical_minute_ratio", "value": alphabetical_ratio},
                {"metric": "max_bucket_alphabetical_ratio", "value": max_bucket_alphabetical_ratio},
                {"metric": "max_bucket_abs_kendall_tau", "value": max_bucket_abs_kendall_tau},
                {"metric": "mean_bucket_abs_kendall_tau", "value": mean_bucket_abs_kendall_tau},
            ]
        )

        return DetectorResult(
            detector=self.name,
            summary={
                "id_timestamp_corr": id_timestamp_corr,
                "alphabetical_minute_ratio": alphabetical_ratio,
                "max_bucket_alphabetical_ratio": max_bucket_alphabetical_ratio,
                "max_bucket_abs_kendall_tau": max_bucket_abs_kendall_tau,
                "mean_bucket_abs_kendall_tau": mean_bucket_abs_kendall_tau,
                "time_monotonic_breaks": time_monotonic_breaks,
            },
            tables={
                "sortedness_metrics": metrics,
                "minute_ordering": minute_ordering,
                "bucket_ordering": bucket_ordering,
                "bucket_ordering_summary": bucket_ordering_summary,
            },
        )
