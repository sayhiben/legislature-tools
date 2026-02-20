from __future__ import annotations

import pandas as pd

from testifier_audit.detectors.base import Detector, DetectorResult


class DuplicatesExactDetector(Detector):
    name = "duplicates_exact"
    DEFAULT_BUCKET_MINUTES = [1, 5, 15, 30, 60, 120, 240]

    def __init__(self, top_n: int, bucket_minutes: list[int] | None = None) -> None:
        self.top_n = top_n
        buckets = bucket_minutes or self.DEFAULT_BUCKET_MINUTES
        self.bucket_minutes = sorted({int(value) for value in buckets if int(value) > 0})

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        grouped = (
            df.groupby("canonical_name", dropna=True)
            .agg(
                n=("id", "count"),
                n_pro=("position_normalized", lambda s: int((s == "Pro").sum())),
                n_con=("position_normalized", lambda s: int((s == "Con").sum())),
                first_seen=("timestamp", "min"),
                last_seen=("timestamp", "max"),
                display_name=("name_display", "first"),
            )
            .reset_index()
        )
        grouped["time_span_minutes"] = (
            (grouped["last_seen"] - grouped["first_seen"]).dt.total_seconds() / 60.0
        ).fillna(0.0)

        repeated = grouped[grouped["n"] > 1].sort_values(
            ["n", "canonical_name"], ascending=[False, True]
        )

        repeated_same_bucket_frames: list[pd.DataFrame] = []
        minute_series = pd.to_datetime(df["minute_bucket"], errors="coerce")

        for bucket_minutes in self.bucket_minutes:
            floor_rule = f"{int(bucket_minutes)}min"
            bucket_start = minute_series.dt.floor(floor_rule)
            grouped_bucket = (
                df.assign(bucket_start=bucket_start)
                .dropna(subset=["bucket_start"])
                .groupby(["canonical_name", "bucket_start"], dropna=True)
                .agg(
                    n=("id", "count"),
                    n_pro=("position_normalized", lambda s: int((s == "Pro").sum())),
                    n_con=("position_normalized", lambda s: int((s == "Con").sum())),
                    n_unknown=("position_normalized", lambda s: int((s == "Unknown").sum())),
                )
                .reset_index()
            )
            grouped_bucket = grouped_bucket[grouped_bucket["n"] > 1]
            if grouped_bucket.empty:
                continue

            grouped_bucket["bucket_minutes"] = int(bucket_minutes)
            grouped_bucket["bucket_end"] = grouped_bucket["bucket_start"] + pd.Timedelta(
                minutes=int(bucket_minutes) - 1,
            )
            repeated_same_bucket_frames.append(grouped_bucket)

        repeated_same_bucket = (
            pd.concat(repeated_same_bucket_frames, ignore_index=True)
            if repeated_same_bucket_frames
            else pd.DataFrame(
                columns=[
                    "canonical_name",
                    "bucket_start",
                    "n",
                    "n_pro",
                    "n_con",
                    "n_unknown",
                    "bucket_minutes",
                    "bucket_end",
                ]
            )
        )
        if not repeated_same_bucket.empty:
            repeated_same_bucket = repeated_same_bucket.sort_values(
                ["bucket_minutes", "n", "canonical_name", "bucket_start"],
                ascending=[True, False, True, True],
            )

        same_minute = repeated_same_bucket[repeated_same_bucket["bucket_minutes"] == 1].copy()
        if same_minute.empty:
            same_minute = pd.DataFrame(
                columns=["canonical_name", "minute_bucket", "n", "n_pro", "n_con", "n_unknown"]
            )
        else:
            same_minute = same_minute.rename(columns={"bucket_start": "minute_bucket"})[
                ["canonical_name", "minute_bucket", "n", "n_pro", "n_con", "n_unknown"]
            ].sort_values("n", ascending=False)

        switching = repeated[(repeated["n_pro"] > 0) & (repeated["n_con"] > 0)].copy()
        switching = switching.sort_values("n", ascending=False)

        bucket_summary = pd.DataFrame()
        if not repeated_same_bucket.empty:
            bucket_summary = (
                repeated_same_bucket.groupby("bucket_minutes", dropna=True)
                .agg(
                    n_duplicate_instances=("canonical_name", "count"),
                    n_distinct_names=("canonical_name", "nunique"),
                    max_repeat_count=("n", "max"),
                    avg_repeat_count=("n", "mean"),
                )
                .reset_index()
                .sort_values("bucket_minutes")
            )

        summary = {
            "n_repeated_names": int(len(repeated)),
            "n_same_minute_instances": int(len(same_minute)),
            "n_same_bucket_instances": int(len(repeated_same_bucket)),
            "n_position_switching_names": int(len(switching)),
            "max_repeat_count": int(repeated["n"].max()) if not repeated.empty else 0,
            "max_same_bucket_repeat_count": (
                int(repeated_same_bucket["n"].max()) if not repeated_same_bucket.empty else 0
            ),
        }

        return DetectorResult(
            detector=self.name,
            summary=summary,
            tables={
                "top_repeated_names": repeated.head(self.top_n),
                "repeated_same_minute": same_minute,
                "repeated_same_bucket": repeated_same_bucket,
                "repeated_same_bucket_summary": bucket_summary,
                "position_switching_names": switching,
            },
        )
