from __future__ import annotations

import pandas as pd

from testifier_audit.detectors.base import Detector, DetectorResult


class SortednessDetector(Detector):
    name = "sortedness"

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        if df.empty:
            empty = pd.DataFrame(columns=["metric", "value"])
            return DetectorResult(
                detector=self.name,
                summary={"id_timestamp_corr": 0.0, "alphabetical_minute_ratio": 0.0},
                tables={"sortedness_metrics": empty, "minute_ordering": pd.DataFrame()},
            )

        working = df.copy()
        id_num = pd.to_numeric(working["id"], errors="coerce")

        timestamp_rank = working["timestamp"].rank(method="average")
        name_rank = (working["last"].fillna("") + "|" + working["first"].fillna("")).rank(method="average")

        id_timestamp_corr = (
            float(id_num.corr(timestamp_rank, method="spearman")) if id_num.notna().sum() >= 2 else 0.0
        )
        id_name_corr = float(id_num.corr(name_rank, method="spearman")) if id_num.notna().sum() >= 2 else 0.0

        timestamp_diff = working["timestamp"].diff()
        time_monotonic_breaks = int((timestamp_diff < pd.Timedelta(0)).sum())

        minute_rows: list[dict[str, object]] = []
        minute_groups = working.groupby("minute_bucket", dropna=True)
        for minute, minute_df in minute_groups:
            if len(minute_df) < 3:
                continue
            names = minute_df["name_display"].fillna("").astype(str).tolist()
            alphabetical = names == sorted(names)
            minute_rows.append(
                {
                    "minute_bucket": minute,
                    "n_records": int(len(minute_df)),
                    "is_alphabetical": bool(alphabetical),
                }
            )

        minute_ordering = pd.DataFrame(minute_rows)
        alphabetical_ratio = (
            float(minute_ordering["is_alphabetical"].mean()) if not minute_ordering.empty else 0.0
        )

        metrics = pd.DataFrame(
            [
                {"metric": "rows", "value": float(len(working))},
                {"metric": "id_timestamp_corr", "value": id_timestamp_corr},
                {"metric": "id_name_corr", "value": id_name_corr},
                {"metric": "time_monotonic_breaks", "value": float(time_monotonic_breaks)},
                {"metric": "alphabetical_minute_ratio", "value": alphabetical_ratio},
            ]
        )

        return DetectorResult(
            detector=self.name,
            summary={
                "id_timestamp_corr": id_timestamp_corr,
                "alphabetical_minute_ratio": alphabetical_ratio,
                "time_monotonic_breaks": time_monotonic_breaks,
            },
            tables={
                "sortedness_metrics": metrics,
                "minute_ordering": minute_ordering,
            },
        )
