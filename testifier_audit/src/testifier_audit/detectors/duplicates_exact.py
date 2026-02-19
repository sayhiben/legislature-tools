from __future__ import annotations

import pandas as pd

from testifier_audit.detectors.base import Detector, DetectorResult


class DuplicatesExactDetector(Detector):
    name = "duplicates_exact"

    def __init__(self, top_n: int) -> None:
        self.top_n = top_n

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

        repeated = grouped[grouped["n"] > 1].sort_values(["n", "canonical_name"], ascending=[False, True])

        same_minute = (
            df.groupby(["canonical_name", "minute_bucket"], dropna=True)
            .agg(n=("id", "count"), n_pro=("position_normalized", lambda s: int((s == "Pro").sum())))
            .reset_index()
        )
        same_minute = same_minute[same_minute["n"] > 1].sort_values("n", ascending=False)

        switching = repeated[(repeated["n_pro"] > 0) & (repeated["n_con"] > 0)].copy()
        switching = switching.sort_values("n", ascending=False)

        summary = {
            "n_repeated_names": int(len(repeated)),
            "n_same_minute_instances": int(len(same_minute)),
            "n_position_switching_names": int(len(switching)),
            "max_repeat_count": int(repeated["n"].max()) if not repeated.empty else 0,
        }

        return DetectorResult(
            detector=self.name,
            summary=summary,
            tables={
                "top_repeated_names": repeated.head(self.top_n),
                "repeated_same_minute": same_minute,
                "position_switching_names": switching,
            },
        )
