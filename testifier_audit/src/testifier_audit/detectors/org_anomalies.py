from __future__ import annotations

import pandas as pd

from testifier_audit.detectors.base import Detector, DetectorResult
from testifier_audit.proportion_stats import (
    DEFAULT_LOW_POWER_MIN_TOTAL,
    low_power_mask,
    wilson_half_width,
    wilson_interval,
)

BUCKET_MINUTES = (1, 5, 15, 30, 60, 120, 240)


def _organization_blank_rate_tables(
    working: pd.DataFrame,
    bucket_minutes: tuple[int, ...] = BUCKET_MINUTES,
    low_power_min_total: int = DEFAULT_LOW_POWER_MIN_TOTAL,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    by_bucket_frames: list[pd.DataFrame] = []
    by_bucket_position_frames: list[pd.DataFrame] = []

    for bucket in bucket_minutes:
        bucket_frame = working.copy()
        bucket_frame["bucket_start"] = pd.to_datetime(
            bucket_frame["minute_bucket"],
            errors="coerce",
        ).dt.floor(f"{bucket}min")
        bucket_frame = bucket_frame.dropna(subset=["bucket_start"])
        if bucket_frame.empty:
            continue

        by_position = (
            bucket_frame.groupby(["bucket_start", "position_normalized"], dropna=False)
            .agg(
                n_total=("id", "count"),
                n_blank_org=("organization_is_blank", "sum"),
            )
            .reset_index()
            .sort_values(["bucket_start", "position_normalized"])
        )
        by_position["n_nonblank_org"] = by_position["n_total"] - by_position["n_blank_org"]
        by_position["blank_org_rate"] = (by_position["n_blank_org"] / by_position["n_total"]).where(
            by_position["n_total"] > 0
        )
        by_position["blank_org_rate_wilson_low"], by_position["blank_org_rate_wilson_high"] = (
            wilson_interval(
                successes=by_position["n_blank_org"],
                totals=by_position["n_total"],
            )
        )
        by_position["blank_org_rate_wilson_half_width"] = wilson_half_width(
            successes=by_position["n_blank_org"],
            totals=by_position["n_total"],
        )
        by_position["is_low_power"] = low_power_mask(
            totals=by_position["n_total"],
            min_total=low_power_min_total,
        )
        by_position["bucket_minutes"] = int(bucket)
        by_bucket_position_frames.append(by_position)

        total = (
            bucket_frame.groupby("bucket_start", dropna=False)
            .agg(
                n_total=("id", "count"),
                n_blank_org=("organization_is_blank", "sum"),
            )
            .reset_index()
            .sort_values("bucket_start")
        )
        total["n_nonblank_org"] = total["n_total"] - total["n_blank_org"]
        total["blank_org_rate"] = (total["n_blank_org"] / total["n_total"]).where(
            total["n_total"] > 0
        )

        pivot_total = by_position.pivot(
            index="bucket_start",
            columns="position_normalized",
            values="n_total",
        )
        pivot_blank = by_position.pivot(
            index="bucket_start",
            columns="position_normalized",
            values="n_blank_org",
        )
        pivot_rate = by_position.pivot(
            index="bucket_start",
            columns="position_normalized",
            values="blank_org_rate",
        )

        total = (
            total.merge(
                pivot_total.rename(
                    columns={
                        "Pro": "n_pro",
                        "Con": "n_con",
                        "Unknown": "n_unknown",
                    }
                ),
                on="bucket_start",
                how="left",
            )
            .merge(
                pivot_blank.rename(
                    columns={
                        "Pro": "n_blank_org_pro",
                        "Con": "n_blank_org_con",
                        "Unknown": "n_blank_org_unknown",
                    }
                ),
                on="bucket_start",
                how="left",
            )
            .merge(
                pivot_rate.rename(
                    columns={
                        "Pro": "pro_blank_org_rate",
                        "Con": "con_blank_org_rate",
                        "Unknown": "unknown_blank_org_rate",
                    }
                ),
                on="bucket_start",
                how="left",
            )
        )
        total["bucket_minutes"] = int(bucket)

        for column in [
            "n_pro",
            "n_con",
            "n_unknown",
            "n_blank_org_pro",
            "n_blank_org_con",
            "n_blank_org_unknown",
        ]:
            if column not in total.columns:
                total[column] = 0
            total[column] = total[column].fillna(0).astype(int)

        total["blank_org_rate_wilson_low"], total["blank_org_rate_wilson_high"] = wilson_interval(
            successes=total["n_blank_org"],
            totals=total["n_total"],
        )
        total["blank_org_rate_wilson_half_width"] = wilson_half_width(
            successes=total["n_blank_org"],
            totals=total["n_total"],
        )
        total["is_low_power"] = low_power_mask(
            totals=total["n_total"],
            min_total=low_power_min_total,
        )

        for prefix, total_col, blank_col, rate_col in [
            ("pro", "n_pro", "n_blank_org_pro", "pro_blank_org_rate"),
            ("con", "n_con", "n_blank_org_con", "con_blank_org_rate"),
            ("unknown", "n_unknown", "n_blank_org_unknown", "unknown_blank_org_rate"),
        ]:
            total[f"{rate_col}_wilson_low"], total[f"{rate_col}_wilson_high"] = wilson_interval(
                successes=total[blank_col],
                totals=total[total_col],
            )
            total[f"{rate_col}_wilson_half_width"] = wilson_half_width(
                successes=total[blank_col],
                totals=total[total_col],
            )
            total[f"{prefix}_is_low_power"] = low_power_mask(
                totals=total[total_col],
                min_total=low_power_min_total,
            )

        by_bucket_frames.append(total)

    by_bucket = (
        pd.concat(by_bucket_frames, ignore_index=True)
        if by_bucket_frames
        else pd.DataFrame(
            columns=[
                "bucket_start",
                "n_total",
                "n_blank_org",
                "n_nonblank_org",
                "blank_org_rate",
                "blank_org_rate_wilson_low",
                "blank_org_rate_wilson_high",
                "blank_org_rate_wilson_half_width",
                "is_low_power",
                "n_pro",
                "n_con",
                "n_unknown",
                "n_blank_org_pro",
                "n_blank_org_con",
                "n_blank_org_unknown",
                "pro_blank_org_rate",
                "pro_blank_org_rate_wilson_low",
                "pro_blank_org_rate_wilson_high",
                "pro_blank_org_rate_wilson_half_width",
                "pro_is_low_power",
                "con_blank_org_rate",
                "con_blank_org_rate_wilson_low",
                "con_blank_org_rate_wilson_high",
                "con_blank_org_rate_wilson_half_width",
                "con_is_low_power",
                "unknown_blank_org_rate",
                "unknown_blank_org_rate_wilson_low",
                "unknown_blank_org_rate_wilson_high",
                "unknown_blank_org_rate_wilson_half_width",
                "unknown_is_low_power",
                "bucket_minutes",
            ]
        )
    )
    by_bucket_position = (
        pd.concat(by_bucket_position_frames, ignore_index=True)
        if by_bucket_position_frames
        else pd.DataFrame(
            columns=[
                "bucket_start",
                "position_normalized",
                "n_total",
                "n_blank_org",
                "n_nonblank_org",
                "blank_org_rate",
                "blank_org_rate_wilson_low",
                "blank_org_rate_wilson_high",
                "blank_org_rate_wilson_half_width",
                "is_low_power",
                "bucket_minutes",
            ]
        )
    )

    by_bucket_summary = (
        by_bucket.groupby("bucket_minutes", dropna=False)
        .agg(
            n_buckets=("bucket_start", "count"),
            n_low_power_buckets=("is_low_power", "sum"),
            avg_blank_org_rate=("blank_org_rate", "mean"),
            median_blank_org_rate=("blank_org_rate", "median"),
            max_blank_org_rate=("blank_org_rate", "max"),
            min_blank_org_rate=("blank_org_rate", "min"),
            avg_blank_org_rate_wilson_half_width=("blank_org_rate_wilson_half_width", "mean"),
        )
        .reset_index()
        .sort_values("bucket_minutes")
    )
    return by_bucket, by_bucket_position, by_bucket_summary


class OrganizationAnomaliesDetector(Detector):
    name = "org_anomalies"

    def __init__(
        self,
        bucket_minutes: list[int] | tuple[int, ...] = BUCKET_MINUTES,
    ) -> None:
        self.bucket_minutes = tuple(
            sorted({int(value) for value in bucket_minutes if int(value) > 0})
        )
        if not self.bucket_minutes:
            self.bucket_minutes = tuple(BUCKET_MINUTES)

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        working = df.copy()
        working["organization_clean"] = (
            working["organization"].fillna("").astype(str).str.strip().str.upper()
        )
        working["organization_is_blank"] = working["organization_clean"] == ""

        nonblank = working[working["organization_clean"] != ""].copy()

        org_counts = (
            nonblank.groupby("organization_clean", dropna=True)
            .agg(
                n=("id", "count"),
                n_pro=("position_normalized", lambda s: int((s == "Pro").sum())),
                n_con=("position_normalized", lambda s: int((s == "Con").sum())),
                first_seen=("timestamp", "min"),
                last_seen=("timestamp", "max"),
            )
            .reset_index()
            .sort_values("n", ascending=False)
        )

        minute_org_counts = (
            nonblank.groupby(["minute_bucket", "organization_clean"], dropna=True)
            .agg(n=("id", "count"))
            .reset_index()
            .sort_values("n", ascending=False)
        )
        if not minute_org_counts.empty:
            threshold = float(minute_org_counts["n"].quantile(0.99))
            org_bursts = minute_org_counts[minute_org_counts["n"] >= max(2.0, threshold)].copy()
            org_bursts["threshold"] = threshold
        else:
            org_bursts = minute_org_counts

        low_power_min_total = DEFAULT_LOW_POWER_MIN_TOTAL
        by_bucket, by_bucket_position, by_bucket_summary = _organization_blank_rate_tables(
            working,
            bucket_minutes=self.bucket_minutes,
            low_power_min_total=low_power_min_total,
        )

        summary = {
            "n_nonblank_org_rows": int(len(nonblank)),
            "n_blank_org_rows": int((working["organization_is_blank"]).sum()),
            "n_distinct_orgs": int(nonblank["organization_clean"].nunique()),
            "nonblank_org_ratio": float(len(nonblank) / len(working)) if len(working) else 0.0,
            "blank_org_ratio": float((working["organization_is_blank"]).mean())
            if len(working)
            else 0.0,
            "max_org_minute_count": int(minute_org_counts["n"].max())
            if not minute_org_counts.empty
            else 0,
            "organization_blank_bucket_minutes": [int(value) for value in self.bucket_minutes],
            "low_power_min_total": int(low_power_min_total),
            "n_low_power_blank_org_buckets": int(by_bucket["is_low_power"].sum())
            if not by_bucket.empty
            else 0,
        }

        return DetectorResult(
            detector=self.name,
            summary=summary,
            tables={
                "organization_counts": org_counts,
                "organization_minute_bursts": org_bursts,
                "organization_blank_rate_by_bucket": by_bucket,
                "organization_blank_rate_by_bucket_position": by_bucket_position,
                "organization_blank_rate_summary": by_bucket_summary,
            },
        )
