from __future__ import annotations

import numpy as np
import pandas as pd

from testifier_audit.detectors.base import Detector, DetectorResult
from testifier_audit.features.rarity import (
    load_name_frequency_lookup,
    normalize_name_token,
    score_name_rarity,
)
from testifier_audit.proportion_stats import (
    DEFAULT_LOW_POWER_MIN_TOTAL,
    low_power_mask,
    wilson_interval,
)

DEFAULT_BUCKET_MINUTES = [1, 5, 15, 30, 60, 120, 240]


class RareNamesDetector(Detector):
    name = "rare_names"

    def __init__(
        self,
        min_window_total: int = 5,
        rarity_enabled: bool = False,
        first_name_frequency_path: str | None = None,
        last_name_frequency_path: str | None = None,
        rarity_epsilon: float = 1e-9,
        bucket_minutes: list[int] | tuple[int, ...] | None = None,
        low_power_min_total: int = DEFAULT_LOW_POWER_MIN_TOTAL,
    ) -> None:
        self.min_window_total = min_window_total
        self.rarity_enabled = rarity_enabled
        self.first_name_frequency_path = first_name_frequency_path
        self.last_name_frequency_path = last_name_frequency_path
        self.rarity_epsilon = rarity_epsilon
        buckets = bucket_minutes or DEFAULT_BUCKET_MINUTES
        self.bucket_minutes = sorted({int(value) for value in buckets if int(value) > 0})
        if not self.bucket_minutes:
            self.bucket_minutes = list(DEFAULT_BUCKET_MINUTES)
        self.low_power_min_total = max(1, int(low_power_min_total))

    @staticmethod
    def _empty_rarity_tables() -> dict[str, pd.DataFrame]:
        return {
            "rarity_by_minute": pd.DataFrame(),
            "rarity_high_windows": pd.DataFrame(),
            "rarity_top_records": pd.DataFrame(),
            "rarity_lookup_coverage": pd.DataFrame(),
            "rarity_unmatched_first_tokens": pd.DataFrame(),
            "rarity_unmatched_last_tokens": pd.DataFrame(),
        }

    def _build_bucketed_name_windows(self, df: pd.DataFrame) -> pd.DataFrame:
        required = {"minute_bucket", "canonical_name", "position_normalized"}
        if not required.issubset(set(df.columns)):
            return pd.DataFrame()

        working = df.copy()
        id_column = "id"
        if id_column not in working.columns:
            id_column = "__row_id"
            working[id_column] = np.arange(len(working), dtype=int)
        working["minute_bucket"] = pd.to_datetime(working["minute_bucket"], errors="coerce")
        working = working.dropna(subset=["minute_bucket"])
        if working.empty:
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []
        for bucket_minutes in self.bucket_minutes:
            grouped = (
                working.assign(
                    bucket_start=working["minute_bucket"].dt.floor(f"{int(bucket_minutes)}min")
                )
                .groupby("bucket_start", dropna=True)
                .agg(
                    n_total=(id_column, "count"),
                    n_unique_names=("canonical_name", "nunique"),
                    n_pro=("position_normalized", lambda s: int((s == "Pro").sum())),
                    n_con=("position_normalized", lambda s: int((s == "Con").sum())),
                )
                .reset_index()
                .rename(columns={"bucket_start": "minute_bucket"})
                .sort_values("minute_bucket")
            )
            if grouped.empty:
                continue

            grouped["bucket_minutes"] = int(bucket_minutes)
            grouped["unique_ratio"] = (grouped["n_unique_names"] / grouped["n_total"]).where(
                grouped["n_total"] > 0
            )
            grouped["pro_rate"] = (grouped["n_pro"] / grouped["n_total"]).where(
                grouped["n_total"] > 0
            )
            grouped["pro_rate_wilson_low"], grouped["pro_rate_wilson_high"] = wilson_interval(
                successes=grouped["n_pro"],
                totals=grouped["n_total"],
            )
            grouped["is_low_power"] = low_power_mask(
                totals=grouped["n_total"],
                min_total=self.low_power_min_total,
            )
            frames.append(grouped)

        if not frames:
            return pd.DataFrame()
        return (
            pd.concat(frames, ignore_index=True)
            .sort_values(["bucket_minutes", "minute_bucket"])
            .reset_index(drop=True)
        )

    def _build_rarity_tables(
        self,
        df: pd.DataFrame,
        bucketed_name_windows: pd.DataFrame,
    ) -> tuple[dict[str, pd.DataFrame], dict[str, float | int | bool]]:
        summary = {
            "rarity_enrichment_enabled": bool(self.rarity_enabled),
            "rarity_enrichment_active": False,
            "n_rarity_high_windows": 0,
            "max_rarity_median": 0.0,
            "first_lookup_size": 0,
            "last_lookup_size": 0,
            "n_rarity_records": 0,
            "first_lookup_match_rate": 0.0,
            "last_lookup_match_rate": 0.0,
            "both_lookup_match_rate": 0.0,
        }
        if not self.rarity_enabled or df.empty:
            return self._empty_rarity_tables(), summary
        if "first" not in df.columns or "last" not in df.columns:
            return self._empty_rarity_tables(), summary

        first_lookup = load_name_frequency_lookup(self.first_name_frequency_path)
        last_lookup = load_name_frequency_lookup(self.last_name_frequency_path)
        if not first_lookup and not last_lookup:
            return self._empty_rarity_tables(), summary
        summary["first_lookup_size"] = int(len(first_lookup))
        summary["last_lookup_size"] = int(len(last_lookup))

        first_tokens = (
            df.get("first_canonical", df["first"])
            .fillna("")
            .astype(str)
            .str.split(" ", n=1)
            .str[0]
            .map(normalize_name_token)
        )
        last_tokens = df["last"].fillna("").astype(str).map(normalize_name_token)
        valid_mask = (first_tokens != "") & (last_tokens != "")
        if not valid_mask.any():
            return self._empty_rarity_tables(), summary

        rarity_score = score_name_rarity(
            first_tokens=first_tokens[valid_mask],
            last_tokens=last_tokens[valid_mask],
            first_lookup=first_lookup,
            last_lookup=last_lookup,
            epsilon=self.rarity_epsilon,
            first_missing_probability=1.0 if not first_lookup else None,
            last_missing_probability=1.0 if not last_lookup else None,
        )

        rarity_records = df.loc[valid_mask].copy()
        rarity_records["first_token"] = first_tokens[valid_mask]
        rarity_records["last_token"] = last_tokens[valid_mask]
        rarity_records["rarity_score"] = rarity_score
        rarity_records["first_lookup_match"] = (
            rarity_records["first_token"].isin(set(first_lookup)) if first_lookup else True
        )
        rarity_records["last_lookup_match"] = (
            rarity_records["last_token"].isin(set(last_lookup)) if last_lookup else True
        )
        rarity_records["both_lookup_match"] = (
            rarity_records["first_lookup_match"] & rarity_records["last_lookup_match"]
        )

        top_columns = [
            column
            for column in [
                "minute_bucket",
                "canonical_name",
                "name_display",
                "position_normalized",
                "rarity_score",
            ]
            if column in rarity_records.columns
        ]
        rarity_top_records = (
            rarity_records[top_columns].sort_values("rarity_score", ascending=False).head(200)
        )

        rarity_frames: list[pd.DataFrame] = []
        rarity_high_frames: list[pd.DataFrame] = []
        for bucket_minutes in self.bucket_minutes:
            bucket_frame = rarity_records.copy()
            bucket_frame["minute_bucket"] = pd.to_datetime(
                bucket_frame["minute_bucket"], errors="coerce"
            )
            bucket_frame = bucket_frame.dropna(subset=["minute_bucket"])
            if bucket_frame.empty:
                continue
            bucket_frame["bucket_start"] = bucket_frame["minute_bucket"].dt.floor(
                f"{int(bucket_minutes)}min"
            )

            rarity_by_bucket = (
                bucket_frame.groupby("bucket_start", dropna=True)
                .agg(
                    n_records=("rarity_score", "count"),
                    rarity_mean=("rarity_score", "mean"),
                    rarity_median=("rarity_score", "median"),
                    rarity_p90=("rarity_score", lambda s: float(np.quantile(s, 0.90))),
                    rarity_p95=("rarity_score", lambda s: float(np.quantile(s, 0.95))),
                )
                .reset_index()
                .rename(columns={"bucket_start": "minute_bucket"})
                .sort_values("minute_bucket")
            )
            if rarity_by_bucket.empty:
                continue

            rarity_by_bucket["bucket_minutes"] = int(bucket_minutes)
            if not bucketed_name_windows.empty:
                totals = bucketed_name_windows[
                    bucketed_name_windows["bucket_minutes"] == int(bucket_minutes)
                ][["minute_bucket", "n_total"]]
                rarity_by_bucket = rarity_by_bucket.merge(
                    totals,
                    on="minute_bucket",
                    how="left",
                )
            if "n_total" not in rarity_by_bucket.columns:
                rarity_by_bucket["n_total"] = rarity_by_bucket["n_records"]
            rarity_by_bucket["n_total"] = pd.to_numeric(
                rarity_by_bucket["n_total"], errors="coerce"
            ).fillna(rarity_by_bucket["n_records"])
            rarity_by_bucket["is_low_power"] = low_power_mask(
                totals=rarity_by_bucket["n_total"],
                min_total=self.low_power_min_total,
            )
            rarity_frames.append(rarity_by_bucket)

            candidate_windows = rarity_by_bucket[
                rarity_by_bucket["n_total"] >= self.min_window_total
            ].copy()
            if candidate_windows.empty:
                continue
            threshold = float(candidate_windows["rarity_median"].quantile(0.99))
            candidate_windows["threshold_rarity_median"] = threshold
            candidate_windows["is_flagged"] = candidate_windows["rarity_median"] >= threshold
            rarity_high_frames.append(candidate_windows[candidate_windows["is_flagged"]].copy())

        rarity_by_minute = (
            pd.concat(rarity_frames, ignore_index=True)
            .sort_values(["bucket_minutes", "minute_bucket"])
            .reset_index(drop=True)
            if rarity_frames
            else pd.DataFrame()
        )
        rarity_high_windows = (
            pd.concat(rarity_high_frames, ignore_index=True)
            .sort_values(["bucket_minutes", "minute_bucket"])
            .reset_index(drop=True)
            if rarity_high_frames
            else pd.DataFrame()
        )

        unmatched_first = pd.DataFrame(columns=["first_token", "n_records"])
        unmatched_last = pd.DataFrame(columns=["last_token", "n_records"])
        if first_lookup:
            unmatched_first = (
                rarity_records[~rarity_records["first_lookup_match"]]
                .groupby("first_token", dropna=True)
                .size()
                .rename("n_records")
                .reset_index()
                .sort_values(["n_records", "first_token"], ascending=[False, True])
                .head(200)
            )
        if last_lookup:
            unmatched_last = (
                rarity_records[~rarity_records["last_lookup_match"]]
                .groupby("last_token", dropna=True)
                .size()
                .rename("n_records")
                .reset_index()
                .sort_values(["n_records", "last_token"], ascending=[False, True])
                .head(200)
            )

        n_rarity_records = int(len(rarity_records))
        first_lookup_match_rate = (
            float(rarity_records["first_lookup_match"].mean()) if n_rarity_records else 0.0
        )
        last_lookup_match_rate = (
            float(rarity_records["last_lookup_match"].mean()) if n_rarity_records else 0.0
        )
        both_lookup_match_rate = (
            float(rarity_records["both_lookup_match"].mean()) if n_rarity_records else 0.0
        )
        coverage_table = pd.DataFrame(
            [
                {
                    "n_rarity_records": n_rarity_records,
                    "first_lookup_size": int(len(first_lookup)),
                    "last_lookup_size": int(len(last_lookup)),
                    "first_lookup_match_rate": first_lookup_match_rate,
                    "last_lookup_match_rate": last_lookup_match_rate,
                    "both_lookup_match_rate": both_lookup_match_rate,
                    "n_unmatched_first_tokens": int(len(unmatched_first)),
                    "n_unmatched_last_tokens": int(len(unmatched_last)),
                }
            ]
        )

        summary["rarity_enrichment_active"] = True
        summary["n_rarity_high_windows"] = int(len(rarity_high_windows))
        summary["max_rarity_median"] = (
            float(rarity_by_minute["rarity_median"].max()) if not rarity_by_minute.empty else 0.0
        )
        summary["n_rarity_records"] = n_rarity_records
        summary["first_lookup_match_rate"] = first_lookup_match_rate
        summary["last_lookup_match_rate"] = last_lookup_match_rate
        summary["both_lookup_match_rate"] = both_lookup_match_rate
        return (
            {
                "rarity_by_minute": rarity_by_minute,
                "rarity_high_windows": rarity_high_windows,
                "rarity_top_records": rarity_top_records,
                "rarity_lookup_coverage": coverage_table,
                "rarity_unmatched_first_tokens": unmatched_first,
                "rarity_unmatched_last_tokens": unmatched_last,
            },
            summary,
        )

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        name_frequency = features.get("name_frequency", pd.DataFrame())
        text_features = features.get("name_text_features", pd.DataFrame())
        bucketed_name_windows = self._build_bucketed_name_windows(df=df)

        if name_frequency.empty:
            empty = pd.DataFrame()
            return DetectorResult(
                detector=self.name,
                summary={
                    "n_singletons": 0,
                    "singleton_ratio": 0.0,
                    "n_flagged_unique_windows": 0,
                    "rarity_enrichment_enabled": bool(self.rarity_enabled),
                    "rarity_enrichment_active": False,
                    "n_rarity_high_windows": 0,
                    "max_rarity_median": 0.0,
                    "first_lookup_size": 0,
                    "last_lookup_size": 0,
                    "n_rarity_records": 0,
                    "first_lookup_match_rate": 0.0,
                    "last_lookup_match_rate": 0.0,
                    "both_lookup_match_rate": 0.0,
                },
                tables={
                    "singleton_names": empty,
                    "unique_ratio_windows": empty,
                    "weird_names": empty,
                    **self._empty_rarity_tables(),
                },
            )

        singleton = name_frequency[name_frequency["n"] == 1].copy()
        singleton_ratio = (
            float(len(singleton) / len(name_frequency)) if len(name_frequency) else 0.0
        )

        unique_windows = pd.DataFrame()
        if not bucketed_name_windows.empty:
            bucket_frames: list[pd.DataFrame] = []
            for bucket_minutes in sorted(
                pd.to_numeric(bucketed_name_windows["bucket_minutes"], errors="coerce")
                .dropna()
                .astype(int)
                .unique()
                .tolist()
            ):
                candidate = bucketed_name_windows[
                    (bucketed_name_windows["bucket_minutes"] == int(bucket_minutes))
                    & (bucketed_name_windows["n_total"] >= self.min_window_total)
                ].copy()
                if candidate.empty:
                    continue
                threshold = float(candidate["unique_ratio"].quantile(0.99))
                candidate["threshold_unique_ratio"] = threshold
                candidate["is_flagged_unique_ratio"] = candidate["unique_ratio"] >= threshold
                bucket_frames.append(candidate)
            if bucket_frames:
                unique_windows = (
                    pd.concat(bucket_frames, ignore_index=True)
                    .sort_values(["bucket_minutes", "minute_bucket"])
                    .reset_index(drop=True)
                )

        weird_names = pd.DataFrame()
        if not text_features.empty:
            weird_names = (
                text_features.groupby("canonical_name", dropna=True)
                .agg(
                    weirdness_score=("weirdness_score", "mean"),
                    name_length=("name_length", "mean"),
                    non_alpha_fraction=("non_alpha_fraction", "mean"),
                    name_entropy=("name_entropy", "mean"),
                    sample_name=("name_normalized", "first"),
                )
                .reset_index()
                .sort_values("weirdness_score", ascending=False)
                .head(200)
            )

        rarity_tables, rarity_summary = self._build_rarity_tables(
            df=df,
            bucketed_name_windows=bucketed_name_windows,
        )

        summary = {
            "n_singletons": int(len(singleton)),
            "singleton_ratio": singleton_ratio,
            "n_flagged_unique_windows": (
                int(unique_windows["is_flagged_unique_ratio"].sum())
                if not unique_windows.empty and "is_flagged_unique_ratio" in unique_windows.columns
                else int(len(unique_windows))
            ),
            "max_unique_ratio": float(unique_windows["unique_ratio"].max())
            if not unique_windows.empty
            else 0.0,
            **rarity_summary,
        }

        return DetectorResult(
            detector=self.name,
            summary=summary,
            tables={
                "singleton_names": singleton,
                "unique_ratio_windows": unique_windows,
                "weird_names": weird_names,
                **rarity_tables,
            },
        )
