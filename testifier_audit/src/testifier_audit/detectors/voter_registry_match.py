from __future__ import annotations

import pandas as pd

from testifier_audit.detectors.base import Detector, DetectorResult
from testifier_audit.io.vrdb_postgres import count_registry_rows, fetch_matching_voter_names
from testifier_audit.proportion_stats import (
    DEFAULT_LOW_POWER_MIN_TOTAL,
    low_power_mask,
    wilson_half_width,
    wilson_interval,
)


class VoterRegistryMatchDetector(Detector):
    name = "voter_registry_match"

    def __init__(
        self,
        enabled: bool = False,
        db_url: str | None = None,
        table_name: str = "voter_registry",
        bucket_minutes: int | list[int] | tuple[int, ...] = 30,
        active_only: bool = True,
        low_power_min_total: int = DEFAULT_LOW_POWER_MIN_TOTAL,
    ) -> None:
        self.enabled = enabled
        self.db_url = db_url
        self.table_name = table_name
        if isinstance(bucket_minutes, (list, tuple, set)):
            parsed = sorted({max(1, int(value)) for value in bucket_minutes if int(value) > 0})
        else:
            parsed = [max(1, int(bucket_minutes))]
        self.bucket_minutes = parsed or [30]
        self.active_only = active_only
        self.low_power_min_total = max(1, int(low_power_min_total))

    @staticmethod
    def _empty_tables() -> dict[str, pd.DataFrame]:
        return {
            "match_overview": pd.DataFrame(
                columns=[
                    "n_records",
                    "n_matches",
                    "n_unmatched",
                    "match_rate",
                    "match_rate_wilson_low",
                    "match_rate_wilson_high",
                    "match_rate_wilson_half_width",
                    "unmatched_rate",
                    "is_low_power",
                    "n_unique_names",
                    "n_unique_matches",
                    "n_unique_unmatched",
                    "unique_match_rate",
                    "registry_row_count",
                    "active_only",
                    "bucket_minutes",
                    "bucket_minutes_list",
                ]
            ),
            "match_by_position": pd.DataFrame(
                columns=[
                    "position_normalized",
                    "n_total",
                    "n_matches",
                    "n_unmatched",
                    "match_rate",
                    "match_rate_wilson_low",
                    "match_rate_wilson_high",
                    "match_rate_wilson_half_width",
                    "unmatched_rate",
                    "is_low_power",
                ]
            ),
            "match_by_bucket": pd.DataFrame(
                columns=[
                    "bucket_start",
                    "n_total",
                    "n_matches",
                    "n_pro",
                    "n_matches_pro",
                    "n_con",
                    "n_matches_con",
                    "n_unmatched",
                    "match_rate",
                    "match_rate_wilson_low",
                    "match_rate_wilson_high",
                    "match_rate_wilson_half_width",
                    "pro_match_rate",
                    "pro_match_rate_wilson_low",
                    "pro_match_rate_wilson_high",
                    "pro_match_rate_wilson_half_width",
                    "con_match_rate",
                    "con_match_rate_wilson_low",
                    "con_match_rate_wilson_high",
                    "con_match_rate_wilson_half_width",
                    "is_low_power",
                    "pro_is_low_power",
                    "con_is_low_power",
                    "bucket_minutes",
                ]
            ),
            "match_by_bucket_position": pd.DataFrame(
                columns=[
                    "bucket_start",
                    "position_normalized",
                    "n_total",
                    "n_matches",
                    "n_unmatched",
                    "match_rate",
                    "match_rate_wilson_low",
                    "match_rate_wilson_high",
                    "match_rate_wilson_half_width",
                    "is_low_power",
                    "bucket_minutes",
                ]
            ),
            "matched_names": pd.DataFrame(
                columns=["canonical_name", "n_records", "n_registry_rows"]
            ),
            "unmatched_names": pd.DataFrame(columns=["canonical_name", "n_records"]),
        }

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        if not self.enabled:
            return DetectorResult(
                detector=self.name,
                summary={
                    "enabled": False,
                    "active": False,
                    "reason": "voter_registry_match_disabled",
                },
                tables=self._empty_tables(),
            )

        if not self.db_url:
            return DetectorResult(
                detector=self.name,
                summary={
                    "enabled": True,
                    "active": False,
                    "reason": "missing_db_url",
                },
                tables=self._empty_tables(),
            )

        if df.empty:
            return DetectorResult(
                detector=self.name,
                summary={
                    "enabled": True,
                    "active": True,
                    "n_records": 0,
                    "match_rate": 0.0,
                },
                tables=self._empty_tables(),
            )

        required = {"canonical_name", "position_normalized", "minute_bucket"}
        missing = [column for column in required if column not in df.columns]
        if missing:
            raise ValueError(
                "Missing required columns for voter registry matching: "
                + ", ".join(sorted(missing))
            )

        working = df.copy()
        working["canonical_name"] = working["canonical_name"].fillna("").astype(str)
        working["position_normalized"] = (
            working["position_normalized"].fillna("Unknown").astype(str)
        )
        working["minute_bucket"] = pd.to_datetime(working["minute_bucket"], errors="coerce")
        working = working.dropna(subset=["minute_bucket"]).copy()
        if working.empty:
            return DetectorResult(
                detector=self.name,
                summary={
                    "enabled": True,
                    "active": True,
                    "n_records": 0,
                    "match_rate": 0.0,
                },
                tables=self._empty_tables(),
            )

        submission_names = sorted(
            {value for value in working["canonical_name"].tolist() if value and value != "|"}
        )

        try:
            matched_lookup = fetch_matching_voter_names(
                db_url=self.db_url,
                table_name=self.table_name,
                canonical_names=submission_names,
                active_only=self.active_only,
            )
            registry_row_count = count_registry_rows(
                db_url=self.db_url,
                table_name=self.table_name,
                active_only=self.active_only,
            )
        except Exception as exc:
            return DetectorResult(
                detector=self.name,
                summary={
                    "enabled": True,
                    "active": False,
                    "reason": "database_query_failed",
                    "error": str(exc),
                },
                tables=self._empty_tables(),
            )

        matched_names = set(matched_lookup["canonical_name"].dropna().astype(str).tolist())
        working["is_match"] = working["canonical_name"].isin(matched_names)

        n_records = int(len(working))
        n_matches = int(working["is_match"].sum())
        n_unmatched = int(n_records - n_matches)

        unique_names = set(
            value for value in working["canonical_name"].tolist() if value and value != "|"
        )
        n_unique_names = int(len(unique_names))
        n_unique_matches = int(len(unique_names.intersection(matched_names)))
        n_unique_unmatched = int(n_unique_names - n_unique_matches)
        overview_match_rate_wilson_low, overview_match_rate_wilson_high = wilson_interval(
            successes=pd.Series([n_matches]),
            totals=pd.Series([n_records]),
        )
        overview_match_rate_wilson_half_width = wilson_half_width(
            successes=pd.Series([n_matches]),
            totals=pd.Series([n_records]),
        )
        overview_is_low_power = low_power_mask(
            totals=pd.Series([n_records]),
            min_total=self.low_power_min_total,
        )

        overview = pd.DataFrame(
            [
                {
                    "n_records": n_records,
                    "n_matches": n_matches,
                    "n_unmatched": n_unmatched,
                    "match_rate": (n_matches / n_records) if n_records else 0.0,
                    "match_rate_wilson_low": float(overview_match_rate_wilson_low[0]),
                    "match_rate_wilson_high": float(overview_match_rate_wilson_high[0]),
                    "match_rate_wilson_half_width": float(overview_match_rate_wilson_half_width[0]),
                    "unmatched_rate": (n_unmatched / n_records) if n_records else 0.0,
                    "is_low_power": bool(overview_is_low_power[0]),
                    "n_unique_names": n_unique_names,
                    "n_unique_matches": n_unique_matches,
                    "n_unique_unmatched": n_unique_unmatched,
                    "unique_match_rate": (n_unique_matches / n_unique_names)
                    if n_unique_names
                    else 0.0,
                    "registry_row_count": int(registry_row_count),
                    "active_only": bool(self.active_only),
                    "bucket_minutes": int(self.bucket_minutes[0]),
                    "bucket_minutes_list": ",".join(str(value) for value in self.bucket_minutes),
                }
            ]
        )

        by_position = (
            working.groupby("position_normalized", dropna=False)
            .agg(
                n_total=("canonical_name", "count"),
                n_matches=("is_match", "sum"),
            )
            .reset_index()
            .sort_values("n_total", ascending=False)
        )
        by_position["n_unmatched"] = by_position["n_total"] - by_position["n_matches"]
        by_position["match_rate"] = (by_position["n_matches"] / by_position["n_total"]).where(
            by_position["n_total"] > 0
        )
        by_position["unmatched_rate"] = (by_position["n_unmatched"] / by_position["n_total"]).where(
            by_position["n_total"] > 0
        )
        by_position["match_rate_wilson_low"], by_position["match_rate_wilson_high"] = (
            wilson_interval(
                successes=by_position["n_matches"],
                totals=by_position["n_total"],
            )
        )
        by_position["match_rate_wilson_half_width"] = wilson_half_width(
            successes=by_position["n_matches"],
            totals=by_position["n_total"],
        )
        by_position["is_low_power"] = low_power_mask(
            totals=by_position["n_total"],
            min_total=self.low_power_min_total,
        )

        by_bucket_frames: list[pd.DataFrame] = []
        by_bucket_position_frames: list[pd.DataFrame] = []
        for bucket_minutes in self.bucket_minutes:
            bucketed = working.copy()
            bucketed["bucket_start"] = bucketed["minute_bucket"].dt.floor(
                f"{int(bucket_minutes)}min"
            )

            by_bucket_totals = (
                bucketed.groupby("bucket_start", dropna=False)
                .agg(
                    n_total=("canonical_name", "count"),
                    n_matches=("is_match", "sum"),
                )
                .reset_index()
            )

            by_bucket_position = (
                bucketed.groupby(["bucket_start", "position_normalized"], dropna=False)
                .agg(
                    n_total=("canonical_name", "count"),
                    n_matches=("is_match", "sum"),
                )
                .reset_index()
                .sort_values(["bucket_start", "position_normalized"])
            )
            by_bucket_position["n_unmatched"] = (
                by_bucket_position["n_total"] - by_bucket_position["n_matches"]
            )
            by_bucket_position["match_rate"] = (
                by_bucket_position["n_matches"] / by_bucket_position["n_total"]
            ).where(by_bucket_position["n_total"] > 0)
            (
                by_bucket_position["match_rate_wilson_low"],
                by_bucket_position["match_rate_wilson_high"],
            ) = wilson_interval(
                successes=by_bucket_position["n_matches"],
                totals=by_bucket_position["n_total"],
            )
            by_bucket_position["match_rate_wilson_half_width"] = wilson_half_width(
                successes=by_bucket_position["n_matches"],
                totals=by_bucket_position["n_total"],
            )
            by_bucket_position["is_low_power"] = low_power_mask(
                totals=by_bucket_position["n_total"],
                min_total=self.low_power_min_total,
            )
            by_bucket_position["bucket_minutes"] = int(bucket_minutes)
            by_bucket_position_frames.append(by_bucket_position)

            by_bucket_position_pro = (
                by_bucket_position[by_bucket_position["position_normalized"] == "Pro"][
                    ["bucket_start", "n_total", "n_matches"]
                ]
                .rename(columns={"n_total": "n_pro", "n_matches": "n_matches_pro"})
                .reset_index(drop=True)
            )
            by_bucket_position_con = (
                by_bucket_position[by_bucket_position["position_normalized"] == "Con"][
                    ["bucket_start", "n_total", "n_matches"]
                ]
                .rename(columns={"n_total": "n_con", "n_matches": "n_matches_con"})
                .reset_index(drop=True)
            )

            by_bucket = (
                by_bucket_totals.merge(by_bucket_position_pro, on="bucket_start", how="left")
                .merge(by_bucket_position_con, on="bucket_start", how="left")
                .fillna({"n_pro": 0, "n_matches_pro": 0, "n_con": 0, "n_matches_con": 0})
                .sort_values("bucket_start")
            )
            for column in ["n_pro", "n_matches_pro", "n_con", "n_matches_con"]:
                by_bucket[column] = by_bucket[column].astype(int)

            by_bucket["n_unmatched"] = by_bucket["n_total"] - by_bucket["n_matches"]
            by_bucket["match_rate"] = (by_bucket["n_matches"] / by_bucket["n_total"]).where(
                by_bucket["n_total"] > 0
            )
            by_bucket["pro_match_rate"] = (by_bucket["n_matches_pro"] / by_bucket["n_pro"]).where(
                by_bucket["n_pro"] > 0
            )
            by_bucket["con_match_rate"] = (by_bucket["n_matches_con"] / by_bucket["n_con"]).where(
                by_bucket["n_con"] > 0
            )
            by_bucket["match_rate_wilson_low"], by_bucket["match_rate_wilson_high"] = (
                wilson_interval(
                    successes=by_bucket["n_matches"],
                    totals=by_bucket["n_total"],
                )
            )
            by_bucket["match_rate_wilson_half_width"] = wilson_half_width(
                successes=by_bucket["n_matches"],
                totals=by_bucket["n_total"],
            )
            by_bucket["pro_match_rate_wilson_low"], by_bucket["pro_match_rate_wilson_high"] = (
                wilson_interval(
                    successes=by_bucket["n_matches_pro"],
                    totals=by_bucket["n_pro"],
                )
            )
            by_bucket["pro_match_rate_wilson_half_width"] = wilson_half_width(
                successes=by_bucket["n_matches_pro"],
                totals=by_bucket["n_pro"],
            )
            by_bucket["con_match_rate_wilson_low"], by_bucket["con_match_rate_wilson_high"] = (
                wilson_interval(
                    successes=by_bucket["n_matches_con"],
                    totals=by_bucket["n_con"],
                )
            )
            by_bucket["con_match_rate_wilson_half_width"] = wilson_half_width(
                successes=by_bucket["n_matches_con"],
                totals=by_bucket["n_con"],
            )
            by_bucket["is_low_power"] = low_power_mask(
                totals=by_bucket["n_total"],
                min_total=self.low_power_min_total,
            )
            by_bucket["pro_is_low_power"] = low_power_mask(
                totals=by_bucket["n_pro"],
                min_total=self.low_power_min_total,
            )
            by_bucket["con_is_low_power"] = low_power_mask(
                totals=by_bucket["n_con"],
                min_total=self.low_power_min_total,
            )
            by_bucket["bucket_minutes"] = int(bucket_minutes)
            by_bucket_frames.append(by_bucket)

        by_bucket = (
            pd.concat(by_bucket_frames, ignore_index=True)
            .sort_values(["bucket_minutes", "bucket_start"])
            .reset_index(drop=True)
            if by_bucket_frames
            else self._empty_tables()["match_by_bucket"]
        )
        by_bucket_position = (
            pd.concat(by_bucket_position_frames, ignore_index=True)
            .sort_values(["bucket_minutes", "bucket_start", "position_normalized"])
            .reset_index(drop=True)
            if by_bucket_position_frames
            else self._empty_tables()["match_by_bucket_position"]
        )

        matched_name_counts = (
            working[working["is_match"]]
            .groupby("canonical_name", dropna=False)
            .agg(n_records=("canonical_name", "count"))
            .reset_index()
            .sort_values("n_records", ascending=False)
        )
        if not matched_name_counts.empty and not matched_lookup.empty:
            matched_name_counts = matched_name_counts.merge(
                matched_lookup,
                on="canonical_name",
                how="left",
            )
        unmatched_name_counts = (
            working[~working["is_match"]]
            .groupby("canonical_name", dropna=False)
            .agg(n_records=("canonical_name", "count"))
            .reset_index()
            .sort_values("n_records", ascending=False)
        )

        return DetectorResult(
            detector=self.name,
            summary={
                "enabled": True,
                "active": True,
                "bucket_minutes": [int(value) for value in self.bucket_minutes],
                "n_records": n_records,
                "n_matches": n_matches,
                "n_unmatched": n_unmatched,
                "match_rate": (n_matches / n_records) if n_records else 0.0,
                "n_unique_names": n_unique_names,
                "n_unique_matches": n_unique_matches,
                "unique_match_rate": (n_unique_matches / n_unique_names) if n_unique_names else 0.0,
                "registry_row_count": int(registry_row_count),
                "active_only": bool(self.active_only),
                "low_power_min_total": int(self.low_power_min_total),
                "n_low_power_match_buckets": int(by_bucket["is_low_power"].sum())
                if not by_bucket.empty
                else 0,
            },
            tables={
                "match_overview": overview,
                "match_by_position": by_position,
                "match_by_bucket": by_bucket,
                "match_by_bucket_position": by_bucket_position,
                "matched_names": matched_name_counts.head(500),
                "unmatched_names": unmatched_name_counts.head(500),
            },
        )
