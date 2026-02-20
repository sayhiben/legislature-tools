from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler

from testifier_audit.detectors.base import Detector, DetectorResult
from testifier_audit.proportion_stats import (
    DEFAULT_LOW_POWER_MIN_TOTAL,
    low_power_mask,
    wilson_half_width,
    wilson_interval,
)


class MultivariateAnomaliesDetector(Detector):
    name = "multivariate_anomalies"

    def __init__(
        self,
        enabled: bool = True,
        bucket_minutes: int | list[int] | tuple[int, ...] = 15,
        contamination: float = 0.03,
        min_bucket_total: int = 25,
        top_n: int = 50,
        random_seed: int = 42,
        low_power_min_total: int = DEFAULT_LOW_POWER_MIN_TOTAL,
    ) -> None:
        self.enabled = bool(enabled)
        if isinstance(bucket_minutes, (list, tuple, set)):
            parsed = sorted({max(1, int(value)) for value in bucket_minutes if int(value) > 0})
        else:
            parsed = [max(1, int(bucket_minutes))]
        self.bucket_minutes = parsed or [15]
        self.contamination = float(np.clip(contamination, 0.001, 0.5))
        self.min_bucket_total = max(1, int(min_bucket_total))
        self.top_n = max(1, int(top_n))
        self.random_seed = int(random_seed)
        self.low_power_min_total = max(1, int(low_power_min_total))

    def _empty_bucket_scores(self) -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "bucket_start",
                "bucket_minutes",
                "n_total",
                "n_pro",
                "n_con",
                "pro_rate",
                "pro_rate_wilson_low",
                "pro_rate_wilson_high",
                "pro_rate_wilson_half_width",
                "dup_name_fraction_weighted",
                "n_blank_org",
                "blank_org_rate",
                "log_n_total",
                "delta_log_n_total",
                "delta_pro_rate",
                "abs_delta_pro_rate",
                "is_low_power",
                "is_model_eligible",
                "anomaly_score",
                "anomaly_score_percentile",
                "is_anomaly",
            ]
        )

    @staticmethod
    def _empty_top_anomalies() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "bucket_start",
                "bucket_minutes",
                "n_total",
                "n_pro",
                "n_con",
                "pro_rate",
                "dup_name_fraction_weighted",
                "blank_org_rate",
                "anomaly_score",
                "anomaly_score_percentile",
                "is_anomaly",
            ]
        )

    def _blank_org_by_bucket(self, df: pd.DataFrame, bucket_minutes: int) -> pd.DataFrame:
        required_columns = {"minute_bucket", "organization", "id"}
        if not required_columns.issubset(set(df.columns)):
            return pd.DataFrame(columns=["bucket_start", "n_blank_org", "blank_org_rate"])

        working = df.loc[:, ["minute_bucket", "organization", "id"]].copy()
        working["minute_bucket"] = pd.to_datetime(working["minute_bucket"], errors="coerce")
        working = working.dropna(subset=["minute_bucket"]).copy()
        if working.empty:
            return pd.DataFrame(columns=["bucket_start", "n_blank_org", "blank_org_rate"])

        working["bucket_start"] = working["minute_bucket"].dt.floor(f"{int(bucket_minutes)}min")
        working["organization_is_blank"] = (
            working["organization"].fillna("").astype(str).str.strip() == ""
        )

        bucket = (
            working.groupby("bucket_start", dropna=True)
            .agg(
                n_total_records=("id", "count"),
                n_blank_org=("organization_is_blank", "sum"),
            )
            .reset_index()
            .sort_values("bucket_start")
        )
        bucket["blank_org_rate"] = (bucket["n_blank_org"] / bucket["n_total_records"]).where(
            bucket["n_total_records"] > 0
        )
        return bucket[["bucket_start", "n_blank_org", "blank_org_rate"]]

    def _build_bucket_table(
        self,
        df: pd.DataFrame,
        counts: pd.DataFrame,
        bucket_minutes: int,
    ) -> pd.DataFrame:
        working = counts.copy()
        working["minute_bucket"] = pd.to_datetime(working["minute_bucket"], errors="coerce")
        working = working.dropna(subset=["minute_bucket"]).copy()
        if working.empty:
            return self._empty_bucket_scores()

        for column in ("n_total", "n_pro", "n_con", "dup_name_fraction"):
            if column not in working.columns:
                working[column] = 0.0
            working[column] = (
                pd.to_numeric(working[column], errors="coerce").fillna(0.0).astype(float)
            )

        working["bucket_start"] = working["minute_bucket"].dt.floor(f"{int(bucket_minutes)}min")
        working["dup_x_total"] = working["dup_name_fraction"] * working["n_total"]

        bucketed = (
            working.groupby("bucket_start", dropna=True)
            .agg(
                n_total=("n_total", "sum"),
                n_pro=("n_pro", "sum"),
                n_con=("n_con", "sum"),
                dup_x_total=("dup_x_total", "sum"),
            )
            .reset_index()
            .sort_values("bucket_start")
        )

        bucketed["dup_name_fraction_weighted"] = (
            bucketed["dup_x_total"] / bucketed["n_total"]
        ).where(bucketed["n_total"] > 0)
        bucketed = bucketed.drop(columns=["dup_x_total"])

        bucketed["pro_rate"] = (bucketed["n_pro"] / bucketed["n_total"]).where(
            bucketed["n_total"] > 0
        )
        bucketed["pro_rate_wilson_low"], bucketed["pro_rate_wilson_high"] = wilson_interval(
            successes=bucketed["n_pro"],
            totals=bucketed["n_total"],
        )
        bucketed["pro_rate_wilson_half_width"] = wilson_half_width(
            successes=bucketed["n_pro"],
            totals=bucketed["n_total"],
        )
        bucketed["is_low_power"] = low_power_mask(
            totals=bucketed["n_total"],
            min_total=self.low_power_min_total,
        )

        org_blank_bucket = self._blank_org_by_bucket(df, bucket_minutes=int(bucket_minutes))
        bucketed = bucketed.merge(org_blank_bucket, on="bucket_start", how="left")
        if "n_blank_org" not in bucketed.columns:
            bucketed["n_blank_org"] = 0
        bucketed["n_blank_org"] = (
            pd.to_numeric(bucketed["n_blank_org"], errors="coerce").fillna(0).astype(int)
        )
        bucketed["blank_org_rate"] = pd.to_numeric(bucketed["blank_org_rate"], errors="coerce")

        bucketed["bucket_minutes"] = int(bucket_minutes)
        bucketed["log_n_total"] = np.log1p(bucketed["n_total"].astype(float))
        bucketed["delta_log_n_total"] = bucketed["log_n_total"].diff().fillna(0.0)
        bucketed["delta_pro_rate"] = bucketed["pro_rate"].diff().fillna(0.0)
        bucketed["abs_delta_pro_rate"] = bucketed["delta_pro_rate"].abs()
        bucketed["is_model_eligible"] = bucketed["n_total"] >= float(self.min_bucket_total)
        bucketed["anomaly_score"] = np.nan
        bucketed["anomaly_score_percentile"] = np.nan
        bucketed["is_anomaly"] = False

        return bucketed[
            [
                "bucket_start",
                "bucket_minutes",
                "n_total",
                "n_pro",
                "n_con",
                "pro_rate",
                "pro_rate_wilson_low",
                "pro_rate_wilson_high",
                "pro_rate_wilson_half_width",
                "dup_name_fraction_weighted",
                "n_blank_org",
                "blank_org_rate",
                "log_n_total",
                "delta_log_n_total",
                "delta_pro_rate",
                "abs_delta_pro_rate",
                "is_low_power",
                "is_model_eligible",
                "anomaly_score",
                "anomaly_score_percentile",
                "is_anomaly",
            ]
        ]

    @staticmethod
    def _safe_fill_numeric(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        working = frame.copy()
        for column in columns:
            values = pd.to_numeric(working[column], errors="coerce")
            median = float(values.median()) if values.notna().any() else 0.0
            working[column] = values.fillna(median)
        return working

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        if not self.enabled:
            return DetectorResult(
                detector=self.name,
                summary={
                    "enabled": False,
                    "active": False,
                    "reason": "multivariate_anomalies_disabled",
                },
                tables={
                    "bucket_anomaly_scores": self._empty_bucket_scores(),
                    "top_bucket_anomalies": self._empty_top_anomalies(),
                },
            )

        counts = features.get("counts_per_minute", pd.DataFrame())
        if counts.empty:
            return DetectorResult(
                detector=self.name,
                summary={
                    "enabled": True,
                    "active": True,
                    "n_buckets_total": 0,
                    "n_buckets_model_eligible": 0,
                    "n_anomaly_buckets": 0,
                    "bucket_minutes": [int(value) for value in self.bucket_minutes],
                    "contamination": float(self.contamination),
                },
                tables={
                    "bucket_anomaly_scores": self._empty_bucket_scores(),
                    "top_bucket_anomalies": self._empty_top_anomalies(),
                },
            )

        feature_columns = [
            "log_n_total",
            "pro_rate",
            "dup_name_fraction_weighted",
            "delta_log_n_total",
            "abs_delta_pro_rate",
            "blank_org_rate",
        ]

        bucketed_frames: list[pd.DataFrame] = []
        top_frames: list[pd.DataFrame] = []
        active_buckets = 0
        model_reasons: list[str] = []

        for bucket_minutes in self.bucket_minutes:
            bucketed = self._build_bucket_table(
                df=df,
                counts=counts,
                bucket_minutes=int(bucket_minutes),
            )
            if bucketed.empty:
                continue

            eligible_mask = bucketed["is_model_eligible"].fillna(False).astype(bool)
            model_reason = ""

            if int(eligible_mask.sum()) < 8:
                model_reason = "insufficient_model_eligible_buckets"
            else:
                model_input = self._safe_fill_numeric(
                    bucketed.loc[eligible_mask, feature_columns], feature_columns
                )
                scaler = RobustScaler(quantile_range=(10.0, 90.0))
                scaled = scaler.fit_transform(model_input.to_numpy(dtype=float))

                forest = IsolationForest(
                    n_estimators=300,
                    contamination=self.contamination,
                    random_state=self.random_seed,
                    n_jobs=-1,
                )
                forest.fit(scaled)
                decision = forest.decision_function(scaled)
                predictions = forest.predict(scaled)

                anomaly_score = -decision.astype(float)
                percentile = (
                    pd.Series(anomaly_score).rank(method="average", pct=True).to_numpy(dtype=float)
                )

                bucketed.loc[eligible_mask, "anomaly_score"] = anomaly_score
                bucketed.loc[eligible_mask, "anomaly_score_percentile"] = percentile
                bucketed.loc[eligible_mask, "is_anomaly"] = predictions == -1
                active_buckets += 1

            if model_reason:
                model_reasons.append(f"{int(bucket_minutes)}m:{model_reason}")

            eligible_scores = bucketed[bucketed["is_model_eligible"]].copy()
            eligible_scores = eligible_scores.sort_values("anomaly_score", ascending=False)
            top_anomalies = eligible_scores[eligible_scores["is_anomaly"]].copy()
            if top_anomalies.empty:
                top_anomalies = eligible_scores.head(self.top_n).copy()
            else:
                top_anomalies = top_anomalies.head(self.top_n).copy()

            bucketed_frames.append(bucketed)
            if not top_anomalies.empty:
                top_frames.append(top_anomalies)

        if not bucketed_frames:
            return DetectorResult(
                detector=self.name,
                summary={
                    "enabled": True,
                    "active": True,
                    "n_buckets_total": 0,
                    "n_buckets_model_eligible": 0,
                    "n_anomaly_buckets": 0,
                    "bucket_minutes": [int(value) for value in self.bucket_minutes],
                    "contamination": float(self.contamination),
                },
                tables={
                    "bucket_anomaly_scores": self._empty_bucket_scores(),
                    "top_bucket_anomalies": self._empty_top_anomalies(),
                },
            )

        bucketed = (
            pd.concat(bucketed_frames, ignore_index=True)
            .sort_values(["bucket_minutes", "bucket_start"])
            .reset_index(drop=True)
        )
        top_anomalies = (
            pd.concat(top_frames, ignore_index=True)
            .sort_values("anomaly_score", ascending=False)
            .head(self.top_n)
            .reset_index(drop=True)
            if top_frames
            else self._empty_top_anomalies()
        )

        summary = {
            "enabled": True,
            "active": bool(active_buckets > 0),
            "reason": "" if active_buckets > 0 else "insufficient_model_eligible_buckets",
            "model": "isolation_forest",
            "feature_columns": feature_columns,
            "bucket_minutes": [int(value) for value in self.bucket_minutes],
            "contamination": float(self.contamination),
            "min_bucket_total": int(self.min_bucket_total),
            "n_buckets_total": int(len(bucketed)),
            "n_buckets_model_eligible": int(bucketed["is_model_eligible"].sum()),
            "n_anomaly_buckets": int(bucketed["is_anomaly"].sum()),
            "n_low_power_buckets": int(bucketed["is_low_power"].sum()),
            "active_bucket_count": int(active_buckets),
            "inactive_bucket_reasons": model_reasons,
            "max_anomaly_score": (
                float(pd.to_numeric(bucketed["anomaly_score"], errors="coerce").max())
                if pd.to_numeric(bucketed["anomaly_score"], errors="coerce").notna().any()
                else 0.0
            ),
        }

        return DetectorResult(
            detector=self.name,
            summary=summary,
            tables={
                "bucket_anomaly_scores": bucketed,
                "top_bucket_anomalies": top_anomalies,
            },
        )
