from __future__ import annotations

import numpy as np
import pandas as pd

from testifier_audit.detectors.base import Detector, DetectorResult


def _flag_minutes_from_windows(
    base_minutes: pd.Series,
    windows: pd.DataFrame,
    start_col: str,
    end_col: str,
) -> pd.Series:
    flags = np.zeros(len(base_minutes), dtype=float)
    if windows.empty:
        return pd.Series(flags, index=base_minutes.index)

    minute_values = base_minutes.to_numpy()
    for row in windows.itertuples(index=False):
        start = getattr(row, start_col)
        end = getattr(row, end_col)
        mask = (minute_values >= start) & (minute_values <= end)
        flags[mask] = 1.0
    return pd.Series(flags, index=base_minutes.index)


def _flag_minutes_from_points(
    base_minutes: pd.Series,
    points: pd.DataFrame,
    point_col: str,
    radius_minutes: int,
) -> pd.Series:
    flags = np.zeros(len(base_minutes), dtype=float)
    if points.empty:
        return pd.Series(flags, index=base_minutes.index)

    minute_values = pd.to_datetime(base_minutes)
    radius = pd.Timedelta(minutes=radius_minutes)
    for point in pd.to_datetime(points[point_col].dropna()):
        mask = (minute_values >= (point - radius)) & (minute_values <= (point + radius))
        flags[mask.to_numpy()] = 1.0
    return pd.Series(flags, index=base_minutes.index)


def _window_min_metric(
    base_minutes: pd.Series,
    windows: pd.DataFrame,
    start_col: str,
    end_col: str,
    metric_col: str,
) -> pd.Series:
    metric = np.full(len(base_minutes), np.nan, dtype=float)
    if windows.empty or metric_col not in windows.columns:
        return pd.Series(metric, index=base_minutes.index)

    minute_values = base_minutes.to_numpy()
    for row in windows.itertuples(index=False):
        start = getattr(row, start_col)
        end = getattr(row, end_col)
        metric_value = float(getattr(row, metric_col))
        mask = (minute_values >= start) & (minute_values <= end)
        existing = metric[mask]
        metric[mask] = np.where(
            np.isnan(existing), metric_value, np.minimum(existing, metric_value)
        )
    return pd.Series(metric, index=base_minutes.index)


class CompositeScoreDetector(Detector):
    name = "composite_score"

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        counts = features.get("counts_per_minute", pd.DataFrame())
        if counts.empty:
            empty = pd.DataFrame()
            return DetectorResult(
                detector=self.name,
                summary={"n_ranked_windows": 0, "n_high_priority_windows": 0},
                tables={"ranked_windows": empty, "high_priority_windows": empty},
            )

        ranked = counts.copy()
        ranked["volume_score"] = ranked["n_total"].rank(method="average", pct=True)
        ranked["duplicate_score"] = ranked["dup_name_fraction"].rank(method="average", pct=True)

        bursts_significant = features.get("bursts.burst_significant_windows", pd.DataFrame())
        swings_significant = features.get("procon_swings.swing_significant_windows", pd.DataFrame())
        rare_unique_windows = features.get("rare_names.unique_ratio_windows", pd.DataFrame())
        rare_rarity_windows = features.get("rare_names.rarity_high_windows", pd.DataFrame())
        volume_changepoints = features.get("changepoints.volume_changepoints", pd.DataFrame())
        pro_rate_changepoints = features.get("changepoints.pro_rate_changepoints", pd.DataFrame())
        multivariate_scores = features.get(
            "multivariate_anomalies.bucket_anomaly_scores", pd.DataFrame()
        )

        ranked["burst_signal"] = _flag_minutes_from_windows(
            ranked["minute_bucket"],
            bursts_significant,
            "start_minute",
            "end_minute",
        )
        ranked["swing_signal"] = _flag_minutes_from_windows(
            ranked["minute_bucket"],
            swings_significant,
            "start_minute",
            "end_minute",
        )
        ranked["burst_q_value_min"] = _window_min_metric(
            ranked["minute_bucket"],
            bursts_significant,
            "start_minute",
            "end_minute",
            "q_value",
        )
        ranked["swing_q_value_min"] = _window_min_metric(
            ranked["minute_bucket"],
            swings_significant,
            "start_minute",
            "end_minute",
            "q_value",
        )

        ranked["unique_signal"] = 0.0
        if not rare_unique_windows.empty and "minute_bucket" in rare_unique_windows.columns:
            flagged_minutes = set(rare_unique_windows["minute_bucket"].tolist())
            ranked["unique_signal"] = ranked["minute_bucket"].isin(flagged_minutes).astype(float)

        ranked["rarity_signal"] = 0.0
        if not rare_rarity_windows.empty and "minute_bucket" in rare_rarity_windows.columns:
            rarity_minutes = set(rare_rarity_windows["minute_bucket"].tolist())
            ranked["rarity_signal"] = ranked["minute_bucket"].isin(rarity_minutes).astype(float)

        ranked["changepoint_signal"] = 0.0
        ranked["changepoint_signal"] = np.maximum(
            _flag_minutes_from_points(
                ranked["minute_bucket"],
                volume_changepoints,
                "change_minute",
                radius_minutes=15,
            ),
            _flag_minutes_from_points(
                ranked["minute_bucket"],
                pro_rate_changepoints,
                "change_minute",
                radius_minutes=15,
            ),
        )

        ranked["ml_anomaly_signal"] = 0.0
        ranked["ml_anomaly_score_pct"] = 0.0
        if not multivariate_scores.empty and {"bucket_start", "anomaly_score"}.issubset(
            set(multivariate_scores.columns)
        ):
            bucketed_scores = multivariate_scores.copy()
            bucketed_scores["bucket_start"] = pd.to_datetime(
                bucketed_scores["bucket_start"], errors="coerce"
            )
            bucketed_scores["anomaly_score"] = pd.to_numeric(
                bucketed_scores["anomaly_score"],
                errors="coerce",
            )
            bucketed_scores = bucketed_scores.dropna(subset=["bucket_start", "anomaly_score"])

            if not bucketed_scores.empty:
                score_rank = (
                    bucketed_scores["anomaly_score"]
                    .rank(method="average", pct=True)
                    .astype(float)
                    .to_numpy(dtype=float)
                )
                bucketed_scores["anomaly_score_pct"] = score_rank

                if "is_anomaly" in bucketed_scores.columns:
                    bucketed_scores["is_anomaly"] = (
                        bucketed_scores["is_anomaly"].fillna(False).astype(bool)
                    )
                else:
                    bucketed_scores["is_anomaly"] = False

                bucket_minutes = 1
                if (
                    "bucket_minutes" in bucketed_scores.columns
                    and bucketed_scores["bucket_minutes"].notna().any()
                ):
                    bucket_minutes = max(1, int(bucketed_scores["bucket_minutes"].dropna().iloc[0]))

                score_map = dict(
                    zip(
                        bucketed_scores["bucket_start"].tolist(),
                        bucketed_scores["anomaly_score_pct"].tolist(),
                    )
                )
                flag_map = dict(
                    zip(
                        bucketed_scores["bucket_start"].tolist(),
                        bucketed_scores["is_anomaly"].tolist(),
                    )
                )

                minute_floor = pd.to_datetime(ranked["minute_bucket"], errors="coerce").dt.floor(
                    f"{bucket_minutes}min"
                )
                ranked["ml_anomaly_score_pct"] = (
                    minute_floor.map(score_map).fillna(0.0).astype(float)
                )
                ranked["ml_anomaly_signal"] = (
                    minute_floor.map(flag_map).fillna(False).astype(bool).astype(float)
                )

        ranked["composite_score"] = (
            0.26 * ranked["volume_score"]
            + 0.18 * ranked["duplicate_score"]
            + 0.14 * ranked["burst_signal"]
            + 0.14 * ranked["swing_signal"]
            + 0.09 * ranked["changepoint_signal"]
            + 0.05 * ranked["unique_signal"]
            + 0.05 * ranked["rarity_signal"]
            + 0.09 * ranked["ml_anomaly_score_pct"]
        )

        ranked = ranked.sort_values("composite_score", ascending=False)
        high_priority = ranked[ranked["composite_score"] >= 0.85]
        evidence_bundle = (
            ranked.loc[
                :,
                [
                    "minute_bucket",
                    "n_total",
                    "pro_rate",
                    "dup_name_fraction",
                    "composite_score",
                    "burst_signal",
                    "burst_q_value_min",
                    "swing_signal",
                    "swing_q_value_min",
                    "changepoint_signal",
                    "unique_signal",
                    "rarity_signal",
                    "ml_anomaly_signal",
                    "ml_anomaly_score_pct",
                ],
            ]
            .assign(
                evidence_count=lambda frame: (
                    frame["burst_signal"]
                    + frame["swing_signal"]
                    + frame["changepoint_signal"]
                    + frame["unique_signal"]
                    + frame["rarity_signal"]
                    + frame["ml_anomaly_signal"]
                ).astype(int),
                evidence_flags=lambda frame: frame.apply(
                    lambda row: ",".join(
                        name
                        for name, signal in (
                            ("burst", row["burst_signal"]),
                            ("swing", row["swing_signal"]),
                            ("changepoint", row["changepoint_signal"]),
                            ("unique_spike", row["unique_signal"]),
                            ("rarity_spike", row["rarity_signal"]),
                            ("multivariate", row["ml_anomaly_signal"]),
                        )
                        if float(signal) > 0.0
                    ),
                    axis=1,
                ),
            )
            .query("evidence_count > 0")
            .sort_values(["evidence_count", "composite_score"], ascending=[False, False])
            .head(500)
        )

        summary = {
            "n_ranked_windows": int(len(ranked)),
            "n_high_priority_windows": int(len(high_priority)),
            "n_evidence_bundle_windows": int(len(evidence_bundle)),
            "max_composite_score": float(ranked["composite_score"].max())
            if not ranked.empty
            else 0.0,
        }

        return DetectorResult(
            detector=self.name,
            summary=summary,
            tables={
                "ranked_windows": ranked.head(500),
                "high_priority_windows": high_priority.head(500),
                "evidence_bundle_windows": evidence_bundle,
            },
        )
