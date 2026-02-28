from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from testifier_audit.detectors.base import Detector, DetectorResult
from testifier_audit.proportion_stats import DEFAULT_LOW_POWER_MIN_TOTAL

from .off_hours_pipeline import build_window_control_profile, select_primary_bucket
from .off_hours_statistics import InferenceConfig
from .off_hours_tables import build_off_hours_tables_and_summary


class OffHoursDetector(Detector):
    name = "off_hours"
    DEFAULT_BUCKET_MINUTES = (1, 5, 15, 30, 60, 120, 240, 480, 720, 1440)

    def __init__(
        self,
        *,
        bucket_minutes: Iterable[int] | None = None,
        min_window_total: int = DEFAULT_LOW_POWER_MIN_TOTAL,
        fdr_alpha: float = 0.05,
        primary_bucket_minutes: int = 30,
        model_min_rows: int = 24,
        model_hour_harmonics: int = 3,
        alert_off_hours_min_fraction: float = 1.0,
        primary_alert_min_abs_delta: float = 0.03,
    ) -> None:
        if bucket_minutes is None:
            resolved_buckets = list(self.DEFAULT_BUCKET_MINUTES)
        else:
            resolved_buckets = sorted({int(value) for value in bucket_minutes if int(value) > 0})
        self.bucket_minutes = tuple(int(value) for value in resolved_buckets)
        self.min_window_total = max(1, int(min_window_total))
        self.fdr_alpha = float(min(0.5, max(1e-6, fdr_alpha)))
        self.primary_bucket_minutes = int(primary_bucket_minutes)
        self.model_min_rows = max(8, int(model_min_rows))
        self.model_hour_harmonics = max(1, min(6, int(model_hour_harmonics)))
        self.alert_off_hours_min_fraction = float(
            min(1.0, max(0.5, alert_off_hours_min_fraction))
        )
        self.primary_alert_min_abs_delta = float(min(1.0, max(0.0, primary_alert_min_abs_delta)))

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        _ = features
        if df.empty:
            empty = pd.DataFrame()
            return DetectorResult(
                detector=self.name,
                summary={"off_hours_ratio": 0.0, "chi_square_p_value": 1.0},
                tables={
                    "off_hours_summary": empty,
                    "hourly_distribution": empty,
                    "hour_of_week_distribution": empty,
                    "date_hour_distribution": empty,
                    "date_hour_primary_residual_distribution": empty,
                    "window_control_profile": empty,
                    "model_fit_diagnostics": empty,
                    "flag_channel_summary": empty,
                    "flagged_window_diagnostics": empty,
                },
            )

        working = df.copy()
        if "is_off_hours" not in working.columns:
            working["is_off_hours"] = False
        working["is_pro"] = working["position_normalized"] == "Pro"
        working["is_con"] = working["position_normalized"] == "Con"

        inference_config = InferenceConfig(
            min_window_total=self.min_window_total,
            fdr_alpha=self.fdr_alpha,
            model_min_rows=self.model_min_rows,
            model_hour_harmonics=self.model_hour_harmonics,
            alert_off_hours_min_fraction=self.alert_off_hours_min_fraction,
            primary_alert_min_abs_delta=self.primary_alert_min_abs_delta,
        )

        window_control_profile = build_window_control_profile(
            working,
            bucket_minutes=self.bucket_minutes,
            config=inference_config,
        )
        primary_bucket_minutes, primary_bucket_profile = select_primary_bucket(
            window_control_profile,
            primary_bucket_minutes=self.primary_bucket_minutes,
        )

        summary, tables = build_off_hours_tables_and_summary(
            working,
            window_control_profile=window_control_profile,
            primary_bucket_minutes=primary_bucket_minutes,
            primary_bucket_profile=primary_bucket_profile,
            min_window_total=self.min_window_total,
            fdr_alpha=self.fdr_alpha,
            model_min_rows=self.model_min_rows,
            model_hour_harmonics=self.model_hour_harmonics,
            alert_off_hours_min_fraction=self.alert_off_hours_min_fraction,
            primary_alert_min_abs_delta=self.primary_alert_min_abs_delta,
        )

        return DetectorResult(
            detector=self.name,
            summary=summary,
            tables=tables,
        )
