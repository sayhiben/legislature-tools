from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency

from testifier_audit.detectors.base import Detector, DetectorResult
from testifier_audit.proportion_stats import (
    DEFAULT_LOW_POWER_MIN_TOTAL,
    low_power_mask,
    wilson_half_width,
    wilson_interval,
)


class OffHoursDetector(Detector):
    name = "off_hours"

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        if df.empty:
            empty = pd.DataFrame()
            return DetectorResult(
                detector=self.name,
                summary={"off_hours_ratio": 0.0, "chi_square_p_value": 1.0},
                tables={
                    "off_hours_summary": empty,
                    "hourly_distribution": empty,
                    "hour_of_week_distribution": empty,
                },
            )

        working = df.copy()
        working["is_pro"] = working["position_normalized"] == "Pro"
        working["is_con"] = working["position_normalized"] == "Con"

        off_hours = working[working["is_off_hours"]]
        on_hours = working[~working["is_off_hours"]]

        total = int(len(working))
        off_count = int(len(off_hours))
        on_count = int(len(on_hours))

        off_pro = int(off_hours["is_pro"].sum())
        off_con = int(off_hours["is_con"].sum())
        on_pro = int(on_hours["is_pro"].sum())
        on_con = int(on_hours["is_con"].sum())
        off_known_total = int(off_pro + off_con)
        on_known_total = int(on_pro + on_con)
        low_power_min_total = DEFAULT_LOW_POWER_MIN_TOTAL

        p_value = 1.0
        contingency = np.array([[off_pro, off_con], [on_pro, on_con]], dtype=float)
        if (
            contingency.sum() > 0
            and contingency.shape == (2, 2)
            and (contingency.sum(axis=1) > 0).all()
        ):
            _chi2, p_value, _dof, _expected = chi2_contingency(contingency, correction=False)

        summary_table = pd.DataFrame(
            [
                {
                    "total": total,
                    "off_hours": off_count,
                    "on_hours": on_count,
                    "off_hours_ratio": (off_count / total) if total else 0.0,
                    "off_hours_pro_rate": (off_pro / off_known_total)
                    if off_known_total
                    else np.nan,
                    "on_hours_pro_rate": (on_pro / on_known_total) if on_known_total else np.nan,
                    "chi_square_p_value": p_value,
                }
            ]
        )
        off_low, off_high = wilson_interval(
            successes=pd.Series([off_pro]),
            totals=pd.Series([off_known_total]),
        )
        on_low, on_high = wilson_interval(
            successes=pd.Series([on_pro]),
            totals=pd.Series([on_known_total]),
        )
        summary_table["off_hours_pro_rate_wilson_low"] = float(off_low[0])
        summary_table["off_hours_pro_rate_wilson_high"] = float(off_high[0])
        summary_table["off_hours_pro_rate_wilson_half_width"] = float(
            wilson_half_width(
                successes=pd.Series([off_pro]),
                totals=pd.Series([off_known_total]),
            )[0]
        )
        summary_table["on_hours_pro_rate_wilson_low"] = float(on_low[0])
        summary_table["on_hours_pro_rate_wilson_high"] = float(on_high[0])
        summary_table["on_hours_pro_rate_wilson_half_width"] = float(
            wilson_half_width(
                successes=pd.Series([on_pro]),
                totals=pd.Series([on_known_total]),
            )[0]
        )
        summary_table["off_hours_is_low_power"] = bool(
            low_power_mask(
                totals=pd.Series([off_known_total]),
                min_total=low_power_min_total,
            )[0]
        )
        summary_table["on_hours_is_low_power"] = bool(
            low_power_mask(
                totals=pd.Series([on_known_total]),
                min_total=low_power_min_total,
            )[0]
        )

        hourly_distribution = (
            working.groupby("hour", dropna=True)
            .agg(
                n_total=("id", "count"),
                n_pro=("is_pro", "sum"),
                n_con=("is_con", "sum"),
            )
            .reset_index()
            .sort_values("hour")
        )
        hourly_distribution["pro_rate"] = (
            hourly_distribution["n_pro"]
            / (hourly_distribution["n_pro"] + hourly_distribution["n_con"])
        ).where((hourly_distribution["n_pro"] + hourly_distribution["n_con"]) > 0)
        hourly_distribution["pro_rate_wilson_low"], hourly_distribution["pro_rate_wilson_high"] = (
            wilson_interval(
                successes=hourly_distribution["n_pro"],
                totals=hourly_distribution["n_pro"] + hourly_distribution["n_con"],
            )
        )
        hourly_distribution["pro_rate_wilson_half_width"] = wilson_half_width(
            successes=hourly_distribution["n_pro"],
            totals=hourly_distribution["n_pro"] + hourly_distribution["n_con"],
        )
        hourly_distribution["is_low_power"] = low_power_mask(
            totals=hourly_distribution["n_pro"] + hourly_distribution["n_con"],
            min_total=low_power_min_total,
        )

        day_name_lookup = {
            0: "Monday",
            1: "Tuesday",
            2: "Wednesday",
            3: "Thursday",
            4: "Friday",
            5: "Saturday",
            6: "Sunday",
        }
        day_index_lookup = {value: key for key, value in day_name_lookup.items()}

        day_labels = pd.Series(["Unknown"] * len(working), index=working.index, dtype="string")
        day_indices = pd.Series([-1] * len(working), index=working.index, dtype="int64")
        timestamps = (
            pd.to_datetime(working["timestamp"], errors="coerce")
            if "timestamp" in working.columns
            else pd.Series(pd.NaT, index=working.index)
        )
        has_valid_timestamps = bool(timestamps.notna().any())
        if has_valid_timestamps:
            day_labels = timestamps.dt.day_name().fillna("Unknown")
            day_indices = timestamps.dt.dayofweek.fillna(-1).astype(int)
        elif "day_of_week" in working.columns:
            day_values = working["day_of_week"]
            numeric_days = pd.to_numeric(day_values, errors="coerce")
            numeric_mask = numeric_days.notna()
            if numeric_mask.any():
                day_labels = numeric_days.astype("Int64").map(day_name_lookup).fillna("Unknown")
                day_indices = numeric_days.fillna(-1).astype(int)
            else:
                text_days = day_values.astype("string").fillna("Unknown")
                normalized_days = text_days.str.strip().str.title()
                day_labels = normalized_days.where(normalized_days != "", "Unknown")
                day_indices = (
                    day_labels.map(day_index_lookup)
                    .fillna(-1)
                    .astype(int)
                )

        record_counter = "id" if "id" in working.columns else "hour"
        hour_of_week_distribution = (
            working.assign(
                day_of_week=day_labels,
                day_of_week_index=day_indices,
            )
            .groupby(["day_of_week", "day_of_week_index", "hour"], dropna=False)
            .agg(
                n_total=(record_counter, "count"),
                n_pro=("is_pro", "sum"),
                n_con=("is_con", "sum"),
                n_off_hours=("is_off_hours", "sum"),
            )
            .reset_index()
            .sort_values(["day_of_week_index", "hour", "day_of_week"])
        )
        hour_of_week_distribution["off_hours_fraction"] = (
            hour_of_week_distribution["n_off_hours"]
            / hour_of_week_distribution["n_total"].replace(0, np.nan)
        ).fillna(0.0)
        hour_of_week_distribution["pro_rate"] = (
            hour_of_week_distribution["n_pro"]
            / (hour_of_week_distribution["n_pro"] + hour_of_week_distribution["n_con"])
        ).where((hour_of_week_distribution["n_pro"] + hour_of_week_distribution["n_con"]) > 0)
        (
            hour_of_week_distribution["pro_rate_wilson_low"],
            hour_of_week_distribution["pro_rate_wilson_high"],
        ) = wilson_interval(
            successes=hour_of_week_distribution["n_pro"],
            totals=hour_of_week_distribution["n_pro"] + hour_of_week_distribution["n_con"],
        )
        hour_of_week_distribution["is_low_power"] = low_power_mask(
            totals=hour_of_week_distribution["n_pro"] + hour_of_week_distribution["n_con"],
            min_total=low_power_min_total,
        )
        hour_of_week_distribution = hour_of_week_distribution[
            hour_of_week_distribution["day_of_week"] != "Unknown"
        ].copy()

        return DetectorResult(
            detector=self.name,
            summary={
                "off_hours_ratio": float(summary_table.loc[0, "off_hours_ratio"]),
                "chi_square_p_value": float(p_value),
                "low_power_min_total": int(low_power_min_total),
                "off_hours_is_low_power": bool(summary_table.loc[0, "off_hours_is_low_power"]),
                "on_hours_is_low_power": bool(summary_table.loc[0, "on_hours_is_low_power"]),
                "hour_of_week_cells": int(len(hour_of_week_distribution)),
                "max_hour_of_week_pro_rate": float(
                    pd.to_numeric(hour_of_week_distribution["pro_rate"], errors="coerce")
                    .fillna(0.0)
                    .max()
                )
                if not hour_of_week_distribution.empty
                else 0.0,
            },
            tables={
                "off_hours_summary": summary_table,
                "hourly_distribution": hourly_distribution,
                "hour_of_week_distribution": hour_of_week_distribution,
            },
        )
