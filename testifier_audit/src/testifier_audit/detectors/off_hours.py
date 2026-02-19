from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency

from testifier_audit.detectors.base import Detector, DetectorResult


class OffHoursDetector(Detector):
    name = "off_hours"

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        if df.empty:
            empty = pd.DataFrame()
            return DetectorResult(
                detector=self.name,
                summary={"off_hours_ratio": 0.0, "chi_square_p_value": 1.0},
                tables={"off_hours_summary": empty, "hourly_distribution": empty},
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

        p_value = 1.0
        contingency = np.array([[off_pro, off_con], [on_pro, on_con]], dtype=float)
        if contingency.sum() > 0 and contingency.shape == (2, 2) and (contingency.sum(axis=1) > 0).all():
            _chi2, p_value, _dof, _expected = chi2_contingency(contingency, correction=False)

        summary_table = pd.DataFrame(
            [
                {
                    "total": total,
                    "off_hours": off_count,
                    "on_hours": on_count,
                    "off_hours_ratio": (off_count / total) if total else 0.0,
                    "off_hours_pro_rate": (off_pro / (off_pro + off_con)) if (off_pro + off_con) else np.nan,
                    "on_hours_pro_rate": (on_pro / (on_pro + on_con)) if (on_pro + on_con) else np.nan,
                    "chi_square_p_value": p_value,
                }
            ]
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
            hourly_distribution["n_pro"] / (hourly_distribution["n_pro"] + hourly_distribution["n_con"])
        ).where((hourly_distribution["n_pro"] + hourly_distribution["n_con"]) > 0)

        return DetectorResult(
            detector=self.name,
            summary={
                "off_hours_ratio": float(summary_table.loc[0, "off_hours_ratio"]),
                "chi_square_p_value": float(p_value),
            },
            tables={
                "off_hours_summary": summary_table,
                "hourly_distribution": hourly_distribution,
            },
        )
