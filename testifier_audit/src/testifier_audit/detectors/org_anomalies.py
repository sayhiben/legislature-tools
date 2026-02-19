from __future__ import annotations

import pandas as pd

from testifier_audit.detectors.base import Detector, DetectorResult


class OrganizationAnomaliesDetector(Detector):
    name = "org_anomalies"

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        working = df.copy()
        working["organization_clean"] = (
            working["organization"].fillna("").astype(str).str.strip().str.upper()
        )

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

        summary = {
            "n_nonblank_org_rows": int(len(nonblank)),
            "n_distinct_orgs": int(nonblank["organization_clean"].nunique()),
            "nonblank_org_ratio": float(len(nonblank) / len(working)) if len(working) else 0.0,
            "max_org_minute_count": int(minute_org_counts["n"].max()) if not minute_org_counts.empty else 0,
        }

        return DetectorResult(
            detector=self.name,
            summary=summary,
            tables={
                "organization_counts": org_counts,
                "organization_minute_bursts": org_bursts,
            },
        )
