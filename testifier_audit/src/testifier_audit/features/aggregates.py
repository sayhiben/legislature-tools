from __future__ import annotations

import pandas as pd


def build_counts_per_minute(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby("minute_bucket", dropna=True)
        .agg(
            n_total=("id", "count"),
            n_unique_names=("canonical_name", "nunique"),
            n_pro=("position_normalized", lambda s: int((s == "Pro").sum())),
            n_con=("position_normalized", lambda s: int((s == "Con").sum())),
            n_unknown=("position_normalized", lambda s: int((s == "Unknown").sum())),
        )
        .sort_index()
    )

    if grouped.empty:
        return grouped.reset_index()

    full_index = pd.date_range(
        start=grouped.index.min(),
        end=grouped.index.max(),
        freq="min",
    )
    grouped = grouped.reindex(full_index, fill_value=0)
    grouped.index.name = "minute_bucket"

    grouped = grouped.reset_index()
    grouped["dup_name_fraction"] = 0.0
    nonzero_total = grouped["n_total"] > 0
    grouped.loc[nonzero_total, "dup_name_fraction"] = (
        1 - (grouped.loc[nonzero_total, "n_unique_names"] / grouped.loc[nonzero_total, "n_total"])
    )

    grouped["pro_rate"] = (grouped["n_pro"] / grouped["n_total"]).where(nonzero_total)
    grouped["con_rate"] = (grouped["n_con"] / grouped["n_total"]).where(nonzero_total)
    grouped["unique_ratio"] = (grouped["n_unique_names"] / grouped["n_total"]).where(nonzero_total)
    return grouped


def build_name_frequency(df: pd.DataFrame) -> pd.DataFrame:
    aggregated = (
        df.groupby("canonical_name", dropna=True)
        .agg(
            n=("id", "count"),
            n_pro=("position_normalized", lambda s: int((s == "Pro").sum())),
            n_con=("position_normalized", lambda s: int((s == "Con").sum())),
            n_unknown=("position_normalized", lambda s: int((s == "Unknown").sum())),
            first_seen=("timestamp", "min"),
            last_seen=("timestamp", "max"),
            display_name=("name_display", "first"),
        )
        .reset_index()
    )

    aggregated["time_span_minutes"] = (
        (aggregated["last_seen"] - aggregated["first_seen"]).dt.total_seconds() / 60.0
    ).fillna(0.0)
    aggregated["positions"] = aggregated.apply(
        lambda row: ",".join(
            label
            for label, count in (("Con", row["n_con"]), ("Pro", row["n_pro"]), ("Unknown", row["n_unknown"]))
            if int(count) > 0
        ),
        axis=1,
    )

    return aggregated.sort_values(["n", "canonical_name"], ascending=[False, True])


def build_counts_per_hour(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby(["day_of_week", "hour"], dropna=True)
        .agg(
            n_total=("id", "count"),
            n_pro=("position_normalized", lambda s: int((s == "Pro").sum())),
            n_con=("position_normalized", lambda s: int((s == "Con").sum())),
        )
        .reset_index()
        .sort_values(["day_of_week", "hour"])
    )
    grouped["pro_rate"] = (grouped["n_pro"] / grouped["n_total"]).where(grouped["n_total"] > 0)
    return grouped


def build_basic_quality(df: pd.DataFrame) -> pd.DataFrame:
    metrics = [
        ("rows_total", int(len(df))),
        ("missing_name", int(df["name"].isna().sum() + (df["name"].astype(str).str.strip() == "").sum())),
        (
            "missing_organization",
            int(df["organization"].isna().sum() + (df["organization"].astype(str).str.strip() == "").sum()),
        ),
        ("unknown_position", int((df["position_normalized"] == "Unknown").sum())),
        ("invalid_timestamp", int(df["timestamp"].isna().sum())),
    ]
    return pd.DataFrame(metrics, columns=["metric", "value"])
