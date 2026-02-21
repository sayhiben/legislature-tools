from __future__ import annotations

import pandas as pd

from testifier_audit.proportion_stats import (
    DEFAULT_LOW_POWER_MIN_TOTAL,
    low_power_mask,
    wilson_half_width,
    wilson_interval,
)


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
    grouped.loc[nonzero_total, "dup_name_fraction"] = 1 - (
        grouped.loc[nonzero_total, "n_unique_names"] / grouped.loc[nonzero_total, "n_total"]
    )

    dedup_subset = ["minute_bucket", "canonical_name"]
    if "canonical_name" not in df.columns:
        dedup_subset = ["minute_bucket", "id"] if "id" in df.columns else ["minute_bucket"]

    dedup_sort_columns = [
        column
        for column in ("minute_bucket", "timestamp", "id")
        if column in df.columns
    ]
    dedup_source = df.sort_values(dedup_sort_columns) if dedup_sort_columns else df.copy()
    dedup_source = dedup_source.drop_duplicates(subset=dedup_subset, keep="first")
    dedup_unique_column = "canonical_name" if "canonical_name" in dedup_source.columns else "id"
    dedup_grouped = (
        dedup_source.groupby("minute_bucket", dropna=True)
        .agg(
            n_total_dedup=("id", "count"),
            n_pro_dedup=("position_normalized", lambda s: int((s == "Pro").sum())),
            n_con_dedup=("position_normalized", lambda s: int((s == "Con").sum())),
            n_unknown_dedup=("position_normalized", lambda s: int((s == "Unknown").sum())),
            n_unique_names_dedup=(dedup_unique_column, "nunique"),
        )
        .sort_index()
    )
    dedup_grouped = dedup_grouped.reindex(full_index, fill_value=0)
    dedup_grouped.index.name = "minute_bucket"
    dedup_grouped = dedup_grouped.reset_index()

    grouped = grouped.merge(dedup_grouped, on="minute_bucket", how="left")
    for column in (
        "n_total_dedup",
        "n_pro_dedup",
        "n_con_dedup",
        "n_unknown_dedup",
        "n_unique_names_dedup",
    ):
        grouped[column] = pd.to_numeric(grouped[column], errors="coerce").fillna(0.0)
    nonzero_total_dedup = grouped["n_total_dedup"] > 0

    grouped["pro_rate"] = (grouped["n_pro"] / grouped["n_total"]).where(nonzero_total)
    grouped["con_rate"] = (grouped["n_con"] / grouped["n_total"]).where(nonzero_total)
    grouped["unique_ratio"] = (grouped["n_unique_names"] / grouped["n_total"]).where(nonzero_total)
    grouped["pro_rate_dedup"] = (grouped["n_pro_dedup"] / grouped["n_total_dedup"]).where(
        nonzero_total_dedup
    )
    grouped["con_rate_dedup"] = (grouped["n_con_dedup"] / grouped["n_total_dedup"]).where(
        nonzero_total_dedup
    )
    grouped["unique_ratio_dedup"] = (
        grouped["n_unique_names_dedup"] / grouped["n_total_dedup"]
    ).where(nonzero_total_dedup)
    grouped["dup_name_fraction_dedup"] = 1 - grouped["unique_ratio_dedup"].fillna(1.0)
    grouped["dedup_drop_fraction"] = (
        (grouped["n_total"] - grouped["n_total_dedup"]) / grouped["n_total"]
    ).where(nonzero_total)
    grouped["dedup_multiplier"] = (grouped["n_total"] / grouped["n_total_dedup"]).where(
        nonzero_total_dedup
    )
    grouped["pro_rate_wilson_low"], grouped["pro_rate_wilson_high"] = wilson_interval(
        successes=grouped["n_pro"],
        totals=grouped["n_total"],
    )
    grouped["pro_rate_wilson_half_width"] = wilson_half_width(
        successes=grouped["n_pro"],
        totals=grouped["n_total"],
    )
    grouped["is_low_power"] = low_power_mask(
        totals=grouped["n_total"],
        min_total=DEFAULT_LOW_POWER_MIN_TOTAL,
    )
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
            for label, count in (
                ("Con", row["n_con"]),
                ("Pro", row["n_pro"]),
                ("Unknown", row["n_unknown"]),
            )
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
    grouped["pro_rate_wilson_low"], grouped["pro_rate_wilson_high"] = wilson_interval(
        successes=grouped["n_pro"],
        totals=grouped["n_total"],
    )
    grouped["pro_rate_wilson_half_width"] = wilson_half_width(
        successes=grouped["n_pro"],
        totals=grouped["n_total"],
    )
    grouped["is_low_power"] = low_power_mask(
        totals=grouped["n_total"],
        min_total=DEFAULT_LOW_POWER_MIN_TOTAL,
    )
    return grouped


def build_basic_quality(df: pd.DataFrame) -> pd.DataFrame:
    duplicate_id_count = 0
    if "id" in df.columns:
        duplicate_id_count = int(df["id"].duplicated(keep=False).sum())

    non_monotonic_timestamp_vs_id = 0
    if "id" in df.columns and "timestamp" in df.columns:
        id_numeric = pd.to_numeric(df["id"], errors="coerce")
        timestamps = pd.to_datetime(df["timestamp"], errors="coerce")
        monotonic_input = pd.DataFrame({"id_numeric": id_numeric, "timestamp": timestamps}).dropna()
        if len(monotonic_input) > 1:
            monotonic_input = monotonic_input.sort_values(
                ["id_numeric", "timestamp"],
                kind="mergesort",
            ).reset_index(drop=True)
            diffs = monotonic_input["timestamp"].diff()
            non_monotonic_timestamp_vs_id = int(
                (diffs < pd.Timedelta(0)).fillna(False).sum()
            )

    metrics = [
        ("rows_total", int(len(df))),
        (
            "missing_name",
            int(df["name"].isna().sum() + (df["name"].astype(str).str.strip() == "").sum()),
        ),
        (
            "missing_organization",
            int(
                df["organization"].isna().sum()
                + (df["organization"].astype(str).str.strip() == "").sum()
            ),
        ),
        ("unknown_position", int((df["position_normalized"] == "Unknown").sum())),
        ("invalid_timestamp", int(df["timestamp"].isna().sum())),
        ("duplicate_ids", duplicate_id_count),
        ("non_monotonic_timestamp_vs_id", non_monotonic_timestamp_vs_id),
    ]
    return pd.DataFrame(metrics, columns=["metric", "value"])
