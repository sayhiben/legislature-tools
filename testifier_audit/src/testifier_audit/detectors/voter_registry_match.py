from __future__ import annotations

import json
from itertools import combinations
from hashlib import sha1
from pathlib import Path
from time import perf_counter

import pandas as pd

from testifier_audit.detectors.base import Detector, DetectorResult
from testifier_audit.io.vrdb_postgres import (
    count_registry_rows,
    fetch_matching_voter_names,
    fetch_voter_candidates_by_last_name,
)
from testifier_audit.names.linkage import (
    LinkageThresholds,
    classify_name_linkage,
    split_canonical_name,
)
from testifier_audit.names.stat_tests import fisher_pairwise_rate_test
from testifier_audit.names.nickname_map import load_nickname_map
from testifier_audit.profiling import (
    record_runtime_counter,
    record_runtime_timing,
)
from testifier_audit.proportion_stats import (
    DEFAULT_LOW_POWER_MIN_TOTAL,
    low_power_mask,
    wilson_interval,
)


PRIMARY_OUTCOMES = ("matched_unique", "matched_ambiguous", "unmatched")
MODE_TO_OUTCOME_COLUMN = {
    "conservative": "primary_outcome",
    "balanced": "balanced_outcome",
    "broad": "broad_outcome",
}
REPORT_MATCH_MODE_TO_OUTCOME_COLUMN = {
    "strict": "strict_outcome",
    "loose": "loose_outcome",
}
DEFAULT_REPORT_MATCH_MODE = "loose"
STATUS_MODES = {"single", "dual_bounds"}


def _safe_str_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str)


def _display_name_from_canonical(canonical_name: str) -> str:
    last_name, first_name = split_canonical_name(str(canonical_name or ""))
    display_name = f"{last_name}, {first_name}".strip(", ").strip()
    return display_name if display_name else str(canonical_name or "")


class VoterRegistryMatchDetector(Detector):
    name = "voter_registry_match"
    _ATTRIBUTION_CAVEAT = (
        "Voter linkage is supporting evidence only and cannot independently establish identity "
        "or intent."
    )

    def __init__(
        self,
        enabled: bool = False,
        db_url: str | None = None,
        table_name: str = "voter_registry",
        bucket_minutes: int | list[int] | tuple[int, ...] = 30,
        active_only: bool = True,
        low_power_min_total: int = DEFAULT_LOW_POWER_MIN_TOTAL,
        primary_match_mode: str = "conservative",
        strong_fuzzy_min_score: float = 92.0,
        weak_fuzzy_min_score: float = 84.0,
        ambiguous_score_gap: float = 2.0,
        pairwise_alpha: float = 0.05,
        nickname_map_path: str = "",
        status_mode: str = "single",
        registry_snapshot_date: str | None = None,
        lookup_cache_dir: str | None = None,
        lookup_cache_db_snapshot: str | None = None,
    ) -> None:
        self.enabled = bool(enabled)
        self.db_url = db_url
        self.table_name = table_name
        if isinstance(bucket_minutes, (list, tuple, set)):
            parsed = sorted({max(1, int(value)) for value in bucket_minutes if int(value) > 0})
        else:
            parsed = [max(1, int(bucket_minutes))]
        self.bucket_minutes = parsed or [30]
        self.active_only = bool(active_only)
        self.low_power_min_total = max(1, int(low_power_min_total))

        resolved_mode = str(primary_match_mode or "conservative").strip().lower()
        self.primary_match_mode = resolved_mode if resolved_mode in MODE_TO_OUTCOME_COLUMN else "conservative"

        resolved_weak = min(max(float(weak_fuzzy_min_score), 0.0), 100.0)
        resolved_strong = min(max(float(strong_fuzzy_min_score), 0.0), 100.0)
        if resolved_strong < resolved_weak:
            resolved_weak, resolved_strong = resolved_strong, resolved_weak
        self.strong_fuzzy_min_score = resolved_strong
        self.weak_fuzzy_min_score = resolved_weak
        self.ambiguous_score_gap = max(0.0, float(ambiguous_score_gap))
        self.pairwise_alpha = max(0.0, min(float(pairwise_alpha), 1.0))
        self.nickname_map_path = str(nickname_map_path or "").strip()
        resolved_status_mode = str(status_mode or "single").strip().lower()
        self.status_mode = resolved_status_mode if resolved_status_mode in STATUS_MODES else "single"
        self.registry_snapshot_date = str(registry_snapshot_date or "").strip()
        self.lookup_cache_dir = str(lookup_cache_dir or "").strip()
        self.lookup_cache_db_snapshot = str(lookup_cache_db_snapshot or "").strip()

    @staticmethod
    def _empty_tables() -> dict[str, pd.DataFrame]:
        return {
            "linkage_overview": pd.DataFrame(),
            "linkage_by_position_rows": pd.DataFrame(),
            "linkage_by_position_unique": pd.DataFrame(),
            "position_pairwise_tests": pd.DataFrame(),
            "sensitivity_modes": pd.DataFrame(),
            "match_assignments": pd.DataFrame(),
            "match_by_bucket": pd.DataFrame(),
            "match_by_bucket_position": pd.DataFrame(),
            "unmatched_names": pd.DataFrame(),
            "position_bounds": pd.DataFrame(),
            "linkage_overview_bounds": pd.DataFrame(),
        }

    @staticmethod
    def _normalize_candidate_lookup(candidates: pd.DataFrame) -> dict[str, list[dict[str, object]]]:
        if candidates.empty:
            return {}
        if "canonical_name" not in candidates.columns:
            return {}

        has_last_col = "canonical_last" in candidates.columns
        has_first_col = "canonical_first" in candidates.columns
        has_count_col = "n_registry_rows" in candidates.columns

        lookup: dict[str, list[dict[str, object]]] = {}
        for row in candidates.itertuples(index=False):
            canonical_name = str(getattr(row, "canonical_name", "") or "").strip()
            if not canonical_name:
                continue

            parsed_last = ""
            parsed_first = ""
            if not has_last_col or not has_first_col:
                if "|" in canonical_name:
                    parsed_last, parsed_first = canonical_name.split("|", 1)
                    parsed_last = parsed_last.strip()
                    parsed_first = parsed_first.strip()
                else:
                    parsed_last = canonical_name

            last = (
                str(getattr(row, "canonical_last", "") or "").strip()
                if has_last_col
                else parsed_last
            )
            first = (
                str(getattr(row, "canonical_first", "") or "").strip()
                if has_first_col
                else parsed_first
            )
            if not last:
                continue

            registry_rows = 0
            if has_count_col:
                raw_rows = getattr(row, "n_registry_rows", 0)
                try:
                    if pd.notna(raw_rows):
                        registry_rows = int(raw_rows)
                except Exception:
                    try:
                        registry_rows = int(float(raw_rows))
                    except Exception:
                        registry_rows = 0

            lookup.setdefault(last, []).append(
                {
                    "canonical_name": canonical_name,
                    "canonical_first": first,
                    "n_registry_rows": registry_rows,
                }
            )
        return lookup

    def _prepare_bucket_cache(self, working: pd.DataFrame) -> dict[int, dict[str, object]]:
        if working.empty:
            return {}
        minute_bucket = pd.to_datetime(working.get("minute_bucket"), errors="coerce")
        position_normalized = _safe_str_series(working.get("position_normalized")).replace("", "Unknown")
        cache: dict[int, dict[str, object]] = {}
        for bucket_minutes in self.bucket_minutes:
            normalized_bucket = int(bucket_minutes)
            bucket_start = minute_bucket.dt.floor(f"{normalized_bucket}min")
            non_null_mask = bucket_start.notna()
            if not bool(non_null_mask.any()):
                continue
            skeleton_base = pd.DataFrame(
                {
                    "bucket_start": bucket_start[non_null_mask],
                    "position_normalized": position_normalized[non_null_mask],
                }
            )
            skeleton_base["_is_pro"] = (skeleton_base["position_normalized"] == "Pro").astype(int)
            skeleton_base["_is_con"] = (skeleton_base["position_normalized"] == "Con").astype(int)
            by_bucket_skeleton = (
                skeleton_base.groupby("bucket_start", dropna=False)
                .agg(
                    n_total=("position_normalized", "count"),
                    n_pro=("_is_pro", "sum"),
                    n_con=("_is_con", "sum"),
                )
                .reset_index()
                .sort_values("bucket_start")
            )
            by_bucket_position_skeleton = (
                skeleton_base.groupby(["bucket_start", "position_normalized"], dropna=False)
                .agg(n_total=("position_normalized", "count"))
                .reset_index()
                .sort_values(["bucket_start", "position_normalized"])
            )
            cache[normalized_bucket] = {
                "bucket_start": bucket_start,
                "non_null_mask": non_null_mask,
                "by_bucket_skeleton": by_bucket_skeleton,
                "by_bucket_position_skeleton": by_bucket_position_skeleton,
            }
        return cache

    def _lookup_cache_snapshot_token(self) -> str:
        override = str(self.lookup_cache_db_snapshot or "").strip()
        if override:
            return override
        return str(self.registry_snapshot_date or "").strip()

    def _registry_lookup_cache_path(
        self,
        *,
        submission_names: list[str],
        active_only: bool,
    ) -> Path | None:
        cache_dir_value = str(self.lookup_cache_dir or "").strip()
        if not cache_dir_value:
            return None
        snapshot_token = self._lookup_cache_snapshot_token()
        if not snapshot_token:
            # Require an explicit snapshot token to avoid stale cross-snapshot reuse.
            return None
        names_hash = sha1("\n".join(submission_names).encode("utf-8")).hexdigest()
        payload = {
            "version": 1,
            "submission_names_hash": names_hash,
            "active_only": bool(active_only),
            "db_snapshot": snapshot_token,
            "table_name": str(self.table_name),
            "db_url_hash": sha1(str(self.db_url or "").encode("utf-8")).hexdigest(),
        }
        cache_key = sha1(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return Path(cache_dir_value) / f"registry-lookup-{cache_key}.pkl"

    def _load_registry_lookup_cache(
        self,
        cache_path: Path | None,
    ) -> tuple[pd.DataFrame, pd.DataFrame, int] | None:
        if cache_path is None or not cache_path.exists():
            return None
        try:
            payload = pd.read_pickle(cache_path)
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        exact_lookup_frame = payload.get("exact_lookup_frame")
        candidate_frame = payload.get("candidate_frame")
        registry_row_count = payload.get("registry_row_count")
        if not isinstance(exact_lookup_frame, pd.DataFrame):
            return None
        if not isinstance(candidate_frame, pd.DataFrame):
            return None
        try:
            resolved_row_count = int(registry_row_count)
        except Exception:
            return None
        return exact_lookup_frame, candidate_frame, resolved_row_count

    def _save_registry_lookup_cache(
        self,
        cache_path: Path | None,
        *,
        exact_lookup_frame: pd.DataFrame,
        candidate_frame: pd.DataFrame,
        registry_row_count: int,
    ) -> None:
        if cache_path is None:
            return
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "exact_lookup_frame": exact_lookup_frame.copy(),
                "candidate_frame": candidate_frame.copy(),
                "registry_row_count": int(registry_row_count),
            }
            temp_path = cache_path.with_suffix(".tmp")
            pd.to_pickle(payload, temp_path)
            temp_path.replace(cache_path)
        except Exception:
            return

    def _build_linkage_by_position(
        self,
        frame: pd.DataFrame,
        *,
        outcome_column: str,
        unit_label: str,
        position_column: str = "position_normalized",
    ) -> pd.DataFrame:
        started = perf_counter()
        if frame.empty:
            result = pd.DataFrame(
                columns=[
                    "unit",
                    "position_normalized",
                    "n_total",
                    "n_matched_unique",
                    "n_matched_ambiguous",
                    "n_unmatched",
                    "matched_rate",
                    "unmatched_rate",
                    "matched_rate_wilson_low",
                    "matched_rate_wilson_high",
                    "unmatched_rate_wilson_low",
                    "unmatched_rate_wilson_high",
                    "is_low_power",
                ]
            )
            record_runtime_timing(
                "detector.voter_registry_match.build_linkage_by_position",
                (perf_counter() - started) * 1000.0,
            )
            return result

        working = frame.copy()
        working["_is_matched_unique"] = (working[outcome_column] == "matched_unique").astype(int)
        working["_is_matched_ambiguous"] = (working[outcome_column] == "matched_ambiguous").astype(int)
        working["_is_unmatched"] = (working[outcome_column] == "unmatched").astype(int)
        grouped = (
            working.groupby(position_column, dropna=False)
            .agg(
                n_total=(outcome_column, "count"),
                n_matched_unique=("_is_matched_unique", "sum"),
                n_matched_ambiguous=("_is_matched_ambiguous", "sum"),
                n_unmatched=("_is_unmatched", "sum"),
            )
            .reset_index()
            .rename(columns={position_column: "position_normalized"})
        )
        grouped["n_matched"] = grouped["n_matched_unique"] + grouped["n_matched_ambiguous"]
        grouped["matched_rate"] = (grouped["n_matched"] / grouped["n_total"]).where(
            grouped["n_total"] > 0,
            0.0,
        )
        grouped["unmatched_rate"] = (grouped["n_unmatched"] / grouped["n_total"]).where(
            grouped["n_total"] > 0,
            0.0,
        )
        matched_low, matched_high = wilson_interval(
            successes=grouped["n_matched"],
            totals=grouped["n_total"],
        )
        unmatched_low, unmatched_high = wilson_interval(
            successes=grouped["n_unmatched"],
            totals=grouped["n_total"],
        )
        grouped["matched_rate_wilson_low"] = matched_low.astype(float)
        grouped["matched_rate_wilson_high"] = matched_high.astype(float)
        grouped["unmatched_rate_wilson_low"] = unmatched_low.astype(float)
        grouped["unmatched_rate_wilson_high"] = unmatched_high.astype(float)
        grouped["is_low_power"] = low_power_mask(
            totals=grouped["n_total"],
            min_total=self.low_power_min_total,
        )
        grouped["unit"] = unit_label
        grouped["position_normalized"] = _safe_str_series(grouped["position_normalized"]).replace(
            "",
            "Unknown",
        )
        result = grouped[
            [
                "unit",
                "position_normalized",
                "n_total",
                "n_matched_unique",
                "n_matched_ambiguous",
                "n_unmatched",
                "matched_rate",
                "unmatched_rate",
                "matched_rate_wilson_low",
                "matched_rate_wilson_high",
                "unmatched_rate_wilson_low",
                "unmatched_rate_wilson_high",
                "is_low_power",
            ]
        ].sort_values(["position_normalized"])
        record_runtime_timing(
            "detector.voter_registry_match.build_linkage_by_position",
            (perf_counter() - started) * 1000.0,
        )
        record_runtime_counter(
            "detector.voter_registry_match.build_linkage_by_position.rows",
            int(len(result)),
        )
        return result

    def _build_pairwise_tests(self, grouped: pd.DataFrame, *, unit_label: str) -> pd.DataFrame:
        started = perf_counter()
        if grouped.empty:
            record_runtime_timing(
                "detector.voter_registry_match.build_pairwise_tests",
                (perf_counter() - started) * 1000.0,
            )
            return pd.DataFrame()
        rows: list[dict[str, object]] = []
        positions = sorted(
            {
                str(value)
                for value in grouped["position_normalized"].tolist()
                if str(value).strip()
            }
        )
        by_pos = grouped.set_index("position_normalized")
        for left, right in combinations(positions, 2):
            if left not in by_pos.index or right not in by_pos.index:
                continue
            left_total = int(by_pos.at[left, "n_total"])
            right_total = int(by_pos.at[right, "n_total"])
            left_unmatched = int(by_pos.at[left, "n_unmatched"])
            right_unmatched = int(by_pos.at[right, "n_unmatched"])
            stats = fisher_pairwise_rate_test(
                successes_left=left_unmatched,
                total_left=left_total,
                successes_right=right_unmatched,
                total_right=right_total,
            )
            left_low_power = bool(left_total < self.low_power_min_total)
            right_low_power = bool(right_total < self.low_power_min_total)
            rows.append(
                {
                    "unit": unit_label,
                    "position_left": left,
                    "position_right": right,
                    "left_n_total": left_total,
                    "left_n_unmatched": left_unmatched,
                    "left_unmatched_rate": float(stats["left_rate"]),
                    "left_unmatched_wilson_low": float(stats["left_wilson_low"]),
                    "left_unmatched_wilson_high": float(stats["left_wilson_high"]),
                    "right_n_total": right_total,
                    "right_n_unmatched": right_unmatched,
                    "right_unmatched_rate": float(stats["right_rate"]),
                    "right_unmatched_wilson_low": float(stats["right_wilson_low"]),
                    "right_unmatched_wilson_high": float(stats["right_wilson_high"]),
                    "rate_difference": float(stats["rate_difference"]),
                    "odds_ratio": float(stats["odds_ratio"]),
                    "p_value": float(stats["p_value"]),
                    "alpha": float(self.pairwise_alpha),
                    "is_significant": bool(stats["p_value"] <= self.pairwise_alpha),
                    "left_is_low_power": left_low_power,
                    "right_is_low_power": right_low_power,
                    "inference_status": "descriptive_only"
                    if left_low_power or right_low_power
                    else "tested",
                    "test_method": "fisher_exact",
                }
            )
        result = pd.DataFrame(rows)
        record_runtime_timing(
            "detector.voter_registry_match.build_pairwise_tests",
            (perf_counter() - started) * 1000.0,
        )
        record_runtime_counter(
            "detector.voter_registry_match.build_pairwise_tests.rows",
            int(len(result)),
        )
        return result

    def _build_match_by_bucket(
        self,
        working: pd.DataFrame,
        *,
        outcome_column: str,
        bucket_cache: dict[int, dict[str, object]] | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        started = perf_counter()
        bucket_frames: list[pd.DataFrame] = []
        bucket_position_frames: list[pd.DataFrame] = []

        if working.empty:
            record_runtime_timing(
                "detector.voter_registry_match.build_match_by_bucket",
                (perf_counter() - started) * 1000.0,
            )
            return pd.DataFrame(), pd.DataFrame()

        resolved_bucket_cache = bucket_cache or self._prepare_bucket_cache(working)
        outcome_series = _safe_str_series(working.get(outcome_column, pd.Series(dtype=str))).replace(
            "",
            "unmatched",
        )
        position_series = _safe_str_series(working.get("position_normalized", pd.Series(dtype=str))).replace(
            "",
            "Unknown",
        )

        for bucket_minutes in self.bucket_minutes:
            normalized_bucket = int(bucket_minutes)
            cache_entry = resolved_bucket_cache.get(normalized_bucket)
            if cache_entry is None:
                continue
            bucket_start = cache_entry.get("bucket_start")
            non_null_mask = cache_entry.get("non_null_mask")
            by_bucket_skeleton = cache_entry.get("by_bucket_skeleton")
            by_bucket_position_skeleton = cache_entry.get("by_bucket_position_skeleton")
            if (
                not isinstance(bucket_start, pd.Series)
                or not isinstance(non_null_mask, pd.Series)
                or not isinstance(by_bucket_skeleton, pd.DataFrame)
                or by_bucket_skeleton.empty
                or not isinstance(by_bucket_position_skeleton, pd.DataFrame)
                or by_bucket_position_skeleton.empty
            ):
                continue

            mode_base = pd.DataFrame(
                {
                    "bucket_start": bucket_start[non_null_mask],
                    "position_normalized": position_series[non_null_mask],
                    "_outcome": outcome_series[non_null_mask],
                }
            )
            if mode_base.empty:
                continue
            mode_base["_is_matched_unique"] = (mode_base["_outcome"] == "matched_unique").astype(int)
            mode_base["_is_matched_ambiguous"] = (mode_base["_outcome"] == "matched_ambiguous").astype(int)
            mode_base["_is_unmatched"] = (mode_base["_outcome"] == "unmatched").astype(int)

            bucket_mode_counts = (
                mode_base.groupby("bucket_start", dropna=False)
                .agg(
                    n_matched_unique=("_is_matched_unique", "sum"),
                    n_matched_ambiguous=("_is_matched_ambiguous", "sum"),
                    n_unmatched=("_is_unmatched", "sum"),
                )
                .reset_index()
            )
            by_bucket = by_bucket_skeleton.merge(
                bucket_mode_counts,
                on="bucket_start",
                how="left",
            ).sort_values("bucket_start")
            by_bucket["n_matched_unique"] = (
                pd.to_numeric(by_bucket["n_matched_unique"], errors="coerce").fillna(0).astype(int)
            )
            by_bucket["n_matched_ambiguous"] = (
                pd.to_numeric(by_bucket["n_matched_ambiguous"], errors="coerce").fillna(0).astype(int)
            )
            by_bucket["n_unmatched"] = (
                pd.to_numeric(by_bucket["n_unmatched"], errors="coerce").fillna(0).astype(int)
            )
            by_bucket["n_matched"] = by_bucket["n_matched_unique"] + by_bucket["n_matched_ambiguous"]
            by_bucket["matched_rate"] = (by_bucket["n_matched"] / by_bucket["n_total"]).where(
                by_bucket["n_total"] > 0,
                0.0,
            )
            by_bucket["unmatched_rate"] = (by_bucket["n_unmatched"] / by_bucket["n_total"]).where(
                by_bucket["n_total"] > 0,
                0.0,
            )
            matched_low, matched_high = wilson_interval(
                successes=by_bucket["n_matched"],
                totals=by_bucket["n_total"],
            )
            unmatched_low, unmatched_high = wilson_interval(
                successes=by_bucket["n_unmatched"],
                totals=by_bucket["n_total"],
            )
            by_bucket["matched_rate_wilson_low"] = matched_low.astype(float)
            by_bucket["matched_rate_wilson_high"] = matched_high.astype(float)
            by_bucket["unmatched_rate_wilson_low"] = unmatched_low.astype(float)
            by_bucket["unmatched_rate_wilson_high"] = unmatched_high.astype(float)
            by_bucket["is_low_power"] = low_power_mask(
                totals=by_bucket["n_total"],
                min_total=self.low_power_min_total,
            )
            by_bucket["bucket_minutes"] = normalized_bucket
            bucket_frames.append(by_bucket)

            bucket_pos_mode_counts = (
                mode_base.groupby(["bucket_start", "position_normalized"], dropna=False)
                .agg(
                    n_matched_unique=("_is_matched_unique", "sum"),
                    n_matched_ambiguous=("_is_matched_ambiguous", "sum"),
                    n_unmatched=("_is_unmatched", "sum"),
                )
                .reset_index()
            )
            by_bucket_pos = by_bucket_position_skeleton.merge(
                bucket_pos_mode_counts,
                on=["bucket_start", "position_normalized"],
                how="left",
            ).sort_values(["bucket_start", "position_normalized"])
            by_bucket_pos["n_matched_unique"] = (
                pd.to_numeric(by_bucket_pos["n_matched_unique"], errors="coerce").fillna(0).astype(int)
            )
            by_bucket_pos["n_matched_ambiguous"] = (
                pd.to_numeric(by_bucket_pos["n_matched_ambiguous"], errors="coerce").fillna(0).astype(int)
            )
            by_bucket_pos["n_unmatched"] = (
                pd.to_numeric(by_bucket_pos["n_unmatched"], errors="coerce").fillna(0).astype(int)
            )
            by_bucket_pos["n_matched"] = (
                by_bucket_pos["n_matched_unique"] + by_bucket_pos["n_matched_ambiguous"]
            )
            by_bucket_pos["matched_rate"] = (
                by_bucket_pos["n_matched"] / by_bucket_pos["n_total"]
            ).where(by_bucket_pos["n_total"] > 0, 0.0)
            by_bucket_pos["unmatched_rate"] = (
                by_bucket_pos["n_unmatched"] / by_bucket_pos["n_total"]
            ).where(by_bucket_pos["n_total"] > 0, 0.0)
            matched_low_pos, matched_high_pos = wilson_interval(
                successes=by_bucket_pos["n_matched"],
                totals=by_bucket_pos["n_total"],
            )
            unmatched_low_pos, unmatched_high_pos = wilson_interval(
                successes=by_bucket_pos["n_unmatched"],
                totals=by_bucket_pos["n_total"],
            )
            by_bucket_pos["matched_rate_wilson_low"] = matched_low_pos.astype(float)
            by_bucket_pos["matched_rate_wilson_high"] = matched_high_pos.astype(float)
            by_bucket_pos["unmatched_rate_wilson_low"] = unmatched_low_pos.astype(float)
            by_bucket_pos["unmatched_rate_wilson_high"] = unmatched_high_pos.astype(float)
            by_bucket_pos["is_low_power"] = low_power_mask(
                totals=by_bucket_pos["n_total"],
                min_total=self.low_power_min_total,
            )
            by_bucket_pos["bucket_minutes"] = normalized_bucket
            bucket_position_frames.append(by_bucket_pos)

        by_bucket = (
            pd.concat(bucket_frames, ignore_index=True)
            .sort_values(["bucket_minutes", "bucket_start"])
            .reset_index(drop=True)
            if bucket_frames
            else pd.DataFrame()
        )
        by_bucket_position = (
            pd.concat(bucket_position_frames, ignore_index=True)
            .sort_values(["bucket_minutes", "bucket_start", "position_normalized"])
            .reset_index(drop=True)
            if bucket_position_frames
            else pd.DataFrame()
        )
        record_runtime_timing(
            "detector.voter_registry_match.build_match_by_bucket",
            (perf_counter() - started) * 1000.0,
        )
        record_runtime_counter(
            "detector.voter_registry_match.build_match_by_bucket.rows",
            int(len(by_bucket)),
        )
        record_runtime_counter(
            "detector.voter_registry_match.build_match_by_bucket.position_rows",
            int(len(by_bucket_position)),
        )
        return by_bucket, by_bucket_position

    def _run_single(
        self,
        df: pd.DataFrame,
        features: dict[str, pd.DataFrame],
        *,
        active_only: bool,
    ) -> DetectorResult:
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
                summary={"enabled": True, "active": True, "n_rows": 0},
                tables=self._empty_tables(),
            )

        required = {"canonical_name", "position_normalized", "minute_bucket"}
        missing = sorted(column for column in required if column not in df.columns)
        if missing:
            raise ValueError(
                "Missing required columns for voter registry matching: " + ", ".join(missing)
            )

        working_prepare_started = perf_counter()
        working = df.copy()
        working["canonical_name"] = _safe_str_series(working["canonical_name"])
        working = working[working["canonical_name"].isin(["", "|"]) == False].copy()
        working["position_normalized"] = _safe_str_series(working["position_normalized"]).replace(
            "",
            "Unknown",
        )
        working["minute_bucket"] = pd.to_datetime(working["minute_bucket"], errors="coerce")
        working = working.dropna(subset=["minute_bucket"]).copy()
        record_runtime_timing(
            "detector.voter_registry_match.prepare_working",
            (perf_counter() - working_prepare_started) * 1000.0,
        )
        record_runtime_counter("detector.voter_registry_match.rows.working", int(len(working)))
        if working.empty:
            return DetectorResult(
                detector=self.name,
                summary={"enabled": True, "active": True, "n_rows": 0},
                tables=self._empty_tables(),
            )

        submission_index_started = perf_counter()
        submission_names = sorted(
            {
                value
                for value in _safe_str_series(working["canonical_name"]).tolist()
                if value and value != "|"
            }
        )
        submission_name_parts = [split_canonical_name(value) for value in submission_names]
        submission_last_names = sorted(
            {
                last_name
                for last_name, _first_name in submission_name_parts
                if last_name
            }
        )
        record_runtime_timing(
            "detector.voter_registry_match.build_submission_name_index",
            (perf_counter() - submission_index_started) * 1000.0,
        )
        record_runtime_counter(
            "detector.voter_registry_match.submission_names.count",
            int(len(submission_names)),
        )
        record_runtime_counter(
            "detector.voter_registry_match.submission_last_names.count",
            int(len(submission_last_names)),
        )

        fetch_started = perf_counter()
        lookup_cache_path = self._registry_lookup_cache_path(
            submission_names=submission_names,
            active_only=active_only,
        )
        try:
            cached_lookup = self._load_registry_lookup_cache(lookup_cache_path)
            if cached_lookup is not None:
                exact_lookup_frame, candidate_frame, registry_row_count = cached_lookup
                record_runtime_counter("detector.voter_registry_match.fetch_registry_data.cache_hit", 1)
            else:
                record_runtime_counter("detector.voter_registry_match.fetch_registry_data.cache_miss", 1)
                exact_lookup_frame = fetch_matching_voter_names(
                    db_url=self.db_url,
                    table_name=self.table_name,
                    canonical_names=submission_names,
                    active_only=active_only,
                )
                if isinstance(exact_lookup_frame, pd.DataFrame):
                    exact_presence = exact_lookup_frame.copy()
                else:
                    exact_presence = pd.DataFrame(columns=["canonical_name", "n_registry_rows"])
                exact_presence["canonical_name"] = _safe_str_series(
                    exact_presence.get("canonical_name", pd.Series(dtype=str))
                )
                exact_presence["n_registry_rows"] = (
                    pd.to_numeric(exact_presence.get("n_registry_rows", 0), errors="coerce")
                    .fillna(0)
                    .astype(int)
                )
                exact_present_names = set(
                    exact_presence.loc[
                        (exact_presence["canonical_name"] != "") & (exact_presence["n_registry_rows"] > 0),
                        "canonical_name",
                    ].tolist()
                )
                unresolved_last_names = sorted(
                    {
                        last_name
                        for canonical_name, (last_name, _first_name) in zip(
                            submission_names,
                            submission_name_parts,
                            strict=False,
                        )
                        if last_name and canonical_name not in exact_present_names
                    }
                )
                record_runtime_counter(
                    "detector.voter_registry_match.unresolved_last_names.count",
                    int(len(unresolved_last_names)),
                )
                if unresolved_last_names:
                    candidate_frame = fetch_voter_candidates_by_last_name(
                        db_url=self.db_url,
                        table_name=self.table_name,
                        canonical_lasts=unresolved_last_names,
                        active_only=active_only,
                    )
                else:
                    candidate_frame = pd.DataFrame(
                        columns=[
                            "canonical_last",
                            "canonical_first",
                            "canonical_name",
                            "canonical_middle_initial",
                            "canonical_suffix",
                            "canonical_key_strict",
                            "canonical_key_medium",
                            "n_registry_rows",
                        ]
                    )
                registry_row_count = int(
                    count_registry_rows(
                        db_url=self.db_url,
                        table_name=self.table_name,
                        active_only=active_only,
                    )
                )
                self._save_registry_lookup_cache(
                    lookup_cache_path,
                    exact_lookup_frame=exact_lookup_frame,
                    candidate_frame=candidate_frame,
                    registry_row_count=registry_row_count,
                )
            record_runtime_counter(
                "detector.voter_registry_match.fetch_registry_data.cache_enabled",
                1 if lookup_cache_path is not None else 0,
            )
            record_runtime_timing(
                "detector.voter_registry_match.fetch_registry_data",
                (perf_counter() - fetch_started) * 1000.0,
            )
        except Exception as exc:
            record_runtime_timing(
                "detector.voter_registry_match.fetch_registry_data",
                (perf_counter() - fetch_started) * 1000.0,
            )
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

        lookup_normalization_started = perf_counter()
        if not isinstance(exact_lookup_frame, pd.DataFrame):
            exact_lookup_frame = pd.DataFrame(columns=["canonical_name", "n_registry_rows"])
        exact_lookup_frame = exact_lookup_frame.copy()
        exact_lookup_frame["canonical_name"] = _safe_str_series(
            exact_lookup_frame.get("canonical_name", pd.Series(dtype=str))
        )
        exact_lookup_frame["n_registry_rows"] = (
            pd.to_numeric(exact_lookup_frame.get("n_registry_rows", 0), errors="coerce")
            .fillna(0)
            .astype(int)
        )
        exact_lookup_frame = (
            exact_lookup_frame[exact_lookup_frame["canonical_name"] != ""]
            .groupby("canonical_name", dropna=False)
            .agg(n_registry_rows=("n_registry_rows", "max"))
            .reset_index()
        )
        exact_lookup = dict(
            zip(
                exact_lookup_frame["canonical_name"].tolist(),
                exact_lookup_frame["n_registry_rows"].tolist(),
                strict=False,
            )
        )

        if not isinstance(candidate_frame, pd.DataFrame):
            candidate_frame = pd.DataFrame(
                columns=["canonical_last", "canonical_first", "canonical_name", "n_registry_rows"]
            )
        candidate_lookup = self._normalize_candidate_lookup(candidate_frame)
        record_runtime_timing(
            "detector.voter_registry_match.normalize_lookup_frames",
            (perf_counter() - lookup_normalization_started) * 1000.0,
        )
        record_runtime_counter(
            "detector.voter_registry_match.lookup.exact_name_count",
            int(len(exact_lookup)),
        )
        record_runtime_counter(
            "detector.voter_registry_match.lookup.candidate_last_count",
            int(len(candidate_lookup)),
        )

        classify_started = perf_counter()
        nickname_map: dict[str, str] = {}
        if self.nickname_map_path:
            nickname_map = load_nickname_map(self.nickname_map_path)

        assignments = classify_name_linkage(
            submission_names=submission_names,
            exact_lookup=exact_lookup,
            candidate_lookup_by_last=candidate_lookup,
            nickname_map=nickname_map,
            thresholds=LinkageThresholds(
                strong_fuzzy_min_score=self.strong_fuzzy_min_score,
                weak_fuzzy_min_score=self.weak_fuzzy_min_score,
                ambiguous_score_gap=self.ambiguous_score_gap,
            ),
        )
        if assignments.empty:
            assignments = pd.DataFrame(
                columns=[
                    "canonical_name",
                    "match_tier",
                    "primary_outcome",
                    "balanced_outcome",
                    "broad_outcome",
                    "matched_registry_name",
                    "matched_registry_rows",
                    "best_similarity_score",
                    "candidate_pool_size",
                    "is_ambiguous",
                    "match_caveat",
                ]
            )
        record_runtime_timing(
            "detector.voter_registry_match.classify_name_linkage",
            (perf_counter() - classify_started) * 1000.0,
        )

        assignment_normalize_started = perf_counter()
        assignments["canonical_name"] = _safe_str_series(assignments["canonical_name"])
        assignments["matched_registry_name"] = _safe_str_series(assignments["matched_registry_name"])
        assignments["matched_registry_rows"] = (
            pd.to_numeric(assignments["matched_registry_rows"], errors="coerce")
            .fillna(0)
            .astype(int)
        )
        assignments["best_similarity_score"] = pd.to_numeric(
            assignments["best_similarity_score"], errors="coerce"
        )
        assignments["candidate_pool_size"] = (
            pd.to_numeric(assignments["candidate_pool_size"], errors="coerce")
            .fillna(0)
            .astype(int)
        )
        assignments["is_ambiguous"] = assignments["is_ambiguous"].map(bool)
        assignments["match_caveat"] = _safe_str_series(assignments["match_caveat"])

        assignments["match_tier"] = _safe_str_series(assignments.get("match_tier", pd.Series(dtype=str))).replace(
            "", "unmatched"
        )
        for mode, outcome_column in MODE_TO_OUTCOME_COLUMN.items():
            if outcome_column not in assignments.columns:
                assignments[outcome_column] = "unmatched"
            assignments[outcome_column] = _safe_str_series(assignments[outcome_column]).replace("", "unmatched")

        # Strict mode: exact canonical-name matches only.
        assignments["strict_outcome"] = "unmatched"
        strict_mask = assignments["match_tier"] == "exact"
        assignments.loc[strict_mask, "strict_outcome"] = assignments.loc[
            strict_mask, "primary_outcome"
        ]

        # Loose mode: exact + nickname-equivalent first-name matches.
        assignments["loose_outcome"] = "unmatched"
        loose_mask = assignments["match_tier"].isin({"exact", "nickname_exact"})
        assignments.loc[loose_mask, "loose_outcome"] = assignments.loc[
            loose_mask, "primary_outcome"
        ]
        for outcome_column in REPORT_MATCH_MODE_TO_OUTCOME_COLUMN.values():
            assignments[outcome_column] = (
                _safe_str_series(assignments[outcome_column])
                .where(_safe_str_series(assignments[outcome_column]).isin(PRIMARY_OUTCOMES), "unmatched")
                .replace("", "unmatched")
            )
        record_runtime_timing(
            "detector.voter_registry_match.normalize_assignments",
            (perf_counter() - assignment_normalize_started) * 1000.0,
        )
        record_runtime_counter("detector.voter_registry_match.assignments.rows", int(len(assignments)))

        working_merge_started = perf_counter()
        working = working.merge(assignments, on="canonical_name", how="left")
        for mode, outcome_column in MODE_TO_OUTCOME_COLUMN.items():
            if outcome_column not in working.columns:
                working[outcome_column] = "unmatched"
            working[outcome_column] = _safe_str_series(working[outcome_column]).replace("", "unmatched")
            working[f"is_matched_{mode}"] = working[outcome_column].isin(
                {"matched_unique", "matched_ambiguous"}
            )
        for match_mode, outcome_column in REPORT_MATCH_MODE_TO_OUTCOME_COLUMN.items():
            if outcome_column not in working.columns:
                working[outcome_column] = "unmatched"
            working[outcome_column] = (
                _safe_str_series(working[outcome_column])
                .where(_safe_str_series(working[outcome_column]).isin(PRIMARY_OUTCOMES), "unmatched")
                .replace("", "unmatched")
            )
            working[f"is_matched_{match_mode}"] = working[outcome_column].isin(
                {"matched_unique", "matched_ambiguous"}
            )
        record_runtime_timing(
            "detector.voter_registry_match.merge_assignments_into_rows",
            (perf_counter() - working_merge_started) * 1000.0,
        )

        # Name-level position assignment for unique-name inference unit.
        unique_position_started = perf_counter()
        unique_position = (
            working.groupby(["canonical_name", "position_normalized"], dropna=False)
            .size()
            .rename("n_rows")
            .reset_index()
        )
        if unique_position.empty:
            unique_names = pd.DataFrame(
                columns=["canonical_name", "dominant_position", "n_rows", "n_positions"]
            )
        else:
            unique_position = unique_position.sort_values(
                ["canonical_name", "n_rows", "position_normalized"],
                ascending=[True, False, True],
            )
            max_rows = unique_position.groupby("canonical_name", dropna=False)["n_rows"].transform("max")
            top_rows = unique_position[max_rows == unique_position["n_rows"]].copy()

            top_counts = (
                top_rows.groupby("canonical_name", dropna=False)
                .size()
                .rename("n_top_positions")
            )
            dominant_single = (
                top_rows.drop_duplicates("canonical_name", keep="first")
                .set_index("canonical_name")["position_normalized"]
                .rename("dominant_single")
            )
            unique_summary = unique_position.groupby("canonical_name", dropna=False).agg(
                n_rows=("n_rows", "sum"),
                n_positions=("position_normalized", "nunique"),
            )
            unique_summary = unique_summary.join(top_counts, how="left").join(dominant_single, how="left")
            unique_summary["n_top_positions"] = (
                pd.to_numeric(unique_summary["n_top_positions"], errors="coerce")
                .fillna(0)
                .astype(int)
            )
            unique_summary["dominant_position"] = _safe_str_series(
                unique_summary["dominant_single"]
            ).where(unique_summary["n_top_positions"] == 1, "Mixed")
            unique_names = unique_summary.reset_index()[
                ["canonical_name", "dominant_position", "n_rows", "n_positions"]
            ].copy()

        assignments = assignments.merge(unique_names, on="canonical_name", how="left")
        assignments["dominant_position"] = _safe_str_series(
            assignments.get("dominant_position", pd.Series(dtype=str))
        ).replace("", "Unknown")
        assignments["n_rows"] = (
            pd.to_numeric(assignments.get("n_rows", 0), errors="coerce").fillna(0).astype(int)
        )
        assignments["n_positions"] = (
            pd.to_numeric(assignments.get("n_positions", 0), errors="coerce").fillna(0).astype(int)
        )
        record_runtime_timing(
            "detector.voter_registry_match.derive_unique_positions",
            (perf_counter() - unique_position_started) * 1000.0,
        )

        def _build_unmatched_names(
            *,
            source: pd.DataFrame,
            match_mode: str,
        ) -> pd.DataFrame:
            if source.empty:
                return pd.DataFrame(
                    columns=[
                        "match_mode",
                        "display_name",
                        "canonical_name",
                        "n_rows",
                        "n_pro",
                        "n_con",
                        "first_seen",
                        "last_seen",
                        "top_caveat",
                        "best_similarity_score",
                        "candidate_pool_size",
                    ]
                )
            working_source = source.copy()
            if "display_name" in working_source.columns:
                working_source["display_name"] = _safe_str_series(working_source["display_name"])
            elif "name_display" in working_source.columns:
                working_source["display_name"] = _safe_str_series(working_source["name_display"])
            else:
                working_source["display_name"] = ""
            working_source["display_name"] = working_source["display_name"].where(
                working_source["display_name"].str.strip() != "",
                working_source["canonical_name"].map(_display_name_from_canonical),
            )
            grouped = (
                working_source.groupby("canonical_name", dropna=False)
                .agg(
                    display_name=(
                        "display_name",
                        lambda series: next(
                            (value for value in _safe_str_series(series).tolist() if value.strip()),
                            "",
                        ),
                    ),
                    n_rows=("canonical_name", "count"),
                    n_pro=("position_normalized", lambda series: int((series == "Pro").sum())),
                    n_con=("position_normalized", lambda series: int((series == "Con").sum())),
                    first_seen=("minute_bucket", "min"),
                    last_seen=("minute_bucket", "max"),
                    top_caveat=(
                        "match_caveat",
                        lambda series: str(series.mode().iloc[0]) if not series.mode().empty else "",
                    ),
                    best_similarity_score=("best_similarity_score", "max"),
                    candidate_pool_size=("candidate_pool_size", "max"),
                )
                .reset_index()
                .sort_values("n_rows", ascending=False)
            )
            grouped["match_mode"] = str(match_mode)
            return grouped

        linkage_overview_frames: list[pd.DataFrame] = []
        linkage_by_position_rows_frames: list[pd.DataFrame] = []
        linkage_by_position_unique_frames: list[pd.DataFrame] = []
        position_pairwise_frames: list[pd.DataFrame] = []
        match_by_bucket_frames: list[pd.DataFrame] = []
        match_by_bucket_position_frames: list[pd.DataFrame] = []
        unmatched_names_frames: list[pd.DataFrame] = []
        sensitivity_rows: list[dict[str, object]] = []
        bucket_cache_started = perf_counter()
        bucket_cache = self._prepare_bucket_cache(working)
        record_runtime_timing(
            "detector.voter_registry_match.prepare_bucket_cache",
            (perf_counter() - bucket_cache_started) * 1000.0,
        )
        record_runtime_counter(
            "detector.voter_registry_match.prepare_bucket_cache.entries",
            int(len(bucket_cache)),
        )

        mode_loop_started = perf_counter()
        for match_mode, outcome_column in REPORT_MATCH_MODE_TO_OUTCOME_COLUMN.items():
            record_runtime_counter("detector.voter_registry_match.mode.iterations", 1)
            mode_started = perf_counter()
            row_counts = working[outcome_column].value_counts().reindex(PRIMARY_OUTCOMES, fill_value=0)
            unique_counts = assignments[outcome_column].value_counts().reindex(PRIMARY_OUTCOMES, fill_value=0)
            n_rows_total = int(len(working))
            n_rows_unique = int(len(assignments))

            matched_rows = int(row_counts["matched_unique"] + row_counts["matched_ambiguous"])
            matched_unique_rows = int(row_counts["matched_unique"])
            matched_ambiguous_rows = int(row_counts["matched_ambiguous"])
            unmatched_rows = int(row_counts["unmatched"])

            matched_unique_unique = int(unique_counts["matched_unique"])
            matched_ambiguous_unique = int(unique_counts["matched_ambiguous"])
            unmatched_unique = int(unique_counts["unmatched"])

            matched_rows_low, matched_rows_high = wilson_interval(
                successes=pd.Series([matched_rows]),
                totals=pd.Series([n_rows_total]),
            )
            unmatched_rows_low, unmatched_rows_high = wilson_interval(
                successes=pd.Series([unmatched_rows]),
                totals=pd.Series([n_rows_total]),
            )

            linkage_overview_frames.append(
                pd.DataFrame(
                    [
                        {
                            "match_mode": str(match_mode),
                            "primary_match_mode": str(match_mode),
                            "primary_outcome_column": outcome_column,
                            "n_rows": n_rows_total,
                            "n_matched_unique_rows": matched_unique_rows,
                            "n_matched_ambiguous_rows": matched_ambiguous_rows,
                            "n_unmatched_rows": unmatched_rows,
                            "matched_rate_rows": (matched_rows / n_rows_total) if n_rows_total else 0.0,
                            "unmatched_rate_rows": (unmatched_rows / n_rows_total) if n_rows_total else 0.0,
                            "matched_rate_rows_wilson_low": float(matched_rows_low[0]),
                            "matched_rate_rows_wilson_high": float(matched_rows_high[0]),
                            "unmatched_rate_rows_wilson_low": float(unmatched_rows_low[0]),
                            "unmatched_rate_rows_wilson_high": float(unmatched_rows_high[0]),
                            "n_unique_names": n_rows_unique,
                            "n_matched_unique_unique": matched_unique_unique,
                            "n_matched_ambiguous_unique": matched_ambiguous_unique,
                            "n_unmatched_unique": unmatched_unique,
                            "matched_rate_unique": (
                                (matched_unique_unique + matched_ambiguous_unique) / n_rows_unique
                                if n_rows_unique
                                else 0.0
                            ),
                            "unmatched_rate_unique": (
                                unmatched_unique / n_rows_unique if n_rows_unique else 0.0
                            ),
                            "strong_fuzzy_min_score": float(self.strong_fuzzy_min_score),
                            "weak_fuzzy_min_score": float(self.weak_fuzzy_min_score),
                            "ambiguous_score_gap": float(self.ambiguous_score_gap),
                            "pairwise_alpha": float(self.pairwise_alpha),
                            "active_only": bool(active_only),
                            "registry_row_count": int(registry_row_count),
                            "voter_signal_role": "supporting_evidence_only",
                            "match_language_rule": "unmatched_to_wa_active_voter_file_only",
                            "attribution_caveat": self._ATTRIBUTION_CAVEAT,
                            "is_low_power": bool(n_rows_total < self.low_power_min_total),
                        }
                    ]
                )
            )
            sensitivity_rows.append(
                {
                    "mode": str(match_mode),
                    "match_mode": str(match_mode),
                    "n_rows": n_rows_total,
                    "n_matched_unique_rows": matched_unique_rows,
                    "n_matched_ambiguous_rows": matched_ambiguous_rows,
                    "n_unmatched_rows": unmatched_rows,
                    "matched_rate_rows": (matched_rows / n_rows_total) if n_rows_total else 0.0,
                    "unmatched_rate_rows": (unmatched_rows / n_rows_total) if n_rows_total else 0.0,
                    "n_unique_names": n_rows_unique,
                    "n_matched_unique_unique": matched_unique_unique,
                    "n_matched_ambiguous_unique": matched_ambiguous_unique,
                    "n_unmatched_unique": unmatched_unique,
                    "matched_rate_unique": (
                        float(matched_unique_unique + matched_ambiguous_unique) / float(n_rows_unique)
                        if n_rows_unique
                        else 0.0
                    ),
                    "unmatched_rate_unique": (
                        float(unmatched_unique) / float(n_rows_unique) if n_rows_unique else 0.0
                    ),
                }
            )

            linkage_by_position_rows = self._build_linkage_by_position(
                working,
                outcome_column=outcome_column,
                unit_label="rows",
            )
            if not linkage_by_position_rows.empty:
                linkage_by_position_rows["match_mode"] = str(match_mode)
                linkage_by_position_rows_frames.append(linkage_by_position_rows)

            linkage_by_position_unique = self._build_linkage_by_position(
                assignments,
                outcome_column=outcome_column,
                unit_label="unique_names",
                position_column="dominant_position",
            )
            if not linkage_by_position_unique.empty:
                linkage_by_position_unique["match_mode"] = str(match_mode)
                linkage_by_position_unique_frames.append(linkage_by_position_unique)

            pairwise_rows = self._build_pairwise_tests(linkage_by_position_rows, unit_label="rows")
            if not pairwise_rows.empty:
                pairwise_rows["match_mode"] = str(match_mode)
                position_pairwise_frames.append(pairwise_rows)
            pairwise_unique = self._build_pairwise_tests(
                linkage_by_position_unique,
                unit_label="unique_names",
            )
            if not pairwise_unique.empty:
                pairwise_unique["match_mode"] = str(match_mode)
                position_pairwise_frames.append(pairwise_unique)

            match_by_bucket, match_by_bucket_position = self._build_match_by_bucket(
                working,
                outcome_column=outcome_column,
                bucket_cache=bucket_cache,
            )
            if not match_by_bucket.empty:
                match_by_bucket["match_mode"] = str(match_mode)
                match_by_bucket_frames.append(match_by_bucket)
            if not match_by_bucket_position.empty:
                match_by_bucket_position["match_mode"] = str(match_mode)
                match_by_bucket_position_frames.append(match_by_bucket_position)

            unmatched_source = working[working[outcome_column] == "unmatched"].copy()
            unmatched_names_frames.append(
                _build_unmatched_names(source=unmatched_source, match_mode=match_mode)
            )
            record_runtime_timing(
                "detector.voter_registry_match.mode.total",
                (perf_counter() - mode_started) * 1000.0,
            )
            record_runtime_counter(
                "detector.voter_registry_match.mode.rows_processed",
                int(n_rows_total),
            )
        record_runtime_timing(
            "detector.voter_registry_match.mode.loop_total",
            (perf_counter() - mode_loop_started) * 1000.0,
        )

        assemble_tables_started = perf_counter()
        linkage_overview = (
            pd.concat(linkage_overview_frames, ignore_index=True)
            if linkage_overview_frames
            else pd.DataFrame()
        )
        linkage_by_position_rows = (
            pd.concat(linkage_by_position_rows_frames, ignore_index=True)
            if linkage_by_position_rows_frames
            else pd.DataFrame()
        )
        linkage_by_position_unique = (
            pd.concat(linkage_by_position_unique_frames, ignore_index=True)
            if linkage_by_position_unique_frames
            else pd.DataFrame()
        )
        position_pairwise_tests = (
            pd.concat(position_pairwise_frames, ignore_index=True)
            if position_pairwise_frames
            else pd.DataFrame()
        )
        sensitivity_modes = pd.DataFrame(sensitivity_rows)
        match_by_bucket = (
            pd.concat(match_by_bucket_frames, ignore_index=True)
            .sort_values(["match_mode", "bucket_minutes", "bucket_start"])
            .reset_index(drop=True)
            if match_by_bucket_frames
            else pd.DataFrame()
        )
        match_by_bucket_position = (
            pd.concat(match_by_bucket_position_frames, ignore_index=True)
            .sort_values(["match_mode", "bucket_minutes", "bucket_start", "position_normalized"])
            .reset_index(drop=True)
            if match_by_bucket_position_frames
            else pd.DataFrame()
        )
        unmatched_nonempty_frames = [frame for frame in unmatched_names_frames if not frame.empty]
        unmatched_names = (
            pd.concat(unmatched_nonempty_frames, ignore_index=True)
            .sort_values(["match_mode", "n_rows", "display_name"], ascending=[True, False, True])
            if unmatched_nonempty_frames
            else pd.DataFrame()
        )
        if not unmatched_names.empty:
            unmatched_names = unmatched_names.groupby(
                "match_mode", dropna=False, group_keys=False
            ).head(1000)
        record_runtime_timing(
            "detector.voter_registry_match.assemble_tables",
            (perf_counter() - assemble_tables_started) * 1000.0,
        )

        summary_started = perf_counter()
        available_mode_values = (
            set(sensitivity_modes["mode"])
            if isinstance(sensitivity_modes, pd.DataFrame) and "mode" in sensitivity_modes.columns
            else set()
        )
        match_mode_options = [
            mode for mode in REPORT_MATCH_MODE_TO_OUTCOME_COLUMN if mode in available_mode_values
        ]
        if not match_mode_options:
            match_mode_options = [DEFAULT_REPORT_MATCH_MODE]
        primary_match_mode = (
            DEFAULT_REPORT_MATCH_MODE if DEFAULT_REPORT_MATCH_MODE in match_mode_options else match_mode_options[0]
        )
        primary_outcome_column = REPORT_MATCH_MODE_TO_OUTCOME_COLUMN.get(
            primary_match_mode,
            REPORT_MATCH_MODE_TO_OUTCOME_COLUMN[DEFAULT_REPORT_MATCH_MODE],
        )

        if "match_mode" in linkage_overview.columns:
            primary_linkage = linkage_overview[
                linkage_overview["match_mode"].astype(str) == str(primary_match_mode)
            ].copy()
        else:
            primary_linkage = pd.DataFrame()
        if primary_linkage.empty and not linkage_overview.empty:
            primary_linkage = linkage_overview.head(1).copy()
            primary_match_mode = str(primary_linkage["match_mode"].iloc[0])
            primary_outcome_column = str(primary_linkage["primary_outcome_column"].iloc[0])
        primary_row = (
            primary_linkage.iloc[0].to_dict()
            if not primary_linkage.empty
            else {
                "n_rows": 0,
                "n_unique_names": 0,
                "n_matched_unique_rows": 0,
                "n_matched_ambiguous_rows": 0,
                "n_unmatched_rows": 0,
                "matched_rate_rows": 0.0,
                "unmatched_rate_rows": 0.0,
                "n_unmatched_unique": 0,
                "unmatched_rate_unique": 0.0,
            }
        )

        # Match assignments table (one row per canonical name) with primary labels surfaced.
        match_assignments = assignments.copy()
        match_assignments["primary_match_mode"] = str(primary_match_mode)
        match_assignments["primary_outcome_selected"] = _safe_str_series(
            match_assignments.get(primary_outcome_column, pd.Series(dtype=str))
        ).replace("", "unmatched")
        match_assignments["strict_outcome_selected"] = _safe_str_series(
            match_assignments.get("strict_outcome", pd.Series(dtype=str))
        ).replace("", "unmatched")
        match_assignments["loose_outcome_selected"] = _safe_str_series(
            match_assignments.get("loose_outcome", pd.Series(dtype=str))
        ).replace("", "unmatched")
        match_assignments = match_assignments.sort_values(
            ["primary_outcome_selected", "n_rows", "canonical_name"],
            ascending=[True, False, True],
        )

        summary = {
            "enabled": True,
            "active": True,
            "status_mode": "single",
            "active_only": bool(active_only),
            "registry_snapshot_date": self.registry_snapshot_date or None,
            "primary_match_mode": str(primary_match_mode),
            "primary_outcome_column": str(primary_outcome_column),
            "match_mode_default": str(primary_match_mode),
            "match_mode_options": [str(value) for value in match_mode_options],
            "n_rows": int(primary_row.get("n_rows", 0) or 0),
            "n_unique_names": int(primary_row.get("n_unique_names", 0) or 0),
            "n_matched_unique_rows": int(primary_row.get("n_matched_unique_rows", 0) or 0),
            "n_matched_ambiguous_rows": int(primary_row.get("n_matched_ambiguous_rows", 0) or 0),
            "n_unmatched_rows": int(primary_row.get("n_unmatched_rows", 0) or 0),
            "matched_rate_rows": float(primary_row.get("matched_rate_rows", 0.0) or 0.0),
            "unmatched_rate_rows": float(primary_row.get("unmatched_rate_rows", 0.0) or 0.0),
            "n_unmatched_unique": int(primary_row.get("n_unmatched_unique", 0) or 0),
            "unmatched_rate_unique": float(primary_row.get("unmatched_rate_unique", 0.0) or 0.0),
            "registry_row_count": int(registry_row_count),
            "bucket_minutes": [int(value) for value in self.bucket_minutes],
            "voter_signal_role": "supporting_evidence_only",
            "attribution_caveat": self._ATTRIBUTION_CAVEAT,
        }
        record_runtime_timing(
            "detector.voter_registry_match.build_summary_and_assignments",
            (perf_counter() - summary_started) * 1000.0,
        )

        tables = {
            "linkage_overview": linkage_overview,
            "linkage_by_position_rows": linkage_by_position_rows,
            "linkage_by_position_unique": linkage_by_position_unique,
            "position_pairwise_tests": position_pairwise_tests,
            "sensitivity_modes": sensitivity_modes,
            "match_assignments": match_assignments,
            "match_by_bucket": match_by_bucket,
            "match_by_bucket_position": match_by_bucket_position,
            "unmatched_names": unmatched_names,
            "position_bounds": pd.DataFrame(),
            "linkage_overview_bounds": pd.DataFrame(),
        }
        return DetectorResult(detector=self.name, summary=summary, tables=tables)

    def _build_dual_bounds_result(
        self,
        *,
        lower: DetectorResult,
        upper: DetectorResult,
    ) -> DetectorResult:
        if not bool(lower.summary.get("active")) or not bool(upper.summary.get("active")):
            return lower

        lower_summary = dict(lower.summary)
        upper_summary = dict(upper.summary)
        lower_summary["status_mode"] = "dual_bounds"
        lower_summary["registry_snapshot_date"] = self.registry_snapshot_date or None
        lower_summary["matched_rate_rows_lower"] = float(lower_summary.get("matched_rate_rows", 0.0) or 0.0)
        lower_summary["matched_rate_rows_upper"] = float(upper_summary.get("matched_rate_rows", 0.0) or 0.0)
        lower_summary["matched_rate_rows_span"] = float(
            max(
                lower_summary["matched_rate_rows_upper"] - lower_summary["matched_rate_rows_lower"],
                0.0,
            )
        )
        lower_summary["unmatched_rate_rows_lower"] = float(
            lower_summary.get("unmatched_rate_rows", 0.0) or 0.0
        )
        lower_summary["unmatched_rate_rows_upper"] = float(
            upper_summary.get("unmatched_rate_rows", 0.0) or 0.0
        )
        lower_summary["unmatched_rate_rows_span"] = float(
            max(
                lower_summary["unmatched_rate_rows_lower"] - lower_summary["unmatched_rate_rows_upper"],
                0.0,
            )
        )

        lower_overview = lower.tables.get("linkage_overview", pd.DataFrame()).copy()
        upper_overview = upper.tables.get("linkage_overview", pd.DataFrame()).copy()
        if not lower_overview.empty:
            lower_overview["bound"] = "lower_active_only_true"
        if not upper_overview.empty:
            upper_overview["bound"] = "upper_active_only_false"
        linkage_overview_bounds = (
            pd.concat([lower_overview, upper_overview], ignore_index=True)
            if (not lower_overview.empty or not upper_overview.empty)
            else pd.DataFrame()
        )

        lower_positions = lower.tables.get("linkage_by_position_rows", pd.DataFrame()).copy()
        upper_positions = upper.tables.get("linkage_by_position_rows", pd.DataFrame()).copy()
        if not lower_positions.empty:
            lower_positions = lower_positions.rename(
                columns={
                    "n_total": "n_total_lower",
                    "matched_rate": "matched_rate_lower",
                    "unmatched_rate": "unmatched_rate_lower",
                    "is_low_power": "is_low_power_lower",
                }
            )
        if not upper_positions.empty:
            upper_positions = upper_positions.rename(
                columns={
                    "n_total": "n_total_upper",
                    "matched_rate": "matched_rate_upper",
                    "unmatched_rate": "unmatched_rate_upper",
                    "is_low_power": "is_low_power_upper",
                }
            )
        if not lower_positions.empty and not upper_positions.empty:
            position_bounds = lower_positions.merge(
                upper_positions[
                    [
                        "match_mode",
                        "unit",
                        "position_normalized",
                        "n_total_upper",
                        "matched_rate_upper",
                        "unmatched_rate_upper",
                        "is_low_power_upper",
                    ]
                ],
                on=["match_mode", "unit", "position_normalized"],
                how="outer",
            )
        elif not lower_positions.empty:
            position_bounds = lower_positions.copy()
        elif not upper_positions.empty:
            position_bounds = upper_positions.copy()
        else:
            position_bounds = pd.DataFrame()

        if not position_bounds.empty:
            position_bounds["matched_rate_lower"] = pd.to_numeric(
                position_bounds.get("matched_rate_lower", 0.0), errors="coerce"
            ).fillna(0.0)
            position_bounds["matched_rate_upper"] = pd.to_numeric(
                position_bounds.get("matched_rate_upper", 0.0), errors="coerce"
            ).fillna(0.0)
            position_bounds["unmatched_rate_lower"] = pd.to_numeric(
                position_bounds.get("unmatched_rate_lower", 0.0), errors="coerce"
            ).fillna(0.0)
            position_bounds["unmatched_rate_upper"] = pd.to_numeric(
                position_bounds.get("unmatched_rate_upper", 0.0), errors="coerce"
            ).fillna(0.0)
            position_bounds["matched_rate_span"] = (
                position_bounds["matched_rate_upper"] - position_bounds["matched_rate_lower"]
            ).clip(lower=0.0)
            position_bounds["unmatched_rate_span"] = (
                position_bounds["unmatched_rate_lower"] - position_bounds["unmatched_rate_upper"]
            ).clip(lower=0.0)
            position_bounds["inference_status"] = "tested"
            low_power_lower = pd.Series(
                position_bounds.get(
                    "is_low_power_lower",
                    pd.Series(False, index=position_bounds.index),
                ),
                index=position_bounds.index,
            ).fillna(False).astype(bool)
            low_power_upper = pd.Series(
                position_bounds.get(
                    "is_low_power_upper",
                    pd.Series(False, index=position_bounds.index),
                ),
                index=position_bounds.index,
            ).fillna(False).astype(bool)
            position_bounds.loc[low_power_lower | low_power_upper, "inference_status"] = "descriptive_only"
            position_bounds = position_bounds.sort_values(
                ["match_mode", "unit", "position_normalized"]
            ).reset_index(drop=True)

        lower_assignments = lower.tables.get("match_assignments", pd.DataFrame()).copy()
        upper_assignments = upper.tables.get("match_assignments", pd.DataFrame()).copy()
        if not lower_assignments.empty and not upper_assignments.empty:
            upper_subset = upper_assignments[
                [
                    "canonical_name",
                    "primary_outcome_selected",
                    "strict_outcome_selected",
                    "loose_outcome_selected",
                ]
            ].rename(
                columns={
                    "primary_outcome_selected": "primary_outcome_selected_upper",
                    "strict_outcome_selected": "strict_outcome_selected_upper",
                    "loose_outcome_selected": "loose_outcome_selected_upper",
                }
            )
            lower_assignments = lower_assignments.merge(
                upper_subset,
                on="canonical_name",
                how="left",
            )
            lower_assignments["primary_outcome_selected_lower"] = _safe_str_series(
                lower_assignments.get("primary_outcome_selected")
            ).replace("", "unmatched")
            lower_assignments["strict_outcome_selected_lower"] = _safe_str_series(
                lower_assignments.get("strict_outcome_selected")
            ).replace("", "unmatched")
            lower_assignments["loose_outcome_selected_lower"] = _safe_str_series(
                lower_assignments.get("loose_outcome_selected")
            ).replace("", "unmatched")
            lower_assignments["primary_outcome_selected_upper"] = _safe_str_series(
                lower_assignments.get("primary_outcome_selected_upper")
            ).replace("", "unmatched")
            lower_assignments["strict_outcome_selected_upper"] = _safe_str_series(
                lower_assignments.get("strict_outcome_selected_upper")
            ).replace("", "unmatched")
            lower_assignments["loose_outcome_selected_upper"] = _safe_str_series(
                lower_assignments.get("loose_outcome_selected_upper")
            ).replace("", "unmatched")

        merged_tables = dict(lower.tables)
        merged_tables["position_bounds"] = position_bounds
        merged_tables["linkage_overview_bounds"] = linkage_overview_bounds
        merged_tables["match_assignments"] = lower_assignments if not lower_assignments.empty else lower.tables.get("match_assignments", pd.DataFrame())
        return DetectorResult(detector=self.name, summary=lower_summary, tables=merged_tables)

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        if self.status_mode != "dual_bounds":
            return self._run_single(df=df, features=features, active_only=self.active_only)

        lower = self._run_single(df=df, features=features, active_only=True)
        upper = self._run_single(df=df, features=features, active_only=False)
        return self._build_dual_bounds_result(lower=lower, upper=upper)
