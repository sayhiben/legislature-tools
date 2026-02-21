from __future__ import annotations

import pandas as pd
from rapidfuzz import fuzz

from testifier_audit.detectors.base import Detector, DetectorResult
from testifier_audit.io.vrdb_postgres import (
    count_registry_rows,
    fetch_matching_voter_names,
    fetch_voter_candidates_by_last_name,
)
from testifier_audit.proportion_stats import (
    DEFAULT_LOW_POWER_MIN_TOTAL,
    low_power_mask,
    wilson_half_width,
    wilson_interval,
)


class VoterRegistryMatchDetector(Detector):
    name = "voter_registry_match"
    _MATCH_TIERS = ("exact", "strong_fuzzy", "weak_fuzzy", "unmatched")
    _STRONG_FUZZY_CONFIDENCE_RANGE = (0.80, 0.95)
    _WEAK_FUZZY_CONFIDENCE_RANGE = (0.55, 0.79)
    _UNCERTAINTY_CAVEAT_DESCRIPTIONS = {
        "no_last_name_candidates": (
            "No registry candidates were available for the submission last name."
        ),
        "below_similarity_threshold": (
            "Best fuzzy candidate did not meet the configured weak similarity threshold."
        ),
        "ambiguous_top_candidate": (
            "Top fuzzy candidates had very similar scores; attribution is uncertain."
        ),
        "weak_similarity": (
            "Matched only at weak-fuzzy confidence; treat as low-confidence support."
        ),
        "missing_first_name_token": "Submission first-name token was empty after normalization.",
        "none_detected": "No additional fuzzy-linkage caveat flags were emitted for this run.",
    }
    _ATTRIBUTION_CAVEAT = (
        "Voter registry linkage is supporting evidence for anomaly context and is not standalone "
        "attribution of identity or intent."
    )

    def __init__(
        self,
        enabled: bool = False,
        db_url: str | None = None,
        table_name: str = "voter_registry",
        bucket_minutes: int | list[int] | tuple[int, ...] = 30,
        active_only: bool = True,
        low_power_min_total: int = DEFAULT_LOW_POWER_MIN_TOTAL,
        strong_fuzzy_min_score: float = 92.0,
        weak_fuzzy_min_score: float = 84.0,
        ambiguous_score_gap: float = 2.0,
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
        resolved_weak = min(max(float(weak_fuzzy_min_score), 0.0), 100.0)
        resolved_strong = min(max(float(strong_fuzzy_min_score), 0.0), 100.0)
        if resolved_strong < resolved_weak:
            resolved_weak, resolved_strong = resolved_strong, resolved_weak
        self.strong_fuzzy_min_score = resolved_strong
        self.weak_fuzzy_min_score = resolved_weak
        self.ambiguous_score_gap = max(0.0, float(ambiguous_score_gap))

    @staticmethod
    def _split_canonical_name(value: str) -> tuple[str, str]:
        raw = str(value or "")
        if "|" not in raw:
            return "", ""
        last, first = raw.split("|", 1)
        return last.strip(), first.strip()

    @staticmethod
    def _interpolate_confidence(
        score: float,
        floor: float,
        ceiling: float,
        lower: float,
        upper: float,
    ) -> float:
        if ceiling <= floor:
            return float(upper)
        bounded = min(max(float(score), float(floor)), float(ceiling))
        fraction = (bounded - float(floor)) / (float(ceiling) - float(floor))
        return float(lower) + (float(upper) - float(lower)) * fraction

    @staticmethod
    def _empty_tables() -> dict[str, pd.DataFrame]:
        return {
            "match_overview": pd.DataFrame(
                columns=[
                    "n_records",
                    "n_matches",
                    "n_unmatched",
                    "n_exact_matches",
                    "n_strong_fuzzy_matches",
                    "n_weak_fuzzy_matches",
                    "match_rate",
                    "exact_match_rate",
                    "strong_fuzzy_match_rate",
                    "weak_fuzzy_match_rate",
                    "expected_match_count",
                    "expected_match_rate",
                    "mean_match_confidence",
                    "matched_confidence_mean",
                    "match_rate_wilson_low",
                    "match_rate_wilson_high",
                    "match_rate_wilson_half_width",
                    "unmatched_rate",
                    "is_low_power",
                    "n_ambiguous_fuzzy_matches",
                    "n_unique_names",
                    "n_unique_matches",
                    "n_unique_unmatched",
                    "unique_match_rate",
                    "registry_row_count",
                    "active_only",
                    "strong_fuzzy_min_score",
                    "weak_fuzzy_min_score",
                    "uncertainty_caveat",
                    "attribution_caveat",
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
                    "n_exact_matches",
                    "n_strong_fuzzy_matches",
                    "n_weak_fuzzy_matches",
                    "match_rate",
                    "exact_match_rate",
                    "strong_fuzzy_match_rate",
                    "weak_fuzzy_match_rate",
                    "expected_matches",
                    "expected_match_rate",
                    "mean_match_confidence",
                    "matched_confidence_mean",
                    "match_rate_wilson_low",
                    "match_rate_wilson_high",
                    "match_rate_wilson_half_width",
                    "unmatched_rate",
                    "n_ambiguous_fuzzy_matches",
                    "is_low_power",
                ]
            ),
            "match_by_bucket": pd.DataFrame(
                columns=[
                    "bucket_start",
                    "n_total",
                    "n_matches",
                    "n_exact_matches",
                    "n_strong_fuzzy_matches",
                    "n_weak_fuzzy_matches",
                    "n_pro",
                    "n_matches_pro",
                    "n_con",
                    "n_matches_con",
                    "n_unmatched",
                    "match_rate",
                    "exact_match_rate",
                    "strong_fuzzy_match_rate",
                    "weak_fuzzy_match_rate",
                    "expected_matches",
                    "expected_match_rate",
                    "mean_match_confidence",
                    "matched_confidence_mean",
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
                    "n_ambiguous_fuzzy_matches",
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
                    "n_exact_matches",
                    "n_strong_fuzzy_matches",
                    "n_weak_fuzzy_matches",
                    "match_rate",
                    "exact_match_rate",
                    "strong_fuzzy_match_rate",
                    "weak_fuzzy_match_rate",
                    "expected_matches",
                    "expected_match_rate",
                    "mean_match_confidence",
                    "matched_confidence_mean",
                    "match_rate_wilson_low",
                    "match_rate_wilson_high",
                    "match_rate_wilson_half_width",
                    "n_ambiguous_fuzzy_matches",
                    "is_low_power",
                    "bucket_minutes",
                ]
            ),
            "matched_names": pd.DataFrame(
                columns=[
                    "canonical_name",
                    "n_records",
                    "n_registry_rows",
                    "match_tier",
                    "matched_registry_name",
                    "match_confidence_mean",
                    "best_similarity_score",
                    "n_ambiguous_fuzzy_matches",
                ]
            ),
            "unmatched_names": pd.DataFrame(
                columns=[
                    "canonical_name",
                    "n_records",
                    "best_similarity_score",
                    "candidate_pool_size",
                    "match_caveat",
                ]
            ),
            "match_tier_summary": pd.DataFrame(
                columns=[
                    "match_tier",
                    "n_records",
                    "record_rate",
                    "mean_match_confidence",
                    "min_match_confidence",
                    "max_match_confidence",
                ]
            ),
            "match_uncertainty_summary": pd.DataFrame(
                columns=["caveat_flag", "n_records", "record_rate", "description"]
            ),
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
        submission_last_names = sorted(
            {
                last
                for value in submission_names
                for last, _first in [self._split_canonical_name(value)]
                if last
            }
        )

        try:
            matched_lookup = fetch_matching_voter_names(
                db_url=self.db_url,
                table_name=self.table_name,
                canonical_names=submission_names,
                active_only=self.active_only,
            )
            fuzzy_lookup = fetch_voter_candidates_by_last_name(
                db_url=self.db_url,
                table_name=self.table_name,
                canonical_lasts=submission_last_names,
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

        if (
            not isinstance(matched_lookup, pd.DataFrame)
            or "canonical_name" not in matched_lookup.columns
        ):
            matched_lookup = pd.DataFrame(columns=["canonical_name", "n_registry_rows"])
        matched_lookup = matched_lookup.copy()
        matched_lookup["canonical_name"] = matched_lookup["canonical_name"].fillna("").astype(str)
        if "n_registry_rows" not in matched_lookup.columns:
            matched_lookup["n_registry_rows"] = 0
        matched_lookup["n_registry_rows"] = (
            pd.to_numeric(matched_lookup["n_registry_rows"], errors="coerce")
            .fillna(0)
            .astype(int)
        )
        matched_lookup = (
            matched_lookup[matched_lookup["canonical_name"] != ""]
            .groupby("canonical_name", dropna=False)
            .agg(n_registry_rows=("n_registry_rows", "max"))
            .reset_index()
        )

        if (
            not isinstance(fuzzy_lookup, pd.DataFrame)
            or "canonical_name" not in fuzzy_lookup.columns
        ):
            fuzzy_lookup = pd.DataFrame(
                columns=["canonical_last", "canonical_first", "canonical_name", "n_registry_rows"]
            )
        fuzzy_lookup = fuzzy_lookup.copy()
        fuzzy_lookup["canonical_name"] = fuzzy_lookup["canonical_name"].fillna("").astype(str)
        split_names = fuzzy_lookup["canonical_name"].str.split("|", n=1, expand=True)
        if "canonical_last" not in fuzzy_lookup.columns:
            fuzzy_lookup["canonical_last"] = split_names[0] if not split_names.empty else ""
        if "canonical_first" not in fuzzy_lookup.columns:
            fuzzy_lookup["canonical_first"] = (
                split_names[1] if split_names.shape[1] > 1 else ""
            )
        fuzzy_lookup["canonical_last"] = fuzzy_lookup["canonical_last"].fillna("").astype(str)
        fuzzy_lookup["canonical_first"] = fuzzy_lookup["canonical_first"].fillna("").astype(str)
        if "n_registry_rows" not in fuzzy_lookup.columns:
            fuzzy_lookup["n_registry_rows"] = 0
        fuzzy_lookup["n_registry_rows"] = (
            pd.to_numeric(fuzzy_lookup["n_registry_rows"], errors="coerce")
            .fillna(0)
            .astype(int)
        )
        fuzzy_lookup = fuzzy_lookup[fuzzy_lookup["canonical_name"] != ""].copy()

        exact_lookup_map = dict(
            zip(
                matched_lookup["canonical_name"].tolist(),
                matched_lookup["n_registry_rows"].tolist(),
                strict=False,
            )
        )

        candidate_lookup_by_last: dict[str, list[dict[str, object]]] = {}
        for row in fuzzy_lookup.itertuples(index=False):
            canonical_last = str(getattr(row, "canonical_last", "") or "").strip()
            canonical_first = str(getattr(row, "canonical_first", "") or "").strip()
            canonical_name = str(getattr(row, "canonical_name", "") or "").strip()
            n_registry_rows = int(getattr(row, "n_registry_rows", 0) or 0)
            if not canonical_last or not canonical_name:
                continue
            candidate_lookup_by_last.setdefault(canonical_last, []).append(
                {
                    "canonical_name": canonical_name,
                    "canonical_first": canonical_first,
                    "n_registry_rows": n_registry_rows,
                }
            )

        assignment_rows: list[dict[str, object]] = []
        for canonical_name in submission_names:
            last_name, first_name = self._split_canonical_name(canonical_name)
            if canonical_name in exact_lookup_map:
                assignment_rows.append(
                    {
                        "canonical_name": canonical_name,
                        "match_tier": "exact",
                        "match_confidence": 1.0,
                        "is_match": True,
                        "matched_registry_name": canonical_name,
                        "matched_registry_rows": int(exact_lookup_map.get(canonical_name, 0)),
                        "best_similarity_score": 1.0,
                        "second_best_similarity_score": None,
                        "candidate_pool_size": len(candidate_lookup_by_last.get(last_name, [])),
                        "is_ambiguous": False,
                        "match_caveat": "",
                    }
                )
                continue

            candidates = candidate_lookup_by_last.get(last_name, [])
            if not first_name:
                assignment_rows.append(
                    {
                        "canonical_name": canonical_name,
                        "match_tier": "unmatched",
                        "match_confidence": 0.0,
                        "is_match": False,
                        "matched_registry_name": "",
                        "matched_registry_rows": 0,
                        "best_similarity_score": None,
                        "second_best_similarity_score": None,
                        "candidate_pool_size": len(candidates),
                        "is_ambiguous": False,
                        "match_caveat": "missing_first_name_token",
                    }
                )
                continue
            if not candidates:
                assignment_rows.append(
                    {
                        "canonical_name": canonical_name,
                        "match_tier": "unmatched",
                        "match_confidence": 0.0,
                        "is_match": False,
                        "matched_registry_name": "",
                        "matched_registry_rows": 0,
                        "best_similarity_score": None,
                        "second_best_similarity_score": None,
                        "candidate_pool_size": 0,
                        "is_ambiguous": False,
                        "match_caveat": "no_last_name_candidates",
                    }
                )
                continue

            scored_candidates: list[dict[str, object]] = []
            for candidate in candidates:
                candidate_first = str(candidate.get("canonical_first", "") or "").strip()
                if not candidate_first:
                    continue
                similarity_score = float(fuzz.ratio(first_name, candidate_first))
                scored_candidates.append(
                    {
                        "canonical_name": str(candidate.get("canonical_name", "") or ""),
                        "n_registry_rows": int(candidate.get("n_registry_rows", 0) or 0),
                        "score": similarity_score,
                    }
                )

            if not scored_candidates:
                assignment_rows.append(
                    {
                        "canonical_name": canonical_name,
                        "match_tier": "unmatched",
                        "match_confidence": 0.0,
                        "is_match": False,
                        "matched_registry_name": "",
                        "matched_registry_rows": 0,
                        "best_similarity_score": None,
                        "second_best_similarity_score": None,
                        "candidate_pool_size": len(candidates),
                        "is_ambiguous": False,
                        "match_caveat": "no_last_name_candidates",
                    }
                )
                continue

            scored_candidates.sort(key=lambda item: float(item["score"]), reverse=True)
            best = scored_candidates[0]
            second_best = scored_candidates[1] if len(scored_candidates) > 1 else None
            best_score = float(best["score"])
            second_best_score = (
                float(second_best["score"]) if second_best is not None else None
            )
            is_ambiguous = bool(
                second_best_score is not None
                and abs(best_score - second_best_score) <= self.ambiguous_score_gap
            )

            match_tier = "unmatched"
            match_confidence = 0.0
            caveats: list[str] = []
            if best_score >= self.strong_fuzzy_min_score:
                match_tier = "strong_fuzzy"
                match_confidence = self._interpolate_confidence(
                    score=best_score,
                    floor=self.strong_fuzzy_min_score,
                    ceiling=100.0,
                    lower=self._STRONG_FUZZY_CONFIDENCE_RANGE[0],
                    upper=self._STRONG_FUZZY_CONFIDENCE_RANGE[1],
                )
            elif best_score >= self.weak_fuzzy_min_score:
                match_tier = "weak_fuzzy"
                match_confidence = self._interpolate_confidence(
                    score=best_score,
                    floor=self.weak_fuzzy_min_score,
                    ceiling=max(self.strong_fuzzy_min_score - 0.01, self.weak_fuzzy_min_score),
                    lower=self._WEAK_FUZZY_CONFIDENCE_RANGE[0],
                    upper=self._WEAK_FUZZY_CONFIDENCE_RANGE[1],
                )
            else:
                caveats.append("below_similarity_threshold")

            if is_ambiguous:
                caveats.append("ambiguous_top_candidate")
                if match_tier == "strong_fuzzy":
                    match_tier = "weak_fuzzy"
                    match_confidence = min(match_confidence, 0.78)
            if match_tier == "weak_fuzzy":
                caveats.append("weak_similarity")

            assignment_rows.append(
                {
                    "canonical_name": canonical_name,
                    "match_tier": match_tier,
                    "match_confidence": float(match_confidence),
                    "is_match": match_tier != "unmatched",
                    "matched_registry_name": str(best.get("canonical_name", "") or "")
                    if match_tier != "unmatched"
                    else "",
                    "matched_registry_rows": int(best.get("n_registry_rows", 0) or 0)
                    if match_tier != "unmatched"
                    else 0,
                    "best_similarity_score": best_score / 100.0,
                    "second_best_similarity_score": (
                        second_best_score / 100.0 if second_best_score is not None else None
                    ),
                    "candidate_pool_size": len(candidates),
                    "is_ambiguous": is_ambiguous and match_tier != "unmatched",
                    "match_caveat": ",".join(caveats),
                }
            )

        linkage_assignments = pd.DataFrame(assignment_rows)
        if linkage_assignments.empty:
            linkage_assignments = pd.DataFrame(
                columns=[
                    "canonical_name",
                    "match_tier",
                    "match_confidence",
                    "is_match",
                    "matched_registry_name",
                    "matched_registry_rows",
                    "best_similarity_score",
                    "second_best_similarity_score",
                    "candidate_pool_size",
                    "is_ambiguous",
                    "match_caveat",
                ]
            )

        working = working.merge(linkage_assignments, on="canonical_name", how="left")
        working["match_tier"] = working["match_tier"].fillna("unmatched").astype(str)
        working["match_confidence"] = (
            pd.to_numeric(working["match_confidence"], errors="coerce")
            .fillna(0.0)
            .clip(lower=0.0, upper=1.0)
        )
        working["is_match"] = working["match_tier"].isin({"exact", "strong_fuzzy", "weak_fuzzy"})
        working["matched_registry_name"] = (
            working["matched_registry_name"].fillna("").astype(str)
        )
        working["matched_registry_rows"] = (
            pd.to_numeric(working["matched_registry_rows"], errors="coerce").fillna(0).astype(int)
        )
        working["best_similarity_score"] = pd.to_numeric(
            working["best_similarity_score"], errors="coerce"
        )
        working["second_best_similarity_score"] = pd.to_numeric(
            working["second_best_similarity_score"], errors="coerce"
        )
        working["candidate_pool_size"] = (
            pd.to_numeric(working["candidate_pool_size"], errors="coerce").fillna(0).astype(int)
        )
        working["is_ambiguous"] = working["is_ambiguous"].map(
            lambda value: bool(value) if pd.notna(value) else False
        )
        working["match_caveat"] = working["match_caveat"].fillna("").astype(str)
        working["tier_exact"] = (working["match_tier"] == "exact").astype(int)
        working["tier_strong_fuzzy"] = (working["match_tier"] == "strong_fuzzy").astype(int)
        working["tier_weak_fuzzy"] = (working["match_tier"] == "weak_fuzzy").astype(int)
        working["tier_unmatched"] = (working["match_tier"] == "unmatched").astype(int)
        working["matched_confidence_component"] = working["match_confidence"].where(
            working["is_match"], 0.0
        )

        n_records = int(len(working))
        n_matches = int(working["is_match"].sum())
        n_unmatched = int(n_records - n_matches)
        n_exact_matches = int(working["tier_exact"].sum())
        n_strong_fuzzy_matches = int(working["tier_strong_fuzzy"].sum())
        n_weak_fuzzy_matches = int(working["tier_weak_fuzzy"].sum())
        expected_match_count = float(working["match_confidence"].sum())
        mean_match_confidence = float(working["match_confidence"].mean()) if n_records else 0.0
        matched_confidence_mean = (
            float(working.loc[working["is_match"], "match_confidence"].mean()) if n_matches else 0.0
        )
        n_ambiguous_fuzzy_matches = int((working["is_ambiguous"] & working["is_match"]).sum())

        unique_names = set(
            value for value in working["canonical_name"].tolist() if value and value != "|"
        )
        n_unique_names = int(len(unique_names))
        unique_assignment = linkage_assignments[
            linkage_assignments["canonical_name"].isin(unique_names)
        ]
        n_unique_matches = (
            int(unique_assignment["is_match"].sum()) if not unique_assignment.empty else 0
        )
        n_unique_unmatched = int(n_unique_names - n_unique_matches)

        caveat_counts: dict[str, int] = {}
        for raw in working["match_caveat"].tolist():
            for token in [item.strip() for item in str(raw).split(",") if item.strip()]:
                caveat_counts[token] = caveat_counts.get(token, 0) + 1
        uncertainty_rows = [
            {
                "caveat_flag": caveat_flag,
                "n_records": int(count),
                "record_rate": (float(count) / n_records) if n_records else 0.0,
                "description": self._UNCERTAINTY_CAVEAT_DESCRIPTIONS.get(
                    caveat_flag,
                    "Uncertainty caveat emitted by probabilistic voter linkage.",
                ),
            }
            for caveat_flag, count in sorted(
                caveat_counts.items(), key=lambda item: item[1], reverse=True
            )
        ]
        if not uncertainty_rows:
            uncertainty_rows.append(
                {
                    "caveat_flag": "none_detected",
                    "n_records": 0,
                    "record_rate": 0.0,
                    "description": self._UNCERTAINTY_CAVEAT_DESCRIPTIONS["none_detected"],
                }
            )
        uncertainty_summary = pd.DataFrame(uncertainty_rows)
        uncertainty_caveat = (
            "; ".join(f"{row['caveat_flag']}:{row['n_records']}" for row in uncertainty_rows)
            if uncertainty_rows
            else "none_detected"
        )

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
                    "n_exact_matches": n_exact_matches,
                    "n_strong_fuzzy_matches": n_strong_fuzzy_matches,
                    "n_weak_fuzzy_matches": n_weak_fuzzy_matches,
                    "match_rate": (n_matches / n_records) if n_records else 0.0,
                    "exact_match_rate": (n_exact_matches / n_records) if n_records else 0.0,
                    "strong_fuzzy_match_rate": (
                        n_strong_fuzzy_matches / n_records if n_records else 0.0
                    ),
                    "weak_fuzzy_match_rate": (
                        n_weak_fuzzy_matches / n_records if n_records else 0.0
                    ),
                    "expected_match_count": expected_match_count,
                    "expected_match_rate": (expected_match_count / n_records) if n_records else 0.0,
                    "mean_match_confidence": mean_match_confidence,
                    "matched_confidence_mean": matched_confidence_mean,
                    "match_rate_wilson_low": float(overview_match_rate_wilson_low[0]),
                    "match_rate_wilson_high": float(overview_match_rate_wilson_high[0]),
                    "match_rate_wilson_half_width": float(overview_match_rate_wilson_half_width[0]),
                    "unmatched_rate": (n_unmatched / n_records) if n_records else 0.0,
                    "is_low_power": bool(overview_is_low_power[0]),
                    "n_ambiguous_fuzzy_matches": n_ambiguous_fuzzy_matches,
                    "n_unique_names": n_unique_names,
                    "n_unique_matches": n_unique_matches,
                    "n_unique_unmatched": n_unique_unmatched,
                    "unique_match_rate": (n_unique_matches / n_unique_names)
                    if n_unique_names
                    else 0.0,
                    "registry_row_count": int(registry_row_count),
                    "active_only": bool(self.active_only),
                    "strong_fuzzy_min_score": float(self.strong_fuzzy_min_score),
                    "weak_fuzzy_min_score": float(self.weak_fuzzy_min_score),
                    "uncertainty_caveat": uncertainty_caveat,
                    "attribution_caveat": self._ATTRIBUTION_CAVEAT,
                    "bucket_minutes": int(self.bucket_minutes[0]),
                    "bucket_minutes_list": ",".join(str(value) for value in self.bucket_minutes),
                }
            ]
        )

        match_tier_rows: list[dict[str, object]] = []
        for tier in self._MATCH_TIERS:
            tier_mask = working["match_tier"] == tier
            tier_count = int(tier_mask.sum())
            tier_confidence = working.loc[tier_mask, "match_confidence"]
            match_tier_rows.append(
                {
                    "match_tier": tier,
                    "n_records": tier_count,
                    "record_rate": (tier_count / n_records) if n_records else 0.0,
                    "mean_match_confidence": float(tier_confidence.mean()) if tier_count else 0.0,
                    "min_match_confidence": float(tier_confidence.min()) if tier_count else 0.0,
                    "max_match_confidence": float(tier_confidence.max()) if tier_count else 0.0,
                }
            )
        match_tier_summary = pd.DataFrame(match_tier_rows)

        by_position = (
            working.groupby("position_normalized", dropna=False)
            .agg(
                n_total=("canonical_name", "count"),
                n_matches=("is_match", "sum"),
                n_exact_matches=("tier_exact", "sum"),
                n_strong_fuzzy_matches=("tier_strong_fuzzy", "sum"),
                n_weak_fuzzy_matches=("tier_weak_fuzzy", "sum"),
                expected_matches=("match_confidence", "sum"),
                mean_match_confidence=("match_confidence", "mean"),
                matched_confidence_total=("matched_confidence_component", "sum"),
                n_ambiguous_fuzzy_matches=("is_ambiguous", "sum"),
            )
            .reset_index()
            .sort_values("n_total", ascending=False)
        )
        by_position["n_unmatched"] = by_position["n_total"] - by_position["n_matches"]
        by_position["match_rate"] = (by_position["n_matches"] / by_position["n_total"]).where(
            by_position["n_total"] > 0
        )
        by_position["exact_match_rate"] = (
            by_position["n_exact_matches"] / by_position["n_total"]
        ).where(by_position["n_total"] > 0)
        by_position["strong_fuzzy_match_rate"] = (
            by_position["n_strong_fuzzy_matches"] / by_position["n_total"]
        ).where(by_position["n_total"] > 0)
        by_position["weak_fuzzy_match_rate"] = (
            by_position["n_weak_fuzzy_matches"] / by_position["n_total"]
        ).where(by_position["n_total"] > 0)
        by_position["expected_match_rate"] = (
            by_position["expected_matches"] / by_position["n_total"]
        ).where(by_position["n_total"] > 0)
        by_position["matched_confidence_mean"] = (
            by_position["matched_confidence_total"] / by_position["n_matches"]
        ).where(by_position["n_matches"] > 0)
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
        by_position = by_position.drop(columns=["matched_confidence_total"])

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
                    n_exact_matches=("tier_exact", "sum"),
                    n_strong_fuzzy_matches=("tier_strong_fuzzy", "sum"),
                    n_weak_fuzzy_matches=("tier_weak_fuzzy", "sum"),
                    expected_matches=("match_confidence", "sum"),
                    mean_match_confidence=("match_confidence", "mean"),
                    matched_confidence_total=("matched_confidence_component", "sum"),
                    n_ambiguous_fuzzy_matches=("is_ambiguous", "sum"),
                )
                .reset_index()
            )

            by_bucket_position = (
                bucketed.groupby(["bucket_start", "position_normalized"], dropna=False)
                .agg(
                    n_total=("canonical_name", "count"),
                    n_matches=("is_match", "sum"),
                    n_exact_matches=("tier_exact", "sum"),
                    n_strong_fuzzy_matches=("tier_strong_fuzzy", "sum"),
                    n_weak_fuzzy_matches=("tier_weak_fuzzy", "sum"),
                    expected_matches=("match_confidence", "sum"),
                    mean_match_confidence=("match_confidence", "mean"),
                    matched_confidence_total=("matched_confidence_component", "sum"),
                    n_ambiguous_fuzzy_matches=("is_ambiguous", "sum"),
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
            by_bucket_position["exact_match_rate"] = (
                by_bucket_position["n_exact_matches"] / by_bucket_position["n_total"]
            ).where(by_bucket_position["n_total"] > 0)
            by_bucket_position["strong_fuzzy_match_rate"] = (
                by_bucket_position["n_strong_fuzzy_matches"] / by_bucket_position["n_total"]
            ).where(by_bucket_position["n_total"] > 0)
            by_bucket_position["weak_fuzzy_match_rate"] = (
                by_bucket_position["n_weak_fuzzy_matches"] / by_bucket_position["n_total"]
            ).where(by_bucket_position["n_total"] > 0)
            by_bucket_position["expected_match_rate"] = (
                by_bucket_position["expected_matches"] / by_bucket_position["n_total"]
            ).where(by_bucket_position["n_total"] > 0)
            by_bucket_position["matched_confidence_mean"] = (
                by_bucket_position["matched_confidence_total"] / by_bucket_position["n_matches"]
            ).where(by_bucket_position["n_matches"] > 0)
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
            by_bucket_position = by_bucket_position.drop(columns=["matched_confidence_total"])
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
            by_bucket["exact_match_rate"] = (
                by_bucket["n_exact_matches"] / by_bucket["n_total"]
            ).where(by_bucket["n_total"] > 0)
            by_bucket["strong_fuzzy_match_rate"] = (
                by_bucket["n_strong_fuzzy_matches"] / by_bucket["n_total"]
            ).where(by_bucket["n_total"] > 0)
            by_bucket["weak_fuzzy_match_rate"] = (
                by_bucket["n_weak_fuzzy_matches"] / by_bucket["n_total"]
            ).where(by_bucket["n_total"] > 0)
            by_bucket["expected_match_rate"] = (
                by_bucket["expected_matches"] / by_bucket["n_total"]
            ).where(by_bucket["n_total"] > 0)
            by_bucket["matched_confidence_mean"] = (
                by_bucket["matched_confidence_total"] / by_bucket["n_matches"]
            ).where(by_bucket["n_matches"] > 0)
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
            by_bucket = by_bucket.drop(columns=["matched_confidence_total"])
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

        def _mode_or_blank(values: pd.Series) -> str:
            cleaned = values.dropna().astype(str).map(lambda value: value.strip())
            cleaned = cleaned[cleaned != ""]
            if cleaned.empty:
                return ""
            modes = cleaned.mode()
            if not modes.empty:
                return str(modes.iloc[0])
            return str(cleaned.iloc[0])

        matched_name_counts = (
            working[working["is_match"]]
            .groupby("canonical_name", dropna=False)
            .agg(
                n_records=("canonical_name", "count"),
                n_registry_rows=("matched_registry_rows", "max"),
                match_tier=("match_tier", _mode_or_blank),
                matched_registry_name=("matched_registry_name", _mode_or_blank),
                match_confidence_mean=("match_confidence", "mean"),
                best_similarity_score=("best_similarity_score", "max"),
                n_ambiguous_fuzzy_matches=("is_ambiguous", "sum"),
            )
            .reset_index()
            .sort_values("n_records", ascending=False)
        )
        unmatched_name_counts = (
            working[~working["is_match"]]
            .groupby("canonical_name", dropna=False)
            .agg(
                n_records=("canonical_name", "count"),
                best_similarity_score=("best_similarity_score", "max"),
                candidate_pool_size=("candidate_pool_size", "max"),
                match_caveat=("match_caveat", _mode_or_blank),
            )
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
                "n_exact_matches": n_exact_matches,
                "n_strong_fuzzy_matches": n_strong_fuzzy_matches,
                "n_weak_fuzzy_matches": n_weak_fuzzy_matches,
                "match_rate": (n_matches / n_records) if n_records else 0.0,
                "exact_match_rate": (n_exact_matches / n_records) if n_records else 0.0,
                "strong_fuzzy_match_rate": (
                    n_strong_fuzzy_matches / n_records if n_records else 0.0
                ),
                "weak_fuzzy_match_rate": (
                    n_weak_fuzzy_matches / n_records if n_records else 0.0
                ),
                "expected_match_count": expected_match_count,
                "expected_match_rate": (expected_match_count / n_records) if n_records else 0.0,
                "mean_match_confidence": mean_match_confidence,
                "matched_confidence_mean": matched_confidence_mean,
                "n_ambiguous_fuzzy_matches": n_ambiguous_fuzzy_matches,
                "n_unique_names": n_unique_names,
                "n_unique_matches": n_unique_matches,
                "n_unique_unmatched": n_unique_unmatched,
                "unique_match_rate": (n_unique_matches / n_unique_names) if n_unique_names else 0.0,
                "registry_row_count": int(registry_row_count),
                "active_only": bool(self.active_only),
                "low_power_min_total": int(self.low_power_min_total),
                "strong_fuzzy_min_score": float(self.strong_fuzzy_min_score),
                "weak_fuzzy_min_score": float(self.weak_fuzzy_min_score),
                "uncertainty_caveat": uncertainty_caveat,
                "uncertainty_caveat_summary": uncertainty_rows,
                "voter_signal_role": "supporting_evidence_only",
                "attribution_caveat": self._ATTRIBUTION_CAVEAT,
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
                "match_tier_summary": match_tier_summary,
                "match_uncertainty_summary": uncertainty_summary,
            },
        )
