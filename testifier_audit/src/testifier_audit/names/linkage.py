from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from rapidfuzz import fuzz

from testifier_audit.names.nickname_map import nickname_root


@dataclass(frozen=True, slots=True)
class LinkageThresholds:
    strong_fuzzy_min_score: float = 92.0
    weak_fuzzy_min_score: float = 84.0
    ambiguous_score_gap: float = 2.0


def split_canonical_name(value: str) -> tuple[str, str]:
    raw = str(value or "")
    if "|" not in raw:
        return "", ""
    last, first = raw.split("|", 1)
    return last.strip(), first.strip()


def classify_name_linkage(
    *,
    submission_names: list[str],
    exact_lookup: dict[str, int],
    candidate_lookup_by_last: dict[str, list[dict[str, object]]],
    nickname_map: dict[str, str],
    thresholds: LinkageThresholds,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for canonical_name in submission_names:
        last_name, first_name = split_canonical_name(canonical_name)
        first_root = nickname_root(first_name, nickname_map)
        candidates = candidate_lookup_by_last.get(last_name, [])
        caveats: list[str] = []

        if canonical_name in exact_lookup:
            registry_rows = int(exact_lookup.get(canonical_name, 0) or 0)
            primary_outcome = "matched_unique" if registry_rows == 1 else "matched_ambiguous"
            rows.append(
                {
                    "canonical_name": canonical_name,
                    "match_tier": "exact",
                    "primary_outcome": primary_outcome,
                    "balanced_outcome": primary_outcome,
                    "broad_outcome": primary_outcome,
                    "matched_registry_name": canonical_name,
                    "matched_registry_rows": registry_rows,
                    "best_similarity_score": 1.0,
                    "candidate_pool_size": len(candidates),
                    "is_ambiguous": primary_outcome == "matched_ambiguous",
                    "match_caveat": "",
                }
            )
            continue

        nickname_candidates = []
        for candidate in candidates:
            candidate_first = str(candidate.get("canonical_first", "") or "").strip()
            if not candidate_first:
                continue
            if nickname_root(candidate_first, nickname_map) == first_root and first_root:
                nickname_candidates.append(candidate)

        if nickname_candidates:
            best = nickname_candidates[0]
            matched_registry_name = str(best.get("canonical_name", "") or "")
            matched_registry_rows = int(best.get("n_registry_rows", 0) or 0)
            ambiguous = len(nickname_candidates) > 1 or matched_registry_rows > 1
            primary_outcome = "matched_ambiguous" if ambiguous else "matched_unique"
            rows.append(
                {
                    "canonical_name": canonical_name,
                    "match_tier": "nickname_exact",
                    "primary_outcome": primary_outcome,
                    "balanced_outcome": primary_outcome,
                    "broad_outcome": primary_outcome,
                    "matched_registry_name": matched_registry_name,
                    "matched_registry_rows": matched_registry_rows,
                    "best_similarity_score": 1.0,
                    "candidate_pool_size": len(candidates),
                    "is_ambiguous": ambiguous,
                    "match_caveat": "nickname_equivalent",
                }
            )
            continue

        if not first_name:
            rows.append(
                {
                    "canonical_name": canonical_name,
                    "match_tier": "unmatched",
                    "primary_outcome": "unmatched",
                    "balanced_outcome": "unmatched",
                    "broad_outcome": "unmatched",
                    "matched_registry_name": "",
                    "matched_registry_rows": 0,
                    "best_similarity_score": None,
                    "candidate_pool_size": len(candidates),
                    "is_ambiguous": False,
                    "match_caveat": "missing_first_name_token",
                }
            )
            continue

        if not candidates:
            rows.append(
                {
                    "canonical_name": canonical_name,
                    "match_tier": "unmatched",
                    "primary_outcome": "unmatched",
                    "balanced_outcome": "unmatched",
                    "broad_outcome": "unmatched",
                    "matched_registry_name": "",
                    "matched_registry_rows": 0,
                    "best_similarity_score": None,
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
            score = float(fuzz.ratio(first_name, candidate_first))
            scored_candidates.append(
                {
                    "canonical_name": str(candidate.get("canonical_name", "") or ""),
                    "n_registry_rows": int(candidate.get("n_registry_rows", 0) or 0),
                    "score": score,
                }
            )
        if not scored_candidates:
            rows.append(
                {
                    "canonical_name": canonical_name,
                    "match_tier": "unmatched",
                    "primary_outcome": "unmatched",
                    "balanced_outcome": "unmatched",
                    "broad_outcome": "unmatched",
                    "matched_registry_name": "",
                    "matched_registry_rows": 0,
                    "best_similarity_score": None,
                    "candidate_pool_size": len(candidates),
                    "is_ambiguous": False,
                    "match_caveat": "no_first_name_candidates",
                }
            )
            continue
        scored_candidates.sort(key=lambda item: float(item["score"]), reverse=True)
        best = scored_candidates[0]
        second_best = scored_candidates[1] if len(scored_candidates) > 1 else None
        best_score = float(best["score"])
        second_best_score = float(second_best["score"]) if second_best else None
        is_ambiguous = bool(
            second_best_score is not None
            and abs(best_score - second_best_score) <= float(thresholds.ambiguous_score_gap)
        )
        matched_registry_rows = int(best.get("n_registry_rows", 0) or 0)

        match_tier = "unmatched"
        if best_score >= float(thresholds.strong_fuzzy_min_score):
            match_tier = "strong_fuzzy"
        elif best_score >= float(thresholds.weak_fuzzy_min_score):
            match_tier = "weak_fuzzy"
        else:
            caveats.append("below_similarity_threshold")

        primary_outcome = "unmatched"
        balanced_outcome = "unmatched"
        broad_outcome = "unmatched"
        if match_tier in {"strong_fuzzy", "weak_fuzzy"}:
            if is_ambiguous or matched_registry_rows > 1:
                broad_outcome = "matched_ambiguous"
            else:
                broad_outcome = "matched_unique"
        if match_tier == "strong_fuzzy":
            if is_ambiguous or matched_registry_rows > 1:
                balanced_outcome = "matched_ambiguous"
            else:
                balanced_outcome = "matched_unique"

        if is_ambiguous:
            caveats.append("ambiguous_top_candidate")
        if match_tier == "weak_fuzzy":
            caveats.append("weak_similarity")

        rows.append(
            {
                "canonical_name": canonical_name,
                "match_tier": match_tier,
                "primary_outcome": primary_outcome,
                "balanced_outcome": balanced_outcome,
                "broad_outcome": broad_outcome,
                "matched_registry_name": str(best.get("canonical_name", "") or "")
                if match_tier != "unmatched"
                else "",
                "matched_registry_rows": matched_registry_rows if match_tier != "unmatched" else 0,
                "best_similarity_score": best_score / 100.0 if match_tier != "unmatched" else None,
                "candidate_pool_size": len(candidates),
                "is_ambiguous": is_ambiguous,
                "match_caveat": ",".join(caveats),
            }
        )
    return pd.DataFrame(rows)
