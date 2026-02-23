from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from rapidfuzz import fuzz, process

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
    candidate_index: dict[
        str,
        dict[str, object],
    ] = {}
    for raw_last_name, raw_candidates in candidate_lookup_by_last.items():
        last_name = str(raw_last_name or "").strip()
        if not last_name:
            continue
        prepared_candidates: list[dict[str, object]] = []
        prepared_by_root: dict[str, list[dict[str, object]]] = {}
        candidate_firsts: list[str] = []
        for candidate in raw_candidates:
            candidate_first = str(candidate.get("canonical_first", "") or "").strip()
            if not candidate_first:
                continue
            candidate_name = str(candidate.get("canonical_name", "") or "").strip()
            if not candidate_name:
                continue
            registry_rows_raw = candidate.get("n_registry_rows", 0)
            try:
                registry_rows = int(registry_rows_raw or 0)
            except (TypeError, ValueError):
                registry_rows = 0
            first_root = nickname_root(candidate_first, nickname_map)
            prepared = {
                "canonical_name": candidate_name,
                "canonical_first": candidate_first,
                "n_registry_rows": registry_rows,
            }
            prepared_candidates.append(prepared)
            candidate_firsts.append(candidate_first)
            if first_root:
                prepared_by_root.setdefault(first_root, []).append(prepared)
        candidate_index[last_name] = {
            "pool_size": len(raw_candidates),
            "candidates": prepared_candidates,
            "candidate_firsts": candidate_firsts,
            "by_root": prepared_by_root,
        }

    rows: list[dict[str, object]] = []
    for canonical_name in submission_names:
        last_name, first_name = split_canonical_name(canonical_name)
        first_root = nickname_root(first_name, nickname_map)
        candidate_bundle = candidate_index.get(last_name)
        if candidate_bundle:
            candidate_pool_size = int(candidate_bundle["pool_size"])
            candidates = candidate_bundle["candidates"]
            candidate_firsts = candidate_bundle["candidate_firsts"]
            nickname_candidates = (
                candidate_bundle["by_root"].get(first_root, []) if first_root else []
            )
        else:
            candidate_pool_size = 0
            candidates = []
            candidate_firsts = []
            nickname_candidates = []
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
                    "candidate_pool_size": candidate_pool_size,
                    "is_ambiguous": primary_outcome == "matched_ambiguous",
                    "match_caveat": "",
                }
            )
            continue

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
                    "candidate_pool_size": candidate_pool_size,
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
                    "candidate_pool_size": candidate_pool_size,
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
                    "candidate_pool_size": candidate_pool_size,
                    "is_ambiguous": False,
                    "match_caveat": "no_last_name_candidates",
                }
            )
            continue

        best_candidate: dict[str, object] | None = None
        if candidate_firsts:
            top_matches = process.extract(first_name, candidate_firsts, scorer=fuzz.ratio, limit=2)
            if top_matches:
                best_index = int(top_matches[0][2])
                best_candidate = candidates[best_index]
                best_score = float(top_matches[0][1])
                second_best_score = float(top_matches[1][1]) if len(top_matches) > 1 else None
            else:
                best_score = -1.0
                second_best_score = None
        else:
            best_score = -1.0
            second_best_score = None

        if best_candidate is None:
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
                    "candidate_pool_size": candidate_pool_size,
                    "is_ambiguous": False,
                    "match_caveat": "no_first_name_candidates",
                }
            )
            continue

        is_ambiguous = bool(
            second_best_score is not None
            and abs(best_score - second_best_score) <= float(thresholds.ambiguous_score_gap)
        )
        matched_registry_rows = int(best_candidate.get("n_registry_rows", 0) or 0)

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
                "matched_registry_name": str(best_candidate.get("canonical_name", "") or "")
                if match_tier != "unmatched"
                else "",
                "matched_registry_rows": matched_registry_rows if match_tier != "unmatched" else 0,
                "best_similarity_score": best_score / 100.0 if match_tier != "unmatched" else None,
                "candidate_pool_size": candidate_pool_size,
                "is_ambiguous": is_ambiguous,
                "match_caveat": ",".join(caveats),
            }
        )
    return pd.DataFrame(rows)
