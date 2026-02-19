from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher

import pandas as pd

try:
    from rapidfuzz import fuzz
except ImportError:  # pragma: no cover
    class _FallbackFuzz:
        @staticmethod
        def token_set_ratio(left: str, right: str) -> float:
            left_tokens = " ".join(sorted(set(left.split())))
            right_tokens = " ".join(sorted(set(right.split())))
            return SequenceMatcher(a=left_tokens, b=right_tokens).ratio() * 100.0

    fuzz = _FallbackFuzz()

from testifier_audit.detectors.base import Detector, DetectorResult
from testifier_audit.features.similarity import build_blocking_key


@dataclass
class _UnionFind:
    parent: dict[str, str]

    def __init__(self) -> None:
        self.parent = {}

    def find(self, item: str) -> str:
        root = self.parent.setdefault(item, item)
        if root != item:
            root = self.find(root)
            self.parent[item] = root
        return root

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root


class DuplicatesNearDetector(Detector):
    name = "duplicates_near"

    def __init__(self, similarity_threshold: int, max_candidates_per_block: int) -> None:
        self.similarity_threshold = similarity_threshold
        self.max_candidates_per_block = max_candidates_per_block

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        required = ["canonical_name", "name_display", "last", "first", "first_canonical"]
        missing = [column for column in required if column not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns for near-duplicate detection: {', '.join(missing)}")

        name_nodes = df[required].drop_duplicates(subset=["canonical_name"]).copy()
        name_nodes["block_key"] = build_blocking_key(name_nodes)

        candidate_blocks = (
            name_nodes.groupby("block_key", dropna=True)
            .agg(n_unique_names=("canonical_name", "nunique"))
            .reset_index()
            .sort_values("n_unique_names", ascending=False)
        )
        candidate_blocks = candidate_blocks[candidate_blocks["n_unique_names"] > 1]

        edge_rows: list[dict[str, object]] = []
        skipped_blocks: list[dict[str, object]] = []

        for block_key, block in name_nodes.groupby("block_key", dropna=True):
            if len(block) < 2:
                continue
            if len(block) > self.max_candidates_per_block:
                skipped_blocks.append(
                    {
                        "block_key": block_key,
                        "n_candidates": int(len(block)),
                        "reason": "exceeds_max_candidates_per_block",
                    }
                )
                continue

            records = block[["canonical_name", "name_display"]].to_dict("records")
            for left_idx in range(len(records)):
                left_name = str(records[left_idx]["canonical_name"])
                left_display = str(records[left_idx]["name_display"])
                for right_idx in range(left_idx + 1, len(records)):
                    right_name = str(records[right_idx]["canonical_name"])
                    right_display = str(records[right_idx]["name_display"])
                    if left_name == right_name:
                        continue

                    score = int(fuzz.token_set_ratio(left_display, right_display))
                    if score >= self.similarity_threshold:
                        edge_rows.append(
                            {
                                "block_key": block_key,
                                "left_canonical_name": left_name,
                                "right_canonical_name": right_name,
                                "left_display_name": left_display,
                                "right_display_name": right_display,
                                "similarity": score,
                            }
                        )

        edges = pd.DataFrame(edge_rows)
        skipped = pd.DataFrame(skipped_blocks)

        if edges.empty:
            summary = {
                "candidate_blocks": int(len(candidate_blocks)),
                "n_similarity_edges": 0,
                "n_clusters": 0,
                "max_cluster_size": 0,
                "n_skipped_blocks": int(len(skipped)),
            }
            return DetectorResult(
                detector=self.name,
                summary=summary,
                tables={
                    "candidate_blocks": candidate_blocks,
                    "similarity_edges": edges,
                    "cluster_summary": pd.DataFrame(),
                    "cluster_members": pd.DataFrame(),
                    "skipped_blocks": skipped,
                },
            )

        uf = _UnionFind()
        for row in edges.itertuples(index=False):
            uf.union(str(row.left_canonical_name), str(row.right_canonical_name))

        components: dict[str, set[str]] = {}
        for name in pd.unique(edges[["left_canonical_name", "right_canonical_name"]].values.ravel("K")):
            root = uf.find(str(name))
            components.setdefault(root, set()).add(str(name))

        component_items = sorted(components.items(), key=lambda item: (-len(item[1]), item[0]))
        cluster_lookup: dict[str, str] = {}
        for idx, (_, names) in enumerate(component_items, start=1):
            cluster_id = f"cluster_{idx:04d}"
            for name in names:
                cluster_lookup[name] = cluster_id

        cluster_members = (
            name_nodes[name_nodes["canonical_name"].isin(cluster_lookup.keys())]
            .copy()
            .assign(cluster_id=lambda frame: frame["canonical_name"].map(cluster_lookup))
        )

        name_activity = (
            df.groupby("canonical_name", dropna=True)
            .agg(
                n_records=("id", "count"),
                first_seen=("timestamp", "min"),
                last_seen=("timestamp", "max"),
                n_pro=("position_normalized", lambda s: int((s == "Pro").sum())),
                n_con=("position_normalized", lambda s: int((s == "Con").sum())),
            )
            .reset_index()
        )

        cluster_members = cluster_members.merge(name_activity, on="canonical_name", how="left")

        cluster_summary = (
            cluster_members.groupby("cluster_id", dropna=True)
            .agg(
                cluster_size=("canonical_name", "nunique"),
                n_records=("n_records", "sum"),
                first_seen=("first_seen", "min"),
                last_seen=("last_seen", "max"),
                n_pro=("n_pro", "sum"),
                n_con=("n_con", "sum"),
                members_preview=(
                    "name_display",
                    lambda s: " | ".join(sorted({str(value) for value in s})[:10]),
                ),
            )
            .reset_index()
            .sort_values(["cluster_size", "n_records", "cluster_id"], ascending=[False, False, True])
        )
        cluster_summary["time_span_minutes"] = (
            (cluster_summary["last_seen"] - cluster_summary["first_seen"]).dt.total_seconds() / 60.0
        ).fillna(0.0)

        summary = {
            "candidate_blocks": int(len(candidate_blocks)),
            "n_similarity_edges": int(len(edges)),
            "n_clusters": int(cluster_summary["cluster_id"].nunique()),
            "max_cluster_size": int(cluster_summary["cluster_size"].max()) if not cluster_summary.empty else 0,
            "n_skipped_blocks": int(len(skipped)),
        }

        return DetectorResult(
            detector=self.name,
            summary=summary,
            tables={
                "candidate_blocks": candidate_blocks,
                "similarity_edges": edges.sort_values("similarity", ascending=False),
                "cluster_summary": cluster_summary,
                "cluster_members": cluster_members.sort_values(["cluster_id", "name_display"]),
                "skipped_blocks": skipped,
            },
        )
