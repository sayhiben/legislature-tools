#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import re
import unicodedata
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path


SOURCE_COMMIT = "e13a5c051689bebe5178c0b2d4730cb46a3cb698"
SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    f"carltonnorthern/nicknames/{SOURCE_COMMIT}/names.csv"
)
SUPPLEMENTAL_SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "tfmorris/Names/master/eval/src/main/resources/givenname_nicknames.txt"
)
OUTPUT_PATH = Path(__file__).resolve().parents[2] / "configs" / "nicknames.csv"
DEFAULT_CONFLICT_SAMPLE_LIMIT = 50

# Preserve the project-specific nicknames that were previously curated.
MANUAL_OVERRIDES: dict[str, str] = {
    "BOB": "ROBERT",
    "BILL": "WILLIAM",
    "BECKY": "REBECCA",
    "JIM": "JAMES",
}

TOKEN_RE = re.compile(r"[^A-Z'\- ]")
WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class MergeStats:
    primary_unambiguous_aliases: int
    supplemental_unambiguous_aliases: int
    supplemental_aliases_added: int
    supplemental_alias_conflicts: int
    supplemental_aliases_skipped_root_flip: int
    manual_overrides_added: int
    manual_overrides_updated: int
    output_aliases: int
    conflict_samples: list[dict[str, str]]
    root_flip_samples: list[dict[str, str]]


def normalize_token(value: str) -> str:
    text = unicodedata.normalize("NFKD", value)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.strip().upper()
    text = TOKEN_RE.sub("", text)
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text


def _load_text(url: str) -> str:
    with urllib.request.urlopen(url, timeout=30) as response:
        return response.read().decode("utf-8")


def load_primary_rows() -> list[dict[str, str]]:
    payload = _load_text(SOURCE_URL)
    reader = csv.DictReader(io.StringIO(payload))
    return [dict(row) for row in reader]


def load_supplemental_lines() -> list[str]:
    payload = _load_text(SUPPLEMENTAL_SOURCE_URL)
    return payload.splitlines()


def _unambiguous_alias_mapping(alias_to_canonicals: dict[str, set[str]]) -> dict[str, str]:
    return {
        alias: next(iter(canonicals))
        for alias, canonicals in alias_to_canonicals.items()
        if len(canonicals) == 1
    }


def build_primary_mapping(rows: list[dict[str, str]]) -> dict[str, str]:
    by_alias: dict[str, set[str]] = defaultdict(set)

    for row in rows:
        if (row.get("relationship") or "").strip() != "has_nickname":
            continue
        canonical = normalize_token(row.get("name1") or "")
        alias = normalize_token(row.get("name2") or "")
        if not canonical or not alias:
            continue
        if alias == canonical:
            continue
        if " " in alias or " " in canonical:
            # The pipeline canonicalizes only the first token.
            continue
        by_alias[alias].add(canonical)

    return _unambiguous_alias_mapping(by_alias)


def build_supplemental_mapping(lines: list[str]) -> dict[str, str]:
    by_alias: dict[str, set[str]] = defaultdict(set)
    for raw_line in lines:
        line = str(raw_line or "").strip()
        if not line or line.startswith("#"):
            continue

        raw_tokens = [token for token in line.split(" ") if token.strip()]
        tokens = [normalize_token(token) for token in raw_tokens]
        tokens = [token for token in tokens if token]
        if len(tokens) < 2:
            continue

        canonical = tokens[0]
        if " " in canonical:
            continue

        for alias in tokens[1:]:
            if alias == canonical:
                continue
            if " " in alias:
                continue
            by_alias[alias].add(canonical)

    return _unambiguous_alias_mapping(by_alias)


def merge_mappings(
    *,
    primary_mapping: dict[str, str],
    supplemental_mapping: dict[str, str],
    protected_aliases: set[str],
    conflict_sample_limit: int = DEFAULT_CONFLICT_SAMPLE_LIMIT,
) -> tuple[dict[str, str], int, list[dict[str, str]], int, list[dict[str, str]]]:
    merged = dict(primary_mapping)
    supplemental_conflicts = 0
    conflict_samples: list[dict[str, str]] = []
    root_flip_skips = 0
    root_flip_samples: list[dict[str, str]] = []

    for alias, supplemental_canonical in supplemental_mapping.items():
        primary_canonical = merged.get(alias)
        if primary_canonical is None:
            # Prevent root flips: do not add a supplemental alias if that alias
            # is already a primary canonical token (for example TONY or DEBBIE).
            if alias in protected_aliases:
                root_flip_skips += 1
                if len(root_flip_samples) < max(0, int(conflict_sample_limit)):
                    root_flip_samples.append(
                        {
                            "alias": alias,
                            "supplemental_canonical": supplemental_canonical,
                        }
                    )
                continue
            merged[alias] = supplemental_canonical
            continue
        if primary_canonical == supplemental_canonical:
            continue

        supplemental_conflicts += 1
        if len(conflict_samples) < max(0, int(conflict_sample_limit)):
            conflict_samples.append(
                {
                    "alias": alias,
                    "primary_canonical": primary_canonical,
                    "supplemental_canonical": supplemental_canonical,
                }
            )

    return merged, supplemental_conflicts, conflict_samples, root_flip_skips, root_flip_samples


def apply_manual_overrides(mapping: dict[str, str]) -> tuple[dict[str, str], int, int]:
    merged = dict(mapping)
    manual_added = 0
    manual_updated = 0
    for alias, canonical in MANUAL_OVERRIDES.items():
        existing = merged.get(alias)
        if existing is None:
            manual_added += 1
        elif existing != canonical:
            manual_updated += 1
        merged[alias] = canonical
    return merged, manual_added, manual_updated


def build_mapping(*, include_supplemental: bool) -> tuple[dict[str, str], MergeStats]:
    primary_rows = load_primary_rows()
    primary_mapping = build_primary_mapping(primary_rows)
    primary_canonical_tokens = set(primary_mapping.values())

    merged = dict(primary_mapping)
    supplemental_mapping: dict[str, str] = {}
    supplemental_conflicts = 0
    conflict_samples: list[dict[str, str]] = []
    root_flip_skips = 0
    root_flip_samples: list[dict[str, str]] = []

    if include_supplemental:
        supplemental_lines = load_supplemental_lines()
        supplemental_mapping = build_supplemental_mapping(supplemental_lines)
        (
            merged,
            supplemental_conflicts,
            conflict_samples,
            root_flip_skips,
            root_flip_samples,
        ) = merge_mappings(
            primary_mapping=merged,
            supplemental_mapping=supplemental_mapping,
            protected_aliases=primary_canonical_tokens,
        )

    supplemental_added = sum(1 for alias in supplemental_mapping if alias not in primary_mapping)
    supplemental_added -= int(root_flip_skips)

    merged, manual_added, manual_updated = apply_manual_overrides(merged)
    stats = MergeStats(
        primary_unambiguous_aliases=len(primary_mapping),
        supplemental_unambiguous_aliases=len(supplemental_mapping),
        supplemental_aliases_added=int(supplemental_added),
        supplemental_alias_conflicts=int(supplemental_conflicts),
        supplemental_aliases_skipped_root_flip=int(root_flip_skips),
        manual_overrides_added=int(manual_added),
        manual_overrides_updated=int(manual_updated),
        output_aliases=len(merged),
        conflict_samples=conflict_samples,
        root_flip_samples=root_flip_samples,
    )
    return merged, stats


def write_mapping(mapping: dict[str, str], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["canonical", "alias"])
        writer.writeheader()
        for alias in sorted(mapping):
            writer.writerow({"canonical": mapping[alias], "alias": alias})


def write_report(
    *,
    report_path: Path,
    include_supplemental: bool,
    stats: MergeStats,
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "primary_source_url": SOURCE_URL,
        "supplemental_source_url": SUPPLEMENTAL_SOURCE_URL if include_supplemental else None,
        "include_supplemental": bool(include_supplemental),
        "primary_unambiguous_aliases": int(stats.primary_unambiguous_aliases),
        "supplemental_unambiguous_aliases": int(stats.supplemental_unambiguous_aliases),
        "supplemental_aliases_added": int(stats.supplemental_aliases_added),
        "supplemental_alias_conflicts": int(stats.supplemental_alias_conflicts),
        "supplemental_aliases_skipped_root_flip": int(stats.supplemental_aliases_skipped_root_flip),
        "manual_overrides_added": int(stats.manual_overrides_added),
        "manual_overrides_updated": int(stats.manual_overrides_updated),
        "output_aliases": int(stats.output_aliases),
        "conflict_samples": list(stats.conflict_samples),
        "root_flip_samples": list(stats.root_flip_samples),
    }
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Regenerate nickname alias mapping CSV.")
    parser.set_defaults(include_supplemental=True)
    merge_group = parser.add_mutually_exclusive_group()
    merge_group.add_argument(
        "--include-supplemental",
        dest="include_supplemental",
        action="store_true",
        help="Merge the supplemental tfmorris nickname source (default behavior).",
    )
    merge_group.add_argument(
        "--primary-only",
        dest="include_supplemental",
        action="store_false",
        help="Skip supplemental merge and use only the primary source + manual overrides.",
    )
    parser.add_argument(
        "--output-path",
        default=str(OUTPUT_PATH),
        help="Destination CSV path (default: configs/nicknames.csv).",
    )
    parser.add_argument(
        "--report-path",
        default="",
        help="Optional JSON report path with merge/conflict stats.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    output_path = Path(args.output_path).resolve()
    include_supplemental = bool(args.include_supplemental)
    mapping, stats = build_mapping(include_supplemental=include_supplemental)
    write_mapping(mapping, output_path=output_path)

    report_path_raw = str(args.report_path or "").strip()
    if report_path_raw:
        write_report(
            report_path=Path(report_path_raw).resolve(),
            include_supplemental=include_supplemental,
            stats=stats,
        )

    print(f"primary_source_url={SOURCE_URL}")
    print(f"supplemental_source_url={SUPPLEMENTAL_SOURCE_URL}")
    print(f"include_supplemental={include_supplemental}")
    print(f"output_path={output_path}")
    print(f"primary_unambiguous_aliases={stats.primary_unambiguous_aliases}")
    print(f"supplemental_unambiguous_aliases={stats.supplemental_unambiguous_aliases}")
    print(f"supplemental_aliases_added={stats.supplemental_aliases_added}")
    print(f"supplemental_alias_conflicts={stats.supplemental_alias_conflicts}")
    print(f"supplemental_aliases_skipped_root_flip={stats.supplemental_aliases_skipped_root_flip}")
    print(f"manual_overrides_added={stats.manual_overrides_added}")
    print(f"manual_overrides_updated={stats.manual_overrides_updated}")
    print(f"rows_written={stats.output_aliases}")
    if report_path_raw:
        print(f"report_path={Path(report_path_raw).resolve()}")


if __name__ == "__main__":
    main()
