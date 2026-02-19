from __future__ import annotations

from pathlib import Path

import pandas as pd

from testifier_audit.detectors.rare_names import RareNamesDetector
from testifier_audit.features.rarity import load_name_frequency_lookup


def test_load_name_frequency_lookup_from_counts(tmp_path: Path) -> None:
    lookup_path = tmp_path / "first_names.csv"
    lookup_path.write_text(
        "\n".join(
            [
                "name,count",
                "Jane,100",
                "JANE,25",
                "John,150",
            ]
        ),
        encoding="utf-8",
    )

    lookup = load_name_frequency_lookup(str(lookup_path))
    assert set(lookup.keys()) == {"JANE", "JOHN"}
    assert lookup["JOHN"] > lookup["JANE"]
    assert abs(sum(lookup.values()) - 1.0) < 1e-9


def test_rare_names_detector_rarity_enrichment(tmp_path: Path) -> None:
    first_lookup_path = tmp_path / "first.csv"
    first_lookup_path.write_text(
        "\n".join(
            [
                "name,count",
                "JANE,500",
                "JOHN,900",
                "X AE A-12,1",
            ]
        ),
        encoding="utf-8",
    )
    last_lookup_path = tmp_path / "last.csv"
    last_lookup_path.write_text(
        "\n".join(
            [
                "name,count",
                "DOE,1200",
                "SMITH,2200",
                "RARELAST,1",
            ]
        ),
        encoding="utf-8",
    )

    minute_bucket = pd.to_datetime(
        [
            "2026-02-03 17:00:00-08:00",
            "2026-02-03 17:00:00-08:00",
            "2026-02-03 17:01:00-08:00",
            "2026-02-03 17:01:00-08:00",
            "2026-02-03 17:02:00-08:00",
        ]
    )
    df = pd.DataFrame(
        {
            "minute_bucket": minute_bucket,
            "canonical_name": ["DOE|JANE", "SMITH|JOHN", "DOE|JANE", "SMITH|JOHN", "RARELAST|X AE A-12"],
            "name_display": ["DOE, JANE", "SMITH, JOHN", "DOE, JANE", "SMITH, JOHN", "RARELAST, X AE A-12"],
            "position_normalized": ["Pro", "Con", "Pro", "Con", "Pro"],
            "first": ["JANE", "JOHN", "JANE", "JOHN", "X AE A-12"],
            "first_canonical": ["JANE", "JOHN", "JANE", "JOHN", "X AE A-12"],
            "last": ["DOE", "SMITH", "DOE", "SMITH", "RARELAST"],
        }
    )
    name_frequency = pd.DataFrame(
        {
            "canonical_name": ["DOE|JANE", "SMITH|JOHN", "RARELAST|X AE A-12"],
            "n": [2, 2, 1],
        }
    )
    counts_per_minute = pd.DataFrame(
        {
            "minute_bucket": pd.to_datetime(
                [
                    "2026-02-03 17:00:00-08:00",
                    "2026-02-03 17:01:00-08:00",
                    "2026-02-03 17:02:00-08:00",
                ]
            ),
            "n_total": [2, 2, 1],
            "unique_ratio": [1.0, 1.0, 1.0],
        }
    )

    detector = RareNamesDetector(
        min_window_total=1,
        rarity_enabled=True,
        first_name_frequency_path=str(first_lookup_path),
        last_name_frequency_path=str(last_lookup_path),
        rarity_epsilon=1e-9,
    )
    result = detector.run(
        df=df,
        features={
            "name_frequency": name_frequency,
            "counts_per_minute": counts_per_minute,
            "name_text_features": pd.DataFrame(),
        },
    )

    assert result.summary["rarity_enrichment_enabled"]
    assert result.summary["rarity_enrichment_active"]
    assert result.summary["first_lookup_size"] > 0
    assert result.summary["last_lookup_size"] > 0
    assert not result.tables["rarity_by_minute"].empty
    assert not result.tables["rarity_top_records"].empty
    assert not result.tables["rarity_lookup_coverage"].empty
