from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from testifier_audit.detectors.base import DetectorResult
from testifier_audit.report import render
from testifier_audit.report.render import (
    _artifact_rows_from_disk,
    _evidence_bundle_preview_from_disk,
    _evidence_bundle_preview_from_results,
    _load_summaries_from_disk,
    _load_table_previews_from_disk,
    _periodicity_table_preview_from_disk,
    _periodicity_table_preview_from_results,
    _rare_names_table_preview_from_disk,
    _rare_names_table_preview_from_results,
    _serialize_value,
    render_report,
)


def test_serialize_value_handles_timestamp_timedelta_nan_and_item_paths(
    monkeypatch,
) -> None:
    assert _serialize_value(pd.Timestamp("2026-02-01T12:00:00Z")) == "2026-02-01T12:00:00+00:00"
    assert _serialize_value(pd.Timedelta(minutes=5)) == "0 days 00:05:00"
    assert _serialize_value(np.nan) is None

    class HasItem:
        def item(self) -> int:
            return 7

    assert _serialize_value(HasItem()) == 7

    class BrokenItem:
        def item(self) -> int:
            raise RuntimeError("boom")

    broken = BrokenItem()
    monkeypatch.setattr(render.pd, "isna", lambda _value: (_ for _ in ()).throw(TypeError("x")))
    assert _serialize_value(broken) == str(broken)


def test_disk_summary_artifact_and_table_preview_loaders(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    summary_dir = out_dir / "summary"
    artifacts_dir = out_dir / "artifacts"
    tables_dir = out_dir / "tables"
    summary_dir.mkdir(parents=True)
    artifacts_dir.mkdir(parents=True)
    tables_dir.mkdir(parents=True)

    (summary_dir / "bursts.json").write_text(json.dumps({"n": 1}), encoding="utf-8")
    pd.DataFrame({"value": [1, 2]}).to_csv(artifacts_dir / "counts.csv", index=False)
    pd.DataFrame({"value": [1, 2, 3]}).to_parquet(artifacts_dir / "counts_parquet.parquet", index=False)

    (tables_dir / "skip_me.csv").write_text("a,b\n1,2\n", encoding="utf-8")  # no "__" in stem
    (tables_dir / "detector__unsupported.txt").write_text("ignore-me", encoding="utf-8")
    (tables_dir / "detector__bad.parquet").write_text("not parquet bytes", encoding="utf-8")
    pd.DataFrame({"x": [1], "y": ["v"]}).to_csv(tables_dir / "detector__table.csv", index=False)

    assert _load_summaries_from_disk(out_dir)["bursts"]["n"] == 1

    artifact_rows = _artifact_rows_from_disk(out_dir)
    assert artifact_rows["counts"] == 2
    assert artifact_rows["counts_parquet"] == 3

    previews = _load_table_previews_from_disk(out_dir, max_rows=5)
    assert "detector" in previews
    assert "table" in previews["detector"]
    assert previews["detector"]["table"][0]["x"] == 1


def test_preview_helpers_cover_results_and_disk_paths(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    tables_dir = out_dir / "tables"
    tables_dir.mkdir(parents=True)

    assert _evidence_bundle_preview_from_results({}) == []
    assert _evidence_bundle_preview_from_disk(tmp_path / "missing") == []
    assert _rare_names_table_preview_from_results({}, "rarity_lookup_coverage") == []
    assert _rare_names_table_preview_from_disk(tmp_path / "missing", "rarity_lookup_coverage") == []
    assert _periodicity_table_preview_from_results({}, "clockface_top_minutes") == []
    assert _periodicity_table_preview_from_disk(tmp_path / "missing", "clockface_top_minutes") == []

    empty_composite_csv = tables_dir / "composite_score__evidence_bundle_windows.csv"
    empty_composite_csv.write_text("minute_bucket\n", encoding="utf-8")
    assert _evidence_bundle_preview_from_disk(out_dir) == []

    pd.DataFrame({"minute_bucket": ["2026-02-01T00:00:00Z"], "score": [0.9]}).to_csv(
        tables_dir / "composite_score__evidence_bundle_windows.csv",
        index=False,
    )
    evidence_disk = _evidence_bundle_preview_from_disk(out_dir)
    assert evidence_disk and evidence_disk[0]["score"] == 0.9

    pd.DataFrame({"metric": ["a"], "value": [1]}).to_parquet(
        tables_dir / "rare_names__rarity_lookup_coverage.parquet",
        index=False,
    )
    rare_disk = _rare_names_table_preview_from_disk(out_dir, "rarity_lookup_coverage")
    assert rare_disk and rare_disk[0]["metric"] == "a"

    pd.DataFrame({"minute_of_hour": [0], "n_events": [10]}).to_parquet(
        tables_dir / "periodicity__clockface_top_minutes.parquet",
        index=False,
    )
    periodic_disk = _periodicity_table_preview_from_disk(out_dir, "clockface_top_minutes")
    assert periodic_disk and periodic_disk[0]["minute_of_hour"] == 0

    results = {
        "composite_score": DetectorResult(
            detector="composite_score",
            summary={},
            tables={"evidence_bundle_windows": pd.DataFrame({"window": [1]})},
        ),
        "rare_names": DetectorResult(
            detector="rare_names",
            summary={},
            tables={"rarity_lookup_coverage": pd.DataFrame({"metric": ["x"]})},
        ),
        "periodicity": DetectorResult(
            detector="periodicity",
            summary={},
            tables={"clockface_top_minutes": pd.DataFrame({"minute_of_hour": [5]})},
        ),
    }
    assert _evidence_bundle_preview_from_results(results)[0]["window"] == 1
    assert _rare_names_table_preview_from_results(results, "rarity_lookup_coverage")[0]["metric"] == "x"
    assert _periodicity_table_preview_from_results(results, "clockface_top_minutes")[0]["minute_of_hour"] == 5


def test_render_report_uses_disk_fallback_when_results_are_empty(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    (out_dir / "summary").mkdir(parents=True)
    (out_dir / "artifacts").mkdir(parents=True)
    (out_dir / "tables").mkdir(parents=True)
    (out_dir / "figures").mkdir(parents=True)

    (out_dir / "summary" / "bursts.json").write_text(json.dumps({"n_significant_windows": 2}), encoding="utf-8")
    pd.DataFrame({"value": [1]}).to_csv(out_dir / "artifacts" / "counts_per_minute.csv", index=False)
    pd.DataFrame({"window": [1], "score": [0.99]}).to_csv(
        out_dir / "tables" / "composite_score__evidence_bundle_windows.csv",
        index=False,
    )
    pd.DataFrame({"metric": ["coverage"], "value": [0.5]}).to_csv(
        out_dir / "tables" / "rare_names__rarity_lookup_coverage.csv",
        index=False,
    )
    pd.DataFrame({"token": ["ZZZ"], "count": [1]}).to_csv(
        out_dir / "tables" / "rare_names__rarity_unmatched_first_tokens.csv",
        index=False,
    )
    pd.DataFrame({"token": ["YYY"], "count": [1]}).to_csv(
        out_dir / "tables" / "rare_names__rarity_unmatched_last_tokens.csv",
        index=False,
    )
    pd.DataFrame({"minute_of_hour": [12], "n_events": [5]}).to_csv(
        out_dir / "tables" / "periodicity__clockface_top_minutes.csv",
        index=False,
    )
    (out_dir / "figures" / "example.png").write_bytes(b"png")

    report_path = render_report(results={}, artifacts={}, out_dir=out_dir)
    rendered = report_path.read_text(encoding="utf-8")
    assert report_path.exists()
    assert "Evidence Bundle Windows" in rendered
    assert "Rarity Coverage" in rendered
    assert "Clock-face Timing" in rendered
