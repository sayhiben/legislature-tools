from __future__ import annotations

import json
import math
from pathlib import Path

from testifier_audit.report.global_baselines import (
    build_feature_vector,
    build_global_baselines_from_reports_dir,
    build_leave_one_out_baseline_from_reports_dir,
    load_cross_hearing_baseline,
    normalize_leave_one_out_baseline_payload,
    write_global_baselines,
)


def _feature_payload(
    *,
    report_id: str,
    total_submissions: int,
    overall_pro_rate: float,
    top_name_count: int,
    off_hours_ratio: float,
    dedup_drop_fraction: float,
    chamber: str = "House",
    committee_name: str = "Appropriations",
) -> dict[str, object]:
    return {
        "report_id": report_id,
        "chamber": chamber,
        "committee_name": committee_name,
        "cohort": {"chamber": chamber, "committee_name": committee_name},
        "metrics": {
            "total_submissions": total_submissions,
            "overall_pro_rate": overall_pro_rate,
            "window_high_share": 0.05 + (total_submissions % 7) * 0.01,
            "window_top_score": 0.4 + (total_submissions % 11) * 0.03,
            "window_top_abs_z": 1.5 + (total_submissions % 13) * 0.2,
            "window_top_dup_fraction": 0.02 + (total_submissions % 5) * 0.02,
            "top_name_max_records": top_name_count,
            "off_hours_ratio": off_hours_ratio,
            "dedup_drop_fraction": dedup_drop_fraction,
        },
        "top_repeated_names": [
            {
                "canonical_name": "DOE|JANE",
                "display_name": "Doe, Jane",
                "n_records": top_name_count,
            }
        ],
    }


def _write_feature_vector(report_dir: Path, payload: dict[str, object]) -> None:
    (report_dir / "summary").mkdir(parents=True)
    (report_dir / "summary" / "feature_vector.json").write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )


def test_build_feature_vector_emits_expected_metrics_and_cohort_fields() -> None:
    triage_summary = {
        "total_submissions": 120,
        "overall_pro_rate": 0.4,
        "overall_con_rate": 0.6,
        "lens": "side_by_side",
        "off_hours_summary": {"off_hours_ratio": 0.2},
        "top_repeated_names": [
            {
                "display_name": "Doe, Jane",
                "canonical_name": "DOE|JANE",
                "n_records": 12,
                "n_pro": 5,
                "n_con": 7,
            }
        ],
        "total_submissions_raw": 120,
        "total_submissions_exact_row_dedup": 110,
    }
    windows = [
        {
            "window_id": "w1",
            "score": 0.95,
            "z": 4.2,
            "dup_fraction": 0.4,
            "q_value": 0.0001,
            "evidence_tier": "high",
        },
        {
            "window_id": "w2",
            "score": 0.65,
            "z": 2.4,
            "dup_fraction": 0.1,
            "q_value": 0.02,
            "evidence_tier": "medium",
        },
    ]
    data_quality_panel = {
        "triage_raw_vs_dedup_metrics": [
            {"metric": "total_submissions", "material_change": True},
            {"metric": "overall_pro_rate", "material_change": False},
        ]
    }
    hearing_context_panel = {
        "source": {"chamber": "Senate", "committee_name": "Ways & Means"},
    }

    vector = build_feature_vector(
        report_id="SB0000-20260210-0900",
        triage_summary=triage_summary,
        window_evidence_queue=windows,
        record_evidence_queue=[],
        cluster_evidence_queue=[],
        data_quality_panel=data_quality_panel,
        detector_summaries={},
        hearing_context_panel=hearing_context_panel,
    )

    assert vector["report_id"] == "SB0000-20260210-0900"
    assert vector["chamber"] == "Senate"
    assert vector["committee_name"] == "Ways & Means"
    assert vector["cohort"]["chamber"] == "Senate"
    assert vector["metrics"]["total_submissions"] == 120
    assert vector["metrics"]["window_high_count"] == 1
    assert vector["metrics"]["window_high_share"] == 0.5
    assert vector["metrics"]["window_top_score"] == 0.95
    assert vector["metrics"]["window_top_abs_z"] == 4.2
    assert vector["metrics"]["window_top_dup_fraction"] == 0.4
    assert vector["metrics"]["off_hours_ratio"] == 0.2
    assert round(vector["metrics"]["dedup_drop_fraction"], 6) == round((120 - 110) / 120, 6)
    assert vector["material_quality_metric_count"] == 1
    assert vector["total_submissions"] == 120
    assert vector["window_queue_size"] == 2


def test_global_baselines_build_and_load_round_trip(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    report_a = reports_dir / "SB1111-20260201-1000"
    report_b = reports_dir / "SB2222-20260202-1000"
    _write_feature_vector(
        report_a,
        _feature_payload(
            report_id=report_a.name,
            total_submissions=100,
            overall_pro_rate=0.35,
            top_name_count=8,
            off_hours_ratio=0.12,
            dedup_drop_fraction=0.03,
            chamber="Senate",
            committee_name="Ways & Means",
        ),
    )
    _write_feature_vector(
        report_b,
        _feature_payload(
            report_id=report_b.name,
            total_submissions=250,
            overall_pro_rate=0.61,
            top_name_count=15,
            off_hours_ratio=0.08,
            dedup_drop_fraction=0.01,
            chamber="Senate",
            committee_name="Ways & Means",
        ),
    )

    payload = build_global_baselines_from_reports_dir(reports_dir)
    assert payload["report_count"] == 2
    assert "SB1111-20260201-1000" in payload["by_report"]

    report_a_payload = payload["by_report"]["SB1111-20260201-1000"]
    assert report_a_payload["available"] is True
    metric_keys = {row["metric"] for row in report_a_payload["metric_comparators"]}
    assert "total_submissions" in metric_keys
    assert "window_top_score" in metric_keys

    output_path = write_global_baselines(reports_dir=reports_dir, payload=payload)
    assert output_path.exists()

    loaded = load_cross_hearing_baseline(
        out_dir=reports_dir / "SB1111-20260201-1000",
        report_id="SB1111-20260201-1000",
    )
    assert loaded["available"] is True
    assert loaded["report_count"] == 2
    assert isinstance(loaded["metric_comparators"], list)


def test_leave_one_out_baseline_emits_dual_channels_and_compatibility_aliases(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "reports"
    target = reports_dir / "HB3000-20260201-1000"
    _write_feature_vector(
        target,
        _feature_payload(
            report_id=target.name,
            total_submissions=410,
            overall_pro_rate=0.52,
            top_name_count=11,
            off_hours_ratio=0.07,
            dedup_drop_fraction=0.02,
            chamber="House",
            committee_name="Appropriations",
        ),
    )

    for index in range(25):
        report_dir = reports_dir / f"HB9{index:03d}-20260201-1000"
        is_cohort = index < 12
        _write_feature_vector(
            report_dir,
            _feature_payload(
                report_id=report_dir.name,
                total_submissions=200 + index * 5,
                overall_pro_rate=0.32 + index * 0.01,
                top_name_count=4 + (index % 6),
                off_hours_ratio=0.02 + index * 0.001,
                dedup_drop_fraction=0.005 + index * 0.0005,
                chamber="House",
                committee_name="Appropriations" if is_cohort else "Finance",
            ),
        )

    payload = build_leave_one_out_baseline_from_reports_dir(
        reports_dir=reports_dir,
        target_report_id=target.name,
    )

    assert payload["target_report_id"] == target.name
    assert payload["schema_version"] >= 2
    assert payload["selected_channel"] == "cohort_loo"
    assert set(payload["channels"]) == {"cohort_loo", "global_loo"}
    assert payload["channels"]["cohort_loo"]["available"] is True
    assert payload["channels"]["cohort_loo"]["report_count"] == 12
    assert payload["channels"]["cohort_loo"]["support_tier"] == "descriptive_only"
    assert payload["channels"]["global_loo"]["available"] is True
    assert payload["channels"]["global_loo"]["report_count"] == 25
    assert payload["channels"]["global_loo"]["support_tier"] == "supported"

    # Top-level compatibility aliases mirror selected channel.
    assert payload["available"] is True
    assert payload["report_count"] == payload["channels"]["cohort_loo"]["report_count"]
    assert payload["metric_comparators"] == payload["channels"]["cohort_loo"]["metric_comparators"]

    by_metric = {row["metric"]: row for row in payload["metric_comparators"]}
    total_submissions = by_metric["total_submissions"]
    assert total_submissions["observed"] == 410.0
    assert total_submissions["expected"] is not None
    assert total_submissions["delta"] is not None
    assert total_submissions["n_reports"] == 12
    assert total_submissions["support_tier"] == "descriptive_only"
    assert total_submissions["comparator_available"] is True
    assert 0.0 <= float(total_submissions["empirical_tail_p_two_sided"]) <= 1.0
    for channel in payload["channels"].values():
        for row in channel["metric_comparators"]:
            for field in (
                "observed",
                "expected",
                "delta",
                "percentile",
                "band_p10",
                "band_p50",
                "band_p90",
                "robust_z",
                "empirical_tail_p_two_sided",
            ):
                value = row.get(field)
                if value is None:
                    continue
                assert math.isfinite(float(value))


def test_leave_one_out_baseline_cohort_falls_back_to_chamber(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    target = reports_dir / "HB4000-20260201-1000"
    _write_feature_vector(
        target,
        _feature_payload(
            report_id=target.name,
            total_submissions=350,
            overall_pro_rate=0.44,
            top_name_count=10,
            off_hours_ratio=0.05,
            dedup_drop_fraction=0.01,
            chamber="House",
            committee_name="Appropriations",
        ),
    )

    for index in range(16):
        report_dir = reports_dir / f"HB8{index:03d}-20260201-1000"
        committee = "Appropriations" if index < 4 else "Rules"
        _write_feature_vector(
            report_dir,
            _feature_payload(
                report_id=report_dir.name,
                total_submissions=180 + index * 6,
                overall_pro_rate=0.25 + index * 0.012,
                top_name_count=3 + (index % 5),
                off_hours_ratio=0.01 + index * 0.002,
                dedup_drop_fraction=0.001 + index * 0.0008,
                chamber="House",
                committee_name=committee,
            ),
        )

    payload = build_leave_one_out_baseline_from_reports_dir(
        reports_dir=reports_dir,
        target_report_id=target.name,
    )
    cohort = payload["channels"]["cohort_loo"]
    assert cohort["available"] is True
    assert cohort["report_count"] == 16
    assert cohort["metadata"]["selected_level"] == "chamber"


def test_leave_one_out_baseline_marks_insufficient_support_as_unavailable(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    target = reports_dir / "SB5000-20260201-1000"
    _write_feature_vector(
        target,
        _feature_payload(
            report_id=target.name,
            total_submissions=140,
            overall_pro_rate=0.51,
            top_name_count=6,
            off_hours_ratio=0.03,
            dedup_drop_fraction=0.02,
            chamber="Senate",
            committee_name="Ways & Means",
        ),
    )
    for index in range(8):
        report_dir = reports_dir / f"SB7{index:03d}-20260201-1000"
        _write_feature_vector(
            report_dir,
            _feature_payload(
                report_id=report_dir.name,
                total_submissions=90 + index * 3,
                overall_pro_rate=0.2 + index * 0.03,
                top_name_count=2 + index,
                off_hours_ratio=0.01 + index * 0.001,
                dedup_drop_fraction=0.004 + index * 0.0004,
                chamber="Senate",
                committee_name="Transportation",
            ),
        )

    payload = build_leave_one_out_baseline_from_reports_dir(
        reports_dir=reports_dir,
        target_report_id=target.name,
    )

    assert payload["available"] is False
    assert payload["support_tier"] == "unavailable"
    assert payload["reason"] == "insufficient_support"
    assert payload["channels"]["cohort_loo"]["available"] is False
    assert payload["channels"]["global_loo"]["available"] is False
    for channel in payload["channels"].values():
        assert channel["support_tier"] == "unavailable"
        assert channel["metric_comparators"] == []
        for row in channel["metric_comparators"]:
            assert math.isfinite(float(row["observed"]))


def test_normalize_leave_one_out_legacy_payload_infers_support_tier() -> None:
    legacy_payload = {
        "schema_version": 1,
        "available": True,
        "report_count": 101,
        "comparison_report_ids": ["HB0001-20260201-1000"],
        "metric_comparators": [{"metric": "total_submissions", "n_reports": 101}],
        "top_name_cues": [],
        "report_id": "SB6346-20260206-1330",
    }

    normalized = normalize_leave_one_out_baseline_payload(legacy_payload)
    global_channel = normalized["channels"]["global_loo"]
    assert global_channel["available"] is True
    assert global_channel["report_count"] == 101
    assert global_channel["support_tier"] == "supported"
    assert global_channel["descriptive_only"] is False
    assert global_channel["low_power"] is False
    assert normalized["selected_channel"] == "global_loo"
    assert normalized["support_tier"] == "supported"


def test_leave_one_out_baseline_handles_missing_target(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    report_a = reports_dir / "SB1111-20260201-1000"
    _write_feature_vector(
        report_a,
        _feature_payload(
            report_id=report_a.name,
            total_submissions=100,
            overall_pro_rate=0.35,
            top_name_count=4,
            off_hours_ratio=0.1,
            dedup_drop_fraction=0.01,
            chamber="Senate",
            committee_name="Law & Justice",
        ),
    )

    payload = build_leave_one_out_baseline_from_reports_dir(
        reports_dir=reports_dir,
        target_report_id="DOES-NOT-EXIST",
    )
    assert payload["available"] is False
    assert payload["reason"] == "target_report_not_found"
    assert payload["report_count"] == 0
    assert payload["comparison_report_ids"] == []


def test_leave_one_out_baseline_handles_empty_comparison_corpus(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    report_a = reports_dir / "SB1111-20260201-1000"
    _write_feature_vector(
        report_a,
        _feature_payload(
            report_id=report_a.name,
            total_submissions=100,
            overall_pro_rate=0.35,
            top_name_count=4,
            off_hours_ratio=0.1,
            dedup_drop_fraction=0.01,
            chamber="Senate",
            committee_name="Law & Justice",
        ),
    )

    payload = build_leave_one_out_baseline_from_reports_dir(
        reports_dir=reports_dir,
        target_report_id=report_a.name,
    )
    assert payload["available"] is False
    assert payload["reason"] == "no_comparison_reports"
    assert payload["report_count"] == 0
    assert payload["comparison_report_ids"] == []
