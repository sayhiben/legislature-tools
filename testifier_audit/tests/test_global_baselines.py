from __future__ import annotations

from pathlib import Path

from testifier_audit.report.global_baselines import (
    build_feature_vector,
    build_global_baselines_from_reports_dir,
    load_cross_hearing_baseline,
    write_global_baselines,
)


def test_build_feature_vector_emits_phase7_metrics_and_compatibility_keys() -> None:
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
        "top_near_dup_clusters": [
            {
                "cluster_id": "cluster_0001",
                "cluster_size": 4,
                "n_records": 9,
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
    records = [{"canonical_name": "DOE|JANE", "n_records": 12}]
    clusters = [{"cluster_id": "cluster_0001", "cluster_size": 4, "n_records": 9}]
    data_quality_panel = {
        "triage_raw_vs_dedup_metrics": [
            {"metric": "total_submissions", "material_change": True},
            {"metric": "overall_pro_rate", "material_change": False},
        ]
    }

    vector = build_feature_vector(
        report_id="SB0000-20260210-0900",
        triage_summary=triage_summary,
        window_evidence_queue=windows,
        record_evidence_queue=records,
        cluster_evidence_queue=clusters,
        data_quality_panel=data_quality_panel,
    )

    assert vector["report_id"] == "SB0000-20260210-0900"
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
    (report_a / "summary").mkdir(parents=True)
    (report_b / "summary").mkdir(parents=True)

    (report_a / "summary" / "feature_vector.json").write_text(
        """
        {
          "report_id": "SB1111-20260201-1000",
          "metrics": {
            "total_submissions": 100,
            "overall_pro_rate": 0.35,
            "window_high_share": 0.2,
            "window_top_score": 0.82,
            "window_top_abs_z": 3.4,
            "window_top_dup_fraction": 0.18,
            "top_name_max_records": 8,
            "top_cluster_max_records": 7,
            "off_hours_ratio": 0.12,
            "dedup_drop_fraction": 0.03
          },
          "top_repeated_names": [
            {"canonical_name": "DOE|JANE", "display_name": "Doe, Jane", "n_records": 8}
          ],
          "top_near_dup_clusters": [
            {"cluster_id": "cluster_0001", "cluster_size": 3, "n_records": 7}
          ]
        }
        """.strip(),
        encoding="utf-8",
    )
    (report_b / "summary" / "feature_vector.json").write_text(
        """
        {
          "report_id": "SB2222-20260202-1000",
          "metrics": {
            "total_submissions": 250,
            "overall_pro_rate": 0.61,
            "window_high_share": 0.45,
            "window_top_score": 0.97,
            "window_top_abs_z": 6.1,
            "window_top_dup_fraction": 0.31,
            "top_name_max_records": 15,
            "top_cluster_max_records": 11,
            "off_hours_ratio": 0.08,
            "dedup_drop_fraction": 0.01
          },
          "top_repeated_names": [
            {"canonical_name": "DOE|JANE", "display_name": "Doe, Jane", "n_records": 15},
            {"canonical_name": "SMITH|JOHN", "display_name": "Smith, John", "n_records": 9}
          ],
          "top_near_dup_clusters": [
            {"cluster_id": "cluster_0009", "cluster_size": 5, "n_records": 11}
          ]
        }
        """.strip(),
        encoding="utf-8",
    )

    payload = build_global_baselines_from_reports_dir(reports_dir)
    assert payload["report_count"] == 2
    assert "SB1111-20260201-1000" in payload["by_report"]

    report_a_payload = payload["by_report"]["SB1111-20260201-1000"]
    assert report_a_payload["available"] is True
    metric_keys = {row["metric"] for row in report_a_payload["metric_comparators"]}
    assert "total_submissions" in metric_keys
    assert "window_top_score" in metric_keys

    name_cues = {row["canonical_name"]: row for row in report_a_payload["top_name_cues"]}
    assert "DOE|JANE" in name_cues
    assert name_cues["DOE|JANE"]["report_count"] == 2

    output_path = write_global_baselines(reports_dir=reports_dir, payload=payload)
    assert output_path.exists()

    loaded = load_cross_hearing_baseline(
        out_dir=reports_dir / "SB1111-20260201-1000",
        report_id="SB1111-20260201-1000",
    )
    assert loaded["available"] is True
    assert loaded["report_count"] == 2
    assert isinstance(loaded["metric_comparators"], list)
