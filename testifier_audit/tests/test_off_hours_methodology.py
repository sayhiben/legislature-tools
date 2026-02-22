from __future__ import annotations

import pandas as pd

from testifier_audit.detectors.off_hours_methodology import (
    off_hours_column_method_map,
    off_hours_method_specs,
)
from testifier_audit.detectors.off_hours_pipeline import build_window_control_profile
from testifier_audit.detectors.off_hours_statistics import InferenceConfig


def _sample_working_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "position_normalized": ["Con", "Con", "Pro", "Pro", "Pro", "Con", "Pro", "Con"],
            "is_off_hours": [True, True, False, False, False, False, False, False],
            "timestamp": pd.to_datetime(
                [
                    "2026-02-01T01:05:00Z",
                    "2026-02-01T01:35:00Z",
                    "2026-02-01T10:05:00Z",
                    "2026-02-01T10:35:00Z",
                    "2026-02-01T11:05:00Z",
                    "2026-02-01T11:35:00Z",
                    "2026-02-02T10:05:00Z",
                    "2026-02-02T10:35:00Z",
                ]
            ),
            "minute_bucket": pd.to_datetime(
                [
                    "2026-02-01T01:05:00Z",
                    "2026-02-01T01:35:00Z",
                    "2026-02-01T10:05:00Z",
                    "2026-02-01T10:35:00Z",
                    "2026-02-01T11:05:00Z",
                    "2026-02-01T11:35:00Z",
                    "2026-02-02T10:05:00Z",
                    "2026-02-02T10:35:00Z",
                ]
            ),
        }
    )
    frame["is_pro"] = frame["position_normalized"] == "Pro"
    frame["is_con"] = frame["position_normalized"] == "Con"
    return frame


def test_off_hours_methodology_specs_are_complete() -> None:
    specs = off_hours_method_specs()
    ids = {spec.method_id for spec in specs}
    assert ids == {
        "overall_off_vs_on_chi_square",
        "wilson_interval_low_power",
        "day_global_baseline_fallback",
        "day_fixed_plus_harmonic_hour_glm",
        "normal_approx_control_limits",
        "binomial_exact_tail_tests",
        "bh_fdr_by_bucket_and_tail",
        "primary_alert_decision_rule",
    }

    for spec in specs:
        assert spec.label.strip()
        assert spec.purpose.strip()
        assert spec.formula.strip()
        assert spec.assumptions
        assert spec.inputs
        assert spec.outputs
        assert spec.caveats
        assert spec.source_columns


def test_off_hours_column_method_map_references_valid_method_ids() -> None:
    specs = off_hours_method_specs()
    valid_ids = {spec.method_id for spec in specs}
    column_map = off_hours_column_method_map()

    assert column_map
    for method_id in column_map.values():
        assert method_id in valid_ids


def test_pipeline_inferential_columns_are_mapped_to_method_specs() -> None:
    working = _sample_working_frame()
    profile = build_window_control_profile(
        working,
        bucket_minutes=(60,),
        config=InferenceConfig(
            min_window_total=1,
            fdr_alpha=0.05,
            model_min_rows=4,
            model_hour_harmonics=2,
            alert_off_hours_min_fraction=1.0,
            primary_alert_min_abs_delta=0.03,
        ),
    )

    assert not profile.empty
    column_map = off_hours_column_method_map()

    inferential_prefixes = (
        "expected_pro_rate_",
        "control_",
        "z_score_",
        "delta_pro_rate_",
        "p_value_",
        "q_value_",
        "is_significant_",
        "is_below_",
        "is_above_",
        "is_outside_",
    )
    inferential_explicit = {
        "pro_rate_wilson_low",
        "pro_rate_wilson_high",
        "pro_rate_wilson_half_width",
        "is_low_power",
        "off_hours_fraction",
        "is_off_hours_window",
        "is_pure_off_hours_window",
        "is_alert_off_hours_window",
        "day_on_hours_pro_rate",
        "baseline_source",
        "is_model_baseline_available",
        "model_baseline_source",
        "model_fit_method",
        "model_fit_rows",
        "model_fit_unique_days",
        "model_fit_unique_hours",
        "model_fit_converged",
        "model_fit_aic",
        "model_fit_used_harmonics",
        "primary_baseline_source",
        "is_material_primary_shift",
        "is_material_primary_lower_shift",
        "is_material_primary_upper_shift",
        "is_primary_alert_window",
        "is_primary_spc_998_two_sided",
        "is_primary_fdr_two_sided",
        "is_primary_any_flag_channel",
        "is_primary_both_flag_channels",
    }

    inferential_columns = {
        column
        for column in profile.columns
        if column.startswith(inferential_prefixes) or column in inferential_explicit
    }
    unmapped = sorted(inferential_columns - set(column_map))
    assert unmapped == []
