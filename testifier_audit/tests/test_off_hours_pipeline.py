from __future__ import annotations

import pandas as pd
import pytest

from testifier_audit.detectors import off_hours_statistics
from testifier_audit.detectors.off_hours_pipeline import build_window_control_profile
from testifier_audit.detectors.off_hours_statistics import InferenceConfig


def _model_supported_frame() -> pd.DataFrame:
    records: list[dict[str, object]] = []
    idx = 1
    for date in ("2026-02-01", "2026-02-02"):
        for hour, is_off_hours, positions in (
            (1, True, ("Con", "Con", "Pro", "Con")),
            (3, True, ("Con", "Con", "Con", "Pro")),
            (10, False, ("Pro", "Pro", "Con", "Pro")),
            (14, False, ("Pro", "Con", "Pro", "Pro")),
            (19, False, ("Pro", "Pro", "Con", "Con")),
        ):
            minute = 5
            for position in positions:
                timestamp = pd.Timestamp(f"{date}T{hour:02d}:{minute:02d}:00Z")
                records.append(
                    {
                        "id": idx,
                        "position_normalized": position,
                        "is_off_hours": is_off_hours,
                        "timestamp": timestamp,
                        "minute_bucket": timestamp,
                        "is_pro": position == "Pro",
                        "is_con": position == "Con",
                    }
                )
                idx += 1
                minute += 2
    return pd.DataFrame(records)


def _mixed_window_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "position_normalized": ["Con", "Con", "Pro", "Pro", "Pro", "Pro", "Con", "Con"],
            "is_off_hours": [True, True, False, False, False, False, False, False],
            "timestamp": pd.to_datetime(
                [
                    "2026-02-01T01:02:00Z",
                    "2026-02-01T01:10:00Z",
                    "2026-02-01T01:20:00Z",
                    "2026-02-01T01:40:00Z",
                    "2026-02-01T10:05:00Z",
                    "2026-02-01T10:14:00Z",
                    "2026-02-01T10:31:00Z",
                    "2026-02-01T10:47:00Z",
                ]
            ),
        }
    )
    frame["minute_bucket"] = frame["timestamp"]
    frame["is_pro"] = frame["position_normalized"] == "Pro"
    frame["is_con"] = frame["position_normalized"] == "Con"
    return frame


def test_pipeline_uses_model_baseline_when_support_is_sufficient() -> None:
    profile = build_window_control_profile(
        _model_supported_frame(),
        bucket_minutes=(60,),
        config=InferenceConfig(
            min_window_total=1,
            fdr_alpha=0.05,
            model_min_rows=8,
            model_hour_harmonics=4,
            alert_off_hours_min_fraction=1.0,
            primary_alert_min_abs_delta=0.03,
        ),
    )

    off_hours_rows = profile.loc[profile["is_off_hours_window"]].copy()
    assert not off_hours_rows.empty
    assert bool(off_hours_rows["is_model_baseline_available"].any()) is True
    assert bool(off_hours_rows["expected_pro_rate_model"].notna().all()) is True
    assert bool((off_hours_rows["primary_baseline_source"] == "model_day_hour").all()) is True
    assert bool((off_hours_rows["model_fit_used_harmonics"] == 4).all()) is True


def test_pipeline_falls_back_to_regularized_glm_when_standard_fit_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_fit(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        raise RuntimeError("forced failure")

    monkeypatch.setattr(off_hours_statistics.sm.GLM, "fit", _raise_fit)
    profile = build_window_control_profile(
        _model_supported_frame(),
        bucket_minutes=(60,),
        config=InferenceConfig(
            min_window_total=1,
            fdr_alpha=0.05,
            model_min_rows=8,
            model_hour_harmonics=3,
            alert_off_hours_min_fraction=1.0,
            primary_alert_min_abs_delta=0.03,
        ),
    )

    assert not profile.empty
    assert set(profile["model_fit_method"].astype(str).unique()) == {"glm_regularized"}


def test_pipeline_uses_day_global_fallback_when_model_is_unavailable() -> None:
    profile = build_window_control_profile(
        _mixed_window_frame(),
        bucket_minutes=(60,),
        config=InferenceConfig(
            min_window_total=1,
            fdr_alpha=0.05,
            model_min_rows=50,
            model_hour_harmonics=3,
            alert_off_hours_min_fraction=1.0,
            primary_alert_min_abs_delta=0.03,
        ),
    )

    assert not profile.empty
    assert bool(profile["expected_pro_rate_model"].isna().all()) is True
    assert bool((profile["model_fit_method"] == "unavailable_insufficient_rows").all()) is True
    assert bool((profile["primary_baseline_source"] != "model_day_hour").all()) is True


def test_pipeline_alert_eligibility_and_support_gating() -> None:
    profile = build_window_control_profile(
        _mixed_window_frame(),
        bucket_minutes=(60,),
        config=InferenceConfig(
            min_window_total=10,
            fdr_alpha=0.05,
            model_min_rows=50,
            model_hour_harmonics=3,
            alert_off_hours_min_fraction=1.0,
            primary_alert_min_abs_delta=0.03,
        ),
    )

    mixed_window = profile.loc[
        profile["bucket_start"] == pd.Timestamp("2026-02-01T01:00:00Z")
    ].iloc[0]
    assert bool(mixed_window["is_off_hours_window"]) is True
    assert bool(mixed_window["is_alert_off_hours_window"]) is False
    assert bool(profile["is_primary_alert_window"].any()) is False
    assert int((profile["is_alert_off_hours_window"] & (~profile["is_low_power"])).sum()) == 0
