from __future__ import annotations

from pathlib import Path

import pandas as pd

from testifier_audit.viz.time_series import plot_multivariate_anomaly_scores


def test_plot_multivariate_anomaly_scores_writes_file(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "bucket_start": pd.to_datetime(
                [
                    "2026-02-01 00:00:00",
                    "2026-02-01 00:15:00",
                    "2026-02-01 00:30:00",
                    "2026-02-01 00:45:00",
                ]
            ),
            "bucket_minutes": [15, 15, 15, 15],
            "n_total": [120, 130, 400, 140],
            "anomaly_score": [0.02, 0.03, 0.18, 0.01],
            "is_anomaly": [False, False, True, False],
            "is_low_power": [False, False, False, False],
        }
    )
    output_path = tmp_path / "multivariate_anomaly_scores.png"

    result = plot_multivariate_anomaly_scores(
        bucket_anomaly_scores=frame,
        output_path=output_path,
    )

    assert result == output_path
    assert output_path.exists()


def test_plot_multivariate_anomaly_scores_returns_none_for_empty_data(tmp_path: Path) -> None:
    output_path = tmp_path / "unused.png"
    result = plot_multivariate_anomaly_scores(
        bucket_anomaly_scores=pd.DataFrame(),
        output_path=output_path,
    )

    assert result is None
    assert not output_path.exists()
