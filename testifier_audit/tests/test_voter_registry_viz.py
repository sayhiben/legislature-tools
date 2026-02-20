from __future__ import annotations

from pathlib import Path

import pandas as pd

from testifier_audit.viz.time_series import plot_voter_registry_match_rates


def test_plot_voter_registry_match_rates_writes_file(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "bucket_start": pd.to_datetime(
                [
                    "2026-02-01 00:00:00",
                    "2026-02-01 00:30:00",
                    "2026-02-01 01:00:00",
                ]
            ),
            "n_total": [12, 80, 120],
            "n_matches": [8, 52, 86],
            "n_pro": [7, 40, 70],
            "n_matches_pro": [5, 25, 52],
            "n_con": [5, 40, 50],
            "n_matches_con": [3, 27, 34],
            "match_rate": [0.7, 0.65, 0.72],
            "pro_match_rate": [0.75, 0.62, 0.77],
            "con_match_rate": [0.62, 0.68, 0.64],
            "bucket_minutes": [30, 30, 30],
        }
    )
    output_path = tmp_path / "voter_registry_match_rates.png"

    result = plot_voter_registry_match_rates(match_by_bucket=frame, output_path=output_path)

    assert result == output_path
    assert output_path.exists()


def test_plot_voter_registry_match_rates_returns_none_for_empty_data(tmp_path: Path) -> None:
    output_path = tmp_path / "unused.png"
    result = plot_voter_registry_match_rates(
        match_by_bucket=pd.DataFrame(), output_path=output_path
    )

    assert result is None
    assert not output_path.exists()
