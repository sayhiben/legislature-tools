from __future__ import annotations

from pathlib import Path

import pandas as pd

from testifier_audit.viz.time_series import plot_organization_blank_rates


def test_plot_organization_blank_rates_writes_figure(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "bucket_start": pd.to_datetime(
                [
                    "2026-02-03 10:00:00",
                    "2026-02-03 10:01:00",
                    "2026-02-03 10:02:00",
                    "2026-02-03 10:03:00",
                ]
            ),
            "bucket_minutes": [1, 1, 1, 1],
            "n_total": [12, 28, 45, 52],
            "n_blank_org": [5, 14, 16, 31],
            "n_pro": [6, 14, 23, 25],
            "n_blank_org_pro": [2, 6, 7, 10],
            "n_con": [6, 14, 22, 27],
            "n_blank_org_con": [3, 8, 9, 21],
            "blank_org_rate": [0.4, 0.5, 0.35, 0.6],
            "pro_blank_org_rate": [0.3, 0.45, 0.3, 0.5],
            "con_blank_org_rate": [0.5, 0.55, 0.4, 0.7],
        }
    )
    output_path = tmp_path / "organization_blank_rates.png"

    result = plot_organization_blank_rates(blank_rate_by_bucket=frame, output_path=output_path)

    assert result == output_path
    assert output_path.exists()
