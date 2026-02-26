from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from testifier_audit.io.hearing_metadata import HearingMetadata


@dataclass(slots=True)
class PayloadBuildContext:
    table_map: dict[str, pd.DataFrame]
    detector_summaries: dict[str, dict[str, Any]]
    default_dedup_mode: str | None = None
    min_cell_n_for_rates: int = 25
    hearing_metadata: HearingMetadata | None = None


@dataclass(slots=True)
class SectionBuildResult:
    charts: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    bucket_options_by_analysis: dict[str, list[int]] = field(default_factory=dict)
    supplemental_chart_ids: set[str] = field(default_factory=set)
    controls_patch: dict[str, Any] = field(default_factory=dict)
