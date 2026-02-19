from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class DetectorResult:
    detector: str
    summary: dict[str, Any]
    tables: dict[str, pd.DataFrame]
    record_scores: pd.Series | None = None
    record_flags: pd.Series | None = None


class Detector:
    name: str

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        raise NotImplementedError
