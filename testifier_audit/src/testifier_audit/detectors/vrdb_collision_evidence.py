from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Final

import pandas as pd

from testifier_audit.detectors.base import Detector, DetectorResult
from testifier_audit.io.vrdb_collision_null import compute_vrdb_collision_null_for_slices

LOGGER = logging.getLogger(__name__)

_PROBABILITY_COLUMNS: Final[list[str]] = [
    "name_key",
    "name_key_type",
    "count",
    "probability",
    "denominator",
    "geo_level",
    "geo_value",
    "baseline_variant",
    "vrdb_version",
    "normalization_version",
]

_BACKOFF_COLUMNS: Final[list[str]] = [
    "baseline_variant",
    "requested_geo_level",
    "requested_geo_value",
    "effective_geo_level",
    "effective_geo_value",
    "fallback_steps",
    "backoff_reason",
    "effective_denominator",
]


class VrdbCollisionEvidenceDetector(Detector):
    name = "vrdb_collision_evidence"

    def __init__(
        self,
        *,
        bucket_minutes: list[int] | tuple[int, ...],
        monte_carlo_draws: int = 400,
        top_name_limit: int = 20,
        baseline_variant: str = "all_registrants",
        name_key_type: str = "full_name_key",
        requested_geo_level: str = "state",
        requested_geo_value: str = "WA",
        probability_rows_path: str | None = None,
        backoff_rows_path: str | None = None,
    ) -> None:
        # Keep sidecar runtime bounded by focusing on coarse timeline slices.
        selected_buckets = sorted({int(value) for value in bucket_minutes if int(value) >= 30})
        self.bucket_minutes = tuple(selected_buckets or [60])
        self.monte_carlo_draws = int(max(int(monte_carlo_draws), 0))
        self.top_name_limit = int(max(int(top_name_limit), 1))
        self.baseline_variant = str(baseline_variant or "").strip() or "all_registrants"
        self.name_key_type = str(name_key_type or "").strip() or "full_name_key"
        self.requested_geo_level = str(requested_geo_level or "").strip() or "state"
        self.requested_geo_value = str(requested_geo_value or "").strip() or "WA"
        self.probability_rows_path = str(probability_rows_path or "").strip()
        self.backoff_rows_path = str(backoff_rows_path or "").strip()

        self._repo_root = Path(__file__).resolve().parents[4]
        self._default_probability_candidates = [
            self._repo_root / "data" / "metadata" / "vrdb_name_probabilities.csv",
            self._repo_root / "output" / "dup003" / "vrdb_name_probabilities.csv",
        ]
        self._default_backoff_candidates = [
            self._repo_root / "data" / "metadata" / "vrdb_geo_backoff.csv",
            self._repo_root / "output" / "dup003" / "vrdb_geo_backoff.csv",
        ]

    def _resolve_path(
        self,
        *,
        explicit_path: str,
        env_var: str,
        defaults: list[Path],
    ) -> Path | None:
        if explicit_path:
            candidate = Path(explicit_path)
            if candidate.exists():
                return candidate
        env_value = str(os.getenv(env_var, "") or "").strip()
        if env_value:
            candidate = Path(env_value)
            if candidate.exists():
                return candidate
        for candidate in defaults:
            if candidate.exists():
                return candidate
        return None

    def _empty_result(self, *, reason: str, enabled: bool = False) -> DetectorResult:
        summary = {
            "enabled": bool(enabled),
            "active": False,
            "reason": str(reason),
            "baseline_variant": self.baseline_variant,
            "name_key_type": self.name_key_type,
            "requested_geo_level": self.requested_geo_level,
            "requested_geo_value": self.requested_geo_value,
            "bucket_minutes": list(self.bucket_minutes),
            "monte_carlo_draws": self.monte_carlo_draws,
        }
        return DetectorResult(
            detector=self.name,
            summary=summary,
            tables={
                "slice_metrics": pd.DataFrame(),
                "top_overrun_names": pd.DataFrame(),
            },
        )

    def _load_probability_rows(self, path: Path) -> pd.DataFrame:
        if path.suffix.lower() == ".parquet":
            frame = pd.read_parquet(path, columns=_PROBABILITY_COLUMNS)
            return frame[
                (frame["baseline_variant"].astype(str) == self.baseline_variant)
                & (frame["name_key_type"].astype(str) == self.name_key_type)
                & (frame["geo_level"].astype(str) == "state")
                & (frame["geo_value"].astype(str) == "WA")
            ].reset_index(drop=True)

        chunks: list[pd.DataFrame] = []
        for chunk in pd.read_csv(path, usecols=_PROBABILITY_COLUMNS, chunksize=250_000):
            keep = chunk[
                (chunk["baseline_variant"].astype(str) == self.baseline_variant)
                & (chunk["name_key_type"].astype(str) == self.name_key_type)
                & (chunk["geo_level"].astype(str) == "state")
                & (chunk["geo_value"].astype(str) == "WA")
            ]
            if not keep.empty:
                chunks.append(keep)
        if not chunks:
            return pd.DataFrame(columns=_PROBABILITY_COLUMNS)
        return pd.concat(chunks, ignore_index=True)

    def _load_backoff_rows(self, path: Path) -> pd.DataFrame:
        if path.suffix.lower() == ".parquet":
            return pd.read_parquet(path, columns=_BACKOFF_COLUMNS)
        return pd.read_csv(path, usecols=_BACKOFF_COLUMNS)

    def _build_slice_rows(self, working: pd.DataFrame) -> pd.DataFrame:
        base = pd.DataFrame(
            {
                "slice_id": "full_hearing",
                "slice_type": "full_hearing",
                "name_key": working["full_name_key"].astype(str),
                "baseline_variant": self.baseline_variant,
                "name_key_type": self.name_key_type,
                "requested_geo_level": self.requested_geo_level,
                "requested_geo_value": self.requested_geo_value,
            }
        )
        slice_frames: list[pd.DataFrame] = [base]
        for bucket in self.bucket_minutes:
            bucket_start = working["timestamp"].dt.floor(f"{int(bucket)}min")
            bucket_frame = pd.DataFrame(
                {
                    "slice_id": "bucket_"
                    + str(int(bucket))
                    + "m:"
                    + bucket_start.dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
                    "slice_type": f"bucket_{int(bucket)}m",
                    "name_key": working["full_name_key"].astype(str),
                    "baseline_variant": self.baseline_variant,
                    "name_key_type": self.name_key_type,
                    "requested_geo_level": self.requested_geo_level,
                    "requested_geo_value": self.requested_geo_value,
                }
            )
            slice_frames.append(bucket_frame)
        return pd.concat(slice_frames, ignore_index=True)

    def _attach_bucket_fields(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame
        working = frame.copy()
        match = working["slice_id"].fillna("").astype(str).str.extract(
            r"^bucket_(\d+)m:(.+)$",
            expand=True,
        )
        working["bucket_minutes"] = pd.to_numeric(match[0], errors="coerce").fillna(0).astype(int)
        working["bucket_start"] = pd.to_datetime(match[1], format="%Y-%m-%dT%H:%M:%S%z", errors="coerce")
        return working

    def run(self, df: pd.DataFrame, features: dict[str, pd.DataFrame]) -> DetectorResult:
        _ = features
        required_columns = {"full_name_key", "timestamp"}
        if not required_columns.issubset(set(df.columns)):
            return self._empty_result(reason="missing_required_columns")

        probability_path = self._resolve_path(
            explicit_path=self.probability_rows_path,
            env_var="TESTIFIER_AUDIT_VRDB_PROBABILITIES_CSV",
            defaults=self._default_probability_candidates,
        )
        backoff_path = self._resolve_path(
            explicit_path=self.backoff_rows_path,
            env_var="TESTIFIER_AUDIT_VRDB_BACKOFF_CSV",
            defaults=self._default_backoff_candidates,
        )
        if probability_path is None:
            return self._empty_result(reason="missing_probability_artifact")
        if backoff_path is None:
            return self._empty_result(reason="missing_backoff_artifact")

        working = df[["full_name_key", "timestamp"]].copy()
        working["full_name_key"] = working["full_name_key"].fillna("").astype(str)
        working["timestamp"] = pd.to_datetime(working["timestamp"], errors="coerce")
        working = working[(working["full_name_key"] != "") & (working["timestamp"].notna())].copy()
        if working.empty:
            return self._empty_result(reason="no_valid_name_rows", enabled=True)

        probability_rows = self._load_probability_rows(probability_path)
        if probability_rows.empty:
            return self._empty_result(reason="missing_probability_rows_for_variant", enabled=True)
        backoff_rows = self._load_backoff_rows(backoff_path)
        slice_rows = self._build_slice_rows(working)

        metrics_rows, expected_name_rows = compute_vrdb_collision_null_for_slices(
            slice_rows=slice_rows,
            probability_rows=probability_rows,
            backoff_rows=backoff_rows,
            monte_carlo_draws=self.monte_carlo_draws,
            top_name_limit=self.top_name_limit,
        )
        metrics_rows = self._attach_bucket_fields(metrics_rows)
        expected_name_rows = self._attach_bucket_fields(expected_name_rows)

        top_overrun_names = expected_name_rows.copy()
        if not top_overrun_names.empty:
            top_overrun_names = top_overrun_names[
                pd.to_numeric(top_overrun_names["overrun_count"], errors="coerce").fillna(0.0) > 0.0
            ].copy()
            top_overrun_names["rank"] = (
                top_overrun_names.sort_values(
                    ["slice_id", "overrun_count", "observed_count", "name_key"],
                    ascending=[True, False, False, True],
                )
                .groupby("slice_id", dropna=False)
                .cumcount()
                + 1
            )
            top_overrun_names = top_overrun_names[top_overrun_names["rank"] <= self.top_name_limit].copy()

            summary_names = (
                top_overrun_names.sort_values(["slice_id", "rank"])
                .groupby("slice_id", dropna=False)["name_key"]
                .apply(lambda values: ", ".join(str(value) for value in values.head(3)))
                .rename("top_overrun_names")
            )
            metrics_rows = metrics_rows.merge(
                summary_names.reset_index(),
                on="slice_id",
                how="left",
            )
        else:
            metrics_rows["top_overrun_names"] = ""

        LOGGER.info(
            "VRDB sidecar rows=%s slices=%s buckets=%s probabilities=%s artifact=%s",
            len(working),
            len(metrics_rows),
            list(self.bucket_minutes),
            len(probability_rows),
            probability_path,
        )
        summary = {
            "enabled": True,
            "active": bool(not metrics_rows.empty),
            "reason": "" if not metrics_rows.empty else "no_slice_metrics_generated",
            "baseline_variant": self.baseline_variant,
            "name_key_type": self.name_key_type,
            "requested_geo_level": self.requested_geo_level,
            "requested_geo_value": self.requested_geo_value,
            "bucket_minutes": list(self.bucket_minutes),
            "monte_carlo_draws": self.monte_carlo_draws,
            "probability_rows": int(len(probability_rows)),
            "slice_metrics_rows": int(len(metrics_rows)),
            "top_overrun_rows": int(len(top_overrun_names)),
            "probability_artifact_path": str(probability_path),
            "backoff_artifact_path": str(backoff_path),
        }
        return DetectorResult(
            detector=self.name,
            summary=summary,
            tables={
                "slice_metrics": metrics_rows,
                "top_overrun_names": top_overrun_names,
            },
        )
