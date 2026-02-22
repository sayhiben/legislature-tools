from __future__ import annotations

from copy import deepcopy
import math
from dataclasses import dataclass, field
from typing import Any, Literal, cast

EvidenceKind = Literal["stat_fdr", "calibrated_empirical", "heuristic"]
EvidenceTier = Literal["high", "medium", "watch"]
ExplanationLabel = Literal[
    "data_quality_artifact",
    "legitimate_mobilization",
    "potential_manipulation",
    "mixed",
    "insufficient_evidence",
    "none",
]
QueueKind = Literal["window", "record", "cluster"]

ALLOWED_EVIDENCE_KINDS = frozenset({"stat_fdr", "calibrated_empirical", "heuristic"})
ALLOWED_EVIDENCE_TIERS = frozenset({"high", "medium", "watch"})
ALLOWED_EXPLANATION_LABELS = frozenset(
    {
        "data_quality_artifact",
        "legitimate_mobilization",
        "potential_manipulation",
        "mixed",
        "insufficient_evidence",
        "none",
    }
)
ALLOWED_QUEUE_KINDS = frozenset({"window", "record", "cluster"})

_COLOR_SEMANTICS: dict[str, dict[str, Any]] = {
    "light": {
        "series": {
            "primary": "#0072B2",
            "volume": "#94A3B8",
            "context": "#009E73",
            "interval": "#8B99A8",
            "reference": "#475569",
        },
        "alert": {
            "lower": "#D55E00",
            "upper": "#CC79A7",
        },
        "state": {
            "low_power": "#E69F00",
            "outlier": "#56B4E9",
        },
        "band": {
            "alert_run": "rgba(213,94,0,0.12)",
            "comparator": "rgba(0,114,178,0.10)",
        },
        "heatmap": {
            "rate_diverging": ["#2C7FB8", "#9ECAE1", "#F7F7F7", "#FDD49E", "#D95F0E"],
            "residual_diverging": ["#B13A00", "#F4A259", "#F5F7FA", "#82B1D8", "#1F6AA5"],
            "volume_seq": ["#F8FAFC", "#CBD5E1", "#475569"],
        },
        "categorical_palette": [
            "#0072B2",
            "#009E73",
            "#E69F00",
            "#CC79A7",
            "#56B4E9",
            "#D55E00",
            "#8B99A8",
            "#475569",
        ],
    },
    "dark": {
        "series": {
            "primary": "#5AB0FF",
            "volume": "#64748B",
            "context": "#2FC79A",
            "interval": "#A8B5C5",
            "reference": "#94A3B8",
        },
        "alert": {
            "lower": "#FF8A3D",
            "upper": "#F2A7D4",
        },
        "state": {
            "low_power": "#F2C14E",
            "outlier": "#7CC7FF",
        },
        "band": {
            "alert_run": "rgba(255,138,61,0.18)",
            "comparator": "rgba(90,176,255,0.14)",
        },
        "heatmap": {
            "rate_diverging": ["#6BAED6", "#2E4C66", "#111827", "#6B4A2D", "#F4A259"],
            "residual_diverging": ["#FF8A3D", "#C9723A", "#1E293B", "#5A8DB8", "#8CC7FF"],
            "volume_seq": ["#0F172A", "#334155", "#94A3B8"],
        },
        "categorical_palette": [
            "#5AB0FF",
            "#2FC79A",
            "#F2C14E",
            "#F2A7D4",
            "#7CC7FF",
            "#FF8A3D",
            "#A8B5C5",
            "#94A3B8",
        ],
    },
}


def _ensure_probability(name: str, value: float | None) -> None:
    if value is None:
        return
    if not math.isfinite(value) or value < 0.0 or value > 1.0:
        raise ValueError(f"{name} must be finite and in [0, 1], got {value!r}.")


def _ensure_score(name: str, value: float) -> None:
    if not math.isfinite(value) or value < 0.0 or value > 1.0:
        raise ValueError(f"{name} must be finite and in [0, 1], got {value!r}.")


def _normalize_caveat_flags(flags: tuple[str, ...]) -> tuple[str, ...]:
    normalized = sorted({str(flag).strip() for flag in flags if str(flag).strip()})
    return tuple(normalized)


def default_color_semantics() -> dict[str, dict[str, Any]]:
    return deepcopy(_COLOR_SEMANTICS)


@dataclass(slots=True, frozen=True)
class TriageTierThresholds:
    high: float = 0.80
    medium: float = 0.60
    min_support_n: int = 25

    def __post_init__(self) -> None:
        _ensure_score("high", self.high)
        _ensure_score("medium", self.medium)
        if self.high < self.medium:
            raise ValueError("high threshold must be >= medium threshold.")
        if self.min_support_n <= 0:
            raise ValueError("min_support_n must be > 0.")


@dataclass(slots=True, frozen=True)
class EvidenceSignal:
    signal_id: str
    detector: str | None
    evidence_kind: EvidenceKind
    signal_score: float
    support_n: int
    effect_size: float | None = None
    p_value: float | None = None
    q_value: float | None = None
    is_low_power: bool = False
    caveat_flags: tuple[str, ...] = ()
    explanation_hint: ExplanationLabel | None = None
    context: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.signal_id.strip():
            raise ValueError("signal_id must be non-empty.")
        if self.evidence_kind not in ALLOWED_EVIDENCE_KINDS:
            raise ValueError(f"Unsupported evidence_kind: {self.evidence_kind!r}.")
        if self.explanation_hint and self.explanation_hint not in ALLOWED_EXPLANATION_LABELS:
            raise ValueError(f"Unsupported explanation_hint: {self.explanation_hint!r}.")
        _ensure_score("signal_score", self.signal_score)
        if self.support_n < 0:
            raise ValueError("support_n must be >= 0.")
        _ensure_probability("p_value", self.p_value)
        _ensure_probability("q_value", self.q_value)
        if self.effect_size is not None and not math.isfinite(self.effect_size):
            raise ValueError("effect_size must be finite when provided.")

        object.__setattr__(self, "caveat_flags", _normalize_caveat_flags(self.caveat_flags))
        object.__setattr__(self, "context", dict(self.context))

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "detector": self.detector,
            "evidence_kind": self.evidence_kind,
            "signal_score": self.signal_score,
            "support_n": self.support_n,
            "effect_size": self.effect_size,
            "p_value": self.p_value,
            "q_value": self.q_value,
            "is_low_power": self.is_low_power,
            "caveat_flags": list(self.caveat_flags),
            "explanation_hint": self.explanation_hint,
            "context": dict(self.context),
        }


@dataclass(slots=True, frozen=True)
class TriageEvidenceItem:
    queue_id: str
    queue_kind: QueueKind
    score: float
    support_n: int
    contributors: tuple[EvidenceSignal, ...]
    evidence_tier: EvidenceTier
    primary_explanation: ExplanationLabel
    secondary_explanation: ExplanationLabel = "none"
    caveat_flags: tuple[str, ...] = ()
    is_low_power: bool = False

    def __post_init__(self) -> None:
        if not self.queue_id.strip():
            raise ValueError("queue_id must be non-empty.")
        if self.queue_kind not in ALLOWED_QUEUE_KINDS:
            raise ValueError(f"Unsupported queue_kind: {self.queue_kind!r}.")
        _ensure_score("score", self.score)
        if self.support_n < 0:
            raise ValueError("support_n must be >= 0.")
        if not self.contributors:
            raise ValueError("contributors must be non-empty.")
        if self.evidence_tier not in ALLOWED_EVIDENCE_TIERS:
            raise ValueError(f"Unsupported evidence_tier: {self.evidence_tier!r}.")
        if self.primary_explanation not in ALLOWED_EXPLANATION_LABELS:
            raise ValueError(f"Unsupported primary_explanation: {self.primary_explanation!r}.")
        if self.secondary_explanation not in ALLOWED_EXPLANATION_LABELS:
            raise ValueError(
                f"Unsupported secondary_explanation: {self.secondary_explanation!r}."
            )

        object.__setattr__(self, "contributors", tuple(self.contributors))
        object.__setattr__(self, "caveat_flags", _normalize_caveat_flags(self.caveat_flags))

    @property
    def evidence_kinds(self) -> tuple[EvidenceKind, ...]:
        ordered = sorted({signal.evidence_kind for signal in self.contributors})
        return cast(tuple[EvidenceKind, ...], tuple(ordered))

    def to_dict(self) -> dict[str, Any]:
        return {
            "queue_id": self.queue_id,
            "queue_kind": self.queue_kind,
            "score": self.score,
            "support_n": self.support_n,
            "evidence_tier": self.evidence_tier,
            "evidence_kinds": list(self.evidence_kinds),
            "primary_explanation": self.primary_explanation,
            "secondary_explanation": self.secondary_explanation,
            "is_low_power": self.is_low_power,
            "caveat_flags": list(self.caveat_flags),
            "contributors": [signal.to_dict() for signal in self.contributors],
        }
