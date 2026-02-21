from __future__ import annotations

from typing import Any

_EVIDENCE_TAXONOMY: tuple[dict[str, str], ...] = (
    {
        "kind": "stat_fdr",
        "label": "Statistical (FDR-controlled)",
        "description": "Hypothesis-tested signal with false-discovery-rate control.",
    },
    {
        "kind": "calibrated_empirical",
        "label": "Calibrated empirical",
        "description": "Simulation/permutation calibrated evidence without closed-form p-values.",
    },
    {
        "kind": "heuristic",
        "label": "Heuristic",
        "description": "Structured indicator without formal significance control.",
    },
)

_THEME_OPTIONS: tuple[dict[str, str], ...] = (
    {"id": "light", "label": "Light"},
    {"id": "dark", "label": "Dark"},
)

_METHODOLOGY_DEFINITIONS: tuple[dict[str, str], ...] = (
    {
        "term": "Window evidence queue",
        "definition": (
            "Ranked candidate windows with score, support, caveats, and provenance fields "
            "used for investigator prioritization."
        ),
    },
    {
        "term": "Evidence kind",
        "definition": (
            "Signal provenance label (stat_fdr, calibrated_empirical, heuristic) that "
            "controls how strongly a result can be interpreted."
        ),
    },
    {
        "term": "Low-power",
        "definition": (
            "Buckets/windows with sparse support where rate metrics are unstable; low-power "
            "items are capped below high tier."
        ),
    },
    {
        "term": "Wilson interval",
        "definition": (
            "Binomial confidence interval for proportions; wider intervals indicate greater "
            "uncertainty from limited support."
        ),
    },
    {
        "term": "q_value",
        "definition": (
            "False-discovery-rate adjusted p-value used to control multiple testing for "
            "hypothesis-tested detector families."
        ),
    },
    {
        "term": "Score tiering",
        "definition": (
            "Triage score thresholds and support requirements that separate high, medium, "
            "and watch items."
        ),
    },
)

_TESTS_USED: tuple[dict[str, str], ...] = (
    {
        "analysis_family": "Burst windows",
        "test_or_calibration": "Poisson/empirical burst exceedance with FDR q-values",
        "evidence_kind": "stat_fdr, calibrated_empirical",
        "notes": "Prioritize contiguous elevated windows over isolated spikes.",
    },
    {
        "analysis_family": "Pro/Con swings and runs",
        "test_or_calibration": "Proportion deltas with Wilson bounds and directional run summaries",
        "evidence_kind": "stat_fdr, heuristic",
        "notes": "Interpret sustained directional runs more strongly than single buckets.",
    },
    {
        "analysis_family": "Periodicity and regularity",
        "test_or_calibration": "Clockface z-scores, autocorrelation, FFT peaks, rolling Fano",
        "evidence_kind": "stat_fdr, heuristic",
        "notes": "Require repeatability across adjacent lags/windows for escalation.",
    },
    {
        "analysis_family": "Duplicate and near-duplicate concentration",
        "test_or_calibration": "Exact duplicate concentration and near-duplicate cluster timing",
        "evidence_kind": "heuristic, calibrated_empirical",
        "notes": "Combine with volume/timing context to reduce false positives.",
    },
    {
        "analysis_family": "Voter linkage (supporting context)",
        "test_or_calibration": "Tiered probabilistic matching with confidence summaries",
        "evidence_kind": "heuristic",
        "notes": "Supporting context only; never standalone attribution.",
    },
)

_MULTIPLE_TESTING_POLICY: tuple[str, ...] = (
    "Use q_value (FDR-adjusted) outputs for hypothesis-tested detector families.",
    "Treat non-FDR heuristic/calibrated metrics as supporting evidence, not sole proof.",
    "Escalate when multiple independent indicators align in the same time window.",
)

_CAVEATS: tuple[str, ...] = (
    "Sparse windows can produce large but unstable rate swings.",
    "Name/organization data quality issues can inflate duplicate or rarity signals.",
    "Metadata-absent runs cannot support hearing-relative process interpretations.",
    "Cross-hearing percentiles are corpus-relative and can shift as reports are added.",
)

_INTERPRETATION_GUIDANCE: tuple[str, ...] = (
    "Start with high-tier windows that have adequate support and non-low-power status.",
    "Confirm each candidate with at least one independent detector family.",
    "Prefer persistent multi-window patterns over one-bucket extremes.",
    "Document plausible benign explanations before escalation.",
)

_ETHICAL_GUARDRAILS: tuple[dict[str, str], ...] = (
    {
        "standard": "Use statistical-irregularity language",
        "requirement": (
            "Describe findings as statistical irregularity requiring review, not proof of "
            "intent or misconduct."
        ),
    },
    {
        "standard": "No standalone attribution",
        "requirement": (
            "Do not attribute coordination or identity claims from one detector or one "
            "artifact in isolation."
        ),
    },
    {
        "standard": "Voter linkage caveat",
        "requirement": (
            "Treat probabilistic voter linkage as probabilistic supporting context only; "
            "never use it as "
            "standalone attribution."
        ),
    },
)


def _copy_rows(rows: tuple[dict[str, str], ...]) -> list[dict[str, str]]:
    return [dict(row) for row in rows]


def default_evidence_taxonomy() -> list[dict[str, str]]:
    return _copy_rows(_EVIDENCE_TAXONOMY)


def default_theme_options() -> list[dict[str, str]]:
    return _copy_rows(_THEME_OPTIONS)


def build_methodology_content(
    *,
    evidence_taxonomy: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    taxonomy = evidence_taxonomy if evidence_taxonomy is not None else default_evidence_taxonomy()
    return {
        "definitions": _copy_rows(_METHODOLOGY_DEFINITIONS),
        "tests_used": _copy_rows(_TESTS_USED),
        "multiple_testing_policy": list(_MULTIPLE_TESTING_POLICY),
        "caveats": list(_CAVEATS),
        "interpretation_guidance": list(_INTERPRETATION_GUIDANCE),
        "ethical_guardrails": _copy_rows(_ETHICAL_GUARDRAILS),
        "evidence_taxonomy": [dict(row) for row in taxonomy],
    }
