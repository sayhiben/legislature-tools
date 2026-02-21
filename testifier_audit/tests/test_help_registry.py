from __future__ import annotations

from testifier_audit.report.help_registry import (
    build_methodology_content,
    default_evidence_taxonomy,
    default_theme_options,
)


def test_help_registry_emits_expected_taxonomy_and_theme_options() -> None:
    taxonomy = default_evidence_taxonomy()
    kinds = [row["kind"] for row in taxonomy]
    assert kinds == ["stat_fdr", "calibrated_empirical", "heuristic"]

    theme_options = default_theme_options()
    assert [row["id"] for row in theme_options] == ["light", "dark"]


def test_methodology_contract_includes_guardrail_language() -> None:
    methodology = build_methodology_content()

    assert isinstance(methodology["definitions"], list)
    assert isinstance(methodology["tests_used"], list)
    assert isinstance(methodology["multiple_testing_policy"], list)
    assert isinstance(methodology["caveats"], list)
    assert isinstance(methodology["interpretation_guidance"], list)
    assert isinstance(methodology["ethical_guardrails"], list)

    requirements = [
        str(row.get("requirement", "")).lower()
        for row in methodology["ethical_guardrails"]
    ]
    assert any("statistical irregularity" in requirement for requirement in requirements)
    assert any("standalone attribution" in requirement for requirement in requirements)


def test_methodology_uses_copy_semantics_for_taxonomy() -> None:
    taxonomy = default_evidence_taxonomy()
    methodology = build_methodology_content(evidence_taxonomy=taxonomy)
    taxonomy[0]["label"] = "mutated"

    preserved = methodology["evidence_taxonomy"][0]["label"]
    assert preserved == "Statistical (FDR-controlled)"
