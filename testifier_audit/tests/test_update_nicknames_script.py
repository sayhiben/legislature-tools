from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


def _load_update_nicknames_module():
    module_path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "data"
        / "update_nicknames.py"
    )
    module_name = "update_nicknames_script"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load module spec from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_build_primary_mapping_filters_ambiguous_and_multi_token_aliases() -> None:
    module = _load_update_nicknames_module()
    rows = [
        {"relationship": "has_nickname", "name1": "Robert", "name2": "Bob"},
        {"relationship": "has_nickname", "name1": "Roberta", "name2": "Bob"},
        {"relationship": "has_nickname", "name1": "Rebecca", "name2": "Becky"},
        {"relationship": "has_nickname", "name1": "Mary Ann", "name2": "Molly"},
        {"relationship": "other", "name1": "William", "name2": "Bill"},
    ]

    out = module.build_primary_mapping(rows)

    assert out.get("BOB") is None
    assert out.get("BECKY") == "REBECCA"
    assert "MOLLY" not in out
    assert "BILL" not in out


def test_build_supplemental_mapping_parses_givenname_rows() -> None:
    module = _load_update_nicknames_module()
    lines = [
        "rebecca becca becky reba",
        "abigail abby",
        "anne ann",
        "anna ann",  # ann becomes ambiguous and should be dropped
        "# comment",
        "",
    ]

    out = module.build_supplemental_mapping(lines)

    assert out["BECCA"] == "REBECCA"
    assert out["BECKY"] == "REBECCA"
    assert out["REBA"] == "REBECCA"
    assert out["ABBY"] == "ABIGAIL"
    assert "ANN" not in out


def test_build_mapping_include_supplemental_adds_aliases_and_keeps_primary_conflicts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_update_nicknames_module()

    monkeypatch.setattr(
        module,
        "load_primary_rows",
        lambda: [
            {"relationship": "has_nickname", "name1": "Agatha", "name2": "Aggie"},
            {"relationship": "has_nickname", "name1": "William", "name2": "Will"},
        ],
    )
    monkeypatch.setattr(
        module,
        "load_supplemental_lines",
        lambda: [
            "augusta aggie",  # conflicts with primary AGGIE->AGATHA
            "abigail abby",
            "robert bobby",
        ],
    )

    merged, stats = module.build_mapping(include_supplemental=True)

    assert merged["AGGIE"] == "AGATHA"
    assert merged["ABBY"] == "ABIGAIL"
    assert merged["BOBBY"] == "ROBERT"
    # Manual overrides are always enforced.
    assert merged["BOB"] == "ROBERT"

    assert stats.primary_unambiguous_aliases == 2
    assert stats.supplemental_unambiguous_aliases == 3
    assert stats.supplemental_aliases_added == 2
    assert stats.supplemental_alias_conflicts == 1
    assert stats.supplemental_aliases_skipped_root_flip == 0
    assert any(sample["alias"] == "AGGIE" for sample in stats.conflict_samples)
    assert stats.root_flip_samples == []


def test_build_mapping_skips_supplemental_alias_that_flips_primary_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_update_nicknames_module()

    monkeypatch.setattr(
        module,
        "load_primary_rows",
        lambda: [
            {"relationship": "has_nickname", "name1": "Tony", "name2": "Anthony"},
        ],
    )
    monkeypatch.setattr(
        module,
        "load_supplemental_lines",
        lambda: [
            "anthony tony",
        ],
    )

    merged, stats = module.build_mapping(include_supplemental=True)

    # Keep the primary direction (ANTHONY -> TONY), skip reverse alias (TONY -> ANTHONY).
    assert merged["ANTHONY"] == "TONY"
    assert merged.get("TONY") is None

    assert stats.supplemental_unambiguous_aliases == 1
    assert stats.supplemental_aliases_added == 0
    assert stats.supplemental_alias_conflicts == 0
    assert stats.supplemental_aliases_skipped_root_flip == 1
    assert any(sample["alias"] == "TONY" for sample in stats.root_flip_samples)


def test_parse_args_defaults_to_include_supplemental(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_update_nicknames_module()
    monkeypatch.setattr(sys, "argv", ["update_nicknames.py"])
    args = module._parse_args()
    assert bool(args.include_supplemental) is True


def test_parse_args_primary_only_disables_supplemental(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_update_nicknames_module()
    monkeypatch.setattr(sys, "argv", ["update_nicknames.py", "--primary-only"])
    args = module._parse_args()
    assert bool(args.include_supplemental) is False
