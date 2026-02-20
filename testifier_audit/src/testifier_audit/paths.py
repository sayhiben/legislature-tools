from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class OutputPaths:
    root: Path
    tables: Path
    figures: Path
    summary: Path
    flags: Path
    artifacts: Path


def build_output_paths(out_dir: Path) -> OutputPaths:
    paths = OutputPaths(
        root=out_dir,
        tables=out_dir / "tables",
        figures=out_dir / "figures",
        summary=out_dir / "summary",
        flags=out_dir / "flags",
        artifacts=out_dir / "artifacts",
    )
    for path in (
        paths.root,
        paths.tables,
        paths.figures,
        paths.summary,
        paths.flags,
        paths.artifacts,
    ):
        path.mkdir(parents=True, exist_ok=True)
    return paths
