#!/usr/bin/env python3
"""Generate a browsable reports index page for GitHub Pages."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class ReportEntry:
    """Metadata for one rendered report directory."""

    report_id: str
    report_href: str
    generated_utc: str
    table_count: int
    figure_count: int
    summary_count: int
    screenshot_href: str | None


def project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def count_files(directory: Path) -> int:
    if not directory.exists():
        return 0
    return sum(1 for path in directory.iterdir() if path.is_file())


def newest_screenshot_href(report_dir: Path) -> str | None:
    screenshots_dir = report_dir / "screenshots"
    if not screenshots_dir.exists():
        return None
    png_files = sorted(
        (
            path
            for path in screenshots_dir.iterdir()
            if path.is_file() and path.suffix.lower() == ".png"
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not png_files:
        return None
    return f"./{report_dir.name}/screenshots/{png_files[0].name}"


def collect_entries(reports_dir: Path) -> list[ReportEntry]:
    entries: list[ReportEntry] = []
    for report_dir in sorted(
        (path for path in reports_dir.iterdir() if path.is_dir()),
        reverse=True,
    ):
        report_html = report_dir / "report.html"
        if not report_html.exists():
            continue
        generated_utc = datetime.fromtimestamp(
            report_html.stat().st_mtime,
            tz=timezone.utc,
        ).strftime("%Y-%m-%d %H:%M:%S UTC")
        entries.append(
            ReportEntry(
                report_id=report_dir.name,
                report_href=f"./{report_dir.name}/report.html",
                generated_utc=generated_utc,
                table_count=count_files(report_dir / "tables"),
                figure_count=count_files(report_dir / "figures"),
                summary_count=count_files(report_dir / "summary"),
                screenshot_href=newest_screenshot_href(report_dir),
            )
        )
    return entries


def render_index(entries: list[ReportEntry], generated_at_utc: str) -> str:
    items = []
    for entry in entries:
        preview = ""
        if entry.screenshot_href:
            preview = (
                '<div class="preview">'
                f'<img loading="lazy" src="{entry.screenshot_href}" '
                f'alt="Preview screenshot for {entry.report_id}">'
                "</div>"
            )
        items.append(
            (
                '<li class="card">'
                '<div class="card-header">'
                f'<a class="title-link" href="{entry.report_href}">{entry.report_id}</a>'
                f'<span class="generated">{entry.generated_utc}</span>'
                "</div>"
                '<div class="meta">'
                f"<span>{entry.figure_count} figures</span>"
                f"<span>{entry.table_count} tables</span>"
                f"<span>{entry.summary_count} summaries</span>"
                "</div>"
                '<div class="actions">'
                f'<a href="{entry.report_href}">Open report</a>'
                f'<a href="./{entry.report_id}/">Open directory</a>'
                "</div>"
                f"{preview}"
                "</li>"
            )
        )

    body = "\n".join(items) if items else '<p class="empty">No rendered reports found yet.</p>'

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Legislature Tools Reports</title>
    <style>
      :root {{
        color-scheme: light;
        --bg: #ecf1f7;
        --surface: #ffffff;
        --ink: #1b2a3a;
        --muted: #58697d;
        --border: #d3dce8;
        --accent: #1f4f82;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        font-family: "Avenir Next", "Segoe UI", sans-serif;
        color: var(--ink);
        background: linear-gradient(180deg, #f4f7fb 0%, var(--bg) 100%);
      }}
      main {{
        max-width: 1100px;
        margin: 0 auto;
        padding: 2rem 1rem 3rem;
      }}
      header {{
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 1rem 1.25rem;
      }}
      h1 {{
        margin: 0 0 0.35rem 0;
        font-size: 1.75rem;
      }}
      .subtitle {{
        margin: 0;
        color: var(--muted);
      }}
      .card-list {{
        list-style: none;
        margin: 1rem 0 0 0;
        padding: 0;
        display: grid;
        gap: 0.85rem;
      }}
      .card {{
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 0.85rem 1rem;
      }}
      .card-header {{
        display: flex;
        gap: 0.6rem;
        flex-wrap: wrap;
        justify-content: space-between;
        align-items: baseline;
      }}
      .title-link {{
        color: var(--accent);
        text-decoration: none;
        font-weight: 700;
        font-size: 1.05rem;
      }}
      .title-link:hover {{ text-decoration: underline; }}
      .generated {{
        color: var(--muted);
        font-size: 0.85rem;
      }}
      .meta {{
        margin-top: 0.45rem;
        display: flex;
        gap: 0.8rem;
        flex-wrap: wrap;
        color: var(--muted);
        font-size: 0.92rem;
      }}
      .actions {{
        margin-top: 0.55rem;
        display: flex;
        gap: 0.85rem;
        flex-wrap: wrap;
      }}
      .actions a {{
        color: var(--accent);
      }}
      .preview {{
        margin-top: 0.75rem;
      }}
      .preview img {{
        width: 100%;
        max-height: 280px;
        object-fit: cover;
        object-position: top;
        border: 1px solid var(--border);
        border-radius: 8px;
      }}
      .empty {{
        margin-top: 1rem;
        background: var(--surface);
        border: 1px dashed var(--border);
        border-radius: 12px;
        padding: 1rem;
      }}
    </style>
  </head>
  <body>
    <main>
      <header>
        <h1>Legislature Tools Reports</h1>
        <p class="subtitle">Generated index: {generated_at_utc}</p>
      </header>
      <ul class="card-list">
        {body}
      </ul>
    </main>
  </body>
</html>
"""


def main() -> None:
    repo_root = project_root()
    reports_dir = repo_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    entries = collect_entries(reports_dir)
    generated_at_utc = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    output_path = reports_dir / "index.html"
    output_path.write_text(render_index(entries, generated_at_utc), encoding="utf-8")
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
