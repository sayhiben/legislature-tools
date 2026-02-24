#!/usr/bin/env python3
"""Generate a sortable/filterable reports index page for GitHub Pages."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

PACIFIC_TZ = ZoneInfo("America/Los_Angeles")


@dataclass(frozen=True)
class ReportEntry:
    """Metadata for one rendered report directory."""

    report_id: str
    report_href: str
    report_label: str
    bill_description: str
    meeting_local: str
    meeting_epoch: int | None
    generated_local: str
    generated_epoch: int
    total_testifiers: int | None
    pro_pct: float | None
    con_pct: float | None


def project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _load_json_file(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _parse_scalar(value: str) -> Any:
    text = value.strip()
    if text == "":
        return ""
    lowered = text.lower()
    if lowered in {"null", "none", "~"}:
        return None
    if (text.startswith("\"") and text.endswith("\"")) or (
        text.startswith("'") and text.endswith("'")
    ):
        return text[1:-1]
    try:
        if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
            return int(text)
        return float(text)
    except ValueError:
        return text


def _parse_sidecar_subset(path: Path) -> dict[str, Any]:
    """Parse a constrained subset of the sidecar YAML without third-party deps.

    This parser is intentionally minimal and targets the generated sidecar shape.
    """

    out: dict[str, Any] = {}
    current_section: str | None = None
    current_key: str | None = None

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return out

    for raw_line in lines:
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        stripped = raw_line.strip()

        if indent == 0 and stripped.endswith(":"):
            section_name = stripped[:-1]
            if section_name in {"stats", "source"}:
                current_section = section_name
                out.setdefault(section_name, {})
                current_key = None
            else:
                current_section = None
                current_key = None
            continue

        if indent == 0 and ":" in stripped:
            key, raw_value = stripped.split(":", 1)
            out[key.strip()] = _parse_scalar(raw_value)
            current_section = None
            current_key = None
            continue

        if current_section is None or current_section not in out:
            continue

        section_obj = out.get(current_section)
        if not isinstance(section_obj, dict):
            continue

        if indent >= 2 and ":" in stripped:
            key, raw_value = stripped.split(":", 1)
            parsed_value = _parse_scalar(raw_value)
            key_name = key.strip()
            section_obj[key_name] = parsed_value
            current_key = key_name
            continue

        if indent >= 2 and current_key is not None:
            existing_value = section_obj.get(current_key)
            if isinstance(existing_value, str):
                section_obj[current_key] = f"{existing_value} {stripped}".strip()

    return out


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=PACIFIC_TZ)
    return parsed


def _format_us_datetime_pacific(value: datetime | None) -> str:
    if value is None:
        return "—"
    pacific = value.astimezone(PACIFIC_TZ)
    month = pacific.strftime("%b")
    day = pacific.day
    hour = pacific.strftime("%I").lstrip("0") or "0"
    minute = pacific.strftime("%M")
    am_pm = pacific.strftime("%p")
    tz_abbr = pacific.strftime("%Z")
    return f"{month} {day}, {pacific.year} {hour}:{minute} {am_pm} {tz_abbr}"


def _format_epoch_pacific(epoch_seconds: int) -> str:
    dt_utc = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
    return _format_us_datetime_pacific(dt_utc)


def _clean_text(value: Any, *, fallback: str = "—") -> str:
    if isinstance(value, str):
        text = value.strip()
        return text if text else fallback
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


def _load_sidecar_data(
    *,
    report_id: str,
    reports_dir: Path,
    repo_root: Path,
) -> dict[str, Any]:
    default_path = repo_root / "data" / "metadata" / f"{report_id}.hearing.yaml"
    if default_path.exists():
        return _parse_sidecar_subset(default_path)

    report_data_index = reports_dir / report_id / "report_data" / "index.json"
    index_payload = _load_json_file(report_data_index)
    hearing_panel = (
        index_payload.get("interactive_charts", {}) if isinstance(index_payload, dict) else {}
    )
    if not isinstance(hearing_panel, dict):
        return {}
    panel = hearing_panel.get("hearing_context_panel", {})
    if not isinstance(panel, dict):
        return {}
    source_path_raw = panel.get("source_path")
    if not isinstance(source_path_raw, str) or not source_path_raw.strip():
        return {}
    source_path = Path(source_path_raw)
    if not source_path.exists():
        return {}
    return _parse_sidecar_subset(source_path)


def _load_summary_fallback(report_dir: Path) -> dict[str, Any]:
    summary_path = report_dir / "summary" / "investigation_summary.json"
    summary_payload = _load_json_file(summary_path)
    if summary_payload:
        return summary_payload
    feature_path = report_dir / "summary" / "feature_vector.json"
    feature_payload = _load_json_file(feature_path)
    metrics = feature_payload.get("metrics") if isinstance(feature_payload, dict) else {}
    if isinstance(metrics, dict):
        fallback: dict[str, Any] = {
            "total_submissions": metrics.get("total_submissions"),
            "overall_pro_rate": metrics.get("overall_pro_rate"),
            "overall_con_rate": metrics.get("overall_con_rate"),
        }
        return fallback
    return {}


def _build_entry(
    *,
    report_dir: Path,
    reports_dir: Path,
    repo_root: Path,
) -> ReportEntry | None:
    report_html = report_dir / "report.html"
    if not report_html.exists():
        return None

    generated_epoch = int(report_html.stat().st_mtime)
    generated_local = _format_epoch_pacific(generated_epoch)

    sidecar = _load_sidecar_data(
        report_id=report_dir.name,
        reports_dir=reports_dir,
        repo_root=repo_root,
    )
    stats = sidecar.get("stats", {})
    source = sidecar.get("source", {})
    if not isinstance(stats, dict):
        stats = {}
    if not isinstance(source, dict):
        source = {}

    report_label = _clean_text(source.get("short_bill_id"), fallback=report_dir.name)
    bill_description = _clean_text(
        source.get("agenda_item_description") or source.get("bill_title"),
        fallback="—",
    )

    meeting_dt = _parse_iso_datetime(sidecar.get("meeting_start"))
    meeting_local = _format_us_datetime_pacific(meeting_dt)
    meeting_epoch = int(meeting_dt.timestamp()) if meeting_dt is not None else None

    total_testifiers = _coerce_int(stats.get("total_rows"))
    pro_pct = _coerce_float(stats.get("total_pro_pct"))
    con_pct = _coerce_float(stats.get("total_con_pct"))

    if (pro_pct is None or con_pct is None) and total_testifiers is not None and total_testifiers > 0:
        total_pro = _coerce_float(stats.get("total_pro"))
        total_con = _coerce_float(stats.get("total_con"))
        if pro_pct is None and total_pro is not None:
            pro_pct = (total_pro / float(total_testifiers)) * 100.0
        if con_pct is None and total_con is not None:
            con_pct = (total_con / float(total_testifiers)) * 100.0

    if total_testifiers is None or pro_pct is None or con_pct is None:
        summary = _load_summary_fallback(report_dir)
        if total_testifiers is None:
            total_testifiers = _coerce_int(summary.get("total_submissions"))
        if pro_pct is None:
            pro_rate = _coerce_float(summary.get("overall_pro_rate"))
            if pro_rate is not None:
                pro_pct = pro_rate * 100.0
        if con_pct is None:
            con_rate = _coerce_float(summary.get("overall_con_rate"))
            if con_rate is not None:
                con_pct = con_rate * 100.0

    return ReportEntry(
        report_id=report_dir.name,
        report_href=f"./{report_dir.name}/report.html",
        report_label=report_label,
        bill_description=bill_description,
        meeting_local=meeting_local,
        meeting_epoch=meeting_epoch,
        generated_local=generated_local,
        generated_epoch=generated_epoch,
        total_testifiers=total_testifiers,
        pro_pct=pro_pct,
        con_pct=con_pct,
    )


def collect_entries(reports_dir: Path, repo_root: Path) -> list[ReportEntry]:
    entries: list[ReportEntry] = []
    for report_dir in sorted(
        (path for path in reports_dir.iterdir() if path.is_dir()),
        reverse=True,
    ):
        entry = _build_entry(report_dir=report_dir, reports_dir=reports_dir, repo_root=repo_root)
        if entry is not None:
            entries.append(entry)
    return entries


def render_index(entries: list[ReportEntry], generated_at_local: str) -> str:
    table_rows = [
        {
            "report_id": entry.report_id,
            "report_href": entry.report_href,
            "report_label": entry.report_label,
            "bill_description": entry.bill_description,
            "meeting_local": entry.meeting_local,
            "meeting_epoch": entry.meeting_epoch,
            "generated_local": entry.generated_local,
            "generated_epoch": entry.generated_epoch,
            "total_testifiers": entry.total_testifiers,
            "pro_pct": entry.pro_pct,
            "con_pct": entry.con_pct,
        }
        for entry in entries
    ]
    table_data_json = json.dumps(table_rows, ensure_ascii=False)

    if not entries:
        table_markup = '<p class="empty">No rendered reports found yet.</p>'
    else:
        table_markup = (
            '<section class="controls">'
            '  <label for="report-search">Global filter</label>'
            '  <input id="report-search" class="search-input" type="search" '
            '    placeholder="Filter by bill, description, or date/time">'
            '  <p class="helper">Tip: headers are sortable and each column has its own filter input.</p>'
            '</section>'
            '<section class="table-shell">'
            '  <div id="reports-table"></div>'
            '  <p id="table-stats" class="stats"></p>'
            '</section>'
        )

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Legislature Tools Reports</title>
    <link
      rel="stylesheet"
      href="https://unpkg.com/tabulator-tables@6.3.0/dist/css/tabulator.min.css"
    >
    <style>
      :root {{
        color-scheme: light;
        --bg: #edf2f7;
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
        font-size: 14px;
        line-height: 1.35;
        color: var(--ink);
        background: linear-gradient(180deg, #f8fbff 0%, var(--bg) 100%);
      }}
      main {{
        max-width: 1500px;
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
        font-size: 1.45rem;
      }}
      .subtitle {{
        margin: 0;
        color: var(--muted);
        font-size: 0.92rem;
      }}
      .controls {{
        margin-top: 1rem;
        display: grid;
        gap: 0.6rem;
      }}
      .controls label {{
        color: var(--muted);
        font-size: 0.84rem;
      }}
      .search-input {{
        width: min(560px, 100%);
        border: 1px solid var(--border);
        border-radius: 8px;
        font-size: 0.9rem;
        padding: 0.62rem 0.72rem;
      }}
      .search-input:focus {{
        outline: 2px solid color-mix(in srgb, var(--accent) 35%, white);
        outline-offset: 1px;
      }}
      .helper {{
        margin: 0;
        color: var(--muted);
        font-size: 0.84rem;
      }}
      .table-shell {{
        margin-top: 0.9rem;
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 0.75rem;
      }}
      .stats {{
        margin-top: 0.6rem;
        color: var(--muted);
        font-size: 0.82rem;
      }}
      .tabulator {{
        border: none;
        background: transparent;
        font-size: 0.86rem;
      }}
      .tabulator .tabulator-header {{
        border-bottom: 1px solid var(--border);
      }}
      .tabulator .tabulator-header .tabulator-col {{
        background: #f8fbff;
        border-right: 1px solid var(--border);
      }}
      .tabulator .tabulator-header .tabulator-col .tabulator-col-title {{
        font-size: 0.82rem;
      }}
      .tabulator .tabulator-row .tabulator-cell {{
        border-right: 1px solid #e9eff8;
      }}
      .tabulator .tabulator-cell {{
        padding: 6px 8px;
      }}
      .tabulator .tabulator-footer {{
        border-top: 1px solid var(--border);
        font-size: 0.8rem;
      }}
      .tabulator .tabulator-responsive-collapse table {{
        font-size: 0.8rem;
      }}
      .tabulator .tabulator-responsive-collapse table td {{
        padding: 4px 6px;
      }}
      .report-link {{
        color: var(--accent);
        text-decoration: none;
        font-weight: 600;
      }}
      .report-link:hover {{
        text-decoration: underline;
      }}
      .empty {{
        margin-top: 1rem;
        background: var(--surface);
        border: 1px dashed var(--border);
        border-radius: 12px;
        padding: 1rem;
      }}
      @media (max-width: 900px) {{
        main {{
          padding: 1rem 0.6rem 2rem;
        }}
        header {{
          padding: 0.8rem 0.9rem;
        }}
        h1 {{
          font-size: 1.26rem;
        }}
        .table-shell {{
          padding: 0.45rem;
        }}
        .tabulator .tabulator-header .tabulator-col {{
          min-height: 42px;
        }}
        .tabulator .tabulator-header .tabulator-col .tabulator-col-title {{
          white-space: normal;
          line-height: 1.12;
        }}
      }}
      @media (max-width: 640px) {{
        body {{
          font-size: 13px;
        }}
        .search-input {{
          width: 100%;
          font-size: 0.86rem;
          padding: 0.55rem 0.62rem;
        }}
        .tabulator {{
          font-size: 0.8rem;
        }}
        .tabulator .tabulator-cell {{
          padding: 5px 6px;
        }}
        .tabulator .tabulator-header .tabulator-col .tabulator-col-title {{
          font-size: 0.75rem;
        }}
      }}
    </style>
  </head>
  <body>
    <main>
      <header>
        <h1>Legislature Tools Reports</h1>
        <p class="subtitle">Generated index: {generated_at_local}</p>
      </header>
      {table_markup}
    </main>
    <script src="https://unpkg.com/tabulator-tables@6.3.0/dist/js/tabulator.min.js"></script>
    <script>
      const tableRows = {table_data_json};
      const tableElement = document.getElementById("reports-table");
      const tableStats = document.getElementById("table-stats");
      const globalSearchInput = document.getElementById("report-search");

      if (tableElement && typeof window.Tabulator !== "undefined") {{
        const isMobile = window.matchMedia("(max-width: 700px)").matches;
        const asLink = (href, label) => {{
          if (!href) {{
            return "<span>-</span>";
          }}
          const anchor = document.createElement("a");
          anchor.className = "report-link";
          anchor.href = String(href);
          anchor.textContent = String(label);
          return anchor.outerHTML;
        }};

        const formatInt = (value) => {{
          const numeric = Number(value);
          if (!Number.isFinite(numeric)) {{
            return "-";
          }}
          return new Intl.NumberFormat("en-US").format(Math.trunc(numeric));
        }};

        const formatPct = (value) => {{
          const numeric = Number(value);
          if (!Number.isFinite(numeric)) {{
            return "-";
          }}
          return `${{numeric.toFixed(1)}}%`;
        }};

        const sortEpoch = (a, b) => {{
          const aRaw = Number(a);
          const bRaw = Number(b);
          const aEpoch = Number.isFinite(aRaw) ? aRaw : Number.NEGATIVE_INFINITY;
          const bEpoch = Number.isFinite(bRaw) ? bRaw : Number.NEGATIVE_INFINITY;
          return aEpoch - bEpoch;
        }};

        const matchesDisplayText = (headerValue, rowData, displayField) => {{
          const needle = String(headerValue || "").trim().toLowerCase();
          if (!needle) {{
            return true;
          }}
          return String(rowData?.[displayField] || "").toLowerCase().includes(needle);
        }};

        const toPlainText = (value) => {{
          if (value === null || value === undefined) {{
            return "";
          }}
          if (typeof value === "string") {{
            const parser = document.createElement("div");
            parser.innerHTML = value;
            return (parser.textContent || "").trim();
          }}
          if (value instanceof Node) {{
            return (value.textContent || "").trim();
          }}
          return String(value).trim();
        }};

        const renderResponsiveCollapse = (items) => {{
          const table = document.createElement("table");
          const seen = new Set();

          for (const item of items || []) {{
            const title = String(item?.title || "").trim();
            const valueText = toPlainText(item?.value);
            if (!title || title === "Details") {{
              continue;
            }}
            const key = `${{title}}::${{valueText}}`;
            if (seen.has(key)) {{
              continue;
            }}
            seen.add(key);

            const row = document.createElement("tr");
            const titleCell = document.createElement("td");
            const strong = document.createElement("strong");
            strong.textContent = title;
            titleCell.appendChild(strong);

            const valueCell = document.createElement("td");
            valueCell.textContent = valueText;

            row.appendChild(titleCell);
            row.appendChild(valueCell);
            table.appendChild(row);
          }}

          return table;
        }};

        const table = new window.Tabulator(tableElement, {{
          data: tableRows,
          layout: "fitColumns",
          responsiveLayout: isMobile ? "collapse" : false,
          responsiveLayoutCollapseStartOpen: false,
          responsiveLayoutCollapseUseFormatters: true,
          responsiveLayoutCollapseFormatter: renderResponsiveCollapse,
          pagination: "local",
          paginationSize: isMobile ? 10 : 25,
          paginationSizeSelector: [10, 25, 50, 100],
          initialSort: [{{ column: "meeting_epoch", dir: "desc" }}],
          columns: [
            {{
              title: "Report",
              field: "report_label",
              minWidth: 150,
              headerFilter: "input",
              formatter: (cell) => {{
                const row = cell.getRow().getData();
                return asLink(row.report_href, row.report_label || row.report_id);
              }},
            }},
            {{
              title: "Bill Description",
              field: "bill_description",
              minWidth: 420,
              headerFilter: "input",
              formatter: (cell) => {{
                const value = String(cell.getValue() || "-");
                return `<span title="${{value}}">${{value}}</span>`;
              }},
            }},
            {{
              title: "Total Testifiers",
              field: "total_testifiers",
              hozAlign: "right",
              width: 150,
              sorter: "number",
              headerFilter: "number",
              formatter: (cell) => formatInt(cell.getValue()),
            }},
            {{
              title: "Pro %",
              field: "pro_pct",
              hozAlign: "right",
              width: 95,
              sorter: "number",
              headerFilter: "number",
              formatter: (cell) => formatPct(cell.getValue()),
            }},
            {{
              title: "Con %",
              field: "con_pct",
              hozAlign: "right",
              width: 95,
              sorter: "number",
              headerFilter: "number",
              formatter: (cell) => formatPct(cell.getValue()),
            }},
            {{
              title: "Meeting Datetime (PT)",
              field: "meeting_epoch",
              minWidth: 210,
              headerFilter: "input",
              headerFilterFunc: (headerValue, _rowValue, rowData) =>
                matchesDisplayText(headerValue, rowData, "meeting_local"),
              sorter: sortEpoch,
              formatter: (cell) => String(cell.getRow().getData().meeting_local || "—"),
            }},
            {{
              title: "Last Updated (PT)",
              field: "generated_epoch",
              minWidth: 210,
              headerFilter: "input",
              headerFilterFunc: (headerValue, _rowValue, rowData) =>
                matchesDisplayText(headerValue, rowData, "generated_local"),
              sorter: sortEpoch,
              formatter: (cell) => String(cell.getRow().getData().generated_local || "—"),
            }},
            ...(isMobile ? [{{
              title: "Details",
              field: "__details_toggle",
              formatter: "responsiveCollapse",
              width: 86,
              minWidth: 86,
              hozAlign: "center",
              headerHozAlign: "center",
              headerSort: false,
              resizable: false,
              headerFilter: false,
            }}] : []),
          ],
        }});

        const updateStats = () => {{
          if (!tableStats) {{
            return;
          }}
          const shown = typeof table.getDataCount === "function"
            ? table.getDataCount("active")
            : table.getRows("active").length;
          const total = tableRows.length;
          tableStats.textContent = `Showing ${{shown}} of ${{total}} reports`;
        }};

        table.on("tableBuilt", updateStats);
        table.on("dataLoaded", updateStats);
        table.on("dataFiltered", updateStats);
        table.on("pageLoaded", updateStats);
        table.on("renderComplete", updateStats);
        window.requestAnimationFrame(updateStats);

        if (globalSearchInput) {{
          globalSearchInput.addEventListener("input", (event) => {{
            const needle = String(event.target.value || "").trim().toLowerCase();
            if (!needle) {{
              table.clearFilter();
              updateStats();
              return;
            }}
            table.setFilter((rowData) => {{
              return (
                String(rowData.report_label || "").toLowerCase().includes(needle)
                || String(rowData.bill_description || "").toLowerCase().includes(needle)
                || String(rowData.meeting_local || "").toLowerCase().includes(needle)
                || String(rowData.generated_local || "").toLowerCase().includes(needle)
              );
            }});
            updateStats();
          }});
        }}
      }}
    </script>
  </body>
</html>
"""


def main() -> None:
    repo_root = project_root()
    reports_dir = repo_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    entries = collect_entries(reports_dir, repo_root)
    generated_at_local = _format_us_datetime_pacific(datetime.now(tz=timezone.utc))
    output_path = reports_dir / "index.html"
    output_path.write_text(render_index(entries, generated_at_local), encoding="utf-8")
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
