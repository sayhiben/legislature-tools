from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from testifier_audit.io.wa_committee_service import build_meeting_bill_index


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a local WA CommitteeMeetingService meeting/bill index artifact."
    )
    parser.add_argument("--start-date", required=True, help="Inclusive start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", required=True, help="Inclusive end date (YYYY-MM-DD).")
    parser.add_argument(
        "--output-json",
        default="data/metadata/wa_meeting_bill_index.json",
        help="Output JSON manifest path.",
    )
    parser.add_argument(
        "--output-csv",
        default="data/metadata/wa_meeting_bill_index.csv",
        help="Output CSV path.",
    )
    parser.add_argument(
        "--skip-revised",
        action="store_true",
        help="Skip GetRevisedCommitteeMeetings and use scheduled meetings only.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help="HTTP timeout per request.",
    )
    return parser.parse_args(argv)


def sha256_json_rows(rows: list[dict[str, Any]]) -> str:
    canonical = json.dumps(rows, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def write_index_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "agenda_id",
        "agenda_item_id",
        "bill_id",
        "item_description",
        "hearing_type_description",
        "meeting_date",
        "revised_date",
        "agency",
        "committee_acronym",
        "committee_name",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def build_index_payload(
    *,
    start_date: date,
    end_date: date,
    include_revised: bool,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    endpoint_list = ["GetCommitteeMeetings", "GetCommitteeMeetingItems"]
    if include_revised:
        endpoint_list.append("GetRevisedCommitteeMeetings")
    return {
        "schema_version": 1,
        "retrieved_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "source": {
            "service": "wa_committee_meeting_service",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "include_revised": bool(include_revised),
            "endpoints": endpoint_list,
        },
        "row_count": int(len(rows)),
        "rows_sha256": sha256_json_rows(rows),
        "rows": rows,
    }


def write_index_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def run_build_meeting_bill_index(
    *,
    start_date: date,
    end_date: date,
    output_json: Path,
    output_csv: Path,
    include_revised: bool = True,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    rows = build_meeting_bill_index(
        begin_date=start_date,
        end_date=end_date,
        include_revised=include_revised,
        timeout_seconds=float(timeout_seconds),
    )
    payload = build_index_payload(
        start_date=start_date,
        end_date=end_date,
        include_revised=include_revised,
        rows=rows,
    )
    write_index_json(output_json, payload)
    write_index_csv(output_csv, rows)
    return payload


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    start_date = date.fromisoformat(str(args.start_date))
    end_date = date.fromisoformat(str(args.end_date))
    include_revised = not bool(args.skip_revised)
    output_json = Path(args.output_json).resolve()
    output_csv = Path(args.output_csv).resolve()
    payload = run_build_meeting_bill_index(
        start_date=start_date,
        end_date=end_date,
        output_json=output_json,
        output_csv=output_csv,
        include_revised=include_revised,
        timeout_seconds=float(args.timeout_seconds),
    )
    print(f"Wrote meeting/bill index JSON: {output_json} ({int(payload['row_count'])} rows)")
    print(f"Wrote meeting/bill index CSV:  {output_csv}")


if __name__ == "__main__":
    main()
