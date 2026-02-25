from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from testifier_audit.io.csi_testifiers import CSIDownloadError, download_csi_testifier_csv


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Materialize a local baseline corpus of CSI CSVs from a meeting/bill index."
    )
    parser.add_argument(
        "--index-json",
        default="data/metadata/wa_meeting_bill_index.json",
        help="Meeting/bill index JSON path.",
    )
    parser.add_argument(
        "--csv-out-dir",
        default="data/raw/baseline_corpus",
        help="Output directory for downloaded CSV files.",
    )
    parser.add_argument(
        "--metadata-out-dir",
        default="data/metadata/baseline_corpus",
        help="Output directory for hearing metadata sidecars.",
    )
    parser.add_argument(
        "--manifest-out",
        default="data/metadata/baseline_corpus_manifest.json",
        help="Run manifest JSON path.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Optional max rows from index.")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing CSV/metadata files if present.",
    )
    parser.add_argument("--top", type=int, default=100, help="CSI search top parameter.")
    parser.add_argument("--timeout-seconds", type=float, default=30.0, help="HTTP timeout.")
    parser.add_argument("--max-retries", type=int, default=3, help="HTTP retry count.")
    return parser.parse_args(argv)


def read_index_rows(index_path: Path) -> list[dict[str, Any]]:
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    rows = payload.get("rows", []) if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            out.append(dict(row))
    return out


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def materialize_baseline_corpus(
    *,
    index_path: Path,
    csv_out_dir: Path,
    metadata_out_dir: Path,
    limit: int = 0,
    overwrite: bool = False,
    top: int = 100,
    timeout_seconds: float = 30.0,
    max_retries: int = 3,
) -> dict[str, Any]:
    rows = read_index_rows(index_path)
    if limit > 0:
        rows = rows[: int(limit)]
    successes: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for row in rows:
        bill_id = str(row.get("bill_id") or "").strip()
        agenda_id = str(row.get("agenda_id") or "").strip()
        agenda_item_id = str(row.get("agenda_item_id") or "").strip()
        if not bill_id:
            continue
        try:
            result = download_csi_testifier_csv(
                bill_query=bill_id,
                csv_out_dir=csv_out_dir,
                metadata_out_dir=metadata_out_dir,
                meeting_family_id=agenda_id or None,
                agenda_item_id=agenda_item_id or None,
                top=max(1, int(top)),
                timeout_seconds=float(timeout_seconds),
                max_retries=max(0, int(max_retries)),
                overwrite=bool(overwrite),
            )
            successes.append(
                {
                    "bill_id": bill_id,
                    "agenda_id": agenda_id,
                    "agenda_item_id": agenda_item_id,
                    "csv_path": str(result.csv_path.resolve()),
                    "csv_sha256": sha256_file(result.csv_path),
                    "metadata_path": str(result.metadata_path.resolve()),
                    "metadata_sha256": sha256_file(result.metadata_path),
                    "total_rows": int(result.total_rows),
                }
            )
        except CSIDownloadError as exc:
            failures.append(
                {
                    "bill_id": bill_id,
                    "agenda_id": agenda_id,
                    "agenda_item_id": agenda_item_id,
                    "error": str(exc),
                }
            )
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "index_path": str(index_path),
        "requested_rows": int(len(rows)),
        "downloaded_rows": int(len(successes)),
        "failed_rows": int(len(failures)),
        "successes": successes,
        "failures": failures,
    }


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    index_path = Path(args.index_json).resolve()
    csv_out_dir = Path(args.csv_out_dir).resolve()
    metadata_out_dir = Path(args.metadata_out_dir).resolve()
    manifest = materialize_baseline_corpus(
        index_path=index_path,
        csv_out_dir=csv_out_dir,
        metadata_out_dir=metadata_out_dir,
        limit=int(args.limit),
        overwrite=bool(args.overwrite),
        top=int(args.top),
        timeout_seconds=float(args.timeout_seconds),
        max_retries=int(args.max_retries),
    )
    manifest_path = Path(args.manifest_out).resolve()
    write_manifest(manifest_path, manifest)
    print(
        "Materialized baseline corpus: "
        f"{int(manifest['downloaded_rows'])} success(es), "
        f"{int(manifest['failed_rows'])} failure(s)."
    )
    print(f"Wrote manifest: {manifest_path}")


if __name__ == "__main__":
    main()
