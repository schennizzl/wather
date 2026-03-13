from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def build_metadata_fields(args: Any) -> dict[str, str]:
    return {
        key: value
        for key, value in {
            "source_file": args.meta_source_file,
            "ingested_at": args.meta_ingested_at,
            "dt": args.meta_dt,
            "hour": args.meta_hour,
        }.items()
        if value is not None
    }


def write_enveloped_ndjson(
    records: list[dict[str, Any]],
    output_path: Path,
    metadata_fields: dict[str, Any] | None = None,
    mode: str = "w",
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_fields = metadata_fields or {}
    with output_path.open(mode, encoding="utf-8") as handle:
        for record in records:
            envelope = {
                "payload": json.dumps(record, ensure_ascii=True, sort_keys=True),
                **metadata_fields,
            }
            handle.write(json.dumps(envelope, ensure_ascii=True) + "\n")
