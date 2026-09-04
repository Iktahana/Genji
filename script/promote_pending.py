#!/usr/bin/env python3
"""把人工補完讀音的待審詞条移入正式資料路徑。

合格條件：讀音為合法假名、``meta.needs_reading`` 已由人工移除、UUID 已依新讀音
完成 UUIDv5 遷移。預設 dry-run；加上 ``--apply`` 才會移動。
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

from check_data_quality import DEFAULT_DATA, DEFAULT_PENDING, _atomic_write_json, _remove_empty_parents
from dictionary_rules import compute_uuid_v5, expected_data_path, is_valid_reading


def _candidate_files(pending_root: Path, requested: list[Path]) -> list[Path]:
    if not requested:
        return sorted(pending_root.rglob("*.json")) if pending_root.exists() else []
    files: set[Path] = set()
    for value in requested:
        path = value.resolve()
        if path != pending_root and pending_root not in path.parents:
            raise ValueError(f"candidate is outside pending root: {path}")
        if path.is_dir():
            files.update(path.rglob("*.json"))
        elif path.suffix == ".json":
            files.add(path)
    return sorted(files)


def promote(data_root: Path, pending_root: Path, requested: list[Path],
            apply: bool = False) -> dict:
    files = _candidate_files(pending_root, requested)
    eligible: list[tuple[Path, dict, Path]] = []
    skipped: list[dict[str, str]] = []
    destination_cache: dict[Path, list[object]] = {}

    for path in files:
        loaded = json.loads(path.read_text(encoding="utf-8"))
        rows = loaded if isinstance(loaded, list) else [loaded]
        for item in rows:
            entry = item.get("entry") if isinstance(item, dict) else None
            reading_block = item.get("reading") if isinstance(item, dict) else None
            reading = reading_block.get("primary") if isinstance(reading_block, dict) else None
            meta = item.get("meta") if isinstance(item, dict) else None
            reason = None
            if not isinstance(item, dict) or not isinstance(entry, str):
                reason = "invalid entry structure"
            elif not isinstance(reading, str) or not is_valid_reading(reading):
                reason = "reading is not valid Kana"
            elif not isinstance(meta, dict) or "needs_reading" in meta:
                reason = "remove meta.needs_reading after human review"
            elif item.get("uuid") != compute_uuid_v5(entry, reading):
                reason = "UUID does not match the reviewed reading; run migrate_uuids.py first"
            destination = expected_data_path(data_root, reading) if reason is None else None
            if destination is None and reason is None:
                reason = "reading cannot be routed"
            if reason is not None:
                skipped.append({"path": str(path), "entry": str(entry or ""), "reason": reason})
                continue
            existing = destination_cache.get(destination)
            if existing is None:
                if destination.exists():
                    loaded_destination = json.loads(destination.read_text(encoding="utf-8"))
                    existing = loaded_destination if isinstance(loaded_destination, list) else [loaded_destination]
                else:
                    existing = []
                destination_cache[destination] = existing
            collision = next((row for row in existing if isinstance(row, dict) and (
                row.get("uuid") == item.get("uuid") or
                (row.get("entry") == entry and (row.get("reading") or {}).get("primary") == reading)
            )), None)
            if collision is not None:
                skipped.append({"path": str(path), "entry": entry,
                                "reason": "formal destination already contains this UUID or entry/reading"})
                continue
            existing.append(item)
            eligible.append((path, item, destination))

    if apply and eligible:
        # Re-read sources and match stable UUIDs rather than relying on object identity.
        by_source: dict[Path, set[str]] = defaultdict(set)
        for source, item, _ in eligible:
            by_source[source].add(item["uuid"])
        for destination, rows in destination_cache.items():
            if any(target == destination for _, _, target in eligible):
                _atomic_write_json(destination, rows)
        for source, uuids in by_source.items():
            loaded = json.loads(source.read_text(encoding="utf-8"))
            rows = loaded if isinstance(loaded, list) else [loaded]
            kept = [row for row in rows if not (isinstance(row, dict) and row.get("uuid") in uuids)]
            if kept:
                _atomic_write_json(source, kept)
            else:
                source.unlink(missing_ok=True)
        _remove_empty_parents(pending_root)
    return {
        "scanned_files": len(files),
        "eligible": len(eligible),
        "promoted": len(eligible) if apply else 0,
        "skipped": skipped,
        "moves": [{"entry": item["entry"], "from": str(source), "to": str(target)}
                  for source, item, target in eligible],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", type=Path, help="待審 JSON 或目錄；省略時掃描全部")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--pending-dir", type=Path, default=DEFAULT_PENDING)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        report = promote(args.data_dir.resolve(), args.pending_dir.resolve(), args.paths, args.apply)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        print(f"Pending promotion aborted: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
