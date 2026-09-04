#!/usr/bin/env python3
"""產生並套用 Genji UUIDv5 遷移對照表。

預設唯讀並把對照表輸出到 stdout。``--apply`` 必須搭配 ``--map``；程序會先
原子寫入包含舊/新 UUID、詞條、讀音與原路徑的 JSON，再修改資料檔。
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

from check_data_quality import DEFAULT_DATA, DEFAULT_PENDING, PROJECT_ROOT, _atomic_write_json
from dictionary_rules import compute_uuid_v5


def build_mapping(data_root: Path, pending_root: Path) -> list[dict[str, str]]:
    mapping: list[dict[str, str]] = []
    owners: dict[str, tuple[Path, str]] = {}
    candidates: list[tuple[Path, str, dict]] = []
    for scope, root in (("data", data_root), ("pending", pending_root)):
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.json")):
            loaded = json.loads(path.read_text(encoding="utf-8"))
            rows = loaded if isinstance(loaded, list) else [loaded]
            for item in rows:
                if not isinstance(item, dict):
                    continue
                entry = item.get("entry")
                reading_block = item.get("reading")
                reading = reading_block.get("primary") if isinstance(reading_block, dict) else None
                old_uuid = item.get("uuid")
                if not all(isinstance(value, str) and value for value in (entry, reading, old_uuid)):
                    continue
                if old_uuid in owners:
                    raise ValueError(
                        f"refusing ambiguous duplicate UUID {old_uuid}: {owners[old_uuid][0]} and {path}"
                    )
                owners[old_uuid] = (path, entry)
                candidates.append((path, scope, item))
    for path, scope, item in candidates:
        entry = item["entry"]
        reading = item["reading"]["primary"]
        old_uuid = item["uuid"]
        new_uuid = compute_uuid_v5(entry, reading)
        if old_uuid == new_uuid:
            continue
        owner = owners.get(new_uuid)
        if owner is not None:
            raise ValueError(
                f"refusing UUID collision: {new_uuid} is already used by {owner[1]!r} in {owner[0]}"
            )
        try:
            original_path = str(path.relative_to(PROJECT_ROOT))
        except ValueError:
            original_path = str(path)
        mapping.append({
            "old_uuid": old_uuid,
            "new_uuid": new_uuid,
            "entry": entry,
            "reading": reading,
            "path": original_path,
            "scope": scope,
        })
    return mapping


def apply_mapping(mapping: list[dict[str, str]]) -> int:
    by_path: dict[Path, dict[str, str]] = defaultdict(dict)
    for row in mapping:
        path = Path(row["path"])
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        by_path[path][row["old_uuid"]] = row["new_uuid"]
    changed = 0
    for path, replacements in by_path.items():
        loaded = json.loads(path.read_text(encoding="utf-8"))
        rows = loaded if isinstance(loaded, list) else [loaded]
        file_changed = False
        for item in rows:
            if isinstance(item, dict) and item.get("uuid") in replacements:
                item["uuid"] = replacements[item["uuid"]]
                file_changed = True
        if file_changed:
            _atomic_write_json(path, rows)
            changed += 1
    return changed


def _write_map(path: Path, mapping: list[dict[str, str]]) -> None:
    _atomic_write_json(path.resolve(), mapping)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--pending-dir", type=Path, default=DEFAULT_PENDING)
    parser.add_argument("--map", type=Path, help="對照表 JSON 輸出路徑")
    parser.add_argument("--apply", action="store_true", help="寫入對照表後套用遷移")
    args = parser.parse_args()
    if args.apply and args.map is None:
        parser.error("--apply requires --map so the migration ledger exists before data changes")
    try:
        mapping = build_mapping(args.data_dir.resolve(), args.pending_dir.resolve())
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        print(f"UUID migration aborted: {exc}", file=sys.stderr)
        return 1
    if args.map is not None:
        _write_map(args.map, mapping)
    else:
        print(json.dumps(mapping, ensure_ascii=False, indent=2))
    changed = apply_mapping(mapping) if args.apply else 0
    print(f"UUID migrations: {len(mapping)}; changed files: {changed}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
