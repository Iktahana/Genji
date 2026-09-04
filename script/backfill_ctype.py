#!/usr/bin/env python3
"""Conservatively normalize and backfill ``grammar.ctype`` values.

The command is read-only unless ``--apply`` is explicitly supplied.  Existing
non-empty values always win; empty values are filled only when the POS labels
produce exactly one conjugation-type candidate.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Any


_SCRIPT_DIR = Path(__file__).resolve().parent
_DEFAULT_DATA_DIR = _SCRIPT_DIR.parent / "data"

_ROWS = {
    "u": "ワ",
    "ku": "カ",
    "gu": "ガ",
    "su": "サ",
    "zu": "ザ",
    "tsu": "タ",
    "dzu": "ダ",
    "nu": "ナ",
    "hu/fu": "ハ",
    "bu": "バ",
    "mu": "マ",
    "yu": "ヤ",
    "ru": "ラ",
}

_EXISTING_ALIASES = {
    "五段-ウ行": "五段-ワア行",
    "五段-ワ行": "五段-ワア行",
    "五段活用-ウ行": "五段-ワア行",
    "五段活用-ワ行": "五段-ワア行",
    "五段活用-ワア行": "五段-ワア行",
    "一段活用": "一段",
    "一段動詞": "一段",
    "サ変": "サ行変格",
    "サ変活用": "サ行変格",
    "サ行変格活用": "サ行変格",
    "カ変": "カ行変格",
    "カ変活用": "カ行変格",
    "カ行変格活用": "カ行変格",
    "イ形容詞": "形容詞",
    "形容詞活用": "形容詞",
}

_MODERN_ROWS = "アカガサザタダナハバマヤラワ"
_KNOWN_EXACT = {
    "一段",
    "サ行変格",
    "カ行変格",
    "形容詞",
    "文語サ行変格",
    "文語カ行変格",
    "文語ナ行変格",
    "文語ラ行変格",
    "文語形容詞-ク",
    "文語形容詞-シク",
    "文語形容動詞-ナリ",
    "文語形容動詞-タリ",
    # Values already used by the corpus.  They remain more specific than a
    # POS-derived 一段 value and must not be discarded.
    "助動詞-ダ",
    "助動詞-タ",
    "文語助動詞-ケム",
}


def _clean_text(value: str) -> str:
    """Apply the non-semantic normalization allowed for existing values."""
    value = unicodedata.normalize("NFC", value)
    value = " ".join(value.split())
    value = re.sub(r"\s*[-‐‑‒–—―]\s*", "-", value)
    return value.strip()


def normalize_existing_ctype(value: str) -> tuple[str, bool]:
    """Return normalized existing ctype and whether it is a known value."""
    normalized = _clean_text(value)
    normalized = _EXISTING_ALIASES.get(normalized, normalized)
    return normalized, is_known_ctype(normalized)


def is_known_ctype(value: str) -> bool:
    if value in _KNOWN_EXACT:
        return True
    if re.fullmatch(rf"五段-[{_MODERN_ROWS}]行", value):
        return True
    if value == "五段-ワア行":
        return True
    if re.fullmatch(rf"(?:上|下)一段-[{_MODERN_ROWS}]行", value):
        return True
    if re.fullmatch(rf"文語(?:四段|上二段|下二段|上一段|下一段)-[{_MODERN_ROWS}]行", value):
        return True
    return False


def ctype_from_pos(pos: str) -> str | None:
    """Map one POS label to a conjugation type, or return no candidate."""
    pos = _clean_text(pos)

    modern = re.fullmatch(r"動詞-五段-([アカガサザタダナハバマヤラワウ])行", pos)
    if modern:
        row = modern.group(1)
        return "五段-ワア行" if row in {"ウ", "ワ"} else f"五段-{row}行"

    godan_english = re.fullmatch(r"Godan verb with '([^']+)' ending", pos)
    if godan_english and godan_english.group(1) in _ROWS:
        row = _ROWS[godan_english.group(1)]
        return "五段-ワア行" if row == "ワ" else f"五段-{row}行"

    if pos in {"動詞-五段-行く"}:
        return "五段-カ行"
    if pos in {"動詞-五段-ある", "動詞-五段-ラ行-不規則"}:
        return "五段-ラ行"
    if pos == "動詞-五段-ウ行-特殊":
        return "五段-ワア行"
    if pos == "動詞-五段-ナ行":
        return "五段-ナ行"
    if pos.startswith("動詞-一段"):
        return "一段"
    if pos in {"動詞-サ変", "動詞-サ変-特殊", "動詞-サ変-する", "動詞-サ変-す", "動詞-ずる変"}:
        return "サ行変格"
    if pos == "動詞-来る":
        return "カ行変格"
    if pos in {"形容詞", "形容詞-良い型"}:
        return "形容詞"

    classical = re.fullmatch(
        r"動詞-(四段|上二段|下二段)-([アカガサザタダナハバマヤラワ])行-古典",
        pos,
    )
    if classical:
        kind, row = classical.groups()
        return f"文語{kind}-{row}行"
    if pos == "動詞-二段-ウ行-古典":
        # This source label denotes the historical ア行 lower-bigrade class,
        # not a modern ワア-row godan verb.
        return "文語下二段-ア行"
    if pos == "動詞-り変":
        return "文語ラ行変格"
    if pos == "動詞-ぬ変":
        return "文語ナ行変格"

    archaic_verb = re.fullmatch(
        r"(Yodan verb|Nidan verb \((upper|lower) class\)) with '([^']+)' ending(?: and 'we' conjugation)? \(archaic\)",
        pos,
    )
    if archaic_verb:
        family, level, ending = archaic_verb.groups()
        row = _ROWS.get(ending)
        if row:
            kind = "四段" if family == "Yodan verb" else ("上二段" if level == "upper" else "下二段")
            return f"文語{kind}-{row}行"

    if pos == "'ku' adjective (archaic)":
        return "文語形容詞-ク"
    if pos == "'shiku' adjective (archaic)":
        return "文語形容詞-シク"
    if pos == "archaic/formal form of na-adjective":
        return "文語形容動詞-ナリ"
    if pos == "形容詞-たる":
        return "文語形容動詞-タリ"
    return None


def _detail(path: Path, data_dir: Path, item: dict[str, Any]) -> dict[str, Any]:
    try:
        display_path = str(path.relative_to(data_dir))
    except ValueError:
        display_path = str(path)
    return {
        "path": display_path,
        "uuid": item.get("uuid"),
        "entry": item.get("entry"),
    }


def _atomic_write_json(path: Path, value: Any) -> None:
    """Replace one JSON file atomically while retaining its permission bits."""
    payload = json.dumps(value, ensure_ascii=False, indent=2) + "\n"
    mode = path.stat().st_mode
    temp_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            temp_name = handle.name
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp_name, mode)
        os.replace(temp_name, path)
        temp_name = None
    finally:
        if temp_name is not None:
            try:
                os.unlink(temp_name)
            except FileNotFoundError:
                pass


def _atomic_write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    temp_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            temp_name = handle.name
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
        temp_name = None
    finally:
        if temp_name is not None:
            try:
                os.unlink(temp_name)
            except FileNotFoundError:
                pass


def process_data(data_dir: Path, *, apply: bool = False) -> dict[str, Any]:
    """Scan a data tree, optionally applying planned changes, and return a report."""
    data_dir = data_dir.resolve()
    paths = sorted(data_dir.rglob("*.json"))
    summary = Counter(
        files_scanned=len(paths),
        entries_scanned=0,
        existing_ctype=0,
        fillable_ctype=0,
        conflicts=0,
        unmapped_empty=0,
        unknown_existing=0,
        invalid_data=0,
        affected_files=0,
        files_written=0,
    )
    current_distribution: Counter[str] = Counter()
    projected_distribution: Counter[str] = Counter()
    unmapped_pos: Counter[str] = Counter()
    conflicts: list[dict[str, Any]] = []
    unknown_existing: list[dict[str, Any]] = []
    invalid_data: list[dict[str, Any]] = []

    for path in paths:
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            summary["invalid_data"] += 1
            invalid_data.append({"path": str(path.relative_to(data_dir)), "error": str(exc)})
            continue

        if isinstance(document, dict):
            items: list[Any] = [document]
        elif isinstance(document, list):
            items = document
        else:
            summary["invalid_data"] += 1
            invalid_data.append({
                "path": str(path.relative_to(data_dir)),
                "error": f"root must be an object or array, got {type(document).__name__}",
            })
            continue

        dirty = False
        for index, raw_item in enumerate(items):
            summary["entries_scanned"] += 1
            if not isinstance(raw_item, dict):
                summary["invalid_data"] += 1
                invalid_data.append({
                    "path": str(path.relative_to(data_dir)),
                    "index": index,
                    "error": f"entry must be an object, got {type(raw_item).__name__}",
                })
                continue
            item: dict[str, Any] = raw_item
            grammar = item.get("grammar")
            if not isinstance(grammar, dict):
                summary["invalid_data"] += 1
                invalid_data.append({**_detail(path, data_dir, item), "error": "grammar must be an object"})
                continue

            raw_ctype = grammar.get("ctype")
            if raw_ctype is not None and not isinstance(raw_ctype, str):
                summary["invalid_data"] += 1
                invalid_data.append({
                    **_detail(path, data_dir, item),
                    "error": f"grammar.ctype must be a string or null, got {type(raw_ctype).__name__}",
                })
                continue

            if isinstance(raw_ctype, str) and _clean_text(raw_ctype):
                normalized, known = normalize_existing_ctype(raw_ctype)
                summary["existing_ctype"] += 1
                current_distribution[normalized] += 1
                projected_distribution[normalized] += 1
                if not known:
                    summary["unknown_existing"] += 1
                    unknown_existing.append({
                        **_detail(path, data_dir, item),
                        "ctype": raw_ctype,
                        "normalized_ctype": normalized,
                    })

                if grammar.get("ctype") != normalized:
                    grammar["ctype"] = normalized
                    dirty = True
                preserve_provenance = grammar.get("ctype_source") in {"manual", "pos-derived"}
                if not preserve_provenance:
                    if grammar.get("ctype_source") != "existing":
                        grammar["ctype_source"] = "existing"
                        dirty = True
                    expected_confidence = "high" if known else "medium"
                    if grammar.get("ctype_confidence") != expected_confidence:
                        grammar["ctype_confidence"] = expected_confidence
                        dirty = True
                continue

            pos_values = grammar.get("pos")
            if not isinstance(pos_values, list) or any(not isinstance(pos, str) for pos in pos_values):
                summary["invalid_data"] += 1
                invalid_data.append({
                    **_detail(path, data_dir, item),
                    "error": "grammar.pos must be an array of strings",
                })
                continue

            candidates = sorted({candidate for pos in pos_values if (candidate := ctype_from_pos(pos))})
            if len(candidates) == 1:
                candidate = candidates[0]
                summary["fillable_ctype"] += 1
                projected_distribution[candidate] += 1
                if grammar.get("ctype") != candidate:
                    grammar["ctype"] = candidate
                    dirty = True
                if grammar.get("ctype_source") != "pos-derived":
                    grammar["ctype_source"] = "pos-derived"
                    dirty = True
                if grammar.get("ctype_confidence") != "high":
                    grammar["ctype_confidence"] = "high"
                    dirty = True
            else:
                projected_distribution["<null>"] += 1
                if len(candidates) > 1:
                    summary["conflicts"] += 1
                    conflicts.append({
                        **_detail(path, data_dir, item),
                        "pos": pos_values,
                        "candidates": candidates,
                    })
                else:
                    summary["unmapped_empty"] += 1
                    unmapped_pos.update(pos_values or ["<empty>"])
                # A null/blank ctype must never carry misleading provenance.
                if grammar.get("ctype") is not None:
                    grammar["ctype"] = None
                    dirty = True
                for key in ("ctype_source", "ctype_confidence"):
                    if key in grammar:
                        del grammar[key]
                        dirty = True

        if dirty:
            summary["affected_files"] += 1
            if apply:
                _atomic_write_json(path, document)
                summary["files_written"] += 1

    return {
        "mode": "apply" if apply else "dry-run",
        "data_dir": str(data_dir),
        "summary": dict(summary),
        "ctype_distribution": {
            "current_non_null": dict(sorted(current_distribution.items())),
            "projected": dict(sorted(projected_distribution.items())),
        },
        "unmapped_pos_distribution": dict(sorted(unmapped_pos.items(), key=lambda pair: (-pair[1], pair[0]))),
        "conflicts": conflicts,
        "unknown_existing": unknown_existing,
        "invalid_data": invalid_data,
    }


def _print_summary(report: dict[str, Any]) -> None:
    summary = report["summary"]
    print(f"ctype backfill ({report['mode']})")
    print(f"  data directory:       {report['data_dir']}")
    print(f"  files scanned:        {summary['files_scanned']:,}")
    print(f"  entries scanned:      {summary['entries_scanned']:,}")
    print(f"  existing ctype:       {summary['existing_ctype']:,}")
    print(f"  fillable from POS:    {summary['fillable_ctype']:,}")
    print(f"  candidate conflicts: {summary['conflicts']:,}")
    print(f"  unmapped empty:       {summary['unmapped_empty']:,}")
    print(f"  unknown existing:     {summary['unknown_existing']:,}")
    print(f"  invalid data:         {summary['invalid_data']:,}")
    print(f"  affected files:       {summary['affected_files']:,}")
    if report["mode"] == "apply":
        print(f"  files written:        {summary['files_written']:,}")
    else:
        print("  no files written (pass --apply to persist changes)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="atomically write planned changes")
    parser.add_argument("--data-dir", type=Path, default=_DEFAULT_DATA_DIR, help="dictionary data root")
    parser.add_argument("--report", type=Path, help="write a detailed JSON report")
    args = parser.parse_args(argv)

    if not args.data_dir.is_dir():
        parser.error(f"data directory does not exist: {args.data_dir}")
    report = process_data(args.data_dir, apply=args.apply)
    _print_summary(report)
    if args.report:
        _atomic_write_report(args.report.resolve(), report)
        print(f"  report written:       {args.report.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
