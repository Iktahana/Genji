#!/usr/bin/env python3
"""Genji 詞条資料的一致性巡檢與安全、冪等修復。

預設同時檢查正式 ``data/`` 與 ``pending/needs_reading/``，不寫入檔案。
硬錯誤令程序以 1 結束；只有警告時仍以 0 結束。``--fix`` 只執行不需
語義判斷的修復，且不會變更 UUID（UUID 請使用 ``migrate_uuids.py``）。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import unicodedata
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from dictionary_rules import (
    UNICODE_VERSION,
    compute_uuid_v5,
    expected_data_path,
    forbidden_identifier_chars,
    invalid_reading_chars,
    is_valid_reading,
    quarantine_relative_path,
    unicode_label,
)


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_DATA = PROJECT_ROOT / "data"
DEFAULT_PENDING = PROJECT_ROOT / "pending" / "needs_reading"
MAX_COMPONENT_BYTES = 255
LONG_ENTRY_LENGTH = 80
LONG_READING_LENGTH = 160

KNOWN_POS = frozenset({
    "名詞", "動詞", "形容詞", "形容動詞", "副詞", "助詞", "助動詞", "接続詞",
    "連体詞", "感動詞", "数詞", "コピュラ", "接頭辞", "接尾辞", "補助形容詞", "補助動詞",
    "表現", "慣用句", "ことわざ", "固有名詞", "人名", "姓", "名", "地名", "組織名",
})


@dataclass(frozen=True)
class Issue:
    severity: str
    code: str
    scope: str
    path: str
    message: str
    entry: str | None = None

    def as_dict(self) -> dict[str, str]:
        result = {"path": self.path, "message": self.message}
        if self.entry is not None:
            result["entry"] = self.entry
        return result


@dataclass
class AuditResult:
    files: int = 0
    entries: int = 0
    issues: list[Issue] = field(default_factory=list)
    issue_counts: Counter[str] = field(default_factory=Counter)
    severity_counts: Counter[str] = field(default_factory=Counter)
    scope_files: Counter[str] = field(default_factory=Counter)
    scope_entries: Counter[str] = field(default_factory=Counter)
    quarantined: int = 0
    changed_files: int = 0
    deduplicated: int = 0
    examples_deduplicated: int = 0

    @property
    def error_count(self) -> int:
        return self.severity_counts["error"]

    @property
    def warning_count(self) -> int:
        return self.severity_counts["warning"]


def _atomic_write_json(path: Path, rows: list[object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(rows, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        os.replace(temp_path, path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def _add_issue(result: AuditResult, severity: str, code: str, scope: str,
               path: Path, message: str, entry: str | None = None) -> None:
    result.issues.append(Issue(severity, code, scope, str(path), message, entry))
    result.issue_counts[code] += 1
    result.severity_counts[severity] += 1


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _check_identifier(value: object, field_name: str, path: Path, scope: str,
                      result: AuditResult, entry: str | None) -> bool:
    if not isinstance(value, str) or not value:
        _add_issue(result, "error", f"schema.{field_name}", scope, path,
                   f"{field_name} must be a non-empty string", entry)
        return False
    forbidden = forbidden_identifier_chars(value)
    if forbidden:
        labels = ", ".join(unicode_label(char) for char in forbidden)
        _add_issue(result, "error", "unicode.forbidden", scope, path,
                   f"{field_name} contains forbidden Unicode characters: {labels}", entry)
    if value != unicodedata.normalize("NFC", value):
        _add_issue(result, "error", "unicode.not_nfc", scope, path,
                   f"{field_name} is not Unicode NFC-normalized", entry)
    if value != value.strip():
        _add_issue(result, "error", "unicode.edge_whitespace", scope, path,
                   f"{field_name} has leading or trailing whitespace", entry)
    return True


def _portable_path_key(relative: Path) -> str:
    return unicodedata.normalize("NFKC", relative.as_posix()).casefold()


def _check_path(path: Path, root: Path, scope: str, result: AuditResult,
                seen_paths: dict[tuple[str, str], Path]) -> None:
    relative = path.relative_to(root)
    for component in relative.parts:
        if component != unicodedata.normalize("NFC", component):
            _add_issue(result, "error", "path.not_nfc", scope, path,
                       f"path component {component!r} is not NFC-normalized")
        if len(component.encode("utf-8")) > MAX_COMPONENT_BYTES:
            _add_issue(result, "error", "path.component_too_long", scope, path,
                       f"path component exceeds {MAX_COMPONENT_BYTES} UTF-8 bytes")
    key = (scope, _portable_path_key(relative))
    previous = seen_paths.get(key)
    if previous is not None and previous != path:
        _add_issue(result, "error", "path.portable_collision", scope, path,
                   f"path collides after Unicode normalization/case folding with {previous}")
    else:
        seen_paths[key] = path


def _json_key(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _dedupe_list(values: list[object]) -> int:
    seen: set[str] = set()
    kept: list[object] = []
    for value in values:
        key = _json_key(value)
        if key not in seen:
            seen.add(key)
            kept.append(value)
    removed = len(values) - len(kept)
    if removed:
        values[:] = kept
    return removed


def _information_score(value: object) -> tuple[int, int]:
    if isinstance(value, dict):
        children = [_information_score(item) for item in value.values()]
        return sum(s[0] for s in children), len(value) + sum(s[1] for s in children)
    if isinstance(value, list):
        children = [_information_score(item) for item in value]
        return sum(s[0] for s in children), len(value) + sum(s[1] for s in children)
    return (1 if value not in (None, "", False) else 0), 1


def _merge_missing(base: dict, other: dict) -> None:
    for key, value in other.items():
        if key not in base or base[key] in (None, ""):
            base[key] = value
        elif isinstance(base[key], dict) and isinstance(value, dict):
            _merge_missing(base[key], value)
        elif isinstance(base[key], list) and isinstance(value, list):
            _extend_unique(base[key], value)


def _deduplicate_examples(definition: dict) -> int:
    examples = definition.get("examples")
    if not isinstance(examples, dict):
        return 0
    groups: dict[str, list[tuple[str, int, dict]]] = defaultdict(list)
    order: list[tuple[str, int, str]] = []
    for kind, values in examples.items():
        if not isinstance(values, list):
            continue
        for index, example in enumerate(values):
            if not isinstance(example, dict) or not isinstance(example.get("text"), str):
                continue
            normalized = unicodedata.normalize("NFC", example["text"]).strip()
            groups[normalized].append((kind, index, example))
            order.append((kind, index, normalized))

    winners: dict[str, dict] = {}
    first_location: dict[str, tuple[str, int]] = {}
    for normalized, records in groups.items():
        first_location[normalized] = (records[0][0], records[0][1])
        winner = max((record[2] for record in records), key=_information_score)
        for _, _, record in records:
            if record is not winner:
                _merge_missing(winner, record)
        winners[normalized] = winner

    removed = sum(max(0, len(records) - 1) for records in groups.values())
    if not removed:
        return 0

    rebuilt: dict[str, list[object]] = {kind: [] for kind in examples}
    emitted: set[str] = set()
    for kind, index, normalized in order:
        if normalized not in emitted and first_location[normalized] == (kind, index):
            rebuilt[kind].append(winners[normalized])
            emitted.add(normalized)
    for kind, values in examples.items():
        if not isinstance(values, list):
            continue
        rebuilt[kind].extend(value for value in values if not (
            isinstance(value, dict) and isinstance(value.get("text"), str)
        ))
        values[:] = rebuilt[kind]
    return removed


def _extend_unique(target: list, additions: list) -> None:
    seen = {_json_key(value) for value in target}
    for value in additions:
        key = _json_key(value)
        if key not in seen:
            target.append(value)
            seen.add(key)


def _record_score(item: dict) -> tuple[int, int, int, int, str]:
    definitions = item.get("definitions") if isinstance(item.get("definitions"), list) else []
    glosses = sum(bool(d.get("gloss")) for d in definitions if isinstance(d, dict))
    examples = sum(len(values) for d in definitions if isinstance(d, dict)
                   for block in [d.get("examples")] if isinstance(block, dict)
                   for values in block.values() if isinstance(values, list))
    relations = item.get("relations")
    relation_count = sum(len(v) for v in relations.values() if isinstance(v, list)) \
        if isinstance(relations, dict) else 0
    meta = item.get("meta")
    updated = meta.get("updated_at", "") if isinstance(meta, dict) else ""
    return glosses, len(definitions), examples, relation_count, str(updated)


def _merge_duplicate(base: dict, other: dict) -> dict:
    """同一 UUID stale copy を index ではなく gloss + register 単位で統合する。"""
    for container_name, list_name in (("reading", "alternatives"), ("grammar", "pos")):
        left, right = base.get(container_name), other.get(container_name)
        if isinstance(left, dict) and isinstance(right, dict):
            a, b = left.get(list_name), right.get(list_name)
            if isinstance(a, list) and isinstance(b, list):
                _extend_unique(a, b)
    left_rel, right_rel = base.get("relations"), other.get("relations")
    if isinstance(left_rel, dict) and isinstance(right_rel, dict):
        for name, values in right_rel.items():
            if isinstance(values, list):
                target = left_rel.setdefault(name, [])
                if isinstance(target, list):
                    _extend_unique(target, values)
    left_meta, right_meta = base.get("meta"), other.get("meta")
    if isinstance(left_meta, dict) and isinstance(right_meta, dict):
        _merge_missing(left_meta, right_meta)
        left_freq, right_freq = left_meta.get("frequencies"), right_meta.get("frequencies")
        if isinstance(left_freq, dict) and isinstance(right_freq, dict):
            for source, value in right_freq.items():
                if _is_number(value) and (source not in left_freq or value > left_freq[source]):
                    left_freq[source] = value
    left_defs, right_defs = base.get("definitions"), other.get("definitions")
    if isinstance(left_defs, list) and isinstance(right_defs, list):
        by_sense = {(d.get("gloss"), d.get("register")): d for d in left_defs if isinstance(d, dict)}
        for definition in right_defs:
            if not isinstance(definition, dict):
                continue
            key = (definition.get("gloss"), definition.get("register"))
            target = by_sense.get(key)
            if target is None:
                left_defs.append(definition)
                by_sense[key] = definition
                continue
            target_examples, other_examples = target.get("examples"), definition.get("examples")
            if isinstance(target_examples, dict) and isinstance(other_examples, dict):
                for kind, values in other_examples.items():
                    if isinstance(values, list):
                        destination = target_examples.setdefault(kind, [])
                        if isinstance(destination, list):
                            _extend_unique(destination, values)
            _merge_missing(target, definition)
        for index, definition in enumerate(left_defs, 1):
            if isinstance(definition, dict):
                definition["index"] = index
                _deduplicate_examples(definition)
    return base


def _deduplicate_rows(rows: list[object]) -> tuple[list[object], int]:
    groups: dict[str, list[dict]] = defaultdict(list)
    order: list[tuple[str | None, object]] = []
    for row in rows:
        uid = row.get("uuid") if isinstance(row, dict) else None
        if not isinstance(uid, str):
            order.append((None, row))
            continue
        if uid not in groups:
            order.append((uid, row))
        groups[uid].append(row)
    removed = sum(max(0, len(group) - 1) for group in groups.values())
    if not removed:
        return rows, 0
    merged: dict[str, dict] = {}
    for uid, group in groups.items():
        ranked = sorted(group, key=_record_score, reverse=True)
        base = ranked[0]
        for other in ranked[1:]:
            _merge_duplicate(base, other)
        merged[uid] = base
    return [merged[key] if key is not None else value for key, value in order], removed


def _safe_fix_item(item: dict, scope: str) -> tuple[int, int]:
    changed = 0
    examples_removed = 0
    reading = item.get("reading")
    if isinstance(reading, dict) and isinstance(reading.get("alternatives"), list):
        changed += _dedupe_list(reading["alternatives"])
        primary = reading.get("primary")
        if isinstance(primary, str):
            primary_key = unicodedata.normalize("NFC", primary)
            kept = [value for value in reading["alternatives"] if not (
                isinstance(value, str) and unicodedata.normalize("NFC", value) == primary_key
            )]
            changed += len(reading["alternatives"]) - len(kept)
            reading["alternatives"][:] = kept
    grammar = item.get("grammar")
    if isinstance(grammar, dict) and isinstance(grammar.get("pos"), list):
        changed += _dedupe_list(grammar["pos"])
    relations = item.get("relations")
    if isinstance(relations, dict):
        for values in relations.values():
            if isinstance(values, list):
                changed += _dedupe_list(values)
    meta = item.get("meta")
    if scope == "pending" and not isinstance(meta, dict):
        meta = {}
        item["meta"] = meta
        changed += 1
    if isinstance(meta, dict):
        if scope == "pending" and meta.get("needs_reading") is not True:
            meta["needs_reading"] = True
            changed += 1
        variants = meta.get("variant_writings")
        if isinstance(variants, list):
            changed += _dedupe_list(variants)
    definitions = item.get("definitions")
    if isinstance(definitions, list):
        for index, definition in enumerate(definitions, 1):
            if not isinstance(definition, dict):
                continue
            if definition.get("index") != index:
                definition["index"] = index
                changed += 1
            removed = _deduplicate_examples(definition)
            examples_removed += removed
            changed += removed
    return changed, examples_removed


def _check_string_array(value: object, field_name: str, path: Path, scope: str,
                        result: AuditResult, entry: str | None) -> list[str] | None:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        _add_issue(result, "error", f"schema.{field_name}", scope, path,
                   f"{field_name} must be an array of strings", entry)
        return None
    seen: set[str] = set()
    duplicates: set[str] = set()
    for item in value:
        marker = unicodedata.normalize("NFC", item)
        if marker in seen:
            duplicates.add(item)
        seen.add(marker)
    if duplicates:
        _add_issue(result, "error", f"duplicate.{field_name}", scope, path,
                   f"{field_name} contains duplicate values: {sorted(duplicates)!r}", entry)
    return value


def _check_examples(definition: dict, path: Path, scope: str,
                    result: AuditResult, entry: str | None) -> None:
    examples = definition.get("examples")
    if not isinstance(examples, dict):
        _add_issue(result, "error", "schema.examples", scope, path,
                   "definitions[*].examples must be an object", entry)
        return
    normalized_seen: dict[str, str] = {}
    standard_count = 0
    for kind, values in examples.items():
        if not isinstance(kind, str) or not isinstance(values, list):
            _add_issue(result, "error", "schema.example_collection", scope, path,
                       "example collections must be named arrays", entry)
            continue
        if kind == "standard":
            standard_count = len(values)
        for example in values:
            if not isinstance(example, dict):
                _add_issue(result, "error", "schema.example_object", scope, path,
                           "every example must be an object", entry)
                continue
            text = example.get("text")
            if not isinstance(text, str) or not text.strip():
                _add_issue(result, "error", "schema.example_text", scope, path,
                           "every example text must be a non-empty string", entry)
                continue
            normalized = unicodedata.normalize("NFC", text).strip()
            previous_kind = normalized_seen.get(normalized)
            if previous_kind is not None:
                _add_issue(result, "error", "duplicate.example_text", scope, path,
                           f"normalized example text is duplicated ({previous_kind}, {kind})", entry)
            else:
                normalized_seen[normalized] = kind
    if standard_count == 0:
        _add_issue(result, "warning", "content.no_standard_example", scope, path,
                   "definition has no standard example", entry)


def _known_pos(value: str) -> bool:
    if value in KNOWN_POS:
        return True
    return any(value.startswith(prefix) for prefix in (
        "名詞-", "動詞-", "形容詞-", "副詞-", "助詞-", "助動詞-",
        "接頭辞", "接尾辞", "補助", "感動詞", "接続詞", "連体詞", "数詞", "コピュラ",
    ))


def _check_entry(item: object, path: Path, scope: str, data_root: Path,
                 result: AuditResult,
                 relation_refs: list[tuple[Path, str, str, str]]) -> tuple[bool, str, str]:
    if not isinstance(item, dict):
        _add_issue(result, "error", "schema.entry_object", scope, path,
                   "top-level array members must be objects")
        return False, "", ""
    entry_value = item.get("entry")
    entry = entry_value if isinstance(entry_value, str) else ""
    _check_identifier(entry_value, "entry", path, scope, result, entry or None)
    if len(entry) > LONG_ENTRY_LENGTH:
        _add_issue(result, "warning", "content.long_entry", scope, path,
                   f"entry is unusually long ({len(entry)} characters)", entry or None)

    reading_block = item.get("reading")
    if not isinstance(reading_block, dict):
        _add_issue(result, "error", "schema.reading_object", scope, path,
                   "reading must be an object", entry or None)
        reading = ""
    else:
        reading_value = reading_block.get("primary")
        reading = reading_value if isinstance(reading_value, str) else ""
        _check_identifier(reading_value, "reading.primary", path, scope, result, entry or None)
        if len(reading) > LONG_READING_LENGTH:
            _add_issue(result, "warning", "content.long_reading", scope, path,
                       f"reading is unusually long ({len(reading)} characters)", entry or None)
        alternatives = _check_string_array(reading_block.get("alternatives", []),
                                           "reading.alternatives", path, scope,
                                           result, entry or None)
        if alternatives is not None:
            primary_key = unicodedata.normalize("NFC", reading)
            for alternative in alternatives:
                invalid = invalid_reading_chars(alternative) if alternative else [""]
                if invalid:
                    labels = ", ".join(unicode_label(char) for char in invalid if char)
                    _add_issue(result, "error", "reading.alternative_invalid_unicode", scope, path,
                               f"alternative reading is not valid Kana: {alternative!r} {labels}", entry or None)
                if unicodedata.normalize("NFC", alternative) == primary_key:
                    _add_issue(result, "error", "duplicate.reading_primary_alternative", scope, path,
                               "reading.alternatives contains reading.primary", entry or None)
        heteronym = reading_block.get("is_heteronym")
        if heteronym is not None and not isinstance(heteronym, bool):
            _add_issue(result, "error", "schema.reading.is_heteronym", scope, path,
                       "reading.is_heteronym must be a boolean", entry or None)

    invalid_primary = invalid_reading_chars(reading) if reading else []
    expected = expected_data_path(data_root, reading)
    should_quarantine = scope == "data" and (not is_valid_reading(reading) or expected is None)
    if scope == "data" and invalid_primary:
        labels = ", ".join(unicode_label(char) for char in invalid_primary)
        _add_issue(result, "error", "reading.invalid_unicode_script", scope, path,
                   f"reading must contain only Unicode Kana and approved marks; found {labels}", entry or None)

    uid = item.get("uuid")
    valid_uuid = False
    if isinstance(uid, str):
        try:
            parsed = uuid.UUID(uid)
            valid_uuid = str(parsed) == uid and parsed.version == 5
        except ValueError:
            pass
    if not valid_uuid:
        _add_issue(result, "error", "uuid.invalid_format", scope, path,
                   "uuid must be a canonical lowercase UUIDv5 string", entry or None)
    elif entry and reading:
        expected_uuid = compute_uuid_v5(entry, reading)
        if uid != expected_uuid:
            _add_issue(result, "error", "uuid.not_reproducible", scope, path,
                       f"uuid does not match UUIDv5 rule; expected {expected_uuid}", entry or None)

    grammar = item.get("grammar")
    if not isinstance(grammar, dict):
        _add_issue(result, "error", "schema.grammar", scope, path,
                   "grammar must be an object", entry or None)
    else:
        pos = _check_string_array(grammar.get("pos"), "grammar.pos", path, scope,
                                  result, entry or None)
        if pos is not None:
            for label in pos:
                if label == "未分類" or not _known_pos(label):
                    _add_issue(result, "warning", "content.unknown_pos", scope, path,
                               f"unknown or unclassified part of speech: {label!r}", entry or None)

    meta = item.get("meta")
    if not isinstance(meta, dict):
        _add_issue(result, "error", "schema.meta", scope, path,
                   "meta must be an object", entry or None)
        meta = {}
    needs_reading = meta.get("needs_reading")
    if needs_reading is not None and not isinstance(needs_reading, bool):
        _add_issue(result, "error", "schema.meta.needs_reading", scope, path,
                   "meta.needs_reading must be a boolean", entry or None)
    if scope == "data" and "needs_reading" in meta:
        _add_issue(result, "error", "reading.needs_reading_in_data", scope, path,
                   "formal data must not contain meta.needs_reading", entry or None)
        should_quarantine = True
    if scope == "pending" and needs_reading is not True:
        _add_issue(result, "error", "reading.pending_marker_missing", scope, path,
                   "pending entries must set meta.needs_reading=true", entry or None)
    needs_gloss = meta.get("needs_gloss")
    if needs_gloss is not None and not isinstance(needs_gloss, bool):
        _add_issue(result, "error", "schema.meta.needs_gloss", scope, path,
                   "meta.needs_gloss must be a boolean", entry or None)
    freq_rank = meta.get("freq_rank")
    if freq_rank is not None and (not isinstance(freq_rank, int) or isinstance(freq_rank, bool) or freq_rank < 1):
        _add_issue(result, "error", "schema.meta.freq_rank", scope, path,
                   "meta.freq_rank must be a positive integer", entry or None)
    frequencies = meta.get("frequencies")
    if frequencies is not None:
        if not isinstance(frequencies, dict):
            _add_issue(result, "error", "schema.meta.frequencies", scope, path,
                       "meta.frequencies must be an object", entry or None)
        else:
            for source, value in frequencies.items():
                if not isinstance(source, str) or not source or not isinstance(value, int) \
                        or isinstance(value, bool) or value < 0:
                    _add_issue(result, "error", "schema.meta.frequency_value", scope, path,
                               "frequency names must be non-empty strings and values non-negative integers",
                               entry or None)
    variants = meta.get("variant_writings")
    if variants is not None:
        _check_string_array(variants, "meta.variant_writings", path, scope,
                            result, entry or None)

    definitions = item.get("definitions")
    if not isinstance(definitions, list) or not definitions:
        _add_issue(result, "error", "schema.definitions", scope, path,
                   "definitions must be a non-empty array", entry or None)
    else:
        actual_indices = [d.get("index") if isinstance(d, dict) else None for d in definitions]
        if actual_indices != list(range(1, len(definitions) + 1)):
            _add_issue(result, "error", "definition.non_contiguous_index", scope, path,
                       "definition indices must be consecutive starting at 1", entry or None)
        for definition in definitions:
            if not isinstance(definition, dict):
                _add_issue(result, "error", "schema.definition_object", scope, path,
                           "definitions members must be objects", entry or None)
                continue
            gloss = definition.get("gloss")
            if not isinstance(gloss, str):
                _add_issue(result, "error", "schema.definition.gloss", scope, path,
                           "definition gloss must be a string", entry or None)
            elif needs_gloss is True and gloss != "":
                _add_issue(result, "error", "gloss.needs_gloss_inconsistent", scope, path,
                           "needs_gloss=true requires every gloss to remain empty", entry or None)
            elif needs_gloss is not True and gloss == "":
                _add_issue(result, "error", "gloss.missing_marker", scope, path,
                           "an empty gloss requires meta.needs_gloss=true", entry or None)
            _check_examples(definition, path, scope, result, entry or None)

    relations = item.get("relations")
    if not isinstance(relations, dict):
        _add_issue(result, "error", "schema.relations", scope, path,
                   "relations must be an object", entry or None)
    else:
        for relation_name, targets in relations.items():
            if not isinstance(relation_name, str):
                _add_issue(result, "error", "schema.relation_name", scope, path,
                           "relation names must be strings", entry or None)
                continue
            checked = _check_string_array(targets, f"relations.{relation_name}", path,
                                          scope, result, entry or None)
            if checked is None:
                continue
            for target in checked:
                if target == entry:
                    _add_issue(result, "error", "relation.self_reference", scope, path,
                               f"relations.{relation_name} contains the entry itself", entry or None)
                relation_refs.append((path, scope, entry, target))

    if scope == "data" and expected is not None and path != expected:
        _add_issue(result, "error", "path.reading_mismatch", scope, path,
                   f"reading {reading!r} belongs at {expected.relative_to(data_root)}", entry or None)
    return should_quarantine, entry, reading


def _merge_quarantine(path: Path, additions: list[dict]) -> None:
    existing: list[object] = []
    if path.exists():
        loaded = json.loads(path.read_text(encoding="utf-8"))
        existing = loaded if isinstance(loaded, list) else [loaded]
    existing.extend(additions)
    merged, _ = _deduplicate_rows(existing)
    _atomic_write_json(path, merged)


def _remove_empty_parents(root: Path) -> None:
    if not root.exists():
        return
    for directory in sorted((p for p in root.rglob("*") if p.is_dir()),
                            key=lambda value: len(value.parts), reverse=True):
        try:
            directory.rmdir()
        except OSError:
            pass


def _merge_cross_file_duplicates(groups: dict[str, set[Path]], data_root: Path,
                                 pending_root: Path) -> tuple[int, int]:
    removed = 0
    writes = 0
    for uid, paths in groups.items():
        records: list[dict] = []
        loaded_by_path: dict[Path, list[object]] = {}
        for path in sorted(paths):
            if not path.exists():
                continue
            loaded = json.loads(path.read_text(encoding="utf-8"))
            rows = loaded if isinstance(loaded, list) else [loaded]
            loaded_by_path[path] = rows
            records.extend(row for row in rows if isinstance(row, dict) and row.get("uuid") == uid)
        if len(records) < 2:
            continue
        ranked = sorted(records, key=_record_score, reverse=True)
        merged = ranked[0]
        for other in ranked[1:]:
            _merge_duplicate(merged, other)
        removed += len(records) - 1
        meta = merged.get("meta") if isinstance(merged.get("meta"), dict) else {}
        reading = (merged.get("reading") or {}).get("primary") \
            if isinstance(merged.get("reading"), dict) else ""
        if meta.get("needs_reading") is True or not is_valid_reading(reading):
            destination = pending_root / quarantine_relative_path(str(merged.get("entry", "")))
        else:
            destination = expected_data_path(data_root, reading)
        if destination is None:
            continue
        for path, rows in loaded_by_path.items():
            kept = [row for row in rows if not (isinstance(row, dict) and row.get("uuid") == uid)]
            if path == destination:
                kept.append(merged)
            if kept:
                _atomic_write_json(path, kept)
            else:
                path.unlink(missing_ok=True)
            writes += 1
        if destination not in loaded_by_path:
            _merge_quarantine(destination, [merged])
            writes += 1
    return removed, writes


def audit(data_root: Path, pending_root: Path | None = None, fix: bool = False,
          include_pending: bool = True) -> AuditResult:
    data_root = data_root.resolve()
    pending_root = (pending_root or DEFAULT_PENDING).resolve()
    result = AuditResult()
    seen_uuid: dict[str, Path] = {}
    duplicate_groups: dict[str, set[Path]] = defaultdict(set)
    seen_entry_reading: dict[tuple[str, str], Path] = {}
    seen_paths: dict[tuple[str, str], Path] = {}
    all_entries: set[str] = set()
    relation_refs: list[tuple[Path, str, str, str]] = []
    roots = [("data", data_root)]
    if include_pending:
        roots.append(("pending", pending_root))

    for scope, root in roots:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.json")):
            result.files += 1
            result.scope_files[scope] += 1
            _check_path(path, root, scope, result, seen_paths)
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                _add_issue(result, "error", "file.invalid_json", scope, path, str(exc))
                continue
            if not isinstance(loaded, list):
                _add_issue(result, "error", "file.root_not_array", scope, path,
                           "top-level JSON value must be an array")
                rows: list[object] = [loaded]
            else:
                rows = loaded
            if not rows:
                _add_issue(result, "error", "file.empty", scope, path,
                           "JSON entry array must not be empty")
                continue

            changed = False
            if fix:
                rows, local_removed = _deduplicate_rows(rows)
                if local_removed:
                    result.deduplicated += local_removed
                    changed = True
                for item in rows:
                    if isinstance(item, dict):
                        fixes, example_removed = _safe_fix_item(item, scope)
                        changed = changed or bool(fixes)
                        result.examples_deduplicated += example_removed

            kept: list[object] = []
            quarantines: dict[Path, list[dict]] = defaultdict(list)
            for item in rows:
                result.entries += 1
                result.scope_entries[scope] += 1
                should_quarantine, entry, reading = _check_entry(
                    item, path, scope, data_root, result, relation_refs
                )
                actual_path = path
                if fix and should_quarantine and isinstance(item, dict):
                    meta = item.setdefault("meta", {})
                    if isinstance(meta, dict):
                        meta["needs_reading"] = True
                    actual_path = pending_root / quarantine_relative_path(entry, path)
                    quarantines[actual_path].append(item)
                    result.quarantined += 1
                    changed = True
                else:
                    kept.append(item)

                if entry:
                    all_entries.add(entry)
                if isinstance(item, dict):
                    uid = item.get("uuid")
                    if isinstance(uid, str):
                        previous = seen_uuid.get(uid)
                        if previous is not None:
                            _add_issue(result, "error", "duplicate.uuid", scope, path,
                                       f"uuid also occurs in {previous}", entry or None)
                            duplicate_groups[uid].update((previous, actual_path))
                        else:
                            seen_uuid[uid] = actual_path
                    key = (entry, reading)
                    if entry and reading:
                        previous = seen_entry_reading.get(key)
                        if previous is not None:
                            _add_issue(result, "error", "duplicate.entry_reading", scope, path,
                                       f"entry/reading pair also occurs in {previous}", entry)
                        else:
                            seen_entry_reading[key] = actual_path

            if fix and changed:
                for target, additions in quarantines.items():
                    _merge_quarantine(target, additions)
                if kept:
                    _atomic_write_json(path, kept)
                else:
                    path.unlink(missing_ok=True)
                result.changed_files += 1

    for path, scope, entry, target in relation_refs:
        if target and target not in all_entries:
            _add_issue(result, "warning", "content.missing_relation_target", scope, path,
                       f"relation target does not exist: {target!r}", entry or None)
    if fix and duplicate_groups:
        removed, writes = _merge_cross_file_duplicates(duplicate_groups, data_root, pending_root)
        result.deduplicated += removed
        result.changed_files += writes
    if fix:
        _remove_empty_parents(data_root)
        _remove_empty_parents(pending_root)
    return result


def _print_human(result: AuditResult, limit: int) -> None:
    for issue in result.issues[:limit]:
        suffix = f" entry={issue.entry!r}" if issue.entry is not None else ""
        print(f"{issue.severity.upper()} [{issue.code}] ({issue.scope}) "
              f"{issue.path}: {issue.message}{suffix}")
    hidden = len(result.issues) - min(len(result.issues), limit)
    if hidden:
        print(f"... {hidden:,} more issues omitted (use --limit to change output)")
    print()
    print(f"Unicode UCD: {UNICODE_VERSION}")
    print(f"Scanned: {result.files:,} files / {result.entries:,} entries")
    print(f"Errors: {result.error_count:,}; warnings: {result.warning_count:,}")
    for code, count in result.issue_counts.most_common():
        print(f"  {code}: {count:,}")
    if result.quarantined:
        print(f"Quarantined: {result.quarantined:,} entries")
    if result.deduplicated:
        print(f"Deduplicated: {result.deduplicated:,} stale UUID records")
    if result.examples_deduplicated:
        print(f"Examples deduplicated: {result.examples_deduplicated:,}")
    if result.changed_files:
        print(f"Changed files: {result.changed_files:,}")


def _json_report(result: AuditResult) -> dict:
    grouped: dict[str, dict[str, dict[str, list[dict[str, str]]]]] = {}
    for issue in result.issues:
        grouped.setdefault(issue.severity, {}).setdefault(issue.code, {}).setdefault(
            issue.scope, []).append(issue.as_dict())
    return {
        "unicode_version": UNICODE_VERSION,
        "summary": {
            "files": result.files,
            "entries": result.entries,
            "by_scope": {scope: {"files": result.scope_files[scope],
                                 "entries": result.scope_entries[scope]}
                         for scope in sorted(result.scope_files)},
            "by_severity": dict(sorted(result.severity_counts.items())),
            "by_code": dict(sorted(result.issue_counts.items())),
            "quarantined": result.quarantined,
            "changed_files": result.changed_files,
            "deduplicated": result.deduplicated,
            "examples_deduplicated": result.examples_deduplicated,
        },
        "issues": grouped,
    }


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--pending-dir", type=Path, default=DEFAULT_PENDING)
    parser.add_argument("--formal-only", action="store_true",
                        help="只檢查 data/（供 SQLite 建置守門使用）")
    parser.add_argument("--fix", action="store_true",
                        help="執行隔離、標記、陣列/例句去重與 definition 重編號；不改 UUID")
    parser.add_argument("--json", action="store_true", help="輸出按 severity/code/scope 分組的 JSON")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args(list(argv) if argv is not None else None)
    data_root = args.data_dir.resolve()
    pending_root = args.pending_dir.resolve()
    if data_root == pending_root or data_root in pending_root.parents:
        parser.error("--pending-dir must be outside --data-dir")
    result = audit(data_root, pending_root, args.fix, include_pending=not args.formal_only)
    if args.json:
        print(json.dumps(_json_report(result), ensure_ascii=False, indent=2))
    else:
        _print_human(result, max(args.limit, 0))
    return 0 if args.fix else (1 if result.error_count else 0)


if __name__ == "__main__":
    sys.exit(main())
