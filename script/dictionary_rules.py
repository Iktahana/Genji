#!/usr/bin/env python3
"""Genji の語彙データに適用する決定的な Unicode / 配置規則。

字種の判定は Python が同梱する Unicode Character Database (UCD) の
General Category と文字名を使う。目視や ``str.isalpha()`` には依存しない。
"""

from __future__ import annotations

import unicodedata
import uuid
from pathlib import Path


UNICODE_VERSION = unicodedata.unidata_version
UUID_NAMESPACE = uuid.UUID(bytes=b"\x00" * 16)

# 読みの内部で意味を持つ、日本語固有の記号。
_READING_MARKS = frozenset({
    "\u309d",  # HIRAGANA ITERATION MARK
    "\u309e",  # HIRAGANA VOICED ITERATION MARK
    "\u30fb",  # KATAKANA MIDDLE DOT（複合外来語の区切り）
    "\u30fc",  # KATAKANA-HIRAGANA PROLONGED SOUND MARK
    "\u30fd",  # KATAKANA ITERATION MARK
    "\u30fe",  # KATAKANA VOICED ITERATION MARK
    "\u3001",  # IDEOGRAPHIC COMMA（句・ことわざの区切り）
    "\u3002",  # IDEOGRAPHIC FULL STOP
    "\u301c",  # WAVE DASH（擬音の延伸）
    "=",       # FULLWIDTH EQUALS SIGN の NFKC 形（固有名の区切り）
    "\u309f",  # HIRAGANA DIGRAPH YORI（歴史的仮名）
})

_COMBINING_READING_MARKS = frozenset({"\u3099", "\u309a"})

_SMALL_TO_LARGE = {
    "ぁ": "あ", "ぃ": "い", "ぅ": "う", "ぇ": "え", "ぉ": "お",
    "っ": "つ", "ゃ": "や", "ゅ": "ゆ", "ょ": "よ", "ゎ": "わ",
    "ァ": "ア", "ィ": "イ", "ゥ": "ウ", "ェ": "エ", "ォ": "オ",
    "ッ": "ツ", "ャ": "ヤ", "ュ": "ユ", "ョ": "ヨ", "ヮ": "ワ",
}

# データ識別子に入ってはならない Unicode General Category。
# Cc/Cf: 制御・不可視書式、Cs: surrogate、Co: private use、Cn: 未割当、
# Zl/Zp: 行・段落区切り。通常スペース (Zs) は語中でのみ別途許可できる。
FORBIDDEN_IDENTIFIER_CATEGORIES = frozenset({"Cc", "Cf", "Cs", "Co", "Cn", "Zl", "Zp"})


def unicode_label(char: str) -> str:
    """監査メッセージ用の安定した Unicode 表記を返す。"""
    return f"U+{ord(char):04X} {unicodedata.category(char)} {unicodedata.name(char, 'UNNAMED')}"


def forbidden_identifier_chars(value: str) -> list[str]:
    """識別子で禁止する不可視・未割当文字を、出現順かつ重複なしで返す。"""
    seen: set[str] = set()
    result: list[str] = []
    for char in value:
        if unicodedata.category(char) in FORBIDDEN_IDENTIFIER_CATEGORIES and char not in seen:
            seen.add(char)
            result.append(char)
    return result


def _is_kana_letter(char: str) -> bool:
    """UCD の文字名に基づき、平仮名・片仮名の文字か判定する。"""
    # 半角片仮名も NFKC 後に通常の片仮名として判定する。
    normalized = unicodedata.normalize("NFKC", char)
    if len(normalized) != 1:
        return False
    name = unicodedata.name(normalized, "")
    return name.startswith("HIRAGANA LETTER ") or name.startswith("KATAKANA LETTER ")


def invalid_reading_chars(reading: str) -> list[str]:
    """仮名読みとして使えない文字を、出現順かつ重複なしで返す。

    読みは平仮名・片仮名（Unicode の Kana 系文字）に加え、長音・反復記号・
    結合濁点と辞書表現に必要な日本語句読点だけを許可する。漢字、Latin、数字、
    単独の spacing 濁点 U+309B/U+309C は読みとして扱わない。
    """
    seen: set[str] = set()
    invalid: list[str] = []
    previous_was_kana = False
    for char in reading:
        normalized = unicodedata.normalize("NFKC", char)
        is_kana = _is_kana_letter(char)
        is_combining = len(normalized) == 1 and normalized in _COMBINING_READING_MARKS
        # 結合濁点・半濁点は仮名の直後だけに置ける。spacing 版の
        # U+309B/U+309C は NFKC 時に空白を生むため、引き続き拒否する。
        ok = is_kana or char == "\u309f" or (
            len(normalized) == 1 and normalized in _READING_MARKS
        )
        if is_combining:
            ok = previous_was_kana
        if not ok and char not in seen:
            seen.add(char)
            invalid.append(char)
        previous_was_kana = is_kana
    return invalid


def is_valid_reading(reading: object) -> bool:
    """非空かつ、Unicode 上の仮名読みとして完結している場合だけ True。"""
    return isinstance(reading, str) and bool(reading) and not invalid_reading_chars(reading)


def reading_to_file_key(reading: str) -> str:
    """読みを NFKC 全角化し、平仮名だけ片仮名へ変換する。"""
    reading = unicodedata.normalize("NFKC", reading)
    return "".join(
        chr(ord(char) + 0x60) if "\u3041" <= char <= "\u3096" else char
        for char in reading
    )


def reading_bucket(reading: str) -> str | None:
    """読みから正式データの分桶名を返す。無効な読みなら None。"""
    if not is_valid_reading(reading):
        return None
    first = reading_to_file_key(reading)[0]
    if "\u30a1" <= first <= "\u30f6":
        first = chr(ord(first) - 0x60)
    first = _SMALL_TO_LARGE.get(first, first)
    # 先頭に許す非文字は既存辞書で使われる長音・仮名反復記号だけ。
    if _is_kana_letter(first) or first in {"ー", "ゝ", "ゞ", "ヽ", "ヾ"}:
        return first
    return None


def expected_data_path(data_root: Path, reading: str) -> Path | None:
    """読みが有効なら、そのエントリが属すべき JSON パスを返す。"""
    bucket = reading_bucket(reading)
    if bucket is None:
        return None
    return data_root / bucket / f"{reading_to_file_key(reading)}.json"


def quarantine_relative_path(entry: str, source_path: Path | None = None) -> Path:
    """未確定読みを衝突しにくい Unicode コードポイント単位のパスへ分ける。"""
    seed = entry or (source_path.stem if source_path else "UNNAMED")
    first = seed[0] if seed else "\0"
    page_start = ord(first) & ~0xFF
    page = f"U+{page_start:04X}-U+{page_start + 0xFF:04X}"
    filename = source_path.name if source_path else f"{seed}.json"
    return Path(page) / filename


def compute_uuid_v5(entry: str, reading: str) -> str:
    """辞書の公開識別子規則に従う UUIDv5 を返す。"""
    return str(uuid.uuid5(UUID_NAMESPACE, f"{entry}:{reading}"))
