#!/usr/bin/env python3
"""
既存の data/**/*.json に「出典別の総出現回数」(meta.frequencies) を
in-place で注入するバックフィルスクリプト。

build_dictionary.py をフル再実行すると後付けの enrich（AI 生成例文など）が
失われるため、頻度フィールドだけを既存 JSON に追記する用途で使う。

頻度テーブルの供給は 2 通り:
  1) --freq-table aozora_freq.json.gz を渡す（事前生成済み・軽量。Aozora/Sudachi 不要）
  2) 省略時は build_dictionary.build_corpus_frequency で青空文庫を解析して生成
     （リポジトリ clone 済み aozora ディレクトリと sudachipy が必要・重い）

使用例:
    # 重い解析を別環境で済ませてテーブルだけ持ち込む場合
    python3 script/backfill_frequency.py --freq-table /tmp/aozora_freq.json.gz

    # その場で青空文庫から生成して注入する場合
    python3 script/backfill_frequency.py --aozora-dir /tmp/aozorabunko_text --workers 8

    # 中身を変えずに件数だけ確認
    python3 script/backfill_frequency.py --freq-table /tmp/aozora_freq.json.gz --dry-run
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

# build_dictionary.py を同じディレクトリからインポート
sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_dictionary as bd  # noqa: E402

_REPO_ROOT = Path(__file__).resolve().parents[1]

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("backfill_frequency")


def load_freq_table(path: Path) -> dict[str, int]:
    """事前生成済みの頻度テーブル（gzip JSON: {word: count}）を読み込む。"""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as f:
        data = json.load(f)
    return {str(k): int(v) for k, v in data.items()}


def backfill_file(
    file_path: Path,
    freq_counts: dict[str, int],
    source: str,
    dry_run: bool,
) -> tuple[int, int]:
    """
    1 ファイル（エントリ配列）の meta.frequencies[source] を更新する。
    Returns: (このファイルで頻度を持つエントリ数, 値が変化したエントリ数)
    """
    try:
        with file_path.open(encoding="utf-8") as f:
            entries = json.load(f)
    except Exception as exc:
        log.warning("読み込み失敗（スキップ）: %s (%s)", file_path, exc)
        return 0, 0

    if not isinstance(entries, list):
        return 0, 0

    have = 0
    changed = 0
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        word = entry.get("entry")
        count = freq_counts.get(word, 0) if word else 0
        meta = entry.setdefault("meta", {})
        freqs = meta.get("frequencies")
        if not isinstance(freqs, dict):
            freqs = {}

        old = freqs.get(source)
        if count > 0:
            if old != count:
                freqs[source] = count
                changed += 1
            have += 1
        else:
            # 0 件は疎フィールド維持のためキーを除去
            if source in freqs:
                del freqs[source]
                changed += 1

        if freqs:
            meta["frequencies"] = freqs
        elif "frequencies" in meta:
            del meta["frequencies"]

    if changed and not dry_run:
        # 既存 data/ JSON の形式（indent=2・末尾改行なし）に厳密一致させ、
        # 実行時に整形差分が出ないようにする
        tmp_path = file_path.with_suffix(".json.tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(entries, f, ensure_ascii=False, indent=2)
        tmp_path.rename(file_path)

    return have, changed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="既存 data/ JSON に meta.frequencies を in-place 注入する",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--data-dir",   default=_REPO_ROOT / "data", type=Path,
                        help="辞典 JSON のルートディレクトリ")
    parser.add_argument("--source",     default="aozora",
                        help="頻度の出典名（meta.frequencies のキー）")
    parser.add_argument("--freq-table", default=None, type=Path,
                        help="事前生成済み頻度テーブル（gzip JSON）。指定時はこれを使い再解析しない")
    parser.add_argument("--aozora-dir", default=Path("/tmp/aozorabunko_text"), type=Path,
                        help="青空文庫テキストのルート（--freq-table 未指定時に解析）")
    parser.add_argument("--tmp-dir",    default=Path("/tmp"), type=Path,
                        help="チェックポイント／生成テーブルの保存先")
    parser.add_argument("--workers",    default=None, type=int,
                        help="解析の並列数（省略時: CPU コア数）")
    parser.add_argument("--resume",     action="store_true",
                        help="頻度カウントを前回チェックポイントから再開")
    parser.add_argument("--dry-run",    action="store_true",
                        help="ファイルを書き換えず件数のみ表示")
    args = parser.parse_args()

    start = time.perf_counter()

    # ── 頻度テーブルの用意 ─────────────────────────────────
    if args.freq_table:
        if not args.freq_table.exists():
            log.error("頻度テーブルが見つかりません: %s", args.freq_table)
            sys.exit(1)
        bd.Progress.group(f"頻度テーブル読み込み: {args.freq_table}")
        freq_counts = load_freq_table(args.freq_table)
        bd.Progress.ok(f"{len(freq_counts):,} 語")
        bd.Progress.endgroup()
    else:
        n_workers = args.workers or (os.cpu_count() or 4)
        freq_counts = bd.build_corpus_frequency(
            args.aozora_dir,
            n_workers=n_workers,
            checkpoint_path=args.tmp_dir / bd._FREQ_CHECKPOINT_NAME,
            table_path=args.tmp_dir / bd._FREQ_TABLE_NAME,
            resume=args.resume,
        )

    if not freq_counts:
        log.error("頻度テーブルが空です。中止します。")
        sys.exit(1)

    # ── data/ への注入 ─────────────────────────────────────
    bd.Progress.group(f"data/ への {args.source} 頻度注入  (dry_run={args.dry_run})")
    files = sorted(args.data_dir.rglob("*.json"))
    bd.Progress.step(f"対象ファイル: {len(files):,}")

    total_files_touched = 0
    total_entries_have  = 0
    total_changed       = 0
    last_report = time.perf_counter()

    for i, fp in enumerate(files, 1):
        have, changed = backfill_file(fp, freq_counts, args.source, args.dry_run)
        total_entries_have += have
        total_changed      += changed
        if changed:
            total_files_touched += 1

        now = time.perf_counter()
        if now - last_report >= 3.0:
            last_report = now
            bd.Progress.bar_line(
                i, len(files),
                f"{i:,}/{len(files):,} files  "
                f"{total_entries_have:,} entries with freq  "
                f"{total_changed:,} changed",
            )

    bd.Progress.ok(
        f"{total_files_touched:,} ファイル更新  "
        f"{total_entries_have:,} エントリに {args.source} 頻度  "
        f"{total_changed:,} 件変更"
        + ("（dry-run: 未書き込み）" if args.dry_run else "")
    )
    bd.Progress.endgroup()
    log.info("完了: %.1fs", time.perf_counter() - start)


if __name__ == "__main__":
    main()
