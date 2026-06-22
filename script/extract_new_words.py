#!/usr/bin/env python3
# ──────────────────────────────────────────────────────────
# extract_new_words.py
#
# 青空文庫（aozorahack/aozorabunko_text）を素材庫として形態素解析し、
# 幻辭（genji.db）にまだ存在しない語を発見してリポジトリ直下の
# new_words.txt に「語<TAB>出現回数」形式・頻度降順で書き出す。
#
# 新語条（エントリ）の自動生成は後工程。本スクリプトは新語候補リストの
# 抽出のみを担う。
#
# 設計:
#   - 形態素解析・本文抽出・チェックポイント等の重い部品は
#     build_dictionary.py の関数群を import して再利用する。
#   - 固有名詞（名詞-固有名詞）は既定で除外（--include-proper-nouns で含める）。
#   - ProcessPoolExecutor による並行処理、/tmp のチェックポイントで断点継続。
#   - 素材庫は /tmp/aozorabunko_text に clone してキャッシュ。
#
# 使い方:
#   python3 script/extract_new_words.py --workers 8
#   python3 script/extract_new_words.py --resume          # 中断から再開
#   python3 script/extract_new_words.py --dry-run --verbose
# ──────────────────────────────────────────────────────────
from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

SCRIPT_DIR   = Path(__file__).resolve().parent          # script/
PROJECT_ROOT = SCRIPT_DIR.parent                        # Genji/

# build_dictionary.py のヘルパーを再利用するため import 可能にする
sys.path.insert(0, str(SCRIPT_DIR))
import build_dictionary as bd  # noqa: E402

log = logging.getLogger(__name__)

# 青空文庫リポジトリ
_AOZORA_URL  = "https://github.com/aozorahack/aozorabunko_text"
_AOZORA_NAME = "aozorabunko_text"

# /tmp 配下のキャッシュ名（build_dictionary の頻度テーブルとは別フィルタなので衝突させない）
_CKPT_NAME  = "aozora_newword_freq_checkpoint.json.gz"
_TABLE_NAME = "aozora_newword_freq.json.gz"


# ──────────────────────────────────────────────────────────
# ワーカー（ProcessPoolExecutor の制約上モジュールレベル必須）
# ──────────────────────────────────────────────────────────
def _newword_freq_worker(
    file_paths: list[str],
    exclude_proper: bool,
) -> tuple[dict[str, int], int, int]:
    """
    build_dictionary._freq_worker とほぼ同一だが、固有名詞除外オプション付き。
    Returns: (partial_counts, processed_files, counted_tokens)
    """
    from sudachipy import SplitMode  # 遅延 import

    tok = bd._get_freq_tokenizer()
    counts: dict[str, int] = defaultdict(int)
    files_done = 0
    tokens_done = 0

    for path_str in file_paths:
        try:
            txt_file = Path(path_str)
            try:
                content = txt_file.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                content = txt_file.read_text(encoding="shift_jis", errors="replace")

            body = bd._extract_aozora_body_text(content.splitlines())
            for raw_line in body.split("\n"):
                line = raw_line.strip()
                if not line:
                    continue
                for chunk in bd._safe_chunks(line):
                    try:
                        morphemes = tok.tokenize(chunk, SplitMode.C)
                    except Exception:
                        continue  # 解析不能な行はスキップ
                    for m in morphemes:
                        pos = m.part_of_speech()
                        if pos[0] in bd._FREQ_SKIP_POS:
                            continue
                        # 固有名詞（人名・地名・組織名・一般）を除外
                        if exclude_proper and len(pos) > 1 and pos[1] == "固有名詞":
                            continue
                        lemma = m.dictionary_form()
                        if not lemma or not bd.is_japanese_text(lemma):
                            continue
                        counts[lemma] += 1
                        tokens_done += 1
            files_done += 1
        except Exception:
            files_done += 1  # スキップしてもカウント
            continue

    return dict(counts), files_done, tokens_done


# ──────────────────────────────────────────────────────────
# コーパス取得（clone / pull）
# ──────────────────────────────────────────────────────────
def ensure_aozora(tmp_dir: Path) -> Path:
    """/tmp/aozorabunko_text を確保する（無ければ shallow clone、あれば pull）。"""
    dest = tmp_dir / _AOZORA_NAME
    git_dir = dest / ".git"
    if git_dir.exists():
        bd.Progress.step(f"青空文庫 pull: {dest}")
        try:
            subprocess.run(
                ["git", "-C", str(dest), "pull", "--ff-only"],
                check=True, capture_output=True, timeout=300,
            )
        except subprocess.SubprocessError as e:
            bd.Progress.warn(f"pull 失敗（既存キャッシュを使用）: {e}")
    else:
        bd.Progress.step(f"青空文庫 clone（--depth=1）: {dest}")
        subprocess.run(
            ["git", "clone", "--depth=1", _AOZORA_URL, str(dest)],
            check=True, timeout=900,
        )
    return dest


# ──────────────────────────────────────────────────────────
# 頻度テーブル構築（並行・断点継続）
# ──────────────────────────────────────────────────────────
def build_freq_table(
    aozora_dir: Path,
    n_workers: int,
    checkpoint_path: Path,
    table_path: Optional[Path],
    resume: bool,
    exclude_proper: bool,
) -> dict[str, int]:
    """
    青空文庫テキストを形態素解析し、{dictionary_form: 総出現回数} を構築する。
    build_dictionary.build_corpus_frequency と同じオーケストレーション構造。
    """
    if not aozora_dir.exists():
        bd.Progress.warn(f"青空文庫ディレクトリなし: {aozora_dir}")
        return {}

    # Sudachi の早期チェック
    try:
        bd._get_freq_tokenizer()
    except Exception as exc:
        bd.Progress.warn(f"Sudachi が利用できません: {exc}")
        bd.Progress.warn("`pip install -r requirements.txt` で sudachipy / sudachidict_core を導入してください")
        return {}

    bd.Progress.group(f"形態素解析 │ 頻度カウント（workers={n_workers}, 固有名詞除外={exclude_proper}）")

    counts: dict[str, int] = defaultdict(int)
    already_processed: set[str] = set()
    if resume and checkpoint_path.exists():
        preloaded, already_processed = bd._load_freq_checkpoint(checkpoint_path)
        for w, c in preloaded.items():
            counts[w] += c
        bd.Progress.step(f"チェックポイント復元: {len(already_processed):,} ファイル  {len(counts):,} 語")

    all_txt_files = sorted(aozora_dir.rglob("*.txt"))
    txt_files = [f for f in all_txt_files if str(f) not in already_processed]
    total_files     = len(all_txt_files)
    remaining_files = len(txt_files)
    bd.Progress.step(f"対象テキスト: {remaining_files:,} / {total_files:,} ファイル  workers: {n_workers}")

    if remaining_files == 0:
        bd.Progress.ok(f"処理対象なし（全 {total_files:,} ファイル処理済み）  {len(counts):,} 語")
        bd.Progress.endgroup()
        return dict(counts)

    n_chunks   = max(n_workers, n_workers * 8)
    chunk_size = max(1, (remaining_files + n_chunks - 1) // n_chunks)
    chunks: list[list[str]] = [
        [str(f) for f in txt_files[i: i + chunk_size]]
        for i in range(0, remaining_files, chunk_size)
    ]

    done_files      = len(already_processed)
    done_tokens     = 0
    phase_t         = time.perf_counter()
    last_report     = phase_t
    last_checkpoint = phase_t
    processed_in_run: set[str] = set()

    shutdown_evt = threading.Event()
    _orig_sigint = signal.getsignal(signal.SIGINT)

    def _sigint_handler(sig: int, frame: object) -> None:
        print("\n[SIGINT] 終了要求を受信しました。チェックポイント保存後に終了します...", flush=True)
        shutdown_evt.set()

    signal.signal(signal.SIGINT, _sigint_handler)

    try:
        with ProcessPoolExecutor(max_workers=n_workers) as pool:
            futures_map = {
                pool.submit(_newword_freq_worker, chunk, exclude_proper): chunk
                for chunk in chunks
            }
            for fut in as_completed(futures_map):
                if shutdown_evt.is_set():
                    for pending in futures_map:
                        pending.cancel()
                    break

                chunk_files_list = futures_map[fut]
                partial, chunk_files, chunk_tokens = fut.result()
                done_files  += chunk_files
                done_tokens += chunk_tokens
                processed_in_run.update(chunk_files_list)

                for word, c in partial.items():
                    counts[word] += c

                now = time.perf_counter()
                if now - last_checkpoint >= bd._CHECKPOINT_SEC:
                    last_checkpoint = now
                    all_proc = already_processed | processed_in_run
                    bd._save_freq_checkpoint(checkpoint_path, all_proc, dict(counts))

                if now - last_report >= bd._REPORT_SEC:
                    last_report = now
                    bd.Progress.bar_line(
                        done_files, total_files,
                        f"{done_files:>6,} / {total_files:,} files  "
                        f"{len(counts):,} 語  {done_tokens:,} tokens",
                    )
    finally:
        signal.signal(signal.SIGINT, _orig_sigint)
        all_proc = already_processed | processed_in_run
        bd._save_freq_checkpoint(checkpoint_path, all_proc, dict(counts))
        log.info("頻度チェックポイント保存完了: %s", checkpoint_path)

    if shutdown_evt.is_set():
        print(f"  頻度チェックポイント保存完了: {checkpoint_path}", flush=True)
        sys.exit(130)

    result = dict(counts)
    if table_path:
        try:
            with gzip.open(table_path, "wt", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False)
            bd.Progress.step(f"頻度テーブル保存: {table_path}")
        except Exception as exc:
            log.warning("頻度テーブル保存失敗: %s", exc)

    bd.Progress.ok(f"{done_files:,} ファイル  {done_tokens:,} tokens  {len(result):,} 語")
    bd.Progress.endgroup()
    return result


# ──────────────────────────────────────────────────────────
# DB 既知語のロード
# ──────────────────────────────────────────────────────────
def load_known_words(db_path: Path) -> set[str]:
    """genji.db の見出し語（entry）と読み（reading_primary）を集合で返す。"""
    if not db_path.exists():
        raise FileNotFoundError(f"DB が見つかりません: {db_path}")
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        known: set[str] = set()
        for (entry,) in conn.execute("SELECT entry FROM entries"):
            if entry:
                known.add(entry)
        for (reading,) in conn.execute("SELECT reading_primary FROM entries"):
            if reading:
                known.add(reading)
    finally:
        conn.close()
    return known


# ──────────────────────────────────────────────────────────
# 差分抽出・出力
# ──────────────────────────────────────────────────────────
def write_new_words(
    freq_table: dict[str, int],
    known: set[str],
    output_path: Path,
    min_count: int,
    dry_run: bool,
) -> int:
    """新語候補（既知でない語）を頻度降順で output_path に書き出す。件数を返す。"""
    candidates = [
        (word, count)
        for word, count in freq_table.items()
        if count >= min_count and word not in known
    ]
    # 出現回数の降順 → 語の昇順
    candidates.sort(key=lambda wc: (-wc[1], wc[0]))

    if dry_run:
        bd.Progress.step(f"[dry-run] 新語候補 {len(candidates):,} 件（書き込みなし）")
        return len(candidates)

    # アトミック書き込み
    tmp_path = output_path.with_name(output_path.name + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        for word, count in candidates:
            f.write(f"{word}\t{count}\n")
    os.replace(tmp_path, output_path)
    return len(candidates)


# ──────────────────────────────────────────────────────────
# main
# ──────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="青空文庫から幻辭に未収録の新語を抽出して new_words.txt に書き出す",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--tmp-dir", type=Path, default=Path("/tmp"),
                        help="一時/キャッシュ先（clone・チェックポイント）")
    parser.add_argument("--db", type=Path, default=PROJECT_ROOT / "genji.db",
                        help="幻辭 SQLite DB")
    parser.add_argument("--output", type=Path, default=PROJECT_ROOT / "new_words.txt",
                        help="新語リストの出力先")
    parser.add_argument("--workers", type=int, default=None,
                        help="並列ワーカー数（既定: CPU 数）")
    parser.add_argument("--min-count", type=int, default=1,
                        help="この回数以上出現した語のみ記録")
    parser.add_argument("--include-proper-nouns", action="store_true",
                        help="固有名詞（人名・地名・組織名等）も含める")
    parser.add_argument("--resume", action="store_true",
                        help="チェックポイントから再開")
    parser.add_argument("--no-clone", action="store_true",
                        help="コーパスの clone/pull をスキップ（既存キャッシュを使用）")
    parser.add_argument("--dry-run", action="store_true",
                        help="件数のみ算出し new_words.txt は書き換えない")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    n_workers = args.workers or (os.cpu_count() or 4)
    exclude_proper = not args.include_proper_nouns
    checkpoint_path = args.tmp_dir / _CKPT_NAME
    table_path      = args.tmp_dir / _TABLE_NAME

    t0 = time.perf_counter()

    # 1. コーパス取得
    if args.no_clone:
        aozora_dir = args.tmp_dir / _AOZORA_NAME
        bd.Progress.step(f"clone スキップ。既存キャッシュ: {aozora_dir}")
    else:
        aozora_dir = ensure_aozora(args.tmp_dir)

    # 2. 頻度テーブル構築（形態素解析）
    freq_table = build_freq_table(
        aozora_dir, n_workers, checkpoint_path, table_path,
        resume=args.resume, exclude_proper=exclude_proper,
    )
    if not freq_table:
        bd.Progress.warn("頻度テーブルが空のため終了します。")
        sys.exit(1)

    # 3. DB 既知語のロード
    bd.Progress.step(f"DB 既知語ロード: {args.db}")
    known = load_known_words(args.db)
    bd.Progress.step(f"既知語: {len(known):,} 件")

    # 4. 差分抽出・出力
    n_new = write_new_words(freq_table, known, args.output, args.min_count, args.dry_run)

    elapsed = time.perf_counter() - t0
    bd.Progress.ok(
        f"完了  コーパス語数 {len(freq_table):,}  既知語 {len(known):,}  "
        f"新語 {n_new:,}  ({elapsed:.1f}s)"
    )
    if not args.dry_run:
        bd.Progress.step(f"出力: {args.output}")


if __name__ == "__main__":
    main()
