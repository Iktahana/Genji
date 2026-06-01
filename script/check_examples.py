"""
Genji 例文品質チェック・修正スクリプト（examples.standard の検査と補充）

build_dictionary.py / generate_examples.py が生成した AI 例文（examples.standard）には
以下の問題が混在する:
  1. 見出し語を一切含まない例文（別語の混入・汎用フィラー・"例句" プレースホルダ等）
  2. テンプレ／メタ説明文のゴミ例文
       例: 「私は毎日◯◯について考えている」「◯◯という言葉は日本語で重要だ」
           「彼は◯◯の意味を理解している」「この文には◯◯が含まれている」
  3. そもそも AI 例文が無い（欠落）定義

本スクリプトはこれを 2 フェーズで検査・修正する。examples.literary（実在出典の
文学例文）は触らない。

2フェーズ構成:
  Phase 1  クリーニング（--clean） — AI不要・確定的・安全
           各 examples.standard を検査し、見出し語を含まない例文（活用対応）と
           テンプレ系ゴミ例文を削除して書き戻す。--dry-run で集計のみ。
  Phase 2  補充生成（--fill） — AI生成
           有効な standard 例文が TARGET 未満の定義に対し、Gemini + Claude 統合
           プールで例文を生成・補充する。生成結果も「見出し語包含＋非ゴミ」で
           検証してから採用する（generate_relations.py と同方式）。

使い方:
  python check_examples.py                 # 両フェーズ（clean → fill）
  python check_examples.py --clean         # クリーニングのみ（高速・API不要）
  python check_examples.py --clean --dry-run  # 削除件数を集計するだけ（無書込）
  python check_examples.py --fill          # 補充生成のみ（cleanが先行済み前提）
  python check_examples.py --status        # 進捗表示
  python check_examples.py --clear-checkpoint
"""

import os
import json
import re
import subprocess
import time
import sys
import signal
import threading
import atexit
import unicodedata
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ──────────────────────────────────────────────────────────
# 設定
# ──────────────────────────────────────────────────────────

SCRIPT_DIR   = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_ROOT    = PROJECT_ROOT / "data"
CHECKPOINT_DIR = PROJECT_ROOT / ".checkpoints" / "examples_qc"
MAX_WORKERS  = 3      # AI フェーズの並列ワーカー数
REPORT_SEC   = 3.0    # 進捗表示の更新間隔（秒）
CHECKPOINT_SAVE_INTERVAL = 20  # N ファイル/バッチごとに checkpoint を自動保存
BATCH_SIZE   = 30     # 一回のAPI呼び出しで処理する定義数
AUTO_COMMIT_INTERVAL = 1800  # 自動コミットの間隔（秒 = 30分）
GLOBAL_RPM_INTERVAL  = 1.5   # グローバルリクエスト間隔（秒）≈ 40 RPM

TARGET_EXAMPLES = 3   # 各定義が持つべき standard 例文の目標数

# AIモデルプール。"provider:model_id" 形式。Gemini + Claude 統合。
MODELS = [
    "gemini:gemini-2.5-flash",
    "gemini:gemini-2.5-flash-lite",
    "gemini:gemini-3-flash-preview",
    "gemini:gemini-2.5-pro",
    "gemini:gemini-3.1-pro-preview",
    "claude:claude-haiku-4-5-20251001",
    "claude:claude-sonnet-4-6",
]

CLAUDE_TIMEOUT = 300
GEMINI_TIMEOUT = 180

# Claude CLI（Claude Code）はツールを持つエージェント。system-prompt で純粋な
# 生成器に上書きし、全ツールを禁止して即時 JSON 出力させる。
CLAUDE_SYSTEM_PROMPT = (
    "あなたは日本語辞書の例文生成器です。ツールは一切使用せず、"
    "要求されたJSONのみを即座に出力してください。説明・前置きは不要です。"
)
CLAUDE_DISALLOWED_TOOLS = (
    "Bash,Read,Write,Edit,Glob,Grep,WebFetch,WebSearch,Task,NotebookEdit,TodoWrite"
)


def provider_of(model_key: str) -> str:
    return model_key.split(":", 1)[0]


def model_id_of(model_key: str) -> str:
    return model_key.split(":", 1)[1]


def parse_retry_delay(err: str) -> int:
    """429 エラーメッセージからリトライ待機時間（秒）を解析。既定 60。"""
    m = re.search(r'retryDelay[^0-9]*(\d+)m(\d+)s', err)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    m = re.search(r'retryDelay[^0-9]*(\d+)m', err)
    if m:
        return int(m.group(1)) * 60
    m = re.search(r'retryDelay[^0-9]*(\d+)s', err)
    if m:
        return int(m.group(1))
    m = re.search(r'retry.{0,15}after[:\s]+(\d+)', err, re.IGNORECASE)
    if m:
        return int(m.group(1))
    m = re.search(r'Retry-After[:\s]+(\d+)', err, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return 60


# ──────────────────────────────────────────────────────────
# 例文の品質判定（見出し語包含・ゴミテンプレ）
# ──────────────────────────────────────────────────────────

_CJK = re.compile(r'[一-鿿㐀-䶿]')


def _has_kanji(s: str) -> bool:
    return bool(_CJK.search(s))


def _is_kana(ch: str) -> bool:
    return '぀' <= ch <= 'ヿ'


def _nfkc(s: str) -> str:
    return unicodedata.normalize('NFKC', s) if s else s


def acceptable_forms(entry: str, pos: str) -> list[str]:
    """
    見出し語が例文中に現れ得る表記候補（活用語幹を含む）を返す。
    全て NFKC 正規化済み。pos は日本語ラベル（動詞-五段-サ行 等）と
    英語ラベル（Godan verb / Ichidan / adjective 等）の両方に対応。
    """
    e = _nfkc(entry)
    if not e:
        return []
    forms = {e}
    posl = pos.lower()
    is_adj     = ('形容詞' in pos) or ('adjective' in posl)
    is_verb    = ('動詞' in pos) or ('verb' in posl)
    is_ichidan = ('一段' in pos) or ('ichidan' in posl)
    is_godan   = ('五段' in pos) or ('godan' in posl)
    is_sahen   = ('サ変' in pos) or ('suru verb' in posl) or ('-suru' in posl)
    is_kahen   = ('カ変' in pos)

    if is_adj and e.endswith('い') and len(e) >= 2:
        forms.add(e[:-1])                       # 美しい → 美し
    if is_verb:
        if (is_ichidan or is_kahen) and e.endswith('る') and len(e) >= 2:
            forms.add(e[:-1])                   # 食べる → 食べ
        elif is_godan and len(e) >= 2 and _is_kana(e[-1]):
            forms.add(e[:-1])                   # 垂らす → 垂ら / 電報を打つ → 電報を打
        elif is_sahen:
            pass                                # サ変は語幹そのもの（〜する）
        elif len(e) >= 2 and _is_kana(e[-1]):
            forms.add(e[:-1])                   # 一般動詞フォールバック

    # 仮名1字の語幹は誤検出が多いので、漢字を含むか len>=2 のものだけ採用。
    return [f for f in forms if f and (_has_kanji(f) or len(f) >= 2)]


def word_in_text(text: str, entry: str, reading: str, pos: str) -> bool:
    """例文 text が見出し語（活用形・全半角・仮名書きを許容）を含むか。"""
    if not text:
        return False
    t = _nfkc(text)
    for f in acceptable_forms(entry, pos):
        if f in t:
            return True
    r = _nfkc(reading)
    # 仮名書き表記のフォールバック（読みが 2 文字以上のときのみ）
    if r and len(r) >= 2 and r in t:
        return True
    return False


# generate_examples.py 由来の低品質テンプレート + ユーザー報告のメタ説明ゴミ。
_JUNK_PATTERN_STRINGS = [
    r"私たちの生活に欠かせません",
    r"ビジネスシーンでは.*重要です",
    r"科学的研究が進みました",
    r"物語の中心となって",
    r"学校の教室で.*学びました",
    r"医師から.*アドバイスを受けました",
    r"法律では.*定義されています",
    r"スポーツの試合では.*勝敗を決めました",
    r"自然界では.*見られる現象です",
    r"歴史的に.*重要な位置づけです",
    r"料理において.*重要な食材です",
    r"旅行中に.*見学することができました",
    r"朝食の時に.*いただきました",
    r"営業会議で.*議論されました",
    r"実験の結果、.*性質が明らかになりました",
    r"著者は.*象徴的に表現しています",
    r"教科書の第三章は.*内容です",
    r"健康診断で.*相談しました",
    r"法的な観点から.*重要な問題です",
    r"アスリートは.*訓練しています",
    r"^例句\d*$",
    r"^例文\d*$",
    r"この言葉は日常会話で頻繁に使用されます",
    r"文脈によって意味が変わることがあります",
    r"ビジネス会話では特に重要な表現です",
    r"日本の伝統文化に関連する言葉です",
    r"学校教育で教えられる基本的な言葉です",
    r"医学分野でも使用される専門用語です",
    r"法律文書でこの表現がよく見られます",
    r"スポーツ界でも一般的な言い回しです",
    r"環境問題に関する文脈で使用されます",
    r"料理や食文化の説明に用いられます",
    r"旅行会話で役立つ重要な言葉です",
    r"日本の歴史的背景を反映しています",
    r"社会問題の議論で言及されることが多いです",
    r"技術用語としても広く認識されています",
    r"地域によって方言的な変形があります",
    r"若い世代も自然に使用する一般的な言葉です",
    r"文語的な表現として古典に登場します",
    r"その語源は興味深い歴史があります",
    r"現代でも使用頻度が高い重要語彙です",
    # ── ユーザー報告のメタ説明テンプレ（語を当てはめただけのゴミ）──
    r"私は毎日.{1,14}について考えて",
    r".{1,16}という言葉は日本語で重要",
    r".{1,16}という言葉は.{0,8}重要",
    r".{1,16}の意味を理解してい",
    r"この(?:文|文章|例文|センテンス)に(?:は)?.{1,16}が含まれて",
    r".{1,16}は日本語で(?:重要|大切)",
    r".{1,16}について(?:説明|学習|勉強)してい",
]

JUNK_PATTERNS = [re.compile(p) for p in _JUNK_PATTERN_STRINGS]


def is_junk_text(text: str) -> bool:
    if not text or not text.strip():
        return True
    return any(p.search(text) for p in JUNK_PATTERNS)


def is_good_example(ex, entry: str, reading: str, pos: str) -> bool:
    """standard 例文 1 件が有効か（dict・非ゴミ・見出し語包含）。"""
    if not isinstance(ex, dict):
        return False
    txt = ex.get('text', '')
    if is_junk_text(txt):
        return False
    if not word_in_text(txt, entry, reading, pos):
        return False
    return True


# ──────────────────────────────────────────────────────────
# ModelManager（レート制限管理）
# ──────────────────────────────────────────────────────────

class ModelManager:
    def __init__(self, models):
        self.models = models
        self.lock = threading.Lock()
        self.cooldowns: dict[str, float] = {m: 0.0 for m in models}
        self.semaphores: dict[str, threading.BoundedSemaphore] = {
            m: threading.BoundedSemaphore(1) for m in models
        }
        self.preferred_index = 0
        self._last_global_request = 0.0

    def acquire_model(self) -> str:
        while True:
            if _shutdown_requested.is_set():
                return self.models[0]

            with self.lock:
                now = time.time()
                n = len(self.models)
                candidates = []
                for offset in range(n):
                    idx = (self.preferred_index + offset) % n
                    model = self.models[idx]
                    if self.cooldowns[model] <= now:
                        candidates.append((idx, model))

            for idx, model in candidates:
                if self.semaphores[model].acquire(blocking=False):
                    with self.lock:
                        now = time.time()
                        wait = max(0.0, self._last_global_request + GLOBAL_RPM_INTERVAL - now)
                        self._last_global_request = now + wait
                        self.preferred_index = idx
                    if wait > 0:
                        time.sleep(wait)
                    return model

            with self.lock:
                now = time.time()
                all_cooling = all(self.cooldowns[m] > now for m in self.models)
                if all_cooling:
                    soonest_model = min(self.models, key=lambda m: self.cooldowns[m])
                    wait_sec = max(0.0, self.cooldowns[soonest_model] - now)
                else:
                    wait_sec = 0.5

            if all_cooling:
                ts = datetime.now().strftime('%H:%M:%S')
                resume = datetime.fromtimestamp(self.cooldowns[soonest_model]).strftime('%H:%M:%S')
                print(
                    f"\n[Rate Limit] 全モデルがクールダウン中。"
                    f"{wait_sec:.0f}秒待機 ({ts} → {resume}) ...",
                    flush=True
                )
                deadline = time.time() + wait_sec
                while time.time() < deadline:
                    if _shutdown_requested.is_set():
                        return self.models[0]
                    time.sleep(min(5, max(0.1, deadline - time.time())))
            else:
                time.sleep(wait_sec)

    def release_model(self, model: str) -> None:
        try:
            self.semaphores[model].release()
        except ValueError:
            pass

    def mark_rate_limited(self, model: str, retry_after: int) -> None:
        with self.lock:
            self.cooldowns[model] = time.time() + retry_after
        ts = datetime.fromtimestamp(self.cooldowns[model]).strftime('%H:%M:%S')
        print(f"\n[Rate Limit] {model} → {retry_after}s 待機 (解除: {ts})", flush=True)

    def mark_success(self, model: str) -> None:
        with self.lock:
            try:
                self.preferred_index = self.models.index(model)
            except ValueError:
                pass

    def mark_unavailable(self, model: str) -> None:
        with self.lock:
            self.cooldowns[model] = time.time() + 3600

    def status(self) -> list[tuple[str, str]]:
        now = time.time()
        result = []
        with self.lock:
            for m in self.models:
                cd = self.cooldowns[m]
                if cd <= now:
                    result.append((m, "available"))
                else:
                    result.append((m, f"cooldown {int(cd - now)}s"))
        return result


model_manager = ModelManager(MODELS)

# ──────────────────────────────────────────────────────────
# Checkpoint管理（中断・再開機能）
# ──────────────────────────────────────────────────────────

class CheckpointManager:
    """フェーズごとに独立した checkpoint ファイルを持つ。"""
    def __init__(self, name: str):
        self.checkpoint_dir = CHECKPOINT_DIR
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.checkpoint_dir / f"{name}.json"
        self.processed_files: set[str] = set()
        self.updated_files: set[str] = set()
        self.lock = threading.Lock()
        self._dirty_count = 0
        self._load_checkpoint()

    def _load_checkpoint(self):
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processed_files = set(data.get('processed', []))
                    self.updated_files = set(data.get('updated', []))
                    timestamp = data.get('timestamp', 'Unknown')
                    print(f"✓ Checkpoint loaded ({self.checkpoint_file.name}): "
                          f"{len(self.processed_files)} files processed at {timestamp}")
            except Exception as e:
                print(f"! Checkpoint load failed: {e}. Starting fresh.")
                self.processed_files = set()
                self.updated_files = set()

    def add_processed(self, file_path: str):
        with self.lock:
            self.processed_files.add(file_path)
            self._dirty_count += 1
            should_save = self._dirty_count >= CHECKPOINT_SAVE_INTERVAL
        if should_save:
            self.save_checkpoint()

    def add_updated(self, file_path: str):
        with self.lock:
            self.updated_files.add(file_path)

    def is_processed(self, file_path: str) -> bool:
        with self.lock:
            return file_path in self.processed_files

    def save_checkpoint(self):
        try:
            with self.lock:
                self._dirty_count = 0
                data = {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'processed': sorted(self.processed_files),
                    'updated': sorted(self.updated_files),
                    'processed_count': len(self.processed_files),
                    'updated_count': len(self.updated_files)
                }
            tmp_file = self.checkpoint_file.with_suffix(
                f'.{os.getpid()}.{threading.get_ident()}.tmp'
            )
            with open(tmp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            tmp_file.replace(self.checkpoint_file)
        except Exception as e:
            print(f"! Checkpoint save failed: {e}")

    def clear_checkpoint(self):
        with self.lock:
            self.processed_files.clear()
            self.updated_files.clear()
            self._dirty_count = 0
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
        print(f"✓ Checkpoint cleared ({self.checkpoint_file.name})")


# ──────────────────────────────────────────────────────────
# 進捗表示（GitHub Actions 対応）
# ──────────────────────────────────────────────────────────

class Progress:
    IS_GHA = os.environ.get("GITHUB_ACTIONS") == "true"
    BAR_W  = 28

    @staticmethod
    def _bar(done: int, total: int) -> str:
        if total <= 0:
            return f"[{'░' * Progress.BAR_W}]  0.0%"
        pct    = min(done / total, 1.0)
        filled = round(pct * Progress.BAR_W)
        return f"[{'█' * filled}{'░' * (Progress.BAR_W - filled)}] {pct:5.1%}"

    @staticmethod
    def group(title: str) -> None:
        if Progress.IS_GHA:
            print(f"::group::{title}", flush=True)
        else:
            print(f"\n┌─ {title}", flush=True)

    @staticmethod
    def endgroup() -> None:
        if Progress.IS_GHA:
            print("::endgroup::", flush=True)

    @staticmethod
    def step(msg: str) -> None:
        print(f"  │  {msg}", flush=True)

    @staticmethod
    def ok(msg: str) -> None:
        print(f"  └✓ {msg}", flush=True)

    @staticmethod
    def bar_line(done: int, total: int, suffix: str = "") -> None:
        bar = Progress._bar(done, total)
        print(f"  │  {bar}  {suffix}", flush=True)


# ──────────────────────────────────────────────────────────
# ユーティリティ
# ──────────────────────────────────────────────────────────

_ANSI_RE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')


def clean_ansi(text: str) -> str:
    return _ANSI_RE.sub('', text)


def _to_checkpoint_key(file_path: Path) -> str:
    try:
        return str(file_path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(file_path)


def all_data_files() -> list[Path]:
    all_dirs = sorted([d for d in DATA_ROOT.iterdir() if d.is_dir()], key=lambda x: x.name)
    files = []
    for d in all_dirs:
        files.extend(sorted(list(d.glob("*.json"))))
    return files


def _entry_meta(entry_obj: dict) -> tuple[str, str, str]:
    entry_text = entry_obj.get('entry', '')
    reading = entry_obj.get('reading', {}).get('primary', '')
    pos = ",".join(entry_obj.get('grammar', {}).get('pos', []))
    return entry_text, reading, pos


# ──────────────────────────────────────────────────────────
# Phase 1: クリーニング（AI不要・確定的）
# ──────────────────────────────────────────────────────────

clean_stats_lock = threading.Lock()
clean_deleted_total = 0


def clean_file(fp: Path, dry_run: bool = False) -> tuple[bool, int]:
    """
    1ファイルの examples.standard を検査し、見出し語を含まない例文と
    ゴミテンプレ例文を削除する。examples.literary は触らない。
    returns: (changed, n_deleted)
    """
    try:
        with open(fp, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"\n[エラー] {fp}: {e}")
        return False, 0

    modified = False
    n_deleted = 0
    for entry_obj in data:
        entry_text, reading, pos = _entry_meta(entry_obj)
        for definition in entry_obj.get('definitions', []):
            ex_block = definition.get('examples')
            if not isinstance(ex_block, dict):
                continue
            std = ex_block.get('standard')
            if not isinstance(std, list) or not std:
                continue
            kept = [ex for ex in std if is_good_example(ex, entry_text, reading, pos)]
            if len(kept) != len(std):
                n_deleted += len(std) - len(kept)
                modified = True
                if not dry_run:
                    ex_block['standard'] = kept

    if modified and not dry_run:
        if data and 'meta' in data[0]:
            data[0]['meta']['updated_at'] = datetime.now(timezone.utc).isoformat() + 'Z'
        with open(fp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    return modified, n_deleted


def run_clean_phase(dry_run: bool = False):
    global clean_deleted_total
    cp = CheckpointManager("clean")
    atexit.register(cp.save_checkpoint)

    title = "Phase 1: 例文クリーニング（見出し語包含＋ゴミ判定）"
    if dry_run:
        title += "  [DRY-RUN: 集計のみ・無書込]"
    Progress.group(title)

    files = all_data_files()
    total = len(files)
    # dry-run は checkpoint を進めない（何度でも集計し直せるように）
    if dry_run:
        pending = files
    else:
        pending = [f for f in files if not cp.is_processed(_to_checkpoint_key(f))]
    Progress.step(f"総ファイル数: {total:,}  /  対象: {len(pending):,}")

    if not pending:
        Progress.ok("全ファイル処理済みです。")
        Progress.endgroup()
        return

    updated = 0
    done = 0
    last_t = time.perf_counter()

    def worker(fp: Path):
        changed, ndel = clean_file(fp, dry_run=dry_run)
        if not dry_run:
            cp.add_processed(_to_checkpoint_key(fp))
            if changed:
                cp.add_updated(_to_checkpoint_key(fp))
        return changed, ndel

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(worker, fp): fp for fp in pending}
        for fut in as_completed(futures):
            if _shutdown_requested.is_set():
                ex.shutdown(wait=False, cancel_futures=True)
                break
            try:
                changed, ndel = fut.result()
                if changed:
                    updated += 1
                if ndel:
                    with clean_stats_lock:
                        clean_deleted_total += ndel
            except Exception as e:
                print(f"\n[ERROR] {e}", flush=True)
            done += 1
            now = time.perf_counter()
            if now - last_t >= REPORT_SEC:
                last_t = now
                Progress.bar_line(done, len(pending),
                                  f"{done:,}/{len(pending):,} "
                                  f"(要修正: {updated:,} files / 削除: {clean_deleted_total:,} 例文)")

    if not dry_run:
        cp.save_checkpoint()
    verb = "削除対象" if dry_run else "削除"
    Progress.ok(f"Phase 1 完了。{updated:,} ファイルで {clean_deleted_total:,} 例文を{verb}。")
    Progress.endgroup()


# ──────────────────────────────────────────────────────────
# Phase 2: 補充生成（AI）
# ──────────────────────────────────────────────────────────

ai_cp = None  # AI フェーズ用 CheckpointManager（main で初期化）

progress_lock   = threading.Lock()
updated_count   = 0
processed_count = 0
total_batches   = 0


def _valid_standard(definition: dict, entry: str, reading: str, pos: str) -> list:
    ex_block = definition.get('examples')
    if not isinstance(ex_block, dict):
        return []
    std = ex_block.get('standard')
    if not isinstance(std, list):
        return []
    return [ex for ex in std if is_good_example(ex, entry, reading, pos)]


def scan_pending_items(files_to_process: list) -> list[tuple]:
    """
    補充生成が必要な定義を抽出。
    （有効な standard 例文が TARGET 未満の定義）
    returns: [(fp, entry_idx, def_idx, entry, reading, pos, gloss), ...]
    """
    pending = []
    skipped = 0
    for fp in files_to_process:
        try:
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)
            file_pending = []
            for ei, entry_obj in enumerate(data):
                entry_text, reading, pos = _entry_meta(entry_obj)
                for di, definition in enumerate(entry_obj.get('definitions', [])):
                    valid = _valid_standard(definition, entry_text, reading, pos)
                    if len(valid) < TARGET_EXAMPLES:
                        file_pending.append(
                            (fp, ei, di, entry_text, reading, pos, definition.get('gloss', ''))
                        )
            if file_pending:
                pending.extend(file_pending)
            else:
                ai_cp.add_processed(_to_checkpoint_key(fp))
                skipped += 1
        except Exception as e:
            print(f"\n[エラー] {fp}: {e}")
    if skipped > 0:
        Progress.step(f"スキップ（補充不要）: {skipped:,} files")
    return pending


def create_batches(pending_items: list) -> list[list[tuple]]:
    """BATCH_SIZE で切分。同一ファイルの定義は同一バッチに収める。"""
    batches = []
    current_batch = []
    current_file = None
    for item in pending_items:
        fp = item[0]
        if len(current_batch) >= BATCH_SIZE and fp != current_file:
            batches.append(current_batch)
            current_batch = []
        current_batch.append(item)
        current_file = fp
    if current_batch:
        batches.append(current_batch)
    return batches


def _build_prompt(items: list) -> str:
    """items: [(entry, reading, pos, gloss), ...]"""
    word_lines = "\n".join([
        f"{i+1}. 表記:{entry} 読み:{reading} 品詞:{pos} 意味:{gloss}"
        for i, (entry, reading, pos, gloss) in enumerate(items)
    ])
    return (
        f'各語に自然な例文を{TARGET_EXAMPLES}個、JSON出力。'
        'キー="1","2",...、値=[{"text":"例文"}]。'
        '各例文に必ず見出し語（表記そのもの、または自然な活用形）を含めること。'
        '具体的な場面の文にする。'
        'メタ説明文（「◯◯という言葉は重要」「◯◯の意味を理解している」'
        '「この文には◯◯が含まれる」等）やテンプレ文は禁止。'
        '感動詞は会話文にする。\n\n'
        f'{word_lines}\n\n'
        '{"1":[{"text":"..."}],"2":[{"text":"..."}]}'
    )


def _call_gemini(model_id: str, prompt: str) -> tuple[str, str]:
    res = subprocess.run(
        ['gemini', '-m', model_id, '-p', prompt, '-o', 'json'],
        capture_output=True, text=True, encoding='utf-8', timeout=GEMINI_TIMEOUT
    )
    raw_out = res.stdout.strip()
    err = res.stderr.strip()

    if any(x in err for x in ("429", "Quota exceeded", "Rate limit")):
        return "", f"rate:{parse_retry_delay(err)}"
    if any(x in err for x in ("ModelNotFoundError", "not found", "INVALID_ARGUMENT")):
        return "", "unavailable"

    try:
        envelope = json.loads(raw_out)
        if "error" in envelope:
            err_msg = envelope["error"].get("message", "")
            if any(x in err_msg for x in ("429", "Quota", "Rate")):
                return "", f"rate:{parse_retry_delay(err_msg)}"
            return "", "error"
        return envelope.get("response", ""), "ok"
    except json.JSONDecodeError:
        return clean_ansi(raw_out), "ok"


def _call_claude(model_id: str, prompt: str) -> tuple[str, str]:
    res = subprocess.run(
        ['claude', '-p', prompt, '--output-format', 'json', '--model', model_id,
         '--system-prompt', CLAUDE_SYSTEM_PROMPT,
         '--disallowed-tools', CLAUDE_DISALLOWED_TOOLS],
        capture_output=True, text=True, encoding='utf-8', timeout=CLAUDE_TIMEOUT
    )
    raw_out = res.stdout.strip()
    err = res.stderr.strip()

    try:
        envelope = json.loads(raw_out)
    except json.JSONDecodeError:
        if any(x in err for x in ("429", "rate", "overloaded", "Overloaded")):
            return "", "rate:60"
        return "", "error"

    if envelope.get("is_error"):
        msg = (str(envelope.get("result", "")) + " "
               + str(envelope.get("api_error_status", "")) + " " + err)
        if any(x in msg for x in ("429", "rate", "overloaded", "Overloaded", "529")):
            return "", f"rate:{parse_retry_delay(msg)}"
        if any(x in msg for x in ("not found", "invalid model", "does not exist")):
            return "", "unavailable"
        return "", "error"

    return envelope.get("result", ""), "ok"


def generate_examples_batch(items: list) -> tuple[dict, str]:
    """
    items: [(entry, reading, pos, gloss), ...]
    returns: ({"1":[{"text":"..."}], ...}, model_key)
    """
    max_retries = len(MODELS) * 3
    prompt = _build_prompt(items)

    for _ in range(max_retries):
        if _shutdown_requested.is_set():
            return {}, ""

        model_key = model_manager.acquire_model()
        if _shutdown_requested.is_set():
            model_manager.release_model(model_key)
            return {}, ""

        try:
            provider = provider_of(model_key)
            model_id = model_id_of(model_key)
            if provider == "claude":
                response_text, status = _call_claude(model_id, prompt)
            else:
                response_text, status = _call_gemini(model_id, prompt)

            if status.startswith("rate:"):
                model_manager.mark_rate_limited(model_key, int(status.split(":", 1)[1]))
                continue
            if status == "unavailable":
                model_manager.mark_unavailable(model_key)
                continue
            if status != "ok":
                time.sleep(1)
                continue

            match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if match:
                for attempt in [match.group(0), re.sub(r',\s*([}\]])', r'\1', match.group(0))]:
                    try:
                        result = json.loads(attempt)
                        model_manager.mark_success(model_key)
                        return result, model_key
                    except json.JSONDecodeError:
                        continue
            time.sleep(1)

        except subprocess.TimeoutExpired:
            model_manager.mark_rate_limited(model_key, 30)
        except Exception:
            pass
        finally:
            model_manager.release_model(model_key)

    return {}, ""


def process_batch(batch: list) -> int:
    """1バッチを処理: API → ファイル書き戻し → checkpoint。"""
    global updated_count, processed_count

    if _shutdown_requested.is_set():
        return 0

    batch_input = [(item[3], item[4], item[5], item[6]) for item in batch]
    batch_results, model_key = generate_examples_batch(batch_input)
    author = "Claude" if provider_of(model_key) == "claude" else "Gemini"
    note = model_id_of(model_key) if model_key else ""

    # ファイル別に整理: (entry_idx, def_idx, entry, reading, pos, [生成例文])
    file_updates: dict[Path, list] = {}
    for j, item in enumerate(batch):
        val = batch_results.get(str(j + 1))
        if isinstance(val, list):
            fp, ei, di = item[0], item[1], item[2]
            file_updates.setdefault(fp, []).append((ei, di, item[3], item[4], item[5], val))

    local_updated = 0
    all_files_in_batch = set(item[0] for item in batch)

    for fp, updates in file_updates.items():
        try:
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)

            modified = False
            for ei, di, entry_text, reading, pos, new_exs in updates:
                if ei >= len(data):
                    continue
                try:
                    definition = data[ei]['definitions'][di]
                except (KeyError, IndexError, TypeError):
                    continue

                # 既存の有効例文を保持し、生成例文を検証して TARGET まで補充
                existing = _valid_standard(definition, entry_text, reading, pos)
                seen = {_nfkc(ex.get('text', '')) for ex in existing if isinstance(ex, dict)}
                combined = list(existing)
                for ex in new_exs:
                    if len(combined) >= TARGET_EXAMPLES:
                        break
                    if not isinstance(ex, dict):
                        continue
                    txt = ex.get('text', '').strip()
                    if not txt or _nfkc(txt) in seen:
                        continue
                    if is_junk_text(txt) or not word_in_text(txt, entry_text, reading, pos):
                        continue
                    seen.add(_nfkc(txt))
                    combined.append({
                        "text": txt,
                        "citation": {"source": "Illusions AI", "author": author, "note": note}
                    })

                if not combined:
                    continue
                ex_block = definition.setdefault('examples', {"standard": [], "literary": []})
                if not isinstance(ex_block, dict):
                    ex_block = {"standard": [], "literary": []}
                    definition['examples'] = ex_block
                # 内容が変わる場合のみ更新
                if ex_block.get('standard') != combined:
                    ex_block['standard'] = combined
                    modified = True

            if modified:
                if data and 'meta' in data[0]:
                    data[0]['meta']['updated_at'] = datetime.now(timezone.utc).isoformat() + 'Z'
                with open(fp, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                ai_cp.add_updated(_to_checkpoint_key(fp))
                local_updated += 1
        except Exception as e:
            print(f"\n[エラー] {fp}: {e}")

    for fp in all_files_in_batch:
        ai_cp.add_processed(_to_checkpoint_key(fp))

    with progress_lock:
        updated_count += local_updated
        processed_count += 1

    return local_updated


# ──────────────────────────────────────────────────────────
# 自動コミット（30分ごと）
# ──────────────────────────────────────────────────────────

def auto_commit_worker():
    while not _shutdown_requested.wait(timeout=AUTO_COMMIT_INTERVAL):
        try:
            subprocess.run(['git', 'add', 'data/'],
                           capture_output=True, cwd=str(PROJECT_ROOT), timeout=60)
            status = subprocess.run(
                ['git', 'status', '--porcelain', 'data/'],
                capture_output=True, text=True, cwd=str(PROJECT_ROOT), timeout=30
            )
            if not status.stdout.strip():
                continue
            ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
            commit = subprocess.run(
                ['git', 'commit', '-m', f'data(examples): auto-commit QC ({ts})'],
                capture_output=True, text=True, cwd=str(PROJECT_ROOT), timeout=60
            )
            if commit.returncode == 0:
                print(f"\n[Auto-commit] ✓ commit しました ({ts})", flush=True)
        except Exception as e:
            print(f"\n[Auto-commit] エラー (スキップ): {e}", flush=True)


# ──────────────────────────────────────────────────────────
# 起動時チェック（モデル疎通）
# ──────────────────────────────────────────────────────────

def _probe_model(model_key: str) -> tuple[str, str]:
    provider = provider_of(model_key)
    model_id = model_id_of(model_key)
    try:
        if provider == "claude":
            res = subprocess.run(
                ['claude', '-p', 'OK とだけ答えて', '--output-format', 'json', '--model', model_id,
                 '--system-prompt', CLAUDE_SYSTEM_PROMPT,
                 '--disallowed-tools', CLAUDE_DISALLOWED_TOOLS],
                capture_output=True, text=True, encoding='utf-8', timeout=CLAUDE_TIMEOUT
            )
            try:
                env = json.loads(res.stdout.strip())
                if env.get("is_error"):
                    return model_key, "✗  Error"
                return model_key, "✓  OK"
            except json.JSONDecodeError:
                return model_key, f"?  Unknown (rc={res.returncode})"
        else:
            res = subprocess.run(
                ['gemini', '-m', model_id, '-p', '1'],
                capture_output=True, text=True, encoding='utf-8', timeout=20
            )
            err = clean_ansi(res.stderr).strip()
            out = clean_ansi(res.stdout).strip()
            if any(x in err for x in ("429", "Quota exceeded", "Rate limit")):
                delay = parse_retry_delay(err)
                model_manager.mark_rate_limited(model_key, delay)
                return model_key, f"⚠  Rate limited (reset in {delay}s)"
            if any(x in err for x in ("ModelNotFoundError", "not found", "INVALID_ARGUMENT")):
                model_manager.mark_unavailable(model_key)
                return model_key, "✗  Unavailable"
            if res.returncode == 0 and out:
                return model_key, "✓  OK"
            return model_key, f"?  Unknown (rc={res.returncode})"
    except subprocess.TimeoutExpired:
        return model_key, "✗  Timeout"
    except Exception as e:
        return model_key, f"✗  Error: {e}"


def startup_check() -> None:
    sep = "─" * 60
    print(f"\n{sep}", flush=True)
    print("  Genji 例文品質チェック・補充スクリプト（AIフェーズ）", flush=True)
    print(sep, flush=True)
    print("\n[Models] 疎通確認中...", flush=True)
    results: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=len(MODELS)) as ex:
        futures = {ex.submit(_probe_model, m): m for m in MODELS}
        for fut in as_completed(futures):
            model_key, status = fut.result()
            results[model_key] = status
    for m in MODELS:
        print(f"  {m:<36} {results.get(m, '?')}", flush=True)
    available = [m for m in MODELS if results.get(m, '').startswith("✓")]
    if not available:
        print("\n  [WARNING] 利用可能なモデルがありません。クールダウン後に自動リトライします。", flush=True)
    print(f"\n{sep}\n", flush=True)


# ──────────────────────────────────────────────────────────
# AIフェーズ本体
# ──────────────────────────────────────────────────────────

def run_fill_phase():
    global ai_cp, total_batches
    ai_cp = CheckpointManager("fill")
    atexit.register(ai_cp.save_checkpoint)

    startup_check()

    commit_thread = threading.Thread(target=auto_commit_worker, daemon=True, name="auto-commit")
    commit_thread.start()

    Progress.group(
        f"Phase 2: 例文補充 AI生成 "
        f"(並列={MAX_WORKERS}, バッチ={BATCH_SIZE}定義/APIコール, 目標={TARGET_EXAMPLES}例文/定義)"
    )
    Progress.step(f"モデルプール: {', '.join(MODELS)}")

    all_files = all_data_files()
    total_files = len(all_files)
    files_to_process = [f for f in all_files if not ai_cp.is_processed(_to_checkpoint_key(f))]
    Progress.step(f"総ファイル数: {total_files:,}  /  スキャン対象: {len(files_to_process):,}")

    Progress.step("ファイルスキャン中...")
    pending_items = scan_pending_items(files_to_process)
    if not pending_items:
        Progress.ok("全定義が目標例文数を満たしています。")
        Progress.endgroup()
        return

    unique_files = len(set(item[0] for item in pending_items))
    Progress.step(f"補充対象: {len(pending_items):,} definitions ({unique_files:,} files)")

    batches = create_batches(pending_items)
    total_batches = len(batches)
    Progress.step(f"バッチ数: {total_batches:,}")

    last_report_t = time.perf_counter()
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_batch, batch): i for i, batch in enumerate(batches)}
            for future in as_completed(futures):
                if _shutdown_requested.is_set():
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                try:
                    future.result()
                except Exception as e:
                    print(f"\n[ERROR] バッチ処理中にエラー: {e}", flush=True)
                now = time.perf_counter()
                if now - last_report_t >= REPORT_SEC:
                    with progress_lock:
                        last_report_t = now
                        Progress.bar_line(
                            processed_count, total_batches,
                            f"batch {processed_count:,}/{total_batches:,} "
                            f"(更新: {updated_count:,} files)"
                        )
        ai_cp.save_checkpoint()
        if _shutdown_requested.is_set():
            print("\n[INFO] 安全にシャットダウンしました。次回実行時に再開できます。", flush=True)
        else:
            Progress.ok(f"Phase 2 完了。合計 {updated_count:,} 件のファイルを更新しました。")
        Progress.endgroup()
    except Exception as e:
        ai_cp.save_checkpoint()
        print(f"\n[ERROR] 予期しないエラー: {e}", flush=True)
        raise


# ──────────────────────────────────────────────────────────
# 実行
# ──────────────────────────────────────────────────────────

_shutdown_requested = threading.Event()


def _install_signal_handlers():
    def signal_handler(sig, frame):
        if _shutdown_requested.is_set():
            print("\n[INFO] 強制終了します。", flush=True)
            sys.exit(1)
        _shutdown_requested.set()
        print("\n\n[INFO] シャットダウン要求を受信。ワーカーの完了を待っています...", flush=True)
        print("[INFO] もう一度 Ctrl+C で強制終了。", flush=True)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def main():
    args = sys.argv[1:]

    if "--status" in args:
        for name in ("clean", "fill"):
            cp = CheckpointManager(name)
            print(f"[{name}] processed={len(cp.processed_files)} updated={len(cp.updated_files)}")
        return

    if "--clear-checkpoint" in args:
        for name in ("clean", "fill"):
            CheckpointManager(name).clear_checkpoint()
        return

    if args and args[0] in ("-h", "--help"):
        print(__doc__)
        return

    dry_run = "--dry-run" in args

    _install_signal_handlers()

    do_clean = "--fill" not in args
    do_fill = "--clean" not in args and not dry_run

    if do_clean:
        run_clean_phase(dry_run=dry_run)
    if do_fill and not _shutdown_requested.is_set():
        run_fill_phase()


if __name__ == "__main__":
    main()
