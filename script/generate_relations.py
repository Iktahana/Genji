"""
Genji 関連語生成スクリプト（relations: homophones / synonyms / antonyms / related）

build_dictionary.py は relations を全エントリ空配列で初期化するだけなので、
本スクリプトでそれを埋める。

2フェーズ構成:
  Phase 1  同音語（homophones）  — 確定的生成（AI不要）
           ファイル名＝カタカナ読みなので、同読み・別表記の兄弟エントリが
           同一 JSON に同居している。それを拾うだけで 100% 正確に埋まる。
  Phase 2  類義語/対義語/関連語   — AI生成
           Gemini CLI と Claude CLI を1つのモデルプールに統合し、
           レート制限時に自動フェイルオーバー（generate_examples.py と同方式）。

使い方:
  python generate_relations.py                 # 両フェーズ実行
  python generate_relations.py --homophones    # 同音語のみ（高速・API不要）
  python generate_relations.py --ai            # AI関連語のみ
  python generate_relations.py --status        # 進捗表示
  python generate_relations.py --clear-checkpoint
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
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ──────────────────────────────────────────────────────────
# 設定
# ──────────────────────────────────────────────────────────

SCRIPT_DIR   = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_ROOT    = PROJECT_ROOT / "data"
CHECKPOINT_DIR = PROJECT_ROOT / ".checkpoints" / "relations"
MAX_WORKERS  = 3      # AI フェーズの並列ワーカー数
REPORT_SEC   = 3.0    # 進捗表示の更新間隔（秒）
CHECKPOINT_SAVE_INTERVAL = 20  # N バッチごとに checkpoint を自動保存
BATCH_SIZE   = 30     # 一回のAPI呼び出しで処理するエントリ数
AUTO_COMMIT_INTERVAL = 1800  # 自動コミットの間隔（秒 = 30分）
GLOBAL_RPM_INTERVAL  = 1.5   # グローバルリクエスト間隔（秒）≈ 40 RPM

# 関連語の上限（AIの暴走・水増し防止）
MAX_SYNONYMS = 6
MAX_ANTONYMS = 4
MAX_RELATED  = 6
MAX_HOMOPHONES = 30   # 同読みが極端に多い場合のみ切る

# AIモデルプール。"provider:model_id" 形式。
# Gemini（無料/高速）と Claude（CLI 経由・低速だが高品質）を統合。
MODELS = [
    "gemini:gemini-2.5-flash",
    "gemini:gemini-2.5-flash-lite",
    "gemini:gemini-3-flash-preview",
    "gemini:gemini-2.5-pro",
    "gemini:gemini-3.1-pro-preview",
    "claude:claude-haiku-4-5-20251001",
    "claude:claude-sonnet-4-6",
]

# Claude CLI はエージェント起動オーバーヘッドが大きい（~2分）。タイムアウトを長めに。
CLAUDE_TIMEOUT = 300
GEMINI_TIMEOUT = 180

# Claude CLI（Claude Code）はツールを持つエージェント。何もしないと関連語生成依頼に
# 対して「Python を実行してよいか」等と振る舞うため、system-prompt で純粋な生成器に
# 上書きし、全ツールを禁止して即時 JSON 出力させる。
CLAUDE_SYSTEM_PROMPT = (
    "あなたは日本語辞書の関連語生成器です。ツールは一切使用せず、"
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


class ModelManager:
    """
    複数モデル（Gemini + Claude）のレート制限を管理。
    generate_examples.py と同方式:
    - 各モデルのクールダウン時刻を記録し、利用可能なモデルを自動選択
    - BoundedSemaphore で同一モデルへの同時リクエスト数を 1 に制限
    - グローバル token bucket で全モデル合計の RPM を制御（~40 RPM）
    """
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
                    secs = int(cd - now)
                    result.append((m, f"cooldown {secs}s"))
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
            # tmp ファイル名はプロセス/スレッドごとにユニークにする。
            # 全スレッドが同一の '.tmp' を共有すると、あるスレッドの replace() が
            # tmp を消費した後に別スレッドの replace() がソース欠如で失敗する競合が起きる。
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


def _norm_list(value, cap: int, exclude: set[str]) -> list[str]:
    """AI出力の配列を正規化: 文字列のみ・前後空白除去・重複/自己/空除去・上限切り。"""
    if not isinstance(value, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for v in value:
        if not isinstance(v, str):
            continue
        w = v.strip()
        if not w or w in exclude or w in seen:
            continue
        seen.add(w)
        out.append(w)
        if len(out) >= cap:
            break
    return out


# ──────────────────────────────────────────────────────────
# Phase 1: 同音語（homophones）— 確定的生成
# ──────────────────────────────────────────────────────────

def compute_homophones(entries: list) -> dict[int, list[str]]:
    """
    同一ファイル内の同読み・別表記エントリを homophones として返す。
    returns: {entry_idx: [homophone表記, ...]}
    """
    by_reading: dict[str, list[str]] = {}
    for e in entries:
        reading = e.get('reading', {}).get('primary', '')
        entry_text = e.get('entry', '')
        if reading and entry_text:
            by_reading.setdefault(reading, [])
            if entry_text not in by_reading[reading]:
                by_reading[reading].append(entry_text)

    result: dict[int, list[str]] = {}
    for i, e in enumerate(entries):
        reading = e.get('reading', {}).get('primary', '')
        entry_text = e.get('entry', '')
        siblings = [w for w in by_reading.get(reading, []) if w != entry_text]
        result[i] = siblings[:MAX_HOMOPHONES]
    return result


def fill_homophones_file(fp: Path) -> bool:
    """1ファイルの homophones を埋める。変更があれば書き戻して True を返す。"""
    try:
        with open(fp, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"\n[エラー] {fp}: {e}")
        return False

    homo = compute_homophones(data)
    modified = False
    for i, entry_obj in enumerate(data):
        rel = entry_obj.setdefault('relations', {
            "homophones": [], "synonyms": [], "antonyms": [], "related": []
        })
        new_h = homo.get(i, [])
        if rel.get('homophones', []) != new_h:
            rel['homophones'] = new_h
            modified = True

    if modified:
        if data and 'meta' in data[0]:
            data[0]['meta']['updated_at'] = datetime.now(timezone.utc).isoformat() + 'Z'
        with open(fp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    return modified


def run_homophones_phase():
    cp = CheckpointManager("homophones")
    atexit.register(cp.save_checkpoint)

    Progress.group("Phase 1: 同音語（homophones）確定的生成")
    files = all_data_files()
    total = len(files)
    pending = [f for f in files if not cp.is_processed(_to_checkpoint_key(f))]
    Progress.step(f"総ファイル数: {total:,}  /  未処理: {len(pending):,}")

    if not pending:
        Progress.ok("全ファイル処理済みです。")
        Progress.endgroup()
        return

    updated = 0
    done = 0
    last_t = time.perf_counter()

    def worker(fp: Path):
        changed = fill_homophones_file(fp)
        cp.add_processed(_to_checkpoint_key(fp))
        if changed:
            cp.add_updated(_to_checkpoint_key(fp))
        return changed

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(worker, fp): fp for fp in pending}
        for fut in as_completed(futures):
            if _shutdown_requested.is_set():
                ex.shutdown(wait=False, cancel_futures=True)
                break
            try:
                if fut.result():
                    updated += 1
            except Exception as e:
                print(f"\n[ERROR] {e}", flush=True)
            done += 1
            now = time.perf_counter()
            if now - last_t >= REPORT_SEC:
                last_t = now
                Progress.bar_line(done, len(pending),
                                  f"{done:,}/{len(pending):,} (更新: {updated:,} files)")

    cp.save_checkpoint()
    Progress.ok(f"Phase 1 完了。{updated:,} ファイルに同音語を付与しました。")
    Progress.endgroup()


# ──────────────────────────────────────────────────────────
# Phase 2: 類義語/対義語/関連語 — AI生成
# ──────────────────────────────────────────────────────────

ai_cp = None  # AI フェーズ用 CheckpointManager（main で初期化）

progress_lock   = threading.Lock()
updated_count   = 0
processed_count = 0
total_batches   = 0


def _entry_needs_ai(entry_obj: dict) -> bool:
    rel = entry_obj.get('relations', {})
    return not (rel.get('synonyms') or rel.get('antonyms') or rel.get('related'))


def scan_pending_items(files_to_process: list) -> list[tuple]:
    """
    AI生成が必要なエントリを抽出。
    returns: [(fp, entry_idx, entry_text, reading, pos, gloss), ...]
    """
    pending = []
    skipped = 0
    for fp in files_to_process:
        try:
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)
            file_pending = []
            for ei, entry_obj in enumerate(data):
                if _entry_needs_ai(entry_obj):
                    entry_text = entry_obj.get('entry', '')
                    reading = entry_obj.get('reading', {}).get('primary', '')
                    pos = ",".join(entry_obj.get('grammar', {}).get('pos', []))
                    glosses = "; ".join(
                        d.get('gloss', '') for d in entry_obj.get('definitions', []) if d.get('gloss')
                    )
                    file_pending.append((fp, ei, entry_text, reading, pos, glosses))
            if file_pending:
                pending.extend(file_pending)
            else:
                ai_cp.add_processed(_to_checkpoint_key(fp))
                skipped += 1
        except Exception as e:
            print(f"\n[エラー] {fp}: {e}")
    if skipped > 0:
        Progress.step(f"スキップ（更新不要）: {skipped:,} files")
    return pending


def create_batches(pending_items: list) -> list[list[tuple]]:
    """BATCH_SIZE で切分。同一ファイルのエントリは同一バッチに収める。"""
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
    word_lines = "\n".join([
        f"{i+1}. 表記:{entry} 読み:{reading} 品詞:{pos} 意味:{gloss}"
        for i, (entry, reading, pos, gloss) in enumerate(items)
    ])
    return (
        '各語の類義語・対義語・関連語をJSON出力。'
        'キー="1","2",...、値={"synonyms":[],"antonyms":[],"related":[]}。'
        '値は実在する一般的な日本語の語のみ。該当語が無ければ空配列。創作・水増し禁止。'
        f'類義語≤{MAX_SYNONYMS} 対義語≤{MAX_ANTONYMS} 関連語≤{MAX_RELATED}。\n\n'
        f'{word_lines}\n\n'
        '{"1":{"synonyms":["..."],"antonyms":[],"related":["..."]}}'
    )


def _call_gemini(model_id: str, prompt: str) -> tuple[str, str]:
    """returns (response_text, status). status: ok / rate:<sec> / unavailable / error"""
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
    """claude CLI を print モードで呼ぶ。returns (response_text, status)."""
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


def generate_relations_batch(items: list) -> tuple[dict, str]:
    """
    複数語の関連語を一括生成。
    items: [(entry, reading, pos, gloss), ...]
    returns: ({"1": {"synonyms":[...],"antonyms":[...],"related":[...]}, ...}, model_key)
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

    batch_input = [(item[2], item[3], item[4], item[5]) for item in batch]
    batch_results, _model_key = generate_relations_batch(batch_input)

    # ファイル別に整理
    file_updates: dict[Path, list] = {}
    for j, item in enumerate(batch):
        key = str(j + 1)
        val = batch_results.get(key)
        if isinstance(val, dict):
            fp, ei = item[0], item[1]
            file_updates.setdefault(fp, []).append((ei, item[2], item[3], val))

    local_updated = 0
    all_files_in_batch = set(item[0] for item in batch)

    for fp, updates in file_updates.items():
        try:
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)

            modified = False
            for ei, entry_text, reading, val in updates:
                if ei >= len(data):
                    continue
                exclude = {entry_text, reading}
                syn = _norm_list(val.get('synonyms'), MAX_SYNONYMS, exclude)
                ant = _norm_list(val.get('antonyms'), MAX_ANTONYMS, exclude)
                rel = _norm_list(val.get('related'), MAX_RELATED, exclude)
                if not (syn or ant or rel):
                    continue
                relations = data[ei].setdefault('relations', {
                    "homophones": [], "synonyms": [], "antonyms": [], "related": []
                })
                relations['synonyms'] = syn
                relations['antonyms'] = ant
                relations['related'] = rel
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
                ['git', 'commit', '-m', f'data(relations): auto-commit ({ts})'],
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
    print("  Genji 関連語生成スクリプト（AIフェーズ）", flush=True)
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

def run_ai_phase():
    global ai_cp, total_batches
    ai_cp = CheckpointManager("ai_relations")
    atexit.register(ai_cp.save_checkpoint)

    startup_check()

    commit_thread = threading.Thread(target=auto_commit_worker, daemon=True, name="auto-commit")
    commit_thread.start()

    Progress.group(
        f"Phase 2: 類義語/対義語/関連語 AI生成 "
        f"(並列={MAX_WORKERS}, バッチ={BATCH_SIZE}語/APIコール)"
    )
    Progress.step(f"モデルプール: {', '.join(MODELS)}")

    all_files = all_data_files()
    total_files = len(all_files)
    files_to_process = [f for f in all_files if not ai_cp.is_processed(_to_checkpoint_key(f))]
    Progress.step(f"総ファイル数: {total_files:,}  /  スキャン対象: {len(files_to_process):,}")

    Progress.step("ファイルスキャン中...")
    pending_items = scan_pending_items(files_to_process)
    if not pending_items:
        Progress.ok("全エントリ処理済みです。")
        Progress.endgroup()
        return

    unique_files = len(set(item[0] for item in pending_items))
    Progress.step(f"処理対象: {len(pending_items):,} entries ({unique_files:,} files)")

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
        for name in ("homophones", "ai_relations"):
            cp = CheckpointManager(name)
            print(f"[{name}] processed={len(cp.processed_files)} updated={len(cp.updated_files)}")
        return

    if "--clear-checkpoint" in args:
        for name in ("homophones", "ai_relations"):
            CheckpointManager(name).clear_checkpoint()
        return

    if args and args[0] in ("-h", "--help"):
        print(__doc__)
        return

    _install_signal_handlers()

    do_homophones = "--ai" not in args
    do_ai = "--homophones" not in args

    if do_homophones:
        run_homophones_phase()
    if do_ai and not _shutdown_requested.is_set():
        run_ai_phase()


if __name__ == "__main__":
    main()
