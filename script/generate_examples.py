
import os
import json
import re
import subprocess
import time
import sys
import threading
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ──────────────────────────────────────────────────────────
# 設定
# ──────────────────────────────────────────────────────────

DATA_ROOT   = Path("data")
MAX_WORKERS = 30    # 並列実行エージェント数
REPORT_SEC  = 3.0   # 進捗表示の更新間隔（秒）

# AIモデル情報
MODELS = [
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.5-pro",
    "gemini-3-flash-preview",
    "gemini-3.1-pro-preview"
]

class ModelManager:
    def __init__(self, models):
        self.models = models
        self.current_index = 0
        self.lock = threading.Lock()
        self.failures_in_row = 0

    def get_current_model(self):
        with self.lock:
            return self.models[self.current_index]

    def switch_to_next_model(self):
        with self.lock:
            self.current_index = (self.current_index + 1) % len(self.models)
            self.failures_in_row += 1
            model = self.models[self.current_index]
            # 全モデル試した場合は少し待つ
            if self.failures_in_row >= len(self.models):
                print(f"\n[INFO] 全モデルのクォータ制限に達した可能性があります。60秒待機します...", flush=True)
                time.sleep(60)
                self.failures_in_row = 0
            return model

    def reset_failure_count(self):
        with self.lock:
            self.failures_in_row = 0

model_manager = ModelManager(MODELS)

# 低品質なテンプレートの定義（これらに該当する例文は破棄・再生成の対象）
JUNK_PATTERNS = [
# ... (rest of JUNK_PATTERNS)
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
    r"教科書の第三章は.*內容です",
    r"健康診断で.*相談しました",
    r"法的な観点から.*重要な問題です",
    r"アスリートは.*訓練しています",
    r"例句\d+",
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
    r"現代でも使用頻度が高い重要語彙です"
]

# ──────────────────────────────────────────────────────────
# 進捗表示（GitHub Actions 対応）
# ──────────────────────────────────────────────────────────

class Progress:
    IS_GHA = os.environ.get("GITHUB_ACTIONS") == "true"
    BAR_W  = 28

    @staticmethod
    def _bar(done: int, total: int) -> str:
        if total <= 0: return f"[{'░' * Progress.BAR_W}]  0.0%"
        pct    = min(done / total, 1.0)
        filled = round(pct * Progress.BAR_W)
        return f"[{'█' * filled}{'░' * (Progress.BAR_W - filled)}] {pct:5.1%}"

    @staticmethod
    def group(title: str) -> None:
        if Progress.IS_GHA: print(f"::group::{title}", flush=True)
        else: print(f"\n┌─ {title}", flush=True)

    @staticmethod
    def endgroup() -> None:
        if Progress.IS_GHA: print("::endgroup::", flush=True)

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

progress_lock   = threading.Lock()
updated_count   = 0
processed_count = 0
last_report_t   = 0

def clean_ansi(text: str) -> str:
    """出力から制御文字を削除"""
    return re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])').sub('', text)

def is_low_quality(examples: list) -> bool:
    """既存の例文がテンプレート等の低品質なものか判定"""
    if not examples: return True
    for ex in examples:
        txt = ex.get('text', '')
        if any(re.search(p, txt) for p in JUNK_PATTERNS): return True
    return False

def generate_examples_jp(entry: str, reading: str, gloss: str, pos: str) -> list:
    """Gemini CLIを使用して高品質な例文を生成。失敗時はモデルを切り替えてリトライ。"""
    max_retries = len(MODELS) * 2  # 各モデル2回ずつくらいは試せるように
    
    for _ in range(max_retries):
        current_model = model_manager.get_current_model()
        prompt = f"""
以下の日本語の單語について、國語辭典の掲載に適した、自然で實用的な例文を5〜8個作成してください。

【對象單語】
表記: {entry}
読み: {reading}
品詞: {pos}
意味: {gloss}

【作成ルール】
1. 汎用的なテンプレート表現（「生活に欠かせない」「重要です」等）は厳禁です。
2. その語が実際に使われる具体的なシーン（ニュース、専門分野、日常生活等）を想定してください。
3. 自然な日本語のコロケーション（語の繋がり）を重視してください。
4. 質を最優先してください。無理に多く作る必要はありません。難解な語の場合は3個程度でも構いません。
5. 感動詞や副詞は「」を用いた會話文形式にしてください。

【出力形式】
JSON配列形式のみを出力してください。
各オブジェクトは "text" キーと、以下の構造を持つ "citation" キーを含めてください。
"citation": {{
  "source": "幻辭AI",
  "author": "Gemini",
  "note": "{current_model}"
}}

出力例:
[
  {{
    "text": "具体的な例文1",
    "citation": {{ "source": "幻辭AI", "author": "Gemini", "note": "{current_model}" }}
  }}
]
"""
        try:
            res = subprocess.run(['gemini', '-m', current_model, '-p', prompt], 
                                 capture_output=True, text=True, encoding='utf-8', timeout=120)
            
            out = clean_ansi(res.stdout).strip()
            err = clean_ansi(res.stderr).strip()
            
            # クォータエラー（429）やその他のAPIエラーをチェック
            if "429" in err or "Quota exceeded" in err or "Rate limit" in err or "ModelNotFoundError" in err:
                # print(f"\n[INFO] モデル {current_model} で制限発生。次のモデルに切り替えます...", flush=True)
                model_manager.switch_to_next_model()
                continue

            match = re.search(r'\[\s*\{.*\}\s*\]', out, re.DOTALL)
            if match:
                try:
                    res_json = json.loads(match.group(0))
                    model_manager.reset_failure_count() # 成功したらリセット
                    return res_json
                except json.JSONDecodeError:
                    try:
                        fixed = re.sub(r',\s*\]', ']', match.group(0))
                        res_json = json.loads(fixed)
                        model_manager.reset_failure_count()
                        return res_json
                    except:
                        pass
            
            # JSONが見つからない場合や解析失敗も失敗とみなしてリトライ（モデル切り替えはしない）
            time.sleep(1)
            
        except subprocess.TimeoutExpired:
            model_manager.switch_to_next_model()
        except Exception:
            pass
            
    return None

def process_file(file_path: Path) -> bool:
    global updated_count, processed_count
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        modified = False
        for entry_obj in data:
            entry_text = entry_obj.get('entry', '')
            reading    = entry_obj.get('reading', {}).get('primary', '')
            pos        = ",".join(entry_obj.get('grammar', {}).get('pos', []))
            
            for definition in entry_obj.get('definitions', []):
                if 'examples' not in definition:
                    definition['examples'] = {'standard': [], 'literary': []}
                
                std_examples = definition['examples'].get('standard', [])
                
                # 品質チェック
                if is_low_quality(std_examples):
                    new_exs = generate_examples_jp(entry_text, reading, definition.get('gloss', ''), pos)
                    if new_exs:
                        # テンプレート混入チェック
                        valid_new = [ex for ex in new_exs if not any(re.search(p, ex.get('text', '')) for p in JUNK_PATTERNS)]
                        if valid_new:
                            definition['examples']['standard'] = valid_new
                            modified = True
        
        if modified:
            if 'meta' in data[0]:
                data[0]['meta']['updated_at'] = datetime.now(timezone.utc).isoformat() + 'Z'
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            with progress_lock: updated_count += 1
        
        with progress_lock: processed_count += 1
        return modified
    except Exception as e:
        print(f"\n[エラー] {file_path.name}: {e}")
        return False

# ──────────────────────────────────────────────────────────
# 実行
# ──────────────────────────────────────────────────────────

def main():
    Progress.group(f"例文の自動生成・品質改善プロセスを開始します (並列エージェント数={MAX_WORKERS})")
    Progress.step(f"使用モデル候補: {', '.join(MODELS)}")

    all_dirs  = sorted([d for d in DATA_ROOT.iterdir() if d.is_dir()], key=lambda x: x.name)
    all_files = []
    for d in all_dirs:
        all_files.extend(sorted(list(d.glob("*.json"))))

    total_files = len(all_files)
    Progress.step(f"スキャン対象: {total_files:,} ファイル")

    
    global last_report_t
    last_report_t = time.perf_counter()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_file, f): f for f in all_files}
        
        for future in as_completed(futures):
            now = time.perf_counter()
            if now - last_report_t >= REPORT_SEC:
                with progress_lock:
                    last_report_t = now
                    Progress.bar_line(processed_count, total_files, f"{processed_count:,} / {total_files:,} files (更新済み: {updated_count:,})")

    Progress.ok(f"プロセス完了。合計 {updated_count:,} 件のファイルを更新・最適化しました。")
    Progress.endgroup()

if __name__ == "__main__":
    main()
