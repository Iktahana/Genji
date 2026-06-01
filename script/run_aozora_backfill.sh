#!/usr/bin/env bash
#
# 青空文庫の頻度を解析し、既存 data/**/*.json の meta.frequencies に
# in-place で注入するワンショットスクリプト。
#
# 使い方:
#   bash script/run_aozora_backfill.sh            # 解析 → data/ へ書き込み
#   bash script/run_aozora_backfill.sh --dry-run  # 件数確認のみ（書き込まない）
#
# 環境変数で上書き可:
#   PYTHON=/usr/local/bin/python3  AOZORA_DIR=/tmp/aozorabunko_text  WORKERS=8
#
set -euo pipefail

PYTHON="${PYTHON:-/usr/local/bin/python3}"
AOZORA_DIR="${AOZORA_DIR:-/tmp/aozorabunko_text}"
WORKERS="${WORKERS:-8}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> 依存インストール (sudachipy / sudachidict_core)"
"$PYTHON" -m pip install -r requirements.txt

if [ ! -d "$AOZORA_DIR" ]; then
  echo "==> 青空文庫コーパス取得 (初回のみ・数GB): $AOZORA_DIR"
  git clone --depth 1 https://github.com/aozorahack/aozorabunko_text "$AOZORA_DIR"
else
  echo "==> 既存コーパスを使用: $AOZORA_DIR"
fi

echo "==> 頻度解析 + data/ 注入  (追加引数: ${*:-なし})"
"$PYTHON" script/backfill_frequency.py \
  --aozora-dir "$AOZORA_DIR" \
  --workers "$WORKERS" \
  "$@"

echo
echo "完了。差分を確認してコミットしてください:"
echo "  git add data"
echo "  git commit -m 'data: backfill aozora frequencies into meta.frequencies'"
echo "  git push"
