# 『幻辞』 Genji API (Go + Gin)

幻辞プロジェクトの語彙データベース（約21万5千語）を提供する REST API の Go 実装。
**OpenAPI-first**（[`openapi.yaml`](./openapi.yaml) を真実の源とし、`oapi-codegen` でサーバーコードを生成）。

既存の Datasette API（ポート 8001）と並立する追加サービスで、read-only。
DB は単一の `genji.db`（SQLite + FTS5）を参照する。

## エンドポイント

| メソッド・パス | 説明 | パラメータ |
|---|---|---|
| `GET /healthz` | ヘルスチェック（DB 疎通） | — |
| `GET /v1/metadata` | ビルドメタデータ（version / commit / entry_count 等） | — |
| `GET /v1/entries/{uuid}` | UUID で1件取得 | path: `uuid` |
| `GET /v1/lookup/entry` | 見出し語の完全一致 | query: `word`（必須） |
| `GET /v1/lookup/reading` | 読み（かな）の完全一致 | query: `reading`（必須） |
| `GET /v1/search/entries` | 見出し語・読みの全文検索（FTS5） | query: `q`（必須）, `limit`（既定50, 上限200） |
| `GET /v1/search/definitions` | 語釈(gloss)の全文検索（FTS5） | query: `q`（必須）, `limit`（既定50, 上限200） |
| `GET /v1/random` | ランダム取得 | query: `count`（既定5, 上限100） |
| `GET /openapi.yaml` | OpenAPI 仕様（バイナリ同梱） | — |
| `GET /docs` | API ドキュメント（Redoc） | — |

### リクエスト例

```bash
curl 'http://localhost:8080/healthz'
curl 'http://localhost:8080/v1/lookup/entry?word=雪'
curl 'http://localhost:8080/v1/lookup/reading?reading=ゆき'
curl 'http://localhost:8080/v1/search/entries?q=雪'
curl 'http://localhost:8080/v1/search/definitions?q=snow&limit=10'
curl 'http://localhost:8080/v1/random?count=3'
curl 'http://localhost:8080/v1/metadata'
```

### レスポンス例（`/v1/lookup/entry?word=雪`）

```json
{
  "count": 1,
  "entries": [
    {
      "uuid": "…",
      "entry": "雪",
      "reading": { "primary": "ゆき", "alternatives": [], "is_heteronym": false },
      "grammar": { "pos": ["名詞"] },
      "definitions": [ { "index": 1, "gloss": "snow", "register": "standard" } ],
      "relations": { "homophones": [], "synonyms": [], "antonyms": [], "related": [] },
      "meta": { "version": "1.0.0", "source": "…", "updated_at": "…" }
    }
  ]
}
```

API 仕様の全体はサーバー起動後にブラウザで `http://localhost:8080/docs` から閲覧できる。

## 環境変数

| 変数 | 既定値 | 説明 |
|---|---|---|
| `GENJI_DB_PATH` | `genji.db` | SQLite DB へのパス（Docker イメージ内は `/data/genji.db`） |
| `PORT` | `8080` | HTTP リッスンポート |
| `GENJI_REDIS_ADDR` | （空） | Redis アドレス（例 `localhost:6379`）。**空ならキャッシュ無効** |
| `GENJI_REDIS_PASSWORD` | （空） | Redis パスワード |
| `GENJI_REDIS_DB` | `0` | Redis DB 番号 |
| `GENJI_CACHE_TTL` | `1h` | キャッシュ TTL（`time.ParseDuration` 形式、例 `30m`） |

> **キャッシュは任意**。`GENJI_REDIS_ADDR` が未設定、または Redis に接続できない場合は
> 自動的にキャッシュ無効（Noop）で動作する。API の可用性は Redis に依存しない。

## キャッシュの仕組み

read-only な辞書 API なので、クエリ結果を Redis にキャッシュできる。Redis は**必須ではない**。

### 有効化とフォールバック（`internal/cache`）

- `GENJI_REDIS_ADDR` が**空** → `NoopCache`（一切キャッシュしない）。
- アドレス指定あり → Redis クライアントを生成し **3 秒タイムアウトで PING** を実行。
  - 成功 → `RedisCache` を使用（ログ `cache: enabled`）。
  - **失敗 → エラーで落とさず `NoopCache` にフォールバック**（ログ `cache: redis ping failed ...`）。
    起動も応答も Redis 障害に巻き込まれない。

`GET /healthz` のレスポンスの `cache` フィールドで現在の状態（`enabled` / `disabled`）を確認できる。

### Read-through の流れ（`internal/server` の `cached()` ヘルパー）

1. `cache.Get(key)` を試す。**ヒットして JSON デコードに成功したら、その値をそのまま返す**（DB を引かない）。
2. ミス、またはデコード失敗 → store（DB）を引く。
3. 成功した結果を `json.Marshal` し、`cache.Set(key, value, TTL)` で保存してから返す。
4. `Set` の失敗はログのみで、レスポンスには影響しない。

キャッシュに保存される値は、各エンドポイントのレスポンス構造体（`EntryList` など）の **JSON バイト列**。

### キャッシュ対象とキー

| エンドポイント | キャッシュキー |
|---|---|
| `GET /v1/metadata` | `genji:v1:metadata` |
| `GET /v1/entries/{uuid}` | `genji:v1:entry:{uuid}` |
| `GET /v1/lookup/entry` | `genji:v1:lookup_entry:{word}` |
| `GET /v1/lookup/reading` | `genji:v1:lookup_reading:{reading}` |
| `GET /v1/search/entries` | `genji:v1:search_entries:{limit}:{q}` |
| `GET /v1/search/definitions` | `genji:v1:search_definitions:{limit}:{q}` |

**キャッシュしないエンドポイント**:

- `GET /v1/random` — 毎回異なる結果を返すべきなので、キャッシュを参照も保存もしない。
- `GET /healthz` — 常に DB の即時状態を反映する必要があるため。

### TTL

すべての `Set` に `GENJI_CACHE_TTL`（既定 `1h`）が適用される。データ更新（新しい `genji.db` のデプロイ）を
即時反映したい場合は、TTL を短くするか、デプロイ時に Redis をフラッシュする。
キーは `genji:v1:` プレフィックスで名前空間化されているため、`redis-cli --scan --pattern 'genji:v1:*'` で
まとめて確認・削除できる。

## ローカル開発

FTS5 を使うため、**`mattn/go-sqlite3` を `sqlite_fts5` ビルドタグ + cgo（`CGO_ENABLED=1`）でビルドする必要がある**。`Makefile` がこれを内包している。

```bash
make generate   # openapi.yaml からコード生成（*.gen.go と内蔵用 openapi.yaml を更新）
make build      # bin/genji-api をビルド
make test       # テスト実行
make vet        # go vet

# 起動（ローカルの genji.db を指定）
GENJI_DB_PATH=../genji.db make run
```

`genji.db` が手元に無い場合は、リポジトリ root で `make db`（`script/json_to_sqlite.py`）で生成できる。

## Docker

イメージには `genji.db` を**内蔵**する（外部マウント不要の self-contained イメージ）。
DB の供給は `GENJI_DB_SOURCE` で切り替える。

```bash
# 既定 (remote): GitHub Releases から最新 DB を取得して内蔵
docker build -t genji-api ./api
docker run -p 8080:8080 genji-api

# 特定バージョンの DB を内蔵
docker build --build-arg GENJI_DB_VERSION=2026.4.1.120000 -t genji-api ./api

# ローカルの genji.db を内蔵（api/genji.db を配置してから）
cp genji.db api/genji.db
docker build --build-arg GENJI_DB_SOURCE=local -t genji-api ./api
```

### Redis 併用（任意）

```bash
docker run -p 8080:8080 -e GENJI_REDIS_ADDR=host.docker.internal:6379 genji-api
```

`docker compose up` でも `api-go`（+ コメントアウト済みの `redis`）サービスとして起動できる。

## 公開イメージ（GHCR）

`main` への push で CI（`.github/workflows/build-and-release.yml` の `docker-api` ジョブ）が
multi-arch（amd64/arm64）でビルドし、辞書イメージと同じ日付バージョン体系で公開する。

```bash
docker pull ghcr.io/iktahana/genji-api:latest
# または特定版
docker pull ghcr.io/iktahana/genji-api:2026.4.1.120000
```

## アーキテクチャ

```
cmd/genji-api      エントリポイント（DB open, router, CORS, graceful shutdown）
internal/api       OpenAPI 生成コード（型・Gin strict server）+ /openapi.yaml・/docs 配信
internal/server    StrictServerInterface の実装（cache → store の順で参照）
internal/store     genji.db への read-only アクセス（SQL + FTS5 サニタイズ）
internal/cache     Cache 抽象（Redis 実装 / Noop 実装 + フォールバック）
internal/config    環境変数の読み込み
```
