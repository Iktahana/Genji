# Genji Word Database

https://幻辞.com

幻辞.comで使用される全語彙データを収録した、オープンソースの日本語語彙データベースです。

本リポジトリでは、複数の信頼できるソースと独自のクローリングシステムを統合・加工した語彙データを、扱いやすい **SQLite 形式** で提供しています。

## 📦 特徴

- **SQLite 形式**: データベースファイルをダウンロードするだけで、すぐにアプリケーションに組み込み可能です。
- **自動更新**: Genji の自動クローリングシステムにより、不定期にデータがビルドされ、常に最新の語彙が反映されます。
- **軽量かつ高速**: インデックスが最適化されており、数十万件のデータから瞬時に検索が可能です。

## 📂 データソース (Data Sources)

本データベースは、以下の優れたリソースを統合し、独自の加工を施したものです：

1.  **[JMdict (yomidevs/jmdict-yomitan)](https://github.com/yomidevs/jmdict-yomitan)** - 広範な辞書定義および語彙データ。
2.  **[Japanese Word Frequency (hingston/japanese)](https://github.com/hingston/japanese)** - 語彙の頻度・優先順位データ。
3.  **Genji Crawler System** - 独自のクローリングシステムによる最新のトレンド語彙および語法データ。

## 🚀 使い方

### API

SQLite を直接使用できない環境向けに、Go + Gin 製の **OpenAPI-first** な REST API を提供しています（`api/`、read-only）。`genji.db` を内蔵した self-contained な Docker イメージで配布され、任意で Redis キャッシュを併用できます（未設定ならキャッシュ無効で動作）。

**公開Endpoint: `https://dict-api.illusions.app`**

| メソッド・パス | 説明 |
|---|---|
| `GET /` | API トップ情報（バージョン・収録語数・エンドポイント一覧） |
| `GET /v1/lookup/entry?word=雪` | 見出し語の完全一致 |
| `GET /v1/lookup/reading?reading=ゆき` | 読みの完全一致 |
| `GET /v1/search/entries?q=雪` | 見出し語・読みの全文検索（FTS5） |
| `GET /v1/search/definitions?q=snow` | 語釈の全文検索（FTS5） |
| `GET /v1/random?count=5` | ランダム取得 |
| `GET /v1/entries/{uuid}` | UUID で取得 |
| `GET /v1/sitemap?page=1&page_size=1000` | 全語彙を熱度順にページング（sitemap 用） |
| `GET /v1/metadata` ・ `GET /healthz` | メタデータ・ヘルスチェック |
| `GET /docs` | API ドキュメント（Redoc） |

```bash
docker pull ghcr.io/illusions-lab/genji-api:latest
docker run -p 8080:8080 ghcr.io/illusions-lab/genji-api:latest
# http://localhost:8080/docs で仕様を閲覧
```

詳細は [`api/README.md`](./api/README.md) を参照してください。

### SQLite を直接使用する

[Releases](/releases) ページから最新の `genji.db.gz` をダウンロードし、解凍して使用してください。

```bash
gunzip genji.db.gz
```

#### クエリ例
```sql
-- 見出し語で検索
SELECT raw_json FROM entries WHERE entry = '幻辞';

-- 読みで検索
SELECT e.entry, d.gloss FROM entries e
JOIN definitions d ON d.entry_uuid = e.uuid
WHERE e.reading_primary = 'ゆき';

-- 全文検索（FTS5）
SELECT e.entry, e.reading_primary FROM fts_entries fts
JOIN entries e ON e.uuid = fts.uuid
WHERE fts_entries MATCH '雪';

-- 頻度順に上位 10 件を取得する
SELECT entry, reading_primary, json_extract(meta, '$.freq_rank') AS freq
FROM entries WHERE freq IS NOT NULL
ORDER BY freq ASC LIMIT 10;
```

#### メタデータの確認

データベースにはビルド情報を格納する `_metadata` テーブルが含まれています。

```sql
SELECT * FROM _metadata;
-- version, commit, branch, repository, build_date, entry_count
```

### Docker

API の Docker イメージは GHCR で配布しています（`linux/amd64` / `linux/arm64` 対応、`genji.db` 内蔵）。

```bash
docker pull ghcr.io/illusions-lab/genji-api:latest
docker run -p 8080:8080 ghcr.io/illusions-lab/genji-api:latest
```

ローカルからビルドして起動する場合:

```bash
docker compose up -d --build
```

`http://localhost:8080` で API、`http://localhost:8080/docs` で仕様を閲覧できます。
詳細なビルドオプション（`GENJI_DB_SOURCE` や Redis 併用）は [`api/README.md`](./api/README.md) を参照してください。
