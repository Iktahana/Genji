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
3.  **[青空文庫 (aozorahack/aozorabunko_text)](https://github.com/aozorahack/aozorabunko_text)** - 文学作品コーパス。形態素解析（Sudachi）による未収録語の抽出・出典別出現頻度、および実例（`examples.literary`）の付与に使用。
4.  **Genji Crawler System** - 独自のクローリングシステムによる最新のトレンド語彙および語法データ。

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
| `GET /v1/sitemap?page=1&page_size=1000` | 対象品詞（名詞・動詞・形容詞系列）の語を熱度順にページング（sitemap 用） |
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

## 🌸 青空文庫由来の語彙拡充（新語スケルトン・表記揺れ吸収）

青空文庫コーパスを形態素解析（Sudachi）して未収録語を抽出し、語彙を継続的に拡充しています（`script/extract_new_words.py` → `script/create_entries.py`）。この過程で次のデータ・メタフィールドが追加されます。

### 追加された `meta` フィールド

| フィールド | 型 | 意味 |
|---|---|---|
| `meta.frequencies` | `object` | 出典別の総出現回数。例: `{"aozora": 1234}`。 |
| `meta.variant_writings` | `string[]` | 既存エントリへ吸収した**異表記**（旧字体・歴史的仮名遣い）。例: `亜` に `["亞"]`、`居る` に `["ゐる"]`、`来` に `["來"]`。検索のエイリアスとして利用できます。 |
| `meta.needs_gloss` | `bool` | `true` の場合、語義（`definitions[*].gloss`）が**未生成のスケルトン**。読み・品詞・青空文庫実例（`examples.literary`）のみ確定済みで、語義は後続のバックフィルで埋められます。 |
| `meta.needs_reading` | `bool` | `true` の場合、読み（`reading.primary`）が未確定で暫定的に表記を格納。読みが付かない語は `reading` 別ディレクトリではなく `data/記号/` に格納されます。 |

> **⚠️ 注意:** `meta.needs_gloss = true` のエントリは `definitions[*].gloss` が空文字です。語義の有無で絞り込む場合は次のように除外してください。
>
> ```sql
> -- 語義が確定済みのエントリのみ
> SELECT e.entry, d.gloss FROM entries e
> JOIN definitions d ON d.entry_uuid = e.uuid
> WHERE d.gloss <> '' AND json_extract(e.meta, '$.needs_gloss') IS NULL;
> ```

旧字体・歴史的仮名遣いの表記は、可能な限り現代表記の見出しへ正規化（`ゐる→居る`・`來→来`・`氣→気` 等）して吸収し、原表記は `meta.variant_writings` に保持します。

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
