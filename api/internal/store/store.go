// Package store は genji.db (SQLite + FTS5) への read-only アクセスを提供する。
//
// 見出し語・読みの完全一致、FTS5 全文検索、ランダム取得などのクエリを提供する。
package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Iktahana/Genji/api/internal/api"
	_ "github.com/mattn/go-sqlite3"
)

// ErrNotFound はエントリが存在しない場合に返る。
var ErrNotFound = errors.New("not found")

// Store は DB 接続をラップする。
type Store struct {
	db *sql.DB
}

// Open は SQLite DB を read-only で開く。
func Open(path string) (*Store, error) {
	// mode=ro で read-only、immutable=1 は使わない（更新検知のため通常 ro）。
	dsn := fmt.Sprintf("file:%s?mode=ro&_query_only=1", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close は DB 接続を閉じる。
func (s *Store) Close() error { return s.db.Close() }

// Ping はヘルスチェック用に DB 疎通を確認する。
func (s *Store) Ping() error { return s.db.Ping() }

// entryFromRawJSON は entries.raw_json をパースして完全な Entry を返す。
func entryFromRawJSON(raw string) (api.Entry, error) {
	var e api.Entry
	err := json.Unmarshal([]byte(raw), &e)
	return e, err
}

// GetByUUID は UUID でエントリを1件取得する。
func (s *Store) GetByUUID(uuid string) (api.Entry, error) {
	var raw string
	err := s.db.QueryRow(`SELECT raw_json FROM entries WHERE uuid = ?`, uuid).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return api.Entry{}, ErrNotFound
	}
	if err != nil {
		return api.Entry{}, err
	}
	return entryFromRawJSON(raw)
}

// LookupByEntry は見出し語の完全一致でエントリ一覧を取得する。
func (s *Store) LookupByEntry(word string) ([]api.Entry, error) {
	return s.lookup(`SELECT raw_json FROM entries WHERE entry = ? ORDER BY uuid`, word)
}

// LookupByReading は読みの完全一致でエントリ一覧を取得する。
func (s *Store) LookupByReading(reading string) ([]api.Entry, error) {
	return s.lookup(`SELECT raw_json FROM entries WHERE reading_primary = ? ORDER BY entry`, reading)
}

func (s *Store) lookup(query, arg string) ([]api.Entry, error) {
	rows, err := s.db.Query(query, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make([]api.Entry, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		e, err := entryFromRawJSON(raw)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// Random はランダムなエントリを count 件取得する。
func (s *Store) Random(count int) ([]api.Entry, error) {
	rows, err := s.db.Query(`SELECT raw_json FROM entries ORDER BY RANDOM() LIMIT ?`, count)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make([]api.Entry, 0, count)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		e, err := entryFromRawJSON(raw)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// SearchEntries は見出し語・読みを FTS5 で検索する。
func (s *Store) SearchEntries(q string, limit int) ([]api.SearchResult, error) {
	match := sanitizeFTS(q)
	rows, err := s.db.Query(`
		SELECT e.uuid, e.entry, e.reading_primary, e.pos,
		       snippet(fts_entries, 1, '<b>', '</b>', '...', 32) AS match_highlight
		FROM fts_entries fts
		JOIN entries e ON e.uuid = fts.uuid
		WHERE fts_entries MATCH ?
		LIMIT ?`, match, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]api.SearchResult, 0)
	for rows.Next() {
		var (
			uuid, entry string
			reading     sql.NullString
			posJSON     sql.NullString
			highlight   sql.NullString
		)
		if err := rows.Scan(&uuid, &entry, &reading, &posJSON, &highlight); err != nil {
			return nil, err
		}
		r := api.SearchResult{Uuid: uuid, Entry: entry}
		if reading.Valid {
			r.ReadingPrimary = &reading.String
		}
		if pos := parseStringArray(posJSON); pos != nil {
			r.Pos = &pos
		}
		if highlight.Valid {
			r.MatchHighlight = &highlight.String
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

// SearchDefinitions は語釈(gloss)を FTS5 で検索する。
func (s *Store) SearchDefinitions(q string, limit int) ([]api.DefinitionSearchResult, error) {
	match := sanitizeFTS(q)
	rows, err := s.db.Query(`
		SELECT e.uuid, e.entry, e.reading_primary, d.gloss, d.def_index,
		       snippet(fts_definitions, 1, '<b>', '</b>', '...', 64) AS match_highlight
		FROM fts_definitions fts
		JOIN definitions d ON d.entry_uuid = fts.entry_uuid
		JOIN entries e ON e.uuid = d.entry_uuid
		WHERE fts_definitions MATCH ?
		LIMIT ?`, match, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]api.DefinitionSearchResult, 0)
	for rows.Next() {
		var (
			uuid, entry string
			reading     sql.NullString
			gloss       sql.NullString
			defIndex    sql.NullInt64
			highlight   sql.NullString
		)
		if err := rows.Scan(&uuid, &entry, &reading, &gloss, &defIndex, &highlight); err != nil {
			return nil, err
		}
		r := api.DefinitionSearchResult{Uuid: uuid, Entry: entry}
		if reading.Valid {
			r.ReadingPrimary = &reading.String
		}
		if gloss.Valid {
			r.Gloss = &gloss.String
		}
		if defIndex.Valid {
			idx := int(defIndex.Int64)
			r.DefIndex = &idx
		}
		if highlight.Valid {
			r.MatchHighlight = &highlight.String
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

// Metadata は _metadata テーブルを key/value で取得し Metadata 型に詰める。
func (s *Store) Metadata() (api.Metadata, error) {
	rows, err := s.db.Query(`SELECT key, value FROM _metadata`)
	if err != nil {
		return api.Metadata{}, err
	}
	defer rows.Close()

	var m api.Metadata
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return api.Metadata{}, err
		}
		val := v
		switch k {
		case "version":
			m.Version = &val
		case "commit":
			m.Commit = &val
		case "commit_short":
			m.CommitShort = &val
		case "branch":
			m.Branch = &val
		case "repository":
			m.Repository = &val
		case "build_date":
			m.BuildDate = &val
		case "entry_count":
			m.EntryCount = &val
		}
	}
	return m, rows.Err()
}

// parseStringArray は JSON 配列文字列を []string に変換する。失敗時は nil。
func parseStringArray(ns sql.NullString) []string {
	if !ns.Valid || ns.String == "" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(ns.String), &out); err != nil {
		return nil
	}
	return out
}

// sanitizeFTS はユーザー入力を安全な FTS5 MATCH 式に変換する。
//
// 空白で分割し、各トークンをダブルクオートで囲んで（内部の " は "" にエスケープ）
// フレーズ化する。これにより -, *, :, NEAR などの FTS 構文記号による
// 構文エラーや意図しない挙動を防ぐ。複数トークンは暗黙の AND になる。
func sanitizeFTS(q string) string {
	fields := strings.Fields(q)
	if len(fields) == 0 {
		// 空クエリは決してマッチしないトークンにする。
		return `""`
	}
	quoted := make([]string, 0, len(fields))
	for _, f := range fields {
		f = strings.ReplaceAll(f, `"`, `""`)
		quoted = append(quoted, `"`+f+`"`)
	}
	return strings.Join(quoted, " ")
}

// SitemapRow は sitemap 用の軽量な語彙行。Heat は呼び出し側（heat 連携）で設定する。
type SitemapRow struct {
	UUID           string
	Entry          string
	ReadingPrimary *string
	UpdatedAt      *string
	Heat           *float64
}

// CountEntries は収録語彙の総数を返す。
func (s *Store) CountEntries() (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM entries`).Scan(&n)
	return n, err
}

// AllUUIDs は全エントリの uuid を返す（heat ランキングの土台 seed 用）。
func (s *Store) AllUUIDs() ([]string, error) {
	rows, err := s.db.Query(`SELECT uuid FROM entries`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	uuids := make([]string, 0, 1024)
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			return nil, err
		}
		uuids = append(uuids, u)
	}
	return uuids, rows.Err()
}

// FreqEntry は uuid と meta.frequencies の合計出現回数。熱度の seed に使う。
type FreqEntry struct {
	UUID    string
	FreqSum int64
}

// EntryFreqSums は全エントリの uuid と meta.frequencies 値合計を返す。
// frequencies が無い／meta が NULL の場合は合計 0。
func (s *Store) EntryFreqSums() ([]FreqEntry, error) {
	rows, err := s.db.Query(`
		SELECT uuid,
		       COALESCE((
		           SELECT SUM(CAST(value AS INTEGER))
		           FROM json_each(meta, '$.frequencies')
		       ), 0) AS freq_sum
		FROM entries`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]FreqEntry, 0, 1024)
	for rows.Next() {
		var e FreqEntry
		if err := rows.Scan(&e.UUID, &e.FreqSum); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// SitemapByUUIDs は指定 uuid 群の sitemap 行を取得する（順序は呼び出し側で復元する）。
func (s *Store) SitemapByUUIDs(uuids []string) (map[string]SitemapRow, error) {
	out := make(map[string]SitemapRow, len(uuids))
	if len(uuids) == 0 {
		return out, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(uuids)), ",")
	args := make([]any, len(uuids))
	for i, u := range uuids {
		args[i] = u
	}
	query := `SELECT uuid, entry, reading_primary, json_extract(meta, '$.updated_at')
	          FROM entries WHERE uuid IN (` + placeholders + `)`
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			r       SitemapRow
			reading sql.NullString
			updated sql.NullString
		)
		if err := rows.Scan(&r.UUID, &r.Entry, &reading, &updated); err != nil {
			return nil, err
		}
		if reading.Valid {
			r.ReadingPrimary = &reading.String
		}
		if updated.Valid {
			r.UpdatedAt = &updated.String
		}
		out[r.UUID] = r
	}
	return out, rows.Err()
}

// SitemapByFreq は Redis 無効時のフォールバック。freq_rank 昇順（無いものは後ろ）→ 見出し語順で返す。
func (s *Store) SitemapByFreq(limit, offset int) ([]SitemapRow, error) {
	rows, err := s.db.Query(`
		SELECT uuid, entry, reading_primary,
		       json_extract(meta, '$.updated_at') AS updated_at,
		       CAST(json_extract(meta, '$.freq_rank') AS INTEGER) AS freq
		FROM entries
		ORDER BY (freq IS NULL), freq ASC, entry ASC
		LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]SitemapRow, 0, limit)
	for rows.Next() {
		var (
			r       SitemapRow
			reading sql.NullString
			updated sql.NullString
			freq    sql.NullInt64
		)
		if err := rows.Scan(&r.UUID, &r.Entry, &reading, &updated, &freq); err != nil {
			return nil, err
		}
		if reading.Valid {
			r.ReadingPrimary = &reading.String
		}
		if updated.Valid {
			r.UpdatedAt = &updated.String
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
