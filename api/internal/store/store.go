// Package store は genji.db (SQLite + FTS5) への read-only アクセスを提供する。
//
// Datasette の metadata.yml に定義された canned query を Go に移植したもの。
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
