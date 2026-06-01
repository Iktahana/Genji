package store

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// sampleRawJSON は data/ の実エントリと同形の JSON。
const sampleRawJSON = `{
  "uuid": "11111111-1111-1111-1111-111111111111",
  "entry": "雪",
  "reading": {"primary": "ゆき", "alternatives": [], "is_heteronym": false},
  "grammar": {"pos": ["名詞"], "ctype": null, "inflections": null},
  "definitions": [
    {"index": 1, "gloss": "snow", "register": "standard", "nuance": null,
     "scenarios": [], "sensory_tags": {"colors": [], "temperature": null, "sounds": [], "emotions": []},
     "collocations": [], "examples": {"standard": [], "literary": []}}
  ],
  "relations": {"homophones": [], "synonyms": [], "antonyms": [], "related": []},
  "meta": {"version": "1.0.0", "source": "test", "updated_at": "2026-01-01T00:00:00Z"}
}`

// buildTestDB は数件のエントリを持つ一時 DB を作り、パスを返す。
func buildTestDB(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	schema := `
	CREATE TABLE entries (
		uuid TEXT PRIMARY KEY, entry TEXT NOT NULL, reading_primary TEXT,
		reading_alternatives TEXT, is_heteronym INTEGER DEFAULT 0,
		pos TEXT, ctype TEXT, inflections TEXT, relations TEXT, meta TEXT, raw_json TEXT NOT NULL
	);
	CREATE TABLE definitions (
		id INTEGER PRIMARY KEY AUTOINCREMENT, entry_uuid TEXT NOT NULL,
		def_index INTEGER, gloss TEXT, register TEXT, nuance TEXT,
		scenarios TEXT, sensory_tags TEXT, collocations TEXT, examples TEXT
	);
	CREATE TABLE _metadata (key TEXT PRIMARY KEY, value TEXT);
	CREATE VIRTUAL TABLE fts_entries USING fts5(uuid UNINDEXED, entry, reading_primary, tokenize='unicode61');
	CREATE VIRTUAL TABLE fts_definitions USING fts5(entry_uuid UNINDEXED, gloss, tokenize='unicode61');`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("schema: %v", err)
	}

	if _, err := db.Exec(
		`INSERT INTO entries (uuid, entry, reading_primary, pos, raw_json) VALUES (?, ?, ?, ?, ?)`,
		"11111111-1111-1111-1111-111111111111", "雪", "ゆき", `["名詞"]`, sampleRawJSON,
	); err != nil {
		t.Fatalf("insert entry: %v", err)
	}
	db.Exec(`INSERT INTO definitions (entry_uuid, def_index, gloss) VALUES (?, ?, ?)`,
		"11111111-1111-1111-1111-111111111111", 1, "snow")
	db.Exec(`INSERT INTO fts_entries (uuid, entry, reading_primary) VALUES (?, ?, ?)`,
		"11111111-1111-1111-1111-111111111111", "雪", "ゆき")
	db.Exec(`INSERT INTO fts_definitions (entry_uuid, gloss) VALUES (?, ?)`,
		"11111111-1111-1111-1111-111111111111", "snow")
	db.Exec(`INSERT INTO _metadata (key, value) VALUES ('version', 'test-1'), ('entry_count', '1')`)

	return path
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(buildTestDB(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestGetByUUID(t *testing.T) {
	s := openTestStore(t)
	e, err := s.GetByUUID("11111111-1111-1111-1111-111111111111")
	if err != nil {
		t.Fatalf("GetByUUID: %v", err)
	}
	if e.Entry != "雪" {
		t.Errorf("entry = %q, want 雪", e.Entry)
	}
	if e.Reading == nil || e.Reading.Primary == nil || *e.Reading.Primary != "ゆき" {
		t.Errorf("reading not parsed from raw_json: %+v", e.Reading)
	}
	if e.Definitions == nil || len(*e.Definitions) != 1 {
		t.Fatalf("definitions = %v, want 1", e.Definitions)
	}
}

func TestGetByUUIDNotFound(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.GetByUUID("does-not-exist"); err != ErrNotFound {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestLookupByEntry(t *testing.T) {
	s := openTestStore(t)
	entries, err := s.LookupByEntry("雪")
	if err != nil {
		t.Fatalf("LookupByEntry: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}
}

func TestLookupByReading(t *testing.T) {
	s := openTestStore(t)
	entries, err := s.LookupByReading("ゆき")
	if err != nil {
		t.Fatalf("LookupByReading: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}
}

func TestSearchEntries(t *testing.T) {
	s := openTestStore(t)
	results, err := s.SearchEntries("雪", 50)
	if err != nil {
		t.Fatalf("SearchEntries: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].Pos == nil || len(*results[0].Pos) != 1 || (*results[0].Pos)[0] != "名詞" {
		t.Errorf("pos not parsed: %+v", results[0].Pos)
	}
}

func TestSearchDefinitions(t *testing.T) {
	s := openTestStore(t)
	results, err := s.SearchDefinitions("snow", 50)
	if err != nil {
		t.Fatalf("SearchDefinitions: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].Gloss == nil || *results[0].Gloss != "snow" {
		t.Errorf("gloss = %v, want snow", results[0].Gloss)
	}
}

func TestSearchHandlesSpecialChars(t *testing.T) {
	s := openTestStore(t)
	// FTS 構文記号を含む入力でもエラーにならず（サニタイズで）安全に処理される。
	for _, q := range []string{`"`, `snow*`, `-foo`, `a OR b`, `   `} {
		if _, err := s.SearchEntries(q, 50); err != nil {
			t.Errorf("SearchEntries(%q) errored: %v", q, err)
		}
	}
}

func TestRandom(t *testing.T) {
	s := openTestStore(t)
	entries, err := s.Random(5)
	if err != nil {
		t.Fatalf("Random: %v", err)
	}
	if len(entries) != 1 { // テスト DB には1件のみ
		t.Fatalf("got %d entries, want 1", len(entries))
	}
}

func TestMetadata(t *testing.T) {
	s := openTestStore(t)
	m, err := s.Metadata()
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if m.Version == nil || *m.Version != "test-1" {
		t.Errorf("version = %v, want test-1", m.Version)
	}
	if m.EntryCount == nil || *m.EntryCount != "1" {
		t.Errorf("entry_count = %v, want 1", m.EntryCount)
	}
}

func TestSanitizeFTS(t *testing.T) {
	cases := map[string]string{
		"雪":       `"雪"`,
		"a b":     `"a" "b"`,
		`he"llo`:  `"he""llo"`,
		"   ":     `""`,
		"snow*":   `"snow*"`,
	}
	for in, want := range cases {
		if got := sanitizeFTS(in); got != want {
			t.Errorf("sanitizeFTS(%q) = %q, want %q", in, got, want)
		}
	}
}
