package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3"

	"github.com/Iktahana/Genji/api/internal/api"
	"github.com/Iktahana/Genji/api/internal/cache"
	"github.com/Iktahana/Genji/api/internal/store"
)

// memCache はテスト用の計測可能なインメモリキャッシュ。
type memCache struct {
	mu       sync.Mutex
	data     map[string][]byte
	getCalls int
	setCalls int
}

func newMemCache() *memCache { return &memCache{data: map[string][]byte{}} }

func (m *memCache) Get(_ context.Context, key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getCalls++
	v, ok := m.data[key]
	return v, ok
}

func (m *memCache) Set(_ context.Context, key string, val []byte, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setCalls++
	cp := make([]byte, len(val))
	copy(cp, val)
	m.data[key] = cp
}

func (m *memCache) Enabled() bool  { return true }
func (m *memCache) Close() error   { return nil }
func (m *memCache) keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ks := make([]string, 0, len(m.data))
	for k := range m.data {
		ks = append(ks, k)
	}
	return ks
}

const sampleRawJSON = `{
  "uuid": "u1", "entry": "雪",
  "reading": {"primary": "ゆき", "alternatives": [], "is_heteronym": false},
  "grammar": {"pos": ["名詞"]},
  "definitions": [{"index": 1, "gloss": "snow", "register": "standard"}],
  "relations": {"homophones": [], "synonyms": [], "antonyms": [], "related": []},
  "meta": {"version": "1.0.0", "source": "test", "updated_at": "2026-01-01T00:00:00Z"}
}`

func buildTestDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	schema := `
	CREATE TABLE entries (uuid TEXT PRIMARY KEY, entry TEXT NOT NULL, reading_primary TEXT,
		reading_alternatives TEXT, is_heteronym INTEGER DEFAULT 0, pos TEXT, ctype TEXT,
		inflections TEXT, relations TEXT, meta TEXT, raw_json TEXT NOT NULL);
	CREATE TABLE definitions (id INTEGER PRIMARY KEY AUTOINCREMENT, entry_uuid TEXT NOT NULL,
		def_index INTEGER, gloss TEXT, register TEXT, nuance TEXT, scenarios TEXT,
		sensory_tags TEXT, collocations TEXT, examples TEXT);
	CREATE TABLE _metadata (key TEXT PRIMARY KEY, value TEXT);
	CREATE VIRTUAL TABLE fts_entries USING fts5(uuid UNINDEXED, entry, reading_primary, tokenize='unicode61');
	CREATE VIRTUAL TABLE fts_definitions USING fts5(entry_uuid UNINDEXED, gloss, tokenize='unicode61');`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO entries (uuid, entry, reading_primary, pos, raw_json) VALUES (?,?,?,?,?)`,
		"u1", "雪", "ゆき", `["名詞"]`, sampleRawJSON); err != nil {
		t.Fatalf("insert: %v", err)
	}
	db.Exec(`INSERT INTO definitions (entry_uuid, def_index, gloss) VALUES ('u1',1,'snow')`)
	db.Exec(`INSERT INTO fts_entries (uuid, entry, reading_primary) VALUES ('u1','雪','ゆき')`)
	db.Exec(`INSERT INTO fts_definitions (entry_uuid, gloss) VALUES ('u1','snow')`)
	db.Exec(`INSERT INTO _metadata (key,value) VALUES ('version','e2e'),('entry_count','1')`)
	return path
}

// newTestServer はテスト用の gin router と memCache を返す。
func newTestServer(t *testing.T, c cache.Cache) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)

	st, err := store.Open(buildTestDB(t))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	h := NewHandler(st, c, time.Minute)
	r := gin.New()
	api.RegisterDocs(r)
	api.RegisterHandlers(r, api.NewStrictHandler(h, nil))
	return r
}

func doGet(t *testing.T, r http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestHealthz(t *testing.T) {
	r := newTestServer(t, cache.NoopCache{})
	w := doGet(t, r, "/healthz")
	if w.Code != 200 {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var h api.Health
	if err := json.Unmarshal(w.Body.Bytes(), &h); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if h.Status != "ok" {
		t.Errorf("status = %q, want ok", h.Status)
	}
	if h.Cache == nil || *h.Cache != "disabled" {
		t.Errorf("cache = %v, want disabled", h.Cache)
	}
}

func TestHealthzCacheEnabled(t *testing.T) {
	r := newTestServer(t, newMemCache())
	w := doGet(t, r, "/healthz")
	var h api.Health
	json.Unmarshal(w.Body.Bytes(), &h)
	if h.Cache == nil || *h.Cache != "enabled" {
		t.Errorf("cache = %v, want enabled", h.Cache)
	}
}

func TestLookupByEntryEndpoint(t *testing.T) {
	r := newTestServer(t, cache.NoopCache{})
	w := doGet(t, r, "/v1/lookup/entry?word=雪")
	if w.Code != 200 {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	var list api.EntryList
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if list.Count != 1 || len(list.Entries) != 1 {
		t.Fatalf("count = %d, want 1", list.Count)
	}
	if list.Entries[0].Entry != "雪" {
		t.Errorf("entry = %q, want 雪", list.Entries[0].Entry)
	}
}

func TestLookupByReadingEndpoint(t *testing.T) {
	r := newTestServer(t, cache.NoopCache{})
	w := doGet(t, r, "/v1/lookup/reading?reading=ゆき")
	if w.Code != 200 {
		t.Fatalf("status = %d", w.Code)
	}
	var list api.EntryList
	json.Unmarshal(w.Body.Bytes(), &list)
	if list.Count != 1 {
		t.Errorf("count = %d, want 1", list.Count)
	}
}

func TestSearchEntriesEndpoint(t *testing.T) {
	r := newTestServer(t, cache.NoopCache{})
	w := doGet(t, r, "/v1/search/entries?q=雪")
	if w.Code != 200 {
		t.Fatalf("status = %d", w.Code)
	}
	var list api.SearchResultList
	json.Unmarshal(w.Body.Bytes(), &list)
	if list.Count != 1 || list.Results[0].MatchHighlight == nil {
		t.Errorf("unexpected results: %+v", list)
	}
}

func TestSearchDefinitionsEndpoint(t *testing.T) {
	r := newTestServer(t, cache.NoopCache{})
	w := doGet(t, r, "/v1/search/definitions?q=snow")
	if w.Code != 200 {
		t.Fatalf("status = %d", w.Code)
	}
	var list api.DefinitionSearchResultList
	json.Unmarshal(w.Body.Bytes(), &list)
	if list.Count != 1 || list.Results[0].Gloss == nil || *list.Results[0].Gloss != "snow" {
		t.Errorf("unexpected results: %+v", list)
	}
}

func TestGetEntryByUUIDEndpoint(t *testing.T) {
	r := newTestServer(t, cache.NoopCache{})
	w := doGet(t, r, "/v1/entries/u1")
	if w.Code != 200 {
		t.Fatalf("status = %d", w.Code)
	}
	var e api.Entry
	json.Unmarshal(w.Body.Bytes(), &e)
	if e.Uuid != "u1" {
		t.Errorf("uuid = %q, want u1", e.Uuid)
	}
}

func TestGetEntryByUUIDNotFound(t *testing.T) {
	r := newTestServer(t, cache.NoopCache{})
	w := doGet(t, r, "/v1/entries/missing")
	if w.Code != 404 {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	var e api.Error
	json.Unmarshal(w.Body.Bytes(), &e)
	if e.Code != 404 {
		t.Errorf("error code = %d, want 404", e.Code)
	}
}

func TestRandomEndpoint(t *testing.T) {
	r := newTestServer(t, cache.NoopCache{})
	w := doGet(t, r, "/v1/random?count=3")
	if w.Code != 200 {
		t.Fatalf("status = %d", w.Code)
	}
	var list api.EntryList
	json.Unmarshal(w.Body.Bytes(), &list)
	if list.Count != 1 { // テスト DB には1件のみ
		t.Errorf("count = %d, want 1", list.Count)
	}
}

func TestMetadataEndpoint(t *testing.T) {
	r := newTestServer(t, cache.NoopCache{})
	w := doGet(t, r, "/v1/metadata")
	if w.Code != 200 {
		t.Fatalf("status = %d", w.Code)
	}
	var m api.Metadata
	json.Unmarshal(w.Body.Bytes(), &m)
	if m.Version == nil || *m.Version != "e2e" {
		t.Errorf("version = %v, want e2e", m.Version)
	}
}

func TestDocsEndpoints(t *testing.T) {
	r := newTestServer(t, cache.NoopCache{})
	if w := doGet(t, r, "/openapi.yaml"); w.Code != 200 || w.Body.Len() == 0 {
		t.Errorf("/openapi.yaml status=%d len=%d", w.Code, w.Body.Len())
	}
	if w := doGet(t, r, "/docs"); w.Code != 200 {
		t.Errorf("/docs status=%d", w.Code)
	}
}

// TestCachePopulatedOnMiss は miss 時に結果がキャッシュへ書き込まれることを確認する。
func TestCachePopulatedOnMiss(t *testing.T) {
	mc := newMemCache()
	r := newTestServer(t, mc)

	doGet(t, r, "/v1/lookup/entry?word=雪")
	wantKey := "genji:v1:lookup_entry:雪"
	if _, ok := mc.data[wantKey]; !ok {
		t.Errorf("expected cache key %q to be set, keys=%v", wantKey, mc.keys())
	}
	if mc.setCalls != 1 {
		t.Errorf("setCalls = %d, want 1", mc.setCalls)
	}
}

// TestCacheServedOnHit は preload した値がそのまま返り、store を介さないことを確認する。
func TestCacheServedOnHit(t *testing.T) {
	mc := newMemCache()
	r := newTestServer(t, mc)

	key := "genji:v1:lookup_entry:雪"
	preload := api.EntryList{Count: 42, Entries: []api.Entry{{Uuid: "cached", Entry: "キャッシュ"}}}
	b, _ := json.Marshal(preload)
	mc.data[key] = b

	w := doGet(t, r, "/v1/lookup/entry?word=雪")
	var list api.EntryList
	json.Unmarshal(w.Body.Bytes(), &list)
	if list.Count != 42 || list.Entries[0].Uuid != "cached" {
		t.Errorf("response not served from cache: %+v", list)
	}
	if mc.setCalls != 0 {
		t.Errorf("setCalls = %d, want 0 (hit should not re-set)", mc.setCalls)
	}
}

// TestRandomNotCached は random がキャッシュされないことを確認する。
func TestRandomNotCached(t *testing.T) {
	mc := newMemCache()
	r := newTestServer(t, mc)

	doGet(t, r, "/v1/random?count=3")
	if mc.getCalls != 0 || mc.setCalls != 0 {
		t.Errorf("random touched cache: get=%d set=%d, want 0/0", mc.getCalls, mc.setCalls)
	}
}

func TestClamp(t *testing.T) {
	five := 5
	cases := []struct {
		v              *int
		def, min, max  int
		want           int
	}{
		{nil, 50, 1, 200, 50},
		{&five, 50, 1, 200, 5},
		{ptr(0), 50, 1, 200, 1},
		{ptr(999), 50, 1, 200, 200},
	}
	for _, c := range cases {
		if got := clamp(c.v, c.def, c.min, c.max); got != c.want {
			t.Errorf("clamp(%v,%d,%d,%d) = %d, want %d", c.v, c.def, c.min, c.max, got, c.want)
		}
	}
}

func ptr(n int) *int { return &n }
