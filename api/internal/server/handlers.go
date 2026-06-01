// Package server は生成された StrictServerInterface を store/cache で実装する。
package server

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/Iktahana/Genji/api/internal/api"
	"github.com/Iktahana/Genji/api/internal/cache"
	"github.com/Iktahana/Genji/api/internal/heat"
	"github.com/Iktahana/Genji/api/internal/store"
)

// Handler は StrictServerInterface の実装。store・cache・heat を保持する。
type Handler struct {
	store *store.Store
	cache cache.Cache
	heat  heat.Service
	ttl   time.Duration
}

// NewHandler は Handler を生成する。
func NewHandler(s *store.Store, c cache.Cache, h heat.Service, ttl time.Duration) *Handler {
	return &Handler{store: s, cache: c, heat: h, ttl: ttl}
}

// 確実に StrictServerInterface を満たすことをコンパイル時に保証する。
var _ api.StrictServerInterface = (*Handler)(nil)

// cached はキャッシュ経由で値を取得する汎用ヘルパー。
// ヒット時はキャッシュの JSON を T にデコードして返す。ミス時は produce を呼び結果をキャッシュする。
func cached[T any](ctx context.Context, h *Handler, key string, produce func() (T, error)) (T, error) {
	var zero T
	if data, ok := h.cache.Get(ctx, key); ok {
		var v T
		if err := json.Unmarshal(data, &v); err == nil {
			return v, nil
		}
	}
	v, err := produce()
	if err != nil {
		return zero, err
	}
	if data, err := json.Marshal(v); err == nil {
		h.cache.Set(ctx, key, data, h.ttl)
	}
	return v, nil
}

// frontendURL は利用者向け前端サイト。
const frontendURL = "https://dict.illusions.app"

// apiEndpoints はトップページに掲載する主要エンドポイント一覧。
var apiEndpoints = []api.EndpointInfo{
	{Path: "/v1/metadata", Summary: "ビルドメタデータ取得"},
	{Path: "/v1/entries/{uuid}", Summary: "UUID でエントリ取得"},
	{Path: "/v1/lookup/entry", Summary: "見出し語で検索（完全一致）"},
	{Path: "/v1/lookup/reading", Summary: "読みで検索（完全一致）"},
	{Path: "/v1/search/entries", Summary: "見出し語・読みを全文検索"},
	{Path: "/v1/search/definitions", Summary: "語釈を全文検索"},
	{Path: "/v1/random", Summary: "ランダムな語を取得"},
	{Path: "/v1/sitemap", Summary: "全収録語彙を熱度順にページング取得"},
	{Path: "/healthz", Summary: "ヘルスチェック"},
}

// GetApiInfo は辞書バージョン等の基本情報とエンドポイント一覧を返す（トップページ）。
func (h *Handler) GetApiInfo(ctx context.Context, _ api.GetApiInfoRequestObject) (api.GetApiInfoResponseObject, error) {
	info, err := cached(ctx, h, "genji:v1:root", func() (api.ApiInfo, error) {
		m, err := h.store.Metadata()
		if err != nil {
			return api.ApiInfo{}, err
		}
		return api.ApiInfo{
			Name:        "『幻辞』 Genji API",
			Description: "幻辞プロジェクトの語彙データベース API（read-only）。約21万5千語を収録。",
			Version:     m.Version,
			EntryCount:  m.EntryCount,
			BuildDate:   m.BuildDate,
			CommitShort: m.CommitShort,
			Docs:        "/docs",
			Openapi:     "/openapi.yaml",
			Frontend:    frontendURL,
			Endpoints:   apiEndpoints,
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return api.GetApiInfo200JSONResponse(info), nil
}

// GetHealth は DB 疎通を確認する。
func (h *Handler) GetHealth(_ context.Context, _ api.GetHealthRequestObject) (api.GetHealthResponseObject, error) {
	cacheState := "disabled"
	if h.cache.Enabled() {
		cacheState = "enabled"
	}
	if err := h.store.Ping(); err != nil {
		return api.GetHealth503JSONResponse{Code: 503, Message: "database unavailable"}, nil
	}
	return api.GetHealth200JSONResponse{Status: "ok", Cache: &cacheState}, nil
}

// GetMetadata は _metadata テーブルを返す。
func (h *Handler) GetMetadata(ctx context.Context, _ api.GetMetadataRequestObject) (api.GetMetadataResponseObject, error) {
	m, err := cached(ctx, h, "genji:v1:metadata", h.store.Metadata)
	if err != nil {
		return nil, err
	}
	return api.GetMetadata200JSONResponse(m), nil
}

// GetEntryByUUID は UUID で1件取得する。
func (h *Handler) GetEntryByUUID(ctx context.Context, request api.GetEntryByUUIDRequestObject) (api.GetEntryByUUIDResponseObject, error) {
	e, err := cached(ctx, h, "genji:v1:entry:"+request.Uuid, func() (api.Entry, error) {
		return h.store.GetByUUID(request.Uuid)
	})
	if errors.Is(err, store.ErrNotFound) {
		return api.GetEntryByUUID404JSONResponse{Code: 404, Message: "entry not found"}, nil
	}
	if err != nil {
		return nil, err
	}
	h.heat.Hit(e.Uuid)
	return api.GetEntryByUUID200JSONResponse(e), nil
}

// LookupByEntry は見出し語の完全一致検索。
func (h *Handler) LookupByEntry(ctx context.Context, request api.LookupByEntryRequestObject) (api.LookupByEntryResponseObject, error) {
	list, err := cached(ctx, h, "genji:v1:lookup_entry:"+request.Params.Word, func() (api.EntryList, error) {
		entries, err := h.store.LookupByEntry(request.Params.Word)
		if err != nil {
			return api.EntryList{}, err
		}
		return api.EntryList{Count: len(entries), Entries: entries}, nil
	})
	if err != nil {
		return nil, err
	}
	h.heat.Hit(entryUUIDs(list.Entries)...)
	return api.LookupByEntry200JSONResponse(list), nil
}

// LookupByReading は読みの完全一致検索。
func (h *Handler) LookupByReading(ctx context.Context, request api.LookupByReadingRequestObject) (api.LookupByReadingResponseObject, error) {
	list, err := cached(ctx, h, "genji:v1:lookup_reading:"+request.Params.Reading, func() (api.EntryList, error) {
		entries, err := h.store.LookupByReading(request.Params.Reading)
		if err != nil {
			return api.EntryList{}, err
		}
		return api.EntryList{Count: len(entries), Entries: entries}, nil
	})
	if err != nil {
		return nil, err
	}
	h.heat.Hit(entryUUIDs(list.Entries)...)
	return api.LookupByReading200JSONResponse(list), nil
}

// SearchEntries は見出し語・読みの全文検索。
func (h *Handler) SearchEntries(ctx context.Context, request api.SearchEntriesRequestObject) (api.SearchEntriesResponseObject, error) {
	limit := clamp(request.Params.Limit, 50, 1, 200)
	q := request.Params.Q
	key := "genji:v1:search_entries:" + itoa(limit) + ":" + q
	list, err := cached(ctx, h, key, func() (api.SearchResultList, error) {
		results, err := h.store.SearchEntries(q, limit)
		if err != nil {
			return api.SearchResultList{}, err
		}
		return api.SearchResultList{Count: len(results), Query: &q, Results: results}, nil
	})
	if err != nil {
		return nil, err
	}
	return api.SearchEntries200JSONResponse(list), nil
}

// SearchDefinitions は語釈の全文検索。
func (h *Handler) SearchDefinitions(ctx context.Context, request api.SearchDefinitionsRequestObject) (api.SearchDefinitionsResponseObject, error) {
	limit := clamp(request.Params.Limit, 50, 1, 200)
	q := request.Params.Q
	key := "genji:v1:search_definitions:" + itoa(limit) + ":" + q
	list, err := cached(ctx, h, key, func() (api.DefinitionSearchResultList, error) {
		results, err := h.store.SearchDefinitions(q, limit)
		if err != nil {
			return api.DefinitionSearchResultList{}, err
		}
		return api.DefinitionSearchResultList{Count: len(results), Query: &q, Results: results}, nil
	})
	if err != nil {
		return nil, err
	}
	return api.SearchDefinitions200JSONResponse(list), nil
}

// RandomEntries はランダム取得。毎回異なるべきなのでキャッシュしない。
func (h *Handler) RandomEntries(_ context.Context, request api.RandomEntriesRequestObject) (api.RandomEntriesResponseObject, error) {
	count := clamp(request.Params.Count, 5, 1, 100)
	entries, err := h.store.Random(count)
	if err != nil {
		return nil, err
	}
	return api.RandomEntries200JSONResponse(api.EntryList{Count: len(entries), Entries: entries}), nil
}

// GetSitemap は全収録語彙を熱度降順でページングして返す。
// Redis 有効時は heat ランキング、無効時は freq_rank→見出し語の安定順（heat=null）。
func (h *Handler) GetSitemap(ctx context.Context, request api.GetSitemapRequestObject) (api.GetSitemapResponseObject, error) {
	page := clamp(request.Params.Page, 1, 1, math.MaxInt32)
	size := clamp(request.Params.PageSize, 1000, 1, 50000)
	offset := (page - 1) * size

	// heat はゆっくり更新されるため、ページ結果は短 TTL でキャッシュする。
	key := "genji:v1:sitemap:" + itoa(size) + ":" + itoa(page)
	out, err := cached(ctx, h, key, func() (api.SitemapPage, error) {
		return h.buildSitemap(ctx, page, size, offset)
	})
	if err != nil {
		return nil, err
	}
	return api.GetSitemap200JSONResponse(out), nil
}

func (h *Handler) buildSitemap(ctx context.Context, page, size, offset int) (api.SitemapPage, error) {
	var (
		items []api.SitemapItem
		total int
	)

	if h.heat.Enabled() {
		ranked, t, err := h.heat.Page(ctx, offset, size)
		if err != nil {
			return api.SitemapPage{}, err
		}
		total = t
		uuids := make([]string, len(ranked))
		for i, r := range ranked {
			uuids[i] = r.UUID
		}
		rows, err := h.store.SitemapByUUIDs(uuids)
		if err != nil {
			return api.SitemapPage{}, err
		}
		// ランキングの順序を維持して組み立てる。
		items = make([]api.SitemapItem, 0, len(ranked))
		for _, r := range ranked {
			row, ok := rows[r.UUID]
			if !ok {
				continue // DB に無い uuid（バージョン差異等）はスキップ
			}
			heatVal := r.Heat
			items = append(items, api.SitemapItem{
				Uuid:           row.UUID,
				Entry:          row.Entry,
				ReadingPrimary: row.ReadingPrimary,
				UpdatedAt:      row.UpdatedAt,
				Heat:           &heatVal,
			})
		}
	} else {
		t, err := h.store.CountSitemapEntries()
		if err != nil {
			return api.SitemapPage{}, err
		}
		total = t
		rows, err := h.store.SitemapByFreq(size, offset)
		if err != nil {
			return api.SitemapPage{}, err
		}
		items = make([]api.SitemapItem, 0, len(rows))
		for _, row := range rows {
			items = append(items, api.SitemapItem{
				Uuid:           row.UUID,
				Entry:          row.Entry,
				ReadingPrimary: row.ReadingPrimary,
				UpdatedAt:      row.UpdatedAt,
				Heat:           nil,
			})
		}
	}

	totalPages := 0
	if size > 0 {
		totalPages = (total + size - 1) / size
	}
	return api.SitemapPage{
		Page:       page,
		PageSize:   size,
		Total:      total,
		TotalPages: totalPages,
		Items:      items,
	}, nil
}

// entryUUIDs は entry スライスから uuid を抽出する（heat カウント用）。
func entryUUIDs(entries []api.Entry) []string {
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.Uuid != "" {
			ids = append(ids, e.Uuid)
		}
	}
	return ids
}

// clamp は *int の値を [min, max] に収める。nil なら def を使う。
func clamp(v *int, def, min, max int) int {
	n := def
	if v != nil {
		n = *v
	}
	if n < min {
		return min
	}
	if n > max {
		return max
	}
	return n
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
