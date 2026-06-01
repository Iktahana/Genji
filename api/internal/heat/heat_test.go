package heat

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestHeat(t *testing.T) (*redisHeat, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	// wFreq=0: 既存テストはアクセス数のみで heat を検証する（頻度寄与は別テスト）。
	return &redisHeat{client: client, w30: 2, w365: 1, wFreq: 0, aggInterval: time.Minute}, mr
}

// seeds は uuid 群を頻度 0 の SeedEntry スライスに変換する。
func seeds(uuids ...string) []SeedEntry {
	s := make([]SeedEntry, len(uuids))
	for i, u := range uuids {
		s[i] = SeedEntry{UUID: u}
	}
	return s
}

func TestNewNilClientIsNoop(t *testing.T) {
	s := New(nil, 2, 1, 1, time.Minute)
	if s.Enabled() {
		t.Fatal("nil client should yield disabled heat service")
	}
	items, total, err := s.Page(context.Background(), 0, 10)
	if err != nil || total != 0 || items != nil {
		t.Errorf("Noop.Page = %v,%d,%v", items, total, err)
	}
}

func TestSeedPopulatesAllAtZero(t *testing.T) {
	h, _ := newTestHeat(t)
	ctx := context.Background()
	if err := h.Seed(ctx, seeds("a", "b", "c"), "v1"); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	n, _ := h.client.ZCard(ctx, entriesAllKey).Result()
	if n != 3 {
		t.Errorf("entries:all card = %d, want 3", n)
	}
	// 同バージョンの再 seed はスキップされる（変更なし）。
	if err := h.Seed(ctx, seeds("a"), "v1"); err != nil {
		t.Fatalf("re-seed: %v", err)
	}
	if n, _ := h.client.ZCard(ctx, entriesAllKey).Result(); n != 3 {
		t.Errorf("entries:all card after same-version reseed = %d, want 3", n)
	}
}

func TestAggregateWeightsAndOrder(t *testing.T) {
	h, _ := newTestHeat(t)
	ctx := context.Background()

	// d は seed されるが訪問なし（heat 0）。
	if err := h.Seed(ctx, seeds("a", "b", "c", "d"), "v1"); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	now := time.Now().UTC()
	today := dayKey(now)
	old := dayKey(now.AddDate(0, 0, -100)) // 直近30日外・365日内

	// a: 直近30日に3回 → c30=3, c365=3 → heat=2*3+1*3=9
	h.client.ZIncrBy(ctx, today, 3, "a")
	// b: 直近30日に1回 → heat=2*1+1*1=3
	h.client.ZIncrBy(ctx, today, 1, "b")
	// c: 100日前に5回（30日外） → c30=0, c365=5 → heat=5
	h.client.ZIncrBy(ctx, old, 5, "c")

	h.aggregate(ctx)

	want := map[string]float64{"a": 9, "b": 3, "c": 5, "d": 0}
	for uuid, exp := range want {
		got, err := h.client.ZScore(ctx, indexKey, uuid).Result()
		if err != nil {
			t.Fatalf("ZScore %s: %v", uuid, err)
		}
		if got != exp {
			t.Errorf("heat[%s] = %v, want %v", uuid, got, exp)
		}
	}

	// Page は heat 降順: a(9), c(5), b(3), d(0)
	items, total, err := h.Page(ctx, 0, 10)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if total != 4 {
		t.Errorf("total = %d, want 4", total)
	}
	wantOrder := []string{"a", "c", "b", "d"}
	if len(items) != 4 {
		t.Fatalf("got %d items, want 4", len(items))
	}
	for i, w := range wantOrder {
		if items[i].UUID != w {
			t.Errorf("items[%d] = %s, want %s (full: %+v)", i, items[i].UUID, w, items)
		}
	}
	if items[0].Heat != 9 {
		t.Errorf("top heat = %v, want 9", items[0].Heat)
	}
}

func TestAggregateIncludesFrequency(t *testing.T) {
	h, _ := newTestHeat(t)
	h.wFreq = 1 // 頻度の下地を有効化
	ctx := context.Background()

	// a: 頻度 999（未訪問）。b: 頻度 0。
	const freqA = 999
	if err := h.Seed(ctx, []SeedEntry{
		{UUID: "a", FreqSum: freqA},
		{UUID: "b", FreqSum: 0},
	}, "v1"); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	// b に直近30日 1 回アクセス → heat_b = 2*1+1*1 + 1*log1p(0) = 3。
	h.client.ZIncrBy(ctx, dayKey(time.Now().UTC()), 1, "b")
	h.aggregate(ctx)

	// a は未訪問でも頻度の下地で heat = wFreq*log1p(999) を持つ。
	heatA, err := h.client.ZScore(ctx, indexKey, "a").Result()
	if err != nil {
		t.Fatalf("ZScore a: %v", err)
	}
	wantA := math.Log1p(freqA) // ≈ 6.908
	if math.Abs(heatA-wantA) > 0.01 {
		t.Errorf("heat[a] = %v, want ≈%v (頻度のみ)", heatA, wantA)
	}
	// b は頻度 0 + アクセス 3 = 3。
	heatB, _ := h.client.ZScore(ctx, indexKey, "b").Result()
	if math.Abs(heatB-3.0) > 0.01 {
		t.Errorf("heat[b] = %v, want ≈3.0 (アクセスのみ)", heatB)
	}
}

func TestAggregateExcludesUnseeded(t *testing.T) {
	h, _ := newTestHeat(t)
	ctx := context.Background()

	// a のみ seed（= sitemap 対象品詞）。z は seed されない非対象語。
	if err := h.Seed(ctx, seeds("a"), "v1"); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	// 両方アクセスされる。
	today := dayKey(time.Now().UTC())
	h.client.ZIncrBy(ctx, today, 5, "a")
	h.client.ZIncrBy(ctx, today, 99, "z")

	h.aggregate(ctx)

	// a は index に存在する。
	if _, err := h.client.ZScore(ctx, indexKey, "a").Result(); err != nil {
		t.Errorf("a should be in index: %v", err)
	}
	// z は seed されていないので、アクセスされても index に漏れない。
	if _, err := h.client.ZScore(ctx, indexKey, "z").Result(); err != redis.Nil {
		t.Errorf("z should NOT be in index, err = %v", err)
	}
	if total, _ := h.client.ZCard(ctx, indexKey).Result(); total != 1 {
		t.Errorf("index card = %d, want 1 (a only)", total)
	}
}

func TestPagePagination(t *testing.T) {
	h, _ := newTestHeat(t)
	ctx := context.Background()
	h.Seed(ctx, seeds("a", "b", "c", "d"), "v1")
	h.client.ZIncrBy(ctx, dayKey(time.Now().UTC()), 10, "a")
	h.aggregate(ctx)

	// 2件目から2件 → c, b（a が先頭なので offset=1 で c,b）
	items, total, err := h.Page(ctx, 1, 2)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if total != 4 {
		t.Errorf("total = %d, want 4", total)
	}
	if len(items) != 2 {
		t.Fatalf("got %d items, want 2", len(items))
	}
}

func TestHitIncrementsDayKey(t *testing.T) {
	h, mr := newTestHeat(t)
	h.Hit("x", "y", "x")

	// Hit は非同期なので少し待ってから確認する。
	today := dayKey(time.Now().UTC())
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if mr.Exists(today) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	ctx := context.Background()
	if score, _ := h.client.ZScore(ctx, today, "x").Result(); score != 2 {
		t.Errorf("hit count for x = %v, want 2", score)
	}
	if score, _ := h.client.ZScore(ctx, today, "y").Result(); score != 1 {
		t.Errorf("hit count for y = %v, want 1", score)
	}
	// TTL が設定されていること。
	if ttl := mr.TTL(today); ttl <= 0 {
		t.Errorf("day key TTL not set: %v", ttl)
	}
}
