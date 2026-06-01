package heat

import (
	"context"
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
	return &redisHeat{client: client, w30: 2, w365: 1, aggInterval: time.Minute}, mr
}

func TestNewNilClientIsNoop(t *testing.T) {
	s := New(nil, 2, 1, time.Minute)
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
	if err := h.Seed(ctx, []string{"a", "b", "c"}, "v1"); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	n, _ := h.client.ZCard(ctx, entriesAllKey).Result()
	if n != 3 {
		t.Errorf("entries:all card = %d, want 3", n)
	}
	// 同バージョンの再 seed はスキップされる（変更なし）。
	if err := h.Seed(ctx, []string{"a"}, "v1"); err != nil {
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
	if err := h.Seed(ctx, []string{"a", "b", "c", "d"}, "v1"); err != nil {
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

func TestPagePagination(t *testing.T) {
	h, _ := newTestHeat(t)
	ctx := context.Background()
	h.Seed(ctx, []string{"a", "b", "c", "d"}, "v1")
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
