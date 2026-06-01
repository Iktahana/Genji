package cache

import (
	"context"
	"testing"
	"time"
)

func TestNoopCache(t *testing.T) {
	var c Cache = NoopCache{}
	ctx := context.Background()

	if c.Enabled() {
		t.Error("NoopCache.Enabled() = true, want false")
	}
	// Set は何もせず、Get は常に miss。
	c.Set(ctx, "k", []byte("v"), time.Minute)
	if _, ok := c.Get(ctx, "k"); ok {
		t.Error("NoopCache.Get returned a hit, want miss")
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
}

func TestNewWithoutRedisIsNoop(t *testing.T) {
	// addr が空なら NoopCache（接続を試みない）。
	c := New("", "", 0)
	if c.Enabled() {
		t.Error("New(\"\") should be disabled")
	}
	if _, ok := c.Get(context.Background(), "any"); ok {
		t.Error("disabled cache should always miss")
	}
}

func TestNewWithUnreachableRedisFallsBack(t *testing.T) {
	// 到達不能な addr でもエラーで落とさず Noop にフォールバックする。
	c := New("127.0.0.1:1", "", 0)
	if c.Enabled() {
		t.Error("unreachable redis should fall back to disabled cache")
	}
}
