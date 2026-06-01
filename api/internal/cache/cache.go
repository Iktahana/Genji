// Package cache はクエリ結果のキャッシュを提供する。
//
// Redis が設定されていれば RedisCache を、未設定または接続失敗時は
// NoopCache（常に miss）を使う。これにより API の可用性は Redis に依存しない。
package cache

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cache はキャッシュの抽象。
type Cache interface {
	// Get はキーに対応する値を返す。存在しなければ ok=false。
	Get(ctx context.Context, key string) ([]byte, bool)
	// Set は値を ttl の期限付きで保存する。失敗しても致命的に扱わない。
	Set(ctx context.Context, key string, val []byte, ttl time.Duration)
	// Enabled はキャッシュが実際に有効かどうかを返す。
	Enabled() bool
	// Close はバックエンド接続を閉じる。
	Close() error
}

// NoopCache は何もキャッシュしない実装。
type NoopCache struct{}

func (NoopCache) Get(context.Context, string) ([]byte, bool)        { return nil, false }
func (NoopCache) Set(context.Context, string, []byte, time.Duration) {}
func (NoopCache) Enabled() bool                                     { return false }
func (NoopCache) Close() error                                      { return nil }

// RedisCache は Redis を使う実装。
type RedisCache struct {
	client *redis.Client
}

func (c *RedisCache) Get(ctx context.Context, key string) ([]byte, bool) {
	val, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		return nil, false
	}
	return val, true
}

func (c *RedisCache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) {
	if err := c.client.Set(ctx, key, val, ttl).Err(); err != nil {
		log.Printf("cache: set failed for %q: %v", key, err)
	}
}

func (c *RedisCache) Enabled() bool { return true }

func (c *RedisCache) Close() error { return c.client.Close() }

// New はキャッシュを構築する。
//
// addr が空なら NoopCache を返す。addr 指定時は接続して PING を試み、
// 失敗した場合はエラーで落とさず NoopCache にフォールバックする。
func New(addr, password string, db int) Cache {
	if addr == "" {
		log.Println("cache: disabled (GENJI_REDIS_ADDR not set)")
		return NoopCache{}
	}

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Printf("cache: redis ping failed (%v), falling back to disabled cache", err)
		_ = client.Close()
		return NoopCache{}
	}

	log.Printf("cache: enabled (redis %s db=%d)", addr, db)
	return &RedisCache{client: client}
}
