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

	"github.com/Iktahana/Genji/api/internal/redisx"
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
	client     *redis.Client
	ownsClient bool // true なら Close() で client を閉じる
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

func (c *RedisCache) Close() error {
	if c.ownsClient {
		return c.client.Close()
	}
	return nil
}

// New は自前で Redis に接続してキャッシュを構築する（client を所有し Close で閉じる）。
//
// addr が空、または接続失敗時はエラーで落とさず NoopCache にフォールバックする。
func New(addr, password string, db int) Cache {
	client := redisx.Connect(addr, password, db)
	if client == nil {
		return NoopCache{}
	}
	return &RedisCache{client: client, ownsClient: true}
}

// NewWithClient は既存の共有 Redis クライアントからキャッシュを構築する。
//
// client が nil なら NoopCache を返す。client の Close は呼び出し側が管理する
// （Close() は何もしない）。
func NewWithClient(client *redis.Client) Cache {
	if client == nil {
		log.Println("cache: disabled (no redis client)")
		return NoopCache{}
	}
	log.Println("cache: enabled")
	return &RedisCache{client: client, ownsClient: false}
}
