// Package redisx は Redis クライアントの接続ヘルパーを提供する。
//
// 接続先が未指定、または疎通に失敗した場合は nil を返す（エラーで落とさない）。
// 呼び出し側はこの単一クライアントを cache と heat で共有し、自身で Close する。
package redisx

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// Connect は Redis に接続する。addr が空、または PING 失敗時は nil を返す。
func Connect(addr, password string, db int) *redis.Client {
	if addr == "" {
		log.Println("redis: disabled (GENJI_REDIS_ADDR not set)")
		return nil
	}
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Printf("redis: ping failed (%v), running without redis", err)
		_ = client.Close()
		return nil
	}
	log.Printf("redis: connected (%s db=%d)", addr, db)
	return client
}
