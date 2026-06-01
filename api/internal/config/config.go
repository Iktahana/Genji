// Package config は環境変数から実行時設定を読み込む。
package config

import (
	"os"
	"strconv"
	"time"
)

// Config はサーバーの実行時設定。
type Config struct {
	// DBPath は genji.db (SQLite) へのパス。
	DBPath string
	// Port は HTTP リッスンポート。
	Port string

	// RedisAddr が空ならキャッシュ無効（Noop）。
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	// CacheTTL はキャッシュ項目の有効期限。
	CacheTTL time.Duration
}

// Load は環境変数から Config を構築する。未設定の項目には既定値を使う。
func Load() Config {
	return Config{
		DBPath:        getenv("GENJI_DB_PATH", "genji.db"),
		Port:          getenv("PORT", "8080"),
		RedisAddr:     os.Getenv("GENJI_REDIS_ADDR"),
		RedisPassword: os.Getenv("GENJI_REDIS_PASSWORD"),
		RedisDB:       getenvInt("GENJI_REDIS_DB", 0),
		CacheTTL:      getenvDuration("GENJI_CACHE_TTL", time.Hour),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getenvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
