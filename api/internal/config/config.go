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

	// RedisAddr が空ならキャッシュ・熱度集計とも無効（Noop）。
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	// CacheTTL はキャッシュ項目の有効期限。
	CacheTTL time.Duration

	// HeatAggInterval は熱度ランキングの集計間隔。
	HeatAggInterval time.Duration
	// HeatW30 / HeatW365 は 30日 / 365日アクセス数の重み。
	HeatW30  float64
	HeatW365 float64
}

// Load は環境変数から Config を構築する。未設定の項目には既定値を使う。
func Load() Config {
	return Config{
		DBPath:          getenv("GENJI_DB_PATH", "genji.db"),
		Port:            getenv("PORT", "8080"),
		RedisAddr:       os.Getenv("GENJI_REDIS_ADDR"),
		RedisPassword:   os.Getenv("GENJI_REDIS_PASSWORD"),
		RedisDB:         getenvInt("GENJI_REDIS_DB", 0),
		CacheTTL:        getenvDuration("GENJI_CACHE_TTL", time.Hour),
		HeatAggInterval: getenvDuration("GENJI_HEAT_AGG_INTERVAL", 15*time.Minute),
		HeatW30:         getenvFloat("GENJI_HEAT_W30", 2),
		HeatW365:        getenvFloat("GENJI_HEAT_W365", 1),
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

func getenvFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
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
