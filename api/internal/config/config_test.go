package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	// 関連する環境変数をすべてクリアして既定値を確認する。
	for _, k := range []string{
		"GENJI_DB_PATH", "PORT", "GENJI_REDIS_ADDR",
		"GENJI_REDIS_PASSWORD", "GENJI_REDIS_DB", "GENJI_CACHE_TTL",
	} {
		t.Setenv(k, "")
	}

	cfg := Load()
	if cfg.DBPath != "genji.db" {
		t.Errorf("DBPath = %q, want genji.db", cfg.DBPath)
	}
	if cfg.Port != "8080" {
		t.Errorf("Port = %q, want 8080", cfg.Port)
	}
	if cfg.RedisAddr != "" {
		t.Errorf("RedisAddr = %q, want empty", cfg.RedisAddr)
	}
	if cfg.RedisDB != 0 {
		t.Errorf("RedisDB = %d, want 0", cfg.RedisDB)
	}
	if cfg.CacheTTL != time.Hour {
		t.Errorf("CacheTTL = %v, want 1h", cfg.CacheTTL)
	}
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("GENJI_DB_PATH", "/data/genji.db")
	t.Setenv("PORT", "9000")
	t.Setenv("GENJI_REDIS_ADDR", "redis:6379")
	t.Setenv("GENJI_REDIS_PASSWORD", "secret")
	t.Setenv("GENJI_REDIS_DB", "3")
	t.Setenv("GENJI_CACHE_TTL", "30m")

	cfg := Load()
	if cfg.DBPath != "/data/genji.db" {
		t.Errorf("DBPath = %q", cfg.DBPath)
	}
	if cfg.Port != "9000" {
		t.Errorf("Port = %q", cfg.Port)
	}
	if cfg.RedisAddr != "redis:6379" {
		t.Errorf("RedisAddr = %q", cfg.RedisAddr)
	}
	if cfg.RedisPassword != "secret" {
		t.Errorf("RedisPassword = %q", cfg.RedisPassword)
	}
	if cfg.RedisDB != 3 {
		t.Errorf("RedisDB = %d, want 3", cfg.RedisDB)
	}
	if cfg.CacheTTL != 30*time.Minute {
		t.Errorf("CacheTTL = %v, want 30m", cfg.CacheTTL)
	}
}

func TestLoadInvalidValuesFallBackToDefault(t *testing.T) {
	t.Setenv("GENJI_REDIS_DB", "not-a-number")
	t.Setenv("GENJI_CACHE_TTL", "garbage")

	cfg := Load()
	if cfg.RedisDB != 0 {
		t.Errorf("RedisDB = %d, want default 0 on parse error", cfg.RedisDB)
	}
	if cfg.CacheTTL != time.Hour {
		t.Errorf("CacheTTL = %v, want default 1h on parse error", cfg.CacheTTL)
	}
}
