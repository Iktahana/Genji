// Command genji-api は Genji 辞書データベースの REST API サーバー。
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Iktahana/Genji/api/internal/api"
	"github.com/Iktahana/Genji/api/internal/cache"
	"github.com/Iktahana/Genji/api/internal/config"
	"github.com/Iktahana/Genji/api/internal/server"
	"github.com/Iktahana/Genji/api/internal/store"
)

func main() {
	cfg := config.Load()

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		log.Fatalf("failed to open database %q: %v", cfg.DBPath, err)
	}
	defer st.Close()
	log.Printf("database opened: %s", cfg.DBPath)

	c := cache.New(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	defer c.Close()

	handler := server.NewHandler(st, c, cfg.CacheTTL)

	router := newRouter(handler)

	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: router,
	}

	// graceful shutdown
	go func() {
		log.Printf("listening on :%s", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}

func newRouter(handler *server.Handler) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	r.Use(corsMiddleware())
	r.Use(noIndexMiddleware())

	// ドキュメント配信・robots.txt
	api.RegisterDocs(r)

	// 生成された strict ハンドラを配線
	strict := api.NewStrictHandler(handler, nil)
	api.RegisterHandlers(r, strict)

	return r
}

// noIndexMiddleware は全レスポンスに X-Robots-Tag: noindex を付与し、
// 検索エンジンによる API レスポンスのインデックスを防ぐ（robots.txt と合わせた二重防御）。
func noIndexMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("X-Robots-Tag", "noindex, nofollow")
		c.Next()
	}
}

// corsMiddleware は Datasette の datasette-cors と同等に全オリジンを許可する。
func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type")
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}
