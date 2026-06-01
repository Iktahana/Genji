package api

import (
	_ "embed"
	"net/http"

	"github.com/gin-gonic/gin"
)

// openAPISpec はビルド時にバイナリへ同梱される OpenAPI 仕様。
// `go generate` 時に ../../openapi.yaml からコピーされる。
//
//go:embed openapi.yaml
var openAPISpec []byte

// redocHTML は Redoc を CDN から読み込み /openapi.yaml を表示する最小 HTML。
const redocHTML = `<!DOCTYPE html>
<html>
  <head>
    <title>『幻辞』 Genji API — Docs</title>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>body { margin: 0; padding: 0; }</style>
  </head>
  <body>
    <redoc spec-url="/openapi.yaml"></redoc>
    <script src="https://cdn.redoc.ly/redoc/latest/bundles/redoc.standalone.js"></script>
  </body>
</html>`

// robotsTxt は全クローラに対し API のクロールを禁止し、
// 代わりに前端サイト（https://dict.illusions.app）へ誘導する。
// API は機械可読データを返すため、検索エンジンにインデックスさせない。
const robotsTxt = `# このホストは Genji の機械可読 API です。検索エンジンによるクロールは行いません。
# コンテンツのインデックスは前端サイトをご利用ください: https://dict.illusions.app
User-agent: *
Disallow: /

Sitemap: https://dict.illusions.app/sitemap.xml
`

// RegisterDocs はドキュメント配信用ルート（/openapi.yaml・/docs・/robots.txt）を登録する。
func RegisterDocs(r gin.IRouter) {
	r.GET("/openapi.yaml", func(c *gin.Context) {
		c.Data(http.StatusOK, "application/yaml; charset=utf-8", openAPISpec)
	})
	r.GET("/docs", func(c *gin.Context) {
		c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(redocHTML))
	})
	r.GET("/robots.txt", func(c *gin.Context) {
		c.Data(http.StatusOK, "text/plain; charset=utf-8", []byte(robotsTxt))
	})
}
