# Genji API (Go + Gin) ビルド自動化
#
# FTS5 を使うため mattn/go-sqlite3 には sqlite_fts5 ビルドタグが必須。
# cgo も必須（CGO_ENABLED=1）。

BINARY      := genji-api
PKG         := ./cmd/genji-api
TAGS        := sqlite_fts5
export CGO_ENABLED := 1

.PHONY: help generate build run test vet tidy clean

help: ## このヘルプを表示
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

generate: ## OpenAPI からコードを生成し openapi.yaml を同梱用にコピー
	go generate ./...

build: ## バイナリをビルド
	go build -tags "$(TAGS)" -o bin/$(BINARY) $(PKG)

run: ## ローカル起動（GENJI_DB_PATH で DB を指定）
	go run -tags "$(TAGS)" $(PKG)

test: ## テスト実行
	go test -tags "$(TAGS)" ./...

vet: ## go vet
	go vet -tags "$(TAGS)" ./...

tidy: ## 依存整理
	go mod tidy

clean: ## 生成バイナリを削除
	rm -rf bin
