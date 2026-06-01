//go:build tools

// Package tools は開発用ツールの依存を go.mod に固定するためのもの。
// ビルドには含まれない（tools ビルドタグ）。
package tools

import (
	_ "github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen"
)
