// Package api は OpenAPI 仕様から生成されたサーバーコードと、その実装を保持する。
package api

//go:generate go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen -config ../../oapi-codegen.yaml ../../openapi.yaml
//go:generate cp ../../openapi.yaml openapi.yaml
